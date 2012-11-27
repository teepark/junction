from __future__ import absolute_import

import collections
import inspect
import logging
import sys
import traceback

from . import backend, connection, const
from .. import errors, hooks


log = logging.getLogger("junction.dispatch")

STOP = object()


class Dispatcher(object):
    def __init__(self, rpc_client, hub, hooks=None):
        self.rpc_client = rpc_client
        self.hub = hub
        self.hooks = hooks
        self.peer_subs = {}
        self.local_subs = {}
        self.clients = {}
        self.peers = {}
        self.reconnecting = {}
        self.inflight_proxies = {}
        self.proxying_channels = {}
        self.received_channels = {}
        self.outgoing_channels = {}

    def add_local_subscription(self, msg_type, service, mask, value, method,
            handler, schedule):
        # storage in local_subs is shaped like so:
        # {(msg_type, service): [
        #     (mask, value, {method: (handler, schedule), ...}), ...], ...}

        # sanity check that no 1 bits in the value would be masked out.
        # in that case, there is no routing id that could possibly match
        if value & ~mask:
            raise errors.ImpossibleSubscription(msg_type, service, mask, value)

        existing = self.local_subs.setdefault((msg_type, service), [])
        for pmask, pvalue, phandlers in existing:
            if pmask & value == mask & pvalue:
                if method in phandlers:
                    # (mask, value) overlaps with a previous
                    # subscription with the same method
                    raise errors.OverlappingSubscription(
                            (msg_type, service, mask, value, method),
                            (msg_type, service, pmask, pvalue, method))
                elif mask == pmask and value == pvalue:
                    # same (mask, value) as a previous subscription but for a
                    # different method, so piggy-back on that data structure
                    phandlers[method] = (handler, schedule)

                    # also bail out. we can skip the MSG_TYPE_ANNOUNCE
                    # below b/c peers don't route with their peers' methods
                    return

        existing.append((mask, value, {method: (handler, schedule)}))

        # let peers know about the new subscription
        for peer in self.peers.itervalues():
            if not peer.up:
                continue
            peer.push((const.MSG_TYPE_ANNOUNCE,
                    (msg_type, service, mask, value)))

    def remove_local_subscription(self, msg_type, service, mask, value):
        group = self.local_subs.get((msg_type, service), 0)
        if not group:
            return False
        for i, (pmask, pvalue, phandlers) in enumerate(group):
            if (mask, value) == (pmask, pvalue):
                del group[i]
                if not group:
                    del self.local_subs[(msg_type, service)]
                for peer in self.peers.itervalues():
                    if not peer.up:
                        continue
                    peer.push((const.MSG_TYPE_UNSUBSCRIBE,
                        (msg_type, service, mask, value)))
                return True
        return False

    def incoming_unsubscribe(self, peer, msg):
        if not isinstance(msg, tuple) or len(msg) != 4:
            # badly formatted message
            log.warn("received malformed unsubscribe from %r" % (peer.ident,))
            return

        log.debug("received unsubscribe %r from %r" % (msg, peer.ident))

        msg_type, service, mask, value = msg

        groups = self.peer_subs.get((msg_type, service), 0)
        if not groups:
            log.warn(("unsubscribe from %r described an unrecognized" +
                    " subscription (msg_type, service)") % (peer.ident,))
            return
        for i, group in enumerate(groups):
            if (mask, value, peer) == group:
                del groups[i]
                if not groups:
                    del self.peer_subs[(msg_type, service)]
                break
        else:
            log.warn(("unsubscribe from %r described an " +
                    "unrecognized subscription %r") % (peer.ident, msg))

    def find_local_handler(self, msg_type, service, routing_id, method):
        group = self.local_subs.get((msg_type, service), 0)
        if not group:
            return None, False
        for mask, value, handlers in group:
            if routing_id & mask == value and method in handlers:
                return handlers[method]
        return None, False

    def locally_handles(self, msg_type, service, routing_id):
        group = self.local_subs.get((msg_type, service), [])
        if not group:
            return False
        for mask, value, handlers in group:
            if routing_id & mask == value:
                return True
        return False

    def multipush(self, targets, msg):
        for target in targets:
            if target.up:
                target.push(msg)

    def multipush_str(self, targets, msg):
        for target in targets:
            if target.up:
                target.push_string(msg)

    def local_subscriptions(self):
        for key, value in self.local_subs.iteritems():
            msg_type, service = key
            for mask, value, handlers in value:
                yield (msg_type, service, mask, value)

    def add_reconnecting(self, addr, peer):
        self.reconnecting[addr] = peer

    def store_peer(self, peer, subscriptions):
        loser = None
        if peer.ident in self.peers:
            winner, loser = connection.compare(peer, self.peers[peer.ident])
            if peer is loser:
                winner.established.set()
                peer.established.set()
                return False
        elif peer.ident in self.reconnecting and not peer.initiator:
            log.info("terminating reconnect loop in favor of incoming conn")
            loser = self.reconnecting.pop(peer.ident)

        peer.established.set()
        if loser is not None:
            loser.established.set()
            loser.go_down(reconnect=False, expected=True)

        self.peers[peer.ident] = peer
        self.add_peer_subscriptions(peer, subscriptions)
        self.connection_received(peer, subscriptions)
        return True

    def connection_received(self, peer, subs):
        if not peer.initiator and peer.ident not in self.hub._started_peers:
            backend.schedule(hooks._get(self.hooks, "connection_received"),
                    (peer.ident, subs))

    def connection_lost(self, peer, subs):
        backend.schedule(hooks._get(self.hooks, "connection_lost"),
                (peer.ident, subs))

    def drop_peer(self, peer):
        self.peers.pop(peer.ident, None)
        subs = self.drop_peer_subscriptions(peer)

        channels = self.proxying_channels.pop(peer.ident, {})
        for source_counter, entry in channels.iteritems():
            self.multipush(entry['targets'],
                (entry['type'] + 3, const.RPC_ERR_LOST_CONN, None))

        # reply to all in-flight proxied RPCs to the dropped peer
        # with the "lost connection" error
        for counter in self.rpc_client.by_peer.get(id(peer), []):
            if counter in self.inflight_proxies:
                self.proxied_response(counter, const.RPC_ERR_LOST_CONN, None)

        peer_ident = peer.ident or id(peer)

        # stop sender greenlets for any outgoing chunked messages to this peer
        channels = self.outgoing_channels.pop(peer_ident, {})
        for msgtype, counter in channels.keys():
            backend.end(channels.pop((msgtype, counter)))

        # give a LostConnection error to any in-progress
        # chunked messages and cork them with a STOP
        channels = self.received_channels.get(peer_ident, {})
        for (msgtype, counter), (ev, deq) in channels.items():
            self.handle_chunk_arrival(peer_ident or id(peer), msgtype, counter,
                    1, errors.LostConnection(peer.ident))

        self.rpc_client.connection_down(peer)
        return subs

    def register_outgoing_channel(self, peers, msgtype, counter, glet):
        for peer in peers:
            if isinstance(peer, LocalTarget):
                continue
            peer_addr = peer.ident or id(peer)
            bypeer = self.outgoing_channels.setdefault(peer_addr, {})
            bypeer[(msgtype, counter)] = glet

    def unregister_outgoing_channel(self, peers, msgtype, counter):
        for peer in peers:
            if isinstance(peer, LocalTarget):
                continue
            peer_addr = peer.ident or id(peer)
            bypeer = self.outgoing_channels.setdefault(peer_addr, {})
            if bypeer.pop((msgtype, counter), None) and not bypeer:
                del self.outgoing_channels[peer_addr]

    def incoming_announce(self, peer, msg):
        if not isinstance(msg, tuple) or len(msg) != 4:
            # drop malformed messages
            log.warn("received malformed announce from %r" % (peer.ident,))
            return

        log.debug("received announce %r from %r" % (msg, peer.ident))

        self.add_peer_subscriptions(peer, [msg])

    def add_peer_subscriptions(self, peer, subscriptions):
        # format for peer_subs:
        # {(msg_type, service): [(mask, value, connection)]}
        for msg_type, service, mask, value in subscriptions:
            self.peer_subs.setdefault((msg_type, service), []).append(
                    (mask, value, peer))

    def drop_peer_subscriptions(self, peer):
        removed = []
        for (msg_type, service), subs in self.peer_subs.items():
            for (mask, value, conn) in subs:
                if conn is peer:
                    removed.append((msg_type, service, mask, value))
                    subs.remove((mask, value, conn))
            if not subs:
                del self.peer_subs[(msg_type, service)]
        return removed

    def find_peer_routes(self, msg_type, service, routing_id):
        for mask, value, peer in self.peer_subs.get((msg_type, service), []):
            if peer.up and routing_id & mask == value:
                yield peer

    def send_publish(self, client, service, routing_id, method, args, kwargs,
            forwarded=False, singular=False):
        # get the peers registered for this publish
        peers = list(self.find_peer_routes(
                const.MSG_TYPE_PUBLISH, service, routing_id))

        # handle locally if we have a hander for it
        handler, schedule = self.find_local_handler(
                const.MSG_TYPE_PUBLISH, service, routing_id, method)

        targets = peers[:]
        if handler:
            targets.append(LocalTarget(self, handler, schedule, client))

        if singular:
            targets = [self.target_selection(
                targets, service, routing_id, method)]
            if not isinstance(targets[0], LocalTarget):
                handler = None

        if args and hasattr(args[0], "__iter__") \
                and not hasattr(args[0], "__len__"):
            counter = self.rpc_client.next_counter()
            glet = backend.greenlet(self.send_chunked_publish,
                    (service, routing_id, method, counter,
                        args, kwargs, targets, False))
            self.register_outgoing_channel(peers,
                    const.MSG_TYPE_PUBLISH_IS_CHUNKED, counter, glet)
            backend.schedule(glet)
            return bool(handler or peers)

        msg = (const.MSG_TYPE_PUBLISH,
                (service, routing_id, method, args, kwargs))

        if handler is not None:
            log.debug("locally handling publish %r %s" %
                    (msg[1][:3], "scheduled" if schedule else "immediately"))

        if peers and not (singular and handler):
            log.debug("sending publish %r to %d peers" % (
                msg[1][:3], len(peers)))

        self.multipush(targets, msg)

        return bool(handler or peers)

    def send_chunked_publish(self, service, routing_id, method,
            counter, args, kwargs, targets, proxied=False):
        chunks, args = args[0], args[1:]
        msgtype = const.MSG_TYPE_PUBLISH_IS_CHUNKED
        if proxied:
            msgtype += 9

        log.debug("sending publish_is_chunked %r" %
                ((service, routing_id, method, counter),))
        self.multipush(targets, (msgtype,
                (service, routing_id, method, counter, args, kwargs)))

        chunks = iter(chunks)
        err = False
        while not err:
            try:
                chunk = chunks.next()
                rc = 0
            except StopIteration:
                break
            except errors.HandledError, exc:
                log.error("sending RPC_ERR_KNOWN(%d) as final publish chunk" %
                        (exc.code,))
                rc = const.RPC_ERR_KNOWN
                chunk = (exc.code, exc.args)
                backend.handle_exception(*sys.exc_info())
                err = True
            except Exception:
                log.error("sending RPC_ERR_UNKNOWN as final publish chunk")
                rc = const.RPC_ERR_UNKOWN
                chunk = ''.join(traceback.format_exception(*sys.exc_info()))
                backend.handle_exception(*sys.exc_info())
                err = True

            try:
                msg = connection.dump((msgtype + 3, (counter, rc, chunk)))
            except TypeError:
                log.error("sending RPC_ERR_UNSER_RESP as final publish chunk")
                msg = connection.dump((msgtype + 3,
                        (counter, const.RPC_ERR_UNSER_RESP, repr(chunk))))
                err = True

            if not err:
                log.debug("sending publish_chunk %r" % ((counter, rc),))

            self.multipush_str(targets, msg)
            backend.pause()

        if not err:
            log.debug("sending publish_end_chunks %d" % counter)
            self.multipush(targets, (msgtype + 6, counter))

        self.unregister_outgoing_channel(targets,
                const.MSG_TYPE_PUBLISH_IS_CHUNKED, counter)

    def cleanup_forwarded_chunk(self, peer_ident, counter):
        entry = self.proxying_channels.get(peer_ident, {}).pop(counter, None)
        if entry is not None and not self.proxying_channels[peer_ident]:
            del self.proxying_channels[peer_ident]
        return entry

    def send_proxied_rpc(
            self, service, routing_id, method, args, kwargs, singular):
        if args and hasattr(args[0], '__iter__') and \
                not hasattr(args[0], '__len__'):
            log.debug("sending proxied chunked rpc %r" %
                    ((service, routing_id, method),))
            counter = self.rpc_client.next_counter()
            routes = [self.peers.values()[0]]
            rpc = self.rpc_client.chunked_request(counter, routes, singular)
            if rpc:
                glet = backend.greenlet(self.send_chunked_rpc,
                        args=(service, routing_id, method, args, kwargs,
                            routes, counter, singular, True))
                self.register_outgoing_channel(routes,
                        const.MSG_TYPE_REQUEST_IS_CHUNKED, counter, glet)
                backend.schedule(glet)
            return rpc

        log.debug("sending proxied_rpc %r" % ((service, routing_id, method),))
        return self.rpc_client.request(
                [self.peers.values()[0]],
                (service, routing_id, method, bool(singular), args, kwargs),
                singular)

    def target_selection(self, peers, service, routing_id, method):
        by_addr = {}
        for peer in peers:
            if isinstance(peer, LocalTarget):
                by_addr[None] = peer
            else:
                by_addr[peer.ident] = peer
        choice = hooks._get(self.hooks, 'select_peer')(
                by_addr.keys(), service, routing_id, method)
        return by_addr[choice]

    def send_rpc(self, service, routing_id, method, args, kwargs,
            singular):
        handler, schedule = self.find_local_handler(
                const.MSG_TYPE_RPC_REQUEST, service, routing_id, method)
        routes = []
        if handler is not None:
            routes.append(LocalTarget(self, handler, schedule))

        peers = list(self.find_peer_routes(
            const.MSG_TYPE_RPC_REQUEST, service, routing_id))
        routes.extend(peers)

        if singular and len(peers) > 1:
            routes = [self.target_selection(
                    routes, service, routing_id, method)]
            if not isinstance(routes[0], LocalTarget):
                handler = None

        if handler is not None:
            log.debug("locally handling rpc_request %r %s" %
                    ((service, routing_id, method),
                    "scheduled" if schedule else "immediately"))

        if peers and not (singular and handler):
            log.debug("sending rpc_request %r to %d peers" %
                    ((service, routing_id, method),
                    len(routes) - bool(handler)))

        if args and hasattr(args[0], '__iter__') and \
                not hasattr(args[0], '__len__'):
            counter = self.rpc_client.next_counter()
            rpc = self.rpc_client.chunked_request(counter, routes, singular)
            if rpc:
                glet = backend.greenlet(self.send_chunked_rpc,
                        args=(service, routing_id, method, args, kwargs,
                            routes, counter, singular))
                self.register_outgoing_channel(peers,
                        const.MSG_TYPE_REQUEST_IS_CHUNKED, counter, glet)
                backend.schedule(glet)
            return rpc

        return self.rpc_client.request(
                routes, (service, routing_id, method, args, kwargs), singular)

    def send_chunked_rpc(self, service, routing_id, method, args, kwargs,
            targets, counter, singular=False, proxied=False):
        chunks, args = args[0], args[1:]
        msgtype = const.MSG_TYPE_REQUEST_IS_CHUNKED
        if proxied:
            msgtype += 9
            is_chunked_msg = (msgtype, (service, routing_id, method, singular,
                    counter, args, kwargs))
        else:
            is_chunked_msg = (msgtype,
                    (service, routing_id, method, counter, args, kwargs))
        self.multipush_str(targets, connection.dump(is_chunked_msg))

        chunks = iter(chunks)
        err = False
        while not err:
            try:
                chunk = chunks.next()
                rc = 0
            except StopIteration:
                break
            except errors.HandledError, exc:
                log.error("sending RPC_ERR_KNOWN(%d) as final request chunk" %
                        (exc.code,))
                rc = const.RPC_ERR_KNOWN
                chunk = (exc.code, exc.args)
                backend.handle_exception(*sys.exc_info())
                err = True
            except Exception:
                log.error("sending RPC_ERR_UNKNOWN as final request chunk")
                rc = const.RPC_ERR_UNKNOWN
                chunk = ''.join(traceback.format_exception(*sys.exc_info()))
                backend.handle_exception(*sys.exc_info())
                err = True

            try:
                msg = connection.dump((msgtype + 3, (counter, rc, chunk)))
            except TypeError:
                log.error("sending RPC_ERR_UNSER_RESP as final request chunk")
                msg = connection.dump((msgtype + 3,
                        (counter, const.RPC_ERR_UNSER_RESP, repr(chunk))))
                err = True

            self.multipush_str(targets, msg)
            if not err:
                backend.pause()

        if not err:
            self.multipush(targets, (msgtype + 6, counter))

        self.unregister_outgoing_channel(targets,
                const.MSG_TYPE_REQUEST_IS_CHUNKED, counter)

    def send_chunked_response(self, peer, counter, chunks, proxied):
        msgtype = const.MSG_TYPE_RESPONSE_IS_CHUNKED
        ident = self.hub._ident
        if proxied:
            msgtype += 9

        msg = (counter, ident)
        if not proxied: msg = msg[0]
        peer.push((msgtype, msg))

        chunks = iter(chunks)
        err = False
        prefix = (ident,) if proxied else ()
        while not err:
            try:
                chunk = chunks.next()
                rc = 0
            except StopIteration:
                break
            except errors.HandledError, exc:
                log.error("sending RPC_ERR_KNOWN(%d) as final response chunk" %
                        (exc.code,))
                rc = const.RPC_ERR_KNOWN
                chunk = (exc.code, exc.args)
                backend.handle_exception(*sys.exc_info())
                err = True
            except Exception:
                log.error("sending RPC_ERR_UNKNOWN as final response chunk")
                rc = const.RPC_ERR_UNKNOWN
                chunk = ''.join(traceback.format_exception(*sys.exc_info()))
                backend.handle_exception(*sys.exc_info())
                err = True

            try:
                msg = peer.dump((msgtype + 3, prefix + (counter, rc, chunk)))
            except TypeError:
                log.error("sending RPC_ERR_UNSER_RESP as final response chunk")
                msg = peer.dump((msgtype + 3,
                    prefix + (counter, const.RPC_ERR_UNSER_RESP, repr(chunk))))
                err = True

            peer.push_string(msg)
            if not err:
                backend.pause()

        if not err:
            msg = (counter, ident)
            if not proxied: msg = msg[0]
            peer.push((msgtype + 6, msg))

        self.unregister_outgoing_channel([peer],
                const.MSG_TYPE_RESPONSE_IS_CHUNKED, counter)


    def send_proxied_publish(self, service, routing_id, method, args, kwargs,
            singular=False):
        log.debug("sending proxied_publish %r" %
                ((service, routing_id, method),))
        peer = self.peers.values()[0]
        if args and hasattr(args[0], "__iter__") \
                and not hasattr(args[0], "__len__"):
            counter = self.rpc_client.next_counter()
            glet = backend.greenlet(self.send_chunked_publish,
                    args=(service, routing_id, method, counter,
                        args, kwargs, [peer], True))
            self.register_outgoing_channel([peer],
                    const.MSG_TYPE_PUBLISH_IS_CHUNKED, counter, glet)
            backend.schedule(glet)
        else:
            peer.push((const.MSG_TYPE_PROXY_PUBLISH,
                    (service, routing_id, method, args, kwargs, singular)))

    def publish_handler(self, handler, msg, source, args, kwargs):
        log.debug("executing publish handler for %r from %r" % (msg, source))
        try:
            handler(*args, **kwargs)
        except Exception:
            log.error("exception handling publish %r from %r" % (msg, source))
            backend.handle_exception(*sys.exc_info())

    def rpc_handler(self, peer, counter, handler, args, kwargs,
            proxied=False, scheduled=False):
        req_type = "proxy_request" if proxied else "rpc_request"
        log.debug("executing %s handler for %d from %r" %
                (req_type, counter, peer.ident))

        response = (proxied and const.MSG_TYPE_PROXY_RESPONSE
                or const.MSG_TYPE_RPC_RESPONSE)

        try:
            rc = 0
            result = handler(*args, **kwargs)
        except errors.HandledError, exc:
            log.error("responding with RPC_ERR_KNOWN (%d) to %s %d" %
                    (exc.code, req_type, counter))
            rc = const.RPC_ERR_KNOWN
            result = (exc.code, exc.args)
            backend.handle_exception(*sys.exc_info())
        except TypeError:
            if len(traceback.extract_tb(sys.exc_info()[2])) == 1:
                log.error("responding with RPC_ERR_BADARGS to %s %d" %
                        (req_type, counter))
                rc = const.RPC_ERR_BADARGS
                spec = inspect.getargspec(handler)
                result = (len(spec.args) - len(spec.defaults or ()),
                        (spec.defaults or ())[len(spec.args):],
                        bool(spec.varargs), bool(spec.keywords))
                backend.handle_exception(*sys.exc_info())
            else:
                log.error("responding with RPC_ERR_UNKNOWN to %s %d" %
                        (req_type, counter))
                rc = const.RPC_ERR_UNKNOWN
                result = ''.join(traceback.format_exception(*sys.exc_info()))
                backend.handle_exception(*sys.exc_info())
        except Exception:
            log.error("responding with RPC_ERR_UNKNOWN to %s %d" %
                    (req_type, counter))
            rc = const.RPC_ERR_UNKNOWN
            result = ''.join(traceback.format_exception(*sys.exc_info()))
            backend.handle_exception(*sys.exc_info())

        if hasattr(result, "__iter__") and not hasattr(result, "__len__"):
            if scheduled:
                self.register_outgoing_channel([peer],
                        const.MSG_TYPE_RESPONSE_IS_CHUNKED, counter,
                        backend.getcurrent())
                self.send_chunked_response(peer, counter, result, proxied)
            else:
                glet = backend.greenlet(self.send_chunked_response,
                        args=(peer, counter, result, proxied))
                self.register_outgoing_channel([peer],
                        const.MSG_TYPE_RESPONSE_IS_CHUNKED, counter, glet)
                backend.schedule(glet)
            return

        try:
            msg = peer.dump((response, (counter, rc, result)))
        except TypeError:
            log.error("responding with RPC_ERR_UNSER_RESP to %s %d" %
                    (req_type, counter))
            msg = peer.dump((response,
                (counter, const.RPC_ERR_UNSER_RESP, repr(result))))
            backend.handle_exception(*sys.exc_info())
        else:
            log.debug("responding with MSG_TYPE_RESPONSE to %s %d" %
                    (req_type, counter))

        peer.push_string(msg)

    def _generate_received_chunks(self, event, deque):
        while 1:
            while deque:
                item = deque.popleft()
                if item is STOP:
                    return
                yield item
            event.wait()

    def handle_start_request_chunks(self, peer, counter, handler, args,
            kwargs, proxied=False, client_counter=None):
        ev = backend.Event()
        deq = collections.deque()
        bypeer = self.received_channels.setdefault(peer.ident or id(peer), {})
        bypeer[(const.MSG_TYPE_REQUEST_IS_CHUNKED, counter)] = (ev, deq)
        gen = self._generate_received_chunks(ev, deq)
        client_counter = client_counter or counter
        backend.schedule(self.rpc_handler,
                args=(peer, client_counter, handler, (gen,) + args, kwargs,
                        proxied, True))

    def handle_start_response_chunks(self, peer_ident, counter):
        ev = backend.Event()
        deq = collections.deque()
        bypeer = self.received_channels.setdefault(peer_ident, {})
        bypeer[(const.MSG_TYPE_RESPONSE_IS_CHUNKED, counter)] = (ev, deq)
        return self._generate_received_chunks(ev, deq)

    def handle_start_publish_chunks(
            self, peer_ident, counter, handler, args, kwargs):
        ev = backend.Event()
        deq = collections.deque()
        bypeer = self.received_channels.setdefault(peer_ident, {})
        bypeer[(const.MSG_TYPE_PUBLISH_IS_CHUNKED, counter)] = (ev, deq)
        gen = self._generate_received_chunks(ev, deq)
        backend.schedule(handler, args=(gen,) + args, kwargs=kwargs)

    def handle_chunk_arrival(self, peer_ident, msgtype, counter, rc, chunk):
        ev, deq = self.received_channels[peer_ident][(msgtype, counter)]
        deq.append(chunk)
        ev.set()
        ev.clear()
        if rc:
            self.cleanup_incoming_chunks(peer_ident, msgtype, counter)

    def cleanup_incoming_chunks(self, peer_ident, msgtype, counter):
        ev, deq = self.received_channels.get(peer_ident, {}).pop(
                (msgtype, counter), (None, None))
        if ev is not None:
            deq.append(STOP)
            ev.set()
            ev.clear()
            if not self.received_channels[peer_ident]:
                del self.received_channels[peer_ident]

    def forward_proxy_response_is_chunked(self, source, source_counter):
        entry = self.inflight_proxies[source_counter]
        entry['awaiting'] -= 1
        if not entry['awaiting']:
            del self.inflight_proxies[source_counter]

        bypeer = self.proxying_channels.setdefault(source.ident, {})
        bypeer[source_counter] = {
            'dest_counter': entry['client_counter'],
            'targets': [entry['peer']],
            'type': const.MSG_TYPE_RESPONSE_IS_CHUNKED,
        }
        self.rpc_client.response(source, source_counter, 0, None)

        log.debug("forwarding proxied response_is_chunked to " +
                "%r, %d remaining" % (entry['peer'].ident, entry['awaiting']))

        entry['peer'].push((const.MSG_TYPE_PROXY_RESPONSE_IS_CHUNKED,
            (entry['client_counter'], source.ident)))

    def forward_proxy_response_chunk(self, source, source_counter, rc, chunk):
        entry = self.proxying_channels[source][source_counter]
        entry['targets'][0].push((const.MSG_TYPE_PROXY_RESPONSE_CHUNK,
                (source, entry['dest_counter'], rc, chunk)))

        if rc:
            self.cleanup_forwarded_proxy_response_chunk(
                    source, source_counter, False)

    def cleanup_forwarded_proxy_response_chunk(self, source, source_counter,
            send_end_chunks=False):
        entry = self.proxying_channels[source].pop(source_counter)
        if not self.proxying_channels[source]:
            del self.proxying_channels[source]

        if send_end_chunks:
            entry['targets'][0].push((const.MSG_TYPE_PROXY_RESPONSE_END_CHUNKS,
                (entry['dest_counter'], source)))

    # callback for peer objects to pass up a message
    def incoming(self, peer, msg):
        msg_type, msg = msg
        handler = self.handlers.get(msg_type, None)

        if handler is None:
            # drop unrecognized messages
            log.warn("received unrecognized message type %r from %r" %
                    (msg_type, peer.ident))
            return

        handler(self, peer, msg)

    def incoming_publish(self, peer, msg):
        if not isinstance(msg, tuple) or len(msg) != 5:
            # drop malformed messages
            log.warn("received malformed publish from %r" % (peer.ident,))
            return

        service, routing_id, method, args, kwargs = msg

        handler, schedule = self.find_local_handler(
                const.MSG_TYPE_PUBLISH, service, routing_id, method)
        if handler is None:
            # drop mis-delivered messages
            log.warn("received mis-delivered publish %r from %r" %
                    (msg[:3], peer.ident))
            return

        log.debug("handling publish %r from %r %s" %
                (msg[:3], peer.ident,
                "scheduled" if schedule else "immediately"))

        if schedule:
            backend.schedule(self.publish_handler,
                    args=(handler, msg[:3], peer.ident, args, kwargs))
        else:
            self.publish_handler(handler, msg[:3], peer.ident, args, kwargs)

    def incoming_rpc_request(self, peer, msg):
        if not isinstance(msg, tuple) or len(msg) != 6:
            # drop malformed messages
            log.warn("received malformed rpc_request from %r" % (peer.ident,))
            return

        counter, service, routing_id, method, args, kwargs = msg

        handler, schedule = self.find_local_handler(
                const.MSG_TYPE_RPC_REQUEST, service, routing_id, method)
        if handler is None:
            if any(routing_id & mask == value
                    for mask, value, handlers in self.local_subs.get(
                            (const.MSG_TYPE_RPC_REQUEST, service), [])):
                log.warn("received rpc_request %r for unknown method from %r" %
                        (msg[:4], peer.ident))
                rc = const.RPC_ERR_NOMETHOD
            else:
                log.warn("received mis-delivered rpc_request %r from %r" %
                        (msg[:4], peer.ident))
                rc = const.RPC_ERR_NOHANDLER

            # mis-delivered message
            peer.push((const.MSG_TYPE_RPC_RESPONSE, (counter, rc, None)))
            return

        log.debug("handling rpc_request %r from %r %s" % (
                msg[:4], peer.ident,
                "scheduled" if schedule else "immediately"))

        if schedule:
            backend.schedule(self.rpc_handler,
                    args=(peer, counter, handler, args, kwargs),
                    kwargs={'scheduled': True})
        else:
            self.rpc_handler(peer, counter, handler, args, kwargs)

    def incoming_rpc_response(self, peer, msg):
        if not isinstance(msg, tuple) or len(msg) != 3:
            # drop malformed responses
            log.warn("received malformed rpc_response from %r" % (peer.ident,))
            return

        counter, rc, result = msg

        if counter in self.inflight_proxies:
            log.debug("received a proxied response %r from %r" %
                    (msg[:2], peer.ident))
            self.proxied_response(counter, rc, result)
        elif (counter not in self.rpc_client.inflight or
                peer.ident not in self.rpc_client.inflight[counter]):
            # drop mistaken responses
            log.warn("received mis-delivered rpc_response %r from %r" %
                    (msg[:2], peer.ident))
            return

        log.debug("received rpc_response %r from %r" % (msg[:2], peer.ident))

        self.rpc_client.response(peer, counter, rc, result)

    def proxied_response(self, counter, rc, result):
        entry = self.inflight_proxies[counter]
        entry['awaiting'] -= 1
        if not entry['awaiting']:
            del self.inflight_proxies[counter]

        log.debug("forwarding proxied response to %r, %d remaining" %
                (entry['peer'].ident, entry['awaiting']))

        entry['peer'].push((const.MSG_TYPE_PROXY_RESPONSE,
                (entry['client_counter'], rc, result)))

    def incoming_proxy_publish(self, peer, msg):
        if not isinstance(msg, tuple) or len(msg) != 6:
            # drop malformed messages
            log.warn("received malformed proxy_publish from %r" %
                    (peer.ident,))
            return

        log.debug("forwarding a proxy_publish %r from %r" %
                (msg[:3], peer.ident))

        self.send_publish(peer, *(msg[:5] + (True, msg[5])))

    def incoming_proxy_request(self, peer, msg):
        if not isinstance(msg, tuple) or len(msg) != 7:
            # drop badly formed messages
            log.warn("received malformed proxy_request from %r" %
                    (peer.ident,))
            return
        cli_counter, service, routing_id, method, singular, args, kwargs = msg

        # find local handlers
        handler, schedule = self.find_local_handler(
                const.MSG_TYPE_RPC_REQUEST, service, routing_id, method)

        # find remote targets and count up total handlers
        targets = list(self.find_peer_routes(
                const.MSG_TYPE_RPC_REQUEST, service, routing_id))
        target_count = len(targets) + bool(handler)

        # pick the single target for 'singular' proxy RPCs
        if target_count > 1 and singular:
            target_count = 1
            target = self.target_selection(
                    targets + [LocalTarget(self, handler, schedule, client)],
                    service, routing_id, method)
            if isinstance(target, LocalTarget):
                targets = []
            else:
                handler = None
                targets = [target]

        # handle it locally if it's aimed at us
        if handler is not None:
            log.debug("locally handling proxy_request %r %s" % (
                    msg[:4], "scheduled" if schedule else "immediately"))
            if schedule:
                backend.schedule(self.rpc_handler,
                        args=(peer, cli_counter, handler, args, kwargs),
                        kwargs={'proxied': True, 'scheduled': True})
            else:
                self.rpc_handler(
                        peer, cli_counter, handler, args, kwargs, True)

        if targets:
            log.debug("forwarding proxy_request %r to %d peers" %
                    (msg[:4], target_count - bool(handler)))

            rpc = self.rpc_client.request(
                    targets, (service, routing_id, method, args, kwargs))

            self.inflight_proxies[rpc.counter] = {
                'awaiting': len(targets),
                'client_counter': cli_counter,
                'peer': peer,
            }

        send_nomethod = False
        if handler is None and not targets and self.locally_handles(
                const.MSG_TYPE_RPC_REQUEST, service, routing_id):
            # if there are no remote handlers and we only fail locally because
            # of the method, send a NOMETHOD error and include ourselves in the
            # target_count so the client can distinguish between "no method"
            # and "unroutable"
            log.warn("received proxy_request %r for unknown method" %
                    (msg[:4],))
            target_count += 1
            send_nomethod = True

        peer.push((const.MSG_TYPE_PROXY_RESPONSE_COUNT,
                (cli_counter, target_count)))

        # must send the response after the response_count
        # or the client gets confused
        if send_nomethod:
            peer.push((const.MSG_TYPE_PROXY_RESPONSE,
                (cli_counter, const.RPC_ERR_NOMETHOD, None)))

    def incoming_proxy_query_count(self, peer, msg):
        if not isinstance(msg, tuple) or len(msg) != 5:
            # drop malformed queries
            log.warn("received malformed proxy_query_count from %r" %
                    (peer.ident,))
            return
        counter, msg_type, service, routing_id, method = msg

        log.debug("received proxy_query_count %r from %r" %
                (msg, peer.ident))

        local, scheduled = self.find_local_handler(
                msg_type, service, routing_id, method)
        target_count = (local is not None) + len(list(
            self.find_peer_routes(msg_type, service, routing_id)))

        log.debug("sending proxy_response %r for query_count %r to %r" %
                ((counter, 0, target_count), msg, peer.ident))

        peer.push((const.MSG_TYPE_PROXY_RESPONSE, (counter, 0, target_count)))

    def incoming_proxy_response(self, peer, msg):
        if not isinstance(msg, tuple) or len(msg) != 3:
            # drop malformed responses
            log.warn("received malformed proxy_response from %r" %
                    (peer.ident,))
            return

        counter, rc, result = msg

        if counter not in self.rpc_client.inflight:
            # drop mistaken responses
            log.warn("received mis-delivered proxy_response %r from %r" %
                    (msg[:2], peer.ident))
            return

        log.debug("received proxy_response %r from %r" %
                (msg[:2], peer.ident))

        self.rpc_client.response(peer, counter, rc, result)

    def incoming_proxy_response_count(self, peer, msg):
        if not isinstance(msg, tuple) or len(msg) != 2:
            # drop malformed responses
            log.warn("received malformed proxy_response_count from %r" %
                    (peer.ident,))
            return
        counter, target_count = msg

        log.debug("received proxy_response_count %r from %r" %
                (msg, peer.ident))

        self.rpc_client.expect(peer, counter, target_count)

    def incoming_proxy_publish_is_chunked(self, peer, msg):
        if not isinstance(msg, tuple) or len(msg) != 6:
            log.warn("received malformed proxy_publish_is_chunked from %r" %
                    (peer.addr,))
            return

        service, routing_id, method, source_counter, args, kwargs = msg

        log.debug("received proxy_publish_is_chunked %r from %r" %
                (msg[:4], peer.addr))

        dest_counter = self.rpc_client.next_counter()
        peers = list(self.find_peer_routes(
                const.MSG_TYPE_PUBLISH, service, routing_id))
        targets = peers[:]

        handler, schedule = self.find_local_handler(
                const.MSG_TYPE_PUBLISH, service, routing_id, method)
        if handler:
            targets.append(LocalTarget(self, handler, schedule, peer))

        peer_addr = id(peer)
        bypeer = self.proxying_channels.setdefault(peer_addr, {})
        bypeer[source_counter] = {
            'dest_counter': dest_counter,
            'targets': targets,
            'type': const.MSG_TYPE_PUBLISH_IS_CHUNKED,
        }

        self.multipush(targets, (const.MSG_TYPE_PUBLISH_IS_CHUNKED,
                (service, routing_id, method, dest_counter, args, kwargs)))

    def incoming_proxy_publish_chunk(self, peer, msg):
        if not isinstance(msg, tuple) or len(msg) != 3:
            log.warn("received malformed proxy_publish_chunk from %r" %
                    (peer.addr,))
            return

        source_counter, rc, chunk = msg

        peer_addr = id(peer)
        entry = self.proxying_channels.get(peer_addr, {}).get(
                source_counter, None)
        if entry is None:
            log.warn("received misdelivered proxy_publish_chunk " +
                    "%r from %r" % (source_counter, peer.addr))
            return

        log.debug("received proxy_publish_chunk %r from %r" %
                (source_counter, peer.addr))

        if rc:
            self.cleanup_forwarded_chunk(peer_addr, source_counter)

        self.multipush(entry['targets'], (const.MSG_TYPE_PUBLISH_CHUNK,
            (entry['dest_counter'], rc, chunk)))

    def incoming_proxy_publish_end_chunks(self, peer, msg):
        if not isinstance(msg, (int, long)):
            log.warn("received malformed proxy_publish_end_chunks from %r" %
                    (peer.addr,))
            return

        entry = self.cleanup_forwarded_chunk(id(peer), msg)
        if entry is None:
            log.warn("received misdelivered proxy_publish_end_chunks " +
                    "%r from %r" % (msg, peer.addr))
            return

        log.debug("received proxy_publish_end_chunks %r from %r" %
                (msg, peer.addr))

        self.multipush(entry['targets'],
                (const.MSG_TYPE_PUBLISH_END_CHUNKS, entry['dest_counter']))

    def incoming_publish_is_chunked(self, peer, msg):
        if not isinstance(msg, tuple) or len(msg) != 6:
            log.warn("received malformed publish_is_chunked from %r" %
                    (peer.ident,))
            return

        service, routing_id, method, counter, args, kwargs = msg

        handler, schedule = self.find_local_handler(
                const.MSG_TYPE_PUBLISH, service, routing_id, method)
        if handler is None:
            log.warn("received mis-delivered publish_is_chunked %r from %r" %
                    (msg[:4], peer.ident))
            return

        log.debug("received publish_is_chunked %r from %r" %
                (msg[:4], peer.ident))

        self.handle_start_publish_chunks(
                peer.ident, counter, handler, args, kwargs)

    def incoming_publish_chunk(self, peer, msg):
        if not isinstance(msg, tuple) or len(msg) != 3:
            log.warn("received malformed publish_chunk from %r" %
                    (peer.ident,))
            return

        counter, rc, chunk = msg

        peer_ident = peer.ident or id(peer)
        if ((const.MSG_TYPE_PUBLISH_IS_CHUNKED, counter) not in
                self.received_channels.get(peer_ident, ())):
            log.warn("received mis-delivered publish_chunk %r from %r" %
                    ((counter, rc), peer_ident))
            return

        log.debug("received publish_chunk %r from %r" %
                ((counter, rc), peer.ident))

        self.handle_chunk_arrival(peer.ident,
                const.MSG_TYPE_PUBLISH_IS_CHUNKED, counter, rc,
                _check_error(log, peer.ident, rc, chunk))

    def incoming_publish_end_chunks(self, peer, msg):
        if not isinstance(msg, (int, long)):
            log.warn("received malformed publish_end_chunks from %r" %
                    (peer.ident,))
            return

        log.debug("received publish_end_chunks %r from %r" % (msg, peer.ident))

        self.cleanup_incoming_chunks(peer.ident,
                const.MSG_TYPE_PUBLISH_IS_CHUNKED, msg)

    def incoming_request_is_chunked(self, peer, msg):
        if not isinstance(msg, tuple) or len(msg) != 6:
            log.warn("received malformed request_is_chunked from %r" %
                    (peer.ident,))
            return

        service, routing_id, method, counter, args, kwargs = msg

        handler, schedule = self.find_local_handler(
                const.MSG_TYPE_RPC_REQUEST, service, routing_id, method)
        if handler is None:
            if any(routing_id & mask == value
                    for mask, value, handlers in self.local_subs.get(
                        (const.MSG_TYPE_RPC_REQUEST, service), [])):
                log.warn("received request_is_chunked " +
                        "%r for unknown method from %r" %
                        (msg[:4], peer.ident))
                rc = const.RPC_ERR_NOMETHOD
            else:
                log.warn("received mis-delivered request_is_chunked " +
                        "%r from %r" % (msg[:4], peer.ident))
                rc = const.RPC_ERR_NOHANDLER

            # some form of mis-delivered message
            peer.push((const.MSG_TYPE_RPC_RESPONSE, (counter, rc, None)))
            return

        log.debug("handling request_is_chunked %r from %r scheduled" % (
                msg[:4], peer.ident))

        self.handle_start_request_chunks(peer, counter, handler, args, kwargs)

    def incoming_request_chunk(self, peer, msg):
        if not isinstance(msg, tuple) or len(msg) != 3:
            log.warn("received malformed request_chunk from %r" %
                    (peer.ident,))
            return

        counter, rc, chunk = msg

        peer_ident = peer.ident or id(peer)
        if ((const.MSG_TYPE_REQUEST_IS_CHUNKED, counter) not in
                self.received_channels.get(peer_ident, ())):
            log.warn("received mis-delivered request_chunk %r from %r" %
                    ((counter, rc), peer_ident))
            return

        log.debug("received request_chunk %r from %r" %
                ((counter, rc), peer.ident))

        self.handle_chunk_arrival(peer.ident,
                const.MSG_TYPE_REQUEST_IS_CHUNKED, counter, rc,
                _check_error(log, peer.ident, rc, chunk))

    def incoming_request_end_chunks(self, peer, msg):
        if not isinstance(msg, (int, long)):
            log.warn("received malformed request_end_chunks from %r" %
                    (peer.ident,))
            return

        log.debug("received request_end_chunks %r from %r" % (msg, peer.ident))

        self.cleanup_incoming_chunks(peer.ident,
                const.MSG_TYPE_REQUEST_IS_CHUNKED, msg)

    def incoming_proxy_request_is_chunked(self, peer, msg):
        if not isinstance(msg, tuple) or len(msg) != 7:
            log.warn("received malformed proxy_request_is_chunked from %r" %
                    (peer.addr,))
            return

        (service, routing_id, method, singular,
                source_counter, args, kwargs) = msg

        log.debug("received proxy_request_is_chunked %r from %r" %
                (msg[:4], peer.addr))

        peers = list(self.find_peer_routes(
            const.MSG_TYPE_RPC_REQUEST, service, routing_id))
        targets = peers[:]

        dest_counter = self.rpc_client.next_counter()
        self.rpc_client.sent(dest_counter, peers)

        handler, schedule = self.find_local_handler(
                const.MSG_TYPE_RPC_REQUEST, service, routing_id, method)
        if handler:
            targets.append(LocalTarget(self, handler, schedule, peer,
                source_counter))

        if len(targets) > 1 and singular:
            targets = [self.target_selection(
                    targets, service, routing_id, method)]

        peer_addr = id(peer)
        bypeer = self.proxying_channels.setdefault(peer_addr, {})
        bypeer[source_counter] = {
            'dest_counter': dest_counter,
            'targets': targets,
            'type': const.MSG_TYPE_REQUEST_IS_CHUNKED,
        }

        if peers:
            self.inflight_proxies[dest_counter] = {
                'awaiting': len(peers),
                'client_counter': source_counter,
                'peer': peer
            }

        send_nomethod = False
        if handler is None and not targets and self.locally_handles(
                const.MSG_TYPE_RPC_REQUEST, service, routing_id):
            # if there are no remote handlers and we only fail locally because
            # of the method, send a NOMETHOD error and include ourselves in the
            # target_count so the client can distinguish between "no method"
            # and "unroutable"
            log.warn("received proxy_request %r for unknown method" %
                    (msg[:3],))
            send_nomethod = True

        peer.push((const.MSG_TYPE_PROXY_RESPONSE_COUNT,
            (source_counter, len(targets) + bool(send_nomethod))))

        if send_nomethod:
            peer.push((const.MSG_TYPE_PROXY_RESPONSE,
                (source_counter, const.RPC_ERR_NOMETHOD, None)))

        self.multipush(targets, (const.MSG_TYPE_REQUEST_IS_CHUNKED,
            (service, routing_id, method, dest_counter, args, kwargs)))

    def incoming_proxy_request_chunk(self, peer, msg):
        if not isinstance(msg, tuple) or len(msg) != 3:
            log.warn("received malformed proxy_request_chunk from %r" %
                    (peer.ident,))

        source_counter, rc, chunk = msg

        peer_addr = id(peer)
        entry = self.proxying_channels.get(peer_addr, {}).get(
                source_counter, None)
        if entry is None:
            log.warn("received misdelivered proxy_request_chunk " +
                    "%r from %r" % (source_counter, peer.addr))
            return

        log.debug("received proxy_request_chunk %r from %r" %
                (source_counter, peer.addr))

        if rc:
            self.cleanup_forwarded_chunk(peer_addr, source_counter)

        self.multipush(entry['targets'], (const.MSG_TYPE_REQUEST_CHUNK,
            (entry['dest_counter'], rc, chunk)))

    def incoming_proxy_request_end_chunks(self, peer, msg):
        if not isinstance(msg, (int, long)):
            log.warn("received malformed proxy_request_end_chunks from %r" %
                    (peer.ident,))
            return

        entry = self.cleanup_forwarded_chunk(id(peer), msg)
        if entry is None:
            log.warn("received misdelivered proxy_request_end_chunks " +
                    "%r from %r" % (msg, peer.addr))
            return

        log.debug("received proxy_request_end_chunks %r from %r" %
                (msg, peer.addr))

        self.multipush(entry['targets'],
                (const.MSG_TYPE_REQUEST_END_CHUNKS, entry['dest_counter']))

    def incoming_response_is_chunked(self, peer, msg):
        if not isinstance(msg, (int, long)):
            log.warn("received malformed response_is_chunked from %r" %
                    (peer.ident,))
            return

        if msg in self.inflight_proxies:
            log.debug("received a proxied response_is_chunked %r from %r" %
                    (msg, peer.ident))
            self.forward_proxy_response_is_chunked(peer, msg)
            return
        elif (msg not in self.rpc_client.inflight or
                peer.ident not in self.rpc_client.inflight[msg]):
            # drop mistaken responses
            log.warn("received mis-delivered response_is_chunked %r from %r" %
                    (msg, peer.ident))
            return

        log.debug("received response_is_chunked %r from %r" %
                (msg, peer.ident))

        self.rpc_client.response(peer, msg, 0,
                self.handle_start_response_chunks(peer.ident, msg))

    def incoming_response_chunk(self, peer, msg):
        if not isinstance(msg, tuple) or len(msg) != 3:
            log.warn("received malformed response_chunk from %r" %
                    (peer.ident,))
            return

        counter, rc, chunk = msg

        if counter in self.proxying_channels.get(peer.ident, ()):
            log.debug("forwarding a response_chunk %r from %r" %
                    ((counter, rc), peer.ident))
            self.forward_proxy_response_chunk(peer.ident, counter, rc, chunk)
            return
        elif ((const.MSG_TYPE_RESPONSE_IS_CHUNKED, counter) not in
                self.received_channels.get(peer.ident, ())):
            log.warn("received mis-delivered response_chunk %r from %r" %
                    ((counter, rc), peer.ident))
            return

        log.debug("received response_chunk %r from %r" %
                ((counter, rc), peer.ident))

        self.handle_chunk_arrival(peer.ident,
                const.MSG_TYPE_RESPONSE_IS_CHUNKED, counter, rc,
                _check_error(log, peer.ident, rc, chunk))

    def incoming_response_end_chunks(self, peer, msg):
        if not isinstance(msg, (int, long)):
            log.warn("received malformed response_end_chunks from %r" %
                    (peer.ident,))
            return

        if msg in self.proxying_channels.get(peer.ident, ()):
            log.debug("forwarding a response_end_chunks %r from %r" %
                    (msg, peer.ident))
            self.cleanup_forwarded_proxy_response_chunk(peer.ident, msg, True)
            return

        log.debug("received response_end_chunks %r from %r" %
                (msg, peer.ident))

        self.cleanup_incoming_chunks(peer.ident,
                const.MSG_TYPE_RESPONSE_IS_CHUNKED, msg)

    def incoming_proxy_response_is_chunked(self, peer, msg):
        if not isinstance(msg, tuple) or len(msg) != 2:
            log.warn("received malformed proxy_response_is_chunked from %r" %
                    (peer.ident,))
            return

        counter, source = msg

        if counter not in self.rpc_client.inflight:
            log.warn("received mis-delivered proxy_response_is_chunked " +
                    "%r from %r" % (counter, peer.ident))
            return

        log.debug("received proxy_response_is_chunked %r from %r" %
                (counter, peer.ident))

        self.rpc_client.response(peer, counter, 0,
                self.handle_start_response_chunks(source, counter))

    def incoming_proxy_response_chunk(self, peer, msg):
        if not isinstance(msg, tuple) or len(msg) != 4:
            log.warn("received malformed proxy_response_chunk from %r" %
                    (peer.ident,))
            return

        source, counter, rc, chunk = msg

        if ((const.MSG_TYPE_RESPONSE_IS_CHUNKED, counter) not in
                self.received_channels.get(source, ())):
            log.warn("received mis-delivered proxy_response_chunk " +
                    "%r from %r" % ((counter, rc), peer.ident))
            return

        log.debug("received proxy_response_chunk %r from %r" %
                ((counter, rc), peer.ident))

        self.handle_chunk_arrival(source,
                const.MSG_TYPE_RESPONSE_IS_CHUNKED, counter, rc,
                _check_error(log, source, rc, chunk))

    def incoming_proxy_response_end_chunks(self, peer, msg):
        if not isinstance(msg, tuple) or len(msg) != 2:
            log.warn("received malformed proxy_response_end_chunks from %r" %
                    (peer.ident,))
            return

        counter, source = msg

        log.debug("received proxy_response_end_chunks %r from %r" %
                ((counter, source), peer.ident))

        self.cleanup_incoming_chunks(source,
                const.MSG_TYPE_RESPONSE_IS_CHUNKED, counter)

    handlers = {
        const.MSG_TYPE_ANNOUNCE: incoming_announce,
        const.MSG_TYPE_UNSUBSCRIBE: incoming_unsubscribe,
        const.MSG_TYPE_PUBLISH: incoming_publish,
        const.MSG_TYPE_RPC_REQUEST: incoming_rpc_request,
        const.MSG_TYPE_RPC_RESPONSE: incoming_rpc_response,
        const.MSG_TYPE_PROXY_PUBLISH: incoming_proxy_publish,
        const.MSG_TYPE_PROXY_REQUEST: incoming_proxy_request,
        const.MSG_TYPE_PROXY_RESPONSE: incoming_proxy_response,
        const.MSG_TYPE_PROXY_RESPONSE_COUNT: incoming_proxy_response_count,
        const.MSG_TYPE_PROXY_QUERY_COUNT: incoming_proxy_query_count,
        const.MSG_TYPE_PUBLISH_IS_CHUNKED: incoming_publish_is_chunked,
        const.MSG_TYPE_PUBLISH_CHUNK: incoming_publish_chunk,
        const.MSG_TYPE_PUBLISH_END_CHUNKS: incoming_publish_end_chunks,
        const.MSG_TYPE_PROXY_PUBLISH_IS_CHUNKED:
                incoming_proxy_publish_is_chunked,
        const.MSG_TYPE_PROXY_PUBLISH_CHUNK: incoming_proxy_publish_chunk,
        const.MSG_TYPE_PROXY_PUBLISH_END_CHUNKS:
                incoming_proxy_publish_end_chunks,

        const.MSG_TYPE_REQUEST_IS_CHUNKED: incoming_request_is_chunked,
        const.MSG_TYPE_REQUEST_CHUNK: incoming_request_chunk,
        const.MSG_TYPE_REQUEST_END_CHUNKS: incoming_request_end_chunks,
        const.MSG_TYPE_PROXY_REQUEST_IS_CHUNKED:
                incoming_proxy_request_is_chunked,
        const.MSG_TYPE_PROXY_REQUEST_CHUNK: incoming_proxy_request_chunk,
        const.MSG_TYPE_PROXY_REQUEST_END_CHUNKS:
                incoming_proxy_request_end_chunks,
        const.MSG_TYPE_RESPONSE_IS_CHUNKED: incoming_response_is_chunked,
        const.MSG_TYPE_RESPONSE_CHUNK: incoming_response_chunk,
        const.MSG_TYPE_RESPONSE_END_CHUNKS: incoming_response_end_chunks,
        const.MSG_TYPE_PROXY_RESPONSE_IS_CHUNKED:
                incoming_proxy_response_is_chunked,
        const.MSG_TYPE_PROXY_RESPONSE_CHUNK: incoming_proxy_response_chunk,
        const.MSG_TYPE_PROXY_RESPONSE_END_CHUNKS:
                incoming_proxy_response_end_chunks,
    }


class LocalTarget(object):
    def __init__(self, dispatcher, handler, schedule, client=None,
            client_counter=None):
        self.dispatcher = dispatcher
        self.handler = handler
        self.schedule = schedule
        self.ident = None
        self.up = True
        self.client = client
        self.client_counter = client_counter

    def push(self, msg):
        msgtype, msg = msg
        if msgtype == const.MSG_TYPE_RPC_REQUEST:
            counter, service, routing_id, method, args, kwargs = msg
            if self.schedule:
                backend.schedule(self.dispatcher.rpc_handler,
                        args=(self, counter, self.handler, args, kwargs))
            else:
                self.dispatcher.rpc_handler(
                        self, counter, self.handler, args, kwargs)

        elif msgtype == const.MSG_TYPE_RPC_RESPONSE:
            # sent back here via dispatcher.rpc_handler
            counter, rc, result = msg
            self.dispatcher.rpc_client.response(self, counter, rc, result)

        elif msgtype == const.MSG_TYPE_PUBLISH:
            service, routing_id, method, args, kwargs = msg
            if self.schedule:
                backend.schedule(self.handler, args=args, kwargs=kwargs)
            else:
                try:
                    self.handler(*args, **kwargs)
                except Exception:
                    log.error("exception handling local publish %r" %
                            ((service, routing_id, method),))
                    backend.handle_exception(*sys.exc_info())

        elif msgtype == const.MSG_TYPE_PUBLISH_IS_CHUNKED:
            service, routing_id, method, counter, args, kwargs = msg
            client = id(self.client) if self.client else None
            self.dispatcher.handle_start_publish_chunks(
                    client, counter, self.handler, args, kwargs)

        elif msgtype == const.MSG_TYPE_REQUEST_IS_CHUNKED:
            service, routing_id, method, counter, args, kwargs = msg
            client = self.client or self
            self.dispatcher.handle_start_request_chunks(
                    client, counter, self.handler, args, kwargs, True,
                    self.client_counter)

        elif msgtype in (
                const.MSG_TYPE_PUBLISH_CHUNK, const.MSG_TYPE_REQUEST_CHUNK):
            counter, rc, chunk = msg
            client_id = id(self.client) if self.client else None
            self.dispatcher.handle_chunk_arrival(client_id, msgtype - 3,
                    counter, rc, _check_error(log, None, rc, chunk))

        elif msgtype in (const.MSG_TYPE_PUBLISH_END_CHUNKS,
                const.MSG_TYPE_REQUEST_END_CHUNKS):
            client = id(self.client) if self.client else None
            self.dispatcher.cleanup_incoming_chunks(client, msgtype - 6, msg)

    # trick RPCClient.request
    # in the case of a local handler it doesn't have to go over the wire, so
    # there's no issue with unserializable arguments (or return values). so
    # we'll skip the "dump" phase and just "push" the object itself
    push_string = push
    def dump(self, msg):
        return msg


def _check_error(log, source_peer, rc, data):
    if not rc:
        return data

    if rc == const.RPC_ERR_MALFORMED:
        log.error("'malformed message' error from %r" % (source_peer,))
        return errors.JunctionSystemError("malformed message")

    if rc == const.RPC_ERR_NOHANDLER:
        log.error("'no handler' error from %r" % (source_peer,))
        return errors.NoRemoteHandler("message mistakenly sent to %r" %
                (source_peer,))

    if rc == const.RPC_ERR_NOMETHOD:
        log.error("'unsupported method' error from %r" % (source_peer,))
        return errors.UnsupportedRemoteMethod(
                "peer at %r doesn't support the method" % (source_peer,))

    if rc == const.RPC_ERR_KNOWN:
        err_code, err_args = data
        if err_code in errors.HANDLED_ERROR_TYPES:
            err_klass = errors.HANDLED_ERROR_TYPES.get(err_code)
            log.error("%s error raised in handler at %r" %
                    (err_klass.__name__, source_peer))
            err = err_klass(source_peer, *err_args)
            return err
        else:
            rc = const.RPC_ERR_UNKNOWN

    if rc == const.RPC_ERR_UNKNOWN:
        log.error("exception in handler at %r" % (source_peer,))
        return errors.RemoteException(source_peer, data)

    if rc == const.RPC_ERR_LOST_CONN:
        log.error("failure from lost connection to %r" % (source_peer,))
        return errors.LostConnection(source_peer)

    if rc == const.RPC_ERR_UNSER_RESP:
        log.error("handler at %r returned an unserializable object" %
                (source_peer,))
        return errors.UnserializableResponse(data)

    if rc == const.RPC_ERR_BADARGS:
        log.error("wrong arguments provided for handler at %r" %
                (source_peer,))
        return errors.BadArguments(data)

    log.error("error message with unrecognized return code from %r" %
            (source_peer,))
    return errors.UnrecognizedRemoteProblem(source_peer, rc, data)
