from __future__ import absolute_import

import itertools
import sys
import traceback

from greenhouse import scheduler
from . import connection, const
from .. import errors


class Dispatcher(object):
    def __init__(self, rpc_client):
        self.rpc_client = rpc_client
        self.peer_subs = {}
        self.local_subs = {}
        self.peers = {}
        self.inflight_proxies = {}

    def add_local_subscription(self, msg_type, service, mask, value, method,
            handler, schedule):
        # storage in local_subs is shaped like so:
        # {(msg_type, service): [
        #     (mask, value, {method: (handler, schedule), ...}), ...], ...}
        if value & ~mask:
            raise errors.ImpossibleSubscription(msg_type, service, mask, value)

        existing = self.local_subs.setdefault((msg_type, service), [])
        for pmask, pvalue, phandlers in existing:
            if pmask & value == mask & pvalue:
                if method in phandlers:
                    raise errors.OverlappingSubscription(
                            (msg_type, service, mask, value, method),
                            (msg_type, service, pmask, pvalue, method))
                elif (mask, value) == (pmask, pvalue):
                    phandlers[method] = (handler, schedule)
                    return
        else:
            existing.append((mask, value, {method: (handler, schedule)}))

        for peer in self.peers.itervalues():
            if not peer.up:
                continue
            peer.push((const.MSG_TYPE_ANNOUNCE,
                    [(msg_type, service, mask, value)]))

    def remove_local_subscription(
            self, msg_type, service, mask, value):
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
        if not isinstance(msg, tuple) or len(msg) != 5:
            # badly formatted message
            return

        msg_type, service, mask, value = msg

        groups = self.peer_subs.get((msg_type, service), 0)
        if not groups:
            return
        for i, group in enumerate(groups):
            if (mask, value, peer) == group:
                del groups[i]
                if not groups:
                    del self.peer_subs[(msg_type, service)]
                break

    def find_local_handler(self, msg_type, service, routing_id, method):
        group = self.local_subs.get((msg_type, service), 0)
        if not group:
            return None, False
        for mask, value, handlers in group:
            if routing_id & mask == value and method in handlers:
                return handlers[method]
        return None, False

    def local_subscriptions(self):
        for key, value in self.local_subs.iteritems():
            msg_type, service = key
            for mask, value, handlers in value:
                yield (msg_type, service, mask, value)

    def store_peer(self, peer, subscriptions):
        if peer.ident in self.peers:
            winner, loser = connection.compare(peer, self.peers[peer.ident])
            if loser is peer:
                return False
            loser.go_down(reconnect=False)
            self.drop_peer_subscriptions(loser)

        self.peers[peer.ident] = peer
        self.add_peer_subscriptions(peer, subscriptions)
        return True

    def drop_peer(self, peer):
        self.peers.pop(peer.ident, None)
        self.drop_peer_subscriptions(peer)

        # reply to all in-flight proxied RPCs to the dropped peer
        # with the "lost connection" error
        for counter in self.rpc_client.by_peer.get(id(peer), []):
            if counter in self.inflight_proxies:
                self.proxied_response(counter, const.RPC_ERR_LOST_CONN, None)

        self.rpc_client.connection_down(peer)

    def add_peer_subscriptions(self, peer, subscriptions):
        # format for peer_subs:
        # {(msg_type, service): [(mask, value, connection)]}
        for msg_type, service, mask, value in subscriptions:
            self.peer_subs.setdefault((msg_type, service), []).append(
                    (mask, value, peer))

    def drop_peer_subscriptions(self, peer):
        for key, group in self.peer_subs.items():
            group = [g for g in group if g[2] is not peer]
            if group:
                self.peer_subs[key] = group
            else:
                del self.peer_subs[key]

    def find_peer_routes(self, msg_type, service, routing_id):
        for mask, value, peer in self.peer_subs.get((msg_type, service), []):
            if peer.up and routing_id & mask == value:
                yield peer

    def send_publish(self, service, routing_id, method, args, kwargs):
        msg = (const.MSG_TYPE_PUBLISH,
                (service, routing_id, method, args, kwargs))
        found_one = False

        # handle locally if we have a hander for it
        handler, schedule = self.find_local_handler(
                const.MSG_TYPE_PUBLISH, service, routing_id, method)
        if handler is not None:
            found_one = True
            if schedule:
                scheduler.schedule(handler, args=args, kwargs=kwargs)
            else:
                try:
                    handler(*args, **kwargs)
                except Exception:
                    scheduler.handle_exception(*sys.exc_info())

        # send publishes to peers with handlers
        for peer in self.find_peer_routes(
                const.MSG_TYPE_PUBLISH, service, routing_id):
            found_one = True
            peer.push(msg)

        return found_one

    def send_rpc(self, service, routing_id, method, args, kwargs):
        handler, schedule = self.find_local_handler(
                const.MSG_TYPE_RPC_REQUEST, service, routing_id, method)
        routes = []
        if handler is not None:
            routes.append(LocalTarget(self, handler, schedule))
        routes.extend(self.find_peer_routes(
                const.MSG_TYPE_RPC_REQUEST, service, routing_id))
        return self.rpc_client.request(
                routes, service, routing_id, method, args, kwargs)

    def send_proxied_publish(self, service, routing_id, method, args, kwargs):
        self.peers.values()[0].push(
                (const.MSG_TYPE_PROXY_PUBLISH,
                    (service, routing_id, method, args, kwargs)))

    def rpc_handler(self, peer, counter, handler, args, kwargs, proxied=False):
        response = (proxied and const.MSG_TYPE_PROXY_RESPONSE
                or const.MSG_TYPE_RPC_RESPONSE)

        try:
            rc = 0
            result = handler(*args, **kwargs)
        except errors.HandledError, exc:
            rc = const.RPC_ERR_KNOWN
            result = (exc.code, exc.args)
            scheduler.handle_exception(*sys.exc_info())
        except Exception:
            rc = const.RPC_ERR_UNKNOWN
            result = traceback.format_exception(*sys.exc_info())
            scheduler.handle_exception(*sys.exc_info())

        try:
            msg = peer.dump((response, (counter, rc, result)))
        except TypeError:
            msg = peer.dump((response,
                (counter, const.RPC_ERR_UNSER_RESP, repr(result))))
            scheduler.handle_exception(*sys.exc_info())

        peer.push_string(msg)

    # callback for peer objects to pass up a message
    def incoming(self, peer, msg):
        msg_type, msg = msg
        handler = self.handlers.get(msg_type, None)

        if handler is None:
            # drop unrecognized messages
            return

        handler(self, peer, msg)

    def incoming_publish(self, peer, msg):
        if not isinstance(msg, tuple) or len(msg) != 5:
            # drop malformed messages
            return
        service, routing_id, method, args, kwargs = msg

        handler, schedule = self.find_local_handler(
                const.MSG_TYPE_PUBLISH, service, routing_id, method)
        if handler is None:
            # drop mis-delivered messages
            return

        if schedule:
            scheduler.schedule(handler, args=args, kwargs=kwargs)
        else:
            try:
                handler(*args, **kwargs)
            except Exception:
                scheduler.handle_exception(*sys.exc_info())

    def incoming_rpc_request(self, peer, msg):
        if not isinstance(msg, tuple) or len(msg) != 6:
            # drop malformed messages
            return
        counter, service, routing_id, method, args, kwargs = msg

        handler, schedule = self.find_local_handler(
                const.MSG_TYPE_RPC_REQUEST, service, routing_id, method)
        if handler is None:
            if any(routing_id & mask == value
                    for mask, value, handlers in self.local_subs.get(
                            (const.MSG_TYPE_RPC_REQUEST, service), [])):
                rc = const.RPC_ERR_NOMETHOD
            else:
                rc = const.RPC_ERR_NOHANDLER

            # mis-delivered message
            peer.push((const.MSG_TYPE_RPC_RESPONSE,
                    (counter, rc, None)))
        elif schedule:
            scheduler.schedule(self.rpc_handler,
                    args=(peer, counter, handler, args, kwargs))
        else:
            self.rpc_handler(peer, counter, handler, args, kwargs)

    def incoming_rpc_response(self, peer, msg):
        if not isinstance(msg, tuple) or len(msg) != 3:
            # drop malformed responses
            return

        counter, rc, result = msg

        if counter in self.inflight_proxies:
            self.proxied_response(counter, rc, result)
        elif (counter not in self.rpc_client.inflight or
                peer.ident not in self.rpc_client.inflight[counter]):
            # drop mistaken responses
            return

        self.rpc_client.response(peer, counter, rc, result)

    def proxied_response(self, counter, rc, result):
        entry = self.inflight_proxies[counter]
        entry['awaiting'] -= 1
        if not entry['awaiting']:
            del self.inflight_proxies[counter]
        entry['peer'].push((const.MSG_TYPE_PROXY_RESPONSE,
                (entry['client_counter'], rc, result)))

    def incoming_proxy_publish(self, peer, msg):
        if not isinstance(msg, tuple) or len(msg) != 5:
            # drop malformed messages
            return
        self.send_publish(*msg)

    def incoming_proxy_request(self, peer, msg):
        if not isinstance(msg, tuple) or len(msg) != 6:
            # drop badly formed messages
            return
        client_counter, service, routing_id, method, args, kwargs = msg

        # handle it locally if it's aimed at us
        handler, schedule = self.find_local_handler(
                const.MSG_TYPE_RPC_REQUEST, service, routing_id, method)
        if handler is not None:
            if schedule:
                scheduler.schedule(self.rpc_handler, args=(
                    peer, client_counter, handler, args, kwargs, True))
            else:
                self.rpc_handler(
                        peer, client_counter, handler, args, kwargs, True)

        target_count = handler is not None and 1 or 0
        targets = list(self.find_peer_routes(
                const.MSG_TYPE_RPC_REQUEST, service, routing_id))

        if targets:
            target_count += len(targets)

            rpc = self.rpc_client.request(
                    targets, service, routing_id, method, args, kwargs)

            self.inflight_proxies[rpc.counter] = {
                'awaiting': len(targets),
                'client_counter': client_counter,
                'peer': peer,
            }

        if handler is None and not targets:
            target_count = int(any(routing_id & mask == value
                    for mask, value, handlers in self.local_subs.get(
                        (const.MSG_TYPE_RPC_REQUEST, service), [])))
            target_count += sum(1 for mask, value, conn
                    in self.peer_subs.get(
                        (const.MSG_TYPE_RPC_REQUEST, service), [])
                    if routing_id & mask == value and conn.up)
            for i in xrange(target_count):
                peer.push((const.MSG_TYPE_PROXY_RESPONSE,
                    (client_counter, const.RPC_ERR_NOMETHOD, None)))

        peer.push((const.MSG_TYPE_PROXY_RESPONSE_COUNT,
                (client_counter, target_count)))

    def incoming_proxy_query_count(self, peer, msg):
        if not isinstance(msg, tuple) or len(msg) != 5:
            return
        counter, msg_type, service, routing_id, method = msg

        local, scheduled = self.find_local_handler(
                msg_type, service, routing_id, method)
        target_count = (local is not None) + len(list(
            self.find_peer_routes(msg_type, service, routing_id)))

        peer.push((const.MSG_TYPE_PROXY_RESPONSE, (counter, 0, target_count)))

    def incoming_proxy_response(self, peer, msg):
        if not isinstance(msg, tuple) or len(msg) != 3:
            # drop malformed responses
            return

        counter, rc, result = msg

        if counter not in self.rpc_client.inflight:
            # drop mistaken responses
            return

        self.rpc_client.response(peer, counter, rc, result)

    def incoming_proxy_response_count(self, peer, msg):
        if not isinstance(msg, tuple) or len(msg) != 2:
            # drop malformed responses
            return
        counter, target_count = msg
        self.rpc_client.expect(peer, counter, target_count)

    handlers = {
        const.MSG_TYPE_ANNOUNCE: add_peer_subscriptions,
        const.MSG_TYPE_UNSUBSCRIBE: incoming_unsubscribe,
        const.MSG_TYPE_PUBLISH: incoming_publish,
        const.MSG_TYPE_RPC_REQUEST: incoming_rpc_request,
        const.MSG_TYPE_RPC_RESPONSE: incoming_rpc_response,
        const.MSG_TYPE_PROXY_PUBLISH: incoming_proxy_publish,
        const.MSG_TYPE_PROXY_REQUEST: incoming_proxy_request,
        const.MSG_TYPE_PROXY_RESPONSE: incoming_proxy_response,
        const.MSG_TYPE_PROXY_RESPONSE_COUNT: incoming_proxy_response_count,
        const.MSG_TYPE_PROXY_QUERY_COUNT: incoming_proxy_query_count,
    }


class LocalTarget(object):
    def __init__(self, dispatcher, handler, schedule):
        self.dispatcher = dispatcher
        self.handler = handler
        self.schedule = schedule
        self.ident = None

    def push(self, msg):
        msgtype, msg = msg
        if msgtype == const.MSG_TYPE_PUBLISH:
            service, routing_id, method, args, kwargs = msg
            if self.schedule:
                scheduler.schedule(self.handler, args=args, kwargs=kwargs)
            else:
                try:
                    self.handler(*args, **kwargs)
                except Exception:
                    scheduler.handle_exception(*sys.exc_info())

        elif msgtype == const.MSG_TYPE_RPC_REQUEST:
            counter, service, routing_id, method, args, kwargs = msg
            if self.schedule:
                scheduler.schedule(self.dispatcher.rpc_handler,
                        args=(self, counter, self.handler, args, kwargs))
            else:
                self.dispatcher.rpc_handler(
                        self, counter, self.handler, args, kwargs)

        elif msgtype == const.MSG_TYPE_RPC_RESPONSE:
            # sent back here via dispatcher.rpc_handler
            counter, rc, result = msg
            self.dispatcher.rpc_client.response(self, counter, rc, result)
