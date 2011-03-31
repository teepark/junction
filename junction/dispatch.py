from __future__ import absolute_import

import itertools
import sys
import traceback

from greenhouse import scheduler
from . import connection, const, errors


class Dispatcher(object):
    def __init__(self, version, rpc_client):
        self.version = version
        self.rpc_client = rpc_client
        self.peer_subs = {}
        self.local_subs = {}
        self.peers = {}
        self.inflight_proxies = {}

    def add_local_subscriptions(self, handler, subscriptions):
        added = []
        for msg_type, service, method, mask, value, schedule in subscriptions:
            # simple sanity check: *anything* could match this
            if value & ~mask:
                continue

            previous_subscriptions = self.local_subs.setdefault(
                    (msg_type, service, method), [])

            # subscriptions must be mutually exclusive. that is, there cannot
            # be more than one handler on a node for any possible message
            for mask2, value2, handler2 in previous_subscriptions:
                if mask2 & value == mask & value2:
                    continue

            previous_subscriptions.append((mask, value, handler, schedule))
            added.append((msg_type, service, method, mask, value))

        # for all connections that have already gone through their handshake,
        # send an ANNOUNCE message with the subscription updates
        if added:
            for peer in self.peers.itervalues():
                if peer.up:
                    peer.push((const.MSG_TYPE_ANNOUNCE, added))

        return len(added)

    def remove_local_subscription(
            self, msg_type, service, method, mask, value, handler):
        group = self.local_subs.get((msg_type, service, method), [])
        for i, (mask2, value2, handler2, schedule) in enumerate(group):
            if mask == mask2 and value == value2 and handler is handler2:
                del group[i]
                return True
        return False
                

    def find_local_handler(self, msg_type, service, method, routing_id):
        group = (msg_type, service, method)
        if group not in self.local_subs:
            return None, False

        for mask, value, handler, schedule in self.local_subs[group]:
            if routing_id & mask == value:
                return handler, schedule

        return None, False

    def local_subscriptions(self):
        for (msg_type, service, method), (mask, value, handler, schedule) \
                in self.local_subs.iteritems():
            yield (msg_type, service, method, mask, value)

    def store_peer(self, peer):
        success = True
        if peer.ident in self.peers:
            winner, loser = connection.compare(peer, self.peers[peer.ident])
            loser.go_down(reconnect=False)
            self.drop_peer_subscriptions(winner)
            success = peer is not loser
            peer = winner

        self.peers[peer.ident] = peer
        self.add_peer_subscriptions(peer, peer.subscriptions, extend=False)
        return success

    def drop_peer(self, peer):
        self.peers.pop(peer.ident, None)
        self.drop_peer_subscriptions(peer)

    def add_peer_subscriptions(self, peer, subscriptions, extend=True):
        for msg_type, service, method, mask, value in subscriptions:
            self.peer_subs.setdefault((msg_type, service, method), []).append(
                    (mask, value, peer))
        if extend:
            peer.subscriptions.extend(subscriptions)

    def drop_peer_subscriptions(self, peer):
        for msg_type, service, method, mask, value in peer.subscriptions:
            group = (msg_type, service, method)
            item = (mask, value, peer)
            if item in self.peer_subs[group]:
                self.peer_subs[group].remove(item)
                if not self.peer_subs[group]:
                    del self.peer_subs[group]

    def find_peer_routes(self, msg_type, service, method, routing_id):
        return (peer for (mask, value, peer)
                in self.peer_subs.get((msg_type, service, method), [])
                if peer.up and routing_id & mask == value)

    def send_publish(self, service, method, routing_id, args, kwargs):
        msg = (const.MSG_TYPE_PUBLISH,
                (service, method, routing_id, args, kwargs))
        found_one = False

        # handle locally if we have a hander for it
        handler, schedule = self.find_local_handler(
                const.MSG_TYPE_PUBLISH, service, method, routing_id)
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
                const.MSG_TYPE_PUBLISH, service, method, routing_id):
            found_one = True
            peer.push(msg)

        return found_one

    def send_rpc(self, service, method, routing_id, args, kwargs):
        routes = self.find_peer_routes(
                const.MSG_TYPE_RPC_REQUEST, service, method, routing_id)

        handler, schedule = self.find_local_handler(
                const.MSG_TYPE_RPC_REQUEST, service, method, routing_id)
        if handler is not None:
            local_target = LocalTarget(self, handler, schedule)
            routes = itertools.chain([local_target], routes)

        rpc = self.rpc_client.request(
                routes, service, method, routing_id, args, kwargs)

        return rpc

    def send_proxied_publish(self, service, method, routing_id, args, kwargs):
        self.peers.values()[0].push(
                (const.MSG_TYPE_PROXY_PUBLISH,
                    (service, method, routing_id, args, kwargs)))

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
        service, method, routing_id, args, kwargs = msg

        handler, schedule = self.find_local_handler(
                const.MSG_TYPE_PUBLISH, service, method, routing_id)
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

    def rpc_handler(self, peer, counter, handler, args, kwargs, proxied=False):
        response = (proxied
                and const.MSG_TYPE_PROXY_RESPONSE
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

        peer.push((response, (counter, rc, result)))

    def incoming_rpc_request(self, peer, msg):
        if not isinstance(msg, tuple) or len(msg) != 6:
            # badly formed messages
            peer.push(
                    (const.MSG_TYPE_RPC_RESPONSE,
                        (counter, const.RPC_ERR_MALFORMED, None)))
            return
        counter, service, method, routing_id, args, kwargs = msg

        handler, schedule = self.find_local_handler(
                const.MSG_TYPE_RPC_REQUEST, service, method, routing_id)
        if handler is None:
            # mis-delivered message
            peer.push(
                    (const.MSG_TYPE_RPC_RESPONSE,
                        (counter, const.RPC_ERR_NOHANDLER, None)))
            return

        if schedule:
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
            entry = self.inflight_proxies[counter]
            entry['awaiting'] -= 1
            if not entry['awaiting']:
                del self.inflight_proxies[counter]
            entry['peer'].push((const.MSG_TYPE_PROXY_RESPONSE,
                    (entry['client_counter'], rc, result)))
        elif (counter not in self.rpc_client.inflight or
                peer.ident not in self.rpc_client.inflight[counter]):
            # drop mistaken responses
            return

        self.rpc_client.response(peer, counter, rc, result)

    def incoming_proxy_publish(self, peer, msg):
        if not isinstance(msg, tuple) or len(msg) != 5:
            # drop malformed messages
            return
        self.send_publish(*msg)

    def incoming_proxy_request(self, peer, msg):
        if not isinstance(msg, tuple) or len(msg) != 6:
            # drop badly formed messages
            return
        client_counter, service, method, routing_id, args, kwargs = msg

        # handle it locally if it's aimed at us
        handler, schedule = self.find_local_handler(
                const.MSG_TYPE_RPC_REQUEST, service, method, routing_id)
        if handler is not None:
            if schedule:
                scheduler.schedule(self.rpc_handler, args=(
                    peer, client_counter, handler, args, kwargs, True))
            else:
                self.rpc_handler(
                        peer, client_counter, handler, args, kwargs, True)

        target_count = handler is not None and 1 or 0
        targets = list(self.find_peer_routes(
                const.MSG_TYPE_RPC_REQUEST, service, method, routing_id))

        if targets:
            target_count += len(targets)

            rpc = self.rpc_client.request(
                    targets, service, method, routing_id, args, kwargs)

            self.inflight_proxies[rpc.counter] = {
                'awaiting': len(targets),
                'client_counter': client_counter,
                'peer': peer,
            }

        peer.push((const.MSG_TYPE_PROXY_RESPONSE_COUNT,
                (client_counter, target_count)))

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
        const.MSG_TYPE_PUBLISH: incoming_publish,
        const.MSG_TYPE_RPC_REQUEST: incoming_rpc_request,
        const.MSG_TYPE_RPC_RESPONSE: incoming_rpc_response,
        const.MSG_TYPE_PROXY_PUBLISH: incoming_proxy_publish,
        const.MSG_TYPE_PROXY_REQUEST: incoming_proxy_request,
        const.MSG_TYPE_PROXY_RESPONSE: incoming_proxy_response,
        const.MSG_TYPE_PROXY_RESPONSE_COUNT: incoming_proxy_response_count,
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
            service, method, routing_id, args, kwargs = msg
            if self.schedule:
                scheduler.schedule(self.handler, args=args, kwargs=kwargs)
            else:
                try:
                    self.handler(*args, **kwargs)
                except Exception:
                    scheduler.handle_exception(*sys.exc_info())

        elif msgtype == const.MSG_TYPE_RPC_REQUEST:
            counter, service, method, routing_id, args, kwargs = msg
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
