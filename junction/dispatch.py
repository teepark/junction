from __future__ import absolute_import

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
                    msg_type, {}).setdefault(
                            service, {}).setdefault(
                                    method, [])

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
                    peer.send_queue.put((const.MSG_TYPE_ANNOUNCE, added))

        return len(added)

    def find_local_handler(self, msg_type, service, method, routing_id):
        route = self.local_subs
        for traversal in (msg_type, service, method):
            if traversal not in route:
                return None, False
            route = route[traversal]

        for mask, value, handler, schedule in route:
            if mask & routing_id == value:
                return handler, schedule

        return None, False

    def local_subscriptions(self):
        for msg_type, rest in self.local_subs.iteritems():
            for service, rest in rest.iteritems():
                for method, rest in rest.iteritems():
                    for mask, value, handler, schedule in rest:
                        yield (msg_type, service, method, mask, value)

    def store_peer(self, peer):
        if peer.ident in self.peers:
            winner, loser = connection.compare(peer, self.peers[peer.ident])
            loser.go_down(reconnect=False)
            self.drop_peer_subscriptions(winner)
            peer = winner

        self.peers[peer.ident] = peer
        self.add_peer_subscriptions(peer, peer.subscriptions, extend=False)

    def drop_peer(self, peer):
        self.peers.pop(peer.ident, None)
        self.drop_peer_subscriptions(peer)

    def add_peer_subscriptions(self, peer, subscriptions, extend=True):
        for msg_type, service, method, mask, value in subscriptions:
            self.peer_subs.setdefault(
                    msg_type, {}).setdefault(
                            service, {}).setdefault(
                                    method, []).append((mask, value, peer))
        if extend:
            peer.subscriptions.extend(subscriptions)

    def drop_peer_subscriptions(self, peer):
        for msg_type, service, method, mask, value in peer.subscriptions:
            item = (mask, value, peer)
            if msg_type in self.peer_subs:
                group1 = self.peer_subs[msg_type]
                if service in group1:
                    group2 = group1[service]
                    if method in group2:
                        group3 = group2[method]
                        if item in group3:
                            group3.remove(item)
                        if not group3:
                            group2.pop(method)
                    if not group2:
                        group1.pop(service)
                if not group1:
                    self.peer_subs.pop(msg_type)

    def find_peer_routes(self, msg_type, service, method, routing_id):
        route = self.peer_subs
        for traversal in (msg_type, service, method):
            if traversal not in route:
                return
            route = route[traversal]

        for mask, value, peer in route:
            if peer.up and mask & routing_id == value:
                yield peer

    def send_publish(self, service, method, routing_id, args, kwargs):
        msg = (const.MSG_TYPE_PUBLISH,
                (service, method, routing_id, args, kwargs))

        found_one = False
        for peer in self.find_peer_routes(
                const.MSG_TYPE_PUBLISH, service, method, routing_id):
            found_one = True
            peer.send_queue.put(msg)

        return found_one

    def send_proxied_publish(self, service, method, routing_id, args, kwargs):
        self.peers.values()[0].send_queue.put(
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

        peer.send_queue.put((response, (counter, rc, result)))

    def incoming_rpc_request(self, peer, msg):
        if not isinstance(msg, tuple) or len(msg) != 6:
            # badly formed messages
            peer.send_queue.put(
                    (const.MSG_TYPE_RPC_RESPONSE,
                        (counter, const.RPC_ERR_MALFORMED, None)))
            return
        counter, service, method, routing_id, args, kwargs = msg

        handler, schedule = self.find_local_handler(
                const.MSG_TYPE_RPC_REQUEST, service, method, routing_id)
        if handler is None:
            # mis-delivered message
            peer.send_queue.put(
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
            entry['peer'].send_queue.put((const.MSG_TYPE_PROXY_RESPONSE,
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
        service, method, routing_id, args, kwargs = msg

        # in case we are the publish's destination
        # (it'll just get dropped if we have no local handler for it)
        self.incoming_publish(peer, msg)

        # in case it needs to be forwarded
        self.send_publish(service, method, routing_id, args, kwargs)

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

        peer.send_queue.put((const.MSG_TYPE_PROXY_RESPONSE_COUNT,
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
        self.rpc_client.expect(counter, target_count)

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
