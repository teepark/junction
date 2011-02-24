from __future__ import absolute_import

import sys
import traceback

from greenhouse import scheduler
from . import const, errors


class Dispatcher(object):
    def __init__(self, version, rpc_client):
        self.version = version
        self.rpc_client = rpc_client
        self.peer_regs = {}
        self.local_regs = {}
        self.all_peers = {}
        self.inflight_proxies = set()

    def add_local_regs(self, handler, regs):
        added = []
        for msg_type, service, method, mask, value, schedule in regs:
            # simple sanity check -- *anything* could match this
            if value & ~mask:
                continue

            previous_regs = self.local_regs.setdefault(
                    msg_type, {}).setdefault(
                            service, {}).setdefault(
                                    method, [])

            # registrations must be mutually exclusive. that is, there cannot
            # be more than one handler on a node for any possible message
            for other_mask, other_value, other_handler in previous_regs:
                if other_mask & value == mask & other_value:
                    continue

            previous_regs.append((mask, value, handler, schedule))
            added.append((msg_type, service, method, mask, value))

        # for all connections that have already gone through their handshake,
        # send an ANNOUNCE message with the registration updates
        if added:
            for peer in self.peers():
                if peer.established.is_set() and not peer._establish_failed:
                    peer.send_queue.put((const.MSG_TYPE_ANNOUNCE, added))

        return len(added)

    def find_local_handler(self, msg_type, service, method, routing_id):
        route = self.local_regs
        for traversal in (msg_type, service, method):
            if traversal not in route:
                return None, False
            route = route[traversal]

        for mask, value, handler, schedule in route:
            if mask & routing_id == value:
                return handler, schedule

        return None, False

    def local_registrations(self):
        for msg_type, rest in self.local_regs.iteritems():
            for service, rest in rest.iteritems():
                for method, rest in rest.iteritems():
                    for mask, value, handler, schedule in rest:
                        yield (msg_type, service, method, mask, value)

    def store_peer(self, peer):
        self.all_peers[peer.addr] = peer

    def add_peer_regs(self, peer, regs):
        for msg_type, service, method, mask, value in regs:
            self.peer_regs.setdefault(
                    msg_type, {}).setdefault(
                            service, {}).setdefault(
                                    method, []).append((mask, value, peer))

    def drop_peer_regs(self, peer):
        for msg_type, service, method, mask, value in peer.regs:
            item = (mask, value, peer)
            if msg_type in self.peer_regs:
                group1 = self.peer_regs[msg_type]
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
                    self.peer_regs.pop(msg_type)

    def find_peer_routes(self, msg_type, service, method, routing_id):
        route = self.peer_regs
        for traversal in (msg_type, service, method):
            if traversal not in route:
                return
            route = route[traversal]

        for mask, value, peer in route:
            if mask & routing_id == value:
                yield peer

    def peers(self):
        return self.all_peers.itervalues()

    def send_publish(self, service, method, routing_id, args, kwargs):
        msg = (const.MSG_TYPE_PUBLISH,
                (service, method, routing_id, args, kwargs))

        found_one = False
        for peer in self.find_peer_routes(
                const.MSG_TYPE_PUBLISH, service, method, routing_id):
            found_one = True
            peer.send_queue.put(msg)

        return found_one

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

    def rpc_handler(self, peer, counter, handler, args, kwargs):
        try:
            rc = 0
            result = handler(*args, **kwargs)
        except errors.HandledError, exc:
            rc = const.RPC_ERR_KNOWN
            result = (exc.code, exc.args)
            scheduler.handle_exception(*sys.exc_info())
        except:
            rc = const.RPC_ERR_UNKNOWN
            result = traceback.format_exception(*sys.exc_info())
            scheduler.handle_exception(*sys.exc_info())

        peer.send_queue.put(
                (const.MSG_TYPE_RPC_RESPONSE, (counter, rc, result)))

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
        self.rpc_client.response(peer, msg)

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
        pass

    def incoming_proxy_response(self, peer, msg):
        pass

    handlers = {
        const.MSG_TYPE_ANNOUNCE: add_peer_regs,
        const.MSG_TYPE_PUBLISH: incoming_publish,
        const.MSG_TYPE_RPC_REQUEST: incoming_rpc_request,
        const.MSG_TYPE_RPC_RESPONSE: incoming_rpc_response,
        const.MSG_TYPE_PROXY_PUBLISH: incoming_proxy_publish,
        const.MSG_TYPE_PROXY_REQUEST: incoming_proxy_request,
        const.MSG_TYPE_PROXY_RESPONSE: incoming_proxy_response,
    }
