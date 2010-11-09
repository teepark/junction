from __future__ import absolute_import

import sys

from . import const


class Dispatcher(object):
    def __init__(self, addr, version):
        self.addr = addr
        self.version = version
        self.peer_regs = {}
        self.local_regs = {}
        self.all_peers = {}

    def add_local_regs(self, handler, regs):
        added = 0
        for msg_type, service, method, mask, value in regs:
            # simple sanity check -- *anything* could match this
            if value & ~mask:
                continue

            previous_regs = self.local_regs.setdefault(
                    msg_type, {}).setdefault(
                            service, {}).setdefault(
                                    method, [])

            # registrations must be mutually exclusive. that is, there cannot
            # be more than one handler on a single node for a single message
            for other_mask, other_value, other_handler in previous_regs:
                if other_mask & value == mask & other_value:
                    continue

            previous_regs.append((mask, value, handler))
            added += 1

        return added

    def find_local_handler(msg_type, service, method, routing_id):
        route = self.local_regs
        for traversal in (msg_type, service, method):
            if traversal not in route:
                return None
            route = route[traversal]

        for mask, value, handler in route:
            if mask & routing_id == value:
                return handler

        return None

    def local_registrations(self):
        for msg_type, rest in self.local_regs.iteritems():
            for service, rest in rest.iteritems():
                for method, rest in rest.iteritems():
                    for mask, value, handler in rest:
                        yield (msg_type, service, method, mask, value)

    def add_peer_regs(self, peer, regs):
        self.all_peers[peer.ident] = peer
        for msg_type, service, method, mask, value in regs:
            self.peer_regs.setdefault(
                    msg_type, {}).setdefault(
                            service, {}).setdefault(
                                    method, []).append((mask, value, peer))

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

        handler = self.find_local_handler(
                const.SERVICE_TYPE_PUBLISH, service, method, routing_id)
        if handler is None:
            # drop mis-delivered messages
            return

        scheduler.schedule(handler, args=args, kwargs=kwargs)

    def scheduled_rpc_handler(self, peer, counter, handler, args, kwargs):
        try:
            rc = 0
            result = handler(*args, **kwargs)
        except HandledError, exc:
            rc = const.RPC_ERR_KNOWN
            result = (exc.code, exc.args)
        except:
            rc = const.RPC_ERR_UNKNOWN
            result = traceback.format_exception(*sys.exc_info())

        peer.send_queue.put(
                (const.MSG_TYPE_RPC_RESPONSE, (counter, rc, result)))

    def incoming_rpc_request(self, peer, msg):
        if not isinstance(msg, tuple) or len(msg) != 6:
            # drop badly formed messages
            return
        counter, service, method, routing_id, args, kwargs = msg

        handler = self.find_local_handler(
                const.SERVICE_TYPE_RPC, service, method, routing_id)
        if handler is None:
            # mis-delivered message
            peer.send_queue.put(
                    (const.MSG_TYPE_RPC_RESPONSE,
                        (counter, const.RPC_ERR_NOHANDLER, None)))
            return

        scheduler.schedule(scheduled_rpc_handler,
                args=(peer, counter, handler, args, kwargs))

    def incoming_rpc_response(self, peer, msg):
        self.rpc_client.response(peer, msg)

    handlers = {
        const.MSG_TYPE_ANNOUNCE: add_peer_regs,
        const.MSG_TYPE_PUBLISH: incoming_publish,
        const.MSG_TYPE_RPC_REQUEST: incoming_rpc_request,
        const.MSG_TYPE_RPC_RESPONSE: incoming_rpc_response,
    }
