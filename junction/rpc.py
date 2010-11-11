from __future__ import absolute_import

from greenhouse import utils
from . import const, errors


class Client(object):
    def __init__(self, dispatcher):
        self.dispatcher = dispatcher
        self.inflight = {}
        self.results = {}
        self.counter = 1
        self.waited_upon = set()

    def request(self, service, method, routing_id, args, kwargs):
        counter = self.counter
        self.counter += 1

        msg = (const.MSG_TYPE_RPC_REQUEST,
                (counter, service, method, routing_id, args, kwargs))

        found_one = False
        targets = set()
        for peer in self.dispatcher.find_peer_routes(
                const.MSG_TYPE_RPC_REQUEST, service, method, routing_id):
            found_one = True
            targets.add(peer.ident)
            peer.send_queue.put(msg)

        if not found_one:
            return 0

        self.inflight[counter] = (targets, utils.Event())
        return counter

    def response(self, peer, msg):
        if not isinstance(msg, tuple) or len(msg) != 3:
            # drop malformed responses
            return
        counter, rc, result = msg

        if counter not in self.inflight:
            # drop mistaken responses
            return
        targets, done = self.inflight[counter]

        if peer.ident not in targets:
            # again, drop mistaken responses
            return
        targets.remove(peer.ident)

        results = self.results.setdefault(counter, [])
        results.append((peer.ident, rc, result))

        if not targets:
            done.set()

    def wait(self, counter, timeout=None):
        if counter not in self.inflight or counter in self.waited_upon:
            return None

        self.waited_upon.add(counter)
        timed_out = self.inflight[counter][1].wait(timeout)

        self.inflight.pop(counter)
        self.waited_upon.remove(counter)
        results = self.results.pop(counter)

        if timed_out:
            raise errors.RPCWaitTimeout()

        return results
