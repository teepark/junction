from __future__ import absolute_import

from . import const


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

        msg = (MSG_TYPE_RPC_REQUEST,
                (counter, service, method, routing_id, args, kwargs))

        found_one = False
        targets = set()
        for peer in self.dispatcher._find_peer_routes(
                MSG_TYPE_RPC_REQUEST, service, method, routing_id):
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
            return False, None
        counter, rc, result = msg

        if counter not in self.inflight:
            # drop mistaken responses
            return False, None
        targets, done = self.inflight[counter]

        if peer.ident not in targets:
            # again, drop mistaken responses
            return False, None
        targets.remove(peer.ident)

        results = self.results.setdefault(counter, [])
        results.append((peer.ident, rc, result))

        if not targets:
            done.set()
            return True, results

        return False, None

    def wait(self, counter):
        if counter not in self.inflight or counter in self.waited_upon:
            return None

        self.waited_upon.add(counter)
        self.inflight[counter][1].wait()
        self.waited_upon.remove(counter)

        return self.results.pop(counter)
