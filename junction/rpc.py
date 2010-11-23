from __future__ import absolute_import

import collections

from greenhouse import utils
from . import const, errors


class Client(object):
    def __init__(self):
        self.inflight = {}
        self.waiters = {}
        self.results = {}
        self.counter = 1

    def request(self, targets, service, method, routing_id, args, kwargs):
        counter = self.counter
        targets = list(targets)
        target_set = set(peer.ident for peer in targets)

        msg = (const.MSG_TYPE_RPC_REQUEST,
                (counter, service, method, routing_id, args, kwargs))

        for peer in targets:
            target_set.add(peer.ident)
            peer.send_queue.put(msg)

        if not target_set:
            return 0

        self.inflight[counter] = target_set
        self.counter += 1
        return counter

    def response(self, peer, msg):
        if not isinstance(msg, tuple) or len(msg) != 3:
            # drop malformed responses
            return

        counter, rc, result = msg
        if counter not in self.inflight:
            # drop mistaken responses
            return

        targets = self.inflight[counter]
        if peer.ident not in targets:
            # again, drop mistaken responses
            return

        self.results.setdefault(counter, []).append((peer.ident, rc, result))

        targets.remove(peer.ident)
        if not targets:
            del self.inflight[counter]
            waiters = self.waiters.get(counter, None)
            if waiters:
                waiter = waiters.popleft()
                waiter.finish(counter)

    def wait(self, counters, timeout=None):
        if not hasattr(counters, "__iter__"):
            counters = [counters]

        waiter = Wait(self, counters)

        for counter in counters:
            if counter in self.results:
                waiter.finish(counter)
                return counter, self.results.pop(counter)

            self.waiters.setdefault(
                    counter, collections.deque()).append(waiter)

        if waiter.done.wait(timeout):
            raise errors.RPCWaitTimeout()

        return waiter.result_counter, self.results.pop(waiter.result_counter)


class Wait(object):
    def __init__(self, client, counters):
        self.client = client
        self.counters = list(counters)
        self.done = utils.Event()
        self.result_counter = None

    def finish(self, counter):
        self.result_counter = counter

        waiters = self.client.waiters
        for c in self.counters:
            counter_waiters = waiters.get(c, [])
            try:
                counter_waiters.remove(self)
            except ValueError:
                pass

            if not counter_waiters:
                waiters.pop(c, None)

        self.done.set()
