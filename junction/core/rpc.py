from __future__ import absolute_import

import weakref

from . import backend, connection, const
from .. import errors, futures


class RPCClient(object):
    REQUEST = const.MSG_TYPE_RPC_REQUEST
    CHUNKED_REQUEST = const.MSG_TYPE_REQUEST_IS_CHUNKED

    def __init__(self):
        self.counter = 1
        self.inflight = {}
        self.by_peer = {}
        self.rpcs = weakref.WeakValueDictionary()

    def next_counter(self):
        counter = self.counter
        self.counter += 1
        return counter

    def request(self, targets, msg, singular=False):
        counter = self.next_counter()

        if not targets:
            return None

        self.sent(counter, targets)

        rpc = futures.RPC(self, counter, len(targets), singular)
        self.rpcs[counter] = rpc

        for peer in targets:
            peer.push((self.REQUEST, (counter,) + msg))

        return rpc

    def chunked_request(self, counter, targets, singular=False):
        if not targets:
            return None

        self.sent(counter, targets)

        rpc = futures.RPC(self, counter, len(targets), singular)
        self.rpcs[counter] = rpc

        return rpc

    def connection_down(self, peer):
        for counter in list(self.by_peer.get(id(peer), [])):
            self.response(peer, counter, const.RPC_ERR_LOST_CONN, None)

    def response(self, peer, counter, rc, result):
        self.arrival(counter, peer)

        if counter in self.rpcs:
            self.rpcs[counter]._incoming(peer.ident, rc, result)
            if not self.inflight[counter]:
                self.rpcs[counter]._complete()
                del self.inflight[counter]
            if not self.by_peer[id(peer)]:
                del self.by_peer[id(peer)]

    def wait(self, rpc_list, timeout=None):
        if not hasattr(rpc_list, "__iter__"):
            rpc_list = [rpc_list]
        else:
            rpc_list = list(rpc_list)

        for rpc in rpc_list:
            if rpc.complete:
                return rpc

        wait = Wait(self, [r._counter for r in rpc_list])

        for rpc in rpc_list:
            rpc._waits.append(wait)

        if wait.done.wait(timeout):
            raise errors.WaitTimeout()

        return wait.completed_rpc

    def sent(self, counter, targets):
        self.inflight[counter] = set(x.ident for x in targets)
        for peer in targets:
            self.by_peer.setdefault(id(peer), set()).add(counter)

    def arrival(self, counter, peer):
        self.inflight[counter].remove(peer.ident)
        self.by_peer[id(peer)].remove(counter)


class ProxiedClient(RPCClient):
    REQUEST = const.MSG_TYPE_PROXY_REQUEST
    CHUNKED_REQUEST = const.MSG_TYPE_PROXY_REQUEST_IS_CHUNKED

    def __init__(self, client):
        super(ProxiedClient, self).__init__()
        self._client = weakref.ref(client)

    def sent(self, counter, targets):
        self.inflight[counter] = 0
        for peer in targets:
            self.by_peer.setdefault(id(peer), {})[counter] = 0

    def arrival(self, counter, peer):
        self.inflight[counter] -= 1
        self.by_peer[id(peer)][counter] -= 1
        if not self.by_peer[id(peer)][counter]:
            del self.by_peer[id(peer)][counter]

    def expect(self, peer, counter, target_count):
        try:
            self.inflight[counter] += target_count
        except KeyError:
            raise
        self.by_peer[id(peer)][counter] += target_count

        if counter in self.rpcs:
            self.rpcs[counter]._target_count = target_count

            if not self.inflight[counter]:
                self.rpcs[counter]._complete()

    def recipient_count(self, target, msg_type, service, routing_id, method):
        counter = self.next_counter()

        target.push((const.MSG_TYPE_PROXY_QUERY_COUNT,
                (counter, msg_type, service, routing_id, method)))

        self.sent(counter, set([target]))

        rpc = futures.RPC(self, counter, 1)
        self.rpcs[counter] = rpc

        self.expect(target, counter, 1)

        return rpc

    def connection_down(self, peer):
        super(ProxiedClient, self).connection_down(peer)

        client = self._client()
        if client:
            client.reset()


class Wait(object):
    def __init__(self, client, counters):
        self.client = client
        self.counters = counters
        self.done = backend.Event()
        self.transfers = {}
        self.completed_rpc = None
        self.finished = False

    def finish(self, rpc):
        if self.finished:
            return
        self.finished = True

        if rpc in self.transfers:
            self.completed_rpc = self.transfers[rpc]
        else:
            self.completed_rpc = rpc

        for counter in self.counters:
            rpc = self.client.rpcs.get(counter, None)
            if rpc:
                rpc._waits.remove(self)

        self.done.set()

    def transfer(self, source, target):
        for i, c in enumerate(self.counters):
            if c == source._counter:
                self.counters[i] = target
                break
        target._waits.append(self)
        self.transfers[target] = source

        if target.complete:
            self.finish(target)
