from __future__ import absolute_import

import weakref

from greenhouse import utils
import mummy
from . import const, errors


class RPCClient(object):
    REQUEST = const.MSG_TYPE_RPC_REQUEST

    def __init__(self):
        self.counter = 1
        self.inflight = {}
        self.by_peer = {}
        self.rpcs = weakref.WeakValueDictionary()

    def request(self, targets, service, method, routing_id, args, kwargs):
        counter = self.counter
        self.counter += 1
        target_set = set()

        msg = (self.REQUEST,
                (counter, service, method, routing_id, args, kwargs))

        target_count = 0
        for peer in targets:
            target_set.add(peer)
            peer.push(msg)
            target_count += 1

        if not target_set:
            return None

        self.sent(counter, target_set)

        rpc = RPC(self, counter, target_count)
        self.rpcs[counter] = rpc

        return rpc

    def connection_down(self, peer):
        for counter in self.by_peer.get(id(peer), []):
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
            if rpc._completed:
                return rpc

        wait = Wait(self, [r.counter for r in rpc_list])

        for rpc in rpc_list:
            rpc._waits.append(wait)

        if wait.done.wait(timeout):
            raise errors.RPCWaitTimeout()

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
        self.inflight[counter] += target_count
        self.by_peer[id(peer)][counter] += target_count

        if counter in self.rpcs:
            self.rpcs[counter]._target_count = target_count

            if not self.inflight[counter]:
                self.rpcs[counter]._complete()


class RPC(object):
    """A representation of a single RPC request/response cycle

    instances of this class shouldn't be created directly, they are returned by
    :meth:`Node.send_rpc() <junction.node.Node.send_rpc>`.
    """
    def __init__(self, client, counter, target_count):
        self._client = client
        self._completed = False
        self._waits = []
        self._results = []

        self._counter = counter
        self._target_count = target_count

    def wait(self, timeout=None):
        """Block the current greenlet until the response arrives

        :param timeout:
            the maximum number of seconds to wait before raising a
            :class:`RPCWaitTimeout <junction.errors.RPCWaitTimeout>`. the
            default of None allows it to wait indefinitely.
        :type timeout: int, float or None

        :returns: a list of the responses returned by the RPC's target peers.

        :raises:
            :class:`RPCWaitTimeout <junction.errors.RPCWaitTimeout>` if
            ``timeout`` is supplied and runs out before the response arrives.
        """
        self._client.wait(self, timeout)
        return self.results

    @property
    def counter(self):
        return self._counter

    @property
    def target_count(self):
        "The number of peer nodes that will return a response for this RPC"
        return self._target_count

    @property
    def partial_results(self):
        """The results that the RPC has received so far

        this may also be the complete results, if :attr:`complete` is True
        """
        return [type(x)(*deepcopy(x.args)) if isinstance(x, Exception)
                else deepcopy(x) for x in self._results]

    @property
    def results(self):
        """The RPC's response, if it has arrived

        :attr:`complete` indicates whether the result is available or not, if
        not then this attribute raises AttributeError.
        """
        if not self._completed:
            raise AttributeError("incomplete response")
        return self.partial_results

    @property
    def complete(self):
        "Whether the RPC's response has arrived yet."
        return self._completed

    def _incoming(self, peer_ident, rc, result):
        self._results.append(self._format_result(peer_ident, rc, result))

    def _complete(self):
        self._completed = True
        if self._waits:
            self._waits[0].finish(self)

    def _format_result(self, peer_ident, rc, result):
        if not rc:
            return result

        if rc == const.RPC_ERR_NOHANDLER:
            return errors.NoRemoteHandler(
                    "RPC mistakenly sent to %r" % (peer_ident,))

        if rc == const.RPC_ERR_KNOWN:
            err_code, err_args = result
            return errors.HANDLED_ERROR_TYPES.get(
                    err_code, errors.HandledError)(peer_ident, *err_args)

        if rc == const.RPC_ERR_UNKNOWN:
            return errors.RemoteException(peer_ident, result)

        if rc == const.RPC_ERR_LOST_CONN:
            return errors.LostConnection(peer_ident)

        return errors.UnrecognizedRemoteProblem(peer_ident, rc, result)


class Wait(object):
    def __init__(self, client, counters):
        self.client = client
        self.counters = counters
        self.done = utils.Event()
        self.completed_rpc = None

    def finish(self, rpc):
        self.completed_rpc = rpc

        rpcs = self.client.rpcs
        for counter in self.counters:
            rpc = rpcs.get(counter, None)
            if not rpc:
                continue
            rpc._waits.remove(self)

        self.done.set()


def deepcopy(item):
    return mummy.loads(mummy.dumps(item))
