from __future__ import absolute_import

import weakref

from greenhouse import scheduler, utils
import mummy
from . import const
from .. import errors


class RPCClient(object):
    REQUEST = const.MSG_TYPE_RPC_REQUEST

    def __init__(self):
        self.counter = 1
        self.inflight = {}
        self.by_peer = {}
        self.rpcs = weakref.WeakValueDictionary()

    def next_counter(self):
        counter = self.counter
        self.counter += 1
        return counter

    def request(self, targets, service, method, routing_id, args, kwargs):
        counter = self.next_counter()
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

    def recipient_count(self, target, msg_type, service, method, routing_id):
        counter = self.counter
        self.counter += 1

        target.push((const.MSG_TYPE_PROXY_QUERY_COUNT,
                (counter, msg_type, service, method, routing_id)))

        self.sent(counter, set([target]))

        rpc = RPC(self, counter, 1)
        self.rpcs[counter] = rpc

        self.expect(target, counter, 1)

        return rpc


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
        self._children = []

        self._counter = counter
        self._target_count = target_count

    def wait(self, timeout=None):
        """Block the current greenlet until the response arrives

        :param timeout:
            the maximum number of seconds to wait before raising a
            :class:`WaitTimeout <junction.errors.WaitTimeout>`. the default of
            None allows it to wait indefinitely.
        :type timeout: int, float or None

        :returns: a list of the responses returned by the RPC's target peers.

        :raises:
            :class:`WaitTimeout <junction.errors.WaitTimeout>` if ``timeout``
            is supplied and runs out before the response arrives.
        """
        self._client.wait(self, timeout)
        return self.results

    def after(self, func, other_parents=None):
        """Schedule ``func`` to run after the RPC has completed

        :param func:
            a callback to run after this RPC and any others in
            ``other_parents`` have completed. the callback should take as many
            arguments as the Dependent has parents (one plus the length of
            other_parents), and each argument will be one parent's results. The
            callback's return value can be an :class:`RPC` instance, in which
            case waiting on the Dependent will also wait for that RPC to
            complete, or any other object, which will appear as the
            :attr:`results <Dependent.results>` attribute
        :type func: function

        :param other_parents:
            any combination of other :class:`RPC`\s and :class:`Dependent`\s
            that should be waited on for completion before firing the callback
        :type other_parents: list or None

        :returns:
            a :class:`Dependent`, which can be waited on, waited on as part of
            a group of other :class:`RPC`\s and :class:`Dependent`\s with
            :meth:`Node.wait_any <junction.node.Node.wait_any>`
        """
        parents = [self] + (other_parents or [])
        counter = self._client.next_counter()

        op = Dependent(self._client, counter, parents, func)
        self._children.append(weakref.ref(op))
        self._client.rpcs[counter] = op

        return op

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

        for wait in self._waits:
            wait.finish(self)
        self._waits = []

        for child in self._children:
            child = child()
            if child is None:
                continue

            child._incoming(self, self.results)
        self._children = []

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


class Dependent(object):
    """A function wrapper queued up to run after the termination of RPCs

    instances of this class shouldn't be created directly, they are returned by
    :meth:`RPC.after <junction.rpc.RPC.after>` and
    :meth:`Dependent.after <junction.rpc.Dependent.after>`
    """
    def __init__(self, client, counter, parents, func):
        self._client = client
        self._counter = counter
        self._parents = parents
        self._parent_results = [None] * len(parents)
        self._children = []
        self._func = func
        self._completed = False
        self._waits = []
        self._result = None

        complete = filter(lambda p: p.complete, self._parents)
        for parent in complete:
            self._incoming(parent, parent.results)

    def wait(self, timeout=None):
        """Block the current greenlet until the operation has completed

        :param timeout:
            the maximum number of seconds to wait before raising a
            :class:`WaitTimeout <junction.errors.WaitTimeout>`, the default of
            None allows it to wait indefinitely.
        :type timeout: int, float or None

        :returns:
            The return value of the operation's function, unless it returned a
            :class:`RPC`, in which case it returns the list of that RPC's
            results

        :raises:
            :class:`WaitTimeout <junction.errors.WaitTimeout>` if ``timeout``
            is supplied and runs out before the response arrives.
        """
        self._client.wait(self, timeout)
        return self.results

    def after(self, func, other_parents=None):
        """Schedule ``func`` to run after the Dependent has completed

        :param func:
            a callback to run after this Dependent and any others in
            ``other_parents`` have completed. the callback should take as many
            arguments as the new Dependent has parents (one plus the length of
            other_parents), and each argument will be one parent's results. The
            callback's return value can be an :class:`RPC` instance, in which
            case waiting on the Dependent will also wait for that RPC to
            complete, or any other object which will then appear as the
            :attr:`results <Dependent.results>` attribute
        :type func: function
        :param other_parents:
            any combination of other :class:`RPC`\s and :class:`Dependent`\s
            that should be waited on for completion before firing the callback
        :type other_parents: list or None

        :returns:
            a :class:`Dependent`, which can be waited on, waited on as part of
            a group of other :class:`RPC`\s and :class:`Dependent`\s with
            :meth:`Node.wait_any <junction.node.Node.wait_any>`
        """
        parents = [self] + (other_parents or [])
        counter = self._client.next_counter()

        op = Dependent(self._client, counter, parents, func)
        self._children.append(weakref.ref(op))
        self._client.rpcs[counter] = op

        return op

    @property
    def results(self):
        """the results of the callback, or the results of the callback's RPC

        :attr:`complete` indicates whether the result is available or not, if
        not then this attribute raises AttributeError.
        """
        if not self._completed:
            raise AttributeError("incomplete operation")
        if isinstance(self._result, RPC):
            return self._result.results
        return self._result

    @property
    def complete(self):
        "Whether the Dependent (and possibly its resulting RPC) has completed"
        if not self._completed:
            return False
        if isinstance(self._result, RPC):
            return self._result.complete
        return True

    def _incoming(self, parent, result):
        index = self._parents.index(parent)
        self._parents[index] = None
        self._parent_results[index] = result

        if all(p is None for p in self._parents):
            self._complete()

    def _func_runner(self):
        self._completed = True

        try:
            self._result = self._func(*self._parent_results)
        except Exception:
            #TODO: exceptions abort the depenent and all descendents
            pass
        self._parent_result = self._parents = None

        if (not isinstance(self._result, RPC)) or self._result.complete:
            for wait in self._waits:
                wait.finish(self)

            for child in self._children:
                child = child()
                if child is None:
                    continue

                child._incoming(self, self.results)
        else:
            for wait in self._waits:
                wait.transfer(self, self._result)
            for child in self._children:
                child = child()
                if child is None:
                    continue

                child._transfer(self, self._result)
                self._result._children.append(weakref.ref(child))

        self._waits = []
        self._children = []

    def _complete(self):
        # at this point we are in a connection's receive coro. if this
        # user-provided function is stupid and blocks, it could potentially
        # wait on something that would need to come back through the connection
        # whose receiver coro it just blocked, resulting in deadlock. so run
        # the callback in a separate coro just in case.
        scheduler.schedule(self._func_runner)

    def _transfer(self, source, target):
        for i, parent in enumerate(self._parents):
            if parent is source:
                self._parents[i] = target
                if target.complete:
                    self._incoming(target, target.results)
                break


def deepcopy(item):
    return mummy.loads(mummy.dumps(item))
