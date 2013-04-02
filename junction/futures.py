from __future__ import absolute_import

import logging
import sys
import weakref

from .core import backend, dispatch
from . import errors


__all__ = ["Future", "after", "wait_any"]


log = logging.getLogger("junction.futures")


class Future(object):
    'A stand-in object for some value that may not have yet arrived'

    def __init__(self):
        self._done = backend.Event()
        self._waits = set()
        self._children = []
        self._value = None
        self._failure = None
        self._cbacks = []
        self._errbacks = []

    @property
    def complete(self):
        'Whether or not this has completed'
        return self._done.is_set()

    @property
    def value(self):
        '''The final value, if it has arrived

        :raises: AttributeError, if not yet complete
        :raises: an exception if the Future was :meth:`abort`\ed
        '''
        if not self._done.is_set():
            raise AttributeError("value")
        if self._failure:
            raise self._failure[0], self._failure[1], self._failure[2]
        return self._value

    def finish(self, value):
        '''Give the future it's value and trigger any associated callbacks

        :param value: the new value for the future
        :raises:
            :class:`AlreadyComplete <junction.errors.AlreadyComplete>` if
            already complete
        '''
        if self._done.is_set():
            raise errors.AlreadyComplete()

        self._value = value

        for cb in self._cbacks:
            backend.schedule(cb, args=(value,))
        self._cbacks = None

        for wait in list(self._waits):
            wait.finish(self)
        self._waits = None

        for child in self._children:
            child = child()
            if child is None:
                continue
            child._incoming(self, value)
        self._children = None

        self._done.set()

    def abort(self, klass, exc, tb=None):
        '''Finish this future (maybe early) in an error state

        Takes a standard exception triple as arguments (like returned by
        ``sys.exc_info``) and will re-raise them as the value.

        Any :class:`Dependents` that are children of this one will also be
        aborted.

        :param class klass: the class of the exception
        :param Exception exc: the exception instance itself
        :param traceback tb: the traceback associated with the exception

        :raises:
            :class:`AlreadyComplete <junction.errors.AlreadyComplete>` if
            already complete
        '''
        if self._done.is_set():
            raise errors.AlreadyComplete()

        self._failure = (klass, exc, tb)

        for wait in self._waits:
            wait.finish(self)
        self._waits = None

        for eb in self._errbacks:
            backend.schedule(eb, args=(klass, exc, tb))
        self._errbacks = None

        for child in self._children:
            child = child()
            if child is None:
                continue
            child.abort(klass, exc, tb)
        self._children = None

        self._done.set()

    def on_finish(self, func):
        '''Assign a callback function to be run when successfully complete

        :param function func:
            A callback to run when complete. It will be given one argument (the
            value that has arrived), and it's return value is ignored.
        '''
        if self._done.is_set():
            if self._failure is None:
                backend.schedule(func, args=(self._value,))
        else:
            self._cbacks.append(func)

    def on_abort(self, func):
        '''Assigna a callback function to be run when :meth:`abort`\ed

        :param function func:
            A callback to run when aborted. It will be given three arguments:

                - ``klass``: the exception class
                - ``exc``: the exception instance
                - ``tb``: the traceback object associated with the exception
        '''
        if self._done.is_set():
            if self._failure is not None:
                backend.schedule(func, args=self._failure)
        else:
            self._errbacks.append(func)

    def wait(self, timeout=None):
        '''Block the current coroutine until complete

        :param timeout: maximum time to block in seconds
        :type timeout: int, float or None

        :raises:
            :class:`WaitTimeout <junction.errors.WaitTimeout>` if ``timeout``
            expires before completion
        '''
        if self._done.wait(timeout):
            raise errors.WaitTimeout()

    def after(self, func=None, other_parents=None):
        '''Create a new Future whose completion depends on this one

        The new future will have a function that it calls once all its parents
        have completed, the return value of which will be its final value.
        There is a special case, however, in which the dependent future's
        callback returns a future or list of futures. In those cases, waiting
        on the dependent will also wait for all those futures, and the result
        (or list of results) of those future(s) will then be the final value.

        :param function func:
            The function to determine the value of the dependent future. It
            will take as many arguments as it has parents, and they will be the
            results of those futures.

        :param other_parents:
            A list of futures, all of which (along with this one) must be
            complete before the dependent's function runs.
        :type other_parents: list or None

        :returns:
            a :class:`Dependent`, which is a subclass of :class:`Future` and
            has all its capabilities.
        '''
        parents = [self]
        if other_parents is not None:
            parents += other_parents
        return after(parents, func)


class RPC(Future):
    '''A representation of a single RPC request/response cycle

    Instances of this class shouldn't be created directly; they are returned by
    :meth:`Hub.send_rpc <junction.hub.Hub.send_rpc>`.
    '''

    def __init__(self, target_count, singular):
        super(RPC, self).__init__()
        self._target_count = target_count
        self._singular = singular
        self._results = []
        self._arrival = backend.Event()

    @property
    def target_count(self):
        'The number of hubs on which this RPC is waiting'
        return self._target_count

    @property
    def arrival(self):
        '''An Event for waiting on partial results

        This event is triggered whenever a response arrives, so it can be used
        to wake a blocking greenlet whenever :attr:`partial_results` receives a
        new item.
        '''
        return self._arrival

    @property
    def partial_results(self):
        '''The results that the RPC has received *so far*

        This may also be the complete results if :attr:`complete` is ``True``.
        '''
        results = []
        for r in self._results:
            if isinstance(r, Exception):
                results.append(type(r)(*deepcopy(r.args)))
            elif hasattr(r, "__iter__") and not hasattr(r, "__len__"):
                # pass generators straight through
                results.append(r)
            else:
                results.append(deepcopy(r))
        return results

    def abort(self, klass, exc, tb=None):
        self._results = None
        super(RPC, self).abort(klass, exc, tb)

    def _expect(self, count):
        if self._done.is_set():
            return

        self._target_count = count
        if count == 0:
            self.abort(errors.Unroutable, errors.Unroutable())
        elif count == len(self._results):
            self._finish()

    def _incoming(self, target, rc, data):
        if self._done.is_set():
            return

        self._results.append(
                dispatch._check_error(log, target, rc, data))
        self._arrival.set()
        self._arrival.clear()

        if len(self._results) == self._target_count:
            self._finish()

    def _finish(self):
        final = self._results
        self._results = None
        if self._singular:
            final = final[0]
        self.finish(final)


class Dependent(Future):
    '''A future with a function to run after termination of it's parent futures

    Instances of this class shouldn't be created directly; they are returned by
    :meth:`Future.after` and :func:`after`.
    '''

    def __init__(self, parents, func):
        super(Dependent, self).__init__()

        self._parents = parents
        self._func = func
        self._parent_results = [None] * len(parents)
        self._parent_indexes = dict((v, i) for i, v in enumerate(parents))
        self._transfer = None

    def _incoming(self, parent, value):
        index = self._parent_indexes.pop(parent)
        self._parents[index] = None
        self._parent_results[index] = value

        if all(p is None for p in self._parents):
            backend.schedule(self._run_func)

    def _run_func(self):
        try:
            value = self._func(*self._parent_results)
        except Exception:
            self.abort(*sys.exc_info())
        else:
            if (isinstance(value, list)
                    and all(isinstance(v, Future) for v in value)):
                value = after(value, lambda *l: l)
            if isinstance(value, Future):
                value.on_finish(self.finish)
                value.on_abort(self.abort)
                self._transfer = value
            else:
                self.finish(value)

    def finish(self, value):
        super(Dependent, self).finish(value)
        self._transfer = None

    def abort(self, klass, exc, tb=None):
        super(Dependent, self).abort(klass, exc, tb)
        self._transfer = None


def after(parents, func=None):
    '''Create a new Future whose completion depends on parent futures

    The new future will have a function that it calls once all its parents
    have completed, the return value of which will be its final value.
    There is a special case, however, in which the dependent future's
    callback returns a future or list of futures. In those cases, waiting
    on the dependent will also wait for all those futures, and the result
    (or list of results) of those future(s) will then be the final value.

    :param parents:
        A list of futures, all of which must be complete before the
        dependent's function runs.
    :type parents: list

    :param function func:
        The function to determine the value of the dependent future. It
        will take as many arguments as it has parents, and they will be the
        results of those futures.

    :returns:
        a :class:`Dependent`, which is a subclass of :class:`Future` and
        has all its capabilities.
    '''
    if func is None:
        return lambda f: after(parents, f)

    dep = Dependent(parents, func)
    for parent in parents:
        if parent.complete:
            dep._incoming(parent, parent.value)
        else:
            parent._children.append(weakref.ref(dep))
    return dep


def wait_any(futures, timeout=None):
    '''Wait for the completion of any (the first) one of multiple futures

    :param list futures: A list of :class:`Future`\s
    :param timeout:
        The maximum time to wait. With ``None``, will block indefinitely.
    :type timeout: float or None

    :returns:
        One of the futures from the provided list -- the first one to become
        complete (or any of the ones that were already complete).
    '''
    for fut in futures:
        if fut.complete:
            return fut

    wait = _Wait(futures)

    for fut in futures:
        fut._waits.add(wait)

    if wait.done.wait(timeout):
        raise errors.WaitTimeout()

    return wait.completed_future


class _Wait(object):
    def __init__(self, futures):
        self.futures = set(futures)
        self.done = backend.Event()
        self.completed_future = None
        self.finished = False

    def finish(self, fut):
        if self.finished:
            return
        self.finished = True

        self.completed_future = fut

        for future in self.futures:
            future._waits.remove(self)

        self.done.set()


def deepcopy(item):
    return mummy.loads(mummy.dumps(item))
