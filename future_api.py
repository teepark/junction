from __future__ import absolute_import

from .core import backend
from . import errors


class Future(object):
    'A stand-in object for some value that may not have yet arrived'

    def __init__(self):
        pass

    @property
    def complete(self):
        'Whether or not this has completed'
        pass

    @property
    def value(self):
        '''The final value, if it has arrived

        :raises: AttributeError, if not yet complete
        '''
        pass

    def finish(self, value):
        '''Give the future it's value and trigger any associated callbacks

        :param value: the new value for the future
        :raises:
            :class:`AlreadyComplete <junction.errors.AlreadyComplete>` if
            already complete
        '''
        pass

    def on_finish(self, func):
        '''Assign a callback function to be run when complete

        :param function func:
            A callback to run when complete. It will be given one argument (the
            value that has arrived), and it's return value is ignored.
        '''
        pass

    def wait(self, timeout=None):
        '''Block the current coroutine until complete

        :param timeout: maximum time to block in seconds
        :type timeout: int, float or None

        :raises:
            :class:`WaitTimeout <junction.errors.WaitTimeout>` if ``timeout``
            expires before completion
        '''
        pass

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
        pass


class RPC(Future):
    '''A representation of a single RPC request/response cycle

    Instances of this class shouldn't be created directly; they are returned by
    :meth:`Hub.send_rpc <junction.hub.Hub.send_rpc>`.
    '''

    def __init__(self, target_count, singular):
        pass

    @property
    def target_count(self):
        'The number of hubs on which this RPC is waiting'
        pass

    @property
    def arrival(self):
        '''An Event for waiting on partial results

        This event is triggered whenever a response arrives, so it can be used
        to wake a blocking greenlet whenever :attr:`partial_results` receives a
        new item.
        '''
        pass

    @property
    def partial_results(self):
        '''The results that the RPC has received *so far*

        This may also be the complete results if :attr:`complete` is ``True``.
        '''
        pass

    def abort(self, klass, exc, tb=None):
        '''Finish this RPC in an error state

        Takes a standard exception triple as arguments (as returned by
        ``sys.exc_info``), and will re-raise them as the value.

        Any :class:`Dependents` that are children of this RPC will also be
        aborted.

        :param class klass: the class of the exception
        :param Exception exc: the exception instance itself
        :param traceback tb: the traceback associated with the exception
        '''
        pass


#TODO: [note to self] don't forget the transfer API
class Dependent(Future):
    '''A future with a function to run after termination of it's parent futures

    Instances of this class shouldn't be created directly; they are returned by
    :meth:`Future.after` and :func:`after`.
    '''

    def __init__(self, parents, func):
        pass

    def abort(self, klass, exc, tb=None):
        '''Finish this future (maybe early) in an error state

        Takes a standard exception triple as arguments (like returned by
        ``sys.exc_info``) and will re-raise them as the value.

        Any other :class:`Dependents` that are children of this one will also
        be aborted.

        :param class klass: the class of the exception
        :param Exception exc: the exception instance itself
        :param traceback tb: the traceback associated with the exception
        '''
        pass


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

    pass
