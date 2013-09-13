.. _programming-with-futures:

========================
Programming With Futures
========================

Junction's asynchronous RPC API includes a
:class:`Future <junction.futures.Future>` base class, the instances of
which represent a value which might be as-of-yet unknown [1]_. These
provide a powerful API for building complex graphs of concurrent
actions.

.. [1] `wikipedia has a good explanation
    <http://en.wikipedia.org/wiki/Futures_and_promises>`_


Overview
--------

Two concrete :class:`Future <junction.futures.Future>` subclasses made
available are :class:`RPC <junction.futures.RPC>` and
:class:`Dependent <junction.futures.Dependent>`.

RPC objects always correspond to a single RPC, and they represent the
response value. They are created by
:meth:`Hub.send_rpc <junction.hub.Hub.send_rpc>` or
:meth:`client.send_rpc <junction.client.Client.send_rpc>`.

Dependents can be a bit more complicated. They are created as a child of
one or more parent Futures and wrap a callback. It will run the callback
when all parent futures have completed, and it represents the function's
return value.



Dependents are futures which depend on some other future completing
before they are able to generate their response. They are created with
the :meth:`Future.after <junction.futures.Future.after>` method.

A simple case of a dependent is if you already have an RPC that will
produce an integer, but when it arrives you will actually want the square
of its return value:

.. sourcecode:: python

    def get_square(hub):
        rpc = hub.send_rpc('SERVICE', 0, 'some_int')
        square = rpc.after(lambda i: i * i)
        return square

In this example ``square`` is still a future which won't become complete
until the RPC has returned, but instead of producing the RPC's result,
it will produce the return value of the callback function it was given,
which isn't called until the parent future (the RPC here) is completed.

Dependents can also be used to chain RPCs in the case where you have an
in-flight RPC and will want to send another one using the response of
the first in the arguments for the second. In that case the second one
truly *depends* on the first, and you can use a Dependent to model the
relationship.


Dependents can be built to wait on any
combination of RPCs and other Dependents, and they can either represent
a not-yet-started RPC or the results of any callback function.
Dependents wrap a callback that isn't fired until all its parent futures
are complete, then calls the callback passing it the results of its
parents, and using the callback result as its own result (though if the
callback returns an RPC, that is then waited on as well and its results
are used as those of the Dependent).

Both objects share a common API, so in some circumstances can be used
interchangeably.

Using RPC instances with the **send_rpc()** and **wait_any()** methods
of :class:`Hub <junction.hub.Hub>` and :class:`Client
<junction.client.Client>` objects is a great way to parallelize RPCs
within a single coroutine, and Dependents are a natural extension of
that for when one needs to make RPC calls whose arguments are taken from
the results of prior RPCs.

But programming with futures can be much more effective when the future
objects themselves are used *instead of*, or *in place of* the results
they generate. Consider a webserver endpoint that will generate HTML for
an HTTP response out of data it collects with RPCs. This process doesn't
usually actually need any of the data it collected until the very last
step, building the HTML (often rendering a template), so there should be
*NO* reason to wait on *anything* until then.

Before then, any place we would have passed RPC-collected data into a
function or had it in a variable, we can instead use a future for that
data in its place. Anything that we would have done with that data, we
can instead do in a Dependent of the future. With this approach we can
maximize the parallelism between webserver and data providers and have
that parallelism track the true dependency graph between RPCs, rather
than end up with choke points scattered throughout the process.

Consider the simple example below of pseudocode validating an entered
username and password:

.. sourcecode:: python

    def validate(hub, username, passwd):
        userid = hub.rpc(USERDATA_SERVICE, 0, "by_username", (username,), {})[0]
        is_valid = hub.rpc(PASSWORD_SERVICE, 0, "validate", (userid, passwd), {})[0]
        return is_valid

.. image:: imgs/single_validate.png

To go async and program with futures, we must use a Dependent for the
second RPC as its arguments include the results from the first RPC.

.. sourcecode:: python

    def validate(hub, username, passwd):
        userid = hub.send_rpc(USERDATA_SERVICE, 0, "by_username", (username,), {})

        @userid.after
        def is_valid(userid):
            return hub.send_rpc(PASSWORD_SERVICE, 0, "validate", (userid[0], passwd))

        return is_valid.wait()[0]

This doesn't help us yet, though. There are only two RPCs here and the
second *must* wait for the first, and the method still blocks until both
RPCs have completed. Where the approach really helps us is when we need
to validate a batch of username, password pairs:

.. sourcecode:: python

    validations = [(un, pw, validate(hub, un, pw)) for un, pw in groups]

.. image:: imgs/seq_validates.png

With just a tiny modification to the futures-based validate method, and
a little supporting code in the batching, we can get each of the
sequential RPC pairs happening together in parallel:

.. sourcecode:: python

    def validate(hub, username, passwd):
        userid = hub.send_rpc(USERDATA_SERVICE, 0, "by_username", (username,), {})

        @userid.after
        def is_valid(userid):
            return hub.send_rpc(PASSWORD_SERVICE, 0, "validate", (userid[0], passwd))

        return is_valid

    with_futures = [(un, pw, validate(hub, un, pw)) for un, pw in groups]
    validations = [(un, pw, f.wait()[0]) for un, pw, f in with_futures]

.. image:: imgs/par_validates.png

The only change to the validate function was that it now returns the
future pointing at the boolean result, rather than the result itself.
Doing it this way allows us to queue up all the RPCs before we have to
wait on any of them, allowing the whole process to be run in parallel
for each username/password pair.


The Future Class API
--------------------

**complete** attribute
    True if the future is finished, and therefore "value" is available

**value** attribute
    Raises AttributeError if the future isn't yet complete, but
    otherwise produces the results of whatever future action the object
    represents. If the future was aborted with
    :meth:`abort <junction.futures.Future.abort>`, this will re-raise
    the given exception.

**finish()** method
    Takes an argument and sets that as the future's value, makes the
    future "complete", activates any on_finish callbacks, and wakes up
    any coroutines that are blocked waiting.

**abort()** method
    Takes three arguments -- an exception triple ``(klass, exception,
    traceback)`` and records it as the future's failure, makes it
    "complete", activates any on_abort callbacks, and wakes up any
    coroutines that are blocked waiting.

**on_finish** method
    Takes a callback function which should accept one argument. It will
    be called in its own coroutine and given the future's value if/when
    it becomes complete.

**on_abort** method
    Takes a callback function which should accept a single argument, an
    exception triple ``(klass, exception, traceback)``. It will be
    called in its own coroutine and given the future's failure if it is
    aborted.

**wait()** method
    Blocks the current coroutine until the future is complete. With an
    optional timeout, raises
    :class:`WaitTimeout <junction.errors.WaitTimeout>` if it expires
    with the future still incomplete.

**after()** method
    Create and return a new
    :class:`Dependent <junction.futures.Dependent>` which depends on
    this future. Takes a function for the new Dependent to wrap.

**get()** method
    Equivalent to calling :meth:`wait() <junction.futures.Future.wait>`
    and then returning the
    :attr:`value <junction.futures.Future.value>`. Also accepts an
    optional timeout.


The Futures Module API
----------------------

**after()** method
    This version of ``after`` takes a list of parent futures, and a
    callback function. This is a way to get a Dependent which has
    *multiple* futures on which it depends (it won't become complete and
    run its function until all parents have completed).

**wait_any()** method
    Accepts a list of futures and an optional timeout and blocks until
    *any* of them is complete (or the timeout expires). Returns the
    complete future, or raises
    :class:`WaitTimeout <junction.errors.WaitTimeout>`.

**wait_all()** method
    Also accepts a list of futures and an optional timeout, but blocks
    until they are *all* complete. No return value, but it may raise
    :class:`WaitTimeout <junction.errors.WaitTimeout>`.


RPCs
----

:class:`RPCs <junction.futures.RPC>` are created by calls to
:meth:`Hub.send_rpc <junction.hub.Hub.send_rpc>` or
:meth:`Client.send_rpc <junction.client.Client.send_rpc>`. These objects
are a representation of the single in-flight RPC call.

They support the full :class:`Future <junction.futures.Future>` API, but
the system will call ``finish()`` when the RPC result arrives, so there
is no need to use that method directly.

RPC objects also add three additional attributes, which mainly apply in
the case of broadcast RPCs (created with ``send_rpc(...,
broadcast=True)``).
:attr:`target_count <junction.futures.RPC.target_count>` is the number
of peers to which the RPC was sent,
:attr:`partial_results <junction.futures.RPC.partial_results>` is a list
of all the results which have arrived **so far**, and
:attr:`arrival <junction.futures.RPC.arrival>` is an ``Event`` object
that gets triggered whenever a result arrives from a peer. So by
repeatedly waiting with ``rpc.arrival.wait(timeout)`` and picking up
``rpc.partial_results`` each time, you can process the individual RPC
results as they arrive.


Dependents
----------

:class:`Dependents <junction.futures.Dependent>` are created by calls
to :meth:`RPC.after <junction.futures.RPC.after>` and
:meth:`Dependent.after <junction.futures.Dependent.after>`. They wrap a
callback function (passed to `after()`), and call it when the future(s)
they depend on have finished.

Although they are created by the `after()` method of a single future, a
Dependent can have more than one parent (it will wait for all of its
parents to complete before firing its callback). To create a
multi-parent Dependent pass any additional future objects in a list as
the `other_parents` argument.

The Dependent's callback should take as many arguments as it has
parents. The values passed in will be the results of the parents in
order (the future on which `.after` was called comes before the
`other_parents`). The return value of the callback matters as well. If
the Dependent callback returns an RPC (by calling `send_rpc` in the
callback), then the Dependent won't be considered complete until that
RPC has completed, and the Dependent's results will be taken straight
from the RPC. Effectively, the RPC takes the Dependent's place in the
dependency graph. In any other case (the callback doesn't return an RPC
instance), the return value is simply used as the Dependent's result.
