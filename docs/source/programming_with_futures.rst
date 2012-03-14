.. _programming-with-futures:

========================
Programming With Futures
========================

Junction's asynchronous RPC api involves a number of objects that meet
the definition of a `"future"`_. The fit the bill because when initially
created, their results are not yet known but will be transparently
filled in when they arrive, or they can be explicitly waited for.


Overview
--------

These are :class:`RPCs <junction.futures.RPC>` and
:class:`Dependents <junction.futures.Dependent>`.
RPCs always correspond to a :meth:`single RPC call
<junction.hub.Hub.send_rpc>`. Dependents can be built to wait on any
combination of RPCs and other Dependents, and they can either represent
a not-yet-started RPC or the results of any callback function.Dependents
wrap a callback that isn't fired until all its parent futures are
complete, then calls the callback passing it the results of its parents,
and using the callback result as its own result (though if the callback
returns an RPC, that is then waited on as well and its results are used
as those of the Dependent).

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
data in its place.  Anything that we would have done with that data, we
can instead do in a Dependent of the future. With this approach we can
maximize the parallelism between webserver and data providers and have
that parallelism track the true dependency graph between RPCs, rather
than end up with choke points scattered throughout the process.

Consider the simple example below of pseudocode validating an entered
username and password::

    def validate(hub, username, passwd):
        userid = hub.rpc(USERDATA_SERVICE, "by_username", 0, (username,), {})[0]
        is_valid = hub.rpc(PASSWORD_SERVICE, "validate", 0, (userid, passwd), {})[0]
        return is_valid

.. image:: imgs/single_validate.png

To go async and program with futures, we must use a Dependent for the
second RPC as its arguments include the results from the first RPC.::

    def validate(hub, username, passwd):
        userid = hub.send_rpc(USERDATA_SERVICE, "by_username", 0, (username,), {})

        @userid.after
        def is_valid(userid):
            return hub.send_rpc(PASSWORD_SERVICE, "validate", 0, (userid[0], passwd))

        return is_valid.wait()[0]

This doesn't help us yet, though. There are only two RPCs here and the
second *must* wait for the first, and the method still blocks until both
RPCs have completed. Where the approach really helps us is when we need
to validate a batch of username, password pairs::

    validations = [(un, pw, validate(hub, un, pw)) for un, pw in groups]

.. image:: imgs/seq_validates.png

With just a tiny modification to the futures-based validate method, and
a little supporting code in the batching, we can get each of the
sequential RPC pairs happening together in parallel::

    def validate(hub, username, passwd):
        userid = hub.send_rpc(USERDATA_SERVICE, "by_username", 0, (username,), {})

        @userid.after
        def is_valid(userid):
            return hub.send_rpc(PASSWORD_SERVICE, "validate", 0, (userid[0], passwd))

        return is_valid

    with_futures = [(un, pw, validate(hub, un, pw)) for un, pw in groups]
    validations = [(un, pw, f.wait()[0]) for un, pw, f in with_futures]

.. image:: imgs/par_validates.png

The only change to the validate function was that it now returns the
future pointing at the boolean result, rather than the result itself.
Doing it this way allows us to queue up all the RPCs before we have to
wait on any of them, allowing the whole process to be run in parallel
for each username/password pair.


.. _`"future"`: http://en.wikipedia.org/wiki/Futures_and_promises


The Common API of RPCs and Dependents
-------------------------------------

**complete** attribute
    True if the future is finished, and therefore "results" is available

**results** attribute
    Raises AttributeError if the future isn't yet complete, but
    otherwise produces the results of whatever future action the object
    represents.

**wait()** method
    Blocks the current coroutine until the future is complete, and then
    returns its results. Also accepts a timeout, however.

**abort()** method
    Prevents the future from completing, waking any coroutines that are
    blocked waiting. Takes a single argument and sets that as the result
    of this future. Also aborts any Dependents made to depend on this
    future, passing the provided result down.

**after()** method
    Creates and returns a new :class:`Dependent
    <junction.futures.Dependent>` that depends on this future. Also
    optionally accepts a list of other future objects on which it should
    depend.

**Hub.wait_any()** and **Client.wait_any()**
    Both :class:`Hub <junction.hub.Hub>` and :class:`Client
    <junction.client.Client>` have a method "wait_all", which accepts a
    list of futures (these can be any mixtures of
    :class:`RPCs <junction.futures.RPC>` and :class:`Dependents
    <junction.futures.Dependent>`), and returns the first complete
    future from the list.


RPCs
----

:class:`RPCs <junction.futures.RPC>` are created by calls to
:meth:`Hub.send_rpc <junction.hub.Hub.send_rpc>` and
:meth:`Client.send_rpc <junction.client.Client.send_rpc>`. These objects
are a representation of the single in-flight RPC call.

The :attr:`results <junction.futures.RPC.results>` attribute will
always be a list, and it will be filled with the returned results from
each peer that was targeted by the RPC. In the event of an :meth:`abort
<junction.futures.RPC.abort>`, the results list will have length 1 --
the result passed into the abort method.

RPC objects also have three additional attributes that Dependents don't:
:attr:`target_count <junction.futures.RPC.target_count>`,
:attr:`partial_results <junction.futures.RPC.partial_results>`, and
:attr:`arrival<junction.futures.RPC.arrival>`. These aren't possible for
Dependents because they don't necessarily correspond to a single RPC, so
target_count doesn't make sense, it doesn't have "arrivals" in the sense
that an RPC does, and if its result is simply the return value of its
callback then there will be no partial results.


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
