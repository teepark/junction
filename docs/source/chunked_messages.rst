.. _chunked-messages:

================
Chunked Messages
================

All of Junction's message types (publish, RPC request, RPC response) can
be sent in chunks, rather than atomically in whole. This can be useful
when transferring large amounts of data, or when something that can be
incrementally processed becomes available a little bit at a time.

There are no methods that support this capability explicitly, rather it
is activated by using generators with the existing
:class:`Hub <junction.hub.Hub>` and
:class:`Client <junction.client.Client>` APIs.


From The Client End
-------------------

For publish and RPC request messages, to cause them to transfer chunked
you must provide a generator object as the first positional argument
(that is, the first element of the ``args`` tuple in
:meth:`rpc <junction.hub.Hub.rpc>`,
:meth:`send_rpc <junction.hub.Hub.send_rpc>`, or
:meth:`publish <junction.hub.Hub.publish>`.

If an RPC response is coming back from a particular server chunked, the
RPC's result for that server will be a generator object, which can block
(waiting for chunks to arrive) when iterated over.

.. note::
        The iteration over the chunk generator will occur in its own
        greenlet started specifically for this purpose, to ensure that
        the sending method doesn't block, even if the generator that
        produces the chunks does.

The APIs around blocking on the results of an RPC
(:meth:`rpc <junction.hub.Hub.rpc>`,
:func:`wait_any <junction.futures.wait_any>`,
:func:`wait_all <junction.futures.wait_all>`,
:meth:`RPC.wait <junction.futures.RPC.wait>`,
:meth:`RPC.after <junction.futures.RPC.after>`) will stop blocking as
soon as the message *indicating that the response is chunked* is
received. They will not wait for any actual chunks to arrive.


From The Server Side
--------------------

Any handler which receives a chunked publish or RPC request will receive
a generator object as the first argument, and iteration over that
generator may block waiting for chunks to arrive.

To send back a chunked RPC response, the return value of the handler
function should be a generator. The easiest way to achieve this is to
``yield`` from the handler itself.

.. note::
        Whether or not a handler was registered with ``schedule=False``
        given to :meth:`accept_rpc <junction.hub.Hub.accept_rpc>` or
        :meth:`accept_publish <junction.hub.Hub.accept_publish>`, when a
        chunked publish or RPC request starts coming in, the handler
        will be run in its own greenlet.


Failure Cases
-------------

If there is an exception in a generator which is producing chunks to be
sent, a corresponding exception object will be yielded out of the
generator at the destination as the final chunk.

If the connection goes down while chunks are being sent, the greenlet
iterating over the producer generator will be exited immediately (via a
``GreenletExit`` exception), and the generator yielding chunks at the
destination will provide a
:class:`LostConnection <junction.errors.LostConnection>` exception as
the final chunk.
