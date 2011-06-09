===============
Getting Started
===============


A Simple Service
----------------

As a simple exercise to demonstrate just the boilerplate that you would
need for any junction service, we'll implement the "hello, world" of
service architectures, an echo server.

.. sourcecode:: python

    import greenhouse
    import junction

    def echo(x):
        return "echoed: %s" % (x,)

    node = junction.Node(("127.0.0.1", 9000), [])
    node.accept_rpc("ECHO", "echo", 0, 0, echo)
    node.start()

    try:
        greenhouse.Event().wait()
    except KeyboardInterrupt:
        pass

Now let's break this down.

``echo`` is defined to just return its argument stringified and with
"echoed: " prefixed (fairly standard echo server behavior).

We'll need a :class:`Node <junction.node.Node>` object and since every
node in junction can connect to any other, it needs a ``(host, port)``
pair on which to run the server it use to accept peer connections from
other nodes. The second argument is the list of nodes (items in this
list would be ``(host, port)`` pairs) to which it should connect. In
this case there are no peers it needs to know about, so that list is
empty.

Next we instruct the node that it will accept RPCs on the "ECHO" service
for the "echo" method, that the mask and value are both 0 (more on those
in a minute), and that the function used to handle those RPCs is the
``echo`` function we defined earlier.

Then we start the node. This will start up the server accepting
connections on ``("127.0.0.1", 9000)`` and initiate all of the
connections we asked it to (none, this time).

The wait at the end is just to get the main greenlet to block - nothing
else has a reference to this ``Event``, so nothing will be waking it
from its wait. By catching ``KeyboardInterrupt``, we allow it to bail
out semi-cleanly with Ctrl-C.


Subscription Specification and Matching
---------------------------------------

Here's the part where we talk about the mask and value. Every
RPC and publish subscription in junction is made with a service name,
method name, and mask and value integers in order to specify precisely
which messages it should be receiving. The service and method are easy,
they just have to match what is in the message exactly. The mask and
value however are different. The corresponding component in an RPC
request is an integer "routing id" which, when bitwise-ANDed against a
subscription's mask, must be equal to the subscription's value to be a
match. So in this case the mask of 0 will always produce 0 when it is
ANDed with any routing id, meaning the ``(mask, value)`` pair of ``(0,
0)`` will always match (in this case we don't care to shard the ECHO
service).

One major benefit to using a mask and value is that two integers are
easily transferable between nodes. What junction does is that when any
connection is made between peers, they trade the information about what
their subscriptions are. So when using a node as a client, it can
*locally* figure out who should be receiving the message and only send
it there.

A more obvious and straightforward approach might have been that a
subscription would take a callback function that accepts the routing id
and returns True if the subscription matches (this would also be more
flexible), but since the callback wouldn't necessarily be serializable,
every message would have to be broadcast to every peer for filtering on
the remote end. But this approach with a mask and value allows
nodes to raise :class:`Unroutable <junction.errors.Unroutable>` from
rpc-sending client methods, or to return from :meth:`rpc_receiver_count
<junction.node.Node.rpc_receiver_count>` without ever blocking.


A Node as a Client
------------------

First the code.

.. sourcecode:: python

    import junction

    node = junction.Node(("127.0.0.1", 9001), [("127.0.0.1", 9000)])
    node.start()

    node.wait_on_connections()

    print node.rpc("ECHO", "echo", 0, ("first request",), {})[0]

    rpcs = []
    for msg in ("second", "third", "fourth"):
        rpcs.append(node.send_rpc("ECHO", "echo", 0, (msg,), {}))

    for rpc in rpcs:
        print rpc.wait()[0]

We get started in a similar way, creating a node. Even though this node
won't be accepting any connections from peers, it still has to start up
a server. All :class:`Nodes <junction.node.Node>` are created equal, and
they all accept connections from peers. This time we do provide a peer
for it to make a connection to; we give it the ``(host, port)`` of the
service we created before.

:meth:`wait_on_connections <junction.node.Node.wait_on_connections>`
will block until it has finished connecting to the list of peers we gave
it. This is necessary, otherwise it would raise :class:`Unroutable
<junction.errors.Unroutable>` from the first :meth:`rpc
<junction.node.Node.rpc>` call. Not having connected to its peer yet, it
wouldn't have collected its subscription information and so it wouldn't
know where to send the RPC (the Unroutable error effectively says "I
haven't met anyone that accepts RPCs to ECHO/echo/0").

Once connected, we can call :meth:`rpc <junction.node.Node.rpc>` with
the service, method, routing id, positional arguments and keyword
arguments. This method will block until all responses come back, and
then return them. Because it is possible that more than one peer might
have had a subscription matching the RPC, the method always returns a
list. In this case we know it is only connected to one peer and that the
peer accepts this message, so we safely just index the first result.

You already have everything you need to know to do synchronous RPCs with
junction. Junction nodes are coroutine-safe, so using greenhouse_ you
can always create multiple coroutines to run multiple RPCs in parallel.

.. _greenhouse: https://teepark.github.com/greenhouse

But there is also an async client API, and that is what is demonstrated
next. The :meth:`send_rpc <junction.node.Node.send_rpc>` method does
just what its name says and *only sends*, so it returns immediately.
Exactly what it returns is an :class:`RPC <junction.futures.RPC>`
instance, which represents an asynchronous in-flight RPC. The code in
the example sends 3 RPCs at once, collecting the RPC objects in a list,
then calls :meth:`wait <junction.futures.RPC.wait>` on them each to
block and get the RPC results. For more advanced usage of RPC objects
and the asynchronous API, hop over to :ref:`Programming With Futures
<programming-with-futures>`.

With the service code running in one terminal, running the client in
another (on the same machine) should print::

    echoed: first request
    echoed: second
    echoed: third
    echoed: fourth


A Client-only Client
--------------------

In the previous client code we still had to create a full :class:`Node
<junction.node.Node>` capable of accepting peer connections, and which
would have to be explicitly connected to any other Node to which it
would make an RPC request. For a case like this client, where we know it
will never accept RPCs or publishes, we can use a :class:`Client
<junction.client.Client>` which, as its name suggests, is like a
client-only node.

.. sourcecode:: python

    import junction

    client = junction.Client(("127.0.0.1", 9000))
    client.start()

    client.wait_on_connections()

    print client.rpc("ECHO", "echo", 0, ("first request",), {})[0]

    rpcs = []
    for msg in ("second", "third", "fourth"):
        rpcs.append(client.send_rpc("ECHO", "echo", 0, (msg,), {}))

    for rpc in rpcs:
        print rpc.wait()[0]

The first thing that should strike you about this code is how similar it
is to the Node-based client. :class:`Client <junction.client.Client>`
has *exactly* the same interface as :class:`Node <junction.node.Node>`
for the client side of RPCs and publishes, so that it is easy to
substitute one for the other, or write utility methods or higher level
APIs that will work with either.

But there is a difference in how we create them. A Client doesn't create
a peer-accepting server, and it doesn't connect to every Node in the
system, rather it just connects to a single node. It can still make RPC
requests that would resolve to any node in the system, but they will
always be proxied by the one node to which the Client is connected.

These are important differences. Making only a single connection means
that Clients start up much quicker, but having all its communications
into the Node network proxied adds a bit of latency, and some overhead
to the node acting as the proxy. Generally :class:`Nodes
<junction.node.Node>` with their slower startup time, static list of
all the Nodes in the system and ability to act as receivers of RPCs and
publishes are more suited for long-running servers, while
:class:`Clients <junction.client.Client>` are more well-suited to
scripts, interactive interpreter use, and environments that don't have a
long-running process (for instance a webserver that is stuck on mod_wsgi
or something else that doesn't allow long-lived module global state).

In this very simple case the extra latency we would expect to see from
Client usage doesn't come into play because the node to which we are
directly connecting is also the only one to which we will make RPC
requests.

We'll just make one final change to the client code for the purpose of
explaining a useful API. Replace the last two lines with the following:

.. sourcecode:: python

    while rpcs:
        rpc = client.wait_any(rpcs)
        print rpc.results[0]
        rpcs.remove(rpc)

The :meth:`Node.wait_any <junction.node.Node.wait_any>` and
:meth:`Client.wait_any <junction.client.Client.wait_any>` methods accept
a list of RPCs and return one of them that is complete. If none of them
are complete already, then it blocks until the first one completes.

This way of collecting parallel RPCs will handle them in the order in
which their resopnses come back, rather than our pre-defined order. If
there were a little more variance in the response times than an echo
server, and especially if we were doing CPU-intensive work on the
response values, it would be handy to be able to deal with the fastest
response first.
