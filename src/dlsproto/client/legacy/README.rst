.. contents ::

Introduction and Core Concepts
================================================================================

Please read the core client documentation before proceeding. This README
only describes features specific to the DLS client.

The DLS client enables asynchronous communication with a Distributed Log Store
(DLS) database. The records consist of a 64-bit timestamp and an associated
value. Record values are arbitrary data. A DLS can be spread over multiple
'nodes', and a round-robin is used to distribute request across the nodes.

Basic DLS Client Usage
================================================================================

Empty Records
--------------------------------------------------------------------------------

It is not possible to store empty records in the DLS. The client checks this and
will cancel the request if the user attempts to Put an empty record.

Get Delegates
--------------------------------------------------------------------------------

Requests which read data from the DLS must provide a delegate which is to be
called when the data is received. A single request will almost always receive
multiple pieces of data from the DLS; this will result in the provided delegate
being called multiple times, once for each piece of data received.

Put Delegates
--------------------------------------------------------------------------------

Requests which write data to the DLS must provide a delegate which is to be
called when the client is ready to send the data for the request, and must
return the data to be sent.

Note that the data provided by the put delegate is only sliced by the DLS
client, and must remain available until the request finished notification is
received.

Iteration Requests
--------------------------------------------------------------------------------

Several DLS requests (GetRange and GetAll, for example) result in an iteration
a subset of records stored in a DLS channel. These requests are executed in
parallel on all nodes in the DLS, and thus will receive the requested data at a
much faster rate than if the data was iterated over in a strictly sequential
fashion. This does of course have the side-effect, however, that the iterated
data is *not* received in order. Thus the client application must do any sorting
of the recieved data which is required.

The Node Handshake
--------------------------------------------------------------------------------

Before any requests can be performed, the DLS client must make an initial query
to all nodes in the DLS. This 'node handshake' establishes the API version of
the node.

The ``nodeHandshake()`` method accepts a delegate which will be called upon
completion of the handshake. The delegate indicates whether the handshake
completed successfully for all nodes in the DLS or not. It also accepts a
request notifier, like all request methods, which will be called multiple times
while the handshake is underway.

The epoll event loop must be active / activated for the node handshake to start.

In the case of a partially successful handshake (some nodes responded while
others did not), it is still possible to use the DLS client but requests which
would be sent to the nodes which did not successfully handshake will be
rejected.

Basic Usage Example
--------------------------------------------------------------------------------

See dlsproto.client.DlsClient module header.

Advanced Client Features
================================================================================

Request Contexts in the DLS Client
--------------------------------------------------------------------------------

If the user does not specify a context for a request, the default context is the
timestamp of the record, if one is given (cast to a ``hash_t``).

Usage Example With RequestContext
--------------------------------------------------------------------------------

See the DHT client documentation for a full example of using the request context
in practice.

Record Filtering
--------------------------------------------------------------------------------

Certain requests (GetRange, for example) support the passing of an optional
filter string to the node, via the request object's ``filter()`` method. This
instructs the node to only return records whose values contain the specified
filter string, and can be used to greatly reduce the bandwidth required when
iterating over large quantities of data.

