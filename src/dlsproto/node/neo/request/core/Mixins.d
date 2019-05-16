/*******************************************************************************

    Request protocol mixins.

    Copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.node.neo.request.core.Mixins;

/*******************************************************************************

    Request core mixin.

*******************************************************************************/

public template RequestCore ( )
{
    import dlsproto.node.neo.request.core.IRequestResources;

    /***************************************************************************

        Shared resources getter instance.

    ***************************************************************************/

    protected IRequestResources resources;

    /***************************************************************************

        Constructor.

        Params:
            resources = DLS request resources getter

    ***************************************************************************/

    public this ( IRequestResources resources )
    {
        this.resources = resources;
    }
}

/*******************************************************************************

    IRequestHandler-based request core mixin.

*******************************************************************************/

public template IRequestHandlerRequestCore ( )
{
    import ocean.core.Verify;
    import swarm.neo.node.RequestOnConn;
    import dlsproto.node.neo.request.core.IRequestResources;

    /// Request-on-conn of this request handler.
    protected RequestOnConn connection;

    /// Event dispatcher for this connection
    protected RequestOnConn.EventDispatcher ed;

    /// Acquired resources of this request.
    protected IRequestResources resources;

    /***************************************************************************

        Passes the request-on-conn and request resource acquirer to the handler.

        Params:
            connection = request-on-conn in which the request handler is called
            resources = request resources acquirer

    ***************************************************************************/

    private void initialise ( RequestOnConn connection, Object resources_object )
    {
        this.connection = connection;
        this.ed = this.connection.event_dispatcher;
        this.resources = cast(IRequestResources)resources_object;
        verify(this.resources !is null);
    }
}
