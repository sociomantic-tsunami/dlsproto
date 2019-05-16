/*******************************************************************************

    Provides fake DLS node implementation, used to emulate environment
    for tested applications that work with DLS.

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedls.DlsNode;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import ocean.util.log.Logger;

import fakedls.ConnectionHandler;

import swarm.node.model.NeoNode;

/*******************************************************************************

    Reference to common fakedls logger instance

*******************************************************************************/

private Logger log;

static this ( )
{
    log = Log.lookup("fakedls");
}

/*******************************************************************************

    Simple DLS node. See fakedls.ConnectionHandler for more
    implementation details

*******************************************************************************/

public class DlsNode : NodeBase!(DlsConnectionHandler)
{
    import core.stdc.stdlib : abort;

    import ocean.io.select.client.model.ISelectClient : IAdvancedSelectClient;
    import ocean.net.server.connection.IConnectionHandlerInfo;
    import ocean.io.select.protocol.generic.ErrnoIOException;

    import dlsproto.client.legacy.DlsConst;
    import swarm.neo.authentication.HmacDef: Key;
    import Neo = swarm.neo.node.ConnectionHandler;

    import fakedls.neo.RequestHandlers;
    import fakedls.neo.SharedResources;
    import swarm.node.connection.ConnectionHandler : ConnectionSetupParams;

    /***************************************************************************

        Flag indicating that unhandled exceptions from the node must be printed
        in test suite trace

    ***************************************************************************/

    public bool log_errors = true;

    /***************************************************************************

        Constructor

        Params:
            node_item = node address & port
            epoll = epoll select dispatcher to be used internally

    ***************************************************************************/

    public this ( DlsConst.NodeItem node_item, EpollSelectDispatcher epoll )
    {
        static immutable backlog = 20;

        auto params = new ConnectionSetupParams;
        params.epoll = epoll;
        params.node_info = this;

        Options neo_options;
        neo_options.requests = requests;
        neo_options.epoll = epoll;
        neo_options.credentials_map["test"] = Key.init;

        ushort neo_port = node_item.Port;

        if (neo_port != 0)
        {
            neo_port++;
        }

        super (node_item, neo_port, params, neo_options, backlog);
        this.error_callback = &this.onError;
    }

    /***************************************************************************

        Simple `shutdown` implementation to stop logging unhandled exceptions
        when it is initiated.

    ***************************************************************************/

    override public void shutdown ( )
    {
        this.log_errors = false;
    }

    /***************************************************************************

        Make any error fatal

    ***************************************************************************/

    private void onError ( Exception exception, IAdvancedSelectClient.Event,
        IConnectionHandlerInfo )
    {
        if (!this.log_errors)
            return;

        .log.warn("Ignoring exception: {} ({}:{})",
            exception.message(), exception.file, exception.line);

        // socket errors can be legitimate, for example if client has terminated
        // the connection early
        if (cast(IOWarning) exception)
            return;

        // can be removed in next major version
        version(none)
        {
            // anything else is unexpected, die at once
            abort();
        }

    }

    /***************************************************************************

        Returns:
            identifier string for this node

    ***************************************************************************/

    override protected cstring id ( )
    {
        return "Fake DLS Node";
    }

    /***************************************************************************

        Scope allocates a request resource acquirer instance and passes it to
        the provided delegate for use in a request.

        Params:
            handle_request_dg = delegate that receives a resources acquirer and
                initiates handling of a request

    ***************************************************************************/

    override protected void getResourceAcquirer (
        scope void delegate ( Object request_resources ) handle_request_dg )
    {
        // In the fake node, we don't actually store a shared resources
        // instance; a new one is simply passed to each request.
        handle_request_dg(new SharedResources);
    }
}
