/*******************************************************************************

    Pool of DLS node socket connections holding IRequest instances

    Copyright:
        Copyright (c) 2011-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.client.legacy.internal.connection.DlsNodeConnectionPool;



/*******************************************************************************

    Imports

*******************************************************************************/

import swarm.client.model.ClientSettings;

import swarm.client.connection.NodeConnectionPool;
import swarm.client.connection.RequestOverflow;

import dlsproto.client.legacy.internal.connection.model.IDlsNodeConnectionPoolInfo;

import dlsproto.client.legacy.DlsConst;
import Hash = swarm.util.Hash;

import dlsproto.client.legacy.internal.DlsClientExceptions;

import dlsproto.client.legacy.internal.connection.SharedResources;
import dlsproto.client.legacy.internal.connection.DlsRequestConnection;

import dlsproto.client.legacy.internal.request.params.RequestParams;

import dlsproto.client.legacy.internal.request.notifier.RequestNotification;

import dlsproto.client.legacy.internal.request.model.IRequest;

debug (SwarmClient) import ocean.io.Stdout;

import ocean.core.Enforce;

import ocean.io.compress.lzo.LzoChunkCompressor;

import ocean.meta.types.Qualifiers;

/*******************************************************************************

    DlsNodeConnectionPool

    Provides a pool of DLS node socket connections where each connection
    instance holds Reqest instances for the DLS requests.

*******************************************************************************/

public class DlsNodeConnectionPool : NodeConnectionPool, IDlsNodeConnectionPoolInfo
{
    /***************************************************************************

        Shared resources instance.

    ***************************************************************************/

    private SharedResources shared_resources;


    /***************************************************************************

        Lzo chunk de/compressor used by this connection pool. Passed as a
        reference to the constructor.

    ***************************************************************************/

    private LzoChunkCompressor lzo;


    /***************************************************************************

        Exceptions thrown on error.

    ***************************************************************************/

    private VersionException version_exception;


    /***************************************************************************

        Flag set when the API version of the DLS node this pool of connections
        is dealing with has been queried and matches the client's.

    ***************************************************************************/

    private bool version_ok;


    /***************************************************************************

        Flag set when the hash range supported by the DLS node this pool of
        connections is dealing with has been queried.

    ***************************************************************************/

    private bool range_queried;



    /***************************************************************************

        Constructor

        Params:
            settings = client settings instance
            epoll = selector dispatcher instances to register the socket and I/O
                events
            address = node address
            port = node service port
            lzo = lzo chunk de/compressor
            request_overflow = overflow handler for requests which don't fit in
                the request queue
            shared_resources = shared resources instance
            error_reporter = error reporter instance to notify on error or
                timeout

    ***************************************************************************/

    public this ( ClientSettings settings, EpollSelectDispatcher epoll,
        mstring address, ushort port, LzoChunkCompressor lzo,
        IRequestOverflow request_overflow, SharedResources shared_resources,
        INodeConnectionPoolErrorReporter error_reporter )
    {
        this.shared_resources = shared_resources;

        this.lzo = lzo;

        this.version_exception = new VersionException;

        super(settings, epoll, address, port, request_overflow, error_reporter);
    }


    /***************************************************************************

        Creates a new instance of the connection request handler class.

        Returns:
            new DlsRequestConnection instance

    ***************************************************************************/

    override protected DlsRequestConnection newConnection ( )
    {
        return new DlsRequestConnection(this.epoll, this.lzo, this,
            this.newRequestParams(), this.fiber_stack_size,
            this.shared_resources);
    }


    /***************************************************************************

        Creates a new instance of the connection request params class.

        Returns:
            new RequestParams instance

    ***************************************************************************/

    override protected IRequestParams newRequestParams ( )
    {
        return new RequestParams;
    }


    /***************************************************************************

        Returns:
            true if the API version of the DLS node which the connections in
            this pool are connected to has been queried and matches the client's

     **************************************************************************/

    override public bool api_version_ok ( )
    {
        return this.version_ok;
    }



    /***************************************************************************

        Checks the API version for the DLS node which the connections in this
        pool are connected to. The received API version must be the same as the
        version this client is compiled with.

        Params:
            api = API version reported by node

        Throws:
            VersionException if the node's API version does not match the
                client's

    ***************************************************************************/

    public void setAPIVerison ( cstring api )
    {
        debug ( SwarmClient ) Stderr.formatln("setAPIVersion: {}:{} -- {}",
            super.address, super.port, api);

        enforce(this.version_exception, api == DlsConst.ApiVersion);

        this.version_ok = true;
    }
}
