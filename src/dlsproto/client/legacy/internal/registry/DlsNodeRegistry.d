/*******************************************************************************

    DLS node connection registry

    Copyright:
        Copyright (c) 2010-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.client.legacy.internal.registry.DlsNodeRegistry;



/******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import ocean.core.Verify;

import swarm.client.ClientCommandParams;

import swarm.client.registry.NodeRegistry;
import swarm.client.registry.NodeSet;

import swarm.client.connection.RequestOverflow;
import dlsproto.client.legacy.internal.connection.SharedResources;

import dlsproto.client.legacy.internal.connection.model.IDlsNodeConnectionPoolInfo;

import dlsproto.client.legacy.internal.registry.model.IDlsNodeRegistryInfo;

import dlsproto.client.legacy.internal.connection.DlsRequestConnection,
               dlsproto.client.legacy.internal.connection.DlsNodeConnectionPool;

import dlsproto.client.legacy.internal.request.params.RequestParams;

import swarm.client.request.context.RequestContext;

import dlsproto.client.legacy.DlsConst;

import dlsproto.client.legacy.internal.DlsClientExceptions;

import ocean.io.select.EpollSelectDispatcher;

import ocean.core.Enforce;

import ocean.io.compress.lzo.LzoChunkCompressor;

debug ( SwarmClient ) import ocean.io.Stdout;



/******************************************************************************

    DlsNodeRegistry

    Registry of DLS node socket connections pools with one connection pool for
    each DLS node.

*******************************************************************************/

public class DlsNodeRegistry : NodeRegistry, IDlsNodeRegistryInfo
{
    /***************************************************************************

        Number of expected nodes in the registry. Used to initialise the
        registry's hash map.

    ***************************************************************************/

    private static immutable expected_nodes = 100;


    /***************************************************************************

        Index of the next node to assign a single-node request to.

    ***************************************************************************/

    private size_t current_node_index = 0;

    /***************************************************************************

        Indicator if the handshake has been started.

    ***************************************************************************/

    private bool handshake_initiated = false;

    /***************************************************************************

        Shared resources instance. Owned by this class and passed to all node
        connection pools.

    ***************************************************************************/

    protected SharedResources shared_resources;


    /***************************************************************************

        Lzo chunk de/compressor shared by all connections and request handlers.

    ***************************************************************************/

    protected LzoChunkCompressor lzo;


    /***************************************************************************

        Exceptions thrown on error.

    ***************************************************************************/

    private VersionException version_exception;

    private RangesNotQueriedException ranges_not_queried_exception;

    private NodeOverlapException node_overlap_exception;

    private RegistryLockedException registry_locked_exception;


    /***************************************************************************

        Constructor

        Params:
            epoll = selector dispatcher instance to register the socket and I/O
                events
            settings = client settings instance
            request_overflow = overflow handler for requests which don't fit in
                the request queue
            error_reporter = error reporter instance to notify on error or
                timeout

    ***************************************************************************/

    public this ( EpollSelectDispatcher epoll, ClientSettings settings,
        IRequestOverflow request_overflow,
        INodeConnectionPoolErrorReporter error_reporter )
    {
        super(epoll, settings, request_overflow,
            new NodeSet(this.expected_nodes), error_reporter);

        this.version_exception = new VersionException;
        this.ranges_not_queried_exception = new RangesNotQueriedException;
        this.node_overlap_exception = new NodeOverlapException;
        this.registry_locked_exception = new RegistryLockedException;

        this.shared_resources = new SharedResources;

        this.lzo = new LzoChunkCompressor;
    }


    /***************************************************************************

        Creates a new instance of the DLS node request pool class.

        Params:
            address = node address
            port = node service port

        Returns:
            new NodeConnectionPool instance

    ***************************************************************************/

    override protected NodeConnectionPool newConnectionPool ( mstring address, ushort port )
    {
        return new DlsNodeConnectionPool(this.settings, this.epoll,
            address, port, this.lzo, this.request_overflow,
            this.shared_resources, this.error_reporter);
    }

    /***************************************************************************

        Gets the connection pool which is responsible for the given request.

        Params:
            params = request parameters

        Returns:
            connection pool responsible for request (null if none found)

    ***************************************************************************/

    override protected NodeConnectionPool getResponsiblePool ( IRequestParams params )
    {
        if ( params.node.set() )
        {
            auto pool = super.inRegistry(params.node.Address, params.node.Port);
            return pool is null ? null : *pool;
        }

        auto dls_params = cast(RequestParams)params;

        auto target_pool = this.nodes.list[this.current_node_index];

        current_node_index = (this.current_node_index + 1) % this.nodes.list.length;

        return cast(DlsNodeConnectionPool)target_pool;
    }

    /***************************************************************************

        Determines whether the given request params describe a request which
        should be sent to all nodes simultaneously.

        Multi-node requests which have not been assigned with a particular node
        specified are sent to all nodes.

        Params:
            params = request parameters

        Returns:
            true if the request should be added to all nodes

    ***************************************************************************/

    override public bool allNodesRequest ( IRequestParams params )
    {
        with ( DlsConst.Command.E ) switch ( params.command )
        {
            // Commands over all nodes
            case GetAll:
            case GetAllFilter:
            case GetChannels:
            case GetSize:
            case GetChannelSize:
            case GetRange:
            case GetRangeFilter:
            case GetRangeRegex:
            case RemoveChannel:
            case GetNumConnections:
            case GetVersion:
            case Redistribute:
                return !params.node.set();

            // Commands over a single node
            case Put:
            case PutBatch:
                return false;

            default:
                assert(false, typeof(this).stringof ~ ".allNodesRequest: invalid request");
        }
    }


    /***************************************************************************

        Adds a request to the individual node specified. If the handshake was
        started and request being assigned is not GetVersion, then the node's
        API version is checked before assigning and an exception thrown if it
        is either unknown or does not match the client's.

        Params:
            params = request parameters
            node_conn_pool = node connection pool to assign request to

        Throws:
            if the request is not GetVersion and the node's API version is
            not ok -- handled by the caller (assignToNode(), in the super class)

    ***************************************************************************/

    override protected void assignToNode_ ( IRequestParams params,
        NodeConnectionPool node_conn_pool )
    {
        auto dls_conn_pool = (cast(DlsNodeConnectionPool)node_conn_pool);
        verify(dls_conn_pool !is null);

        if ( params.command != DlsConst.Command.E.GetVersion )
        {
            enforce(this.version_exception,
                    !this.handshake_initiated || dls_conn_pool.api_version_ok);
        }

        super.assignToNode_(params, node_conn_pool);
    }


    /***************************************************************************

        Checks the API version for a node. The received API version must be the
        same as the version this client is compiled with.

        Params:
            address = address of node to set hash range for
            port = port of node to set hash range for
            api = API version reported by node

        Throws:
            VersionException if the node's API version does not match the
                client's

    ***************************************************************************/

    public void setNodeAPIVersion ( mstring address, ushort port, cstring api )
    {
        auto conn_pool = super.inRegistry(address, port);
        verify(conn_pool !is null, "node not in registry");

        auto dls_conn_pool = (cast(DlsNodeConnectionPool*)conn_pool);
        dls_conn_pool.setAPIVerison(api);
    }


    /***************************************************************************

        Tells if the client is ready to send requests to all nodes in the
        registry (i.e. they have all responded successfully to the handshake).

        Returns:
            true if all node API versions are known. false otherwise.

    ***************************************************************************/

    public bool all_nodes_ok ( )
    {
        // Since handshake is optional, this will report all nodes
        // being ok if the handshake has not been performed,
        // or if the handshake is performed and all received versions
        // are compatible
        auto succeeded = !this.handshake_initiated || this.all_versions_ok;

        debug ( SwarmClient ) Stderr.formatln("DlsNodeRegistry.all_nodes_ok={} ",
            succeeded);

        return succeeded;
    }


    /**************************************************************************

        foreach iterator over connection pool info interfaces.

    **************************************************************************/

    public int opApply ( scope int delegate ( ref IDlsNodeConnectionPoolInfo ) dg )
    {
        int ret;

        foreach ( DlsNodeConnectionPool connpool; this )
        {
            auto info = cast(IDlsNodeConnectionPoolInfo)connpool;
            ret = dg(info);

            if ( ret ) break;
        }

        return ret;
    }

    /***************************************************************************

        Informs the registry that the handshake has been initiated.

    ***************************************************************************/

    public void handshakeInitiated ()
    {
        this.handshake_initiated = true;
    }

    /***************************************************************************

        Returns:
            true if all nodes support the correct API version or false if there
            are nodes in the registry whose API version is currently unknown or
            mismatched.

    ***************************************************************************/

    private bool all_versions_ok ( )
    {
        foreach ( DlsNodeConnectionPool connpool; this )
        {
            if ( !connpool.api_version_ok ) return false;
        }

        return true;
    }


    /***************************************************************************

        foreach iterator over the connection pools in the registry.

    ***************************************************************************/

    private int opApply ( scope int delegate ( ref DlsNodeConnectionPool ) dg )
    {
        int res;
        foreach ( pool; this.nodes.list )
        {
            auto dls_pool = cast(DlsNodeConnectionPool)pool;
            res = dg(dls_pool);

            if ( res ) break;
        }

        return res;
    }


    /***************************************************************************

        foreach iterator over the connection pools in the registry along with
        their indices in the list of connection pools.

    ***************************************************************************/

    private int opApply ( scope int delegate ( ref size_t, ref DlsNodeConnectionPool ) dg )
    {
        int res;
        size_t i;
        foreach ( nodeitem, pool; this.nodes.list )
        {
            auto dls_pool = cast(DlsNodeConnectionPool)pool;
            res = dg(i, dls_pool);
            i++;

            if ( res ) break;
        }

        return res;
    }

}
