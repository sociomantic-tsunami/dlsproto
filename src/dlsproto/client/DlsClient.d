/*******************************************************************************

    Asynchronous/event-driven DLS client using non-blocking socket I/O (epoll)

    Documentation:

    For detailed documentation see dlsproto.client.legacy.README.

    Basic usage example:

    The following steps should be followed to set up and use the DLS client:

        1. Create an EpollSelectDispatcher instance (see ocean.io.select).
        2. Create a DlsClient instance, pass the epoll select dispatcher and the
           maximum number of connections per node as constructor arguments.
        3. Add the DLS nodes connection data by calling addNode() for each DLS
           node to connect to. (Or simply call addNodes(), passing the path of
           a .nodes file describing the list of nodes to connect to.)
        4. Initiate the node handshake, and check that no error occurred.
        5. Add one or multiple requests by calling one of the client request
           methods and passing the resulting object to the client's assign()
           method.

    Example: Use at most five connections to each DLS node, connect to nodes
    running at 192.168.1.234:56789 and 192.168.9.87:65432 and perform a Get
    request.

    ---

        import ocean.io.select.EpollSelectDispatcher;
        import dlsproto.client.DlsClient;

        time_t start = 0xC001D00D;  // start of time range to query
        time_t end   = 0xDEADBEEF;  // end of time range to query


        // Error flag, set to true when a request error occurs.
        bool error;

        // Request notification callback. Sets the error flag on failure.
        void notify ( DlsClient.RequestNotification info )
        {
            if ( info.type == info.type.Finished && !info.succeeded )
            {
                error = true;
            }
        }

        // Handshake callback. Sets the error flag on handshake failure.
        void handshake ( DlsClient.RequestContext context, bool ok )
        {
            error = !ok;
        }

        // Callback delegate to receive value
        void receive_values ( DlsClient.RequestContext context, cstring timestamp,
            cstring value )
        {
            // do something with the received record
        }


        // Initialise epoll -- Step 1
        auto epoll = new EpollSelectDispatcher;

        // Initialise DLS client -- Step 2
        const NumConnections = 5;
        auto dls = new DlsClient(epoll, NumConnections);

        // Add nodes -- Step 3
        dls.addNode("192.168.1.234", 56789);
        dls.addNode("192.168.9.87",  65432);

        // Perform node handshake -- Step 4
        dls.nodeHandshake(&handshake, &notify);
        epoll.eventLoop();

        if ( error )
        {
            throw new Exception("Error during node handshake");
        }

        // Perform a GetRange request -- Step 5
        dls.assign(dls.getRange("my_channel", start, end, &receive_values, &notify));
        epoll.eventLoop();

    ---


    Useful build flags:
    ============================================================================

    -debug=SwarmClient: trace outputs noting when requests begin, end, etc

    -debug=ISelectClient: trace outputs noting epoll registrations and events
        firing

    -debug=Raw: trace outputs noting raw data sent & received via epoll

    Copyright:
        Copyright (c) 2011-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.client.DlsClient;



/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import ocean.core.Verify;

import swarm.util.ExtensibleClass;
import swarm.Const;
import swarm.util.Hash : HashRange;

import swarm.client.model.IClient;
import swarm.client.model.ClientSettings;

import swarm.client.ClientExceptions;
import swarm.client.ClientCommandParams;

import swarm.client.request.model.ISuspendableRequest;
import swarm.client.request.model.IStreamInfo;

import swarm.client.request.notifier.IRequestNotification;

import swarm.client.connection.RequestOverflow;

import swarm.client.helper.GroupRequest;

import swarm.client.plugins.RequestQueueDiskOverflow;
import swarm.client.plugins.RequestScheduler;
import swarm.client.plugins.ScopeRequests;

import dlsproto.client.legacy.DlsConst;

import dlsproto.client.legacy.internal.registry.model.IDlsNodeRegistryInfo;

import dlsproto.client.legacy.internal.registry.DlsNodeRegistry;

import dlsproto.client.legacy.internal.DlsClientExceptions;

import dlsproto.client.legacy.internal.request.notifier.RequestNotification;

import dlsproto.client.legacy.internal.request.params.RequestParams;

import RequestSetup = dlsproto.client.legacy.internal.RequestSetup;

import ocean.core.Array : copy, endsWith;

import ocean.io.select.EpollSelectDispatcher;

import ocean.core.Enforce;

debug ( SwarmClient ) import ocean.io.Stdout;

import ocean.transition;


/*******************************************************************************

    Extensible DLS Client.

    Supported plugin classes can be passed as template parameters, an instance
    of each of these classes must be passed to the constructor. For each plugin
    class members may be added, depending on the particular plugin class.

    Note that the call to setPlugins(), in the class' ctors, *must* occur before
    the super ctor is called. This is because plugins may rely on the ctor being
    able to access their properly initialised instance, usually via an
    overridden method. The RequestQueueDiskOverflow plugin works like this, for
    example.

    Currently supported plugin classes:
        see dlsproto.client.legacy.internal.plugins
        and swarm.client.plugins

*******************************************************************************/

public class ExtensibleDlsClient ( Plugins ... ) : DlsClient
{
    mixin ExtensibleClass!(Plugins);

    /***************************************************************************

        Constructor

        Params:
            epoll = EpollSelectDispatcher instance to use
            plugin_instances = instances of Plugins
            config = Instance of the configuration class
            fiber_stack_size = size of connection fibers' stack (in bytes)

    ***************************************************************************/

    public this ( EpollSelectDispatcher epoll, Plugins plugin_instances,
        IClient.Config config,
        size_t fiber_stack_size = IClient.default_fiber_stack_size )
    {
        this.setPlugins(plugin_instances);

        super(epoll, config, fiber_stack_size);
    }


    /***************************************************************************

        Constructor

        Params:
            epoll = EpollSelectDispatcher instance to use
            plugin_instances = instances of Plugins
            conn_limit = maximum number of connections to each DLS node
            queue_size = maximum size of the per-node request queue
            fiber_stack_size = size of connection fibers' stack (in bytes)

    ***************************************************************************/

    public this ( EpollSelectDispatcher epoll, Plugins plugin_instances,
        size_t conn_limit = IClient.Config.default_connection_limit,
        size_t queue_size = IClient.Config.default_queue_size,
        size_t fiber_stack_size = IClient.default_fiber_stack_size )
    {
        this.setPlugins(plugin_instances);

        super(epoll, conn_limit, queue_size, fiber_stack_size);
    }
}


/*******************************************************************************

    DlsClient with a scheduler, with simplified constructor.

    (This instantiation of the ExtensibleDlsClient template is provided for
    convenience, as it is a commonly used case.)

*******************************************************************************/

public class SchedulingDlsClient : ExtensibleDlsClient!(RequestScheduler)
{
    static class Config : IClient.Config
    {
        /***********************************************************************

            Limit on the number of events which can be managed by the scheduler
            at one time (0 = no limit)

        ***********************************************************************/

        uint scheduler_limit = 0;
    }

    /***************************************************************************

        Constructor

        Adds the nodes in the file specified in the config to the node registry

        Params:
            epoll = EpollSelectorDispatcher instance to use
            config = Config instance
            fiber_stack_size = size of connection fibers' stack (in bytes)

    ***************************************************************************/

    public this ( EpollSelectDispatcher epoll, SchedulingDlsClient.Config config,
        size_t fiber_stack_size = IClient.default_fiber_stack_size )
    {
        super(epoll, new RequestScheduler(epoll, config.scheduler_limit),
            config, fiber_stack_size);
    }


    /***************************************************************************

        Constructor

        Params:
            epoll = EpollSelectorDispatcher instance to use
            conn_limit = maximum number of connections to each DLS node
            queue_size = maximum size of the per-node request queue
            fiber_stack_size = size of connection fibers' stack (in bytes)
            max_events = limit on the number of events which can be managed
                by the scheduler at one time. (0 = no limit)

    ***************************************************************************/

    public this ( EpollSelectDispatcher epoll,
        size_t conn_limit = IClient.Config.default_connection_limit,
        size_t queue_size = IClient.Config.default_queue_size,
        size_t fiber_stack_size = IClient.default_fiber_stack_size,
        uint max_events = 0 )
    {
        super(epoll, new RequestScheduler(epoll, max_events), conn_limit,
            queue_size, fiber_stack_size);
    }
}


/*******************************************************************************

    DLS Client

*******************************************************************************/

public class DlsClient : IClient
{
    /***************************************************************************

        Local alias definitions

    ***************************************************************************/

    public alias .IRequestNotification RequestNotification;
    public alias .ISuspendableRequest ISuspendableRequest;
    public alias .IStreamInfo IStreamInfo;
    public alias .RequestParams RequestParams;


    /***************************************************************************

        Plugin alias definitions

    ***************************************************************************/

    public alias .RequestScheduler RequestScheduler;

    public alias .RequestQueueDiskOverflow RequestQueueDiskOverflow;

    public alias .ScopeRequestsPlugin ScopeRequestsPlugin;

    public alias .DlsConst.FilterMode FilterMode;


    /***************************************************************************

        Node handshake class, used by the DlsClient.nodeHandshake() method to
        synchronize the initial contacting of the DLS nodes and checking of the
        API version and fetching of the nodes' hash ranges.

    ***************************************************************************/

    private class NodeHandshake
    {
        /***********************************************************************

            Delegate to be called when the handshake has finished, indicating
            sucecss or failure.

        ***********************************************************************/

        private RequestParams.GetBoolDg output;


        /***********************************************************************

            Request notification delegate.

        ***********************************************************************/

        private alias RequestNotification.Callback NotifierDg;

        private NotifierDg user_notifier;


        /***********************************************************************

            Counters to track how many out of all registered nodes have returned
            for each request.

        ***********************************************************************/

        private uint version_done_count;


        /***********************************************************************

            opCall -- initialises a node handshake for the specified DLS client.

            Params:
                output = delegate called when handshake is complete
                user_notifier = request notification delegate

        ***********************************************************************/

        public void opCall ( RequestParams.GetBoolDg output,
            NotifierDg user_notifier )
        {
            this.reset(output, user_notifier);

            with ( this.outer )
            {
                assign(getVersion(&this.getVersionIO, &this.handshakeNotifier));
            }
        }


        /***********************************************************************

            Resets all members ready to start a new handshake.

            Params:
                output = delegate called when handshake is complete
                user_notifier = request notification delegate

        ***********************************************************************/

        private void reset ( RequestParams.GetBoolDg output,
            NotifierDg user_notifier )
        {
            this.output = output;
            this.user_notifier = user_notifier;

            this.version_done_count = 0;
        }


        /***********************************************************************

            Notification callback used for all internally assigned DLS requests.

            Params:
                info = request notification info

            TODO: could the bool delegate be replaced with a series of exceptions
            which are sent to the notifier to denote different handshake errors?

        ***********************************************************************/

        private void handshakeNotifier ( DlsClient.RequestNotification info )
        {
            if ( this.user_notifier !is null )
            {
                this.user_notifier(info);
            }

            if ( info.type == info.type.Finished )
            {
                with ( DlsConst.Command.E ) switch ( info.command )
                {
                    case GetVersion:            this.version_done_count++; break;

                    default:
                        assert(false);
                }

                auto dls_registry = cast(IDlsNodeRegistryInfo)this.outer.nodes;
                verify(dls_registry !is null);

                if ( version_done_count == dls_registry.length )
                {
                    this.output(RequestContext(0), dls_registry.all_nodes_ok);
                }
            }
        }


        /***********************************************************************

            GetVersion request callback.

            Params:
                context = request context (not used)
                api_version = api version received from node

        ***********************************************************************/

        private void getVersionIO ( RequestContext context, in cstring address,
            ushort port, in cstring api_version )
        {
            debug ( SwarmClient ) Stderr.formatln("Received version {}:{} = '{}'",
                address, port, api_version);

            (cast(DlsNodeRegistry)this.outer.registry).setNodeAPIVersion(
                address.dup, port, api_version);
        }
    }

    /***************************************************************************

        Node handshake instance.

        TODO: using a single struct instance means that only one node handshake
        can be active at a time. This is ok, but there's no way of enforcing it.
        This could probably be reworked if we implement the request-grouping
        feature for multi-node commands, or if the node handshake is moved to an
        internal-only process.

    ***************************************************************************/

    private NodeHandshake node_handshake;


    /***************************************************************************

        Exceptions thrown in error cases.

    ***************************************************************************/

    private BadChannelNameException bad_channel_exception;

    private NullFilterException null_filter_exception;


    /***************************************************************************

        Constructor -- automatically calls addNodes() with the node definition
        file specified in the Config instance.

        Params:
            epoll = EpollSelectorDispatcher instance to use
            config = Config instance (see swarm.client.model.IClient. The
                Config class is designed to be read from an application's
                config.ini file via ocean.util.config.ClassFiller)
            fiber_stack_size = size (in bytes) of stack of individual connection
                fibers

    ***************************************************************************/

    public this ( EpollSelectDispatcher epoll, IClient.Config config,
        size_t fiber_stack_size = IClient.default_fiber_stack_size )
    {
        with ( config )
        {
            this(epoll, connection_limit(), queue_size(), fiber_stack_size);

            this.addNodes(nodes_file);
        }
    }


    /***************************************************************************

        Constructor

        Params:
            epoll = EpollSelectorDispatcher instance to use
            conn_limit = maximum number of connections to each DLS node
            queue_size = maximum size of the per-node request queue
            fiber_stack_size = size (in bytes) of stack of individual connection
                fibers

    ***************************************************************************/

    public this ( EpollSelectDispatcher epoll,
        size_t conn_limit = IClient.Config.default_connection_limit,
        size_t queue_size = IClient.Config.default_queue_size,
        size_t fiber_stack_size = IClient.default_fiber_stack_size )
    {
        ClientSettings settings;
        settings.conn_limit = conn_limit;
        settings.queue_size = queue_size;
        settings.fiber_stack_size = fiber_stack_size;

        auto node_registry = this.newDlsNodeRegistry(epoll, settings,
            this.requestOverflow, this.errorReporter);
        super(epoll, node_registry);

        this.bad_channel_exception = new BadChannelNameException;
        this.null_filter_exception = new NullFilterException;

        this.node_handshake = new NodeHandshake;
    }

    /**************************************************************************

        Factory method for creating DLS node registry used in the client. Can
        be overriden by the subclass in order to use client with custom registry.

        Params:
            epoll = selector dispatcher instance to register the socket and I/O
                events
            settings = client settings instance
            request_overflow = overflow handler for requests which don't fit in
                the request queue
            error_reporter = error reporter instance to notify on error or
                timeout

        Returns:
            DlsNodeRegistry instance to be used with the client.

    **************************************************************************/

    protected DlsNodeRegistry newDlsNodeRegistry ( EpollSelectDispatcher epoll,
        ClientSettings settings, IRequestOverflow request_overflow,
        INodeConnectionPoolErrorReporter error_reporter )
    {
        return new DlsNodeRegistry(epoll, settings, request_overflow,
            error_reporter);
    }

    /***************************************************************************

        Initiates the connection with all registered DLS nodes. This involves
        the following steps:

            1. The API version number is requested from all registered nodes.
               These version numbers are cross-checked against each other and
               against the client's API version.

        The specified user notification delegate is called for each node for
        each request performed by the node handshake (i.e. GetVersion).

        The specified output delegate is called once when the handshakes with
        all nodes have completed, indicating whether the handshakes were
        successful or not.

        TODO: try restructuring so that the node handshake is done internally
        upon assigning the first normal request (so the node handshake needn't
        be explicitly called by the user). In this case, the notifier in the
        node handshake struct would actually assign the requested method when
        the handshake succeeds, and would call the notifier with an error code
        if it fails (need to make sure all the appropriate error codes exist...
        version mismatch doesn't atm).

        Params:
            output = output delegate which receives a bool telling whether the
                handshake succeeded or not
            user_notifier = notification delegate

    ***************************************************************************/

    public void nodeHandshake ( RequestParams.GetBoolDg output,
        RequestNotification.Callback user_notifier )
    {
        (cast(DlsNodeRegistry)this.nodes).handshakeInitiated();
        this.node_handshake(output, user_notifier);
    }


    /***************************************************************************

        Assigns a new request to the client. The request is validated, and the
        notification callback may be invoked immediately if any errors are
        detected. Otherwise the request is sent to the node registry, where it
        will be either executed immediately (if a free connection is available)
        or queued for later execution.

        Template params:
            T = request type (should be one of the structs defined in this
                module)

        Params:
            request = request to assign

    ***************************************************************************/

    public void assign ( T ) ( T request )
    {
        static if ( is(T : IGroupRequest) )
        {
            request.setClient(this);
        }

        this.scopeRequestParams(
            ( IRequestParams params )
            {
                request.setup(params);

                this.assignParams(params);
            });
    }


    /***************************************************************************

        Creates a Put request, which will send a single value with the specified
        key to the DLS, allowing multiple values to exist for the same key. The
        database record value is read from the specified input delegate, which
        should be of the form:

            cstring delegate ( RequestContext context )

        It is illegal to put empty values to the node.

        Params:
            channel = database channel
            key = database record key
            input = input delegate which provides record value to send
            notifier = notification callback

        Returns:
            instance allowing optional settings to be set and then to be passed
            to assign()

    ***************************************************************************/

    private struct Put
    {
        mixin RequestSetup.RequestBase;
        mixin RequestSetup.IODelegate;       // io(T) method
        mixin RequestSetup.Channel;          // channel(cstring) method
        mixin RequestSetup.Key;              // key ( K ) (K) method

        mixin RequestSetup.RequestParamsSetup; // private setup() method, used by assign()
    }

    public Put put ( Key ) ( cstring channel, Key key, RequestParams.PutValueDg input,
                             RequestNotification.Callback notifier )
    {
        return *Put(DlsConst.Command.E.Put, notifier).channel(channel)
            .key(key).io(input).contextFromKey();
    }


    /***************************************************************************

        Creates a GetRange request, which will receive all values within the
        specified key range from the DLS. The database record keys & values are
        sent to the specified output delegate, which should be of the form:

            void delegate ( RequestContext context, cstring key, cstring value )

        Note that if there are no records in the specified channel and range,
        the output delegate will not be called.

        This is a multi-node request which is executed in parallel over all
        nodes in the DLS.

        Params:
            channel = database channel
            start_key = minimum database record key
            end_key = maximum database record key
            output = output delegate to send record keys & values to
            notifier = notification callback

        Returns:
            instance allowing optional settings to be set and then to be passed
            to assign()

    ***************************************************************************/

    private struct GetRange
    {
        mixin RequestSetup.RequestBase;
        mixin RequestSetup.IODelegate;       // io(T) method
        mixin RequestSetup.Channel;          // channel(cstring) method
        mixin RequestSetup.Filter;           // filter(cstring) and pcre(cstring, bool) methods
        mixin RequestSetup.Range;            // range ( K ) (K, K) method
        mixin RequestSetup.Suspendable;      // suspendable(RequestParams.RegisterSuspendableDg) method
        mixin RequestSetup.StreamInfo;       // stream_info(RequestParams.RegisterStreamInfoDg) method
        mixin RequestSetup.Node;             // node(NodeItem) method

        mixin RequestSetup.RequestParamsSetup; // private setup() method, used by assign()
    }

    public GetRange getRange ( Key ) ( cstring channel, Key start_key, Key end_key,
            RequestParams.GetPairDg output, RequestNotification.Callback notifier )
    {
        return *GetRange(DlsConst.Command.E.GetRange, notifier).channel(channel)
            .range(start_key, end_key).io(output);
    }


    /***************************************************************************

        Creates a GetAll request, which will receive all values in the specified
        channel from the DLS. The database record keys & values are sent to the
        specified output delegate, which should be of the form:

            void delegate ( RequestContext context, cstring key, cstring value )

        Note that if there are no records in the specified channel, the output
        delegate will not be called.

        This is a multi-node request which is executed in parallel over all
        nodes in the DLS.

        Params:
            channel = database channel
            output = output delegate to send record keys & values to
            notifier = notification callback

        Returns:
            instance allowing optional settings to be set and then to be passed
            to assign()

    ***************************************************************************/

    private struct GetAll
    {
        mixin RequestSetup.RequestBase;
        mixin RequestSetup.IODelegate;       // io(T) method
        mixin RequestSetup.Channel;          // channel(cstring) method
        mixin RequestSetup.Filter;           // filter(cstring) method
        mixin RequestSetup.Suspendable;      // suspendable(RequestParams.RegisterSuspendableDg) method
        mixin RequestSetup.StreamInfo;       // stream_info(RequestParams.RegisterStreamInfoDg) method
        mixin RequestSetup.Node;             // node(NodeItem) method

        mixin RequestSetup.RequestParamsSetup; // private setup() method, used by assign()
    }

    public GetAll getAll ( cstring channel, RequestParams.GetPairDg output,
            RequestNotification.Callback notifier )
    {
        return *GetAll(DlsConst.Command.E.GetAll, notifier).channel(channel)
            .io(output);
    }


    /***************************************************************************

        Creates a GetChannels request, which will receive a list of all channels
        which exist in the DLS. The channel names are sent to the specified
        output delegate, which should be of the form:

            void delegate ( RequestContext context, cstring address, ushort port,
                    cstring channel )

        This is a multi-node request which is executed in parallel over all
        nodes in the DLS. This means that the name of each channel will most
        likely be received once from each node.

        Note that if there are no channels in the DLS, the output delegate will
        not be called.

        Params:
            output = output delegate to send channel names to
            notifier = notification callback

        Returns:
            instance allowing optional settings to be set and then to be passed
            to assign()

    ***************************************************************************/

    private struct GetChannels
    {
        mixin RequestSetup.RequestBase;
        mixin RequestSetup.IODelegate;       // io(T) method
        mixin RequestSetup.Node;             // node(NodeItem) method

        mixin RequestSetup.RequestParamsSetup; // private setup() method, used by assign()
    }

    public GetChannels getChannels ( RequestParams.GetNodeValueDg output,
            RequestNotification.Callback notifier )
    {
        return *GetChannels(DlsConst.Command.E.GetChannels, notifier).io(output);
    }


    /***************************************************************************

        Creates a GetSize request, which will receive the number of records and
        bytes which exist in each node in the DLS (a sum of the contents of all
        channels stored in the node). The database sizes are sent to the
        specified output delegate, which should be of the form:

            void delegate ( RequestContext context, cstring address, ushort port, ulong records, ulong bytes )

        This is a multi-node request which is executed in parallel over all
        nodes in the DLS. The output delegate is called once per node.

        Note that if there are no channels in the DLS, the output delegate will
        not be called.

        Params:
            output = output delegate to send size information to
            notifier = notification callback

        Returns:
            instance allowing optional settings to be set and then to be passed
            to assign()

    ***************************************************************************/

    private struct GetSize
    {
        mixin RequestSetup.RequestBase;
        mixin RequestSetup.IODelegate;       // io(T) method
        mixin RequestSetup.Node;             // node(NodeItem) method

        mixin RequestSetup.RequestParamsSetup; // private setup() method, used by assign()
    }

    public GetSize getSize ( RequestParams.GetSizeInfoDg output, RequestNotification.Callback notifier )
    {
        return *GetSize(DlsConst.Command.E.GetSize, notifier).io(output);
    }


    /***************************************************************************

        Creates a GetChannelSize request, which will receive the number of
        records and bytes which exist in the specified channel in each node of
        the DLS. The channel sizes are sent to the specified output delegate,
        which should be of the form:

            void delegate ( RequestContext context, cstring address, ushort port,
                    cstring channel, ulong records, ulong bytes )

        This is a multi-node request which is executed in parallel over all
        nodes in the DLS. The output delegate is called once per node.

        Note that if there are no channels in the DLS, the output delegate will
        not be called.

        Params:
            channel = database channel
            output = output delegate to send size information to
            notifier = notification callback

        Returns:
            instance allowing optional settings to be set and then to be passed
            to assign()

    ***************************************************************************/

    private struct GetChannelSize
    {
        mixin RequestSetup.RequestBase;
        mixin RequestSetup.Channel;          // channel(cstring) method
        mixin RequestSetup.IODelegate;       // io(T) method
        mixin RequestSetup.Node;             // node(NodeItem) method

        mixin RequestSetup.RequestParamsSetup; // private setup() method, used by assign()
    }

    public GetChannelSize getChannelSize ( cstring channel, RequestParams.GetChannelSizeInfoDg output, RequestNotification.Callback notifier )
    {
        return *GetChannelSize(DlsConst.Command.E.GetChannelSize, notifier)
            .channel(channel).io(output);
    }


    /***************************************************************************

        Creates a RemoveChannel request, which will delete all records from the
        specified channel in all nodes of the DLS.

        This is a multi-node request which is executed in parallel over all
        nodes in the DLS.

        Params:
            channel = database channel
            notifier = notification callback

        Returns:
            instance allowing optional settings to be set and then to be passed
            to assign()

    ***************************************************************************/

    private struct RemoveChannel
    {
        mixin RequestSetup.RequestBase;
        mixin RequestSetup.Channel;          // channel(cstring) method
        mixin RequestSetup.Node;             // node(NodeItem) method

        mixin RequestSetup.RequestParamsSetup; // private setup() method, used by assign()
    }

    public RemoveChannel removeChannel ( cstring channel, RequestNotification.Callback notifier )
    {
        return *RemoveChannel(DlsConst.Command.E.RemoveChannel, notifier)
            .channel(channel);
    }


    /***************************************************************************

        Creates a GetNumConnections request, which will receive the count of
        open connections being handled by each node of the DLS. The number of
        connections is sent to the specified output delegate, which should be of
        the form:

            void delegate ( RequestContext context, cstring address, ushort port, size_t connections )

        This is a multi-node request which is executed in parallel over all
        nodes in the DLS. The output delegate is called once per node.

        Note that if there are no channels in the DLS, the output delegate will
        not be called.

        Params:
            output = output delegate to send connection counts to
            notifier = notification callback

        Returns:
            instance allowing optional settings to be set and then to be passed
            to assign()

    ***************************************************************************/

    private struct GetNumConnections
    {
        mixin RequestSetup.RequestBase;
        mixin RequestSetup.IODelegate;       // io(T) method
        mixin RequestSetup.Node;             // node(NodeItem) method

        mixin RequestSetup.RequestParamsSetup; // private setup() method, used by assign()
    }

    public GetNumConnections getNumConnections ( RequestParams.GetNumConnectionsDg output,
            RequestNotification.Callback notifier )
    {
        return *GetNumConnections(DlsConst.Command.E.GetNumConnections, notifier)
            .io(output);
    }


    /***************************************************************************

        Creates a GetVersion request, which will receive the api version used by
        each node of the DLS. The api version is sent to the specified output
        delegate, which should be of the form:

            void delegate ( RequestContext context, cstring address, ushort port,
                cstring api_version )

        This is a multi-node request which is executed in parallel over all
        nodes in the DLS. The output delegate is called once per node.

        Note that if there are no channels in the DLS, the output delegate will
        not be called.

        This request is usually only used internally by the node handshake.

        Params:
            output = output delegate to send api versions to
            notifier = notification callback

        Returns:
            instance allowing optional settings to be set and then to be passed
            to assign()

    ***************************************************************************/

    private struct GetVersion
    {
        mixin RequestSetup.RequestBase;
        mixin RequestSetup.IODelegate;       // io(T) method
        mixin RequestSetup.Node;             // node(NodeItem) method

        mixin RequestSetup.RequestParamsSetup; // private setup() method, used by assign()
    }

    public GetVersion getVersion ( RequestParams.GetNodeValueDg output,
            RequestNotification.Callback notifier )
    {
        return *GetVersion(DlsConst.Command.E.GetVersion, notifier).io(output);
    }


    /***************************************************************************

        Creates a new request params instance (derived from IRequestParams), and
        passes it to the provided delegate.

        This method is used by the request scheduler plugin, which needs to be
        able to construct and use a request params instance without knowing
        which derived type is used by the client.

        Params:
            dg = delegate to receive and use created scope IRequestParams
                instance

    ***************************************************************************/

    override protected void scopeRequestParams (
        void delegate ( IRequestParams params ) dg )
    {
        scope params = new RequestParams;
        dg(params);
    }


    /***************************************************************************

        Checks whether the given channel name is valid. Channel names can only
        contain alphanumeric characters, underscores or dashes.

        If the channel name is not valid then the user specified error callback
        is invoked with the BadChannelName status code.

        Params:
            params = request params to check

        Throws:
            * if the channel name is invalid
            * if a filtering request is being assigned but the filter string is
              empty

            (exceptions will be caught in super.assignParams)

    ***************************************************************************/

    override protected void validateRequestParams_ ( IRequestParams params )
    {
        auto dls_params = cast(RequestParams)params;

        // Validate channel name, for commands which use it
        with ( DlsConst.Command.E ) switch ( params.command )
        {
            case Put:
            case GetRange:
            case GetAll:
            case GetChannelSize:
            case RemoveChannel:
            case GetAllFilter:
            case GetRangeFilter:
                enforce(this.bad_channel_exception,
                    .validateChannelName(dls_params.channel));
                break;
            default:
        }

        // Validate filter string, for commands which use it
        with ( DlsConst.Command.E ) switch ( params.command )
        {
            case GetAllFilter:
            case GetRangeFilter:
            case GetRangeRegex:
                enforce(this.null_filter_exception,
                    dls_params.filter_string.length);
                break;
            default:
        }
    }
}

version ( UnitTest )
{
    import ocean.io.select.EpollSelectDispatcher;
    import swarm.client.request.params.IRequestParams;
}

/*******************************************************************************

    Test instantiating clients with various plugins.

*******************************************************************************/

unittest
{
    auto epoll = new EpollSelectDispatcher;

    {
        auto dls = new ExtensibleDlsClient!(DlsClient.RequestScheduler)
            (epoll, new RequestScheduler(epoll));
    }

    {
        class DummyStore : RequestQueueDiskOverflow.IRequestStore
        {
            ubyte[] store ( IRequestParams params ) { return null; }
            void restore ( void[] stored ) { }
        }

        auto dls = new ExtensibleDlsClient!(DlsClient.RequestQueueDiskOverflow)
            (epoll, new RequestQueueDiskOverflow(new DummyStore, "dummy"));
    }

    {
        auto dls = new ExtensibleDlsClient!(DlsClient.ScopeRequestsPlugin)
            (epoll, new ScopeRequestsPlugin);
    }
}

