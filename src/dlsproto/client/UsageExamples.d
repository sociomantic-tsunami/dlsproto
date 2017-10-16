/*******************************************************************************

    Usage examples for the Neo DLS Client.

    Copyright:
        Copyright (c) 2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.client.UsageExamples;

version (UnitTest)
{
    import ocean.transition;

    import dlsproto.client.DlsClient;

    import ocean.io.model.SuspendableThrottlerCount;
    import ocean.io.select.EpollSelectDispatcher;
    import ocean.task.Scheduler;
    import ocean.task.Task;
    import ocean.util.app.DaemonApp;
    import ocean.util.log.Logger;

    import swarm.neo.authentication.HmacDef : Key;
}


version (UnitTest)
{
    // DaemonApp class showing typical neo DLS client initialisation. The
    // class has a single abstract method -- example() -- which is implemented
    // by various usage examples in this module, each demonstrating a different
    // client feature.
    abstract class ExampleApp: DaemonApp
    {
        // DLS client (see dlsproto.client.DlsClient
        private DlsClient dls_client;

        // Buffer used for message formatting in notifiers
        private mstring msg_buf;

        // Logger used for logging notifications
        protected Logger log;

        // Constructor. Initialises the scheduler
        public this ( )
        {
            super("example", "DLS client neo usage example", VersionInfo.init);

            // Set up the logger for this example
            this.log = Log.lookup("example");

            // Initialize the scheduler
            SchedulerConfiguration scheduler_config;
            initScheduler(scheduler_config);
        }

        // Application run method. Initializes the DLS client and starts the
        // main application task.
        override protected int run ( Arguments args, ConfigParser config )
        {
            // Create a DLS client instance, passing the additional
            // arguments required by neo: the authorisation name and
            // password and the connection notifier (see below)
            auto auth_name = "neotest";
            ubyte[] auth_key = Key.init.content;
            this.dls_client = new DlsClient(theScheduler.epoll,
                    auth_name, auth_key, &this.connNotifier);

            this.dls_client.neo.addNodes("dls.node");

            theScheduler.schedule(new AppTask);
            theScheduler.eventLoop();
            return 0;
        }

        // Application main task
        private class AppTask : Task
        {
            // Thask entry point. Runs the example.
            protected override void run ( )
            {
                this.outer.example();
            }
        }

        // Abstract method containing the logic for each example
        abstract protected void example ( );

        // Notifier which is called when a connection establishment attempt
        // succeeds or fails. (Also called after a re-connection attempt.)
        private void connNotifier ( DlsClient.Neo.ConnNotification info )
        {
            with (info.Active) switch (info.active)
            {
                case connected:
                    // The connection succeeded.
                    break;
                case error_while_connecting:
                    // A connection error occurred. The client will
                    // automatically try to reconnect.
                    break;
                default:
                    assert(false);
            }
        }

    }
}

/*******************************************************************************

    Dummy struct to enable ddoc rendering of usage examples.

*******************************************************************************/

struct UsageExamples
{
}

/// Extensive example of neo Put request usage
unittest
{
    // An example of handling a Put request
    class PutExample: ExampleApp
    {
        override protected void example ()
        {
            // Assign a neo Put request. Note that the channel and value
            // are copied inside the client -- the user does not need to
            // maintain them after calling this method.
            this.dls_client.neo.put("channel", 0x12345678, "value_to_put",
                    &this.putNotifier);
        }

        // Notifier which is called when something of interest happens to
        // the Put request. See dlsproto.client.request.Put for
        // details of the parameters of the notifier. (Each request has a
        // module like this, defining its public API.)
        private void putNotifier ( DlsClient.Neo.Put.Notification info,
            DlsClient.Neo.Put.Args args )
        {
            // `info` is a smart union, where each member of the union
            // represents one possible notification. `info.active` denotes
            // the type of the current notification. Some notifications
            // have fields containing more information:
            with ( info.Active ) switch ( info.active )
            {
                case success:
                    this.log.trace("The request succeeded!");
                    break;

                case failure:
                    this.log.trace("The request failed on all nodes.");
                    break;

                case node_disconnected:
                    this.log.trace("The request failed due to connection "
                        "error {} on {}:{}",
                        getMsg(info.node_disconnected.e),
                        info.node_disconnected.node_addr.address_bytes,
                        info.node_disconnected.node_addr.port);
                    // If there are more nodes left to try, the request will
                    // be retried automatically.
                    break;

                case node_error:
                    this.log.error("The request failed due to a node "
                        "error on {}:{}",
                        info.node_error.node_addr.address_bytes,
                        info.node_error.node_addr.port);
                    // If there are more nodes left to try, the request will
                    // be retried automatically.
                    break;

                case unsupported:
                    switch ( info.unsupported.type )
                    {
                        case info.unsupported.type.RequestNotSupported:
                            this.log.error("The request is not supported by "
                                "node {}:{}",
                                info.unsupported.node_addr.address_bytes,
                                info.unsupported.node_addr.port);
                            break;
                        case info.unsupported.type.RequestVersionNotSupported:
                            this.log.error("The request version is not "
                                "supported by node {}:{}",
                                info.unsupported.node_addr.address_bytes,
                                info.unsupported.node_addr.port);
                            break;
                        default: assert(false);
                    }
                    break;

                default: assert(false);
            }
        }
    }
}

/// Extensive example of neo GetRange request usage, including the usage
/// of controller and request context
unittest
{
    class GetRangeExample : ExampleApp
    {
        // id of the running GetRange request (required for the controller to
        // be able to control it).
        private DlsClient.Neo.RequestId rq_id;

        protected override void example ( )
        {
            int my_context = 1;

            // Assign a neo GetRange request. Note that the all arguments
            // are copied inside the client -- the user does not need to
            // maintain them after calling this method.
            this.rq_id  = this.dls_client.neo.getRange("channel", 0x1234, 0x9999,
                    &this.getRangeNotifier,
                    DlsClient.Neo.Filter(DlsClient.Neo.Filter.FilterMode.PCRE, "action/login"),
                    DlsClient.Neo.RequestContext(my_context));
        }

        // Method which initiates stopping the request. Sends a message to DLS
        // to cleanly stop handling this request
        public void stop ( )
        {
            // The control() method of the client allows you to get access
            // to an interface providing methods which control the state of
            // a request, while it's in progress. The GetRange request
            // controller interface is in dlsproto.client.request.GetRange.
            // Not all requests can be controlled in this way.
            this.dls_client.neo.control(this.rq_id,
                ( DlsClient.Neo.GetRange.IController get_range )
                {
                    // We tell the request to stop. This will cause a
                    // message to be sent to all DLS nodes, telling them to
                    // end the GetRange. More records may be received while
                    // this is happening, but the notifier is called, as
                    // soon as all nodes have stopped. (There are also
                    // controller methods to suspend and resume the request
                    // on the node-side.)
                    get_range.stop();
                }
            );
        }

        // Notifier which is called when something of interest happens to
        // the GetRange request. See dlsproto.client.request.GetRange for
        // details of the parameters of the notifier. (Each request has a
        // module like this, defining its public API.)
        private void getRangeNotifier ( DlsClient.Neo.GetRange.Notification info,
            DlsClient.Neo.GetRange.Args args )
        {
            this.log.trace("Request context was: {}",
                    args.context.integer());

            with ( info.Active ) switch ( info.active )
            {
                case started:
                    break;

                case received:
                    this.log.trace("Received key {} with value {}",
                            info.received.key, info.received.value);
                    break;

                case finished:
                    this.log.trace("Request has finished on all nodes");
                    break;

                case node_disconnected:
                    this.log.trace("GetRange failed due to connection "
                        "error {} on {}:{}",
                        getMsg(info.node_disconnected.e),
                        info.node_disconnected.node_addr.address_bytes,
                        info.node_disconnected.node_addr.port);
                    break;

                case node_error:
                    this.log.error("GetRange failed due to a node "
                        "error on {}:{}",
                        info.node_error.node_addr.address_bytes,
                        info.node_error.node_addr.port);
                    break;

                case unsupported:
                    this.log.error("GetRange failed due to an unsupported error "
                        "on {}:{}",
                        info.unsupported.node_addr.address_bytes,
                        info.unsupported.node_addr.port);
                    break;

                case stopped:
                    this.log.trace("The request stopped on all nodes.");
                    break;

                default: assert(false);
            }
        }
    }
}

/// Example of Task-blocking neo Put request usage with a notifier
unittest
{
    // An example of handling a Put request
    class BlockingPutExample: ExampleApp
    {
        override protected void example ()
        {
            // Assign a neo Put request. Note that the channel and value
            // are copied inside the client -- the user does not need to
            // maintain them after calling this method.
            this.dls_client.blocking.put("channel", 0x12345678, "value_to_put",
                    &this.putNotifier);
        }

        // Notifier which is called when something of interest happens to
        // the Put request. See dlsproto.client.request.Put for
        // details of the parameters of the notifier. (Each request has a
        // module like this, defining its public API.)
        private void putNotifier ( DlsClient.Neo.Put.Notification info,
            DlsClient.Neo.Put.Args args )
        {
            // `info` is a smart union, where each member of the union
            // represents one possible notification. `info.active` denotes
            // the type of the current notification. Some notifications
            // have fields containing more information:
            with ( info.Active ) switch ( info.active )
            {
                case success:
                    this.log.trace("The request succeeded!");
                    break;

                case failure:
                    this.log.trace("The request failed on all nodes.");
                    break;

                case node_disconnected:
                    this.log.trace("The request failed due to connection "
                        "error {} on {}:{}",
                        getMsg(info.node_disconnected.e),
                        info.node_disconnected.node_addr.address_bytes,
                        info.node_disconnected.node_addr.port);
                    // If there are more nodes left to try, the request will
                    // be retried automatically.
                    break;

                case node_error:
                    this.log.error("The request failed due to a node "
                        "error on {}:{}",
                        info.node_error.node_addr.address_bytes,
                        info.node_error.node_addr.port);
                    // If there are more nodes left to try, the request will
                    // be retried automatically.
                    break;

                case unsupported:
                    switch ( info.unsupported.type )
                    {
                        case info.unsupported.type.RequestNotSupported:
                            this.log.error("The request is not supported by "
                                "node {}:{}",
                                info.unsupported.node_addr.address_bytes,
                                info.unsupported.node_addr.port);
                            break;
                        case info.unsupported.type.RequestVersionNotSupported:
                            this.log.error("The request version is not "
                                "supported by node {}:{}",
                                info.unsupported.node_addr.address_bytes,
                                info.unsupported.node_addr.port);
                            break;
                        default: assert(false);
                    }
                    break;

                default: assert(false);
            }
        }
    }
}

/// Example of task-blocking neo Put request without user-provided notifier
unittest
{
    // An example of handling a blocking Put request
    class BlockingPutExample : ExampleApp
    {
        protected override void example ( )
        {
            // Perform a blocking neo Put request and return a result struct
            // indicating success/failure. Notes:
            // 1. In a real application, you probably want more information than
            //    just success/failure and should use the task-blocking method
            //    with a notifier (see example above).
            // 2. The channel and value are copied inside the client -- the user
            //    does not need to maintain them after calling this method.
            auto result = this.dls_client.blocking.put("channel", 0x1234,
                "value_to_put");
            if ( result.succeeded )
                this.log.trace("Put request succeeded");
            else
                this.log.trace("Put failed");
        }
    }
}

/// Example of using a GetRange controller with a suspendable throttler.
unittest
{
    /// ditto
    class SuspendableExample: ExampleApp
    {
        import ocean.io.model.ISuspendableThrottler;

        /// The suspendable thorttler instance that controls receiving records
        /// from the DLS
        private ISuspendableThrottler throttler;

        /// Suspendable instance
        private DlsClient.Neo.Suspendable!(DlsClient.Neo.GetRange.IController) suspendable;

        // Notifier for the GetRange request below. This attaches the
        // suspendable to the throttler after request is started. In addition
        // notifier must call suspendable.handlePending() after previously
        // initiated state-change has been completed (i.e. on suspended or
        // resumed).
        void getRangeNotifier ( DlsClient.Neo.GetRange.Notification info,
            DlsClient.Neo.GetRange.Args args )
        {
            with ( info.Active ) switch ( info.active )
            {
                // Request has started, attach the suspendable
                // to throttler
                case started:
                    this.throttler.addSuspendable(suspendable);
                    break;


                case suspended:
                    // handle state change
                    this.suspendable.handlePending();
                    break;

                case resumed:
                    // handle state change
                    this.suspendable.handlePending();
                    break;

                case finished:
                case stopped:
                    // request has finished, remove suspendable
                    this.throttler.removeSuspendable(suspendable);
                    this.log.trace("Request has finished on all nodes");
                    break;

                case received:
                    this.log.trace("Received key {} with value {}",
                            info.received.key, info.received.value);
                    break;

                case node_disconnected:
                    this.log.trace("GetRange failed due to connection "
                        "error {} on {}:{}",
                        getMsg(info.node_disconnected.e),
                        info.node_disconnected.node_addr.address_bytes,
                        info.node_disconnected.node_addr.port);
                    break;

                case node_error:
                    this.log.trace("GetRange failed due to a node "
                        "error on {}:{}",
                        info.node_error.node_addr.address_bytes,
                        info.node_error.node_addr.port);
                    break;

                case unsupported:
                    this.log.trace("GetRange failed due to an unsupported error "
                        "on {}:{}",
                        info.unsupported.node_addr.address_bytes,
                        info.unsupported.node_addr.port);
                    break;

                default: assert(false);
            }
        }

        protected override void example ( )
        {
            // Start a GetRange request
            auto request_id = this.dls_client.neo.getRange("channel", 0x0000, 0xFFFF,
                    &this.getRangeNotifier);

            // Get a Suspendable interface to the GetRange request
            this.suspendable = this.dls_client.neo.
                new Suspendable!(DlsClient.Neo.GetRange.IController)(request_id);

            // Note that, if the GetRange request finishes (for whatever reason),
            // the suspendable will throw, if used. To avoid this, it should be
            // removed from the throttler, when the request finishes (see notifier).
            // The suspendable is attached to throttler when request has been started.
            this.throttler = new SuspendableThrottlerCount(100, 10);
        }
    }
}
