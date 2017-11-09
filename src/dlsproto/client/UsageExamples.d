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

    import swarm.neo.authentication.HmacDef : Key;

    import swarm.neo.client.requests.NotificationFormatter;
}


version (UnitTest)
{
    // DaemonApp class showing typical neo DLS client initialisation. The
    // class has a single abstract method -- example() -- which is implemented
    // by various usage examples in this module, each demonstrating a different
    // client feature.
    abstract class ExampleApp: DaemonApp
    {
        import ocean.util.log.Logger;

        // DLS client (see dlsproto.client.DlsClient
        private DlsClient dls_client;

        // Logger used for logging notifications
        protected Logger log;

        // Legacy and neo config instances to be read from the config file.
        private DlsClient.Config config;
        private DlsClient.Neo.Config neo_config;

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
            this.dls_client = new DlsClient(theScheduler.epoll,
                    this.config, this.neo_config, &this.connNotifier);

            theScheduler.schedule(new AppTask);
            theScheduler.eventLoop();
            return 0;
        }

        // Reads the required config from the config file.
        override public void processConfig ( IApplication app,
                ConfigParser config_parser )
        {
            ConfigFiller.fill("DLS", this.config, config_parser);
            ConfigFiller.fill("DLS_Neo", this.neo_config, config_parser);
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
            formatNotification(info, this.dls_client.msg_buf);

            with (info.Active) switch (info.active)
            {
                case connected:
                    // The connection succeeded.
                    this.log.trace(this.dls_client.msg_buf);
                    break;
                case error_while_connecting:
                    // A connection error occurred. The client will
                    // automatically try to reconnect.
                    this.log.trace(this.dls_client.msg_buf);
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
            formatNotification(info, this.dls_client.msg_buf);

            // `info` is a smart union, where each member of the union
            // represents one possible notification. `info.active` denotes
            // the type of the current notification. Some notifications
            // have fields containing more information:
            with ( info.Active ) switch ( info.active )
            {
                case success:
                    this.log.trace("The request succeeded!");
                    break;

                case node_disconnected:
                case failure:
                case node_error:
                case unsupported:
                    this.log.error(this.dls_client.msg_buf);
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
            formatNotification(info, this.dls_client.msg_buf);

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
                case node_disconnected:
                case node_error:
                case unsupported:
                    this.log.error(this.dls_client.msg_buf);
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
            formatNotification(info, this.dls_client.msg_buf);

            this.log.trace("Request context was: {}",
                    args.context.integer());

            with ( info.Active ) switch ( info.active )
            {
                case received:
                    this.log.trace("Received key {} with value {}",
                            info.received.key, info.received.value);
                    break;

                case started:
                case finished:
                case node_disconnected:
                case node_error:
                case unsupported:
                case stopped:
                    this.log.error(this.dls_client.msg_buf);
                    break;

                default: assert(false);
            }
        }
    }
}

/// Example of task-blocking Neo GetRange request. Note that this approach
/// doesn't provide advanced features, such as suspend/resume.
unittest
{
    class BlockingGetRangeExample : ExampleApp
    {
        protected override void example ( )
        {
            // Assigns the getRange request and provides opApply-like
            // task blocking interface.

            // buffer for the result
            void[] buf;

            foreach ( key, val;
                this.dls_client.blocking.getRange("channel", buf, 0x1234, 0x5678))
            {
                this.log.trace("Received {}: {}", key, val);
            }
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
            formatNotification(info, this.dls_client.msg_buf);

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
                case node_error:
                case unsupported:
                    this.log.error(this.dls_client.msg_buf);
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
