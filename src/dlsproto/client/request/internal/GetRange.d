/*******************************************************************************

    Client DLS GetRange v0 request handler.

    Copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.client.request.internal.GetRange;

/*******************************************************************************

    Imports

*******************************************************************************/

import core.stdc.time;

import ocean.transition;
import ocean.util.log.Logger;

import swarm.neo.client.RequestOnConn;

import dlsproto.client.internal.SharedResources;

/*******************************************************************************

    Module logger

*******************************************************************************/

static private Logger log;
static this ( )
{
    log = Log.lookup("dlsproto.client.request.internal.GetRange");
}

/*******************************************************************************

    GetRange request implementation.

    Note that request structs act simply as namespaces for the collection of
    symbols required to implement a request. They are never instantiated and
    have no fields or non-static functions.

    The client expects several things to be present in a request struct:
        1. The static constants request_type and request_code
        2. The UserSpecifiedParams struct, containing all user-specified request
            setup (including a notifier)
        3. The Notifier delegate type
        4. Optionally, the Controller type (if the request can be controlled,
           after it has begun)
        5. The handler() function
        6. The all_finished_notifier() function

    The RequestCore mixin provides items 1, 2 and 3.

*******************************************************************************/

public struct GetRange
{
    import dlsproto.common.GetRange;
    import dlsproto.common.RequestCodes;
    import dlsproto.client.request.GetRange;
    import dlsproto.client.internal.SharedResources;

    import ocean.io.select.protocol.generic.ErrnoIOException: IOError;
    import ocean.transition;

    import swarm.neo.client.mixins.AllNodesRequestCore;
    import swarm.neo.client.mixins.BatchRequestCore;
    import swarm.neo.client.mixins.RequestCore;
    import swarm.neo.client.RequestHandlers;
    import swarm.neo.client.RequestOnConn;
    import swarm.neo.client.IRequestSet;

    mixin TypeofThis!();


    /***************************************************************************

        BatchController: template mixin containing a class that implements the
        standard logic for a request controller accessible via the user API.
        Has public methods suspend(), resume(), and stop().

    ***************************************************************************/

    mixin BatchController!(typeof(this), IController);


    /***************************************************************************

        Data which the request needs while it is in progress. An instance of this
        struct is stored per connection on which the request runs and is passed
        to the request handler.

    ***************************************************************************/

    private struct SharedWorking
    {
        /// Shared working data required for core all-nodes request behaviour.
        AllNodesRequestSharedWorkingData all_nodes;

        /// Data required by the BatchController
        BatchRequestSharedWorkingData suspendable_control;

        /// Indicator if the request was stopped (for avoiding extra Finished notification)
        bool stopped;
    }

    // dummy struct, needed for RequestCore interface
    private struct Working {}

    /***************************************************************************

        Request core. Mixes in the types `NotificationInfo`, `Notifier`,
        `Params`, `Context` plus the static constants `request_type` and
        `request_code`.

    ***************************************************************************/

    mixin RequestCore!(RequestType.AllNodes, RequestCode.GetRange, 1,
        Args, SharedWorking, Working, Notification);

    /***************************************************************************

        Request handler. Called from RequestOnConn.runHandler().

        Params:
            conn = connection event dispatcher
            context_blob = untyped chunk of data containing the serialized
                context of the request which is to be handled
            working_blob = untyped chunk of data containing the serialized
                working data for the request on this connection

    ***************************************************************************/

    public static void handler ( RequestOnConn.EventDispatcherAllNodes conn,
        void[] context_blob, void[] working_blob )
    {
        auto context = This.getContext(context_blob);

        auto shared_resources = SharedResources.fromObject(
            context.request_resources.get());
        scope acquired_resources = shared_resources.new RequestResources;
        scope handler = new GetRangeHandler(conn, context, acquired_resources);

        handler.run();
    }

    /***************************************************************************

        Request finished notifier. Called from Request.handlingFinishedNotification().

        Params:
            context_blob = untyped chunk of data containing the serialized
                context of the request which is finishing
            working_data_iter = iterator over the stored working data associated
                with each connection on which this request was run

    ***************************************************************************/

    public static void all_finished_notifier ( void[] context_blob,
        IRequestWorkingData working_data_iter )
    {
        auto context = This.getContext(context_blob);

        if (!context.shared_working.stopped)
        {
            // Final notification, after the request has been finished
            Notification notification;
            notification.finished = NoInfo();
            GetRange.notify(context.user_params, notification);
        }
    }
}

/*******************************************************************************

    GetRange v1 handler class instantiated inside the main handler() function,
    above.

*******************************************************************************/

private scope class GetRangeHandler
{
    import dlsproto.common.GetRange;
    import dlsproto.common.RequestCodes;
    import dlsproto.client.request.GetRange;
    import dlsproto.client.internal.SharedResources;

    import swarm.neo.request.Command : StatusCode;
    import swarm.neo.client.RequestOnConn;
    import swarm.neo.connection.RequestOnConnBase;
    import swarm.neo.client.mixins.AllNodesRequestCore;
    import swarm.neo.client.mixins.BatchRequestCore;
    import swarm.neo.request.RequestEventDispatcher;
    import swarm.neo.util.MessageFiber;


    alias GetRange.BatchRequestSharedWorkingData.Signal ControllerSignal;

    /// Request-on-conn event dispacher
    private RequestOnConn.EventDispatcherAllNodes conn;

    /// Request context
    private GetRange.Context* context;

    /// Request resource acquier.
    private SharedResources.RequestResources resources;

    /// Request event dispatcher
    private RequestEventDispatcher* request_event_dispatcher;

    /***************************************************************************

        Constructor.

        Params:
            conn = request-on-conn event dispatcher to communicate with node
            context_blob = deserialized request context
            resources = request resources acquirer

    ***************************************************************************/

    public this ( RequestOnConn.EventDispatcherAllNodes conn,
                  GetRange.Context* context, SharedResources.RequestResources resources )
    {
        this.conn = conn;
        this.context = context;
        this.resources = resources;
        this.request_event_dispatcher = resources.request_event_dispatcher;
    }

    /***************************************************************************

        Main request handling entry point.

    ***************************************************************************/

    public void run ( )
    {
        auto initialiser = createAllNodesRequestInitialiser!(GetRange)(
            this.conn, this.context, &this.fillInitialPayload, &this.handleStatusCode);

        auto request = createAllNodesRequest!(GetRange)(this.conn, this.context,
            &this.connect, &this.disconnected, initialiser, &this.handle);

        request.run();
    }


    /***************************************************************************

        Connect policy, called from AllNodesRequest template to ensure the
        connection to the node is up.

        Returns:
            true to continue handling the request; false to abort.

    ***************************************************************************/

    private bool connect ( )
    {
        return batchRequestConnector(this.conn);
    }


    /**************************************************************************

        Disconnected policy, called from AllNodesRequest template when an I/O
        error occurs on the connection.

        Params:
            e = exception indicating error which occured on the connection.

    **************************************************************************/

    private void disconnected (Exception e)
    {
        // Notify the user of the disconnection. The user may use the controller
        // at this point, but as the request is not active
        // on this connection, no special behaviour is needed.
        GetRange.Notification notification;
        notification.node_disconnected =
            NodeExceptionInfo(this.conn.remote_address, e);
        GetRange.notify(this.context.user_params, notification);
    }

    /**************************************************************************

        Fill initial payload policy, called from AllNodesRequestInitialiser
        template to add request-specific data to the initial message payload sent
        to the node to begin the request.

        Params:
            payload = message payload to be filled.

    **************************************************************************/

    private void fillInitialPayload (RequestOnConnBase.EventDispatcher.Payload payload)
    {
        payload.addArray(this.context.user_params.args.channel);
        payload.add(this.context.user_params.args.lower_bound);
        payload.add(this.context.user_params.args.upper_bound);
        payload.addArray(this.context.user_params.args.filter_string);
        payload.add(this.context.user_params.args.filter_mode);
    }

    /**************************************************************************

        HandleStatus code policy, called from AllNodesRequestInitialiser
        template to decide how to handle the status code received from the node.

        Params:
            status = status code received from the node in response to the initial
                message

        Returns:
            true to continue handling the request (OK status); false to
            abort (error status).

    **************************************************************************/

    private bool handleStatusCode ( ubyte status )
    {
        auto getrange_status = cast(RequestStatusCode)status;

        if (GetRange.handleGlobalStatusCodes(getrange_status,
                this.context, this.conn.remote_address))
        {
            return false; // Global code, e.g. request/version not supported
        }

        // GetRange specific codes
        switch (getrange_status)
        {
            case getrange_status.Started:
                // Expected, "request started" code
                return true;

            case getrange_status.Error:
            default:
                // The node returned an error code. Notify the user and
                // end the request.
                GetRange.Notification n;
                n.node_error = NodeInfo(this.conn.remote_address);
                GetRange.notify(this.context.user_params, n);
                return false;
        }

        assert(false);
    }

    /**************************************************************************

        Handler policy, called from AllNodesRequest template to run the
        request's main handling logic.

    **************************************************************************/

    private void handle ( )
    {
        scope record_stream = this.new RecordStream;
        scope reader = this.new Reader(record_stream);
        scope controller = this.new Controller(record_stream);

        this.request_event_dispatcher.eventLoop(this.conn);

        with (record_stream.fiber) assert(state == state.TERM);
        with (reader.fiber) assert(state == state.TERM);
        with (controller.fiber) assert(state == state.TERM);
    }

    /**************************************************************************

        Codes for signals sent across the fibers.

    **************************************************************************/

    enum FiberSignal: ubyte
    {
        /// Resumes the `RecordStream` fiber.
        ResumeRecordStream = ControllerSignal.max + 1,
        /// Tells the `Controller` to terminate.
        StopController
    }

    /**************************************************************************

        The fiber that waits for a batch of records to arrive and passes it to
        the user, then sends the `Continue` message to the node, in a loop.
        Handles suspending the request through the controller, for resuming
        call `resume`. Calling `stop` makes this routine terminate after all
        remaining records have been passed to the user.

    ***************************************************************************/

    private /* scope */ class RecordStream
    {
        /// The acquired buffer to store a batch of records.
        private void[]* batch_buffer;

        /// Slices the records in *batch_buffer that haven't been processed yet
        private Const!(void)[] remaining_batch = null;

        /// The fiber.
        private MessageFiber fiber;

        /// tells if the fiber is suspended, and if yes, what it is waiting for.
        enum FiberSuspended: uint
        {
            No,
            WaitingForRecords,
            RequestSuspended
        }

        /// ditto
        private FiberSuspended fiber_suspended;

        /// If true, causes the fiber to exit after processing remaining records
        /// in the batch. Set if the stop method is called.
        private bool stopped;

        /// Token passed to fiber suspend/resume calls.
        private static MessageFiber.Token token =
            MessageFiber.Token(typeof(this).stringof);

        /// Constructor, starts the fiber.
        private this ()
        {
            this.batch_buffer = this.outer.resources.getVoidBuffer();
            this.fiber = this.outer.resources.getFiber(&this.fiberMethod);
            this.fiber.start();
        }

        /**********************************************************************

            Adds a batch of records to be passed to the user notifier
            and resumes the fiber if it is waiting for more records. Called
            by the `Reader` when a `Records` message from the node has arrived.

            Params:
                record_batch = the batch of records to add.

        **********************************************************************/

        public void addRecords ( in void[] record_batch )
        {
            // append record_batch to *this.batch_buffer, which may or may not
            // be empty.

            if (!(*this.batch_buffer).length)
                enableStomping(*this.batch_buffer);

            // Append record_batch, then set this.remaining_batch to reference
            // the remaining records. To void a dangling slice if
            // *this.batch_buffer is relocated, set this.remaining_batch to
            // null first.
            size_t n_processed = (*this.batch_buffer).length - this.remaining_batch.length;
            (*this.batch_buffer) ~= record_batch;
            this.remaining_batch = (*this.batch_buffer)[n_processed .. $];

            if (this.fiber_suspended == fiber_suspended.WaitingForRecords)
                this.resumeFiber();
        }

        /**********************************************************************

            Resumes passing records to the user. Called by `Controller` when
            the user resumes the request through the controller.

        **********************************************************************/

        public void resume ( )
        {
            if (this.fiber_suspended == fiber_suspended.RequestSuspended)
            {
                this.resumeFiber();
            }
        }

        /**********************************************************************

            Requests the fiber to terminate when all remaining records have been
            passed to the user. called when a `Stopped` message from the node
            has arrived.

        **********************************************************************/

        public void stop ( )
        {
            this.stopped = true;
            if (this.fiber_suspended == fiber_suspended.WaitingForRecords)
                this.resumeFiber();
        }

        /**********************************************************************

            Waits for a batch of records to be fed to it by the `Reader` and
            passed it to the user, then sends the `Continue` message to the node,
            in a loop. Handles suspending the request through the
            controller, for resuming call `resume`. Calling `stop` makes this
            routine terminate after all remaining records have been passed to the
            user.

        **********************************************************************/

        private void fiberMethod ( )
        {
            while (this.waitForRecords())
            {
                for (uint yield_count = 0; this.remaining_batch.length; yield_count++)
                {
                    if (yield_count >= 10) //yield every 10 records
                    {
                        yield_count = 0;
                        this.outer.request_event_dispatcher.yield(this.fiber);
                    }

                    // did the user request suspension?
                    if (this.outer.context.shared_working.suspendable_control.suspended)
                    {
                        yield_count = 0;
                        this.suspendFiber(FiberSuspended.RequestSuspended);
                    }

                    this.passRecordToUser(
                        *this.outer.conn.message_parser.getValue!(time_t)(
                            this.remaining_batch),
                        this.outer.conn.message_parser.getArray!(Const!(void))(
                            this.remaining_batch
                        ));

                }

                this.remaining_batch = null;
                (*this.batch_buffer).length = 0;

                if (this.stopped)
                    break;

                this.outer.request_event_dispatcher.send(
                    this.fiber,
                    (conn.Payload payload)
                    {
                        payload.addConstant(MessageType.Continue);
                    }
                );
            }

            this.outer.request_event_dispatcher.signal(this.outer.conn,
                FiberSignal.StopController);
        }

        /**********************************************************************

            Suspends the fiber to be resumed by `addRecords` or `stop`.

            Returns:
                true if the fiber was resumed by `addRecords` or false if
                resumed by `stop`.

        **********************************************************************/

        private bool waitForRecords ()
        {
            this.suspendFiber(FiberSuspended.WaitingForRecords);
            return !this.stopped;
        }

        /**********************************************************************

            Calls the user notifier to pass `record` to the user. Handles a
            request state change (i.e. stopping the request) if the user uses
            the controller in the notifier.

            Params:
                key = key of the record to pass to the user
                record = the record to pass to the user

        **********************************************************************/

        private void passRecordToUser ( time_t key, in void[] record )
        {
            bool initially_stopped =
                this.outer.context.shared_working.suspendable_control.stopped;

            Notification notification;
            notification.received = RequestRecordInfo(this.outer.context.request_id,
                key,
                record);
            GetRange.notify(this.outer.context.user_params, notification);

            if (!initially_stopped &&
                this.outer.context.shared_working.suspendable_control.stopped)
            {
                this.outer.request_event_dispatcher.signal(this.outer.conn,
                    ControllerSignal.Stop);
            }
        }

        /**********************************************************************

            Suspends the fiber, waiting for `FiberSignal.ResumeRecordStream`.
            `why` specifies the current state of the fiber method and determines
            which of the public methods should raise that signal.

            Params:
                why = the event on which the fiber method needs to be resumed.

        **********************************************************************/

        private void suspendFiber (FiberSuspended why)
        {
            this.fiber_suspended = why;
            try
            {
                this.outer.request_event_dispatcher.nextEvent(this.fiber,
                    Signal(FiberSignal.ResumeRecordStream));
            }
            finally
            {
                this.fiber_suspended = this.fiber_suspended.No;
            }
        }

        /**********************************************************************

            Raises `FiberSignal.ResumeRecordStream` to resume the fiber.

        **********************************************************************/

        private void resumeFiber ()
        {
            this.outer.request_event_dispatcher.signal(this.outer.conn,
                FiberSignal.ResumeRecordStream);
        }
    }


    /**************************************************************************

        The fiber that reads messages from the node and notifies `RecordStream`.

    **************************************************************************/

    private /* scope */ class Reader
    {
        import ocean.io.compress.Lzo;

        /// The fiber.
        private MessageFiber fiber;

        /// The `RecordStream` to notify when the message has arrived.
        private RecordStream record_stream;

        /// The acquired buffer to store an uncompressed batch of records
        private void[]* uncompressed_batch;

        /// The lzo object for decompressing the batch
        private Lzo lzo;

        /**********************************************************************

            Constructor, starts the fiber.

            Params:
                record_stream = the `RecordStream` to notify when a message
                has arrived.

        **********************************************************************/

        private this (RecordStream record_stream)
        {
            this.record_stream = record_stream;
            this.lzo = this.outer.resources.getLzo();
            this.uncompressed_batch = this.outer.resources.getVoidBuffer();
            this.fiber = this.outer.resources.getFiber(&this.fiberMethod);
            this.fiber.start();
        }

        /**********************************************************************

            Reads messages from the node and notifies `record_stream` by calling
            its respective methods when a `Records` or `Stopped` message has
            arrived. Quits when a `Stopped` message has arrived because
            `Stopped` is a last message from the node.

        **********************************************************************/

        private void fiberMethod ()
        {
            bool finished;

            do
            {
                auto msg = this.outer.request_event_dispatcher.receive(
                    this.fiber,
                    Message(MessageType.Records),
                    Message(MessageType.Stopped),
                    Message(MessageType.Finished));

                switch (msg.type)
                {
                    case MessageType.Records:
                        Const!(void)[] received_record_batch;
                        size_t uncompressed_batch_size;

                        this.outer.conn.message_parser.parseBody(
                            msg.payload, uncompressed_batch_size,
                            received_record_batch
                        );

                        (*uncompressed_batch).length = uncompressed_batch_size;
                        enableStomping(*uncompressed_batch);

                        this.lzo.uncompress(received_record_batch,
                               *uncompressed_batch);
                        this.record_stream.addRecords(*uncompressed_batch);
                        break;

                    case MessageType.Stopped:
                        this.record_stream.stop();
                        return;

                    case MessageType.Finished:
                        finished = true;
                        break;

                    default:
                        assert(false);
                }
            }
            while (!finished);

            /// Ack finished
            this.outer.resources.request_event_dispatcher.send(this.fiber,
                    (RequestOnConnBase.EventDispatcher.Payload payload)
                    {
                        payload.addConstant(MessageType.Ack);
                    }
            );

            this.record_stream.stop();
        }
    }


    /**************************************************************************

        The fiber that handles user controlled signals.

    **************************************************************************/

    private /* scope */ class Controller
    {
        /// The fiber.
        private MessageFiber fiber;

        /// The `RecordStream` to notify when the request is resumed.
        private RecordStream record_stream;

        /**********************************************************************

            Constructor, starts the fiber.

            Params:
                record_stream = the `RecordStream` to notify when the request is
                resumed.

        **********************************************************************/

        private this (RecordStream record_stream)
        {
            this.record_stream = record_stream;
            this.fiber = this.outer.resources.getFiber(&this.fiberMethod);
            this.fiber.start();
        }

        /**********************************************************************

            Waits for controller signals and handles them. Terminates on
            `FiberSignal.StopController`

        **********************************************************************/

        private void fiberMethod ()
        {
            while (true)
            {
                auto event = this.outer.request_event_dispatcher.nextEvent(
                    this.fiber,
                    Signal(ControllerSignal.Resume),
                    Signal(ControllerSignal.Stop),
                    Signal(FiberSignal.StopController)
                );

                switch (event.signal.code)
                {
                    case ControllerSignal.Resume:
                        this.record_stream.resume();
                        break;

                    case ControllerSignal.Stop:
                        this.outer.request_event_dispatcher.send(
                            this.fiber,
                            (conn.Payload payload)
                            {
                                payload.addConstant(MessageType.Stop);
                            }
                        );
                        this.outer.context.shared_working.stopped = true;
                        break;

                    case FiberSignal.StopController:
                        return;

                    default:
                        assert(false);
                }
            }
        }
    }
}

/*******************************************************************************

    GetRange request implementation.

    Note that request structs act simply as namespaces for the collection of
    symbols required to implement a request. They are never instantiated and
    have no fields or non-static functions.

    The client expects several things to be present in a request struct:
        1. The static constants request_type and request_code
        2. The UserSpecifiedParams struct, containing all user-specified request
            setup (including a notifier)
        3. The Notifier delegate type
        4. Optionally, the Controller type (if the request can be controlled,
           after it has begun)
        5. The handler() function
        6. The all_finished_notifier() function

    The RequestCore mixin provides items 1, 2 and 3.

*******************************************************************************/

version(none) public struct GetRange_v0
{
    import dlsproto.common.GetRange;
    import dlsproto.client.request.GetRange;
    import dlsproto.common.RequestCodes;

    import swarm.neo.client.mixins.RequestCore;

    import ocean.io.select.protocol.generic.ErrnoIOException: IOError;
    import dlsproto.client.internal.SuspendableRequest;

    /***************************************************************************

        Request controller, accessible to the user via the client's `control()`
        method.

    ***************************************************************************/

    public static scope class Controller : IController
    {
        import ocean.core.Enforce;

        /***********************************************************************

            Base mixin.

        ***********************************************************************/

        mixin ControllerBase;

        /***********************************************************************

            Custom fiber resume code, used when the request handling fiber is
            resumed by the controller.

        ***********************************************************************/

        private enum GetRangeFiberResumeCode
        {
            ControlMessage = 3
        }

        /***********************************************************************

            Tells the nodes to stop sending data to this request.

            Returns:
                false if the controller cannot be used because a control change
                is already in progress

        ***********************************************************************/

        override public bool suspend ( )
        {
            return this.changeDesiredState(MessageType.Suspend);
        }

        /***********************************************************************

            Tells the nodes to resume sending data to this request.

            Returns:
                false if the controller cannot be used because a control change
                is already in progress

        ***********************************************************************/

        override public bool resume ( )
        {
            return this.changeDesiredState(MessageType.Resume);
        }

        /***********************************************************************

            Tells the nodes to cleanly end the request.

            Returns:
                false if the controller cannot be used because a control change
                is already in progress

        ***********************************************************************/

        override public bool stop ( )
        {
            return this.changeDesiredState(MessageType.Stop);
        }

        /***********************************************************************

            Changes the desired state to that specified. Sets the desired state
            flag and resumes any handler fibers which are suspended, passing the
            control message flag to the fiber via the return value of suspend().

            If one or more connections are not ready to change state, the
            control change does not occur. A connection is ready to change the
            request state unless the handler is currently waiting for an
            acknowledgement message when beginning the request or changing its
            state.

            Params:
                code = desired state

            Returns:
                true if the state change has been accepted and will be sent to
                all active nodes, false if one or more connections is already in
                the middle of changing state

        ***********************************************************************/

        private bool changeDesiredState ( MessageType code )
        {
            auto context = GetRange.getContext(this.request_controller.context_blob);

            NoInfo info;
            Notification notification;

            SuspendableRequest.SharedWorking.RequestState new_state;

            // Set the desired state in the shared working data
            with ( MessageType ) switch ( code )
            {
                case Resume:
                    new_state = SuspendableRequest.SharedWorking.RequestState.Running;
                    notification.resumed = info;
                    break;
                case Suspend:
                    new_state = SuspendableRequest.SharedWorking.RequestState.Suspended;
                    notification.suspended = info;
                    break;
                case Stop:
                    new_state = SuspendableRequest.SharedWorking.RequestState.Stopped;
                    notification.stopped = info;
                    break;
                default: assert(false,
                    "GetRange.Controller: Unexpected message type");
            }

            return context.shared_working.setDesiredState(new_state,
                {
                    this.request_controller.resumeSuspendedHandlers(
                        GetRangeFiberResumeCode.ControlMessage);
                },
                {
                    GetRange.notify(context.user_params, notification);
                });
        }
    }

    /***************************************************************************

        Data which each request-on-conn needs while it is progress. An instance
        of this struct is stored per connection on which the request runs and is
        passed to the request handler.

    ***************************************************************************/

    private static struct Working
    {
        MessageType requested_control_msg = MessageType.None;
    }

    /***************************************************************************

        Request core. Mixes in the types `NotificationInfo`, `Notifier`,
        `Params`, `Context` plus the static constants `request_type` and
        `request_code`.

    ***************************************************************************/

    mixin RequestCore!(RequestType.AllNodes, RequestCode.GetRange, 0,
        Args, SuspendableRequest.SharedWorking, Working, Notification);

    /***************************************************************************

        Request handler. Called from RequestOnConn.runHandler().

        Params:
            conn = connection event dispatcher
            context_blob = untyped chunk of data containing the serialized
                context of the request which is to be handled
            working_blob = untyped chunk of data containing the serialized
                working data for the request on this connection

    ***************************************************************************/

    public static void handler ( RequestOnConn.EventDispatcherAllNodes conn,
        void[] context_blob, void[] working_blob )
    {
        auto context = This.getContext(context_blob);
        scope resources = (cast(SharedResources)(context.request_resources.get())).new RequestResources;
        scope h = new Handler(conn, context_blob, resources);

        bool reconnect;
        do
        {
            try
            {
                h.run(h.State.EnsuringConnEstablished);
                reconnect = false;
            }
            // Only retry in the case of a connection error, when initializing.
            // If not initializing, report error, but don't reconnect. Other
            // errors indicate internal problems and should be handled exceptionally.
            catch (IOError e)
            {
                if (!h.initialized)
                {
                    // Reset the working data of this connection to the initial state.
                    auto working = GetRange.getWorkingData(working_blob);
                    *working = Working.init;

                    reconnect = true;
                }

                // Notify the user of the disconnection. The user may use the
                // controller, at this point, but as the request is not active
                // on this connection, no special behaviour is needed.
                Notification notification;
                notification.node_disconnected =
                    NodeExceptionInfo(conn.remote_address, e);
                GetRange.notify(h.context.user_params, notification);
            }
            finally
            {
                h.request.setNotReadyForStateChange();
            }
        }
        while ( reconnect );
    }

    /***************************************************************************

        Request finished notifier. Called from Request.handlingFinishedNotification().

        Params:
            context_blob = untyped chunk of data containing the serialized
                context of the request which is finishing
            working_data_iter = iterator over the stored working data associated
                with each connection on which this request was run

    ***************************************************************************/

    public static void all_finished_notifier ( void[] context_blob,
        IRequestWorkingData working_data_iter )
    {
        auto context = This.getContext(context_blob);

        if (context.shared_working.desired_state !=
                SuspendableRequest.SharedWorking.RequestState.Stopped)
        {
            // Final notification, after the request has been finished
            Notification notification;
            notification.finished = NoInfo();
            GetRange.notify(context.user_params, notification);
        }
    }
}

/*******************************************************************************

    GetRange handler class instantiated inside the main handler() function,
    above.

*******************************************************************************/

version (none) private scope class Handler_v0
{
    import dlsproto.common.GetRange;
    import dlsproto.client.request.GetRange;
    import dlsproto.common.RequestCodes;


    import swarm.neo.util.StateMachine;
    import swarm.neo.request.Command : StatusCode;
    import swarm.neo.util.StateMachine;
    import swarm.util.RecordBatcher;

    import dlsproto.client.internal.SuspendableRequest;

    /***************************************************************************

        Convenience alias to the ReceivedMessageAction enum of the
        SuspendableRequest.

    ***************************************************************************/

    alias SuspendableRequest.ReceivedMessageAction ReceivedMessageAction;

    /***************************************************************************

        Mixin core of state machine.

    ***************************************************************************/

    mixin(genStateMachine([
        "EnsuringConnEstablished",
        "Initialising",
        "Receiving",
        "RequestingStateChange",
        "HandlingFinishNotification"
    ]));

    /***************************************************************************
        Maps from SuspendableRequest.State enum to State enum.

        This is a helper method, as the SuspendableRequest is a generic
        tool, where statuses may or may not match the requests statuses,
        so the explicit mapping is needed.

        Params:
            state = SuspendableRequest's state

        Returns:
            Request state corresponding to SuspendableRequest's one

    ***************************************************************************/

    State nextState (SuspendableRequest.State state)
    {
        with (SuspendableRequest.State) switch (state)
        {
            case Initialising:
                return State.Initialising;
            case Receiving:
                return State.Receiving;
            case RequestingStateChange:
                return State.RequestingStateChange;
            case HandlingFinishNotification:
                return State.HandlingFinishNotification;
            case Exit:
                return State.Exit;
            default:
                assert(false);
        }
    }

    /***************************************************************************

        Event dispatcher for this connection.

    ***************************************************************************/

    private RequestOnConn.EventDispatcherAllNodes conn;

    /***************************************************************************

        Deserialized request context.

    ***************************************************************************/

    public GetRange.Context* context;

    /***************************************************************************

        RecordBatch instance used to store and decompress the received
        batch of records.

   ****************************************************************************/

    private RecordBatch record_batch;

    /***************************************************************************

        SuspendableRequest instance. Provides a logic for implementing
        suspendable request.

   ****************************************************************************/

    private SuspendableRequest request;

    /***************************************************************************

        Indicator if the connection with the node has been initialized.

    ***************************************************************************/

    private bool initialized;

    /***************************************************************************

        Constructor.

        Params:
            conn = Event dispatcher for this connection
            context_blob = serialized request context
            resources = object to acquire shared resouces from

    ***************************************************************************/

    public this ( RequestOnConn.EventDispatcherAllNodes conn,
                  void[] context_blob, SharedResources.RequestResources resources )
    {
        this.conn = conn;
        this.context = GetRange.getContext(context_blob);
        this.record_batch = resources.getRecordBatch();
        this.request.initialise(this.conn, &this.context.shared_working);
    }

    /***************************************************************************

        Waits for the connection to be (re)established if it is down or the
        request to be stopped.

        Next state:
            - Initialising (by default)
            - Exit if the desired state becomes Stopped, while connection is in
              progress.

        Returns:
            next state

    ***************************************************************************/

    private State stateEnsuringConnEstablished ( )
    {
        return this.nextState(this.request.establishConnection((int resume_code) {
                return resume_code == GetRange.Controller.GetRangeFiberResumeCode.ControlMessage;
        }));
    }

    /***************************************************************************

        Sends the request code, version, channel, etc. to the node to begin the
        request, then receives the status code from the node (unless the desired
        state is Stopped; nothing is done in this case).

        Next state:
            - Receiving (by default)
            - RequestingStateChange if the user changed the state in the
              notifier
            - Exit if the desired state is Stopped

        Returns:
            next state

    ***************************************************************************/

    private State stateInitialising ( )
    {
        // Figure out what starting state to tell the node to begin handling the
        // request in.
        StartState start_state;
        with (this.context.shared_working) switch (desired_state)
        {
            case desired_state.Running:
                start_state = start_state.Running;
                break;

            case desired_state.Suspended:
                start_state = start_state.Suspended;
                break;

            case desired_state.Stopped:
                return State.Exit;

            default:
                assert(false, typeof(this).stringof ~ ".stateInitialising: invalid desired state");
        }

        return nextState(this.request.initialiseRequest(
                ( conn.Payload payload )
                {
                    payload.add(GetRange.cmd.code);
                    payload.add(GetRange.cmd.ver);
                    payload.addArray(this.context.user_params.args.channel);
                    payload.add(this.context.user_params.args.lower_bound);
                    payload.add(this.context.user_params.args.upper_bound);
                    // Always sending regex here is done in order not to
                    // have to allocate extra buffer for channel name
                    // on the node side (and as well to completely avoid
                    // second send/receive cycle)
                    payload.addArray(this.context.user_params.args.filter_string);
                    payload.add(this.context.user_params.args.filter_mode);
                    payload.add(start_state);
                },
                (StatusCode received_status_code) {
                    return received_status_code == RequestStatusCode.Started;
                },
                {
                    Notification notification;
                    notification.started = NoInfo();
                    GetRange.notify(this.context.user_params, notification);
                    this.initialized = true;
                }));
    }

    /***************************************************************************

        Default running state. Receives one record message from the node and
        passes it to the user's notifier delegate.

        Next state:
            - again Receiving (by default)
            - RequestingStateChange if the user changed the state

        Returns:
            next state

    ***************************************************************************/

    private State stateReceiving ( )
    {
        return nextState(this.request.receive(&this.handleMessage));
    }

    /***************************************************************************

        Sends a request state change message to the node and waits for the
        acknowledgement, handling records arriving in the mean time, as normal.

        Next state:
            - Receiving by default, i.e. if the desired state is Running or
              Suspended
            - Exit if the desired state is Stopped
            - again RequestingStateChange if the user changed the state in the
              notifier

        Returns:
            next state

    ***************************************************************************/

    private State stateRequestingStateChange ( )
    {
        // Based on the desired state, decide which control message to send to
        // the node and which notification type to use.
        MessageType control_msg;
        Notification notification;
        this.stateChangeMsgAndNotification(control_msg, notification);

        return nextState(this.request.requestStateChange(control_msg,
                &this.handleMessage,
                {
                    GetRange.notify(this.context.user_params, notification);
                }
        ));
    }

    /***************************************************************************

        Helper function for stateRequestingStateChange(). Based on the currently
        desired request state, determines:
            1. the MessageType to send to the node
            2. the type of notification to send to the user, once the request
               has changed state on all nodes

        Params:
            control_msg = set to the MessageType to send to the node
            notification = set to the notification to send to the user

    ***************************************************************************/

    private void stateChangeMsgAndNotification ( out MessageType control_msg,
        out Notification notification )
    {
        with ( this.context.shared_working ) switch ( desired_state )
        {
            case desired_state.Running:
                control_msg = MessageType.Resume;
                notification.resumed = NoInfo();
                break;

            case desired_state.Suspended:
                control_msg = MessageType.Suspend;
                notification.suspended = NoInfo();
                break;

            case desired_state.Stopped:
                control_msg = MessageType.Stop;
                notification.stopped = NoInfo();
                break;

            default: assert(false, typeof(this).stringof ~
                ".stateChangeMsgAndNotification: " ~
                "Unexpected desired state requested");
        }
    }

    /***************************************************************************

        Sends the acknowledgment that the finished notification has been received.

    ***************************************************************************/

    private State stateHandlingFinishNotification ( )
    {
        return this.nextState(
            this.request.handleFinishNotification(MessageType.Ack));
    }

    /***************************************************************************

        Helper function to handle messages received from the node. Messages
        containing records are passed to the user's receiving delegate
        (specified in the request params).

        Params:
            payload = raw message payload received from the node

        Returns:
            The message type.

    ***************************************************************************/

    private ReceivedMessageAction handleMessage ( Const!(void)[] payload )
    {
        auto msg_type = *this.conn.message_parser.getValue!(MessageType)(payload);

        with (MessageType) switch (msg_type)
        {
            case Record:
                auto received_data = this.conn.message_parser.getArray!(void)(payload);

                // Now, let's decompress the batch
                this.record_batch.decompress(cast(ubyte[])received_data);

                foreach (timestamp, record; this.record_batch)
                {
                    Notification notification;
                    notification.received = RecordInfo(*(cast(time_t*)timestamp.ptr), record);
                    GetRange.notify(this.context.user_params, notification);
                }

                break;

            // end of sending
            case Finished:
               return ReceivedMessageAction.Finished;

            case Error:
                throw this.conn.shutdownWithProtocolError("Error handling the request");

            case Ack:
                return ReceivedMessageAction.Ack;

            default:
                break;
        }

        return ReceivedMessageAction.Continue;
    }

    /***************************************************************************

        Debug message, printed on state change.

    ***************************************************************************/

    debug (ClientGetRangeState):

    import ocean.io.Stdout;

    private void beforeState ( )
    {
        static char[][] machine_msg =
        [
            State.EnsuringConnEstablished: "EnsuringConnEstablished",
            State.Initialising: "Initialising",
            State.Receiving: "Receiving",
            State.RequestingStateChange: "RequestingStateChange",
            State.Exit: "Exit",
        ];

        alias typeof(this.context.shared_working.desired_state) DesiredState;

        static char[][] request_msg =
        [
            DesiredState.None: "???",
            DesiredState.Running: "Running",
            DesiredState.Suspended: "Suspended",
            DesiredState.Stopped: "Stopped"
        ];

        Stdout.green.formatln("GetRange state: Machine = {}, Request = {}",
            machine_msg[this.state],
            request_msg[this.context.shared_working.desired_state]).default_colour;
    }
}
