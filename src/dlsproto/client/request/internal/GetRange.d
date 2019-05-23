/*******************************************************************************

    Client DLS GetRange v2 request handler.

    Copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.client.request.internal.GetRange;

/*******************************************************************************

    Imports

*******************************************************************************/

import core.stdc.time;

import ocean.transition;
import ocean.core.VersionCheck;
import ocean.core.Verify;
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

    /***************************************************************************

        Request core. Mixes in the types `NotificationInfo`, `Notifier`,
        `Params`, `Context` plus the static constants `request_type` and
        `request_code`.

    ***************************************************************************/

    mixin RequestCore!(RequestType.AllNodes, RequestCode.GetRange, 2,
        Args, SharedWorking, Notification);

    /***************************************************************************

        Request handler. Called from RequestOnConn.runHandler().

        Params:
            conn = connection event dispatcher
            context_blob = untyped chunk of data containing the serialized
                context of the request which is to be handled

    ***************************************************************************/

    public static void handler ( RequestOnConn.EventDispatcherAllNodes conn,
        void[] context_blob )
    {
        auto context = This.getContext(context_blob);

        auto shared_resources = SharedResources.fromObject(
            context.shared_resources);
        scope acquired_resources = shared_resources.new RequestResources;
        scope handler = new GetRangeHandler(conn, context, acquired_resources);

        handler.run();
    }

    /***************************************************************************

        Request finished notifier. Called from Request.handlingFinishedNotification().

        Params:
            context_blob = untyped chunk of data containing the serialized
                context of the request which is finishing

    ***************************************************************************/

    public static void all_finished_notifier ( void[] context_blob )
    {
        auto context = This.getContext(context_blob);

        Notification notification;

        if (context.shared_working.stopped)
        {
            // Stopped notification instead of the final one
            notification.stopped = NoInfo();
        }
        else
        {
            // Final notification, after the request has been finished
            notification.finished = NoInfo();
        }

        GetRange.notify(context.user_params, notification);
    }
}

/*******************************************************************************

    GetRange v2 handler class instantiated inside the main handler() function,
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

    /// Resumes the `RecordStream` fiber.
    private static immutable ubyte ResumeRecordStreamValue = ControllerSignal.max + 1;

    /// Signal to resume receiver on the Timeout
    private static immutable ubyte TimeoutSignal = ControllerSignal.max + 3;

    /// Request-on-conn event dispacher
    private RequestOnConn.EventDispatcherAllNodes conn;

    /// Request context
    private GetRange.Context* context;

    /// Request resource acquier.
    private SharedResources.RequestResources resources;

    /// Request event dispatcher
    private RequestEventDispatcher request_event_dispatcher;

    /// Indicator if the request started on this connection
    private bool request_started;

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
        this.request_event_dispatcher.initialise(&resources.getVoidBuffer);
    }

    /***************************************************************************

        Main request handling entry point.

    ***************************************************************************/

    public void run ( )
    {
        auto initialiser = createAllNodesRequestInitialiser!(GetRange)(
            this.conn, this.context, &this.fillInitialPayload);

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
        // if the request started already, don't restart upon reconnecting,
        // since GetRange is not (yet) capable of resuming from the interruption
        // point, leading to the duplicate data received by the client
        if (this.request_started)
            return false;

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

        Handler policy, called from AllNodesRequest template to run the
        request's main handling logic.

    **************************************************************************/

    private void handle ( )
    {
        switch (this.conn.receiveValue!(RequestStatusCode)())
        {
            case RequestStatusCode.Started:
                break;

            case RequestStatusCode.Error:
            default:
                GetRange.Notification n;
                n.node_error = NodeInfo(this.conn.remote_address);
                GetRange.notify(this.context.user_params, n);
                return;
        }

        this.request_started = true;

        scope record_stream = this.new RecordStream;
        scope reader = this.new Reader(record_stream);
        scope controller = this.new Controller(record_stream);

        this.request_event_dispatcher.eventLoop(this.conn);

        with (record_stream.fiber) verify(state == state.TERM);
        with (reader.fiber) verify(state == state.TERM);
        with (controller.fiber) verify(state == state.TERM);
    }

    /**************************************************************************

        Codes for signals sent across the fibers.

    **************************************************************************/

    enum FiberSignal: ubyte
    {
        /// Resumes the `RecordStream` fiber.
        ResumeRecordStream = ResumeRecordStreamValue,
        /// Tells the `Controller` to terminate.
        StopController
    }

    /// Timer to be timeout waiting for the Stopped message
    private SharedResources.RequestResources.ITimer timer;

    /// Seconds to timeout the Stopped signal after
    private static immutable int seconds_stop_timeout = 60;

    /**************************************************************************

        Sets the timer to stop this RoC, in case no message was received
        from the node for a long period of time.

    **************************************************************************/

    private void setStoppedMessageTimeout ()
    {
        this.timer = this.resources.getTimer(
                this.seconds_stop_timeout * 1000,
                &this.forceStopRequest);
        this.timer.start();
    }

    /**************************************************************************

        Raises the timeout signal which will in turn end the record stream,
        no matter the response from the node.

    **************************************************************************/

    private void forceStopRequest ()
    {
        this.request_event_dispatcher.signal(this.conn, TimeoutSignal);
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
        /// Double buffer structure
        private struct DoubleBuffer
        {
            /// Buffer that's filled with the data that the RecordStream can consume
            /// Once it's empty, it will swap itself with input buffer (see below).
            private void[]* output;

            /// Buffer that's being filled with the data by the Reader, while
            /// RecordStream consumes output buffer.
            private void[]* input;

            /*******************************************************************

                Initializes the double buffer

                Params:
                    getVoidBuffer = delegate to acquire the reusable buffer

            *******************************************************************/

            private void init (void[]* delegate() getVoidBuffer)
            {
                this.output = getVoidBuffer();
                this.input = getVoidBuffer();
            }

           /********************************************************************

                Swaps the front and back buffer

            *******************************************************************/

            private void swap ()
            {
                auto tmp = this.output;
                this.output = this.input;
                this.input = tmp;
            }

            /*******************************************************************

                Returns:
                     false if there are no records in any of the buffers

            *******************************************************************/

            private bool empty ()
            {
                return !this.output.length && !this.input.length;
            }

            /*******************************************************************

                Appends the input buffer with data.

                Params:
                    data = data to copy into input buffer

            *******************************************************************/

            private void append (in void[] data)
            {
                // append record_batch to *this.input, which may or may not
                // be empty.
                if (!(*input).length)
                    enableStomping(*input);

                // Note that the appending here is needed, since the node
                // will send the remaining batch if received the Stop message
                // during sending the batch.
                (*input) ~= data;
            }
        }

        /// Ditto
        private DoubleBuffer buffers;

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
            this.buffers.init(&this.outer.resources.getVoidBuffer);
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
            if (record_batch.length == 0)
            {
                throw this.outer.conn.shutdownWithProtocolError("Received empty batch from the node");
            }

            this.buffers.append(record_batch);

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
            if (this.outer.timer)
                this.outer.timer.cancel();

            this.stopped = true;
            if (this.fiber_suspended == fiber_suspended.WaitingForRecords ||
                this.fiber_suspended == fiber_suspended.RequestSuspended)
            {
                this.resumeFiber();
            }
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
                if (!this.stopped)
                {
                    this.outer.request_event_dispatcher.send(
                        this.fiber,
                        (conn.Payload payload)
                        {
                            payload.addCopy(MessageType_v2.Continue);
                        }
                    );

                    // deprecated, remove in the next major
                    static if (!hasFeaturesFrom!("swarm", 5, 1))
                    {
                        this.outer.conn.flush();
                    }
                }

                Const!(void)[] remaining_batch = *this.buffers.output;
                for (uint yield_count = 0; remaining_batch.length; yield_count++)
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
                            remaining_batch),
                        this.outer.conn.message_parser.getArray!(Const!(void))(
                            remaining_batch
                        ));

                }

                (*this.buffers.output).length = 0;

                if (this.stopped && this.buffers.empty())
                    break;
            }

            this.outer.request_event_dispatcher.signal(this.outer.conn,
                FiberSignal.StopController);
        }

        /**********************************************************************

            Suspends the fiber to be resumed by `addRecords` or `stop`.

            Returns:
                true if the fiber was resumed by `addRecords` or false if
                resumed by `stop` and there's no more records to iterate.

        **********************************************************************/

        private bool waitForRecords ()
        {
            // Wait for the next batch, unless we already have one.
            if (this.buffers.empty())
            {
                this.suspendFiber(FiberSuspended.WaitingForRecords);
            }

            // Grab the back buffer and move it to the front
            this.buffers.swap();

            return !this.stopped || !this.buffers.empty();
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
                auto event = this.outer.request_event_dispatcher.nextEvent(
                    this.fiber,
                    Message(MessageType_v2.Records),
                    Message(MessageType_v2.Stopped),
                    Message(MessageType_v2.Finished),
                    Signal(this.outer.TimeoutSignal));

                with (event.Active) switch (event.active)
                {
                    case message:
                        auto msg = event.message;
                        switch (msg.type)
                        {
                            case MessageType_v2.Records:
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

                            case MessageType_v2.Stopped:
                                this.record_stream.stop();
                                return;

                            case MessageType_v2.Finished:
                                finished = true;
                                break;

                            default:
                                assert(false);
                        }
                        break;
                    case signal:
                        verify(event.signal.code == this.outer.TimeoutSignal);
                        // Got a message from timer. Yield in order to allow it
                        // to unregister itself from epoll, before this RoC
                        // exits (the timer will be destroyed when this
                        // happens).
                        this.outer.request_event_dispatcher.yield(this.fiber);
                        this.record_stream.stop();
                        finished = true;
                        break;
                    default:
                        verify(false);
                        break;
                }
            }
            while (!finished);

            /// Ack finished
            this.outer.request_event_dispatcher.send(this.fiber,
                    (RequestOnConnBase.EventDispatcher.Payload payload)
                    {
                        payload.addCopy(MessageType_v2.Ack);
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
                                payload.addCopy(MessageType_v2.Stop);
                            }
                        );
                        // deprecated, remove in the next major
                        static if (!hasFeaturesFrom!("swarm", 5, 1))
                        {
                            this.outer.conn.flush();
                        }
                        this.outer.context.shared_working.stopped = true;

                        // set the timer to timeout if the node haven't responded
                        // with the Stopped message within a predefined period
                        this.outer.setStoppedMessageTimeout();
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
