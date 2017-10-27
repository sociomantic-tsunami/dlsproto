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

private scope class Handler
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
