/*******************************************************************************

    GetRange request protocol.

    Copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.node.neo.request.GetRange;

/*******************************************************************************

    v0 GetRange request protocol.

*******************************************************************************/

public abstract scope class GetRangeProtocol_v0
{
    import dlsproto.node.neo.request.core.Mixins;

    import swarm.util.RecordBatcher;
    import swarm.neo.node.RequestOnConn;
    import swarm.neo.util.StateMachine;
    import dlsproto.common.GetRange;
    import dlsproto.client.request.GetRange;

    import core.stdc.time;

    import ocean.core.Enforce;
    import ocean.transition;

    import dlsproto.node.neo.internal.SuspendableRequest;

    /***************************************************************************

        The protocol of this request is implemented as a state machine, with
        messages from the client and changes in the storage engine triggering
        transitions between stats.

        This mixin provides the core state machine functionality and defines the
        states which the request may be in. (The logic for each state is
        implemented in the method of the corresponding name.)

    ***************************************************************************/

    mixin(genStateMachine([
        "Sending",
        "Suspended",
        "WaitingForData",
        "Finished"
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
            case Sending:
                return State.Sending;
            case Suspended:
                return State.Suspended;
            case Finished:
                return State.Finished;
            case WaitingForData:
                return State.WaitingForData;
            case Exit:
                return State.Exit;
            default:
                assert(false);
        }
    }

    /***************************************************************************

        Mixin the constructor and resources member.

    ***************************************************************************/

    mixin RequestCore!();

    /***************************************************************************

        Code used when resuming the fiber to interrupt waiting for I/O.

    ***************************************************************************/

    private static immutable NodeFiberResumeCode = 1;

    /***************************************************************************

        Suspendable request helper struct.

    ***************************************************************************/

    private SuspendableRequest request;

    /***************************************************************************

        Indicator if at least one record is in the batch, pending to be sent.

    ***************************************************************************/

    private bool data_pending_in_batch;

    /***************************************************************************

        Request-on-conn event dispatcher, to send and receive messages.

    ***************************************************************************/

    private RequestOnConn.EventDispatcher ed;

    /***************************************************************************

        Message parser

    ***************************************************************************/

    private RequestOnConn.EventDispatcher.MessageParser parser;

    /***************************************************************************

        Aquired buffer in which values read are stored.

    ***************************************************************************/

    private void[]* value_buffer;

    /***************************************************************************

        Aquired buffer in which compressed batch ready for sending is stored

    ***************************************************************************/

    private ubyte[]* batch_buffer;

    /***************************************************************************

        Aquired RecordBatcher instance.

    ***************************************************************************/

    private RecordBatcher batcher;

    /***************************************************************************

        Exception to throw in case handler catches the exception. This is
        needed in order to avoid writing to the socket inside exception handler.
        Instead, we'll save the exception, send error message and then throw
        it.

    ***************************************************************************/

    private Exception saved_exception;

    /***************************************************************************

        Request-on-conn, to get the event dispatcher and control the fiber.

    ***************************************************************************/

    protected RequestOnConn connection;

    /***************************************************************************

        Request handler. Reads the initial request args and starts the state
        machine.

        Params:
            connection = connection to client
            msg_payload = initial message read from client to begin the request
                (the request code and version are assumed to be extracted)

    ***************************************************************************/

    final public void handle ( RequestOnConn connection, Const!(void)[] msg_payload )
    {
        this.connection = connection;
        this.ed = connection.event_dispatcher;
        this.parser = this.ed.message_parser;
        this.request.initialise(this.ed);

        cstring channel_name;
        cstring filter_string;
        time_t low, high;
        StartState start_state;
        Filter.FilterMode filter_mode;

        this.parser.parseBody(msg_payload, channel_name, low, high,
                filter_string, filter_mode,
                start_state);

        State state;
        switch ( start_state )
        {
            case StartState.Running:
                state = state.Sending;
                break;
            case StartState.Suspended:
                state = state.Suspended;
                break;
            default:
                this.ed.shutdownWithProtocolError("invalid start state");
        }

        if ( !this.prepareChannel(channel_name) )
        {
            this.ed.send(
                ( ed.Payload payload )
                {
                    payload.addConstant(RequestStatusCode.Error);
                }
            );
            return;
        }

        if ( !this.prepareRange(low, high) )
        {
            this.ed.send(
                ( ed.Payload payload )
                {
                    payload.addConstant(RequestStatusCode.Error);
                }
            );
            return;
        }

        if (filter_string.length > 0)
        {
            if ( !this.prepareFilter(filter_mode, filter_string) )
            {
                this.ed.send(
                    ( ed.Payload payload )
                    {
                        payload.addConstant(RequestStatusCode.Error);
                    }
                );
                return;
            }
        }

        this.ed.send(
            ( ed.Payload payload )
            {
                payload.addConstant(RequestStatusCode.Started);
            }
        );

        this.value_buffer = this.resources.getVoidBuffer();
        this.batch_buffer = cast(ubyte[]*)this.resources.getVoidBuffer();
        this.saved_exception = this.resources.getException();
        this.batcher = this.resources.getRecordBatcher();

        this.run(state);
    }

    /***************************************************************************

        Performs any logic needed to start reading data from the channel of the
        given name.

        Params:
            channel_name = channel to execute request on

        Returns:
            `true` if the channel may be used

    ***************************************************************************/

    abstract protected bool prepareChannel ( cstring channel_name );

    /***************************************************************************

        Performs any logic needed to start reading records in the given
        range.

        Params:
            low = lower range boundary
            high = higher range boundary

        Returns:
            `true` if the range preparation was sucessfull

    ***************************************************************************/

    abstract protected bool prepareRange ( time_t low, time_t high );


    /***************************************************************************

        Retrieve the next record from the channel, if available.

        Params:
            timestamp = variable to write the record's timestamp into
            value = buffer to write the value into
            wait_for_data = out parameter, will be set if the request should
                            suspend until more data has arrived.

        Returns:
            `true` if there was a record in the channel, false if the channel is
            empty

    ***************************************************************************/

    abstract protected bool getNextRecord ( out time_t timestamp, ref void[] value,
            out bool wait_for_data);

    /***************************************************************************

        Notifies the request when a record has been added to the channel being
        consumed from. The implementing class must call this when notified by
        the storage engine of new data arriving.

    ***************************************************************************/

    final protected void dataReady ( )
    {
        this.request.dataReady(this.connection, NodeFiberResumeCode);
    }

    /***************************************************************************

        Iterates through the prepared range of records, pack the records,
        and returns batches to be sent.

        Params:
            iterate_result = iteration result indicating if the request should
                             suspend, continue, or finish.

        Returns:
            data to be sent, or null if there's yet nothing to be sent.

    ***************************************************************************/

    private Const!(void)[] iterate (out SuspendableRequest.IterateResult iterate_result )
    {
        time_t record_timestamp;

        bool wait_for_data;
        auto got_next = this.getNextRecord(record_timestamp, *this.value_buffer,
                wait_for_data);

        if (wait_for_data)
        {
            iterate_result = SuspendableRequest.IterateResult.WaitForData;
            return null;
        }

        if(!got_next)
        {
            iterate_result = SuspendableRequest.IterateResult.Finished;

            // If there are no more records, and nothing in batch, just exit.
            if (!this.data_pending_in_batch)
            {
                return null;
            }
            else
            {
                // We need to send the remaining batch data.
                this.data_pending_in_batch = false;
                this.batcher.compress(*this.batch_buffer);
                return cast(void[])(*this.batch_buffer);
            }
        }

        iterate_result = SuspendableRequest.IterateResult.Continue;
        auto add_result = this.batcher.add(cast(cstring)((&record_timestamp)[0 .. 1]),
                cast(cstring)(*this.value_buffer));

        switch (add_result) with (RecordBatcher.AddResult)
        {
            case Added:
                // all good, can add more data to batch, so don't send immediately
                this.data_pending_in_batch = true;
                return null;
            case BatchFull:
                this.batcher.compress(*this.batch_buffer);
                // Add the record to the empty batch
                add_result = this.batcher.add(cast(cstring)((&record_timestamp)[0 .. 1]),
                        cast(cstring)(*this.value_buffer));
                assert (add_result == Added);
                return cast(void[])(*this.batch_buffer);
            default:
                enforce(false, "Iterated record too big to fit into batch.");
                break;
        }

        assert (false); // this function should exit in the switch above.
    }


    /***************************************************************************

        Sending state: Get records from storage engine and send them to the
        client.

    ***************************************************************************/

    private State stateSending ( )
    {
        bool error = false;

        // State loop
        SuspendableRequest.State state;
        try
        {
            state = this.request.sendData(&iterate,
                    &this.stateFromMessageType,
                    MessageType.Ack,
                    MessageType.Record);
        }
        catch (Exception e)
        {
            this.saved_exception = e;
            error = true;
        }

        if (error)
        {
            // Inform the client about the error
            this.ed.send(
                ( ed.Payload payload )
                {
                    payload.addConstant(MessageType.Error);
                }
            );

            // Propagate error forward to connection handler
            throw this.saved_exception;
        }

        return this.nextState(state);
    }

    /***************************************************************************

        Suspended state: Wait until the client resumes or stops the request.

    ***************************************************************************/

    private State stateSuspended ( )
    {
        this.requestSuspended();
        return this.nextState(this.request.waitForControlMessage(
                                    &this.stateFromMessageType,
                                    MessageType.Ack));
    }

    /***************************************************************************

        WaitForData state: Wait until node fetches the required data to continue
        the request.

    ***************************************************************************/

    private State stateWaitingForData ( )
    {
        return this.nextState(this.request.waitForData(
                                    &this.stateFromMessageType,
                                    NodeFiberResumeCode,
                                    MessageType.Ack));
    }

    /***************************************************************************

        Finished state: Sent Finished notification to client, and wait for ACK.

    ***************************************************************************/

    private State stateFinished ( )
    {
        /**********************************************************************

            Parses the received messages and confirms if it's the Ack.

            Params:
                received = received message

            Returns:
                true if the received message is Ack, false otherwise.

        **********************************************************************/

        bool is_ack (in void[] received)
        {
            MessageType msg_type;
            this.parser.parseBody(received, msg_type);
            return msg_type == MessageType.Ack;
        }

        this.requestFinished();
        return this.nextState(this.request.finished(&is_ack,
                    MessageType.Finished));
    }

    /***************************************************************************

        Translates `msg_type`, which has been received from the client, into the
        corresponding state. Shuts the connection down if `msg_type` is not a
        control message type.

        Params:
            received = data containing message received from the client where the
                       client is expected to send a control message

        Returns:
            the state to change to according to `msg_type`.

        Throws:
            `ProtocolError` if `msg_type` is not a control message type.

    ***************************************************************************/

    private SuspendableRequest.ReceivedMessageAction stateFromMessageType ( Const!(void)[] received )
    {
        MessageType msg_type;
        this.parser.parseBody(received, msg_type);

        switch (msg_type)
        {
            case MessageType.Suspend:
                return SuspendableRequest.ReceivedMessageAction.Suspend;

            case MessageType.Resume:
                return SuspendableRequest.ReceivedMessageAction.Resume;

            case MessageType.Stop:
                return SuspendableRequest.ReceivedMessageAction.Exit;

            default:
                throw this.ed.shutdownWithProtocolError(
                    "GetRange: expected a control message from the client");
        }
    }

    /***************************************************************************

        Allows request to process read filter string into more efficient form
        and save it before starting actual record iteration.

        Params:
            mode = filter mode
            filter = filter string

        Returns:
            true if preparing filter is successful, false otherwise

    ***************************************************************************/

    abstract protected bool prepareFilter ( Filter.FilterMode mode,
        cstring filter );

    /***************************************************************************

        Called to indicate to the storage engine that this request is going to
        be suspended. Could be used to cancel all background processes allocated
        for this request.

    ***************************************************************************/

    protected void requestSuspended ()
    {
    }

    /***************************************************************************

        Called to indicate to the storage engine that this request is about to
        be stopped. Could be used to cancel all background processes allocated
        for this request.

    ***************************************************************************/

    protected void requestFinished ()
    {
    }
}
