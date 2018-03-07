/*******************************************************************************

    GetRange request protocol.

    Copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.node.neo.request.GetRange;

import ocean.util.log.Logger;

/*******************************************************************************

    Module logger

*******************************************************************************/

static private Logger log;
static this ( )
{
    log = Log.lookup("dlsproto.node.neo.request.GetRange");
}

/*******************************************************************************

    v1 GetRange request protocol.

*******************************************************************************/

public abstract scope class GetRangeProtocol_v1
{
    import dlsproto.node.neo.request.core.Mixins;

    import swarm.util.RecordBatcher;
    import swarm.neo.node.RequestOnConn;
    import dlsproto.common.GetRange;
    public import dlsproto.client.request.GetRange: Filter;

    import core.stdc.time;

    import ocean.core.Enforce;
    import ocean.io.compress.Lzo;
    import ocean.transition;

    /***************************************************************************

        Mixin the constructor and resources member.

    ***************************************************************************/

    mixin RequestCore!();


    /***************************************************************************

        The maximum number of records that should be added to the batch before
        yielding.

    ***************************************************************************/

    private const uint yield_send_count = 10;

    /***************************************************************************

        Code used when resuming the fiber to interrupt waiting for I/O.

    ***************************************************************************/

    private const NodeFiberResumeCode = 1;

    /***************************************************************************

        The minimum size of a batch of records. A batch is sent whenever its
        size if greater than this value after adding one record to the batch.

    ***************************************************************************/

    private const size_t min_batch_length = 100_000;

    /***************************************************************************

        Request-on-conn, to get the event dispatcher and control the fiber.

    ***************************************************************************/

    private RequestOnConn connection;

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

        Aquired buffer in which batch ready for compressing is stored

    ***************************************************************************/

    private void[]* batch_buffer;

    /***************************************************************************

        Acquired buffer in which compressed batch ready for sending is stored.

    ***************************************************************************/

    private void[]* compressed_batch;

    /***************************************************************************

        Exception to throw in case handler catches the exception. This is
        needed in order to avoid writing to the socket inside exception handler.
        Instead, we'll save the exception, send error message and then throw
        it.

    ***************************************************************************/

    private Exception saved_exception;

    /***************************************************************************

        LZO compressor, used for compressing the record batches before sending.

    ***************************************************************************/

    private Lzo lzo;

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
        // Send the finished code to the client to indicate end
        // of the request on the error
        void sendFinishedCode ()
        {
            this.ed.send(
                ( ed.Payload payload )
                {
                    payload.addConstant(MessageType_v1.Finished);
                }
            );
        }

        this.connection = connection;
        this.ed = connection.event_dispatcher;
        this.parser = this.ed.message_parser;

        cstring channel_name;
        cstring filter_string;
        time_t low, high;
        Filter.FilterMode filter_mode;

        this.ed.send(
            ( ed.Payload payload )
            {
                payload.addConstant(RequestStatusCode.Started);
            }
        );

        try
        {
            this.parser.parseBody(msg_payload, channel_name, low, high,
                    filter_string, filter_mode);

            if ( !this.prepareChannel(channel_name) ||
                 !this.prepareRange(low, high) ||
                 (filter_string.length > 0 && !this.prepareFilter(filter_mode, filter_string)))
            {
                sendFinishedCode();
                return;
            }

            this.value_buffer = this.resources.getVoidBuffer();
            this.batch_buffer = this.resources.getVoidBuffer();
            this.compressed_batch = this.resources.getVoidBuffer();
            this.saved_exception = this.resources.getException();
            this.lzo = this.resources.getLzo();

            this.run();
        }
        catch (Exception e)
        {
            log.error("{}", getMsg(e));
            sendFinishedCode();
        }
        finally
        {
            this.requestFinished();
        }
    }

    /***************************************************************************

        Request main loop. Gets records from the storage, sends them to the
        client in batches and handles Continue/Stop client feedback.

    ***************************************************************************/

    private void run ()
    {
        while (true)
        {
            time_t record_timestamp;

            bool wait_for_data;
            auto got_next = this.getNextRecord(record_timestamp, *this.value_buffer,
                    wait_for_data);

            if (wait_for_data)
            {
                auto event = this.ed.nextEvent(ed.NextEventFlags.Receive);

                switch (event.active)
                {
                    // Resumed by the storage engine, now ready to return value
                    // with `getNextRecord`, so we try again.
                    case event.active.resumed:
                        continue; // repeat the `getNextRecord` call.

                    // received from the client
                    case event.active.received:
                        // This should be stop. Acknowledge it exit
                        this.verifyReceivedMessageIsStop(event.received.payload);
                        this.sendStoppedMessage();
                        return;

                    default:
                        throw this.ed.shutdownWithProtocolError(
                            "GetRange: Unexpected fiber resume code");
                }
            }

            if(!got_next)
            {
                // No more data in the range. Ending request.

                // Send the data remaining in the batch, if any
                if ((*this.batch_buffer).length)
                {
                    this.sendBatchAndReceiveFeedback();
                }

                // Send finished message and wait on the ACK
                this.ed.nextEvent(
                    this.ed.NextEventFlags.Receive,
                    (ed.Payload payload)
                    {
                        payload.addConstant(MessageType_v1.Finished);
                    }
                );

                bool acked;
                do
                {
                    auto event = this.ed.nextEvent(
                            this.ed.NextEventFlags.Receive);

                    if (event.active.received)
                    {
                        MessageType_v1 msg_type;
                        this.parser.parseBody(event.received.payload, msg_type);

                        if (msg_type == msg_type.Ack)
                        {
                            acked = true;
                        }
                    }
                }
                while (!acked);

                return;
            }

            size_t record_length = (*this.value_buffer).length;
            (*this.batch_buffer) ~= (&record_timestamp)[0..1];
            (*this.batch_buffer) ~= (&record_length)[0..1];
            (*this.batch_buffer) ~= *this.value_buffer;

            if ((*this.batch_buffer).length >= min_batch_length)
            {
                if (!this.sendBatchAndReceiveFeedback())
                    return;
            }
        }
    }

    /***************************************************************************

        Sends the current batch of records; that is, `*this.record_batch` and
        waits for a `Continue` or `Stop` message. Clears `*this.record_batch`
        when finished.

        Returns:
            `true` if a `Continue` message or `false` if a `Stop` message has
            been received from the client.

    ***************************************************************************/

    private bool sendBatchAndReceiveFeedback ()
    {
        void fillInRecordsMessage ( ed.Payload payload )
        {
            payload.addConstant(MessageType_v1.Records);
            payload.addConstant((*this.batch_buffer).length);
            payload.addArray(*this.compressed_batch);
        }

        scope (exit)
        {
            (*this.batch_buffer).length = 0;
            enableStomping(*this.batch_buffer);
        }

        // Compress the batch
        (*this.compressed_batch).length =
            this.lzo.maxCompressedLength((*this.batch_buffer).length);
        enableStomping(*this.compressed_batch);

        auto compressed_size = this.lzo.compress(*this.batch_buffer,
                *this.compressed_batch);

        (*this.compressed_batch).length = compressed_size;
        enableStomping(*this.compressed_batch);


        // sends the records but be ready to potentially receive a Stop message.
        auto event = this.ed.nextEvent(
            ed.NextEventFlags.Receive, &fillInRecordsMessage
        );

        switch (event.active)
        {
            case event.active.sent:
                this.ed.flush();
                // Records sent: wait for Continue/Stop feedback, ACK Stop
                // stop and return true for Continue or false for stop
                switch (this.ed.receiveValue!(MessageType_v1)())
                {
                    case MessageType_v1.Continue:
                        return true;

                    case MessageType_v1.Stop:
                        this.sendStoppedMessage();
                        return false;

                    default:
                        throw this.ed.shutdownWithProtocolError(
                            "GetRange Expected Stopped or Continue message"
                        );
                }

            case event.active.received:
                // Received message before the records have been sent:
                // It should be Stop. Acknowledge Stop and return false
                this.verifyReceivedMessageIsStop(event.received.payload);
                this.ed.send(&fillInRecordsMessage);
                this.sendStoppedMessage();
                return false;

            default:
                throw this.ed.shutdownWithProtocolError(
                    "GetRange: unexpected fiber resume code"
                );

        }
    }


    /***************************************************************************

        Parses `msg_payload`, excepting the message type to be
        `MessageType_v1.Stop`, and raises a protocol error if it is not so.

        Params:
            msg_payload = the payload of the received message

    ***************************************************************************/

    private void verifyReceivedMessageIsStop ( in void[] msg_payload,
        istring file = __FILE__, int line = __LINE__ )
    {
        MessageType_v1 msg_type;
        this.parser.parseBody(msg_payload, msg_type);

        if (msg_type != msg_type.Stop)
        {
            throw this.ed.shutdownWithProtocolError(
                "GetRange: Message received from the client is not Stop as expected",
                file, line
            );
        }
    }


    /***************************************************************************

        Sends `Stopped` message

    ***************************************************************************/

    private void sendStoppedMessage ()
    {
        this.ed.send(
            (ed.Payload payload)
            {
                payload.addConstant(MessageType_v1.Stopped);
            }
        );

        this.ed.flush();
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
            `true` if there was a record in the channel to iterate over,
            false if the iteration has been completed.

    ***************************************************************************/

    abstract protected bool getNextRecord ( out time_t timestamp, ref void[] value,
            out bool wait_for_data);

    /***************************************************************************

        Notifies the request when a record has been added to the channel being
        read from. The implementing class must call this when notified by
        the storage engine of new data arriving.

    ***************************************************************************/

    final protected void dataReady ( )
    {
        this.connection.resumeFiber(NodeFiberResumeCode);
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

        Called to indicate to the storage engine that this request is about to
        be stopped. Could be used to cancel all background processes allocated
        for this request.

    ***************************************************************************/

    protected void requestFinished ()
    {
    }
}

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

    private const NodeFiberResumeCode = 1;

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
                throw this.ed.shutdownWithProtocolError("invalid start state");
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
                    &this.stateFromMessageType_v0,
                    MessageType_v0.Ack,
                    MessageType_v0.Record);
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
                    payload.addConstant(MessageType_v0.Error);
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
                                    &this.stateFromMessageType_v0,
                                    MessageType_v0.Ack));
    }

    /***************************************************************************

        WaitForData state: Wait until node fetches the required data to continue
        the request.

    ***************************************************************************/

    private State stateWaitingForData ( )
    {
        return this.nextState(this.request.waitForData(
                                    &this.stateFromMessageType_v0,
                                    NodeFiberResumeCode,
                                    MessageType_v0.Ack));
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
            MessageType_v0 msg_type;
            this.parser.parseBody(received, msg_type);
            return msg_type == MessageType_v0.Ack;
        }

        this.requestFinished();
        return this.nextState(this.request.finished(&is_ack,
                    MessageType_v0.Finished));
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

    private SuspendableRequest.ReceivedMessageAction stateFromMessageType_v0 ( Const!(void)[] received )
    {
        MessageType_v0 msg_type;
        this.parser.parseBody(received, msg_type);

        switch (msg_type)
        {
            case MessageType_v0.Suspend:
                return SuspendableRequest.ReceivedMessageAction.Suspend;

            case MessageType_v0.Resume:
                return SuspendableRequest.ReceivedMessageAction.Resume;

            case MessageType_v0.Stop:
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
