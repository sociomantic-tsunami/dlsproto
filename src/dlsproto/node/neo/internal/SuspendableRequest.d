/*******************************************************************************

    Helper struct for node-side suspendable requests which behave as follows:
        * Operate on a single channel.
        * Send a stream of data, broken down into individual messages.
        * The stream can be suspended, resumed, and stopped by the client. These
          actions are known as "state changes".
        * While a state change is in progress, the client is unable to request
          another state change.
        * All state changes are acknowledged by the node by sending a special
          ACK message to the client.

    The struct handles all state change logic and communication with the client.
    Request specifics such as codes and messages sent back and forth between the
    client and node are left deliberately abstract and must be provided by the
    request implementation which uses this helper.

    Copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.node.neo.internal.SuspendableRequest;

/// ditto
public struct SuspendableRequest
{
    import ocean.transition;
    import swarm.neo.node.RequestOnConn;
    import swarm.neo.connection.RequestOnConnBase;

    /***************************************************************************

        Enum defining the states in which the handler of a suspendable request
        may be.

    ***************************************************************************/

    public enum State
    {
        /// Sending data to the client
        Sending,

        /// Suspended, waiting for the client to send a message to resume the
        /// request
        Suspended,

        /// Waiting for more data to be available for sending
        WaitingForData,

        /// Request finished
        Exit,

        /// Notifying the client that request has finished, and wait ACK from them
        Finished
    }


    /***************************************************************************

        Enum defining the actions which a message received from the client may
        trigger.

    ***************************************************************************/

    public enum ReceivedMessageAction
    {
        /// The received message was of an unknown type
        Undefined,

        /// The received message indicates that the request should be suspended
        Suspend,

        /// The received message indicates that the request should be resumed
        Resume,

        /// The received message indicates that the request should end
        Exit
    }


    /****************************************************************************

        Indicator returned from the node describing if the request should
        continue fetching records from the node, suspend waiting more data
        from the node or if it should finish.

    *****************************************************************************/

    public enum IterateResult
    {
        /// Iteration should continue
        Continue,
        /// Request should suspend until storage gets more data
        WaitForData,
        /// There's no more data in the storage engine, request should finish
        Finished
    }


    /***************************************************************************

        Event dispatcher for this request-on-conn.

    ***************************************************************************/

    private RequestOnConn.EventDispatcher conn;

    /***************************************************************************

        The maximum number of records that should be sent in a row before
        yielding.

    ***************************************************************************/

    private const uint yield_send_count = 10;

    /***************************************************************************

        Flag set when the fiber is suspended in waitForData() waiting for either
        a message to be received from the client or data to be ready to send.
        When this flag is true, a call to dataReady() will resume the waiting
        fiber.

    ***************************************************************************/

    private bool fiber_suspended_waiting_for_data;

    /***************************************************************************

        Alias for a delegate which handles a received message and returns a
        decision on what action need be taken.

    ***************************************************************************/

    private alias ReceivedMessageAction delegate ( Const!(void)[] received )
        ReceivedMessageDg;

    /***************************************************************************

        Initialises this instance.

        Params:
            conn = event dispatcher for this request-on-conn

    ***************************************************************************/

    public void initialise ( RequestOnConn.EventDispatcher conn )
    in
    {
        assert(conn !is null);
    }
    body
    {
        this.conn = conn;
    }

    /***************************************************************************

        Indicates that data is now ready to send. If the suspendable request is
        in the state of waiting for data to be ready (see waitForData()), the
        fiber will be resumed, triggering a state change to state Sending.

    ***************************************************************************/

    public void dataReady ( RequestOnConn connection, uint data_ready_code )
    {
        if ( this.fiber_suspended_waiting_for_data )
            connection.resumeFiber(data_ready_code);
    }

    /***************************************************************************

        If the channel being processed is removed, we have to end the request
        and send a special message to the client, informing them of this. This
        method sends the specified "channel removed" message to the client,
        ignoring messages received from the client in the meantime (the request
        is ending, so we don't care about control messages from the client). The
        fiber should not be resumed by data ready events.

        Params:
            channel_removed_msg = code to send to client to inform it that the
                channel has been removed

    ***************************************************************************/

    public void sendChannelRemoved ( ubyte channel_removed_msg )
    {
        bool send_interrupted;

        do
        {
            send_interrupted = false;

            auto event = this.conn.nextEvent(this.conn.NextEventFlags.Receive,
                ( conn.Payload payload )
                {
                    payload.add(channel_removed_msg);
                }
            );
            assert (event.active == event.active.received ||
                    event.active == event.active.sent);

            // All messages from the client are ignored (see the method docs).
            if (event.active == event.active.received)
            {
                send_interrupted = true;
            }

        }
        while ( send_interrupted );
    }

    /***************************************************************************

        Sends data (returned by the provided delegate) to the client, until the
        delegate specifies that the loop should end. Every this.yield_send_count
        records, the fiber yields, allowing other requests to be handled.
        Messages received by the client, while sending data or yielding, are
        handled by a second delegate.

        Params:
            iterate = delegate which returns a data item to be sent or
                null, if there is currently nothing to send. In addition it tells,
                via an out argument, whether the sending should continue, end,
                or whether the sending should be paused while the node fetches
                required data (Note that, if the delegate returns an item *and*
                specifies that the sending should end, the returned item is sent
                first, before exiting this method.)
            handle_received_message = delegate to which received messages are
                passed. The return value determines whether the request
                continues sending, suspends, or exits.
            ack = code to send to the client indicating ACK
            data_msg = code to send to the client indicating a data message.
                This code is sent at the start of a message payload, followed by
                a data item

        Returns:
            the next state to enter (normally WaitingForData; may be Suspended
            or Exit)

        Throws:
            ProtocolError, if handle_received_message returns Undefined (it's
            important that the caller does not swallow this exception)

    ***************************************************************************/

    public State sendData (
        Const!(void)[] delegate ( out IterateResult iterate_result ) iterate,
        ReceivedMessageDg handle_received_message, ubyte ack, ubyte data_msg )
    {
        uint records_sent_without_yielding = 0;

        bool keep_going;
        do
        {
            // Call the iteration delegate, to determine whether another item is
            // ready to send and/or whether we should exit the loop.
            Const!(void)[] data;
            IterateResult iterate_result;

            data = iterate(iterate_result);
            with (IterateResult) switch (iterate_result)
            {
                case Continue:
                    keep_going = true;
                    break;
                case WaitForData:
                    return State.WaitingForData;
                case Finished:
                    keep_going = false;
                    break;
                default:
                  assert(false);
            }

            // Send current data item, if necessary
            ReceivedMessageAction msg_action;

            if ( data !is null )
            {
                auto event = this.conn.nextEvent(this.conn.NextEventFlags.Receive,
                    ( conn.Payload payload )
                    {
                        payload.add(data_msg);
                        payload.addArray(data);
                    }
                );
                assert(event.active == event.active.received ||
                       event.active == event.active.sent);

                // Handle messages received while sending.
                if ( event.active == event.active.received )
                {
                    msg_action = handle_received_message(event.received.payload);
                    // sendReceive() was interrupted while sending, so send again.
                    // The client should not send any message until it has received
                    // the Ack.
                    this.conn.send(
                        ( conn.Payload payload )
                        {
                            payload.addConstant(data_msg);
                            payload.addArray(data);
                        }
                    );

                    // Ack the received control message.
                    this.sendAck(ack);

                    with ( ReceivedMessageAction ) switch ( msg_action )
                    {
                        case Suspend:
                            return State.Suspended;

                        case Exit:
                            return State.Exit;

                        case Resume: // Meaningless but harmless
                            break;

                        default:
                            throw this.conn.shutdownWithProtocolError(
                                "Unexpected control message while sending data");
                    }
                }

                records_sent_without_yielding++;
            }

            // Yield after iterating some data items.
            if (records_sent_without_yielding >= this.yield_send_count)
            {
                auto event = this.conn.nextEvent(
                        this.conn.NextEventFlags.Receive | this.conn.NextEventFlags.Yield);
                assert(event.active == event.active.received ||
                       event.active == event.active.yielded_resumed);

                // Handle messages received while yielding.
                if ( event.active == event.active.received )
                {
                    msg_action = handle_received_message(event.received.payload);

                    // Ack the received control message.
                    this.sendAck(ack);

                    with ( ReceivedMessageAction ) switch ( msg_action )
                    {
                        case Suspend:
                            return State.Suspended;

                        case Exit:
                            return State.Exit;

                        case Resume: // Meaningless but harmless
                            break;

                        default:
                            throw this.conn.shutdownWithProtocolError(
                                "Unexpected control message while yielded");
                    }
                }

                records_sent_without_yielding = 0;
            }
        }
        while ( keep_going );

        return State.Finished;
    }

    /***************************************************************************

        Waits for control messages from the client, until a state change (either
        resume or exit) is requested.

        Params:
            handle_received_message = delegate to which received messages are
                passed. The return value determines whether the request remains
                suspended, resumes, or exits.
            ack = code to send to the client indicating ACK

        Returns:
            the next state to enter (normally Sending; may be Exit)

        Throws:
            ProtocolError, if handle_received_message returns Undefined (it's
            important that the caller does not swallow this exception)

    ***************************************************************************/

    // The request is suspended waiting for a state change
    public State waitForControlMessage ( ReceivedMessageDg handle_received_message,
        ubyte ack )
    {
        ReceivedMessageAction msg_action;
        do
        {
            auto event = this.conn.nextEvent(this.conn.NextEventFlags.Receive);
            assert (event.active == event.active.received,
                "Unexpected fiber resume code when waiting for control message");

            msg_action = handle_received_message(event.received.payload);

            // Ack the received control message.
            this.sendAck(ack);

        }
        while ( msg_action == ReceivedMessageAction.Suspend );

        with ( ReceivedMessageAction ) switch ( msg_action )
        {
            case Suspend:
                assert(false); // Should not have exited the while loop

            case Exit:
                return State.Exit;

            case Resume:
                return State.Sending;

            default:
                throw this.conn.shutdownWithProtocolError(
                    "Unexpected control message while waiting for control message");
        }
        assert(false);
    }

    /***************************************************************************

        Sends the Finished notification to the client and waits its
        acknowledgment. While sending the Finished message, any messages
        received from the client are ignored. Once the Finished message has
        been sent, messages from the client are passed to the provided delegate
        to determine what to do.

        Params:
            is_ack = delegate to parse the received message and confirm if
                     it's the Ack message.
            finished = code to send to the client indicating end of the request

    ***************************************************************************/

    public State finished ( bool delegate (in void[] received) is_ack,
            ubyte finished )
    {
        // Send finished
        bool send_interrupted;
        do
        {
            send_interrupted = false;

            auto event = this.conn.nextEvent(this.conn.NextEventFlags.Receive,
                ( conn.Payload payload )
                {
                    payload.addConstant(finished);
                }
            );
            assert(event.active == event.active.received ||
                   event.active == event.active.sent);

            if (event.active == event.active.received)
            {
                send_interrupted = true;
            }
        }
        while (send_interrupted);

        // wait for ack
        bool ack_received;
        do
        {
            auto event = this.conn.nextEvent(this.conn.NextEventFlags.Receive);
            assert(event.active == event.active.received,
                    "Fiber unexpectedly resumed");

            ack_received = is_ack(event.received.payload);
        }
        while ( !ack_received );

        return State.Exit;
    }



    /***************************************************************************

        Waits for data to be ready to send or a control message to arrive from
        the client.

        Params:
            handle_received_message = delegate to which received messages are
                passed. The return value determines whether the request
                continues waiting, suspends, or exits.
            data_ready_code = resume code expected when the fiber is resumed
                because data is now ready
            ack = code to send to the client indicating ACK

        Returns:
            the next state to enter (normally Sending; may be Suspended or Exit)

        Throws:
            ProtocolError, if handle_received_message returns Undefined (it's
            important that the caller does not swallow this exception)

    ***************************************************************************/

    public State waitForData ( ReceivedMessageDg handle_received_message,
        uint data_ready_code, ubyte ack )
    {
        ReceivedMessageAction msg_action;
        do
        {
            RequestOnConnBase.EventDispatcher.EventNotification event;

            this.fiber_suspended_waiting_for_data = true;
            try
            {
                event =  this.conn.nextEvent(this.conn.NextEventFlags.Receive);
            }
            finally
            {
                this.fiber_suspended_waiting_for_data = false;
            }

            with (event.Active) switch (event.active)
            {
                // positive code => user code => must be data ready
                case resumed:
                    assert(event.resumed.code == data_ready_code,
                        "Unexpected fiber resume code when waiting for data");
                    return State.Sending;

                // If it's not the explicit resume call, a control message must be
                case received:
                    // Parse the message
                    msg_action = handle_received_message(event.received.payload);
                    // Ack the received control message.
                    this.sendAck(ack);
                    break;

                default:
                    assert(false, "Unexpected resume reason.");
            }
        }
        while ( msg_action == ReceivedMessageAction.Resume );

        with ( ReceivedMessageAction ) switch ( msg_action )
        {
            case Suspend:
                return State.Suspended;

            case Exit:
                return State.Exit;

            case Resume:
                assert(false); // Should have not exited the while loop

            default:
                throw this.conn.shutdownWithProtocolError(
                    "Unexpected control message while waiting for data");
        }
        assert(false);
    }

    /***************************************************************************

        Sends an `Ack` message to the client. The client is expected to not send
        a message in the mean time or a protocol error is raised.

        Params:
            ack = code to send to the client indicating ACK

    ***************************************************************************/

    private void sendAck ( ubyte ack )
    {
        this.conn.send(
            ( conn.Payload payload )
            {
                payload.add(ack);
            }
        );
    }
}
