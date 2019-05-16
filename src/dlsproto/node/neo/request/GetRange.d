/*******************************************************************************

    GetRange request protocol.

    Copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.node.neo.request.GetRange;

import ocean.util.log.Logger;
import ocean.core.VersionCheck;
import swarm.neo.node.IRequest;

/*******************************************************************************

    Module logger

*******************************************************************************/

static private Logger log;
static this ( )
{
    log = Log.lookup("dlsproto.node.neo.request.GetRange");
}

/*******************************************************************************

    v2 GetRange request protocol.

*******************************************************************************/

public abstract class GetRangeProtocol_v2: IRequest
{
    import dlsproto.node.neo.request.core.Mixins;
    import swarm.neo.connection.RequestOnConnBase;

    import swarm.util.RecordBatcher;
    import swarm.neo.node.RequestOnConn;
    import dlsproto.common.GetRange;
    public import dlsproto.client.request.GetRange: Filter;

    import core.stdc.time;

    import ocean.core.Enforce;
    import ocean.io.compress.Lzo;
    import ocean.transition;

    /***************************************************************************

        Mixin the initialiser and the connection and resources members.

    ***************************************************************************/

    mixin IRequestHandlerRequestCore!();

    /***************************************************************************

        The maximum number of records that should be added to the batch before
        yielding.

    ***************************************************************************/

    private static immutable uint yield_send_count = 10;

    /***************************************************************************

        Code used when resuming the fiber to interrupt waiting for I/O.

    ***************************************************************************/

    private static immutable NodeFiberResumeCode = 1;

    /***************************************************************************

        The minimum size of a batch of records. A batch is sent whenever its
        size if greater than this value after adding one record to the batch.

    ***************************************************************************/

    private static immutable size_t min_batch_length = 100_000;

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

        Called by the connection handler after the request code and version have
        been parsed from a message received over the connection, and the
        request-supported code sent in response.

        Note: the initial payload passed to this method is a slice of a buffer
        owned by the RequestOnConn. It is thus safe to assume that the contents
        of the buffer will not change over the lifetime of the request.

        Params:
            connection = request-on-conn in which the request handler is called
            resources = request resources acquirer
            init_payload = initial message payload read from the connection

    ***************************************************************************/

    void handle ( RequestOnConn connection, Object resources,
        Const!(void)[] init_payload )
    {
        this.initialise(connection, resources);

        cstring channel_name;
        cstring filter_string;
        time_t low, high;
        Filter.FilterMode filter_mode;

        bool initialised_ok;
        try
        {
            this.ed.message_parser.parseBody(init_payload, channel_name, low, high,
                    filter_string, filter_mode);

            if ( !this.prepareChannel(channel_name) ||
                 !this.prepareRange(low, high) ||
                 (filter_string.length > 0 && !this.prepareFilter(filter_mode, filter_string)))
            {
                return;
            }

            initialised_ok = true;
        }
        catch (Exception e)
        {
            log.error("{}", e.message);
        }

        if (!initialised_ok)
        {
            this.ed.send(
                ( RequestOnConnBase.EventDispatcher.Payload payload )
                {
                    payload.addCopy(RequestStatusCode.Error);
                }
            );
        }
        else
        {
            this.ed.send(
                ( RequestOnConnBase.EventDispatcher.Payload payload )
                {
                    payload.addCopy(RequestStatusCode.Started);
                }
            );

            this.value_buffer = this.resources.getVoidBuffer();
            this.batch_buffer = this.resources.getVoidBuffer();
            this.compressed_batch = this.resources.getVoidBuffer();
            this.saved_exception = this.resources.getException();
            this.lzo = this.resources.getLzo();

            try
            {
                this.run();
            }
            finally
            {
                this.requestFinished();
            }
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

            auto ed = this.ed;
            if (wait_for_data)
            {
                auto event = ed.nextEvent(ed.NextEventFlags.Receive | ed.NextEventFlags.Resume);

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
                        this.ed.shutdownWithProtocolError(
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
                ed.nextEvent(
                    ed.NextEventFlags.Receive,
                    (RequestOnConnBase.EventDispatcher.Payload payload)
                    {
                        payload.addCopy(MessageType_v2.Finished);
                    }
                );

                bool acked;
                do
                {
                    auto event = this.ed.nextEvent(
                            this.ed.NextEventFlags.Receive);

                    if (event.active.received)
                    {
                        MessageType_v2 msg_type;
                        this.ed.message_parser.parseBody(event.received.payload, msg_type);

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
        void fillInRecordsMessage ( RequestOnConnBase.EventDispatcher.Payload payload )
        {
            payload.addCopy(MessageType_v2.Records);
            payload.addCopy((*this.batch_buffer).length);
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
            this.ed.NextEventFlags.Receive, &fillInRecordsMessage
        );

        switch (event.active)
        {
            case event.active.sent:
                // deprecated, remove in next major
                static if (!hasFeaturesFrom!("swarm", 5, 1))
                {
                    this.ed.flush();
                }
                // Records sent: wait for Continue/Stop feedback, ACK Stop
                // stop and return true for Continue or false for stop
                switch (this.ed.receiveValue!(MessageType_v2)())
                {
                    case MessageType_v2.Continue:
                        return true;

                    case MessageType_v2.Stop:
                        this.sendStoppedMessage();
                        return false;

                    default:
                        this.ed.shutdownWithProtocolError(
                            "GetRange Expected Stopped or Continue message"
                        );
                }
                assert(false);

            case event.active.received:
                // Received message before the records have been sent:
                // It should be Stop. Acknowledge Stop and return false
                this.verifyReceivedMessageIsStop(event.received.payload);
                this.ed.send(&fillInRecordsMessage);
                this.sendStoppedMessage();
                return false;

            default:
                this.ed.shutdownWithProtocolError(
                    "GetRange: unexpected fiber resume code"
                );
        }

        assert(false);
    }


    /***************************************************************************

        Parses `msg_payload`, excepting the message type to be
        `MessageType_v2.Stop`, and raises a protocol error if it is not so.

        Params:
            msg_payload = the payload of the received message

    ***************************************************************************/

    private void verifyReceivedMessageIsStop ( in void[] msg_payload,
        istring file = __FILE__, int line = __LINE__ )
    {
        MessageType_v2 msg_type;
        this.ed.message_parser.parseBody(msg_payload, msg_type);

        if (msg_type != msg_type.Stop)
        {
            this.ed.shutdownWithProtocolError(
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
            (RequestOnConnBase.EventDispatcher.Payload payload)
            {
                payload.addCopy(MessageType_v2.Stopped);
            }
        );

        // deprecated, remove in next major
        static if (!hasFeaturesFrom!("swarm", 5, 1))
        {
            this.ed.flush();
        }
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
