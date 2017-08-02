/*******************************************************************************

    Implementation of the neo Put v0 request.

    Copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.node.neo.request.Put;

public abstract scope class PutProtocol_v0
{
    import swarm.neo.node.RequestOnConn;
    import dlsproto.common.Put;
    import ocean.transition;
    import core.stdc.time;

    /***************************************************************************

        Request-on-conn, to get the event dispatcher and control the fiber.

    ***************************************************************************/

    protected RequestOnConn connection;

    /**************************************************************************

        Request handler

        Params:
            connection = connection to the client
            msg_payload = initial message read from client to begin
                the request (the request code and version are
                assumed to be extracted)

    **************************************************************************/

    final public void handle ( RequestOnConn connection,
            Const!(void)[] msg_payload )
    {
        this.connection = connection;
        auto ed = this.connection.event_dispatcher;
        auto parser = ed.message_parser;

        // Extract the channel, record's timestamp and value from the
        // message payload
        cstring channel = parser.getArray!(char)(msg_payload);
        time_t timestamp = *parser.getValue!(time_t)(msg_payload);
        auto value = parser.getArray!(char)(msg_payload);

        // Store the extracted data in StorageEngine
        if (this.prepareChannel(channel))
        {
            if (!this.putInStorage(channel, timestamp, value))
            {
                ed.send(
                    ( ed.Payload payload )
                    {
                        payload.addConstant(RequestStatusCode.Error);
                    }
                );
            }

            ed.send(
                ( ed.Payload payload )
                {
                    payload.addConstant(RequestStatusCode.Put);
                }
            );
        }
        else
        {
            ed.send(
                ( ed.Payload payload )
                {
                    payload.addConstant(RequestStatusCode.Error);
                }
            );
        }
    }

    /**************************************************************************

        Ensures that requested channel exists / can be created and can be
        written to

        Params:
            channel_name = channel to check

        Returns:
            true if requested channel is available, false otherwise

    **************************************************************************/

    abstract protected bool prepareChannel ( cstring channel_name );


    /**************************************************************************

        Puts a record in the specified storage channel

        Params:
            channel_name = channel to push the value to
            timestamp = timestamp of the record
            value = value of the record

        Returns:
            true if the record was successfully put, false otherwise.

    **************************************************************************/

    abstract protected bool putInStorage ( cstring channel_name,
           time_t timestamp, in void[] value );
}

