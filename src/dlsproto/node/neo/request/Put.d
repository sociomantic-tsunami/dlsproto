/*******************************************************************************

    Implementation of the neo Put v1 request.

    Copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.node.neo.request.Put;

import swarm.neo.node.IRequestHandler;

public abstract class PutProtocol_v1: IRequestHandler
{
    import dlsproto.node.neo.request.core.Mixins;
    import swarm.neo.connection.RequestOnConnBase;

    import swarm.neo.node.RequestOnConn;
    import dlsproto.common.Put;
    import ocean.transition;
    import core.stdc.time;

    /***************************************************************************

        Mixin the initialiser and the connection and resources members.

    ***************************************************************************/

    mixin IRequestHandlerRequestCore!();

    /// Response status code to send to client.
    private RequestStatusCode response;

    /**************************************************************************

        Request handler

        Params:
            msg_payload = initial message read from client to begin
                the request (the request code and version are
                assumed to be extracted)

    **************************************************************************/

    public void preSupportedCodeSent ( Const!(void)[] msg_payload )
    {
        // Extract the channel, record's timestamp and value from the
        // message payload
        cstring channel = this.ed.message_parser.getArray!(char)(msg_payload);
        time_t timestamp = *this.ed.message_parser.getValue!(time_t)(msg_payload);
        auto value = this.ed.message_parser.getArray!(char)(msg_payload);

        // Store the extracted data in StorageEngine
        if (this.prepareChannel(channel))
        {
            this.response = this.putInStorage(channel, timestamp, value)?
                RequestStatusCode.Put : RequestStatusCode.Error;
        }
        else
        {
            this.response = RequestStatusCode.Error;
        }
    }

    /***************************************************************************

        Called by the connection handler after the supported code has been sent
        back to the client.

    ***************************************************************************/

    public void postSupportedCodeSent ()
    {
        this.ed.send(
            ( RequestOnConnBase.EventDispatcher.Payload payload )
            {
                payload.addCopy(response);
            }
        );
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

