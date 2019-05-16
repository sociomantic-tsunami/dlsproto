/*******************************************************************************

    Implementation of the neo Put v1 request.

    Copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.node.neo.request.Put;

import swarm.neo.node.IRequest;

public abstract class PutProtocol_v1: IRequest
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

        cstring channel;
        time_t timestamp;
        Const!(char)[] value;
        this.ed.message_parser.parseBody(init_payload, channel, timestamp, value);

        // Store the extracted data in StorageEngine
        RequestStatusCode response;
        if (this.prepareChannel(channel))
            response = this.putInStorage(channel, timestamp, value)?
                RequestStatusCode.Put : RequestStatusCode.Error;
        else
            response = RequestStatusCode.Error;

        // Send the response code.
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

