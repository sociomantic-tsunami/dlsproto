/******************************************************************************

    Fake DLS node Push request implementation.

    Copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedls.neo.request.Put;

import dlsproto.node.neo.request.Put;

import swarm.neo.node.RequestOnConn;
import swarm.neo.request.Command;
import dlsproto.common.RequestCodes;

import ocean.transition;

/*******************************************************************************

    The request handler for the table of handlers. When called, runs in a fiber
    that can be controlled via `connection`.

    Params:
        shared_resources = an opaque object containing resources owned by the
            node which are required by the request
        connection  = performs connection socket I/O and manages the fiber
        cmdver      = the version number of the Put command as specified by
                      the client
        msg_payload = the payload of the first message of this request

*******************************************************************************/

public void handle ( Object shared_resources, RequestOnConn connection,
        Command.Version cmdver, Const!(void)[] msg_payload )
{
    switch (cmdver)
    {
        case 0:
            scope rq = new PutImpl_v0;
            rq.handle(connection, msg_payload);
            break;

        default:
            auto ed = connection.event_dispatcher;
            ed.send(
                    ( ed.Payload payload )
                    {
                        payload.addConstant(GlobalStatusCode.RequestVersionNotSupported);
                    }
            );
            break;
    }
}

/*******************************************************************************

    Node implementation of the PutProtocol_v0.

*******************************************************************************/

private scope class PutImpl_v0: PutProtocol_v0
{
    import swarm.util.Hash;
    import fakedls.Storage;
    import core.stdc.time;


    /***************************************************************************

        Create/get the channel to put the record to.

        Params:
            channel_name = name of channel to be prepared

        Return:
            `true` if it is possible to proceed with request

    ***************************************************************************/

    override protected bool prepareChannel ( cstring channel_name )
    {
        return global_storage.getCreate(channel_name) !is null;
    }

    /***************************************************************************

        Tries storing record in DLS and reports success status

        Params:
            channel = channel to write record to
            timestamp = record's timestamp
            value = record value

        Returns:
            'true' if storing was successful

    ***************************************************************************/

    override protected bool putInStorage ( cstring channel, time_t timestamp, in void[] value )
    {
        char[HexDigest.length] timestamp_buf;
        Hash.toHexString(timestamp, timestamp_buf);

        global_storage.get(channel).put(timestamp_buf.dup, cast(cstring)value);
        return true;
    }
}
