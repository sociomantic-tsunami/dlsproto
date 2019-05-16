/******************************************************************************

    Fake DLS node Push request implementation.

    Copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved.

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

    Node implementation of the PutProtocol_v1.

*******************************************************************************/

public class PutImpl_v1: PutProtocol_v1
{
    import swarm.util.Hash;
    import fakedls.Storage;
    import core.stdc.time;

    /// Request code / version. Required by ConnectionHandler.
    static immutable Command command = Command(RequestCode.Put, 1);

    /// Request name for stats tracking. Required by ConnectionHandler.
    static immutable istring name = "Put";

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
        toHexString(timestamp, timestamp_buf);

        global_storage.get(channel).put(timestamp_buf.dup, cast(cstring)value);
        return true;
    }
}
