/*******************************************************************************

    Protocol base for DLS `PutBatch` request

    Copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.node.request.PutBatch;

/*******************************************************************************

    Imports

*******************************************************************************/

import dlsproto.node.request.model.SingleChannel;

import ocean.transition;

/*******************************************************************************

    PutBatch request protocol

*******************************************************************************/

public abstract scope class PutBatch : SingleChannel
{
    import dlsproto.node.request.model.DlsCommand;

    import swarm.util.RecordBatcher;
    import dlsproto.client.legacy.DlsConst;

    /***************************************************************************

        Used to read the records into.

    ***************************************************************************/

    private ubyte[]* compressed_batch;

    /***************************************************************************

        Constructor

        Params:
            reader = FiberSelectReader instance to use for read requests
            writer = FiberSelectWriter instance to use for write requests
            resources = object providing resource getters

    ***************************************************************************/

    public this ( FiberSelectReader reader, FiberSelectWriter writer,
        DlsCommand.Resources resources )
    {
        super(DlsConst.Command.E.PutBatch, reader, writer, resources);
        this.compressed_batch = this.resources.getPutBatchCompressBuffer();
    }

    /***************************************************************************

        Read batch of records to put into the channel

    ***************************************************************************/

    override protected void readChannelRequestData ( )
    {
        this.reader.readArray(*this.compressed_batch);
    }

    /***************************************************************************

        Params:
            channel_name = channel name for request that was read and validated
                earlier

    ***************************************************************************/

    final override protected void handleChannelRequest ( cstring channel_name )
    {
        auto decompressor = this.resources.getDecompressRecordBatch();
        decompressor.decompress(*this.compressed_batch);

        foreach ( key, value; decompressor )
        {
            if (!value.length)
            {
                this.writer.write(DlsConst.Status.E.EmptyValue);
                return;
            }

            if (!this.putRecord(channel_name, key, value))
            {
                this.writer.write(DlsConst.Status.E.Error);
                return;
            }
        }

        this.writer.write(DlsConst.Status.E.Ok);
    }

    /***************************************************************************

        Tries storing record in DLS and reports success status

        Params:
            channel = channel to write record to
            key = record key
            value = record value

        Returns:
            'true' if storing was successful

    ***************************************************************************/

    abstract protected bool putRecord ( cstring channel, cstring key, cstring value );
 }
