/*******************************************************************************

    Protocol base for DLS `Put` request

    Copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.node.request.Put;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import dlsproto.node.request.model.SingleChannel;

/*******************************************************************************

    Request protocol

*******************************************************************************/

public abstract scope class Put : SingleChannel
{
    import dlsproto.node.request.model.DlsCommand;

    import dlsproto.client.legacy.DlsConst;

    /***************************************************************************

        Used to read the record value into.

    ***************************************************************************/

    private mstring* value_buffer;

    /***************************************************************************

        Pointer to the key buffer, provided to the constructor. Used to read
        the record key into.

    ***************************************************************************/

    private mstring* key_buffer;

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
        super(DlsConst.Command.E.Put, reader, writer, resources);
        this.key_buffer   = this.resources.getKeyBuffer();
        this.value_buffer = this.resources.getValueBuffer();
    }

    /***************************************************************************

        Read the record key/value from the client

    ***************************************************************************/

    final override protected void readChannelRequestData ( )
    {
        this.reader.readArray(*this.key_buffer);
        this.reader.readArray(*this.value_buffer);
    }

    /***************************************************************************

        Stores incoming record

        Params:
            channel_name = channel name for request that was read and validated
                earlier
        
    ***************************************************************************/

    final override protected void handleChannelRequest ( cstring channel_name )
    {
        auto key = *this.key_buffer;

        auto value = *this.value_buffer;

        if (!value.length)
        {
            this.writer.write(DlsConst.Status.E.EmptyValue);
            return;
        }
        
        if (!this.isSizeAllowed(value.length))
        {
            this.writer.write(DlsConst.Status.E.OutOfMemory);
            return;
        }

        if (!this.putRecord(channel_name, key, value))
        {
            this.writer.write(DlsConst.Status.E.Error);
            return;
        }
        else
        {
            this.writer.write(DlsConst.Status.E.Ok);
            return;
        }
    }

    /***************************************************************************

        Verifies that this node is allowed to store records of given size

        Params:
            size = size to check

        Returns:
            'true' if size is allowed

    ***************************************************************************/

    abstract protected bool isSizeAllowed ( size_t size );

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
