/*******************************************************************************

    Protocol base for DLS `GetRange` request

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.node.request.GetRange;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import dlsproto.node.request.model.CompressedBatch;

/*******************************************************************************

    Request protocol

*******************************************************************************/

public abstract scope class GetRange : CompressedBatch!(cstring, cstring)
{
    import dlsproto.node.request.model.DlsCommand;

    import dlsproto.client.legacy.DlsConst;

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
        super(DlsConst.Command.E.GetRange, reader, writer, resources);
    }

    /***************************************************************************

        Read upper and lower bound keys for GetRange request

    ***************************************************************************/

    final override protected void readChannelRequestData ( )
    {
        auto lower = this.resources.getKeyBuffer();
        this.reader.readArray(*lower);
        auto upper = this.resources.getKeyUpperBuffer();
        this.reader.readArray(*upper);

        this.prepareRange(*lower, *upper);
    }

    /***************************************************************************
        
        Communicates requested range to protocol implementation

        Params:
            key_lower = lower bound key in requested range
            key_upper = upper bound key in requested range

    ***************************************************************************/

    abstract protected void prepareRange ( cstring key_lower, cstring key_upper );
}
