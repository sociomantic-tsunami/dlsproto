/*******************************************************************************

    Protocol base for DLS `GetRangeFilter` request

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.node.request.GetRangeFilter;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.meta.types.Qualifiers;

import dlsproto.node.request.model.CompressedBatch;

/*******************************************************************************

    Request protocol

*******************************************************************************/

public abstract scope class GetRangeFilter : CompressedBatch!(cstring, cstring)
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
        super(DlsConst.Command.E.GetRangeFilter, reader, writer, resources);
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

        auto filter = this.resources.getFilterBuffer();
        this.reader.readArray(*filter);

        this.prepareRange(*lower, *upper);
        this.prepareFilter(*filter);
    }

    /***************************************************************************
        
        Communicates requested range to protocol implementation

        Params:
            key_lower = lower bound key in requested range
            key_upper = upper bound key in requested range

    ***************************************************************************/

    abstract protected void prepareRange ( cstring key_lower, cstring key_upper );

    /***************************************************************************
        
        Allows request to process read filter string into more efficient form
        and save it before starting actual record iteration.

        Params:
            filter = filter string

    ***************************************************************************/

    abstract protected void prepareFilter ( cstring filter );
}
