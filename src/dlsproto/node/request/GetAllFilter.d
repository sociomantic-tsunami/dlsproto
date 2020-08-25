/*******************************************************************************

    Protocol base for DLS `GetAllFilter` request

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.node.request.GetAllFilter;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.meta.types.Qualifiers;

import dlsproto.node.request.model.CompressedBatch;

/*******************************************************************************
    
    Request protocol

*******************************************************************************/

public abstract class GetAllFilter : CompressedBatch!(cstring, cstring)
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
        super(DlsConst.Command.E.GetAllFilter, reader, writer, resources);
    }

    /***************************************************************************
    
        Read filter data from the client

    ***************************************************************************/

    final override protected void readChannelRequestData ( )
    {
        auto filter = this.resources.getFilterBuffer();
        this.reader.readArray(*filter);
        this.prepareFilter(*filter);
    }

    /***************************************************************************
        
        Allows request to process read filter string into more efficient form
        and save it before starting actual record iteration.

        Params:
            filter = filter string

    ***************************************************************************/

    abstract protected void prepareFilter ( cstring filter );
}
