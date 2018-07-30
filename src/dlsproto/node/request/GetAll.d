/*******************************************************************************

    Protocol base for DLS `GetAll` request

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.node.request.GetAll;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import dlsproto.node.request.model.CompressedBatch;

/*******************************************************************************

    Request protocol

*******************************************************************************/

public abstract scope class GetAll : CompressedBatch!(cstring, cstring)
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
        super(DlsConst.Command.E.GetAll, reader, writer, resources);
    }
}
