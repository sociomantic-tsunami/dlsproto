/*******************************************************************************

    Protocol base for DLS `GetVersion` request

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.node.request.GetVersion;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import dlsproto.node.request.model.DlsCommand;

/*******************************************************************************

    Request protocol

*******************************************************************************/

public abstract scope class GetVersion : DlsCommand
{
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
        super(DlsConst.Command.E.GetVersion, reader, writer, resources);
    }

    /***************************************************************************

        No-op

    ***************************************************************************/

    final override protected void readRequestData ( ) { }

    /***************************************************************************

        Sends configured version number

    ***************************************************************************/

    final override protected void handleRequest ( )
    {
        this.writer.write(DlsConst.Status.E.Ok);
        this.writer.writeArray(DlsConst.ApiVersion);
    }
}
