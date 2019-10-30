/*******************************************************************************

    Protocol base for DLS `GetChannels` request

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.node.request.GetChannels;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import dlsproto.node.request.model.DlsCommand;

/*******************************************************************************

    GetChannels request protocol

*******************************************************************************/

public abstract scope class GetChannels : DlsCommand
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
        super(DlsConst.Command.E.GetChannels, reader, writer, resources);
    }

    /***************************************************************************

        No data expected for GetChannels request

    ***************************************************************************/

    final override protected void readRequestData ( ) { }

    /***************************************************************************

        Write status and response data

    ***************************************************************************/

    final override protected void handleRequest ( )
    {
        this.writer.write(DlsConst.Status.E.Ok);
        foreach (id; this.getChannelsIds())
        {
            this.writer.writeArray(id);
        }
        this.writer.writeArray(""); // End of list
    }

    /***************************************************************************

        Must return list of all channels stored in this node.

        Returns:
            list of channel names

    ***************************************************************************/

    abstract protected const(char[])[] getChannelsIds ( );
}
