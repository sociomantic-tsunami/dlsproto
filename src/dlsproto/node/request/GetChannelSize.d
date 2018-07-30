/*******************************************************************************

    Protocol base for DLS `GetChannelSize` request

    This request is no longer supported, and it will be removed in the future.

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.node.request.GetChannelSize;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import dlsproto.node.request.model.SingleChannel;

/*******************************************************************************

    RemoveChannel request protocol

*******************************************************************************/

public abstract scope class GetChannelSize : SingleChannel
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
        super(DlsConst.Command.E.GetChannelSize, reader, writer, resources);
    }

    /***************************************************************************

        Replies with ChannelSizeData content as appropriate

        Params:
            channel_name = name of channel to be queried

    ***************************************************************************/

    final override protected void handleChannelRequest ( cstring channel_name )
    {
        this.writer.write(DlsConst.Status.E.NotSupported);
    }
}
