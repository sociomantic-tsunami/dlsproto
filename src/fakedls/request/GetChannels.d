/*******************************************************************************

    Naive implementation of DLS `GetChannels` request

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedls.request.GetChannels;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.meta.types.Qualifiers;

import Protocol = dlsproto.node.request.GetChannels;

/*******************************************************************************

    GetChannels request implementation

*******************************************************************************/

public scope class GetChannels : Protocol.GetChannels
{
    import fakedls.mixins.RequestConstruction;
    import fakedls.Storage;

    /***************************************************************************

        Adds this.resources and constructor to initialize it and forward
        arguments to base

    ***************************************************************************/

    mixin RequestConstruction!();

    /***************************************************************************

        Must return list of all channels stored in this node.

        Returns:
            list of channel names

    ***************************************************************************/

    override protected const(char[])[] getChannelsIds ( )
    {
        return global_storage.getChannelList();
    }
}
