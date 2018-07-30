/*******************************************************************************

    Naive implementation of DLS `GetChannelSize` request

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedls.request.GetChannelSize;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import Protocol = dlsproto.node.request.GetChannelSize;

/*******************************************************************************

    GetChannelSize request implementation

*******************************************************************************/

public scope class GetChannelSize : Protocol.GetChannelSize
{
    import fakedls.mixins.RequestConstruction;
    import fakedls.Storage;

    /***************************************************************************

        Adds this.resources and constructor to initialize it and forward
        arguments to base

    ***************************************************************************/

    mixin RequestConstruction!();
}
