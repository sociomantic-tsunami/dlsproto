/*******************************************************************************

    Naive implementation of DLS `GetSize` request

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedls.request.GetSize;

/*******************************************************************************

    Imports

*******************************************************************************/

import Protocol = dlsproto.node.request.GetSize;

/*******************************************************************************

    GetChannels request protocol

*******************************************************************************/

public scope class GetSize : Protocol.GetSize
{
    import fakedls.mixins.RequestConstruction;
    import fakedls.Storage;

    /***************************************************************************

        Adds this.resources and constructor to initialize it and forward
        arguments to base

    ***************************************************************************/

    mixin RequestConstruction!();
}
