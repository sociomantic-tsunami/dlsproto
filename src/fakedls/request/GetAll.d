/*******************************************************************************

    Naive implementation of DLS `GetAll` request

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedls.request.GetAll;

/*******************************************************************************

    Imports

*******************************************************************************/

import Protocol = dlsproto.node.request.GetAll;

/*******************************************************************************

    Request implementation

*******************************************************************************/

public scope class GetAll : Protocol.GetAll
{
    import fakedls.mixins.RequestConstruction;
    import fakedls.mixins.ChannelIteration;

    /***************************************************************************

        Adds this.resources and constructor to initialize it and forward
        arguments to base

    ***************************************************************************/

    mixin RequestConstruction!();

    /***************************************************************************

        Adds iteration resources and override `getNext` method

    ***************************************************************************/

    mixin ChannelIteration!();
}
