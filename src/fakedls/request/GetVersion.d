/*******************************************************************************

    Naive implementation of DLS `GetVersion` request

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedls.request.GetVersion;

/*******************************************************************************

    Imports

*******************************************************************************/

import Protocol = dlsproto.node.request.GetVersion;

/*******************************************************************************

    Request implementation. Completely provided by base in this case.

*******************************************************************************/

public scope class GetVersion : Protocol.GetVersion
{
    import fakedls.mixins.RequestConstruction;

    /***************************************************************************

        Adds this.resources and constructor to initialize it and forward
        arguments to base

    ***************************************************************************/

    mixin RequestConstruction!();
}
