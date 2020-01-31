/*******************************************************************************

    Stub for PutBatch request (not supported)

    Copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedls.request.PutBatch;

/*******************************************************************************

    Imports

*******************************************************************************/

import Protocol = dlsproto.node.request.PutBatch;

import ocean.meta.types.Qualifiers;

import dlsproto.client.legacy.DlsConst;

static if (!is(typeof(DlsConst.Command.E.PutBatch))) {}
else:

/*******************************************************************************

    Request implementation

*******************************************************************************/

public scope class PutBatch : Protocol.PutBatch
{
    import fakedls.mixins.RequestConstruction;

    /***************************************************************************

        Adds this.resources and constructor to initialize it and forward
        arguments to base

    ***************************************************************************/

    mixin RequestConstruction!();


    /**************************************************************************/

    protected override bool putRecord ( cstring channel, cstring key, cstring value )
    {
        assert (false, "Not supported by the fake DLS node");
    }
}
