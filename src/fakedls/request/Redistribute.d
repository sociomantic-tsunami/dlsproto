/*******************************************************************************

    Stub for Redistribute request (not supported)

    Copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedls.request.Redistribute;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.meta.types.Qualifiers;

import Protocol = dlsproto.node.request.Redistribute;
import dlsproto.client.legacy.DlsConst;

static if (!is(typeof(DlsConst.Command.E.Redistribute))) {}
else:

/*******************************************************************************

    Request implementation

*******************************************************************************/

public class Redistribute : Protocol.Redistribute
{
    import fakedls.mixins.RequestConstruction;

    /***************************************************************************

        Adds this.resources and constructor to initialize it and forward
        arguments to base

    ***************************************************************************/

    mixin RequestConstruction!();

    /**************************************************************************/

    protected override void redistributeData ( NodeItem[] dataset,
           float fraction_of_data_to_send )
    {
        assert (false, "Not supported by the fake DLS node");
    }
}
