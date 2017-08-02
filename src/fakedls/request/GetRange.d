/*******************************************************************************

    Naive implementation of DLS `GetRange` request

    Copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedls.request.GetRange;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import Protocol = dlsproto.node.request.GetRange;

/*******************************************************************************

    Request implementation

*******************************************************************************/

public scope class GetRange : Protocol.GetRange
{
    import fakedls.mixins.RequestConstruction;
    import fakedls.mixins.ChannelIteration;

    /***************************************************************************

        Hash range to operate over

    ***************************************************************************/

    private cstring key_lower, key_upper;

    /***************************************************************************

        Adds this.resources and constructor to initialize it and forward
        arguments to base

    ***************************************************************************/

    mixin RequestConstruction!();

    /***************************************************************************

        Predicate that accepts records that match the range specified.

        Params:
            key = record key to check
            value = record value to check

        Returns:
            'true' if record matches (should not be filtered out)

    ***************************************************************************/

    private bool rangePredicate ( cstring key, cstring value )
    {
        return key >= this.key_lower && key <= this.key_upper;
    }

    /***************************************************************************

        Adds iteration resources and override `getNext` method

    ***************************************************************************/

    mixin ChannelIteration!(rangePredicate);

    /***************************************************************************

        Communicates requested range to protocol implementation

        Params:
            key_lower = lower bound key in requested range
            key_upper = upper bound key in requested range

    ***************************************************************************/

    override protected void prepareRange ( cstring key_lower, cstring key_upper )
    {
        this.key_lower = key_lower;
        this.key_upper = key_upper;
    }
}
