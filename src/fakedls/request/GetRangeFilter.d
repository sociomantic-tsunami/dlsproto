/*******************************************************************************

    Naive implementation of DLS `GetRangeFilter` request

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedls.request.GetRangeFilter;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import Protocol = dlsproto.node.request.GetRangeFilter;

/*******************************************************************************

    Request implementation

*******************************************************************************/

public scope class GetRangeFilter : Protocol.GetRangeFilter
{
    import fakedls.mixins.RequestConstruction;
    import fakedls.mixins.ChannelIteration;

    import ocean.text.Search;

    /***************************************************************************

        Hash range to operate over

    ***************************************************************************/

    private cstring key_lower, key_upper;

    /***************************************************************************

        Sub-string search instance.

    ***************************************************************************/

    private SearchFruct match;

    /***************************************************************************

        Adds this.resources and constructor to initialize it and forward
        arguments to base

    ***************************************************************************/

    mixin RequestConstruction!();

    /***************************************************************************

        Predicate that accepts records that match filter defined by this.match

        Params:
            key = record key to check
            value = record value to check

        Returns:
            'true' if record matches (should not be filtered out)

    ***************************************************************************/

    private bool rangeFilterPredicate ( cstring key, cstring value )
    {
        return key >= this.key_lower && key <= this.key_upper
            && this.match.forward(value) < value.length;
    }

    /***************************************************************************

        Adds this.iterator and prepareChannel override to initialize it
        Defines `getNext` that uses rangeFilterPredicate to filter records

    ***************************************************************************/

    mixin ChannelIteration!(rangeFilterPredicate);

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

    /***************************************************************************

        Initialized regex match based on provided filter string

        Params:
            filter = filter string

    ***************************************************************************/

    final override protected void prepareFilter ( cstring filter )
    {
        this.match = search(filter);
    }
}
