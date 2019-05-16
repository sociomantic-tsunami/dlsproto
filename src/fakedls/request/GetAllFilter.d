/*******************************************************************************

    Naive implementation of DLS `GetAllFilter` request

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedls.request.GetAllFilter;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import Protocol = dlsproto.node.request.GetAllFilter;

/*******************************************************************************

    Request implementation

*******************************************************************************/

public scope class GetAllFilter : Protocol.GetAllFilter
{
    import fakedls.mixins.RequestConstruction;
    import fakedls.mixins.ChannelIteration;

    import ocean.text.Search;

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

    private bool filterPredicate ( cstring key, cstring value )
    {
        return this.match.forward(value) < value.length;
    }

    /***************************************************************************

        Adds this.iterator and prepareChannel override to initialize it
        Defines `getNext` that uses filterPredicate to filter records

    ***************************************************************************/

    mixin ChannelIteration!(filterPredicate);

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
