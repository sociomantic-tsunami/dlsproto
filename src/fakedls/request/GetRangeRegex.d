/*******************************************************************************

    Naive implementation of DLS `GetRangeFilter` request

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedls.request.GetRangeRegex;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import Protocol = dlsproto.node.request.GetRangeRegex;

/*******************************************************************************

    Request implementation

*******************************************************************************/

public scope class GetRangeRegex : Protocol.GetRangeRegex
{
    import ocean.text.regex.PCRE;
    import ocean.text.Search;

    import dlsproto.client.legacy.DlsConst;

    import fakedls.ConnectionHandler;
    import fakedls.mixins.RequestConstruction;
    import fakedls.mixins.ChannelIteration;


    /***************************************************************************

        Sub-string search instance.

    ***************************************************************************/

    private SearchFruct match;

    /***************************************************************************

        Hash range to operate over

    ***************************************************************************/

    private cstring key_lower, key_upper;

    /***************************************************************************

        Regex engine.

    ***************************************************************************/

    private PCRE.CompiledRegex regex;

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
        with ( DlsConst.FilterMode ) switch ( this.mode )
        {
            case StringMatch:
                return key >= this.key_lower && key <= this.key_upper &&
                     this.match.forward(value) < value.length;

            case PCRE:
            case PCRECaseInsensitive:
                try
                {
                    assert (this.regex !is null);
                    return key >= this.key_lower && key <= this.key_upper &&
                         this.regex.match(value);
                }
                catch ( Exception e )
                {
                    return false;
                }
                assert(false);

            default:
                assert(false);
        }
        assert(false);
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
            mode = how to interpret filter string
            filter = filter string

        Returns:
            true if preparing regex filter is successful, false otherwise

    ***************************************************************************/

    override protected bool prepareFilter ( DlsConst.FilterMode mode,
        cstring filter )
    {
        with ( DlsConst.FilterMode ) switch ( this.mode )
        {
            case StringMatch:
                this.match = search(filter);
                break;

            case PCRE:
            case PCRECaseInsensitive:
                try
                {
                    auto case_sens = mode != PCRECaseInsensitive;
                    this.regex =
                        (cast(DlsConnectionHandler.DlsRequestResources)this.resources).getRegex();
                    this.regex.compile(filter, case_sens);
                }
                catch ( Exception e )
                {
                    return false;
                }
                break;

            default:
                assert(false);
        }

        return true;
    }
}
