/******************************************************************************

    Fake DLS node GetRange request implementation.

    Copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedls.neo.request.GetRange;

import ocean.core.Verify;

import dlsproto.node.neo.request.GetRange;
import dlsproto.node.neo.request.core.IRequestResources;

import swarm.neo.node.RequestOnConn;
import swarm.neo.request.Command;
import dlsproto.common.RequestCodes;
import ocean.text.convert.Integer;

import ocean.transition;
import fakedls.neo.SharedResources;


/*******************************************************************************

    Node implementation of the GetRangeProtocol_v2.

*******************************************************************************/

public class GetRangeImpl_v2: GetRangeProtocol_v2
{
    import swarm.util.Hash;
    import fakedls.Storage;
    import core.stdc.time;
    import ocean.text.Search;
    import ocean.text.regex.PCRE;
    import dlsproto.common.GetRange;

    /// Request code / version. Required by ConnectionHandler.
    static immutable Command command = Command(RequestCode.GetRange, 2);

    /// Request name for stats tracking. Required by ConnectionHandler.
    static immutable istring name = "GetRange";

    /// Flag indicating whether timing stats should be gathered for requests of
    /// this type.
    static immutable bool timing = false;

    /// Flag indicating whether this request type is scheduled for removal. (If
    /// true, clients will be warned.)
    static immutable bool scheduled_for_removal = false;

    /***************************************************************************

        Array of remaining keys in AA to iterate

    ***************************************************************************/

    private istring[] remaining_keys;

    /***************************************************************************

        Key associated with the record values in this.values_for_key

    ***************************************************************************/

    private istring current_key;

    /***************************************************************************

        Array of values associated with the current key

    ***************************************************************************/

    private istring[] values_for_key;


    /***************************************************************************

        Lower boundary of the iterated range.

    ***************************************************************************/

    time_t low;

    /***************************************************************************

        Higher boundary of the iterated range.

    ***************************************************************************/

    time_t high;

    /***************************************************************************

        Channel being iterated in the current request.

    ***************************************************************************/

    private Channel channel;

    /***************************************************************************

        Sub-string search instance.

    ***************************************************************************/

    private SearchFruct match;

    /***************************************************************************

        Regex engine.

    ***************************************************************************/

    private PCRE.CompiledRegex regex;


    /***************************************************************************

        Filtering mode.

    ***************************************************************************/

    private Filter.FilterMode mode;

    /***************************************************************************

        Initialize the channel iterator

        Params:
            channel_name = name of channel to be prepared

        Return:
            `true` if it is possible to proceed with request

    ***************************************************************************/

    override protected bool prepareChannel ( cstring channel_name )
    {
        this.channel = global_storage.get(channel_name);
        if (this.channel !is null)
            this.remaining_keys = this.channel.getKeys();
        else
            this.remaining_keys = null;

        return true;
    }

    /***************************************************************************

        Performs logic needed to start sending records in the given
        range.

        Params:
            low = lower range boundary
            high = higher range boundary

        Returns:
            `true` if the range preparation was sucessfull

    ***************************************************************************/

    override protected bool prepareRange (time_t low, time_t high)
    {
        this.low = low;
        this.high = high;

        return this.low <= this.high;
    }

    /***************************************************************************

        Allows request to process read filter string into more efficient form
        and save it before starting actual record iteration.

        Params:
            mode = filter mode
            filter = filter string

        Returns:
            true if preparing regex filter is successful, false otherwise

    ***************************************************************************/

    override protected bool prepareFilter ( Filter.FilterMode mode,
        cstring filter )
    {
        this.mode = mode;

        switch ( mode )
        {
            case Filter.FilterMode.StringMatch:
                this.match = search(filter);
                break;

            case Filter.FilterMode.PCRE:
            case Filter.FilterMode.PCRECaseInsensitive:
                try
                {
                    auto case_sens = mode != Filter.FilterMode.PCRECaseInsensitive;
                    this.regex = (new PCRE).new CompiledRegex;
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

    /***************************************************************************

        Iterates records for the protocol

        Params:
            key = output value for the next record's key
            value = output value for the next record's value
            wait_for_data = out parameter, will be set if the request should
                            suspend until more data has arrived (always false
                            for fakedls)

        Returns:
            `true` if there was data, `false` if request is complete

    ***************************************************************************/

    override protected bool getNextRecord ( out time_t timestamp,
            ref void[] value,
            out bool wait_for_data)
    {

        while (true)
        {
            // Loop over values fetched for the current key
            while (this.values_for_key.length)
            {
                if (!toInteger(this.current_key, timestamp, 16))
                {
                    return false;
                }

                value = cast(void[])this.values_for_key[0];
                this.values_for_key = this.values_for_key[1 .. $];

                if (!rangeFilterPredicate(timestamp, value))
                {
                    continue;
                }

                return true;
            }

            // Fetch values for the next key
            if (!this.remaining_keys.length)
                return false;

            this.current_key = this.remaining_keys[0];
            this.values_for_key = this.channel.get(this.current_key);
            this.remaining_keys = this.remaining_keys[1 .. $];
        }
    }

    /***************************************************************************

        Predicate that filters records based on the range, and string or
        regex matching.

        Params:
            key = record key to check
            value = record value to check

        Returns:
            'true' if record matches (should not be filtered out)

    ***************************************************************************/

    private bool rangeFilterPredicate ( time_t key, ref void[] value )
    {
        if (key < this.low || key > this.high)
        {
            return false;
        }

        with ( Filter.FilterMode ) switch ( this.mode )
        {
            case None:
                return true;

            case StringMatch:
                 return this.match.forward(cast(char[])value) < value.length;

            case PCRE:
            case PCRECaseInsensitive:
                try
                {
                    verify (this.regex !is null);
                    return this.regex.match(cast(char[])value);
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
}
