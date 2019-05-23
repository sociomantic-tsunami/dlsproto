/*******************************************************************************

    Map used to verify the results of operations on the DLS being tested.

    When used in tests, the map should be updated in the same way as the DLS
    being tested (e.g. when a record is put to the DLS, the same record should
    be put to the map). The verifyAgainstDls() method then performs a thorough
    series of tests to confirm that the content of the DLS exactly matches the
    content of the map.

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlstest.util.LocalStore;

struct LocalStore
{
    import dlstest.DlsClient;
    import dlstest.util.Record;
    import ocean.text.regex.PCRE;
    import turtle.runner.Logging;
    import ocean.core.Array : contains;
    import ocean.core.Test;
    import ocean.transition;

    /***************************************************************************

        Data in local store

    ***************************************************************************/

    public cstring[][hash_t] data;

    /***************************************************************************

        Adds a record to the local store.

        Params:
            key = record key
            val = record value

    ***************************************************************************/

    public void put ( hash_t key, cstring val )
    {
        this.data[key] ~= val;
    }

    /***************************************************************************

        Removes a record from the local store.

        Params:
            key = key to remove

    ***************************************************************************/

    public void remove ( hash_t key )
    {
        this.data.remove(key);
    }

    /***************************************************************************

        Performs a series of tests to confirm that the content of the DLS
        exactly matches the content of the map.

        Params:
            dls = DLS client to use to perform tests
            channel = name of channel to compare against in DLS

        Throws:
            TestException upon verification failure

    ***************************************************************************/

    public void verifyAgainstDls ( DlsClient dls, DlsClient.ProtocolType protocol_type, cstring channel )
    {
        // find minimum and maximum key in the local data
        hash_t min = 0;
        hash_t max = 0;

        foreach (k, v; this.data)
        {
            if (k < min) { min = k; }
            else if (k > max) { max = k; }
        }

        // caclulate border values so the range would not be invalid
        auto mid = (min / 2) + (max / 2) - 1;

        if (protocol_type & DlsClient.ProtocolType.Legacy)
        {
            this.verifyGetAll(dls, channel);
            this.verifyGetAllFilter(dls, channel);
        }

        this.verifyGetRange(dls, protocol_type, channel, min, max);
        this.verifyGetRange(dls, protocol_type, channel, min, max - mid);
        this.verifyGetRange(dls, protocol_type, channel, min + mid / 4, max + 1);
        this.verifyGetRange(dls, protocol_type, channel, max + 1, max + 2);
        this.verifyGetRange(dls, protocol_type, channel, min / 2, max);

        // tests including filter
        this.verifyGetRange(dls, protocol_type, channel, min, max, DlsClient.FilterType.StringFilter, "0");
        this.verifyGetRange(dls, protocol_type, channel, min, max - mid, DlsClient.FilterType.StringFilter, "0");
        this.verifyGetRange(dls, protocol_type, channel, min + mid / 4, max + 1, DlsClient.FilterType.StringFilter, "0");
        this.verifyGetRange(dls, protocol_type, channel, max + 1, max + 2, DlsClient.FilterType.StringFilter, "0");
        this.verifyGetRange(dls, protocol_type, channel, min / 2, max, DlsClient.FilterType.StringFilter, "0");

        // tests including regex
        this.verifyGetRange(dls, protocol_type, channel, min, max, DlsClient.FilterType.PCRE, "^1.*");
        this.verifyGetRange(dls, protocol_type, channel, min, max - mid, DlsClient.FilterType.PCRE, "^1.*");
        this.verifyGetRange(dls, protocol_type, channel, min + mid / 4, max + 1, DlsClient.FilterType.PCRE, "^1.*");
        this.verifyGetRange(dls, protocol_type, channel, max + 1, max + 2, DlsClient.FilterType.PCRE, "^1.*");
        this.verifyGetRange(dls, protocol_type, channel, min / 2, max, DlsClient.FilterType.PCRE, "^1.*");
    }

    /***************************************************************************

        Compares all records in the DLS channel against the records in the local
        store, using a DLS GetAll request.

        Params:
            dls = DLS client to use to perform tests
            channel = name of channel to compare against in DLS

        Throws:
            TestException upon verification failure

    ***************************************************************************/

    private void verifyGetAll ( DlsClient dls, cstring channel )
    {
        auto remote = dls.getAll(channel);
        log.trace("\tVerifying channel with GetAll: local:{}, remote:{}",
            this.data.length, remote.length);
        test!("==")(this.data.length, remote.length);

        foreach ( k, v; remote )
        {
            test!("==")(this.data[k].length, remote[k].length);
            test!("in")(k, this.data);

            foreach (val; remote[k])
            {
                auto found_record = false;

                foreach (local_val; this.data[k])
                {
                    if (val == local_val)
                    {
                        found_record = true;
                        break;
                    }
                }

                test!("==")(found_record, true);
            }
        }
    }

    /***************************************************************************

        Compares all records in the DLS channel against the records in the local
        store, with a standard string-match filter applied to both (the filter
        passes records which contain the character "0"), using a DLS
        GetAllFilter request.

        Params:
            dls = DLS client to use to perform tests
            channel = name of channel to compare against in DLS

        Throws:
            TestException upon verification failure

    ***************************************************************************/

    private void verifyGetAllFilter ( DlsClient dls, cstring channel )
    {
        enum filter = "0";

        cstring[][hash_t] local;

        foreach ( k, vals; this.data )
        {
            foreach (v; vals)
            {
               if (v.contains(filter))
                   local[k] ~= v;
            }
        }

        auto remote = dls.getAll(channel, filter);
        log.trace("\tVerifying channel with GetAllFilter: local:{}, remote:{}",
            local.length, remote.length);
        test!("==")(local.length, remote.length);

        foreach ( k, remote_vals; remote )
        {
            auto local_vals = k in local;

            test!("!is")(local_vals, null);
            test!("==")(local_vals.length, remote_vals.length);

            // confirm that all returned values are present in the local
            // storage
            foreach (local_val; *local_vals)
            {
                test(remote_vals.contains(local_val));
            }
        }
    }

    /***************************************************************************

        Compares range of records in the DLS channel against the records in the local
        store, with a standard string-match filter applied to both (the filter
        passes records which contain the character "0"), using a DLS
        GetAllFilter request.

        Params:
            dls = DLS client to use to perform tests
            protocol_type = bitfield describing the protocol type
            channel = name of channel to compare against in DLS
            start = beginning of the key range to fetch
            end = end of the key range to fetch
            filter_type = indicator should GetRange perform filter, and if so, what
                          type
            filter = text string to match the values against

        Throws:
            TestException upon verification failure

    ***************************************************************************/

     private void verifyGetRange ( DlsClient dls, DlsClient.ProtocolType protocol_type,
             cstring channel, hash_t start, hash_t end,
             DlsClient.FilterType filter_type = DlsClient.FilterType.None,
             cstring filter  = null)
     {
        cstring[][hash_t] local;

        // compile regex if passed.
        PCRE.CompiledRegex regex;

        if (filter_type == DlsClient.FilterType.PCRE)
        {
            regex = (new PCRE).new CompiledRegex;
            regex.compile(filter, true);
        }

        foreach ( k, vals; this.data )
        {
            foreach (v; vals)
            {
                if (k >= start && k <= end)
                {
                     if (filter_type == DlsClient.FilterType.StringFilter && !v.contains(filter))
                     {
                         continue;
                     }
                     else if (filter_type == DlsClient.FilterType.PCRE && !regex.match(v))
                     {
                         continue;
                     }


                     local[k] ~= v;
                }
            }
        }

        /***********************************************************************

            Method to verify if the received data is matching the local data

            Params:
                remote = data received from the node

            Throws:
                TestException if the received data doesn't match local one

        ***********************************************************************/

        void verifyReceived (cstring[][hash_t] remote)
        {
            test!("==")(local.length, remote.length);

            foreach ( k, remote_vals; remote )
            {
                auto local_vals = k in local;

                test!("!is")(local_vals, null);
                test!("==")(local_vals.length, remote_vals.length);

                // confirm that all returned values are present in the local
                // storage
                foreach (local_val; *local_vals)
                {
                    test(remote_vals.contains(local_val));
                }
            }

        }

        if (protocol_type & DlsClient.ProtocolType.Legacy)
        {
            // Do a legacy get range, followed by the Neo get range
            log.trace("\tVerifying channel with GetRangeFilter");
            verifyReceived(dls.getRange(channel, start, end, filter_type, filter));
        }

        if (protocol_type & DlsClient.ProtocolType.Neo)
        {
            log.trace("\tVerifying channel with Neo GetRangeFilter");

            // Adapt to the change in APIs
            auto neo_filter_type = DlsClient.Filter.FilterMode.None;
            switch (filter_type)
            {
                case DlsClient.FilterType.StringFilter:
                    neo_filter_type = DlsClient.Filter.FilterMode.StringMatch;
                    break;
                case DlsClient.FilterType.PCRE:
                    neo_filter_type = DlsClient.Filter.FilterMode.PCRE;
                    break;
                default:
                    break;
            }

            verifyReceived(
                    cast(cstring[][hash_t])dls.neo.getRange(channel,
                        start,
                        end,
                        filter,
                        neo_filter_type)
                    );
        }
    }
}
