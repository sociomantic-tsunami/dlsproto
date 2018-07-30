/*******************************************************************************

    Contains set of tests for testing GetRange command

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlstest.cases.legacy.Ranges;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;
import ocean.core.Array;
import ocean.core.Test;
import dlstest.DlsClient;
import dlstest.DlsTestCase;
import ocean.core.Test;
import dlstest.DlsClient;

/*******************************************************************************

    Checks basic GetRange functionality

*******************************************************************************/

class GetRange : DlsTestCase
{
    override public Description description ( )
    {
        Description desc;
        desc.priority = 100;
        desc.name = "GetRange for data contained in the storage";
        return desc;
    }

    public override void run ( )
    {
        // Put some records to the storage channel.
        cstring[][hash_t] records =
        [
            0x0000000000000000: ["record 0"],
            0x0000000000000001: ["record 1"],
            0x0000000000000002: ["record 2"],
            0x0000000000000003: ["record 3"],
            0x0000000000000004: ["record 4"],
            0x0000000000000005: ["record 5"]
        ];

        foreach (k, vals; records)
            foreach (v; vals)
                this.dls.put(this.test_channel, k, v);

        auto start = 1;
        auto cnt = 3;
        auto end = start + cnt - 1;

        // Do a GetRange to retrieve them
        auto fetched = this.dls.getRange(this.test_channel, start, end);

        // Confirm the results
        test!("==")(fetched.length, cnt);
        foreach (k, remote_recs; fetched)
        {
            auto local_recs = k in records;
            test!("!is")(local_recs, null, "GetRange returned wrong key");
            test!("==")((*local_recs).length, remote_recs.length, "GetRange returned wrong amount of values");
            test(k >= start && k <= end, "GetRange returned the key out of requested range");

            // test if all values are here
            foreach (rec; remote_recs)
            {
                test((*local_recs).contains(rec));
            }
        }
    }
}

/*******************************************************************************

    Checks GetRange functionality with range values outside the range present
    in the node

*******************************************************************************/

class GetRangeEmpty : DlsTestCase
{
    override public Description description ( )
    {
        Description desc;
        desc.priority = 100;
        desc.name = "GetRange for range outside the data";
        return desc;
    }

    public override void run ( )
    {
        // Put some records to the storage channel.
        istring[][hash_t] records =
        [
            0x0000000000000000: ["record 0"],
            0x0000000000000001: ["record 1"],
            0x0000000000000002: ["record 2"],
            0x0000000000000003: ["record 3"],
            0x0000000000000004: ["record 4"],
            0x0000000000000005: ["record 5"]
        ];

        foreach (k, vals; records)
            foreach (v; vals)
                this.dls.put(this.test_channel, k, v);

        auto start = records.length;
        auto cnt = 3;
        auto end = start + cnt - 1;

        // Do a GetRange to retrieve them
        auto fetched = this.dls.getRange(this.test_channel, start, end);

        // Confirm the results
        test!("==")(fetched.length, 0);
    }
}

/*******************************************************************************

    Checks GetRange functionality with range values that are exact the range
    present in the node

*******************************************************************************/

class GetRangeExact : DlsTestCase
{
    override public Description description ( )
    {
        Description desc;
        desc.priority = 100;
        desc.name = "GetRange for exact range";
        return desc;
    }

    public override void run ( )
    {
        // Put some records to the storage channel.
        cstring[][hash_t] records =
        [
            0x0000000000000000: ["record 0"],
            0x0000000000000001: ["record 1"],
            0x0000000000000002: ["record 2"],
            0x0000000000000003: ["record 3"],
            0x0000000000000004: ["record 4"],
            0x0000000000000005: ["record 5"]
        ];

        foreach (k, vals; records)
            foreach (v; vals)
                this.dls.put(this.test_channel, k, v);

        auto start = 0;
        auto cnt = records.length;
        auto end = start + cnt - 1;

        // Do a GetRange to retrieve them
        auto fetched = this.dls.getRange(this.test_channel, start, end);

        // Confirm the results
        test!("==")(fetched.length, cnt);
        foreach (k, remote_recs; fetched)
        {
            auto local_recs = k in records;
            test(local_recs !is null, "GetRange returned wrong key");
            test(local_recs.length == remote_recs.length, "GetRange returned wrong amount of values");
            test(k >= start && k <= end, "GetRange returned the key out of requested range");

            // test if all values are here
            foreach (rec; remote_recs)
            {
                test((*local_recs).contains(rec));
            }
        }
    }
}

/*******************************************************************************

    Checks GetRange functionality with PCRE filter enabled.

*******************************************************************************/

class GetRangePCREMatching : DlsTestCase
{
    override public Description description ( )
    {
        Description desc;
        desc.priority = 100;
        desc.name = "GetRange for overlapping range";
        return desc;
    }

    public override void run ( )
    {
        const logline = "http://eu-sonar.sociomantic.com/js/2010-07-01/action/click?&aid=google&fpc=7161999584528497855&aaid=zalando&size=3&cid=445&ao=%5B%7B%22id%22%3A%2216880840621970542745%22%2C%22fsize%22%3A22%7D%5D";


        // Put some records to the storage channel.
        cstring[][hash_t] records =
        [
            0x0000000000000001: [logline],
            0x0000000000000002: [logline],
            0x0000000000000003: [logline]
        ];

        foreach (k, vals; records)
            foreach (v; vals)
                this.dls.put(this.test_channel, k, v);

        const pcre =
            "(aid=google)";

        auto start = 0;
        auto cnt = records.length;
        auto end = cnt * 2;

        // Do a GetRange to retrieve them
        auto fetched = this.dls.getRange(this.test_channel, start, end,
               DlsClient.FilterType.PCRE, pcre);

        // Confirm the results
        test!("==")(fetched.length, cnt);
        foreach (k, remote_recs; fetched)
        {
            auto local_recs = k in records;
            test(local_recs !is null, "GetRange returned wrong key");
            test(local_recs.length == remote_recs.length, "GetRange returned wrong amount of values");
            test(k >= start && k <= end, "GetRange returned the key out of requested range");

            // test if all values are here
            foreach (rec; remote_recs)
            {
                test((*local_recs).contains(rec));
            }
        }
    }
}

/*******************************************************************************

    Checks GetRange functionality with PCRE filter enabled where not all
    the records are matched

*******************************************************************************/

class GetRangePCREHalfMatching : DlsTestCase
{
    override public Description description ( )
    {
        Description desc;
        desc.priority = 100;
        desc.name = "GetRange for overlapping range";
        return desc;
    }

    public override void run ( )
    {
        const logline = "http://eu-sonar.sociomantic.com/js/2010-07-01/action/click?&aid=google&fpc=7161999584528497855&aaid=zalando&size=3&cid=445&ao=%5B%7B%22id%22%3A%2216880840621970542745%22%2C%22fsize%22%3A22%7D%5D";
        const bad_logline = "http://eu-sonar.sociomantic.com/js/2010-07-01/action/click?&aid=facebook&fpc=7161999584528497855&aaid=bbc&size=3&cid=445&ao=%5B%7B%22id%22%3A%2216880840621970542745%22%2C%22fsize%22%3A22%7D%5D";


        // Put some records to the storage channel.
        cstring[][hash_t] records =
        [
            0x0000000000000001: [logline],
            0x0000000000000002: [bad_logline],
            0x0000000000000003: [logline]
        ];

        foreach (k, vals; records)
            foreach (v; vals)
                this.dls.put(this.test_channel, k, v);

        const pcre =
            "(aid=google.*(aaid=zalando|aaid=zalando-fr|aaid=zalando-uk))|((aaid=zalando|aaid=zalando-fr|aaid=zalando-uk).*aid=google)";

        auto start = 0;
        auto cnt = 2;
        auto end = records.length + 1;

        // Do a GetRange to retrieve them
        auto fetched = this.dls.getRange(this.test_channel, start, end,
                DlsClient.FilterType.PCRE, pcre);

        // Confirm the results
        test!("==")(fetched.length, cnt);
        foreach (k, remote_recs; fetched)
        {
            auto local_recs = k in records;
            test(local_recs !is null, "GetRange returned wrong key");
            test(local_recs.length == remote_recs.length, "GetRange returned wrong amount of values");
            test(k >= start && k <= end, "GetRange returned the key out of requested range");

            // test if all values are here
            foreach (rec; remote_recs)
            {
                test((*local_recs).contains(rec));
            }
        }
    }
}

/*******************************************************************************

    Checks GetRange functionality with malformed PCRE filter.

*******************************************************************************/

class GetRangePCREMalformed : DlsTestCase
{
    override public Description description ( )
    {
        Description desc;
        desc.priority = 100;
        desc.name = "GetRange with malformed regex";
        return desc;
    }

    public override void run ( )
    {
        const logline = "http://eu-sonar.sociomantic.com/js/2010-07-01/action/click?&aid=google&fpc=7161999584528497855&aaid=zalando&size=3&cid=445&ao=%5B%7B%22id%22%3A%2216880840621970542745%22%2C%22fsize%22%3A22%7D%5D";


        // Put some records to the storage channel.
        cstring[][hash_t] records =
        [
            0x0000000000000001: [logline],
            0x0000000000000002: [logline],
            0x0000000000000003: [logline]
        ];

        foreach (k, vals; records)
            foreach (v; vals)
                this.dls.put(this.test_channel, k, v);

        const pcre =
            "*vw=1&";

        auto start = 0;
        auto cnt = records.length;
        auto end = cnt * 2;
        auto exception_caught = false;

        cstring[][hash_t] fetched;

        try
        {
            // Do a GetRange to try to retrieve. This should fail
            fetched = this.dls.getRange(this.test_channel, start, end,
                   DlsClient.FilterType.PCRE, pcre);
        }
        catch (Exception e)
        {
            exception_caught = true;
        }

        // Confirm the results
        test!("==")(fetched.length, 0);
        test!("==")(exception_caught, true);
    }
}
