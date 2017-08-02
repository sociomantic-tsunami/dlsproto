/*******************************************************************************

    Contains set of very simple test cases.legacy.for basic DLS commands

    Copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlstest.cases.legacy.Basic;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.core.Array;
import ocean.transition;

import dlstest.DlsTestCase;

/*******************************************************************************

    Checks basic GetAll functionality

*******************************************************************************/

class GetAll : DlsTestCase
{
    override public Description description ( )
    {
        Description desc;
        desc.priority = 100;
        desc.name = "GetAll for predefined data";
        return desc;
    }

    public override void run ( )
    {
        // Put some records to the storage channel.
        cstring[][hash_t] records =
        [
            0x0000000000000000: ["record 0", "record 01"],
            0x0000000000000001: ["record 1", "record 10"],
            0x0000000000000002: ["record 2", "record 20"]
        ];

        foreach (k, v; records)
        {
            foreach (rec; v)
            {
                this.dls.put(this.test_channel, k, rec);
            }
        }

        // Do a GetAll to retrieve them all
        auto fetched = this.dls.getAll(this.test_channel);

        // Confirm the results
        test!("==")(fetched.length, records.length);
        bool[hash_t] checked;
        foreach (k, vals; fetched)
        {
            auto local_vals = k in records;
            test(local_vals.length == vals.length, "GetAll returned wrong key");

            foreach (val; vals)
            {
                test!("==")((*local_vals).contains(val), true);
            }

            test(!(k in checked), "GetAll returned the same key twice");
            checked[k] = true;
        }
    }
}

/*******************************************************************************

    Checks RemoveChannel functionality

*******************************************************************************/

class RemoveChannel : DlsTestCase
{
    override public Description description ( )
    {
        Description desc;
        desc.priority = 100;
        desc.name = "RemoveChannel functionality test";
        return desc;
    }

    public override void run ( )
    {
        // Put some records to the storage channel.
        istring[hash_t] records =
        [
            0x0000000000000000: "record 0",
            0x0000000000000001: "record 1",
            0x0000000000000002: "record 2"
        ];

        foreach (k, v; records)
            this.dls.put(this.test_channel, k, v);

        // call RemoveChannel on it
        this.dls.removeChannel(this.test_channel);

        // Do a GetAll to try retrieve any of them
        auto fetched = this.dls.getAll(this.test_channel);

        // Confirm the results
        test!("==")(fetched.length, 0, "GetAll called immidiately after RemoveChannel returned records");
    }
}
