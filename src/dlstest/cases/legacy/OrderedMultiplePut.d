/*******************************************************************************

    Test for sending a set of records with sequential keys via Put with
    multiple values per key

    Copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlstest.cases.legacy.OrderedMultiplePut;

/*******************************************************************************

    Imports

*******************************************************************************/

import dlstest.DlsTestCase;

/*******************************************************************************

    Checks that a set of records with sequential keys written to the DLS via Put
    are correctly added to the database.

*******************************************************************************/

class OrderedMultiPutTest : DlsTestCase
{
    import dlstest.DlsClient;
    import dlstest.util.LocalStore;
    import dlstest.util.Record;

    public override Description description ( )
    {
        Description desc;
        desc.name = "Ordered Put test with multiple values per key";
        return desc;
    }

    public override void run ( )
    {
        LocalStore local;

        for ( uint i = 0; i < bulk_test_record_count; i++ )
        {
            // let's put multiple values for the same key
            for (auto j = 0; j < bulk_records_per_key; j++)
            {
                auto rec = Record.sequential(i, j);
                this.dls.put(this.test_channel, rec.key, rec.val);
                local.put(rec.key, rec.val);
            }
        }

        local.verifyAgainstDls(this.dls, DlsClient.ProtocolType.Legacy,
                this.test_channel);
    }
}

