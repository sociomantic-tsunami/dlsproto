/*******************************************************************************

    Test for sending a set of records with non-sequential keys via Neo's Put

    Copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlstest.cases.neo.UnorderedMultiplePut;

/*******************************************************************************

    Imports

*******************************************************************************/

import dlstest.NeoDlsTestCase;
import dlstest.DlsClient;

/*******************************************************************************

    Checks that a set of records with non-sequential keys written to the DLS via
    Put are correctly added to the database.

*******************************************************************************/

class UnorderedMultiPutTest : NeoDlsTestCase
{
    import dlstest.util.LocalStore;
    import dlstest.util.Record;

    public override Description description ( )
    {
        Description desc;
        desc.name = "Neo Unordered Put test with multiple values per key";
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
                auto rec = Record.spread(i, j);
                this.dls.neo.put(this.test_channel, rec.key, rec.val);
                local.put(rec.key, rec.val);
            }
        }

        local.verifyAgainstDls(this.dls, DlsClient.ProtocolType.Neo, this.test_channel);
    }
}

