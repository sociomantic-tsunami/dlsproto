/*******************************************************************************

    Test for sending a set of records with non-sequential keys via Put

    Copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlstest.cases.legacy.UnorderedPut;

/*******************************************************************************

    Imports

*******************************************************************************/

import dlstest.DlsTestCase;
import dlstest.DlsClient;

/*******************************************************************************

    Checks that a set of records with non-sequential keys written to the DLS via
    Put are correctly added to the database.

*******************************************************************************/

class UnorderedPutTest : DlsTestCase
{
    import dlstest.DlsClient;
    import dlstest.util.LocalStore;
    import dlstest.util.Record;

    public override Description description ( )
    {
        Description desc;
        desc.name = "Unordered Put test";
        return desc;
    }

    public override void run ( )
    {
        LocalStore local;

        for ( uint i = 0; i < bulk_test_record_count; i++ )
        {
            auto rec = Record.spread(i);
            this.dls.put(this.test_channel, rec.key, rec.val);
            local.put(rec.key, rec.val);
        }

        local.verifyAgainstDls(this.dls, DlsClient.ProtocolType.Legacy,
                this.test_channel);
    }
}

