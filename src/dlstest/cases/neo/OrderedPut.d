/*******************************************************************************

    Test for sending a set of records with sequential keys via Neo's Put.

    Copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlstest.cases.neo.OrderedPut;

/*******************************************************************************

    Imports

*******************************************************************************/

import dlstest.NeoDlsTestCase;
import dlstest.DlsClient;

/*******************************************************************************

    Checks that a set of records with sequential keys written to the DLS via Put
    are correctly added to the database.

*******************************************************************************/

class OrderedPutTest : NeoDlsTestCase
{
    import dlstest.util.LocalStore;
    import dlstest.util.Record;

    public override Description description ( )
    {
        Description desc;
        desc.name = "Neo Ordered Put test";
        return desc;
    }

    public override void run ( )
    {
        LocalStore local;

        for ( uint i = 0; i < bulk_test_record_count; i++ )
        {
            auto rec = Record.sequential(i);
            this.dls.neo.put(this.test_channel, rec.key, rec.val);
            local.put(rec.key, rec.val);
        }

        local.verifyAgainstDls(this.dls, DlsClient.ProtocolType.Neo,
                this.test_channel);
    }
}

