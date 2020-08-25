/*******************************************************************************

    Common base for all dlstest test cases. Provides DLS client instance and
    defines standard name for tested channel. Automatically connects DLS
    client with the node before test starts.

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlstest.DlsTestCase;

/*******************************************************************************

    Imports

*******************************************************************************/

import turtle.TestCase;

/*******************************************************************************

    Test case base. Actual tests are located in `dlstest.cases`.

*******************************************************************************/

abstract class DlsTestCase : TestCase
{
    import ocean.core.Test; // makes `test` available in derivatives
    import dlstest.DlsClient;
    import ocean.meta.types.Qualifiers;

    /***************************************************************************

        Number of records handled in bulk tests. (This value is used by all test
        cases which test reading/writing/removing a large number of records from
        the DLS. Small sanity check test cases do not use it.)

    ***************************************************************************/

    public static size_t bulk_test_record_count = 10_000;

    /***************************************************************************

        Number of records per key handled in bulk tests. (This value is used by
        all test cases which test reading/writing/removing a large number of
        records from the DLS with multiple values per key. Small sanity check
        test cases do not use it.)

    ***************************************************************************/

    public static size_t bulk_records_per_key = 10;

    /***************************************************************************

        DLS client to use in tests. Provides blocking fiber API.

    ***************************************************************************/

    protected DlsClient dls;

    /***************************************************************************

        Standard name for a channel with test data which will be cleaned
        automatically after the test case ends.

    ***************************************************************************/

    protected string test_channel;

    /***************************************************************************

        Protocol type to use.

    ***************************************************************************/

    protected DlsClient.ProtocolType protocol_type;

    /***************************************************************************

        Constructor

        Params:
            protocol_type = protocol type to use


    ***************************************************************************/

    this ( DlsClient.ProtocolType protocol_type )
    {
        this();
        this.protocol_type = protocol_type;
    }

    /***************************************************************************

       Constructor. Tests DLS using Legacy protocol

    ***************************************************************************/

    this ()
    {
        this.protocol_type = DlsClient.ProtocolType.Legacy;
        this.test_channel = "test_channel";
    }

    /***************************************************************************

        Creates new DLS client for a test case and proceeds with connect so
        that client instance will be ready to work by the time `run` methods
        is being run.

    ***************************************************************************/

    override public void prepare ( )
    {
        this.dls = new DlsClient();
        this.dls.addNode(10000);
        this.dls.connect(this.protocol_type);
    }

    /**************************************************************************

          Deletes test channel each time test case finishes to avoid using
          some state by accident between tests.

    **************************************************************************/

    override public void cleanup ( )
    {
        this.dls.removeChannel(this.test_channel);
    }
}
