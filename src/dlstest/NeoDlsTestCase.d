/*******************************************************************************

    Common base for all dlstest test cases, using neo protocol. Provides DLS
    client instance and defines standard name for tested channel. Automatically
    connects DLS client with the node before test starts.

    Copyright:
        Copyright (c) 2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlstest.NeoDlsTestCase;

/*******************************************************************************

    Imports

*******************************************************************************/

import dlstest.DlsTestCase;
import dlstest.DlsClient;

/*******************************************************************************

    Test case base. Actual tests are located in `dlstest.cases`.

*******************************************************************************/

abstract class NeoDlsTestCase : DlsTestCase
{
    /***************************************************************************

        Constructor.

    ***************************************************************************/

    public this()
    {
        super (DlsClient.ProtocolType.Neo);
    }
}
