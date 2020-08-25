/*******************************************************************************

    Reusable test runner class for testing any DLS node implementation, based
    on turtle facilities. In most cases, simply providing DLS node binary name
    to runner constructor should be enough.

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlstest.TestRunner;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.meta.types.Qualifiers;

import turtle.runner.Runner;

import dlstest.cases.legacy.Basic;
import dlstest.cases.legacy.Ranges;
import dlstest.cases.legacy.OrderedPut;
import dlstest.cases.legacy.OrderedMultiplePut;
import dlstest.cases.legacy.UnorderedPut;
import dlstest.cases.legacy.UnorderedMultiplePut;
import dlstest.cases.neo.OrderedPut;
import dlstest.cases.neo.OrderedMultiplePut;
import dlstest.cases.neo.UnorderedPut;
import dlstest.cases.neo.UnorderedMultiplePut;

/*******************************************************************************

    Test runner specialized for DLS nodes

*******************************************************************************/

class DlsTestRunner : TurtleRunnerTask!(TestedAppKind.Daemon)
{
    /***************************************************************************

        No additional configuration necessary, assume localhost and
        hard-coded port number (10000)

    ***************************************************************************/

    override public void prepare ( ) { }

    /***************************************************************************

        No arguments but add small startup delay to let DLS node initialize
        listening socket.

    ***************************************************************************/

    override protected void configureTestedApplication ( out double delay,
        out string[] args, out string[string] env )
    {
        delay = 1.0;
        args  = null;
        env   = null;
    }
}
