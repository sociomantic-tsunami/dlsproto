/*******************************************************************************

    Runs dlstest on a fake DHT node instance.

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module integrationtest.fakedls.main;

import dlstest.TestRunner;
import turtle.runner.Runner;

version ( unittest ) {}
else
int main ( string[] args )
{
    auto runner = new TurtleRunner!(DlsTestRunner)("fakedls", "dlstest.cases.neo");
    return runner.main(args);
}
