module test.fakedls.main;

import dlstest.TestRunner;
import turtle.runner.Runner;
import ocean.transition;

int main ( istring[] args )
{
    auto runner = new TurtleRunner!(DlsTestRunner)("fakedls", "dlstest.cases.neo");
    return runner.main(args);
}
