/*******************************************************************************

    Fake DLS node neo request shared resources getter.

    Copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedls.neo.SharedResources;

import ocean.transition;
import ocean.util.ReusableException;

import dlsproto.node.neo.request.core.IRequestResources;

import swarm.util.RecordBatcher;
import ocean.io.compress.Lzo;

/*******************************************************************************

    Provides resources required by the protocol. As this implementation is fpr
    testing purposes only, it simply allocates as much stuff as necessary to
    keep the code simple.

*******************************************************************************/

class SharedResources : IRequestResources
{
    /***************************************************************************

        Struct wrapper used to workaround D's inability to allocate slices on
        the heap via `new`.

    ***************************************************************************/

    private static struct Buffer
    {
        void[] data;
    }

    /***************************************************************************

        Returns:
            a new buffer to store record values in

    ***************************************************************************/

    override public void[]* getVoidBuffer ( )
    {
        return &((new Buffer).data);
    }

    /***************************************************************************

        Returns:
            a new exception instance to store the exception during request

    ***************************************************************************/

    override public Exception getException ( )
    {
        return new ReusableException;
    }


    /**************************************************************************

        Returns:
            instance of the RecordBatcher to use during the request's lifetime

    ***************************************************************************/

    override public RecordBatcher getRecordBatcher ( )
    {
        return new RecordBatcher(new Lzo);
    }

    /***************************************************************************

        Returns:
            lzo object for compressing the batches.

    ***************************************************************************/

    override public Lzo getLzo ( )
    {
        return new Lzo();
    }
}
