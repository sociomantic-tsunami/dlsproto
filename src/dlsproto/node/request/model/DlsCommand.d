/*******************************************************************************

    Abstract base class that acts as a root for all DLS protocol classes

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.node.request.model.DlsCommand;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;
import ocean.core.Verify;

import swarm.node.protocol.Command;

/*******************************************************************************

    Request protocol base

*******************************************************************************/

public abstract scope class DlsCommand : Command
{
    import swarm.util.RecordBatcher;
    import swarm.Const: NodeItem;
    import dlsproto.client.legacy.DlsConst;

    /***************************************************************************
    
        Holds set of method to access temporary resources used by dlsnode
        protocol classes. Those all are placed into single class to simplify
        maintenance and eventually may be replaced with more automatic approach.

    ***************************************************************************/

    public interface Resources
    {
        mstring*            getChannelBuffer ( );
        mstring*            getKeyBuffer ( );
        mstring*            getKeyUpperBuffer ( );
        mstring*            getFilterBuffer ( );
        mstring*            getValueBuffer ( );
        ubyte[]*            getCompressBuffer ( );
        /// much larger buffers than getCompressBuffer
        ubyte[]*            getPutBatchCompressBuffer ( );
        RecordBatcher       getRecordBatcher ( );
        RecordBatch         getDecompressRecordBatch ( );
        NodeItem[]*         getRedistributeNodeBuffer ( );
    }

    /***************************************************************************

        Resource object instance defined by dlsproto implementor. Passed through
        constructor chain from request implementation classes.

    ***************************************************************************/

    protected Resources resources;

    /***************************************************************************

        Constructor

        Params:
            command = command code
            reader = FiberSelectReader instance to use for read requests
            writer = FiberSelectWriter instance to use for write requests
            resources = object providing resource getters

    ***************************************************************************/

    public this ( DlsConst.Command.E command, FiberSelectReader reader,
        FiberSelectWriter writer, Resources resources )
    {
        auto name = command in DlsConst.Command();
        verify(name !is null);
        super(*name, reader, writer);

        verify(resources !is null);
        this.resources = resources;
    }
}
