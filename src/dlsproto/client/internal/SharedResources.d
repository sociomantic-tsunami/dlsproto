/*******************************************************************************

    Neo Client shared resource manager. Handles acquiring / relinquishing of global
    resources by active request handlers.

    Copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.client.internal.SharedResources;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

/*******************************************************************************

    Resources owned by the client which are needed by the request handlers.

*******************************************************************************/

public final class SharedResources
{
    import ocean.io.compress.Lzo;
    import ocean.util.container.pool.FreeList;
    import swarm.neo.util.AcquiredResources;
    import swarm.util.RecordBatcher;
    import swarm.neo.client.mixins.RequestCore;


    /***************************************************************************

        Pool of buffers to store record values in. (We store ubyte[] buffers
        internally, as a workaround for ambiguities in ocean.core.Buffer because
        void[][] can be implicitly cast to void[].)

    ***************************************************************************/

    private FreeList!(ubyte[]) buffers;

    /***************************************************************************

        Pool of RecordBatch instances to use.

    ***************************************************************************/

    private FreeList!(RecordBatch) record_batches;

    /***************************************************************************

        LZO instance to use for RecordBatchers

    ***************************************************************************/

    private Lzo lzo;

    /***************************************************************************

        Constructor.

    ***************************************************************************/

    public this ( )
    {
        this.lzo = new Lzo;
        this.buffers = new FreeList!(ubyte[]);
        this.record_batches = new FreeList!(RecordBatch);
    }

    /***************************************************************************

        Scope class which may be newed inside request handlers to get access to
        the shared pools of resources. Any acquired resources are relinquished
        in the destructor.

        The class should always be newed as scope, but cannot be declared as
        such because the request handler classes need to store a reference to it
        as a member, which is disallowed for scope instances.

    ***************************************************************************/

    public /*scope*/ class RequestResources
    {
        /***********************************************************************

            Acquired void arrays.

        ***********************************************************************/

        private AcquiredArraysOf!(void) acquired_void_buffers;

        /***********************************************************************

            Acquired RecordBatches

        ***********************************************************************/

        private Acquired!(RecordBatch) acquired_record_batches;

        /***********************************************************************

            Constructor.

        ***********************************************************************/

        this ( )
        {
            this.acquired_void_buffers.initialise(this.outer.buffers);
            this.acquired_record_batches.initialise(this.outer.buffers,
                    this.outer.record_batches);
        }

        /***********************************************************************

            Destructor. Relinquishes any acquired resources back to the shared
            resource pools.

        ***********************************************************************/

        ~this ( )
        {
            this.acquired_void_buffers.relinquishAll();
            this.acquired_record_batches.relinquishAll();
        }


        /***********************************************************************

            Returns:
                a pointer to a new chunk of memory (a void[]) to use during the
                request's lifetime

        ***********************************************************************/

        public void[]* getVoidBuffer ( )
        {
            return this.acquired_void_buffers.acquire();
        }

        /***********************************************************************

            Returns:
                a pointer to a new RecordBatch to use during the request's
                lifetime

        ***********************************************************************/

        public RecordBatch getRecordBatch ( )
        {
            return this.acquired_record_batches.acquire(
                    new RecordBatch(this.outer.lzo));
        }
    }
}
