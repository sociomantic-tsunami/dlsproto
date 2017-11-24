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
    import ocean.core.TypeConvert: downcast;
    import swarm.neo.util.AcquiredResources;
    import swarm.neo.request.RequestEventDispatcher;
    import swarm.util.RecordBatcher;
    import swarm.neo.util.MessageFiber;
    import swarm.neo.client.mixins.RequestCore;


    /***************************************************************************

        Pool of buffers to store record values in. (We store ubyte[] buffers
        internally, as a workaround for ambiguities in ocean.core.Buffer because
        void[][] can be implicitly cast to void[].)

    ***************************************************************************/

    private FreeList!(ubyte[]) buffers;

    /***************************************************************************

        Pool of RequestEventDispatcher instances.

    ***************************************************************************/

    private FreeList!(RequestEventDispatcher) request_event_dispatchers;

    /***************************************************************************

        Pool of MessageFiber instances.

    ***************************************************************************/

    private FreeList!(MessageFiber) fibers;

    /***************************************************************************

        Pool of RecordBatch instances to use.

    ***************************************************************************/

    private FreeList!(RecordBatch) record_batches;

    /***************************************************************************

        LZO instance to use for RecordBatchers

    ***************************************************************************/

    private Lzo lzo;

    /***************************************************************************

        A SharedResource instance is stored in the ConnectionSet as an
        Object. This helper function safely casts from this Object to a
        correctly-typed instance.

        Params:
            obj = object to cast from

        Returns:
            obj cast to SharedResources

    ****************************************************************************/

    public static typeof(this) fromObject ( Object obj )
    {
        auto shared_resources = downcast!(typeof(this))(obj);
        assert (shared_resources !is null);
        return shared_resources;
    }



    /***************************************************************************

        Constructor.

    ***************************************************************************/

    public this ( )
    {
        this.lzo = new Lzo;
        this.buffers = new FreeList!(ubyte[]);
        this.record_batches = new FreeList!(RecordBatch);
        this.request_event_dispatchers = new FreeList!(RequestEventDispatcher);
        this.fibers = new FreeList!(MessageFiber);
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

            Singleton RequestEventDispatcher used by this request.

        ***********************************************************************/

        private AcquiredSingleton!(RequestEventDispatcher) acquired_request_event_dispatcher;


        /***********************************************************************

            Set of acquired fibers.

        ***********************************************************************/

        private Acquired!(MessageFiber) acquired_fibers;

        /***********************************************************************

            Constructor.

        ***********************************************************************/

        this ( )
        {
            this.acquired_void_buffers.initialise(this.outer.buffers);
            this.acquired_record_batches.initialise(this.outer.buffers,
                    this.outer.record_batches);
            this.acquired_fibers.initialise(this.outer.buffers,
                this.outer.fibers);
            this.acquired_request_event_dispatcher.initialise(
                this.outer.request_event_dispatchers
            );
        }

        /***********************************************************************

            Destructor. Relinquishes any acquired resources back to the shared
            resource pools.

        ***********************************************************************/

        ~this ( )
        {
            this.acquired_void_buffers.relinquishAll();
            this.acquired_record_batches.relinquishAll();
            this.acquired_fibers.relinquishAll();
            this.acquired_request_event_dispatcher.relinquish();
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

        /***********************************************************************

            Returns:
                a pointer to a Lzo to use

        ***********************************************************************/

        public Lzo getLzo ( )
        {
            return this.outer.lzo;
        }

        /**********************************************************************

            Returns:
                pointer to singleton (one per request) RequestEventDispatcher
                instance.

        **********************************************************************/

        public RequestEventDispatcher* request_event_dispatcher ()
        {
            return this.acquired_request_event_dispatcher.acquire(
                new RequestEventDispatcher,
                ( RequestEventDispatcher* dispatcher )
                {
                    dispatcher.reset();
                }
            );
        }

        /**********************************************************************

            Gets a fiber to use during the request's lifetime and assigns the
            provided delegate as its entry point.

            Params:
                fiber_method = entry point to assign to acquired fiber

            Returns:
                a new MessageFiber acquired to use during the request's lifetime

        **********************************************************************/

        public MessageFiber getFiber ( void delegate ( ) fiber_method )
        {
            bool new_fiber;

            MessageFiber newFiber ( )
            {
                new_fiber = true;
                return new MessageFiber(fiber_method, 64 * 1024);
            }

            auto fiber = this.acquired_fibers.acquire(newFiber());
            if (!new_fiber)
                fiber.reset(fiber_method);

            return fiber;
        }
    }
}
