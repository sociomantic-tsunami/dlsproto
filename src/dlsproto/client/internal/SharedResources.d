/*******************************************************************************

    Neo Client shared resource manager. Handles acquiring / relinquishing of global
    resources by active request handlers.

    Copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.client.internal.SharedResources;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import ocean.core.Verify;

/*******************************************************************************

    Resources owned by the client which are needed by the request handlers.

*******************************************************************************/

public final class SharedResources
{
    import ocean.io.compress.Lzo;
    import ocean.io.select.EpollSelectDispatcher;
    import ocean.util.container.pool.FreeList;
    import ocean.core.TypeConvert: downcast;
    import ocean.util.container.pool.AcquiredResources;
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

        Pool of MessageFiber instances.

    ***************************************************************************/

    private FreeList!(MessageFiber) fibers;

    /***************************************************************************

        Pool of RecordBatch instances to use.

    ***************************************************************************/

    private FreeList!(RecordBatch) record_batches;

    /***************************************************************************

        Pool of timer instances.

    ***************************************************************************/

    private FreeList!(Timer) timers;

    /***************************************************************************

        LZO instance to use for RecordBatchers

    ***************************************************************************/

    private Lzo lzo;

    /***************************************************************************

        Epoll instance.

    ***************************************************************************/

    private EpollSelectDispatcher epoll;

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
        verify (shared_resources !is null);
        return shared_resources;
    }



    /***************************************************************************

        Constructor.

        Params:
            epoll = instance of EpollSelectDispatcher to register clients to

    ***************************************************************************/

    public this ( EpollSelectDispatcher epoll )
    {
        this.epoll = epoll;
        this.lzo = new Lzo;
        this.buffers = new FreeList!(ubyte[]);
        this.record_batches = new FreeList!(RecordBatch);
        this.fibers = new FreeList!(MessageFiber);
        this.timers = new FreeList!(Timer);
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

            Set of acquired fibers.

        ***********************************************************************/

        private Acquired!(MessageFiber) acquired_fibers;

        /***********************************************************************

            Set of acquired timers.

        ***********************************************************************/

        private Acquired!(Timer) acquired_timers;

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
            this.acquired_timers.initialise(this.outer.buffers,
                this.outer.timers);
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
            this.acquired_timers.relinquishAll();
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

            Gets a fiber to use during the request's lifetime and assigns the
            provided delegate as its entry point.

            Params:
                fiber_method = entry point to assign to acquired fiber

            Returns:
                a new MessageFiber acquired to use during the request's lifetime

        **********************************************************************/

        public MessageFiber getFiber ( scope void delegate ( ) fiber_method )
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

        /**********************************************************************

            Gets a single-shot timer.

            Params:
                period_ms = timer interval in milliseconds
                timer_dg = delegate to call when timer fires.

            Returs:
                ITimer interface to a timer to use during the request's lifetime.

        **********************************************************************/

        public ITimer getTimer ( uint period_ms, scope void delegate ( ) timer_dg )
        {
            auto timer = this.acquired_timers.acquire(new Timer);
            timer.initialise(period_ms, timer_dg);
            return timer;
        }

        /**********************************************************************

            Inteface to a timer to be used during the request's lifetime.

        **********************************************************************/

        interface ITimer
        {
            /// Starts the timer
            void start ( );

            /// Cancels the scheduled timer
            void cancel ( );
        }
    }

    /**************************************************************************

        Timer class implementing ITimer to be used as a timer
        during the request's lifetime.

    **************************************************************************/

    private class Timer: RequestResources.ITimer
    {
        import ocean.io.select.client.TimerEvent;

        /// Flag to set to true when the timer is enabled
        private bool enabled;

        /// Timer event registered with epoll.
        private TimerEvent timer;

        // User's timer delegate.
        private void delegate ( ) timer_dg;

        /***********************************************************************

            Constructor.

        ***********************************************************************/

        private this ( )
        {
            this.timer = new TimerEvent(&this.timerDg);
        }

        /***********************************************************************

            Sets up the timer period and user delegate.

            Params:
                period_ms = timer period in milliseconds
                timer_dg = delegate to call when timer fires. Note that
                    user must ensure that the delegate stays in a valid
                    state during and after eventual context switch (usually
                    this means delaying context switch for one epoll cycle
                    to give the chance the timer's callback to return).

        ***********************************************************************/

        private void initialise ( uint period_ms,
            scope void delegate ( ) timer_dg )
        {
            this.timer_dg = timer_dg;
            auto period_part_s = period_ms / 1000;
            auto period_part_ms = period_ms % 1000;
            this.timer.set(period_part_s, period_part_ms,
                    period_part_s, period_part_ms);
        }

        /***********************************************************************

            Starts the timer, registering it with epoll.

        ***********************************************************************/

        public void start ( )
        {
            this.enabled = true;
            this.outer.epoll.register(this.timer);
        }

        /***********************************************************************

            Stops the timer, unregistering it from epoll.

        ***********************************************************************/

        public void cancel ( )
        {
            this.enabled = false;
            // remove it from the client list as well as unregister it
            this.outer.epoll.unregister(this.timer, true);
        }

        /***********************************************************************

            Internal delegate called when timer fires. Calls the user's delegate.

            Returns:
                always false, to unregister

        ***********************************************************************/

        private bool timerDg ( )
        {
            if (this.enabled)
            {
                this.timer_dg();
            }

            // one shot timer.
            return false;
        }
    }
}
