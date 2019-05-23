/*******************************************************************************

    Neo protocol support for DlsClient

    Copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.client.mixins.NeoSupport;

import ocean.core.Tuple;

/***********************************************************************

    Template to strip the notifier from the variadic arguments.

    Params:
        TypeToErase = type to erase from options
        options = Tuple to erase the members matching TypeToErase

    Returns:
        options without any member with type equal to TypeToErase

    TODO:
        this should be moved to RequestOptions module in swarm

***********************************************************************/

template eraseFromArgs (TypeToErase, options ...)
{
    static if (options.length > 0)
    {
        static if (is(typeof(options[0]) == TypeToErase))
        {
            alias options[1..$] eraseFromArgs;
        }
        else
        {
           alias Tuple!(options[0], eraseFromArgs!(TypeToErase, options[1..$]))
               eraseFromArgs;
        }
    }
    else
    {
        alias Tuple!() eraseFromArgs;
    }
}

/*******************************************************************************

    Template wrapping access to all "neo" features. Mixin this class into
    the DlsClient, and construct and use `neo` object for this.

********************************************************************************/

template NeoSupport ()
{
    import dlsproto.client.internal.SharedResources;
    import swarm.neo.client.request_options.RequestOptions;
    import core.stdc.time;
    import ocean.core.Verify;
    import ocean.core.VersionCheck;

    public import dlsproto.common.GetRange;

    /***************************************************************************

        Class wrapping access to all "neo" features. (When the old protocol is
        removed, the contents of this class will be moved into the top level of
        the client class.)

        Usage example:
            see the documented unittest, in UsageExamples

    ***************************************************************************/

    public class Neo
    {
        import swarm.neo.client.mixins.ClientCore;
        // Required otherwise the legacy RequestContext is picked with the new
        // symbol resolution algorithm
        public import swarm.neo.client.request_options.RequestContext : RequestContext;
        import swarm.neo.client.mixins.Controllers;
        import core.stdc.time;

        /***********************************************************************

            Public imports of the request API modules, for the convenience of
            user code.

        ***********************************************************************/

        public import Put = dlsproto.client.request.Put;
        public import GetRange = dlsproto.client.request.GetRange;

        /***********************************************************************

            Public aliases of the request API parameters struct, for the
            convenience of user code.

        ***********************************************************************/

        public alias GetRange.Filter Filter;

        /***********************************************************************

            Private imports of the request implementation modules.

        ***********************************************************************/

        private struct Internals
        {
            import dlsproto.client.request.internal.Put;
            import dlsproto.client.request.internal.GetRange;
        }

        /***********************************************************************

            Mixin core client internals (see
            swarm.neo.client.mixins.ClientCore).

        ***********************************************************************/

        mixin ClientCore!();

        /***********************************************************************

            Mixin `Controller` and `Suspendable` helper class templates (see
            swarm.neo.client.mixins.Controllers).

        ***********************************************************************/

        mixin Controllers!();

        /***********************************************************************

            Test instantiating the `Controller` and `Suspendable` class
            templates.

        ***********************************************************************/

        unittest
        {
            alias Controller!(GetRange.IController) GetRangeController;
            alias Suspendable!(GetRange.IController) GetRangeSuspendable;
        }

        /***********************************************************************

            DLS request stats class. New an instance of this class to access
            per-request stats.

        ***********************************************************************/

        public alias RequestStatsTemplate!("Put", "GetRange") RequestStats;

        /***********************************************************************

            Assigns a Put request, pushing a value to a storage channel.

            Params:
                channel = name of the channel to put to
                timestamp = timestamp of the record to put
                value = value to put (will be copied internally)
                notifier = notifier delegate
                options ... = optional request settings, see below

            Returns:
                id of newly assigned request

            Throws:
                NoMoreRequests if the pool of active requests is full, or
                Exception if there are no nodes registered.

            Optional parameters allowed for this request are (may be specified
            in any order):
                * RequestContext: user-specified data (integer, pointer, Object)
                  associated with this request. Passed to the notifier.

        ***********************************************************************/

        public RequestId put (C, Options...)( cstring channel,
                time_t timestamp, C value, scope Put.Notifier notifier,
                Options options )
        {
            static assert(is(C: Const!(void)[]),"value must be implicitly castable to" ~
                    " Const!(void)[]");

            RequestContext context;
            scope parse_context = (RequestContext context_)
            {
                context = context_;
            };
            setupOptionalArgs!(options.length)(options, parse_context);

            auto params = Const!(Internals.Put.UserSpecifiedParams)(
                    Const!(Put.Args)(channel, timestamp, value),
                    notifier
                );

            auto id = this.assign!(Internals.Put)(params);
            return id;
        }

        /***********************************************************************

            Assigns a GetRange request, getting the values from the specified
            channel and range.

            Params:
                channel = name of the channel to put to
                low = lower bouond
                high = higher bound
                notifier = notifier delegate
                options ... = optional request settings, see below

            Returns:
                id of newly assigned request

            Throws:
                NoMoreRequests if the pool of active requests is full

            Optional parameters allowed for this request are (may be specified
            in any order):
                * RequestContext: user-specified data (integer, pointer, Object)
                  associated with this request. Passed to the notifier.
                * GetRange.Filter: GetRange.Filter struct instructing node to
                  filter results on PCRE or string matching filter

        ***********************************************************************/

        public RequestId getRange (Options...) ( cstring channel, time_t low, time_t high,
            scope GetRange.Notifier notifier, Options options )
        {
            cstring filter_string;
            Filter.FilterMode filter_mode;
            RequestContext context;

            scope parse_context = (RequestContext context_)
            {
                context = context_;
            };
            scope parse_filter = (GetRange.Filter filter)
            {
                filter_string = filter.filter_string;
                filter_mode = filter.filter_mode;
            };

            setupOptionalArgs!(options.length)(options, parse_filter,
                    parse_context
            );

            auto params = Const!(Internals.GetRange.UserSpecifiedParams)(
                        Const!(GetRange.Args)(channel, low, high,
                            filter_string, filter_mode, context),
                        notifier
                    );

            auto id = this.assign!(Internals.GetRange)(params);
            return id;
        }

        /***********************************************************************

            Gets the type of the wrapper struct of the request associated with
            the specified controller interface.

            Params:
                I = type of controller interface

            Evaluates to:
                the type of the request wrapper struct which contains an
                implementation of the interface I

        ***********************************************************************/

        private template Request ( I )
        {
            static if ( is(I == GetRange.IController ) )
            {
                alias Internals.GetRange Request;
            }
            else
            {
                static assert(false, I.stringof ~ " does not match any request "
                    ~ "controller");
            }
        }

        /***********************************************************************

            Gets access to a controller for the specified request. If the
            request is still active, the controller is passed to the provided
            delegate for use.

            Important usage notes:
                1. The controller is newed on the stack. This means that user
                   code should never store references to it -- it must only be
                   used within the scope of the delegate.
                2. As the id which identifies the request is only known at run-
                   time, it is not possible to statically enforce that the
                   specified ControllerInterface type matches the request. This
                   is asserted at run-time, though (see
                   RequestSet.getRequestController()).

            Params:
                ControllerInterface = type of the controller interface (should
                    be inferred by the compiler)
                id = id of request to get a controller for (the return value of
                    the method which assigned your request)
                dg = delegate which is called with the controller, if the
                    request is still active

            Returns:
                false if the specified request no longer exists; true if the
                controller delegate was called

        ***********************************************************************/

        public bool control ( ControllerInterface ) ( RequestId id,
            scope void delegate ( ControllerInterface ) dg )
        {
            alias Request!(ControllerInterface) R;

            return this.controlImpl!(R)(id, dg);
        }

        /***********************************************************************

            Test instantiating the `control` function template.

        ***********************************************************************/

        unittest
        {
            alias control!(GetRange.IController) getRangeControl;
        }
    }

    /***************************************************************************

        Class wrapping access to all task-blocking "neo" features. (This
        functionality is separated from the main neo functionality as it
        implements methods with the same names and arguments (e.g. a callback-
        based Put request).)

        Usage example:
            see the documented unittest, in UsageExamples module

    ***************************************************************************/

    private class TaskBlocking
    {
        import ocean.core.Array : copy;
        import ocean.task.Task;

        import swarm.neo.client.mixins.TaskBlockingCore;
        import swarm.neo.client.request_options.RequestContext;

        /***********************************************************************

            Mixin core client task-blocking internals (see
            swarm.neo.client.mixins.TaskBlockingCore.

        ***********************************************************************/

        mixin TaskBlockingCore!();

        /***********************************************************************

            Struct returned after a Put request has finished.

        ***********************************************************************/

        private static struct PutResult
        {
            /*******************************************************************

                Set to true if the record was put into the DLS or false if this
                was not possible (i.e. the request was attempted on all nodes
                and failed on all).

            *******************************************************************/

            bool succeeded;
        }

        /***********************************************************************

            Assigns a Put request to the channel and blocks the current
            Task until the request is completed.

            Params:
                channel = name of the channel to put into
                key = key of the record to put
                value = value to put (will be copied internally)
                options... = optional request settings, see below

            Returns:
                PutResult struct, indicating the result of the request

            Throws:
                NoMoreRequests if the pool of active requests is full, or
                Exception if there are no nodes registered.

            Optional parameters allowed for this request are (may be specified
            in any order):
                * RequestContext: user-specified data (integer, pointer, Object)
                  associated with this request. Passed to the notifier.
                * Neo.Put.PutNofifier: notifier delegate, not required for
                  feedback on basic success/failure, but may be desired for more
                  detailed error logging.

        ***********************************************************************/

        public PutResult put(C, Options...) ( cstring channel,
            time_t key, C value,
            Options options)
        {
            static assert(is(C: Const!(void)[]),"value must be implicitly castable to" ~
                    " void[]");

            auto task = Task.getThis();
            verify(task !is null,
                    "This method may only be called from inside a Task");

            enum FinishedStatus
            {
                None,
                Succeeded,
                Failed
            }

            Neo.Put.Notifier user_notifier;
            FinishedStatus state;


            scope parse_notifier = (Neo.Put.Notifier notifier)
            {
                user_notifier = notifier;
            };

            setupOptionalArgs!(options.length)(options,
                    // explicit `delegate` is needed here,
                    // because in D2 this is a function, since it doesn't
                    // use outer context
                    delegate (Neo.RequestContext context)
                    {
                        // Unused here. Passed through to Neo.put()
                    },
                    parse_notifier
            );

            scope notifier = ( Neo.Put.Notification info, Const!(Neo.Put.Args) args )
            {
                if ( user_notifier )
                    user_notifier(info, args);

                with ( info.Active ) switch ( info.active )
                {
                    case success:
                        state = state.Succeeded;
                        if ( task.suspended )
                            task.resume();
                        break;

                    case failure:
                        state = state.Failed;
                        if ( task.suspended )
                            task.resume();
                        break;

                    case node_disconnected:
                    case node_error:
                    case unsupported:
                        break;

                    default: assert(false);
                }
            };

            this.outer.neo.put(channel, key, value,
                    notifier,
                    eraseFromArgs!(Neo.Put.Notifier, options));

            if ( state == state.None ) // if request not completed, suspend
                task.suspend();
            verify(state != state.None);

            PutResult res;
            res.succeeded = state == state.Succeeded;
            return res;
        }

        /***********************************************************************

            Struct to wrap the result of task-blocking GetRange and to provide
            opApply().

            Node that the Task-blocking GetRange consciously provides a
            simplistic, clean API, without any of the more advanced features of
            the request (e.g. suspending). If you need these features, please
            use the callback-based version of the request.

        ***********************************************************************/

        public struct GetRangeResult
        {
            import ocean.core.array.Mutation: copy;

            /// User task to resume/suspend
            private Task task;

            /// Timestamp of the current record
            private time_t record_key;

            /// Value of the current record
            private void[]* record_value;

            /// Possible states of the request
            private enum State
            {
                /// The request is running
                Running,

                /// The user has stopped this request by breaking from foreach
                /// (the request may still be running for some time, but all
                /// records will be ignored.
                Stopped,

                /// The request has finished on all nodes
                Finished
            }

            // Indicator of the request's state.
            private State state;

            /// User notifier to call
            private Neo.GetRange.Notifier user_notifier;

            /// Channel to iterate over
            private cstring channel;

            /// Lower timestamp boundary
            private time_t low;

            /// Higher timestamp boundary
            private time_t high;

            /// Filter provided by user
            private Neo.GetRange.Filter filter;

            /// Neo DlsClient instance
            private Neo neo;

            /// error indicator
            public bool error;

            /// Request id (used internally)
            private DlsClient.Neo.RequestId rq_id;

            /*******************************************************************

                Notifier used to set the local values and resume task

                Params:
                    info = information and payload about the event user has
                           been notified about
                    args = arguments passed by the user when starting request

            *******************************************************************/

            private void notifier ( DlsClient.Neo.GetRange.Notification info,
                Const!(DlsClient.Neo.GetRange.Args) args )
            {
                if (this.user_notifier)
                {
                    this.user_notifier(info, args);
                }

                with ( info.Active ) switch ( info.active )
                {
                    case received:
                        // Ignore all received value on user break
                        if (this.state == State.Stopped)
                            break;

                        // Store the received value
                        this.record_key = info.received.key;

                        copy(*this.record_value, info.received.value);
                        enableStomping(*this.record_value);

                        if (this.task.suspended())
                        {
                            this.task.resume();
                        }

                        break;

                    case stopped:
                    case finished:
                        // Even if the user has requested stopping,
                        // but finished arrived, we will just finish and exit
                        this.state = State.Finished;
                        this.task.resume();
                        break;

                    case node_disconnected:
                    case node_error:
                    case unsupported:
                        // Ignore all errors on user break
                        if (this.state == State.Stopped)
                            break;

                        this.error = true;
                        break;

                    default: assert(false);
                }
            }

            /*******************************************************************

                Task-blocking opApply iteration over GetRange

            *******************************************************************/

            public int opApply (scope int delegate(ref time_t key,
                        ref void[] value) dg)
            {
                int ret;

                this.rq_id = this.neo.getRange(this.channel, this.low,
                        this.high, &this.notifier,
                        this.filter);

                while (this.state != State.Finished)
                {
                    Task.getThis().suspend();

                    // no more records
                    if (this.state == State.Finished
                            || this.state == State.Stopped
                            || this.error)
                        break;

                    ret = dg(this.record_key, *this.record_value);

                    if (ret)
                    {
                        this.state = State.Stopped;

                        this.neo.control(this.rq_id,
                            ( DlsClient.Neo.GetRange.IController get_range )
                            {
                                get_range.stop();
                            });

                        // Wait for the request to finish
                        Task.getThis().suspend();
                        break;
                    }
                }

                return ret;
            }
        }

        /***********************************************************************

            Assigns a task blocking GetRange request, getting the values from
            the specified channel and range. This method only provides
            nothing but the most basic usage (no request context, no way to
            control the request (stop/resume/suspend)), so if that is needed,
            please use non-task blocking getRange.

            Params:
                channel = name of the channel to get the records from
                record_buffer = reusable buffer to store the current record's
                                values into
                low = lower bouond
                high = higher bound
                notifier = notifier delegate
                options ... = optional request settings, see below

            Returns:
                GetRangeResult structure, whose opApply should be used

            Throws:
                NoMoreRequests if the pool of active requests is full

            Optional parameters allowed for this request are (may be specified
            in any order):
                * GetRange.Filter: GetRange.Filter struct instructing node to
                  filter results on PCRE or string matching filter
                * Neo.GetRange.Nofifier: notifier delegate, not required for
                  feedback on basic success/failure, but may be desired for more
                  detailed error logging.

        ***********************************************************************/

        public GetRangeResult getRange (C, Options...) ( C channel,
            ref void[] record_buffer,
            time_t low, time_t high,
            Options options )
        {
            static assert(is(C: Const!(void)[]),"value must be implicitly castable to" ~
                    " Const!(void)[]");

            auto task = Task.getThis();
            verify(task !is null,
                    "This method may only be called from inside a Task");

            GetRangeResult res;
            res.task = task;
            res.neo = this.outer.neo;
            res.record_value = &record_buffer;
            res.channel = channel;
            res.low = low;
            res.high = high;

            scope parse_filter = (Neo.GetRange.Filter filter)
            {
                // Unused here. Passed through to Neo.getRange()
                res.filter = filter;
            };

            scope parse_notifier = (Neo.GetRange.Notifier notifier)
            {
                res.user_notifier = notifier;
            };

            setupOptionalArgs!(options.length)(options,
                parse_filter, parse_notifier
            );

            return res;
        }
    }

    version ( UnitTest )
    {
        import ocean.task.Scheduler;
        import ocean.task.Task;
    }

    unittest
    {
        // Test optional arguments
        static class PutWithOptionalArgsTask : Task
        {
            private DlsClient dls;

            this ( DlsClient dls )
            {
                this.dls = dls;
            }

            private void putNotifier ( DlsClient.Neo.Put.Notification info,
                Const!(DlsClient.Neo.Put.Args) args )
            {
            }

            override public void run ( )
            {
                auto result = this.dls.blocking.put("channel".dup, 0x1234,
                    "value_to_put".dup, &this.putNotifier, Neo.RequestContext(2));

                result = this.dls.blocking.put("channel".dup, 0x1234,
                    "value_to_put".dup, Neo.RequestContext(2));

                result = this.dls.blocking.put("channel".dup, 0x1234,
                    "value_to_put".dup);
            }
        }
    }

    /***************************************************************************

        Object containing all neo task-blocking functionality.

    ***************************************************************************/

    public TaskBlocking blocking;


    /***************************************************************************

        Object containing all neo functionality.

    ***************************************************************************/

    public Neo neo;

    /***************************************************************************

        Helper function to initialise neo components.

        Params:
            auth_name = client name for authorisation
            auth_key = client key (password) for authorisation. This should be a
                properly generated random number which only the client and the
                nodes know. See `swarm/README_client_neo.rst` for suggestions.
                The key must be of the length defined in
                swarm.neo.authentication.HmacDef (128 bytes)
            conn_notifier = delegate which is called when a connection attempt
                succeeds or fails (including when a connection is
                re-established). Of type:
                void delegate ( IPAddress node_address, Exception e )

    ***************************************************************************/

    private void neoInit ( cstring auth_name, ubyte[] auth_key,
        scope Neo.ConnectionNotifier conn_notifier )
    {
        this.neo = new Neo(auth_name, auth_key,
                        Neo.Settings(conn_notifier, new SharedResources(this.epoll)));
        // deprecated, remove in next major
        static if (!hasFeaturesFrom!("swarm", 5, 1))
        {
            this.neo.enableSocketNoDelay();
        }
        this.blocking = new TaskBlocking;
    }

    /***************************************************************************

        Helper function to initialise neo components. Automatically calls
        addNodes() with the node definition files specified in the Config
        instance.

        Params:
            config = swarm.neo.client.mixins.ClientCore.Config instance.
                (The config class is designed to be read from an application's
                config.ini file via ocean.util.config.ConfigFiller).
            conn_notifier = delegate which is called when a connection attempt
                succeeds or fails (including when a connection is
                re-established). Of type:
                void delegate ( IPAddress node_address, Exception e )

    ***************************************************************************/

    private void neoInit ( Neo.Config config,
        scope Neo.ConnectionNotifier conn_notifier )
    {
        this.neo = new Neo(config, Neo.Settings(conn_notifier, new SharedResources(this.epoll)));
        // deprecated, remove in next major
        static if (!hasFeaturesFrom!("swarm", 5, 1))
        {
            this.neo.enableSocketNoDelay();
        }
        this.blocking = new TaskBlocking;
    }
}
