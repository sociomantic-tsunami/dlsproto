/******************************************************************************

    Provides global test client instance used from test case to access
    the node.

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

******************************************************************************/

module dlstest.DlsClient;

/******************************************************************************

    Imports

******************************************************************************/

import ocean.transition;
import ocean.core.Verify;
import ocean.core.VersionCheck;
import ocean.util.log.Logger;

/******************************************************************************

    Class that encapsulates fiber/epoll reference and provides functions
    to emulate blocking API for swarm DLS client.

******************************************************************************/

class DlsClient
{
    import dlstest.util.Record;

    import ocean.core.Enforce;
    import ocean.core.Array : copy;
    import ocean.task.Task;
    import ocean.task.Scheduler;
    import ocean.task.util.Event;

    import swarm.util.Hash;

    static import dlsproto.client.DlsClient;
    import swarm.neo.authentication.HmacDef: Key;

    import core.stdc.time;

    /***************************************************************************

        Protocol Type to use.

    ***************************************************************************/

    public enum ProtocolType
    {
        Legacy,
        Neo
    }

    /***************************************************************************

        Helper class to perform a request and suspend the current task until the
        request is finished.

    ***************************************************************************/

    private final class TaskBlockingRequest
    {
        import ocean.io.select.protocol.generic.ErrnoIOException : IOError;
        import swarm.Const : NodeItem;

        /// Task event to suspend/resume the task while a request is handled.
        private TaskEvent task_event;

        /// Counter of the number of request-on-conns which are not finished.
        private uint pending;

        /// Flag per request-on-conn, set to true if it is queued. Used to
        /// ensure that pending is not incremented twice.
        private bool[NodeItem] queued;

        /// Set if an error occurs in any request-on-conn.
        private bool error;

        /// Stores the last error message.
        private mstring error_msg;

        /***********************************************************************

            Should be called after assigning a request. Suspends the task until
            the request finishes and then checks for errors.

            Throws:
                if an error occurred while handling the request

        ***********************************************************************/

        public void wait ( )
        {
            this.task_event.wait();
            enforce(!this.error, idup(this.error_msg));
        }

        /***********************************************************************

            DLS request notifier to pass to the request being assigned.

            Params:
                info = notification info

        ***********************************************************************/

        public void notify ( RawClient.RequestNotification info )
        {
            switch ( info.type )
            {
                case info.type.Queued:
                    this.queued[info.nodeitem] = true;
                    this.pending++;
                    break;

                case info.type.Started:
                    if ( !(info.nodeitem in this.queued) )
                        this.pending++;
                    break;

                case info.type.Finished:
                    if ( !info.succeeded )
                    {
                        info.message(this.error_msg);

                        if ( cast(IOError) info.exception )
                            this.outer.log.warn("Socket I/O failure : {}",
                                this.error_msg);
                        else
                            this.error = true;
                    }

                    if ( --this.pending == 0 )
                        this.task_event.trigger();
                    break;

                default:
            }
        }
    }

   /****************************************************************************

        Reference to common fakedls logger instance

   ****************************************************************************/

    private Logger log;

    /**************************************************************************

        Convenience aliases.

    **************************************************************************/

    alias RawClient.Neo.Filter Filter;


    /**************************************************************************

        Alias for type of the standard DLS client.

    **************************************************************************/

    alias dlsproto.client.DlsClient.DlsClient RawClient;

    /***************************************************************************

        Shared DLS client instance.

    ***************************************************************************/

    private RawClient raw_client;

    /*************************************************************************

        Neo protocol support.

    *************************************************************************/

    private class Neo
    {
        /**********************************************************************

            Flag which is set (by connect()) when a connection error occurs.

        **********************************************************************/

        private bool connection_error;

        /***********************************************************************

            Task event to suspend/resume the task that handles the connection.

        ***********************************************************************/

        private TaskEvent task_event;

        /***********************************************************************

            Waits until either neo connections to all nodes have been
            established (including authentication) or one connection has failed.

        ***********************************************************************/

        public void connect ( )
        {
            scope stats = this.outer.raw_client.neo.new Stats;

            this.connection_error = false;
            while ( stats.num_connected_nodes < stats.num_registered_nodes
                    && !this.connection_error )
            {
                this.task_event.wait();
            }

            enforce(!this.connection_error, "neo connection error");
        }

        /***********************************************************************

            Connection notifier used by the client (see the outer class' ctor).

            Params:
                info = notification info containing the connection attempt status

        ***********************************************************************/

        private void connectionNotifier ( RawClient.Neo.ConnNotification info)
        {
            with (info.Active) switch (info.active)
            {
            case connected:
                log.trace("Neo connection established (on {}:{})",
                    info.connected.node_addr.address_bytes,
                    info.connected.node_addr.port);

                this.task_event.trigger();

                break;
            case error_while_connecting:
                with (info.error_while_connecting)
                {
                    this.connection_error = true;
                    log.error("Neo connection error: {} (on {}:{})",
                            e.message,
                            node_addr.address_bytes, node_addr.port);
                }
                break;
            default:
                assert(false);
            }
        }

        /***********************************************************************

            Performs a neo Put request, suspending the fiber until it is done.

            Params:
                channels = channels to put the record to
                timestamp = timestamp of the record to put
                data = record value to put

            Throws:
                upon failure

        ***********************************************************************/

        public void put ( cstring channel, time_t timestamp, cstring data )
        {
            auto res = this.outer.raw_client.blocking.put(channel, timestamp, data);

            enforce(res.succeeded, "Neo Put request failed on all nodes");
        }

        /*******************************************************************

            Performs a neo GetRange request, suspending the fiber until it is
            done.

            Params:
                channel = channel to put the record to
                low = lower boundary of records to get
                high = higher boundary of records to get
                regex = filter string to filter the records on
                filter_mode = mode of the filtering

            Returns:
                record set received from the node(s).

        *******************************************************************/

        public cstring[][time_t] getRange ( cstring channel,
                time_t low, time_t high,
                cstring filter_string = null,
                Filter.FilterMode filter_mode = Filter.FilterMode.None )
        {
            auto task = Task.getThis();
            verify (task !is null);

            void[] record_buf;
            cstring[][time_t] records;
            bool error = false;
            bool req_finished;

            auto res = this.outer.raw_client.blocking.getRange(channel,
                    record_buf, low, high,
                    Filter(filter_mode, filter_string));

            foreach (key, value; res)
            {
                records[key] ~= cast(char[])(value.dup);
            }

            // Neo returns time_t as a record keys, but our test suite
            // requires hash_t (as that's what the legacy requests were
            // returning).
            return records;
        }

        /***********************************************************************

            Convenience getter for the neo object of the swarm client owned by
            the outer class.

        ***********************************************************************/

        private RawClient.Neo neo_client ( )
        {
            return this.outer.raw_client.neo;
        }
    }


    /**************************************************************************

        Wrapper object containing all neo requests.

    **************************************************************************/

    public Neo neo;


    /**************************************************************************

        Creates DLS client using the task scheduler's epoll instance.

    **************************************************************************/

    public this ( )
    {
        this.log = Log.lookup("dlstest");

        static immutable max_connections = 2;

        this.neo = new Neo;

        auto auth_name = "test";
        auto auth_key = Key.init;

        this.raw_client = new RawClient(theScheduler.epoll, auth_name,
            auth_key.content,
            &this.neo.connectionNotifier, max_connections);
        // deprecated, remove in next major
        static if (!hasFeaturesFrom!("swarm", 5, 1))
        {
            this.raw_client.neo.enableSocketNoDelay();
        }
    }

    /**************************************************************************

        Adds the address/port to listen to on both old and neo client.

        Params:
            port = standard port number to listen to. Neo port defaults to
                   one higher.

    **************************************************************************/

    public void addNode (ushort port)
    {
        this.raw_client.addNode("127.0.0.1".dup, port);
        ushort neo_port = port;
        neo_port++;
        this.neo.neo_client.addNode("127.0.0.1", neo_port);
    }


   /**************************************************************************

        Connects the Dls client with the nodes, on both legacy and neo port.

    **************************************************************************/

    public void connect ( ProtocolType protocol_type )
    {
        TaskEvent task_event;

        if (protocol_type & ProtocolType.Legacy)
        {
            bool handshake_ok = false;

            void handshake_cb (RawClient.RequestContext, bool ok)
            {
                handshake_ok = ok;

                task_event.trigger();
            }

            this.raw_client.nodeHandshake(&handshake_cb, null);

            task_event.wait();

            enforce(handshake_ok, "Test DLS handshake failed");
        }

        if (protocol_type & ProtocolType.Neo)
        {
            this.neo.connect();
        }
    }

    /**************************************************************************

        Adds a (key, data) pair to the specified DLS channel.

        Params:
            channel = name of DLS channel to which data should be added
            key = key with which to associate data
            data = data to be added to DLS

        Throws:
            upon empty record or request error (Exception.msg set to indicate error

    **************************************************************************/

    public void put ( cstring channel, hash_t key, cstring data )
    {
        scope tbr = new TaskBlockingRequest;

        cstring put ( RawClient.RequestContext context )
        {
            return data;
        }

        this.raw_client.assign(
            this.raw_client.put(channel, key, &put, &tbr.notify)
        );
        tbr.wait();
    }


   /***************************************************************************

        Removes the specified channel.

        Params:
            channel = name of dht channel to remove

        Throws:
            upon request error (Exception.msg set to indicate error)

    ***************************************************************************/

    public void removeChannel ( cstring channel )
    {
        scope tbr = new TaskBlockingRequest;

        this.raw_client.assign(
            this.raw_client.removeChannel(channel, &tbr.notify)
        );
        tbr.wait();
    }

    /***************************************************************************

        Gets all items from the specified channel.

        Params:
            channel = name of dls channel from which items should be fetched
            filter = filter string for filtering the data
            pcre = PCRE expression string for filtering the data

        Returns:
            the set of records fetched

        Throws:
            upon request error (Exception.msg set to indicate error)

    ***************************************************************************/

    public cstring[][hash_t] getAll ( cstring channel, cstring filter = null,
                                    cstring pcre = null )
    {
        scope tbr = new TaskBlockingRequest;

        cstring[][hash_t] result;

        bool hash_error;

        void output ( RawClient.RequestContext context, in cstring key, in cstring value )
        {
            if (!isHash(key))
            {
                hash_error = true;
                return;
            }

            result[straightToHash(key)] ~= value.dup;
        }

        auto params = this.raw_client.getAll(channel, &output, &tbr.notify);

        if (filter.length > 0)
        {
            params.filter(filter);
        }

        if (pcre.length > 0)
        {
            params.pcre(pcre);
        }

        this.raw_client.assign(params);
        tbr.wait();

        enforce(!hash_error, "Bad record hash received");

        return result;
    }

    /***************************************************************************

        Indicates if the verifyGetRange should filter, and if so, should it filter
        using string matching or PCRE.

    ***************************************************************************/

    public enum FilterType
    {
        None,
        StringFilter,
        PCRE
    }

    /***************************************************************************

        Gets range of items from the specified channel.

        Params:
            channel = name of dls channel from which items should be fetched
            start = start of the hash range to fetch
            end = end of the hash range to fetch
            filter_type = indicator should GetRange perform filter, and if so, what
                          type
            filter = filter string for filtering the data

        Returns:
            the set of records fetched

        Throws:
            upon request error (Exception.msg set to indicate error)

    ***************************************************************************/


    public cstring[][hash_t] getRange ( cstring channel, hash_t start, hash_t end,
                               FilterType filter_type = FilterType.None, cstring filter = null)
    {
        scope tbr = new TaskBlockingRequest();

        cstring[][hash_t] result;

        bool hash_error;

        void output ( RawClient.RequestContext context, in cstring key, in cstring value )
        {
            if (!isHash(key))
            {
                hash_error = true;
                return;
            }

            result[straightToHash(key)] ~= value.dup;
        }

        auto params =
            this.raw_client.getRange(channel, start, end, &output, &tbr.notify);

        if (filter_type == FilterType.StringFilter)
        {
            params.filter(filter);
        }

        if (filter_type == FilterType.PCRE)
        {
            params.pcre(filter);
        }

        this.raw_client.assign(
            params
        );
        tbr.wait();

        enforce(!hash_error, "Bad record hash received");

        return result;
    }
}
