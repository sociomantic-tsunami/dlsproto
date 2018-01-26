/******************************************************************************

    Provides global test client instance used from test case to access
    the node.

    Copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

******************************************************************************/

module dlstest.DlsClient;

/******************************************************************************

    Imports

******************************************************************************/

import ocean.transition;
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

    import swarm.client.plugins.ScopeRequests;
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

        /// Task instance to be suspended / resumed while the request is handled
        private Task task;

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

            Constructor. Sets this.task to the current task.

        ***********************************************************************/

        public this ( )
        {
            this.task = Task.getThis();
            assert(this.task !is null);
        }

        /***********************************************************************

            Should be called after assigning a request. Suspends the task until
            the request finishes and then checks for errors.

            Throws:
                if an error occurred while handling the request

        ***********************************************************************/

        public void wait ( )
        {
            if ( this.pending > 0 )
                this.task.suspend();
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

                    if ( --this.pending == 0 && this.task.suspended() )
                        this.task.resume();
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

        Alias for type of the standard DLS client.

    **************************************************************************/

    alias dlsproto.client.DlsClient.DlsClient RawClient;

    /***************************************************************************

        Shared DLS client instance.

    ***************************************************************************/

    private RawClient raw_client;

    /**************************************************************************

        Creates DLS client using the task scheduler's epoll instance.

    **************************************************************************/

    public this ( )
    {
        this.log = Log.lookup("dlstest");

        const max_connections = 2;

        auto auth_name = "test";
        auto auth_key = Key.init;

        this.raw_client = new RawClient(theScheduler.epoll, max_connections);
    }

    /**************************************************************************

        Adds the address/port to listen to.

        Params:
            port = standard port number to listen to.

    **************************************************************************/

    public void addNode (ushort port)
    {
        this.raw_client.addNode("127.0.0.1".dup, port);
    }


   /**************************************************************************

        Connects the Dls client with the nodes.

    **************************************************************************/

    public void connect ( ProtocolType protocol_type )
    {
        bool finished;

        auto task = Task.getThis();
        assert (task !is null);

        if (protocol_type & ProtocolType.Legacy)
        {
            bool handshake_ok = false;

            void handshake_cb (RawClient.RequestContext, bool ok)
            {
                finished = true;
                handshake_ok = ok;

                if (task.suspended())
                    task.resume ();
            }

            this.raw_client.nodeHandshake(&handshake_cb, null);

            if (!finished)
                task.suspend();

            enforce(handshake_ok, "Test DLS handshake failed");
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

    enum FilterType
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
