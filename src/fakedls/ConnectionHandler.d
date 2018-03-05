/*******************************************************************************

    Forwards DLS requests to handlers in fakedls.request.*

    Copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedls.ConnectionHandler;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import ocean.net.server.connection.IConnectionHandler;
import swarm.node.connection.ConnectionHandler;
import dlsproto.client.legacy.DlsConst;

import fakedls.request.GetRange;
import fakedls.request.GetRangeFilter;
import fakedls.request.GetRangeRegex;
import fakedls.request.GetAll;
import fakedls.request.GetAllFilter;
import fakedls.request.GetChannels;
import fakedls.request.GetChannelSize;
import fakedls.request.GetNumConnections;
import fakedls.request.GetVersion;
import fakedls.request.GetSize;
import fakedls.request.Put;
import fakedls.request.RemoveChannel;

static if (is(typeof(DlsConst.Command.E.Redistribute)))
{
    import fakedls.request.PutBatch;
    import fakedls.request.Redistribute;
}

/*******************************************************************************

    Fake node DLS connection handler. Implements requests in terms
    of trivial array based storage backend.

*******************************************************************************/

public class DlsConnectionHandler :
    ConnectionHandlerTemplate!(DlsConst.Command)
{
    import dlsproto.node.request.model.DlsCommand;
    import ocean.text.regex.PCRE;

    import swarm.Const: NodeItem;


    /***************************************************************************

        Reference to the PCRE object shared between all request handlers.
        This is required by the GetRangeRegex request

    ***************************************************************************/

    private PCRE pcre;


    /***************************************************************************

        Creates resources needed by the protocol in most straighforward way,
        allocating new GC chunk each time.

    ***************************************************************************/

    public scope class DlsRequestResources : DlsCommand.Resources
    {
        import swarm.util.RecordBatcher;
        import ocean.io.compress.Lzo;


        /***********************************************************************

            Backs all resource getters.

            Struct wrapper is used to workaround D inability to allocate slice
            itself on heap via `new`.

        ***********************************************************************/

        struct Buffer
        {
            mstring data;
        }

        /***********************************************************************

            Used to write channel names to

        ***********************************************************************/

        override public mstring* getChannelBuffer ( )
        {
            return &((new Buffer).data);
        }

        /***********************************************************************

            Used to write key arguments to

        ***********************************************************************/

        override public mstring* getKeyBuffer ( )
        {
            return &((new Buffer).data);
        }

        /***********************************************************************

            Used to write key arguments to

        ***********************************************************************/

        override public mstring* getKeyUpperBuffer ( )
        {
            return &((new Buffer).data);
        }

        /***********************************************************************

            Used to write filter argument to

        ***********************************************************************/

        override public mstring* getFilterBuffer ( )
        {
            return &((new Buffer).data);
        }

        /***********************************************************************

            Used to write value argument to

        ***********************************************************************/

        override public mstring* getValueBuffer ( )
        {
            return &((new Buffer).data);
        }

        /***********************************************************************

            Used as target compression buffer

        ***********************************************************************/

        ubyte[]* getCompressBuffer ( )
        {
            return cast(ubyte[]*) &((new Buffer).data);
        }

        /***********************************************************************

            Used as target compression buffer for PutBatch requestsr

        ***********************************************************************/

        ubyte[]* getPutBatchCompressBuffer ( )
        {
            return cast(ubyte[]*) &((new Buffer).data);
        }

        /***********************************************************************

           Object that does data compression

        ***********************************************************************/

        RecordBatcher getRecordBatcher ( )
        {
            return new RecordBatcher(new Lzo);
        }

        /***********************************************************************

           Object that does data decompression for batch requests

        ***********************************************************************/

        RecordBatch getDecompressRecordBatch ( )
        {
            return new RecordBatch(new Lzo);
        }

        /***********************************************************************

            Regex engine.

        ***********************************************************************/

        PCRE.CompiledRegex getRegex()
        {
            return this.outer.pcre.new CompiledRegex;
        }

        /***********************************************************************

                Redistribution is not supported by fake node

        ***********************************************************************/

        override NodeItem[]* getRedistributeNodeBuffer ( )
        {
            assert (false);
        }
    }

    /***************************************************************************

        Constructor.

        Params:
            finalize_dg = user-specified finalizer, called when the connection
                is shut down
            setup = struct containing everything needed to set up a connection

    ***************************************************************************/

    public this (scope void delegate(IConnectionHandler) finalize_dg,
        ConnectionSetupParams setup )
    {
        super(finalize_dg, setup);
        this.pcre = new PCRE;
    }

    /***************************************************************************

        Command code 'None' handler. Treated the same as an invalid command
        code.

    ***************************************************************************/

    override protected void handleNone ( )
    {
        this.handleInvalidCommand();
    }

    /***************************************************************************

        Command code 'GetVersion' handler.

    ***************************************************************************/

    override protected void handleGetVersion ( )
    {
        this.handleCommand!(GetVersion);
    }


    /***************************************************************************

        Command code 'GetNumConnections' handler.

    ***************************************************************************/

    override protected void handleGetNumConnections ( )
    {
        this.handleCommand!(GetNumConnections);
    }


    /***************************************************************************

        Command code 'GetChannels' handler.

    ***************************************************************************/

    override protected void handleGetChannels ( )
    {
        this.handleCommand!(GetChannels);
    }


    /***************************************************************************

        Command code 'GetSize' handler.

    ***************************************************************************/

    override protected void handleGetSize ( )
    {
        this.handleCommand!(GetSize);
    }


    /***************************************************************************

        Command code 'GetChannelSize' handler.

    ***************************************************************************/

    override protected void handleGetChannelSize ( )
    {
        this.handleCommand!(GetChannelSize);
    }


    /***************************************************************************

        Command code 'Put' handler.

    ***************************************************************************/

    override protected void handlePut ( )
    {
        this.handleCommand!(Put);
    }


    /***************************************************************************

        Command code 'GetRange' handler.

    ***************************************************************************/

    override protected void handleGetRange ( )
    {
        this.handleCommand!(GetRange);
    }


    /***************************************************************************

        Command code 'GetRangeFilter' handler.

    ***************************************************************************/

    override protected void handleGetRangeFilter ( )
    {
        this.handleCommand!(GetRangeFilter);
    }


    /***************************************************************************

        Command code 'GetRangeFilter' handler.

    ***************************************************************************/

    override protected void handleGetRangeRegex ( )
    {
        this.handleCommand!(GetRangeRegex);
    }

    /***************************************************************************

        Command code 'GetAll' handler.

    ***************************************************************************/

    override protected void handleGetAll ( )
    {
        this.handleCommand!(GetAll);
    }


    /***************************************************************************

        Command code 'GetAllFilter' handler.

    ***************************************************************************/

    override protected void handleGetAllFilter ( )
    {
        this.handleCommand!(GetAllFilter);
    }


    /***************************************************************************

        Command code 'RemoveChannel' handler.

    ***************************************************************************/

    override protected void handleRemoveChannel ( )
    {
        this.handleCommand!(RemoveChannel);
    }


    static if (is(typeof(DlsConst.Command.E.Redistribute)))
    {
        /***********************************************************************

            Command code 'Redistribute' handler.

        ***********************************************************************/

        override protected void handleRedistribute ( )
        {
            this.handleCommand!(Redistribute);
        }

        /***********************************************************************

            Command code 'PutBatch' handler.

        ***********************************************************************/

        override protected void handlePutBatch ( )
        {
            this.handleCommand!(PutBatch);
        }
    }

    /***************************************************************************

        Command handler method template.

        Template params:
            Handler = type of request handler

    ***************************************************************************/

    private void handleCommand ( Handler : DlsCommand ) ( )
    {
        scope resources = new DlsRequestResources;
        scope handler = new Handler(this.reader, this.writer, resources);
        handler.handle();
    }

    /***************************************************************************

        Called when a connection is finished. Unregisters the reader & writer
        from epoll and closes the connection socket (via
        IConnectionhandler.finalize()).

    ***************************************************************************/

    public override void finalize ( )
    {
        this.writer.fiber.epoll.unregister(this.writer);
        this.writer.fiber.epoll.unregister(this.reader);
        super.finalize();
    }
}
