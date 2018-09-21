/*******************************************************************************

    DLS Node emulation environment

    Extends turtle environment node base with methods to directly inspect and
    modify the contents of the fake node.

    Copyright:
        Copyright (c) 2009-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module turtle.env.Dls;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;
import ocean.core.Verify;

import turtle.env.model.TestNode;

import ocean.transition;

import fakedls.DlsNode;
import fakedls.Storage;
import fakedls.ConnectionHandler;

/*******************************************************************************

    Aliases to exceptions thrown on illegal operations with dls storage

    Check `Throws` DDOC sections of methods in this module to see when
    exactly these can be thrown.

*******************************************************************************/

public alias fakedls.Storage.MissingChannelException MissingChannelException;
public alias fakedls.Storage.MissingRecordException MissingRecordException;

/*******************************************************************************

    Returns:
        singleton DLS instance (must first be initialised by calling
        Dls.initialize())

 *******************************************************************************/

public Dls dls()
{
    verify (_dls !is null, "Must call `Dls.initialize` first");
    return _dls;
}

private Dls _dls;


/*******************************************************************************

    The Dls class encapsulates creation/startup of fake DLS node and most
    common operations on data it stores. Only one Dls object is allowed to
    be created.

*******************************************************************************/

public class Dls : TestNode!(DlsConnectionHandler)
{
    import swarm.neo.AddrPort;
    public import dlsproto.client.legacy.DlsConst;
    static import swarm.util.Hash;
    import swarm.node.connection.ConnectionHandler;

    import ocean.core.Enforce;
    import ocean.text.convert.Formatter;
    import ocean.task.Scheduler;
    import swarm.Const: NodeItem;

    import fakedls.neo.RequestHandlers;
    import fakedls.neo.SharedResources;

    /***************************************************************************

        Prepares DLS singleton for usage from tests

TODO

    ***************************************************************************/

    public static void initialize ( cstring addr, ushort port,
        EpollSelectDispatcher epoll )
    {
        if ( !_dls )
        {
            AddrPort node;
            node.setAddress(addr);
            node.port = port;
            _dls = new Dls(node, epoll);
        }
    }

    /***************************************************************************

        Constructor.

        Params:
            node = node addres & port
            TODO

    ***************************************************************************/

    public this ( AddrPort node, EpollSelectDispatcher epoll )
    {
        auto setup = new ConnectionSetupParams;
        setup.epoll = epoll;
        setup.node_info = this;

        Options options;
        options.epoll = epoll;
        options.credentials_map["test"] = Key.init;
        options.requests = requests;
        options.no_delay = true; // favour network turn-around over packet
                                 // efficiency

        // TODO: compare with old code in fakedls.DlsNode
        ushort neo_port = node.port + 1;
        int backlog = 1024;

        super(node, neo_port, setup, options, backlog);
    }

    /***************************************************************************

        Adds a (key, value) pair to the specified DLS channel

        Params:
            channel = name of DLS channel to which the record should be added
            key = key with which to associate the value
            value = value to be added to DLS

    ***************************************************************************/

    public void put ( cstring channel, hash_t key, cstring value )
    {
        enforce(value.length, "Cannot put empty record to the DLS!");

        // The DLS protocol defines keys as strings but env.Dls tries to mimic
        // swarm client API which uses hash_t
        char[swarm.util.Hash.HashDigits] str_key;
        swarm.util.Hash.toHexString(key, str_key);

        global_storage.getCreate(channel).put(str_key.dup, value.dup);
    }

    /***************************************************************************

        Get the records from the specified channel and the specified key.

        Params:
            channel = name of DLS channel from which records should be fetched
            key = the key to get records for

        Throws:
            MissingChannelException if channel does not exist
            MissingRecordException if record with requested key does not exist

        Returns:
            The associated records

    ***************************************************************************/

    public Const!(cstring[]) get ( cstring channel, size_t key )
    {
        // DLS protocol defines keys as strings but env.Dls tries to mimic
        // swarm client API which uses hash_t
        char[swarm.util.Hash.HashDigits] str_key;
        swarm.util.Hash.toHexString(key, str_key);

        return global_storage.getVerify(channel).getVerify(str_key[]);
    }

    /***************************************************************************

        Gets all records in the specified channel.

        Params:
            channel = name of DLS channel from which records should be fetched

        Throws:
            MissingChannelException if channel does not exists

        Returns:
            All records in the channel

    ***************************************************************************/

    public Const!(char[][])[hash_t] getAll ( cstring channel )
    {
        return global_storage.getVerify(channel).getAll();
    }

    /***************************************************************************

        Packs together channel size and length data

    ***************************************************************************/

    struct ChannelSize
    {
        size_t records, bytes;
    }

    /***************************************************************************

        Gets the size of the specified DLS channel (in number of records and
        in bytes) and returns it

        Params:
            channel = name of DLS channel to get size of

        Returns:
            Size of specified channel

    ***************************************************************************/

    public ChannelSize getSize ( cstring channel)
    {
        ChannelSize result;
        if (auto channel_obj = global_storage.get(channel))
            channel_obj.countSize(result.records, result.bytes);
        return result;
    }

    /***************************************************************************

        Removes all data from the fake node service.

    ***************************************************************************/

    override public void clear ( )
    {
        global_storage.clear();
    }

    /***************************************************************************

        Returns:
            identifier string for this node

    ***************************************************************************/

    protected override cstring id ( )
    {
        return "dls";
    }

    /***************************************************************************

        Scope allocates a request resource acquirer instance and passes it to
        the provided delegate for use in a request.

        Params:
            handle_request_dg = delegate that receives a resources acquirer and
                initiates handling of a request

    ***************************************************************************/

    override protected void getResourceAcquirer (
        void delegate ( Object request_resources ) handle_request_dg )
    {
        // In the fake node, we don't actually store a shared resources
        // instance; a new one is simply passed to each request.
        handle_request_dg(new SharedResources);
    }
}

version (UnitTest)
{
    import ocean.core.Test;
    import ocean.io.select.EpollSelectDispatcher;

    void initDls ( )
    {
        global_storage.clear();
        Dls.initialize("127.0.0.1", 10000, new EpollSelectDispatcher);
    }
}

/*******************************************************************************

    Basic put()/get() tests

*******************************************************************************/

unittest
{
    // Put and retrieve a single record
    {
        initDls();
        dls.put("unittest_channel", 123, "abcd"[]);
        auto s = dls.get("unittest_channel", 123);
        test!("==")(s, ["abcd"]);
    }

    // Put and retrieve multiple records to the same key
    {
        initDls();
        dls.put("unittest_channel", 456, "abcd"[]);
        dls.put("unittest_channel", 456, "efgh"[]);
        auto s = dls.get("unittest_channel", 456);
        test!("==")(s, ["abcd", "efgh"]);
    }
}

/*******************************************************************************

    getSize() tests

*******************************************************************************/

unittest
{
    // Empty channel
    {
        initDls();
        auto size = dls.getSize("non_existent_channel");
        test!("==")(size.records, 0);
        test!("==")(size.bytes, 0);
    }

    // Channel with one record
    {
        initDls();
        dls.put("unittest_channel", 123, "abcd"[]);
        auto size = dls.getSize("unittest_channel");
        test!("==")(size.records, 1);
        test!("==")(size.bytes, 4);
    }
}

/*******************************************************************************

    getAll() test

*******************************************************************************/

unittest
{
    {
        initDls();
        dls.put("unittest_channel", 123, "abcd");
        dls.put("unittest_channel", 123, "efgh");
        dls.put("unittest_channel", 345, "test");

        auto s = dls.getAll("unittest_channel");

        test!("==")(s[123].length, 2);
        test!("==")(s[345].length, 1);
    }
}
