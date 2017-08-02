/*******************************************************************************

    DLS Node emulation environment

    Extends turtle environment node base with methods to directly inspect and
    modify the contents of the fake node.

    Copyright:
        Copyright (c) 2009-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module turtle.env.Dls;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import turtle.env.model.Node;

import ocean.transition;

import fakedls.DlsNode;
import fakedls.Storage;

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
in
{
    assert (_dls !is null, "Must call `Dls.initialize` first");
}
body
{
    return _dls;
}

private Dls _dls;


/*******************************************************************************

    The Dls class encapsulates creation/startup of fake DLS node and most
    common operations on data it stores. Only one Dls object is allowed to
    be created.

*******************************************************************************/

public class Dls : Node!(DlsNode, "dls")
{
    import dlsproto.client.legacy.DlsConst;
    import Hash = swarm.util.Hash;

    import ocean.core.Enforce;

    import ocean.task.Scheduler;

    /***************************************************************************

        Prepares DLS singleton for usage from tests

    ***************************************************************************/

    public static void initialize ( )
    {
        if ( !_dls )
            _dls = new Dls();
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
        char[Hash.HashDigits] str_key;
        Hash.toHexString(key, str_key);

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
        char[Hash.HashDigits] str_key;
        Hash.toHexString(key, str_key);

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

        Creates a fake node at the specified address/port.

        Params:
            node_item = address/port

    ***************************************************************************/

    override protected DlsNode createNode ( NodeItem node_item )
    {
        auto epoll = theScheduler.epoll();

        auto node = new DlsNode(node_item, epoll);
        node.register(epoll);

        return node;
    }

    /***************************************************************************

        Returns:
            address/port on which node is listening

    ***************************************************************************/

    override public NodeItem node_item ( )
    {
        assert(this.node);
        return this.node.node_item;
    }

    /***************************************************************************

        Stops the fake DLS service. The node may be started again on the same
        port via restart().

    ***************************************************************************/

    override protected void stopImpl ( )
    {
        this.node.stopListener(theScheduler.epoll);
        this.node.shutdown();
    }

    /***************************************************************************

        Removes all data from the fake node service.

    ***************************************************************************/

    override public void clear ( )
    {
        global_storage.clear();
    }

    /***************************************************************************

        Suppresses log output from the fake dls if used version of dlsproto
        supports it.

    ***************************************************************************/

    override public void ignoreErrors ( )
    {
        static if (is(typeof(this.node.ignoreErrors())))
            this.node.ignoreErrors();
    }
}

version (UnitTest)
{
    import ocean.core.Test;

    void initDls ( )
    {
        global_storage.clear();
        Dls.initialize();
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
