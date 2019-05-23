/*******************************************************************************

    Implements very simple DLS storage based on an associative array mapping
    from char[] keys to char[][] values (the DLS can store multiple records for
    a single key).

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedls.Storage;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import ocean.core.Enforce;
import Hash = swarm.util.Hash;

/*******************************************************************************

    Global storage used by all requests.

*******************************************************************************/

public DLS global_storage;

/*******************************************************************************

    Wraps channel name to channel object AA in struct with extra convenience
    methods.

*******************************************************************************/

struct DLS
{
    /***************************************************************************

        channel name -> channel object AA

    ***************************************************************************/

    private Channel[cstring] channels;

    /***************************************************************************

        Params:
            channel_name = channel name (id) to look for

        Returns:
            requested channel object if present, null otherwise

    ***************************************************************************/

    public Channel get (cstring channel_name)
    {
        auto channel = channel_name in this.channels;
        if (channel is null)
            return null;
        return *channel;
    }

    /***************************************************************************

        Params:
            channel_name = channel name (id) to look for

        Returns:
            requested channel object

        Throws:
            MissingChannelException if not present

    ***************************************************************************/

    public Channel getVerify ( cstring channel_name )
    {
        auto channel = channel_name in this.channels;
        enforce!(MissingChannelException)(channel !is null, idup(channel_name));
        return *channel;
    }

    /***************************************************************************

        Creates requested channel automatically if it wasn't found

        Params:
            channel_name = channel name (id) to look for

        Returns:
            requested channel object

    ***************************************************************************/

    public Channel getCreate (cstring channel_name)
    {
        auto channel = channel_name in this.channels;
        if (channel is null)
        {
            this.channels[idup(channel_name)] = new Channel;
            channel = channel_name in this.channels;
        }
        return *channel;
    }

    /***************************************************************************

        Removes specified channel from the storage

        Params:
            channel_name = channel name (id) to remove

    ***************************************************************************/

    public void remove (cstring channel_name)
    {
        auto channel = this.get(channel_name);
        if (channel !is null)
        {
            this.channels.remove(channel_name);
        }
    }

    /***************************************************************************

        Empties all channels in the storage

    ***************************************************************************/

    public void clear ( )
    {
        auto names = this.channels.keys;
        foreach (name; names)
        {
            this.getVerify(name).data = null;
        }
    }

    /***************************************************************************

        Returns:
            All channels in the storage as a string array

    ***************************************************************************/

    public cstring[] getChannelList ( )
    {
        cstring[] result;

        foreach (key, value; this.channels)
            result ~= key;

        return result;
    }
}

/*******************************************************************************

    Wraps key -> value AA in a class with extra convenience methods.

*******************************************************************************/

class Channel
{
    /***************************************************************************

        Internal key -> value storage

    ***************************************************************************/

    private istring[][istring] data;

    /***************************************************************************

        Returns:
            keys of all records in the channel as a string array

    ***************************************************************************/

    public istring[] getKeys ( )
    {
        istring[] result;

        foreach (key, value; this.data)
            result ~= key;

        return result;
    }

    /***************************************************************************

        Params:
            key = record key to look for

        Returns:
            requested list of record values if present, null array otherwise

    ***************************************************************************/

    public istring[] get ( cstring key )
    {
        auto value = key in this.data;
        return (value is null) ? null : *value;
    }

    /***************************************************************************

        Returns:
            all value[]/key pairs for the channel

    ***************************************************************************/

    public istring[][hash_t] getAll ( )
    {
        istring[][hash_t] result;

        foreach (key, values; this.data)
        {
            result[Hash.toHash(key)] = values.dup;
        }

        return result;
    }

    /***************************************************************************

        Params:
            key = record key to look for

        Returns:
            requested record value

        Throws:
            MissingRecordException if not present

    ***************************************************************************/

    public istring[] getVerify ( cstring key )
    {
        auto value = key in this.data;
        enforce!(MissingRecordException)(value !is null, idup(key));
        return *value;
    }

    /***************************************************************************

        Adds a new record, appending to the list if a record with the specified
        key is already present.

        Note that both key and value are sliced, not copied. TODO: probably
        would make more sense to dup them at this level, rather than making the
        user responsible for that.

        Params:
            key = record key to write to
            value = new record value

    ***************************************************************************/

    public void put ( cstring key, cstring value )
    {
        this.data[key] ~= idup(value);
    }

    /***************************************************************************

        Counts total size taken by all records

        Params:
            records = will contain total record count
            bytes = will contain total record size

    ***************************************************************************/

    public void countSize (out size_t records, out size_t bytes)
    {
        foreach (key, values; this.data)
        {
            records += values.length;
            foreach (value; values)
            {
                bytes += value.length;
            }
        }
    }
}

/*******************************************************************************

    Exception that indicates invalid operation with non-existent channel

*******************************************************************************/

class MissingChannelException : Exception
{
    this ( cstring name, istring file = __FILE__, int line = __LINE__ )
    {
        super("Trying to work with non-existent channel " ~ idup(name), file, line);
    }
}

/*******************************************************************************

    Exception that indicates invalid operation with non-existent record

*******************************************************************************/

class MissingRecordException : Exception
{
    this ( cstring key, istring file = __FILE__, int line = __LINE__ )
    {
        super("Trying to work with non-existent record (key = " ~ idup(key) ~ ")",
            file, line);
    }
}
