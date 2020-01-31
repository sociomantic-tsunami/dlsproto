/*******************************************************************************

    Mixin for shared iteration code

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedls.mixins.ChannelIteration;

/*******************************************************************************

    Common code shared by all requests that implement protocol based on
    dlsproto.node.request.model.CompressedBatch

    Template Params:
        predicate = optional predicate function to filter away some records.
            Defaults to predicate that allows everything.

*******************************************************************************/

public template ChannelIteration ( alias predicate = alwaysTrue )
{
    import fakedls.Storage;

    import ocean.meta.types.Qualifiers;

    /***************************************************************************

        Array of remaining keys in AA to iterate

    ***************************************************************************/

    private istring[] remaining_keys;

    /***************************************************************************

        Key associated with the record values in this.values_for_key

    ***************************************************************************/

    private istring current_key;

    /***************************************************************************

        Array of values associated with the current key

    ***************************************************************************/

    private istring[] values_for_key;

    /***************************************************************************

        Remember iterated channel

    ***************************************************************************/

    private Channel channel;

    /***************************************************************************

        Initialize the channel iterator

        Params:
            channel_name = name of channel to be prepared

        Return:
            `true` if it is possible to proceed with request

    ***************************************************************************/

    override protected bool prepareChannel ( cstring channel_name )
    {
        this.channel = global_storage.get(channel_name);
        if (this.channel !is null)
            this.remaining_keys = this.channel.getKeys();
        else
            this.remaining_keys = null;

        return true;
    }

    /***************************************************************************

        Iterates records for the protocol

        Params:
            key = output value for the next record's key
            value = output value for the next record's value

        Returns:
            `true` if there was data, `false` if request is complete

    ***************************************************************************/

    override protected bool getNext ( out cstring key, out cstring value )
    {
        while (true)
        {
            // Loop over values fetched for the current key
            while (this.values_for_key.length)
            {
                key = this.current_key;
                value = cast(cstring)this.values_for_key[0];
                this.values_for_key = this.values_for_key[1 .. $];

                if (predicate(key, value))
                    return true;
            }

            // Fetch values for the next key
            if (!this.remaining_keys.length)
                return false;

            this.current_key = this.remaining_keys[0];
            this.values_for_key = this.channel.get(this.current_key);
            this.remaining_keys = this.remaining_keys[1 .. $];
        }
    }
}

/*******************************************************************************

    Default predicate which allows all records to be sent to the client.

    Params:
        args = any arguments

    Returns:
        true

*******************************************************************************/

public bool alwaysTrue ( T... ) ( T args )
{
    return true;
}
