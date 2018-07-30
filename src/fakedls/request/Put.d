/*******************************************************************************

    Naive implementation of DLS `Put` request

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedls.request.Put;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import Protocol = dlsproto.node.request.Put;

/*******************************************************************************

    Request implementation

*******************************************************************************/

public scope class Put : Protocol.Put
{
    import fakedls.mixins.RequestConstruction;
    import fakedls.Storage;

    /***************************************************************************

        Adds this.resources and constructor to initialize it and forward
        arguments to base

    ***************************************************************************/

    mixin RequestConstruction!();

    /***************************************************************************

        Verifies that this node is allowed to store records of given size

        Params:
            size = size to check

        Returns:
            'true' if size is allowed

    ***************************************************************************/

    override protected bool isSizeAllowed ( size_t size )
    {
        return true;
    }

    /***************************************************************************

        Tries storing record in DLS and reports success status

        Params:
            channel = channel to write record to
            key = record key
            value = record value

        Returns:
            'true' if storing was successful

    ***************************************************************************/

    override protected bool putRecord ( cstring channel, cstring key, cstring value )
    {
        global_storage.getCreate(channel).put(key, value);
        return true;
    }
}
