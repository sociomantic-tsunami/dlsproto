/******************************************************************************

    Asynchronously/Selector managed DLS GetAll request class

    Sends the received key/value pairs to the provided output delegate.

    Copyright:
        Copyright (c) 2011-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

******************************************************************************/

module dlsproto.client.legacy.internal.request.GetAllRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

import dlsproto.client.legacy.internal.request.model.IBulkGetRequest;

import ocean.io.select.client.FiberSelectEvent;

import ocean.meta.types.Qualifiers;


/*******************************************************************************

    GetAllRequest class

*******************************************************************************/

public scope class GetAllRequest : IBulkGetPairsRequest
{
    /**************************************************************************

        Constructor

        Params:
            reader = FiberSelectReader instance to use for read requests
            writer = FiberSelectWriter instance to use for write requests
            resources = shared resources which might be required by the request

    ***************************************************************************/

    public this ( FiberSelectReader reader, FiberSelectWriter writer,
        IDlsRequestResources resources )
    {
        super(reader, writer, resources);
    }


    /***************************************************************************

        Sends the node any data required by the request.

    ***************************************************************************/

    override protected void sendRequestData__ ( )
    {
    }


    /***************************************************************************

        Processes a received record.

        Params:
            key = record key
            value = record value

    ***************************************************************************/

    override protected void processPair ( cstring key, cstring value )
    {
        auto output = params.io_item.get_pair();

        output(this.params.context, key, value);
    }
}

