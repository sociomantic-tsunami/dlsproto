/******************************************************************************

    Base class for Asynchronously/Selector managed DLS requests with database
    channel and record key

    Copyright:
        Copyright (c) 2011-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

 ******************************************************************************/

module dlsproto.client.legacy.internal.request.model.IKeyRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.core.Verify;

import dlsproto.client.legacy.internal.request.model.IChannelRequest;

import dlsproto.client.legacy.DlsConst;

import dlsproto.client.legacy.internal.request.params.RequestParams;




/*******************************************************************************

    IKeyRequest abstract class

*******************************************************************************/

public scope class IKeyRequest : IChannelRequest
{
    /***************************************************************************

        Constructor.

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

        The base class only sends the key (the command and channel have been
        written by the super classes), and calls the abstract
        sendRequestData___(), which sub-classes must implement.

    ***************************************************************************/

    final override protected void sendRequestData__ ( )
    {
        verify(this.params.key.is_single_hash,
            typeof(this).stringof ~ ".sendRequestData: key type mismatch");

        RequestParams.HexDigest key;
        this.params.keyToString(key);

        this.writer.writeArray(key);

        this.sendRequestData___();
    }

    abstract protected void sendRequestData___ ( );
}

