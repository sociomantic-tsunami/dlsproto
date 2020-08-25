/*******************************************************************************

    PutBatch request handler.

    Copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.client.legacy.internal.request.PutBatchRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

import dlsproto.client.legacy.internal.request.model.IChannelRequest;



public class PutBatchRequest : IChannelRequest
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

    ***************************************************************************/

    override protected void sendRequestData__ ( )
    {
        auto batch = params.io_item.put_batch()(this.params.context);

        auto buf = cast(ubyte[])*this.resources.putbatch_buffer;
        auto compressed = batch.compress(buf);
        this.writer.writeArray(compressed);
    }


    /***************************************************************************

        Handles a request once the request data has been sent and a valid status
        has been received from the node.

    ***************************************************************************/

    override protected void handle__ ( )
    {
    }
}

