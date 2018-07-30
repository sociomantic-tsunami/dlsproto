/******************************************************************************

    Redistribute request class.

    Copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

******************************************************************************/

module dlsproto.client.legacy.internal.request.RedistributeRequest;



/******************************************************************************

    Imports

******************************************************************************/

import dlsproto.client.legacy.internal.request.model.IRequest;


/******************************************************************************

    Redistribute request.

******************************************************************************/

public scope class RedistributeRequest: IRequest
{
    /**************************************************************************

        Constructor

        Params:
            reader = FiberSelectReader instance to use for read requests
            write = FiberSelectWriter instance to use for write requests
            resorces = shared resources which might be required by the request

    **************************************************************************/

    public this ( FiberSelectReader reader, FiberSelectWriter writer,
            IDlsRequestResources resources )
    {
        super (reader, writer, resources);
    }


    /**************************************************************************

        Sends the node any data required by the request.

    **************************************************************************/

    override protected void sendRequestData_ ( )
    {
        auto input = this.params.io_item.redistribute();
        auto redist_info = input(this.params.context);

        this.writer.write(redist_info.fraction_of_data_to_send);
        foreach ( node; redist_info.redist_nodes )
        {
            this.writer.writeArray(node.Address);
            this.writer.write(node.Port);
        }

        this.writer.writeArray([]);
    }

    /**************************************************************************

        Handles a request once the request data has been sent and a valid status
        has been received from the node.

   ***************************************************************************/

    override protected void handle__ ( )
    {
    }
}
