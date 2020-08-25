/*******************************************************************************

    Asynchronously/Selector managed DLS GetRegexFilter request class

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.client.legacy.internal.request.GetRangeRegexRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

private import dlsproto.client.legacy.internal.request.model.IBulkGetRequest;

private import ocean.io.select.client.FiberSelectEvent;

import ocean.meta.types.Qualifiers;


/*******************************************************************************

    GetRangeRegexRequest class

*******************************************************************************/

public class GetRangeRegexRequest : IBulkGetPairsRequest
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
        RequestParams.HexDigest start;
        RequestParams.HexDigest end;
        this.params.rangeToString(start, end);

        this.writer.writeArray(start);
        this.writer.writeArray(end);

        this.writer.write(this.params.filter_mode);
        this.writer.writeArray(this.params.filter_string);
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

