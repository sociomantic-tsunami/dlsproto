/******************************************************************************

    Table of request handlers by command.

    Copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedls.neo.RequestHandlers;

import swarm.neo.node.ConnectionHandler;
import swarm.neo.request.Command;

import dlsproto.common.RequestCodes;

import fakedls.neo.request.Put;
import fakedls.neo.request.GetRange;


/******************************************************************************

    This table of request handlers by command is used by the connection
    handler.  When creating a new request, the function corresponding to the
    request command is called in a fiber.

******************************************************************************/

public ConnectionHandler.RequestMap requests;

static this ( )
{
    requests.add(Command(RequestCode.Put, 0), "Put", PutImpl_v0.classinfo);
    requests.add(Command(RequestCode.GetRange, 1), "GetRange", GetRangeImpl_v1.classinfo);
}
