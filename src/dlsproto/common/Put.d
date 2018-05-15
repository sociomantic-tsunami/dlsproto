/*******************************************************************************

    Shared protocol definition of the DLS Put request.

    Copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.common.Put;

/*******************************************************************************

    Imports

*******************************************************************************/

import swarm.neo.request.Command;

/*******************************************************************************

    Status code enum. Sent from the node to the client.

*******************************************************************************/

public enum RequestStatusCode : StatusCode
{
    None,   // Invalid, default value

    Put,    // Value written to storage
    Error   // Internal node error occurred
}
