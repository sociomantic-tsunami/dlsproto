/*******************************************************************************

    Information about a DLS connection pool

    Copyright:
        Copyright (c) 2011-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.client.legacy.internal.connection.model.IDlsNodeConnectionPoolInfo;



/*******************************************************************************

    Imports

*******************************************************************************/

import swarm.client.connection.model.INodeConnectionPoolInfo;



public interface IDlsNodeConnectionPoolInfo : INodeConnectionPoolInfo
{
    /***************************************************************************

        Returns:
            true if the API version for this pool has been queried and matches
            the client's

    ***************************************************************************/

    public bool api_version_ok ( );


}

