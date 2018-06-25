/******************************************************************************

    Interface defining public / external methods on a DLS client's node
    registry. Instances of this interface can be safely exposed externally to
    the DLS client.

    Copyright:
        Copyright (c) 2010-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.client.legacy.internal.registry.model.IDlsNodeRegistryInfo;



/*******************************************************************************

    Imports

*******************************************************************************/

import dlsproto.client.legacy.DlsConst;

import swarm.client.registry.model.INodeRegistryInfo;

import dlsproto.client.legacy.internal.connection.model.IDlsNodeConnectionPoolInfo;



/*******************************************************************************

    DLS connection registry interface

*******************************************************************************/

public interface IDlsNodeRegistryInfo : INodeRegistryInfo
{
    /**************************************************************************

        Tells if the client is ready to send requests to all nodes in the
        registry (i.e. they have all responded successfully to the handshake).

        Returns:
            true if all node API versions are known, false otherwise.

     **************************************************************************/

    public bool all_nodes_ok ( );


    /**************************************************************************

        foreach iterator over connection pool info interfaces.

    **************************************************************************/

    public int opApply ( scope int delegate ( ref IDlsNodeConnectionPoolInfo ) dg );
}

