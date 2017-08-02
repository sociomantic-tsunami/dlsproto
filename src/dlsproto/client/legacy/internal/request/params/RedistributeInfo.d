/******************************************************************************

    Struct containing the fields for the RedistributeRequest. These are wrapped
    in a struct in order for them to be conveniently returnable by the user's
    input delegate.

    Copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

******************************************************************************/

module dlsproto.client.legacy.internal.request.params.RedistributeInfo;

public struct RedistributeInfo
{
    import swarm.Const: NodeItem;

    /**************************************************************************

        Fraction of the data that node needs to send to other nodes.

    **************************************************************************/

    public float fraction_of_data_to_send;

    /**************************************************************************

      The list of node address/port which the node receiving the Redistribute
      request should forward data to.

    **************************************************************************/

    public NodeItem[] redist_nodes;
}
