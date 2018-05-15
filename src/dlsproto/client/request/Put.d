/*******************************************************************************

    Client DLS Put request definitions / handler.

    Copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.client.request.Put;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;
import ocean.core.SmartUnion;
public import swarm.neo.client.NotifierTypes;
public import swarm.neo.client.request_options.RequestContext;

/*******************************************************************************

    Request-specific arguments provided by the user and passed to the notifier.

*******************************************************************************/

public struct Args
{
    import core.stdc.time;

    mstring channel;
    time_t timestamp;
    void[] value;
    RequestContext context;
}

/*******************************************************************************

    Union of possible notifications.

*******************************************************************************/

private union NotificationUnion
{
    /// The request succeeded.
    NoInfo success;

    /// The request was tried on a node and failed due to a connection error;
    /// it will be retried on any remaining nodes.
    NodeExceptionInfo node_disconnected;

    /// The request was tried on a node and failed due to an internal node
    /// error; it will be retried on any remaining nodes.
    NodeInfo node_error;

    /// The request was tried on a node and failed because it is unsupported;
    /// it will be retried on any remaining nodes.
    RequestNodeUnsupportedInfo unsupported;

    /// The request tried all nodes and failed.
    NoInfo failure;
}

/*******************************************************************************

    Notification smart union.

*******************************************************************************/

public alias SmartUnion!(NotificationUnion) Notification;

/*******************************************************************************

    Type of notifcation delegate.

*******************************************************************************/

public alias void delegate ( Notification, Args ) Notifier;
