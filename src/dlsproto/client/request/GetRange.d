/*******************************************************************************

    Client DLS GetRange request public definitions.

    Copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.client.request.GetRange;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;
import ocean.core.SmartUnion;
public import swarm.neo.client.NotifierTypes;
public import dlsproto.client.NotifierTypes;
public import swarm.neo.client.request_options.RequestContext;
import core.stdc.time: time_t;

/******************************************************************************

    GetRange Filter specifier. Passing this into DlsClient.GetRange
    will make filtering (based on FilterMode) a part of the request.

***************************************************************************/

public struct Filter
{
    /***************************************************************************

        Filter mode. GetRange allows the client to select from several
        different filtering algorithms, as defined by this enum.

    ***************************************************************************/

    public enum FilterMode : ubyte
    {
        /// No filtering should be performed
        None,
        /// Plain string matching filtering.
        StringMatch,
        /// Perl-compatible case sensitive regular expression
        PCRE,
        /// Perl-compatible case insensitive regular expression
        PCRECaseInsensitive
    }

    /// ditto
    FilterMode filter_mode;

    /**************************************************************************

        Filter string.

    **************************************************************************/

    cstring filter_string;
}

/*******************************************************************************

    Request-specific arguments provided by the user and passed to the notifier.

*******************************************************************************/

public struct Args
{
    mstring channel;
    time_t lower_bound;
    time_t upper_bound;
    mstring filter_string;
    Filter.FilterMode filter_mode;
    RequestContext context;
}

/*******************************************************************************

    Enum which is passed to notifications. As the request is handled by all
    known nodes simultaneously, some notifications occur on a per-node basis.

*******************************************************************************/

public union NotificationUnion
{
    /// A value is received from a node.
    RequestRecordInfo received;

    /// The connection to a node disconnected; the request will automatically
    /// restart after reconnection.
    NodeExceptionInfo node_disconnected;

    /// The request was tried on a node and failed due to an internal node
    /// error; it will be continued on any remaining nodes.
    NodeInfo node_error;

    /// The request was tried on a node and failed because it is unsupported;
    /// it will be continued on any remaining nodes.
    RequestNodeUnsupportedInfo unsupported;

    /// All known nodes have either stopped the request (as requested by the
    /// user, via the controller) or are not currently connected. The request is
    /// now finished.
    NoInfo stopped;


    /// The request has finished on all known nodes
    NoInfo finished;
}

/*******************************************************************************

    Notification smart union.

*******************************************************************************/

public alias SmartUnion!(NotificationUnion) Notification;

/*******************************************************************************

    Type of notifcation delegate.

*******************************************************************************/

public alias void delegate ( Notification, Const!(Args) ) Notifier;

/*******************************************************************************

    Request controller, accessible via the client's `control()` method.

    Note that only one control change message can be "in-flight" to the nodes at
    a time. If the controller is used when a control change message is already
    in-flight, the method will return false. The notifier is called when a
    requested control change is carried through.

*******************************************************************************/

public interface IController
{
    /***************************************************************************

        Tells the nodes to stop sending data to this request.

        Returns:
            false if the controller cannot be used because a control change is
            already in progress

    ***************************************************************************/

    bool suspend ( );

    /***************************************************************************

        Tells the nodes to resume sending data to this request.

        Returns:
            false if the controller cannot be used because a control change is
            already in progress

    ***************************************************************************/

    bool resume ( );

    /***************************************************************************

        Tells the nodes to cleanly end the request.

        Returns:
            false if the controller cannot be used because a control change is
            already in progress

    ***************************************************************************/

    bool stop ( );

    /***************************************************************************

        Returns:
            true if the controller is already suspended

    ***************************************************************************/

    bool suspended ( );
}
