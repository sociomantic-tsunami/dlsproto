/*******************************************************************************

    Shared protocol definition of the DLS GetRange request.

    Copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.common.GetRange;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;
import swarm.neo.request.Command;

/*******************************************************************************

    Status code enum. Sent from the node to the client.

*******************************************************************************/

public enum RequestStatusCode : StatusCode
{
    None,      /// Invalid, default value

    Started,   /// GetRange request started
    Finished,  /// GetRange request finished
    Error      /// Internal node error occurred
}

/******************************************************************************

    Message type enum. Each message sent between the client and the node as
    part of the GetRange request is prepended by a type indicator.

******************************************************************************/

public enum MessageType : ubyte
{
    None,    /// Invalid, default value

    // Message types sent from the client to the Cont
    Continue,  /// Requesting the next batch of records
    Stop,      /// Requesting to cleanly end the request

    // Message types sent from the node to the client
    Records,  /// Sent by the node when it sends a records
    Stopped,  /// Acknowledging the request has ended
    Finished, /// Sent by the node when the iteration through record has ended
    Ack,      /// Sent by the client to acknowledge Finished message
    Error     /// Sent by the node when the error during iteration was raised
}

/******************************************************************************

    GetRange request start state enum. Enbles the client to start a GetRange
    request in the suspended state.

******************************************************************************/

public enum StartState : ubyte
{
    Running,
    Suspended
}
