/*******************************************************************************

    Example main file running fake DLS node. Can be used to debug the
    protocol changes manually (in a more controlled environment).

    Copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module test.dlsfake.main;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import fakedls.DlsNode;

import dlsproto.client.legacy.DlsConst;

import ocean.io.select.EpollSelectDispatcher;
import ocean.io.select.client.TimerEvent;
import ocean.core.MessageFiber;

import ocean.util.log.Log;
import ocean.util.log.AppendConsole;

/*******************************************************************************

    Configure logging

*******************************************************************************/

static this ( )
{
    Log.root.add(new AppendConsole);
    Log.root.level(Level.Trace, true);
}

/*******************************************************************************

    Simple app that starts fake DLS and keeps it running until killed

*******************************************************************************/

void main ( )
{
    auto epoll = new EpollSelectDispatcher;
    auto node = new DlsNode(DlsConst.NodeItem("127.0.0.1".dup, 10000), epoll);

    Log ("Registering fake node");
    node.register(epoll);

    Log("Starting infinite event loop, kill the process if not needed anymore");
    epoll.eventLoop();
}
