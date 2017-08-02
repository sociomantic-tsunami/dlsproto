/*******************************************************************************

    DLS client request notifier

    Copyright:
        Copyright (c) 2010-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.client.legacy.internal.request.notifier.RequestNotification;



/*******************************************************************************

    Imports

*******************************************************************************/

import swarm.client.request.notifier.IRequestNotification;

import swarm.Const;

import dlsproto.client.legacy.DlsConst;



/*******************************************************************************

    Request notification

*******************************************************************************/

public scope class RequestNotification : IRequestNotification
{
    /***************************************************************************

        Constructor.

        Params:
            command = command of request to notify about
            context = context of request to notify about

    ***************************************************************************/

    public this ( ICommandCodes.Value command, Context context )
    {
        assert(command in DlsConst.Command());

        super(DlsConst.Command(), DlsConst.Status(), command, context);
    }
}

