/*******************************************************************************

    Protocol base for DLS `GetNumConnections` request

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.node.request.GetNumConnections;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.meta.types.Qualifiers;

import dlsproto.node.request.model.DlsCommand;

/*******************************************************************************

    Request protocol

*******************************************************************************/

public abstract scope class GetNumConnections : DlsCommand
{
    import dlsproto.client.legacy.DlsConst;

    /***************************************************************************

        Constructor

        Params:
            reader = FiberSelectReader instance to use for read requests
            writer = FiberSelectWriter instance to use for write requests
            resources = object providing resource getters

    ***************************************************************************/

    public this ( FiberSelectReader reader, FiberSelectWriter writer,
        DlsCommand.Resources resources )
    {
        super(DlsConst.Command.E.GetNumConnections, reader, writer, resources);
    }

    /***************************************************************************
    
        Payload struct that holds the data requested

    ***************************************************************************/

    protected struct NumConnectionsData
    {
        mstring address;
        ushort port;
        ulong  num_conns;
    }

    /***************************************************************************

        No data expected for GetNumConnections request

    ***************************************************************************/

    final override protected void readRequestData ( ) { }

    /***************************************************************************

        Write status and response data

    ***************************************************************************/

    final override protected void handleRequest ( )
    {
        auto data = this.getConnectionsData();

        this.writer.write(DlsConst.Status.E.Ok);

        // TODO: is there a need to send the addr/port? surely the client knows this anyway?
        this.writer.writeArray(data.address);
        this.writer.write(data.port);
        this.writer.write(data.num_conns);
    }

    /***************************************************************************

        Must return total num_conns of established connections to this node.

        Returns:
            metadata that includes number of established connections

    ***************************************************************************/

    abstract protected NumConnectionsData getConnectionsData ( );
}
