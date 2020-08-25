/*******************************************************************************

    Abstract base class for DLS client requests.

    Copyright:
        Copyright (c) 2011-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.client.legacy.internal.request.model.IRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

import Core = swarm.client.request.model.IRequest;

import dlsproto.client.legacy.DlsConst;

import dlsproto.client.legacy.internal.request.params.RequestParams;

import dlsproto.client.legacy.internal.request.model.IDlsRequestResources;




/*******************************************************************************

    DLS client IRequest class

*******************************************************************************/

public class IRequest : Core.IRequest
{
    /***************************************************************************

        Aliases for the convenience of sub-classes, avoiding public imports.

    ***************************************************************************/

    protected alias .DlsConst DlsConst;

    protected alias .RequestParams RequestParams;

    protected alias .IDlsRequestResources IDlsRequestResources;


    /***************************************************************************

        Shared resources which might be required by the request.

    ***************************************************************************/

    protected IDlsRequestResources resources;


    /***************************************************************************

        Status code received from DLS node.

    ***************************************************************************/

    protected DlsConst.Status.E status_ = DlsConst.Status.E.Undefined;


    /***************************************************************************

        Constructor.

        Params:
            reader = FiberSelectReader instance to use for read requests
            writer = FiberSelectWriter instance to use for write requests
            resources = shared resources which might be required by the request

    ***************************************************************************/

    public this ( FiberSelectReader reader, FiberSelectWriter writer,
        IDlsRequestResources resources )
    {
        super(reader, writer, resources.fatal_error_exception);

        this.resources = resources;
    }


    /***************************************************************************

        Returns:
            status received from DLS node

    ***************************************************************************/

    public DlsConst.Status.E status ( )
    {
        return this.status_;
    }


    /***************************************************************************

        Sends the node any data required by the request.

        The base class only sends the command, and calls the abstract
        sendRequestData_(), which sub-classes must implement.

    ***************************************************************************/

    final override protected void sendRequestData ( )
    {
        this.writer.write(this.params.command);

        this.sendRequestData_();
    }

    abstract protected void sendRequestData_ ( );


    /***************************************************************************

        Receives the status code from the node.

    ***************************************************************************/

    override protected void receiveStatus ( )
    {
        this.reader.read(this.status_);
    }


    /***************************************************************************

        Decides which action to take after receiving a status code from the
        node.

        Returns:
            action enum value (handle request / skip request / kill connection)

    ***************************************************************************/

    override protected StatusAction statusAction ( )
    {
        if ( this.status_ in DlsConst.Status() )
        {
            with ( DlsConst.Status.E ) switch ( this.status_ )
            {
                case Ok:
                    return StatusAction.Handle;
                case Error:
                    return StatusAction.Fatal;
                default:
                    return StatusAction.Skip;
            }
        }
        else
        {
            return StatusAction.Fatal;
        }
    }


    /***************************************************************************

        Accessor method to cast from the abstract IRequestParams instance in the
        base class to the RequestParams class required by derived classes.

    ***************************************************************************/

    protected RequestParams params ( )
    {
        return cast(RequestParams)this.params_;
    }
}
