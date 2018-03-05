/******************************************************************************

    Protocol base for Dls `Redistribute` request.

    Copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

******************************************************************************/

module dlsproto.node.request.Redistribute;

/******************************************************************************

    Imports

******************************************************************************/

import ocean.transition;

import dlsproto.node.request.model.DlsCommand;
import ocean.transition;
import ocean.util.log.Logger;
import dlsproto.client.legacy.DlsConst;

static if (!is(typeof(DlsConst.Command.E.Redistribute))) {}
else:

/******************************************************************************

    Static module logger.

******************************************************************************/

private Logger log;

static this ( )
{
    log = Log.lookup("dlsproto.node.request.Redistribute");
}

/******************************************************************************

    Request Protocol

******************************************************************************/

public abstract scope class Redistribute : DlsCommand
{
    public import swarm.Const: NodeItem;

    /**************************************************************************

        Only a single Redistribute request may be handled at a time.
        This global counter is incremented in the ctor and decremented in the
        dtor. The handler methods checks that it is == 1, and returns an error
        code to the client otherwise.

    **************************************************************************/

    private static uint instance_count;


    /**************************************************************************

        Fraction of the data to send away. Received from the client which
        sent the request.

    **************************************************************************/

    private float fraction_of_data_to_send;


    /**************************************************************************

        Pointer to external data buffer userd to store incoming redistribution
        data.

    **************************************************************************/

    private NodeItem[]* redistribute_node_buffer;


    /**************************************************************************

        Constructor

        Params:
            reader = FiberSelectReader instance to use for read requests
            writer = FiberSelectWriter isntance to use for write requests
            resorces = object providing resource getters

    **************************************************************************/

    public this ( FiberSelectReader reader, FiberSelectWriter writer,
            DlsCommand.Resources resources)
    {
        super(DlsConst.Command.E.Redistribute, reader, writer, resources);

        this.redistribute_node_buffer =
            this.resources.getRedistributeNodeBuffer();

        ++this.instance_count;
    }


    /**************************************************************************

        Destructor

    **************************************************************************/

    ~this()
    {
        --this.instance_count;
    }


    /**************************************************************************

        Reads redistribution data and does basic deserialization.

    **************************************************************************/

    final override protected void readRequestData ( )
    {
        this.reader.read(this.fraction_of_data_to_send);

        (*this.redistribute_node_buffer).length = 0;
        enableStomping(*this.redistribute_node_buffer);

        while (true)
        {
            (*this.redistribute_node_buffer).length =
                (*this.redistribute_node_buffer).length + 1;
            auto next = &((*this.redistribute_node_buffer)[$ - 1]);

            this.reader.readArray(next.Address);
            if (next.Address.length == 0)
                break;

            this.reader.read(next.Port);
        }

        // cut off final "eof" marker
        (*this.redistribute_node_buffer).length =
            (*this.redistribute_node_buffer).length - 1;
        enableStomping(*this.redistribute_node_buffer);
    }

    /**************************************************************************

        Validates if the fraction of data to send away is inside (0, 1) range
        and calls derivative methods to do actual redistribution which is 100%
        implementation derived.

    **************************************************************************/

    final override protected void handleRequest ( )
    {
        if (this.instance_count > 1)
        {
            log.error("Attempted multiple simultaneous Redistribute requests");
            this.writer.write(DlsConst.Status.E.Error);
            return;
        }

        if (this.fraction_of_data_to_send <= 0
                || this.fraction_of_data_to_send > 1)
        {
            log.error("Invalid indicator of fraction of data to send.");
            this.writer.write(DlsConst.Status.E.Error);
            return;
        }

        this.redistributeData(*this.redistribute_node_buffer,
                this.fraction_of_data_to_send);

        this.writer.write(DlsConst.Status.E.Ok);
    }

    /***************************************************************************

        Process actual redistribution in an implementation-defined way

    ***************************************************************************/

    abstract protected void redistributeData ( NodeItem[] dataset,
           float fraction_of_data_to_send );
}

