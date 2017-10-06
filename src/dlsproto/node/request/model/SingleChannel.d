/*******************************************************************************

    Abstract base class for DLS node request protocols over a channel.

    Copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.node.request.model.SingleChannel;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import dlsproto.node.request.model.DlsCommand;
import ocean.text.convert.Formatter;

/*******************************************************************************

    Common base for single channel request protocols

*******************************************************************************/

public abstract scope class SingleChannel : DlsCommand
{
    import dlsproto.client.legacy.DlsConst;
    import swarm.Const : validateChannelName;

    /***************************************************************************

        Pointer to a channel buffer, provided to the constructor. Used to read
        the channel name into.

    ***************************************************************************/

    private mstring* channel_buffer;

    /***************************************************************************

        Constructor

        Params:
            cmd = command code
            reader = FiberSelectReader instance to use for read requests
            writer = FiberSelectWriter instance to use for write requests
            channel_buffer = pointer to buffer to receive channel name

    ***************************************************************************/

    public this ( DlsConst.Command.E cmd, FiberSelectReader reader,
        FiberSelectWriter writer, DlsCommand.Resources resources )
    {
        super(cmd, reader, writer, resources);
        this.channel_buffer = this.resources.getChannelBuffer();
    }

    /***************************************************************************

        Reads any data from the client which is required for the request. If the
        request is invalid in some way then the command can be simply be ignored
        and all client data has been already read, leaving the read buffer in a
        clean state ready for the next request.

    ***************************************************************************/

    final override protected void readRequestData ( )
    {
        this.reader.readArray(*this.channel_buffer);
        this.readChannelRequestData();
    }

    /***************************************************************************
    
        If protocol for derivate request needs any parameters other than
        channel name and request code, this method must be overridden to read
        and store those.

    ***************************************************************************/

    protected void readChannelRequestData ( ) { }


    /***************************************************************************

        If protocol for derivate request needs any processing/validation of input
        data before before handling the request, this method must be overriden
        to process those and to return indicator if the request handling should
        begin.

        Returns:
            true if the request handling should start, false otherwise.

    ***************************************************************************/

    protected bool processAndValidateRequestData ( )
    {
        return true;
    }


    /***************************************************************************

        Validate the channel name that comes from `readRequestData`. 

        Also ensures that channel can be worked with (via method overridden
        in request-specific classes) and makes appropriate status response.

    ***************************************************************************/

    final override protected void handleRequest ( )
    {
        auto channel = *this.channel_buffer;
        if (validateChannelName(channel))
        {
            if (this.prepareChannel(channel) && this.processAndValidateRequestData())
            {
                this.handleChannelRequest(channel);
            }
            else
            {
                this.writer.write(DlsConst.Status.E.Error);
            }
        }
        else
        {
            this.writer.write(DlsConst.Status.E.BadChannelName);
        }
    }

    /***************************************************************************

        Ensures that requested channel exists or can be created and in general
        can be read from or written to. 

        By default this is no-op method that always succeeds.

        Params:
            channel_name = name of channel to be prepared

        Return:
            `true` if it is possible to proceed with request

    ***************************************************************************/

    protected bool prepareChannel ( cstring channel_name ) { return true; }

    /***************************************************************************

        Validate / process any payload specific to exact request type
        implemented by the derivative

        Params:
            channel_name = channel name for request that was read and validated
                earlier
        
    ***************************************************************************/

    abstract protected void handleChannelRequest ( cstring channel_name );

    /***************************************************************************

        Formats a description of this command into the provided buffer. The
        default implementation formats the name of the command and the channel
        on which it operates. Derived request classes may override and add more
        detailed information.

        Params:
            dst = buffer to format description into

        Returns:
            description of command (slice of dst)

    ***************************************************************************/

    override public mstring description ( ref mstring dst )
    {
        super.description(dst);

        auto channel = *this.channel_buffer;
        sformat(dst, " on channel '{}'", channel.length ? channel : "?");
        return dst;
    }
}
