/*******************************************************************************

    Class to do a node handshake and to retry when it fails

    Copyright:
        Copyright (c) 2012-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.client.legacy.internal.helper.RetryHandshake;

import dlsproto.client.DlsClient;

import ocean.io.select.EpollSelectDispatcher,
               ocean.io.select.client.TimerEvent;

/*******************************************************************************

    Class for doing the node handshake and keep doing it when it fails.
    The handshake callback is optional. When you are just calling the eventloop
    to wait till the handshake is done, you don't need a callback.

    Usage Example:
    ---------
    import dlsproto.client.DlsClient;

    import dlsproto.client.legacy.internal.helper.RetryHandshake;

    import ocean.io.select.EpollSelectDispatcher;

    auto retry_delay_seconds = 3;

    auto nodes_file = "etc/dls.nodes";

    auto epoll = new EpollSelectDispatcher;
    auto dls_client = new DlsClient(epoll);
    dls_client.addNodes(nodes_file);

    void handshakeDone ( )
    {
        // do stuff you want done when handshake was successful
    }

    new RetryHandshake(epoll, dls_client, retry_delay_seconds, &handshakeDone);

    epoll.eventLoop;
    ---------

*******************************************************************************/

class RetryHandshake
{
    /***************************************************************************

        Timer to retry the handshake

    ***************************************************************************/

    protected TimerEvent timer;

    /***************************************************************************

        DLS CLIENT. YES!

    ***************************************************************************/

    protected DlsClient dls;

    /***************************************************************************

        Epoll

    ***************************************************************************/

    protected EpollSelectDispatcher epoll;

    /***************************************************************************

        Time to wait before retrying

    ***************************************************************************/

    protected size_t wait_time;

    /***************************************************************************

        Delegate that will be called on success and kept calling until
        it returns false

    ***************************************************************************/

    protected void delegate ( ) dg;

    /***************************************************************************

        Constructor

        Params:
            epoll = epoll instance
            dls   = dls client
            wait_time = time to wait in seconds
            dg        = function to call on success, optional.

    ***************************************************************************/

    public this ( EpollSelectDispatcher epoll, DlsClient dls,
                  size_t wait_time, void delegate ( ) dg = null )
    {
        this.wait_time = wait_time;

        this.dls = dls;

        this.dg = dg;

        this.epoll = epoll;

        this.timer = new TimerEvent(&this.tryHandshake);

        this.tryHandshake();
    }

    /***************************************************************************

        try doing the handshake

        Returns:
            false, so the timer doesn't stay registered

    ***************************************************************************/

    protected bool tryHandshake ( )
    {
        dls.nodeHandshake(&result, &nodeHandshakeCB);

        return false;
    }

    /***************************************************************************

        handshake callback

        Calls the user delegate on success, else retries the handshake after the
        specified wait time


        Params:
            success = whether the handshake was a success

    ***************************************************************************/

    private void result ( DlsClient.RequestContext, bool success )
    {
        if ( !success )
        {
            this.error();

            this.epoll.register(this.timer);

            this.timer.set(this.wait_time, 0, 0, 0);
        }
        else
        {
            this.success();

            if ( this.dg !is null )
            {
                this.dg();
            }
        }
    }


    /***************************************************************************

        Handshake notifier callback

    ***************************************************************************/

    protected void nodeHandshakeCB ( DlsClient.RequestNotification info ) {   }


    /***************************************************************************

        Called when the handshake failed and it will be retried

    ***************************************************************************/

    protected void error (  ) {    }


    /***************************************************************************

        Called when the handshake succeeded and the user delegate will be called

    ***************************************************************************/

    protected void success ( ) {    }
}
