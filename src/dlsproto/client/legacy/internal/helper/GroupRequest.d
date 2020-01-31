/*******************************************************************************

    Group request manager alias template.

    Usage example:

    ---

        import ocean.io.select.EpollSelectDispatcher;
        import dlsproto.client.DlsClient;
        import dlsproto.client.legacy.internal.helper.GroupRequest;

        // Initialise epoll, dls and connect to dls
        auto epoll = new EpollSelectDispatcher;
        auto dls = new DlsClient(epoll);
        dls.addNodes("dls.nodes");
        dls.nodeHandshake((DlsClient.RequestContext c, bool ok){}, null);
        epoll.eventLoop;

        // Request notifier
        void notifier ( DlsClient.RequestNotification info )
        {
            with ( typeof(info.type) ) switch ( info.type )
            {
                case Finished:
                    // GetAll on a single node finished
                break;

                case GroupFinished:
                    // GetAlls over all nodes finished
                break;

                default:
            }
        }

        // Set up group request (with imaginary get callback)
        auto request = dls.getAll("channel", &getCb, &notifier);
        auto get_all = makeGroupRequest(request);

        // Run group request
        dls.assign(get_all);
        epoll.eventLoop;

    ---

    Copyright:
        Copyright (c) 2012-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.client.legacy.internal.helper.GroupRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.meta.types.Qualifiers;

import swarm.client.helper.GroupRequest;

import dlsproto.client.DlsClient;

import dlsproto.client.legacy.internal.request.notifier.RequestNotification;

import dlsproto.client.legacy.internal.request.params.RequestParams;

version ( unittest )
{
    import ocean.io.select.EpollSelectDispatcher;
}

/*******************************************************************************

    Group request manager alias template.

    Template params:
        Request = type of request struct to manage (should be one of the structs
            returned by the DLS client request methods)

*******************************************************************************/

public template GroupRequest ( Request )
{
    alias IGroupRequestTemplate!(Request, RequestParams, RequestNotification)
        GroupRequest;
}

/*******************************************************************************

    Instantiates a GroupRequest instance wrapped around the provided Request
    instance. Used to allow template parameter deduction.

    Params:
        Request = type of the request
        req = request to manage using GroupRequest

    Returns:
        instance of the GroupRequest!(Request) managing the req instance.

*******************************************************************************/

public GroupRequest!(Request) makeGroupRequest(Request)(Request req)
{
    return new GroupRequest!(Request)(req);
}

unittest
{
    // just to satisfy the getRange's interface
    void notify ( DlsClient.RequestNotification info )
    {
    }

    void receive_values ( DlsClient.RequestContext context, in cstring timestamp,
        in cstring value )
    {
    }

    auto epoll = new EpollSelectDispatcher();
    auto client = new DlsClient(epoll);
    auto req = client.getRange("channel", 0UL, 0UL, &receive_values, &notify);

    auto groupReq = makeGroupRequest(req);
}
