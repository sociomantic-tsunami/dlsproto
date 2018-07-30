/*******************************************************************************

    DLS shared resource manager. Handles acquiring / relinquishing of global
    resources by active request handlers.

    Copyright:
        Copyright (c) 2012-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.client.legacy.internal.connection.SharedResources;



/*******************************************************************************

    Imports

    Imports which are required by the DlsConnectionResources struct, below,
    are imported publicly, as they are also needed in
    dlsproto.client.legacy.internal.request.model.IDlsRequestResources (which imports this
    module). This is done to simplify the process of modifying the fields of
    DlsConnectionResources --  forgetting to import something into both
    modules is a common source of very confusing compile errors.

*******************************************************************************/

import swarm.common.connection.ISharedResources;

public import ocean.io.select.client.FiberSelectEvent;

public import swarm.common.request.helper.LoopCeder;

public import swarm.client.request.helper.RequestSuspender;

public import swarm.util.RecordBatcher : RecordBatch;

import swarm.Const;

import ocean.transition;

/*******************************************************************************

    Struct whose fields define the set of shared resources which can be acquired
    by a request. Each request can acquire a single instance of each field.

*******************************************************************************/

public struct DlsConnectionResources
{
    mstring channel_buffer;
    mstring key_buffer;
    mstring value_buffer;
    mstring address_buffer;
    mstring batch_buffer;
    mstring putbatch_buffer;
    ICommandCodes.Value[] codes_list;
    FiberSelectEvent event;
    LoopCeder loop_ceder;
    RecordBatch record_batch;
    RequestSuspender request_suspender;
}



/*******************************************************************************

    Mix in a class called SharedResources which contains a free list for each of
    the fields of DlsConnectionResources. The free lists are used by individual
    requests to acquire and relinquish resources required for handling.

*******************************************************************************/

mixin SharedResources_T!(DlsConnectionResources);

