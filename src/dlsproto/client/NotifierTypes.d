/*******************************************************************************

    Types passed to client request notifier delegates.

    Copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.client.NotifierTypes;

import ocean.transition;
import core.stdc.time;

/*******************************************************************************

    Types passed to client request notifier delegates.

*******************************************************************************/

public struct RequestRecordInfo
{
    import swarm.neo.protocol.Message: RequestId;

    RequestId request_id;
    time_t key;
    const(void)[] value;
}
