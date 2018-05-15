/*******************************************************************************

    Types passed to client request notifier delegates.

    Copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.client.NotifierTypes;

import ocean.transition;
import core.stdc.time;

/*******************************************************************************

    Types passed to client request notifier delegates.

*******************************************************************************/

public struct RecordInfo
{
    time_t key;
    Const!(void)[] value;
}
