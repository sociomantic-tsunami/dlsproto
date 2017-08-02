/*******************************************************************************

    DLS neo request codes.

    Copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.common.RequestCodes;

public enum RequestCode : ubyte
{
    None,
    Put,
    GetRange
}
