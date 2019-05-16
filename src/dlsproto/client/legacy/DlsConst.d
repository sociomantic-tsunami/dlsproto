/*******************************************************************************

    DLS Client & Node Constants

    Copyright:
        Copyright (c) 2009-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.client.legacy.DlsConst;



/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.core.Enum;

import swarm.Const;
import swarm.util.Hash : HashRange;

import ocean.core.Tuple;



/*******************************************************************************

    DlsConst (used as a namespace, all members static)

*******************************************************************************/

public struct DlsConst
{
static:

    /***************************************************************************

        Api version -- this number should be changed whenever the api is
        modified.

        IMPORTANT !!!

            If you change this api version number, please also create an svn tag
            for the old version, so that it's easy to compile code with old DLS
            api versions.

        IMPORTANT !!!

    ***************************************************************************/

    public enum ApiVersion = "20110401";


    /***************************************************************************

        Command Code definitions

        Put                 = add record (allows multiple records per key)
        GetRange            = retrieve records with hashes in specified range
        GetAll              = get all records from a DLS node
        GetChannels         = get the channels in the DLS node
        GetChannelSize      = get the total number of records and the
                              total size (in bytes) in a specified channel
        GetSize             = get the total number of records and the
                              total size (in bytes) of all records in
                              all channels
        RemoveChannel       = remove complete contents of a channel
        GetNumConnections   = gets the current number of active connections
                              from a DLS node
        GetVersion          = requests that the DLS node sends its api version
        GetAllFilter        = get all records from a DLS node which contain the
                              specified filter string
        GetRangeFilter      = retrieve records with hashes in specified range
                              which contain the specified filter string
        GetRangeRegex       = retrieve records with hashes in specified range
                              which match the specified filter, according to the
                              specified filter mode
        Redistribute        = instructs a dls node to starts redistribution of
                              its data to other nodes.
        PutBatch            = sends a batch of records to a dls node.

    ***************************************************************************/

    // TODO: upon API change, the codes can be re-ordered, closing the gaps
    // where dead commands have been removed

    public class Command : ICommandCodes
    {
        mixin EnumBase!([
            "Put"[]:                    3,  // 0x03
            "GetChannels":              17, // 0x11
            "GetChannelSize":           18, // 0x12
            "GetSize":                  19, // 0x13
            "RemoveChannel":            22, // 0x16
            "GetNumConnections":        23, // 0x17
            "GetVersion":               24, // 0x18

            "GetRange":                 28, // 0x1c
            "GetAll":                   29, // 0x1d
            "GetAllFilter":             31, // 0x1f
            "GetRangeFilter":           32, // 0x20
            "GetRangeRegex":            34, // 0x22

            "Redistribute":             35, // 0x23
            "PutBatch":                 36  // 0x24
        ]);
    }


    /***************************************************************************

        Filter mode. GetRangeRegex allows the client to select from several
        different filtering algorithms, as defined by this enum.

    ***************************************************************************/

    public enum FilterMode : ubyte
    {
        StringMatch,
        PCRE,
        PCRECaseInsensitive
    }


    /***************************************************************************

        Status Code definitions (sent from the node to the client)

        Code 0   = Uninitialised value, never returned by the node.
        Code 200 = Node returns OK when request was fulfilled correctly.
        Code 400 = Node throws this error when the received  command is not
                   recognized.
        Code 404 = Node throws this error in case you try to add a hash key to a
                   node that is not responsible for this hash.
        Code 407 = Out of memory error in node (size limit exceeded).
        Code 408 = Attempted to put an empty value (which is illegal).
        Code 409 = Request channel name is invalid.
        Code 500 = This error indicates an internal node error.

    ***************************************************************************/

    public alias IStatusCodes Status;


    /***************************************************************************

        Node Item

    ***************************************************************************/

    public alias .NodeItem NodeItem;


    /**************************************************************************

        Batch size for PutBatch requests.

    ***************************************************************************/

    enum PutBatchSize = 16 * 1024 * 1024;
}
