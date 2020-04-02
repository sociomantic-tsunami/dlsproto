/*******************************************************************************

    Parameters for a DLS request.

    Copyright:
        Copyright (c) 2010-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.client.legacy.internal.request.params.RequestParams;



/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.meta.types.Qualifiers;

import ocean.core.Verify;

import swarm.client.request.params.IChannelRequestParams;

import swarm.client.request.context.RequestContext;

import swarm.client.ClientCommandParams;

import swarm.client.request.model.ISuspendableRequest;

import swarm.client.connection.model.INodeConnectionPoolInfo;

import dlsproto.client.legacy.internal.request.params.IODelegates;

import dlsproto.client.legacy.DlsConst;

import Hash = swarm.util.Hash;

import dlsproto.client.legacy.internal.request.notifier.RequestNotification;

import ocean.core.SmartUnion;




public class RequestParams : IChannelRequestParams
{
    /***************************************************************************

        Local type redefinitions

    ***************************************************************************/

    public alias .PutValueDg PutValueDg;
    public alias .PutBatchDg PutBatchDg;
    public alias .GetValueDg GetValueDg;
    public alias .GetPairDg GetPairDg;
    public alias .GetBoolDg GetBoolDg;
    public alias .GetNumConnectionsDg GetNumConnectionsDg;
    public alias .GetNodeValueDg GetNodeValueDg;
    public alias .RegisterSuspendableDg RegisterSuspendableDg;
    public alias .RegisterStreamInfoDg RegisterStreamInfoDg;
    public alias .RedistributeDg RedistributeDg;

    public alias Hash.HexDigest HexDigest;


    /**************************************************************************

        Request "key" (a hash range). For requests (i.e. Put) which only have a
        single hash, the min and max of the range are both set.

     **************************************************************************/

    public struct Key
    {
        /***********************************************************************

            Hash range

        ***********************************************************************/

        Hash.HashRange range;

        /***********************************************************************

            Single hash setter.

            Params:
                h = hash to store in this.range

        ***********************************************************************/

        public void hash ( hash_t h )
        {
            this.range = Hash.HashRange(h, h);
        }

        /***********************************************************************

            Single hash getter. (May only be called if a single hash has already
            been set by the setter, see above.)

            Returns:
                single hash stored in this.range

        ***********************************************************************/

        public hash_t hash ( )
        {
            verify(this.is_single_hash);
            return this.range.min;
        }

        /***********************************************************************

            Returns:
                true if this.range covers only a single hash. Note that it is
                possible for a Key to be in this state both by setting
                this.range directly and via the hash() setter.

        ***********************************************************************/

        public bool is_single_hash ( )
        {
            return this.range.min == this.range.max;
        }
    }

    /// ditto
    public Key key;

    /***************************************************************************

        Request I/O delegate union

    ***************************************************************************/

    public union IODg
    {
        PutValueDg put_value;
        PutBatchDg put_batch;
        GetValueDg get_value;
        GetPairDg get_pair;
        GetBoolDg get_bool;
        GetNumConnectionsDg get_num_connections;
        GetNodeValueDg get_node_value;
        RedistributeDg redistribute;
    }

    public alias SmartUnion!(IODg) IOItemUnion;

    public IOItemUnion io_item;


    /***************************************************************************

        Request filter mode

    ***************************************************************************/

    public DlsConst.FilterMode filter_mode;


    /***************************************************************************

        Request filter string

    ***************************************************************************/

    public cstring filter_string;


    /***************************************************************************

        Delegate which receives an ISuspendable interface when a suspendable
        request has just started.

    ***************************************************************************/

    public RegisterSuspendableDg suspend_register;


    /***************************************************************************

        Delegate which receives an ISuspendable interface when a suspendable
        request has just finished.

    ***************************************************************************/

    public RegisterSuspendableDg suspend_unregister;


    /***************************************************************************

        Delegate which receives an IStreamInfo interface when a stream request
        has just started.

    ***************************************************************************/

    public RegisterStreamInfoDg stream_info_register;


    /**************************************************************************

        Generates the hexadecimal string representation of the current key.
        This asserts that the key is currently set to a hash_t value.

        Params:
            hash = destination string

        Returns:
            destination string containing the result

     **************************************************************************/

    public mstring keyToString ( mstring hash )
    {
        HexDigest digest;
        verify( hash.length == digest.length );
        return Hash.toHexString(this.key.hash(), hash);
    }


    /**************************************************************************

        Generates the hexadecimal string representations of the keys of the
        range to which the key is currently set.
        This asserts that the key is currently set to a Range.

        Params:
            min_hash = destination string for range minimum key
            max_hash = destination string for range maximum key

     **************************************************************************/

    public void rangeToString ( mstring min_hash, mstring max_hash  )
    {
        Hash.toHexString(this.key.range.min, min_hash);
        Hash.toHexString(this.key.range.max, max_hash);
    }


    /***************************************************************************

        News a DLS client RequestNotification instance and passes it to the
        provided delegate.

        Params:
            info_dg = delegate to receive IRequestNotification instance

    ***************************************************************************/

    override protected void notify_ ( scope void delegate ( IRequestNotification ) info_dg )
    {
        scope info = new RequestNotification(cast(DlsConst.Command.E)this.command,
            this.context);
        info_dg(info);
    }


    /***************************************************************************

        Copies the fields of this instance from another.

        All fields are copied by value. (i.e. all arrays are sliced.)

        Params:
            params = instance to copy fields from

    ***************************************************************************/

    override protected void copy__ ( IRequestParams params )
    {
        auto dls_params = cast(RequestParams)params;
        this.tupleof[] = dls_params.tupleof[];
    }


    /***************************************************************************

        Add the serialisation override methods

    ***************************************************************************/

    mixin Serialize!();
}
