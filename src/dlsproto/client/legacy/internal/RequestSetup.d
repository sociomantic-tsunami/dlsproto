/*******************************************************************************

    Mixins for request setup structs used in DlsClient.

    Copyright:
        Copyright (c) 2011-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.client.legacy.internal.RequestSetup;



/*******************************************************************************

    Imports

    Note that swarm.client.RequestSetup is imported publicly, as all of the
    templates it contains are needed wherever this module is imported.

*******************************************************************************/

import ocean.core.Verify;
public import swarm.client.RequestSetup;

/*******************************************************************************

    Mixin for the methods use by DLS client requests which have an I/O delegate.

*******************************************************************************/

public template IODelegate ( )
{
    import ocean.transition;
    import ocean.core.TypeConvert : downcast;

    alias typeof(this) This;
    static assert (is(This == struct));

    /***************************************************************************

        I/O delegate

    ***************************************************************************/

    private RequestParams.IOItemUnion io_item;


    /***************************************************************************

        Sets the I/O delegate for a request.

        Params:
            io = I/O delegate

        Returns:
            this pointer for method chaining

    ***************************************************************************/

    public This* io ( T ) ( T io )
    {
        this.io_item = this.io_item(io);
        version (D_Version2)
            return &this;
        else
            return this;
    }


    /***************************************************************************

        Copies the value of the io_item member into the provided
        request params class instance.

        Params:
            params = IRequestParams instance to write into

    ***************************************************************************/

    private void setup_io_item ( IRequestParams params )
    {
        auto params_ = downcast!(RequestParams)(params);
        verify(params_ !is null);

        params_.io_item = this.io_item;
    }
}


/*******************************************************************************

    Mixin for the methods used by DLS client requests which pass a filter string
    to the node.

*******************************************************************************/

public template Filter ( )
{
    import ocean.transition;
    import ocean.core.TypeConvert : downcast;

    alias typeof(this) This;
    static assert (is(This == struct));

    /***************************************************************************

        Request filter mode.

    ***************************************************************************/

    private DlsConst.FilterMode filter_mode;


    /***************************************************************************

        Request filter string.

    ***************************************************************************/

    private cstring filter_string;


    /***************************************************************************

        Sets the request in string match filtering mode over the specified
        string.

        Params:
            filter = filter string

        Returns:
            this pointer for method chaining

    ***************************************************************************/

    public This* filter ( cstring filter )
    {
        verify(this.command_code != DlsConst.Command.E.GetAllFilter
            && this.command_code != DlsConst.Command.E.GetRangeFilter
            && this.command_code != DlsConst.Command.E.GetRangeRegex);

        this.filter_mode = this.filter_mode.StringMatch;
        this.filter_string = filter;

        // TODO: this block of code which switches the command type internally
        // should be removed if we want to modify the GetAll / GetRange protocol
        // to make filtering a true built-in option.
        with ( DlsConst.Command.E ) switch ( this.command_code )
        {
            case GetAll:
            case GetAllFilter:
                this.command_code = GetAllFilter;
                break;

            case GetRange:
            case GetRangeFilter:
                this.command_code = GetRangeFilter;
                break;

            case GetRangeRegex:
                this.command_code = GetRangeRegex;
                break;

            default:
                assert(false, "filter method called on command which doesn't support filtering!");
        }

        version (D_Version2)
            return &this;
        else
            return this;
    }


    /***************************************************************************

        Sets the request in PCRE filtering mode over the specified string.

        Params:
            filter = filter string
            case_sensitive = indicates whether the filter should be case
                sentitive or not

        Returns:
            this pointer for method chaining

    ***************************************************************************/

    public This* pcre ( cstring filter, bool case_sensitive = true )
    {
        verify(this.command_code != DlsConst.Command.E.GetAllFilter
            && this.command_code != DlsConst.Command.E.GetRangeFilter
            && this.command_code != DlsConst.Command.E.GetRangeRegex);

        this.filter_mode = case_sensitive ?
            this.filter_mode.PCRE : this.filter_mode.PCRECaseInsensitive;
        this.filter_string = filter;

        // TODO: this block of code which switches the command type internally
        // should be removed if we want to modify the GetAll / GetRange protocol
        // to make filtering a true built-in option.
        with ( DlsConst.Command.E ) switch ( this.command_code )
        {
            case GetRange:
            case GetRangeFilter:
            case GetRangeRegex:
                this.command_code = GetRangeRegex;
                break;

            default:
                assert(false, "filter method called on command which doesn't support filtering!");
        }

        version (D_Version2)
            return &this;
        else
            return this;
    }


    /***************************************************************************

        Copies the value of the filter_mode member into the provided request
        params class instance.

        Params:
            params = IRequestParams instance to write into

    ***************************************************************************/

    private void setup_filter_mode ( IRequestParams params )
    {
        auto params_ = downcast!(RequestParams)(params);
        verify(params_ !is null);

        params_.filter_mode = this.filter_mode;
    }


    /***************************************************************************

        Copies the value of the filter_string member into the provided request
        params class instance.

        Params:
            params = IRequestParams instance to write into

    ***************************************************************************/

    private void setup_filter_string ( IRequestParams params )
    {
        auto params_ = downcast!(RequestParams)(params);
        verify(params_ !is null);

        params_.filter_string = this.filter_string;
    }
}


/*******************************************************************************

    Mixin for the methods used by DLS client requests which operate with a key.

*******************************************************************************/

public template Key ( )
{
    import ocean.transition;
    import ocean.core.TypeConvert : downcast;
    static import swarm.util.Hash;

    alias typeof(this) This;
    static assert (is(This == struct));

    /***************************************************************************

        Request hash.

    ***************************************************************************/

    private hash_t hash;


    /***************************************************************************

        Sets the key for a request.

        Template params:
            Key = type of key

        Params:
            key = request key

        Returns:
            this pointer for method chaining

    ***************************************************************************/

    public This* key ( Key ) ( Key key )
    {
        version (X86_64) static assert(!is( Key == uint),
            "Please use hash_t instead of uint.");

        this.hash = swarm.util.Hash.toHash(key);

        version (D_Version2)
            return &this;
        else
            return this;
    }


    /***************************************************************************

        Sets the context for a request to the key hash.

        Returns:
            this pointer for method chaining

    ***************************************************************************/

    public This* contextFromKey ( )
    {
        this.user_context = RequestContext(this.hash);

        version (D_Version2)
            return &this;
        else
            return this;
    }


    /***************************************************************************

        Copies the value of the hash member into the provided request params
        class instance.

        Params:
            params = IRequestParams instance to write into

    ***************************************************************************/

    private void setup_hash ( IRequestParams params )
    {
        auto params_ = downcast!(RequestParams)(params);
        verify(params_ !is null);

        params_.key.hash = this.hash;
    }
}


/*******************************************************************************

    Mixin for the methods used by DLS client requests which operate with a key
    range.

*******************************************************************************/

public template Range ( )
{
    import ocean.transition;
    import ocean.core.TypeConvert : downcast;
    static import swarm.util.Hash;

    alias typeof(this) This;
    static assert (is(This == struct));

    /***************************************************************************

        Request hash range.

    ***************************************************************************/

    swarm.util.Hash.HashRange hash_range;


    /***************************************************************************

        Sets the key range for a request.

        Template params:
            Key = type of keys

        Params:
            start = range start
            end = range end

        Returns:
            this pointer for method chaining

    ***************************************************************************/

    public This* range ( Key ) ( Key start, Key end )
    {
        this.hash_range =
            swarm.util.Hash.HashRange(swarm.util.Hash.toHash(start),
                    swarm.util.Hash.toHash(end));

        version (D_Version2)
            return &this;
        else
            return this;
    }


    /***************************************************************************

        Copies the value of the hash member into the provided request params
        class instance.

        Params:
            params = IRequestParams instance to write into

    ***************************************************************************/

    private void setup_hash_range ( IRequestParams params )
    {
        auto params_ = downcast!(RequestParams)(params);
        verify(params_ !is null);

        params_.key.range = this.hash_range;
    }
}

