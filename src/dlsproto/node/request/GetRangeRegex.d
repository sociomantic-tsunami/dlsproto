/*******************************************************************************

    Protocol base for DLS `GetRangeRegex` request

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsproto.node.request.GetRangeRegex;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.meta.types.Qualifiers;

import dlsproto.node.request.model.CompressedBatch;

/*******************************************************************************

    Request protocol

*******************************************************************************/

public abstract scope class GetRangeRegex : CompressedBatch!(cstring, cstring)
{
    import dlsproto.node.request.model.DlsCommand;

    import dlsproto.client.legacy.DlsConst;

    /***************************************************************************

        Filter mode specified by client.

    ***************************************************************************/

    protected DlsConst.FilterMode mode;

    /***************************************************************************

        Pointer to buffer containing regex expression.

    ***************************************************************************/

    protected char[]* pcre_filter;

    /***************************************************************************

        Constructor

        Params:
            reader = FiberSelectReader instance to use for read requests
            writer = FiberSelectWriter instance to use for write requests
            resources = object providing resource getters

    ***************************************************************************/

    public this ( FiberSelectReader reader, FiberSelectWriter writer,
        DlsCommand.Resources resources )
    {
        super(DlsConst.Command.E.GetRangeRegex, reader, writer, resources);
    }

    /***************************************************************************

        Read upper and lower bound keys for GetRange request

    ***************************************************************************/

    final override protected void readChannelRequestData ( )
    {
        auto lower = this.resources.getKeyBuffer();
        this.reader.readArray(*lower);
        auto upper = this.resources.getKeyUpperBuffer();
        this.reader.readArray(*upper);

        this.reader.read(this.mode);

        this.pcre_filter = this.resources.getFilterBuffer();
        this.reader.readArray(*this.pcre_filter);

        this.prepareRange(*lower, *upper);
    }

    /***************************************************************************

        Processes PCRE filter ensuring that it's valid.

        Return:
            `true` if filter is valid and it is possible to proceed with request,
            `false` otherwise.

    ***************************************************************************/

    protected override bool processAndValidateRequestData ( )
    {
        return this.prepareFilter(this.mode, *this.pcre_filter);
    }

    /***************************************************************************

        Communicates requested range to protocol implementation

        Params:
            key_lower = lower bound key in requested range
            key_upper = upper bound key in requested range

    ***************************************************************************/

    abstract protected void prepareRange ( cstring key_lower, cstring key_upper );

    /***************************************************************************

        Allows request to process read filter string into more efficient form
        and save it before starting actual record iteration.

        Params:
            mode = filter mode
            filter = filter string

        Returns:
            true if preparing regex filter is successful, false otherwise

    ***************************************************************************/

    abstract protected bool prepareFilter ( DlsConst.FilterMode mode,
        cstring filter );
}

