/*******************************************************************************

    Copyright:
        Copyright (c) 2018 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module test.neotest.main;

import ocean.transition;
import ocean.io.Stdout;

import ocean.task.Scheduler;
import ocean.task.Task;

import ocean.io.select.client.TimerEvent;
import dlsproto.client.DlsClient;
import core.stdc.stdlib;
import core.stdc.time;

abstract class DlsTest
{
    import swarm.neo.AddrPort;
    import swarm.neo.authentication.HmacDef: Key;

    protected DlsClient dls;

    public this ( )
    {
        SchedulerConfiguration config;
        initScheduler(config);

        auto auth_name = "neotest";
        ubyte[] auth_key = Key.init.content;
        this.dls = new DlsClient(theScheduler.epoll, auth_name, auth_key,
            &this.connNotifier);
        this.dls.neo.addNodes("neotest.nodes");
    }

    final public void start ( )
    {
        theScheduler.eventLoop();
    }

    private void connNotifier ( DlsClient.Neo.ConnNotification info )
    {
        with (info.Active) switch (info.active)
        {
        case connected:
            Stdout.formatln("Connected. Let's Go...................................................................");
            this.go();
            break;
        case error_while_connecting:
            with (info.error_while_connecting)
            {
                Stderr.formatln("Connection error: {}", e.message);
                return;
            }
        default:
            assert(false);
        }
    }

    abstract protected void go ( );
}

class PutTest : DlsTest
{
    class PutTask : Task
    {
        import ocean.task.util.Timer;
        import core.stdc.stdlib;

        override public void run ( )
        {
            while ( true )
            {
                this.outer.dls.blocking.put("test".dup, rand(), "whatever".dup);
                wait(1_000);
            }
        }
    }

    override protected void go ( )
    {
        theScheduler.schedule(new PutTask);
    }
}

class GetRangeTest : DlsTest
{
    protected override void go ( )
    {
        Stdout.formatln("Starting GetRange...");
        dls.neo.getRange("test".dup, 0, long.max, &this.getRangeNotifier);
    }

    protected void getRangeNotifier ( DlsClient.Neo.GetRange.Notification info,
        Const!(DlsClient.Neo.GetRange.Args) args )
    {
        with ( info.Active ) switch ( info.active )
        {
            case received:
                Stdout.formatln("GetRange: {}: {}", info.received.key, cast(cstring)info.received.value);
                break;

            case stopped:
                Stdout.formatln("GetRange {} stopped on all nodes.",
                    args.channel);
                theScheduler.shutdown();
                break;

            case finished:
                Stdout.formatln("GetRange {} finished.",
                    args.channel);
                theScheduler.shutdown();
                break;

            case node_disconnected:
                Stdout.formatln("GetRange {} failed due to connection error {} on {}:{}",
                    args.channel,
                    info.node_disconnected.e.message,
                    info.node_disconnected.node_addr.address_bytes,
                    info.node_disconnected.node_addr.port);
                break;

            case node_error:
                Stdout.formatln("GetRange {} failed due to a node error on {}:{}",
                    args.channel,
                    info.node_error.node_addr.address_bytes,
                    info.node_error.node_addr.port);
                break;

            case unsupported:
                switch ( info.unsupported.type )
                {
                    case info.unsupported.type.RequestNotSupported:
                        Stdout.formatln("GetRange {} node {}:{} does not support this request",
                            args.channel,
                            info.unsupported.node_addr.address_bytes,
                            info.unsupported.node_addr.port);
                        break;
                    case info.unsupported.type.RequestVersionNotSupported:
                        Stdout.formatln("GetRange {} node {}:{} does not support this request version",
                            args.channel,
                            info.unsupported.node_addr.address_bytes,
                            info.unsupported.node_addr.port);
                        break;

                    default: assert(false);
                }
                break;

            default: assert(false);
        }
    }
}

void main ( char[][] args )
{
    if ( args.length != 2 )
        throw new Exception("Expected exactly one CLI argument.");

    srand(cast(uint)time(null));

    DlsTest app;
    switch ( args[1] )
    {
        case "getrange":
            app = new GetRangeTest;
            break;
        case "put":
            app = new PutTest;
            break;
        default:
            throw new Exception("Unknown request type.");
    }
    app.start();
}
