%% A PGW-C stand-in for the GTP-C client tests: one UDP socket that
%% answers Echo Requests, Create Session Requests and Delete Session
%% Requests, dropping the datagrams a test tells it to drop. Every
%% received datagram is reported to the owner so a test can assert what
%% was sent and how often.
-module(fake_pgw).

-export([start/1, stop/1, port/1, received/1, set_recovery/2]).

-record(cfg, {owner, sock, drop_first = 0, drop = #{}, recovery = 7, seen = []}).

%% Opts: #{drop_first => N} drops the first N requests of every kind;
%% #{drop => #{Type => N}} the first N requests of that message type only.
start(Opts) ->
    Owner = self(),
    Pid = spawn(fun() ->
        {ok, Sock} = gen_udp:open(0, [binary, {ip, {127, 0, 0, 1}}, {active, true}]),
        Owner ! {fake_pgw_ready, self()},
        loop(#cfg{owner = Owner, sock = Sock,
                  drop_first = maps:get(drop_first, Opts, 0),
                  drop = maps:get(drop, Opts, #{})}, #{})
    end),
    receive {fake_pgw_ready, Pid} -> Pid after 2000 -> error(fake_pgw_start) end.

%% Synchronous: the socket is closed when this returns.
stop(Pid) ->
    Ref = monitor(process, Pid),
    Pid ! stop,
    receive {'DOWN', Ref, process, Pid, _} -> ok after 2000 -> error(fake_pgw_stop) end.

port(Pid) -> call(Pid, port).

%% The datagrams received so far, oldest first, as decoded headers.
received(Pid) -> call(Pid, received).

set_recovery(Pid, R) -> call(Pid, {set_recovery, R}).

call(Pid, Req) ->
    Ref = make_ref(),
    Pid ! {Req, self(), Ref},
    receive {Ref, Reply} -> Reply after 2000 -> error({fake_pgw_timeout, Req}) end.

%% Drops is a map Kind => datagrams dropped so far.
loop(#cfg{sock = Sock} = Cfg, Drops) ->
    receive
        stop -> ok;
        {port, From, Ref} ->
            {ok, P} = inet:port(Sock), From ! {Ref, P}, loop(Cfg, Drops);
        {received, From, Ref} ->
            From ! {Ref, lists:reverse(Cfg#cfg.seen)}, loop(Cfg, Drops);
        {{set_recovery, R}, From, Ref} ->
            From ! {Ref, ok}, loop(Cfg#cfg{recovery = R}, Drops);
        {udp, Sock, IP, Port, Data} ->
            {ok, #{type := Type, seq_num := Seq} = Hdr} = epdg_gtpc_codec:decode_header(Data),
            Seen = [Hdr#{raw => Data} | Cfg#cfg.seen],
            Dropped = maps:get(Type, Drops, 0),
            Limit = max(Cfg#cfg.drop_first, maps:get(Type, Cfg#cfg.drop, 0)),
            case Dropped < Limit of
                true ->
                    loop(Cfg#cfg{seen = Seen}, Drops#{Type => Dropped + 1});
                false ->
                    Resp = response(Type, Seq, Cfg#cfg.recovery),
                    ok = gen_udp:send(Sock, IP, Port, Resp),
                    loop(Cfg#cfg{seen = Seen}, Drops)
            end
    end.

response(1, Seq, Recovery) ->
    epdg_gtpc_codec:encode_echo_response(#{seq_num => Seq, recovery => Recovery});
response(32, Seq, Recovery) ->
    %% Create Session Response: cause accepted, PGW control F-TEID, a bearer
    %% context with EBI 5, Recovery.
    FTeid = <<1:1, 0:1, 32:6, 16#1234:32, 10, 0, 1, 10>>,
    BC = ie(73, <<0:4, 5:4>>),
    IEs = [ie(2, <<16, 0>>), ie(87, FTeid), ie(93, BC), ie(3, <<Recovery>>)],
    header(33, 16#AAAA, Seq, iolist_to_binary(IEs));
response(36, Seq, Recovery) ->
    header(37, 16#AAAA, Seq, iolist_to_binary([ie(2, <<16, 0>>), ie(3, <<Recovery>>)])).

ie(Type, Value) -> <<Type:8, (byte_size(Value)):16, 0:8, Value/binary>>.

header(Type, TEID, Seq, Body) ->
    Len = 8 + byte_size(Body),
    <<2:3, 0:1, 1:1, 0:3, Type:8, Len:16, TEID:32, Seq:24, 0:8, Body/binary>>.
