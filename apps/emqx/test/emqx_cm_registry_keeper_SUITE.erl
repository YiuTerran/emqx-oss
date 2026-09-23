%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_cm_registry_keeper_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("emqx_cm.hrl").

%%--------------------------------------------------------------------
%% CT callbacks
%%--------------------------------------------------------------------

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    AppConfig = "broker.session_history_retain = 2s",
    Apps = emqx_cth_suite:start(
        [{emqx, #{config => AppConfig}}],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(proplists:get_value(apps, Config)).

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, Config) ->
    Config.

t_cleanup_after_retain(_) ->
    Pid = spawn(fun() ->
        receive
            stop -> ok
        end
    end),
    ClientId = <<"clientid">>,
    ClientId2 = <<"clientid2">>,
    emqx_cm_registry:register_channel({ClientId, Pid}),
    emqx_cm_registry:register_channel({ClientId2, Pid}),
    ?assertEqual([Pid], emqx_cm_registry:lookup_channels(ClientId)),
    ?assertEqual([Pid], emqx_cm_registry:lookup_channels(ClientId2)),
    ?assertEqual(2, emqx_cm_registry_keeper:count(0)),
    T0 = erlang:system_time(seconds),
    exit(Pid, kill),
    %% lookup_channels should not return dead pids
    ?assertEqual([], emqx_cm_registry:lookup_channels(ClientId)),
    ?assertEqual([], emqx_cm_registry:lookup_channels(ClientId2)),
    %% simulate a DOWN message triggering a clean up from emqx_cm
    ok = emqx_cm_registry:unregister_channel({ClientId, Pid}),
    ok = emqx_cm_registry:unregister_channel({ClientId2, Pid}),
    %% expect the channels to be around still
    ?assertEqual(2, emqx_cm_registry_keeper:count(T0)),
    ?assertEqual(2, emqx_cm_registry_keeper:count(0)),
    %% and finally cleaned up after retain period
    ?retry(_Interval = 1000, _Attempts = 4, begin
        ?assertEqual(0, emqx_cm_registry_keeper:count(T0)),
        ?assertEqual(0, emqx_cm_registry_keeper:count(0))
    end),
    ok.

%% count is cached when the number of entries is greater than 1000
t_count_cache(_) ->
    Pid = self(),
    ClientsCount = 999,
    ClientIds = lists:map(fun erlang:integer_to_binary/1, lists:seq(1, ClientsCount)),
    Channels = lists:map(fun(ClientId) -> {ClientId, Pid} end, ClientIds),
    lists:foreach(
        fun emqx_cm_registry:register_channel/1,
        Channels
    ),
    T0 = erlang:system_time(seconds),
    ?assertEqual(ClientsCount, emqx_cm_registry_keeper:count(0)),
    ?assertEqual(ClientsCount, emqx_cm_registry_keeper:count(T0)),
    %% insert another one to trigger the cache threshold
    emqx_cm_registry:register_channel({<<"-1">>, Pid}),
    ?assertEqual(ClientsCount + 1, emqx_cm_registry_keeper:count(0)),
    ?assertEqual(ClientsCount, emqx_cm_registry_keeper:count(T0)),
    mnesia:clear_table(?CHAN_REG_TAB),
    ok.

t_stale_local_pid(_) ->
    ClientId = <<"stale-local">>,
    LivePid = self(),
    DeadPid = dead_pid(),
    mria:dirty_write(?CHAN_REG_TAB, channel(ClientId, LivePid)),
    mria:dirty_write(?CHAN_REG_TAB, channel(ClientId, DeadPid)),
    ok = emqx_cm_registry:cleanup_stale_channels(ClientId, [node()]),
    ?assertEqual([LivePid], emqx_cm_registry:lookup_all_channels(ClientId)),
    ?assertEqual([channel(ClientId, LivePid)], mnesia:dirty_read(?CHAN_REG_TAB, ClientId)),
    mria:dirty_delete_object(?CHAN_REG_TAB, channel(ClientId, LivePid)),
    ok.

t_stepdown_prunes_stale_pid(_) ->
    KickClientId = <<"stale-kick">>,
    TakeoverClientId = <<"stale-takeover">>,
    mria:dirty_write(?CHAN_REG_TAB, channel(KickClientId, dead_pid())),
    mria:dirty_write(?CHAN_REG_TAB, channel(TakeoverClientId, dead_pid())),
    ok = emqx_cm:kick_session(KickClientId),
    none = emqx_cm:takeover_session_begin(TakeoverClientId),
    ?assertEqual([], emqx_cm_registry:lookup_all_channels(KickClientId)),
    ?assertEqual([], emqx_cm_registry:lookup_all_channels(TakeoverClientId)),
    ok.

t_stale_remote_pid(_) ->
    {ok, Node} = emqx_cth_peer:start_link(
        stale_registry_peer, emqx_common_test_helpers:ebin_path(), []
    ),
    try
        pong = net_adm:ping(Node),
        RemotePid = erpc:call(Node, erlang, spawn, [timer, sleep, [infinity]]),
        ok = emqx_cth_peer:stop(Node),
        ClientId = <<"stale-remote">>,
        mria:dirty_write(?CHAN_REG_TAB, channel(ClientId, RemotePid)),
        %% A stopped member must retain its registration.
        ok = emqx_cm_registry:cleanup_stale_channels(ClientId, [node(), Node]),
        ?assertEqual([RemotePid], emqx_cm_registry:lookup_all_channels(ClientId)),
        %% Only confirmed removal from cluster membership permits deletion.
        ok = emqx_cm_registry:cleanup_stale_channels(ClientId, [node()]),
        ?assertEqual([], emqx_cm_registry:lookup_all_channels(ClientId)),
        [#channel{pid = HistoryTime}] = mnesia:dirty_read(?CHAN_REG_TAB, ClientId),
        ?assert(is_integer(HistoryTime))
    after
        _ = catch emqx_cth_peer:stop(Node)
    end.

t_stale_gc_with_history(_) ->
    ClientId = <<"stale-gc-history">>,
    mria:dirty_write(?CHAN_REG_TAB, channel(ClientId, dead_pid())),
    whereis(emqx_cm_registry_keeper) ! stale_gc_start,
    #{stale_gc_running := false} = sys:get_state(emqx_cm_registry_keeper),
    ?assertEqual([], emqx_cm_registry:lookup_all_channels(ClientId)),
    [#channel{pid = HistoryTime}] = mnesia:dirty_read(?CHAN_REG_TAB, ClientId),
    ?assert(is_integer(HistoryTime)),
    mnesia:clear_table(?CHAN_REG_TAB),
    ok.

t_stale_gc_without_history(_) ->
    ok = emqx_config:put([broker, session_history_retain], 0),
    try
        mnesia:clear_table(?CHAN_REG_TAB),
        DeadPid = dead_pid(),
        Ids = [integer_to_binary(N) || N <- lists:seq(1, 501)],
        lists:foreach(
            fun(Id) -> mria:dirty_write(?CHAN_REG_TAB, channel(Id, DeadPid)) end,
            Ids
        ),
        whereis(emqx_cm_registry_keeper) ! stale_gc_start,
        #{stale_gc_running := true} = sys:get_state(emqx_cm_registry_keeper),
        %% A scan batch advances at most 500 keys before its one-second pause.
        ?assertEqual(1, mnesia:table_info(?CHAN_REG_TAB, size)),
        whereis(emqx_cm_registry_keeper) ! stale_gc_start,
        #{stale_gc_running := true} = sys:get_state(emqx_cm_registry_keeper),
        ?assertEqual(1, mnesia:table_info(?CHAN_REG_TAB, size)),
        ?retry(_Interval = 500, _Attempts = 5,
            ?assertEqual(0, mnesia:table_info(?CHAN_REG_TAB, size))
        ),
        #{stale_gc_running := false} = sys:get_state(emqx_cm_registry_keeper)
    after
        ok = emqx_config:put([broker, session_history_retain], 2),
        mnesia:clear_table(?CHAN_REG_TAB)
    end.

dead_pid() ->
    {Pid, Monitor} = spawn_monitor(fun() -> receive stop -> ok end end),
    exit(Pid, kill),
    receive
        {'DOWN', Monitor, process, Pid, killed} -> Pid
    end.

channel(Id, Pid) ->
    #channel{chid = Id, pid = Pid}.
