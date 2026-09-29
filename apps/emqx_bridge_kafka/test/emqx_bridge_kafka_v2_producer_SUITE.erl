%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc Common Test suite for the Kafka producer connector and action (v2).
%%
%% Produced messages are consumed back from Kafka with the `brod' client, which
%% is only a test dependency of this application.
-module(emqx_bridge_kafka_v2_producer_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("brod/include/brod.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

%% Docker service names: the Kafka listener advertised to clients inside the
%% CT container is `kafka-N.emqx.net', not localhost.
-define(KAFKA_HOSTS, [{"kafka-1.emqx.net", 9092}]).
%% SASL_PLAINTEXT listener.
-define(KAFKA_SASL_HOSTS, [{"kafka-1.emqx.net", 9093}]).
-define(SCRAM_USER, <<"emqxuser">>).
-define(SCRAM_PASSWORD, <<"password">>).
-define(KAFKA_SASL_OPTS, [{sasl, {scram_sha_512, ?SCRAM_USER, ?SCRAM_PASSWORD}}]).
%% Client id used by this suite to consume messages.
-define(KAFKA_CLIENT, emqx_bridge_kafka_ct_client).

%% Pre-created topics (see `.ci/docker-compose-file/docker-compose-kafka.yaml').
-define(TOPIC_1, <<"test-topic-one-partition">>).

%% Toxiproxy fronting the plain Kafka listeners of each broker.
-define(PROXY_KAFKA_1, "kafka_plain").
-define(PROXY_KAFKA_2, "kafka_2_plain").

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    %% `brod' is only used by this suite to verify the produced messages.
    {ok, _} = application:ensure_all_started(brod),
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_connector,
            emqx_bridge_kafka,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(TestCase, Config) ->
    UniqueNum = integer_to_binary(erlang:unique_integer([positive])),
    Name = iolist_to_binary([atom_to_binary(TestCase), UniqueNum]),
    ConnectorConfig = connector_config(),
    ActionConfig = action_config(#{connector => Name}),
    %% Make sure a previous (failed) testcase did not leave the proxy disabled.
    ok = reset_toxiproxy(),
    [
        {bridge_kind, action},
        {action_type, kafka_producer},
        {action_name, Name},
        {action_config, ActionConfig},
        {connector_type, kafka_producer},
        {connector_name, Name},
        {connector_config, ConnectorConfig}
        | Config
    ].

end_per_testcase(_TestCase, _Config) ->
    stop_kafka_client(),
    ok = reset_toxiproxy(),
    emqx_common_test_helpers:call_janitor(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    ok.

%%------------------------------------------------------------------------------
%% Configuration helpers
%%------------------------------------------------------------------------------

connector_config() ->
    connector_config(_Overrides = #{}).

connector_config(Overrides0) ->
    Overrides = emqx_utils_maps:binary_key_map(Overrides0),
    Defaults =
        #{
            <<"enable">> => true,
            <<"description">> => <<"kafka producer connector">>,
            <<"bootstrap_hosts">> => <<"kafka-1.emqx.net:9092">>,
            <<"connect_timeout">> => <<"5s">>,
            <<"min_metadata_refresh_interval">> => <<"3s">>,
            <<"request_timeout">> => <<"15s">>,
            <<"socket_opts">> =>
                #{
                    <<"nodelay">> => true,
                    <<"sndbuf">> => <<"1024KB">>,
                    <<"recbuf">> => <<"1024KB">>,
                    <<"tcp_keepalive">> => <<"none">>
                },
            <<"ssl">> => #{<<"enable">> => false},
            <<"authentication">> => <<"none">>,
            <<"allow_auto_topic_creation">> => false,
            <<"resource_opts">> =>
                #{
                    <<"health_check_interval">> => <<"1s">>,
                    <<"start_after_created">> => true,
                    <<"start_timeout">> => <<"10s">>
                }
        },
    emqx_utils_maps:deep_merge(Defaults, Overrides).

action_config(Overrides0) ->
    Overrides = emqx_utils_maps:binary_key_map(Overrides0),
    Defaults =
        #{
            <<"enable">> => true,
            <<"description">> => <<"kafka producer action">>,
            <<"connector">> => <<"please override">>,
            <<"parameters">> =>
                #{
                    <<"topic">> => ?TOPIC_1,
                    <<"message">> =>
                        #{
                            %% Schema defaults.
                            <<"key">> => <<"${.clientid}">>,
                            <<"value">> => <<"${.}">>,
                            <<"timestamp">> => <<"${.timestamp}">>
                        },
                    <<"kafka_headers">> => <<"${pub_props}">>,
                    <<"kafka_ext_headers">> =>
                        [
                            #{
                                <<"kafka_ext_header_key">> => <<"clientid">>,
                                <<"kafka_ext_header_value">> => <<"${clientid}">>
                            }
                        ],
                    <<"kafka_header_value_encode_mode">> => <<"none">>,
                    <<"compression">> => <<"no_compression">>,
                    <<"required_acks">> => <<"all_isr">>,
                    <<"partition_strategy">> => <<"random">>,
                    <<"partitions_limit">> => <<"all_partitions">>,
                    <<"max_batch_bytes">> => <<"896KB">>,
                    <<"max_inflight">> => 10,
                    <<"buffer">> => #{<<"mode">> => <<"memory">>}
                },
            <<"resource_opts">> =>
                #{
                    <<"health_check_interval">> => <<"1s">>,
                    <<"metrics_flush_interval">> => <<"500ms">>,
                    <<"query_mode">> => <<"async">>
                }
        },
    emqx_utils_maps:deep_merge(Defaults, Overrides).

%%------------------------------------------------------------------------------
%% Testlib helpers
%%------------------------------------------------------------------------------

create_connector_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(Config, Overrides)
    ).

create_action_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api(Config, Overrides)
    ).

get_connector_api(Config) ->
    Type = ?config(connector_type, Config),
    Name = ?config(connector_name, Config),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_connector_api(Type, Name)
    ).

get_action_api(Config) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_action_api(Config)
    ).

get_action_metrics_api(Config) ->
    emqx_bridge_v2_testlib:get_action_metrics_api(Config).

create_rule_and_action_http(Config, RuleTopic, Opts) ->
    emqx_bridge_v2_testlib:create_rule_and_action_http(
        ?config(action_type, Config), RuleTopic, Config, Opts
    ).

%% Creates the connector and the action, and waits for both to be healthy.
create_connector_and_action(Config, ConnectorOverrides, ActionOverrides) ->
    {201, _} = create_connector_api(Config, ConnectorOverrides),
    ?retry(
        _Sleep = 1_000,
        _Attempts = 20,
        ?assertMatch(
            {200, #{<<"status">> := <<"connected">>}},
            get_connector_api(Config)
        )
    ),
    {201, _} = create_action_api(Config, ActionOverrides),
    ?retry(
        _Sleep = 1_000,
        _Attempts = 20,
        ?assertEqual(<<"connected">>, action_status(Config))
    ),
    ok.

action_status(Config) ->
    {200, #{<<"status">> := Status}} = get_action_api(Config),
    Status.

action_metric(Name, Config) ->
    {200, #{<<"metrics">> := Metrics}} = get_action_metrics_api(Config),
    maps:get(Name, Metrics, 0).

connect_client(ClientId) ->
    {ok, C} = emqtt:start_link(#{proto_ver => v5, clientid => ClientId}),
    on_exit(fun() -> catch emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    C.

publish(Client, Topic, Payload) ->
    {ok, _} = emqtt:publish(Client, Topic, Payload, [{qos, ?QOS_1}]),
    ok.

%% A payload unique to this test run, so that leftovers from other testcases or
%% other suites sharing the same topic are never mistaken for ours.
unique_payload(Prefix) ->
    iolist_to_binary([
        Prefix,
        "-",
        integer_to_binary(erlang:unique_integer([positive]))
    ]).

%%------------------------------------------------------------------------------
%% Kafka consumer helpers (via `brod')
%%------------------------------------------------------------------------------

start_kafka_client() ->
    start_kafka_client(?KAFKA_HOSTS, []).

start_kafka_client(Hosts, Opts) ->
    ok = brod:start_client(Hosts, ?KAFKA_CLIENT, Opts).

stop_kafka_client() ->
    _ = catch brod:stop_client(?KAFKA_CLIENT),
    ok.

%% `brod' does not create the topic if it does not exist, so create it
%% explicitly and ignore "already exists" errors.
ensure_topic(Topic) ->
    try
        _ = brod:create_topics(
            ?KAFKA_HOSTS,
            [#{name => Topic, num_partitions => 1, replication_factor => 1}],
            #{timeout => 10_000}
        ),
        ok
    catch
        Class:Reason ->
            ct:pal("failed to create topic ~p: ~p:~p", [Topic, Class, Reason]),
            ok
    end.

%% Offsets from which to start consuming, i.e. the latest offsets at the time of
%% the call.  Must be called *before* publishing, otherwise the published
%% messages may be skipped.
kafka_offsets(Topic) ->
    kafka_offsets(?KAFKA_HOSTS, Topic, []).

kafka_offsets(Hosts, Topic, ConnCfg) ->
    {ok, NPartitions} = brod:get_partitions_count(?KAFKA_CLIENT, Topic),
    lists:map(
        fun(Partition) ->
            {ok, Offset} = brod:resolve_offset(Hosts, Topic, Partition, latest, ConnCfg),
            {Partition, Offset}
        end,
        lists:seq(0, NPartitions - 1)
    ).

%% Waits until a message with each of `Values' has been fetched.
recv_values(Topic, Offsets, Values, Timeout) ->
    Check =
        fun(Msgs) ->
            Found = maps:from_list([
                {Value, Msg}
             || Msg = #kafka_message{value = Value} <- Msgs, lists:member(Value, Values)
            ]),
            case [Value || Value <- Values, not is_map_key(Value, Found)] of
                [] -> {ok, [maps:get(Value, Found) || Value <- Values]};
                Missing -> {error, Missing}
            end
        end,
    kafka_wait(Topic, Offsets, Check, Timeout).

%% Waits until a message matching `Predicate' has been fetched.
recv_match(Topic, Offsets, Predicate, Timeout) ->
    Check =
        fun(Msgs) ->
            KafkaMsgs = [Msg || Msg = #kafka_message{} <- Msgs],
            case lists:filter(Predicate, KafkaMsgs) of
                [] -> {error, no_matching_message};
                [Msg | _] -> {ok, Msg}
            end
        end,
    kafka_wait(Topic, Offsets, Check, Timeout).

%% Polls the topic's partitions, accumulating the messages fetched so far,
%% until `Check(Messages)' returns `{ok, Result}' or `Timeout' elapses.
kafka_wait(Topic, Offsets, Check, Timeout) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    kafka_wait_loop(Topic, Offsets, Check, Deadline, []).

kafka_wait_loop(Topic, Offsets, Check, Deadline, Acc) ->
    case Check(Acc) of
        {ok, Result} ->
            Result;
        {error, Missing} ->
            case erlang:monotonic_time(millisecond) >= Deadline of
                true ->
                    ct:fail(#{
                        msg => "timed out waiting for kafka messages",
                        topic => Topic,
                        missing => Missing,
                        fetched => length(Acc)
                    });
                false ->
                    {Offsets1, Msgs} = kafka_fetch(Topic, Offsets),
                    kafka_wait_loop(Topic, Offsets1, Check, Deadline, Acc ++ Msgs)
            end
    end.

kafka_fetch(Topic, Offsets) ->
    lists:mapfoldl(
        fun({Partition, Offset}, Acc) ->
            case brod:fetch(?KAFKA_CLIENT, Topic, Partition, Offset) of
                {ok, {_HighWatermark, []}} ->
                    {{Partition, Offset}, Acc};
                {ok, {_HighWatermark, Msgs}} ->
                    {{Partition, next_offset(Msgs, Offset)}, Acc ++ Msgs};
                {error, Reason} ->
                    %% Transient errors (e.g. leader change) are retried by the
                    %% polling loop.
                    ct:pal("kafka fetch failed: topic=~p partition=~p reason=~p", [
                        Topic, Partition, Reason
                    ]),
                    {{Partition, Offset}, Acc}
            end
        end,
        [],
        Offsets
    ).

next_offset(Msgs, Default) ->
    lists:foldl(
        fun
            (#kafka_message{offset = Offset}, Acc) -> max(Offset + 1, Acc);
            (_, Acc) -> Acc
        end,
        Default,
        Msgs
    ).

%%------------------------------------------------------------------------------
%% Toxiproxy helpers
%%------------------------------------------------------------------------------

proxy_host() ->
    os:getenv("PROXY_HOST", "toxiproxy").

proxy_port() ->
    list_to_integer(os:getenv("PROXY_PORT", "8474")).

%% Enables all proxies again; never fails, so that it is safe to call it from
%% `end_per_testcase' even when toxiproxy is not available.
reset_toxiproxy() ->
    try
        _ = emqx_common_test_helpers:reset_proxy(proxy_host(), proxy_port()),
        ok
    catch
        Class:Reason ->
            ct:pal("failed to reset toxiproxy ~s:~p: ~p:~p", [
                proxy_host(), proxy_port(), Class, Reason
            ]),
            ok
    end.

%% Cuts the connection to every broker: the partition leader of the test topic
%% may be either broker.
cut_kafka_proxies() ->
    _ = emqx_common_test_helpers:enable_failure(
        down, ?PROXY_KAFKA_1, proxy_host(), proxy_port()
    ),
    _ = emqx_common_test_helpers:enable_failure(
        down, ?PROXY_KAFKA_2, proxy_host(), proxy_port()
    ),
    ok.

heal_kafka_proxies() ->
    _ = emqx_common_test_helpers:heal_failure(
        down, ?PROXY_KAFKA_1, proxy_host(), proxy_port()
    ),
    _ = emqx_common_test_helpers:heal_failure(
        down, ?PROXY_KAFKA_2, proxy_host(), proxy_port()
    ),
    ok.

is_disconnected_status(<<"connecting">>) ->
    true;
is_disconnected_status(<<"disconnected">>) ->
    true;
is_disconnected_status(_) ->
    false.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

%% Connector and action can be created and updated via the HTTP API.
t_create_via_http(Config) ->
    ok = emqx_bridge_v2_testlib:t_create_via_http(Config),
    ok.

%% The connector can be stopped and started again, and its action follows it.
%%
%% Note: `emqx_bridge_v2_testlib:t_start_stop/2' is not used here because it
%% requires the connector implementation to emit a snabbkaffe trace event of the
%% caller's choosing, and the Kafka producer implementation emits none.
t_start_stop(Config) ->
    ConnectorName = ?config(connector_name, Config),
    ConnectorType = ?config(connector_type, Config),
    {201, _} = create_connector_api(Config, #{}),
    {201, _} = create_action_api(Config, #{}),
    ResourceId = emqx_bridge_v2_testlib:connector_resource_id(Config),
    ?retry(
        _Sleep = 1_000,
        _Attempts = 20,
        ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId))
    ),
    %% Disabling the connector also stops the action's channel.
    {ok, _} = emqx_connector:disable_enable(disable, ConnectorType, ConnectorName),
    ?retry(
        _Sleep = 500,
        _Attempts = 20,
        ?assertMatch({error, _}, emqx_resource_manager:health_check(ResourceId))
    ),
    %% Starting it again must bring both the connector and the action back up.
    {ok, _} = emqx_connector:disable_enable(enable, ConnectorType, ConnectorName),
    ?retry(
        _Sleep = 1_000,
        _Attempts = 20,
        ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId))
    ),
    ?retry(
        _Sleep = 1_000,
        _Attempts = 20,
        ?assertEqual(<<"connected">>, action_status(Config))
    ),
    ok.

%% A message published to the rule topic is produced to Kafka with the default
%% message templates (whole message as JSON value), and carries the configured
%% extra header.
t_send_message(Config) ->
    ClientId = <<"emqx-kafka-ct-send">>,
    Payload = unique_payload(<<"send">>),
    RuleTopic = <<"t/kafka/send">>,
    ok = start_kafka_client(),
    ok = create_connector_and_action(Config, #{}, #{}),
    {ok, _} = create_rule_and_action_http(Config, RuleTopic, #{}),
    Offsets = kafka_offsets(?TOPIC_1),
    Client = connect_client(ClientId),
    ok = publish(Client, RuleTopic, Payload),
    Predicate =
        fun(#kafka_message{value = Value}) ->
            case emqx_utils_json:safe_decode(Value) of
                {ok, Msg} when is_map(Msg) ->
                    maps:get(<<"payload">>, Msg, undefined) =:= Payload;
                _ ->
                    false
            end
        end,
    Msg = recv_match(?TOPIC_1, Offsets, Predicate, 30_000),
    #kafka_message{key = Key, value = Value, headers = Headers} = Msg,
    %% `message.key' defaults to `${.clientid}'.
    ?assertEqual(ClientId, Key),
    %% `message.value' defaults to the whole message, JSON encoded.
    ?assertMatch(
        #{<<"clientid">> := ClientId, <<"payload">> := Payload},
        emqx_utils_json:decode(Value)
    ),
    %% Extra header from `kafka_ext_headers'.
    ?assertMatch({<<"clientid">>, ClientId}, lists:keyfind(<<"clientid">>, 1, Headers)),
    ok.

%% The topic may be a template; messages must land in the rendered topic.
t_dynamic_topic(Config) ->
    ClientId = <<"emqx-kafka-dyn">>,
    Topic = <<ClientId/binary, "-topic">>,
    Payload = unique_payload(<<"dyn">>),
    RuleTopic = <<"t/kafka/dynamic">>,
    ok = start_kafka_client(),
    ok = create_connector_and_action(
        Config,
        #{<<"allow_auto_topic_creation">> => true},
        #{
            <<"parameters">> => #{
                <<"topic">> => <<"${clientid}-topic">>,
                <<"message">> => #{<<"value">> => <<"${payload}">>}
            }
        }
    ),
    {ok, _} = create_rule_and_action_http(Config, RuleTopic, #{}),
    ok = ensure_topic(Topic),
    Offsets = kafka_offsets(Topic),
    Client = connect_client(ClientId),
    ok = publish(Client, RuleTopic, Payload),
    [Msg] = recv_values(Topic, Offsets, [Payload], 30_000),
    ?assertMatch(#kafka_message{key = ClientId, value = Payload}, Msg),
    ok.

%% SCRAM-SHA-512 authentication against the SASL listener.
t_scram_auth(Config) ->
    ClientId = <<"emqx-kafka-ct-scram">>,
    Payload = unique_payload(<<"scram">>),
    RuleTopic = <<"t/kafka/scram">>,
    ok = start_kafka_client(?KAFKA_SASL_HOSTS, ?KAFKA_SASL_OPTS),
    ok = create_connector_and_action(
        Config,
        #{
            <<"bootstrap_hosts">> => <<"kafka-1.emqx.net:9093">>,
            <<"authentication">> =>
                #{
                    <<"mechanism">> => <<"scram_sha_512">>,
                    <<"username">> => ?SCRAM_USER,
                    <<"password">> => ?SCRAM_PASSWORD
                }
        },
        #{}
    ),
    {ok, _} = create_rule_and_action_http(Config, RuleTopic, #{}),
    Offsets = kafka_offsets(?KAFKA_SASL_HOSTS, ?TOPIC_1, ?KAFKA_SASL_OPTS),
    Client = connect_client(ClientId),
    ok = publish(Client, RuleTopic, Payload),
    Msg = recv_match(
        ?TOPIC_1,
        Offsets,
        fun(#kafka_message{key = Key}) -> Key =:= ClientId end,
        30_000
    ),
    ?assertMatch(#kafka_message{key = ClientId}, Msg),
    ok.

%% While Kafka is unreachable, the action must not report itself as connected
%% and must keep the messages buffered (`wolff' internal buffer) until the
%% connection is restored, at which point they must all be produced.
t_connection_failure_buffering(Config) ->
    ClientId = <<"emqx-kafka-ct-buffering">>,
    Payloads = [unique_payload(<<"buffered">>) || _ <- lists:seq(1, 5)],
    RuleTopic = <<"t/kafka/buffering">>,
    ok = start_kafka_client(),
    ok = create_connector_and_action(
        Config,
        #{<<"bootstrap_hosts">> => <<"toxiproxy.emqx.net:9292">>},
        #{<<"parameters">> => #{<<"message">> => #{<<"value">> => <<"${payload}">>}}}
    ),
    {ok, _} = create_rule_and_action_http(Config, RuleTopic, #{}),
    Offsets = kafka_offsets(?TOPIC_1),
    ct:pal("cutting the connection to kafka"),
    ok = cut_kafka_proxies(),
    %% The action must never silently report itself as connected while the
    %% connection is down.
    ?retry(
        _Sleep = 500,
        _Attempts = 60,
        ?assert(is_disconnected_status(action_status(Config)))
    ),
    Client = connect_client(ClientId),
    lists:foreach(fun(Payload) -> ok = publish(Client, RuleTopic, Payload) end, Payloads),
    %% The rule fires and the queries are accepted (and thus buffered) even
    %% though the connector is disconnected.
    ?retry(
        _Sleep = 500,
        _Attempts = 40,
        ?assertEqual(length(Payloads), action_metric(<<"matched">>, Config))
    ),
    ct:pal("restoring the connection to kafka"),
    ok = heal_kafka_proxies(),
    ?retry(
        _Sleep = 1_000,
        _Attempts = 60,
        ?assertMatch(
            {200, #{<<"status">> := <<"connected">>}},
            get_connector_api(Config)
        )
    ),
    Msgs = recv_values(?TOPIC_1, Offsets, Payloads, 60_000),
    ?assertEqual(length(Payloads), length(Msgs)),
    ok.

%% Deleting the action and then the connector must get rid of both.
t_delete_action_and_connector(Config) ->
    Type = ?config(action_type, Config),
    Name = ?config(action_name, Config),
    ConnectorType = ?config(connector_type, Config),
    ConnectorName = ?config(connector_name, Config),
    {201, _} = create_connector_api(Config, #{}),
    {201, _} = create_action_api(Config, #{}),
    ?assertMatch(
        {204, _},
        emqx_bridge_v2_testlib:delete_kind_api(action, Type, Name)
    ),
    ?assertMatch({404, _}, get_action_api(Config)),
    ok = emqx_connector:remove(ConnectorType, ConnectorName),
    ?assertMatch({404, _}, get_connector_api(Config)),
    ok.
