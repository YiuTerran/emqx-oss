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

%% @doc Kafka producer connector/action resource.
%%
%% Messages are buffered by `wolff' itself (using `replayq'), therefore the
%% resource query mode is `simple_sync_internal_buffer' or
%% `simple_async_internal_buffer', which tells the resource framework not to
%% buffer/batch the queries on its own.
-module(emqx_bridge_kafka_impl_producer).

-behaviour(emqx_resource).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

%% `emqx_resource' callbacks
-export([
    resource_type/0,
    callback_mode/0,
    query_mode/1,
    query_opts/1,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_query_async/4,
    on_get_status/2,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channels/1,
    on_get_channel_status/3
]).

%% `wolff' acknowledgement callback
-export([on_kafka_ack/3]).

-ifdef(TEST).
-export([
    replayq_dir/2,
    preproc_topic/1,
    render_topic/2,
    compile_message_template/1,
    render_message/3,
    preproc_ext_headers/1,
    render_headers/4,
    producers_config/5
]).
-endif.

-define(kafka_client_id, kafka_client_id).
-define(kafka_producers, kafka_producers).

%% Topic used to probe the connectivity of the cluster.  It does not need to
%% exist: the probe only checks that metadata requests are answered.
-define(PROBE_TOPIC_NAME, <<"emqx-connector-connectivity-probe">>).

-define(REPLAYQ_ROOT_SUBDIR, "kafka_producer").

%%----------------------------------------------------------------------------------------
%% Behaviour `emqx_resource' callbacks
%%----------------------------------------------------------------------------------------

resource_type() ->
    kafka_producer.

callback_mode() ->
    async_if_possible.

%% The Kafka producer has an internal buffer (`wolff' + `replayq'), so the
%% resource framework must not buffer the messages itself.  In `sync' mode the
%% caller waits for the Kafka acknowledgement; in the default `async' mode the
%% query returns as soon as the message is enqueued in `wolff'.
query_mode(ChannelConfig) ->
    case maps:get(query_mode, emqx_resource:fetch_creation_opts(ChannelConfig), async) of
        sync -> simple_sync_internal_buffer;
        async -> simple_async_internal_buffer
    end.

%% Maximum time a synchronous query waits for the Kafka acknowledgement.
query_opts(ChannelConfig) ->
    Opts = emqx_resource:fetch_creation_opts(ChannelConfig),
    #{timeout => maps:get(request_ttl, Opts, ?DEFAULT_REQUEST_TTL)}.

%%----------------------------------------------------------------------------------------
%% Start/stop
%%----------------------------------------------------------------------------------------

on_start(InstId, Config) ->
    ?SLOG(debug, #{
        msg => "kafka_producer_connector_starting",
        instance_id => InstId,
        config => emqx_utils:redact(Config)
    }),
    ClientId = InstId,
    HealthCheckTopic = maps:get(health_check_topic, Config, ?PROBE_TOPIC_NAME),
    try
        Hosts = emqx_bridge_kafka_impl:hosts(maps:get(bootstrap_hosts, Config)),
        ClientConfig = client_config(Config),
        ok = ensure_client(ClientId, Hosts, ClientConfig),
        ok = emqx_resource:allocate_resource(InstId, ?kafka_client_id, ClientId),
        case check_client_connectivity(InstId, ClientId, HealthCheckTopic) of
            ok ->
                {ok, #{
                    client_id => ClientId,
                    health_check_topic => HealthCheckTopic,
                    installed_channels => #{}
                }};
            {error, StartError} ->
                %% Fail the start so that it is retried: the channels (and thus
                %% the `wolff' producers and their on-disk buffers) must not be
                %% started while the cluster is unreachable.
                ?SLOG(warning, #{
                    msg => "kafka_producer_connector_not_connected",
                    instance_id => InstId,
                    reason => StartError
                }),
                {error, StartError}
        end
    catch
        throw:ThrowReason ->
            ?SLOG(error, #{
                msg => "failed_to_start_kafka_producer_connector",
                instance_id => InstId,
                reason => ThrowReason
            }),
            {error, ThrowReason};
        Class:ErrorReason:Stacktrace ->
            ?SLOG(error, #{
                msg => "failed_to_start_kafka_producer_connector",
                instance_id => InstId,
                exception => Class,
                reason => ErrorReason,
                stacktrace => Stacktrace
            }),
            {error, ErrorReason}
    end.

%% The `wolff' producers (and their replayq) are stopped here, but their
%% on-disk buffer is preserved: when the connector is restarted with the same
%% action name, the pending messages are replayed.
on_stop(InstId, _State) ->
    Allocated = emqx_resource:get_allocated_resources(InstId),
    maps:foreach(
        fun
            ({?kafka_producers, ActionResId}, Producers) ->
                ?SLOG(debug, #{
                    msg => "stopping_kafka_producers",
                    instance_id => InstId,
                    action_id => ActionResId
                }),
                deallocate_producers(Producers);
            (_, _) ->
                ok
        end,
        Allocated
    ),
    case maps:get(?kafka_client_id, Allocated, undefined) of
        undefined ->
            ok;
        ClientId ->
            ?SLOG(debug, #{msg => "stopping_kafka_client", instance_id => InstId}),
            deallocate_client(ClientId)
    end,
    ok.

%%----------------------------------------------------------------------------------------
%% Channels (actions)
%%----------------------------------------------------------------------------------------

on_add_channel(
    InstId,
    #{client_id := ClientId, installed_channels := Channels} = OldState,
    ActionResId,
    ActionConfig
) ->
    case create_producers_for_channel(InstId, ActionResId, ClientId, ActionConfig) of
        {ok, ChannelState} ->
            {ok, OldState#{
                installed_channels => maps:put(ActionResId, ChannelState, Channels)
            }};
        {error, Reason} ->
            {error, Reason}
    end.

on_remove_channel(InstId, #{installed_channels := Channels} = OldState, ActionResId) ->
    case maps:take(ActionResId, Channels) of
        error ->
            {ok, OldState};
        {#{producers := Producers}, Channels1} ->
            ?SLOG(debug, #{
                msg => "removing_kafka_producers",
                instance_id => InstId,
                action_id => ActionResId
            }),
            deallocate_producers(Producers),
            {ok, OldState#{installed_channels => Channels1}}
    end.

on_get_channels(InstId) ->
    emqx_bridge_v2:get_channels_for_connector(InstId).

%% Note: if the connector is reported as `disconnected', the resource manager
%% restarts the connector (and its producers), which may drop the messages
%% pending in the `wolff' replayq.  Hence only `connected' and `connecting' are
%% returned here once the resource has been started.
on_get_status(InstId, #{client_id := ClientId, health_check_topic := HealthCheckTopic}) ->
    case check_client_connectivity(InstId, ClientId, HealthCheckTopic) of
        ok ->
            ?status_connected;
        {error, #{reason := cannot_find_kafka_client}} ->
            %% The Kafka client is being (re)started.
            ?status_connecting;
        {error, Reason} ->
            {?status_connecting, Reason}
    end.

on_get_channel_status(
    _InstId,
    ActionResId,
    #{client_id := ClientId, installed_channels := Channels}
) ->
    case maps:find(ActionResId, Channels) of
        {ok, #{topic := KafkaTopic, partitions_limit := MaxPartitions}} ->
            try
                ok = assert_topic_and_leader_connections(
                    ActionResId, ClientId, KafkaTopic, MaxPartitions
                ),
                ?status_connected
            catch
                throw:{unhealthy_target, Msg} ->
                    {?status_connecting, Msg};
                Class:Reason ->
                    {?status_connecting, {Class, Reason}}
            end;
        error ->
            {?status_connecting, #{reason => channel_not_found, action_id => ActionResId}}
    end.

%%----------------------------------------------------------------------------------------
%% Queries
%%----------------------------------------------------------------------------------------

on_query(_InstId, {ActionResId, Msg}, #{installed_channels := Channels}) ->
    case maps:find(ActionResId, Channels) of
        {ok, ChannelState} ->
            do_query(sync, ActionResId, undefined, ChannelState, Msg);
        error ->
            {error, {unrecoverable_error, {channel_not_found, ActionResId}}}
    end.

on_query_async(_InstId, {ActionResId, Msg}, ReplyFn, #{installed_channels := Channels}) ->
    case maps:find(ActionResId, Channels) of
        {ok, ChannelState} ->
            do_query(async, ActionResId, ReplyFn, ChannelState, Msg);
        error ->
            {error, {unrecoverable_error, {channel_not_found, ActionResId}}}
    end.

do_query(Mode, ActionResId, ReplyFn, ChannelState, Msg) ->
    #{
        topic_template := TopicTemplate,
        message_template := MessageTemplate,
        headers_template := HeadersTemplate,
        ext_headers_template := ExtHeadersTemplate,
        headers_encode_mode := HeadersEncodeMode,
        producers := Producers,
        sync_query_timeout := SyncQueryTimeout
    } = ChannelState,
    try
        KafkaTopic = render_topic(TopicTemplate, Msg),
        KafkaHeaders = render_headers(HeadersTemplate, ExtHeadersTemplate, HeadersEncodeMode, Msg),
        KafkaMessage = render_message(MessageTemplate, KafkaHeaders, Msg),
        ok = emqx_trace:rendered_action_template(ActionResId, #{message => KafkaMessage}),
        do_send_msg(Mode, KafkaTopic, KafkaMessage, Producers, SyncQueryTimeout, ReplyFn)
    catch
        throw:bad_topic ->
            ?SLOG(warning, #{
                msg => "kafka_topic_render_failed",
                action_id => ActionResId,
                message => Msg
            }),
            {error, {unrecoverable_error, failed_to_render_topic}};
        throw:#{cause := unknown_topic_or_partition, topic := Topic} ->
            {error, {unrecoverable_error, {resolved_to_unknown_topic, Topic}}};
        throw:#{cause := invalid_partition_count, count := Count} ->
            {error, {unrecoverable_error, {invalid_partition_count, Count}}};
        throw:{bad_kafka_header, _} = Reason ->
            {error, {unrecoverable_error, Reason}};
        throw:{bad_kafka_headers, _} = Reason ->
            {error, {unrecoverable_error, Reason}}
    end.

do_send_msg(sync, KafkaTopic, KafkaMessage, Producers, SyncTimeout, _ReplyFn) ->
    try
        case wolff:send_sync2(Producers, KafkaTopic, [KafkaMessage], SyncTimeout) of
            {_Partition, Offset} when is_integer(Offset) ->
                ok;
            {_Partition, message_expired} ->
                {error, request_expired};
            {_Partition, DropReason} ->
                {error, DropReason}
        end
    catch
        error:{producer_down, _} = Reason ->
            {error, Reason};
        error:timeout ->
            {error, timeout}
    end;
do_send_msg(async, KafkaTopic, KafkaMessage, Producers, _SyncTimeout, ReplyFn) ->
    %% * Must be a batch because wolff send and cast are batch APIs
    %% * Must be a single element batch because wolff books calls, but not batch
    %%   sizes, for counters and gauges.
    Batch = [KafkaMessage],
    {_Partition, Pid} = wolff:send2(
        Producers, KafkaTopic, Batch, {fun ?MODULE:on_kafka_ack/3, [ReplyFn]}
    ),
    %% This Pid is returned, but not monitored by the caller;
    %% see emqx_resource_buffer_worker:simple_async_internal_buffer
    {ok, Pid}.

%% Called by `wolff' when the produce request is acknowledged by the cluster (or
%% when the message is dropped by wolff's own buffer).
on_kafka_ack(_Partition, Offset, ReplyFn) when is_integer(Offset) ->
    emqx_resource:apply_reply_fun(ReplyFn, ok);
on_kafka_ack(_Partition, buffer_overflow_discarded, ReplyFn) ->
    emqx_resource:apply_reply_fun(ReplyFn, {error, buffer_overflow});
on_kafka_ack(_Partition, message_too_large, ReplyFn) ->
    emqx_resource:apply_reply_fun(ReplyFn, {error, message_too_large});
%% The following reasons only exist in newer `wolff' versions, but they are kept
%% here for forward compatibility.
on_kafka_ack(_Partition, message_expired, ReplyFn) ->
    emqx_resource:apply_reply_fun(ReplyFn, {error, request_expired});
on_kafka_ack(_Partition, max_retry_exceeded, ReplyFn) ->
    emqx_resource:apply_reply_fun(ReplyFn, {error, max_retry_exceeded});
on_kafka_ack(_Partition, partition_lost, ReplyFn) ->
    emqx_resource:apply_reply_fun(ReplyFn, {error, partition_lost});
on_kafka_ack(_Partition, Reason, ReplyFn) ->
    emqx_resource:apply_reply_fun(ReplyFn, {error, Reason}).

%%----------------------------------------------------------------------------------------
%% Kafka client management
%%----------------------------------------------------------------------------------------

client_config(Config) ->
    #{
        min_metadata_refresh_interval =>
            maps:get(min_metadata_refresh_interval, Config, 3_000),
        connect_timeout => maps:get(connect_timeout, Config, 5_000),
        %% Note: wolff also uses this as the metadata request timeout.
        request_timeout => maps:get(request_timeout, Config, 30_000),
        extra_sock_opts =>
            emqx_bridge_kafka_impl:socket_opts(maps:get(socket_opts, Config, #{})),
        sasl => emqx_bridge_kafka_impl:sasl(maps:get(authentication, Config, none)),
        ssl => emqx_bridge_kafka_impl:ssl(maps:get(ssl, Config, #{})),
        allow_auto_topic_creation => maps:get(allow_auto_topic_creation, Config, false)
    }.

ensure_client(ClientId, Hosts, ClientConfig) ->
    case wolff_client_sup:find_client(ClientId) of
        {ok, _Pid} ->
            ok;
        {error, #{reason := no_such_client}} ->
            case wolff:ensure_supervised_client(ClientId, Hosts, ClientConfig) of
                {ok, _Pid} ->
                    ?SLOG(info, #{
                        msg => "kafka_client_started",
                        client_id => ClientId,
                        kafka_hosts => Hosts
                    }),
                    ok;
                {error, Reason} ->
                    throw({failed_to_start_kafka_client, Reason})
            end;
        {error, Reason} ->
            throw({failed_to_find_kafka_client, Reason})
    end.

deallocate_client(ClientId) ->
    _ = emqx_resource:deallocate_resource(ClientId, ?kafka_client_id),
    _ = wolff:stop_and_delete_supervised_client(ClientId),
    ok.

deallocate_producers(Producers) ->
    _ = wolff:stop_and_delete_supervised_producers(Producers),
    ok.

%%----------------------------------------------------------------------------------------
%% Producers (channels)
%%----------------------------------------------------------------------------------------

create_producers_for_channel(ConnResId, ActionResId, ClientId, ActionConfig) ->
    #{
        bridge_type := BridgeType,
        bridge_name := BridgeName,
        parameters := KafkaConfig
    } = ActionConfig,
    TopicTemplate = preproc_topic(maps:get(topic, KafkaConfig)),
    HeadersTemplate = preproc_kafka_headers(maps:get(kafka_headers, KafkaConfig, undefined)),
    ExtHeadersTemplate = preproc_ext_headers(maps:get(kafka_ext_headers, KafkaConfig, [])),
    HeadersEncodeMode = maps:get(kafka_header_value_encode_mode, KafkaConfig, none),
    MaxPartitions = maps:get(partitions_limit, KafkaConfig, all_partitions),
    Topic = topic_of(TopicTemplate),
    IsDryRun = emqx_resource:is_dry_run(ActionResId),
    WolffProducerConfig = producers_config(
        BridgeType, BridgeName, KafkaConfig, IsDryRun, ActionResId
    ),
    try
        ok = assert_topic_and_leader_connections(ActionResId, ClientId, Topic, MaxPartitions),
        case wolff:ensure_supervised_dynamic_producers(ClientId, WolffProducerConfig) of
            {ok, Producers} ->
                ok = add_fixed_topic(TopicTemplate, Producers),
                ok = emqx_resource:allocate_resource(
                    ConnResId, {?kafka_producers, ActionResId}, Producers
                ),
                {ok, #{
                    message_template => compile_message_template(
                        maps:get(message, KafkaConfig, #{})
                    ),
                    topic_template => TopicTemplate,
                    topic => Topic,
                    headers_template => HeadersTemplate,
                    ext_headers_template => ExtHeadersTemplate,
                    headers_encode_mode => HeadersEncodeMode,
                    partitions_limit => MaxPartitions,
                    sync_query_timeout => sync_query_timeout(ActionConfig),
                    producers => Producers,
                    action_resource_id => ActionResId,
                    connector_resource_id => ConnResId
                }};
            {error, StartError} ->
                ?SLOG(error, #{
                    msg => "failed_to_start_kafka_producers",
                    connector_resource_id => ConnResId,
                    action_resource_id => ActionResId,
                    topic => Topic,
                    reason => StartError
                }),
                {error, StartError}
        end
    catch
        throw:ThrowReason ->
            ?SLOG(error, #{
                msg => "failed_to_add_kafka_producers",
                connector_resource_id => ConnResId,
                action_resource_id => ActionResId,
                topic => Topic,
                reason => ThrowReason
            }),
            {error, ThrowReason}
    end.

producers_config(BridgeType, BridgeName, Input, IsDryRun, ActionResId) ->
    #{
        max_linger_time := MaxLingerTime,
        max_linger_bytes := MaxLingerBytes,
        max_batch_bytes := MaxBatchBytes,
        compression := Compression,
        partition_strategy := PartitionStrategy,
        required_acks := RequiredAcks,
        partition_count_refresh_interval := PartitionCountRefreshInterval,
        max_inflight := MaxInflight,
        partitions_limit := MaxPartitions,
        buffer := #{
            mode := BufferMode0,
            per_partition_limit := PerPartitionLimit,
            segment_bytes := SegmentBytes,
            memory_overload_protection := MemoryOverloadProtection
        }
    } = Input,
    %% A dry run (rule test) must not touch the file system.
    BufferMode =
        case IsDryRun of
            true -> memory;
            false -> BufferMode0
        end,
    ReplayqDir =
        case BufferMode of
            memory -> false;
            _ -> replayq_dir(BridgeType, BridgeName)
        end,
    #{
        group => ActionResId,
        partitioner => partitioner(PartitionStrategy),
        %% Note: the schema value is in seconds.
        partition_count_refresh_interval_seconds => PartitionCountRefreshInterval,
        replayq_dir => ReplayqDir,
        replayq_offload_mode => BufferMode =:= hybrid,
        replayq_max_total_bytes => PerPartitionLimit,
        replayq_seg_bytes => SegmentBytes,
        drop_if_highmem => MemoryOverloadProtection,
        required_acks => RequiredAcks,
        max_linger_ms => MaxLingerTime,
        max_linger_bytes => MaxLingerBytes,
        max_batch_bytes => MaxBatchBytes,
        %% wolff counts the batch currently being sent as "sent ahead" too.
        max_send_ahead => max(MaxInflight - 1, 0),
        compression => Compression,
        max_partitions => MaxPartitions,
        telemetry_meta_data => #{bridge_id => ActionResId}
    }.

partitioner(random) ->
    random;
partitioner(key_dispatch) ->
    first_key_dispatch.

%% Directory where the `replayq' buffers of an action are stored.  Within it,
%% wolff creates one sub directory per (action, topic, partition).
replayq_dir(BridgeType, BridgeName) when is_atom(BridgeType) ->
    replayq_dir(atom_to_binary(BridgeType), BridgeName);
replayq_dir(BridgeType, BridgeName) when is_binary(BridgeName) ->
    Node = atom_to_binary(node()),
    DirName = <<BridgeType/binary, $:, BridgeName/binary, $:, Node/binary>>,
    filename:join([emqx:data_dir(), ?REPLAYQ_ROOT_SUBDIR, DirName]).

%%----------------------------------------------------------------------------------------
%% Health check
%%----------------------------------------------------------------------------------------

check_client_connectivity(ConnResId, ClientId, HealthCheckTopic) ->
    try
        assert_topic_and_leader_connections(
            ConnResId, ClientId, HealthCheckTopic, all_partitions
        )
    catch
        throw:{unhealthy_target, _Msg} = Reason ->
            {error, Reason};
        throw:#{reason := {connection_down, _} = Reason} ->
            {error, Reason};
        throw:#{reason := Reason} ->
            {error, Reason};
        throw:Reason ->
            {error, Reason};
        Class:Reason:Stacktrace ->
            {error, {Class, Reason, Stacktrace}}
    end.

assert_topic_and_leader_connections(ActionResId, ClientId, KafkaTopic, MaxPartitions) ->
    case wolff_client_sup:find_client(ClientId) of
        {ok, ClientPid} ->
            case is_binary(KafkaTopic) of
                true ->
                    ok = check_topic_status(ClientId, ClientPid, KafkaTopic),
                    ok = check_if_healthy_leaders(
                        ActionResId, ClientId, ClientPid, KafkaTopic, MaxPartitions
                    );
                false ->
                    %% Dynamic topic: it is only resolved when a message is sent.
                    ok
            end;
        {error, #{reason := no_such_client}} ->
            throw(#{
                reason => cannot_find_kafka_client,
                kafka_client => ClientId,
                kafka_topic => KafkaTopic
            });
        {error, #{reason := client_supervisor_not_initialized}} ->
            throw(#{
                reason => restarting,
                kafka_client => ClientId,
                kafka_topic => KafkaTopic
            });
        {error, Reason} ->
            throw(#{
                reason => Reason,
                kafka_client => ClientId,
                kafka_topic => KafkaTopic
            })
    end.

check_topic_status(ClientId, WolffClientPid, KafkaTopic) ->
    case wolff_client:check_topic_exists_with_client_pid(WolffClientPid, KafkaTopic) of
        ok ->
            ok;
        {error, Reason} when
            KafkaTopic =:= ?PROBE_TOPIC_NAME andalso
                (Reason =:= unknown_topic_or_partition orelse
                    Reason =:= topic_authorization_failed)
        ->
            %% The probing topic is only used to check that metadata requests
            %% can be sent, it does not need to exist.
            ok;
        {error, unknown_topic_or_partition} ->
            Msg = iolist_to_binary([<<"Unknown topic or partition: ">>, KafkaTopic]),
            throw({unhealthy_target, Msg});
        {error, Reason} ->
            throw(#{
                error => failed_to_check_topic_status,
                reason => Reason,
                kafka_client => ClientId,
                kafka_topic => KafkaTopic
            })
    end.

check_if_healthy_leaders(_ActionResId, _ClientId, _ClientPid, ?PROBE_TOPIC_NAME, _MaxPartitions) ->
    %% The probing topic is not expected to have partition leaders.
    ok;
check_if_healthy_leaders(ActionResId, ClientId, ClientPid, KafkaTopic, MaxPartitions) when
    is_pid(ClientPid)
->
    case wolff_client:get_leader_connections(ClientPid, ActionResId, KafkaTopic, MaxPartitions) of
        {ok, Leaders} ->
            %% Kafka is considered healthy as long as any of the partition
            %% leaders is reachable.
            case lists:partition(fun({_Partition, Pid}) -> is_alive(Pid) end, Leaders) of
                {[], Errors} ->
                    throw(
                        error_summary(
                            #{
                                reason => no_connected_partition_leader,
                                kafka_client => ClientId,
                                kafka_topic => KafkaTopic
                            },
                            Errors
                        )
                    );
                {_, []} ->
                    ok;
                {_, Errors} ->
                    ?SLOG(
                        warning,
                        error_summary(
                            #{
                                msg => "not_all_kafka_partitions_connected",
                                kafka_client => ClientId,
                                kafka_topic => KafkaTopic
                            },
                            Errors
                        )
                    ),
                    ok
            end;
        {error, Reason} ->
            %% wolff_client logs the reason for each seed host.
            throw(#{
                reason => Reason,
                kafka_client => ClientId,
                kafka_topic => KafkaTopic
            })
    end.

is_alive(Pid) ->
    is_pid(Pid) andalso erlang:is_process_alive(Pid).

error_summary(Map, [Error]) ->
    Map#{error => Error};
error_summary(Map, [Error | More]) ->
    Map#{first_error => Error, total_errors => length(More) + 1}.

%%----------------------------------------------------------------------------------------
%% Message rendering
%%----------------------------------------------------------------------------------------

preproc_topic(Topic) ->
    Template = emqx_template:parse(Topic),
    case emqx_template:placeholders(Template) of
        [] ->
            {fixed, emqx_utils_conv:bin(Topic)};
        [_ | _] ->
            {dynamic, Template}
    end.

topic_of({fixed, Topic}) ->
    Topic;
topic_of({dynamic, _}) ->
    dynamic.

add_fixed_topic({fixed, KafkaTopic}, Producers) ->
    case wolff:add_topic(Producers, KafkaTopic) of
        ok ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end;
add_fixed_topic({dynamic, _}, _Producers) ->
    ok.

compile_message_template(T) ->
    KeyTemplate = maps:get(key, T, <<"${.clientid}">>),
    ValueTemplate = maps:get(value, T, <<"${.}">>),
    TimestampTemplate = maps:get(timestamp, T, <<"${.timestamp}">>),
    #{
        key => emqx_placeholder:preproc_tmpl(KeyTemplate),
        value => emqx_placeholder:preproc_tmpl(ValueTemplate),
        timestamp => emqx_placeholder:preproc_tmpl(TimestampTemplate)
    }.

render_topic({fixed, KafkaTopic}, _Message) ->
    KafkaTopic;
render_topic({dynamic, Template}, Message) ->
    try
        iolist_to_binary(emqx_template:render_strict(Template, {emqx_jsonish, Message}))
    catch
        error:_Errors ->
            throw(bad_topic)
    end.

render_message(Templates, Headers, Message) ->
    #{key := KeyTemplate, value := ValueTemplate, timestamp := TimestampTemplate} = Templates,
    #{
        key => render(KeyTemplate, Message),
        value => render(ValueTemplate, Message),
        headers => Headers,
        ts => render_timestamp(TimestampTemplate, Message)
    }.

render(Template, Message) ->
    Opts = #{
        var_trans => fun
            (undefined) -> <<"">>;
            (X) -> emqx_utils_conv:bin(X)
        end,
        return => full_binary
    },
    emqx_placeholder:proc_tmpl(Template, Message, Opts).

render_timestamp(Template, Message) ->
    try
        binary_to_integer(render(Template, Message))
    catch
        _:_ ->
            erlang:system_time(millisecond)
    end.

%%----------------------------------------------------------------------------------------
%% Headers
%%----------------------------------------------------------------------------------------

preproc_kafka_headers(HeadersTmpl) when HeadersTmpl =:= <<>>; HeadersTmpl =:= undefined ->
    undefined;
preproc_kafka_headers(HeadersTmpl) ->
    %% The template is validated by the schema.
    emqx_placeholder:preproc_tmpl(HeadersTmpl).

preproc_ext_headers(Headers) ->
    [
        {emqx_placeholder:preproc_tmpl(K), emqx_placeholder:preproc_tmpl(V)}
     || #{kafka_ext_header_key := K, kafka_ext_header_value := V} <- Headers
    ].

render_headers(HeadersTemplate, ExtHeadersTemplates, EncodeMode, Message) ->
    ExtHeaders = proc_ext_headers(ExtHeadersTemplates, Message),
    Headers =
        case HeadersTemplate of
            undefined ->
                ExtHeaders;
            _ ->
                merge_kafka_headers(HeadersTemplate, ExtHeaders, Message)
        end,
    formalize_kafka_headers(Headers, EncodeMode).

proc_ext_headers(ExtHeaders, Msg) ->
    lists:filtermap(
        fun({KTks, VTks}) ->
            try
                Key = proc_ext_headers_key(KTks, Msg),
                Value = proc_ext_headers_value(VTks, Msg),
                {true, {Key, Value}}
            catch
                throw:placeholder_not_found ->
                    false
            end
        end,
        ExtHeaders
    ).

proc_ext_headers_key(KeyTks, Msg) ->
    RawList = emqx_placeholder:proc_tmpl(KeyTks, Msg, #{return => rawlist}),
    list_to_binary(
        lists:map(
            fun
                (undefined) -> throw(placeholder_not_found);
                (Key) -> emqx_utils_conv:bin(Key)
            end,
            RawList
        )
    ).

proc_ext_headers_value(ValTks, Msg) ->
    case emqx_placeholder:proc_tmpl(ValTks, Msg, #{return => rawlist}) of
        [undefined] -> throw(placeholder_not_found);
        [Value] -> Value
    end.

merge_kafka_headers(HeadersTks, ExtHeaders, Msg) ->
    case emqx_placeholder:proc_tmpl(HeadersTks, Msg, #{return => rawlist}) of
        %% Headers given as a map object.
        [Map] when is_map(Map) ->
            maps:to_list(Map) ++ ExtHeaders;
        [KVList] when is_list(KVList) ->
            kvlist_headers(KVList, []) ++ ExtHeaders;
        %% The placeholder cannot be found in the message.
        [undefined] ->
            ExtHeaders;
        [MaybeJson] when is_binary(MaybeJson) ->
            case emqx_utils_json:safe_decode(MaybeJson) of
                {ok, JsonTerm} when is_map(JsonTerm) ->
                    maps:to_list(JsonTerm) ++ ExtHeaders;
                {ok, JsonTerm} when is_list(JsonTerm) ->
                    kvlist_headers(JsonTerm, []) ++ ExtHeaders;
                _ ->
                    throw({bad_kafka_headers, MaybeJson})
            end;
        BadHeaders ->
            throw({bad_kafka_headers, BadHeaders})
    end.

kvlist_headers([], Acc) ->
    lists:reverse(Acc);
kvlist_headers([#{key := K, value := V} | Headers], Acc) ->
    kvlist_headers(Headers, [{K, V} | Acc]);
kvlist_headers([#{<<"key">> := K, <<"value">> := V} | Headers], Acc) ->
    kvlist_headers(Headers, [{K, V} | Acc]);
kvlist_headers([{K, V} | Headers], Acc) ->
    kvlist_headers(Headers, [{K, V} | Acc]);
kvlist_headers([KVList | Headers], Acc) when is_list(KVList) ->
    %% For instance, when the user sets a JSON list as headers, such as
    %% '[{"foo":"bar"}, {"foo2":"bar2"}]'.
    kvlist_headers(KVList ++ Headers, Acc);
kvlist_headers([BadHeader | _], _) ->
    throw({bad_kafka_header, BadHeader}).

-define(IS_STR_KEY(K), (is_list(K) orelse is_atom(K) orelse is_binary(K))).

formalize_kafka_headers(Headers, none) ->
    %% Note that all non-binary values are dropped in the `none' mode.
    [{bin(K), V} || {K, V} <- Headers, is_binary(V) andalso ?IS_STR_KEY(K)];
formalize_kafka_headers(Headers, json) ->
    lists:filtermap(
        fun({K, V}) ->
            try
                {true, {bin(K), emqx_utils_json:encode(V)}}
            catch
                _:_ -> false
            end
        end,
        Headers
    ).

%%----------------------------------------------------------------------------------------
%% Misc
%%----------------------------------------------------------------------------------------

sync_query_timeout(ActionConfig) ->
    Opts = emqx_resource:fetch_creation_opts(ActionConfig),
    maps:get(request_ttl, Opts, ?DEFAULT_REQUEST_TTL).

bin(X) ->
    emqx_utils_conv:bin(X).
