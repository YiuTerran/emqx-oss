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
-module(emqx_bridge_kafka_impl_tests).

-include_lib("eunit/include/eunit.hrl").

-import(emqx_bridge_kafka_impl_producer, [
    compile_message_template/1,
    preproc_ext_headers/1,
    preproc_topic/1,
    render_headers/4,
    render_message/3,
    render_topic/2
]).

%%------------------------------------------------------------------------------
%% Connection configuration
%%------------------------------------------------------------------------------

hosts_binary_test() ->
    ?assertEqual(
        [{"127.0.0.1", 9092}, {"kafka.example.com", 9093}],
        emqx_bridge_kafka_impl:hosts(<<"127.0.0.1:9092,kafka.example.com:9093">>)
    ),
    ?assertEqual(
        [{"127.0.0.1", 9092}],
        emqx_bridge_kafka_impl:hosts(<<"127.0.0.1:9092">>)
    ).

hosts_list_test() ->
    ?assertEqual(
        [{"127.0.0.1", 9092}, {"kafka.example.com", 9093}],
        emqx_bridge_kafka_impl:hosts([<<"127.0.0.1:9092">>, <<"kafka.example.com:9093">>])
    ),
    ?assertEqual(
        [{"127.0.0.1", 9092}],
        emqx_bridge_kafka_impl:hosts(["127.0.0.1:9092"])
    ).

hosts_parsed_test() ->
    ?assertEqual(
        [{"127.0.0.1", 9092}],
        emqx_bridge_kafka_impl:hosts([#{hostname => "127.0.0.1", port => 9092}])
    ).

sasl_none_test() ->
    ?assertEqual(undefined, emqx_bridge_kafka_impl:sasl(none)).

sasl_username_password_test() ->
    ?assertEqual(
        {plain, <<"user">>, <<"pass">>},
        emqx_bridge_kafka_impl:sasl(#{
            mechanism => plain,
            username => <<"user">>,
            password => <<"pass">>
        })
    ),
    %% Secrets may be wrapped in a closure by the schema.
    ?assertEqual(
        {scram_sha_256, <<"user">>, <<"pass">>},
        emqx_bridge_kafka_impl:sasl(#{
            mechanism => scram_sha_256,
            username => <<"user">>,
            password => fun() -> <<"pass">> end
        })
    ).

sasl_kerberos_test() ->
    ?assertEqual(
        {callback, brod_gssapi, {gssapi, <<"/etc/emqx/emqx.keytab">>, <<"emqx@EXAMPLE.COM">>}},
        emqx_bridge_kafka_impl:sasl(#{
            kerberos_principal => <<"emqx@EXAMPLE.COM">>,
            kerberos_keytab_file => <<"/etc/emqx/emqx.keytab">>
        })
    ).

socket_opts_test() ->
    Opts = emqx_bridge_kafka_impl:socket_opts(#{
        nodelay => true,
        sndbuf => 4096,
        recbuf => 8192,
        tcp_keepalive => <<"none">>
    }),
    ?assertEqual(true, proplists:get_value(nodelay, Opts)),
    ?assertEqual(4096, proplists:get_value(sndbuf, Opts)),
    ?assertEqual(8192, proplists:get_value(recbuf, Opts)),
    %% `buffer' must be the maximum between `sndbuf' and `recbuf'
    ?assertEqual(8192, proplists:get_value(buffer, Opts)),
    ?assertEqual([], proplists:get_all_values(tcp_keepalive, Opts)).

socket_opts_buffer_test() ->
    Opts = emqx_bridge_kafka_impl:socket_opts(#{sndbuf => 8192, recbuf => 2048, buffer => 16384}),
    ?assertEqual(16384, proplists:get_value(buffer, Opts)).

socket_opts_tcp_keepalive_test() ->
    %% The keepalive configuration is `Idle,Interval,Probes' in seconds. It is
    %% kept as a string by the schema, but be forgiving about binaries too.
    lists:foreach(
        fun(KeepAlive) ->
            Opts = emqx_bridge_kafka_impl:socket_opts(#{tcp_keepalive => KeepAlive}),
            ?assertEqual(true, proplists:get_value(keepalive, Opts)),
            %% `raw' options are 4-element tuples, not regular properties
            ?assert(lists:keymember(raw, 1, Opts))
        end,
        ["120,60,3", <<"120,60,3">>]
    ),
    %% `none' (the default) must not add any option
    lists:foreach(
        fun(KeepAlive) ->
            ?assertEqual([], emqx_bridge_kafka_impl:socket_opts(#{tcp_keepalive => KeepAlive}))
        end,
        [none, <<"none">>, "none"]
    ).

ssl_disabled_test() ->
    ?assertEqual(false, emqx_bridge_kafka_impl:ssl(#{enable => false})),
    ?assertEqual(false, emqx_bridge_kafka_impl:ssl(#{})).

ssl_enabled_test() ->
    Opts = emqx_bridge_kafka_impl:ssl(#{
        enable => true,
        verify => verify_peer
    }),
    ?assertEqual(verify_peer, proplists:get_value(verify, Opts)),
    ?assertNotEqual([], proplists:get_value(versions, Opts)).

%%------------------------------------------------------------------------------
%% Topic and message rendering
%%------------------------------------------------------------------------------

preproc_topic_test() ->
    ?assertEqual({fixed, <<"my-topic">>}, preproc_topic(<<"my-topic">>)),
    ?assertMatch({dynamic, _}, preproc_topic(<<"${clientid}/topic">>)).

render_topic_test() ->
    ?assertEqual(
        <<"my-topic">>,
        render_topic(preproc_topic(<<"my-topic">>), #{})
    ),
    %% Dynamic clientid
    ?assertEqual(
        <<"my-client/topic">>,
        render_topic(preproc_topic(<<"${clientid}/topic">>), #{clientid => <<"my-client">>})
    ),
    %% JSON-ish context, as provided by the rule engine
    ?assertEqual(
        <<"1/topic">>,
        render_topic(preproc_topic(<<"${a}/topic">>), #{a => 1})
    ),
    %% Rendering failures must not crash the action
    ?assertThrow(
        bad_topic,
        render_topic(preproc_topic(<<"${missing}/topic">>), #{})
    ).

render_message_default_test() ->
    Templates = compile_message_template(#{}),
    Message = #{
        clientid => <<"my-client">>,
        topic => <<"t/1">>,
        payload => <<"hello">>,
        timestamp => 1_234_567_890
    },
    Rendered = render_message(Templates, [], Message),
    ?assertEqual(<<"my-client">>, maps:get(key, Rendered)),
    %% The default value template is the whole message
    ?assertEqual(emqx_utils_json:encode(Message), maps:get(value, Rendered)),
    ?assertEqual(1_234_567_890, maps:get(ts, Rendered)),
    ?assertEqual([], maps:get(headers, Rendered)).

render_message_custom_test() ->
    Templates = compile_message_template(#{
        key => <<"${clientid}">>,
        value => <<"${payload}">>,
        timestamp => <<"${timestamp}">>
    }),
    Rendered = render_message(Templates, [], #{
        clientid => <<"my-client">>,
        payload => #{<<"a">> => 1},
        timestamp => 42
    }),
    %% Maps are JSON encoded
    ?assertEqual(<<"my-client">>, maps:get(key, Rendered)),
    ?assertEqual(<<"{\"a\":1}">>, maps:get(value, Rendered)),
    ?assertEqual(42, maps:get(ts, Rendered)).

render_message_missing_timestamp_test() ->
    Templates = compile_message_template(#{timestamp => <<"${timestamp}">>}),
    Rendered = render_message(Templates, [], #{}),
    %% Falls back to the current time
    ?assert(is_integer(maps:get(ts, Rendered))),
    ?assert(maps:get(ts, Rendered) > 1_600_000_000_000).

render_message_undefined_value_test() ->
    Templates = compile_message_template(#{value => <<"${payload}">>}),
    Rendered = render_message(Templates, [], #{}),
    ?assertEqual(<<"">>, maps:get(value, Rendered)).

%%------------------------------------------------------------------------------
%% Headers
%%------------------------------------------------------------------------------

render_headers_json_map_test() ->
    %% `${pub_props}' resolves to a map, which is converted to a list of pairs
    Headers = render_headers(
        emqx_placeholder:preproc_tmpl(<<"${pub_props}">>),
        [],
        none,
        #{pub_props => #{<<"a">> => <<"1">>}}
    ),
    ?assertEqual([{<<"a">>, <<"1">>}], Headers).

render_headers_json_binary_test() ->
    %% `${pub_props}' may also be a JSON encoded binary
    Headers = render_headers(
        emqx_placeholder:preproc_tmpl(<<"${pub_props}">>),
        [],
        none,
        #{pub_props => <<"{\"a\":\"1\"}">>}
    ),
    ?assertEqual([{<<"a">>, <<"1">>}], Headers),
    %% ... or a list of maps
    Headers1 = render_headers(
        emqx_placeholder:preproc_tmpl(<<"${pub_props}">>),
        [],
        none,
        #{pub_props => <<"[{\"a\":\"1\"},{\"b\":\"2\"}]">>}
    ),
    ?assertEqual([{<<"a">>, <<"1">>}, {<<"b">>, <<"2">>}], Headers1).

render_headers_undefined_test() ->
    %% When the placeholder is missing, only the extra headers remain
    Headers = render_headers(
        emqx_placeholder:preproc_tmpl(<<"${pub_props}">>),
        [],
        none,
        #{}
    ),
    ?assertEqual([], Headers).

render_headers_bad_test() ->
    ?assertThrow(
        {bad_kafka_headers, _},
        render_headers(
            emqx_placeholder:preproc_tmpl(<<"${pub_props}">>),
            [],
            none,
            #{pub_props => <<"not-a-json">>}
        )
    ).

render_headers_ext_headers_test() ->
    ExtHeaders = emqx_bridge_kafka_impl_producer:preproc_ext_headers([
        #{kafka_ext_header_key => <<"k1">>, kafka_ext_header_value => <<"v1">>},
        #{kafka_ext_header_key => <<"${clientid}">>, kafka_ext_header_value => <<"${username}">>},
        %% A missing placeholder is skipped
        #{kafka_ext_header_key => <<"${missing}">>, kafka_ext_header_value => <<"v">>}
    ]),
    Headers = render_headers(
        undefined,
        ExtHeaders,
        none,
        #{clientid => <<"c1">>, username => <<"u1">>}
    ),
    ?assertEqual([{<<"k1">>, <<"v1">>}, {<<"c1">>, <<"u1">>}], Headers).

render_headers_encode_mode_none_test() ->
    %% Non binary values are dropped in `none' mode
    Headers = render_headers(
        emqx_placeholder:preproc_tmpl(<<"${pub_props}">>),
        [],
        none,
        #{pub_props => #{<<"a">> => 1, <<"b">> => <<"2">>}}
    ),
    ?assertEqual([{<<"b">>, <<"2">>}], Headers).

render_headers_encode_mode_json_test() ->
    Headers = render_headers(
        emqx_placeholder:preproc_tmpl(<<"${pub_props}">>),
        [],
        json,
        #{pub_props => #{<<"a">> => 1, <<"b">> => <<"2">>}}
    ),
    ?assertEqual([{<<"a">>, <<"1">>}, {<<"b">>, <<"\"2\"">>}], Headers).

%%------------------------------------------------------------------------------
%% Producer configuration
%%------------------------------------------------------------------------------

producers_config_memory_test() ->
    Config = producers_config(#{buffer => #{mode => memory}}),
    ?assertEqual(false, maps:get(replayq_dir, Config)),
    ?assertEqual(false, maps:get(replayq_offload_mode, Config)),
    ?assertEqual(100, maps:get(replayq_max_total_bytes, Config)),
    ?assertEqual(10, maps:get(replayq_seg_bytes, Config)),
    ?assertEqual(10, maps:get(max_batch_bytes, Config)),
    ?assertEqual(0, maps:get(max_linger_ms, Config)),
    ?assertEqual(0, maps:get(max_linger_bytes, Config)),
    ?assertEqual(all_isr, maps:get(required_acks, Config)),
    ?assertEqual(no_compression, maps:get(compression, Config)),
    ?assertEqual(random, maps:get(partitioner, Config)),
    ?assertEqual(30, maps:get(partition_count_refresh_interval_seconds, Config)),
    ?assertEqual(all_partitions, maps:get(max_partitions, Config)),
    ?assertEqual(true, maps:get(drop_if_highmem, Config)),
    ?assertEqual(<<"action:kafka_producer:my-action">>, maps:get(group, Config)),
    ?assertEqual(
        #{bridge_id => <<"action:kafka_producer:my-action">>},
        maps:get(telemetry_meta_data, Config)
    ).

producers_config_max_send_ahead_test() ->
    %% wolff counts the batch being sent as "sent ahead" as well
    ?assertEqual(9, maps:get(max_send_ahead, producers_config(#{max_inflight => 10}))),
    ?assertEqual(0, maps:get(max_send_ahead, producers_config(#{max_inflight => 1}))),
    ?assertMatch(#{max_send_ahead := 0}, producers_config(#{max_inflight => 0})).

producers_config_disk_test() ->
    Config = producers_config(#{buffer => #{mode => disk}}),
    ?assertEqual(
        emqx_bridge_kafka_impl_producer:replayq_dir(<<"kafka_producer">>, <<"my-action">>),
        maps:get(replayq_dir, Config)
    ),
    ?assertEqual(false, maps:get(replayq_offload_mode, Config)).

producers_config_hybrid_test() ->
    Config = producers_config(#{buffer => #{mode => hybrid}}),
    ?assertEqual(true, maps:get(replayq_offload_mode, Config)).

producers_config_partitioner_test() ->
    ?assertEqual(
        first_key_dispatch,
        maps:get(partitioner, producers_config(#{partition_strategy => key_dispatch}))
    ).

producers_config_dry_run_test() ->
    %% A dry run never touches the file system
    Config = producers_config(#{buffer => #{mode => disk}}, true),
    ?assertEqual(false, maps:get(replayq_dir, Config)).

producers_config(Overrides) ->
    producers_config(Overrides, false).

producers_config(Overrides, IsDryRun) ->
    DefaultBuffer = #{
        mode => memory,
        per_partition_limit => 100,
        segment_bytes => 10,
        memory_overload_protection => true
    },
    Input0 = maps:merge(
        #{
            max_linger_time => 0,
            max_linger_bytes => 0,
            max_batch_bytes => 10,
            compression => no_compression,
            partition_strategy => random,
            required_acks => all_isr,
            partition_count_refresh_interval => 30,
            max_inflight => 10,
            partitions_limit => all_partitions,
            buffer => DefaultBuffer
        },
        maps:remove(buffer, Overrides)
    ),
    Input = Input0#{
        buffer => maps:merge(DefaultBuffer, maps:get(buffer, Overrides, #{}))
    },
    emqx_bridge_kafka_impl_producer:producers_config(
        <<"kafka_producer">>,
        <<"my-action">>,
        Input,
        IsDryRun,
        <<"action:kafka_producer:my-action">>
    ).

%%------------------------------------------------------------------------------
%% Reply handling
%%------------------------------------------------------------------------------

on_kafka_ack_offset_test() ->
    ?assertEqual(ok, apply_reply(0, 42)).

on_kafka_ack_drop_reasons_test() ->
    ?assertEqual({error, buffer_overflow}, apply_reply(0, buffer_overflow_discarded)),
    ?assertEqual({error, message_too_large}, apply_reply(0, message_too_large)),
    ?assertEqual({error, request_expired}, apply_reply(0, message_expired)),
    ?assertEqual({error, max_retry_exceeded}, apply_reply(0, max_retry_exceeded)),
    ?assertEqual({error, partition_lost}, apply_reply(0, partition_lost)),
    %% Unknown reasons are forwarded as is
    ?assertEqual({error, whatever}, apply_reply(0, whatever)).

apply_reply(Partition, Reason) ->
    Self = self(),
    ReplyFn = fun(Result) -> Self ! {reply, Result} end,
    %% The resource framework always passes a `{Fun, Args}' tuple
    ok = emqx_bridge_kafka_impl_producer:on_kafka_ack(Partition, Reason, {ReplyFn, []}),
    receive
        {reply, Result} -> Result
    after 1000 ->
        timeout
    end.
