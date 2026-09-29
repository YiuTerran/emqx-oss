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
-module(emqx_bridge_kafka_schema_tests).

-include_lib("eunit/include/eunit.hrl").

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

%% Parses the configuration the same way the runtime does: `atom_key => false'
%% keeps the binary keys, while `make_serializable => false' keeps the raw values
%% (durations in milliseconds/seconds, enums as atoms, ...).
parse_connector(Overrides) ->
    parse_connector_map(maps:merge(#{<<"bootstrap_hosts">> => <<"localhost:9092">>}, Overrides)).

parse_connector_map(InnerConfig) ->
    RawConf = #{
        <<"connectors">> => #{
            <<"kafka_producer">> => #{<<"test_connector">> => InnerConfig}
        }
    },
    #{
        <<"connectors">> := #{
            <<"kafka_producer">> := #{<<"test_connector">> := Conf}
        }
    } = hocon_tconf:check_plain(emqx_connector_schema, RawConf, parse_opts()),
    Conf.

parse_action(Overrides) ->
    parse_action(Overrides, #{}).

parse_action(Overrides, ActionOverrides) ->
    Parameters = maps:merge(#{<<"topic">> => <<"test-topic">>}, Overrides),
    ActionConfig = maps:merge(
        #{
            <<"connector">> => <<"test_connector">>,
            <<"parameters">> => Parameters
        },
        ActionOverrides
    ),
    RawConf = #{
        <<"actions">> => #{
            <<"kafka_producer">> => #{<<"test_action">> => ActionConfig}
        }
    },
    #{
        <<"actions">> := #{
            <<"kafka_producer">> := #{<<"test_action">> := Conf}
        }
    } = hocon_tconf:check_plain(emqx_bridge_v2_schema, RawConf, parse_opts()),
    Conf.

%% Examples are written with atom keys, which cannot be mixed with the
%% binary keys used by the tests above, so they are normalized first.
normalize(Map) ->
    emqx_utils_json:decode(emqx_utils_json:encode(Map)).

parse_opts() ->
    #{required => false, atom_key => false, make_serializable => false}.

expect_invalid(Fun) ->
    try
        Fun(),
        ?assert(false)
    catch
        _:_ ->
            ok
    end.

%%------------------------------------------------------------------------------
%% Connector schema
%%------------------------------------------------------------------------------

connector_defaults_test() ->
    Conf = parse_connector(#{}),
    ?assertMatch(
        #{
            <<"connect_timeout">> := 5_000,
            <<"min_metadata_refresh_interval">> := 3_000,
            <<"request_timeout">> := 30_000,
            <<"allow_auto_topic_creation">> := false,
            <<"authentication">> := none
        },
        Conf
    ),
    ?assertMatch(#{<<"socket_opts">> := #{<<"nodelay">> := true}}, Conf),
    ?assertMatch(#{<<"ssl">> := #{<<"enable">> := false}}, Conf).

connector_bootstrap_hosts_test() ->
    ?assertMatch(
        #{<<"bootstrap_hosts">> := _},
        parse_connector(#{<<"bootstrap_hosts">> => <<"host1:9092,host2:9092">>})
    ),
    expect_invalid(fun() -> parse_connector(#{<<"bootstrap_hosts">> => <<"not a host">>}) end).

connector_auth_plain_test() ->
    Conf = parse_connector(#{
        <<"authentication">> => #{
            <<"mechanism">> => <<"plain">>,
            <<"username">> => <<"user">>,
            <<"password">> => <<"pass">>
        }
    }),
    Auth = maps:get(<<"authentication">>, Conf),
    ?assertMatch(
        #{
            <<"mechanism">> := plain,
            <<"username">> := <<"user">>
        },
        Auth
    ),
    ?assertEqual(<<"pass">>, emqx_secret:unwrap(maps:get(<<"password">>, Auth))).

connector_auth_scram_test() ->
    Conf = parse_connector(#{
        <<"authentication">> => #{
            <<"mechanism">> => <<"scram_sha_512">>,
            <<"username">> => <<"user">>,
            <<"password">> => <<"pass">>
        }
    }),
    ?assertMatch(#{<<"authentication">> := #{<<"mechanism">> := scram_sha_512}}, Conf).

connector_auth_kerberos_test() ->
    Conf = parse_connector(#{
        <<"authentication">> => #{
            <<"kerberos_principal">> => <<"emqx@EXAMPLE.COM">>,
            <<"kerberos_keytab_file">> => <<"/etc/emqx/emqx.keytab">>
        }
    }),
    ?assertMatch(
        #{
            <<"authentication">> := #{
                <<"kerberos_principal">> := <<"emqx@EXAMPLE.COM">>,
                <<"kerberos_keytab_file">> := <<"/etc/emqx/emqx.keytab">>
            }
        },
        Conf
    ).

connector_auth_invalid_test() ->
    %% Unknown mechanism
    expect_invalid(fun() ->
        parse_connector(#{
            <<"authentication">> => #{
                <<"mechanism">> => <<"kerberos">>,
                <<"username">> => <<"user">>,
                <<"password">> => <<"pass">>
            }
        })
    end),
    %% Missing required fields
    expect_invalid(fun() ->
        parse_connector(#{<<"authentication">> => #{<<"mechanism">> => <<"plain">>}})
    end),
    %% Not a valid authentication configuration at all
    expect_invalid(fun() ->
        parse_connector(#{<<"authentication">> => <<"whatever">>})
    end).

connector_ssl_test() ->
    Conf = parse_connector(#{
        <<"ssl">> => #{
            <<"enable">> => true,
            <<"verify">> => <<"verify_peer">>,
            <<"cacertfile">> => <<"/etc/emqx/ca.pem">>
        }
    }),
    ?assertMatch(
        #{
            <<"ssl">> := #{
                <<"enable">> := true,
                <<"verify">> := verify_peer,
                <<"cacertfile">> := <<"/etc/emqx/ca.pem">>
            }
        },
        Conf
    ).

%%------------------------------------------------------------------------------
%% Action schema
%%------------------------------------------------------------------------------

action_defaults_test() ->
    #{<<"parameters">> := Parameters} = parse_action(#{}),
    ?assertMatch(
        #{
            <<"compression">> := no_compression,
            <<"required_acks">> := all_isr,
            <<"partition_strategy">> := random,
            <<"partitions_limit">> := all_partitions,
            %% in seconds
            <<"partition_count_refresh_interval">> := 60,
            <<"max_batch_bytes">> := 917_504,
            <<"max_inflight">> := 10,
            %% in milliseconds
            <<"max_linger_time">> := 0,
            <<"max_linger_bytes">> := 10_485_760,
            <<"kafka_headers">> := <<"${pub_props}">>,
            <<"kafka_header_value_encode_mode">> := none,
            <<"message">> := #{
                <<"key">> := <<"${.clientid}">>,
                <<"value">> := <<"${.}">>,
                <<"timestamp">> := <<"${.timestamp}">>
            },
            <<"buffer">> := #{
                <<"mode">> := memory,
                <<"per_partition_limit">> := 268_435_456,
                <<"segment_bytes">> := 10_485_760,
                <<"memory_overload_protection">> := true
            }
        },
        Parameters
    ).

action_resource_opts_test() ->
    ?assertMatch(
        #{<<"resource_opts">> := #{<<"query_mode">> := async, <<"request_ttl">> := 45_000}},
        parse_action(#{})
    ),
    ?assertMatch(
        #{<<"resource_opts">> := #{<<"query_mode">> := sync}},
        parse_action(#{}, #{<<"resource_opts">> => #{<<"query_mode">> => <<"sync">>}})
    ).

action_buffer_modes_test() ->
    lists:foreach(
        fun({ModeIn, ModeOut}) ->
            Conf = parse_action(#{
                <<"buffer">> => #{<<"mode">> => ModeIn, <<"per_partition_limit">> => <<"1GB">>}
            }),
            ?assertMatch(
                #{
                    <<"parameters">> := #{
                        <<"buffer">> := #{
                            <<"mode">> := ModeOut,
                            <<"per_partition_limit">> := 1_073_741_824
                        }
                    }
                },
                Conf
            )
        end,
        [{<<"memory">>, memory}, {<<"disk">>, disk}, {<<"hybrid">>, hybrid}]
    ).

action_dynamic_topic_disk_buffer_invalid_test() ->
    %% Disk buffering is not allowed with dynamic topics.
    expect_invalid(fun() ->
        parse_action(#{
            <<"topic">> => <<"${clientid}/topic">>,
            <<"buffer">> => #{<<"mode">> => <<"disk">>}
        })
    end),
    %% ... but it is fine with a fixed topic.
    ?assertMatch(
        #{<<"parameters">> := #{<<"buffer">> := #{<<"mode">> := disk}}},
        parse_action(#{<<"buffer">> => #{<<"mode">> => <<"disk">>}})
    ).

action_key_dispatch_test() ->
    ?assertMatch(
        #{<<"parameters">> := #{<<"partition_strategy">> := key_dispatch}},
        parse_action(#{
            <<"partition_strategy">> => <<"key_dispatch">>,
            <<"message">> => #{<<"key">> => <<"${clientid}">>}
        })
    ),
    %% An empty key cannot be used to dispatch
    expect_invalid(fun() ->
        parse_action(#{
            <<"partition_strategy">> => <<"key_dispatch">>,
            <<"message">> => #{<<"key">> => <<"">>}
        })
    end).

action_kafka_headers_test() ->
    ?assertMatch(
        #{<<"parameters">> := #{<<"kafka_headers">> := <<"${pub_props}">>}},
        parse_action(#{})
    ),
    %% Must be a single placeholder
    expect_invalid(fun() -> parse_action(#{<<"kafka_headers">> => <<"foo">>}) end),
    expect_invalid(fun() ->
        parse_action(#{<<"kafka_headers">> => <<"${clientid}-${username}">>})
    end).

action_ext_headers_test() ->
    Conf = parse_action(#{
        <<"kafka_ext_headers">> => [
            #{
                <<"kafka_ext_header_key">> => <<"static-key">>,
                <<"kafka_ext_header_value">> => <<"static-value">>
            },
            #{
                <<"kafka_ext_header_key">> => <<"${clientid}">>,
                <<"kafka_ext_header_value">> => <<"${username}">>
            }
        ]
    }),
    ?assertMatch(#{<<"parameters">> := #{<<"kafka_ext_headers">> := [_, _]}}, Conf),
    %% Only a single placeholder or a plain string is allowed as value
    expect_invalid(fun() ->
        parse_action(#{
            <<"kafka_ext_headers">> => [
                #{
                    <<"kafka_ext_header_key">> => <<"k">>,
                    <<"kafka_ext_header_value">> => <<"${a}-${b}">>
                }
            ]
        })
    end).

action_topic_required_test() ->
    expect_invalid(fun() ->
        hocon_tconf:check_plain(
            emqx_bridge_v2_schema,
            #{
                <<"actions">> => #{
                    <<"kafka_producer">> => #{
                        <<"test_action">> => #{
                            <<"connector">> => <<"test_connector">>,
                            <<"parameters">> => #{}
                        }
                    }
                }
            },
            parse_opts()
        )
    end).

%%------------------------------------------------------------------------------
%% API examples
%%------------------------------------------------------------------------------

connector_examples_test() ->
    lists:foreach(
        fun(Method) ->
            [
                #{
                    <<"kafka_producer">> := #{
                        value := Values
                    }
                }
            ] = emqx_bridge_kafka:connector_examples(Method),
            %% `name' and `type' are part of the API body, not of the connector
            %% configuration itself.
            InnerConfig = normalize(maps:without([name, type], Values)),
            ?assertMatch(#{<<"bootstrap_hosts">> := _}, parse_connector_map(InnerConfig))
        end,
        [post, put]
    ).

action_examples_test() ->
    lists:foreach(
        fun(Method) ->
            [
                #{
                    <<"kafka_producer">> := #{
                        value := Values
                    }
                }
            ] = emqx_bridge_kafka:bridge_v2_examples(Method),
            ActionConfig = normalize(maps:without([name, type, status, node_status], Values)),
            #{
                <<"actions">> := #{
                    <<"kafka_producer">> := #{<<"my_kafka_producer_action">> := Conf}
                }
            } = hocon_tconf:check_plain(
                emqx_bridge_v2_schema,
                #{
                    <<"actions">> => #{
                        <<"kafka_producer">> => #{
                            <<"my_kafka_producer_action">> => ActionConfig
                        }
                    }
                },
                parse_opts()
            ),
            ?assertMatch(#{<<"parameters">> := #{<<"topic">> := <<"my-topic">>}}, Conf)
        end,
        [post, put]
    ).
