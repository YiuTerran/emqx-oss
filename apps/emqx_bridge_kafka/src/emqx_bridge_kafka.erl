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

-module(emqx_bridge_kafka).

-behaviour(emqx_connector_examples).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-import(hoconsc, [mk/2, enum/1, ref/1, ref/2]).

-export([roots/0, fields/1, namespace/0, desc/1]).

-export([
    bridge_v2_examples/1,
    connector_examples/1
]).

%%======================================================================================
%% Hocon Schema Definitions
%%======================================================================================

namespace() -> "bridge_kafka".

roots() -> [].

%%--------------------------------------------------------------------
%% v1: bridges API and config file
%%
%% Kafka producer is a v2 only action, so its v1 view is the automatic
%% downgrade of the action and connector configs, with the `parameters'
%% of both unindented (see
%% `emqx_action_info:connector_action_config_to_bridge_v1_config/2').
%% See also `emqx_bridge_schema:get_response/0', `put_request/0',
%% `post_request/0' and `emqx_bridge_schema:fields(bridges)'.
%%--------------------------------------------------------------------
fields("post") ->
    [bridge_v1_type_field(), name_field() | fields("config")];
fields("put") ->
    fields("config");
fields("get") ->
    emqx_bridge_schema:status_fields() ++ fields("post");
fields("config") ->
    v1_config_fields();
%%--------------------------------------------------------------------
%% v2: configuration
%%--------------------------------------------------------------------
fields(action) ->
    {kafka_producer,
        mk(hoconsc:map(name, ref(?MODULE, kafka_producer_action)), #{
            desc => ?DESC(kafka_producer),
            required => false
        })};
fields(kafka_producer_action) ->
    Fields = emqx_bridge_v2_schema:make_producer_action_schema(
        ref(?MODULE, action_parameters),
        #{resource_opts_ref => ref(?MODULE, action_resource_opts)}
    ),
    lists:map(
        fun
            ({parameters, _}) ->
                {parameters,
                    mk(ref(?MODULE, action_parameters), #{
                        validator => fun producer_parameters_validator/1
                    })};
            (Field) ->
                Field
        end,
        Fields
    );
fields(action_resource_opts) ->
    emqx_bridge_v2_schema:action_resource_opts_fields();
fields(action_parameters) ->
    [
        {topic,
            mk(emqx_schema:template(), #{
                required => true,
                desc => ?DESC(kafka_topic)
            })},
        {message,
            mk(ref(?MODULE, kafka_message), #{
                required => false,
                desc => ?DESC(kafka_message)
            })},
        {kafka_headers,
            mk(emqx_schema:template(), #{
                required => false,
                default => <<"${pub_props}">>,
                validator => fun kafka_header_validator/1,
                desc => ?DESC(kafka_headers)
            })},
        {kafka_ext_headers,
            mk(hoconsc:array(ref(?MODULE, producer_kafka_ext_headers)), #{
                required => false,
                desc => ?DESC(producer_kafka_ext_headers)
            })},
        {kafka_header_value_encode_mode,
            mk(enum([none, json]), #{
                required => false,
                default => none,
                desc => ?DESC(kafka_header_value_encode_mode)
            })},
        {compression,
            mk(enum([no_compression, snappy, gzip]), #{
                required => false,
                default => no_compression,
                desc => ?DESC(compression)
            })},
        {required_acks,
            mk(enum([all_isr, leader_only, none]), #{
                required => false,
                default => all_isr,
                desc => ?DESC(required_acks)
            })},
        {partition_strategy,
            mk(enum([random, key_dispatch]), #{
                required => false,
                default => random,
                desc => ?DESC(partition_strategy)
            })},
        {partitions_limit,
            mk(hoconsc:union([all_partitions, pos_integer()]), #{
                required => false,
                default => all_partitions,
                desc => ?DESC(partitions_limit)
            })},
        {partition_count_refresh_interval,
            mk(emqx_schema:timeout_duration_s(), #{
                required => false,
                default => <<"60s">>,
                desc => ?DESC(partition_count_refresh_interval)
            })},
        {max_batch_bytes,
            mk(emqx_schema:bytesize(), #{
                required => false,
                default => <<"896KB">>,
                desc => ?DESC(max_batch_bytes)
            })},
        {max_inflight,
            mk(pos_integer(), #{
                required => false,
                default => 10,
                desc => ?DESC(max_inflight)
            })},
        {max_linger_time,
            mk(emqx_schema:duration_ms(), #{
                required => false,
                default => <<"0ms">>,
                desc => ?DESC(max_linger_time)
            })},
        {max_linger_bytes,
            mk(emqx_schema:bytesize(), #{
                required => false,
                default => <<"10MB">>,
                desc => ?DESC(max_linger_bytes)
            })},
        {buffer,
            mk(ref(?MODULE, producer_buffer), #{
                required => false,
                desc => ?DESC(producer_buffer)
            })}
    ];
fields(producer_buffer) ->
    [
        {mode,
            mk(enum([memory, disk, hybrid]), #{
                required => false,
                default => memory,
                desc => ?DESC(buffer_mode)
            })},
        {per_partition_limit,
            mk(emqx_schema:bytesize(), #{
                required => false,
                default => <<"256MB">>,
                desc => ?DESC(buffer_per_partition_limit)
            })},
        {segment_bytes,
            mk(emqx_schema:bytesize(), #{
                required => false,
                default => <<"10MB">>,
                desc => ?DESC(buffer_segment_bytes)
            })},
        {memory_overload_protection,
            mk(boolean(), #{
                required => false,
                default => true,
                desc => ?DESC(buffer_memory_overload_protection)
            })}
    ];
fields(kafka_message) ->
    [
        {key,
            mk(emqx_schema:template(), #{
                required => false,
                default => <<"${.clientid}">>,
                desc => ?DESC(kafka_message_key)
            })},
        {value,
            mk(emqx_schema:template(), #{
                required => false,
                default => <<"${.}">>,
                desc => ?DESC(kafka_message_value)
            })},
        {timestamp,
            mk(emqx_schema:template(), #{
                required => false,
                default => <<"${.timestamp}">>,
                desc => ?DESC(kafka_message_timestamp)
            })}
    ];
fields(producer_kafka_ext_headers) ->
    [
        {kafka_ext_header_key,
            mk(emqx_schema:template(), #{
                required => true,
                desc => ?DESC(producer_kafka_ext_header_key)
            })},
        {kafka_ext_header_value,
            mk(emqx_schema:template(), #{
                required => true,
                validator => fun kafka_ext_header_value_validator/1,
                desc => ?DESC(producer_kafka_ext_header_value)
            })}
    ];
%%--------------------------------------------------------------------
%% v2: API schema
%% The parameter equals to
%%   `get_bridge_v2`, `post_bridge_v2`, `put_bridge_v2` from emqx_bridge_v2_schema:api_schema/1
%%   `get_connector`, `post_connector`, `put_connector` from emqx_connector_schema:api_schema/1
%%--------------------------------------------------------------------
fields("post_" ++ Type) ->
    [type_field(), name_field() | fields("config_" ++ Type)];
fields("put_" ++ Type) ->
    fields("config_" ++ Type);
fields("get_" ++ Type) ->
    emqx_bridge_schema:status_fields() ++ fields("post_" ++ Type);
fields("config_bridge_v2") ->
    fields(kafka_producer_action);
fields("config_connector") ->
    emqx_connector_schema:common_fields() ++
        connector_fields() ++
        emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts);
fields(connector_resource_opts) ->
    emqx_connector_schema:resource_opts_fields();
fields(v1_resource_opts) ->
    dedup_fields(
        emqx_bridge_v2_schema:action_resource_opts_fields() ++
            emqx_connector_schema:resource_opts_fields()
    );
%%--------------------------------------------------------------------
%% connector configuration
%%--------------------------------------------------------------------
fields(socket_opts) ->
    [
        {nodelay,
            mk(boolean(), #{
                required => false,
                default => true,
                desc => ?DESC(socket_nodelay)
            })},
        {sndbuf,
            mk(emqx_schema:bytesize(), #{
                required => false,
                default => <<"1024KB">>,
                desc => ?DESC(socket_send_buffer)
            })},
        {recbuf,
            mk(emqx_schema:bytesize(), #{
                required => false,
                default => <<"1024KB">>,
                desc => ?DESC(socket_receive_buffer)
            })},
        {tcp_keepalive,
            mk(string(), #{
                required => false,
                default => <<"none">>,
                desc => ?DESC(socket_tcp_keepalive),
                validator => fun emqx_schema:validate_tcp_keepalive/1
            })}
    ];
%% Union members of the `authentication' field.
fields(auth_username_password) ->
    [
        {mechanism,
            mk(enum([plain, scram_sha_256, scram_sha_512]), #{
                required => true,
                desc => ?DESC(auth_sasl_mechanism)
            })},
        {username,
            mk(binary(), #{
                required => true,
                desc => ?DESC(auth_sasl_username)
            })},
        {password,
            emqx_schema_secret:mk(#{
                required => true,
                desc => ?DESC(auth_sasl_password)
            })}
    ];
fields(auth_gssapi_kerberos) ->
    [
        {kerberos_principal,
            mk(binary(), #{
                required => true,
                desc => ?DESC(auth_kerberos_principal)
            })},
        {kerberos_keytab_file,
            mk(binary(), #{
                required => true,
                desc => ?DESC(auth_kerberos_keytab_file)
            })}
    ].

connector_fields() ->
    [
        {bootstrap_hosts,
            mk(emqx_schema:comma_separated_list(), #{
                required => true,
                desc => ?DESC(bootstrap_hosts)
            })},
        {connect_timeout,
            mk(emqx_schema:duration_ms(), #{
                required => false,
                default => <<"5s">>,
                desc => ?DESC(connect_timeout)
            })},
        {min_metadata_refresh_interval,
            mk(emqx_schema:duration_ms(), #{
                required => false,
                default => <<"3s">>,
                desc => ?DESC(min_metadata_refresh_interval)
            })},
        {request_timeout,
            mk(emqx_schema:duration_ms(), #{
                required => false,
                default => <<"30s">>,
                desc => ?DESC(request_timeout)
            })},
        {socket_opts,
            mk(ref(?MODULE, socket_opts), #{
                required => false,
                desc => ?DESC(socket_opts)
            })},
        {ssl,
            mk(hoconsc:ref(emqx_schema, "ssl_client_opts"), #{
                required => false,
                default => #{<<"enable">> => false},
                desc => ?DESC(ssl_client_opts)
            })},
        {authentication,
            mk(hoconsc:union(fun auth_union_member_selector/1), #{
                required => false,
                default => none,
                desc => ?DESC(authentication)
            })},
        {allow_auto_topic_creation,
            mk(boolean(), #{
                required => false,
                default => false,
                desc => ?DESC(allow_auto_topic_creation)
            })},
        {health_check_topic,
            mk(binary(), #{
                required => false,
                desc => ?DESC(producer_health_check_topic)
            })}
    ].

auth_union_members() ->
    [
        none,
        ref(?MODULE, auth_username_password),
        ref(?MODULE, auth_gssapi_kerberos)
    ].

auth_union_member_selector(all_union_members) ->
    auth_union_members();
auth_union_member_selector({value, Value}) when is_atom(Value) ->
    auth_union_member_selector({value, atom_to_binary(Value)});
auth_union_member_selector({value, <<"none">>}) ->
    [none];
auth_union_member_selector({value, Value}) when is_map(Value) ->
    case Value of
        #{<<"mechanism">> := Mechanism} ->
            auth_mechanism_union_member(Mechanism);
        #{<<"kerberos_principal">> := _} ->
            [ref(?MODULE, auth_gssapi_kerberos)];
        #{<<"kerberos_keytab_file">> := _} ->
            [ref(?MODULE, auth_gssapi_kerberos)];
        _ ->
            throw_invalid_authentication(Value)
    end;
auth_union_member_selector({value, Value}) ->
    throw_invalid_authentication(Value).

auth_mechanism_union_member(Mechanism) when is_atom(Mechanism) ->
    auth_mechanism_union_member(atom_to_binary(Mechanism));
auth_mechanism_union_member(Mechanism) when
    Mechanism =:= <<"plain">>;
    Mechanism =:= <<"scram_sha_256">>;
    Mechanism =:= <<"scram_sha_512">>
->
    [ref(?MODULE, auth_username_password)];
auth_mechanism_union_member(Mechanism) ->
    throw_invalid_authentication(Mechanism).

throw_invalid_authentication(Value) ->
    throw(#{
        field_name => authentication,
        value => Value,
        reason => <<"Invalid authentication configuration">>
    }).

kafka_header_validator(undefined) ->
    ok;
kafka_header_validator(Value) ->
    case emqx_placeholder:preproc_tmpl(Value) of
        [{var, _}] ->
            ok;
        _ ->
            {error, "The 'kafka_headers' must be a single placeholder like ${pub_props}"}
    end.

kafka_ext_header_value_validator(undefined) ->
    ok;
kafka_ext_header_value_validator(Value) ->
    case emqx_placeholder:preproc_tmpl(Value) of
        [{Type, _}] when Type =:= var orelse Type =:= str ->
            ok;
        _ ->
            {
                error,
                "The value of 'kafka_ext_headers' must either be a single "
                "placeholder or a static string"
            }
    end.

producer_parameters_validator(Conf) ->
    case producer_strategy_key_validator(Conf) of
        ok ->
            producer_buffer_mode_validator(Conf);
        Error ->
            Error
    end.

producer_strategy_key_validator(
    #{
        partition_strategy := _,
        message := #{key := _}
    } = Conf
) ->
    producer_strategy_key_validator(emqx_utils_maps:binary_key_map(Conf));
producer_strategy_key_validator(#{
    <<"partition_strategy">> := key_dispatch,
    <<"message">> := #{<<"key">> := Key}
}) when Key =:= "" orelse Key =:= <<>> ->
    {error, "Message key cannot be empty when `key_dispatch` strategy is used"};
producer_strategy_key_validator(_) ->
    ok.

producer_buffer_mode_validator(#{buffer := _} = Conf) ->
    producer_buffer_mode_validator(emqx_utils_maps:binary_key_map(Conf));
producer_buffer_mode_validator(#{<<"buffer">> := #{<<"mode">> := disk}, <<"topic">> := Topic}) ->
    case emqx_template:placeholders(emqx_template:parse(Topic)) of
        [] ->
            ok;
        [_ | _] ->
            {error, <<"Disk-mode buffering is not allowed when using dynamic topics">>}
    end;
producer_buffer_mode_validator(_) ->
    ok.

desc("config") ->
    ?DESC(desc_config);
desc("config_connector") ->
    ?DESC(desc_config);
desc("config_bridge_v2") ->
    ?DESC(kafka_producer_action);
desc(kafka_producer_action) ->
    ?DESC(kafka_producer_action);
desc(action_parameters) ->
    ?DESC(producer_kafka_opts);
desc(producer_buffer) ->
    ?DESC(producer_buffer);
desc(kafka_message) ->
    ?DESC(kafka_message);
desc(producer_kafka_ext_headers) ->
    ?DESC(producer_kafka_ext_headers);
desc(socket_opts) ->
    ?DESC(socket_opts);
desc(auth_username_password) ->
    ?DESC(auth_username_password);
desc(auth_gssapi_kerberos) ->
    ?DESC(auth_gssapi_kerberos);
desc(connector_resource_opts) ->
    ?DESC(emqx_resource_schema, resource_opts);
desc(action_resource_opts) ->
    ?DESC(emqx_resource_schema, resource_opts);
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for Kafka Producer using `", string:to_upper(Method), "` method."];
desc(_) ->
    undefined.

%%--------------------------------------------------------------------
%% common funcs
%%--------------------------------------------------------------------

%% The v1 view of a Kafka producer holds the union of the fields of its
%% action and connector configs, with `parameters' unindented and the
%% `resource_opts' of both deep merged.
v1_config_fields() ->
    ActionFields = [
        Field
     || {Key, _} = Field <- fields(kafka_producer_action),
        Key =/= connector,
        Key =/= parameters,
        Key =/= resource_opts
    ],
    dedup_fields(
        emqx_bridge_schema:common_bridge_fields() ++
            emqx_connector_schema:common_fields() ++
            connector_fields() ++
            ActionFields ++
            fields(action_parameters) ++
            [
                {resource_opts,
                    mk(ref(?MODULE, v1_resource_opts), #{
                        required => false,
                        default => #{},
                        desc => ?DESC(emqx_resource_schema, resource_opts)
                    })}
            ]
    ).

dedup_fields(Fields) ->
    lists:reverse(
        lists:foldl(
            fun(Field, Acc) ->
                case lists:keymember(element(1, Field), 1, Acc) of
                    true -> Acc;
                    false -> [Field | Acc]
                end
            end,
            [],
            Fields
        )
    ).

%% The v1 alias of the `kafka_producer' action type.
bridge_v1_type_field() ->
    {type,
        mk(
            kafka,
            #{
                required => true,
                desc => ?DESC(desc_type)
            }
        )}.

type_field() ->
    {type,
        mk(
            kafka_producer,
            #{
                required => true,
                desc => ?DESC(desc_type)
            }
        )}.

name_field() ->
    {name,
        mk(
            binary(),
            #{
                required => true,
                desc => ?DESC(desc_name)
            }
        )}.

%%--------------------------------------------------------------------
%% Examples
%%--------------------------------------------------------------------

bridge_v2_examples(Method) ->
    [
        #{
            <<"kafka_producer">> => #{
                summary => <<"Kafka Producer Action">>,
                value => action_values(Method)
            }
        }
    ].

connector_examples(Method) ->
    [
        #{
            <<"kafka_producer">> => #{
                summary => <<"Kafka Producer Connector">>,
                value => connector_values(Method)
            }
        }
    ].

action_values(get) ->
    maps:merge(
        #{
            status => <<"connected">>,
            node_status => [
                #{
                    node => <<"emqx@127.0.0.1">>,
                    status => <<"connected">>
                }
            ]
        },
        action_values(post)
    );
action_values(post) ->
    maps:merge(
        #{
            name => <<"my_kafka_producer_action">>,
            type => <<"kafka_producer">>
        },
        action_values(put)
    );
action_values(put) ->
    #{
        connector => <<"my_kafka_producer">>,
        parameters => #{
            topic => <<"my-topic">>,
            message => #{
                key => <<"${.clientid}">>,
                value => <<"${.}">>,
                timestamp => <<"${.timestamp}">>
            },
            kafka_headers => <<"${pub_props}">>,
            compression => <<"no_compression">>,
            required_acks => <<"all_isr">>,
            partition_strategy => <<"random">>,
            partitions_limit => <<"all_partitions">>,
            partition_count_refresh_interval => <<"60s">>,
            max_batch_bytes => <<"896KB">>,
            max_inflight => 10,
            max_linger_time => <<"0ms">>,
            max_linger_bytes => <<"10MB">>,
            buffer => #{
                mode => <<"memory">>,
                per_partition_limit => <<"256MB">>,
                segment_bytes => <<"10MB">>,
                memory_overload_protection => true
            }
        },
        resource_opts => #{
            health_check_interval => <<"15s">>,
            query_mode => <<"async">>
        }
    }.

connector_values(get) ->
    maps:merge(
        #{
            status => <<"connected">>,
            node_status => [
                #{
                    node => <<"emqx@127.0.0.1">>,
                    status => <<"connected">>
                }
            ]
        },
        connector_values(post)
    );
connector_values(post) ->
    maps:merge(
        #{
            name => <<"my_kafka_producer">>,
            type => <<"kafka_producer">>
        },
        connector_values(put)
    );
connector_values(put) ->
    #{
        bootstrap_hosts => <<"127.0.0.1:9092">>,
        connect_timeout => <<"5s">>,
        min_metadata_refresh_interval => <<"3s">>,
        request_timeout => <<"30s">>,
        socket_opts => #{
            nodelay => true,
            sndbuf => <<"1024KB">>,
            recbuf => <<"1024KB">>,
            tcp_keepalive => <<"none">>
        },
        ssl => #{enable => false},
        authentication => <<"none">>,
        allow_auto_topic_creation => false
    }.
