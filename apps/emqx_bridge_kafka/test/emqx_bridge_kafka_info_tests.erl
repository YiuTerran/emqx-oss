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
-module(emqx_bridge_kafka_info_tests).

-include_lib("eunit/include/eunit.hrl").

%% Verifies that the Kafka producer connector and action are registered, both via
%% the application environment and via the hard coded module lists.
connector_registered_test() ->
    ?assert(lists:member(kafka_producer, emqx_connector_info:connector_types())),
    ?assertEqual(
        emqx_bridge_kafka,
        emqx_connector_info:schema_module(kafka_producer)
    ).

action_registered_test() ->
    ?assert(emqx_action_info:is_action_type(kafka_producer)),
    ?assertEqual(
        kafka_producer,
        emqx_action_info:action_type_to_connector_type(kafka_producer)
    ),
    ?assert(
        lists:member(
            emqx_bridge_kafka,
            [Module || {_Type, Module} <- emqx_action_info:registered_schema_modules_actions()]
        )
    ).

%% Rules may refer to the action as `kafka:<name>', the v1 bridge type name.
v1_alias_test() ->
    ?assertEqual(kafka, emqx_action_info:action_type_to_bridge_v1_type(kafka_producer, undefined)),
    ?assertEqual(kafka_producer, emqx_action_info:bridge_v1_type_to_action_type(kafka)).
