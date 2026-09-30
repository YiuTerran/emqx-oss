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

%% Schema module of the `kafka_producer' connector and action API, i.e. the
%% kafka schemas of `/api/v5/connectors' and `/api/v5/actions'.
%%
%% The dashboard keeps no hard-coded list of connector/action types: it
%% enumerates the components of `/schemas/connectors' and `/schemas/actions',
%% takes the type of a component from its namespace, and renders the form of a
%% type `T' from `bridge_T.post_connector' (`bridge_T.post_bridge_v2' for
%% actions).  `emqx_bridge_kafka' also serves the legacy (v1) `kafka' bridge and
%% is therefore namespaced `bridge_kafka', hence this separate module for the v2
%% `kafka_producer' schemas.  All the actual fields are defined there.
-module(emqx_bridge_kafka_producer_schema).

-behaviour(emqx_connector_examples).

-export([namespace/0, roots/0, fields/1, desc/1]).

-export([
    bridge_v2_examples/1,
    connector_examples/1
]).

namespace() -> "bridge_kafka_producer".

roots() -> [].

fields(Field) ->
    emqx_bridge_kafka:fields(Field).

desc(Field) ->
    emqx_bridge_kafka:desc(Field).

connector_examples(Method) ->
    emqx_bridge_kafka:connector_examples(Method).

bridge_v2_examples(Method) ->
    emqx_bridge_kafka:bridge_v2_examples(Method).
