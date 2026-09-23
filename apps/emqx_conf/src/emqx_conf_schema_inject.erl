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

-module(emqx_conf_schema_inject).

-export([schemas/0]).

schemas() ->
    authn() ++
        authz() ++
        bridges().

authn() ->
    [{emqx_authn_schema, authn_mods()}].

authn_mods() ->
    [
        emqx_authn_mnesia_schema,
        emqx_authn_mysql_schema,
        emqx_authn_postgresql_schema,
        emqx_authn_mongodb_schema,
        emqx_authn_redis_schema,
        emqx_authn_http_schema,
        emqx_authn_jwt_schema,
        emqx_authn_scram_mnesia_schema
    ].

authz() ->
    [{emqx_authz_schema, authz_mods()}].

authz_mods() ->
    [
        emqx_authz_file_schema,
        emqx_authz_mnesia_schema,
        emqx_authz_http_schema,
        emqx_authz_redis_schema,
        emqx_authz_mysql_schema,
        emqx_authz_postgresql_schema,
        emqx_authz_mongodb_schema
    ].

bridges() ->
    [emqx_bridge_mqtt_connector_schema].
