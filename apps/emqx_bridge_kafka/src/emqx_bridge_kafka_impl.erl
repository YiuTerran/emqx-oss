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

%% @doc Helpers to translate the connector configuration into `wolff'/`kafka_protocol'
%% configuration terms.
-module(emqx_bridge_kafka_impl).

-include_lib("emqx/include/logger.hrl").

-export([
    hosts/1,
    sasl/1,
    socket_opts/1,
    ssl/1
]).

%% Parse a comma separated `host:port' list into a `[{Host, Port}]' list.
hosts(Hosts) when is_binary(Hosts) ->
    kpro:parse_endpoints(binary_to_list(Hosts));
hosts([#{hostname := _, port := _} | _] = Servers) ->
    %% Already parsed by a schema validator.
    [{Hostname, Port} || #{hostname := Hostname, port := Port} <- Servers];
hosts(Hosts) when is_list(Hosts) ->
    kpro:parse_endpoints(lists:flatten(lists:join(",", [to_list(H) || H <- Hosts]))).

%% SASL authentication configuration.
sasl(none) ->
    undefined;
sasl(#{mechanism := Mechanism, username := Username, password := Password}) ->
    {Mechanism, Username, emqx_secret:unwrap(Password)};
sasl(#{kerberos_principal := Principal, kerberos_keytab_file := KeytabFile}) ->
    {callback, brod_gssapi, {gssapi, KeytabFile, Principal}}.

%% Extra socket options for the client connections.
socket_opts(Opts) when is_map(Opts) ->
    socket_opts(maps:to_list(Opts));
socket_opts(Opts) when is_list(Opts) ->
    socket_opts_loop(Opts, []).

socket_opts_loop([], Acc) ->
    lists:reverse(Acc);
socket_opts_loop([{tcp_keepalive, KeepAlive} | Rest], Acc) ->
    socket_opts_loop(Rest, tcp_keepalive(KeepAlive) ++ Acc);
socket_opts_loop([{T, Bytes} | Rest], Acc) when
    T =:= sndbuf orelse T =:= recbuf orelse T =:= buffer
->
    %% For TCP it is recommended to have `buffer' >= `recbuf' to avoid
    %% unnecessary copying, see https://www.erlang.org/doc/man/inet.html
    Acc1 = [{T, Bytes} | adjust_buffer(Bytes, Acc)],
    socket_opts_loop(Rest, Acc1);
socket_opts_loop([Other | Rest], Acc) ->
    socket_opts_loop(Rest, [Other | Acc]).

adjust_buffer(Bytes, Opts) ->
    case lists:keytake(buffer, 1, Opts) of
        false ->
            [{buffer, Bytes} | Opts];
        {value, {buffer, Bytes1}, Acc} ->
            [{buffer, max(Bytes1, Bytes)} | Acc]
    end.

tcp_keepalive(undefined) ->
    [];
tcp_keepalive(none) ->
    [];
tcp_keepalive(<<"none">>) ->
    [];
tcp_keepalive("none") ->
    [];
tcp_keepalive(KeepAlive) when is_binary(KeepAlive) ->
    tcp_keepalive(binary_to_list(KeepAlive));
tcp_keepalive(KeepAlive) ->
    {Idle, Interval, Probes} = emqx_schema:parse_tcp_keepalive(KeepAlive),
    case emqx_utils:tcp_keepalive_opts(os:type(), Idle, Interval, Probes) of
        {ok, Opts} ->
            Opts;
        {error, {unsupported_os, OS}} ->
            ?SLOG(warning, #{
                msg => "tcp_keepalive_not_supported",
                os => OS
            }),
            []
    end.

%% TLS client options.
ssl(#{enable := true} = SSL) ->
    emqx_tls_lib:to_client_opts(SSL);
ssl(_) ->
    false.

to_list(X) when is_binary(X) ->
    binary_to_list(X);
to_list(X) when is_list(X) ->
    X.
