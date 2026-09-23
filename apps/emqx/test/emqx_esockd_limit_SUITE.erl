%%--------------------------------------------------------------------
%% Copyright (c) 2026 Contributors to this fork.
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

-module(emqx_esockd_limit_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> [t_ulimit_uses_largest_pollset].

t_ulimit_uses_largest_pollset(_) ->
    CheckIo = erlang:system_info(check_io),
    ?assert(length(CheckIo) > 1),
    Expected = lists:foldl(
        fun(CheckIoResult, Acc) ->
            case lists:keyfind(max_fds, 1, CheckIoResult) of
                {max_fds, N} when is_integer(N), N > 0 -> max(Acc, N);
                _ -> Acc
            end
        end,
        1023,
        CheckIo
    ),
    ?assert(Expected > 1024),
    ?assertEqual(Expected, esockd:ulimit()).
