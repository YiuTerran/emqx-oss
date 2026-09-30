%%--------------------------------------------------------------------
%% Copyright (c) 2019-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-ifndef(EMQX_EXT_TRACE_HRL).
-define(EMQX_EXT_TRACE_HRL, true).

%% --------------------------------------------------------------------
%% Macros

-define(EXT_TRACE_START, '$ext_trace_start').
-define(EXT_TRACE_STOP, '$ext_trace_stop').

-define(EMQX_EXTERNAL_MODULE, emqx_external_trace).
-define(PROVIDER, {?EMQX_EXTERNAL_MODULE, trace_provider}).

-define(EXT_TRACE_ADD_ATTRS(_Attrs), ok).
-define(EXT_TRACE_ADD_ATTRS(_Attrs, _Ctx), ok).
-define(EXT_TRACE_SET_STATUS_OK(), ok).
-define(EXT_TRACE_SET_STATUS_ERROR(), ok).
-define(EXT_TRACE_SET_STATUS_ERROR(_), ok).

-define(EXT_TRACE_ATTR(_Expr_),
    ok
).

-define(EXT_TRACE_CLIENT_CONNECT(_Attrs, Fun, FunArgs),
    erlang:apply(Fun, FunArgs)
).

-define(EXT_TRACE_CLIENT_DISCONNECT(_Attrs, Fun, FunArgs),
    erlang:apply(Fun, FunArgs)
).

-define(EXT_TRACE_CLIENT_SUBSCRIBE(_Attrs, Fun, FunArgs),
    erlang:apply(Fun, FunArgs)
).

-define(EXT_TRACE_CLIENT_UNSUBSCRIBE(_Attrs, Fun, FunArgs),
    erlang:apply(Fun, FunArgs)
).

-define(EXT_TRACE_CLIENT_AUTHN(_Attrs, Fun, FunArgs),
    erlang:apply(Fun, FunArgs)
).

-define(EXT_TRACE_CLIENT_AUTHN_BACKEND(_Attrs, Fun, FunArgs),
    erlang:apply(Fun, FunArgs)
).

-define(EXT_TRACE_CLIENT_AUTHZ(_Attrs, Fun, FunArgs),
    erlang:apply(Fun, FunArgs)
).

-define(EXT_TRACE_CLIENT_AUTHZ_BACKEND(_Attrs, Fun, FunArgs),
    erlang:apply(Fun, FunArgs)
).

-define(EXT_TRACE_BROKER_DISCONNECT(_Attrs, Fun, FunArgs),
    erlang:apply(Fun, FunArgs)
).

-define(EXT_TRACE_BROKER_SUBSCRIBE(_Attrs, Fun, FunArgs),
    erlang:apply(Fun, FunArgs)
).

-define(EXT_TRACE_BROKER_UNSUBSCRIBE(_Attrs, Fun, FunArgs),
    erlang:apply(Fun, FunArgs)
).

-define(EXT_TRACE_CLIENT_PUBLISH(_Attrs, Fun, FunArgs),
    erlang:apply(Fun, FunArgs)
).

-define(EXT_TRACE_CLIENT_PUBACK(_Attrs, Fun, FunArgs),
    erlang:apply(Fun, FunArgs)
).

-define(EXT_TRACE_CLIENT_PUBREC(_Attrs, Fun, FunArgs),
    erlang:apply(Fun, FunArgs)
).

-define(EXT_TRACE_CLIENT_PUBREL(_Attrs, Fun, FunArgs),
    erlang:apply(Fun, FunArgs)
).

-define(EXT_TRACE_CLIENT_PUBCOMP(_Attrs, Fun, FunArgs),
    erlang:apply(Fun, FunArgs)
).

-define(EXT_TRACE_MSG_ROUTE(_Attrs, Fun, FunArgs),
    erlang:apply(Fun, FunArgs)
).

-define(EXT_TRACE_MSG_FORWARD(_Attrs, Fun, FunArgs),
    erlang:apply(Fun, FunArgs)
).

-define(EXT_TRACE_MSG_HANDLE_FORWARD(_Attrs, Fun, FunArgs),
    erlang:apply(Fun, FunArgs)
).

-define(EXT_TRACE_BROKER_PUBLISH(_Attrs, Delivers),
    Delivers
).

-define(EXT_TRACE_OUTGOING_START(_Attrs, Packet),
    Packet
).

-define(EXT_TRACE_OUTGOING_STOP(_Attrs, Packets),
    ok
).

%% EMQX_EXT_TRACE_HRL check end
-endif.
