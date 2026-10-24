#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
while read -r dep expected; do
    actual="$(git -C "_build/default/lib/$dep" rev-parse HEAD)"
    if [ "$actual" != "$expected" ]; then
        echo "Stale dependency $dep: expected $expected, found $actual" >&2
        echo "Use a fresh build directory before rebuilding the security release." >&2
        exit 1
    fi
done <<'PINS'
cowlib 21a12526490e00f38efcd7b8bb8576dda18e6562
certifi 4d9269f3260b80ac205b1722bd809fd9ea12f45e
gen_rpc 2fdf02844b87d656b2d7c707f73eb51b8384a9aa
PINS
erl -pa _build/default/lib/*/ebin -noshell -eval '
    140 = length(certifi:cacerts()),
    536870912 = gen_rpc_helper:get_packet_size(),
    lists:foreach(fun(Header) ->
        {'"'"'EXIT'"'"', {chunk_size_too_long, _}} =
            (catch cow_http_te:stream_chunked(Header, {0, 0}))
    end, [<<"10000000000000000\r\n">>, <<"00000000000000000\r\n">>]),
    io:format("PASS security dependency commits and compiled modules~n"), halt().
'
