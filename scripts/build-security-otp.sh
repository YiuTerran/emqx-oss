#!/usr/bin/env bash
set -euo pipefail

VERSION=27.3.4.19-emqx-oss.1
COMMIT=4559432c17edb53302903b3603132513b81f58f6
SHA256=8f76576e30b1a55508e6c0c91e765aa8067101b184aed28c1f9cde6504a43ae9
PREFIX="${OTP_INSTALL_PREFIX:-/opt/emqx-otp}"
if [ -x "$PREFIX/bin/erl" ] &&
    [ "$(cat "$PREFIX/lib/erlang/releases/27/OTP_VERSION")" = "$VERSION" ]; then
    exit 0
fi

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
curl --fail --location --retry 3 \
    "https://codeload.github.com/YiuTerran/otp-emqx-oss/tar.gz/$COMMIT" \
    -o "$WORK/otp.tar.gz"
echo "$SHA256  $WORK/otp.tar.gz" | sha256sum --check --status
tar -xzf "$WORK/otp.tar.gz" --strip-components=1 -C "$WORK"
cd "$WORK"
test "$(cat OTP_VERSION)" = "$VERSION"
./otp_build autoconf
./configure --prefix="$PREFIX" --without-javac --without-wx \
    --without-debugger --without-observer --without-et
make -j"${OTP_BUILD_JOBS:-4}"
make install
install -d "$PREFIX/share/licenses/erlang"
cp LICENSE.txt "$PREFIX/share/licenses/erlang/"
cp system/COPYRIGHT "$PREFIX/share/licenses/erlang/"
cp README.emqx-oss.md "$PREFIX/share/licenses/erlang/"
"$PREFIX/bin/erl" +S 2:2 -noshell -eval '
    mnesia_hook = mnesia_hook:module_info(module),
    "15.2.7.14" = erlang:system_info(version),
    io:format("Custom security OTP and mnesia_hook verified~n"), halt().
'
