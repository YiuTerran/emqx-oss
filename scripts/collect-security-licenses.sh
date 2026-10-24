#!/usr/bin/env bash
set -euo pipefail
DEST="$1/licenses"
mkdir -p "$DEST/emqx" "$DEST/otp"
cp LICENSE APL.txt NOTICE SECURITY-BACKPORTS.md "$DEST/emqx/"
OTP_PREFIX="${OTP_INSTALL_PREFIX:-/opt/emqx-otp}"
cp "$OTP_PREFIX/share/licenses/erlang/"* "$DEST/otp/"
for dep in _build/default/lib/*; do
    [ -d "$dep" ] || continue
    while IFS= read -r -d '' notice; do
        mkdir -p "$DEST/$(basename "$dep")"
        cp "$notice" "$DEST/$(basename "$dep")/"
    done < <(find "$dep" -maxdepth 1 -type f \( \
        -iname '*license*' -o -iname '*copying*' -o -iname '*notice*' \
        -o -iname '*copyright*' -o -name SECURITY-BACKPORT.md \) -print0)
done
