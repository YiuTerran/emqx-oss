#!/usr/bin/env bash

set -euo pipefail

[ "${DEBUG:-0}" -eq 1 ] && set -x

# PROFILE_STR may include the -pkg suffix.
PROFILE_STR="${1}"

case "$PROFILE_STR" in
    emqx|emqx-pkg)
        dashboard_version="$EMQX_DASHBOARD_VERSION"
        ;;
    *) echo "Unsupported profile: $PROFILE_STR" >&2; exit 1 ;;
esac

# ensure dir
cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

./scripts/get-dashboard.sh "$dashboard_version"

# generate merged config files and English translation of the desc (desc.en.hocon)
./scripts/merge-config.escript

I18N_REPO_BRANCH="v$(./pkg-vsn.sh "${PROFILE_STR}" | tr -d '.' | cut -c 1-2)"

DOWNLOAD_I18N_TRANSLATIONS=${DOWNLOAD_I18N_TRANSLATIONS:-true}
# download desc (i18n) translations
beginfmt='\033[1m'
endfmt='\033[0m'
if [ "$DOWNLOAD_I18N_TRANSLATIONS" = "true" ]; then
  echo "Downloading i18n translation from emqx/emqx-i18n..."
  start=$(date +%s%N)
  curl -L --fail --silent --show-error \
       --output "apps/emqx_dashboard/priv/desc.zh.hocon" \
       "https://raw.githubusercontent.com/emqx/emqx-i18n/${I18N_REPO_BRANCH}/desc.zh.hocon"
  end=$(date +%s%N)
  duration=$(echo "$end $start" | awk '{printf "%.f\n", (($1 - $2)/ 1000000)}')
  if [ "$duration" -gt 1000 ]; then beginfmt='\033[1;33m'; fi
  echo -e "Downloaded i18n translation in $duration milliseconds.\nSet ${beginfmt}DOWNLOAD_I18N_TRANSLATIONS=false${endfmt} to skip"
else
  echo -e "Skipping to download i18n translation from emqx/emqx-i18n.\nSet ${beginfmt}DOWNLOAD_I18N_TRANSLATIONS=true${endfmt} to update"
fi
