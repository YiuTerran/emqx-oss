# Security maintenance baseline

This independently maintained EMQX 5.8 OSS fork remains Apache-2.0. No code
from Business Source License directories is required for these backports.
Upstream names identify provenance, not endorsement or trademark permission.

## Runtime

`scripts/build-security-otp.sh` builds `27.3.4.19-emqx-oss.1` from
`YiuTerran/otp-emqx-oss`, commit
`4559432c17edb53302903b3603132513b81f58f6`. Its source archive SHA-256 is
`8f76576e30b1a55508e6c0c91e765aa8067101b184aed28c1f9cde6504a43ae9`.
The fork merges official OTP 27.3.4.19 onto EMQX OTP 27.3.4.2-9 and preserves
Mnesia hooks, MQTT packet mode and EMQX TLS extensions.

This includes the fixes for CVE-2026-55953, CVE-2026-89422,
CVE-2026-58227, CVE-2026-42789 and CVE-2026-75538. The source, its
modification record and Apache-2.0 license are public in that fork.

Docker source builds and Community CI use the independently built
`tryao/emqx-otp-oss:27.3.4.19-emqx-oss.1-1-debian13` environment, pinned to
multi-architecture digest
`sha256:f80234a9682efa39091c2b8a868a8acdc54989e162be8193f61c3db07eb1448c`.
It is built from Debian 13 and contains no older bootstrap OTP or Elixir.
Its native amd64/arm64 CI validates OTP/ERTS, the Mnesia hook, SSL and compiler
before publishing. Both EMQX build paths verify the installed runtime before
compilation rather than rebuilding OTP on every run. Releases embed this
runtime; the source-build script remains available for independent host builds.

The rebar plugin dependency tree is resolved separately from application deps.
`emqx_plugrel` therefore pins HOCON 0.44.0 there too, avoiding the old parser's
OTP 27 reserved-word errors. PropEr's own coverage plugin is disabled in both
of rebar's plugin keys. Swallowed plugin failures can leave parallel compiler
workers behind with stale options; Community CI rejects such failures rather
than treating a later unrelated dependency error as the root cause.

For a host build, use a dedicated writable prefix, then explicitly activate it:

```sh
OTP_INSTALL_PREFIX="$PWD/.security-otp" bash scripts/build-security-otp.sh
export PATH="$PWD/.security-otp/bin:$PATH"
erl +S 2:2 -noshell -eval 'mnesia_hook = mnesia_hook:module_info(module), halt().'
make emqx-compile test-compile
```

`.tool-versions` intentionally omits an Erlang installer entry: stock OTP from
mise does not provide the custom Mnesia hooks. The retained Elixir entry is
the legacy developer tool, not the compiler used for the rebar release pipeline.
Mix/Elixir runtime compatibility is not covered by this baseline's validation.
Do not run an automatic tool installer over this explicitly selected runtime. Use a fresh
build directory when switching OTP majors; generated parsers and BEAM caches
are not portable between the two compilers. Validate upgrades with new cluster
volumes, and retain backups of existing data before production migration.

## Dependency backports

- cowlib 2.8: `YiuTerran/cowlib-emqx-oss` at
  `21a12526490e00f38efcd7b8bb8576dda18e6562`. Backports upstream
  `a4b8039ce8c93ab00867ef6b7e888822c09f4369` for CVE-2026-7790,
  also rejecting excessive leading zero digits. Original ISC license retained.
- certifi 2.10: `YiuTerran/certifi-emqx-oss` at
  `4d9269f3260b80ac205b1722bd809fd9ea12f45e`. Removes the two exact
  withdrawn roots identified by GHSA-qxwr-jcff-c3wm from the compile-time input.
  Original BSD license retained. Updating system certificates alone is insufficient.
- gen_rpc 3.5.1: `YiuTerran/gen-rpc-emqx-oss` at
  `2fdf02844b87d656b2d7c707f73eb51b8384a9aa`. Limits incoming RPC
  frames on TCP/TLS listen, connect and accepted sockets. Apache-2.0 retained.
  `rpc.socket_packet_size` defaults to 536870912 bytes (512 MiB), allowing a
  maximum MQTT publish plus framing. Accepted range: 1 byte to 1 GiB.
  All nodes should use the same value. Larger transfers must be split or reduced.
  This does not replace the OTP inet driver fix for Erlang distribution sockets.

Both rebar and Mix configurations pin these commits. These are backports,
so a scanner using only original application version numbers can still flag
them; use the source commit and executable regressions as evidence.

## Plugin installation

Apache-2.0 source from EMQX PR 17200, `release-58`, head
`6749d323c57ff11543b55e41d1fd74f9f773547b`, supplies the five-minute
grant TTL, optional SHA-256 binding, BPAPI v4, and tar-path validation for
CVE-2026-44725. File-specific copyright and license headers are retained.
This fork additionally rejects writes into sibling plugin directories and
does not delete pre-existing files when archive pre-validation fails.

Mixed-version clusters do not provide the new security guarantees on old
nodes. SHA-256 grants refuse a cluster without BPAPI v4 everywhere.
An unpinned grant remains available for compatibility; use pinned grants
for production installations, and finish upgrading all nodes first.

## Verification and distribution

Community CI runs plugin TTL/hash/path regressions. Docker release CI runs
`scripts/test/check-cluster.py --restart-node --security-runtime` before
publishing the final multi-platform manifest. The runtime check verifies the
actual OTP/ERTS, compiled CA list, parser rejection, RPC socket options and
plugin API on every node, not just dependency version labels.

Dashboard Axios and vue-i18n pins and token-origin restrictions are maintained
in the separate Dashboard repository. Publish its security release asset
before tagging the backend release; keep `EMQX_DASHBOARD_VERSION` synchronized.

Redistributors must retain this project's and each dependency's licenses,
copyright notices and applicable attribution. The OTP builder installs its
license and modification record; release packages must carry applicable
notices too. Commercial use is permitted under those license conditions;
this is not a license grant for unrelated upstream enterprise code.

License scope: the existing separate Dashboard source snapshot has no
repository-level LICENSE file, and the upstream emqx-dashboard5 repository
reports no detected license in GitHub metadata. The OTP/backport license
verification does not resolve that pre-existing Dashboard licensing gap.
Do not infer a blanket license for the entire bundled UI or all npm assets
from the backend's Apache-2.0 declaration; obtain authoritative upstream
terms before making a commercial redistribution assurance for that bundle.

Durable-session QoS 2 crash-window triage remains separate: the reported
CVE-2026-8741 explicitly identifies newer EMQX releases, and applicability to
this fork has not been demonstrated. It is not claimed fixed here.

## Local verification (2026-10-24)

The locally assembled candidate was tested before publishing the release tag:

- The pinned OTP compiled successfully; its official TLS rogue-server cases
  (4), public_key intermediate-path validation case (1), and
  CVE-2026-75538 inet regression passed. The inet case ran with a 512 MiB
  container memory limit.
- The three dependency forks passed their parser, compiled trust-store and
  real TCP frame-limit regressions. EMQX production and test compilation,
  plugin EUnit (20 cases) and focused plugin Common Test (5 cases) passed.
  Session-registry (7), file-limit (1), TCP listener (2) and machine boot
  (2) regressions also passed. File-limit tests explicitly require an
  8192-file limit rather than Docker's possible 1024 default.
- An isolated three-node core/RLOG cluster passed actual runtime security
  assertions on every node, every directed MQTT node pair at QoS 0/1/2,
  retained messages, delivery while a node was offline, and restart/rejoin.
- MQTT -> rule -> Kafka delivery passed with an independent Kafka CLI
  consumer; action metrics reported matched=1, success=1, failed=0.
- Dashboard v1.10.9 passed authorization, secret-copy and i18n regressions,
  a production build, and real-browser overview/connector-form checks.
  Composition mode fixes both the blank login page and empty setup-time
  connector labels introduced by the vue-i18n security upgrade.

These local checks do not substitute for testing the final CI-built Docker
Hub image. Release CI separately gates manifest publication on its staged
amd64 and arm64 images' three-node tests on native runners. A complete
image SBOM/vulnerability scan and
the Hackney CVE-2025-3864 / JOSE CVE-2023-50966 call-path triage remain
outside these verified fixes. Do not describe this release as vulnerability-free.

The digest-pinned standalone OTP builder also passed a clean Community CI run
after the plugin-tree fixes: [40112520864](https://github.com/YiuTerran/emqx-oss/actions/runs/40112520864).
It completed production/test compilation, the regressions listed above, release
assembly and 1025 simultaneous MQTT client connections without plugin-loading
errors. This is distinct from the multi-architecture Docker release gate.

## Published build.6 verification (2026-10-25)

Source tag `v5.8.9-build.6` resolves to
`c1c7b8bc37641ccddd56480e460dd741f0c5a6ba`. The release includes Dashboard
`v1.10.9` and the custom OTP runtime documented above. The corrected OTP
Docker Hub repository is `tryao/emqx-otp-oss`; the broker repository remains
`tryao/emqx-oss`.

All jobs in the [Docker release run
40113226839](https://github.com/YiuTerran/emqx-oss/actions/runs/40113226839)
succeeded, including native amd64 and arm64 three-node RLOG/security/MQTT
and restart gates before manifest publication. The [tag's Community CI
40113226794](https://github.com/YiuTerran/emqx-oss/actions/runs/40113226794)
also succeeded after retrying a transient Hex dependency-fetch failure.

Published image: `tryao/emqx-oss:5.8.9-build.6`. Immutable digests:

| Manifest/platform | SHA-256 |
| --- | --- |
| Multi-architecture index | `40ffeefbf244ab965285d4799b1c713590e744315f9a57a151ff5f67ebea6ec8` |
| linux/amd64 | `8bb4bb28b4d01dff407b947649afa1020964025428664193a03f1a88f62c6f67` |
| linux/arm64 | `a9c6168d96ceb0a6a9350f7d819e24b6fe02b53577de531c9601e08775b484e1` |

The Docker Hub image was pulled onto the local amd64 host and used to recreate
only the isolated `emqx-security-it` cluster, retaining its existing data
volumes. The older `emqx-cluster-it` build.3 cluster and its volumes were
preserved. The final image passed:

- All three nodes' RLOG/core membership and security-runtime assertions,
  Dashboard v1.10.9 resources, login and nodes APIs.
- All directed node pairs at MQTT QoS 0/1/2, retained messages, delivery with
  emqx2 stopped, and repeat checks after restart/rejoin.
- MQTT -> rule -> Kafka delivery with independent Kafka CLI consumption;
  action metrics were matched=1, success=1, failed=0.
- TLS 1.2 and TLS 1.3 cross-node MQTT QoS 0/1/2 with CA and hostname
  verification. The bundled legacy test certificate lacks AKI, so Python's
  additional `VERIFY_X509_STRICT` flag was disabled for this fixture only.

This suffixed version does not move `latest`. The SBOM/image scan,
Hackney/JOSE and durable-session scope limitations and Dashboard licensing
boundary above still apply.
