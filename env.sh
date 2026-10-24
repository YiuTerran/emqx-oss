# https://github.com/YiuTerran/otp-emqx-oss
export EMQX_BUILDER_VSN=27.3.4.19-emqx-oss.1-1-debian13
export OTP_VSN=27.3.4.19-emqx-oss.1
# Verified multi-architecture builder; no old OTP or Elixir is bundled.
export EMQX_BUILDER=docker.io/tryao/emqx-otp-oss:${EMQX_BUILDER_VSN}@sha256:f80234a9682efa39091c2b8a868a8acdc54989e162be8193f61c3db07eb1448c
export EMQX_DOCKER_BUILD_FROM=${EMQX_BUILDER}
export EMQX_DOCKER_RUN_FROM=debian:13-slim
export BUILD_WITHOUT_QUIC=1
