# https://github.com/YiuTerran/otp-emqx-oss
export EMQX_BUILDER_VSN=27.3.4.19-emqx-oss.1-2-debian13
export OTP_VSN=27.3.4.19-emqx-oss.1
# Verified multi-architecture builder; no old OTP or Elixir is bundled.
export EMQX_BUILDER=docker.io/tryao/emqx-otp-oss:${EMQX_BUILDER_VSN}@sha256:b6e56a84e4b3c55ddbc726a11c77f2696dd8b098dba7db9c8a5f3dfdd1d7cf39
export EMQX_DOCKER_BUILD_FROM=${EMQX_BUILDER}
export EMQX_DOCKER_RUN_FROM=debian:13-slim
export BUILD_WITHOUT_QUIC=1
