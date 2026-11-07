# https://github.com/YiuTerran/otp-emqx-oss
export EMQX_BUILDER_VSN=27.3.4.19-emqx-oss.1-3-debian13
export OTP_VSN=27.3.4.19-emqx-oss.1
# Verified multi-architecture builder; no old OTP or Elixir is bundled.
export EMQX_BUILDER=docker.io/tryao/emqx-otp-oss:${EMQX_BUILDER_VSN}@sha256:5279e7803ce75a49681e5bbc46d6efee81492a369aa12e1b23557361853caf56
export EMQX_DOCKER_BUILD_FROM=${EMQX_BUILDER}
export EMQX_DOCKER_RUN_FROM=debian:13-slim
export BUILD_WITHOUT_QUIC=1
