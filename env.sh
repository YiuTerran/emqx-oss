# https://github.com/emqx/emqx-builder
export EMQX_BUILDER_VSN=5.6-2
export OTP_VSN=27.3.4.19-emqx-oss.1
export ELIXIR_VSN=1.15.7
# Bootstrap compiler only: CI/Docker installs the pinned security OTP before building.
export EMQX_BUILDER=ghcr.io/emqx/emqx-builder/${EMQX_BUILDER_VSN}:${ELIXIR_VSN}-26.2.5.14-1-ubuntu22.04
export EMQX_DOCKER_BUILD_FROM=ghcr.io/emqx/emqx-builder/${EMQX_BUILDER_VSN}:${ELIXIR_VSN}-26.2.5.14-1-debian13
export EMQX_DOCKER_RUN_FROM=debian:13-slim
export BUILD_WITHOUT_QUIC=1
