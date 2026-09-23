简体中文 | [English](./README.md) | [Русский](./README-RU.md)

# EMQX

本 fork 基于 Apache 2.0 许可的 EMQX 5.8.9 社区版，加入会话注册表残留清理和
Apache 2.0 许可的 esockd 连接上限修复。仓库已移除含 BSL 许可的应用目录和企业版构建入口；
LDAP 认证和 MQTT over QUIC 在本 fork 中不可用。请使用本仓库源码及 `emqx` profile 构建；上游 `latest`
镜像不包含本 fork 的修复。

[![GitHub Release](https://img.shields.io/github/release/emqx/emqx?color=brightgreen&label=Release)](https://github.com/emqx/emqx/releases)
[![Docker Pulls](https://img.shields.io/docker/pulls/emqx/emqx?label=Docker%20Pulls)](https://hub.docker.com/r/emqx/emqx)
[![OpenSSF Scorecard](https://img.shields.io/ossf-scorecard/github.com/emqx/emqx?label=OpenSSF%20Scorecard&style=flat)](https://securityscorecards.dev/viewer/?uri=github.com/emqx/emqx)
[![Slack](https://img.shields.io/badge/Slack-EMQ-39AE85?logo=slack)](https://slack-invite.emqx.io/)
[![Discord](https://img.shields.io/discord/931086341838622751?label=Discord&logo=discord)](https://discord.gg/xYGf3fQnES)
[![X](https://img.shields.io/badge/Follow-EMQ-1DA1F2?logo=x)](https://x.com/EMQTech)
[![Community](https://img.shields.io/badge/Community-EMQX-yellow)](https://askemq.com)
[![YouTube](https://img.shields.io/badge/Subscribe-EMQ%20中文-FF0000?logo=youtube)](https://www.youtube.com/channel/UCir_r04HIsLjf2qqyZ4A8Cg)


EMQX 是一款全球下载量超千万的大规模分布式物联网 MQTT 服务器，单集群支持 1 亿物联网设备连接，消息分发时延低于 1 毫秒。为高可靠、高性能的物联网实时数据移动、处理和集成提供动力，助力企业构建关键业务的 IoT 平台与应用。

EMQX 自 2013 年在 GitHub 发布开源版本以来，获得了来自 50 多个国家和地区的 20000 余家企业用户的广泛认可，累计连接物联网关键设备超过 1 亿台。

更多信息请访问 [EMQX 官网](https://www.emqx.com/zh)。

## 快速开始

#### EMQX Cloud

使用 EMQX 最简单的方式是在 EMQX Cloud 上创建完全托管的 MQTT 服务。[免费试用 EMQX Cloud](https://www.emqx.com/zh/signup?utm_source=github.com&utm_medium=referral&utm_campaign=emqx-readme-to-cloud&continue=https://cloud.emqx.com/console/deployments/0?oper=new)，无需绑定信用卡。

#### 使用 Docker 运行 EMQX

```
docker run -d --name emqx -p 1883:1883 -p 8083:8083 -p 8084:8084 -p 8883:8883 -p 18083:18083 emqx/emqx:latest
```

接下来请参考 [入门指南](https://docs.emqx.com/zh/emqx/latest/deploy/install-docker-ce.html) 开启您的 EMQX 之旅。

#### 在 Kubernetes 上运行 EMQX 集群

请参考 [EMQX Operator 文档](https://docs.emqx.com/zh/emqx-operator/latest/getting-started/getting-started.html)。

#### 更多安装方式

您可以从 [emqx.com/zh/downloads-and-install/broker](https://www.emqx.com/zh/downloads-and-install/broker) 下载不同格式的 EMQX 安装包进行手动安装。

也可以直接访问 [EMQX 安装文档](https://docs.emqx.com/zh/emqx/latest/deploy/install-open-source.html) 查看不同安装方式的操作步骤。

## 文档

EMQX 开源版文档：[docs.emqx.com/zh/emqx/latest](https://docs.emqx.com/zh/emqx/latest/)。

EMQX Cloud 文档：[docs.emqx.com/zh/cloud/latest](https://docs.emqx.com/zh/cloud/latest/)。

## 贡献

请参考我们的 [贡献者指南](./CONTRIBUTING.md)。

如果对 EMQX 有改进建议，可以向 [EIP](https://github.com/emqx/eip) 提交 PR 和 ISSUE。

## 社区

- 访问 [EMQ 问答社区](https://askemq.com/) 以获取帮助，也可以分享您的想法或项目。
- 添加小助手微信号 `emqmkt`，加入 EMQ 微信技术交流群。
- 加入我们的 [Discord](https://discord.gg/xYGf3fQnES)，参于实时讨论。
- 关注我们的 [Bilibili](https://space.bilibili.com/522222081)，获取最新物联网技术分享。
- 关注我们的 [微博](https://weibo.com/emqtt) 或 [Twitter](https://twitter.com/EMQTech)，获取 EMQ 最新资讯。

## 相关资源

- [MQTT 入门及进阶](https://www.emqx.com/zh/mqtt)

  EMQ 提供了通俗易懂的技术文章及简单易用的客户端工具，帮助您学习 MQTT 并快速入门 MQTT 客户端编程。

- [MQTT SDKs](https://www.emqx.com/zh/mqtt-client-sdk)

  我们选取了各个编程语言中热门的 MQTT 客户端 SDK，并提供代码示例，帮助您快速掌握 MQTT 客户端库的使用。

- [MQTTX](https://mqttx.app/zh)

  优雅的跨平台 MQTT 5.0 客户端工具，提供了桌面端、命令行、Web 三种版本，帮助您更快的开发和调试 MQTT 服务和应用。

- [车联网平台搭建从入门到精通](https://www.emqx.com/zh/blog/category/internet-of-vehicles)

  结合 EMQ 在车联网领域的实践经验，从协议选择等理论知识，到平台架构设计等实战操作，分享如何搭建一个可靠、高效、符合行业场景需求的车联网平台。

## 从源码构建

本 fork 固定在 EMQX 5.8.9，建议使用 OTP 26。在本仓库根目录构建：

```bash
make emqx-rel
_build/emqx/rel/emqx/bin/emqx console
```

在 macOS 上使用 OTP 26 编译 RocksDB 时，先安装 `snappy` 和 `lz4`，再让
`erlang-rocksdb` 使用 Homebrew 提供的库，避免其捆绑的旧版 Snappy 测试代码
在新版本 Apple Clang 下编译失败：

```bash
brew install snappy lz4
ERLANG_ROCKSDB_OPTS="-DWITH_SNAPPY=ON -DWITH_LZ4=ON -DCMAKE_PREFIX_PATH=$(brew --prefix)" \
  BUILD_WITHOUT_JQ=1 mise exec erlang@26.2.5.21 -- make emqx-rel
```

本仓库 `.tool-versions` 固定的 OTP 补丁版本为 `26.2.5.14-1`；上面的命令使用本机已安装的
`26.2.5.21`。`BUILD_WITHOUT_JQ=1` 跳过本机尚未能编译通过的 JQ 原生依赖，RocksDB 仍会编译。
生成的 RocksDB NIF 动态链接 Homebrew 的 Snappy/LZ4；如果将 macOS 构建结果复制到其他机器，
目标机器也需要这两个库。

## 源码许可

详见 [LICENSE](./LICENSE)。
