# EMQX 5.8.9 社区版 Fork

本仓库基于 EMQX `v5.8.9` 社区版 fork，保留 5.8.9 版本基线，并针对社区版修复问题、清理不适用的功能和构建入口。本仓库的修改不会出现在上游 EMQX 发布包中。

## 本仓库的修改

- **会话注册表：**清理接管、踢除后已确认失效的记录，并在 core 节点周期扫描死亡的本地 PID 和已退出集群节点的记录；保留仍属集群但暂时停机的节点记录及原有历史记录语义。
- **连接上限：**将 esockd 固定到其 Apache 2.0 仓库的[修复提交 `a638fcd78a`](https://github.com/emqx/esockd/commit/a638fcd78a5fd3898b43d50eb55d734f78d884f5)，修复多 pollset 环境下连接上限误判。
- **社区版清理：**移除含 BSL 许可的应用目录、企业版构建入口和发布任务，构建与 CI 使用社区版配置。LDAP 认证随相关目录移除。
- **QUIC 清理：**移除 quicer 依赖、MQTT over QUIC 监听器入口和相关构建配置。本 fork 不提供 MQTT over QUIC。

## 从源码构建

本地开发使用官方 Erlang/OTP `26.2.5.21` 和 Elixir `1.15.7-otp-26`，版本声明在 `.tool-versions` 中。在本仓库根目录执行：

```bash
mise install
make emqx-rel
_build/emqx/rel/emqx/bin/emqx console
```

Docker/CI 构建镜像继续使用 EMQX 定制的 OTP `26.2.5.14-1`。该标签属于 `emqx/otp`，不在 mise 默认下载的 `erlang/otp` 仓库中，因此本地安装使用官方 OTP 26 补丁版本。项目构建检查支持 OTP 25/26，不要直接切换到更高主版本。

在 Debian/Ubuntu/Deepin 上，mise 从源码编译 OTP，需要 C/C++ 编译工具、Autoconf、ncurses 和 OpenSSL 开发库，可先安装：

```bash
sudo apt-get install build-essential autoconf m4 libncurses-dev libssl-dev
```

在 macOS 上，旧版捆绑 Snappy 的测试代码可能无法通过新版 Apple Clang 编译。可安装 Homebrew 的 Snappy/LZ4，并让 RocksDB 使用这两个库：

```bash
brew install snappy lz4
ERLANG_ROCKSDB_OPTS="-DWITH_SNAPPY=ON -DWITH_LZ4=ON -DCMAKE_PREFIX_PATH=$(brew --prefix)" \
  BUILD_WITHOUT_JQ=1 make emqx-rel
```

上例使用 `.tool-versions` 声明的 OTP 版本，请先执行 `mise install`。`BUILD_WITHOUT_JQ=1` 仅跳过 JQ 原生依赖，**不会跳过 RocksDB**。按上例生成的 macOS RocksDB NIF 动态链接 Homebrew 的 Snappy/LZ4，复制构建结果到其他机器时需确保目标机器也有这些库。

## 项目文档

- [贡献指南](./CONTRIBUTING.md)
- [插件说明](./PLUGIN.md)
- [安全漏洞私密报告](./SECURITY.md)
- [源码许可说明](./LICENSE)

本仓库的源码许可以 [LICENSE](./LICENSE) 及各文件声明为准。Apache 2.0 许可文本见 [APL.txt](./APL.txt)。
