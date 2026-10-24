# EMQX 5.8.9 社区版 Fork

本仓库基于 EMQX `v5.8.9` 社区版 fork，保留 5.8.9 版本基线，并针对社区版修复问题、清理不适用的功能和构建入口。本仓库的修改不会出现在上游 EMQX 发布包中。

## 本仓库的修改

- **会话注册表：**清理接管、踢除后已确认失效的记录，并在 core 节点周期扫描死亡的本地 PID 和已退出集群节点的记录；保留仍属集群但暂时停机的节点记录及原有历史记录语义。
- **连接上限：**将 esockd 固定到其 Apache 2.0 仓库的[修复提交 `a638fcd78a`](https://github.com/emqx/esockd/commit/a638fcd78a5fd3898b43d50eb55d734f78d884f5)，修复多 pollset 环境下连接上限误判。
- **社区版清理：**移除含 BSL 许可的应用目录、企业版构建入口和发布任务，构建与 CI 使用社区版配置。LDAP 认证随相关目录移除。
- **QUIC 清理：**移除 quicer 依赖、MQTT over QUIC 监听器入口和相关构建配置。本 fork 不提供 MQTT over QUIC。
- **Kafka：**提供 producer 连接器与动作，修复 bootstrap hosts 校验、headers 转换、producer 清理和指标统计，并包含自定义 Dashboard 的 Kafka 表单及连接器复制修复。
- **安全维护：**使用保留 Mnesia hook 的 Apache-2.0 OTP `27.3.4.19-emqx-oss.1`，回移 cowlib、certifi 和插件安装修复，限制 RPC 包长，并限制 Dashboard Token 的发送范围。具体提交、许可证及验证方式见 [SECURITY-BACKPORTS.md](SECURITY-BACKPORTS.md)。

## 从源码构建

发布构建使用本 fork 固定提交的定制 OTP `27.3.4.19-emqx-oss.1`，先校验源码 SHA-256 再编译。宿主机安装请使用独立目录，不覆盖系统 Erlang：

```bash
OTP_INSTALL_PREFIX="$PWD/.security-otp" bash scripts/build-security-otp.sh
export PATH="$PWD/.security-otp/bin:$PATH"
make emqx-rel
EMQX_NODE__DB_BACKEND=rlog _build/emqx/rel/emqx/bin/emqx console
```

Docker/CI 的 EMQX builder 中旧 OTP 只用于引导编译；实际编译 EMQX 和打入发布镜像的运行时均为上述安全 OTP。支持 OTP 25～27 的编译检查不意味着旧版本具有相同的安全修复。切换 OTP 主版本时请使用干净的构建目录，避免旧 BEAM、解析器和依赖缓存混入。

官方 OTP 不包含 RLOG 所需的 `mnesia_hook` 扩展，mise 默认安装的官方 OTP 不能替代定制运行时。如果使用官方 OTP 请求 RLOG，启动脚本仍会安全回退到 Mnesia，不要删除此保护检查。新定制 OTP 已通过三节点 RLOG 和真实 MQTT 收发测试。商用分发须保留 Apache-2.0、第三方依赖许可证及修改说明。

## Docker 构建与发布

生产镜像使用 `env.sh` 中固定的 EMQX builder，在容器内编译并携带定制 OTP；不依赖宿主机 mise 中的 Erlang，也不复用宿主机 `_build`。在 Linux Docker 宿主机上执行：

```bash
EMQX_IMAGE_TAG=tryao/emqx-oss:local make emqx-docker
```

镜像包含本仓库的社区版后端和 `YiuTerran/emqx-dashboard-oss` 发布的自定义 Dashboard。Dashboard 版本由 `Makefile` 的 `EMQX_DASHBOARD_VERSION` 固定，包含 Kafka 入口、复制认证连接器时的密码处理、Token 范围限制和安全依赖更新。

发布顺序为：先发布 Dashboard tag，等待其 zip asset 生成，再提交后端修改并推送后端版本 tag。后端 GitHub Actions 会校验源码 tag 与包版本，构建 amd64/arm64 镜像，将 staging 镜像推送到 Docker Hub，并在三节点 RLOG 集群验证通过后发布多架构版本 tag。带 `-` 的版本（例如 `v5.8.9-build.4`）不会移动 `latest`。Docker Hub 凭据和镜像名使用仓库 `deploy` 环境中的 `DOCKERHUB_USERNAME`、`DOCKERHUB_TOKEN` 和 `DOCKERHUB_IMAGE`。

## 本地三节点集群验证

在仓库根目录运行，使用已发布的整体镜像：

```bash
export EMQX_IMAGE=tryao/emqx-oss:5.8.9-build.4
docker compose -p emqx-cluster-it -f deploy/docker/compose.cluster.yaml pull
docker compose -p emqx-cluster-it -f deploy/docker/compose.cluster.yaml up -d --wait --wait-timeout 180
python3 scripts/test/check-cluster.py --restart-node --security-runtime
```

Compose 使用三个 core 节点、RLOG、静态发现和各自的数据卷。节点使用 `emqx@emqx1.emqx.local` 等完整域名，并配置相应的 Docker 网络别名，以满足 Erlang 长节点名的要求。验证脚本要求三个节点均包含 `mnesia_hook` 且实际启用 RLOG，检查 Dashboard 的版本、Kafka bundle、登录和集群 API，完成所有节点对之间的 MQTT QoS 0/1/2 收发及保留消息检查，再停止第二个节点验证其余节点继续收发，最后启动该节点并检查重新入群和消息恢复。脚本只使用 Python 3.9+ 标准库。

| 节点 | MQTT | Dashboard |
| --- | --- | --- |
| emqx1 | `127.0.0.1:21881` | `http://127.0.0.1:28081` |
| emqx2 | `127.0.0.1:21882` | `http://127.0.0.1:28082` |
| emqx3 | `127.0.0.1:21883` | `http://127.0.0.1:28083` |

这些端口仅绑定本机。测试 Dashboard 账号为 `admin`，默认密码为 `public`；可以在首次启动前通过 `EMQX_DASHBOARD_PASSWORD` 和 `EMQX_CLUSTER_COOKIE` 设置测试密码与集群 cookie。此 Compose 用于本地验证，生产部署应配置独立的凭据、TLS 和监听端口。

停止集群并保留数据：

```bash
docker compose -p emqx-cluster-it -f deploy/docker/compose.cluster.yaml down
```

需要删除本次测试的数据卷时，再给 `down` 加 `--volumes`。

## 宿主机编译依赖

在 Debian/Ubuntu/Deepin 上，脚本从源码编译 OTP，需要 C/C++ 编译工具、Autoconf、ncurses 和 OpenSSL 开发库，可先安装：

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
