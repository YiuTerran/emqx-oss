# 插件说明

本 fork 保留 EMQX 5.x 的插件机制。插件作为独立项目开发和维护，不放在本仓库的 `apps/` 下。开发时可参考 [EMQX 插件模板](https://github.com/emqx/emqx-plugin-template)，并核对插件与本 fork 的 EMQX 5.8.9 接口及依赖版本是否兼容。

本仓库已移除企业版应用、LDAP 认证和 MQTT over QUIC；插件不应假定这些功能可用。
