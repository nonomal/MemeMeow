# Executor 失败诊断日志

Executor 把失败 attempt 的诊断写入 `/runtime/executor-logs/agent-diagnostics.log`。
该目录使用现有 `mememeow-agent-runtime-data` volume，容器重建后日志仍然保留。
宿主机运行 Executor 时，可以通过 `MEMEMEOW_EXECUTOR_DIAGNOSTICS_ROOT` 指定目录。

在部署使用的 `.env` 中设置文件轮转容量：

```dotenv
MEMEMEOW_AGENT_DIAGNOSTICS_ROTATION=200 MB
```

默认容量为 `200 MB`。Loguru 在单个文件达到容量时开始写入新文件，轮转时清理超过
14 天的日志。容量与保留期限共同决定占用空间；`200 MB` 是单个文件的轮转容量。
配置在 Executor 启动时读取，部署更新后生效。

每条 `executor_attempt_failure` 记录包含 Task 与 attempt 标识、恢复执行标志、最终状态、
错误码、失败阶段、执行耗时、进程退出码与回收状态，以及插件就绪状态和初始化错误。
signal 导致的负数退出码保持原值。

进程输出在临时文件关闭前采集。stdout 读取头尾各至多 256 KiB，stderr 读取头尾各至多
16 KiB；写入日志前移除已知凭据和本地路径，每行至多 500 个字符，每种输出摘要至多
16 KiB。stdout 仅保留 error 事件中的原因与状态码，普通消息正文不进入日志。
`stdout_bytes`、`stderr_bytes` 保存原始字节数，`*_truncated` 表示内容被截断，
`stdout_omitted_lines` 表示过滤的行数。

插件检查失败、进程异常退出、超时、取消和结果文件校验失败均使用同一诊断入口。
日志包含内部故障信息，应通过服务器运维权限读取。
