# 用户消息提醒验收

## 实现与测试

- 公共实现：`1c42c3868ee44544dca3aa716662a42866dfb2fa`。
- Server 普通 merge：`59a4008642df5ef78a12e3431edc1a476af067ed`，已核验公共提交为其祖先。
- 两端 `node --test tests/analysis_reminder.test.mjs`：各 2 项通过，覆盖配置拒绝及真实 SDK 网络错误诊断。
- 两端金额策略和真实 SQLite 测试：各 41 项通过。
- 两端 OpenSpec 严格校验及 Git diff 检查通过。
- Server 使用已有 `./start.sh start` 构建启动，开发服务健康；共享 node_modules 使用镜像既有安装。

## 真实业务结果

- Job：`0713d745-c82b-4f99-a29a-29a80574f440`，最终 `succeeded`。
- Agent Task：`5d35cfeda59c40b5aaa47b1ef5acb580`，最终 `succeeded`。
- Executor attempt：`host-attempt-60b08289d47e48d68d5231fe142d655c`，业务 attempt 为 `completed`。
- 主 session：`ses_f3e091cd6ffe9G0FkLpVKfLy2c`。
- 冻结策略：`model_free`，`medium`，提醒 0.05 美元、终止 0.10 美元。
- 提醒消息：`msg_0c1fa6a04001BjJDHTSNgez32S`，在累计金额 0.0570385 美元时保存。
- `message` 与 `part` 中存在一条普通 user 提醒，正文为“请尽快完成必要工作、验证结果并生成报告。”，没有 synthetic 隐藏标记。
- OpenCode 完整会话 export 能读取同一提醒；TUI 源码的 session messages 路径读取相同消息数据。本次没有进行交互式 TUI 屏幕验收。
- messages hook 将保存返回的消息加入当前请求。后续 assistant 的 `parentID` 为提醒 ID，最终 assistant 为 `finish=stop`；结果校验和业务处理完成。
- 最终金额 0.094709 美元，`reminder_sent=true`，`process_reaped=true`，没有终止原因或错误。
- 提醒正文在整个 session 中仅出现一次。

## 验证范围

该记录证明当前用户提醒路径及一次真实任务成功完成。模型是否在任意输入下均能在限额内完成，不由单次成功保证；Executor 继续独立执行金额终止。change 中其他未完成验收项保持未完成状态。
