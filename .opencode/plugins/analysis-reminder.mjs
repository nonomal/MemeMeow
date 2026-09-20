// OpenCode 主 session 分析用量提醒；进程终止由 Executor 独立负责。
import net from "node:net";
import { OpencodeClient } from "@opencode-ai/sdk/v2";

const runtimeVersion = "1.18.18";
const reminder = "请尽快完成必要工作、验证结果并生成报告。";

/** 通过 Executor 创建的 Unix socket 发送状态，避免 Agent 可写目录成为权威来源。 */
async function publish(socketPath, state) {
  await new Promise((resolve, reject) => {
    const connection = net.createConnection(socketPath);
    let settled = false;
    const finish = (callback, value) => {
      if (settled) return;
      settled = true;
      callback(value);
    };
    connection.once("connect", () => {
      connection.end(JSON.stringify(state), () => finish(resolve));
    });
    connection.once("error", (error) => finish(reject, error));
    connection.once("close", () => {
      if (!settled) finish(reject, new Error("analysis_plugin_status_socket_closed"));
    });
  });
}

/** 从 SDK 错误提取阶段和原因，供当前 attempt 的状态及日志诊断使用。 */
function describeError(stage, error) {
  const reason = error?._tag ?? error?.cause?.code ?? error?.code ?? error?.name ?? "UnknownError";
  const message = error?.message ?? String(error);
  return `${stage}:${reason}:${message}`;
}

/** 使用当前进程的 SDK transport 读取累计金额，每个 attempt 最多提交一次 steer 输入。 */
export default async function analysisReminder({ client }, options) {
  const { attempt_id, policy, socket_path, session_id } = options ?? {};
  if (!attempt_id || !socket_path || !policy || policy.version !== 1) {
    throw new Error("analysis_plugin_configuration_invalid");
  }
  const state = {
    attempt_id, plugin_version: runtimeVersion, policy_version: policy.version, ready: false,
    session_id: session_id ?? null, reminder_sent: false, error: null,
  };
  // 固定版本的 v1 transport 保留 OpenCode 进程内 fetch，v2 SDK 提供累计金额投影。
  const sdk = new OpencodeClient({ client: client._client });
  try {
    const response = await sdk.global.health({ throwOnError: true });
    if (response.data.version !== runtimeVersion) {
      throw new Error("analysis_plugin_runtime_version_incompatible");
    }
    state.ready = true;
    await publish(socket_path, state);
  } catch (error) {
    state.error = error.message === "analysis_plugin_runtime_version_incompatible"
      ? error.message : "analysis_plugin_initialization_failed";
    await publish(socket_path, state);
    throw error;
  }

  let checking = false;
  let submissionStarted = false;
  return {
    "experimental.chat.system.transform": async (input) => {
      // prompt admission 可能引起 hook 重入；重入立即返回，不等待当前提交。
      if (checking || submissionStarted) return;
      if (state.session_id && input.sessionID !== state.session_id) return;
      checking = true;
      let stage = "analysis_reminder_session_read";
      try {
        try {
          if (!input.sessionID) throw new Error("analysis_reminder_session_missing");
          if (state.session_id && input.sessionID !== state.session_id) return;
          const response = await sdk.v2.session.get(
            { sessionID: input.sessionID }, { throwOnError: true },
          );
          const session = response.data.data;
          if (session.parentID) return;
          if (session.id !== input.sessionID) throw new Error("analysis_reminder_session_mismatch");
          state.session_id = session.id;
          if (typeof session.cost !== "number" || !Number.isFinite(session.cost) || session.cost < 0) {
            throw new Error("analysis_reminder_cost_invalid");
          }
          if (session.cost >= Number(policy.reminder_cost)) {
            // 响应丢失时无法确认服务端是否已接收，因此本 attempt 不重复提交。
            submissionStarted = true;
            stage = "analysis_reminder_prompt_submit";
            const admitted = await sdk.v2.session.prompt(
              { sessionID: session.id, prompt: { text: reminder }, delivery: "steer" },
              { throwOnError: true },
            );
            state.reminder_sent = true;
            console.info(JSON.stringify({
              event: "analysis_reminder_admitted", attempt_id,
              session_id: session.id, message_id: admitted.data.data.id,
              admitted_seq: admitted.data.data.admittedSeq,
            }));
          }
          state.error = null;
        } catch (error) {
          state.error = describeError(stage, error);
          console.error(JSON.stringify({
            event: "analysis_reminder_failed", attempt_id,
            reason: state.error, submission_started: submissionStarted,
          }));
        }
        try {
          await publish(socket_path, state);
        } catch (error) {
          // 仅记录状态 socket 错误类别；Executor 的金额检查不依赖提醒状态写入。
          console.error(JSON.stringify({
            event: "analysis_reminder_status_write_failed", attempt_id,
            reason: describeError("analysis_reminder_status_write", error),
          }));
        }
      } finally {
        checking = false;
      }
    },
  };
}
