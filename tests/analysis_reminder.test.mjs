// 使用真实 SDK 网络错误和 Unix socket 验证插件的初始化诊断。
import assert from "node:assert/strict";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import net from "node:net";
import { once } from "node:events";
import path from "node:path";
import test from "node:test";
import { createOpencodeClient } from "../.opencode/node_modules/@opencode-ai/sdk/dist/client.js";
import analysisReminder from "../.opencode/plugins/analysis-reminder.mjs";

test("缺少 attempt 配置时立即拒绝初始化", async () => {
  await assert.rejects(analysisReminder({}, {}), /analysis_plugin_configuration_invalid/);
});

test("真实连接失败通过 Unix socket 保留初始化阶段和网络原因", { timeout: 10000 }, async () => {
  await mkdir(".pytest_cache", { recursive: true });
  const directory = await mkdtemp(path.resolve(".pytest_cache/steer-node-"));
  const socketPath = path.join(directory, "status.sock");
  const server = net.createServer();
  try {
    server.listen(socketPath);
    await once(server, "listening");
    const received = (async () => {
      const [connection] = await once(server, "connection");
      let data = "";
      for await (const chunk of connection) data += chunk;
      return JSON.parse(data);
    })();
    // 端口 0 不提供 HTTP 服务；实际 fetch 必须报告连接错误。
    const client = createOpencodeClient({ baseUrl: "http://127.0.0.1:0" });
    await assert.rejects(analysisReminder({ client }, {
      attempt_id: "steer-network-test", socket_path: socketPath,
      policy: { version: 1, reminder_cost: "0.05" },
    }));
    const state = await received;
    assert.equal(state.ready, false);
    assert.equal(state.reminder_sent, false);
    assert.equal(state.error, "analysis_plugin_initialization_failed");
    assert.match(state.error_detail, /analysis_plugin_initialize:ECONNREFUSED:/);
  } finally {
    await new Promise((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
    await rm(directory, { recursive: true });
  }
});
