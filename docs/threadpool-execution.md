# 同步操作的线程执行

Web 异步代码使用 Starlette 的 `run_in_threadpool` 执行同步操作。同步反向图片 provider、同步 callback、视觉健康请求和模型操作采用此方式，使用当前事件循环的 AnyIO 默认线程容量。

主页 `/search` 将完整的同步搜索处理交给 `run_in_threadpool`，包括缓存检查、embedding 和可选 LLM 请求、媒体地址解析及缩略图查询。请求解析完成后进入工作线程，结果返回后由异步入口交给 FastAPI。线程调度位于 HTTP 模块，搜索服务保持同步接口。

反向图片 callback 保留独立 Task 和 `asyncio.shield`，调用方取消后等待实际执行结束。视觉服务的健康检查与推理共用一个并发名额，防止首次加载模型时并发进入；请求取消后，线程完成才释放名额。

同步线程无法通过取消协程强制终止。AnyIO cancel scope 和直接调用 `asyncio.Task.cancel()` 的行为不同，需要分别考虑。数据库 Session 应在同步业务内部创建、使用和关闭。

验证：`tests/test_threadpool_execution.py` 使用真实线程、ContextVar、Event、文件错误与图片解码错误，验证上下文、取消、容量释放和错误传播。测试不替换生产函数或外部客户端。

`tests/test_search_threadpool.py` 使用真实 PostgreSQL 和搜索入口，通过 SQLAlchemy 事件记录查询线程及 ContextVar，验证缓存检查在工作线程执行，HTTP 错误能够返回调用方。

仍存在同步外部等待的异步调用：配置与 Settings 中的健康请求、任务取消中的 executor 请求，以及应用关闭中的 executor 取消和子进程等待。
