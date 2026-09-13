# B6b：持久、非阻塞的 RAGFlow 解析轮询

2026-09-13 源码候选。单人 Luna 实施/自测；真实业务 RAGFlow 和部署尚未验收。
公共定时语义见 `durable-scheduling.md`，Dataset 准入规则见 `v5-backlog-datasets-luna.md`。

## 执行方式

受理 → 准备/校验不可变制品与上传回执 → 提交或认领解析 → 单次观察 →
未完成时持久排下一条 → 释放 Worker → 到期再次观察 → 成功/失败。

每条后续 poll 只查询一次文档状态，不 sleep、不再次读 S3/源文件、不上传或 POST parse。
它另做一次 provider 租户查询，以复核身份；“一次查询”指文档解析状态，不是整个任务
只能有一个 HTTP 请求。最初 prepare 消息允许提交前查询与提交后即时查询，快路径
可以一次消费结束；长解析不再在同一 Worker 内循环等待。

`_wait_for_document_parse` 仅保留为旧兼容/回归辅助函数；生产摄入编排不再引用它。
只认 DONE 为成功，progress=1 不算完成；FAIL/CANCEL 为不同分类的可重试失败。
兼容数字及文本 run，包括整数 0。未知状态不猜测成功，进入 reconciliation_required。

## 状态及权威边界

唯一执行游标位于 job.metadata_json.ingestion_execution_v1：generation、step、phase、
deadline、interval、ready_at、已验证上传引用、最后观察结果。原始请求仍原样保存在
job.payload；客户端 metadata 的 retry_count 不再是执行代次来源。

服务端首条受理历史保存同名协议标记，客户端 document.metadata 与 Admin 状态 metadata
不得写该保留项（403）。没有该标记的旧任务不自动解释为新格式；不能仅看 metadata
存在类似字典就相信它。历史执行字段及消息的调查/受控迁入仍是发布前置工作。

游标中的 upload 是操作账已确认回执的引用/快照，不替代 knowledge_provider_operation。
每次执行都核对当前准入绑定、provider scope、上传操作原意图和回执、文档 id/name/dataset。
原文仍是不可变制品；RAGFlow 仍是派生系统。没有新表、新迁移或第二队列。

消息仍为 knowledge.ingest.v1，同一 upload_identity 作为 aggregate_key；payload 带
ingestion_id/generation/step。消费者核消息与资源键一致，旧代次/旧游标无副作用退出，
未来游标或无代次的旧消息失败关闭，不猜测补齐。不同 job 可以共享同一上传身份；
parse 操作键包含 upload_identity、job id、generation，避免不同 job 的重试序号混淆。
跨所有这些 parse 键的 executing/unknown 仍阻止盲目重复 POST。

## 事务与恢复

- 受理状态、初始游标、初始 Outbox 同事务。
- running、上传/解析副作用意图及确认回执有必要的中间提交；尤其 executing 必须在
  网络写入前持久化。初始消息此时未写 Inbox，崩溃可从同一消息继续。
- 解析 deadline/interval 在可能产生不确定结果的 parse POST 之前持久化。提交后丢回执
  不重置 deadline；已验证上传游标存在时重试也不再重新下载源文件。
- 普通 poll 不修改 Provider 操作账，只核对确认回执。游标推进、下一条 Outbox、当前
  Inbox 同事务；入队异常不能伪装成 Provider 失败并确认消息。
- DONE 时领域 Document/Version、job 成功、执行状态与 Inbox 同事务；FAIL/CANCEL/
  deadline 超时也与当前 Inbox 同事务。提交失败全部回滚，不产生孤立进度或后继消息。
- 不确定副作用仍保留操作账，job 记 reconciliation_required，但不写 Inbox，不越过
  公共重试/死信/重放机制，也不因 force retry 就清掉未知结果。

执行租约继续由公共 runtime 管；领域另外在 commit 与 ORM autoflush 之前核验当前
generation，防止旧 Worker 覆盖已获准的新重试。retry/dispatch 的行锁读取强制刷新
ORM 缓存。普通 poll 在 HTTP 查询期间不持有 job 行锁，因此管理操作不会被长查询锁住。
本包没有把任意 Admin 状态更新改造成取消协议；统一状态裁量、审批与撤销仍归后续产品设计。

## 时间及配置

- RAGFLOW_PARSE_TIMEOUT_SECONDS：1～86400 秒，默认 120。
- RAGFLOW_PARSE_POLL_INTERVAL_SECONDS：0.1～60 秒，默认 1，拒绝 NaN/Infinity。
- 使用 PostgreSQL clock_timestamp 计算绝对 deadline；首次等待为 interval，之后
  指数退避，最长 60 秒，下一次不晚于 deadline。
- 每条任务复用原 deadline/interval，重放、换进程、配置改变均不能重置；显式 retry
  才生成新代次与新 deadline，但仍要遵守既有未知副作用阻断。
- 每次查询受剩余 deadline 的 asyncio 超时限制，返回过晚也不算成功。deadline 到期
  不再查询 Provider；这证明本地等待预算耗尽，不证明远端没有执行成功。

一次 HTTP 读取失败可持久安排后继，直至 deadline；身份/协议冲突不当作普通网络抖动。
进入 RAGFlow 解析后移除凭据不能降级成 artifact_verified，而应阻断并等待配置恢复/调查。
Settings 有进程缓存，逐次复核不等于即时撤权广播；仍需一致配置、排空旧运行角色。

## 测试与未完成项

test_ingestion_polling_db.py 使用真实隔离 PostgreSQL，覆盖单次查询、事务失败、入队
冲突、deadline/backoff、数字状态、未知回执、旧消息、同源资源键、缓存重试、挂起查询中
并发 retry、租约失效、任务取消、真实子进程被 kill 后的恢复、死信重放及授权复核。
测试的时间推进仅注入可丢弃 schema；未改业务数据。HTTP/Provider/认证为注入，不能
宣称真实 RAGFlow 或跨机部署已验证。保留 metadata 拒绝语义另测实际 ASGI 两面及 Info 消费边。

源码不等于可直接滚动部署：旧消费者不认识 generation/step、执行标记或定时意图，
启用前必须盘点存量消息与任务、排空旧版本并统一运行角色。回滚前停止新生产者并
排空/对账新协议消息；不得删除 Outbox/Inbox/Provider 操作账或把 deadline 重置为现在。
旧任务不能只补一个标记“迁移完成”，必须先核准原始副作用与绑定。

旧 M1-203 的日志降噪子项仍交 B7 统一核验：共享 PostgreSQL 适配在 development 下
开启 echo，公共日志设置未对 SQL/HTTP 单独降噪。本包不偷偷改共享日志策略；也不以
没有循环等待为由声称日志、真实 Provider、容量或运行态回滚验收已经完成。
