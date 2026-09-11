# Knowledge 公共可靠投递与 RAGFlow 回执

本轮开发候选，尚未正式发布。公共投递代码来自模板；领域 handler 注册
`knowledge.ingest.v1`。Admin 与 Internal 共用受理服务，job 与 Outbox 同事务。
相同幂等键的并发请求只建一单；相同键但不同 payload 拒绝。dispatch 只请求排队，
retry 更新代次并同事务入队，不再直接调 Celery 或在 API 进程中执行。

## Provider 操作边界

`knowledge_provider_operation` 是外部副作用意图/回执账，不是领域文档主档。
dataset 以目标名互斥；upload 以 source app + source version + dataset 的稳定 UUID
互斥；不同 ingestion 也不能重复上传同一版本。意图保存 Provider endpoint/tenant
摘要、不可变 artifact、版本、目标 dataset、文件名及内容 SHA-256。

`intent → executing → confirmed`；远端响应丢失写 `unknown`，进程直接死亡留下的
`executing` 同样按结果未知处理。**executing 必须在访问远端之前持久提交。**
重复消费先查询：有回执继续查询/解析，没有回执先对账；单次查无结果不代表从未执行，
不自动重传。每次中间提交仍由公共消费租约 fencing，过期 Worker 不能回写迟到结果。

上传使用含稳定版本 UUID 和内容摘要的文件名。认领文档时同时核对文档 ID、dataset、
文件名，并下载原文件限额校验长度与 SHA-256，不能只凭提供的 document_id 确认。
解析提交也保留回执；RUNNING/SCHEDULE/DONE 只查询，未知提交不能通过新 retry 代次
绕过。已明确确认的失败解析可以在显式 retry 后继续，不重新上传原文。

RAGFlow 当前不提供本实现使用的 Idempotency-Key 或远端 fencing。以上保证约束
本服务发起的操作和本地提交，不宣称能撤销已经发出的网络请求，也不约束其它客户端
在同一 tenant 中独立创建资源。无法证实的情形保留阻断，不能用猜测补回执。

## 运维入口

运行前确认数据库、Provider tenant 与 Secret 是该实例的受控配置；按运维授权执行。
以下命令不会隐式迁移数据库。

```bash
# 查看某单的 dataset/upload/parse 状态及已确认回执
python -m app.cli.provider_receipts show --ingestion-id <uuid>

# 运维已定位候选 document_id 时，校验并认领上传回执
python -m app.cli.provider_receipts recover-upload --ingestion-id <uuid> --document-id <id>

# 公共投递对账、死信及显式重放（不会删除 Inbox）
python -m app.cli.durable_delivery reconcile
python -m app.cli.durable_delivery dead-letters
python -m app.cli.durable_delivery replay --message-id <uuid>
```

`recover-upload` 只对 Provider 发 GET，不上传、不解析、不将 job 标为 succeeded，
也不自动重放。它只接受已经保存稳定意图的上传；任意 document_id、内容/版本/tenant
不符均拒绝。执行者应按既有运维流程留存操作记录。`legacy_unknown` 缺少稳定回执身份，
不能通过此入口绕过，须逐单调查原 Worker/Provider 记录并制定专门的数据恢复方案。

## 迁移与回滚

`20260911_0006` 接在 `20260811_0005` 后，由独立 Migration Job 运行；先停旧 Worker、
Scheduler 与旧 API 写路径，完成备份恢复演练。迁移回填 accepted/running 命令；
已完成单不回填。历史 running 可能已上传，标为 `legacy_unknown`，不能视作新上传。
旧 Celery 任务和裸 `ingest_into_ragflow` 入口显式拒绝执行，禁止新旧混跑。

空增量可 downgrade/re-upgrade；存在未确认命令或 Provider 回执时 downgrade 拒绝
丢弃数据。已运行的候选应按已验证的整库备份恢复流程回滚，并单独处置回滚窗中新请求
及派生 Provider 资源。不得清空回执表来使 downgrade 强行通过。

## 当前证据

原始本地套件 113 passed；最终完整 145 passed（无跳过），包括公共真实 PostgreSQL
13 项、领域真实 PostgreSQL 18 项、Provider API 形状、回执认领、未知 parse 跨重试
代次拒绝和迁移回填回归。Ruff/format/Pyright 通过；其余门禁与边界见父仓
`docs/template-alignment-durable-delivery.md` 及工作区接手记录。

实际 `v0.25.4-sunmoonai.1` 只读源码和端点证明：精确 dataset name 不存在时返回
权限错误，分页总数实际为 `total_datasets`；实现采用授权列表分页后精确过滤，拒绝
不完整查询。文档列表支持 ID/keywords；原文件 GET 用于内容核验。

真实 Provider 使用两个独立 UUID dataset 验证了“创建成功响应丢失”和“上传成功响应
丢失”：均仅创建 1 次、上传 1 次，最终 counter/Inbox 为 1/1；原文件内容验证通过。
临时 dataset 和随机测试 schema 均已删除，可用接手探针重新创建；没有业务文档，
没有调用解析或 embedding 模型。解析响应丢失目前由可控 Provider 故障测试覆盖，
不能把这项实测说成完整真实 RAGFlow 解析验收。
