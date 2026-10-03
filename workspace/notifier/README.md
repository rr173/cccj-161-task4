# 聚合商通知投递模块（独立服务）

需求响应事件的 **发布 / 撤销 / 开始 / 结束 / 报告订正** 发生时，为每个订阅方生成
**不可变签名通知信封**，维护**按订阅方独立的持久待发队列**，并提供订阅校验、签名投递、
确认 API 与投递台页面。模块是一个**独立进程**，只以后向只读方式尾随编排服务的事件日志，
不向编排内核回写任何东西。

- 零依赖、纯 Node.js（≥ 20，全局 `fetch`），无数据库、无构建步骤。
- **只增账本 + SHA-256 哈希链**：信封、入队、租约、送达、失败、死信、重入队、确认、
  密钥轮换全部是只增记录；进程重启逐条重放重建全部投影。任何历史篡改都会断链并进入只读隔离。
- **Ed25519 非对称签名**：信封用聚合商私钥签名，订阅方用公钥独立复验；
  **订阅方回执 HMAC-SHA256** 证明确认来自订阅方。
- 默认端口 **8091**（编排服务是 8080），账本默认 `data/notifier.log`。

```bash
npm run notifier        # 启动投递服务 + 投递台 → http://localhost:8091/
npm run demo:notifier   # 一键演出全部 7 个故障/恢复场景（真实 HTTP 回环，演完即停）
npm run test:notifier   # 24 项真实 HTTP 端到端断言
npm test                # 39 项 node:test 单测（含 12 项通知契约）
```

## 核心不变量（需求如何落地）

| 需求 | 实现 |
| --- | --- |
| 事件时生成不可变通知信封 | 每事件生成全局单调 `envelopeSeq` 的信封并 `Ed25519` 签名，以 `DR_NOTIFICATION` 落链；信封体此后永不更新，`/api/envelopes/:seq/verify` 可独立复算签名 + 链锚点 |
| 按订阅方分别维护持久待发队列 | 每订阅方一条 `queueSeq` 链表（`QUEUE_ENQUEUED`），各自独立游标 `cursor`，随账本持久化 |
| 同一订阅方按事件顺序投递 | 调度器对每个订阅方**严格串行**，且只为“游标之后第一个未确认项”（队首）建租约；`NOT_HEAD` 拒绝跳号；DEAD 停在队首阻塞后续，绝不越过 |
| 稳定幂等键处理超时后再次请求 | 幂等键 = `ack:<subscriberId>:<envelopeSeq>`，与尝试次数无关；租约超时后 `DELIVERY_RECLAIMED` 并用**同一键**重投，订阅方按键去重 |
| 不同订阅方互不阻塞 | 所有订阅方的投递循环并发执行（`Promise.all` + 每订阅方忙标志）；一方慢/失败/死信不影响他方 |
| 重启后继续未确认投递 | 启动重放账本；上次残留的 `INFLIGHT` 租约没有归属，统一追加 `INFLIGHT_RECOVERED` 复位为 `PENDING`，由调度器重新认领 |
| 不能重复推进游标 | 游标只在 `DELIVERY_ACKED` 且 `envelopeSeq === cursor+1` 时推进；重复确认由幂等键表直接短路；apply 层再断言一次，任何路径都无法重复/回退/跳号 |
| 订阅校验 | WebSub 风格：对回调 URL 发 `GET?hub.mode=subscribe&hub.challenge=…` 并带聚合商签名，订阅方须原样回显 challenge；未通过为 `REJECTED`，不收任何通知 |
| 签名通知 | `POST` 回调：body 为签名时的规范化 JSON（键名排序），头带 `X-Aggregator-Signature`、`X-Key-Id`、`X-Idempotency-Key` |
| 确认 API | `POST /api/confirmations`，HMAC-SHA256(secret, 规范化回执体) 放 `X-Subscriber-Signature`；响应幂等（重复确认返回相同 cursor，`duplicate:true`） |
| 密钥轮换期间旧在途通知校验 | 轮换只追加 `KEY_ROTATED`，旧密钥**永不删除**；信封永远携带签发时的 `keyId`，订阅方按 `keyId` 取旧公钥，旧信封/旧在途重投仍验签通过 |
| 死信后人工重新入队 | 连续失败达上限转 `DELIVERY_DEADLETTERED`（DEAD，阻塞本队列后续）；`POST .../queue/:seq/requeue` 追加 `DLQ_REQUEUED`，attempts 归零、位置不变，恢复保序投递 |
| 恢复后顺序和签名证据仍可验证 | `GET .../queue/:seq/evidence` 返回信封、验签结论与全链路账本轨迹（ENQUEUED→LEASED→SENT→RECLAIMED→…→DEADLETTERED→REQUEUED→ACKED） |

## 五个通知类型

| type | 触发（编排日志记录 / 手工 API） |
| --- | --- |
| `dr.event.published` | `EVENT_PUBLISHED` |
| `dr.event.cancelled` | `EVENT_CANCELLED`（`CANCEL_AFTER_START` 内部归位记录不重复通知） |
| `dr.event.started` | `EVENT_ACTIVATED`（时窗开始、基线冻结） |
| `dr.event.ended` | `EVENT_CLOSED` |
| `dr.event.report.corrected` | `REPORT_ISSUED` 且 `version > 1`（订正报告；v1 原始裁决不重复打扰） |

事件接入用稳定 `sourceRef` 去重：尾随编排日志时为 `orch:<orchestratorSeq>`，
**尾随位置丢失、重启、与手工 API 并发都安全**——同一 sourceRef 永远返回同一信封，不重复入队。
设置 `NOTIFIER_TAIL_ORCH=0` 可关闭桥接（只通过 `/api/events/ingest` 手工接入）。

## 一页走查（投递台 http://localhost:8091/）

1. 左上加订阅方，回调 URL 留空即用内置模拟端；创建即发起订阅校验，secret 只显示一次。
2. 中间选通知类型 + 事件 ID 生成信封；右侧“每订阅方持久待发队列”看它按 # 变 INFLIGHT→ACKED、cursor 推进。
3. 左下选订阅方注入故障：
   - **FAIL_NEXT:n**：连续 n 次 503，看同幂等键重试、attempts 增长、最终成功；
   - **LOSE_ACK**：送达但不确认，租约超时后同键重投（“查看收信台账”里见到投递多次但台账只一条），再用“补发确认”推进；
   - **DUPLICATE_ACK**：确认连发两次，游标只推进一次；
   - **REJECT_VALIDATION**：重新注册时拒绝校验，订阅不生效。
4. 点“轮换签名密钥”后，对**轮换前已在途**的信封，投递台证据里 `keyActive=false` 但 `signatureValid=true`。
5. FAIL_NEXT 预算调大让某封进死信：它停在队首、后续信封保持 PENDING；其他订阅方照常。
   “人工重新入队”后该封 attempts 归零、按原位置继续，后续信封依次跟上。
6. “独立验签信封”与“证据”按钮展示 canonical JSON、签名、账本 hash 与完整轨迹。
7. 重启服务（`npm run notifier` 再起）：游标、队列、死信、密钥、收信台账全部从账本恢复。

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/state` | 总览：链状态、密钥、各订阅方队列与游标、死信/在途计数 |
| POST | `/api/subscribers` | 注册订阅方并立即发起校验 `{subscriberId,callbackUrl,topics?,displayName?}`，secret 仅返回一次 |
| GET | `/api/subscribers` / `/api/subscribers/:id` | 订阅方视图（含 queue 每项状态/尝试/错误/时间） |
| POST | `/api/subscribers/:id/validate` | 重新发起订阅校验 |
| GET | `/api/keys` · POST `/api/keys/rotate` | 查看密钥（含旧公钥）· 轮换签名密钥 |
| GET | `/api/notify-types` | 5 类通知类型 |
| POST | `/api/events/ingest` | 手工接入 `{type,eventId,sourceRef?,detail?}`（sourceRef 幂等） |
| POST | `/api/confirmations` | 订阅方确认（回执 HMAC 头 `X-Subscriber-Signature`），幂等 |
| POST | `/api/subscribers/:id/queue/:seq/requeue` | 死信人工重新入队 |
| GET | `/api/envelopes/:seq` · `/api/envelopes/:seq/verify` | 信封内容 · 独立验签 + 链锚点 |
| GET | `/api/subscribers/:id/queue/:seq/evidence` | 证据包（信封/签名/验签/全链轨迹） |
| GET | `/api/chain/verify` · `/api/log?type=&limit=` | 通知链 + 模拟端台账校验 · 账本 JSONL |
| GET/POST | `/mock/subscribers/:id/callback` | 内置模拟订阅端（GET 订阅校验 / POST 收信） |
| POST | `/mock/mode` | 注入故障 `{subscriberId,mode,budget?}` |
| GET | `/mock/subscribers/:id/inbox` · `/logs` | 模拟端收信台账（幂等去重）· 操作日志 |
| POST | `/mock/subscribers/:id/resend-ack/:seq` | 模拟端手工补发确认（确认丢失场景） |

## 账本记录类型（data/notifier.log）

```
KEY_ROTATED · SUBSCRIBER_REGISTERED · SUBSCRIPTION_VALIDATED
DR_NOTIFICATION · QUEUE_ENQUEUED
DELIVERY_LEASED · DELIVERY_RECLAIMED · DELIVERY_SENT
DELIVERY_FAILED · DELIVERY_DEADLETTERED · DELIVERY_ACKED
DLQ_REQUEUED · INFLIGHT_RECOVERED
```

每条记录 `{seq,ts,type,payload,prevHash,hash}`，`hash=sha256(规范化JSON(其余字段))`，
复用 `../src/store.js` 的哈希链与重启恢复契约（末尾断电残尾归档续链；其余损坏只读隔离）。
模拟订阅端另有自己的只增台账 `data/mock-received.log`（`MOCK_RECEIVED` / `MOCK_MODE`），
注意：**重复投递只在内存累加 seen 计数，绝不原地改写账本 payload**。

## 投递状态机

```
QUEUE_ENQUEUED ──PENDING
   │ lease()                 ┌─ 租约超时 ─→ DELIVERY_RECLAIMED（同一幂等键）─┐
   ▼                          ▲                                              │
 INFLIGHT ──SENT(2xx)─────────┤ （仍等确认，确认由订阅方调确认 API）          │
   │                          │                                              │
   ├─ 投递失败/非2xx/超时 → DELIVERY_FAILED → PENDING（指数退避后重试）        │
   │        第 maxAttempts 次失败 → DELIVERY_DEADLETTERED（DEAD，阻塞后续）    │
   │                              └─ 人工 POST requeue → DLQ_REQUEUED → PENDING
   └─ 确认 API（HMAC 合法、键未确认过、envelopeSeq=cursor+1）→ DELIVERY_ACKED → ACKED，cursor+1
                                 重复确认同一键 → 幂等返回，cursor 不动
```

## 配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `NOTIFIER_PORT` | 8091 | 监听端口 |
| `NOTIFIER_LOG` | data/notifier.log | 通知只增账本路径 |
| `NOTIFIER_MOCK_LOG` | data/mock-received.log | 模拟订阅端台账路径 |
| `NOTIFIER_TAIL_ORCH` | `1` | 是否只读尾随编排日志（`0` 关闭） |
| `DR_LOG` | data/events.log | 被尾随的编排服务日志路径 |
| `NOTIFIER_LEASE_MS` | 15000 | 投递租约（超时可用同键重投） |
| `NOTIFIER_POLL_MS` | 100 | 调度轮询间隔 |
| `NOTIFIER_MAX_ATTEMPTS` | 4 | 死信阈值（投递尝试次数） |
| `NOTIFIER_BACKOFF_BASE_MS` / `_MAX_MS` | 300 / 5000 | 失败指数退避 |
| `NOTIFIER_HTTP_TIMEOUT_MS` | 4000 | 单次投递/校验 HTTP 超时 |

## 文件

```
notifier/
  server.js          HTTP API + 投递台静态页 + 组件装配（含 restartFromLedger 热重放）
  engine.js          领域引擎：全部状态只通过追加账本记录改变；队列/游标/密钥/信封投影
  crypto.js          Ed25519 签名/验签、订阅方回执 HMAC、密钥对生成
  dispatcher.js      每订阅方严格串行 + 全局并发；租约、超时重投、退避、死信
  transport.js       HTTP 投递（fetch，超时/非2xx 失败）
  subscription.js    WebSub 风格订阅校验（challenge 回显 + 签名）
  tailer.js          只读尾随编排日志 → 5 类通知（sourceRef 幂等）
  mock-subscriber.js 内置模拟订阅端（故障模式、幂等台账、HMAC 回执）
  demo.mjs           7 场景一键演示
  e2e.mjs            24 项真实 HTTP 端到端断言
  public/index.html  投递台（单文件原生 JS）
test/notifier.test.js 12 项领域/恢复契约单测
```
