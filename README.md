# vertex-master-cf — Vertex Master 全功能 Cloudflare Workers 版

> 基于 [vertex-master](https://github.com/zhu748/vertex-master)（Go）的全功能移植，部署目标为
> **Cloudflare Workers（official 模式免费计划即可；anon + 代理池建议 Workers Standard）**。在 cf-vproxy
> （同作者的 official 模式 Workers 精简版）基础上，**补齐了 anon 匿名端点模式**，并把出站代理层按其
> 设计重写（`cloudflare:sockets` 隧道 + 纯 JS TLS 1.3 + 连接复用池 + 订阅拉取 + 对冲竞速）。
> 当前版本 **v1.3.0**（第二轮逐行比对：Anthropic 协议级 ping 保活 + Workers 专项优化）。
>
> v1.3.0 变更（相对 v1.2.0）：
>
> | 改动 | 说明 |
> |------|------|
> | **Anthropic 协议级 ping** | 流式连接每 15s 发送 `event: ping`（对齐 Go 版 `emitPing`）；Claude Code SDK 靠它判断连接存活，长思考场景不再被误判超时断连。其他协议仍用 SSE 注释帧 |
> | **thinking budget 对齐** | `HIGH` 档在全部 2.5 系统一映射为 24576（对齐 Go `budgetForThinkingLevel`；旧版 pro 误为 32768） |
> | **示例密钥拒绝** | 照抄示例的 `sk-your-key-here` 直接 401（对齐 Go 版 `withAPIKey`） |
> | **闸门范围收窄** | 并发闸门仅作用于出站端点（对齐 Go `isUpstreamWorkloadPath`）：满载流式请求时 `GET /v1/models` 等列表端点仍可用，SDK 启动不再报错 |
> | **Images 并行生成** | `n>1` 时滑动窗口并发（默认 3）：n=8 延迟从串行 8 倍降至 ⌈8/3⌉ 倍；结果保序、任一失败即返回首个错误（语义同 Go 串行 fail-fast） |

**两种上游通道（原项目 `gemini_api_mode` 语义，面板一键切换）：**

| 模式 | 上游 | 鉴权 | 说明 |
|------|------|------|------|
| `anon`（默认） | Cloud Console 匿名 `batchGraphql` 端点 | **无需任何 Google Key**（reCAPTCHA token 现抓现用） | 对齐 vertex-master 初始行为；出口 IP 建议配置代理池轮换 |
| `official` | `generativelanguage.googleapis.com` 官方 API | 单个 Gemini API Key | 无需 reCAPTCHA；支持镜像/中转基地址覆盖 |

## ✨ 功能总览（对齐原项目）

- **四协议兼容**：OpenAI Chat Completions、OpenAI Responses、Anthropic Messages、Gemini 原生接口
- **流式与工具调用**：全部端点支持流式/非流式文本；Function Calling / Tool Use 及结果回传
- **多模态**：图片输入、文生图（`generateContent` + gemini-*-image）、语音合成 TTS
- **假流式**：`fake-`/`假流式-` 前缀全端点生效 + `aggregate_stream` 全局开关
- **出站代理池**：`socks4://`、`socks4a://`、`socks5://`、`http://`（CONNECT）；订阅自动拉取
  （纯文本/Base64，`type://host:port` 行式，上限 1000 节点）、健康巡检、指数冷却、粘性优选、
  对冲竞速、429 单 IP 窗口轮转 / 项目级每日配额硬失败分类、伪造密钥错误接力验证、全池熔断直连兜底
- **Web 管理面板**：`/admin` —— 配置热更新、代理管理、用量统计、请求日志、指标、模型表管理
- **Token 计数**：Anthropic `count_tokens` + Gemini `:countTokens`（single-flight + LRU + TTL 缓存；
  anon 模式走匿名 `CountTokens` 操作）
- **n 多候选**：OpenAI `n` 参数并发多候选合并（`max_n`）
- **Cron 定时任务**：健康巡检 / 订阅差异更新 / keepalive 保活（免费计划可用）
- **请求闸门**：`max_concurrent_requests`（503 + Retry-After）与 `max_request_mb`（413）

## 🚀 三步部署

### 1. 创建 KV 命名空间

```bash
cd vertex-master-cf
npm install
npx wrangler login
npx wrangler kv namespace create VPROXY_KV
```

把命令输出的 `id` 填入 `wrangler.jsonc` 的 `kv_namespaces[0].id`（替换
`REPLACE_WITH_YOUR_KV_NAMESPACE_ID`）。

### 2. 部署

```bash
npx wrangler deploy
```

部署完成后记下地址（默认 `https://vertex-master-cf.<你的子域>.workers.dev`）。

### 3. 初始化配置

- 浏览器打开 `https://<worker地址>/admin` —— 首次访问控制台会输出/可在
  `wrangler tail` 中看到 **管理员密码**（也可用 `wrangler secret put ADMIN_TOKEN` 自定）
- 「配置」页添加 **客户端鉴权 Key**（如 `mykey123`，无需 `sk-` 前缀）—— 不配置则所有业务请求 403
- 选择 **上游通道模式**：
  - `anon`（默认）：无需 Key 即可使用；**强烈建议同时配置代理池**（见下）
  - `official`：填入 Gemini API Key（`wrangler secret put GEMINI_API_KEY` 或面板配置页均可）

## 🔌 客户端接入

同一个本地 API Key 可用于以下协议：

| 协议 | 端点 | 鉴权 |
|------|------|------|
| OpenAI Chat Completions | `POST /v1/chat/completions` | `Authorization: Bearer <key>` |
| OpenAI Responses | `POST /v1/responses` | `Authorization: Bearer <key>` |
| OpenAI Images / TTS | `POST /v1/images/generations` `POST /v1/audio/speech` | `Authorization: Bearer <key>` |
| Anthropic Messages | `POST /v1/messages` | `x-api-key: <key>` |
| Anthropic Token Count | `POST /v1/messages/count_tokens` | `x-api-key: <key>` |
| Gemini GenerateContent | `POST /v1beta/models/{model}:generateContent` | `x-goog-api-key: <key>` 或 `?key=` |
| Gemini StreamGenerateContent | `POST /v1beta/models/{model}:streamGenerateContent?alt=sse` | `x-goog-api-key: <key>` 或 `?key=` |

示例（OpenAI 兼容客户端把 API 地址填为 `https://<worker地址>/v1` 即可）：

```bash
curl https://<worker地址>/v1/chat/completions \
  -H "Authorization: Bearer mykey123" \
  -H "Content-Type: application/json" \
  -d '{"model":"gemini-3.6-flash","messages":[{"role":"user","content":"你好"}],"stream":true}'
```

## 🔑 上游通道详解

### anon 模式（默认，免 Key）

- 上游为 Cloud Console 匿名端点 `cloudconsole-pa.clients6.google.com` 的
  `batchGraphql`（`StreamGenerateContentAnonymous` / `CountTokens` 操作），
  常量逐字节对齐原项目（querySignature / clientVersion / pagePath 等）。
- **reCAPTCHA token 现抓现用**：anchor iframe GET 抠 base token → reload POST 拿 `rresp`，
  失败自动重试；`x-browser-validation` 头由 WebCrypto SHA-1 现算。
- 鉴权失败重试语义从原项目移植：匿名端点首帧预期 verify-fail → 同 token 重试一次 →
  换 token 重试 → 429 冷却节点接力（每出口 IP 独立限流窗口，轮转即解）。
- **token 与出口 IP 绑定**，因此 anon 模式按「健康候选接力」（最多 3 个候选）顺序执行，
  不做对冲竞速（避免 token/IP 失配导致的 verify-fail 风暴）。

> ⚠️ **已知风险（如实说明）**：原项目 anon 模式依赖 Chrome TLS 指纹伪装
> （bogdanfinn/tls-client），Workers 沙箱无法复刻。本移植的 anon 请求经代理隧道出站时
> TLS 握手由纯 JS TLS 1.3 客户端完成（ClientHello 非浏览器指纹）；无代理时回退
> `fetch()`（Cloudflare TLS 栈）。**Google 若加强匿名端点指纹校验，anon 模式可能被拒**
> —— 届时面板切换 `official` 模式即可（一键切换，立即生效）。

### official 模式（单 Key 直连）

- 所有请求（对话/绘图/TTS/计数）改走官方 API，`x-goog-api-key` 鉴权，无需 reCAPTCHA。
- 支持镜像/中转基地址（`gemini_base_url`，裸域名自动补 `/v1beta`）。
- 代理池照常生效：上游按 IP 限速时，429 让该节点进入冷却并自动接力下一个节点
  （每个代理 IP 独立窗口，轮转即解）。
- 切换方式（三选一，保存即时生效）：
  1. **管理面板**：配置页「上游通道模式」下拉框
  2. **配置 API**：`POST /admin/config`，body 带 `"apimode": "official"` 或 `"anon"`
  3. **环境变量**：`wrangler secret put API_MODE`（`official`/`anon`；KV 配置优先）

## 🌐 代理池配置

- **订阅链接**（推荐）：面板「代理」页填入订阅地址（`type://host:port` 纯文本或整段
  Base64），按 `subscription_refresh_minutes` 惰性刷新，KV 缓存跨 isolate 共享。
- **静态列表**：面板逐条添加，或 `wrangler secret put PROXY_URLS`（逗号分隔）。
- 支持协议：`socks4://`、`socks4a://`、`socks5://`、`http://`；
  `https://` 及 vmess/vless/ss 等 **Workers 无法 TLS-in-TLS，入口自动剔除并计数**。
- 出站链路：`cloudflare:sockets` TCP → SOCKS/HTTP CONNECT 握手 → **纯 JS TLS 1.3**
  （X25519 + AES-128-GCM + HKDF 全走 WebCrypto；GTS 根证书链 + SAN +
  CertificateVerify 完整校验，MITM 节点直接拒绝）→ HTTP/1.1（keep-alive 连接复用池）。
- **Smart Placement 已在 wrangler.jsonc 默认开启**：Worker 执行点自动迁移到 Google 附近，
  直连出口不再被 "User location is not supported" 拦截（需少量真实流量供 CF 完成画像）。

## ⚙️ 环境变量（wrangler secret / vars）

| 变量 | 必填 | 说明 |
|------|------|------|
| `API_KEYS` | 推荐 | 客户端鉴权 Key（逗号分隔）；KV 配置优先 |
| `ADMIN_TOKEN` | 推荐 | 管理端点 token；未设置时首次启动日志输出随机值 |
| `API_MODE` | 否 | 上游通道模式 `anon`（默认）/ `official` |
| `GEMINI_API_KEY` | official 模式 | 官方 Gemini API Key |
| `GEMINI_BASE_URL` | 否 | official 模式镜像/中转基地址 |
| `PROXY_URLS` | 否 | 静态代理列表（逗号分隔） |

> 敏感信息一律用 `wrangler secret put`（不要写进 wrangler.jsonc 的 vars）。

## 📁 目录结构

```
vertex-master-cf/
├── wrangler.jsonc            # Worker 配置（KV / Smart Placement / Cron）
├── src/
│   ├── index.ts              # 入口（fetch + scheduled）+ 服务信息
│   ├── router.ts / gate.ts   # 业务路由表 / 并发与体积闸门（仅出站端点占名额）
│   ├── config.ts / types.ts  # KV 配置（含 apimode）+ 类型
│   ├── upstream.ts           # 上游胶水层：callGemini 按 apimode 分流
│   ├── bounded.ts            # 有界并发原语（Images n 张并行生成）
│   ├── anon/                 # ★ anon 匿名端点（本次移植核心）
│   │   ├── payload.ts        #   batchGraphql 壳 + Chrome 头 + x-browser-validation
│   │   ├── recaptcha.ts      #   anchor/reload token 现抓（经代理或直连）
│   │   ├── streamparse.ts    #   拼接 JSON 对象流扫描 + 解包 + protobuf 清洗 + 聚合
│   │   └── upstream.ts       #   候选接力（token/IP 同源）→ Gemini 原生 Response
│   ├── egress/               # ★ 出站代理层（借鉴 cf-vproxy 设计重写）
│   │   ├── sockets.ts        #   cloudflare:sockets TCP 封装 + 超时
│   │   ├── handshake.ts      #   SOCKS4/4a/5 + HTTP CONNECT 握手
│   │   ├── tls13.ts          #   纯 JS TLS 1.3 客户端（PSK 会话恢复）
│   │   ├── httpio.ts         #   HTTP/1.1 编解码 + chunked + 响应体流
│   │   ├── connpool.ts       #   keep-alive 连接复用池
│   │   ├── pool.ts           #   代理条目解析/清洗 + 订阅拉取缓存
│   │   ├── racefetch.ts      #   对冲竞速/接力/429 分类/熔断兜底
│   │   └── modelfetch.ts     #   官方 ListModels 分页拉取
│   ├── convert/              # OpenAI/Anthropic ⇄ Gemini 转换
│   ├── handlers/             # 各协议端点 + admin + panel（含模式切换 UI）
│   └── racing.ts / cron.ts / usage.ts / metrics.ts / logs.ts / ...
└── test/                     # node --test 单测（107 例，纯逻辑全量回归）
```

## 🧪 开发与验证

```bash
npm run typecheck   # tsc --noEmit
npm test            # node --test（不依赖 Workers 运行时）
npm run dev         # wrangler dev 本地调试（cloudflare:sockets 走远程默认）
npm run deploy      # 部署
```

## ⚡ Cloudflare Workers 平台限制与调优

本项目针对 Workers 平台做了如下适配与优化，理解这些约束有助于选择合适的付费计划与配置：

| 平台约束 | 数值（免费 / 付费） | 本项目的应对 |
|----------|--------------------|--------------|
| 每请求子请求预算 | 50 / 1000 | 模型表、配置、订阅缓存均走 isolate 内存缓存（30–60s TTL），热路径零 KV 读；anon 接力上限 3 候选 × 4 尝试，控制在预算内；巡检 batch_size ≤ 40；Images n 张并发窗口 3（n=8 仅 8 个子请求，远低于限额） |
| 每请求 CPU 时间 | 10ms / 30s | official 模式（fetch 直连）免费计划即可；anon 模式走代理需纯 JS TLS 1.3 握手（密码学运算经 WebCrypto 原生加速），**建议 Workers Standard 付费计划** |
| isolate 内存 | 128MB | 并发闸门名额在读 body 前获取，在飞大体积缓冲受 max_concurrent_requests 约束；recaptcha/上游响应 4MiB 上限整读 |
| KV 写入额度 | 1000 次/天 | 用量/指标/健康度全部「内存累积 + 25s 批量合并刷盘」，常量级写入 |
| 同时 TCP 连接 | 6 条/请求 | keep-alive 连接池暖连接复用（跳过 TCP+代理+TLS 握手），超限自动降级冷路径 |
| 冷启动 | — | wrangler `minify` 开启（包体 431→276 KiB，gzip 83 KiB）+ 内置模型表惰性加载 + PSK 会话恢复减少重复握手 |
| 地理限制（Google 侧） | — | Smart Placement 开启，执行点自动迁移到 Google 后端附近（需少量真实流量画像生效） |

**付费计划选择建议**：仅用 official 模式 → 免费计划足够；要用 anon 模式 + 代理池 → 推荐 $5/mo Workers Standard（30s CPU 预算可覆盖纯 JS TLS 握手与多候选接力）。

## ⚠️ 与原项目（Go 版）的差异

**v1.2.0 已逐行比对 Go 版路由表与配置结构补齐**：`/health`、`/keepalive`、`/api/hello`（Claude Code 网关探测）、`/favicon.ico`、`GET /v1beta/models/{model}`（Gemini 格式模型详情）、`safety_settings` 配置与 safetySettings 归一化（每个 anon 出站请求恒携带 5 类阈值，语义同 Go 版 `BuildVertexVariables`）。

**v1.3.0 第二轮比对再补齐**：Anthropic 流式协议级 `event: ping`（Go `emitPing`，15s）、`levelToBudget` HIGH → 24576（Go `budgetForThinkingLevel`）、示例密钥 `sk-your-key-here` 拒绝（Go `withAPIKey`）、并发闸门豁免模型列表端点（Go `isUpstreamWorkloadPath`）、Images n 张有界并发生成（超出 Go 串行行为的 Workers 优化）。

剩余差异（平台本质限制或面板 UX 取舍，不影响核心代理功能）：

| 能力 | 原项目 | 本移植 | 原因 |
|------|--------|--------|------|
| anon 匿名端点 | ✅（Chrome TLS 指纹） | ✅（非浏览器 TLS 指纹，存在被拒风险） | Workers 沙箱无法伪装 TLS 指纹 |
| official 直连 | ✅ | ✅ | — |
| 代理池 socks/http | ✅ | ✅ | cloudflare:sockets 隧道 |
| mihomo 节点（vmess/vless/ss） | ✅ | ❌ 自动剔除 | Workers 无法 TLS-in-TLS |
| TLS 指纹伪装 | ✅ | ❌ | 同上 |
| SQLite 持久化 | ✅ | KV 快照（健康度/用量/日志） | Workers 无本地盘 |
| 图片生成 imagen/veo（:predict） | official 直连 | ✅ official / ❌ anon | 匿名端点无 predict 操作签名 |
| `request_timeout` 配置 | ✅（上游会话超时可配） | 固化常量（连接/握手/TLS/响应头分级超时） | Workers 分级超时已覆盖语义 |
| `max_spill_mb`（磁盘 spool） | ✅ | ❌ | Workers 无磁盘；流式处理不落盘 |
| 节点批量管理 UX（导入/排序/批量启停/测速进度） | ✅（面板内建） | 池管理走配置 + 订阅 + 巡检 | 面板为独立实现，核心池能力等价 |
| 面板外观自定义（背景图/字体等） | ✅ | ❌ | 装饰性功能 |
| `debug_pprof` / `debug_mode` | ✅ | ❌（Dashboard 观测日志替代） | Go 特有调试设施 |
| 部署形态 | VPS/Docker/Termux | Cloudflare Workers（official 免费 / anon+代理池建议 Standard） | — |

## 📜 许可证

PolyForm Noncommercial License 1.0.0（与上游 vertex-master / cf-vproxy 一致）。
anon 端点常量与转换层语义移植自 vertex-master（Go）；Workers 适配架构与协议转换层
基于同作者 cf-vproxy 迭代；出站代理层按其设计重写。
