# 改造计划:双后端(Codex 原生 + OpenAI 兼容)

> 状态:已实现核心功能 | 目标分支:`dev` | 关联 docs:`architecture.md`

## 实现进度(更新于本次迭代)

已完成:
- [x] `config.rs`:`BackendKind` + `OpenAIBackendConfig` + TOML/`.env` 双源解析
      (`openai_backend_from_toml` / `openai_backend_from_env_file`,含 `${ENV}` 展开与
      `OPENAI_API_KEY` / `DEEPSEEK_API_KEY` 回落)。
- [x] `backend/openai.rs`:Grok Responses → OpenAI ChatCompletions 变换
      (instructions→system、input 项→messages、function/x_search tool 映射) + SSE 桥接
      (content delta、tool_calls 累积、`response.completed` 收尾)。
- [x] 复用 `CodexSseTransformer` 生成终端 `output` 数组;命中统计支持
      `usage.prompt_tokens_details.cached_tokens`(`extract_cache_usage` 扩展)。
- [x] 分发:`proxy_responses` 按 `openai_backend.is_some()` 分流,`/health` 上报 `backend`。
- [x] openai 模式不注入 `prompt_cache_key`(隐式前缀缓存),丢弃 `x_search`。
- [x] 测试:`tests/openai_backend.rs`(端到端文本/工具桥接、默认 model)+ 模块单测。
      `cargo test --workspace` 与 `cargo clippy --workspace --all-targets` 均绿。

已完成(第二次迭代):
- [x] 将 codex 相关逻辑(WebSocket 池/401 刷新/prompt_cache_key/请求变换)抽到
      `backend/codex.rs`,`handle_codex` 成为 codex 入口;共享壳(鉴权/并发/诊断/SSE)留在
      `lib.rs`。行为未变,现有测试全绿。
- [x] CLI 接线:daemon 从 `proxy.toml`(或 `backend.env`)读取并注入 `with_openai_backend`;
      配置到 openai 模式时跳过 Codex model 白名单与 OAuth reasoning clamp。
- [x] openai 模式 web_search:**统一禁用**(`x_search` 直接丢弃,不做白名单)。

已完成(第五次迭代 — 多 provider,按模型名前缀路由):
- [x] `OpenAIBackends { default, named }`;`[backend.openai]` 为默认/兜底,
      `[backend.openai.providers.<name>]` 支持任意 N 个命名 provider。
- [x] 路由:`select_openai_backend(model)` 最长前缀匹配命名 provider → 默认 → 单命名兜底;
      `transform_to_chat_completions` 剥前缀后发上游(`opencode/deepseek-v4-flash`→`deepseek-v4-flash`)。
- [x] `models sync` 遍历全部 provider,命名 provider 模型 id 加前缀,alias 不冲突。
- [x] 日志带 `provider="<name>"`;`/health` 仍上报 `backend=openai`。
- [x] 测试:配置解析(N 个 provider/前缀缺省/缺 key)、前缀剥离、双上游路由集成。

配置入口(`~/.grok/codex-oauth/proxy.toml`):

```toml
[backend.openai]
base_url = "https://api.deepseek.com/v1"
api_key = "${DEEPSEEK_API_KEY}"   # 或字面量;缺省回落 OPENAI_API_KEY / DEEPSEEK_API_KEY
model = "deepseek-v4-flash"       # 默认模型;未知请求 model 回落到它
thinking = true                   # DeepSeek thinking/推理模式;false 关闭

# 按模型上下文窗口,`models sync` 时写入 Grok 模型条目;未配置留 Grok 默认(272K)
[backend.openai.model_context_windows]
"deepseek-v4-flash" = 1048576     # 1M
"deepseek-v4-pro" = 1048576
```

或 `backend.env`:`OPENAI_BASE_URL` / `OPENAI_API_KEY` / `OPENAI_MODEL` / `OPENAI_HEADER_<NAME>`(env 不支持 per-model 上下文/thinking,也不支持命名 provider)。

### 多 provider(按模型名前缀路由,N 个可扩展)

`[backend.openai]` 是**默认/兜底** provider;额外的命名 provider 放在
`[backend.openai.providers.<name>]` 下,想加几个加几个:

```toml
[backend.openai]                       # 默认/兜底 provider(deepseek)
base_url = "https://api.deepseek.com/v1"
api_key = "${DEEPSEEK_API_KEY}"
model = "deepseek-v4-flash"
thinking = true
reasoning_efforts = ["low", "high", "max"]
default_reasoning_effort = "high"
[backend.openai.model_context_windows]
"deepseek-v4-flash" = 1048576
"deepseek-v4-pro" = 1048576

[backend.openai.providers.opencode-zen]   # 命名 provider:opencode zen
base_url = "https://opencode.ai/zen/v1"   # 注意:不要带 /chat/completions 后缀
api_key = "${OPENCODE_ZEN_API_KEY}"       # 命名 provider 必须自带 key,不回落 OPENAI_*
model = "opencode/deepseek-v4-flash"      # 默认模型(带前缀)
model_prefix = "opencode/"                # 路由前缀;省略时默认 "{name}/"
thinking = true
reasoning_efforts = ["low", "high", "max"]
default_reasoning_effort = "high"
[backend.openai.providers.opencode-zen.model_context_windows]
"opencode/deepseek-v4-flash" = 1048576
"opencode/deepseek-v4-pro" = 1048576
```

路由规则:
- 请求 `model` 按**最长前缀**匹配命名 provider 的 `model_prefix` → 命中者;剥掉前缀后发往上游
  (`opencode/deepseek-v4-flash` → zen 收到 `deepseek-v4-flash`)。
- 无匹配 → 默认 provider(`[backend.openai]`)。
- 无默认且只有一个命名 provider → 兜底到它(单 provider 简易模式)。
- 无默认、多个命名、且无前缀匹配 → 报错。

`models sync` 会遍历全部 provider,命名 provider 的模型 id 统一加前缀
(`opencode/deepseek-v4-flash`),alias 自动不冲突
(`codex-deepseek-v4-flash` vs `codex-opencode-deepseek-v4-flash`)。日志带
`provider="<name>"` 便于区分。

### Token 用量上报(累计总 token,不每次清零)

Grok 的上下文计数(上下文栏 `⇣Nk` / `/context`)用 `response.completed.usage.total_tokens`
**覆盖**显示值(不累加),所以桥接端必须保证:

- 每个请求只发**一个** `response.completed`(修复前 finish chunk 内联发一次、
  `finish()` 再发一次,第二次因 `pending_usage` 已被 metrics 抽走而报 0 → 计数器每次清零)。
- `total_tokens` = 本轮 `input + output`(完整上下文重发,随对话单调增长 → 呈现"累计")。
- `output_tokens` / `output_tokens_details.reasoning_tokens` 从上游 usage 真实读取
  (修复前硬编码 0)。
- 上游请求带 `stream_options: { include_usage: true }`,并兼容 usage 在 finish chunk
  (DeepSeek)或末尾空 choices chunk(opencode zen)两种位置。
- 传输错误时先 flush `state.finish()` 再报错,避免模型已完成后断流导致客户端等不到完成事件。

`/usage` 会话记账仍由客户端逐条累加 `input_tokens`/`output_tokens`,此处保持每轮真实值。

已完成(第三次迭代 — DeepSeek 端到端):
- [x] DeepSeek 缓存命中接入监控:openai 后端捕获 ChatCompletions 流式 usage 尾块
      (`usage.prompt_tokens` + `prompt_tokens_details.cached_tokens`),`cache_hit_ratio_pct` 实测 98%+。
- [x] Grok 解码兼容:桥接事件补齐 `sequence_number`、终端 `response.completed` 补全
      `created_at`/`model`/`status`/`usage`(含 `total_tokens`)及 Responses 标准字段。
- [x] DeepSeek 推理透传:桥接捕获 `delta.reasoning_content` 以 `reasoning` 输出项回发,
      转换端把 Grok 回传的 `reasoning` 项映射回 assistant `reasoning_content` → thinking 模式
      工具循环不再 400。
- [x] 工具/输出项事件补 `item_id`(`output_item.added`/`function_call_arguments.delta|done`)。
- [x] `thinking` 可配置(`true`/`false` 发 `thinking:{type:...}`)。
- [x] 未知模型回落:有已同步模型列表时,`grok-4.5` 等内部模型映射到 `default_model`。
- [x] CLI `models sync` 支持 openai 后端:fetch `{base_url}/models` 写 Grok 模型条目
      (含 per-model `context_window`)。

已完成(第四次迭代 — DeepSeek 推理 + TUI 显示):
- [x] Grok 只给"推理能力"模型显示 thinking UI:sync 时把配置的 `reasoning_efforts` /
      `default_reasoning_effort` 写入模型条目(`supports_reasoning_effort = true`)。
- [x] 流式推理显示:桥接把 DeepSeek 的 `reasoning_content` 实时转成
      `response.reasoning_text.delta`(含 `sequence_number`/`item_id`/`output_index`/
      `content_index`)发回;结尾再发终端 `reasoning` 项。Grok TUI 显示 `▲` 思考折叠块。
- [x] DeepSeek 确实产 reasoning_content(强推理问题 5850 增量),TUI 已验证显示。
- [x] 排除并发干扰:移除 Grok 会话对 `config.rs` 的多 provider 重构,只保留本项目改动。

遗留(后续按需):
- [ ] reasoning 内容若为 Grok 加密(`reasoning.encrypted_content`)时无法透传(当前走明文 `content`/`summary`)。
- [x] 多 provider(多个命名 OpenAI 兼容后端 + 选择键)已实现:见下方「多 provider(按模型名前缀路由)」。

## 1. 背景与目标

当前 `codex-proxy` 是单一后端:上游固定指向 ChatGPT/Codex OAuth Responses API(支持 WebSocket
增量),凭据走 OAuth keyring。用户希望提供**两种可切换的后端形式**,统一通过 TOML 或 `.env`
配置 `base_url` / `api_key` 等:

| 后端 | 上游协议 | 传输 | 凭据 | 缓存 | Web search |
|------|----------|------|------|------|------------|
| `codex`(现有) | Responses API | WebSocket(增量)+ HTTP/SSE 回退 | OAuth 会话 | 显式 `prompt_cache_key` | 通过 OAuth 后端 |
| `openai`(新增) | OpenAI Chat `/chat/completions` | HTTP/SSE | 静态 `api_key` + `base_url` | 隐式前缀缓存 | provider 特定或禁用 |

关键差异(参考 opencode 的实现):
- opencode 中 `openai-compatible` 直接复用 OpenAI Chat 协议,只覆盖 `baseURL` + Bearer auth;
  缓存策略明确"OpenAI 系 = 隐式前缀缓存",跳过显式 marker;只有 Responses 路径才写
  `prompt_cache_key`(config 开关 `setCacheKey`)。本改造沿用同一分界线。

## 2. 现状分析

`crates/codex-proxy/src/lib.rs`(2465 行,单文件)职责混叠:

- 通用壳:`build_router` / `/health` / 本地 secret 鉴权 / 并发信号量 / body limit /
  model 白名单 / `RequestDiagnostics`(日志)/ `CompletionMetrics`(缓存命中统计)/
  SSE 帧桥接 / 错误归一化。
- Codex 特定:Groki→Codex 请求变换(`codex-protocol`)、`resolve_upstream_request_ids`
  注入 `prompt_cache_key`、OAuth 凭据、WebSocket 复用池、HTTP/SSE 重试与 401 刷新。

`codex-protocol` 已经封装了 Responses 变换与 SSE 适配,是可复用的下界;`codex-proxy` 是
需要拆的主战场。

## 3. 目标架构

分层:共享壳在上,后端适配在下,中间用 trait 隔离。

```text
Grok Responses client
      |  POST /v1/responses + 本地随机 bearer
      v
┌──────────────────── codex-proxy 共享壳 ───────────────────┐
│ HTTP 路由 / /health / 鉴权 / 并发 / body limit / model 白名单 │
│ RequestDiagnostics + CompletionMetrics + SSE 帧桥接         │
│  session/cache key 解析(共享)                             │
└───────────────┬────────────────────────────────────────────┘
        Backend 分发(enum: Codex | Openai)
        v
┌─────────────────────── UpstreamBackend trait ──────────────┐
│  authorize() -> 凭据(OAuth 或静态 key)                     │
│  prepare(request, ids) -> 线格式字节                        │
│  send(prepared, auth) -> SSE 字节流                         │
│  cache_policy() -> 显式 or 隐式(仅 codex 注入 key)          │
└──────────┬────────────────────────────────┬─────────────────┘
   Codex 后端                          OpenAI 兼容后端
   - 变换→Responses                     - 变换→Chat/completions
   - OAuth + WebSocket/SSE              - 静态 key + SSE
   - 注入 prompt_cache_key              - 不注入(隐式前缀)
   - web_search 走 OAuth                - 禁用或 provider 特定
```

## 4. 代码拆分(目标目录)

`crates/codex-proxy/src/`:

```
lib.rs           共享壳:build_router/health/proxy_responses(变薄)/鉴权/并发/
                diagnostics/SSE 帧桥接/错误/常量 → 仅保留
backend/mod.rs   `Backend` enum + `UpstreamBackend` trait + 分发工厂
backend/codex.rs 现有 Codex 逻辑迁移:变换、prompt_cache_key、OAuth、
                WebSocket 池、401 刷新、web_search 路由
backend/openai.rs 新增:ChatCompletions 变换、静态 key、SSE、缓存统计直读
config.rs        `BackendConfig`(TOML/.env)解析与校验
protocol.rs      泛化请求变换入口(按后端选择)
```

`codex-protocol` 保持不变(继续充当 Responses 适配器)。新增一个轻量
`chat-completions` 映射(建议放 `backend/openai.rs` 内,若复用度高再抽 crate)。

### 共享壳(抽出,不变量最大)

以下必须留作单一实现,两个后端共用,避免重复:

- `build_router`、`/health`、本地 secret 校验、并发 semaphore、`DefaultBodyLimit`、model 白名单
- `RequestDiagnostics`(请求收到/响应头/失败/完成日志)、`CompletionMetrics`、
  `extract_cache_usage`(输入与 cached 读取,支持两种路径的字段)
- `UpstreamRequestIds` 与 session/cache key 解析(共享;是否落到 wire 由后端决定)
- `error_response` / `sanitize_error_body` / `redact_sensitive_fields` / header 拷贝

### `UpstreamBackend` trait(草案)

```rust
#[async_trait]
pub trait UpstreamBackend: Send + Sync {
    fn kind(&self) -> BackendKind;

    // 凭据解析:OAuth access_credential() 或静态 api_key
    async fn authorize(&self) -> Result<UpstreamAuth, BackendError>;

    // Grok Responses 请求 → 后端线格式;返回可 POST 的字节与目标 URL。
    // 仅 Codex 在此注入 prompt_cache_key。
    fn prepare(
        &self,
        request: &Value,
        headers: &HeaderMap,
        ids: &UpstreamRequestIds,
    ) -> Result<PreparedRequest, BackendError>;

    // 发送并返回统一 SSE 字节流(Codex 走 WebSocket/SSE,OpenAI 走 SSE)。
    async fn send(
        &self,
        prepared: PreparedRequest,
        auth: &UpstreamAuth,
        client: &reqwest::Client,
    ) -> Result<BoxStream<'static, Result<Bytes, BackendError>>, BackendError>;

    // 缓存记账:是否需要显式 key(仅 Codex 需要)。
    fn explicit_cache_key(&self) -> bool { false }
}
```

`PreparedRequest { url: Url, bytes: Vec<u8>, cache_key: Option<String> }`。
`proxy_responses` 收敛为:鉴权→并发→解析→按 `Backend` 分发 `prepare`→`authorize`→`send`
→统一走 SSE 帧桥接与 diagnostics。WebSocket 复用池/401 刷新/回退全留在 Codex 后端内部。

## 5. 配置设计

新增 `config.rs`,统一 TOML 或 `.env` 双源(值优先级:进程 env > .env > toml 默认)。

### TOML 示例 `~/.grok/codex-oauth/proxy.toml`

```toml
[proxy]
port = 1456
backend = "openai"          # codex | openai
local_secret = "random-secret"   # 缺省自动生成

[backend.codex]
# 沿用现有 OAuth/keyring,无需额外字段(可选覆盖上游仅限测试)

[backend.openai]
base_url = "https://api.deepseek.com/v1"
# api_key 支持字面量或 ${ENV_VAR} 引用;缺省回落到 OPENAI_API_KEY / DEEPSEEK_API_KEY
api_key = "${DEEPSEEK_API_KEY}"
model = "deepseek-chat"     # 默认模型(可被请求内 model 覆盖)
extra_headers = { "HTTP-Referer" = "...", "X-Title" = "grok" }
```

### `.env` 等价形式

```
PROXY_PORT=1456
PROXY_BACKEND=openai
OPENAI_BASE_URL=https://api.deepseek.com/v1
OPENAI_API_KEY=sk-...
OPENAI_MODEL=deepseek-chat
```

`ProxyConfig` 保留为共享基础;新增 `BackendConfig` 负责后端差异字段并做校验
(openai 必须有 base_url;api_key 缺失时报清晰错误并提示 env 名)。

## 6. 缓存策略

- **Codex**:保持现状,`prepare` 内注入 `prompt_cache_key`,`extract_cache_usage` 读
  `/response/usage/.../cached_tokens` → `cache_hit_ratio_pct`。
- **OpenAI**:`explicit_cache_key() == false`,**不注入任何显式 key**;命中依赖:
  - 固定 system prompt(guidelines/instructions 序列化稳定);
  - 固定 tool 定义顺序(遍历排序稳定,不依赖 HashMap 随机序);
  - 跨轮复用完整 messages 数组(不截断、不重排)。
  - 从 `usage.prompt_tokens_details.cached_tokens` 读取统计(deepseek 支持;opencode
    视情况),拿不到则 `cache_usage_present=false`。
- 结论:OpenAI 模式的 `cached_input_tokens`/`cache_hit_ratio_pct` 监控继续可用,但触发是
  "前缀稳定" 而非 "会话 key",README 需注明。

## 7. Web search

- **Codex**:现有 OAuth 路由不变。
- **OpenAI**:Chat `/chat/completions` 无标准 `web_search` tool。默认**禁用**并返回明确
  的 `unsupported` 错误;若目标 provider(如 opencode API)暴露 OpenAI 风格
  `web_search_options` 或 search tool,作为 provider 特定能力在 `backend/openai.rs`
  按 base_url 白名单开启。不做通用 web_search 适配。

## 8. 迁移步骤(分阶段,每阶段可合并、可回滚)

### 阶段 1 — 抽出共享壳(纯重构,行为不变)
- [ ] 将鉴权/并发/body limit/model 白名单/`RequestDiagnostics`/`CompletionMetrics`/
      `extract_cache_usage`/SSE 帧桥接/错误辅助移入共享模块,保持 `build_router` 签名兼容。
- [ ] 现有测试全绿(`cargo test --workspace`),确认零行为变化。

### 阶段 2 — 引入 `UpstreamBackend` trait + Codex 实现
- [ ] 定义 trait、`Backend` enum、`PreparedRequest`/`UpstreamAuth`/`BackendError`。
- [ ] 把现有 WebSocket/SSE/OAuth/401 刷新/prompt_cache_key 逻辑迁到 `backend/codex.rs`,
      `proxy_responses` 改为经 trait 分发。
- [ ] 测试依旧全绿,作为回归基线。

### 阶段 3 — OpenAI 兼容后端 + 配置
- [ ] `config.rs` 解析 TOML/.env,`Backend` 工厂按 `backend` 选择。
- [ ] `backend/openai.rs`:Grok→ChatCompletions 变换(系统提示/tools/messages/stream)、
      静态 key、SSE 消费、缓存统计直读、model 覆盖。
- [ ] 用 mock 上游写集成测试:单轮/多轮工具循环、缓存命中字段、错误归一化。

### 阶段 4 — 收尾
- [ ] web_search 在 openai 模式的禁用/白名单逻辑 + 文档。
- [ ] 更新 `architecture.md`、README(双后端对比表、配置示例)。
- [ ] 端到端联调:`grok-codex-oauth` 新增 `--backend` 入口与 `ensure-daemon` 生效。

## 9. 测试策略

- 沿用现有 `for_test` loopback 注入缝:新增 openai 后端的 mock `/chat/completions`。
- 关键用例:
  - Grok Responses → ChatCompletions 字段映射(instructions→system、tools、tool_result、
    reasoning 折叠、stream=true)。
  - 缓存:重复请求时 `cached_tokens` 传递;openai 模式不产生 `prompt_cache_key`。
  - 错误:401(静态 key 不刷新,直接归一化)、超时、SSE `[DONE]`。
  - 并发/白名单/body limit 在两个后端下保持一致。
- 既有 WebSocket 增量、401 刷新、会话复用测试全部保留,确保 Codex 路径无回归。

## 10. 风险与兼容性

- **风险**:ChatCompletions 表达能力弱于 Responses(无原生 web_search、reasoning 字段差异),
  Grok 部分能力(增量/后台事件)在 openai 模式不可用 → 需在文档与错误提示中明确。
- **风险**:`.env`/TOML 双源解析优先级需测试覆盖,避免 api_key 泄漏进日志
  (沿用 `redact_sensitive_fields`)。
- **兼容**:`ProxyConfig` 默认仍是 `codex`,现有用户零配置迁移;新后端 opt-in。
- **边界**:`codex-protocol` 不动;若未来需要第三个后端,复用同一 trait,成本最低。
