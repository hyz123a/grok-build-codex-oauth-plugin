# 改造计划:OpenAI 兼容层硬化

> 状态:**已实现**(1/2/3/6/7)| 关联 docs:`plan-openai-backend.md`、`architecture.md`、`tdd.md`、`openai-provider-matrix.md`

## 已实现(本次迭代)

- [x] **2 compat 标志**:`[backend.openai.compat] { stateless, supports_reasoning_effort,
      max_tokens_field }`(TOML + `OPENAI_COMPAT_*` env);默认值等价 DeepSeek 现状。
      `stateless = false` 保留 `previous_response_id`;`supports_reasoning_effort = false`
      跳过 effort 写入/钳制(`codex-protocol::TransformOptions`)与 CLI `models sync`
      的 reasoning 广告。`include` 恒剥(Grok 私有字段)。
- [x] **1 wire_format**:`wire_format = "chat-completions" | "responses"` 显式化;
      `responses_api` 保留为兼容别名(未设置时按它推断);非法值报
      `ConfigError::InvalidEnumValue`(TOML 与 `.env` 均报错)。
- [x] **3 缓存扩展**:`cache_policy = "implicit" | "explicit"`;`explicit` + Responses 模式
      注入 `prompt_cache_key`(请求体 key 优先 → `x-grok-session-id` → 回退 implicit 并 warn);
      `stable_request_prefix_fingerprint`(指令 + 工具集合,canonicalized、乱序不敏感)落库并在
      openai 诊断日志输出 `stable_prefix_fingerprint=` 供跨轮对比;单测锁定确定性/乱序稳定性。
- [x] **6 Chat 模式补强**:并发工具调用集成测试锁定(单 chunk 双 `tool_calls` → 双
      `output_item.added` + 终端 output);`max_output_tokens` 按 `compat.max_tokens_field`
      转发;usage 缺失**保守处理**(保留 0,`warn` + `usage_estimated=true`,不估算);
      新增 `docs/openai-provider-matrix.md` 能力矩阵(README 挂链)。
- [x] **7 成本可见性**:`[backend.openai.cost] { input, output, cache_read, cache_write }`
      (TOML + `OPENAI_COST_*`);完成日志 `estimated_cost_usd=`(cached 按 cache_read
      单价计),**Chat 桥与 Responses 桥(`stream_response`)都接入**,codex 路径传 `None`;
      `/health` 新增 `providers` 列表(wire_format / cache_policy / cost),codex
      模式返回 `[]`(`tests/proxy.rs` 精确 JSON 断言已同步)。
- [x] `cargo test --workspace` 全绿(150 tests)、`cargo clippy --workspace --all-targets` 无警告。

## 背景

`plan-openai-backend.md` 已落地"双模式(Chat 翻译 + Responses 直通)"。对照主流 agent
(opencode / pi / reasonix)的实现,当前 openai 兼容层有五个可改进点,本文按选定顺序给出
实现方案。编号沿用讨论时的条目号(**1 线格式抽象 / 2 compat 标志 / 3 缓存扩展 / 6 Chat
模式补强 / 7 成本可见性**;4 重试与 fallback、5 运行时模型发现本轮不做)。

**总原则:所有新增配置默认值必须等价于现状(DeepSeek 行为),现有测试全绿,零行为变化。**
测试优先级遵循 `tdd.md`:公开 HTTP/SSE 字节契约 > 配置幂等 > 内部 helper。

---

## 1. 线格式显式抽象(wire format)

### 现状

- `OpenAIBackendConfig.responses_api: bool`(`config.rs` L50),`handle_openai` 据此二选一
  (`backend/openai.rs` L44–48):`false` → Chat 翻译、`true` → Responses 直通。
- `.env` 键:`OPENAI_RESPONSES_API`(`config.rs` L237)。
- 隐含分支,新增第三种线格式(如 anthropic-messages)没有扩展点。

### 目标

显式 `wire_format` 字段,`responses_api` 降级为兼容别名:

```toml
[backend.openai]
wire_format = "responses"   # "chat-completions" | "responses"
```

```text
.env: OPENAI_WIRE_FORMAT=responses
```

### 改动

- `config.rs`:
  - 新增 `pub enum WireFormat { ChatCompletions, Responses }`(Derive Clone/Copy/Debug/PartialEq)。
  - `OpenAIBackendConfig` 增加 `pub wire_format: WireFormat`,删除或保留 `responses_api`
    (建议保留为派生字段,避免大范围调用点改动)。
  - `RawProviderFields` / `RawOpenAIConfig` 增加 `wire_format: Option<String>`;
    `resolve_provider` 解析映射,规则:
    - `wire_format` 合法值 `chat-completions` / `responses`;非法值 → 新增
      `ConfigError::InvalidWireFormat { value: String }`(带清晰错误信息,列出合法值)。
    - 未设置时回退 `responses_api == true` → `Responses`,否则 `ChatCompletions`
      (旧配置行为不变)。`responses_api` 文档标注 deprecated。
  - `.env` 增加 `OPENAI_WIRE_FORMAT`(非法值报错或忽略?与 TOML 一致报错更稳,但 .env
    解析无错误通道之外的路径——沿用现有"未知键忽略"策略会静默丢配置;建议非法值返回
    `Err`,在 `openai_backend_from_env_file` 加一个 `ConfigError` 分支)。
- `backend/openai.rs`:`handle_openai` 的 `if config.responses_api` 改为
  `match config.wire_format { WireFormat::ChatCompletions => …, WireFormat::Responses => … }`。
- `backend/mod.rs`:为未来第三种线格式留注释性分发说明(不实现)。

### 测试

- `config.rs` 单测:①`wire_format = "responses"` 解析;②未设置 + `responses_api = true`
  仍推断为 Responses(回归);③非法值报 `InvalidWireFormat`;④`.env` 两种键互斥时的优先级
  (`OPENAI_WIRE_FORMAT` 优先)。
- `tests/openai_backend.rs`:现有双模式集成测试不变(回归);新增一个断言 `wire_format`
  TOML 配置走到 `/responses` 端点的用例(复用现有 mock 上游)。

### 风险

- 唯一风险是反序列化兼容;按上面回退规则,现有 TOML/.env 全部原样可用。

---

## 2. provider 兼容标志(compat flags)

### 现状(DeepSeek 假设硬编码)

- `transform_to_responses_api` 无条件 `remove("previous_response_id") / ("prompt_cache_key") /
  ("include")`(`backend/openai.rs` L239–242)。DeepSeek 无状态、不认这些字段;但 OpenAI 官方
  Responses 是**状态化**的,`previous_response_id` 必须保留,否则多轮能力丢失。
- `apply_thinking` 一律用 `reasoning.effort` 语义(`backend/openai.rs` L385–415);不支持
  "不广告 reasoning" 的 provider。
- `grok-codex-oauth-cli/src/main.rs` `sync_openai_models` 无条件把 `reasoning_efforts` 写入
  Grok 模型条目(`supports_reasoning_effort = true`),无论 provider 是否支持。

### 目标

嵌套 `[backend.openai.compat]` 表(参考 pi 的 `compat` 字段),默认值保持现状:

```toml
[backend.openai.compat]
stateless = true                 # false = 状态化 Responses,保留 previous_response_id
supports_reasoning_effort = true # false = 不发送/不广告 reasoning.effort
max_tokens_field = "max_tokens"  # "max_tokens" | "max_completion_tokens"(见第 6 项)
```

```text
.env: OPENAI_COMPAT_STATELESS=1
      OPENAI_COMPAT_SUPPORTS_REASONING_EFFORT=1
      OPENAI_COMPAT_MAX_TOKENS_FIELD=max_completion_tokens
```

`include` 字段是 Grok 私有请求字段,与 provider 语义无关 → 永远剥,不做标志(文档说明)。

### 改动

- `config.rs`:
  - 新增 `#[derive(Clone, Debug)] pub struct CompatFlags { stateless: bool,
    supports_reasoning_effort: bool, max_tokens_field: MaxTokensField }`,默认
    `stateless=true, supports_reasoning_effort=true, max_tokens_field=MaxTokensField::MaxTokens`。
  - `RawProviderFields` / `RawOpenAIConfig` 增加 `compat: Option<CompatRaw>`(嵌套表反序列化);
    `.env` 三个平铺键。
- `backend/openai.rs`:
  - `transform_to_responses_api`:仅当 `compat.stateless` 时 `remove("previous_response_id")`;
    `prompt_cache_key` 的剥/留改由第 3 项缓存策略决定;`include` 仍剥。
  - `apply_thinking`:当 `compat.supports_reasoning_effort == false` 时不写
    `reasoning.effort`(直接 return),保留其他字段。
- `codex-protocol/src/lib.rs`:`TransformOptions` 增加 `supports_reasoning_effort: bool`,
  `transform_responses_request` 的 reasoning clamp 受其门控;`transform_options` 从
  `OpenAIBackendConfig` 透传。
- `grok-codex-oauth-cli/src/main.rs`:`sync_openai_models` 仅当
  `compat.supports_reasoning_effort` 时写入 reasoning 条目,否则跳过(模型条目不广告 thinking)。

### 测试

- `config.rs` 单测:compat 表解析、默认值、`.env` 键。
- `backend/openai.rs` 单测:`stateless=false` 时响应体保留 `previous_response_id`;
  `supports_reasoning_effort=false` 时 `reasoning.effort` 不出现。
- `tests/openai_backend.rs` 集成:mock 上游捕获请求体断言以上两点(现有
  `Captures` 已能拿到请求体)。
- `codex-protocol` 单测:门控后 clamp 行为。

### 风险

- `previous_response_id` 保留后,若 Grok 回传的 id 与上游会话不匹配,上游可能 400——只对
  `stateless=false` 的用户暴露,默认路径不受影响。

---

## 3. 缓存策略扩展

### 现状

- openai 模式一律不注入 `prompt_cache_key`(依赖隐式前缀缓存),Responses 模式还主动剥掉
  (见第 2 项现状)。
- 命中统计:`extract_cache_usage` 读 `usage.prompt_tokens_details.cached_tokens` →
  `cache_hit_ratio_pct`(`lib.rs` diagnostics;`tests/proxy.rs` L1764 断言日志)。
- **无"前缀稳定性"回归测试**:system prompt / tool schema 一旦随请求变化(如工作目录混进
  系统提示),隐式前缀缓存整段失效——opencode 在 llama.cpp 上实测过此问题,reasonix 为此
  专门维护"工具 schema 契约 + 回归审查"。

### 目标

1. 显式缓存 key 注入(为 OpenAI/xAI 这类"无显式 key 不缓存"的 Responses provider 预留):
   `[backend.openai] cache_policy = "implicit" | "explicit"`(默认 implicit)。
   - `explicit` 且 Responses 模式:请求体注入 `prompt_cache_key`,取值优先级:
     请求体已有的 `prompt_cache_key` → 请求头 `x-grok-session-id`(codex 后端已用此头
     派生会话 id,`backend/codex.rs` L339–343)→ 回退 implicit 并打 warn 日志。
   - Chat 模式:`explicit` 不生效(OpenAI Chat 协议无此字段),文档说明仍走隐式。
   - `.env`:`OPENAI_CACHE_POLICY=explicit`。
2. 前缀稳定性回归测试(对齐 reasonix 的 tool schema contract):
   - 已实现:`crate::stable_request_prefix_fingerprint(&request)` 落库于 `lib.rs`
     (指令从 `instructions` 或 Chat 翻译后的 system 消息提取 + 全部 tool 定义,
     canonicalized、工具乱序不敏感的 SHA-256 指纹);openai 诊断日志输出
     `stable_prefix_fingerprint=` 供跨轮对比(前缀漂移监控)。
   - 测试:①同一输入两次指纹相等;②工具定义以不同顺序出现时指纹相等
     (当前实现按数组序保序,HashMap 不参与序列化,此测试防未来回归);③instructions
     或工具内容变化时指纹不同。文档记录"哪些部分必须保持稳定":instructions、tool 定义
     集合、reasoning 默认 effort。

### 改动

- `config.rs`:`cache_policy`(enum `CachePolicy { Implicit, Explicit }`,默认 Implicit),
  透传到 `OpenAIBackendConfig`。
- `backend/openai.rs`:`transform_to_responses_api` 增加 `prompt_cache_key` 注入;
  Chat 翻译路径不动;新增指纹 helper。
- `lib.rs`:`proxy_responses` 解析 `x-grok-session-id` 传入 openai 分支(当前只传了
  `client_request_id`;签名 `handle_openai` 加一个 `session_id: Option<&str>` 参数)。

### 测试

- `backend/openai.rs` 单测:指纹稳定性(同输入、乱序 tools);`explicit` 注入优先级三档。
- `tests/openai_backend.rs` 集成:mock 上游断言 `prompt_cache_key` 出现在请求体;
  `implicit` 时不出现在请求体(回归)。
- `config.rs` 单测:`OPENAI_CACHE_POLICY` 解析与默认值。

### 风险

- `prompt_cache_key` 是 Grok 侧私有的会话标识,注入上游后若 provider 把它当缓存键但不校验
  一致性,无风险;若 provider 校验长度/格式(如 xAI),需在文档标注 provider 特定约束。
- 指纹测试只覆盖"代理侧不破坏前缀";Grok 侧 compaction 不受控(超出本项目边界,文档说明)。

---

## 6. Chat 模式补强 + 能力矩阵

### 现状

- 并发工具调用:连续 `function_call` 项已合并为单条 assistant `tool_calls` 消息
  (`backend/openai.rs` L536–590 `pending_tool_calls` 机制),**但无测试锁定此行为**。
- usage 缺失:provider 不返回 usage 尾块时,`ChatStreamState.terminal_event` 用
  `final_usage.unwrap_or_default()`(`backend/openai.rs` L929–935)→ 全 0,误导 Grok 计数。
- `max_output_tokens` 未转发到 Chat 请求。

### 目标

1. **并发工具调用**:补集成测试锁定现有合并行为(两个连续 `function_call` → 单条 assistant
   `tool_calls` 数组,随后两条 `tool` 结果一一对应)。
2. **usage 缺失保守处理**(已定案):provider 不返回 usage 尾块时,`response.completed` 保持
   `output_tokens = 0`(**不做估算**),`CompletionMetrics.usage_estimated = true` 进诊断日志
   (`usage_estimated=true`),并 `warn` 一次("upstream stream omitted usage")。近似值会误导
   Grok 的上下文计数,故不采用;如需估算可作为后续可选开关。
3. **`max_output_tokens` 转发**:Chat 请求在 Grok 带 `max_output_tokens` 时写入
   `compat.max_tokens_field`(见第 2 项;默认 `max_tokens`)。
4. **能力矩阵文档**:`docs/openai-provider-matrix.md`(已建,README 挂链),行 = 能力
   (web_search / reasoning 透传 / 并发工具 / 显式缓存 key / max_output_tokens / usage 上报 /
   成本估算 / 会话状态),列 = 线格式 × provider 示例,单元格 = 支持情况或配置项名。

### 改动

- `backend/openai.rs`:`ChatStreamState` 增加 `output_chars` 计数与 `estimated_output_tokens()`;
  `terminal_event` 分支;`transform_to_chat_completions` 转发 `max_output_tokens`。
- `lib.rs`:`CompletionMetrics` 增加 `usage_estimated: bool`;`StreamDiagnostics`/openai
  桥接日志透传。
- 新文档 `docs/openai-provider-matrix.md`;README 挂链接。

### 测试

- `tests/openai_backend.rs`:
  - 并发工具循环:mock 上游按 `tool_calls` 数量逐个回 `tool` 结果,断言终端 `output`
    含两条 `function_call_output`(现有测试只覆盖单工具,见 `openai_backend_bridges_function_calls_into_responses_output`)。
  - usage 缺失:mock 上游不发 usage 尾块,断言 `usage_estimated=true` 且 `output_tokens>0`。
  - `max_output_tokens` 转发:请求体断言 `max_tokens`(及 `compat.max_tokens_field`
    = `max_completion_tokens` 时的字段名)。

### 风险

- 近似 output_tokens 会让 Grok 上下文栏略偏;文档明确"缺失时估算"。若 Grok 侧对估算值
  有严格校验(目前没有),可降级为保留 0 + 仅日志,决策点标注。

---

## 7. 成本可见性

### 现状

- 代理侧只有 token 统计与 `cache_hit_ratio_pct`(`lib.rs` diagnostics);无美元成本。
- `/health`(`lib.rs` L506–518)只返回 `status/backend/websocket_proxy/
  system_prompt_policy_fingerprint`;`tests/proxy.rs` L170–194 断言**精确 JSON**——
  任何新增字段都是公开契约变更,必须同步更新该测试。
- Grok 的 `/usage` 记账在客户端(Grok 侧),代理无该路由。

### 目标

1. per-provider 成本表(字段对齐 pi 的 `cost`):

```toml
[backend.openai.cost]
input = 0.27            # 每百万 input token 美元
output = 1.10
cache_read = 0.07       # 命中前缀缓存的 input token 单价
cache_write = 0.27      # 写入缓存的 input token 单价(部分 provider 计)
```

```text
.env: OPENAI_COST_INPUT=0.27 / OPENAI_COST_OUTPUT=1.10
      OPENAI_COST_CACHE_READ=0.07 / OPENAI_COST_CACHE_WRITE=0.27
```

2. 输出:
   - 诊断日志增加 `estimated_cost_usd`(按实际 `input/output/cached` token × cost 表;
     无 cost 表时省略该字段,日志为 `estimated_cost_usd="<none>"`)。
   - `/health` 增加 `providers: [{ name, wire_format, cache_policy, cost }]`(已实现;
     openai 模式返回各 provider,codex 模式返回 `[]`;`tests/proxy.rs` 精确 JSON 断言已同步)。
   - 客户端 `/usage` 记账不动(估算成本由代理日志提供)。

### 改动

- `config.rs`:`Cost` 结构 + `Option<Cost>` 进 `OpenAIBackendConfig`;TOML 嵌套表 + `.env`
  四个平铺键;`RawOpenAIConfig::default_fields` 透传。
- `lib.rs`:
  - `log_completed` 输出 `estimated_cost_usd`;`stream_response`(Responses 桥)新增
    `cost` 参数,`StreamDiagnostics` 采集 `output_tokens`(`extract_output_tokens`)并计算
    成本(codex 路径传 `None`,不输出)。
  - `health` 增加 `providers` 字段;`tests/proxy.rs` 断言同步更新(openai 集成测试断言
    providers 内容)。
- `backend/openai.rs`:Chat 桥(`bridge_openai_sse`)与 Responses 桥(`stream_response`)
  均传 `config.cost`。

### 测试

- `config.rs` 单测:cost 解析、缺省 None、`.env` 四键。
- `tests/proxy.rs`:更新 `/health` 精确 JSON 断言(新增 `providers`);新增 openai 模式
  `/health` 返回 providers 的用例。
- 日志断言:带 cost 表时 `estimated_cost_usd=` 出现且计算正确(如
  `input=1000,cached=750,output=500` × 表中单价)。

### 风险

- `/health` 是公开契约,新增字段前先改测试(红)→ 加字段(绿),符合 tdd.md。
- cost 表是配置输入,算错不崩(缺省 None 即省略日志字段)。

---

## 实施顺序与验收

1. **第 2 项 compat 标志**(纯新增默认值,行为零变化)→ 2. **第 1 项 wire_format**
   (重构 + 别名兼容)→ 3. **第 3 项缓存**(策略 + 指纹回归测试)→ 4. **第 6 项 Chat 补强**
   (测试先行锁定现有并发行为)→ 5. **第 7 项成本**。**已按此顺序完成**。
2. 每步完成跑 `cargo test --workspace` 与 `cargo clippy --workspace --all-targets`;
   验收标准:现有测试全绿 + 新增用例覆盖上述断言。**当前 150 tests 全绿、clippy 无警告**。
3. 可拆分独立 issue:①wire_format 与 compat(config 层);②缓存策略与指纹回归;
   ③Chat 模式保真度;④成本表与 /health 扩展。

## 不做(本轮)

- 第 4 项:重试 / provider 故障切换(涉及请求生命周期,建议独立一轮,含幂等性与超时预算设计)。
- 第 5 项:代理运行时模型发现(`models sync` 保持 CLI 入口)。
- anthropic-messages 线格式(第 1 项只留扩展缝,不实现)。
