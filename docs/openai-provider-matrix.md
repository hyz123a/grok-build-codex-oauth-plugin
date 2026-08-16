# OpenAI 兼容后端能力矩阵

线格式与 provider 的能力对照。配置详见 `docs/plan-openai-backend.md`(基础双模式)与
`docs/plan-openai-compat-hardening.md`(wire_format / compat / cache_policy / cost)。

| 能力 | Chat 模式(`/chat/completions`) | Responses 模式(`/responses`,如 DeepSeek) |
|------|-------------------------------|-------------------------------------------|
| 文本流式输出 | ✅ SSE `content` delta 桥接为 `response.output_text.delta` | ✅ 直通(复用 `CodexSseTransformer`) |
| 工具调用 | ✅ 函数工具,连续 `function_call` 合并为单条 assistant `tool_calls`;并发工具调用按 index 累积 | ✅ 函数工具直通;`Tool names must be unique` 去重 |
| reasoning 透传 | ⚠️ 部分:`delta.reasoning_content` 桥接为 `response.reasoning_text.delta`,回传折叠为 assistant `reasoning_content`;`compat.supports_reasoning_effort = false` 时全部跳过 | ✅ `reasoning.effort` clamp + `thinking` 开关;`supports_reasoning_effort = false` 时不动 effort |
| web_search | ❌ 无标准 tool,`x_search` 直接丢弃 | ✅ `x_search` → `web_search` 服务端工具;`web_search = false` 关闭;`web_search_call` 往返归一化(action/queries) |
| 显式缓存 key | ❌ Chat 协议无 `prompt_cache_key` 字段,`cache_policy = "explicit"` 不生效 | ✅ `cache_policy = "explicit"` 注入(请求体 key 优先,否则 `x-grok-session-id`);默认 `implicit` 前缀缓存 |
| 隐式前缀缓存统计 | ✅ 读 `usage.prompt_tokens_details.cached_tokens` → `cache_hit_ratio_pct` | ✅ 同左(Responses usage 路径) |
| `max_output_tokens` 转发 | ✅ 写 `max_tokens`(或 `compat.max_tokens_field = "max_completion_tokens"`) | ⚠️ 无状态 Responses 协议中剥掉(provider 不消费) |
| usage 上报缺失 | ⚠️ 保持 0,日志 `usage_estimated=true` 标记 | ✅ 上游不报则同左 |
| 成本估算 | ✅ `[backend.openai.cost]` → 日志 `estimated_cost_usd` | ✅ 同左 |
| 会话状态 | n/a(每次全量重发) | ⚠️ 默认无状态(`compat.stateless = true` 剥 `previous_response_id`);状态化 provider 设 `stateless = false` 保留 |

## provider 示例

| Provider | 建议配置 | 备注 |
|----------|----------|------|
| DeepSeek(原生 Responses) | `wire_format = "responses"`,`web_search = true`,`cache_policy = "implicit"` | 默认语义即此配置,`compat` 全默认 |
| DeepSeek Chat | 默认(`wire_format = "chat-completions"`) | 无服务端搜索,`x_search` 丢弃 |
| OpenAI 官方 Responses | `wire_format = "responses"`,`compat.stateless = false`,`cache_policy = "explicit"` | 状态化多轮 + 显式缓存 key;需实测字段兼容性 |
| opencode zen 等 Chat 网关 | 默认 | usage 尾块位置不同已兼容(finish chunk 或独立尾块) |
| 本地网关(llama.cpp 等) | 默认 | 隐式前缀缓存依赖 system prompt/tool 稳定,见 `stable_request_prefix_fingerprint` 日志 |
