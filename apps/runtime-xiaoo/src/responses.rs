//! Responses transport, including stateless reasoning/tool-call replay.
use agent_contracts::llm::{LlmProvider, ProviderCapabilities};
use agent_types::{
    AssistantMessage, ContentBlock, LlmError, LlmRequest, LlmResponse, MessageRole,
    ReasoningEffort, ResponseFormat, StopReason, StreamChunk, ToolChoice, ToolUseBlock, Usage,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct ResponsesState {
    scope: String,
    /// Raw output preserves reasoning signatures/encrypted content that the
    /// SDK's generic ChatMessage cannot represent. No API credentials here.
    output: BTreeMap<String, Vec<Value>>,
}

impl ResponsesState {
    pub(crate) fn retain_history(&mut self, messages: &[agent_types::ChatMessage]) {
        let live: std::collections::BTreeSet<_> = messages
            .iter()
            .filter(|m| m.role == MessageRole::Assistant)
            .map(|m| block_key(&m.blocks))
            .collect();
        self.output.retain(|key, _| live.contains(key));
    }
}

pub(crate) struct ResponsesProvider {
    client: reqwest::Client,
    endpoint: String,
    key: Option<String>,
    caps: ProviderCapabilities,
    state: Arc<Mutex<ResponsesState>>,
    scope: String,
}
impl ResponsesProvider {
    pub fn new(
        config: &llm_client::ResolvedConfig,
        model: &str,
        state: Arc<Mutex<ResponsesState>>,
    ) -> Self {
        Self {
            client: reqwest::Client::new(),
            endpoint: format!("{}/responses", config.base_url.trim_end_matches('/')),
            key: config.api_key.clone(),
            caps: ProviderCapabilities {
                supports_streaming: true,
                supports_tool_calls: true,
                supports_json_mode: true,
                max_context_window: 128_000,
                model_name: model.into(),
            },
            scope: format!("{}\n{model}", config.base_url),
            state,
        }
    }
    fn body(&self, request: &LlmRequest, stream: bool) -> Result<Value, LlmError> {
        let mut input = Vec::new();
        let mut state = self.state.lock().expect("Responses state lock");
        if state.scope != self.scope {
            state.scope = self.scope.clone();
            state.output.clear();
        }
        for message in &request.messages {
            if message.role == MessageRole::Assistant {
                if let Some(raw) = state.output.get(&block_key(&message.blocks)) {
                    input.extend(raw.clone());
                    continue;
                }
            }
            let role = match message.role {
                MessageRole::System => "system",
                MessageRole::User => "user",
                MessageRole::Assistant => "assistant",
                MessageRole::Tool => "user",
            };
            for block in &message.blocks {
                match block {
                    ContentBlock::Text{text} => input.push(json!({"role":role,"content":text})),
                    ContentBlock::ToolUse{call_id,tool_name,input:args}=>input.push(json!({"type":"function_call","call_id":call_id,"name":tool_name,"arguments":args.to_string()})),
                    ContentBlock::ToolResult{call_id,output,..}=>input.push(json!({"type":"function_call_output","call_id":call_id,"output":output})),
                    ContentBlock::Image{..} | ContentBlock::Document{..} => return Err(LlmError::ConfigError("xiaoO input blocks do not contain media payloads".into())),
                }
            }
        }
        let mut body = json!({"model":self.caps.model_name,"input":input,"stream":stream,"store":false,"include":["reasoning.encrypted_content"]});
        if let Some(limit) = request.max_tokens {
            body["max_output_tokens"] = json!(limit);
        }
        if let Some(temp) = request.temperature {
            body["temperature"] = json!(temp);
        }
        if request.reasoning_effort != ReasoningEffort::Off {
            body["reasoning"] = json!({"effort":if request.reasoning_effort==ReasoningEffort::Max {"xhigh"}else{"high"},"summary":"auto"});
        }
        if !request.tools.is_empty() {
            body["tools"]=json!(request.tools.iter().map(|tool|json!({"type":"function","name":tool.name,"description":tool.description,"parameters":tool.parameters,"strict":false})).collect::<Vec<_>>());
            body["tool_choice"] = match &request.tool_choice {
                ToolChoice::Auto => json!("auto"),
                ToolChoice::None => json!("none"),
                ToolChoice::Required => json!("required"),
                ToolChoice::Specific(name) => json!({"type":"function","name":name}),
            };
        }
        body["text"] = json!({"format":match &request.response_format {
            ResponseFormat::Text=>json!({"type":"text"}),ResponseFormat::JsonObject=>json!({"type":"json_object"}),
            ResponseFormat::JsonSchema{name,schema}=>json!({"type":"json_schema","name":if name.is_empty(){"response"}else{name},"schema":schema})
        }});
        Ok(body)
    }
    async fn send(&self, body: Value) -> Result<reqwest::Response, LlmError> {
        let mut req = self
            .client
            .post(&self.endpoint)
            .timeout(std::time::Duration::from_secs(300))
            .json(&body);
        if let Some(key) = &self.key {
            req = req.bearer_auth(key);
        }
        let response = req.send().await.map_err(|e| {
            if e.is_timeout() {
                LlmError::Timeout
            } else {
                LlmError::HttpError("connection failed".into())
            }
        })?;
        match response.status().as_u16() {
            200..=299 => Ok(response),
            401 | 403 => Err(LlmError::AuthError {
                message: "authentication failed".into(),
            }),
            404 => Err(LlmError::ModelNotFound {
                model: self.caps.model_name.clone(),
            }),
            429 => Err(LlmError::RateLimited {
                retry_after_ms: 0,
                message: "rate limited".into(),
            }),
            _ => Err(LlmError::ApiError("model service error".into())),
        }
    }
    fn parse(&self, value: Value) -> Result<LlmResponse, LlmError> {
        let status = value["status"].as_str().ok_or_else(malformed)?;
        if !matches!(status, "completed" | "incomplete") {
            return Err(LlmError::ApiError("Responses request failed".into()));
        }
        let output = value["output"].as_array().ok_or_else(malformed)?;
        let mut text = String::new();
        let mut reasoning = String::new();
        let mut calls = Vec::new();
        let mut refused = false;
        for item in output {
            match item["type"].as_str() {
                Some("message") => {
                    for block in item["content"].as_array().ok_or_else(malformed)? {
                        match block["type"].as_str() {
                            Some("output_text") => {
                                text.push_str(block["text"].as_str().ok_or_else(malformed)?)
                            }
                            Some("refusal") => {
                                refused = true;
                                text.push_str(block["refusal"].as_str().ok_or_else(malformed)?);
                            }
                            _ => return Err(malformed()),
                        }
                    }
                }
                Some("function_call") => calls.push(ToolUseBlock {
                    call_id: item["call_id"].as_str().ok_or_else(malformed)?.into(),
                    tool_name: item["name"].as_str().ok_or_else(malformed)?.into(),
                    input: serde_json::from_str(item["arguments"].as_str().ok_or_else(malformed)?)
                        .map_err(|_| malformed())?,
                }),
                Some("reasoning") => {
                    if let Some(summary) = item["summary"].as_array() {
                        for block in summary {
                            if let Some(s) = block["text"].as_str() {
                                reasoning.push_str(s);
                            }
                        }
                    }
                }
                _ => return Err(malformed()),
            }
        }
        let stop_reason = if status == "incomplete"
            && value["incomplete_details"]["reason"] == "content_filter"
        {
            StopReason::ContentFilter
        } else if status == "incomplete" {
            StopReason::MaxTokens
        } else if refused {
            StopReason::ContentFilter
        } else if !calls.is_empty() {
            StopReason::ToolUse
        } else {
            StopReason::EndTurn
        };
        let message = AssistantMessage {
            text: (!text.is_empty()).then_some(text),
            reasoning_content: (!reasoning.is_empty()).then_some(reasoning),
            tool_calls: calls,
            stop_reason,
            usage: Usage {
                prompt_tokens: value["usage"]["input_tokens"].as_u64().unwrap_or(0) as usize,
                completion_tokens: value["usage"]["output_tokens"].as_u64().unwrap_or(0) as usize,
                total_tokens: value["usage"]["total_tokens"].as_u64().unwrap_or(0) as usize,
                cached_tokens: value["usage"]["input_tokens_details"]["cached_tokens"]
                    .as_u64()
                    .unwrap_or(0) as usize,
            },
        };
        let mut blocks = Vec::new();
        if let Some(text) = &message.text {
            blocks.push(ContentBlock::Text { text: text.clone() });
        }
        for call in &message.tool_calls {
            blocks.push(ContentBlock::ToolUse {
                call_id: call.call_id.clone(),
                tool_name: call.tool_name.clone(),
                input: call.input.clone(),
            });
        }
        self.state
            .lock()
            .expect("Responses state lock")
            .output
            .insert(block_key(&blocks), output.clone());
        Ok(LlmResponse {
            message,
            kv_cache_chunk_hashes: vec![],
        })
    }
}
fn malformed() -> LlmError {
    LlmError::ParseError("invalid Responses response".into())
}
fn block_key(blocks: &[ContentBlock]) -> String {
    serde_json::to_string(blocks).expect("serializable blocks")
}

#[async_trait]
impl LlmProvider for ResponsesProvider {
    async fn complete(&self, request: &LlmRequest) -> Result<LlmResponse, LlmError> {
        let value = self
            .send(self.body(request, false)?)
            .await?
            .json()
            .await
            .map_err(|_| malformed())?;
        self.parse(value)
    }
    async fn complete_stream(
        &self,
        request: &LlmRequest,
        callback: &(dyn Fn(StreamChunk) + Send + Sync),
    ) -> Result<LlmResponse, LlmError> {
        let mut response = self.send(self.body(request, true)?).await?;
        let mut bytes = Vec::new();
        let mut data = String::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| LlmError::StreamError {
            message: "Responses stream interrupted".into(),
        })? {
            bytes.extend_from_slice(&chunk);
            while let Some(end) = bytes.iter().position(|b| *b == b'\n') {
                let line = bytes.drain(..=end).collect::<Vec<_>>();
                let line = std::str::from_utf8(&line)
                    .map_err(|_| malformed())?
                    .trim_end_matches(['\r', '\n']);
                if line.is_empty() {
                    if data.is_empty() {
                        continue;
                    }
                    let event: Value = serde_json::from_str(&data).map_err(|_| malformed())?;
                    data.clear();
                    match event["type"].as_str() {
                        Some("response.completed" | "response.incomplete") => {
                            return self.parse(event["response"].clone())
                        }
                        Some("response.failed" | "error") => {
                            return Err(LlmError::ApiError("Responses stream failed".into()))
                        }
                        Some("response.output_text.delta") => callback(StreamChunk {
                            delta_text: Some(event["delta"].as_str().ok_or_else(malformed)?.into()),
                            delta_reasoning: None,
                            delta_tool_call: None,
                        }),
                        Some("response.reasoning_summary_text.delta") => callback(StreamChunk {
                            delta_text: None,
                            delta_reasoning: Some(
                                event["delta"].as_str().ok_or_else(malformed)?.into(),
                            ),
                            delta_tool_call: None,
                        }),
                        _ => {}
                    }
                } else if let Some(value) = line.strip_prefix("data:") {
                    if !data.is_empty() {
                        data.push('\n');
                    }
                    data.push_str(value.trim_start());
                }
            }
        }
        Err(LlmError::StreamError {
            message: "Responses stream ended before completion".into(),
        })
    }
    fn capabilities(&self) -> &ProviderCapabilities {
        &self.caps
    }
}
