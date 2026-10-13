//! One configuration path for admission, probes and worker model switches.
use crate::{
    policy::invalid,
    responses::{ResponsesProvider, ResponsesState},
    xiaoo_backend::PersistedLlm,
    XiaooRuntimeExt,
};
use agent_contracts::llm::{LlmProvider, ProviderCapabilities};
use agent_types::{LlmError, LlmRequest, LlmResponse, ReasoningEffort, StreamChunk};
use async_trait::async_trait;
use llm_client::{resolve_config, ProtocolFamily, ResolveInput, ResolvedConfig};
use session_protocol::{LlmApi, LlmCapabilities, LlmOverrideRequest, ResolvedLlmDescriptor};
use std::sync::{Arc, Mutex};
use tracing::instrument::WithSubscriber;
use xgovernor_core::SessionDomainError;

pub(crate) fn validate_override(
    value: Option<&LlmOverrideRequest>,
) -> Result<(), SessionDomainError> {
    if let Some(value) = value {
        if value.api_key.is_some() {
            return Err(invalid("xiaoO rejects inline api_key; use llm.api_key_env"));
        }
        for (name, value) in [
            ("provider", &value.provider),
            ("model", &value.model),
            ("api_key_env", &value.api_key_env),
            ("api_base", &value.api_base),
        ] {
            if value.as_ref().is_some_and(|v| v.trim().is_empty()) {
                return Err(invalid(format!("llm.{name} must not be empty")));
            }
        }
        if let Some(caps) = &value.capabilities {
            caps.validate().map_err(invalid)?;
        }
    }
    Ok(())
}

pub(crate) fn initial_llm(
    ext: &XiaooRuntimeExt,
    value: Option<&LlmOverrideRequest>,
) -> Result<PersistedLlm, SessionDomainError> {
    PersistedLlm {
        provider: ext.provider.clone(),
        model: ext.model.clone(),
        api_key_env: ext.api_key_env.clone(),
        api_base: ext.api_base.clone(),
        api: None,
        capabilities: None,
    }
    .apply(value)
}

impl PersistedLlm {
    pub(crate) fn apply(
        &self,
        value: Option<&LlmOverrideRequest>,
    ) -> Result<Self, SessionDomainError> {
        validate_override(value)?;
        let mut next = self.clone();
        if let Some(value) = value {
            if value.provider.as_ref().is_some_and(|p| p != &self.provider) {
                next.api_base = None;
                next.api = None;
                next.capabilities = None;
            } else if value.model.as_ref().is_some_and(|m| m != &self.model) {
                next.capabilities = None;
            }
            if let Some(v) = &value.provider {
                next.provider = v.trim().into();
            }
            if let Some(v) = &value.model {
                next.model = v.trim().into();
            }
            if let Some(v) = &value.api_key_env {
                next.api_key_env = v.trim().into();
            }
            if let Some(v) = &value.api_base {
                next.api_base = Some(v.trim().into());
            }
            if let Some(v) = value.api {
                next.api = Some(v);
            }
            if let Some(v) = &value.capabilities {
                let caps = next.capabilities.get_or_insert_with(Default::default);
                if v.reasoning.is_some() {
                    caps.reasoning = v.reasoning;
                }
                if v.input.is_some() {
                    caps.input = v.input.clone();
                }
                if v.context_window.is_some() {
                    caps.context_window = v.context_window;
                }
                if v.max_tokens.is_some() {
                    caps.max_tokens = v.max_tokens;
                }
            }
        }
        next.resolve()?;
        Ok(next)
    }

    pub(crate) fn resolve(&self) -> Result<ResolvedConfig, SessionDomainError> {
        if self.provider.trim().is_empty()
            || self.model.trim().is_empty()
            || self.api_key_env.trim().is_empty()
        {
            return Err(invalid(
                "xiaoO requires provider, model and api_key_env in llm or ext.xiaoo",
            ));
        }
        self.effective_capabilities().validate().map_err(invalid)?;
        if let Some(base) = &self.api_base {
            let url = reqwest::Url::parse(base).map_err(|_| invalid("invalid llm.api_base URL"))?;
            if !matches!(url.scheme(), "http" | "https")
                || url.host_str().is_none()
                || !url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some()
            {
                return Err(invalid("llm.api_base must be an HTTP(S) endpoint without credentials, query or fragment"));
            }
        }
        let profile = llm_client::resolve_provider_profile(&self.provider);
        let native = profile.as_ref().map(|p| p.protocol_family);
        let explicit = self.api.map(|api| match api {
            LlmApi::AnthropicMessages => ProtocolFamily::Anthropic,
            _ => ProtocolFamily::OpenAiCompatible,
        });
        if self.api_base.is_none()
            && self.api.is_some()
            && (explicit != native || self.api == Some(LlmApi::OpenaiResponses))
        {
            return Err(invalid(
                "explicit protocol conflicts with the built-in provider; supply llm.api_base",
            ));
        }
        // Preserve legacy provider-specific custom endpoints. Unknown custom
        // providers default to Chat Completions, as pi does.
        let custom = self.api_base.is_some() && (explicit.is_some() || profile.is_none());
        resolve_config(ResolveInput {
            provider: if custom {
                None
            } else {
                Some(self.provider.clone())
            },
            protocol: if custom {
                Some(
                    match explicit.unwrap_or(ProtocolFamily::OpenAiCompatible) {
                        ProtocolFamily::Anthropic => "anthropic",
                        _ => "openai",
                    }
                    .into(),
                )
            } else {
                None
            },
            api_key_env: Some(self.api_key_env.clone()),
            base_url: self.api_base.clone(),
            ..Default::default()
        })
        .map_err(|_| {
            invalid("invalid xiaoO model configuration or missing credential environment variable")
        })
    }

    pub(crate) fn effective_capabilities(&self) -> LlmCapabilities {
        let mut caps = self.capabilities.clone().unwrap_or_default();
        let context = caps.context_window.unwrap_or_else(|| {
            llm_client::get_known_model_context_length(&self.model)
                .and_then(|v| v.try_into().ok())
                .unwrap_or(128_000)
        });
        caps.context_window = Some(context);
        caps.max_tokens = Some(caps.max_tokens.unwrap_or((context / 10).max(1)));
        caps
    }

    pub(crate) fn descriptor(&self) -> ResolvedLlmDescriptor {
        let api = self
            .api
            .or_else(|| {
                llm_client::resolve_provider_profile(&self.provider).and_then(|p| {
                    match p.protocol_family {
                        ProtocolFamily::OpenAiCompatible => Some(LlmApi::OpenaiCompletions),
                        ProtocolFamily::Anthropic => Some(LlmApi::AnthropicMessages),
                        _ => None,
                    }
                })
            })
            .or_else(|| {
                (llm_client::resolve_provider_profile(&self.provider).is_none()
                    && self.api_base.is_some())
                .then_some(LlmApi::OpenaiCompletions)
            });
        ResolvedLlmDescriptor {
            provider: self.provider.clone(),
            model: self.model.clone(),
            api_base: self.api_base.clone(),
            api,
            capabilities: Some(self.effective_capabilities()),
            credential_source: format!("env:{}", self.api_key_env),
        }
    }
}

pub(crate) fn provider(
    llm: &PersistedLlm,
    state: Arc<Mutex<ResponsesState>>,
) -> Result<Arc<dyn LlmProvider>, SessionDomainError> {
    let mut resolved = llm.resolve()?;
    // An explicit protocol uses its canonical endpoint instead of the SDK's
    // legacy URL guessing. Legacy configurations retain their fallback rules.
    if llm.api == Some(LlmApi::OpenaiCompletions)
        && !resolved.base_url.ends_with("/chat/completions")
    {
        resolved.base_url.push_str("/chat/completions");
    }
    let inner: Arc<dyn LlmProvider> = if llm.api == Some(LlmApi::OpenaiResponses) {
        Arc::new(ResponsesProvider::new(&resolved, &llm.model, state))
    } else {
        llm_client::create_llm_provider_from_resolved(&resolved, llm.model.clone(), None, None)
            .map_err(|_| invalid("failed to construct xiaoO model provider"))?
            .inner()
    };
    let declared = llm.effective_capabilities();
    let mut capabilities = inner.capabilities().clone();
    capabilities.max_context_window = declared.context_window.unwrap() as usize;
    Ok(Arc::new(ConfiguredProvider {
        inner,
        capabilities,
        declared,
        anthropic: resolved.protocol == ProtocolFamily::Anthropic,
    }))
}

struct ConfiguredProvider {
    inner: Arc<dyn LlmProvider>,
    capabilities: ProviderCapabilities,
    declared: LlmCapabilities,
    anthropic: bool,
}
impl ConfiguredProvider {
    fn request(&self, request: &LlmRequest) -> Result<LlmRequest, LlmError> {
        if self.declared.reasoning == Some(false)
            && request.reasoning_effort != ReasoningEffort::Off
        {
            return Err(LlmError::ConfigError(
                "model does not support reasoning".into(),
            ));
        }
        if self.anthropic
            && self.declared.max_tokens.unwrap() < 2048
            && request.reasoning_effort != ReasoningEffort::Off
        {
            return Err(LlmError::ConfigError(
                "Anthropic reasoning requires an output limit of at least 2048 in this SDK".into(),
            ));
        }
        let mut request = request.clone();
        let limit = self.declared.max_tokens.unwrap() as usize;
        request.max_tokens = Some(request.max_tokens.unwrap_or(limit).min(limit));
        Ok(request)
    }
}
#[async_trait]
impl LlmProvider for ConfiguredProvider {
    async fn complete(&self, request: &LlmRequest) -> Result<LlmResponse, LlmError> {
        self.inner
            .complete(&self.request(request)?)
            .with_subscriber(tracing::subscriber::NoSubscriber::default())
            .await
            .and_then(validate_response)
            .map_err(sanitize)
    }
    async fn complete_stream(
        &self,
        request: &LlmRequest,
        callback: &(dyn Fn(StreamChunk) + Send + Sync),
    ) -> Result<LlmResponse, LlmError> {
        self.inner
            .complete_stream(&self.request(request)?, callback)
            .with_subscriber(tracing::subscriber::NoSubscriber::default())
            .await
            .and_then(validate_response)
            .map_err(sanitize)
    }
    fn capabilities(&self) -> &ProviderCapabilities {
        &self.capabilities
    }
}
fn validate_response(response: LlmResponse) -> Result<LlmResponse, LlmError> {
    let message = &response.message;
    if matches!(message.stop_reason, agent_types::StopReason::EndTurn)
        && message.text.as_ref().is_none_or(|text| text.is_empty())
        && message.tool_calls.is_empty()
    {
        return Err(LlmError::ParseError(
            "model returned no assistant output".into(),
        ));
    }
    Ok(response)
}

fn sanitize(error: LlmError) -> LlmError {
    // Upstream SDK errors may contain URLs, response bodies or echoed keys.
    let message = match error {
        LlmError::ApiError(ref message) => classify_sdk_api_error(message),
        LlmError::AuthError { .. } => "model authentication failed",
        LlmError::ModelNotFound { .. } => "model not found",
        LlmError::Timeout => "model request timed out",
        LlmError::RateLimited { .. } => "model rate limited",
        LlmError::HttpError(_) | LlmError::IoError(_) => "model connection failed",
        LlmError::ParseError(_) => "invalid model response",
        LlmError::ContextLengthExceeded { .. } => "model context limit exceeded",
        LlmError::ConfigError(_) => "invalid model request configuration",
        _ => "model service request failed",
    };
    LlmError::RequestFailed {
        message: message.into(),
    }
}

fn classify_sdk_api_error(message: &str) -> &'static str {
    // The SDK wraps endpoint failures in a summary. Inspect only the first
    // structured error prefix, never copy its response body into diagnostics.
    let prefix = if message.starts_with("All ") {
        message
            .lines()
            .nth(1)
            .and_then(|line| line.split_once(" → "))
            .map(|(_, error)| error)
            .unwrap_or("")
    } else {
        message
    };
    let prefix = prefix.strip_prefix("API error: ").unwrap_or(prefix);
    if prefix.starts_with("HTTP 401 ") || prefix.starts_with("HTTP 403 ") {
        "model authentication failed"
    } else if prefix.starts_with("HTTP 404 ") {
        "model not found"
    } else if prefix.starts_with("HTTP error: Request timeout:") {
        "model request timed out"
    } else if prefix.starts_with("HTTP error:") {
        "model connection failed"
    } else if prefix.starts_with("parse error:") {
        "invalid model response"
    } else {
        "model service request failed"
    }
}

pub(crate) async fn probe(llm: &PersistedLlm) -> Result<(), SessionDomainError> {
    let provider = provider(llm, Arc::new(Mutex::new(Default::default())))?;
    let request: LlmRequest = serde_json::from_value(serde_json::json!({
        "messages":[{"role":"user","blocks":[{"type":"text","text":"Hi"}],"message_id":null,"timestamp_ms":0,"api_usage_tokens":null}],
        "tools":[],"tool_choice":"none","max_tokens":16,"temperature":null,"response_format":"text"
    })).expect("static probe request");
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        provider.complete(&request),
    )
    .await
    .map_err(|_| invalid("model preflight timed out"))?
    .map(|_| ())
    .map_err(|error| invalid(format!("model preflight: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn config(api: Option<LlmApi>, base: Option<String>) -> PersistedLlm {
        PersistedLlm {
            provider: "openai".into(),
            model: "test-model".into(),
            api_key_env: "PATH".into(),
            api_base: base,
            api,
            capabilities: None,
        }
    }
    #[test]
    fn protocols_capabilities_and_legacy_states() {
        let legacy:PersistedLlm=serde_json::from_value(json!({"provider":"anthropic","model":"test","api_key_env":"PATH","api_base":"http://localhost:123/v1"})).unwrap();
        assert_eq!(
            legacy.resolve().unwrap().protocol,
            ProtocolFamily::Anthropic
        );
        assert_eq!(legacy.descriptor().api, Some(LlmApi::AnthropicMessages));
        assert_eq!(
            config(None, Some("http://localhost/v1".into()))
                .resolve()
                .unwrap()
                .protocol,
            ProtocolFamily::OpenAiCompatible
        );
        assert!(config(Some(LlmApi::OpenaiResponses), None)
            .resolve()
            .is_err());
        assert!(config(Some(LlmApi::AnthropicMessages), None)
            .resolve()
            .is_err());
        assert_eq!(
            config(
                Some(LlmApi::AnthropicMessages),
                Some("http://localhost/v1".into())
            )
            .resolve()
            .unwrap()
            .protocol,
            ProtocolFamily::Anthropic
        );
        let base = config(None, None);
        let changed=base.apply(Some(&serde_json::from_value(json!({"capabilities":{"reasoning":false,"input":["text"],"context_window":1000,"max_tokens":99}})).unwrap())).unwrap();
        let changed = changed
            .apply(Some(
                &serde_json::from_value(json!({"capabilities":{"max_tokens":88}})).unwrap(),
            ))
            .unwrap();
        assert_eq!(changed.effective_capabilities().reasoning, Some(false));
        assert_eq!(changed.effective_capabilities().max_tokens, Some(88));
        assert!(changed
            .apply(Some(
                &serde_json::from_value(json!({"capabilities":{"context_window":50}})).unwrap()
            ))
            .is_err());
        let frozen = serde_json::to_value(&changed).unwrap();
        assert!(!frozen.to_string().contains(&std::env::var("PATH").unwrap()));
        let restored: PersistedLlm = serde_json::from_value(frozen).unwrap();
        assert_eq!(restored.descriptor(), changed.descriptor());
        assert_eq!(
            restored
                .apply(Some(
                    &serde_json::from_value(json!({"model":"other"})).unwrap()
                ))
                .unwrap()
                .capabilities,
            None
        );
        for value in [
            json!({"api_key":"secret"}),
            json!({"model":""}),
            json!({"capabilities":{"input":[]}}),
            json!({"capabilities":{"max_tokens":0}}),
        ] {
            assert!(base
                .apply(Some(&serde_json::from_value(value).unwrap()))
                .is_err());
        }
        for url in [
            "http://user:secret@localhost/v1",
            "http://localhost/v1?key=secret",
            "file:///tmp/model",
        ] {
            assert!(config(None, Some(url.into())).resolve().is_err());
        }
    }
    #[tokio::test]
    async fn probes_use_all_three_real_provider_transports() {
        for (api, body, path) in [
            (
                LlmApi::OpenaiCompletions,
                json!({"id":"c1","object":"chat.completion","created":1,"model":"test-model","choices":[{"index":0,"message":{"role":"assistant","content":"OK"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}),
                "/v1/chat/completions",
            ),
            (
                LlmApi::AnthropicMessages,
                json!({"id":"a1","type":"message","role":"assistant","model":"test-model","content":[{"type":"text","text":"OK"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}),
                "/v1/messages",
            ),
            (
                LlmApi::OpenaiResponses,
                json!({"id":"r1","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"OK"}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}),
                "/v1/responses",
            ),
        ] {
            let (base, server) =
                crate::test_support::mock(200, "application/json", body.to_string()).await;
            let mut llm = config(Some(api), Some(base));
            llm.capabilities = Some(LlmCapabilities {
                context_window: Some(1000),
                max_tokens: Some(7),
                ..Default::default()
            });
            probe(&llm).await.unwrap();
            let (headers, request) = server.await.unwrap();
            assert!(
                headers.starts_with(&format!("POST {path} ")),
                "unexpected path: {}",
                headers.lines().next().unwrap()
            );
            assert!(headers
                .to_lowercase()
                .contains(if api == LlmApi::AnthropicMessages {
                    "x-api-key:"
                } else {
                    "authorization: bearer "
                }));
            assert_eq!(request["model"], "test-model");
            if api == LlmApi::OpenaiResponses {
                assert_eq!(request["max_output_tokens"], 7);
                assert_eq!(request["store"], false);
                assert!(request["input"].is_array());
            } else {
                assert_eq!(request["max_tokens"], 7);
                assert!(request["messages"].is_array());
            }
        }
    }
    #[tokio::test]
    async fn errors_are_sanitized_and_invalid_responses_fail_preflight() {
        for api in [
            LlmApi::OpenaiCompletions,
            LlmApi::OpenaiResponses,
            LlmApi::AnthropicMessages,
        ] {
            for status in [401, 404, 500, 200] {
                let (base, server) = crate::test_support::mock(
                    status,
                    "application/json",
                    json!({"error":{"message":"secret-key"}}).to_string(),
                )
                .await;
                let error = probe(&config(Some(api), Some(base)))
                    .await
                    .unwrap_err()
                    .to_string();
                assert!(!error.contains("secret-key"));
                assert!(!error.contains("127.0.0.1"));
                assert!(error.contains(match status {
                    401 => "authentication",
                    404 => "not found",
                    500 => "service",
                    _ => "invalid model response",
                }));
                server.await.unwrap();
            }
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}/v1", listener.local_addr().unwrap());
        drop(listener);
        assert!(probe(&config(Some(LlmApi::OpenaiResponses), Some(base)))
            .await
            .unwrap_err()
            .to_string()
            .contains("connection"));
    }
}

#[cfg(test)]
mod timeout_tests {
    use super::*;
    #[tokio::test]
    async fn probe_timeout_cancels_the_connection() {
        use tokio::io::AsyncReadExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}/v1", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = [0; 4096];
            loop {
                if socket.read(&mut buf).await.unwrap() == 0 {
                    break;
                }
            }
        });
        let llm = PersistedLlm {
            provider: "gateway".into(),
            model: "test".into(),
            api_key_env: "PATH".into(),
            api_base: Some(base),
            api: Some(LlmApi::OpenaiResponses),
            capabilities: None,
        };
        assert!(probe(&llm)
            .await
            .unwrap_err()
            .to_string()
            .contains("timed out"));
        tokio::time::timeout(std::time::Duration::from_secs(1), server)
            .await
            .expect("probe must release its connection")
            .unwrap();
    }
}
