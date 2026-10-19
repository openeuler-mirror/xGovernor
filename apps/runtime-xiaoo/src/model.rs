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
