use std::collections::BTreeMap;
use std::time::Duration;

use agent_runtime_protocol::RecentTurnText;
use serde::{Deserialize, Serialize};
use session_protocol::SessionTurnRequest;
use xgovernor_core::{SecurityContext, SessionApplication};

/// How many prior turns are consolidated before materialization. The current
/// `/mcts` message is intercepted before `submit_turn`, so it never reaches
/// the runtime transcript and must be appended separately.
const HISTORY_LIMIT: usize = 20;
const EXTRACT_TIMEOUT: Duration = Duration::from_secs(15);

pub(super) const MAX_FILE_PATHS: usize = 32;
pub(super) const MAX_TEXT_FILE_BYTES: usize = 256 * 1024;
pub(super) const MAX_TEXT_TOTAL_BYTES: usize = 1024 * 1024;

/// Consolidated task description produced before checkpointing: an explicit
/// goal plus an optional code source the materializer will flatten into
/// `/home/user/workspace/repo`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(super) struct MctsTurnDirective {
    pub goal: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code_source: Option<MctsCodeSource>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(super) struct MctsCodeSource {
    pub kind: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub paths: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub files: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, rename = "ref", skip_serializing_if = "Option::is_none")]
    pub git_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subdir: Option<String>,
}

/// Extract a consolidated directive from prior conversation history plus the
/// current `/mcts` message. Extraction is best-effort: any LLM/config/parse
/// failure degrades to the raw `/mcts` goal with `code_source=None`, which is
/// exactly the P5 checkpoint-in-place behavior.
pub(super) async fn extract_directive(
    app: &SessionApplication,
    ctx: &SecurityContext,
    request: &SessionTurnRequest,
    fallback_goal: &str,
) -> MctsTurnDirective {
    let history = match app
        .conversation_context(ctx, &request.runtime_id, HISTORY_LIMIT)
        .await
    {
        Ok(history) => history,
        Err(error) => {
            tracing::warn!(
                runtime_id = %request.runtime_id,
                error = %error,
                "mcts.context: failed to read conversation history; extracting from current message only"
            );
            Vec::new()
        }
    };

    let consolidation = build_consolidation_context(&history, &request.text);
    match run_extraction(&consolidation).await {
        Ok(mut directive) => {
            if directive.goal.trim().is_empty() {
                directive.goal = fallback_goal.to_string();
            }
            log_extraction(&directive);
            directive
        }
        Err(error) => {
            tracing::warn!(
                runtime_id = %request.runtime_id,
                error = %error,
                "mcts.context.extract_failed: falling back to raw /mcts goal"
            );
            MctsTurnDirective {
                goal: fallback_goal.to_string(),
                code_source: None,
            }
        }
    }
}

fn log_extraction(directive: &MctsTurnDirective) {
    match directive.code_source.as_ref() {
        Some(source) => tracing::info!(
            goal = %directive.goal,
            kind = %source.kind,
            "mcts.context: extracted code_source"
        ),
        None => tracing::info!(goal = %directive.goal, "mcts.context: extracted goal only"),
    }
}

fn build_consolidation_context(history: &[RecentTurnText], current: &str) -> String {
    let mut parts = Vec::with_capacity(history.len() + 2);
    parts.push("前序对话（可能为空）:".to_string());
    for item in history {
        parts.push(format!("[{}] {}", item.role, item.text));
    }
    parts.push("当前 /mcts 指令:".to_string());
    parts.push(current.trim().to_string());
    parts.join("\n\n")
}

async fn run_extraction(context: &str) -> Result<MctsTurnDirective, String> {
    let Some(config) = LlmConfig::from_env() else {
        return Err("LLM context extraction is not configured".to_string());
    };
    let client = reqwest::Client::builder()
        .timeout(EXTRACT_TIMEOUT)
        .build()
        .map_err(|error| error.to_string())?;

    let payload = serde_json::json!({
        "model": config.model,
        "temperature": 0.0,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": context}
        ],
    });

    let mut request = client.post(config.endpoint()).json(&payload);
    if let Some(api_key) = config.api_key.as_ref() {
        request = request.bearer_auth(api_key);
    }
    let response = request.send().await.map_err(|error| error.to_string())?;
    let status = response.status();
    let body = response.text().await.map_err(|error| error.to_string())?;
    if !status.is_success() {
        return Err(format!("LLM context extraction HTTP {status}: {body}"));
    }
    let content = parse_chat_content(&body)?;
    parse_directive(&content)
}

struct LlmConfig {
    api_base: String,
    model: String,
    api_key: Option<String>,
}

impl LlmConfig {
    fn from_env() -> Option<Self> {
        let env = |name: &str| {
            std::env::var(name)
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        };
        let api_base = env("XGOVERNOR_LLM_API_BASE")?;
        let model = env("XGOVERNOR_LLM_MODEL")
            .unwrap_or_else(|| "DeepSeek-V4.1-Flash-1017".to_string());
        let api_key = env("XGOVERNOR_LLM_KEY_ENV").and_then(|key_env| env(&key_env));
        Some(Self {
            api_base,
            model,
            api_key,
        })
    }

    fn endpoint(&self) -> String {
        let base = self.api_base.trim_end_matches('/');
        if base.ends_with("/chat/completions") {
            base.to_string()
        } else {
            format!("{base}/chat/completions")
        }
    }
}

#[derive(Deserialize)]
struct ChatResponse {
    choices: Vec<ChatChoice>,
}

#[derive(Deserialize)]
struct ChatChoice {
    message: ChatMessageContent,
}

#[derive(Deserialize)]
struct ChatMessageContent {
    content: Option<String>,
}

fn parse_chat_content(body: &str) -> Result<String, String> {
    let parsed: ChatResponse = serde_json::from_str(body)
        .map_err(|error| format!("invalid chat completion response: {error}"))?;
    parsed
        .choices
        .into_iter()
        .next()
        .and_then(|choice| choice.message.content)
        .filter(|content| !content.trim().is_empty())
        .ok_or_else(|| "chat completion has no assistant text content".to_string())
}

fn parse_directive(content: &str) -> Result<MctsTurnDirective, String> {
    let mut cleaned = content.trim();
    if let Some(stripped) = cleaned.strip_prefix("```json") {
        cleaned = stripped;
    } else if let Some(stripped) = cleaned.strip_prefix("```") {
        cleaned = stripped;
    }
    while cleaned.ends_with("```") {
        cleaned = cleaned[..cleaned.len() - 3].trim_end();
    }
    let directive: MctsTurnDirective =
        serde_json::from_str(cleaned).map_err(|error| format!("directive is not valid JSON: {error}"))?;
    if directive.goal.trim().is_empty() {
        return Err("directive goal is empty".to_string());
    }
    if let Some(source) = directive.code_source.as_ref() {
        let valid = match source.kind.as_str() {
            "file" => !source.paths.is_empty() && source.paths.len() <= MAX_FILE_PATHS,
            "text" => !source.files.is_empty(),
            "git" => source
                .url
                .as_deref()
                .is_some_and(|url| url.starts_with("https://")),
            "empty" => true,
            _ => false,
        };
        if !valid {
            return Err(format!(
                "invalid code_source: kind={} (payload missing or malformed)",
                source.kind
            ));
        }
    }
    Ok(directive)
}

const SYSTEM_PROMPT: &str = "\
你是任务梳理助手。根据前序对话和当前 /mcts 指令，输出一个严格 JSON（不要 Markdown），用于把任务交给 MCTS。

输出结构：
{\"goal\":\"任务描述\",\"code_source\":{\"kind\":\"file|text|git|empty\", ...}}

code_source 规则：
- file：代码已经在容器 /home/user/workspace 里，paths 是相对该目录的路径数组（最多 32 个）。
- text：代码在对话文本里，files 是 {\"目标相对路径\":\"完整文件内容\"}。
- git：需要从远程仓库克隆，url 必须以 https:// 开头；可选 ref、subdir。
- empty：生成类任务，无初始代码。

要求：
1. goal 必须非空，把任务明确落到 /home/user/workspace/repo 下。
2. 综合前序对话判断代码来源；当前指令里的代码/路径也要纳入。
3. 只输出 JSON，不要解释。";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_fenced_directive() {
        let directive = parse_directive(
            "```json\n{\"goal\":\"实现加法器\",\"code_source\":{\"kind\":\"empty\"}}\n```",
        )
        .unwrap();
        assert_eq!(directive.goal, "实现加法器");
        assert_eq!(directive.code_source.unwrap().kind, "empty");
    }

    #[test]
    fn rejects_missing_goal() {
        assert!(parse_directive(r#"{"code_source":{"kind":"empty"}}"#).is_err());
    }

    #[test]
    fn rejects_invalid_kind() {
        assert!(parse_directive(r#"{"goal":"g","code_source":{"kind":"nope"}}"#).is_err());
    }

    #[test]
    fn git_requires_https_url() {
        assert!(
            parse_directive(r#"{"goal":"g","code_source":{"kind":"git","url":"git@github.com:x/y.git"}}"#)
                .is_err()
        );
        assert!(
            parse_directive(
                r#"{"goal":"g","code_source":{"kind":"git","url":"https://github.com/x/y.git"}}"#
            )
            .is_ok()
        );
    }
}
