use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use xgovernor_core::SessionDomainError;

pub(crate) const TOOLS: &[&str] = &[
    "bash",
    "file_read",
    "file_write",
    "file_edit",
    "glob",
    "grep",
    "ask_user_question",
];

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct ToolPolicy {
    pub allow: Option<BTreeSet<String>>,
    pub deny: Option<BTreeSet<String>>,
}
impl ToolPolicy {
    pub fn validate(&self) -> Result<(), SessionDomainError> {
        for name in self.allow.iter().chain(self.deny.iter()).flatten() {
            if !TOOLS.contains(&name.as_str()) {
                return Err(invalid(format!("unknown xiaoO tool: {name}")));
            }
        }
        Ok(())
    }
    pub fn allows(&self, name: &str) -> bool {
        TOOLS.contains(&name)
            && self.allow.as_ref().is_none_or(|names| names.contains(name))
            && self.deny.as_ref().is_none_or(|names| !names.contains(name))
    }
    pub fn merge(&mut self, next: Self) {
        if next.allow.is_some() {
            self.allow = next.allow;
        }
        if next.deny.is_some() {
            self.deny = next.deny;
        }
    }
}

/// Embedded xiaoO has no external resource loaders. Disabled declarations are
/// supported; enabling an unavailable loader must not be silently ignored.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct DiscoveryPolicy {
    pub extensions: Option<bool>,
    pub skills: Option<bool>,
    pub prompt_templates: Option<bool>,
    pub themes: Option<bool>,
    pub context_files: Option<bool>,
}
impl DiscoveryPolicy {
    pub fn validate(&self) -> Result<(), SessionDomainError> {
        for (name, enabled) in [
            ("extensions", self.extensions),
            ("skills", self.skills),
            ("prompt_templates", self.prompt_templates),
            ("themes", self.themes),
            ("context_files", self.context_files),
        ] {
            if enabled == Some(true) {
                return Err(invalid(format!(
                    "xiaoO does not support enabling discovery.{name}"
                )));
            }
        }
        Ok(())
    }
}
pub(crate) fn invalid(message: impl Into<String>) -> SessionDomainError {
    SessionDomainError::InvalidRequest {
        message: message.into(),
    }
}
