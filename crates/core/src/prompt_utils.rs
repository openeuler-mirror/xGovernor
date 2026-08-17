use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

/// A predefined subagent role available for delegation via `spawn_subagent`.
///
/// Relocated from the deleted `gateway/session_record.rs` (see
/// `docs/legacy_gateway_design_notes.md`) — this definition itself had no
/// dependency on any dead crate, only its former home did.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubagentRoleRecord {
    pub role_id: String,
    pub description: String,
    #[serde(default)]
    pub prompt: Option<String>,
    #[serde(default)]
    pub max_turns: Option<u32>,
    #[serde(default)]
    pub tools: BTreeMap<String, bool>,
}

pub fn compose_subagent_delegation_rules(
    subagent_roles: &BTreeMap<String, SubagentRoleRecord>,
) -> Option<String> {
    if subagent_roles.is_empty() {
        return None;
    }

    let roles_list = subagent_roles
        .values()
        .map(|role| format!("- \"{}\": {}", role.role_id, role.description))
        .collect::<Vec<_>>()
        .join("\n");

    Some(format!(
        "\n\n## Subagent Delegation\n\n\
        When a predefined subagent role matches the user's request, delegate to it using `spawn_subagent` with `subagent_role_id`. **Workflow**: spawn → wait → `join_subagent` → process results.\n\n\
        **Available Roles**:\n{}\n\n\
        Always delegate to matching roles instead of handling directly.",
        roles_list
    ))
}

pub fn generate_skills_dirs_table(skills_dirs: &[PathBuf]) -> String {
    if skills_dirs.is_empty() {
        return "| Priority | Directory | Purpose |\n|----------|-----------|---------|\n| (none configured) | - | - |".to_string();
    }

    let mut table =
        "| Priority | Directory | Purpose |\n|----------|-----------|---------|\n".to_string();

    let mut config_counter = 0;

    for dir in skills_dirs {
        let dir_str = dir.display().to_string();
        let (priority, purpose) = classify_skill_dir(&dir_str, &mut config_counter);

        table.push_str(&format!("| {} | `{}` | {} |\n", priority, dir_str, purpose));
    }

    table.trim_end().to_string()
}

fn classify_skill_dir(dir: &str, config_counter: &mut usize) -> (String, String) {
    if dir == ".xiaoo/skills" {
        ("Project".to_string(), "Project-specific skills".to_string())
    } else if dir == "/usr/lib/.xiaoo/skills" {
        ("System".to_string(), "Built-in skills".to_string())
    } else if dir.ends_with("/.xiaoo/skills")
        && (dir.starts_with('~') || dir.starts_with("/home/") || dir.starts_with("/root/"))
    {
        ("User".to_string(), "Personal skills".to_string())
    } else {
        *config_counter += 1;
        (
            "Config".to_string(),
            format!("Configured skill dir {}", config_counter),
        )
    }
}
