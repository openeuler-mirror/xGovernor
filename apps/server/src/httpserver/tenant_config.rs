//! `tenants.toml` — the declarative policy file `docs/tenancy_design.md` §4
//! calls for ("一个文件，不是一个子系统"), loaded by `apps/server/src/main.rs`
//! at startup and re-loaded on `SIGHUP`.
//!
//! Scope note: §4 sketches a much larger target schema (`allowed_workspace_kinds`,
//! `git_host_allowlist`, `capability_ceiling`, `min_isolation_boundary`,
//! `allowed_runtimes`, ...). This file only implements the fields that have an
//! actual enforcement point in the codebase today — credentials, role, and the
//! two [`xgovernor_core::TenantQuota`] fields (`max_sessions`,
//! `max_requests_per_minute`) — mirroring exactly what the old
//! `XGOVERNOR_BEARER_TOKEN`/`XGOVERNOR_TENANT_TOKENS_JSON` env-var pair
//! configured. Adding a TOML field with no consumer would just be dead config;
//! the remaining §4 fields land here once something actually reads them.
//!
//! Example file:
//!
//! ```toml
//! [admin]
//! tokens = ["admin-token-1", "admin-token-2"]  # supports rotation
//!
//! [[tenant]]
//! tenant_id = "acme"
//! tokens = ["acme-token-a", "acme-token-b"]
//! principal = "acme-ops"          # optional, defaults to "tenant"
//! max_sessions = 20               # optional, defaults to unlimited
//! max_requests_per_minute = 120   # optional, defaults to unlimited
//!
//! [[tenant]]
//! tenant_id = "beta"
//! tokens = ["beta-token"]
//! ```

use std::collections::{HashMap, HashSet};
use std::path::Path;

use serde::{Deserialize, Serialize};
use xgovernor_core::{SecurityContext, TenantQuota};

/// Label baked into every admin `SecurityContext` sourced from this file —
/// audit-only (`SecurityContext::principal` is never used for authorization
/// decisions), mirrors the old `"env:XGOVERNOR_BEARER_TOKEN"` fixed label.
/// Unlike tenants, admin tokens don't carry a per-entry `principal` override:
/// there's exactly one admin identity, multiple tokens are for rotation, not
/// distinguishing operators.
const ADMIN_PRINCIPAL_LABEL: &str = "tenants.toml:admin";

fn default_principal() -> String {
    "tenant".to_string()
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TenantsFile {
    #[serde(default)]
    pub(crate) admin: AdminSection,
    #[serde(default)]
    pub(crate) tenant: Vec<TenantSection>,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AdminSection {
    #[serde(default)]
    pub(crate) tokens: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TenantSection {
    pub(crate) tenant_id: String,
    pub(crate) tokens: Vec<String>,
    #[serde(default = "default_principal")]
    pub(crate) principal: String,
    #[serde(default)]
    pub(crate) max_sessions: Option<u32>,
    #[serde(default)]
    pub(crate) max_requests_per_minute: Option<u32>,
}

#[derive(Debug)]
pub enum TenantConfigError {
    /// Wraps the raw `std::fs::read_to_string` error — callers that need to
    /// special-case "file does not exist" (`main.rs`'s default-path dev-mode
    /// fallback) match on `.kind() == std::io::ErrorKind::NotFound` here.
    Read(std::io::Error),
    Parse(toml::de::Error),
    /// Same token string appears twice (as two admin tokens, two tenant
    /// tokens, or one of each) — silently letting the later entry win would
    /// hide a copy-paste mistake that lets one identity impersonate another.
    DuplicateToken(String),
    /// Same `tenant_id` appears in more than one `[[tenant]]` block — which
    /// quota/principal would apply is ambiguous, so this is rejected rather
    /// than picking one arbitrarily.
    DuplicateTenantId(String),
    /// A `[[tenant]]` block with zero tokens can never be reached by any
    /// request — almost certainly a mistake, not an intentionally-disabled
    /// tenant (comment the block out instead).
    EmptyTokenList(String),
    /// Serializing the updated [`TenantsFile`] or writing/renaming the temp
    /// file failed (`httpserver::admin_tenants`'s write path via
    /// [`write_tenants_file_atomically`]) — disk full, permissions, the
    /// directory disappeared, etc. Distinct from `Read`: this is the admin
    /// API failing to persist a change, not a startup/reload failing to
    /// load one.
    Write(std::io::Error),
}

impl std::fmt::Display for TenantConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Read(error) => write!(f, "could not read tenants config file: {error}"),
            Self::Parse(error) => write!(f, "could not parse tenants config file: {error}"),
            Self::DuplicateToken(token) => write!(
                f,
                "token {token:?} is configured more than once (as an admin and/or tenant \
                 credential) — every token must resolve to exactly one identity"
            ),
            Self::DuplicateTenantId(tenant_id) => write!(
                f,
                "tenant_id {tenant_id:?} appears in more than one [[tenant]] block — merge them \
                 into a single block"
            ),
            Self::EmptyTokenList(tenant_id) => write!(
                f,
                "tenant {tenant_id:?} has an empty tokens list — it can never be reached by any \
                 request; remove the block or give it at least one token"
            ),
            Self::Write(error) => write!(f, "could not write tenants config file: {error}"),
        }
    }
}

impl std::error::Error for TenantConfigError {}

/// Reads and parses `path`, then validates it into a flat
/// token → [`SecurityContext`] map — the exact shape [`super::auth::TokenTable`]
/// stores, whether this is the initial load at startup or a `SIGHUP` reload.
pub fn load_tenants_file(
    path: &Path,
) -> Result<HashMap<String, SecurityContext>, TenantConfigError> {
    into_entries(read_tenants_file_raw(path)?)
}

/// Reads and parses `path` into the raw [`TenantsFile`] shape, *without* the
/// duplicate-token/duplicate-tenant-id/empty-token-list validation
/// [`into_entries`] does — `httpserver::admin_tenants`'s create/patch/delete
/// handlers need the raw, still-mutable struct (to add/edit/remove one
/// `[[tenant]]` block) before re-running that same validation on the result
/// via `into_entries`, so the read and validate steps are split apart here
/// rather than folded together the way [`load_tenants_file`] folds them for
/// its read-only callers (startup, `SIGHUP`).
pub(crate) fn read_tenants_file_raw(path: &Path) -> Result<TenantsFile, TenantConfigError> {
    let contents = std::fs::read_to_string(path).map_err(TenantConfigError::Read)?;
    toml::from_str(&contents).map_err(TenantConfigError::Parse)
}

pub(crate) fn write_tenants_file_atomically(
    path: &Path,
    file: &TenantsFile,
) -> Result<(), TenantConfigError> {
    let contents = toml::to_string_pretty(file).map_err(|error| {
        TenantConfigError::Write(std::io::Error::new(std::io::ErrorKind::InvalidData, error))
    })?;
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let mut temp_file = tempfile::NamedTempFile::new_in(dir).map_err(TenantConfigError::Write)?;
    std::io::Write::write_all(&mut temp_file, contents.as_bytes())
        .map_err(TenantConfigError::Write)?;
    temp_file
        .persist(path)
        .map_err(|error| TenantConfigError::Write(error.error))?;
    Ok(())
}

pub(crate) fn into_entries(
    file: TenantsFile,
) -> Result<HashMap<String, SecurityContext>, TenantConfigError> {
    let mut entries = HashMap::new();

    for token in file.admin.tokens {
        if entries
            .insert(token.clone(), SecurityContext::admin(ADMIN_PRINCIPAL_LABEL))
            .is_some()
        {
            return Err(TenantConfigError::DuplicateToken(token));
        }
    }

    let mut seen_tenant_ids = HashSet::new();
    for tenant in file.tenant {
        if !seen_tenant_ids.insert(tenant.tenant_id.clone()) {
            return Err(TenantConfigError::DuplicateTenantId(tenant.tenant_id));
        }
        if tenant.tokens.is_empty() {
            return Err(TenantConfigError::EmptyTokenList(tenant.tenant_id));
        }
        let quota = TenantQuota {
            max_sessions: tenant.max_sessions,
            max_requests_per_minute: tenant.max_requests_per_minute,
        };
        for token in tenant.tokens {
            let ctx = SecurityContext::tenant(tenant.tenant_id.clone(), tenant.principal.clone())
                .with_quota(quota);
            if entries.insert(token.clone(), ctx).is_some() {
                return Err(TenantConfigError::DuplicateToken(token));
            }
        }
    }

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use xgovernor_core::Role;

    fn write_temp_toml(contents: &str) -> tempfile::TempPath {
        let mut file = tempfile::NamedTempFile::new().expect("create temp file");
        std::io::Write::write_all(&mut file, contents.as_bytes()).expect("write temp file");
        file.into_temp_path()
    }

    #[test]
    fn admin_and_tenant_tokens_resolve_to_the_right_identities() {
        let path = write_temp_toml(
            r#"
            [admin]
            tokens = ["admin-token"]

            [[tenant]]
            tenant_id = "tenant-a"
            tokens = ["t-a"]
            principal = "alice"

            [[tenant]]
            tenant_id = "tenant-b"
            tokens = ["t-b"]
            "#,
        );
        let entries = load_tenants_file(&path).expect("must load");
        assert_eq!(entries.len(), 3);

        let admin = &entries["admin-token"];
        assert_eq!(admin.role, Role::Admin);

        let tenant_a = &entries["t-a"];
        assert_eq!(tenant_a.role, Role::Tenant);
        assert_eq!(tenant_a.tenant_id(), Some("tenant-a"));
        assert_eq!(tenant_a.principal, "alice");

        let tenant_b = &entries["t-b"];
        assert_eq!(
            tenant_b.principal, "tenant",
            "principal defaults when omitted"
        );
    }

    #[test]
    fn admin_section_is_optional() {
        let path = write_temp_toml(
            r#"
            [[tenant]]
            tenant_id = "tenant-a"
            tokens = ["t-a"]
            "#,
        );
        let entries = load_tenants_file(&path).expect("must load");
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn empty_file_yields_empty_entries_not_an_error() {
        let path = write_temp_toml("");
        let entries = load_tenants_file(&path).expect("must load");
        assert!(entries.is_empty());
    }

    #[test]
    fn quota_fields_default_to_unlimited_and_can_be_set() {
        let path = write_temp_toml(
            r#"
            [[tenant]]
            tenant_id = "tenant-a"
            tokens = ["t-a"]
            max_sessions = 5
            max_requests_per_minute = 30

            [[tenant]]
            tenant_id = "tenant-b"
            tokens = ["t-b"]
            "#,
        );
        let entries = load_tenants_file(&path).expect("must load");
        assert_eq!(entries["t-a"].quota.max_sessions, Some(5));
        assert_eq!(entries["t-a"].quota.max_requests_per_minute, Some(30));
        assert_eq!(
            entries["t-b"].quota.max_sessions, None,
            "omitted quota fields mean unlimited"
        );
        assert_eq!(entries["t-b"].quota.max_requests_per_minute, None);
    }

    #[test]
    fn duplicate_token_across_tenants_is_rejected() {
        let path = write_temp_toml(
            r#"
            [[tenant]]
            tenant_id = "tenant-a"
            tokens = ["shared-token"]

            [[tenant]]
            tenant_id = "tenant-b"
            tokens = ["shared-token"]
            "#,
        );
        let error = load_tenants_file(&path).unwrap_err();
        assert!(matches!(error, TenantConfigError::DuplicateToken(t) if t == "shared-token"));
    }

    #[test]
    fn duplicate_token_between_admin_and_tenant_is_rejected() {
        let path = write_temp_toml(
            r#"
            [admin]
            tokens = ["shared-token"]

            [[tenant]]
            tenant_id = "tenant-a"
            tokens = ["shared-token"]
            "#,
        );
        let error = load_tenants_file(&path).unwrap_err();
        assert!(matches!(error, TenantConfigError::DuplicateToken(t) if t == "shared-token"));
    }

    #[test]
    fn duplicate_tenant_id_is_rejected() {
        let path = write_temp_toml(
            r#"
            [[tenant]]
            tenant_id = "tenant-a"
            tokens = ["t-a-1"]

            [[tenant]]
            tenant_id = "tenant-a"
            tokens = ["t-a-2"]
            "#,
        );
        let error = load_tenants_file(&path).unwrap_err();
        assert!(matches!(error, TenantConfigError::DuplicateTenantId(t) if t == "tenant-a"));
    }

    #[test]
    fn empty_tokens_list_is_rejected() {
        let path = write_temp_toml(
            r#"
            [[tenant]]
            tenant_id = "tenant-a"
            tokens = []
            "#,
        );
        let error = load_tenants_file(&path).unwrap_err();
        assert!(matches!(error, TenantConfigError::EmptyTokenList(t) if t == "tenant-a"));
    }

    #[test]
    fn unknown_field_is_a_parse_error_not_silently_ignored() {
        let path = write_temp_toml(
            r#"
            [[tenant]]
            tenant_id = "tenant-a"
            tokens = ["t-a"]
            max_session = 5
            "#,
        );
        assert!(matches!(
            load_tenants_file(&path).unwrap_err(),
            TenantConfigError::Parse(_)
        ));
    }

    #[test]
    fn malformed_toml_is_a_parse_error() {
        let path = write_temp_toml("this is not [ valid toml");
        assert!(matches!(
            load_tenants_file(&path).unwrap_err(),
            TenantConfigError::Parse(_)
        ));
    }

    #[test]
    fn missing_file_is_a_not_found_read_error() {
        let missing = std::env::temp_dir().join("xgovernor-tenants-config-does-not-exist.toml");
        let error = load_tenants_file(&missing).unwrap_err();
        match error {
            TenantConfigError::Read(io_error) => {
                assert_eq!(io_error.kind(), std::io::ErrorKind::NotFound);
            }
            other => panic!("expected Read(NotFound), got {other:?}"),
        }
    }
}
