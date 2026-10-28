//! Admin tenant-management wire contract (`docs/tenancy_design.md` §4's
//! closing line — "...需要自助开通时再考虑数据库与管理 API，现在建即是过度
//! 工程" — this is that management API, now that a real user asked for it).
//! Create/patch/delete a `[[tenant]]` block in `tenants.toml` from the
//! admin-only HTTP surface; see `apps/server/src/httpserver/admin_tenants.rs`
//! for the handlers that produce/consume these types.

use serde::{Deserialize, Deserializer, Serialize};
use std::collections::BTreeMap;

/// Backend IDs supported by the server's tenant sandbox policy. Unknown
/// names are rejected so a typo cannot silently remove a quota override.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SandboxBackend {
    Local,
    E2b,
    Docker,
}

pub type SandboxLimits = BTreeMap<SandboxBackend, u32>;

/// `POST /api/v1/admin/tenants` request. `tenant_id` must not collide with
/// an existing `[[tenant]]` block. `principal` defaults to `"tenant"` when
/// omitted — the same default `tenants.toml` itself uses for a block with no
/// `principal` key. Session/rate quotas default to unlimited (`None`). Sandbox
/// entries override each backend's default; omitted entries inherit it.
/// The bearer token itself is never
/// part of this request — see [`TenantCreateResponse::token`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TenantCreateRequest {
    pub tenant_id: String,
    #[serde(default)]
    pub principal: Option<String>,
    #[serde(default)]
    pub max_sessions: Option<u32>,
    #[serde(default)]
    pub max_sandboxes: SandboxLimits,
    #[serde(default)]
    pub max_requests_per_minute: Option<u32>,
}

/// `POST /api/v1/admin/tenants` response. `token` is a server-generated
/// bearer credential, returned in plaintext exactly once, here. It is never
/// re-displayed by any later API call (`GET`-ing a tenant back, once that
/// exists, will not include it) — write it down now. There is no rotation
/// endpoint yet; a lost token today means deleting and recreating the
/// tenant.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TenantCreateResponse {
    pub tenant_id: String,
    pub token: String,
    pub principal: String,
    #[serde(default)]
    pub max_sessions: Option<u32>,
    #[serde(default)]
    pub max_sandboxes: SandboxLimits,
    #[serde(default)]
    pub max_requests_per_minute: Option<u32>,
}

/// `PATCH /api/v1/admin/tenants/{tenant_id}` request — partial-update
/// semantics for quota fields. A field absent from the request body
/// leaves the tenant's current value untouched; present as JSON `null`
/// clears it to unlimited; present with a number sets it. Plain
/// `Option<u32>` can't distinguish "absent" from "null" (both deserialize to
/// `None`), so this uses the standard serde "double `Option`" idiom:
/// [`deserialize_present_field`] only runs at all when the key is present in
/// the JSON object, and wraps whatever `Option<u32>` it produces (`None` for
/// `null`, `Some(v)` for a number) in an outer `Some(..)`. `#[serde(default)]`
/// supplies the outer `None` when the key is missing entirely.
/// `max_sandboxes` merges backend entries: a number sets that backend's
/// limit, a null entry restores its default, and a top-level null clears all
/// sandbox overrides. An omitted map or backend leaves its value unchanged.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TenantPatchRequest {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_field"
    )]
    pub max_sessions: Option<Option<u32>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_field"
    )]
    pub max_sandboxes: Option<Option<BTreeMap<SandboxBackend, Option<u32>>>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_field"
    )]
    pub max_requests_per_minute: Option<Option<u32>>,
}

fn deserialize_present_field<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    T: Deserialize<'de>,
    D: Deserializer<'de>,
{
    Deserialize::deserialize(deserializer).map(Some)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TenantPatchResponse {
    pub tenant_id: String,
    pub principal: String,
    #[serde(default)]
    pub max_sessions: Option<u32>,
    #[serde(default)]
    pub max_sandboxes: SandboxLimits,
    #[serde(default)]
    pub max_requests_per_minute: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantDeleteResponse {
    pub tenant_id: String,
}
