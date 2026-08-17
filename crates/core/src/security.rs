//! The server-side identity fact threaded through the application layer.
//!
//! Normative design: `docs/tenancy_design.md` §0–§3. The load-bearing rule is
//! §1: **wire requests never carry a `tenant_id` field.** A `SecurityContext`
//! is never deserialized from a client payload — it is the *result* of a
//! transport-layer credential check (today: `apps/server`'s bearer-token
//! table), constructed exactly once per request and passed to every
//! [`crate::SessionApplication`] method as an independent parameter, never
//! merged into a request DTO. Whoever "declares" a tenant in a request body
//! has already breached the boundary; this type exists so that declaration
//! is structurally impossible.

use serde::{Deserialize, Serialize};

/// admin 百无禁忌
pub const ADMIN_OWNER_REF: &str = "admin";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantQuota {
    #[serde(default)]
    pub max_sessions: Option<u32>,
    #[serde(default)]
    pub max_requests_per_minute: Option<u32>,
}

/// The two trust worlds of `docs/tenancy_design.md` §0.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// World A: the confirmed local-deployment operator. Trusted without
    /// restriction once authenticated — bypasses every tenant-ownership
    /// check (`SecurityContext::owns` always returns `true`).
    Admin,
    /// World B: every other caller. Scoped to exactly one tenant; every
    /// session lookup is filtered by `tenant_id`.
    Tenant,
}

/// The authenticated caller of the current request. Constructed once by the
/// transport-layer auth middleware and passed down as a plain parameter.
///
/// `tenant_id` is deliberately private: the only way to read it is
/// [`SecurityContext::tenant_id`], and the only way to ask "can this caller
/// see that record" is [`SecurityContext::owns`] — collapsing every
/// ownership decision in the codebase onto one function instead of letting
/// call sites compare `Option<String>`s by hand (and inevitably get an edge
/// case, e.g. `None == None`, wrong).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecurityContext {
    pub role: Role,
    tenant_id: Option<String>,
    /// Human-readable identity label carried for audit only
    /// (`docs/tenancy_design.md` §6) — never used for authorization
    /// decisions.
    pub principal: String,
    /// `docs/tenancy_design.md` §7 step 4. Unlimited (`TenantQuota::default()`)
    /// unless set via [`SecurityContext::with_quota`] — admission checks in
    /// `SessionApplication` always compare against this, but `is_admin()`
    /// bypasses them regardless of what this field holds (§0: admin is
    /// "百无禁忌").
    pub quota: TenantQuota,
}

impl SecurityContext {
    pub fn admin(principal: impl Into<String>) -> Self {
        Self {
            role: Role::Admin,
            tenant_id: None,
            principal: principal.into(),
            quota: TenantQuota::default(),
        }
    }

    pub fn tenant(tenant_id: impl Into<String>, principal: impl Into<String>) -> Self {
        Self {
            role: Role::Tenant,
            tenant_id: Some(tenant_id.into()),
            principal: principal.into(),
            quota: TenantQuota::default(),
        }
    }

    /// Attach a quota ceiling. Opt-in builder (mirrors `SessionApplication::
    /// with_lease_table`) so every existing `admin(..)`/`tenant(..)` call
    /// site keeps its current unlimited behavior unless it explicitly asks
    /// for a limit.
    pub fn with_quota(mut self, quota: TenantQuota) -> Self {
        self.quota = quota;
        self
    }

    pub fn is_admin(&self) -> bool {
        matches!(self.role, Role::Admin)
    }

    /// `None` for an admin context — §0 does not scope admin to a tenant.
    pub fn tenant_id(&self) -> Option<&str> {
        self.tenant_id.as_deref()
    }

    /// Whether this caller may see/operate on a record whose owner is
    /// `record_tenant_id` (`None` = an admin-opened record). Admin always
    /// passes; a tenant context passes only against its own `tenant_id`.
    /// This is the sole ownership predicate in the codebase — every
    /// `SessionApplication::require_session` call funnels through it
    /// (`docs/tenancy_design.md` §3.2).
    pub fn owns(&self, record_tenant_id: Option<&str>) -> bool {
        match self.role {
            Role::Admin => true,
            Role::Tenant => self.tenant_id.as_deref() == record_tenant_id,
        }
    }

    pub fn owner_ref(&self) -> String {
        match self.role {
            Role::Admin => ADMIN_OWNER_REF.to_string(),
            Role::Tenant => format!("tenant/{}", self.tenant_id.as_deref().unwrap_or_default()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admin_owns_everything_including_other_tenants_and_admin_records() {
        let admin = SecurityContext::admin("root");
        assert!(admin.owns(None));
        assert!(admin.owns(Some("tenant-a")));
    }

    #[test]
    fn tenant_owns_only_its_own_records() {
        let tenant_a = SecurityContext::tenant("tenant-a", "alice");
        assert!(tenant_a.owns(Some("tenant-a")));
        assert!(!tenant_a.owns(Some("tenant-b")));
        assert!(
            !tenant_a.owns(None),
            "an admin-opened (tenant_id: None) record must not be visible to a tenant"
        );
    }

    #[test]
    fn tenant_id_accessor_is_none_for_admin() {
        assert_eq!(SecurityContext::admin("root").tenant_id(), None);
        assert_eq!(
            SecurityContext::tenant("tenant-a", "alice").tenant_id(),
            Some("tenant-a")
        );
    }

    #[test]
    fn owner_ref_is_mechanically_derived_not_client_supplied() {
        assert_eq!(SecurityContext::admin("root").owner_ref(), ADMIN_OWNER_REF);
        assert_eq!(
            SecurityContext::tenant("tenant-a", "alice").owner_ref(),
            "tenant/tenant-a"
        );
        // Different principals under the same tenant must derive the same
        // owner_ref — quota is scoped to the tenant, not the individual
        // caller.
        assert_eq!(
            SecurityContext::tenant("tenant-a", "bob").owner_ref(),
            "tenant/tenant-a"
        );
    }
}
