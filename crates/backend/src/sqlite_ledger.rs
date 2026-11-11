//! SQLite-backed [`ProviderInstanceLedger`].
//!
//! Mirrors `crates/core/src/sqlite_repository.rs`'s shape deliberately
//! (open/open_in_memory/init, `Arc<Mutex<Connection>>`, `spawn_blocking`
//! wrapping every call, WAL mode) — the two crates are architectural
//! siblings (neither depends on the other) and can each open an
//! independent `rusqlite::Connection` onto the *same* physical file under
//! `~/.xgovernor` in WAL mode without any new shared crate to hold one
//! connection object; see the assembly-layer wiring in
//! `apps/server/src/main.rs` for where both paths converge.
//!

use crate::ledger::{ActiveLedgerEntry, ProviderInstanceLedger};
use async_trait::async_trait;
use provider_protocol::{
    BackendId, ProviderCapabilities, ProviderControlError, ProviderEndpoint, ProviderInstance,
    ProviderInstanceId, ProviderKind, ProviderLifecycleState, ProviderResourceAllocation,
    ProviderSnapshot,
};
use rusqlite::{params, Connection, Row};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

pub struct SqliteProviderInstanceLedger {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteProviderInstanceLedger {
    /// Opens (creating if absent, including parent directories) the SQLite
    /// file at `path`, creates the `provider_instances` table if it doesn't
    /// exist yet, and enables WAL mode.
    pub fn open(path: &Path) -> rusqlite::Result<Self> {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let conn = Connection::open(path)?;
        Self::init(&conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// In-memory database. For tests only.
    pub fn open_in_memory() -> rusqlite::Result<Self> {
        let conn = Connection::open_in_memory()?;
        Self::init(&conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    fn init(conn: &Connection) -> rusqlite::Result<()> {
        let _ = conn.pragma_update(None, "journal_mode", "WAL");
        let _ = conn.pragma_update(None, "busy_timeout", 5_000i64);
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS provider_instances (
                runtime_id TEXT PRIMARY KEY,
                owner_ref TEXT NOT NULL,
                backend_id TEXT NOT NULL,
                provider TEXT NOT NULL,
                instance_id TEXT NOT NULL,
                state TEXT NOT NULL,
                desired_state TEXT NOT NULL,
                endpoint_json TEXT,
                snapshot_json TEXT,
                capabilities_json TEXT NOT NULL,
                resources_json TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_provider_instances_owner_ref
                ON provider_instances(owner_ref);",
        )
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

/// `ProviderLifecycleState` already derives `Serialize`/`Deserialize` with
/// `snake_case` fieldless variants, so `serde_json` round-trips it to a
/// quoted string (`"active"`) without a hand-written string mapping.
fn state_to_column(state: ProviderLifecycleState) -> String {
    serde_json::to_string(&state).unwrap_or_else(|_| "\"unknown\"".to_string())
}

fn transport_error(message: impl Into<String>) -> ProviderControlError {
    ProviderControlError::Transport {
        message: message.into(),
    }
}

/// Inverse of the JSON columns `record_created` writes. Column order must
/// match the `SELECT` in [`SqliteProviderInstanceLedger::list_active`].
fn row_to_active_entry(row: &Row<'_>) -> rusqlite::Result<Result<ActiveLedgerEntry, String>> {
    let runtime_id: String = row.get(0)?;
    let owner_ref: String = row.get(1)?;
    let backend_id: String = row.get(2)?;
    let provider: String = row.get(3)?;
    let instance_id: String = row.get(4)?;
    let state_str: String = row.get(5)?;
    let endpoint_json: Option<String> = row.get(6)?;
    let snapshot_json: Option<String> = row.get(7)?;
    let capabilities_json: String = row.get(8)?;
    let resources_json: String = row.get(9)?;
    let metadata_json: String = row.get(10)?;
    let created_at_ms: i64 = row.get(11)?;
    let updated_at_ms: i64 = row.get(12)?;

    let parsed = (|| -> Result<ActiveLedgerEntry, serde_json::Error> {
        let state: ProviderLifecycleState = serde_json::from_str(&state_str)?;
        let endpoint: Option<ProviderEndpoint> = endpoint_json
            .as_deref()
            .map(serde_json::from_str)
            .transpose()?;
        let snapshot: Option<ProviderSnapshot> = snapshot_json
            .as_deref()
            .map(serde_json::from_str)
            .transpose()?;
        let capabilities: ProviderCapabilities = serde_json::from_str(&capabilities_json)?;
        let resources: ProviderResourceAllocation = serde_json::from_str(&resources_json)?;
        let metadata: serde_json::Value = serde_json::from_str(&metadata_json)?;

        Ok(ActiveLedgerEntry {
            runtime_id,
            owner_ref,
            instance: ProviderInstance {
                backend_id: BackendId(backend_id),
                provider: ProviderKind(provider),
                instance_id: ProviderInstanceId(instance_id),
                state,
                endpoint,
                snapshot,
                capabilities,
                resources,
                metadata,
                created_at_ms: created_at_ms as u64,
                updated_at_ms: updated_at_ms as u64,
            },
        })
    })();

    Ok(parsed.map_err(|error| error.to_string()))
}

#[async_trait]
impl ProviderInstanceLedger for SqliteProviderInstanceLedger {
    async fn record_created(
        &self,
        runtime_id: &str,
        owner_ref: &str,
        instance: &ProviderInstance,
    ) -> Result<(), ProviderControlError> {
        let conn = self.conn.clone();
        let runtime_id = runtime_id.to_string();
        let owner_ref = owner_ref.to_string();
        let instance = instance.clone();

        let result = tokio::task::spawn_blocking(move || -> Result<(), String> {
            let state_str = state_to_column(instance.state);
            let desired_state_str = state_to_column(ProviderLifecycleState::Active);
            let endpoint_json = instance
                .endpoint
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(|error| error.to_string())?;
            let snapshot_json = instance
                .snapshot
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(|error| error.to_string())?;
            let capabilities_json =
                serde_json::to_string(&instance.capabilities).map_err(|error| error.to_string())?;
            let resources_json =
                serde_json::to_string(&instance.resources).map_err(|error| error.to_string())?;
            let metadata_json =
                serde_json::to_string(&instance.metadata).map_err(|error| error.to_string())?;

            let conn = conn
                .lock()
                .map_err(|_| "sqlite provider instance ledger lock poisoned".to_string())?;
            conn.execute(
                "INSERT INTO provider_instances (
                    runtime_id, owner_ref, backend_id, provider, instance_id,
                    state, desired_state, endpoint_json, snapshot_json,
                    capabilities_json, resources_json, metadata_json,
                    created_at_ms, updated_at_ms
                ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)
                ON CONFLICT(runtime_id) DO UPDATE SET
                    owner_ref = excluded.owner_ref,
                    backend_id = excluded.backend_id,
                    provider = excluded.provider,
                    instance_id = excluded.instance_id,
                    state = excluded.state,
                    desired_state = excluded.desired_state,
                    endpoint_json = excluded.endpoint_json,
                    snapshot_json = excluded.snapshot_json,
                    capabilities_json = excluded.capabilities_json,
                    resources_json = excluded.resources_json,
                    metadata_json = excluded.metadata_json,
                    updated_at_ms = excluded.updated_at_ms",
                params![
                    runtime_id,
                    owner_ref,
                    instance.backend_id.0,
                    instance.provider.0,
                    instance.instance_id.0,
                    state_str,
                    desired_state_str,
                    endpoint_json,
                    snapshot_json,
                    capabilities_json,
                    resources_json,
                    metadata_json,
                    instance.created_at_ms as i64,
                    instance.updated_at_ms as i64,
                ],
            )
            .map_err(|error| error.to_string())?;
            Ok(())
        })
        .await
        .map_err(|error| {
            transport_error(format!("ledger record_created task panicked: {error}"))
        })?;

        result.map_err(|error| transport_error(format!("ledger record_created failed: {error}")))
    }

    async fn record_deleted(&self, runtime_id: &str) -> Result<(), ProviderControlError> {
        let conn = self.conn.clone();
        let runtime_id = runtime_id.to_string();
        let updated_at_ms = now_ms() as i64;

        let result = tokio::task::spawn_blocking(move || -> Result<(), String> {
            let deleted_state = state_to_column(ProviderLifecycleState::Deleted);
            let conn = conn
                .lock()
                .map_err(|_| "sqlite provider instance ledger lock poisoned".to_string())?;
            // No-op (not an error) if `runtime_id` has no row — see the
            // trait doc on `record_deleted` for why.
            conn.execute(
                "UPDATE provider_instances
                 SET state = ?1, desired_state = ?1, updated_at_ms = ?2
                 WHERE runtime_id = ?3",
                params![deleted_state, updated_at_ms, runtime_id],
            )
            .map_err(|error| error.to_string())?;
            Ok(())
        })
        .await
        .map_err(|error| {
            transport_error(format!("ledger record_deleted task panicked: {error}"))
        })?;

        result.map_err(|error| transport_error(format!("ledger record_deleted failed: {error}")))
    }

    async fn list_active(
        &self,
        provider: &ProviderKind,
    ) -> Result<Vec<ActiveLedgerEntry>, ProviderControlError> {
        let conn = self.conn.clone();
        let active_state = state_to_column(ProviderLifecycleState::Active);
        let provider = provider.0.clone();

        let result =
            tokio::task::spawn_blocking(move || -> Result<Vec<ActiveLedgerEntry>, String> {
                let conn = conn
                    .lock()
                    .map_err(|_| "sqlite provider instance ledger lock poisoned".to_string())?;
                let mut statement = conn
                    .prepare(
                        "SELECT runtime_id, owner_ref, backend_id, provider, instance_id, state,
                            endpoint_json, snapshot_json, capabilities_json, resources_json,
                            metadata_json, created_at_ms, updated_at_ms
                     FROM provider_instances
                     WHERE desired_state = ?1 AND provider = ?2",
                    )
                    .map_err(|error| error.to_string())?;
                let rows = statement
                    .query_map(params![active_state, provider], row_to_active_entry)
                    .map_err(|error| error.to_string())?;

                let mut entries = Vec::new();
                for row in rows {
                    let entry = row.map_err(|error| error.to_string())?;
                    entries.push(
                        entry.map_err(|error| format!("row deserialization failed: {error}"))?,
                    );
                }
                Ok(entries)
            })
            .await
            .map_err(|error| {
                transport_error(format!("ledger list_active task panicked: {error}"))
            })?;

        result.map_err(|error| transport_error(format!("ledger list_active failed: {error}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use provider_protocol::{
        BackendId, ProviderCapabilities, ProviderInstanceId, ProviderKind,
        ProviderOperationCapabilities, ProviderResourceAllocation,
    };

    fn sample_instance(instance_id: &str) -> ProviderInstance {
        ProviderInstance {
            backend_id: BackendId("local".to_string()),
            provider: ProviderKind("local".to_string()),
            instance_id: ProviderInstanceId(instance_id.to_string()),
            state: ProviderLifecycleState::Active,
            endpoint: None,
            snapshot: None,
            capabilities: ProviderCapabilities {
                lifecycle: Default::default(),
                operation_plane: ProviderOperationCapabilities::default(),
            },
            resources: ProviderResourceAllocation::default(),
            metadata: serde_json::json!({"k": "v"}),
            created_at_ms: 1,
            updated_at_ms: 2,
        }
    }

    #[tokio::test]
    async fn record_created_then_read_back_via_raw_query() {
        let ledger = SqliteProviderInstanceLedger::open_in_memory().expect("open in-memory db");
        ledger
            .record_created(
                "runtime-1",
                "tenant/owner-1",
                &sample_instance("instance-1"),
            )
            .await
            .expect("record_created must succeed");

        let conn = ledger.conn.lock().unwrap();
        let (owner_ref, state, desired_state): (String, String, String) = conn
            .query_row(
                "SELECT owner_ref, state, desired_state FROM provider_instances WHERE runtime_id = ?1",
                params!["runtime-1"],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("row must exist");
        assert_eq!(owner_ref, "tenant/owner-1");
        assert_eq!(state, "\"active\"");
        assert_eq!(desired_state, "\"active\"");
    }

    #[tokio::test]
    async fn record_created_twice_upserts_rather_than_conflicting() {
        let ledger = SqliteProviderInstanceLedger::open_in_memory().expect("open in-memory db");
        ledger
            .record_created(
                "runtime-1",
                "tenant/owner-1",
                &sample_instance("instance-1"),
            )
            .await
            .expect("first record_created must succeed");
        ledger
            .record_created(
                "runtime-1",
                "tenant/owner-1",
                &sample_instance("instance-2"),
            )
            .await
            .expect("second record_created must succeed");

        let conn = ledger.conn.lock().unwrap();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM provider_instances WHERE runtime_id = ?1",
                params!["runtime-1"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "upsert must not create a second row");

        let instance_id: String = conn
            .query_row(
                "SELECT instance_id FROM provider_instances WHERE runtime_id = ?1",
                params!["runtime-1"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(instance_id, "instance-2");
    }

    #[tokio::test]
    async fn record_deleted_soft_deletes_an_existing_row() {
        let ledger = SqliteProviderInstanceLedger::open_in_memory().expect("open in-memory db");
        ledger
            .record_created(
                "runtime-1",
                "tenant/owner-1",
                &sample_instance("instance-1"),
            )
            .await
            .expect("record_created must succeed");

        ledger
            .record_deleted("runtime-1")
            .await
            .expect("record_deleted must succeed");

        let conn = ledger.conn.lock().unwrap();
        let (state, desired_state): (String, String) = conn
            .query_row(
                "SELECT state, desired_state FROM provider_instances WHERE runtime_id = ?1",
                params!["runtime-1"],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("row must still exist (soft delete)");
        assert_eq!(state, "\"deleted\"");
        assert_eq!(desired_state, "\"deleted\"");
    }

    #[tokio::test]
    async fn record_deleted_on_unknown_runtime_id_is_not_an_error() {
        let ledger = SqliteProviderInstanceLedger::open_in_memory().expect("open in-memory db");
        ledger
            .record_deleted("does-not-exist")
            .await
            .expect("record_deleted on an unknown runtime_id must be a no-op, not an error");

        let conn = ledger.conn.lock().unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM provider_instances", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 0, "no row should have been created");
    }

    #[tokio::test]
    async fn list_active_returns_only_active_rows_with_full_instance_data() {
        let ledger = SqliteProviderInstanceLedger::open_in_memory().expect("open in-memory db");
        ledger
            .record_created(
                "runtime-1",
                "tenant/owner-1",
                &sample_instance("instance-1"),
            )
            .await
            .expect("record_created must succeed");
        ledger
            .record_created(
                "runtime-2",
                "tenant/owner-2",
                &sample_instance("instance-2"),
            )
            .await
            .expect("record_created must succeed");
        ledger
            .record_deleted("runtime-2")
            .await
            .expect("record_deleted must succeed");

        let mut active = ledger
            .list_active(&ProviderKind("local".to_string()))
            .await
            .expect("list_active must succeed");
        active.sort_by(|a, b| a.runtime_id.cmp(&b.runtime_id));

        assert_eq!(active.len(), 1, "deleted rows must not be returned");
        assert_eq!(active[0].runtime_id, "runtime-1");
        assert_eq!(active[0].owner_ref, "tenant/owner-1");
        assert_eq!(active[0].instance.instance_id.0, "instance-1");
        assert_eq!(active[0].instance.state, ProviderLifecycleState::Active);
        assert_eq!(active[0].instance.metadata, serde_json::json!({"k": "v"}));
    }

    #[tokio::test]
    async fn list_active_on_an_empty_ledger_is_an_empty_list() {
        let ledger = SqliteProviderInstanceLedger::open_in_memory().expect("open in-memory db");
        let active = ledger
            .list_active(&ProviderKind("local".to_string()))
            .await
            .expect("list_active must succeed");
        assert!(active.is_empty());
    }

    /// The regression test for the real E2E finding: two `InstanceManager`s
    /// (one per backend, e.g. `local` + `e2b`) share one physical ledger
    /// table. Without a `provider` filter, one backend's `reconcile()` would
    /// see (and soft-delete as an orphan) rows that belong to the *other*
    /// backend entirely.
    #[tokio::test]
    async fn list_active_only_returns_rows_for_the_requested_provider() {
        let ledger = SqliteProviderInstanceLedger::open_in_memory().expect("open in-memory db");
        let mut local_instance = sample_instance("local-instance-1");
        local_instance.provider = ProviderKind("local".to_string());
        let mut e2b_instance = sample_instance("e2b-instance-1");
        e2b_instance.provider = ProviderKind("e2b".to_string());

        ledger
            .record_created("runtime-local-1", "tenant/owner-1", &local_instance)
            .await
            .expect("record_created must succeed");
        ledger
            .record_created("runtime-e2b-1", "tenant/owner-1", &e2b_instance)
            .await
            .expect("record_created must succeed");

        let local_active = ledger
            .list_active(&ProviderKind("local".to_string()))
            .await
            .expect("list_active must succeed");
        assert_eq!(local_active.len(), 1);
        assert_eq!(local_active[0].runtime_id, "runtime-local-1");

        let e2b_active = ledger
            .list_active(&ProviderKind("e2b".to_string()))
            .await
            .expect("list_active must succeed");
        assert_eq!(e2b_active.len(), 1);
        assert_eq!(e2b_active[0].runtime_id, "runtime-e2b-1");
    }

    #[tokio::test]
    async fn survives_reopening_the_same_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("nested").join("xgovernor.db");

        {
            let ledger = SqliteProviderInstanceLedger::open(&path).expect("open must succeed");
            ledger
                .record_created(
                    "runtime-1",
                    "tenant/owner-1",
                    &sample_instance("instance-1"),
                )
                .await
                .expect("record_created must succeed");
        }

        let ledger = SqliteProviderInstanceLedger::open(&path).expect("reopen must succeed");
        let conn = ledger.conn.lock().unwrap();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM provider_instances WHERE runtime_id = ?1",
                params!["runtime-1"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "record must have survived reopening the file");
    }
}
