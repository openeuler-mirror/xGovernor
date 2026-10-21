//! Local writer ownership for an explicitly provisioned SQLite deployment.
use crate::{JointBudgetPolicy, SessionDomainError, SqliteSessionRepository};
use rusqlite::{params, OptionalExtension};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

fn unavailable() -> SessionDomainError {
    SessionDomainError::Unavailable {
        message: "local deployment ownership or configuration unavailable".into(),
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeploymentBudgetConfiguration {
    pub policy_ref: String,
    pub resources: JointBudgetPolicy,
}
impl DeploymentBudgetConfiguration {
    pub fn parse(input: &str) -> Result<Self, SessionDomainError> {
        let result: Self = serde_json::from_str(input).map_err(|_| unavailable())?;
        result.validate()?;
        Ok(result)
    }
    fn validate(&self) -> Result<(), SessionDomainError> {
        if self.policy_ref.is_empty()
            || self.policy_ref.len() > 256
            || !self
                .policy_ref
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"/._:@+-".contains(&b))
            || self.resources.iter().any(|(kind, b)| {
                ![
                    "session",
                    "root",
                    "replica",
                    "checkpoint",
                    "repair",
                    "disk",
                    "operation",
                    "record_bytes",
                ]
                .contains(&kind.as_str())
                    || b.recovery_reserve > b.total
            })
        {
            return Err(unavailable());
        }
        Ok(())
    }
}

struct LockedObject {
    file: File,
    path: PathBuf,
    device: u64,
    inode: u64,
}
impl LockedObject {
    fn acquire(path: &Path, exclusive: bool) -> Result<Self, SessionDomainError> {
        let path = path.canonicalize().map_err(|_| unavailable())?;
        let file = File::open(&path).map_err(|_| unavailable())?;
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            let mut filesystem: libc::statfs = unsafe { std::mem::zeroed() };
            // Limit object identity and advisory-lock assumptions to local filesystems.
            if unsafe { libc::fstatfs(file.as_raw_fd(), &mut filesystem) } != 0
                || ![0xef53, 0x58465342, 0x9123683e, 0x01021994, 0x794c7630]
                    .contains(&filesystem.f_type)
            {
                return Err(unavailable());
            }
        }
        #[cfg(not(target_os = "linux"))]
        return Err(unavailable());
        if exclusive {
            file.try_lock()
        } else {
            file.try_lock_shared()
        }
        .map_err(|_| unavailable())?;
        let metadata = file.metadata().map_err(|_| unavailable())?;
        if !(metadata.is_file() || metadata.is_dir()) {
            return Err(unavailable());
        }
        let result = Self {
            file,
            path,
            device: metadata.dev(),
            inode: metadata.ino(),
        };
        result.validate()?;
        Ok(result)
    }
    fn validate(&self) -> Result<(), SessionDomainError> {
        let current = std::fs::metadata(&self.path).map_err(|_| unavailable())?;
        let held = self.file.metadata().map_err(|_| unavailable())?;
        if current.dev() != self.device
            || current.ino() != self.inode
            || held.dev() != self.device
            || held.ino() != self.inode
            || held.nlink() == 0
        {
            return Err(unavailable());
        }
        Ok(())
    }
}

/// Held by assembly until the daemon exits, independently of repository connections.
/// Resource directories must already exist and must never be replaced while serving.
pub struct DeploymentGuard {
    objects: Vec<LockedObject>,
    database_path: PathBuf,
    database_identity: String,
    deployment_id: String,
    policy_hash: String,
    policy_ref: String,
    protected_values: Arc<Mutex<BTreeSet<String>>>,
    server_only_values: Mutex<BTreeSet<String>>,
    resource_paths: Vec<PathBuf>,
    data_capacity: u64,
}
impl DeploymentGuard {
    /// Acquires real object locks before opening business admission. No backend is started.
    /// The lock namespace is a pre-provisioned protected directory shared by deployment copies.
    pub fn acquire(
        database_path: &Path,
        deployment_id: &str,
        lock_namespace: &Path,
        resources: &[PathBuf],
        budget: &JointBudgetPolicy,
    ) -> Result<Arc<Self>, SessionDomainError> {
        Self::acquire_with_configuration(
            database_path,
            deployment_id,
            lock_namespace,
            resources,
            &DeploymentBudgetConfiguration {
                policy_ref: "local/assembled".into(),
                resources: budget.clone(),
            },
        )
    }
    pub fn acquire_with_configuration(
        database_path: &Path,
        deployment_id: &str,
        lock_namespace: &Path,
        resources: &[PathBuf],
        configuration: &DeploymentBudgetConfiguration,
    ) -> Result<Arc<Self>, SessionDomainError> {
        configuration.validate()?;
        let budget = &configuration.resources;
        if deployment_id.trim().is_empty() || resources.is_empty() {
            return Err(unavailable());
        }
        let database_path = database_path.canonicalize().map_err(|_| unavailable())?;
        let mut requested = BTreeMap::<(u64, u64), (PathBuf, bool)>::new();
        for root in std::iter::once(&database_path).chain(resources.iter()) {
            let canonical = root.canonicalize().map_err(|_| unavailable())?;
            if root != &database_path && !canonical.is_dir() {
                return Err(unavailable());
            }
            for (index, path) in canonical.ancestors().enumerate() {
                let metadata = std::fs::metadata(path).map_err(|_| unavailable())?;
                let entry = requested
                    .entry((metadata.dev(), metadata.ino()))
                    .or_insert((path.to_path_buf(), false));
                entry.1 |= index == 0;
            }
        }
        let mut objects = requested
            .values()
            .map(|(path, exclusive)| LockedObject::acquire(path, *exclusive))
            .collect::<Result<Vec<_>, _>>()?;
        let namespace = lock_namespace.canonicalize().map_err(|_| unavailable())?;
        if !namespace.is_dir()
            || namespace.metadata().map_err(|_| unavailable())?.mode() & 0o077 != 0
        {
            return Err(unavailable());
        }
        let repository = SqliteSessionRepository::open(&database_path)
            .map_err(|_| unavailable())?
            .with_joint_budget_policy(budget.clone())?;
        let mut connection = repository.conn.lock().map_err(|_| unavailable())?;
        let tx = connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(|_| unavailable())?;
        tx.execute_batch("CREATE TABLE IF NOT EXISTS local_deployment (singleton INTEGER PRIMARY KEY CHECK(singleton=1), database_identity TEXT NOT NULL, deployment_id TEXT NOT NULL, lock_namespace TEXT NOT NULL, budget_hash TEXT NOT NULL, budget_policy_ref TEXT NOT NULL, database_path TEXT NOT NULL, holder_instance_id TEXT NOT NULL, boot_id TEXT NOT NULL, lock_identity_refs TEXT NOT NULL, status TEXT NOT NULL, row_version INTEGER NOT NULL)")
            .map_err(|_| unavailable())?;
        let old: Option<(String, String, String)> = tx.query_row(
            "SELECT database_identity,deployment_id,lock_namespace FROM local_deployment WHERE singleton=1", [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        ).optional().map_err(|_| unavailable())?;
        let database_identity = if let Some((identity, deployment, original_namespace)) = old {
            if deployment != deployment_id || original_namespace != namespace.to_string_lossy() {
                return Err(unavailable());
            }
            uuid::Uuid::parse_str(&identity).map_err(|_| unavailable())?;
            identity
        } else {
            uuid::Uuid::new_v4().to_string()
        };
        // This stable identity lock also excludes consistent database copies at other paths.
        let identity_path = namespace.join(format!("{database_identity}.lock"));
        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&identity_path)
            .map_err(|_| unavailable())?;
        objects.push(LockedObject::acquire(&identity_path, true)?);
        let policy_hash = budget_hash(&configuration.policy_ref, budget);
        let boot_id = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
            .map_err(|_| unavailable())?;
        uuid::Uuid::parse_str(boot_id.trim()).map_err(|_| unavailable())?;
        let locks = serde_json::to_string(
            &objects
                .iter()
                .map(|o| serde_json::json!({"path":o.path,"device":o.device,"inode":o.inode}))
                .collect::<Vec<_>>(),
        )
        .map_err(|_| unavailable())?;
        tx.execute("INSERT INTO local_deployment VALUES(1,?1,?2,?3,?4,?5,?6,?7,?8,?9,'configuration_verified',1) ON CONFLICT(singleton) DO UPDATE SET budget_hash=excluded.budget_hash,budget_policy_ref=excluded.budget_policy_ref,database_path=excluded.database_path,holder_instance_id=excluded.holder_instance_id,boot_id=excluded.boot_id,lock_identity_refs=excluded.lock_identity_refs,status=excluded.status,row_version=local_deployment.row_version+1",
            params![database_identity, deployment_id, namespace.to_string_lossy(), policy_hash,configuration.policy_ref,database_path.to_string_lossy(),uuid::Uuid::new_v4().to_string(),boot_id.trim(),locks])
            .map_err(|_| unavailable())?;
        tx.commit().map_err(|_| unavailable())?;
        let result = Arc::new(Self {
            objects,
            database_path,
            database_identity,
            deployment_id: deployment_id.into(),
            policy_hash,
            policy_ref: configuration.policy_ref.clone(),
            protected_values: Arc::new(Mutex::new(BTreeSet::new())),
            server_only_values: Mutex::new(BTreeSet::new()),
            data_capacity: budget
                .get("record_bytes")
                .map(|b| u64::from(b.total - b.recovery_reserve))
                .unwrap_or(0),
            resource_paths: resources
                .iter()
                .map(|p| p.canonicalize().map_err(|_| unavailable()))
                .collect::<Result<_, _>>()?,
        });
        result.validate()?;
        Ok(result)
    }
    /// Preserves a previously recorded deployment identity across local restarts.
    pub fn acquire_local(
        database_path: &Path,
        lock_namespace: &Path,
        resources: &[PathBuf],
        budget: &JointBudgetPolicy,
    ) -> Result<Arc<Self>, SessionDomainError> {
        let connection = rusqlite::Connection::open(database_path).map_err(|_| unavailable())?;
        let exists:bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='local_deployment')",[],|r|r.get(0)).map_err(|_|unavailable())?;
        let id = if exists {
            connection
                .query_row(
                    "SELECT deployment_id FROM local_deployment WHERE singleton=1",
                    [],
                    |r| r.get::<_, String>(0),
                )
                .map_err(|_| unavailable())?
        } else {
            uuid::Uuid::new_v4().to_string()
        };
        Self::acquire(database_path, &id, lock_namespace, resources, budget)
    }
    pub fn database_identity(&self) -> &str {
        &self.database_identity
    }
    pub fn deployment_id(&self) -> &str {
        &self.deployment_id
    }
    pub fn validate(&self) -> Result<(), SessionDomainError> {
        for object in &self.objects {
            object.validate()?;
        }
        Ok(())
    }
    /// Retains the legacy split lifecycle without enabling joint admission or its budgets.
    pub fn bind_split_repository(
        self: &Arc<Self>,
        mut repository: SqliteSessionRepository,
    ) -> Result<SqliteSessionRepository, SessionDomainError> {
        self.validate()?;
        if crate::SessionRepository::supports_joint_transactions(&repository) {
            return Err(unavailable());
        }
        self.check_policy(&repository.joint_budget_policy)?;
        let connection = repository.conn.lock().map_err(|_| unavailable())?;
        let path = connection.path().ok_or_else(unavailable)?;
        let connected = std::fs::metadata(path).map_err(|_| unavailable())?;
        let held = std::fs::metadata(&self.database_path).map_err(|_| unavailable())?;
        if connected.dev() != held.dev() || connected.ino() != held.ino() {
            return Err(unavailable());
        }
        drop(connection);
        repository.deployment = Some(self.clone());
        Ok(repository)
    }
    /// Opens another internal connection under this daemon's exact policy.
    pub fn repository(
        self: &Arc<Self>,
        budget: JointBudgetPolicy,
    ) -> Result<SqliteSessionRepository, SessionDomainError> {
        self.validate()?;
        if budget_hash(&self.policy_ref, &budget) != self.policy_hash {
            return Err(unavailable());
        }
        let mut repository = SqliteSessionRepository::open(&self.database_path)
            .map_err(|_| unavailable())?
            .with_joint_budget_policy(budget)?;
        repository.deployment = Some(self.clone());
        self.install_content_guard(&repository)?;
        Ok(repository)
    }
    pub fn protects_directory(&self, path: &Path) -> Result<(), SessionDomainError> {
        self.validate()?;
        let path = path.canonicalize().map_err(|_| unavailable())?;
        if !path.is_dir()
            || !self
                .resource_paths
                .iter()
                .any(|root| path.starts_with(root))
        {
            return Err(unavailable());
        }
        Ok(())
    }
    pub fn validate_bytes(&self, bytes: &[u8]) -> Result<(), SessionDomainError> {
        self.validate()?;
        if contains_protected(
            bytes,
            &*self.protected_values.lock().map_err(|_| unavailable())?,
            0,
        ) {
            return Err(unavailable());
        }
        Ok(())
    }
    pub fn protect_server_values(
        &self,
        values: impl IntoIterator<Item = String>,
    ) -> Result<(), SessionDomainError> {
        let values: Vec<_> = values.into_iter().filter(|v| !v.is_empty()).collect();
        self.server_only_values
            .lock()
            .map_err(|_| unavailable())?
            .extend(values.iter().cloned());
        self.protect_values(values)
    }
    pub fn validate_host_environment(
        &self,
        environment: &BTreeMap<String, String>,
    ) -> Result<(), SessionDomainError> {
        let values = self.server_only_values.lock().map_err(|_| unavailable())?;
        if environment
            .values()
            .any(|v| contains_protected(v.as_bytes(), &values, 0))
        {
            return Err(unavailable());
        }
        Ok(())
    }
    /// Values remain in memory and are checked before SQLite writes any business row.
    pub fn protect_values(
        &self,
        values: impl IntoIterator<Item = String>,
    ) -> Result<(), SessionDomainError> {
        let mut protected = self.protected_values.lock().map_err(|_| unavailable())?;
        protected.extend(values.into_iter().filter(|s| !s.is_empty()));
        Ok(())
    }
    fn install_content_guard(
        &self,
        repository: &SqliteSessionRepository,
    ) -> Result<(), SessionDomainError> {
        use rusqlite::{functions::FunctionFlags, types::ValueRef};
        let values = self.protected_values.clone();
        let connection = repository.conn.lock().map_err(|_| unavailable())?;
        connection
            .create_scalar_function(
                "safe_deployment_content",
                1,
                FunctionFlags::SQLITE_UTF8,
                move |context| {
                    let data = match context.get_raw(0) {
                        ValueRef::Text(v) | ValueRef::Blob(v) => v,
                        _ => return Ok(true),
                    };
                    let protected = values.lock().map_err(|_| rusqlite::Error::InvalidQuery)?;
                    Ok(!contains_protected(data, &protected, 0))
                },
            )
            .map_err(|_| unavailable())?;
        connection
            .create_scalar_function(
                "deployment_request_digest",
                1,
                FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
                |context| {
                    let body: String = context.get(0)?;
                    Ok(crate::host_bytes_digest(body.as_bytes()))
                },
            )
            .map_err(|_| unavailable())?;
        let configured:bool=connection.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='execution_configurations')",[],|r|r.get(0)).map_err(|_|unavailable())?;
        if configured {
            connection.execute_batch("CREATE TEMP TRIGGER bind_execution_admission AFTER INSERT ON main.joint_operations BEGIN
                SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM main.execution_configurations c WHERE c.revision=json_extract(NEW.request_json,'$.context.configuration_revision') AND json_extract(c.references_json,'$.owner_kind')=json_extract(NEW.request_json,'$.binding.owner_kind') AND json_extract(c.references_json,'$.runtime_kind')=json_extract(NEW.request_json,'$.context.runtime_kind')) THEN RAISE(ABORT,'execution configuration unavailable') END;
                INSERT INTO execution_admissions SELECT NEW.id,c.revision,json_extract(NEW.request_json,'$.context.operation'),NEW.tenant_key,json_extract(NEW.request_json,'$.context.owner_ref'),json_extract(NEW.request_json,'$.context.principal'),deployment_request_digest(NEW.request_json),json_extract(c.references_json,'$.policy_digest'),json_extract(c.references_json,'$.declaration.declaration_id'),json_object('profile',json_extract(c.references_json,'$.required_capabilities'),'operation',json_extract(NEW.request_json,'$.context.operation')),json_extract(c.references_json,'$.usage_requirement'),json_object('requested',json_extract(NEW.request_json,'$.reservations'),'operation_slot',NEW.id,'owned_record_bytes',NEW.id),'allowed','fixed_profile_matched',(SELECT budget_policy_ref FROM main.local_deployment WHERE singleton=1),(SELECT budget_hash FROM main.local_deployment WHERE singleton=1) FROM main.execution_configurations c WHERE c.revision=json_extract(NEW.request_json,'$.context.configuration_revision'); END;")
                .map_err(|_|unavailable())?;
        }
        let tables: Vec<String> = connection
            .prepare(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
            )
            .map_err(|_| unavailable())?
            .query_map([], |r| r.get(0))
            .map_err(|_| unavailable())?
            .collect::<rusqlite::Result<_>>()
            .map_err(|_| unavailable())?;
        for (index, table) in tables.iter().enumerate() {
            let escaped = table.replace('"', "\"\"");
            let columns: Vec<String> = connection
                .prepare(&format!("PRAGMA table_info(\"{escaped}\")"))
                .map_err(|_| unavailable())?
                .query_map([], |r| r.get(1))
                .map_err(|_| unavailable())?
                .collect::<rusqlite::Result<_>>()
                .map_err(|_| unavailable())?;
            let condition = columns
                .iter()
                .map(|c| {
                    format!(
                        "NOT safe_deployment_content(NEW.\"{}\")",
                        c.replace('"', "\"\"")
                    )
                })
                .collect::<Vec<_>>()
                .join(" OR ");
            for operation in ["INSERT", "UPDATE"] {
                connection.execute_batch(&format!("CREATE TEMP TRIGGER content_guard_{index}_{operation} BEFORE {operation} ON main.\"{escaped}\" WHEN {condition} BEGIN SELECT RAISE(ABORT,'protected deployment content rejected'); END;"))
                    .map_err(|_| unavailable())?;
            }
        }
        let ordinary_slots = repository
            .joint_budget_policy
            .get("operation")
            .copied()
            .unwrap_or_default();
        let slot_limit = ordinary_slots.total - ordinary_slots.recovery_reserve;
        connection.execute_batch(&format!("CREATE TEMP TRIGGER operation_capacity BEFORE INSERT ON main.joint_operations WHEN (SELECT count(*) FROM main.joint_operations WHERE stage NOT IN ('published','released','compensated','settled') OR id IN (SELECT intent_id FROM main.joint_host_intents WHERE json_extract(body,'$.cleanup_state')='pending')) >= {slot_limit} BEGIN SELECT RAISE(ABORT,'local operation capacity exhausted'); END;"))
            .map_err(|_| unavailable())?;
        let data_budget = repository
            .joint_budget_policy
            .get("record_bytes")
            .copied()
            .unwrap_or_default();
        let byte_limit = data_budget.total - data_budget.recovery_reserve;
        let mut sizes = Vec::new();
        let mut definitions = Vec::new();
        for (index, table) in tables.iter().enumerate() {
            if table == "local_deployment" {
                continue;
            }
            let escaped = table.replace('"', "\"\"");
            let columns: Vec<String> = connection
                .prepare(&format!("PRAGMA table_info(\"{escaped}\")"))
                .map_err(|_| unavailable())?
                .query_map([], |r| r.get(1))
                .map_err(|_| unavailable())?
                .collect::<rusqlite::Result<_>>()
                .map_err(|_| unavailable())?;
            let sum = |prefix: &str| {
                columns
                    .iter()
                    .map(|c| {
                        format!(
                            "coalesce(length(CAST({prefix}\"{}\" AS BLOB)),0)",
                            c.replace('"', "\"\"")
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("+")
            };
            sizes.push(format!(
                "(SELECT coalesce(sum({}),0) FROM main.\"{escaped}\")",
                sum("")
            ));
            definitions.push((index, escaped, sum("NEW."), sum("OLD.")));
        }
        let used = sizes.join("+");
        for (index, table, new, old) in definitions {
            for (operation, delta) in [
                ("INSERT", new.clone()),
                ("UPDATE", format!("({new})-({old})")),
            ] {
                connection.execute_batch(&format!("CREATE TEMP TRIGGER data_capacity_{index}_{operation} BEFORE {operation} ON main.\"{table}\" WHEN ({delta}) > 0 AND ({used}) + ({delta}) > {byte_limit} BEGIN SELECT RAISE(ABORT,'local record capacity exhausted'); END;"))
                    .map_err(|_| unavailable())?;
            }
        }
        Ok(())
    }
    /// A revision's safe configuration references cannot be replaced by a new default.
    pub fn register_configuration(
        &self,
        revision: &str,
        digest: &str,
        references: &serde_json::Value,
    ) -> Result<(), SessionDomainError> {
        self.validate()?;
        let body = serde_json::to_string(references).map_err(|_| unavailable())?;
        if contains_protected(
            body.as_bytes(),
            &*self.protected_values.lock().map_err(|_| unavailable())?,
            0,
        ) {
            return Err(unavailable());
        }
        let mut connection =
            rusqlite::Connection::open(&self.database_path).map_err(|_| unavailable())?;
        let tx = connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(|_| unavailable())?;
        tx.execute_batch("CREATE TABLE IF NOT EXISTS execution_configurations(revision TEXT PRIMARY KEY, digest TEXT NOT NULL, references_json TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS execution_defaults(runtime_kind TEXT PRIMARY KEY, owner_kind TEXT NOT NULL, revision TEXT NOT NULL, digest TEXT NOT NULL, enabled INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS execution_admissions(intent_id TEXT PRIMARY KEY REFERENCES joint_operations(id),profile_revision TEXT NOT NULL REFERENCES execution_configurations(revision),operation_kind TEXT NOT NULL,tenant_key TEXT NOT NULL,owner_ref TEXT NOT NULL,principal TEXT NOT NULL,request_digest TEXT NOT NULL,policy_digest TEXT NOT NULL,declaration_ref TEXT NOT NULL,required_capabilities_json TEXT NOT NULL,usage_requirement TEXT NOT NULL,reservation_refs_json TEXT NOT NULL,decision TEXT NOT NULL,reason TEXT NOT NULL,budget_policy_ref TEXT NOT NULL,budget_policy_hash TEXT NOT NULL)").map_err(|_| unavailable())?;
        let old: Option<(String, String)> = tx
            .query_row(
                "SELECT digest,references_json FROM execution_configurations WHERE revision=?1",
                [revision],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .map_err(|_| unavailable())?;
        if let Some(old) = old {
            if old != (digest.to_string(), body) {
                return Err(unavailable());
            }
        } else {
            let added = (revision.len() + digest.len() + body.len()) as u64;
            if owned_record_bytes(&tx)?.saturating_add(added) > self.data_capacity {
                return Err(unavailable());
            }
            tx.execute(
                "INSERT INTO execution_configurations VALUES(?1,?2,?3)",
                params![revision, digest, body],
            )
            .map_err(|_| unavailable())?;
        }
        tx.commit().map_err(|_| unavailable())
    }
    pub fn activate_default(
        &self,
        owner: &str,
        runtime: &str,
        revision: &str,
        digest: &str,
        enabled: bool,
    ) -> Result<(), SessionDomainError> {
        self.check_configuration(revision, digest)?;
        let mut connection =
            rusqlite::Connection::open(&self.database_path).map_err(|_| unavailable())?;
        let tx = connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(|_| unavailable())?;
        let previous:Option<u64>=tx.query_row("SELECT length(CAST(runtime_kind AS BLOB))+length(CAST(owner_kind AS BLOB))+length(CAST(revision AS BLOB))+length(CAST(digest AS BLOB))+1 FROM execution_defaults WHERE runtime_kind=?1",[runtime],|r|r.get(0)).optional().map_err(|_|unavailable())?;
        let new = (runtime.len() + owner.len() + revision.len() + digest.len() + 1) as u64;
        let growth = new.saturating_sub(previous.unwrap_or(0));
        if growth > 0 && owned_record_bytes(&tx)?.saturating_add(growth) > self.data_capacity {
            return Err(unavailable());
        }
        tx.execute("INSERT INTO execution_defaults VALUES(?1,?2,?3,?4,?5) ON CONFLICT(runtime_kind) DO UPDATE SET owner_kind=excluded.owner_kind,revision=excluded.revision,digest=excluded.digest,enabled=excluded.enabled",params![runtime,owner,revision,digest,enabled]).map_err(|_|unavailable())?;
        tx.commit().map_err(|_| unavailable())
    }
    pub fn check_default(
        &self,
        owner: &str,
        runtime: &str,
        revision: &str,
        digest: &str,
    ) -> Result<(), SessionDomainError> {
        self.check_configuration(revision, digest)?;
        let connection =
            rusqlite::Connection::open(&self.database_path).map_err(|_| unavailable())?;
        let matches:bool=connection.query_row("SELECT EXISTS(SELECT 1 FROM execution_defaults WHERE runtime_kind=?1 AND owner_kind=?2 AND revision=?3 AND digest=?4 AND enabled=1)",params![runtime,owner,revision,digest],|r|r.get(0)).map_err(|_|unavailable())?;
        if !matches {
            return Err(unavailable());
        }
        Ok(())
    }
    pub fn check_configuration(
        &self,
        revision: &str,
        digest: &str,
    ) -> Result<(), SessionDomainError> {
        self.validate()?;
        let connection =
            rusqlite::Connection::open(&self.database_path).map_err(|_| unavailable())?;
        let old: String = connection
            .query_row(
                "SELECT digest FROM execution_configurations WHERE revision=?1",
                [revision],
                |r| r.get(0),
            )
            .map_err(|_| unavailable())?;
        if old != digest {
            return Err(unavailable());
        }
        Ok(())
    }
    pub fn repository_with_configuration(
        self: &Arc<Self>,
        configuration: &DeploymentBudgetConfiguration,
    ) -> Result<SqliteSessionRepository, SessionDomainError> {
        configuration.validate()?;
        if configuration.policy_ref != self.policy_ref {
            return Err(unavailable());
        }
        self.repository(configuration.resources.clone())
    }
    pub(crate) fn check_policy(
        &self,
        budget: &JointBudgetPolicy,
    ) -> Result<(), SessionDomainError> {
        self.validate()?;
        if budget_hash(&self.policy_ref, budget) != self.policy_hash {
            return Err(unavailable());
        }
        Ok(())
    }
}
fn owned_record_bytes(connection: &rusqlite::Connection) -> Result<u64, SessionDomainError> {
    let tables:Vec<String> = connection.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name!='local_deployment'").map_err(|_|unavailable())?
        .query_map([],|r|r.get(0)).map_err(|_|unavailable())?.collect::<rusqlite::Result<_>>().map_err(|_|unavailable())?;
    let mut total = 0u64;
    for table in tables {
        let table = table.replace('"', "\"\"");
        let columns: Vec<String> = connection
            .prepare(&format!("PRAGMA table_info(\"{table}\")"))
            .map_err(|_| unavailable())?
            .query_map([], |r| r.get(1))
            .map_err(|_| unavailable())?
            .collect::<rusqlite::Result<_>>()
            .map_err(|_| unavailable())?;
        let sum = columns
            .iter()
            .map(|c| {
                format!(
                    "coalesce(length(CAST(\"{}\" AS BLOB)),0)",
                    c.replace('"', "\"\"")
                )
            })
            .collect::<Vec<_>>()
            .join("+");
        let bytes: u64 = connection
            .query_row(
                &format!("SELECT coalesce(sum({sum}),0) FROM \"{table}\""),
                [],
                |r| r.get(0),
            )
            .map_err(|_| unavailable())?;
        total = total.saturating_add(bytes);
    }
    Ok(total)
}
fn contains_protected(bytes: &[u8], values: &BTreeSet<String>, depth: usize) -> bool {
    use base64::Engine;
    if values
        .iter()
        .any(|v| bytes.windows(v.len()).any(|w| w == v.as_bytes()))
    {
        return true;
    }
    if depth >= 16 {
        return !values.is_empty();
    }
    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(bytes) {
        fn scan(v: &serde_json::Value, values: &BTreeSet<String>, depth: usize) -> bool {
            match v {
                serde_json::Value::String(s) => contains_protected(s.as_bytes(), values, depth + 1),
                serde_json::Value::Array(a) => a.iter().any(|v| scan(v, values, depth + 1)),
                serde_json::Value::Object(o) => o.iter().any(|(k, v)| {
                    contains_protected(k.as_bytes(), values, depth + 1)
                        || scan(v, values, depth + 1)
                }),
                _ => false,
            }
        }
        if scan(&value, values, depth + 1) {
            return true;
        }
    }
    if let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(bytes) {
        if !decoded.is_empty() && contains_protected(&decoded, values, depth + 1) {
            return true;
        }
    }
    false
}
fn budget_hash(reference: &str, budget: &JointBudgetPolicy) -> String {
    crate::host_digest(&serde_json::json!({"policy_ref":reference,"resources":budget}))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::JointResourceBudget;
    use std::{
        process::{Child, Command, Stdio},
        time::{Duration, Instant},
    };
    use tempfile::TempDir;

    pub(super) struct Fixture {
        pub(super) root: TempDir,
        pub(super) database: PathBuf,
        pub(super) resources: Vec<PathBuf>,
        pub(super) namespace: PathBuf,
    }
    impl Fixture {
        pub(super) fn new() -> Self {
            let root = tempfile::tempdir().unwrap();
            let database = root.path().join("db");
            drop(SqliteSessionRepository::open(&database).unwrap());
            let resources = vec![root.path().join("resources")];
            let namespace = root.path().join("locks");
            std::fs::create_dir(&resources[0]).unwrap();
            std::fs::create_dir(&namespace).unwrap();
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&namespace, std::fs::Permissions::from_mode(0o700)).unwrap();
            seed_owned_records(&database, &resources[0]);
            Self {
                root,
                database,
                resources,
                namespace,
            }
        }
        pub(super) fn guard(&self) -> Arc<DeploymentGuard> {
            DeploymentGuard::acquire(
                &self.database,
                "deployment",
                &self.namespace,
                &self.resources,
                &budget(4),
            )
            .unwrap()
        }
        fn child(&self, database: &Path, resource: &Path, expected: &str) -> Child {
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "deployment::tests::writer_child", "--nocapture"])
                .env("WRITER_TEST_DB", database)
                .env("WRITER_TEST_RESOURCE", resource)
                .env("WRITER_TEST_LOCKS", &self.namespace)
                .env("WRITER_TEST_EXPECT", expected)
                .env("WRITER_TEST_SIGNAL", self.root.path().join("signal"))
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap()
        }
    }
    fn seed_owned_records(path: &Path, workspace: &Path) {
        use crate::*;
        let binding = JointBinding {
            owner_kind: "fixture-owner".into(),
            binding_id: "original-binding".into(),
            configuration_revision: "original".into(),
        };
        let record = SessionRecord {
            execution_backing: SessionExecutionBacking::JointV1 {
                binding: binding.clone(),
            },
            runtime_id: "existing-session".into(),
            conversation_id: "conversation".into(),
            sender_id: "sender".into(),
            status: SessionStatus::Running,
            created_at_ms: 1,
            updated_at_ms: 2,
            workspace: WorkspaceFacts {
                workspace_id: "workspace".into(),
                root: workspace.to_string_lossy().into(),
                access: WorkspaceAccess::ReadWrite,
                revision: None,
                metadata: serde_json::json!({}),
            },
            isolation: IsolationFacts {
                boundary: IsolationBoundary::Host,
                workspace_access: WorkspaceAccess::ReadWrite,
                network: NetworkIsolation::None,
                metadata: serde_json::json!({}),
            },
            capabilities: EffectiveCapabilities {
                sandbox: Default::default(),
                runtime: Default::default(),
            },
            runtime: OpaqueRuntimeState {
                runtime_kind: "fixture".into(),
                schema_version: 1,
                state: serde_json::json!({"binding_id":"original-binding"}),
            },
            llm: None,
            lease: None,
            lineage: None,
            last_error: None,
            tenant_id: None,
            created_by: "operator".into(),
        };
        let context = JointOperationContext {
            intent_id: "original-open".into(),
            operation: "open".into(),
            tenant_id: None,
            owner_ref: ADMIN_OWNER_REF.into(),
            principal: "operator".into(),
            actor_role: Role::Admin,
            quota: Default::default(),
            runtime_kind: "fixture".into(),
            configuration_revision: "original".into(),
            source_runtime_id: None,
            target_runtime_id: Some("existing-session".into()),
            checkpoint_id: None,
        };
        let native = JointNativeIntent {
            step: "create".into(),
            operation_id: uuid::Uuid::new_v4().to_string(),
            digest: "0".repeat(64),
            method: "thinkthread.create".into(),
            params: serde_json::json!({"fs":workspace}),
            effect: NativeEffect::Create,
            compensation: false,
        };
        let request = JointPrepareRequest {
            context,
            binding: binding.clone(),
            root_id: "original-root".into(),
            source_id: "existing-session".into(),
            exclusive_root: true,
            parent_binding: None,
            reservations: vec![
                JointResourceReservation {
                    kind: "session".into(),
                    units: 1,
                    limit: 0,
                },
                JointResourceReservation {
                    kind: "root".into(),
                    units: 1,
                    limit: 0,
                },
            ],
            native: vec![native],
            publication_input: Some(record.clone()),
            turn: None,
        };
        let admission = JointAdmission {
            intent_id: "original-open".into(),
            binding: binding.clone(),
            generation: 1,
            root_epoch: 1,
            source_epoch: None,
        };
        let conn = rusqlite::Connection::open(path).unwrap();
        conn.execute("INSERT INTO joint_bindings VALUES('original-binding','fixture-owner','original','fixture','admin','original-root',1,'active','original-open',NULL)",[]).unwrap();
        conn.execute("INSERT INTO joint_roots VALUES('original-root','fixture-owner','original','admin','original-binding')",[]).unwrap();
        conn.execute("INSERT INTO joint_operations VALUES('original-open',?1,?2,'original-binding','admin','published',NULL)",params![serde_json::to_string(&request).unwrap(),serde_json::to_string(&admission).unwrap()]).unwrap();
        conn.execute(
            "INSERT INTO joint_reservations VALUES('original-open','session',1,'committed')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO joint_reservations VALUES('original-open','root',1,'committed')",
            [],
        )
        .unwrap();
        conn.execute("INSERT INTO joint_objects VALUES('original-binding',1,'original-root','root','native-original-root',0)",[]).unwrap();
        conn.execute("INSERT INTO joint_sessions VALUES('existing-session','original-binding',NULL,'running',2,?1)",[serde_json::to_string(&record).unwrap()]).unwrap();
        conn.execute(
            "INSERT INTO joint_refs VALUES('session','existing-session','original-binding')",
            [],
        )
        .unwrap();
        conn.execute("INSERT INTO joint_fences VALUES('root','original-root','original-root','admin',1,NULL)",[]).unwrap();
        conn.execute("INSERT INTO joint_fences VALUES('source','existing-session','original-root','admin',1,'unknown-turn')",[]).unwrap();
        let mut pending = request.clone();
        pending.context.operation = "turn".into();
        pending.context.intent_id = "unknown-turn".into();
        pending.context.source_runtime_id = Some("existing-session".into());
        pending.context.target_runtime_id = None;
        pending.exclusive_root = false;
        pending.reservations.clear();
        pending.publication_input = None;
        pending.turn = Some(JointTurnIdentity {
            turn_id: "original-turn".into(),
            client_request_id: Some("original-key".into()),
            input_digest: "0".repeat(64),
        });
        pending.native[0].step = "message".into();
        pending.native[0].method = "message.send".into();
        pending.native[0].operation_id = uuid::Uuid::new_v4().to_string();
        pending.native[0].params = serde_json::json!({"recipientThinkthreadId":"native-original-root","text":"retained original input"});
        pending.native[0].effect = NativeEffect::Mutate;
        let mut held = admission;
        held.intent_id = "unknown-turn".into();
        held.source_epoch = Some(1);
        conn.execute("INSERT INTO joint_operations VALUES('unknown-turn',?1,?2,'original-binding','admin','unknown',NULL)",params![serde_json::to_string(&pending).unwrap(),serde_json::to_string(&held).unwrap()]).unwrap();
        conn.execute(
            "INSERT INTO joint_native_steps VALUES('unknown-turn','message',?1,?2,?3,?4)",
            params![
                pending.native[0].operation_id,
                pending.native[0].digest,
                serde_json::to_string(&pending.native[0]).unwrap(),
                serde_json::to_string(&JointNativeOutcome {
                    state: NativeOperationState::Unknown,
                    resources: vec![],
                    payload: serde_json::json!({"delivery":"sent_unknown"})
                })
                .unwrap()
            ],
        )
        .unwrap();
        std::fs::write(workspace.join("sentinel"), b"preserved workspace").unwrap();
    }
    pub(super) fn new_allocation(path: &Path, id: &str) -> crate::JointPrepareRequest {
        let conn = rusqlite::Connection::open(path).unwrap();
        let mut request: crate::JointPrepareRequest = serde_json::from_str(
            &conn
                .query_row(
                    "SELECT request_json FROM joint_operations WHERE id='original-open'",
                    [],
                    |r| r.get::<_, String>(0),
                )
                .unwrap(),
        )
        .unwrap();
        request.context.intent_id = id.into();
        request.context.target_runtime_id = Some(id.into());
        request.binding.binding_id = format!("binding-{id}");
        request.root_id = format!("root-{id}");
        request.source_id = id.into();
        request.native[0].operation_id = uuid::Uuid::new_v4().to_string();
        request.native[0].params = serde_json::json!({"fs":format!("/fixture/{id}")});
        let record = request.publication_input.as_mut().unwrap();
        record.runtime_id = id.into();
        record.status = crate::SessionStatus::Idle;
        record.execution_backing = crate::SessionExecutionBacking::JointV1 {
            binding: request.binding.clone(),
        };
        record.runtime.state = serde_json::json!({"binding_id":request.binding.binding_id});
        request
    }
    pub(super) fn complete_budget(slots: u32, bytes: u32) -> JointBudgetPolicy {
        let mut p = budget(4);
        p.insert(
            "root".into(),
            JointResourceBudget {
                total: 4,
                recovery_reserve: 1,
            },
        );
        p.insert(
            "operation".into(),
            JointResourceBudget {
                total: slots,
                recovery_reserve: 1,
            },
        );
        p.insert(
            "record_bytes".into(),
            JointResourceBudget {
                total: bytes,
                recovery_reserve: 0,
            },
        );
        p
    }
    fn budget(total: u32) -> JointBudgetPolicy {
        JointBudgetPolicy::from([(
            "session".into(),
            JointResourceBudget {
                total,
                recovery_reserve: 1,
            },
        )])
    }
    fn wait(child: &mut Child) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                assert!(status.success(), "writer child failed: {status}");
                break;
            }
            if Instant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("writer child timed out");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    #[test]
    fn writer_child() {
        let Some(path) = std::env::var_os("WRITER_TEST_DB") else {
            return;
        };
        let result = DeploymentGuard::acquire(
            Path::new(&path),
            "deployment",
            Path::new(&std::env::var_os("WRITER_TEST_LOCKS").unwrap()),
            &[PathBuf::from(
                std::env::var_os("WRITER_TEST_RESOURCE").unwrap(),
            )],
            &budget(4),
        );
        match std::env::var("WRITER_TEST_EXPECT").unwrap().as_str() {
            "denied" => assert!(result.is_err()),
            "acquired" => {
                assert!(result.is_ok());
            }
            "race" => {
                let owned = result.is_ok();
                std::fs::write(
                    std::env::var_os("WRITER_TEST_RESULT").unwrap(),
                    if owned { "owned" } else { "denied" },
                )
                .unwrap();
                if owned {
                    let release = PathBuf::from(std::env::var_os("WRITER_TEST_RELEASE").unwrap());
                    let deadline = Instant::now() + Duration::from_secs(10);
                    while !release.exists() && Instant::now() < deadline {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    assert!(release.exists());
                }
            }
            "hold" => {
                let guard = result.unwrap();
                std::fs::write(
                    std::env::var_os("WRITER_TEST_SIGNAL").unwrap(),
                    guard.database_identity(),
                )
                .unwrap();
                std::thread::sleep(Duration::from_secs(30));
            }
            _ => panic!("invalid writer test mode"),
        }
    }
    #[test]
    fn simultaneous_process_contenders_have_one_holder_before_and_after_exit() {
        let f = Fixture::new();
        for round in 0..2 {
            let release = f.root.path().join(format!("release-{round}"));
            let paths = [
                f.root.path().join(format!("contender-{round}-0")),
                f.root.path().join(format!("contender-{round}-1")),
            ];
            let mut children = paths
                .iter()
                .map(|result| {
                    Command::new(std::env::current_exe().unwrap())
                        .args(["--exact", "deployment::tests::writer_child", "--nocapture"])
                        .env("WRITER_TEST_DB", &f.database)
                        .env("WRITER_TEST_RESOURCE", &f.resources[0])
                        .env("WRITER_TEST_LOCKS", &f.namespace)
                        .env("WRITER_TEST_EXPECT", "race")
                        .env("WRITER_TEST_RESULT", result)
                        .env("WRITER_TEST_RELEASE", &release)
                        .stdout(Stdio::null())
                        .stderr(Stdio::null())
                        .spawn()
                        .unwrap()
                })
                .collect::<Vec<_>>();
            let deadline = Instant::now() + Duration::from_secs(8);
            while !paths.iter().all(|p| p.exists()) {
                if Instant::now() >= deadline {
                    for child in &mut children {
                        let _ = child.kill();
                        let _ = child.wait();
                    }
                    panic!("writer competition timed out");
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            let statuses = paths
                .iter()
                .map(|p| std::fs::read_to_string(p).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(statuses.iter().filter(|s| s.as_str() == "owned").count(), 1);
            assert_eq!(
                statuses.iter().filter(|s| s.as_str() == "denied").count(),
                1
            );
            std::fs::write(release, b"release").unwrap();
            for child in &mut children {
                wait(child);
            }
            let conn = rusqlite::Connection::open(&f.database).unwrap();
            let stage: String = conn
                .query_row(
                    "SELECT stage FROM joint_operations WHERE id='unknown-turn'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(stage, "unknown");
            assert_eq!(
                std::fs::read(f.resources[0].join("sentinel")).unwrap(),
                b"preserved workspace"
            );
        }
    }
    #[test]
    fn independent_process_cannot_acquire_the_same_database() {
        let f = Fixture::new();
        let guard = f.guard();
        wait(&mut f.child(&f.database, &f.resources[0], "denied"));
        drop(guard);
        wait(&mut f.child(&f.database, &f.resources[0], "acquired"));
    }
    #[test]
    fn independent_databases_cannot_share_managed_resources() {
        let f = Fixture::new();
        let _guard = f.guard();
        let other = f.root.path().join("other-db");
        drop(SqliteSessionRepository::open(&other).unwrap());
        wait(&mut f.child(&other, &f.resources[0], "denied"));
        let separate = f.root.path().join("separate");
        std::fs::create_dir(&separate).unwrap();
        wait(&mut f.child(&other, &separate, "acquired"));
    }
    #[test]
    fn file_directory_and_hardlink_aliases_share_object_locks() {
        let f = Fixture::new();
        let _guard = f.guard();
        let link = f.root.path().join("db-link");
        std::os::unix::fs::symlink(&f.database, &link).unwrap();
        wait(&mut f.child(&link, &f.resources[0], "denied"));
        std::fs::remove_file(&link).unwrap();
        std::fs::hard_link(&f.database, &link).unwrap();
        wait(&mut f.child(&link, &f.resources[0], "denied"));
        let other = f.root.path().join("other-db");
        drop(SqliteSessionRepository::open(&other).unwrap());
        let alias = f.root.path().join("resource-link");
        std::os::unix::fs::symlink(&f.resources[0], &alias).unwrap();
        wait(&mut f.child(&other, &alias.join("."), "denied"));
    }
    #[test]
    fn database_copy_retains_identity_and_cannot_take_over() {
        let f = Fixture::new();
        let guard = f.guard();
        let copy = f.root.path().join("copy");
        let conn = rusqlite::Connection::open(&f.database).unwrap();
        conn.execute("VACUUM INTO ?1", [copy.to_str().unwrap()])
            .unwrap();
        let separate = f.root.path().join("separate");
        std::fs::create_dir(&separate).unwrap();
        wait(&mut f.child(&copy, &separate, "denied"));
        let identity: String = rusqlite::Connection::open(copy)
            .unwrap()
            .query_row("SELECT database_identity FROM local_deployment", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(identity, guard.database_identity());
    }
    #[test]
    fn locks_outlive_repository_connections_and_detect_replacement() {
        let f = Fixture::new();
        let guard = f.guard();
        let repo = guard.repository(budget(4)).unwrap();
        drop(repo);
        wait(&mut f.child(&f.database, &f.resources[0], "denied"));
        std::fs::rename(&f.resources[0], f.root.path().join("moved")).unwrap();
        std::fs::create_dir(&f.resources[0]).unwrap();
        assert!(guard.validate().is_err());
        assert!(guard.repository(budget(4)).is_err());
    }
    #[test]
    fn abnormal_exit_releases_only_local_locks() {
        let f = Fixture::new();
        let mut child = f.child(&f.database, &f.resources[0], "hold");
        let signal = f.root.path().join("signal");
        let deadline = Instant::now() + Duration::from_secs(10);
        while !signal.exists() {
            if Instant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("writer did not start");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(DeploymentGuard::acquire(
            &f.database,
            "deployment",
            &f.namespace,
            &f.resources,
            &budget(4)
        )
        .is_err());
        let identity = std::fs::read_to_string(&signal).unwrap();
        child.kill().unwrap();
        child.wait().unwrap();
        let guard = f.guard();
        assert_eq!(guard.database_identity(), identity);
    }
    #[tokio::test]
    async fn internal_connections_require_exact_budget_and_live_holder() {
        let f = Fixture::new();
        let guard = f.guard();
        let a = guard.repository(budget(4)).unwrap();
        let b = guard.repository(budget(4)).unwrap();
        assert!(guard.repository(budget(5)).is_err());
        assert!(guard.repository(JointBudgetPolicy::new()).is_err());
        assert!(guard
            .repository(budget(4))
            .unwrap()
            .with_joint_budget_policy(budget(5))
            .is_err());
        a.joint_transaction(|_| Ok(())).await.unwrap();
        b.joint_transaction(|_| Ok(())).await.unwrap();
        let unguarded = SqliteSessionRepository::open(&f.database)
            .unwrap()
            .with_joint_budget_policy(budget(4))
            .unwrap();
        assert!(unguarded.joint_transaction(|_| Ok(())).await.is_err());
        drop(a);
        drop(b);
        drop(guard);
        let lower = DeploymentGuard::acquire(
            &f.database,
            "deployment",
            &f.namespace,
            &f.resources,
            &budget(2),
        )
        .unwrap();
        lower
            .repository(budget(2))
            .unwrap()
            .joint_transaction(|_| Ok(()))
            .await
            .unwrap();
    }
}

#[cfg(test)]
mod capacity_tests {
    use super::tests::{complete_budget, new_allocation, Fixture};
    use super::*;
    use crate::SessionRepository;

    #[tokio::test]
    async fn ordinary_operation_slots_preserve_recovery_capacity_and_rollback_competition() {
        let f = Fixture::new();
        let policy = complete_budget(3, 1_000_000);
        let guard = DeploymentGuard::acquire(
            &f.database,
            "deployment",
            &f.namespace,
            &f.resources,
            &policy,
        )
        .unwrap();
        let a = guard
            .repository(policy.clone())
            .unwrap()
            .with_joint_transactions();
        let b = guard
            .repository(policy.clone())
            .unwrap()
            .with_joint_transactions();
        let original = serde_json::to_value(a.pending_joint_operations().await.unwrap()).unwrap();
        let one = new_allocation(&f.database, "one");
        let two = new_allocation(&f.database, "two");
        let (x, y) = tokio::join!(
            a.prepare_joint_operation(one),
            b.prepare_joint_operation(two)
        );
        assert_eq!(usize::from(x.is_ok()) + usize::from(y.is_ok()), 1);
        let conn = rusqlite::Connection::open(&f.database).unwrap();
        let count:u64=conn.query_row("SELECT count(*) FROM joint_operations WHERE stage NOT IN ('published','released','compensated','settled')",[],|r|r.get(0)).unwrap();
        assert_eq!(count, 2);
        let loser = if x.is_ok() {
            "binding-two"
        } else {
            "binding-one"
        };
        let left: u64 = conn
            .query_row(
                "SELECT count(*) FROM joint_bindings WHERE id=?1",
                [loser],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(left, 0);
        let retained = a
            .pending_joint_operations()
            .await
            .unwrap()
            .into_iter()
            .find(|p| p.admission.intent_id == "unknown-turn")
            .unwrap();
        assert_eq!(serde_json::to_value(retained).unwrap(), original[0]);
        assert!(a
            .prepare_joint_operation(new_allocation(&f.database, "third"))
            .await
            .is_err());
    }
    #[tokio::test]
    async fn zero_and_exhausted_owned_record_budgets_preserve_unknown_records() {
        let f = Fixture::new();
        let policy = complete_budget(4, 1);
        let guard = DeploymentGuard::acquire(
            &f.database,
            "deployment",
            &f.namespace,
            &f.resources,
            &policy,
        )
        .unwrap();
        let repo = guard.repository(policy).unwrap().with_joint_transactions();
        let before = serde_json::to_value(repo.pending_joint_operations().await.unwrap()).unwrap();
        assert!(repo
            .prepare_joint_operation(new_allocation(&f.database, "over-byte-budget"))
            .await
            .is_err());
        assert_eq!(
            serde_json::to_value(repo.pending_joint_operations().await.unwrap()).unwrap(),
            before
        );
        assert!(repo.get("existing-session").await.unwrap().is_some());
    }
    #[tokio::test]
    async fn split_repository_uses_the_holder_without_joint_quota_or_profile_activation() {
        let f = Fixture::new();
        let guard = DeploymentGuard::acquire(
            &f.database,
            "deployment",
            &f.namespace,
            &f.resources,
            &JointBudgetPolicy::new(),
        )
        .unwrap();
        let raw = SqliteSessionRepository::open(&f.database).unwrap();
        let repo = guard.bind_split_repository(raw).unwrap();
        assert!(!repo.supports_joint_transactions());
        assert_eq!(repo.pending_joint_operations().await.unwrap().len(), 1);
        let mut record = repo.get("existing-session").await.unwrap().unwrap();
        record.runtime_id = "legacy-split".into();
        record.execution_backing = crate::SessionExecutionBacking::SplitV1;
        record.runtime.state = serde_json::json!({});
        repo.save(record.clone()).await.unwrap();
        assert_eq!(repo.get("legacy-split").await.unwrap(), Some(record));
    }
    #[test]
    fn owned_record_content_has_an_exact_finite_boundary() {
        let f = Fixture::new();
        let connection = rusqlite::Connection::open(&f.database).unwrap();
        connection
            .execute_batch("CREATE TABLE owned_record_probe(content BLOB NOT NULL)")
            .unwrap();
        let used = owned_record_bytes(&connection).unwrap();
        drop(connection);
        let policy = complete_budget(4, u32::try_from(used + 6).unwrap());
        let guard = DeploymentGuard::acquire(
            &f.database,
            "deployment",
            &f.namespace,
            &f.resources,
            &policy,
        )
        .unwrap();
        let repo = guard.repository(policy).unwrap();
        let connection = repo.conn.lock().unwrap();
        connection
            .execute(
                "INSERT INTO owned_record_probe VALUES(?1)",
                [b"abcdef".as_slice()],
            )
            .unwrap();
        assert_eq!(owned_record_bytes(&connection).unwrap(), used + 6);
        assert!(connection
            .execute(
                "INSERT INTO owned_record_probe VALUES(?1)",
                [b"g".as_slice()]
            )
            .is_err());
        assert!(connection
            .execute(
                "UPDATE owned_record_probe SET content=?1",
                [b"abcdefg".as_slice()]
            )
            .is_err());
        let content: Vec<u8> = connection
            .query_row("SELECT content FROM owned_record_probe", [], |r| r.get(0))
            .unwrap();
        assert_eq!(content, b"abcdef");
        assert_eq!(owned_record_bytes(&connection).unwrap(), used + 6);
        drop(connection);
        assert_eq!(
            business_snapshot(&f.database)
                .iter()
                .find(|(name, _)| name == "joint_operations")
                .unwrap()
                .1
                .len(),
            2
        );
    }
    fn business_snapshot(path: &Path) -> Vec<(String, Vec<Vec<String>>)> {
        let connection = rusqlite::Connection::open(path).unwrap();
        let tables:Vec<String>=connection.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name!='local_deployment' ORDER BY name").unwrap().query_map([],|r|r.get(0)).unwrap().collect::<rusqlite::Result<_>>().unwrap();
        tables
            .into_iter()
            .map(|table| {
                let mut statement = connection
                    .prepare(&format!("SELECT * FROM \"{table}\" ORDER BY rowid"))
                    .unwrap();
                let count = statement.column_count();
                let rows = statement
                    .query_map([], |r| {
                        Ok((0..count)
                            .map(|i| format!("{:?}", r.get_ref(i).unwrap()))
                            .collect())
                    })
                    .unwrap()
                    .collect::<rusqlite::Result<_>>()
                    .unwrap();
                (table, rows)
            })
            .collect()
    }
    #[test]
    fn budget_configuration_parses_strictly_and_never_normalizes_invalid_capacity() {
        for input in [
            r#"{}"#,
            r#"{"policy_ref":"budget/v1"}"#,
            r#"{"policy_ref":"budget/v1","resources":{"unknown":{"total":1}}}"#,
            r#"{"policy_ref":"budget/v1","resources":{"session":{"total":1,"recovery_reserve":2}}}"#,
            r#"{"policy_ref":"budget/v1","resources":{"session":{"total":1,"unknown":true}}}"#,
            r#"{"policy_ref":"budget/v1","resources":{"session":{"total":-1}}}"#,
            r#"{"policy_ref":"budget/v1","resources":{"session":{"total":"infinity"}}}"#,
        ] {
            assert!(DeploymentBudgetConfiguration::parse(input).is_err());
        }
        let zero = DeploymentBudgetConfiguration::parse(
            r#"{"policy_ref":"budget/v1","resources":{"session":{"total":0}}}"#,
        )
        .unwrap();
        assert_eq!(zero.resources["session"].total, 0);
        assert_eq!(zero.resources["session"].recovery_reserve, 0);
    }
    #[tokio::test]
    async fn policy_reference_and_hash_are_persisted_and_required_for_every_connection() {
        let f = Fixture::new();
        let configuration = DeploymentBudgetConfiguration {
            policy_ref: "budget/v1".into(),
            resources: complete_budget(4, 1_000_000),
        };
        let guard = DeploymentGuard::acquire_with_configuration(
            &f.database,
            "deployment",
            &f.namespace,
            &f.resources,
            &configuration,
        )
        .unwrap();
        let a = guard.repository_with_configuration(&configuration).unwrap();
        let b = guard.repository_with_configuration(&configuration).unwrap();
        let before = business_snapshot(&f.database);
        let mut different = configuration.clone();
        different.policy_ref = "budget/v2".into();
        assert!(guard.repository_with_configuration(&different).is_err());
        different = configuration.clone();
        different
            .resources
            .get_mut("root")
            .unwrap()
            .recovery_reserve = 0;
        assert!(guard.repository_with_configuration(&different).is_err());
        assert_eq!(a.pending_joint_operations().await.unwrap().len(), 1);
        assert_eq!(b.pending_joint_operations().await.unwrap().len(), 1);
        assert_eq!(business_snapshot(&f.database), before);
        let connection = rusqlite::Connection::open(&f.database).unwrap();
        let (reference, hash): (String, String) = connection
            .query_row(
                "SELECT budget_policy_ref,budget_hash FROM local_deployment",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(reference, "budget/v1");
        assert_eq!(hash.len(), 64);
    }
    #[tokio::test]
    async fn omitted_and_zero_resource_capacities_deny_before_any_partial_transaction() {
        for kind in ["session", "root", "operation", "record_bytes"] {
            for omitted in [true, false] {
                let f = Fixture::new();
                let mut policy = complete_budget(4, 1_000_000);
                if omitted {
                    policy.remove(kind);
                } else {
                    let resource = policy.get_mut(kind).unwrap();
                    resource.total = 0;
                    resource.recovery_reserve = 0;
                }
                let guard = DeploymentGuard::acquire(
                    &f.database,
                    "deployment",
                    &f.namespace,
                    &f.resources,
                    &policy,
                )
                .unwrap();
                let repo = guard.repository(policy).unwrap().with_joint_transactions();
                let before = business_snapshot(&f.database);
                assert!(
                    repo.prepare_joint_operation(new_allocation(&f.database, "denied-allocation"))
                        .await
                        .is_err(),
                    "{kind} omitted={omitted}"
                );
                assert_eq!(business_snapshot(&f.database), before);
                println!("sample budget {kind} omitted={omitted}: unchanged business rows");
            }
        }
    }
    #[test]
    fn unsupported_filesystems_and_replaced_lock_objects_fail_closed() {
        assert!(LockedObject::acquire(Path::new("/proc/self/status"), true).is_err());
        let f = Fixture::new();
        let guard = f.guard();
        let path = f
            .namespace
            .join(format!("{}.lock", guard.database_identity()));
        std::fs::rename(&path, f.namespace.join("old-lock")).unwrap();
        std::fs::write(&path, b"").unwrap();
        assert!(guard.validate().is_err());
    }
}
