//! Durable file snapshots. Engine resources are never inferred from CLI success alone.
use super::*;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::sync::Mutex;

const SNAPSHOT: &str = "io.xgovernor.snapshot";
#[derive(Clone, Serialize, Deserialize)]
struct Record {
    id: String,
    request: String,
    source: String,
    owner: String,
    parent_image: String,
    image: Option<String>,
    bytes: u64,
    state: String,
    paused: bool,
    created: u64,
    #[serde(default = "protocol_version")]
    helper_protocol: u32,
    #[serde(default = "policy_version")]
    policy_version: u32,
    #[serde(default)]
    last_error: Option<String>,
    #[serde(default)]
    retry_count: u32,
    #[serde(default)]
    retry_at_ms: u64,
}
fn protocol_version() -> u32 {
    2
}
fn policy_version() -> u32 {
    1
}
pub(super) struct SnapshotStore {
    db: Mutex<Connection>,
}
impl SnapshotStore {
    pub fn open(config: &DockerConfig) -> Result<Self, ProviderControlError> {
        let db = Connection::open(
            config
                .state_dir
                .join(format!("{}-snapshots.sqlite", config.deployment)),
        )
        .map_err(transport)?;
        db.busy_timeout(std::time::Duration::from_secs(5))
            .map_err(transport)?;
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS snapshots(id TEXT PRIMARY KEY, body TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS snapshot_loads(request TEXT PRIMARY KEY, snapshot TEXT NOT NULL, container TEXT, done INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS aborted_loads(request TEXT PRIMARY KEY);
            PRAGMA user_version=1;").map_err(transport)?;
        Ok(Self { db: Mutex::new(db) })
    }
    fn put(&self, r: &Record) -> Result<(), ProviderControlError> {
        self.db.lock().unwrap().execute("INSERT INTO snapshots VALUES(?1,?2) ON CONFLICT(id) DO UPDATE SET body=excluded.body",params![r.id,serde_json::to_string(r).map_err(transport)?]).map_err(transport)?;
        Ok(())
    }
    fn all(&self) -> Result<Vec<Record>, ProviderControlError> {
        let db = self.db.lock().unwrap();
        let mut q = db
            .prepare("SELECT body FROM snapshots")
            .map_err(transport)?;
        let values = q
            .query_map([], |r| r.get::<_, String>(0))
            .map_err(transport)?;
        values
            .map(|s| serde_json::from_str(&s.map_err(transport)?).map_err(transport))
            .collect()
    }
    fn get(&self, id: &str) -> Result<Record, ProviderControlError> {
        self.all()?
            .into_iter()
            .find(|r| r.id == id)
            .ok_or_else(|| ProviderControlError::NotFound {
                resource_ref: id.into(),
            })
    }
    fn pin(&self, request: &str, snapshot: &str) -> Result<(), ProviderControlError> {
        let db = self.db.lock().unwrap();
        let previous: Option<String> = db
            .query_row(
                "SELECT snapshot FROM snapshot_loads WHERE request=?1",
                [request],
                |r| r.get(0),
            )
            .optional()
            .map_err(transport)?;
        if previous.as_deref().is_some_and(|id| id != snapshot) {
            return Err(invalid("load request reused for another snapshot"));
        }
        db.execute(
            "INSERT OR IGNORE INTO snapshot_loads(request,snapshot) VALUES(?1,?2)",
            params![request, snapshot],
        )
        .map_err(transport)?;
        Ok(())
    }
    fn finish_pin(
        &self,
        request: &str,
        container: Option<&str>,
    ) -> Result<(), ProviderControlError> {
        self.db
            .lock()
            .unwrap()
            .execute(
                "UPDATE snapshot_loads SET container=?2,done=1 WHERE request=?1",
                params![request, container],
            )
            .map_err(transport)?;
        Ok(())
    }
    fn abort_load(&self, request: &str) -> Result<(), ProviderControlError> {
        self.db
            .lock()
            .unwrap()
            .execute("INSERT OR IGNORE INTO aborted_loads VALUES(?1)", [request])
            .map_err(transport)?;
        Ok(())
    }
    fn aborted(&self, request: &str) -> Result<bool, ProviderControlError> {
        self.db
            .lock()
            .unwrap()
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM aborted_loads WHERE request=?1)",
                [request],
                |r| r.get(0),
            )
            .map_err(transport)
    }
    fn pins(&self) -> Result<Vec<(String, String, Option<String>, bool)>, ProviderControlError> {
        let db = self.db.lock().unwrap();
        let mut q = db
            .prepare("SELECT request,snapshot,container,done FROM snapshot_loads")
            .map_err(transport)?;
        let rows = q
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
            .map_err(transport)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(transport)
    }
}
fn conflict(message: &str) -> ProviderControlError {
    ProviderControlError::Conflict {
        message: message.into(),
    }
}
impl DockerProvider {
    fn job_dir(&self, id: &str) -> std::path::PathBuf {
        self.config
            .state_dir
            .join(format!("{}-jobs", self.config.deployment))
            .join(id)
    }
    fn start_commit_job(&self, r: &Record) -> Result<(), ProviderControlError> {
        let dir = self.job_dir(&r.id);
        std::fs::create_dir_all(&dir).map_err(transport)?;
        if dir.join("done.json").exists() {
            return Ok(());
        }
        let path = dir.join("request.json");
        if !path.exists() {
            use std::io::Write;
            let argv = vec![
                self.config.executable.clone(),
                "--host".into(),
                format!("unix://{}", self.config.socket),
                "commit".into(),
                "--change".into(),
                format!("LABEL {SNAPSHOT}={}", r.id),
                r.source.clone(),
                self.snapshot_tag(&r.id),
            ];
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create(true).truncate(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let temp = dir.join("request.tmp");
            let mut file = options.open(&temp).map_err(transport)?;
            file.write_all(
                serde_json::to_string(&json!({"argv":argv}))
                    .unwrap()
                    .as_bytes(),
            )
            .map_err(transport)?;
            file.sync_all().map_err(transport)?;
            std::fs::rename(temp, &path).map_err(transport)?;
            std::fs::File::open(&dir)
                .and_then(|f| f.sync_all())
                .map_err(transport)?;
        }
        // Detached receipt writer: HTTP cancellation / governor SIGKILL must not
        // erase knowledge of whether the Engine completed commit. flock prevents
        // duplicate jobs when recovery races a surviving writer.
        let mut child = tokio::process::Command::new("/usr/bin/python3")
            .args(["-I", "-c", include_str!("snapshot_job.py")])
            .arg(&dir)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(false)
            .spawn()
            .map_err(transport)?;
        tokio::spawn(async move {
            let _ = child.wait().await;
        });
        Ok(())
    }
    async fn await_commit_job(&self, r: &Record) -> Result<(), ProviderControlError> {
        self.start_commit_job(r)?;
        let done = self.job_dir(&r.id).join("done.json");
        tokio::time::timeout(
            std::time::Duration::from_millis(self.config.limits.snapshot_timeout_ms),
            async {
                while !done.exists() {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            },
        )
        .await
        .map_err(|_| transport("commit still pending; durable job retained"))?;
        Ok(())
    }
    fn commit_confirmed(&self, r: &Record) -> Result<bool, ProviderControlError> {
        let path = self.job_dir(&r.id).join("done.json");
        if !path.exists() {
            return Ok(false);
        }
        let data = std::fs::read(path).map_err(transport)?;
        let receipt: Value = serde_json::from_slice(&data).map_err(transport)?;
        Ok(receipt["exit_code"] == 0)
    }
    fn snapshots_enabled(&self) -> Result<(), ProviderControlError> {
        if self.config.limits.snapshot_enabled {
            Ok(())
        } else {
            Err(unsupported("snapshot disabled"))
        }
    }
    fn snapshot_tag(&self, id: &str) -> String {
        format!(
            "xgovernor-snapshot/{}:{}",
            self.config.deployment.to_ascii_lowercase(),
            id
        )
    }
    async fn image_info(&self, image: &str) -> Result<Value, ProviderControlError> {
        if image.is_empty() || image.starts_with('-') {
            return Err(invalid("invalid image identity"));
        }
        let out = self
            .config
            .run(&args(&["image", "inspect", image]), None, 30_000)
            .await?;
        if !out.status.success() {
            let msg = String::from_utf8_lossy(&out.stderr);
            if msg.contains("No such image") || msg.contains("No such object") {
                return Err(ProviderControlError::NotFound {
                    resource_ref: image.into(),
                });
            }
            return Err(transport(msg));
        }
        let values: Vec<Value> = serde_json::from_slice(&out.stdout).map_err(transport)?;
        values
            .into_iter()
            .next()
            .ok_or_else(|| transport("empty image inspect"))
    }
    fn validate_snapshot_image(
        &self,
        r: &Record,
        image: &Value,
    ) -> Result<(), ProviderControlError> {
        if r.helper_protocol != 2
            || r.policy_version != 1
            || image["Config"]["Labels"][LABEL] != self.config.deployment
            || image["Config"]["Labels"][OWNER] != r.owner
            || image["Config"]["Labels"][SNAPSHOT] != r.id
            || r.image.as_ref().is_some_and(|id| image["Id"] != *id)
            || image["Config"]["User"] != "10001:10001"
            || image["Config"]["WorkingDir"] != WORKSPACE
            || image["Config"]["Volumes"]
                .as_object()
                .is_some_and(|v| !v.is_empty())
        {
            return Err(invalid("snapshot image identity or policy mismatch"));
        }
        Ok(())
    }
    fn snapshot_value(&self, r: &Record) -> ProviderSnapshot {
        ProviderSnapshot {
            snapshot_id: ProviderSnapshotId(r.id.clone()),
            provider: self.kind.clone(),
            source_instance_id: Some(ProviderInstanceId(r.source.clone())),
            serialized_handle: None,
            metadata: json!({"image_id":r.image,"base_image_id":r.parent_image,"owner_ref":r.owner,"deployment":self.config.deployment,"helper_protocol":2,"policy_version":1,"filesystem_only":true,"bytes":r.bytes}),
            created_at_ms: r.created,
        }
    }
    fn snapshot_budget(&self, extra: u64) -> Result<(), ProviderControlError> {
        let records: Vec<_> = self
            .snapshots
            .all()?
            .into_iter()
            .filter(|r| r.state != "deleted")
            .collect();
        let mut ids = std::collections::HashSet::new();
        let total = records
            .iter()
            .filter(|r| ids.insert(r.image.as_ref().unwrap_or(&r.id).clone()))
            .try_fold(0u64, |n, r| n.checked_add(r.bytes))
            .ok_or_else(|| invalid("snapshot accounting overflow"))?;
        if records.len() >= self.config.limits.snapshot_max_count
            || total
                .checked_add(extra)
                .is_none_or(|n| n > self.config.limits.snapshot_max_bytes)
        {
            return Err(ProviderControlError::ResourceLimitExceeded {
                provider: self.kind.clone(),
                owner_ref: "snapshot storage".into(),
                current: records.len(),
                max: self.config.limits.snapshot_max_count,
            });
        }
        Ok(())
    }
    pub(super) async fn capture_snapshot(
        &self,
        backend_id: BackendId,
        instance_id: ProviderInstanceId,
        correlation: Value,
        paused: bool,
    ) -> Result<ProviderSnapshot, ProviderControlError> {
        self.snapshots_enabled()?;
        if backend_id.0 != "docker" {
            return Err(invalid("backend must be docker"));
        }
        let _snapshots = self.snapshot_lock.lock().await;
        let control = self.control(&instance_id.0)?;
        let _gate = control
            .gate
            .try_lock()
            .map_err(|_| conflict("container has an active operation"))?;
        if control.state()? != "ready" || control.turn.lock().unwrap().is_some() {
            return Err(conflict("snapshot requires an idle ready container"));
        }
        let request = correlation["request_id"]
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        if let Some(r) = self
            .snapshots
            .all()?
            .into_iter()
            .find(|r| r.request == request)
        {
            if r.source != instance_id.0 || r.paused != paused {
                return Err(conflict("snapshot request identity mismatch"));
            }
            if r.state == "ready" {
                self.validate_snapshot_image(
                    &r,
                    &self.image_info(r.image.as_ref().unwrap()).await?,
                )?;
                return Ok(self.snapshot_value(&r));
            }
            return Err(conflict("snapshot request is pending or retired"));
        }
        let value = self.raw_inspect(&instance_id.0).await?;
        self.validate_container(&value, None)?;
        let parent = self
            .image_info(
                value["Image"]
                    .as_str()
                    .ok_or_else(|| invalid("missing image"))?,
            )
            .await?;
        let estimate = parent["Size"]
            .as_u64()
            .unwrap_or(0)
            .checked_add(self.config.limits.disk_bytes)
            .ok_or_else(|| invalid("snapshot size overflow"))?;
        self.snapshot_budget(estimate)?;
        if self.config.free_bytes().await?
            < self.config.limits.reserve_bytes.saturating_add(estimate)
        {
            return Err(transport("insufficient disk reserve for snapshot"));
        }
        let mut r = Record {
            id: format!("snapshot-{}", uuid::Uuid::new_v4().simple()),
            request,
            source: instance_id.0,
            owner: value["Config"]["Labels"][OWNER]
                .as_str()
                .unwrap_or("")
                .into(),
            parent_image: value["Image"].as_str().unwrap().into(),
            image: None,
            bytes: estimate,
            state: "prepared".into(),
            paused,
            created: now_ms(),
            helper_protocol: 2,
            policy_version: 1,
            last_error: None,
            retry_count: 0,
            retry_at_ms: 0,
        };
        self.snapshots.put(&r)?;
        control.invalidate();
        control.set("snapshotting")?;
        let result = self.capture_stopped(&mut r, &control).await;
        if let Err(error) = &result {
            r.last_error = Some(error.to_string());
            self.snapshots.put(&r)?;
            // Leave unknown commit outcomes durable. Only the reconciler may retire them.
            if r.state != "committing" {
                r.state = "deleting".into();
                self.snapshots.put(&r)?;
                if control.state()? == "snapshotting" {
                    control.set("cleanup")?;
                    let _ = control.recover().await;
                }
            }
        }
        result
    }
    async fn capture_stopped(
        &self,
        r: &mut Record,
        control: &Control,
    ) -> Result<ProviderSnapshot, ProviderControlError> {
        self.config
            .checked(args(&["stop", "--time", "0", &r.source]))
            .await?;
        if self.raw_inspect(&r.source).await?["State"]["Running"] != false {
            return Err(transport("container did not stop"));
        }
        if control.state()? != "snapshotting" {
            return Err(conflict(
                "snapshot superseded by close or resource protection",
            ));
        }
        r.state = "committing".into();
        self.snapshots.put(r)?;
        let tag = self.snapshot_tag(&r.id);
        self.await_commit_job(r).await?;
        let image = match self.image_info(&tag).await {
            Ok(v) => v,
            Err(e) => {
                if self.commit_confirmed(r)? {
                    r.state = "captured".into();
                    self.snapshots.put(r)?;
                }
                return Err(e);
            }
        };
        self.validate_snapshot_image(r, &image)?;
        r.image = Some(
            image["Id"]
                .as_str()
                .ok_or_else(|| invalid("missing snapshot image ID"))?
                .into(),
        );
        r.bytes = image["Size"]
            .as_u64()
            .ok_or_else(|| invalid("missing image size"))?;
        r.state = "captured".into();
        self.snapshots.put(r)?;
        let total = self
            .snapshots
            .all()?
            .iter()
            .filter(|x| x.state != "deleted")
            .fold(0u64, |n, x| n.saturating_add(x.bytes));
        if total > self.config.limits.snapshot_max_bytes
            || self.config.free_bytes().await? < self.config.limits.reserve_bytes
        {
            return Err(transport("snapshot exceeded storage budget"));
        }
        if control.state()? != "snapshotting" {
            return Err(conflict("snapshot superseded"));
        }
        if r.paused {
            control.set("paused")?;
        } else {
            self.config.checked(args(&["start", &r.source])).await?;
            self.validate_container(
                &self.raw_inspect(&r.source).await?,
                Some(&Value::String(r.parent_image.clone())),
            )?;
            self.snapshot_health(&r.source).await?;
            control.set("ready")?;
        }
        r.state = "ready".into();
        self.snapshots.put(r)?;
        Ok(self.snapshot_value(r))
    }
    async fn snapshot_health(&self, id: &str) -> Result<(), ProviderControlError> {
        let out = self
            .config
            .run(
                &args(&[
                    "exec",
                    "-i",
                    id,
                    "/usr/local/bin/python3",
                    "-I",
                    "/usr/local/lib/xgovernor/helper.py",
                ]),
                Some(serde_json::to_vec(&json!({"protocol":2,"op":"hello"})).unwrap()),
                30_000,
            )
            .await?;
        let v: Value = serde_json::from_slice(&out.stdout).map_err(transport)?;
        if !out.status.success()
            || v["ok"] != true
            || v["value"]["protocol"] != 2
            || v["value"]["uid"] != 10001
        {
            return Err(invalid("snapshot helper handshake failed"));
        }
        Ok(())
    }
    pub(super) async fn load_snapshot(
        &self,
        request: ProviderLoadRequest,
    ) -> Result<ProviderInstance, ProviderControlError> {
        self.snapshots_enabled()?;
        if request.backend_id.0 != "docker" {
            return Err(invalid("backend must be docker"));
        }
        Self::validate_request_options(&request.provider_options, &request.resource_limits)?;
        let _snapshots = self.snapshot_lock.lock().await;
        match request.source {
            ProviderLoadSource::Instance(id) => {
                let value = self.raw_inspect(&id.0).await?;
                if value["Config"]["Labels"][OWNER] != request.owner_ref {
                    return Err(invalid("instance owner mismatch"));
                }
                self.validate_container(&value, None)?;
                let control = self.control(&id.0)?;
                let _gate = control.gate.lock().await;
                if control.state()? != "paused" {
                    return Err(conflict("instance is not explicitly paused"));
                }
                self.config.checked(args(&["start", &id.0])).await?;
                self.snapshot_health(&id.0).await?;
                control.set("ready")?;
                self.instance(&self.raw_inspect(&id.0).await?)
            }
            ProviderLoadSource::Snapshot(id) => {
                let r = self.snapshots.get(&id.0)?;
                if r.state != "ready" {
                    return Err(ProviderControlError::NotFound { resource_ref: id.0 });
                }
                if r.owner != request.owner_ref {
                    return Err(invalid("snapshot owner mismatch"));
                }
                let image = r
                    .image
                    .as_ref()
                    .ok_or_else(|| transport("snapshot has no image"))?;
                self.validate_snapshot_image(&r, &self.image_info(image).await?)?;
                let request_id = request.correlation["request_id"]
                    .as_str()
                    .ok_or_else(|| invalid("stable load request_id required"))?
                    .to_owned();
                if self.snapshots.aborted(&request_id)? {
                    return Err(conflict("load request was abandoned"));
                }
                self.snapshots.pin(&request_id, &r.id)?;
                let _creation = self.creation_lock.lock().await;
                let result = self
                    .create_container(
                        ProviderCreateRequest {
                            backend_id: request.backend_id,
                            owner_ref: request.owner_ref,
                            reason: request.reason,
                            resource_limits: request.resource_limits,
                            provider_options: request.provider_options,
                            correlation: request.correlation,
                        },
                        Some(image),
                    )
                    .await;
                match result {
                    Ok(instance) => {
                        self.snapshots
                            .finish_pin(&request_id, Some(&instance.instance_id.0))?;
                        Ok(instance)
                    }
                    Err(e) => {
                        if matches!(
                            e,
                            ProviderControlError::InvalidRequest { .. }
                                | ProviderControlError::ResourceLimitExceeded { .. }
                                | ProviderControlError::UnsupportedCapability { .. }
                        ) {
                            self.snapshots.finish_pin(&request_id, None)?;
                        }
                        Err(e)
                    } // Unknown create outcome retains its pin.
                }
            }
            ProviderLoadSource::SerializedHandle(_) => Err(unsupported("SerializedHandle")),
        }
    }
    pub(super) async fn abort_snapshot_load(
        &self,
        request: &str,
    ) -> Result<(), ProviderControlError> {
        let _snapshots = self.snapshot_lock.lock().await;
        let _creation = self.creation_lock.lock().await;
        self.snapshots.abort_load(request)?;
        self.collect_snapshots().await
    }
    pub(super) async fn retire_snapshot(&self, id: &str) -> Result<(), ProviderControlError> {
        let _snapshots = self.snapshot_lock.lock().await;
        let mut r = if let Some(request) = id.strip_prefix("request:") {
            self.snapshots
                .all()?
                .into_iter()
                .find(|r| r.request == request)
                .ok_or_else(|| ProviderControlError::NotFound {
                    resource_ref: id.into(),
                })?
        } else {
            self.snapshots.get(id)?
        };
        if r.state == "deleted" {
            return Ok(());
        }
        if r.state == "committing" {
            self.start_commit_job(&r)?;
            if !self.job_dir(&r.id).join("done.json").exists() {
                return Err(transport("commit job is pending"));
            }
            match self.image_info(&self.snapshot_tag(&r.id)).await {
                Ok(image) => {
                    self.validate_snapshot_image(&r, &image)?;
                    r.image = image["Id"].as_str().map(str::to_owned);
                }
                Err(ProviderControlError::NotFound { .. }) if self.commit_confirmed(&r)? => {}
                Err(e) => return Err(e),
            }
        }
        r.state = "deleting".into();
        self.snapshots.put(&r)?;
        let control = self.control(&r.source)?;
        let _gate = control.gate.lock().await;
        if control.state()? == "snapshotting" {
            control.set("cleanup")?;
            control.recover().await?;
        }
        drop(_gate);
        self.collect_snapshots().await
    }
    pub(super) async fn reconcile_snapshots(&self) -> Result<(), ProviderControlError> {
        let Ok(_snapshots) = self.snapshot_lock.try_lock() else {
            return Ok(());
        };
        for mut r in self.snapshots.all()? {
            if matches!(r.state.as_str(), "prepared" | "committing" | "captured") {
                if r.state == "committing" {
                    self.start_commit_job(&r)?;
                    if !self.job_dir(&r.id).join("done.json").exists() {
                        continue;
                    }
                    match self.image_info(&self.snapshot_tag(&r.id)).await {
                        Ok(image) => {
                            self.validate_snapshot_image(&r, &image)?;
                            r.image = image["Id"].as_str().map(str::to_owned);
                            r.bytes = image["Size"].as_u64().unwrap_or(r.bytes);
                        }
                        Err(ProviderControlError::NotFound { .. })
                            if self.commit_confirmed(&r)? => {} // A failed transport can leave Engine commit running.
                        Err(e) => return Err(e),
                    }
                }
                r.state = "deleting".into();
                self.snapshots.put(&r)?;
            }
            if r.state == "deleting" {
                let control = self.control(&r.source)?;
                let _gate = control.gate.lock().await;
                if control.state()? == "snapshotting" {
                    control.set("cleanup")?;
                    control.recover().await?;
                }
            }
        }
        self.collect_snapshots().await
    }
    async fn collect_snapshots(&self) -> Result<(), ProviderControlError> {
        let mut containers = self.list_instances().await?;
        // An unfinished load may have created its container before the process crashed.
        for (request, _, container, done) in self.snapshots.pins()? {
            if self.snapshots.aborted(&request)? {
                for c in containers
                    .iter()
                    .filter(|c| c.metadata["request_id"] == request)
                {
                    Box::pin(self.delete(ProviderDeleteRequest {
                        backend_id: c.backend_id.clone(),
                        instance_id: Some(c.instance_id.clone()),
                        snapshot_id: None,
                        reason: ProviderLifecycleReason::ErrorCleanup,
                        correlation: Value::Null,
                    }))
                    .await?;
                }
                containers.retain(|c| c.metadata["request_id"] != request);
                self.snapshots.finish_pin(&request, None)?;
                continue;
            }
            if !done {
                if let Some(c) = containers
                    .iter()
                    .find(|c| c.metadata["request_id"] == request)
                {
                    self.snapshots
                        .finish_pin(&request, Some(&c.instance_id.0))?;
                }
                // If no container exists, absence is not enough to disprove an in-flight create.
            } else if let Some(id) = container {
                if !containers.iter().any(|c| c.instance_id.0 == id) {
                    self.snapshots.finish_pin(&request, None)?;
                }
            }
        }
        let records = self.snapshots.all()?;
        let pins = self.snapshots.pins()?;
        for mut r in records.iter().filter(|r| r.state == "deleting").cloned() {
            if r.retry_at_ms > now_ms() {
                continue;
            }
            if pins
                .iter()
                .any(|(_, id, c, done)| id == &r.id && (!done || c.is_some()))
            {
                continue;
            }
            if let Some(image) = &r.image {
                if containers.iter().any(|c| c.metadata["image_id"] == *image)
                    || records.iter().any(|child| {
                        child.id != r.id && child.state != "deleted" && child.parent_image == *image
                    })
                {
                    continue;
                }
                match self.image_info(image).await {
                    Ok(v) => {
                        self.validate_snapshot_image(&r, &v)?;
                        if let Err(error) = self.config.checked(args(&["image", "rm", image])).await
                        {
                            r.last_error = Some(error.to_string());
                            r.retry_count = r.retry_count.saturating_add(1);
                            r.retry_at_ms = now_ms()
                                .saturating_add((1000u64 << r.retry_count.min(5)).min(30_000));
                            self.snapshots.put(&r)?;
                            continue;
                        }
                    }
                    Err(ProviderControlError::NotFound { .. }) => {}
                    Err(e) => return Err(e),
                }
            }
            r.state = "deleted".into();
            self.snapshots.put(&r)?;
            let dir = self.job_dir(&r.id);
            if dir.exists() {
                std::fs::remove_dir_all(dir).map_err(transport)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn config(path: &std::path::Path) -> DockerConfig {
        DockerConfig {
            executable: "docker".into(),
            socket: "/var/run/docker.sock".into(),
            image: "test:v2".into(),
            deployment: "snapshot-unit".into(),
            state_dir: path.into(),
            limits: DockerLimits::default(),
        }
    }
    fn record(id: &str, image: &str) -> Record {
        Record {
            id: id.into(),
            request: id.into(),
            source: "container".into(),
            owner: "admin".into(),
            parent_image: "base".into(),
            image: Some(image.into()),
            bytes: 100,
            state: "ready".into(),
            paused: false,
            created: 1,
            helper_protocol: 2,
            policy_version: 1,
            last_error: None,
            retry_count: 0,
            retry_at_ms: 0,
        }
    }
    #[test]
    fn durable_pins_and_abort_tombstones_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let c = config(dir.path());
        {
            let store = SnapshotStore::open(&c).unwrap();
            store.put(&record("one", "image")).unwrap();
            store.pin("load", "one").unwrap();
            store.abort_load("load").unwrap();
        }
        let store = SnapshotStore::open(&c).unwrap();
        assert!(store.aborted("load").unwrap());
        assert_eq!(
            store.pins().unwrap()[0],
            ("load".into(), "one".into(), None, false)
        );
        assert!(store.pin("load", "other").is_err());
        store.finish_pin("load", Some("child")).unwrap();
        assert_eq!(store.pins().unwrap()[0].2.as_deref(), Some("child"));
    }
    #[test]
    fn budget_counts_pending_deletes_and_distinct_images() {
        let dir = tempfile::tempdir().unwrap();
        let mut c = config(dir.path());
        c.limits.snapshot_max_count = 3;
        c.limits.snapshot_max_bytes = 150;
        let p = DockerProvider::new(c).unwrap();
        p.snapshots.put(&record("one", "same-image")).unwrap();
        let mut two = record("two", "same-image");
        two.state = "deleting".into();
        p.snapshots.put(&two).unwrap();
        assert!(p.snapshot_budget(50).is_ok());
        assert!(p.snapshot_budget(51).is_err());
        p.snapshots.put(&record("three", "same-image")).unwrap();
        assert!(p.snapshot_budget(0).is_err());
    }
    #[test]
    fn snapshot_identity_and_policy_are_validated() {
        let dir = tempfile::tempdir().unwrap();
        let p = DockerProvider::new(config(dir.path())).unwrap();
        let r = record("one", "sha256:test");
        let image = json!({"Id":"sha256:test","Config":{"User":"10001:10001","WorkingDir":"/workspace","Labels":{LABEL:"snapshot-unit",OWNER:"admin",SNAPSHOT:"one"}}});
        assert!(p.validate_snapshot_image(&r, &image).is_ok());
        for (field, value) in [("User", json!("0")), ("Volumes", json!({"/host":{}}))] {
            let mut bad = image.clone();
            bad["Config"][field] = value;
            assert!(p.validate_snapshot_image(&r, &bad).is_err());
        }
        let mut bad = r.clone();
        bad.policy_version = 99;
        assert!(p.validate_snapshot_image(&bad, &image).is_err());
    }
    #[test]
    fn failed_receipt_does_not_confirm_engine_commit_absence() {
        let dir = tempfile::tempdir().unwrap();
        let p = DockerProvider::new(config(dir.path())).unwrap();
        let r = record("one", "image");
        assert!(!p.commit_confirmed(&r).unwrap());
        std::fs::create_dir_all(p.job_dir(&r.id)).unwrap();
        let path = p.job_dir(&r.id).join("done.json");
        std::fs::write(&path, r#"{"exit_code":1}"#).unwrap();
        assert!(!p.commit_confirmed(&r).unwrap());
        std::fs::write(&path, r#"{"exit_code":0}"#).unwrap();
        assert!(p.commit_confirmed(&r).unwrap());
    }
}
