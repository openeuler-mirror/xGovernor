//! Durable, deployment-scoped execution intent. A dirty record is never treated
//! as evidence of success: reattachment first retires the entire container.
use super::{args, transport, DockerConfig};
use provider_protocol::ProviderControlError;
use rusqlite::{params, Connection, OptionalExtension};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use tokio::sync::{Mutex as AsyncMutex, Notify};

pub(super) struct Journal {
    db: Mutex<Connection>,
}
impl Journal {
    pub fn open(config: &DockerConfig) -> Result<Self, ProviderControlError> {
        std::fs::create_dir_all(&config.state_dir).map_err(transport)?;
        let db = Connection::open(
            config
                .state_dir
                .join(format!("{}.sqlite", config.deployment)),
        )
        .map_err(transport)?;
        db.busy_timeout(std::time::Duration::from_secs(5))
            .map_err(transport)?;
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS docker_control (
                identity TEXT PRIMARY KEY, state TEXT NOT NULL, generation INTEGER NOT NULL DEFAULT 0,
                updated_ms INTEGER NOT NULL, detail TEXT NOT NULL DEFAULT '{}');
            PRAGMA user_version=1;").map_err(transport)?;
        Ok(Self { db: Mutex::new(db) })
    }
    pub fn state(&self, id: &str) -> Result<String, ProviderControlError> {
        Ok(self
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT state FROM docker_control WHERE identity=?1",
                [id],
                |r| r.get(0),
            )
            .optional()
            .map_err(transport)?
            .unwrap_or_else(|| "ready".into()))
    }
    pub fn abandoned_create(&self, id: &str, before: u64) -> Result<bool, ProviderControlError> {
        self.db
            .lock()
            .unwrap()
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM docker_control WHERE identity=?1
            AND state IN ('creating','created') AND updated_ms < ?2)",
                params![id, before],
                |r| r.get(0),
            )
            .map_err(transport)
    }
    pub fn generation(&self, id: &str) -> Result<u64, ProviderControlError> {
        Ok(self
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT generation FROM docker_control WHERE identity=?1",
                [id],
                |r| r.get(0),
            )
            .optional()
            .map_err(transport)?
            .unwrap_or(0))
    }
    pub fn set(
        &self,
        id: &str,
        state: &str,
        generation: u64,
        detail: serde_json::Value,
    ) -> Result<(), ProviderControlError> {
        self.db
            .lock()
            .unwrap()
            .execute(
                "INSERT INTO docker_control(identity,state,generation,updated_ms,detail)
            VALUES(?1,?2,?3,?4,?5) ON CONFLICT(identity) DO UPDATE SET state=excluded.state,
            generation=excluded.generation,updated_ms=excluded.updated_ms,detail=excluded.detail",
                params![id, state, generation, super::now_ms(), detail.to_string()],
            )
            .map_err(transport)?;
        Ok(())
    }
}

pub(super) struct Control {
    pub config: Arc<DockerConfig>,
    pub journal: Arc<Journal>,
    pub id: String,
    pub gate: AsyncMutex<()>,
    pub generation: AtomicU64,
    pub changed: Notify,
    pub turn: Mutex<Option<(String, bool)>>,
}
impl Control {
    pub fn new(
        config: Arc<DockerConfig>,
        journal: Arc<Journal>,
        id: String,
    ) -> Result<Self, ProviderControlError> {
        let generation = AtomicU64::new(journal.generation(&id)?);
        Ok(Self {
            config,
            journal,
            id,
            gate: AsyncMutex::new(()),
            generation,
            changed: Notify::new(),
            turn: Mutex::new(None),
        })
    }
    pub fn state(&self) -> Result<String, ProviderControlError> {
        self.journal.state(&self.id)
    }
    pub fn set(&self, state: &str) -> Result<(), ProviderControlError> {
        let db = self.journal.db.lock().unwrap();
        let previous: String = db
            .query_row(
                "SELECT state FROM docker_control WHERE identity=?1",
                [&self.id],
                |r| r.get(0),
            )
            .optional()
            .map_err(transport)?
            .unwrap_or_else(|| "ready".into());
        // A concurrent disk monitor cannot replace a durable close intent.
        if previous == "closed" && state == "closing" {
            return Ok(());
        }
        let allowed = match previous.as_str() {
            "closed" => state == "closed",
            "closing" => matches!(state, "closing" | "closed"),
            "resource_stopped" => matches!(state, "resource_stopped" | "closing" | "closed"),
            _ => true,
        };
        if !allowed {
            return Err(transport(format!("container is {previous}")));
        }
        db.execute("INSERT INTO docker_control(identity,state,generation,updated_ms) VALUES(?1,?2,?3,?4)
            ON CONFLICT(identity) DO UPDATE SET state=excluded.state,generation=excluded.generation,updated_ms=excluded.updated_ms",
            params![self.id,state,self.generation.load(Ordering::SeqCst),super::now_ms()]).map_err(transport)?;
        Ok(())
    }
    pub fn invalidate(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
        self.changed.notify_waiters();
    }
    /// Caller holds gate. Closing/resource guards win over every restart path.
    pub async fn recover(&self) -> Result<(), ProviderControlError> {
        let state = self.state()?;
        if matches!(
            state.as_str(),
            "closing" | "closed" | "resource_stopped" | "paused" | "snapshotting"
        ) {
            return Err(transport(format!("container is {state}")));
        }
        self.set("cleanup")?;
        self.config
            .checked(args(&["stop", "--time", "0", &self.id]))
            .await?;
        if self.state()? != "cleanup" {
            return Err(transport(
                "restart superseded by close or resource protection",
            ));
        }
        self.config.checked(args(&["start", &self.id])).await?;
        self.set("ready")
    }
    pub async fn cancel(&self, turn: Option<&str>) -> Result<(), ProviderControlError> {
        if let Some(turn) = turn {
            let mut current = self.turn.lock().unwrap();
            match current.as_mut() {
                Some((id, cancelled)) if id == turn => *cancelled = true,
                _ => return Ok(()),
            }
        }
        self.invalidate();
        let _guard = self.gate.lock().await;
        if matches!(self.state()?.as_str(), "executing" | "cleanup") {
            self.recover().await?;
        }
        Ok(())
    }
}
/// Dropping a request never drops its responsibility to retire a remote exec.
/// Dirty state survives even if the service dies before this task runs.
pub(super) struct InFlight {
    pub control: Arc<Control>,
    pub finished: bool,
}
impl Drop for InFlight {
    fn drop(&mut self) {
        if !self.finished {
            self.control.invalidate();
            let control = self.control.clone();
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(async move {
                    let _guard = control.gate.lock().await;
                    if matches!(control.state().as_deref(), Ok("executing" | "cleanup")) {
                        if let Err(error) = control.recover().await {
                            tracing::warn!(%error,container=%control.id,"Docker cleanup pending");
                        }
                    }
                });
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn close_intent_wins_over_resource_monitor_and_restart() {
        let dir = tempfile::tempdir().unwrap();
        let c = Arc::new(DockerConfig {
            executable: "docker".into(),
            socket: "/run/docker.sock".into(),
            image: "test".into(),
            deployment: "test".into(),
            limits: Default::default(),
            state_dir: dir.path().into(),
        });
        let j = Arc::new(Journal::open(&c).unwrap());
        let control = Control::new(c, j, "id".into()).unwrap();
        control.set("executing").unwrap();
        control.set("closing").unwrap();
        assert!(control.set("resource_stopped").is_err());
        assert!(control.set("ready").is_err());
        control.set("closed").unwrap();
        control.set("closing").unwrap();
        assert_eq!(control.state().unwrap(), "closed");
        assert!(control.set("resource_stopped").is_err());
    }

    #[test]
    fn journal_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let c = DockerConfig {
            executable: "docker".into(),
            socket: "/run/docker.sock".into(),
            image: "test".into(),
            deployment: "test".into(),
            limits: Default::default(),
            state_dir: dir.path().into(),
        };
        let j = Journal::open(&c).unwrap();
        j.set("id", "cleanup", 7, serde_json::json!({})).unwrap();
        drop(j);
        let j = Journal::open(&c).unwrap();
        assert_eq!(j.state("id").unwrap(), "cleanup");
        assert_eq!(j.generation("id").unwrap(), 7);
    }
}
