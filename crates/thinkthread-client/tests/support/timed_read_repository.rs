//! Stage timing for a read fixture, delegating to the real SQLite repository.
use serde_json::Value;
use std::{future::Future, sync::Arc, time::Instant};
use xgovernor_core::*;

pub struct TimedReadRepository(pub Arc<SqliteSessionRepository>);
struct CallTimer {
    name: &'static str,
    start: Instant,
    completed: bool,
}
impl Drop for CallTimer {
    fn drop(&mut self) {
        eprintln!(
            "sample binary framing/stage=repository-{}/completed={}/elapsed_us={}",
            self.name,
            self.completed,
            self.start.elapsed().as_micros()
        );
    }
}
async fn timed<T>(name: &'static str, future: impl Future<Output = T>) -> T {
    let mut timer = CallTimer {
        name,
        start: Instant::now(),
        completed: false,
    };
    let result = future.await;
    timer.completed = true;
    result
}
#[async_trait::async_trait]
impl SessionRepository for TimedReadRepository {
    fn host_repository(&self) -> Option<&dyn HostRepository> {
        Some(self)
    }
    fn supports_joint_transactions(&self) -> bool {
        self.0.supports_joint_transactions()
    }
    async fn list_active(
        &self,
        tenant: Option<&str>,
        limit: usize,
    ) -> Result<SessionListPage, SessionDomainError> {
        self.0.list_active(tenant, limit).await
    }
    async fn get(&self, id: &str) -> Result<Option<SessionRecord>, SessionDomainError> {
        self.0.get(id).await
    }
    async fn save(&self, record: SessionRecord) -> Result<(), SessionDomainError> {
        self.0.save(record).await
    }
}
#[async_trait::async_trait]
impl HostRepository for TimedReadRepository {
    async fn host_target(
        &self,
        runtime_id: String,
    ) -> Result<(HostTargetV1, HostCapabilityPolicyV1), SessionDomainError> {
        timed("host_target", self.0.host_target(runtime_id)).await
    }
    async fn prepare_host(
        &self,
        prepared: HostPreparedV1,
    ) -> Result<HostAccessTokenV1, SessionDomainError> {
        timed("prepare_host", self.0.prepare_host(prepared)).await
    }
    async fn host_intent(&self, id: String) -> Result<HostIntentV1, SessionDomainError> {
        timed("host_intent", self.0.host_intent(id)).await
    }
    async fn claim_host_step(
        &self,
        access: HostAccessTokenV1,
        step: usize,
    ) -> Result<bool, SessionDomainError> {
        timed("claim_host_step", self.0.claim_host_step(access, step)).await
    }
    async fn record_host_step(
        &self,
        access: HostAccessTokenV1,
        step: usize,
        observation: HostObservationV1,
    ) -> Result<(), SessionDomainError> {
        timed(
            "record_host_step",
            self.0.record_host_step(access, step, observation),
        )
        .await
    }
    async fn finish_host(
        &self,
        access: HostAccessTokenV1,
        result: Value,
    ) -> Result<Value, SessionDomainError> {
        timed("finish_host", self.0.finish_host(access, result)).await
    }
    async fn fail_stale_host_read(
        &self,
        access: HostAccessTokenV1,
    ) -> Result<Value, SessionDomainError> {
        timed("fail_stale_host_read", self.0.fail_stale_host_read(access)).await
    }
    async fn finish_host_read(
        &self,
        access: HostAccessTokenV1,
        result: Value,
        revision_before: String,
        revision_after: String,
    ) -> Result<Value, SessionDomainError> {
        timed(
            "finish_host_read",
            self.0
                .finish_host_read(access, result, revision_before, revision_after),
        )
        .await
    }
}
