//! Execution adapter: Tokio context propagation and optional control dispatch.
use async_trait::async_trait;
use operation_protocol::{OperationBackend, OperationContext, OperationError};

tokio::task_local! { pub static OPERATION_CONTEXT: OperationContext; }

/// Runtime adapter convenience. Backends without execution control retain
/// their existing behavior; no lifecycle methods are required of them.
#[async_trait]
pub trait OperationExecutionExt: OperationBackend {
    async fn begin_turn(&self, turn_id: &str) -> Result<(), OperationError> {
        match self.execution_control() {
            Some(control) => control.begin_turn(turn_id).await,
            None => Ok(()),
        }
    }
    fn block_turn(&self, turn_id: &str) -> Result<(), OperationError> {
        match self.execution_control() {
            Some(control) => control.block_turn(turn_id),
            None => Ok(()),
        }
    }
    async fn cancel_turn(&self, turn_id: &str) -> Result<(), OperationError> {
        match self.execution_control() {
            Some(control) => control.cancel_turn(turn_id).await,
            None => Ok(()),
        }
    }
    async fn finish_turn(&self, turn_id: &str) -> Result<(), OperationError> {
        match self.execution_control() {
            Some(control) => control.finish_turn(turn_id).await,
            None => Ok(()),
        }
    }
}
impl<T: OperationBackend + ?Sized> OperationExecutionExt for T {}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn backend_without_control_keeps_noop_lifecycle() {
        let dir = tempfile::tempdir().unwrap();
        let backend = crate::local::local_backend(dir.path().into(), None, None, None).unwrap();
        assert!(backend.execution_control().is_none());
        backend.begin_turn("turn").await.unwrap();
        backend.block_turn("turn").unwrap();
        backend.cancel_turn("turn").await.unwrap();
        backend.finish_turn("turn").await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_contexts_are_isolated_and_removed_after_scope() {
        assert!(OPERATION_CONTEXT.try_with(Clone::clone).is_err());
        let scoped = |id: &'static str| async move {
            OPERATION_CONTEXT
                .scope(
                    OperationContext {
                        turn_id: id.into(),
                        operation_id: format!("op-{id}"),
                    },
                    async move {
                        tokio::task::yield_now().await;
                        let context = OPERATION_CONTEXT.with(Clone::clone);
                        assert_eq!(context.turn_id, id);
                        assert_eq!(context.operation_id, format!("op-{id}"));
                    },
                )
                .await;
        };
        tokio::join!(scoped("one"), scoped("two"));
        assert!(OPERATION_CONTEXT.try_with(Clone::clone).is_err());
    }
}
