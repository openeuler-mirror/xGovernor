//! Runtime-neutral execution control contracts; no context storage mechanism.
use crate::OperationError;
use async_trait::async_trait;

/// Correlation attached by the runtime to a tool operation.
#[derive(Clone, Debug)]
pub struct OperationContext {
    pub turn_id: String,
    pub operation_id: String,
}

/// Optional control implemented by backends that coordinate turn cleanup.
/// A terminal event must wait for `finish_turn` to confirm cleanup.
#[async_trait]
pub trait OperationExecutionControl: Send + Sync {
    async fn begin_turn(&self, turn_id: &str) -> Result<(), OperationError>;
    fn block_turn(&self, turn_id: &str) -> Result<(), OperationError>;
    async fn cancel_turn(&self, turn_id: &str) -> Result<(), OperationError>;
    async fn finish_turn(&self, turn_id: &str) -> Result<(), OperationError>;
}
