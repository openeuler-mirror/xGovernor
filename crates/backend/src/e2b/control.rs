//! Per-backend ownership of remote command supervision. Dropping a caller
//! never drops the start/cleanup task, and unknown cleanup fences the turn.
use crate::execution::OPERATION_CONTEXT;
use operation_protocol::{OperationContext, OperationError};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Default)]
pub(crate) struct E2bExecutionControl {
    state: Mutex<ControlState>,
}
#[derive(Default)]
struct ControlState {
    closed: bool,
    turn: Option<(String, bool)>,
    flights: HashMap<String, Arc<Flight>>,
}
pub(crate) struct Flight {
    pub(crate) context: OperationContext,
    pub(crate) tag: String,
    pub(crate) prefix: Vec<u8>,
    pub(crate) status: Mutex<FlightStatus>,
}
#[derive(Default)]
pub(crate) struct FlightStatus {
    pub(crate) ready: bool,
    pub(crate) cancelled: bool,
    pub(crate) done: bool,
    pub(crate) clean: bool,
}
fn failure(message: &str) -> OperationError {
    OperationError::Transport {
        message: message.into(),
    }
}
impl E2bExecutionControl {
    pub(crate) fn check_context(&self) -> Result<(), OperationError> {
        if let Ok(context) = OPERATION_CONTEXT.try_with(Clone::clone) {
            self.check(&self.state.lock().unwrap(), &context)?;
        }
        Ok(())
    }
    fn check(
        &self,
        state: &ControlState,
        context: &OperationContext,
    ) -> Result<(), OperationError> {
        if state.closed
            || context.operation_id.is_empty()
            || !state
                .turn
                .as_ref()
                .is_some_and(|(id, blocked)| id == &context.turn_id && !blocked)
        {
            return Err(failure("stale or cancelled E2B tool context"));
        }
        Ok(())
    }
    pub(crate) fn register(
        &self,
        context: OperationContext,
    ) -> Result<Arc<Flight>, OperationError> {
        let mut state = self.state.lock().unwrap();
        self.check(&state, &context)?;
        let tag = format!("xgovernor-{}", uuid::Uuid::new_v4());
        let prefix = format!("\0XGOVERNOR-GUARD:{tag}\0").into_bytes();
        let flight = Arc::new(Flight {
            context,
            tag: tag.clone(),
            prefix,
            status: Mutex::new(FlightStatus::default()),
        });
        state.flights.insert(tag, flight.clone());
        Ok(flight)
    }
    pub(crate) fn confirm_shutdown(&self) {
        let mut state = self.state.lock().unwrap();
        // Only successful provider deletion proves that unknown remote work is
        // gone. A failed close must leave the admission fence intact.
        state.closed = true;
        state.turn = None;
        state.flights.clear();
    }
    pub(crate) fn begin_turn(&self, turn: &str) -> Result<(), OperationError> {
        let mut state = self.state.lock().unwrap();
        if state.closed || turn.is_empty() || state.turn.is_some() || !state.flights.is_empty() {
            return Err(failure("E2B turn or cleanup already pending"));
        }
        state.turn = Some((turn.into(), false));
        Ok(())
    }
    pub(crate) fn block_turn(&self, turn: &str) {
        let mut state = self.state.lock().unwrap();
        if let Some((id, blocked)) = state.turn.as_mut() {
            if id == turn {
                *blocked = true;
            }
        }
        for flight in state
            .flights
            .values()
            .filter(|flight| flight.context.turn_id == turn)
        {
            flight.status.lock().unwrap().cancelled = true;
        }
    }
    pub(crate) async fn drain(&self, turn: &str, finish: bool) -> Result<(), OperationError> {
        self.block_turn(turn);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            {
                let mut state = self.state.lock().unwrap();
                if !state.turn.as_ref().is_some_and(|(id, _)| id == turn) {
                    return Ok(());
                }
                let flights: Vec<_> = state
                    .flights
                    .values()
                    .filter(|flight| flight.context.turn_id == turn)
                    .collect();
                if flights.iter().any(|flight| {
                    let s = flight.status.lock().unwrap();
                    s.done && !s.clean
                }) {
                    return Err(failure(
                        "E2B process cleanup is unconfirmed; turn remains blocked",
                    ));
                }
                if flights
                    .iter()
                    .all(|flight| flight.status.lock().unwrap().done)
                {
                    if finish {
                        state
                            .flights
                            .retain(|_, flight| flight.context.turn_id != turn);
                        state.turn = None;
                    }
                    return Ok(());
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(failure(
                    "E2B process cleanup timed out; turn remains blocked",
                ));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn context(turn: &str) -> OperationContext {
        OperationContext {
            turn_id: turn.into(),
            operation_id: "tool-op".into(),
        }
    }
    #[tokio::test]
    async fn cancelled_and_old_contexts_cannot_start_remote_operations() {
        let control = E2bExecutionControl::default();
        assert!(control.register(context("one")).is_err());
        control.begin_turn("one").unwrap();
        control.block_turn("one");
        assert!(control.register(context("one")).is_err());
        control.drain("one", true).await.unwrap();
        control.begin_turn("two").unwrap();
        assert!(control.register(context("one")).is_err());
        assert!(control.register(context("two")).is_ok());
    }
    #[tokio::test]
    async fn stale_cancel_does_not_fence_current_turn_or_other_backend() {
        let one = E2bExecutionControl::default();
        let sibling = E2bExecutionControl::default();
        one.begin_turn("current").unwrap();
        sibling.begin_turn("current").unwrap();
        one.drain("stale", true).await.unwrap();
        assert!(one.register(context("current")).is_ok());
        one.block_turn("current");
        assert!(sibling.register(context("current")).is_ok());
    }
    #[tokio::test]
    async fn terminal_waits_for_detached_operation_cleanup() {
        let control = Arc::new(E2bExecutionControl::default());
        control.begin_turn("turn").unwrap();
        let flight = control.register(context("turn")).unwrap();
        let drain = {
            let control = control.clone();
            tokio::spawn(async move { control.drain("turn", true).await })
        };
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!drain.is_finished());
        assert!(flight.status.lock().unwrap().cancelled);
        assert!(control.begin_turn("next").is_err());
        {
            let mut status = flight.status.lock().unwrap();
            status.done = true;
            status.clean = true;
        }
        drain.await.unwrap().unwrap();
        control.begin_turn("next").unwrap();
    }
    #[tokio::test]
    async fn unknown_cleanup_stays_dirty_and_blocks_admission() {
        let control = E2bExecutionControl::default();
        control.begin_turn("turn").unwrap();
        let flight = control.register(context("turn")).unwrap();
        flight.status.lock().unwrap().done = true;
        assert!(control.drain("turn", true).await.is_err());
        assert!(control.drain("turn", true).await.is_err());
        assert!(control.begin_turn("next").is_err());
        assert!(control.register(context("turn")).is_err());
    }
    #[tokio::test]
    async fn confirmed_sandbox_deletion_retires_unknown_cleanup_without_readmitting() {
        let control = E2bExecutionControl::default();
        control.begin_turn("turn").unwrap();
        let flight = control.register(context("turn")).unwrap();
        flight.status.lock().unwrap().done = true;
        assert!(control.drain("turn", true).await.is_err());
        control.confirm_shutdown();
        control.drain("turn", true).await.unwrap();
        assert!(control.begin_turn("next").is_err());
        assert!(control.register(context("turn")).is_err());
    }
    #[tokio::test]
    async fn public_host_context_remains_optional_but_stale_tool_is_rejected() {
        let control = E2bExecutionControl::default();
        control.check_context().unwrap();
        assert!(OPERATION_CONTEXT
            .scope(context("old"), async { control.check_context() })
            .await
            .is_err());
        control.begin_turn("current").unwrap();
        OPERATION_CONTEXT
            .scope(context("current"), async { control.check_context() })
            .await
            .unwrap();
        control.block_turn("current");
        assert!(OPERATION_CONTEXT
            .scope(context("current"), async { control.check_context() })
            .await
            .is_err());
    }
}
