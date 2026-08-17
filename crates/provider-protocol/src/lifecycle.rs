use crate::ProviderControlError;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderLifecycleState {
    Unknown,
    Creating,
    Active,
    Pausing,
    Paused,
    Loading,
    Deleting,
    Deleted,
    Failed,
}

impl ProviderLifecycleState {
    pub fn is_transient(self) -> bool {
        matches!(
            self,
            Self::Creating | Self::Pausing | Self::Loading | Self::Deleting
        )
    }

    pub fn is_terminal(self) -> bool {
        self == Self::Deleted
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderLifecycleOperation {
    Create,
    Load,
    Pause,
    Delete,
    Inspect,
}

impl ProviderLifecycleOperation {
    fn in_progress_state(self) -> Option<ProviderLifecycleState> {
        match self {
            Self::Create => Some(ProviderLifecycleState::Creating),
            Self::Load => Some(ProviderLifecycleState::Loading),
            Self::Pause => Some(ProviderLifecycleState::Pausing),
            Self::Delete => Some(ProviderLifecycleState::Deleting),
            Self::Inspect => None,
        }
    }

    fn success_state(self) -> Option<ProviderLifecycleState> {
        match self {
            Self::Create | Self::Load => Some(ProviderLifecycleState::Active),
            Self::Pause => Some(ProviderLifecycleState::Paused),
            Self::Delete => Some(ProviderLifecycleState::Deleted),
            Self::Inspect => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum ProviderLifecycleEvent {
    Begin {
        operation: ProviderLifecycleOperation,
    },
    CompleteSuccess {
        operation: ProviderLifecycleOperation,
    },
    CompleteFailure {
        operation: ProviderLifecycleOperation,
        message: String,
    },
    Inspected {
        state: ProviderLifecycleState,
    },
}

/// Pure, I/O-free executable lifecycle specification.
pub struct ProviderLifecycleStateMachine;

impl ProviderLifecycleStateMachine {
    pub fn transition(
        current: Option<ProviderLifecycleState>,
        event: &ProviderLifecycleEvent,
    ) -> Result<ProviderLifecycleState, ProviderControlError> {
        match event {
            ProviderLifecycleEvent::Begin { operation } => Self::begin(current, *operation),
            ProviderLifecycleEvent::CompleteSuccess { operation } => Self::complete_success(
                current.unwrap_or(ProviderLifecycleState::Unknown),
                *operation,
            ),
            ProviderLifecycleEvent::CompleteFailure { operation, .. } => Self::complete_failure(
                current.unwrap_or(ProviderLifecycleState::Unknown),
                *operation,
            ),
            ProviderLifecycleEvent::Inspected { state } => Ok(*state),
        }
    }

    pub fn begin(
        current: Option<ProviderLifecycleState>,
        operation: ProviderLifecycleOperation,
    ) -> Result<ProviderLifecycleState, ProviderControlError> {
        if operation == ProviderLifecycleOperation::Inspect {
            return Ok(current.unwrap_or(ProviderLifecycleState::Unknown));
        }

        let allowed = matches!(
            (current, operation),
            (None, ProviderLifecycleOperation::Create)
                | (None, ProviderLifecycleOperation::Load)
                | (
                    Some(ProviderLifecycleState::Paused),
                    ProviderLifecycleOperation::Load
                )
                | (
                    Some(ProviderLifecycleState::Active),
                    ProviderLifecycleOperation::Pause
                )
                | (
                    Some(ProviderLifecycleState::Active),
                    ProviderLifecycleOperation::Delete
                )
                | (
                    Some(ProviderLifecycleState::Paused),
                    ProviderLifecycleOperation::Delete
                )
                | (
                    Some(ProviderLifecycleState::Failed),
                    ProviderLifecycleOperation::Delete
                )
        );
        if current == Some(ProviderLifecycleState::Deleted)
            && operation == ProviderLifecycleOperation::Delete
        {
            return Ok(ProviderLifecycleState::Deleted);
        }
        if !allowed {
            return Err(invalid_state(
                current,
                operation,
                "operation is not valid for current state",
            ));
        }
        Ok(operation
            .in_progress_state()
            .expect("non-inspect operation has an in-progress state"))
    }

    pub fn complete_success(
        current: ProviderLifecycleState,
        operation: ProviderLifecycleOperation,
    ) -> Result<ProviderLifecycleState, ProviderControlError> {
        if current == ProviderLifecycleState::Deleted
            && operation == ProviderLifecycleOperation::Delete
        {
            return Ok(current);
        }
        let expected = operation.in_progress_state().ok_or_else(|| {
            invalid_state(
                Some(current),
                operation,
                "inspect has no completion transition",
            )
        })?;
        if current != expected {
            return Err(invalid_state(
                Some(current),
                operation,
                format!("expected {expected:?} before success completion"),
            ));
        }
        Ok(operation
            .success_state()
            .expect("mutating operation has a success state"))
    }

    pub fn complete_failure(
        current: ProviderLifecycleState,
        operation: ProviderLifecycleOperation,
    ) -> Result<ProviderLifecycleState, ProviderControlError> {
        if operation == ProviderLifecycleOperation::Inspect {
            return Ok(current);
        }
        if current == ProviderLifecycleState::Deleted
            && operation == ProviderLifecycleOperation::Delete
        {
            return Ok(current);
        }
        let expected = operation
            .in_progress_state()
            .expect("mutating operation has state");
        if current != expected {
            return Err(invalid_state(
                Some(current),
                operation,
                format!("expected {expected:?} before failure completion"),
            ));
        }
        Ok(ProviderLifecycleState::Failed)
    }
}

fn invalid_state(
    current: Option<ProviderLifecycleState>,
    operation: ProviderLifecycleOperation,
    message: impl Into<String>,
) -> ProviderControlError {
    ProviderControlError::InvalidState {
        current,
        operation,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_happy_path_and_idempotent_delete() {
        let creating =
            ProviderLifecycleStateMachine::begin(None, ProviderLifecycleOperation::Create).unwrap();
        let active = ProviderLifecycleStateMachine::complete_success(
            creating,
            ProviderLifecycleOperation::Create,
        )
        .unwrap();
        let pausing =
            ProviderLifecycleStateMachine::begin(Some(active), ProviderLifecycleOperation::Pause)
                .unwrap();
        let paused = ProviderLifecycleStateMachine::complete_success(
            pausing,
            ProviderLifecycleOperation::Pause,
        )
        .unwrap();
        let deleting =
            ProviderLifecycleStateMachine::begin(Some(paused), ProviderLifecycleOperation::Delete)
                .unwrap();
        let deleted = ProviderLifecycleStateMachine::complete_success(
            deleting,
            ProviderLifecycleOperation::Delete,
        )
        .unwrap();
        assert_eq!(
            ProviderLifecycleStateMachine::begin(Some(deleted), ProviderLifecycleOperation::Delete)
                .unwrap(),
            deleted
        );
    }

    #[test]
    fn invalid_transition_is_rejected() {
        assert!(ProviderLifecycleStateMachine::begin(
            Some(ProviderLifecycleState::Paused),
            ProviderLifecycleOperation::Pause
        )
        .is_err());
    }
}
