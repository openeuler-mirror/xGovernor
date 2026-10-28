use std::collections::BTreeSet;

use mcts_protocol::{Capability, MethodName};

use crate::error::PluginHostError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum HostCapability {
    Checkpoint,
    Fork,
    Interaction,
    Exec,
    FileRead,
    FileWrite,
}

impl HostCapability {
    pub fn from_plugin_capability(capability: Capability) -> Self {
        match capability {
            Capability::Snapshot => Self::Checkpoint,
            Capability::Fork => Self::Fork,
            Capability::AgentTurn => Self::Interaction,
            Capability::Exec => Self::Exec,
            Capability::Inspect | Capability::Diff => Self::Exec,
            Capability::FileRead => Self::FileRead,
            Capability::FileWrite => Self::FileWrite,
            Capability::Release => Self::Checkpoint,
        }
    }

    pub fn required_by(method: MethodName) -> Option<Self> {
        match method {
            MethodName::HostSnapshot => Some(Self::Checkpoint),
            MethodName::HostFork => Some(Self::Fork),
            MethodName::HostAgentTurn => Some(Self::Interaction),
            MethodName::HostInspect | MethodName::HostExec | MethodName::HostDiff => Some(Self::Exec),
            MethodName::HostFileRead => Some(Self::FileRead),
            MethodName::HostFileWrite => Some(Self::FileWrite),
            MethodName::HostRelease => Some(Self::Checkpoint),
            _ => None,
        }
    }
}

pub fn map_granted_capabilities(granted: &BTreeSet<Capability>) -> BTreeSet<HostCapability> {
    granted
        .iter()
        .map(|capability| HostCapability::from_plugin_capability(*capability))
        .collect()
}

pub fn ensure_capability_available(
    available: &BTreeSet<Capability>,
    required: Capability,
) -> Result<(), PluginHostError> {
    if available.contains(&required) {
        Ok(())
    } else {
        Err(PluginHostError::CapabilityDenied(format!(
            "runtime does not provide required MCTS capability: {required:?}"
        )))
    }
}

pub fn ensure_method_available(
    available: &BTreeSet<Capability>,
    method: MethodName,
) -> Result<(), PluginHostError> {
    let Some(_required) = HostCapability::required_by(method.clone()) else {
        return Ok(());
    };
    let required: Capability = match method {
        MethodName::HostSnapshot | MethodName::HostRelease => Capability::Snapshot,
        MethodName::HostFork => Capability::Fork,
        MethodName::HostAgentTurn => Capability::AgentTurn,
        MethodName::HostInspect | MethodName::HostExec | MethodName::HostDiff => Capability::Exec,
        MethodName::HostFileRead => Capability::FileRead,
        MethodName::HostFileWrite => Capability::FileWrite,
        _ => return Ok(()),
    };
    ensure_capability_available(available, required)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_each_mcts_capability_to_host_capability() {
        assert_eq!(
            HostCapability::from_plugin_capability(Capability::Snapshot),
            HostCapability::Checkpoint
        );
        assert_eq!(
            HostCapability::from_plugin_capability(Capability::Fork),
            HostCapability::Fork
        );
        assert_eq!(
            HostCapability::from_plugin_capability(Capability::AgentTurn),
            HostCapability::Interaction
        );
        assert_eq!(
            HostCapability::from_plugin_capability(Capability::Exec),
            HostCapability::Exec
        );
        assert_eq!(
            HostCapability::from_plugin_capability(Capability::FileRead),
            HostCapability::FileRead
        );
        assert_eq!(
            HostCapability::from_plugin_capability(Capability::FileWrite),
            HostCapability::FileWrite
        );
    }

    #[test]
    fn ensures_required_capability_is_present() {
        let available: BTreeSet<Capability> = [Capability::Snapshot, Capability::Fork]
            .into_iter()
            .collect();
        assert!(ensure_capability_available(&available, Capability::Snapshot).is_ok());
        assert!(ensure_capability_available(&available, Capability::Exec).is_err());
    }
}
