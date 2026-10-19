use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use tokio::process::Command;

use crate::error::PluginHostError;
use crate::process::PluginProcess;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginStatus {
    Spawning,
    Running,
    Stopping,
    Exited,
}

pub struct PluginRegistry {
    inner: RwLock<BTreeMap<String, PluginStatus>>,
}

impl PluginRegistry {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn register(&self, plugin_id: &str) -> Result<(), PluginHostError> {
        let mut inner = self.inner.write().map_err(|_| {
            PluginHostError::Manifest("plugin registry lock poisoned".to_string())
        })?;
        if inner.insert(plugin_id.to_string(), PluginStatus::Spawning).is_some() {
            return Err(PluginHostError::Manifest(format!(
                "plugin already registered: {plugin_id}"
            )));
        }
        Ok(())
    }

    pub fn set_status(&self, plugin_id: &str, status: PluginStatus) {
        if let Ok(mut inner) = self.inner.write() {
            inner.insert(plugin_id.to_string(), status);
        }
    }

    pub fn unregister(&self, plugin_id: &str) {
        if let Ok(mut inner) = self.inner.write() {
            inner.remove(plugin_id);
        }
    }

    pub fn status(&self, plugin_id: &str) -> Option<PluginStatus> {
        self.inner
            .read()
            .ok()
            .and_then(|inner| inner.get(plugin_id).copied())
    }
}

pub struct PluginManager {
    registry: Arc<PluginRegistry>,
}

impl PluginManager {
    pub fn new() -> Self {
        Self {
            registry: Arc::new(PluginRegistry::new()),
        }
    }

    pub fn registry(&self) -> Arc<PluginRegistry> {
        self.registry.clone()
    }

    pub async fn spawn_named(
        &self,
        plugin_id: &str,
        command: Command,
        envs: &[(&str, &str)],
    ) -> Result<PluginProcess, PluginHostError> {
        self.registry.register(plugin_id)?;
        match PluginProcess::spawn(command, envs).await {
            Ok(process) => {
                self.registry.set_status(plugin_id, PluginStatus::Running);
                Ok(process)
            }
            Err(error) => {
                self.registry.set_status(plugin_id, PluginStatus::Exited);
                Err(error)
            }
        }
    }
}
