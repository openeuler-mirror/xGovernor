//! Explicit Host-side connection configuration for the pinned Management SDK.
//!
//! This crate is not registered as a Session runtime or execution backend.

mod cancel_message;
pub mod durable_operation;
pub mod message;
#[cfg(unix)]
pub mod profile;
pub mod root_owner;

use std::collections::BTreeMap;
use std::fmt;
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use sdk::{ClientOptions, ManagementError, ThinkThreadClient};
pub use thinkthread_management as sdk;

/// The Management v2 contract paired with the pinned alpha11 SDK.
pub const EXPECTED_CONTRACT_FINGERPRINT: &str =
    "8e65d3658ebc674bf3f263b93c813e630e991bf870bc2118535ab0f2313d9fb6";

/// Explicit deployment context; no roots or environment are inherited by default.
#[derive(Clone)]
pub struct ManagementConfig {
    pub host_executable: PathBuf,
    pub state_root: PathBuf,
    pub runtime_root: PathBuf,
    pub config_root: PathBuf,
    pub cache_root: PathBuf,
    pub working_directory: PathBuf,
    pub fs_root: PathBuf,
    pub home_directory: PathBuf,
    pub executable_search_path: Vec<PathBuf>,
    pub base_environment: BTreeMap<String, String>,
    pub connect_timeout: Duration,
}

impl fmt::Debug for ManagementConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ManagementConfig")
            .field("host_executable", &self.host_executable)
            .field("state_root", &self.state_root)
            .field("runtime_root", &self.runtime_root)
            .field("config_root", &self.config_root)
            .field("cache_root", &self.cache_root)
            .field("working_directory", &self.working_directory)
            .field("fs_root", &self.fs_root)
            .field("home_directory", &self.home_directory)
            .field("executable_search_path", &self.executable_search_path)
            .field("environment_keys", &self.base_environment.keys())
            .field("connect_timeout", &self.connect_timeout)
            .finish()
    }
}

/// Connection failures retain the SDK's protocol and delivery distinctions.
#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    #[error("invalid Management configuration field {field}: {reason}")]
    Configuration {
        field: &'static str,
        reason: &'static str,
    },
    #[error("Management Host connection timed out")]
    Timeout,
    #[error(transparent)]
    Sdk(#[from] ManagementError),
}

impl ManagementConfig {
    /// Validates deployment inputs without creating directories or a process.
    pub fn validate(&self) -> Result<(), ConnectError> {
        if self.connect_timeout.is_zero() {
            return Err(invalid("connect_timeout", "must be positive"));
        }
        for (field, path) in [
            ("host_executable", &self.host_executable),
            ("state_root", &self.state_root),
            ("runtime_root", &self.runtime_root),
            ("config_root", &self.config_root),
            ("cache_root", &self.cache_root),
            ("working_directory", &self.working_directory),
            ("fs_root", &self.fs_root),
            ("home_directory", &self.home_directory),
        ] {
            validate_path(field, path)?;
        }
        for path in &self.executable_search_path {
            validate_path("executable_search_path", path)?;
        }
        for (key, value) in &self.base_environment {
            let mut bytes = key.bytes();
            let valid_first = bytes
                .next()
                .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_');
            if !valid_first
                || !bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
                || key.starts_with("THINKTHREAD_")
                || value.contains('\0')
            {
                return Err(invalid("base_environment", "contains an invalid entry"));
            }
        }
        Ok(())
    }

    /// Starts only the configured Host and lets the SDK enforce exact handshake.
    /// The returned native client performs no automatic mutation retries.
    pub async fn connect(&self) -> Result<ThinkThreadClient, ConnectError> {
        self.validate()?;
        let options = ClientOptions {
            host_executable: Some(self.host_executable.clone()),
            state_root: Some(self.state_root.clone()),
            runtime_root: Some(self.runtime_root.clone()),
            config_root: Some(self.config_root.clone()),
            cache_root: Some(self.cache_root.clone()),
            working_directory: Some(self.working_directory.clone()),
            fs_root: Some(self.fs_root.clone()),
            home_directory: Some(self.home_directory.clone()),
            executable_search_path: Some(self.executable_search_path.clone()),
            base_environment: Some(self.base_environment.clone()),
        };
        // Cancelling connect drops the SDK's kill_on_drop Host child. No native
        // lifecycle or Message mutation has been exposed before handshake/bind.
        tokio::time::timeout(self.connect_timeout, ThinkThreadClient::connect(options))
            .await
            .map_err(|_| ConnectError::Timeout)?
            .map_err(ConnectError::Sdk)
    }
}

fn invalid(field: &'static str, reason: &'static str) -> ConnectError {
    ConnectError::Configuration { field, reason }
}

fn validate_path(field: &'static str, path: &Path) -> Result<(), ConnectError> {
    if !path.is_absolute()
        || path.to_str().is_none()
        || path.as_os_str().as_encoded_bytes().len() > 4096
        || path.as_os_str().as_encoded_bytes().contains(&0)
        || path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::CurDir | Component::Prefix(_)
            )
        })
    {
        return Err(invalid(
            field,
            "must be an absolute UTF-8 path without traversal",
        ));
    }
    Ok(())
}

pub mod host;
