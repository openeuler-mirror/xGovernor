use super::{args, invalid, transport};
use provider_protocol::ProviderControlError;
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::Command,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct DockerLimits {
    pub snapshot_enabled: bool,
    pub snapshot_max_count: usize,
    pub snapshot_max_bytes: u64,
    pub snapshot_timeout_ms: u64,
    pub cpus: u32,
    pub memory_mb: u64,
    pub pids: u32,
    pub max_containers: usize,
    pub timeout_ms: u64,
    pub max_timeout_ms: u64,
    pub file_bytes: usize,
    pub output_bytes: usize,
    pub search_entries: usize,
    pub disk_bytes: u64,
    pub reserve_bytes: u64,
    pub monitor_ms: u64,
}
impl Default for DockerLimits {
    fn default() -> Self {
        Self {
            snapshot_enabled: false,
            snapshot_max_count: 16,
            snapshot_max_bytes: 8 * 1024 * 1024 * 1024,
            snapshot_timeout_ms: 300_000,
            cpus: 1,
            memory_mb: 1024,
            pids: 128,
            max_containers: 2,
            timeout_ms: 60_000,
            max_timeout_ms: 3_600_000,
            file_bytes: 4 * 1024 * 1024,
            output_bytes: 4 * 1024 * 1024,
            search_entries: 1000,
            disk_bytes: 2 * 1024 * 1024 * 1024,
            reserve_bytes: 5 * 1024 * 1024 * 1024,
            monitor_ms: 5000,
        }
    }
}
#[derive(Clone, Debug)]
pub struct DockerConfig {
    pub executable: String,
    pub socket: String,
    pub image: String,
    pub deployment: String,
    pub limits: DockerLimits,
    pub state_dir: PathBuf,
}
fn env(name: &str, default: &str) -> String {
    std::env::var(format!("XGOVERNOR_DOCKER_{name}")).unwrap_or_else(|_| default.into())
}
fn number<T: std::str::FromStr>(name: &str, default: T) -> Result<T, ProviderControlError> {
    match std::env::var(format!("XGOVERNOR_DOCKER_{name}")) {
        Ok(v) => v
            .parse()
            .map_err(|_| invalid(format!("invalid XGOVERNOR_DOCKER_{name}"))),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(_) => Err(invalid(format!("invalid XGOVERNOR_DOCKER_{name}"))),
    }
}
impl DockerConfig {
    pub fn from_env() -> Result<Self, ProviderControlError> {
        let mut limits = DockerLimits::default();
        macro_rules! load { ($($field:ident => $name:literal),*) => {$ (limits.$field = number($name, limits.$field)?;)*}; }
        load!(cpus=>"CPUS", memory_mb=>"MEMORY_MB", pids=>"PIDS", max_containers=>"MAX_CONTAINERS",
            timeout_ms=>"TIMEOUT_MS", max_timeout_ms=>"MAX_TIMEOUT_MS", file_bytes=>"FILE_BYTES",
            output_bytes=>"OUTPUT_BYTES", search_entries=>"SEARCH_ENTRIES", disk_bytes=>"DISK_BYTES",
            reserve_bytes=>"RESERVE_BYTES", monitor_ms=>"MONITOR_MS");
        limits.snapshot_enabled = match env("SNAPSHOT_ENABLED", "0").as_str() {
            "0" => false,
            "1" => true,
            _ => return Err(invalid("SNAPSHOT_ENABLED must be 0 or 1")),
        };
        load!(snapshot_max_count=>"SNAPSHOT_MAX_COUNT", snapshot_max_bytes=>"SNAPSHOT_MAX_BYTES", snapshot_timeout_ms=>"SNAPSHOT_TIMEOUT_MS");
        let fallback = std::env::var("XGOVERNOR_DB_PATH")
            .ok()
            .map(PathBuf::from)
            .and_then(|p| p.parent().map(|p| p.join("docker-state")))
            .unwrap_or_else(|| PathBuf::from(".xgovernor/docker-state"));
        let config = Self {
            executable: env("CLI", "docker"),
            socket: env("SOCKET", "/var/run/docker.sock"),
            image: env("IMAGE", "xgovernor-tool-prototype:v2"),
            deployment: env("DEPLOYMENT", "docker-prototype"),
            limits,
            state_dir: std::env::var_os("XGOVERNOR_DOCKER_STATE_DIR")
                .map(PathBuf::from)
                .unwrap_or(fallback),
        };
        config.validate()?;
        Ok(config)
    }
    pub(super) fn validate(&self) -> Result<(), ProviderControlError> {
        let l = &self.limits;
        if !self.socket.starts_with('/')
            || self.executable.is_empty()
            || self.image.is_empty()
            || self.image.starts_with('-')
            || self.deployment.is_empty()
            || self.deployment.len() > 64
            || !self
                .deployment
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        {
            return Err(invalid(
                "Docker requires a local socket, executable, image and safe deployment identifier",
            ));
        }
        if l.snapshot_max_count == 0
            || l.snapshot_max_count > 100_000
            || l.snapshot_max_bytes == 0
            || !(1000..=3_600_000).contains(&l.snapshot_timeout_ms)
            || l.cpus == 0
            || l.cpus > 1024
            || l.memory_mb < 16
            || l.memory_mb > 1_048_576
            || l.pids < 8
            || l.max_containers == 0
            || l.max_containers > 1000
            || l.timeout_ms == 0
            || l.timeout_ms > l.max_timeout_ms
            || l.max_timeout_ms > 86_400_000
            || l.file_bytes == 0
            || l.file_bytes > 64 * 1024 * 1024
            || l.output_bytes == 0
            || l.output_bytes > 64 * 1024 * 1024
            || l.search_entries == 0
            || l.search_entries > 100_000
            || l.disk_bytes == 0
            || l.reserve_bytes == 0
            || !(100..=60_000).contains(&l.monitor_ms)
        {
            return Err(invalid("invalid Docker resource or operation limits"));
        }
        Ok(())
    }
    pub(crate) async fn run(
        &self,
        args: &[String],
        input: Option<Vec<u8>>,
        timeout_ms: u64,
    ) -> Result<std::process::Output, ProviderControlError> {
        let mut command = Command::new(&self.executable);
        command
            .arg("--host")
            .arg(format!("unix://{}", self.socket))
            .args(args)
            .env_remove("DOCKER_CONTEXT")
            .env_remove("DOCKER_HOST")
            .env_remove("DOCKER_TLS_VERIFY")
            .env_remove("DOCKER_CERT_PATH")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn().map_err(transport)?;
        let mut stdin = child.stdin.take().expect("piped stdin");
        let stdout = child.stdout.take().expect("piped stdout");
        let stderr = child.stderr.take().expect("piped stderr");
        // JSON/base64 framing overhead is bounded separately from raw tool output.
        let cap = self
            .limits
            .file_bytes
            .max(self.limits.output_bytes)
            .saturating_mul(2)
            + 1024 * 1024;
        let result = tokio::time::timeout(Duration::from_millis(timeout_ms), async {
            let write = async move {
                if let Some(input) = input {
                    stdin.write_all(&input).await.map_err(transport)?;
                }
                drop(stdin);
                Ok::<_, ProviderControlError>(())
            };
            let (_, stdout, stderr, status) = tokio::try_join!(
                write,
                bounded(stdout, cap),
                bounded(stderr, 1024 * 1024),
                async { child.wait().await.map_err(transport) }
            )?;
            Ok(std::process::Output {
                status,
                stdout,
                stderr,
            })
        })
        .await
        .unwrap_or_else(|_| {
            Err(ProviderControlError::Timeout {
                operation: "docker CLI".into(),
                timeout_ms,
            })
        });
        if result.is_err() {
            let _ = child.kill().await;
        }
        result
    }
    pub(super) async fn checked(
        &self,
        args: Vec<String>,
    ) -> Result<std::process::Output, ProviderControlError> {
        let output = self.run(&args, None, 30_000).await?;
        if !output.status.success() {
            return Err(transport(String::from_utf8_lossy(&output.stderr)));
        }
        Ok(output)
    }
    pub(super) async fn free_bytes(&self) -> Result<u64, ProviderControlError> {
        let out = self
            .checked(args(&["info", "--format", "{{.DockerRootDir}}"]))
            .await?;
        let path = String::from_utf8(out.stdout).map_err(transport)?;
        #[cfg(unix)]
        {
            let path = std::ffi::CString::new(path.trim()).map_err(transport)?;
            let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
            if unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) } != 0 {
                return Err(transport(std::io::Error::last_os_error()));
            }
            let stat = unsafe { stat.assume_init() };
            Ok((stat.f_bavail as u64).saturating_mul(stat.f_frsize as u64))
        }
        #[cfg(not(unix))]
        {
            let _ = path;
            Err(invalid("Docker backend requires a local Unix host"))
        }
    }
}
async fn bounded(
    reader: impl AsyncRead + Unpin,
    limit: usize,
) -> Result<Vec<u8>, ProviderControlError> {
    let mut bytes = Vec::new();
    reader
        .take(limit as u64 + 1)
        .read_to_end(&mut bytes)
        .await
        .map_err(transport)?;
    if bytes.len() > limit {
        return Err(transport("Docker response exceeds configured buffer limit"));
    }
    Ok(bytes)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn bounds_transport_output() {
        assert!(bounded(&b"12345"[..], 4).await.is_err());
        assert_eq!(bounded(&b"1234"[..], 4).await.unwrap(), b"1234");
    }
    #[test]
    fn validates_limits() {
        let mut c = DockerConfig {
            executable: "docker".into(),
            socket: "/run/docker.sock".into(),
            image: "test".into(),
            deployment: "test".into(),
            limits: Default::default(),
            state_dir: "/tmp/test".into(),
        };
        assert!(c.validate().is_ok());
        c.limits.timeout_ms = c.limits.max_timeout_ms + 1;
        assert!(c.validate().is_err());
        c.limits = Default::default();
        c.limits.file_bytes = usize::MAX;
        assert!(c.validate().is_err());
        c.limits = Default::default();
        c.limits.max_containers = 0;
        assert!(c.validate().is_err());
    }
}
