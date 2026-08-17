//! Optional e2b bootstrap archive injection.
//!
//! Packages a local workspace directory and a caller-resolved list of skill
//! directories into a deterministic, uncompressed tar archive, uploads it
//! into the sandbox over envd, and extracts it under the sandbox's
//! configured workspace root and a configurable skills root.
//!
//! Design note (see task history): xiaoO's original implementation also
//! *selected* which skills to inject, via a `SkillsConfig`/`load_skills`
//! dependency on a `skill` crate that this workspace does not (and, per the
//! xiaoO/xGovernor two-repo split, should not) depend on. This port
//! deliberately narrows scope: the e2b provider only packages and uploads
//! whatever directories it is given in `skill_dirs` — selection, parsing of
//! SKILL.md/SKILL.toml, and deduplication are entirely the caller's
//! responsibility (e.g. a layer that already depends on skill-resolution
//! logic). This keeps the provider a pure infrastructure/transport concern.
//!
//! Also unlike the original (which built the archive on disk via a
//! `tempfile`-backed, `Drop`-cleaned file), this builds the archive
//! in-memory (`Vec<u8>`, bounded by [`E2B_BOOTSTRAP_MAX_TOTAL_BYTES`]) since
//! there is no cross-process handoff to support here — avoids an extra
//! dependency and disk-cleanup bookkeeping for what is a bounded, one-shot
//! payload.

use std::fs::{File, Metadata};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use reqwest::header::{ACCEPT, CONTENT_TYPE};
use reqwest::Method;
use sha2::{Digest, Sha256};
use thiserror::Error;

#[cfg(unix)]
use std::os::unix::fs::{FileTypeExt, MetadataExt};

use super::backend::{http_error, shell_quote, E2bBackendState};
use super::exec::E2bExec;

pub const E2B_BOOTSTRAP_MAX_ENTRIES: u64 = 100_000;
pub const E2B_BOOTSTRAP_MAX_FILE_BYTES: u64 = 128 * 1024 * 1024;
pub const E2B_BOOTSTRAP_MAX_TOTAL_BYTES: u64 = 1024 * 1024 * 1024;

/// Caller-resolved bootstrap payload: what to package, and where to put it
/// remotely. The provider does not resolve or select skills itself —
/// `skill_dirs` is expected to already be the final, deduplicated list the
/// caller wants injected verbatim.
#[derive(Debug, Clone, Default)]
pub struct E2bBootstrapPlan {
    pub workspace: Option<PathBuf>,
    pub skill_dirs: Vec<PathBuf>,
    pub remote_skills_root: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct E2bBootstrapSkillEntry {
    pub source: PathBuf,
    pub remote_dir: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct E2bBootstrapSummary {
    pub archive_sha256: String,
    pub archive_size_bytes: u64,
    pub remote_workspace_root: String,
    pub remote_skills_root: String,
    pub skills: Vec<E2bBootstrapSkillEntry>,
}

#[derive(Debug, Error)]
pub enum E2bBootstrapError {
    #[error("invalid bootstrap path: {message}")]
    InvalidPath { message: String },
    #[error("bootstrap source changed while being archived: {path}")]
    SourceChanged { path: PathBuf },
    #[error("bootstrap capacity exceeded: {message}")]
    CapacityExceeded { message: String },
    #[error("failed to build bootstrap archive: {message}")]
    BuildFailed { message: String },
    #[error("failed to upload bootstrap archive: {message}")]
    UploadFailed { message: String },
    #[error("failed to extract bootstrap archive: {message}")]
    ExtractFailed { message: String },
}

pub fn canonicalize_bootstrap_dir(path: &Path) -> Result<PathBuf, E2bBootstrapError> {
    if !path.is_absolute() {
        return Err(E2bBootstrapError::InvalidPath {
            message: format!("path must be absolute: {}", path.display()),
        });
    }
    let canonical =
        std::fs::canonicalize(path).map_err(|error| E2bBootstrapError::InvalidPath {
            message: format!("cannot access {}: {error}", path.display()),
        })?;
    let metadata =
        std::fs::metadata(&canonical).map_err(|error| E2bBootstrapError::InvalidPath {
            message: format!("cannot read {}: {error}", canonical.display()),
        })?;
    if !metadata.is_dir() {
        return Err(E2bBootstrapError::InvalidPath {
            message: format!("path is not a directory: {}", canonical.display()),
        });
    }
    std::fs::read_dir(&canonical).map_err(|error| E2bBootstrapError::InvalidPath {
        message: format!("directory is not readable {}: {error}", canonical.display()),
    })?;
    Ok(canonical)
}

/// Build the archive, upload it into the sandbox, and extract it under the
/// sandbox's configured workspace root (from `state`) and `plan`'s skills
/// root. Returns a summary suitable for embedding in instance metadata.
pub async fn apply_e2b_bootstrap(
    state: &Arc<E2bBackendState>,
    plan: &E2bBootstrapPlan,
) -> Result<E2bBootstrapSummary, E2bBootstrapError> {
    let workspace = plan
        .workspace
        .as_deref()
        .map(canonicalize_bootstrap_dir)
        .transpose()?;
    let skill_dirs = plan
        .skill_dirs
        .iter()
        .map(|dir| canonicalize_bootstrap_dir(dir))
        .collect::<Result<Vec<_>, _>>()?;

    let remote_workspace_root = state.workspace_root.0.clone();
    let (bytes, sha256, size_bytes, skills) = build_archive(
        workspace.as_deref(),
        &skill_dirs,
        remote_workspace_root.as_str(),
        plan.remote_skills_root.as_str(),
    )?;

    let remote_archive_path = format!("{}/.xgovernor-bootstrap.tar", state.temp_root.0);
    upload_bytes(state, remote_archive_path.as_str(), bytes).await?;

    let exec = E2bExec::new(Arc::clone(state));
    let script = format!(
        "set -e\nmkdir -p {workspace_root} {skills_root}\ntmp=$(mktemp -d)\ntar -xf {archive} -C \"$tmp\"\ncp -a \"$tmp/workspace/.\" {workspace_root}/\ncp -a \"$tmp/skills/.\" {skills_root}/ 2>/dev/null || true\nrm -rf \"$tmp\" {archive}\n",
        workspace_root = shell_quote(remote_workspace_root.as_str()),
        skills_root = shell_quote(plan.remote_skills_root.as_str()),
        archive = shell_quote(remote_archive_path.as_str()),
    );
    let output = exec
        .run_shell_script_detailed(script.as_str(), None)
        .await
        .map_err(|failure| E2bBootstrapError::ExtractFailed {
            message: failure.message().to_string(),
        })?;
    if output.exit_code != Some(0) {
        return Err(E2bBootstrapError::ExtractFailed {
            message: String::from_utf8_lossy(output.stderr.as_slice()).to_string(),
        });
    }

    Ok(E2bBootstrapSummary {
        archive_sha256: sha256,
        archive_size_bytes: size_bytes,
        remote_workspace_root,
        remote_skills_root: plan.remote_skills_root.clone(),
        skills,
    })
}

async fn upload_bytes(
    state: &Arc<E2bBackendState>,
    remote_path: &str,
    content: Vec<u8>,
) -> Result<(), E2bBootstrapError> {
    let request = state
        .envd_request(Method::POST, "/files")
        .query(&[("path", remote_path)])
        .header(ACCEPT, "application/json");
    let response = if state.envd_file_upload_multipart {
        use reqwest::multipart::{Form, Part};
        let name = remote_path
            .rsplit('/')
            .next()
            .unwrap_or("bootstrap.tar")
            .to_string();
        request
            .multipart(Form::new().part("file", Part::bytes(content).file_name(name)))
            .send()
    } else {
        request
            .header(CONTENT_TYPE, "application/octet-stream")
            .body(content)
            .send()
    }
    .await
    .map_err(|error| E2bBootstrapError::UploadFailed {
        message: format!("failed to upload bootstrap archive: {error}"),
    })?;

    if response.status().is_success() {
        return Ok(());
    }
    let error = http_error("upload e2b bootstrap archive", response).await;
    Err(E2bBootstrapError::UploadFailed {
        message: error.to_string(),
    })
}

#[allow(clippy::type_complexity)]
fn build_archive(
    workspace: Option<&Path>,
    skill_dirs: &[PathBuf],
    remote_workspace_root: &str,
    remote_skills_root: &str,
) -> Result<(Vec<u8>, String, u64, Vec<E2bBootstrapSkillEntry>), E2bBootstrapError> {
    let mut bytes = Vec::new();
    let mut limits = ArchiveLimits::default();
    let mut skills = Vec::new();
    {
        let mut tar = tar::Builder::new(&mut bytes);
        tar.mode(tar::HeaderMode::Deterministic);
        if let Some(source) = workspace {
            let metadata = std::fs::metadata(source).map_err(build_io("stat workspace root"))?;
            append_dir(&mut tar, Path::new("workspace"), &metadata)?;
        } else {
            append_synthetic_dir(&mut tar, Path::new("workspace"))?;
        }
        append_synthetic_dir(&mut tar, Path::new("skills"))?;

        if let Some(source) = workspace {
            limits.add_entry(source)?;
            append_tree(
                &mut tar,
                source,
                Path::new("workspace"),
                Path::new(remote_workspace_root),
                &mut limits,
            )?;
        }

        for (ordinal, dir) in skill_dirs.iter().enumerate() {
            let source = std::fs::canonicalize(dir).map_err(build_io("canonicalize skill"))?;
            let leaf = format!("skill-{ordinal:05}");
            let archive_dir = PathBuf::from(format!("skills/{leaf}"));
            let remote_dir = PathBuf::from(remote_skills_root).join(&leaf);
            let metadata = std::fs::metadata(&source).map_err(build_io("stat skill directory"))?;
            limits.add_entry(&source)?;
            append_dir(&mut tar, &archive_dir, &metadata)?;
            append_tree(&mut tar, &source, &archive_dir, &remote_dir, &mut limits)?;
            skills.push(E2bBootstrapSkillEntry {
                source,
                remote_dir: remote_dir.to_string_lossy().to_string(),
            });
        }
        tar.finish().map_err(build_io("finish archive"))?;
    }

    let mut digest = Sha256::new();
    digest.update(&bytes);
    let sha256 = format!("{:x}", digest.finalize());
    let size_bytes = bytes.len() as u64;

    Ok((bytes, sha256, size_bytes, skills))
}

#[derive(Default)]
struct ArchiveLimits {
    entries: u64,
    total_bytes: u64,
}

impl ArchiveLimits {
    fn add_entry(&mut self, path: &Path) -> Result<(), E2bBootstrapError> {
        self.entries = self.entries.saturating_add(1);
        if self.entries > E2B_BOOTSTRAP_MAX_ENTRIES {
            return Err(E2bBootstrapError::CapacityExceeded {
                message: format!(
                    "more than {E2B_BOOTSTRAP_MAX_ENTRIES} entries (at {})",
                    path.display()
                ),
            });
        }
        Ok(())
    }

    fn add_file(&mut self, path: &Path, size: u64) -> Result<(), E2bBootstrapError> {
        self.add_entry(path)?;
        if size > E2B_BOOTSTRAP_MAX_FILE_BYTES {
            return Err(E2bBootstrapError::CapacityExceeded {
                message: format!(
                    "file {} is {size} bytes; per-file limit is {E2B_BOOTSTRAP_MAX_FILE_BYTES}",
                    path.display()
                ),
            });
        }
        self.total_bytes = self.total_bytes.saturating_add(size);
        if self.total_bytes > E2B_BOOTSTRAP_MAX_TOTAL_BYTES {
            return Err(E2bBootstrapError::CapacityExceeded {
                message: format!(
                    "regular-file content exceeds {E2B_BOOTSTRAP_MAX_TOTAL_BYTES} bytes"
                ),
            });
        }
        Ok(())
    }
}

fn append_tree(
    tar: &mut tar::Builder<&mut Vec<u8>>,
    source_root: &Path,
    archive_root: &Path,
    remote_root: &Path,
    limits: &mut ArchiveLimits,
) -> Result<(), E2bBootstrapError> {
    let before = std::fs::metadata(source_root).map_err(build_io("stat source directory"))?;
    append_children(
        tar,
        source_root,
        source_root,
        archive_root,
        remote_root,
        limits,
    )?;
    let after = std::fs::metadata(source_root).map_err(build_io("restat source directory"))?;
    if !same_metadata(&before, &after) {
        return Err(E2bBootstrapError::SourceChanged {
            path: source_root.to_path_buf(),
        });
    }
    Ok(())
}

fn append_children(
    tar: &mut tar::Builder<&mut Vec<u8>>,
    source_root: &Path,
    source_dir: &Path,
    archive_dir: &Path,
    remote_root: &Path,
    limits: &mut ArchiveLimits,
) -> Result<(), E2bBootstrapError> {
    let before = std::fs::symlink_metadata(source_dir).map_err(build_io("stat directory"))?;
    let mut entries = std::fs::read_dir(source_dir)
        .map_err(build_io("read source directory"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(build_io("read source entry"))?;
    entries.sort_by_key(|entry| entry.file_name());

    for entry in entries {
        let source = entry.path();
        let metadata = std::fs::symlink_metadata(&source).map_err(build_io("stat source entry"))?;
        let destination = archive_dir.join(entry.file_name());
        let file_type = metadata.file_type();
        if file_type.is_dir() {
            limits.add_entry(&source)?;
            append_dir(tar, &destination, &metadata)?;
            append_children(tar, source_root, &source, &destination, remote_root, limits)?;
        } else if file_type.is_file() {
            limits.add_file(&source, metadata.len())?;
            append_file(tar, &source, &destination, &metadata)?;
        } else if file_type.is_symlink() {
            limits.add_entry(&source)?;
            append_symlink(
                tar,
                source_root,
                remote_root,
                &source,
                &destination,
                &metadata,
            )?;
        } else {
            #[cfg(unix)]
            let kind = if file_type.is_socket() {
                "socket"
            } else if file_type.is_fifo() {
                "FIFO"
            } else if file_type.is_block_device() {
                "block device"
            } else if file_type.is_char_device() {
                "character device"
            } else {
                "special file"
            };
            #[cfg(not(unix))]
            let kind = "special file";
            return Err(E2bBootstrapError::InvalidPath {
                message: format!("unsupported {kind}: {}", source.display()),
            });
        }
    }
    let after = std::fs::symlink_metadata(source_dir).map_err(build_io("restat directory"))?;
    if !same_metadata(&before, &after) {
        return Err(E2bBootstrapError::SourceChanged {
            path: source_dir.to_path_buf(),
        });
    }
    Ok(())
}

fn append_synthetic_dir(
    tar: &mut tar::Builder<&mut Vec<u8>>,
    path: &Path,
) -> Result<(), E2bBootstrapError> {
    let mut header = tar::Header::new_gnu();
    header.set_entry_type(tar::EntryType::Directory);
    header.set_size(0);
    header.set_mode(0o755);
    header.set_uid(0);
    header.set_gid(0);
    header.set_mtime(0);
    header.set_cksum();
    tar.append_data(&mut header, path, io::empty())
        .map_err(build_io("append directory"))
}

fn append_dir(
    tar: &mut tar::Builder<&mut Vec<u8>>,
    destination: &Path,
    metadata: &Metadata,
) -> Result<(), E2bBootstrapError> {
    let mut header = tar::Header::new_gnu();
    header.set_entry_type(tar::EntryType::Directory);
    header.set_size(0);
    header.set_mode(mode(metadata));
    header.set_uid(0);
    header.set_gid(0);
    header.set_mtime(0);
    header.set_cksum();
    tar.append_data(&mut header, destination, io::empty())
        .map_err(build_io("append directory"))
}

fn append_file(
    tar: &mut tar::Builder<&mut Vec<u8>>,
    source: &Path,
    destination: &Path,
    before: &Metadata,
) -> Result<(), E2bBootstrapError> {
    let mut input = File::open(source).map_err(build_io("open source file"))?;
    let mut header = tar::Header::new_gnu();
    header.set_entry_type(tar::EntryType::Regular);
    header.set_size(before.len());
    header.set_mode(mode(before));
    header.set_uid(0);
    header.set_gid(0);
    header.set_mtime(0);
    header.set_cksum();
    let append_result = tar.append_data(&mut header, destination, &mut input);
    let after = input.metadata().map_err(build_io("restat source file"))?;
    if let Err(error) = append_result {
        if !same_metadata(before, &after) {
            return Err(E2bBootstrapError::SourceChanged {
                path: source.to_path_buf(),
            });
        }
        return Err(E2bBootstrapError::BuildFailed {
            message: format!("append source file: {error}"),
        });
    }
    if !same_metadata(before, &after) {
        return Err(E2bBootstrapError::SourceChanged {
            path: source.to_path_buf(),
        });
    }
    Ok(())
}

fn append_symlink(
    tar: &mut tar::Builder<&mut Vec<u8>>,
    source_root: &Path,
    remote_root: &Path,
    source: &Path,
    destination: &Path,
    metadata: &Metadata,
) -> Result<(), E2bBootstrapError> {
    let raw_target = std::fs::read_link(source).map_err(build_io("read symbolic link"))?;
    let resolved = if raw_target.is_absolute() {
        raw_target.clone()
    } else {
        source.parent().unwrap_or(source_root).join(&raw_target)
    };
    let resolved =
        std::fs::canonicalize(&resolved).map_err(|error| E2bBootstrapError::InvalidPath {
            message: format!("invalid symbolic link {}: {error}", source.display()),
        })?;
    if !resolved.starts_with(source_root) {
        return Err(E2bBootstrapError::InvalidPath {
            message: format!(
                "symbolic link escapes input root: {} -> {}",
                source.display(),
                raw_target.display()
            ),
        });
    }
    let target = if raw_target.is_absolute() {
        remote_root.join(resolved.strip_prefix(source_root).unwrap_or(Path::new("")))
    } else {
        raw_target.clone()
    };
    let mut header = tar::Header::new_gnu();
    header.set_entry_type(tar::EntryType::Symlink);
    header.set_size(0);
    header.set_mode(mode(metadata));
    header.set_uid(0);
    header.set_gid(0);
    header.set_mtime(0);
    header
        .set_link_name(&target)
        .map_err(build_io("set symbolic link target"))?;
    header.set_cksum();
    tar.append_data(&mut header, destination, io::empty())
        .map_err(build_io("append symbolic link"))?;
    let after = std::fs::symlink_metadata(source).map_err(build_io("restat symbolic link"))?;
    let current = std::fs::read_link(source).map_err(build_io("reread symbolic link"))?;
    if !same_metadata(metadata, &after) || current != raw_target {
        return Err(E2bBootstrapError::SourceChanged {
            path: source.to_path_buf(),
        });
    }
    Ok(())
}

#[cfg(unix)]
fn mode(metadata: &Metadata) -> u32 {
    metadata.mode() & 0o7777
}

#[cfg(not(unix))]
fn mode(_metadata: &Metadata) -> u32 {
    0o644
}

#[cfg(unix)]
fn same_metadata(left: &Metadata, right: &Metadata) -> bool {
    left.dev() == right.dev()
        && left.ino() == right.ino()
        && left.mode() == right.mode()
        && left.size() == right.size()
        && left.mtime() == right.mtime()
        && left.mtime_nsec() == right.mtime_nsec()
        && left.ctime() == right.ctime()
        && left.ctime_nsec() == right.ctime_nsec()
}

#[cfg(not(unix))]
fn same_metadata(left: &Metadata, right: &Metadata) -> bool {
    left.len() == right.len()
        && left.modified().ok() == right.modified().ok()
        && left.permissions().readonly() == right.permissions().readonly()
}

fn build_io(operation: &'static str) -> impl FnOnce(io::Error) -> E2bBootstrapError {
    move |error| E2bBootstrapError::BuildFailed {
        message: format!("{operation}: {error}"),
    }
}
