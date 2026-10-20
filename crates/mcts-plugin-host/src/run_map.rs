use std::collections::{BTreeMap, BTreeSet};
use std::sync::RwLock;

use crate::error::PluginHostError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobStatus {
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone)]
pub struct BranchRecord {
    pub run_id: String,
    pub parent_state_id: String,
}

#[derive(Debug, Clone)]
pub struct StateRecord {
    pub run_id: String,
    pub branch_id: String,
}

#[derive(Debug, Clone)]
pub struct JobRecord {
    pub run_id: String,
    pub branch_id: String,
    pub status: JobStatus,
}

#[derive(Default)]
struct RunMapInner {
    runs: BTreeSet<String>,
    branches: BTreeMap<String, BranchRecord>,
    states: BTreeMap<String, StateRecord>,
    jobs: BTreeMap<String, JobRecord>,
}

#[derive(Default)]
pub struct RunMap {
    inner: RwLock<RunMapInner>,
}

impl RunMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_run(&self, run_id: &str) -> Result<(), PluginHostError> {
        let mut inner = self.inner.write().map_err(|_| {
            PluginHostError::Manifest("run map lock poisoned".to_string())
        })?;
        if inner.runs.insert(run_id.to_string()) {
            Ok(())
        } else {
            Err(PluginHostError::IdempotencyConflict(format!(
                "run already registered: {run_id}"
            )))
        }
    }

    pub fn require_run(&self, run_id: &str) -> Result<(), PluginHostError> {
        let inner = self.inner.read().map_err(|_| {
            PluginHostError::Manifest("run map lock poisoned".to_string())
        })?;
        if inner.runs.contains(run_id) {
            Ok(())
        } else {
            Err(PluginHostError::RunNotFound(run_id.to_string()))
        }
    }

    pub fn add_branch(
        &self,
        run_id: &str,
        branch_id: &str,
        parent_state_id: &str,
    ) -> Result<(), PluginHostError> {
        self.require_run(run_id)?;
        let mut inner = self.inner.write().map_err(|_| {
            PluginHostError::Manifest("run map lock poisoned".to_string())
        })?;
        if inner.branches.contains_key(branch_id) {
            return Err(PluginHostError::IdempotencyConflict(format!(
                "branch already exists: {branch_id}"
            )));
        }
        inner.branches.insert(
            branch_id.to_string(),
            BranchRecord {
                run_id: run_id.to_string(),
                parent_state_id: parent_state_id.to_string(),
            },
        );
        Ok(())
    }

    pub fn remove_branch(&self, branch_id: &str) -> Result<(), PluginHostError> {
        let mut inner = self.inner.write().map_err(|_| {
            PluginHostError::Manifest("run map lock poisoned".to_string())
        })?;
        if inner.branches.remove(branch_id).is_some() {
            Ok(())
        } else {
            Err(PluginHostError::BranchNotFound(branch_id.to_string()))
        }
    }

    pub fn require_branch_in_run(&self, run_id: &str, branch_id: &str) -> Result<(), PluginHostError> {
        self.require_run(run_id)?;
        let inner = self.inner.read().map_err(|_| {
            PluginHostError::Manifest("run map lock poisoned".to_string())
        })?;
        match inner.branches.get(branch_id) {
            Some(branch) if branch.run_id == run_id => Ok(()),
            Some(_) => Err(PluginHostError::BranchNotFound(format!(
                "branch {branch_id} does not belong to run {run_id}"
            ))),
            None => Err(PluginHostError::BranchNotFound(branch_id.to_string())),
        }
    }

    pub fn add_state(
        &self,
        run_id: &str,
        state_id: &str,
        branch_id: &str,
    ) -> Result<(), PluginHostError> {
        self.require_branch_in_run(run_id, branch_id)?;
        let mut inner = self.inner.write().map_err(|_| {
            PluginHostError::Manifest("run map lock poisoned".to_string())
        })?;
        inner.states.insert(
            state_id.to_string(),
            StateRecord {
                run_id: run_id.to_string(),
                branch_id: branch_id.to_string(),
            },
        );
        Ok(())
    }

    pub fn require_state(&self, state_id: &str) -> Result<(), PluginHostError> {
        let inner = self.inner.read().map_err(|_| {
            PluginHostError::Manifest("run map lock poisoned".to_string())
        })?;
        if inner.states.contains_key(state_id) {
            Ok(())
        } else {
            Err(PluginHostError::BranchNotFound(format!(
                "state not found: {state_id}"
            )))
        }
    }

    pub fn add_job(&self, run_id: &str, branch_id: &str, job_id: &str) -> Result<(), PluginHostError> {
        self.require_branch_in_run(run_id, branch_id)?;
        let mut inner = self.inner.write().map_err(|_| {
            PluginHostError::Manifest("run map lock poisoned".to_string())
        })?;
        inner.jobs.insert(
            job_id.to_string(),
            JobRecord {
                run_id: run_id.to_string(),
                branch_id: branch_id.to_string(),
                status: JobStatus::Running,
            },
        );
        Ok(())
    }

    pub fn set_job_status(&self, job_id: &str, status: JobStatus) -> Result<(), PluginHostError> {
        let mut inner = self.inner.write().map_err(|_| {
            PluginHostError::Manifest("run map lock poisoned".to_string())
        })?;
        let job = inner
            .jobs
            .get_mut(job_id)
            .ok_or_else(|| PluginHostError::BranchNotFound(format!("job not found: {job_id}")))?;
        job.status = status;
        Ok(())
    }

    pub fn job(&self, job_id: &str) -> Option<JobRecord> {
        self.inner
            .read()
            .ok()
            .and_then(|inner| inner.jobs.get(job_id).cloned())
    }

    pub fn branch_record(&self, branch_id: &str) -> Option<BranchRecord> {
        self.inner
            .read()
            .ok()
            .and_then(|inner| inner.branches.get(branch_id).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracks_branches_states_and_run_ownership() {
        let map = RunMap::new();
        map.register_run("run-1").unwrap();
        map.add_branch("run-1", "branch-1", "state-root").unwrap();
        map.add_state("run-1", "state-1", "branch-1").unwrap();

        assert!(map.require_branch_in_run("run-1", "branch-1").is_ok());
        assert!(map.require_branch_in_run("run-2", "branch-1").is_err());
        assert!(map.require_state("state-1").is_ok());
        assert!(map.require_state("state-missing").is_err());
    }

    #[test]
    fn rejects_duplicate_branch() {
        let map = RunMap::new();
        map.register_run("run-1").unwrap();
        map.add_branch("run-1", "branch-1", "state-root").unwrap();
        assert!(map.add_branch("run-1", "branch-1", "state-root").is_err());
    }
}
