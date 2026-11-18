use std::sync::Mutex;

use orchestration_contract::execution::*;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;

use crate::{error, internal};

/// Run-private durable identities. An unresolved row survives process loss;
/// callers must reclaim it rather than dispatching a replacement mutation.
pub(crate) struct Store(Mutex<Connection>);
impl Store {
    pub fn open(path: &std::path::Path) -> BackendResult<Self> {
        let db = Connection::open(path).map_err(internal)?;
        db.execute_batch("PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS own_objects (kind TEXT, id TEXT, body TEXT NOT NULL, PRIMARY KEY(kind,id));
            CREATE TABLE IF NOT EXISTS own_mutations (method TEXT, key TEXT, args TEXT NOT NULL, result TEXT, PRIMARY KEY(method,key));")
            .map_err(internal)?;
        Ok(Self(Mutex::new(db)))
    }
    pub fn get<T: DeserializeOwned>(&self, kind: &str, id: &str) -> BackendResult<Option<T>> {
        let value: Option<String> = self
            .0
            .lock()
            .unwrap()
            .query_row(
                "SELECT body FROM own_objects WHERE kind=?1 AND id=?2",
                params![kind, id],
                |r| r.get(0),
            )
            .optional()
            .map_err(internal)?;
        value
            .map(|v| serde_json::from_str(&v).map_err(internal))
            .transpose()
    }
    pub fn put<T: Serialize>(&self, kind: &str, id: &str, value: &T) -> BackendResult<()> {
        self.0.lock().unwrap().execute("INSERT INTO own_objects VALUES (?1,?2,?3) ON CONFLICT(kind,id) DO UPDATE SET body=excluded.body",
            params![kind,id,serde_json::to_string(value).map_err(internal)?]).map_err(internal)?;
        Ok(())
    }
    pub fn update<T: DeserializeOwned + Serialize>(
        &self,
        kind: &str,
        id: &str,
        update: impl FnOnce(&mut T),
    ) -> BackendResult<T> {
        let db = self.0.lock().unwrap();
        let value: String = db
            .query_row(
                "SELECT body FROM own_objects WHERE kind=?1 AND id=?2",
                params![kind, id],
                |r| r.get(0),
            )
            .map_err(internal)?;
        let mut value: T = serde_json::from_str(&value).map_err(internal)?;
        update(&mut value);
        db.execute(
            "UPDATE own_objects SET body=?3 WHERE kind=?1 AND id=?2",
            params![kind, id, serde_json::to_string(&value).map_err(internal)?],
        )
        .map_err(internal)?;
        Ok(value)
    }
    pub fn list<T: DeserializeOwned>(&self, kind: &str) -> BackendResult<Vec<(String, T)>> {
        let db = self.0.lock().unwrap();
        let mut statement = db
            .prepare("SELECT id,body FROM own_objects WHERE kind=?1")
            .map_err(internal)?;
        let rows = statement
            .query_map([kind], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })
            .map_err(internal)?;
        rows.map(|r| {
            let (id, body) = r.map_err(internal)?;
            Ok((id, serde_json::from_str(&body).map_err(internal)?))
        })
        .collect()
    }
    pub fn begin(
        &self,
        method: &str,
        key: &str,
        args: Value,
    ) -> BackendResult<Option<Result<MutationResult, BackendError>>> {
        if key.trim().is_empty() {
            return Err(error(ErrorCode::InvalidParams, "empty idempotency key"));
        }
        let db = self.0.lock().unwrap();
        let existing: Option<(String, Option<String>)> = db
            .query_row(
                "SELECT args,result FROM own_mutations WHERE method=?1 AND key=?2",
                params![method, key],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .map_err(internal)?;
        let mut args = args;
        // The wait budget may change on retry; the dispatched payload may not.
        args.as_object_mut().unwrap().remove("timeout_ms");
        let args = serde_json::to_string(&args).map_err(internal)?;
        if let Some((original, result)) = existing {
            if original != args {
                return Err(error(
                    ErrorCode::IdempotencyConflict,
                    "idempotency arguments changed",
                ));
            }
            return result
                .map(|v| serde_json::from_str(&v).map_err(internal))
                .transpose()
                .map(|v| v.or(Some(Err(error(ErrorCode::BranchBusy, "operation pending")))));
        }
        db.execute(
            "INSERT INTO own_mutations VALUES (?1,?2,?3,NULL)",
            params![method, key, args],
        )
        .map_err(internal)?;
        Ok(None)
    }
    pub fn finish(
        &self,
        op: &OperationIdentity,
        result: &Result<MutationResult, BackendError>,
    ) -> BackendResult<()> {
        self.0
            .lock()
            .unwrap()
            .execute(
                "UPDATE own_mutations SET result=?3 WHERE method=?1 AND key=?2",
                params![
                    op.method,
                    op.idempotency_key,
                    serde_json::to_string(result).map_err(internal)?
                ],
            )
            .map_err(internal)?;
        Ok(())
    }
    pub fn unresolved(&self) -> BackendResult<Vec<String>> {
        let db = self.0.lock().unwrap();
        let mut statement = db
            .prepare(
                "SELECT method,key,result FROM own_mutations WHERE method != 'host.reclaim_run'",
            )
            .map_err(internal)?;
        let rows = statement
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, Option<String>>(2)?,
                ))
            })
            .map_err(internal)?;
        let mut unresolved = Vec::new();
        for row in rows {
            let (method, key, result) = row.map_err(internal)?;
            if result.is_none()
                || result.as_ref().is_some_and(|s| {
                    serde_json::from_str::<Result<MutationResult, BackendError>>(s)
                        .is_ok_and(|r| matches!(r, Err(BackendError::OutcomeUnknown { .. })))
                })
            {
                let identity = format!("{method}:{key}");
                let reclaimed: bool = db.query_row(
                    "SELECT EXISTS(SELECT 1 FROM own_objects WHERE kind='reclaimed_operation' AND id=?1)",
                    [&identity], |r| r.get(0),
                ).map_err(internal)?;
                if method != "host.fork" || !reclaimed {
                    unresolved.push(identity);
                }
            }
        }
        Ok(unresolved)
    }
    pub fn resolution(&self, op: &OperationIdentity) -> BackendResult<OperationResolution> {
        let value: Option<Option<String>> = self
            .0
            .lock()
            .unwrap()
            .query_row(
                "SELECT result FROM own_mutations WHERE method=?1 AND key=?2",
                params![op.method, op.idempotency_key],
                |r| r.get(0),
            )
            .optional()
            .map_err(internal)?;
        Ok(match value {
            None => OperationResolution::NotStarted,
            Some(None) => OperationResolution::Unknown,
            Some(Some(v)) => match serde_json::from_str::<Result<MutationResult, BackendError>>(&v)
                .map_err(internal)?
            {
                Ok(result) => OperationResolution::Succeeded(result),
                Err(BackendError::Rejected { code, message }) => {
                    OperationResolution::Rejected { code, message }
                }
                Err(BackendError::OutcomeUnknown { .. }) => OperationResolution::Unknown,
            },
        })
    }
}
