//! Current receipt lifetime/window and real Local checkpoint rejection.
//! Runtime events are fixtures; these tests do not attest LLM/E2B readiness.
use super::*;
use std::sync::atomic::{AtomicU32, Ordering};

async fn drain(mut submission: SessionSubmission) -> String {
    let id = submission.receipt.turn_id.clone();
    let mut events = submission.events.take().expect("new turn has a stream");
    while events.recv().await.is_some() {}
    id
}

#[tokio::test]
async fn session_contract_receipt_survives_application_clone_but_not_repository_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("sessions.db");
    let records = Arc::new(SqliteSessionRepository::open(&db).unwrap());
    records.save(test_record()).await.unwrap();
    let runtime = Arc::new(CountingRuntime::default());
    let ids = Arc::new(SequencedIds(AtomicU32::new(1)));
    let make_app = |records: Arc<SqliteSessionRepository>| {
        SessionApplication::new(
            runtime.clone(),
            test_provider_managers(),
            records,
            ids.clone(),
            Arc::new(FixedTurnId),
            Arc::new(UnusedEnvironment),
            Arc::new(FixedTurnId),
        )
    };
    let app = make_app(records.clone());
    let clone = app.clone();
    assert_eq!(
        drain(submit_when_gate_frees(&app, plain_turn_request(Some("request"))).await).await,
        "turn-1"
    );
    let replay = submit_when_gate_frees(&clone, plain_turn_request(Some("request"))).await;
    assert_eq!(replay.receipt.turn_id, "turn-1");
    assert!(replay.events.is_none());
    assert_eq!(runtime.submissions.load(Ordering::SeqCst), 1);
    drop(clone);
    drop(app);
    drop(records);
    let reopened = Arc::new(SqliteSessionRepository::open(&db).unwrap());
    assert!(reopened.get("runtime-1").await.unwrap().is_some());
    let restarted = make_app(reopened);
    assert_eq!(
        drain(submit_when_gate_frees(&restarted, plain_turn_request(Some("request"))).await).await,
        "turn-2"
    );
    assert_eq!(
        runtime.submissions.load(Ordering::SeqCst),
        2,
        "durable Session rows do not make the receipt cache durable exactly-once"
    );
}

#[tokio::test]
async fn session_contract_receipt_fifo_window_evicts_the_oldest_of_65_keys() {
    let runtime = Arc::new(CountingRuntime::default());
    let app = SessionApplication::new(
        runtime.clone(),
        test_provider_managers(),
        Arc::new(EmptyRepository),
        Arc::new(SequencedIds(AtomicU32::new(1))),
        Arc::new(FixedTurnId),
        Arc::new(UnusedEnvironment),
        Arc::new(FixedTurnId),
    );
    // Current bounded window is 64; no time-based or durable retry promise.
    for n in 1..=65 {
        let key = format!("request-{n}");
        assert_eq!(
            drain(submit_when_gate_frees(&app, plain_turn_request(Some(&key))).await).await,
            format!("turn-{n}")
        );
    }
    let replay = submit_when_gate_frees(&app, plain_turn_request(Some("request-65"))).await;
    assert_eq!(replay.receipt.turn_id, "turn-65");
    assert!(replay.events.is_none());
    assert_eq!(runtime.submissions.load(Ordering::SeqCst), 65);
    assert_eq!(
        drain(submit_when_gate_frees(&app, plain_turn_request(Some("request-1"))).await).await,
        "turn-66"
    );
    assert_eq!(runtime.submissions.load(Ordering::SeqCst), 66);
}

#[tokio::test]
async fn session_contract_local_open_does_not_admit_snapshot_capability() {
    let runtime = Arc::new(CountingRuntime::default());
    let records = Arc::new(SqliteSessionRepository::open_in_memory().unwrap());
    let app = SessionApplication::new(
        runtime.clone(),
        test_provider_managers(),
        records.clone(),
        Arc::new(FixedTurnId),
        Arc::new(FixedTurnId),
        Arc::new(TestEnvironment),
        Arc::new(FixedTurnId),
    );
    let mut request = open_request(Default::default());
    request
        .requested_capabilities
        .sandbox
        .insert(session_protocol::SessionSandboxCapability::Snapshot);
    assert!(matches!(app.open(&admin_ctx(), request).await,
        Err(SessionDomainError::UnsupportedCapability { capability, .. }) if capability == "snapshot"));
    assert!(records.get("runtime-fixed").await.unwrap().is_none());
    assert_eq!(runtime.submissions.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn session_contract_real_local_full_is_rejected_and_session_stays_usable() {
    let records = Arc::new(SqliteSessionRepository::open_in_memory().unwrap());
    let app = SessionApplication::new(
        Arc::new(CountingRuntime {
            state_export: true,
            ..Default::default()
        }),
        test_provider_managers(),
        records.clone(),
        Arc::new(FixedTurnId),
        Arc::new(FixedTurnId),
        Arc::new(TestEnvironment),
        Arc::new(FixedTurnId),
    );
    let opened = app
        .open(&admin_ctx(), open_request(Default::default()))
        .await
        .unwrap();
    assert!(!opened
        .effective_capabilities
        .sandbox
        .contains(&session_protocol::SessionSandboxCapability::Snapshot));
    assert!(!opened
        .effective_capabilities
        .runtime
        .contains(&session_protocol::SessionRuntimeCapability::Checkpoint));
    for scope in [Some(SessionCheckpointScope::Full), None] {
        assert!(
            matches!(app.checkpoint(&admin_ctx(), SessionCheckpointRequest {
            runtime_id: opened.runtime_id.clone(), name: None, requested_scope: scope, lease: Default::default(),
        }).await, Err(SessionDomainError::UnsupportedCapability { capability, .. }) if capability == "checkpoint")
        );
    }
    assert!(matches!(
        app.checkpoint(
            &admin_ctx(),
            SessionCheckpointRequest {
                runtime_id: opened.runtime_id.clone(),
                name: None,
                requested_scope: Some(SessionCheckpointScope::WorkspaceOnly),
                lease: Default::default(),
            }
        )
        .await,
        Err(SessionDomainError::InvalidRequest { .. })
    ));
    let mut request = plain_turn_request(None);
    request.runtime_id = opened.runtime_id.clone();
    drain(submit_when_gate_frees(&app, request).await).await;
    app.close(&admin_ctx(), &opened.runtime_id, Default::default())
        .await
        .unwrap();
    assert_eq!(
        records
            .get(&opened.runtime_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        SessionStatus::Closed
    );
}
