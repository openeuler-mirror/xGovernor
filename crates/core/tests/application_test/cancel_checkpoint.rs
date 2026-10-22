//! Session cancel targeting, terminal settlement and checkpoint row retention.
//! Runtime events and saved checkpoint contents are fixtures, not full snapshots.
use super::*;

fn holding_application(
    runtime: Arc<HoldingRuntime>,
    records: Arc<SqliteSessionRepository>,
    providers: HashMap<String, Arc<InstanceManager>>,
) -> SessionApplication {
    SessionApplication::new(
        runtime,
        providers,
        records,
        Arc::new(SequencedIds(std::sync::atomic::AtomicU32::new(1))),
        Arc::new(FixedTurnId),
        Arc::new(UnusedEnvironment),
        Arc::new(FixedTurnId),
    )
}

async fn finish_cancelled_turn(runtime: &HoldingRuntime, mut submission: SessionSubmission) {
    let usage = SessionUsage {
        input_tokens: 3,
        output_tokens: 2,
        total_tokens: 5,
    };
    let sender = runtime.turn_sender.lock().await.take().unwrap();
    sender
        .send(RuntimeEvent::Completed {
            outcome: SessionTurnOutcome::Cancelled,
            usage: usage.clone(),
        })
        .await
        .unwrap();
    drop(sender);
    let receipt = submission.receipt;
    let mut events = submission.events.take().unwrap();
    let terminal_count = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        let mut count = 0;
        while let Some(event) = events.recv().await {
            match event {
                SessionEvent::TurnCompleted {
                    runtime_id,
                    turn_id,
                    outcome,
                    usage: actual,
                } => {
                    assert_eq!(runtime_id, receipt.runtime_id);
                    assert_eq!(turn_id, receipt.turn_id);
                    assert_eq!(outcome, SessionTurnOutcome::Cancelled);
                    assert_eq!(actual, session_protocol::SessionUsageReport::Known(usage));
                    count += 1;
                }
                SessionEvent::TurnFailed { error, .. } => panic!("unexpected failure: {error:?}"),
                _ => {}
            }
        }
        count
    })
    .await
    .expect("cancelled turn stream must settle");
    assert_eq!(terminal_count, 1);
}

#[tokio::test]
async fn exact_cancel_rejects_stale_receipt_without_cancelling_the_new_turn() {
    let runtime = Arc::new(HoldingRuntime::default());
    let records = Arc::new(SqliteSessionRepository::open_in_memory().unwrap());
    records.save(test_record()).await.unwrap();
    let app = holding_application(runtime.clone(), records, test_provider_managers());
    let first = app
        .submit_turn(&admin_ctx(), plain_turn_request(None))
        .await
        .unwrap();
    let first_id = first.receipt.turn_id.clone();

    assert!(matches!(
        app.cancel(&admin_ctx(), "runtime-1", Some("wrong-turn"))
            .await,
        Err(SessionDomainError::NotFound { .. })
    ));
    assert!(runtime.cancellations.lock().await.is_empty());
    app.cancel(&admin_ctx(), "runtime-1", Some(&first_id))
        .await
        .unwrap();
    assert_eq!(
        *runtime.cancellations.lock().await,
        vec![RuntimeCancelRequest {
            runtime_id: "runtime-1".into(),
            turn_id: Some(first_id.clone()),
        }]
    );
    assert!(
        matches!(
            app.submit_turn(&admin_ctx(), plain_turn_request(None))
                .await,
            Err(SessionDomainError::Conflict { .. })
        ),
        "cancel acknowledgement must not release the active turn before its terminal"
    );
    finish_cancelled_turn(&runtime, first).await;

    let second = submit_when_gate_frees(&app, plain_turn_request(None)).await;
    let second_id = second.receipt.turn_id.clone();
    assert_ne!(first_id, second_id);
    assert!(matches!(
        app.cancel(&admin_ctx(), "runtime-1", Some(&first_id)).await,
        Err(SessionDomainError::NotFound { .. })
    ));
    assert_eq!(
        runtime.cancellations.lock().await.len(),
        1,
        "a delayed cancel for the old receipt must not reach the runtime"
    );
    app.cancel(&admin_ctx(), "runtime-1", Some(&second_id))
        .await
        .unwrap();
    assert_eq!(
        runtime.cancellations.lock().await[1],
        RuntimeCancelRequest {
            runtime_id: "runtime-1".into(),
            turn_id: Some(second_id.clone()),
        }
    );
    finish_cancelled_turn(&runtime, second).await;
    assert!(matches!(
        app.cancel(&admin_ctx(), "runtime-1", Some(&second_id))
            .await,
        Err(SessionDomainError::NotFound { .. })
    ));
    assert_eq!(runtime.cancellations.lock().await.len(), 2);
}

#[tokio::test]
async fn exact_cancel_rejects_foreign_tenant_without_touching_the_owner_turn() {
    let runtime = Arc::new(HoldingRuntime::default());
    let records = Arc::new(SqliteSessionRepository::open_in_memory().unwrap());
    records.save(tenant_test_record()).await.unwrap();
    let app = holding_application(runtime.clone(), records, test_provider_managers());
    let submission = app
        .submit_turn(&tenant_ctx(), plain_turn_request(None))
        .await
        .unwrap();
    let turn_id = submission.receipt.turn_id.clone();
    let foreign = SecurityContext::tenant("tenant-b", "other");
    assert!(matches!(
        app.cancel(&foreign, "runtime-1", Some(&turn_id)).await,
        Err(SessionDomainError::NotFound { .. })
    ));
    assert!(runtime.cancellations.lock().await.is_empty());
    app.cancel(&tenant_ctx(), "runtime-1", Some(&turn_id))
        .await
        .unwrap();
    assert_eq!(
        *runtime.cancellations.lock().await,
        vec![RuntimeCancelRequest {
            runtime_id: "runtime-1".into(),
            turn_id: Some(turn_id),
        }]
    );
    finish_cancelled_turn(&runtime, submission).await;
}

#[tokio::test]
async fn cancel_and_close_preserve_checkpoint_rows_across_repository_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("sessions.db");
    let records = Arc::new(SqliteSessionRepository::open(&database).unwrap());
    records.save(tenant_test_record()).await.unwrap();
    let checkpoint = checkpoint_manager_record();
    records.save_checkpoint(checkpoint.clone()).await.unwrap();
    let providers = test_provider_managers();
    start_test_provider(&providers, "runtime-1").await;
    let runtime = Arc::new(HoldingRuntime::default());
    let app = holding_application(runtime.clone(), records.clone(), providers);
    let submission = app
        .submit_turn(&tenant_ctx(), plain_turn_request(None))
        .await
        .unwrap();
    app.cancel(
        &tenant_ctx(),
        "runtime-1",
        Some(&submission.receipt.turn_id),
    )
    .await
    .unwrap();
    finish_cancelled_turn(&runtime, submission).await;
    assert_eq!(
        records
            .get_checkpoint(&checkpoint.checkpoint_id)
            .await
            .unwrap(),
        Some(checkpoint.clone())
    );
    app.close(&tenant_ctx(), "runtime-1", Default::default())
        .await
        .unwrap();
    assert_eq!(
        records.get("runtime-1").await.unwrap().unwrap().status,
        SessionStatus::Closed
    );
    assert_eq!(
        records
            .get_checkpoint(&checkpoint.checkpoint_id)
            .await
            .unwrap(),
        Some(checkpoint.clone())
    );
    drop(app);
    drop(records);
    let reopened = SqliteSessionRepository::open(&database).unwrap();
    assert_eq!(
        reopened
            .get_checkpoint(&checkpoint.checkpoint_id)
            .await
            .unwrap(),
        Some(checkpoint)
    );
    assert_eq!(
        reopened.get("runtime-1").await.unwrap().unwrap().status,
        SessionStatus::Closed
    );
}
