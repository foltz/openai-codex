use super::*;
use codex_plugin::PluginId;
use pretty_assertions::assert_eq;

#[test]
fn only_workspace_listed_materializations_are_eligible() {
    let materialization =
        |name: &str,
         scope: RemotePluginScope,
         discoverability: Option<RemotePluginShareDiscoverability>| {
            RemotePluginMaterialization {
                plugin_id: PluginId::new(name.to_string(), "test".to_string())
                    .expect("valid plugin id"),
                scope,
                discoverability,
                authenticated_account_id: Some("account-123".to_string()),
            }
        };

    let mut materializations = vec![
        materialization(
            "eligible",
            RemotePluginScope::Workspace,
            Some(RemotePluginShareDiscoverability::Listed),
        ),
        materialization(
            "unlisted",
            RemotePluginScope::Workspace,
            Some(RemotePluginShareDiscoverability::Unlisted),
        ),
        materialization(
            "private",
            RemotePluginScope::Workspace,
            Some(RemotePluginShareDiscoverability::Private),
        ),
        materialization("workspace-missing", RemotePluginScope::Workspace, None),
        materialization("global", RemotePluginScope::Global, None),
        materialization("user", RemotePluginScope::User, None),
    ];
    let mut wrong_account = materialization(
        "wrong-account",
        RemotePluginScope::Workspace,
        Some(RemotePluginShareDiscoverability::Listed),
    );
    wrong_account.authenticated_account_id = Some("account-456".to_string());
    materializations.push(wrong_account);

    let plugin_ids = workspace_listed_plugin_ids(materializations, "account-123");

    assert_eq!(plugin_ids, BTreeSet::from(["eligible@test".to_string()]));
}

#[test]
fn hook_trusted_hash_edit_targets_only_escaped_leaf() {
    assert_eq!(
        hook_trusted_hash_edit(r#"plugin."quoted"\path"#, "sha256:current"),
        ConfigEdit {
            key_path: r#"hooks.state."plugin.\"quoted\"\\path".trusted_hash"#.to_string(),
            value: serde_json::json!("sha256:current"),
            merge_strategy: MergeStrategy::Replace,
        }
    );
}

fn callback_access(
    tasks: &crate::processor_task_retirement::ProcessorTasks,
    permits: &crate::account_work_permits::AccountWorkPermits,
) -> codex_model_provider::AuthTaskAccess {
    use codex_extension_api::TurnStartAdmission;
    let operation = crate::account_turn_admission::AccountTurnAdmission {
        shutdown: Default::default(),
        permits: permits.clone(),
        work: Default::default(),
        auth_tasks: Some(tasks.ticket()),
    }
    .admit_operation_work()
    .unwrap()
    .unwrap();
    let spawner = operation.auth_task_spawner().unwrap();
    drop(operation);
    codex_model_provider::AuthTaskAccess::Admitted(spawner)
}

#[tokio::test]
async fn callback_original_outlives_dropped_observer_and_closed_fresh_admission() {
    use crate::account_work_permits::AccountWorkPermits;
    use crate::processor_task_retirement::ProcessorTasks;
    use tokio::time::Instant;
    let tasks = ProcessorTasks::default();
    let permits = AccountWorkPermits::new();
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let observation =
        codex_model_provider::with_auth_task_scope(callback_access(&tasks, &permits), async move {
            spawn_plugin_change_task(async move {
                entered.send(()).unwrap();
                released.await.unwrap();
            })
        })
        .await
        .unwrap()
        .unwrap();
    assert!(observation.is_some());
    drop(observation);
    tokio::time::timeout(crate::managed_transition::DRAIN_DEADLINE, started)
        .await
        .unwrap()
        .unwrap();
    permits.close();
    assert!(permits.try_acquire().is_none());
    assert!(permits.admitted_count() > 0);
    assert!(!tasks.observe_until(Instant::now()).await.terminal);
    release.send(()).unwrap();
    assert!(
        tasks
            .shutdown_until(Instant::now() + crate::managed_transition::DRAIN_DEADLINE)
            .await
            .is_clean()
    );
    assert_eq!(permits.admitted_count(), 0);
}

#[tokio::test]
async fn reusable_callback_takes_each_invoking_root_not_its_factory_root() {
    use crate::account_work_permits::AccountWorkPermits;
    use crate::processor_task_retirement::ProcessorTasks;
    use tokio::time::Instant;
    // Constructed without a host scope; no authority can reside in this factory.
    let callback = |released: tokio::sync::oneshot::Receiver<()>| {
        spawn_plugin_change_task(async move {
            released.await.unwrap();
        })
        .unwrap()
    };
    let a = ProcessorTasks::default();
    let b = ProcessorTasks::default();
    let pa = AccountWorkPermits::new();
    let pb = AccountWorkPermits::new();
    let (ra, wa) = tokio::sync::oneshot::channel();
    let (rb, wb) = tokio::sync::oneshot::channel();
    drop(
        codex_model_provider::with_auth_task_scope(callback_access(&a, &pa), async {
            callback(wa)
        })
        .await
        .unwrap(),
    );
    drop(
        codex_model_provider::with_auth_task_scope(callback_access(&b, &pb), async {
            callback(wb)
        })
        .await
        .unwrap(),
    );
    pa.close();
    pb.close();
    assert!(pa.admitted_count() > 0 && pb.admitted_count() > 0);
    ra.send(()).unwrap();
    assert!(
        a.shutdown_until(Instant::now() + crate::managed_transition::DRAIN_DEADLINE)
            .await
            .is_clean()
    );
    assert_eq!(pa.admitted_count(), 0);
    assert!(pb.admitted_count() > 0);
    assert!(!b.observe_until(Instant::now()).await.terminal);
    rb.send(()).unwrap();
    assert!(
        b.shutdown_until(Instant::now() + crate::managed_transition::DRAIN_DEADLINE)
            .await
            .is_clean()
    );
    assert_eq!(pb.admitted_count(), 0);
}

#[tokio::test]
async fn callback_closed_ticket_refuses_body_and_records_sticky_failure() {
    use crate::account_work_permits::AccountWorkPermits;
    use crate::processor_task_retirement::ProcessorTasks;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    let tasks = ProcessorTasks::default();
    let permits = AccountWorkPermits::new();
    let access = callback_access(&tasks, &permits);
    tasks.close_registration().unwrap();
    permits.close();
    let polled = Arc::new(AtomicBool::new(false));
    let child = polled.clone();
    let result = codex_model_provider::with_auth_task_scope(access, async move {
        assert!(matches!(
            spawn_plugin_change_task(async move {
                child.store(true, Ordering::SeqCst);
            }),
            Err(codex_model_provider::AuthTaskFailure::Unavailable)
        ));
    })
    .await;
    assert!(
        result.is_err(),
        "caller cannot hide a rejected callback original"
    );
    assert!(!polled.load(Ordering::SeqCst));
    assert_eq!(permits.admitted_count(), 0);
    assert_eq!(tasks.pending_len_for_test(), 0);
}

#[tokio::test]
async fn callback_descendant_remains_counted_after_callback_body_returns() {
    use crate::account_work_permits::AccountWorkPermits;
    use crate::processor_task_retirement::ProcessorTasks;
    use tokio::time::Instant;
    let tasks = ProcessorTasks::default();
    let permits = AccountWorkPermits::new();
    let (release, released) = tokio::sync::oneshot::channel();
    let (entered, started) = tokio::sync::oneshot::channel();
    drop(
        codex_model_provider::with_auth_task_scope(callback_access(&tasks, &permits), async move {
            spawn_plugin_change_task(async move {
                drop(
                    codex_model_provider::spawn_provider_task(Box::pin(async move {
                        entered.send(()).unwrap();
                        released.await.unwrap();
                    }))
                    .unwrap(),
                );
            })
        })
        .await
        .unwrap()
        .unwrap(),
    );
    tokio::time::timeout(crate::managed_transition::DRAIN_DEADLINE, started)
        .await
        .unwrap()
        .unwrap();
    permits.close();
    assert!(permits.admitted_count() > 0);
    assert!(!tasks.observe_until(Instant::now()).await.terminal);
    release.send(()).unwrap();
    assert!(
        tasks
            .shutdown_until(Instant::now() + crate::managed_transition::DRAIN_DEADLINE)
            .await
            .is_clean()
    );
    assert_eq!(permits.admitted_count(), 0);
}

#[tokio::test]
async fn callback_panic_is_nonclean_after_original_compaction() {
    use crate::account_work_permits::AccountWorkPermits;
    use crate::processor_task_retirement::ProcessorTasks;
    use tokio::time::Instant;
    let tasks = ProcessorTasks::default();
    let permits = AccountWorkPermits::new();
    drop(
        codex_model_provider::with_auth_task_scope(callback_access(&tasks, &permits), async {
            spawn_plugin_change_task(async {
                panic!("synthetic callback original panic");
            })
        })
        .await
        .unwrap()
        .unwrap(),
    );
    let result = tasks
        .shutdown_until(Instant::now() + crate::managed_transition::DRAIN_DEADLINE)
        .await;
    assert!(result.terminal && result.panicked && !result.is_clean());
    assert!(tasks.observe_until(Instant::now()).await.panicked);
    assert_eq!(permits.admitted_count(), 0);
}

#[tokio::test]
async fn independent_callback_keeps_ordinary_tokio_dispatch_without_host() {
    let (sent, received) = tokio::sync::oneshot::channel();
    let observation = spawn_plugin_change_task(async move {
        sent.send(17).unwrap();
    })
    .unwrap();
    assert!(observation.is_none());
    assert_eq!(
        tokio::time::timeout(crate::managed_transition::DRAIN_DEADLINE, received)
            .await
            .unwrap()
            .unwrap(),
        17
    );
}

#[tokio::test]
async fn retained_queued_callback_keeps_account_work_until_body_completion() {
    use crate::account_work_permits::AccountWorkPermits;
    use crate::processor_task_retirement::ProcessorTasks;
    use codex_extension_api::TurnStartAdmission;
    use tokio::time::Instant;
    let auth_tasks = ProcessorTasks::default();
    let auxiliary_tasks = ProcessorTasks::default();
    let permits = AccountWorkPermits::new();
    let work = crate::account_turn_admission::AccountTurnAdmission {
        shutdown: Default::default(),
        permits: permits.clone(),
        work: Default::default(),
        auth_tasks: Some(auth_tasks.ticket()),
    }
    .admit_operation_work()
    .unwrap();
    let queues = RequestSerializationQueues::default();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let ticket = auxiliary_tasks.ticket();
    let originator = codex_otel::auth_storage::AuthStorageOriginator::current();
    let observation = auxiliary_tasks
        .spawn(async move {
            queues
                .enqueue_background_and_wait(
                    RequestSerializationQueueKey::Global("owned-plugin-callback"),
                    RequestSerializationAccess::Exclusive,
                    run_retained_plugin_change(ticket, work, originator, async move {
                        entered_tx.send(()).unwrap();
                        release_rx.await.unwrap();
                    }),
                )
                .await
                .unwrap();
        })
        .unwrap();
    drop(observation);
    tokio::time::timeout(crate::managed_transition::DRAIN_DEADLINE, entered_rx)
        .await
        .unwrap()
        .unwrap();
    permits.close();
    assert!(permits.try_acquire().is_none());
    assert!(permits.admitted_count() > 0);
    assert!(!auxiliary_tasks.observe_until(Instant::now()).await.terminal);
    release_tx.send(()).unwrap();
    let deadline = Instant::now() + crate::managed_transition::DRAIN_DEADLINE;
    assert!(auxiliary_tasks.shutdown_until(deadline).await.is_clean());
    assert!(auth_tasks.shutdown_until(deadline).await.is_clean());
    assert_eq!(permits.admitted_count(), 0);
}

#[tokio::test]
async fn retained_callback_refuses_missing_auth_custody_before_polling_effects() {
    use codex_extension_api::TurnStartAdmission;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    let tasks = crate::processor_task_retirement::ProcessorTasks::default();
    let work = crate::account_turn_admission::AccountTurnAdmission {
        shutdown: Default::default(),
        permits: crate::account_work_permits::AccountWorkPermits::new(),
        work: Default::default(),
        auth_tasks: None,
    }
    .admit_operation_work()
    .unwrap();
    let polled = Arc::new(AtomicBool::new(false));
    let observed = Arc::clone(&polled);
    run_retained_plugin_change(
        tasks.ticket(),
        work,
        codex_otel::auth_storage::AuthStorageOriginator::current(),
        async move {
            observed.store(true, Ordering::SeqCst);
        },
    )
    .await;
    assert!(!polled.load(Ordering::SeqCst));
    assert!(
        !tasks
            .observe_until(tokio::time::Instant::now())
            .await
            .is_clean()
    );
}
