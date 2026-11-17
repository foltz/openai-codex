use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

#[tokio::test]
async fn real_processor_intake_closes_admission_and_latches_existing_cleanup_owners() {
    let home = TempDir::new().unwrap();
    let config = Arc::new(
        codex_core::config::ConfigBuilder::default()
            .codex_home(home.path().to_path_buf())
            .build()
            .await
            .unwrap(),
    );
    let auth = codex_login::AuthManager::shared_from_config(config.as_ref(), false)
        .await
        .expect("initialize fixture auth manager");
    let (processor, _outgoing) =
        super::message_processor_tracing_tests::build_test_processor(config, auth).await;
    let permit = processor
        .turn_admission
        .admit()
        .expect("initial request admitted");
    let first = tokio::time::Instant::now() + Duration::from_secs(35);
    processor.begin_shutdown_at_intake(first);
    assert!(processor.turn_admission.admit().is_err());
    assert_eq!(processor.intake_shutdown_deadline(), Some(first));
    assert_eq!(
        processor.command_exec_processor.sandbox_cleanup_deadline(),
        Some(first.into_std())
    );
    assert!(processor.intake_thread_binding().unwrap().is_complete());
    processor.begin_shutdown_at_intake(first + Duration::from_secs(30));
    assert_eq!(processor.intake_shutdown_deadline(), Some(first));
    assert_eq!(
        processor.command_exec_processor.sandbox_cleanup_deadline(),
        Some(first.into_std())
    );
    drop(permit);
    assert_eq!(
        processor.models_refresh_worker.shutdown_until(first).await,
        crate::models_refresh_worker::ModelsRefreshShutdown::Joined
    );
    processor.clear_runtime_references();
}

#[tokio::test]
async fn registered_intake_callback_keeps_only_weak_processor_custody() {
    let home = TempDir::new().unwrap();
    let config = Arc::new(
        codex_core::config::ConfigBuilder::default()
            .codex_home(home.path().to_path_buf())
            .build()
            .await
            .unwrap(),
    );
    let auth = codex_login::AuthManager::shared_from_config(config.as_ref(), false)
        .await
        .expect("initialize fixture auth manager");
    let (processor, _outgoing) =
        super::message_processor_tracing_tests::build_test_processor(config, auth).await;
    let (latch, _custody) = crate::shutdown_intake::OwnerLatch::new();
    let latch = Arc::new(latch);
    let strong = Arc::strong_count(&processor);
    crate::standalone_shutdown::register_processor_intake(&latch, &processor);
    assert_eq!(Arc::strong_count(&processor), strong);
    crate::shutdown_intake::install_intake_latch(&latch);
    latch.expiry().mark_shutdown_edge();
    assert_eq!(
        processor.intake_shutdown_deadline(),
        latch.expiry().deadline()
    );
    assert!(processor.turn_admission.admit().is_err());
    assert_eq!(
        processor
            .models_refresh_worker
            .shutdown_until(latch.expiry().deadline().unwrap())
            .await,
        crate::models_refresh_worker::ModelsRefreshShutdown::Joined
    );
    processor.clear_runtime_references();
}

#[tokio::test]
async fn published_intake_deadline_without_binding_receipt_is_not_clean() {
    let home = TempDir::new().unwrap();
    let config = Arc::new(
        codex_core::config::ConfigBuilder::default()
            .codex_home(home.path().to_path_buf())
            .build()
            .await
            .unwrap(),
    );
    let auth = codex_login::AuthManager::shared_from_config(config.as_ref(), false)
        .await
        .expect("initialize fixture auth manager");
    let (processor, _outgoing) =
        super::message_processor_tracing_tests::build_test_processor(config, auth).await;
    let first = tokio::time::Instant::now() + Duration::from_secs(35);
    processor.intake_shutdown_deadline.set(first).unwrap();
    let pending = processor
        .intake_thread_binding()
        .expect("published intake must have a census state");
    assert_eq!(pending.unavailable, 1);
    assert!(!pending.is_complete());
    assert_eq!(
        processor.models_refresh_worker.shutdown_until(first).await,
        crate::models_refresh_worker::ModelsRefreshShutdown::Joined
    );
    processor.clear_runtime_references();
}
