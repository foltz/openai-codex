//! An old handle must never reach a newly acquired writer with the same public ID.
use super::*;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn sealed_writer_clones_cannot_mutate_or_discard_a_same_id_successor() {
    for mode in [ThreadHistoryMode::Legacy, ThreadHistoryMode::Paginated] {
        let home = TempDir::new().unwrap();
        let store = Arc::new(LocalThreadStore::new(test_config(home.path()), None));
        let id = ThreadId::new();
        let item = |label: &str| {
            if mode == ThreadHistoryMode::Legacy {
                return user_message_item(label);
            }
            RolloutItem::EventMsg(EventMsg::ItemCompleted(ItemCompletedEvent {
                thread_id: id,
                turn_id: label.to_owned(),
                item: TurnItem::UserMessage(UserMessageItem {
                    id: label.to_owned(),
                    client_id: None,
                    content: Vec::new(),
                }),
                started_at_ms: Some(0),
                completed_at_ms: 1,
            }))
        };
        let mut params = create_thread_params(id);
        params.history_mode = mode;
        let old = LiveThread::create(store.clone(), params).await.unwrap();
        let stale = old.clone();
        old.append_items(&[item("before recovery")]).await.unwrap();
        old.persist(PersistContext::Standard).await.unwrap();
        let path = old.local_rollout_path().await.unwrap();
        old.shutdown().await.unwrap();
        let supplied_history = if mode == ThreadHistoryMode::Paginated {
            let (items, _, _) =
                RolloutRecorder::load_rollout_items(path.as_ref().unwrap().as_path())
                    .await
                    .unwrap();
            Some(Arc::new(items))
        } else {
            None
        };
        let successor = LiveThread::resume(
            store.clone(),
            mode,
            ResumeThreadParams {
                thread_id: id,
                rollout_path: path,
                history: supplied_history,
                include_archived: false,
                metadata: thread_metadata(),
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            stale.append_items(&[item("stale")]).await,
            Err(ThreadStoreError::ThreadNotFound { .. })
        ));
        assert!(stale.persist(PersistContext::Standard).await.is_err());
        assert!(stale.flush().await.is_err());
        assert!(
            stale
                .update_memory_mode(ThreadMemoryMode::Disabled, false)
                .await
                .is_err()
        );
        assert!(
            stale
                .update_metadata(ThreadMetadataPatch::default(), false)
                .await
                .is_err()
        );
        stale.shutdown().await.unwrap();
        assert!(stale.discard().await.is_err());
        successor
            .append_items(&[item("after recovery")])
            .await
            .unwrap();
        successor.flush().await.unwrap();
        let path = successor.local_rollout_path().await.unwrap().unwrap();
        let (history, _, _) = RolloutRecorder::load_rollout_items(path.as_path())
            .await
            .unwrap();
        let text = format!("{history:?}");
        assert!(text.contains("after recovery"), "{mode:?}: {text}");
        assert!(!text.contains("stale"));
        successor.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn sealed_writer_drains_an_admitted_backend_write_before_shutdown() {
    let home = TempDir::new().unwrap();
    let store = Arc::new(LocalThreadStore::new(test_config(home.path()), None));
    let id = ThreadId::new();
    let live = LiveThread::create(store.clone(), create_thread_params(id))
        .await
        .unwrap();
    let backend = store.live_writer_locks.lock(id).await;
    let items = [user_message_item("admitted before seal")];
    let mut write = Box::pin(live.append_items(&items));
    assert!(futures::poll!(&mut write).is_pending());
    let mut shutdown = Box::pin(live.shutdown());
    assert!(futures::poll!(&mut shutdown).is_pending());
    drop(backend);
    write.await.unwrap();
    shutdown.await.unwrap();
    let history = store
        .load_history(LoadThreadHistoryParams {
            thread_id: id,
            include_archived: false,
        })
        .await
        .unwrap();
    assert!(format!("{history:?}").contains("admitted before seal"));
}

#[tokio::test]
async fn sealed_writer_applies_to_non_local_storage_without_backend_calls() {
    let store = Arc::new(crate::InMemoryThreadStore::default());
    let id = ThreadId::new();
    let old = LiveThread::create(store.clone(), create_thread_params(id))
        .await
        .unwrap();
    old.shutdown().await.unwrap();
    let successor = LiveThread::resume(
        store.clone(),
        ThreadHistoryMode::Legacy,
        ResumeThreadParams {
            thread_id: id,
            rollout_path: None,
            history: None,
            include_archived: false,
            metadata: thread_metadata(),
        },
    )
    .await
    .unwrap();
    let before = store.calls().await;
    assert!(old.append_items(&[]).await.is_err());
    assert!(old.persist(PersistContext::Standard).await.is_err());
    assert!(old.flush().await.is_err());
    old.shutdown().await.unwrap();
    assert!(old.discard().await.is_err());
    let after = store.calls().await;
    assert_eq!(after.append_items, before.append_items);
    assert_eq!(after.shutdown_thread, before.shutdown_thread);
    assert_eq!(after.discard_thread, before.discard_thread);
    successor
        .append_items(&[user_message_item("new nonlocal writer")])
        .await
        .unwrap();
    assert!(store.calls().await.append_items > before.append_items);
    successor.shutdown().await.unwrap();
}
