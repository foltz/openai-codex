use super::*;

#[tokio::test]
async fn clear_lineage_is_persisted_without_losing_current_account_scope() {
    for mode in [ThreadHistoryMode::Legacy, ThreadHistoryMode::Paginated] {
        let home = TempDir::new().unwrap();
        let store = Arc::new(LocalThreadStore::new(test_config(home.path()), None));
        let predecessor = ThreadId::new();
        let successor = ThreadId::new();
        let transition = uuid::Uuid::new_v4().to_string();
        let mut params = create_thread_params(successor);
        params.history_mode = mode;
        params.clear_lineage = Some(crate::ClearThreadLineage {
            predecessor_thread_id: predecessor,
            transition_id: transition.clone(),
        });
        let live = LiveThread::create(store, params).await.unwrap();
        live.persist(PersistContext::Standard).await.unwrap();
        let path = live.local_rollout_path().await.unwrap().unwrap();
        live.shutdown().await.unwrap();
        let (items, _, _) = RolloutRecorder::load_rollout_items(path.as_path())
            .await
            .unwrap();
        let meta = items
            .iter()
            .find_map(|item| match item {
                RolloutItem::SessionMeta(meta) => Some(&meta.meta),
                _ => None,
            })
            .expect("persisted session metadata");
        assert_eq!(meta.id, successor);
        assert_eq!(meta.clear_predecessor_thread_id, Some(predecessor));
        assert_eq!(
            meta.clear_transition_id.as_deref(),
            Some(transition.as_str())
        );
        assert!(
            meta.account_scoped,
            "current account-scoping policy must survive the port"
        );
    }
}

#[test]
fn old_session_metadata_does_not_invent_clear_lineage() {
    let meta = codex_protocol::protocol::SessionMeta::default();
    let value = serde_json::to_value(&meta).unwrap();
    assert!(value.get("clear_predecessor_thread_id").is_none());
    assert!(value.get("clear_transition_id").is_none());
    let decoded: codex_protocol::protocol::SessionMeta = serde_json::from_value(value).unwrap();
    assert_eq!(decoded.clear_predecessor_thread_id, None);
    assert_eq!(decoded.clear_transition_id, None);
}
