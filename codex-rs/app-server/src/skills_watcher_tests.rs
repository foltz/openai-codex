use super::*;
use pretty_assertions::assert_eq;

fn with_task(task: JoinHandle<()>) -> SkillsWatcher {
    let (subscriber, _rx) = Arc::new(FileWatcher::noop()).add_subscriber();
    let shutdown_token = CancellationToken::new();
    SkillsWatcher {
        subscriber,
        runtime_extra_roots_registration: Mutex::new(WatchRegistration::default()),
        _shutdown_drop_guard: shutdown_token.clone().drop_guard(),
        shutdown_token,
        completion: observe_event_loop(Some(task)),
        shutdown_deadline: Mutex::new(None),
    }
}

#[tokio::test(start_paused = true)]
async fn observer_timeout_keeps_join_and_first_deadline() {
    let resource = Arc::new(());
    let weak = Arc::downgrade(&resource);
    let (release, released) = tokio::sync::oneshot::channel();
    let watcher = with_task(tokio::spawn(async move {
        let _resource = resource;
        let _ = released.await;
    }));
    let deadline = Instant::now() + Duration::from_secs(1);
    let mut observer = Box::pin(watcher.shutdown_until(deadline));
    assert!(futures::poll!(observer.as_mut()).is_pending());
    drop(observer);
    assert_eq!(
        watcher
            .shutdown_until(deadline + Duration::from_secs(10))
            .await,
        SkillsWatcherShutdown::TimedOut
    );
    assert_eq!(Instant::now(), deadline);
    assert!(weak.upgrade().is_some());
    release.send(()).unwrap();
    assert_eq!(
        watcher.completion.clone().await,
        SkillsWatcherShutdown::Joined
    );
    let (first, second) = tokio::join!(
        watcher.shutdown_until(deadline),
        watcher.shutdown_until(deadline)
    );
    assert_eq!(
        (first, second),
        (SkillsWatcherShutdown::Joined, SkillsWatcherShutdown::Joined)
    );
    assert!(weak.upgrade().is_none());
}

#[tokio::test]
async fn actual_cancelled_and_panicked_joins_are_distinct_and_sticky() {
    let task = tokio::spawn(std::future::pending());
    task.abort();
    let cancelled = with_task(task);
    let panicked = with_task(tokio::spawn(async { panic!("controlled watcher panic") }));
    for (watcher, expected) in [
        (cancelled, SkillsWatcherShutdown::Cancelled),
        (panicked, SkillsWatcherShutdown::Panicked),
    ] {
        assert_eq!(
            watcher
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await,
            expected
        );
        assert_eq!(watcher.shutdown_until(Instant::now()).await, expected);
    }
}

#[tokio::test]
async fn production_watcher_shutdown_observes_real_event_loop() {
    let home = tempfile::TempDir::new().unwrap();
    let home_path = AbsolutePathBuf::try_from(home.path()).unwrap();
    let service = Arc::new(HostSkillsService::new(
        home_path.clone(),
        /*bundled_skills_enabled*/ false,
    ));
    let weak = Arc::downgrade(&service);
    let (sender, _receiver) = tokio::sync::mpsc::channel(1);
    let outgoing = Arc::new(OutgoingMessageSender::new(
        sender,
        codex_analytics::AnalyticsEventsClient::disabled(),
    ));
    let watcher = SkillsWatcher::new(service, &home_path, outgoing);
    assert_eq!(
        watcher
            .shutdown_until(Instant::now() + Duration::from_secs(2))
            .await,
        SkillsWatcherShutdown::Joined
    );
    assert!(weak.upgrade().is_none());
    assert_eq!(
        watcher.shutdown_until(Instant::now()).await,
        SkillsWatcherShutdown::Joined
    );
}
