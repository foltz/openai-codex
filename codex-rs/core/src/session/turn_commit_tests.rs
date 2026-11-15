#![allow(clippy::expect_used)]

use super::*;
use codex_extension_api::TurnCommittedInput;
use codex_extension_api::TurnLifecycleContributor;
use pretty_assertions::assert_eq;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

struct CommitRecorder {
    committed: AtomicUsize,
    session: std::sync::Mutex<std::sync::Weak<Session>>,
}

impl TurnLifecycleContributor for CommitRecorder {
    fn on_turn_start<'a>(
        &'a self,
        input: codex_extension_api::TurnStartInput<'a>,
    ) -> codex_extension_api::ExtensionFuture<'a, ()> {
        Box::pin(async move {
            input.turn_store.insert(input.turn_id.to_owned());
        })
    }

    fn on_turn_committed(&self, input: TurnCommittedInput<'_>) {
        assert_eq!(input.turn_id, input.turn_store.level_id());
        assert_eq!(
            input.turn_id,
            input
                .turn_store
                .get::<String>()
                .expect("prepared identity")
                .as_str()
        );
        let session = self
            .session
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .upgrade()
            .expect("live session");
        let active = session
            .active_turn
            .try_lock()
            .expect("callback must run outside active_turn lock");
        let task = active
            .as_ref()
            .and_then(|turn| turn.task.as_ref())
            .expect("exact task installed");
        assert_eq!(input.turn_id, task.turn_context.sub_id);
        assert!(
            !task.commit_ready.is_cancelled(),
            "task is not released before publication"
        );
        self.committed.fetch_add(1, Ordering::SeqCst);
    }
}

struct CheckCommitTask {
    kind: TaskKind,
    recorder: Arc<CommitRecorder>,
    ran: Arc<tokio::sync::Semaphore>,
}

impl SessionTask for CheckCommitTask {
    fn kind(&self) -> TaskKind {
        self.kind
    }
    fn span_name(&self) -> &'static str {
        "session_task.check_commit"
    }
    async fn run(
        self: Arc<Self>,
        _session: Arc<Session>,
        context: Arc<TurnContext>,
        _input: Vec<TurnInput>,
        cancellation: CancellationToken,
    ) -> SessionTaskResult {
        assert_eq!(1, self.recorder.committed.load(Ordering::SeqCst));
        assert_eq!(
            context.sub_id,
            *context.extension_data.get::<String>().expect("same store")
        );
        self.ran.add_permits(1);
        cancellation.cancelled().await;
        Ok(None)
    }
}

#[tokio::test]
async fn turn_commit_precedes_task_run_for_regular_compact_and_review() {
    for kind in [TaskKind::Regular, TaskKind::Compact, TaskKind::Review] {
        let (mut session, context) = make_session_and_context().await;
        let recorder = Arc::new(CommitRecorder {
            committed: AtomicUsize::new(0),
            session: Default::default(),
        });
        let ran = Arc::new(tokio::sync::Semaphore::new(0));
        let mut builder =
            codex_extension_api::ExtensionRegistryBuilder::<crate::config::Config>::new();
        builder.turn_lifecycle_contributor(recorder.clone());
        session.services.extensions = Arc::new(builder.build());
        let session = Arc::new(session);
        *recorder
            .session
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::downgrade(&session);
        session
            .spawn_task(
                Arc::new(context),
                Vec::new(),
                CheckCommitTask {
                    kind,
                    recorder: Arc::clone(&recorder),
                    ran: Arc::clone(&ran),
                },
            )
            .await;
        tokio::time::timeout(Duration::from_secs(5), ran.acquire())
            .await
            .expect("task ran")
            .expect("semaphore open")
            .forget();
        session.abort_all_tasks(TurnAbortReason::Interrupted).await;
        assert_eq!(1, recorder.committed.load(Ordering::SeqCst));
    }
}

#[tokio::test]
async fn rejected_preparation_does_not_commit_even_when_context_is_retained() {
    struct Paused {
        entered: tokio::sync::Semaphore,
        release: tokio::sync::Semaphore,
    }
    impl TurnLifecycleContributor for Paused {
        fn on_turn_start<'a>(
            &'a self,
            _input: codex_extension_api::TurnStartInput<'a>,
        ) -> codex_extension_api::ExtensionFuture<'a, ()> {
            Box::pin(async move {
                self.entered.add_permits(1);
                self.release.acquire().await.expect("release").forget();
            })
        }
    }
    let (mut session, context) = make_session_and_context().await;
    let recorder = Arc::new(CommitRecorder {
        committed: AtomicUsize::new(0),
        session: Default::default(),
    });
    let pause = Arc::new(Paused {
        entered: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
    });
    let mut builder = codex_extension_api::ExtensionRegistryBuilder::<crate::config::Config>::new();
    builder.turn_lifecycle_contributor(recorder.clone());
    builder.turn_lifecycle_contributor(pause.clone());
    session.services.extensions = Arc::new(builder.build());
    let session = Arc::new(session);
    *recorder
        .session
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::downgrade(&session);
    let retained = Arc::new(context);
    let starting = Arc::clone(&session);
    let context = Arc::clone(&retained);
    let start = tokio::spawn(async move {
        starting
            .spawn_task(
                context,
                Vec::new(),
                NeverEndingTask {
                    kind: TaskKind::Regular,
                    listen_to_cancellation_token: true,
                },
            )
            .await;
    });
    tokio::time::timeout(Duration::from_secs(5), pause.entered.acquire())
        .await
        .expect("prepared")
        .expect("semaphore open")
        .forget();
    session.close_task_admission().await;
    pause.release.add_permits(1);
    start.await.expect("refused start returned");
    assert!(retained.extension_data.get::<String>().is_some());
    assert_eq!(0, recorder.committed.load(Ordering::SeqCst));
    assert!(session.active_turn.lock().await.is_none());
}

#[tokio::test]
async fn contributor_panic_does_not_undo_committed_installation() {
    struct Panics;
    impl TurnLifecycleContributor for Panics {
        fn on_turn_committed(&self, _input: TurnCommittedInput<'_>) {
            panic!("intentional contributor failure");
        }
    }
    let (mut session, context) = make_session_and_context().await;
    let recorder = Arc::new(CommitRecorder {
        committed: AtomicUsize::new(0),
        session: Default::default(),
    });
    let mut builder = codex_extension_api::ExtensionRegistryBuilder::<crate::config::Config>::new();
    builder.turn_lifecycle_contributor(Arc::new(Panics));
    builder.turn_lifecycle_contributor(recorder.clone());
    session.services.extensions = Arc::new(builder.build());
    let session = Arc::new(session);
    *recorder
        .session
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::downgrade(&session);
    let ran = Arc::new(tokio::sync::Semaphore::new(0));
    session
        .spawn_task(
            Arc::new(context),
            Vec::new(),
            CheckCommitTask {
                kind: TaskKind::Regular,
                recorder,
                ran: Arc::clone(&ran),
            },
        )
        .await;
    tokio::time::timeout(Duration::from_secs(5), ran.acquire())
        .await
        .expect("task ran despite panic")
        .expect("semaphore open")
        .forget();
    assert!(
        session
            .active_turn
            .lock()
            .await
            .as_ref()
            .expect("accepted turn")
            .task
            .is_some()
    );
    session.abort_all_tasks(TurnAbortReason::Interrupted).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turn_commit_abort_waits_for_publication_without_holding_active_lock() {
    struct ContendedPublication {
        lock: Arc<std::sync::Mutex<()>>,
        entered: tokio::sync::Semaphore,
        calls: Arc<std::sync::Mutex<Vec<&'static str>>>,
        session: std::sync::Mutex<std::sync::Weak<Session>>,
        cancellation: std::sync::Mutex<Option<CancellationToken>>,
    }
    impl TurnLifecycleContributor for ContendedPublication {
        fn on_turn_committed(&self, _input: TurnCommittedInput<'_>) {
            let session = self
                .session
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .upgrade()
                .expect("live session");
            let cancellation = {
                let active = session.active_turn.try_lock().expect("core lock released");
                active
                    .as_ref()
                    .and_then(|turn| turn.task.as_ref())
                    .expect("installed task")
                    .cancellation_token
                    .clone()
            };
            *self
                .cancellation
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(cancellation);
            self.entered.add_permits(1);
            let _guard = self
                .lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push("commit");
        }
        fn on_turn_abort<'a>(
            &'a self,
            _input: codex_extension_api::TurnAbortInput<'a>,
        ) -> codex_extension_api::ExtensionFuture<'a, ()> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push("abort");
            })
        }
    }
    let (mut session, context) = make_session_and_context().await;
    let lock = Arc::new(std::sync::Mutex::new(()));
    let held_lock = Arc::clone(&lock);
    let (held_tx, held_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let holder = std::thread::spawn(move || {
        let _held = held_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        held_tx.send(()).expect("fixture acquired lock");
        release_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("bounded fixture release");
    });
    held_rx.await.expect("fixture holds publication mutex");
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let contributor = Arc::new(ContendedPublication {
        lock,
        entered: tokio::sync::Semaphore::new(0),
        calls: Arc::clone(&calls),
        session: Default::default(),
        cancellation: Default::default(),
    });
    let mut builder = codex_extension_api::ExtensionRegistryBuilder::<crate::config::Config>::new();
    builder.turn_lifecycle_contributor(contributor.clone());
    session.services.extensions = Arc::new(builder.build());
    let session = Arc::new(session);
    *contributor
        .session
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::downgrade(&session);
    let starting = Arc::clone(&session);
    let start = tokio::spawn(async move {
        starting
            .spawn_task(
                Arc::new(context),
                Vec::new(),
                NeverEndingTask {
                    kind: TaskKind::Regular,
                    listen_to_cancellation_token: true,
                },
            )
            .await;
    });
    tokio::time::timeout(Duration::from_secs(5), contributor.entered.acquire())
        .await
        .expect("publication entered")
        .expect("semaphore open")
        .forget();
    let aborting = Arc::clone(&session);
    let abort = tokio::spawn(async move {
        aborting.abort_all_tasks(TurnAbortReason::Interrupted).await;
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while session.active_turn.lock().await.is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("abort takes installed task without waiting under active_turn");
    assert!(
        calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
    );
    assert!(
        !abort.is_finished(),
        "terminal cleanup waits for publication"
    );
    assert!(
        contributor
            .cancellation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .expect("task cancellation captured")
            .is_cancelled(),
        "abort must prevent task execution before publication completes"
    );
    release_tx.send(()).expect("release publication mutex");
    holder.join().expect("fixture exited");
    start.await.expect("installation completed");
    abort.await.expect("abort completed");
    assert_eq!(
        vec!["commit", "abort"],
        *calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turn_commit_replacement_waits_after_another_abort_takes_the_task() {
    struct Publication {
        lock: Arc<std::sync::Mutex<()>>,
        first: String,
        entered: tokio::sync::Semaphore,
        winner: tokio::sync::Semaphore,
        calls: std::sync::Mutex<Vec<String>>,
    }
    impl TurnLifecycleContributor for Publication {
        fn on_turn_committed(&self, input: TurnCommittedInput<'_>) {
            if input.turn_id == self.first {
                self.entered.add_permits(1);
                let _held = self
                    .lock
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                self.calls
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(input.turn_id.to_owned());
            } else {
                self.calls
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(input.turn_id.to_owned());
                self.winner.add_permits(1);
            }
        }
    }
    let (mut session, context) = make_session_and_context().await;
    let first = context.sub_id.clone();
    let lock = Arc::new(std::sync::Mutex::new(()));
    let contributor = Arc::new(Publication {
        lock: Arc::clone(&lock),
        first: first.clone(),
        entered: tokio::sync::Semaphore::new(0),
        winner: tokio::sync::Semaphore::new(0),
        calls: Default::default(),
    });
    let mut builder = codex_extension_api::ExtensionRegistryBuilder::<crate::config::Config>::new();
    builder.turn_lifecycle_contributor(contributor.clone());
    session.services.extensions = Arc::new(builder.build());
    let session = Arc::new(session);
    let next = session.new_default_turn().await;
    let winner = next.sub_id.clone();
    let (held_tx, held_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let holder = std::thread::spawn(move || {
        let _held = lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        held_tx.send(()).expect("held");
        release_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("bounded release");
    });
    held_rx.await.expect("publication lock held");
    let starting = Arc::clone(&session);
    let first_start = tokio::spawn(async move {
        let started = starting
            .try_spawn_task(
                Arc::new(context),
                Vec::new(),
                NeverEndingTask {
                    kind: TaskKind::Regular,
                    listen_to_cancellation_token: true,
                },
            )
            .await;
        assert!(started, "first accepted");
    });
    tokio::time::timeout(Duration::from_secs(5), contributor.entered.acquire())
        .await
        .expect("first handoff entered")
        .expect("open")
        .forget();
    let aborting = Arc::clone(&session);
    let abort = tokio::spawn(async move {
        aborting.abort_all_tasks(TurnAbortReason::Interrupted).await;
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while session.active_turn.lock().await.is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("another abort took the first task");
    let starting = Arc::clone(&session);
    let (starting_tx, starting_rx) = tokio::sync::oneshot::channel();
    let next_start = tokio::spawn(async move {
        starting_tx
            .send(())
            .expect("observe replacement scheduling");
        let started = starting
            .try_spawn_task(
                next,
                Vec::new(),
                NeverEndingTask {
                    kind: TaskKind::Regular,
                    listen_to_cancellation_token: true,
                },
            )
            .await;
        assert!(started, "winner accepted");
    });
    starting_rx.await.expect("replacement scheduled");
    // A direct replacement's own abort sees no active task. It must still
    // wait for the first publication, rather than overtaking its late write.
    assert!(
        tokio::time::timeout(Duration::from_millis(200), contributor.winner.acquire())
            .await
            .is_err(),
        "winner published before its predecessor"
    );
    release_tx.send(()).expect("release first publication");
    holder.join().expect("holder exited");
    first_start.await.expect("first start completed");
    abort.await.expect("abort completed");
    next_start.await.expect("winner start completed");
    assert_eq!(
        vec![first, winner.clone()],
        *contributor
            .calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    );
    assert_eq!(
        Some(winner),
        session
            .active_turn
            .lock()
            .await
            .as_ref()
            .and_then(|turn| turn.task.as_ref())
            .map(|task| task.turn_context.sub_id.clone())
    );
    session.abort_all_tasks(TurnAbortReason::Interrupted).await;
}

struct PermitPreparation {
    permit: Arc<tokio::sync::Semaphore>,
    entered: CancellationToken,
    release: CancellationToken,
    commits: AtomicUsize,
}
impl TurnLifecycleContributor for PermitPreparation {
    fn on_turn_start<'a>(
        &'a self,
        input: codex_extension_api::TurnStartInput<'a>,
    ) -> codex_extension_api::ExtensionFuture<'a, ()> {
        Box::pin(async move {
            let permit = Arc::clone(&self.permit)
                .acquire_owned()
                .await
                .expect("permit open");
            assert!(
                input
                    .turn_store
                    .get::<codex_extension_api::TurnPreparationAccess>()
                    .expect("host preparation resources")
                    .retain(input.turn_id, permit)
                    .is_ok()
            );
            self.entered.cancel();
            self.release.cancelled().await;
        })
    }
    fn on_turn_committed(&self, _: TurnCommittedInput<'_>) {
        assert_eq!(
            0,
            self.permit.available_permits(),
            "publication owns preparation permit"
        );
        self.commits.fetch_add(1, Ordering::SeqCst);
    }
}
struct PermitRunTask {
    contributor: Arc<PermitPreparation>,
    ran: CancellationToken,
}
impl SessionTask for PermitRunTask {
    fn kind(&self) -> TaskKind {
        TaskKind::Regular
    }
    fn span_name(&self) -> &'static str {
        "session_task.preparation_permit"
    }
    async fn run(
        self: Arc<Self>,
        _: Arc<Session>,
        _: Arc<TurnContext>,
        _: Vec<TurnInput>,
        cancel: CancellationToken,
    ) -> SessionTaskResult {
        assert_eq!(1, self.contributor.commits.load(Ordering::SeqCst));
        assert_eq!(
            1,
            self.contributor.permit.available_permits(),
            "release before worker readiness"
        );
        self.ran.cancel();
        cancel.cancelled().await;
        Ok(None)
    }
}

#[tokio::test]
async fn retained_context_cannot_keep_preparation_permit_after_cancel_and_retry() {
    let (mut session, context) = make_session_and_context().await;
    let contributor = Arc::new(PermitPreparation {
        permit: Arc::new(tokio::sync::Semaphore::new(1)),
        entered: CancellationToken::new(),
        release: CancellationToken::new(),
        commits: AtomicUsize::new(0),
    });
    let mut builder = codex_extension_api::ExtensionRegistryBuilder::<crate::config::Config>::new();
    builder.turn_lifecycle_contributor(contributor.clone());
    session.services.extensions = Arc::new(builder.build());
    let session = Arc::new(session);
    let context = Arc::new(context);
    let before = session.state.lock().await.last_started_turn_id.clone();
    let mut start = Box::pin(session.start_task(
        Arc::clone(&context),
        Vec::new(),
        NeverEndingTask {
            kind: TaskKind::Regular,
            listen_to_cancellation_token: true,
        },
    ));
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! { _ = contributor.entered.cancelled() => {}, _ = &mut start => panic!("returned before held preparation") }
    }).await.expect("preparation entered");
    assert_eq!(0, contributor.permit.available_permits());
    drop(start);
    assert_eq!(before, session.state.lock().await.last_started_turn_id);
    assert_eq!(0, contributor.commits.load(Ordering::SeqCst));
    let permit = Arc::clone(&contributor.permit)
        .try_acquire_owned()
        .expect("cancel releases actual owned semaphore");
    let retained_access = context
        .extension_data
        .get::<codex_extension_api::TurnPreparationAccess>()
        .expect("retained context access");
    let refused = retained_access.retain(&context.sub_id, permit);
    assert!(
        refused.is_err(),
        "ended preparation never accepts resources"
    );
    drop(refused);
    assert_eq!(1, contributor.permit.available_permits());
    contributor.release.cancel();
    let ran = CancellationToken::new();
    assert!(
        session
            .start_task(
                context,
                Vec::new(),
                PermitRunTask {
                    contributor: Arc::clone(&contributor),
                    ran: ran.clone()
                }
            )
            .await
    );
    tokio::time::timeout(Duration::from_secs(5), ran.cancelled())
        .await
        .expect("committed task runs after permit release");
    session.abort_all_tasks(TurnAbortReason::Replaced).await;
}

#[tokio::test]
async fn final_admission_refusal_releases_preparation_permit_with_context_retained() {
    let (mut session, context) = make_session_and_context().await;
    let contributor = Arc::new(PermitPreparation {
        permit: Arc::new(tokio::sync::Semaphore::new(1)),
        entered: CancellationToken::new(),
        release: CancellationToken::new(),
        commits: AtomicUsize::new(0),
    });
    let mut builder = codex_extension_api::ExtensionRegistryBuilder::<crate::config::Config>::new();
    builder.turn_lifecycle_contributor(contributor.clone());
    session.services.extensions = Arc::new(builder.build());
    let session = Arc::new(session);
    let context = Arc::new(context);
    let mut start = Box::pin(session.start_task(
        Arc::clone(&context),
        Vec::new(),
        NeverEndingTask {
            kind: TaskKind::Regular,
            listen_to_cancellation_token: true,
        },
    ));
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! { _ = contributor.entered.cancelled() => {}, _ = &mut start => panic!("returned before held preparation") }
    }).await.expect("preparation entered");
    session.close_task_admission().await;
    contributor.release.cancel();
    assert!(!start.await);
    assert_eq!(0, contributor.commits.load(Ordering::SeqCst));
    assert_eq!(1, contributor.permit.available_permits());
    assert!(
        context
            .extension_data
            .get::<codex_extension_api::TurnPreparationAccess>()
            .is_some()
    );
    assert!(session.active_turn.lock().await.is_none());
}
