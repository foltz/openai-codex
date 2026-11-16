//! Real callback dispatch with synthetic finite account owners; no provider execution.
use super::*;
use codex_extension_api::{ExtensionData, ExtensionFuture};
use codex_mcp::{McpAttemptRefused, McpAttemptWork};
use codex_protocol::protocol::SessionSource;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Default)]
struct Counts {
    live: AtomicUsize,
    derives: AtomicUsize,
    refuse: AtomicBool,
}
struct Work(Arc<Counts>);
impl Work {
    fn new(c: &Arc<Counts>) -> Self {
        c.live.fetch_add(1, Ordering::SeqCst);
        Self(Arc::clone(c))
    }
}
impl Drop for Work {
    fn drop(&mut self) {
        self.0.live.fetch_sub(1, Ordering::SeqCst);
    }
}
impl McpAttemptWork for Work {
    fn derive_attempt(&self) -> Result<Box<dyn McpAttemptWork>, McpAttemptRefused> {
        self.0.derives.fetch_add(1, Ordering::SeqCst);
        if self.0.refuse.load(Ordering::SeqCst) {
            return Err(McpAttemptRefused);
        }
        Ok(Box::new(Self::new(&self.0)))
    }
}
#[derive(Default)]
struct Callback {
    calls: AtomicUsize,
    refused: AtomicUsize,
    unscoped: AtomicUsize,
    park: AtomicBool,
    wait: std::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    worker: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}
impl ThreadLifecycleContributor<()> for Callback {
    fn on_thread_start<'a>(&'a self, input: ThreadStartInput<'a, ()>) -> ExtensionFuture<'a, ()> {
        Box::pin(async move {
            let child = match input.mcp_access {
                Ok(McpAttemptAccess::Admitted(work)) => Some(work.derive_attempt().unwrap()),
                Ok(McpAttemptAccess::Unscoped) => {
                    self.unscoped.fetch_add(1, Ordering::SeqCst);
                    None
                }
                Err(_) => {
                    self.refused.fetch_add(1, Ordering::SeqCst);
                    return;
                }
            };
            self.calls.fetch_add(1, Ordering::SeqCst);
            input.thread_store.insert(true); // retained metadata, never account work
            if self.park.load(Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
            let wait = self.wait.lock().unwrap().take();
            if let Some(wait) = wait {
                *self.worker.lock().unwrap() = Some(tokio::spawn(async move {
                    let _work = child;
                    let _ = wait.await;
                }));
            }
        })
    }
}
fn input<'a>(
    access: Result<McpAttemptAccess<'a>, TurnWorkRefused>,
    session: &'a ExtensionData,
    thread: &'a ExtensionData,
) -> ThreadStartInput<'a, ()> {
    ThreadStartInput {
        mcp_access: access,
        config: &(),
        session_source: &SessionSource::Exec,
        persistent_thread_state_available: false,
        environments: &[],
        mcp_resource_client: None,
        extension_metrics: None,
        session_store: session,
        thread_store: thread,
    }
}
fn contributors(callback: &Arc<Callback>) -> Vec<Arc<dyn ThreadLifecycleContributor<()>>> {
    vec![callback.clone()]
}
#[tokio::test]
async fn thread_start_dispatch_releases_work_without_keeping_it_in_state() {
    let counts = Arc::new(Counts::default());
    let root = Work::new(&counts);
    let callback = Arc::new(Callback::default());
    let session = ExtensionData::new("session");
    let thread = ExtensionData::new("thread");
    emit_thread_start_lifecycle(
        &contributors(&callback),
        input(Ok(McpAttemptAccess::Admitted(&root)), &session, &thread),
    )
    .await;
    assert_eq!(callback.calls.load(Ordering::SeqCst), 1);
    assert_eq!(counts.derives.load(Ordering::SeqCst), 2);
    assert_eq!(counts.live.load(Ordering::SeqCst), 1);
    drop(root);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    assert!(*thread.get::<bool>().unwrap());
}
#[tokio::test]
async fn hook_free_thread_start_does_not_derive_work() {
    let counts = Arc::new(Counts::default());
    let root = Work::new(&counts);
    counts.refuse.store(true, Ordering::SeqCst);
    let session = ExtensionData::new("session");
    let thread = ExtensionData::new("thread");
    emit_thread_start_lifecycle::<()>(
        &[],
        input(Ok(McpAttemptAccess::Admitted(&root)), &session, &thread),
    )
    .await;
    assert_eq!(counts.derives.load(Ordering::SeqCst), 0);
    drop(root);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn thread_start_missing_exhausted_and_retry_authority_refuse_without_reacquisition() {
    let session = ExtensionData::new("session");
    let thread = ExtensionData::new("thread");
    let callback = Arc::new(Callback::default());
    emit_thread_start_lifecycle(
        &contributors(&callback),
        input(Err(TurnWorkRefused::Unavailable), &session, &thread),
    )
    .await;
    let counts = Arc::new(Counts::default());
    let root = Work::new(&counts);
    counts.refuse.store(true, Ordering::SeqCst);
    emit_thread_start_lifecycle(
        &contributors(&callback),
        input(Ok(McpAttemptAccess::Admitted(&root)), &session, &thread),
    )
    .await;
    let (wake, wait) = tokio::sync::oneshot::channel::<()>();
    emit_thread_start_lifecycle(
        &contributors(&callback),
        input(
            Err(TurnWorkRefused::RetryAfter(Box::pin(async move {
                let _ = wait.await;
            }))),
            &session,
            &thread,
        ),
    )
    .await;
    assert!(wake.send(()).is_err(), "one-shot refusal retains no wakeup");
    assert_eq!(callback.refused.load(Ordering::SeqCst), 3);
    assert_eq!(callback.calls.load(Ordering::SeqCst), 0);
    assert_eq!(callback.unscoped.load(Ordering::SeqCst), 0);
    assert_eq!(counts.derives.load(Ordering::SeqCst), 1);
    drop(root);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn cancelled_thread_start_callback_releases_its_finite_descendants() {
    let counts = Arc::new(Counts::default());
    let root = Work::new(&counts);
    let callback = Arc::new(Callback::default());
    callback.park.store(true, Ordering::SeqCst);
    let session = ExtensionData::new("session");
    let thread = ExtensionData::new("thread");
    let callbacks = contributors(&callback);
    let mut start = Box::pin(emit_thread_start_lifecycle(
        &callbacks,
        input(Ok(McpAttemptAccess::Admitted(&root)), &session, &thread),
    ));
    assert!(futures::poll!(&mut start).is_pending());
    assert_eq!(callback.calls.load(Ordering::SeqCst), 1);
    assert_eq!(counts.live.load(Ordering::SeqCst), 3);
    drop(start);
    assert_eq!(counts.live.load(Ordering::SeqCst), 1);
    drop(root);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn spawned_thread_start_work_lives_until_actual_task_destruction() {
    let counts = Arc::new(Counts::default());
    let root = Work::new(&counts);
    let callback = Arc::new(Callback::default());
    let (release, wait) = tokio::sync::oneshot::channel();
    *callback.wait.lock().unwrap() = Some(wait);
    let session = ExtensionData::new("session");
    let thread = ExtensionData::new("thread");
    emit_thread_start_lifecycle(
        &contributors(&callback),
        input(Ok(McpAttemptAccess::Admitted(&root)), &session, &thread),
    )
    .await;
    assert_eq!(counts.live.load(Ordering::SeqCst), 2);
    drop(root);
    assert_eq!(counts.live.load(Ordering::SeqCst), 1);
    let worker = callback.worker.lock().unwrap().take().unwrap();
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    assert!(release.send(()).is_err());
    assert!(*thread.get::<bool>().unwrap());
}
