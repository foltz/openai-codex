//! Physical future destruction and admission fencing, with no provider or helper.
use super::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

struct Payload {
    live: Arc<AtomicUsize>,
    saw_registered_drop: Arc<AtomicBool>,
    owner: Arc<ClassifierTasks>,
}
impl Payload {
    fn new(owner: &Arc<ClassifierTasks>, live: &Arc<AtomicUsize>, saw: &Arc<AtomicBool>) -> Self {
        live.fetch_add(1, Ordering::SeqCst);
        Self {
            live: Arc::clone(live),
            saw_registered_drop: Arc::clone(saw),
            owner: Arc::clone(owner),
        }
    }
}
impl Drop for Payload {
    fn drop(&mut self) {
        self.saw_registered_drop
            .store(!self.owner.runtime.tasks.is_empty(), Ordering::SeqCst);
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}
#[tokio::test]
async fn shutdown_waits_for_unpolled_payload_destruction_before_terminal_token() {
    let owner = Arc::new(ClassifierTasks::default());
    let live = Arc::new(AtomicUsize::new(0));
    let saw = Arc::new(AtomicBool::new(false));
    let payload = Payload::new(&owner, &live, &saw);
    let task = owner
        .spawn(async move {
            let _payload = payload;
            std::future::pending::<()>().await;
        })
        .unwrap();
    let mut stop = Box::pin(owner.shutdown());
    assert!(
        std::future::Future::poll(
            stop.as_mut(),
            &mut Context::from_waker(std::task::Waker::noop())
        )
        .is_pending()
    );
    assert_eq!(live.load(Ordering::SeqCst), 1);
    tokio::time::timeout(Duration::from_secs(5), stop)
        .await
        .unwrap();
    task.await.unwrap();
    assert_eq!(live.load(Ordering::SeqCst), 0);
    assert!(
        saw.load(Ordering::SeqCst),
        "payload was destroyed before its terminal token"
    );
    assert!(owner.runtime.tasks.is_empty());
}
#[tokio::test]
async fn closed_classifier_owner_refuses_and_drops_never_started_payloads() {
    let owner = Arc::new(ClassifierTasks::default());
    owner.shutdown().await;
    let live = Arc::new(AtomicUsize::new(0));
    let saw = Arc::new(AtomicBool::new(false));
    let payload = Payload::new(&owner, &live, &saw);
    let ran = Arc::new(AtomicBool::new(false));
    let inside = Arc::clone(&ran);
    assert!(
        owner
            .spawn(async move {
                let _payload = payload;
                inside.store(true, Ordering::SeqCst);
            })
            .is_none()
    );
    assert_eq!(live.load(Ordering::SeqCst), 0);
    assert!(!ran.load(Ordering::SeqCst));
    assert!(owner.track(async {}).is_none());
    assert!(
        owner
            .with_open(|| panic!("closed cache publication"))
            .is_none()
    );
    owner.shutdown().await;
}
#[tokio::test]
async fn aborting_parent_does_not_complete_registered_child_cleanup() {
    let owner = Arc::new(ClassifierTasks::default());
    let live = Arc::new(AtomicUsize::new(0));
    let saw = Arc::new(AtomicBool::new(false));
    let payload = Payload::new(&owner, &live, &saw);
    let (release, wait) = tokio::sync::oneshot::channel::<()>();
    let (child_ready, child_handle) = tokio::sync::oneshot::channel();
    let parent_owner = Arc::clone(&owner);
    let parent = owner
        .spawn(async move {
            // Deliberately held child: only real destruction can release its token.
            let child = parent_owner
                .track(async move {
                    let _payload = payload;
                    wait.await.unwrap();
                })
                .unwrap();
            child_ready.send(tokio::spawn(child)).unwrap();
            std::future::pending::<()>().await;
        })
        .unwrap();
    let child = child_handle.await.unwrap();
    parent.abort();
    assert!(parent.await.unwrap_err().is_cancelled());
    let mut stop = Box::pin(owner.shutdown());
    assert!(
        std::future::Future::poll(
            stop.as_mut(),
            &mut Context::from_waker(std::task::Waker::noop())
        )
        .is_pending()
    );
    assert_eq!(live.load(Ordering::SeqCst), 1);
    release.send(()).unwrap();
    child.await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), stop)
        .await
        .unwrap();
    assert_eq!(live.load(Ordering::SeqCst), 0);
    assert!(saw.load(Ordering::SeqCst));
}
#[tokio::test]
async fn cancelled_shutdown_waiter_preserves_closed_state_and_original_join() {
    let owner = Arc::new(ClassifierTasks::default());
    let live = Arc::new(AtomicUsize::new(0));
    let saw = Arc::new(AtomicBool::new(false));
    let payload = Payload::new(&owner, &live, &saw);
    let task = owner
        .spawn(async move {
            let _payload = payload;
            std::future::pending::<()>().await;
        })
        .unwrap();
    let mut first = Box::pin(owner.shutdown());
    assert!(
        std::future::Future::poll(
            first.as_mut(),
            &mut Context::from_waker(std::task::Waker::noop())
        )
        .is_pending()
    );
    drop(first);
    assert!(owner.spawn(async {}).is_none());
    let mut second = Box::pin(owner.shutdown());
    assert!(
        std::future::Future::poll(
            second.as_mut(),
            &mut Context::from_waker(std::task::Waker::noop())
        )
        .is_pending()
    );
    assert_eq!(live.load(Ordering::SeqCst), 1);
    tokio::time::timeout(Duration::from_secs(5), second)
        .await
        .unwrap();
    task.await.unwrap();
    assert_eq!(live.load(Ordering::SeqCst), 0);
    assert!(saw.load(Ordering::SeqCst));
}
#[tokio::test]
async fn poisoned_classifier_registration_refuses_but_existing_tasks_still_join() {
    let owner = Arc::new(ClassifierTasks::default());
    let task = owner.spawn(std::future::pending()).unwrap();
    let poison = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = owner.closed.lock().unwrap();
        panic!("synthetic classifier admission poison");
    }));
    assert!(poison.is_err());
    assert!(owner.track(async {}).is_none());
    assert!(
        owner
            .with_open(|| panic!("poison is not authority"))
            .is_none()
    );
    tokio::time::timeout(Duration::from_secs(5), owner.shutdown())
        .await
        .unwrap();
    task.await.unwrap();
    assert!(owner.runtime.tasks.is_empty());
}
