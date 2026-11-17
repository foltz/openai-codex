use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::time::{Duration, Instant};

#[tokio::test(start_paused = true)]
async fn timeout_retains_the_original_until_later_positive_join() {
    let (release, receiver) = tokio::sync::oneshot::channel();
    let completion = TurnCostCompletion::new(tokio::spawn(async move {
        receiver.await.unwrap();
    }));
    assert_eq!(
        completion
            .observe_until(Instant::now() + Duration::from_secs(1))
            .await,
        TurnCostShutdown::TimedOut
    );
    release.send(()).unwrap();
    assert_eq!(
        completion
            .observe_until(Instant::now() + Duration::from_secs(1))
            .await,
        TurnCostShutdown::Joined
    );
    assert_eq!(
        completion.observe_until(Instant::now()).await,
        TurnCostShutdown::Joined
    );
}

#[tokio::test]
async fn cancelled_observer_cannot_discard_original_join_custody() {
    let (release, receiver) = tokio::sync::oneshot::channel();
    let completion = TurnCostCompletion::new(tokio::spawn(async move {
        receiver.await.unwrap();
    }));
    let mut first = Box::pin(completion.observe_until(Instant::now() + Duration::from_secs(1)));
    assert!(futures::poll!(first.as_mut()).is_pending());
    drop(first);
    release.send(()).unwrap();
    assert_eq!(
        completion
            .observe_until(Instant::now() + Duration::from_secs(1))
            .await,
        TurnCostShutdown::Joined
    );
}

#[tokio::test]
async fn cancelled_original_is_sticky_failure_not_success() {
    let original = tokio::spawn(std::future::pending::<()>());
    original.abort();
    let completion = TurnCostCompletion::new(original);
    assert_eq!(
        completion
            .observe_until(Instant::now() + Duration::from_secs(1))
            .await,
        TurnCostShutdown::Cancelled
    );
    assert_eq!(
        completion.observe_until(Instant::now()).await,
        TurnCostShutdown::Cancelled
    );
}

#[tokio::test]
async fn panic_payload_drops_outside_completion_lock_and_failure_stays_sticky() {
    struct Payload {
        completion: std::sync::Weak<TurnCostCompletion>,
        unlocked: Arc<AtomicBool>,
    }
    impl Drop for Payload {
        fn drop(&mut self) {
            let completion = self.completion.upgrade().unwrap();
            self.unlocked
                .store(completion.state.try_lock().is_ok(), Ordering::SeqCst);
        }
    }
    let unlocked = Arc::new(AtomicBool::new(false));
    let observed = unlocked.clone();
    let (send, receive) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let completion = receive.await.unwrap();
        std::panic::panic_any(Payload {
            completion,
            unlocked: observed,
        });
    });
    let completion = Arc::new(TurnCostCompletion::new(task));
    send.send(Arc::downgrade(&completion)).unwrap();
    assert_eq!(
        completion
            .observe_until(Instant::now() + Duration::from_secs(1))
            .await,
        TurnCostShutdown::Panicked
    );
    assert!(unlocked.load(Ordering::SeqCst));
    assert_eq!(
        completion.observe_until(Instant::now()).await,
        TurnCostShutdown::Panicked
    );
}
