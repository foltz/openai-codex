use super::*;
use pretty_assertions::assert_eq;
use std::time::Duration;

#[tokio::test]
async fn join_observation_preserves_single_use_result_without_debug_disclosure() {
    let owner = CallbackJoin::new(tokio::spawn(async {
        Err(io::Error::other("credential-sentinel-for-retained-result"))
    }));
    assert_eq!(owner.wait().await, CallbackOutcome::Returned);
    assert!(!format!("{owner:?}").contains("credential-sentinel"));
    assert_eq!(
        owner.take_result().await.unwrap_err().to_string(),
        "credential-sentinel-for-retained-result"
    );
    assert_eq!(owner.wait().await, CallbackOutcome::Returned);
}

#[tokio::test]
async fn cancelled_observer_retains_original_callback_join_and_result() {
    let (release, held) = tokio::sync::oneshot::channel();
    let owner = CallbackJoin::new(tokio::spawn(async move {
        held.await.unwrap();
        Ok(LoginCallbackResult::default())
    }));
    assert!(
        tokio::time::timeout(Duration::from_millis(10), owner.take_result())
            .await
            .is_err()
    );
    assert!(matches!(*owner.0.lock().await, State::Running(_)));
    release.send(()).unwrap();
    assert_eq!(
        owner.take_result().await.unwrap(),
        LoginCallbackResult::default()
    );
    assert!(matches!(
        *owner.0.lock().await,
        State::Finished {
            outcome: CallbackOutcome::Returned,
            result: None
        }
    ));
}

#[tokio::test]
async fn cancelled_and_panicked_callbacks_remain_distinct_after_result_consumption() {
    let task = tokio::spawn(std::future::pending::<io::Result<LoginCallbackResult>>());
    task.abort();
    let cancelled = CallbackJoin::new(task);
    assert!(cancelled.take_result().await.is_err());
    assert!(matches!(
        *cancelled.0.lock().await,
        State::Finished {
            outcome: CallbackOutcome::Cancelled,
            result: None
        }
    ));
    let panicked = CallbackJoin::new(tokio::spawn(async { panic!("callback probe") }));
    assert!(panicked.take_result().await.is_err());
    assert!(matches!(
        *panicked.0.lock().await,
        State::Finished {
            outcome: CallbackOutcome::Panicked,
            result: None
        }
    ));
}
