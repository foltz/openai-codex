use super::*;
use pretty_assertions::assert_eq;
use std::time::Duration;

#[tokio::test(start_paused = true)]
async fn unavailable_and_expired_control_never_enqueue_and_cancelled_observer_keeps_command() {
    let home = tempfile::tempdir().unwrap();
    let config = Arc::new(
        codex_core::config::ConfigBuilder::default()
            .codex_home(home.path().to_path_buf())
            .build()
            .await
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(2);
    assert_eq!(
        TelemetryResetControl::default()
            .reset_until(7, Arc::clone(&config), deadline)
            .await,
        Err(TelemetryResetError::Unavailable)
    );
    let (control, mut commands) = TelemetryResetControl::channel();
    assert_eq!(
        control
            .reset_until(7, Arc::clone(&config), Instant::now())
            .await,
        Err(TelemetryResetError::TimedOut)
    );
    assert!(commands.try_recv().is_err());
    let mut observer = Box::pin(control.reset_until(7, Arc::clone(&config), deadline));
    assert!(futures::poll!(observer.as_mut()).is_pending());
    drop(observer);
    let command = commands.try_recv().unwrap();
    assert_eq!((command.generation, command.deadline), (7, deadline));
    assert!(Arc::ptr_eq(&command.config, &config));
    assert!(command.reply.is_closed());
}

#[tokio::test(start_paused = true)]
async fn queue_wait_and_response_share_one_deadline() {
    let home = tempfile::tempdir().unwrap();
    let config = Arc::new(
        codex_core::config::ConfigBuilder::default()
            .codex_home(home.path().to_path_buf())
            .build()
            .await
            .unwrap(),
    );
    let (control, mut commands) = TelemetryResetControl::channel();
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut first = Box::pin(control.reset_until(1, Arc::clone(&config), deadline));
    assert!(futures::poll!(first.as_mut()).is_pending());
    assert_eq!(
        control.reset_until(2, config, deadline).await,
        Err(TelemetryResetError::TimedOut)
    );
    assert_eq!(Instant::now(), deadline);
    assert_eq!(first.await, Err(TelemetryResetError::TimedOut));
    assert_eq!(commands.try_recv().unwrap().generation, 1);
    assert!(commands.try_recv().is_err());
}
