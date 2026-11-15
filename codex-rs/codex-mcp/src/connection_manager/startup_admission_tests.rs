use super::*;
use crate::{McpAttemptAccess, McpAttemptRefused, McpAttemptRequirement, McpAttemptWork};
use pretty_assertions::assert_eq;
use std::sync::atomic::AtomicUsize;

#[tokio::test]
async fn dormant_refusal_never_wakes_driver_and_admitted_original_survives_waiter_drop() {
    struct Work(Arc<AtomicUsize>);
    impl McpAttemptWork for Work {
        fn derive_attempt(&self) -> Result<Box<dyn McpAttemptWork>, McpAttemptRefused> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(Self(Arc::clone(&self.0))))
        }
    }
    impl Drop for Work {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }
    let live = Arc::new(AtomicUsize::new(1));
    let work = Work(Arc::clone(&live));
    let effects = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&effects);
    let (finish, receive) = tokio::sync::oneshot::channel();
    let mut client = tests::create_ready_async_managed_client(Vec::new()).await;
    client.client = crate::rmcp_client::ClientStartup::new(
        async move {
            counted.fetch_add(1, Ordering::SeqCst);
            receive.await.unwrap();
            Err(StartupOutcomeError::Cancelled)
        }
        .boxed()
        .shared(),
        McpAttemptRequirement::Required,
    );
    client.startup_complete.store(false, Ordering::SeqCst);
    let driver_client = client.clone();
    let mut manager = McpConnectionSet::empty(false);
    manager.insert_test_client("docs", client);
    let (trigger, mut receiver) = watch::channel(false);
    let connection =
        Arc::get_mut(&mut manager.servers.get_mut("docs").unwrap().connection).unwrap();
    connection.startup_trigger = Some(trigger);
    assert!(matches!(
        connection.client().await,
        Err(StartupOutcomeError::Refused(_))
    ));
    assert!(!*receiver.borrow());
    assert!(connection.startup_is_dormant());
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    assert_eq!(live.load(Ordering::SeqCst), 1);
    let driver = tokio::spawn(async move {
        receiver.wait_for(|started| *started).await.unwrap();
        driver_client.client().await
    });
    let mut observer =
        Box::pin(connection.client_with_authority(McpAttemptAccess::Admitted(&work)));
    assert!(futures::poll!(observer.as_mut()).is_pending());
    assert_eq!(live.load(Ordering::SeqCst), 2);
    drop(observer);
    drop(work);
    assert_eq!(live.load(Ordering::SeqCst), 1);
    finish.send(()).unwrap();
    let outcome = tokio::time::timeout(Duration::from_secs(3), driver)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(outcome, Err(StartupOutcomeError::Cancelled)));
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(live.load(Ordering::SeqCst), 0);
    assert!(!connection.startup_is_dormant());
}
