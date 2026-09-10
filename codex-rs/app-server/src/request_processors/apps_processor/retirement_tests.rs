use super::*;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn reserves_before_use_and_compacts_only_proven_controls() {
    let owners = AppsRuntimeOwners::default();
    let ticket = owners.ticket();
    let first = ticket.reserve().unwrap();
    assert!(!first.is_retired());
    assert_eq!(owners.state.lock().unwrap().runtimes.len(), 1);
    assert!(first.shutdown_until(Instant::now()).await.is_complete());
    let second = ticket.reserve().unwrap();
    assert_eq!(owners.state.lock().unwrap().runtimes.len(), 1);
    assert!(!second.is_retired());
    let report = owners.shutdown_until(Instant::now()).await;
    assert_eq!(
        report,
        AppsRuntimeDrain {
            unavailable: false,
            reports: vec![RuntimeTerminationReport {
                connections: vec![],
                tasks: vec![]
            }]
        }
    );
    assert!(second.is_retired());
    assert!(ticket.reserve().is_err());
    assert!(owners.state.lock().unwrap().runtimes.is_empty());
    assert_eq!(owners.shutdown_until(Instant::now()).await, report);
    drop(owners);
    assert!(ticket.reserve().is_err());
}

#[tokio::test]
async fn poisoned_collection_closes_known_controls_without_claiming_success() {
    let owners = AppsRuntimeOwners::default();
    let control = owners.ticket().reserve().unwrap();
    let state = Arc::clone(&owners.state);
    assert!(
        std::thread::spawn(move || {
            let _guard = state.lock().unwrap();
            panic!("controlled runtime collection poison");
        })
        .join()
        .is_err()
    );
    let report = owners.shutdown_until(Instant::now()).await;
    assert!(report.unavailable);
    assert!(control.is_retired());
    assert!(owners.ticket().reserve().is_err());
    let state = match owners.state.lock() {
        Ok(_) => panic!("poison must remain observable"),
        Err(error) => error.into_inner(),
    };
    assert_eq!(state.runtimes.len(), 1);
}
