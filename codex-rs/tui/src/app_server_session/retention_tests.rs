use super::*;
use codex_app_server_client::AppServerClient;
use codex_app_server_client::CloudClient;
use codex_app_server_client::CloudOperation;

#[tokio::test]
async fn abandoned_new_grant_releases_the_exact_thread_and_grant() {
    let (client, mut commands, _events) = CloudClient::channel(4);
    let client = AppServerClient::Cloud(client);
    let (retention, _warnings) = RetentionClient::new();
    let handle = client.request_handle();
    let acquiring = tokio::spawn(async move {
        retention
            .retain_thread(handle, "successor".to_owned())
            .await
    });
    let command = commands.recv().await.expect("acquisition is sent");
    let CloudOperation::Request(request) = command.operation else {
        panic!("expected request")
    };
    let ClientRequest::ThreadRetentionAcquire { params, .. } = *request else {
        panic!("expected acquire")
    };
    assert_eq!(params.thread_id, "successor");
    command
        .response
        .send(Ok(Ok(serde_json::to_value(
            ThreadRetentionAcquireResponse::Acquired {
                grant_id: "new-grant".into(),
            },
        )
        .unwrap())))
        .unwrap();
    let pending = acquiring.await.unwrap().unwrap();
    drop(pending);
    let command = commands
        .recv()
        .await
        .expect("abandoned adoption releases custody");
    let CloudOperation::Request(request) = command.operation else {
        panic!("expected request")
    };
    let ClientRequest::ThreadRetentionRelease { params, .. } = *request else {
        panic!("expected release")
    };
    assert_eq!(
        (params.thread_id.as_str(), params.grant_id.as_str()),
        ("successor", "new-grant")
    );
    command
        .response
        .send(Ok(Ok(serde_json::to_value(
            ThreadRetentionReleaseResponse::Released {},
        )
        .unwrap())))
        .unwrap();
    tokio::task::yield_now().await;
}

#[tokio::test]
async fn abandoning_existing_grant_does_not_release_another_adoption() {
    let (client, mut commands, _events) = CloudClient::channel(4);
    let client = AppServerClient::Cloud(client);
    let (retention, _warnings) = RetentionClient::new();
    let acquiring = tokio::spawn(async move {
        retention
            .adopt_thread(
                client.request_handle(),
                "existing".into(),
                Some(ThreadRetentionAcquireResponse::AlreadyHeld {
                    grant_id: "existing-grant".into(),
                }),
            )
            .await
            .unwrap()
    });
    let command = commands
        .recv()
        .await
        .expect("existing grant is revalidated");
    let CloudOperation::Request(request) = command.operation else {
        panic!("request")
    };
    assert!(matches!(
        *request,
        ClientRequest::ThreadRetentionAcquire { .. }
    ));
    command
        .response
        .send(Ok(Ok(serde_json::to_value(
            ThreadRetentionAcquireResponse::AlreadyHeld {
                grant_id: "existing-grant".into(),
            },
        )
        .unwrap())))
        .unwrap();
    let pending = acquiring.await.unwrap();
    drop(pending);
    tokio::task::yield_now().await;
    assert!(matches!(
        commands.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
}

#[tokio::test]
async fn ineligible_adoption_warns_once_without_minting_or_releasing_a_grant() {
    let (client, mut commands, _events) = CloudClient::channel(4);
    let client = AppServerClient::Cloud(client);
    let (retention, mut warnings) = RetentionClient::new();
    for id in ["first", "second"] {
        retention
            .adopt_thread(
                client.request_handle(),
                id.into(),
                Some(ThreadRetentionAcquireResponse::Refused {
                    reason: ThreadRetentionRefusalReason::IneligiblePrincipal,
                }),
            )
            .await
            .unwrap()
            .commit();
    }
    assert_eq!(warnings.recv().await.unwrap().message, UNRETAINED_WARNING);
    assert!(matches!(
        warnings.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    assert!(matches!(
        commands.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
}

#[tokio::test]
async fn stale_already_held_evidence_is_revalidated_after_abandonment_release() {
    let (client, mut commands, _events) = CloudClient::channel(4);
    let client = AppServerClient::Cloud(client);
    let (retention, _warnings) = RetentionClient::new();
    let first = retention
        .adopt_thread(
            client.request_handle(),
            "same-thread".into(),
            Some(ThreadRetentionAcquireResponse::Acquired {
                grant_id: "old".into(),
            }),
        )
        .await
        .unwrap();
    drop(first);
    let old_release = commands.recv().await.expect("old release begins");
    let CloudOperation::Request(request) = &old_release.operation else {
        panic!("request")
    };
    assert!(
        matches!(request.as_ref(), ClientRequest::ThreadRetentionRelease { params, .. } if params.grant_id == "old")
    );
    let second = tokio::spawn(async move {
        retention
            .adopt_thread(
                client.request_handle(),
                "same-thread".into(),
                // The resume completed before the old release, so this evidence
                // describes the grant that is now being removed.
                Some(ThreadRetentionAcquireResponse::AlreadyHeld {
                    grant_id: "old".into(),
                }),
            )
            .await
            .unwrap()
    });
    tokio::task::yield_now().await;
    assert!(!second.is_finished());
    assert!(matches!(
        commands.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    old_release
        .response
        .send(Ok(Ok(serde_json::to_value(
            ThreadRetentionReleaseResponse::Released {},
        )
        .unwrap())))
        .unwrap();
    let acquire = timeout(Duration::from_secs(1), commands.recv())
        .await
        .unwrap()
        .expect("revalidation must not trust stale evidence");
    let CloudOperation::Request(request) = &acquire.operation else {
        panic!("request")
    };
    assert!(
        matches!(request.as_ref(), ClientRequest::ThreadRetentionAcquire { params, .. } if params.thread_id == "same-thread")
    );
    acquire
        .response
        .send(Ok(Ok(serde_json::to_value(
            ThreadRetentionAcquireResponse::Acquired {
                grant_id: "new".into(),
            },
        )
        .unwrap())))
        .unwrap();
    drop(second.await.unwrap());
    let new_release = timeout(Duration::from_secs(1), commands.recv())
        .await
        .unwrap()
        .expect("newly minted grant is independently abandoned");
    let CloudOperation::Request(request) = &new_release.operation else {
        panic!("request")
    };
    assert!(
        matches!(request.as_ref(), ClientRequest::ThreadRetentionRelease { params, .. } if params.grant_id == "new" && params.thread_id == "same-thread")
    );
    new_release
        .response
        .send(Ok(Ok(serde_json::to_value(
            ThreadRetentionReleaseResponse::Released {},
        )
        .unwrap())))
        .unwrap();
}
