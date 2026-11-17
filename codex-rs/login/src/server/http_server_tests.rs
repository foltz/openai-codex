use super::*;
use pretty_assertions::assert_eq;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

#[tokio::test]
async fn real_response_is_written_and_closed_before_delivery_ack() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (server, mut requests) = Server::start(listener).unwrap();
    let mut client = TcpStream::connect(address).await.unwrap();
    client
        .write_all(b"GET /cancel HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    let request = tokio::time::timeout(Duration::from_secs(5), requests.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(request.url, "/cancel");
    request
        .respond(http::Response::new(Full::new(Bytes::from_static(
            b"cancelled",
        ))))
        .await
        .unwrap();
    let mut wire = String::new();
    tokio::time::timeout(Duration::from_secs(5), client.read_to_string(&mut wire))
        .await
        .unwrap()
        .unwrap();
    assert!(wire.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(wire.contains("connection: close\r\n"));
    assert!(wire.ends_with("\r\ncancelled"));
    server.close();
    let expected = Report {
        acceptor: WorkerOutcome::Joined,
        connections: ConnectionOutcomes {
            joined: 1,
            ..Default::default()
        },
        unavailable: false,
    };
    assert_eq!(server.wait().await, expected);
    assert_eq!(server.wait().await, expected);
}

#[tokio::test]
async fn idle_and_partial_connections_are_joined_after_close() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (server, _requests) = Server::start(listener).unwrap();
    let mut idle = TcpStream::connect(address).await.unwrap();
    let mut partial = TcpStream::connect(address).await.unwrap();
    partial
        .write_all(b"GET /cancel HTTP/1.1\r\n")
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while server.state.lock().unwrap().connections.len() != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    server.close();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), server.wait())
            .await
            .unwrap(),
        Report {
            acceptor: WorkerOutcome::Joined,
            connections: ConnectionOutcomes {
                interrupted: 2,
                ..Default::default()
            },
            unavailable: false,
        }
    );
    for client in [&mut idle, &mut partial] {
        let result = client.read(&mut [0; 1]).await;
        assert!(
            matches!(result, Ok(0))
                || result.is_err_and(|e| e.kind() == io::ErrorKind::ConnectionReset)
        );
    }
    assert!(TcpStream::connect(address).await.is_err());
}

#[tokio::test]
async fn cancelled_observer_retains_exact_child_join_and_resource() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (server, _requests) = Server::start(listener).unwrap();
    let resource = Arc::new(());
    let weak_resource = Arc::downgrade(&resource);
    let (release, held) = oneshot::channel();
    register(&Arc::downgrade(&server.state), async move {
        let _resource = resource;
        held.await.unwrap();
        WorkerOutcome::Joined
    })
    .unwrap();
    server.close();
    let mut observer = Box::pin(server.wait());
    assert!(futures::poll!(observer.as_mut()).is_pending());
    drop(observer);
    assert!(weak_resource.upgrade().is_some());
    assert_eq!(server.state.lock().unwrap().connections.len(), 1);
    release.send(()).unwrap();
    let (first, second) = tokio::join!(server.wait(), server.wait());
    assert_eq!(first, second);
    assert_eq!(first.connections.joined, 1);
    assert!(!first.unavailable);
    assert!(weak_resource.upgrade().is_none());
    assert!(server.state.lock().unwrap().connections.is_empty());
}

#[tokio::test]
async fn panicked_child_is_recorded_and_cannot_erase_other_joins() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (server, _requests) = Server::start(listener).unwrap();
    register(&Arc::downgrade(&server.state), async {
        panic!("connection panic probe");
    })
    .unwrap();
    register(&Arc::downgrade(&server.state), async {
        WorkerOutcome::Joined
    })
    .unwrap();
    server.close();
    let report = server.wait().await;
    assert_eq!(
        report,
        Report {
            acceptor: WorkerOutcome::Joined,
            connections: ConnectionOutcomes {
                joined: 1,
                panicked: 1,
                ..Default::default()
            },
            unavailable: false,
        }
    );
    assert_eq!(server.wait().await, report);
}

#[tokio::test]
async fn response_ack_does_not_replace_the_wrapping_task_join() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (server, _unused_requests) = Server::start(listener).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (stream, _) = listener.accept().await.unwrap();
    let (requests, mut receiver) = mpsc::channel(16);
    let (entered, parked) = oneshot::channel();
    let (release, held) = oneshot::channel();
    let resource = Arc::new(());
    let weak_resource = Arc::downgrade(&resource);
    let shutdown = server.shutdown.clone();
    register(&Arc::downgrade(&server.state), async move {
        let _resource = resource;
        let result = connection(stream, requests, shutdown).await;
        entered.send(()).unwrap();
        held.await.unwrap();
        result
    })
    .unwrap();
    client
        .write_all(b"GET /success HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    receiver
        .recv()
        .await
        .unwrap()
        .respond(http::Response::new(Full::new(Bytes::from_static(b"done"))))
        .await
        .unwrap();
    parked.await.unwrap();
    let mut bytes = Vec::new();
    client.read_to_end(&mut bytes).await.unwrap();
    assert!(bytes.ends_with(b"done"));
    server.close();
    let mut observer = Box::pin(server.wait());
    assert!(futures::poll!(observer.as_mut()).is_pending());
    drop(observer);
    assert!(weak_resource.upgrade().is_some());
    release.send(()).unwrap();
    assert_eq!(server.wait().await.connections.joined, 1);
    assert!(weak_resource.upgrade().is_none());
}

#[tokio::test]
async fn acceptor_cancellation_does_not_destroy_connection_custody() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (server, _requests) = Server::start(listener).unwrap();
    let mut client = TcpStream::connect(address).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while server.state.lock().unwrap().connections.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    server
        .state
        .lock()
        .unwrap()
        .acceptor
        .as_ref()
        .unwrap()
        .ready
        .abort();
    server.close();
    assert_eq!(
        server.wait().await,
        Report {
            acceptor: WorkerOutcome::Cancelled,
            connections: ConnectionOutcomes {
                interrupted: 1,
                ..Default::default()
            },
            unavailable: false,
        }
    );
    assert_eq!(client.read(&mut [0; 1]).await.unwrap(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connection_registration_and_close_share_one_gate() {
    for _ in 0..16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (server, _requests) = Server::start(listener).unwrap();
        let server = Arc::new(server);
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let register_barrier = Arc::clone(&barrier);
        let state = Arc::downgrade(&server.state);
        let resource = Arc::new(());
        let weak_resource = Arc::downgrade(&resource);
        let registering = tokio::spawn(async move {
            register_barrier.wait().await;
            register(&state, async move {
                let _resource = resource;
                WorkerOutcome::Joined
            })
        });
        barrier.wait().await;
        server.close();
        let admitted = match registering.await.unwrap() {
            Ok(()) => true,
            Err(AdmissionError::Closed) => false,
            Err(AdmissionError::Unavailable) => panic!("registry should remain available"),
        };
        assert_eq!(
            server.wait().await,
            Report {
                acceptor: WorkerOutcome::Joined,
                connections: ConnectionOutcomes {
                    joined: u64::from(admitted),
                    ..Default::default()
                },
                unavailable: false,
            }
        );
        assert!(weak_resource.upgrade().is_none());
    }
}
