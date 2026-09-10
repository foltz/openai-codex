use super::*;
use std::time::Duration;

#[tokio::test(start_paused = true)]
async fn direct_close_timeout_preserves_exit_observer_in_both_protocol_modes() {
    for protocol_mode in [McpProtocolMode::Legacy, McpProtocolMode::V20260728] {
        let directory = tempfile::tempdir().expect("test gate directory");
        let gate = directory.path().join("exit");
        let mut command = Command::new("/bin/sh");
        command
            .args(["-c", "while [ ! -e \"$1\" ]; do sleep 0.01; done", "test-child"])
            .arg(&gate);
        let (mut transport, _) =
            LocalStdioTransport::spawn(command, "test-server".to_string(), protocol_mode)
                .expect("spawn test child");
        let mut observer = transport.exit_observer();
        let supervisor = observer.supervisor.abort_handle.clone();
        assert_eq!(
            transport.close().await.expect_err("live child must time out").kind(),
            io::ErrorKind::TimedOut
        );
        assert!(!supervisor.is_finished(), "timeout must retain terminal observation");
        std::fs::write(gate, b"exit").expect("release owned child");
        tokio::time::resume();
        tokio::time::timeout(Duration::from_secs(5), observer.wait())
            .await
            .expect("child exit observation completes")
            .expect("real exit remains observable after timeout");
        observer.wait().await.expect("terminal receipt replays");
        tokio::time::pause();
    }
}

#[tokio::test]
async fn final_local_observer_drop_aborts_the_child_supervisor() {
    for protocol_mode in [McpProtocolMode::Legacy, McpProtocolMode::V20260728] {
        let mut command = Command::new("/bin/sleep");
        command.arg("30");
        let (transport, _) =
            LocalStdioTransport::spawn(command, "test-server".to_string(), protocol_mode)
                .expect("spawn test child");
        let observer = transport.exit_observer();
        let supervisor = observer.supervisor.abort_handle.clone();
        drop(transport);
        tokio::task::yield_now().await;
        assert!(!supervisor.is_finished(), "a remaining observer owns the child");
        drop(observer);
        let stopped = tokio::time::timeout(Duration::from_secs(1), async {
            while !supervisor.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await;
        // Preserve cleanup even when this discriminator detects a regression.
        supervisor.abort();
        stopped.expect("the last observer must not detach its child supervisor");
    }
}

#[tokio::test]
async fn local_exit_observer_waits_for_child_exit_in_both_protocol_modes() {
    for protocol_mode in [McpProtocolMode::Legacy, McpProtocolMode::V20260728] {
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "exit 0"]);
        let (transport, _) =
            LocalStdioTransport::spawn(command, "test-server".to_string(), protocol_mode)
                .expect("spawn test child");
        let mut observer = transport.exit_observer();
        tokio::time::timeout(Duration::from_secs(1), observer.wait())
            .await
            .expect("child should exit promptly")
            .expect("observer should report terminal exit");
    }
}
