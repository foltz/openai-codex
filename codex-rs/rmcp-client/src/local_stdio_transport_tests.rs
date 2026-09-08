use super::*;
use std::time::Duration;

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
