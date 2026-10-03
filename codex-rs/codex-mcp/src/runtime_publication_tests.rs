use super::*;
use crate::connection_manager::tests::retirement_runtime_input;
use crate::connection_manager::tests::retirement_stdio_config;
use futures::FutureExt;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn cancelled_replacements_wait_before_reuse_selection_and_leave_external_current_unarmed()
-> anyhow::Result<()> {
    let home = tempfile::tempdir()?;
    let marker = home.path().join("gated-pid");
    let context = McpRuntimeContext::new(
        Arc::new(EnvironmentManager::default_for_tests()),
        home.path().to_path_buf(),
    );
    let input = || {
        retirement_runtime_input(
            home.path(),
            retirement_stdio_config(&marker, "gate"),
            context.clone(),
            McpToolCatalogCache::default(),
            McpStartupPolicy::Eager,
        )
    };
    let control = crate::McpRuntimeRetirement::default();
    let runtime = McpRuntime::new_in_retirement(input(), control.clone()).await;
    let binding = runtime
        .current_binding_with_requirements(&["docs".to_owned()], &HashSet::new())
        .await
        .unwrap();
    let before = runtime.current.load_full();
    let guard = runtime.replacement_gate.acquire().await.unwrap();
    assert!(runtime.replace(input()).now_or_never().is_none());
    assert!(runtime.replace_fresh(input()).now_or_never().is_none());
    assert!(Arc::ptr_eq(&before, &runtime.current.load_full()));
    assert_eq!(std::fs::read_to_string(&marker)?.lines().count(), 1);
    drop(guard);
    drop((before, binding, runtime));
    let pid = std::fs::read_to_string(&marker)?;
    assert!(
        std::process::Command::new("/bin/kill")
            .args(["-0", pid.trim()])
            .status()?
            .success(),
        "initial empty publication and cancelled replacement must leave the external current unarmed"
    );
    let report = control
        .shutdown_until(tokio::time::Instant::now() + Duration::from_secs(5))
        .await;
    assert!(report.is_complete(), "{report:?}");
    Ok(())
}
