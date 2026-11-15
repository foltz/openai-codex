use super::*;
use crate::config::PermissionProfileSnapshot;
use crate::environment_selection::EnvironmentConfigOrigin;
use crate::tools::sandboxing::SandboxAttempt;
use codex_protocol::config_types::WindowsSandboxLevel;
use codex_protocol::models::AdditionalPermissionProfile;
use codex_protocol::models::FileSystemPermissions;
use codex_protocol::models::PermissionProfile;
use codex_protocol::permissions::NetworkSandboxPolicy;
use codex_protocol::protocol::EnvironmentConfig;
use codex_protocol::protocol::EnvironmentConfigState;
use codex_protocol::protocol::GranularApprovalConfig;
use codex_protocol::protocol::TurnEnvironmentSelection;
use codex_protocol::sandbox::SandboxOverride;
use codex_sandboxing::SandboxManager;
use codex_sandboxing::SandboxType;
use codex_sandboxing::policy_transforms::effective_file_system_sandbox_policy;
use codex_sandboxing::policy_transforms::effective_network_sandbox_policy;
use codex_utils_path_uri::PathUri;
use core_test_support::PathBufExt;
use core_test_support::PathExt;
use pretty_assertions::assert_eq;
use std::collections::HashMap;
fn test_turn_environment(environment_id: &str) -> crate::session::turn_context::TurnEnvironment {
    crate::session::turn_context::TurnEnvironment::new(
        TurnEnvironmentSelection {
            selected_capability_roots: Default::default(),
            environment_id: environment_id.to_string(),
            cwd: PathUri::from_abs_path(&std::env::temp_dir().abs()),
            workspace_roots: Vec::new(),
            config: EnvironmentConfigState::Ready(EnvironmentConfig {
                environment_hooks: Default::default(),
                allow_login_shell: true,
                workspace_roots: Vec::new(),
                windows_sandbox_level: WindowsSandboxLevel::Disabled,
                windows_sandbox_type: SandboxType::None,
                use_legacy_landlock: false,
                permission_profile: PermissionProfileSnapshot::legacy(
                    PermissionProfile::read_only(),
                ),
                shell_environment_policy: Default::default(),
                exec_policy: None,
                mcp_policy: None,
                network_policy: None,
                selected_capability_roots: Vec::new(),
            }),
        },
        EnvironmentConfigOrigin::Thread,
        std::sync::Arc::new(codex_exec_server::Environment::default_for_tests()),
        /*shell*/ None,
    )
}

#[test]
fn wants_no_sandbox_approval_granular_respects_sandbox_flag() {
    let runtime = ApplyPatchRuntime::new();
    assert!(runtime.wants_no_sandbox_approval(AskForApproval::OnRequest));
    assert!(
        !runtime.wants_no_sandbox_approval(AskForApproval::Granular(GranularApprovalConfig {
            sandbox_approval: false,
            rules: true,
            skill_approval: true,
            request_permissions: true,
            mcp_elicitations: true,
        }))
    );
    assert!(
        runtime.wants_no_sandbox_approval(AskForApproval::Granular(GranularApprovalConfig {
            sandbox_approval: true,
            rules: true,
            skill_approval: true,
            request_permissions: true,
            mcp_elicitations: true,
        }))
    );
}

#[tokio::test]
async fn approval_action_preserves_patch_path_uris() {
    let path = PathUri::parse("file:///C:/workspace/guardian-apply-patch-test.txt")
        .expect("valid foreign path URI");
    let action = ApplyPatchAction::new_add_for_test(&path, "hello".to_string());
    let expected_cwd = action.cwd.clone();
    let expected_patch = action.patch.clone();
    let request = ApplyPatchRequest {
        turn_environment: test_turn_environment(codex_exec_server::LOCAL_ENVIRONMENT_ID),
        action,
        file_paths: vec![path.clone()],
        changes: Arc::new(HashMap::new()),
        exec_approval_requirement: ExecApprovalRequirement::NeedsApproval {
            reason: None,
            proposed_execpolicy_amendment: None,
        },
        additional_permissions: None,
        permissions_preapproved: false,
    };

    let approval_action = request.approval_action("call-1");

    assert_eq!(
        approval_action,
        ApprovalAction::ApplyPatch {
            id: "call-1".to_string(),
            environment_id: codex_exec_server::LOCAL_ENVIRONMENT_ID.to_string(),
            cwd: expected_cwd,
            files: vec![path],
            patch: expected_patch,
            changes: Arc::new(HashMap::new()),
            permissions_preapproved: false,
        }
    );
}

#[tokio::test]
async fn permission_request_payload_uses_apply_patch_hook_name_and_aliases() {
    let path = std::env::temp_dir()
        .join("apply-patch-permission-request-payload.txt")
        .abs();
    let action =
        ApplyPatchAction::new_add_for_test(&PathUri::from_abs_path(&path), "hello".to_string());
    let expected_patch = action.patch.clone();
    let req = ApplyPatchRequest {
        turn_environment: test_turn_environment(codex_exec_server::LOCAL_ENVIRONMENT_ID),
        action,
        file_paths: vec![PathUri::from_abs_path(&path)],
        changes: Arc::new(HashMap::new()),
        exec_approval_requirement: ExecApprovalRequirement::NeedsApproval {
            reason: None,
            proposed_execpolicy_amendment: None,
        },
        additional_permissions: None,
        permissions_preapproved: false,
    };

    let payload = req.approval_action("call-1").permission_request_payload();

    assert_eq!(payload.tool_name.name(), "apply_patch");
    assert_eq!(
        payload.tool_name.matcher_aliases(),
        &["Write".to_string(), "Edit".to_string()]
    );
    assert_eq!(
        payload.tool_input,
        serde_json::json!({ "command": expected_patch })
    );
}

#[tokio::test]
async fn approval_keys_include_environment_id() {
    let runtime = ApplyPatchRuntime::new();
    let path = std::env::temp_dir()
        .join("apply-patch-approval-key.txt")
        .abs();
    let path_uri = PathUri::from_abs_path(&path);
    let req = ApplyPatchRequest {
        turn_environment: test_turn_environment("remote"),
        action: ApplyPatchAction::new_add_for_test(&path_uri, "hello".to_string()),
        file_paths: vec![path_uri.clone()],
        changes: Arc::new(HashMap::new()),
        exec_approval_requirement: ExecApprovalRequirement::Skip {
            bypass_sandbox: false,
            proposed_execpolicy_amendment: None,
        },
        additional_permissions: None,
        permissions_preapproved: false,
    };

    let keys = runtime
        .approval_action(&req, "call-1")
        .expect("build approval action")
        .cache_keys();

    assert_eq!(
        serde_json::to_value(&keys).expect("serialize approval keys"),
        serde_json::json!([
            {
                "environment_id": "remote",
                "path": path_uri,
            }
        ])
    );
}

#[tokio::test]
async fn sandbox_cwd_uses_patch_action_cwd() {
    let runtime = ApplyPatchRuntime::new();
    let path = std::env::temp_dir()
        .join("apply-patch-runtime-sandbox-cwd.txt")
        .abs();
    let req = ApplyPatchRequest {
        turn_environment: test_turn_environment(codex_exec_server::LOCAL_ENVIRONMENT_ID),
        action: ApplyPatchAction::new_add_for_test(
            &PathUri::from_abs_path(&path),
            "hello".to_string(),
        ),
        file_paths: vec![PathUri::from_abs_path(&path)],
        changes: Arc::new(HashMap::new()),
        exec_approval_requirement: ExecApprovalRequirement::Skip {
            bypass_sandbox: false,
            proposed_execpolicy_amendment: None,
        },
        additional_permissions: None,
        permissions_preapproved: false,
    };

    assert_eq!(runtime.sandbox_cwd(&req), Some(&req.action.cwd));
}

#[tokio::test]
async fn file_system_sandbox_context_preserves_executor_workspace_permissions() {
    let path = std::env::temp_dir()
        .join("apply-patch-runtime-attempt.txt")
        .abs();
    let additional_permissions = AdditionalPermissionProfile {
        network: None,
        file_system: Some(FileSystemPermissions::from_read_write_roots(
            Some(vec![path.clone()]),
            Some(Vec::new()),
        )),
    };
    let req = ApplyPatchRequest {
        turn_environment: test_turn_environment(codex_exec_server::LOCAL_ENVIRONMENT_ID),
        action: ApplyPatchAction::new_add_for_test(
            &PathUri::from_abs_path(&path),
            "hello".to_string(),
        ),
        file_paths: vec![PathUri::from_abs_path(&path)],
        changes: Arc::new(HashMap::new()),
        exec_approval_requirement: ExecApprovalRequirement::Skip {
            bypass_sandbox: false,
            proposed_execpolicy_amendment: None,
        },
        additional_permissions: Some(additional_permissions.clone()),
        permissions_preapproved: false,
    };
    let exec_server_permissions = PermissionProfile::workspace_write();
    let file_system_policy = exec_server_permissions.file_system_sandbox_policy();
    let permissions = exec_server_permissions
        .clone()
        .materialize_project_roots_with_workspace_roots(std::slice::from_ref(&path));
    let manager = SandboxManager::new();
    let sandbox_policy_cwd = PathUri::from_abs_path(&path);
    let attempt = SandboxAttempt {
        sandbox_override: SandboxOverride::NoOverride,
        sandbox: SandboxType::MacosSeatbelt,
        sandbox_requested: true,
        permissions: &permissions,
        exec_server_permissions: &exec_server_permissions,
        enforce_managed_network: false,
        manager: &manager,
        sandbox_cwd: &sandbox_policy_cwd,
        workspace_roots: std::slice::from_ref(&sandbox_policy_cwd),
        sandbox_exe: None,
        use_legacy_landlock: true,
        windows_sandbox_type: SandboxType::WindowsRestrictedToken,
        windows_sandbox_level: WindowsSandboxLevel::RestrictedToken,
        network_denial_cancellation_token: None,
        network_proxy: None,
    };

    let sandbox = ApplyPatchRuntime::file_system_sandbox_context_for_attempt(&req, &attempt)
        .expect("sandbox context");

    let file_system_policy =
        effective_file_system_sandbox_policy(&file_system_policy, Some(&additional_permissions));
    let network_policy = effective_network_sandbox_policy(
        NetworkSandboxPolicy::Restricted,
        Some(&additional_permissions),
    );
    let expected_permissions =
        PermissionProfile::from_runtime_permissions(&file_system_policy, network_policy);
    assert_eq!(sandbox.permissions, expected_permissions);
    assert_eq!(
        sandbox.cwd,
        codex_utils_path_uri::PathUri::from_abs_path(&path)
    );
    assert_eq!(
        sandbox.windows_sandbox_selection,
        if cfg!(windows) {
            codex_file_system::WindowsSandboxSelection::RestrictedToken
        } else {
            codex_file_system::WindowsSandboxSelection::Disabled
        }
    );
    assert_eq!(sandbox.use_legacy_landlock, true);
}

#[tokio::test]
async fn file_system_sandbox_context_respects_sandbox_request() {
    let path = std::env::temp_dir()
        .join("apply-patch-runtime-none.txt")
        .abs();
    let mut req = ApplyPatchRequest {
        turn_environment: test_turn_environment(codex_exec_server::LOCAL_ENVIRONMENT_ID),
        action: ApplyPatchAction::new_add_for_test(
            &PathUri::from_abs_path(&path),
            "hello".to_string(),
        ),
        file_paths: vec![PathUri::from_abs_path(&path)],
        changes: Arc::new(HashMap::new()),
        exec_approval_requirement: ExecApprovalRequirement::Skip {
            bypass_sandbox: false,
            proposed_execpolicy_amendment: None,
        },
        additional_permissions: None,
        permissions_preapproved: false,
    };
    let permissions = PermissionProfile::Disabled;
    let manager = SandboxManager::new();
    let sandbox_policy_cwd = PathUri::from_abs_path(&path);
    let attempt = SandboxAttempt {
        sandbox_override: SandboxOverride::NoOverride,
        sandbox: SandboxType::None,
        sandbox_requested: false,
        permissions: &permissions,
        exec_server_permissions: &permissions,
        enforce_managed_network: false,
        manager: &manager,
        sandbox_cwd: &sandbox_policy_cwd,
        workspace_roots: std::slice::from_ref(&sandbox_policy_cwd),
        sandbox_exe: None,
        use_legacy_landlock: false,
        windows_sandbox_type: SandboxType::None,
        windows_sandbox_level: WindowsSandboxLevel::Disabled,
        network_denial_cancellation_token: None,
        network_proxy: None,
    };

    assert_eq!(
        ApplyPatchRuntime::file_system_sandbox_context_for_attempt(&req, &attempt),
        None
    );

    let cwd = PathUri::parse("file:///C:/workspace").expect("Windows workspace URI");
    let user_home_dir = PathUri::parse("file:///C:/Users/remote").expect("Windows home URI");
    req.turn_environment.user_home_dir = Some(user_home_dir.clone());
    let permissions = PermissionProfile::workspace_write();
    let attempt = SandboxAttempt {
        sandbox_requested: true,
        permissions: &permissions,
        exec_server_permissions: &permissions,
        sandbox_cwd: &cwd,
        workspace_roots: std::slice::from_ref(&cwd),
        ..attempt
    };

    assert_eq!(
        ApplyPatchRuntime::file_system_sandbox_context_for_attempt(&req, &attempt),
        Some(FileSystemSandboxContext {
            sandbox_override: SandboxOverride::NoOverride,
            permissions,
            cwd: cwd.clone(),
            workspace_roots: vec![cwd],
            user_home_dir: Some(user_home_dir),
            temporary_directories: None,
            windows_sandbox_selection: codex_file_system::WindowsSandboxSelection::RestrictedToken,
            windows_sandbox_proxy_settings_mode: None,
            use_legacy_landlock: false,
        })
    );
}

#[tokio::test]
async fn every_patch_application_attempt_records_its_own_cleanup() {
    let workspace = tempfile::tempdir().unwrap();
    let path = PathUri::from_abs_path(&workspace.path().join("applied.txt").abs());
    let mut action = ApplyPatchAction::new_add_for_test(&path, "hello".to_string());
    // The metadata-only helper carries an Update patch; this witness executes
    // a real Add patch, consistent with its Add change map.
    action.patch =
        "*** Begin Patch\n*** Add File: applied.txt\n+hello\n*** End Patch\n".to_string();
    let request = ApplyPatchRequest {
        turn_environment: test_turn_environment(codex_exec_server::LOCAL_ENVIRONMENT_ID),
        action,
        file_paths: vec![path],
        changes: Arc::new(HashMap::new()),
        exec_approval_requirement: ExecApprovalRequirement::Skip {
            bypass_sandbox: false,
            proposed_execpolicy_amendment: None,
        },
        additional_permissions: None,
        permissions_preapproved: false,
    };
    let (session, turn) = crate::session::tests::make_session_and_context().await;
    let session = Arc::new(session);
    let turn = Arc::new(turn);
    let cancellation = tokio_util::sync::CancellationToken::new();
    let step = session
        .capture_step_context(Arc::clone(&turn), &cancellation)
        .await
        .unwrap();
    let ctx = ToolCtx {
        session: Arc::clone(&session),
        step_context: step,
        cancellation_token: cancellation,
        call_id: "cleanup-attempt".into(),
        tool_name: codex_tools::ToolName::plain("apply_patch"),
    };
    let permissions = PermissionProfile::Disabled;
    let manager = SandboxManager::new();
    let cwd = PathUri::from_abs_path(&workspace.path().abs());
    let attempt = SandboxAttempt {
        sandbox_override: SandboxOverride::NoOverride,
        sandbox: SandboxType::None,
        sandbox_requested: false,
        permissions: &permissions,
        exec_server_permissions: &permissions,
        enforce_managed_network: false,
        manager: &manager,
        sandbox_cwd: &cwd,
        workspace_roots: std::slice::from_ref(&cwd),
        sandbox_exe: None,
        use_legacy_landlock: false,
        windows_sandbox_type: SandboxType::None,
        windows_sandbox_level: WindowsSandboxLevel::Disabled,
        network_denial_cancellation_token: None,
        network_proxy: None,
    };
    let before = session.services.sandbox_cleanup_census().totals();
    let mut runtime = ApplyPatchRuntime::new();
    for _ in 0..2 {
        let output = runtime.run(&request, &attempt, &ctx).await.unwrap();
        assert_eq!(
            output.exec_output.exit_code, 0,
            "{}",
            output.exec_output.stderr.text
        );
    }
    assert_eq!(
        std::fs::read_to_string(workspace.path().join("applied.txt")).unwrap(),
        "hello\n"
    );
    let after = session.services.sandbox_cleanup_census().totals();
    assert_eq!(after.no_launch, before.no_launch + 2);
    assert_eq!(after.operations, before.operations);
}

#[tokio::test]
async fn direct_and_intercepted_patch_verification_record_separate_cleanup() {
    let workspace = tempfile::tempdir().unwrap();
    let target = workspace.path().join("verified.txt");
    std::fs::write(&target, "before\n").unwrap();
    let patch = format!(
        "*** Begin Patch\n*** Update File: {}\n@@\n-before\n+after\n*** End Patch\n",
        target.display()
    );
    let (session, turn) = crate::session::tests::make_session_and_context().await;
    let turn = Arc::new(turn);
    let cancel = tokio_util::sync::CancellationToken::new();
    let step = session.capture_step_context(turn, &cancel).await.unwrap();
    let mut environment = step.environments.primary().unwrap().clone();
    environment.selection.cwd = PathUri::from_abs_path(&workspace.path().abs());
    environment.config_mut().permission_profile =
        PermissionProfileSnapshot::legacy(PermissionProfile::Disabled);
    let census = session.services.sandbox_cleanup_census();
    let before = census.totals();
    crate::tools::handlers::apply_patch::prepare_patch_request(&patch, &environment, &step, census)
        .await
        .unwrap();
    let command = vec!["apply_patch".to_string(), patch];
    let fs = environment.environment().get_filesystem();
    assert!(
        crate::tools::handlers::apply_patch::prepare_intercepted_patch(
            &command,
            environment.cwd(),
            fs.as_ref(),
            &environment,
            &step,
            census
        )
        .await
        .unwrap()
        .is_some()
    );
    let after = census.totals();
    assert_eq!(after.no_launch, before.no_launch + 2);
    assert_eq!(after.operations, before.operations);
    assert_eq!(
        std::fs::read_to_string(&target).unwrap(),
        "before\n",
        "verification must not apply the patch"
    );
}
