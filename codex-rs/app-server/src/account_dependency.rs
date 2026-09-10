//! Compiler-enforced exhaustive classification of every `ClientRequest`
//! variant into "requires an account-work permit before effect" or
//! "independent of the current account." Owned by this crate deliberately,
//! not `app-server-protocol`, so a new protocol variant fails compilation
//! here without widening the shared protocol macro's blast radius
//! (`CODEX-I05-S03-R009`, plan Preflight A closed-work census).
//!
//! The match below has no wildcard arm on purpose: a future `ClientRequest`
//! variant that omits classification must fail to compile, not silently
//! default to either class.

use codex_app_server_protocol::ClientRequest;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AccountDependency {
    /// Reads/writes no account-bound state; never gated by the barrier.
    Independent,
    /// Reaches auth, provider, MCP, plugin, skill, or model state and must
    /// hold an account-work permit before any effect (`CODEX-I05-S03-R009`,
    /// `R012`).
    Permit,
}

/// Classifies a request per the Preflight A closed-work census
/// (`implementation/preflight-a/codex-initial-plan-completion.md`). The
/// three `ManagedTransition*` control variants are deliberately
/// `Independent`: they are the barrier's own control surface, and gating
/// them on the barrier they themselves close would self-deadlock every
/// transition attempt (delegated design control 1).
pub(crate) fn classify(request: &ClientRequest) -> AccountDependency {
    match request {
        ClientRequest::ThreadStart { .. }
        | ClientRequest::ThreadResume { .. }
        | ClientRequest::ThreadFork { .. }
        | ClientRequest::ThreadClear { .. }
        | ClientRequest::ThreadUnsubscribe { .. }
        | ClientRequest::ThreadArchive { .. }
        | ClientRequest::ThreadDelete { .. }
        | ClientRequest::ThreadUnarchive { .. }
        | ClientRequest::ThreadRollback { .. }
        | ClientRequest::ThreadCompactStart { .. }
        | ClientRequest::ThreadBackgroundTerminalsClean { .. }
        | ClientRequest::ThreadBackgroundTerminalsTerminate { .. }
        | ClientRequest::ThreadShellCommand { .. }
        | ClientRequest::ThreadApproveGuardianDeniedAction { .. }
        | ClientRequest::TurnStart { .. }
        | ClientRequest::ThreadInjectItems { .. }
        | ClientRequest::TurnSteer { .. }
        | ClientRequest::TurnInterrupt { .. }
        | ClientRequest::ThreadRealtimeStart { .. }
        | ClientRequest::ThreadRealtimeAppendAudio { .. }
        | ClientRequest::ThreadRealtimeAppendText { .. }
        | ClientRequest::ThreadRealtimeAppendSpeech { .. }
        | ClientRequest::ThreadRealtimeStop { .. }
        | ClientRequest::ReviewStart { .. }
        | ClientRequest::GetConversationSummary { .. }
        | ClientRequest::SendAddCreditsNudgeEmail { .. }
        | ClientRequest::RemoteControlEnable { .. }
        | ClientRequest::RemoteControlDisable { .. }
        | ClientRequest::RemoteControlPairingStart { .. }
        | ClientRequest::RemoteControlPairingStatus { .. }
        | ClientRequest::RemoteControlClientsList { .. }
        | ClientRequest::RemoteControlClientsRevoke { .. }
        | ClientRequest::McpServerOauthLogin { .. }
        | ClientRequest::McpServerRefresh { .. }
        | ClientRequest::McpServerConfigIdentity { .. }
        | ClientRequest::McpServerStatusList { .. }
        | ClientRequest::McpResourceRead { .. }
        | ClientRequest::McpServerToolCall { .. }
        | ClientRequest::MarketplaceAdd { .. }
        | ClientRequest::MarketplaceRemove { .. }
        | ClientRequest::MarketplaceUpgrade { .. }
        | ClientRequest::PluginList { .. }
        | ClientRequest::PluginSearch { .. }
        | ClientRequest::PluginInstalled { .. }
        | ClientRequest::PluginRead { .. }
        | ClientRequest::PluginSkillRead { .. }
        | ClientRequest::PluginShareSave { .. }
        | ClientRequest::PluginShareUpdateTargets { .. }
        | ClientRequest::PluginShareList { .. }
        | ClientRequest::PluginShareCheckout { .. }
        | ClientRequest::PluginShareDelete { .. }
        | ClientRequest::PluginInstall { .. }
        | ClientRequest::PluginUninstall { .. }
        | ClientRequest::AppsRead { .. }
        | ClientRequest::AppsList { .. }
        | ClientRequest::AppsInstalled { .. }
        | ClientRequest::LoginAccount { .. }
        | ClientRequest::LogoutAccount { .. }
        | ClientRequest::SkillsList { .. }
        | ClientRequest::SkillsExtraRootsSet { .. }
        | ClientRequest::SkillsConfigWrite { .. }
        | ClientRequest::HooksList { .. } => AccountDependency::Permit,

        ClientRequest::CancelLoginAccount { .. }
        | ClientRequest::CollaborationModeList { .. }
        | ClientRequest::CommandExecResize { .. }
        | ClientRequest::CommandExecTerminate { .. }
        | ClientRequest::CommandExecWrite { .. }
        | ClientRequest::ConfigBatchWrite { .. }
        | ClientRequest::ConfigRead { .. }
        | ClientRequest::ConfigRequirementsRead { .. }
        | ClientRequest::ConfigValueWrite { .. }
        | ClientRequest::ConsumeAccountRateLimitResetCredit { .. }
        | ClientRequest::EnvironmentAdd { .. }
        | ClientRequest::EnvironmentInfo { .. }
        | ClientRequest::EnvironmentStatus { .. }
        | ClientRequest::ExperimentalFeatureEnablementSet { .. }
        | ClientRequest::ExperimentalFeatureList { .. }
        | ClientRequest::ExternalAgentConfigDetect { .. }
        | ClientRequest::ExternalAgentConfigImport { .. }
        | ClientRequest::ExternalAgentConfigImportHistoriesRead { .. }
        | ClientRequest::ExternalAgentConfigImportHistoryRecord { .. }
        | ClientRequest::FeedbackUpload { .. }
        | ClientRequest::FsCopy { .. }
        | ClientRequest::FsCreateDirectory { .. }
        | ClientRequest::FsGetMetadata { .. }
        | ClientRequest::FsReadDirectory { .. }
        | ClientRequest::FsReadFile { .. }
        | ClientRequest::FsRemove { .. }
        | ClientRequest::FsUnwatch { .. }
        | ClientRequest::FsWatch { .. }
        | ClientRequest::FsWriteFile { .. }
        | ClientRequest::FuzzyFileSearch { .. }
        | ClientRequest::FuzzyFileSearchSessionStart { .. }
        | ClientRequest::FuzzyFileSearchSessionStop { .. }
        | ClientRequest::FuzzyFileSearchSessionUpdate { .. }
        | ClientRequest::GetAccount { .. }
        | ClientRequest::GetAccountRateLimits { .. }
        | ClientRequest::GetAccountTokenUsage { .. }
        | ClientRequest::GetAuthStatus { .. }
        | ClientRequest::GetWorkspaceMessages { .. }
        | ClientRequest::GitDiffToRemote { .. }
        | ClientRequest::Initialize { .. }
        | ClientRequest::ManagedTransitionCancel { .. }
        | ClientRequest::ManagedTransitionRead { .. }
        | ClientRequest::ManagedTransitionStart { .. }
        | ClientRequest::MemoryReset { .. }
        | ClientRequest::MockExperimentalMethod { .. }
        | ClientRequest::ModelList { .. }
        | ClientRequest::ModelProviderCapabilitiesRead { .. }
        | ClientRequest::OneOffCommandExec { .. }
        | ClientRequest::PermissionProfileList { .. }
        | ClientRequest::ProcessKill { .. }
        | ClientRequest::ProcessResizePty { .. }
        | ClientRequest::ProcessSpawn { .. }
        | ClientRequest::ProcessWriteStdin { .. }
        | ClientRequest::RemoteControlStatusRead { .. }
        | ClientRequest::ServerDiagnostics { .. }
        | ClientRequest::ThreadAttachmentList { .. }
        | ClientRequest::ThreadBackgroundTerminalsList { .. }
        | ClientRequest::ThreadDecrementElicitation { .. }
        | ClientRequest::ThreadGoalClear { .. }
        | ClientRequest::ThreadGoalGet { .. }
        | ClientRequest::ThreadGoalSet { .. }
        | ClientRequest::ThreadIncrementElicitation { .. }
        | ClientRequest::ThreadItemsList { .. }
        | ClientRequest::ThreadList { .. }
        | ClientRequest::ThreadLoadedList { .. }
        | ClientRequest::ThreadMemoryModeSet { .. }
        | ClientRequest::ThreadMetadataUpdate { .. }
        | ClientRequest::ThreadRead { .. }
        | ClientRequest::ThreadRealtimeListVoices { .. }
        | ClientRequest::ThreadSearch { .. }
        | ClientRequest::ThreadSearchOccurrences { .. }
        | ClientRequest::ThreadSectionCreate { .. }
        | ClientRequest::ThreadSectionDelete { .. }
        | ClientRequest::ThreadSectionList { .. }
        | ClientRequest::ThreadSectionMove { .. }
        | ClientRequest::ThreadSectionUpdate { .. }
        | ClientRequest::ThreadSetName { .. }
        | ClientRequest::ThreadSettingsUpdate { .. }
        | ClientRequest::ThreadTurnsList { .. }
        | ClientRequest::WindowsSandboxReadiness { .. }
        | ClientRequest::WindowsSandboxSetupStart { .. } => AccountDependency::Independent,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn managed_transition_control_requests_are_independent_to_avoid_self_deadlock() {
        let start = ClientRequest::ManagedTransitionStart {
            request_id: codex_app_server_protocol::RequestId::Integer(1),
            params: codex_app_server_protocol::StartManagedTransitionParams {
                contract_version:
                    codex_app_server_protocol::MANAGED_AUTH_TRANSITION_CONTRACT_VERSION,
                transition_id: "t".to_owned(),
                process_instance_id: "p".to_owned(),
                intent: codex_app_server_protocol::ManagedTransitionIntent::AdoptManagedAuth,
                expected_auth_revision: 0,
                expected_transition_revision: 0,
                expected_auth_fingerprint: None,
                intended_result_auth_fingerprint: Some("intended-account".to_owned()),
            },
        };
        assert_eq!(classify(&start), AccountDependency::Independent);
    }

    #[test]
    fn representative_permit_class_members_are_permit() {
        let turn_start = ClientRequest::TurnStart {
            request_id: codex_app_server_protocol::RequestId::Integer(1),
            params: Default::default(),
        };
        assert_eq!(classify(&turn_start), AccountDependency::Permit);
    }

    #[test]
    fn credential_mutations_are_permit_gated() {
        let login = ClientRequest::LoginAccount {
            request_id: codex_app_server_protocol::RequestId::Integer(2),
            params: codex_app_server_protocol::LoginAccountParams::Chatgpt {
                codex_streamlined_login: false,
                use_hosted_login_success_page: false,
                app_brand: None,
            },
        };
        let logout = ClientRequest::LogoutAccount {
            request_id: codex_app_server_protocol::RequestId::Integer(3),
            params: None,
        };
        assert_eq!(classify(&login), AccountDependency::Permit);
        assert_eq!(classify(&logout), AccountDependency::Permit);
    }
}
