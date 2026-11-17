//! Exhaustive request admission for the actual 0.169 wire surface.
//! Pure cache observations/control/cancellation remain independent. A request
//! that can resolve credentials or perform account/provider work is counted
//! before bookkeeping or queueing, including warm model-picker requests.
//! Missing historical protocol controls are not retired; adding them to the
//! wire must add explicit arms here (no wildcard fallback).

use codex_app_server_protocol::ClientRequest;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AccountDependency {
    Independent,
    Permit,
}

pub(crate) fn classify(request: &ClientRequest) -> AccountDependency {
    match request {
        ClientRequest::GetAccount { params, .. } => {
            if params.cache_only {
                AccountDependency::Independent
            } else {
                AccountDependency::Permit
            }
        }
        ClientRequest::AccountSessionsList { params, .. } => {
            if params.refresh_workspace_metadata {
                AccountDependency::Permit
            } else {
                AccountDependency::Independent
            }
        }
        ClientRequest::AccountSessionsSwitch { .. }
        | ClientRequest::AppsInstalled { .. }
        | ClientRequest::AppsList { .. }
        | ClientRequest::AppsRead { .. }
        | ClientRequest::BedrockCheckGovCloudRequirements { .. }
        | ClientRequest::BedrockSetup { .. }
        | ClientRequest::ConsumeAccountRateLimitResetCredit { .. }
        | ClientRequest::GatewayOAuthLogin { .. }
        | ClientRequest::GatewayOAuthRead { .. }
        | ClientRequest::GcpSetup { .. }
        | ClientRequest::GetAccountRateLimits { .. }
        | ClientRequest::GetAccountSecuritySetup { .. }
        | ClientRequest::GetAccountTokenUsage { .. }
        | ClientRequest::GetAuthStatus { .. }
        | ClientRequest::GetConversationSummary { .. }
        | ClientRequest::GetWorkspaceMessages { .. }
        | ClientRequest::HooksList { .. }
        | ClientRequest::LoginAccount { .. }
        | ClientRequest::LogoutAccount { .. }
        | ClientRequest::MarketplaceAdd { .. }
        | ClientRequest::MarketplaceRemove { .. }
        | ClientRequest::MarketplaceUpgrade { .. }
        | ClientRequest::McpResourceRead { .. }
        | ClientRequest::McpServerEventStreamStart { .. }
        | ClientRequest::McpServerOauthLogin { .. }
        | ClientRequest::McpServerRefresh { .. }
        | ClientRequest::McpServerStatusList { .. }
        | ClientRequest::McpServerToolCall { .. }
        | ClientRequest::ModelList { .. }
        | ClientRequest::PluginInstall { .. }
        | ClientRequest::PluginInstalled { .. }
        | ClientRequest::PluginList { .. }
        | ClientRequest::PluginRead { .. }
        | ClientRequest::PluginReconcile { .. }
        | ClientRequest::PluginSearch { .. }
        | ClientRequest::PluginShareCheckout { .. }
        | ClientRequest::PluginShareDelete { .. }
        | ClientRequest::PluginShareList { .. }
        | ClientRequest::PluginShareSave { .. }
        | ClientRequest::PluginShareUpdateTargets { .. }
        | ClientRequest::PluginSkillRead { .. }
        | ClientRequest::PluginUninstall { .. }
        | ClientRequest::RemoteControlClientsList { .. }
        | ClientRequest::RemoteControlClientsRevoke { .. }
        | ClientRequest::RemoteControlDisable { .. }
        | ClientRequest::RemoteControlEnable { .. }
        | ClientRequest::RemoteControlPairingStart { .. }
        | ClientRequest::RemoteControlPairingStatus { .. }
        | ClientRequest::ReviewStart { .. }
        | ClientRequest::SendAddCreditsNudgeEmail { .. }
        | ClientRequest::SkillsConfigWrite { .. }
        | ClientRequest::SkillsExtraRootsSet { .. }
        | ClientRequest::SkillsList { .. }
        | ClientRequest::ThreadApproveGuardianDeniedAction { .. }
        | ClientRequest::ThreadArchive { .. }
        | ClientRequest::ThreadBackgroundTerminalsClean { .. }
        | ClientRequest::ThreadBackgroundTerminalsTerminate { .. }
        | ClientRequest::ThreadCompactStart { .. }
        | ClientRequest::ThreadDelete { .. }
        | ClientRequest::ThreadFork { .. }
        | ClientRequest::ThreadGuardianReview { .. }
        | ClientRequest::ThreadInjectItems { .. }
        | ClientRequest::ThreadPrepareApprovedPlan { .. }
        | ClientRequest::ThreadQueueAdd { .. }
        | ClientRequest::ThreadQueueStart { .. }
        | ClientRequest::ThreadRealtimeAppendAudio { .. }
        | ClientRequest::ThreadRealtimeAppendSpeech { .. }
        | ClientRequest::ThreadRealtimeAppendText { .. }
        | ClientRequest::ThreadRealtimeStart { .. }
        | ClientRequest::ThreadRealtimeStop { .. }
        | ClientRequest::ThreadResume { .. }
        | ClientRequest::ThreadRevert { .. }
        | ClientRequest::ThreadSample { .. }
        | ClientRequest::ThreadShellCommand { .. }
        | ClientRequest::ThreadStart { .. }
        | ClientRequest::ThreadUnarchive { .. }
        | ClientRequest::ThreadUnsubscribe { .. }
        | ClientRequest::TurnInterrupt { .. }
        | ClientRequest::TurnStart { .. }
        | ClientRequest::TurnStartIfIdle { .. }
        | ClientRequest::TurnSteer { .. }
        | ClientRequest::UserVerificationDelete { .. }
        | ClientRequest::UserVerificationEnroll { .. }
        | ClientRequest::UserVerificationStatus { .. }
        | ClientRequest::UserVerificationVerify { .. } => AccountDependency::Permit,
        ClientRequest::ManagedTransitionStart { .. }
        | ClientRequest::ManagedTransitionRead { .. }
        | ClientRequest::ManagedTransitionCancel { .. }
        | ClientRequest::BedrockDiscover { .. }
        | ClientRequest::CancelLoginAccount { .. }
        | ClientRequest::CollaborationModeList { .. }
        | ClientRequest::CommandExecResize { .. }
        | ClientRequest::CommandExecTerminate { .. }
        | ClientRequest::CommandExecWrite { .. }
        | ClientRequest::ConfigBatchWrite { .. }
        | ClientRequest::ConfigRead { .. }
        | ClientRequest::ConfigRequirementsRead { .. }
        | ClientRequest::ConfigValueWrite { .. }
        | ClientRequest::EnvironmentAdd { .. }
        | ClientRequest::EnvironmentInfo { .. }
        | ClientRequest::EnvironmentStatus { .. }
        | ClientRequest::ExperimentalFeatureEnablementSet { .. }
        | ClientRequest::ExperimentalFeatureList { .. }
        | ClientRequest::ExternalAgentConfigDetect { .. }
        | ClientRequest::ExternalAgentConfigImport { .. }
        | ClientRequest::ExternalAgentConfigImportHistoriesRead { .. }
        | ClientRequest::ExternalAgentConfigImportHistoryRecord { .. }
        | ClientRequest::FeedbackSave { .. }
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
        | ClientRequest::GatewayOAuthCancel { .. }
        | ClientRequest::GitDiffToRemote { .. }
        | ClientRequest::Initialize { .. }
        | ClientRequest::McpServerEventStreamStop { .. }
        | ClientRequest::MemoryReset { .. }
        | ClientRequest::MemoryStatus { .. }
        | ClientRequest::MockExperimentalMethod { .. }
        | ClientRequest::ModelProviderCapabilitiesRead { .. }
        | ClientRequest::ModelProviderConfigWrite { .. }
        | ClientRequest::ModelProviderList { .. }
        | ClientRequest::OneOffCommandExec { .. }
        | ClientRequest::PermissionProfileList { .. }
        | ClientRequest::ProcessKill { .. }
        | ClientRequest::ProcessResizePty { .. }
        | ClientRequest::ProcessSpawn { .. }
        | ClientRequest::ProcessWriteStdin { .. }
        | ClientRequest::ProjectCreate { .. }
        | ClientRequest::ProjectDelete { .. }
        | ClientRequest::ProjectImport { .. }
        | ClientRequest::ProjectList { .. }
        | ClientRequest::ProjectMove { .. }
        | ClientRequest::ProjectRead { .. }
        | ClientRequest::ProjectUpdate { .. }
        | ClientRequest::RemoteControlStatusRead { .. }
        | ClientRequest::RolloutCompress { .. }
        | ClientRequest::ServerDiagnostics { .. }
        | ClientRequest::ThreadAttachmentAdd { .. }
        | ClientRequest::ThreadAttachmentList { .. }
        | ClientRequest::ThreadAttachmentOwnerList { .. }
        | ClientRequest::ThreadAttachmentRemove { .. }
        | ClientRequest::ThreadBackgroundTerminalsList { .. }
        | ClientRequest::ThreadDecrementElicitation { .. }
        | ClientRequest::ThreadGoalClear { .. }
        | ClientRequest::ThreadGoalGet { .. }
        | ClientRequest::ThreadGoalSet { .. }
        | ClientRequest::ThreadIncrementElicitation { .. }
        | ClientRequest::ThreadItemsList { .. }
        | ClientRequest::ThreadItemsRead { .. }
        | ClientRequest::ThreadList { .. }
        | ClientRequest::ThreadLoadedList { .. }
        | ClientRequest::ThreadMemoryModeSet { .. }
        | ClientRequest::ThreadMemoryRead { .. }
        | ClientRequest::ThreadMetadataUpdate { .. }
        | ClientRequest::ThreadPredictionRequest { .. }
        | ClientRequest::ThreadQueueDelete { .. }
        | ClientRequest::ThreadQueueList { .. }
        | ClientRequest::ThreadQueueReorder { .. }
        | ClientRequest::ThreadQueueUpdate { .. }
        | ClientRequest::ThreadRead { .. }
        | ClientRequest::ThreadReadStateUpdate { .. }
        | ClientRequest::ThreadRealtimeListVoices { .. }
        | ClientRequest::ThreadSampleCancel { .. }
        | ClientRequest::ThreadSearch { .. }
        | ClientRequest::ThreadSearchOccurrences { .. }
        | ClientRequest::ThreadSectionCreate { .. }
        | ClientRequest::ThreadSectionDelete { .. }
        | ClientRequest::ThreadSectionList { .. }
        | ClientRequest::ThreadSectionMove { .. }
        | ClientRequest::ThreadSectionUpdate { .. }
        | ClientRequest::ThreadSetName { .. }
        | ClientRequest::ThreadSettingsUpdate { .. }
        | ClientRequest::ThreadTimelineList { .. }
        | ClientRequest::ThreadTurnsList { .. }
        | ClientRequest::TurnSettingsUpdate { .. }
        | ClientRequest::UserVerificationCancel { .. }
        | ClientRequest::WindowsSandboxReadiness { .. }
        | ClientRequest::WindowsSandboxSetupStart { .. } => AccountDependency::Independent,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_app_server_protocol::ExperimentalApi;

    fn request(method: &str, params: serde_json::Value) -> ClientRequest {
        let wire: codex_app_server_protocol::JSONRPCRequest =
            serde_json::from_value(serde_json::json!({"id":1, "method":method, "params":params}))
                .unwrap();
        ClientRequest::try_from(wire).unwrap()
    }

    #[test]
    fn explicit_account_cache_observations_remain_independent() {
        for refresh in [false, true] {
            let request = request(
                "account/read",
                serde_json::json!({"cacheOnly":true,"refreshToken":refresh}),
            );
            // An inconsistent cache-only/refresh combination is rejected by
            // the handler without needing permission to refresh anything.
            assert_eq!(classify(&request), AccountDependency::Independent);
        }
        assert_eq!(
            classify(&request("account/read", serde_json::json!({}))),
            AccountDependency::Permit
        );
    }

    #[test]
    fn session_list_refresh_is_distinct_from_cached_status() {
        for refresh in [false, true] {
            let actual = request(
                "accountSession/list",
                serde_json::json!({"refreshWorkspaceMetadata":refresh}),
            );
            assert_eq!(
                classify(&actual),
                if refresh {
                    AccountDependency::Permit
                } else {
                    AccountDependency::Independent
                }
            );
        }
    }

    #[test]
    fn credential_and_online_catalog_reads_are_counted_before_effects() {
        for (method, params) in [
            ("getAuthStatus", serde_json::json!({})),
            ("account/rateLimits/read", serde_json::json!({})),
            ("model/list", serde_json::json!({})),
            ("model/list", serde_json::json!({"modelProvider":"other"})),
        ] {
            assert_eq!(
                classify(&request(method, params)),
                AccountDependency::Permit,
                "{method}"
            );
        }
    }

    #[test]
    fn gateway_cancellation_does_not_acquire_fresh_account_work() {
        assert_eq!(
            classify(&request(
                "account/gatewayOAuth/cancel",
                serde_json::json!({})
            )),
            AccountDependency::Independent
        );
    }
    #[test]
    fn transition_controls_remain_independent_and_unserialized() {
        for (method, mut params) in [
            (
                "start",
                serde_json::json!({
                    "intent":"adoptManagedLogout", "expectedAuthRevision":0,
                    "expectedTransitionRevision":0, "expectedAuthFingerprint":null,
                    "intendedResultAuthFingerprint":null
                }),
            ),
            ("read", serde_json::json!({})),
            ("cancel", serde_json::json!({})),
        ] {
            params["contractVersion"] = serde_json::json!(1);
            params["transitionId"] = serde_json::json!("test");
            params["processInstanceId"] = serde_json::json!("test");
            let request = request(&format!("account/managedAuthTransition/{method}"), params);
            assert_eq!(classify(&request), AccountDependency::Independent);
            assert!(request.serialization_scope().is_none());
            assert_eq!(
                request.experimental_reason(),
                Some("account/managedAuthTransition")
            );
        }
    }
}
