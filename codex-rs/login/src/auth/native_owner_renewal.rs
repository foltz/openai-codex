//! Native owner-bound refresh: captured request, locked CAS and cache publication.
use super::super::AuthOwnerSnapshot;
use super::*;

impl AuthManager {
    pub(super) async fn refresh_native_for_owner(
        self: &Arc<Self>,
        expected: &AuthOwnerSnapshot,
        renewal: &AuthRenewalBinding,
        reason: RefreshReason,
    ) -> Result<(), RefreshTokenError> {
        let refused = || permanent_external_auth_error("native renewal owner or source changed");
        let refresh_guard = Arc::new(
            self.refresh_lock
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| refused())?,
        );
        let (attempted, revisions, live) = self.renewal_snapshot().ok_or_else(refused)?;
        if revisions != expected.revisions || live != *renewal || renewal.is_host() {
            return Err(refused());
        }
        let attempted = attempted.ok_or_else(refused)?;
        if attempted.api_auth_mode() != expected.auth_mode.ok_or_else(refused)?
            || attempted.get_account_id() != expected.account_id
            || attempted.get_chatgpt_user_id() != expected.chatgpt_user_id
        {
            return Err(refused());
        }
        let CodexAuth::Chatgpt(auth) = attempted else {
            return Ok(());
        };
        if let Some(error) = self.refresh_failure_for_auth(&CodexAuth::Chatgpt(auth.clone())) {
            return Err(RefreshTokenError::Permanent(error));
        }
        let tokens = auth.current_token_data().ok_or_else(refused)?;
        let endpoint = refresh_token_endpoint(tokens.oauth_provenance.as_ref());
        let client_id = tokens
            .oauth_provenance
            .as_ref()
            .map(|p| p.client_id.clone())
            .unwrap_or_else(oauth_client_id);
        let previous_unsettled = self
            .unsettled_chatgpt_refresh
            .lock()
            .map_err(|_| std::io::Error::other("Refresh progress is unavailable."))?
            .replace(tokens.clone());
        let result: Result<(), RefreshTokenError> = async {
            // Never reload/select another owner before this request.
            let response = request_chatgpt_token_refresh(
                tokens.refresh_token.clone(),
                &endpoint,
                &client_id,
                auth.client(),
                reason,
            )
            .await?;
            let provenance = tokens
                .oauth_provenance
                .clone()
                .filter(|p| endpoint == format!("{}/oauth/token", p.issuer.trim_end_matches('/')));
            let manager = Arc::clone(self);
            let expected = expected.clone();
            let storage = Arc::clone(auth.storage());
            let old_client = auth.client().clone();
            let originator = codex_otel::auth_storage::AuthStorageOriginator::current();
            let persistence_guard = Arc::clone(&refresh_guard);
            codex_protocol::host_auth_task::run_blocking_call(move || {
                let _refresh_guard = persistence_guard;
                originator.sync_scope(|| {
                    persist_tokens_with_commit(
                        &storage,
                        &tokens,
                        provenance,
                        response,
                        |original, updated| {
                            // Native writer lock already held. Keep selected-authority
                            // and cache custody through compare-and-save and publication.
                            let slot = manager.external_auth.read().map_err(|_| {
                                std::io::Error::other("renewal authority unavailable")
                            })?;
                            if slot.is_some() {
                                return Err(std::io::Error::other(
                                    "native renewal authority replaced",
                                ));
                            }
                            let mut cached = manager
                                .inner
                                .write()
                                .map_err(|_| std::io::Error::other("auth cache unavailable"))?;
                            if manager.owner_snapshot_locked(&cached) != expected {
                                return Err(std::io::Error::other(
                                    "native renewal admission changed",
                                ));
                            }
                            let renewed = CodexAuth::Chatgpt(ChatgptAuth {
                                state: ChatgptAuthState {
                                    auth_dot_json: Arc::new(Mutex::new(Some(updated.clone()))),
                                    client: old_client.clone(),
                                },
                                storage: Arc::clone(&storage),
                            });
                            if !same_owner(cached.auth.as_ref(), Some(&renewed))
                                || !same_login(cached.auth.as_ref(), Some(&renewed))
                                || !super::super::source::same_source(
                                    cached.auth.as_ref(),
                                    Some(&renewed),
                                )
                            {
                                return Err(std::io::Error::other(
                                    "native renewal identity or grant changed",
                                ));
                            }
                            validate_auth_restrictions(
                                Some(&manager.allowed_login_methods()),
                                manager.effective_chatgpt_workspaces().as_deref(),
                                &renewed,
                            )
                            .map_err(std::io::Error::other)?;
                            storage.compare_and_save(original, updated)?;
                            manager.set_cached_auth_locked(&mut cached, Some(renewed));
                            Ok(())
                        },
                    )
                    .map(|_| ())
                })
            })
            .await
            .map_err(|_| std::io::Error::other("owner-bound persistence task failed"))??;
            Ok(())
        }
        .await;
        // Cancellation leaves attempted-generation bookkeeping intact, as in
        // upstream. Successful persistence settles; known failure restores prior.
        *self
            .unsettled_chatgpt_refresh
            .lock()
            .map_err(|_| std::io::Error::other("Refresh progress is unavailable."))? =
            if result.is_ok() {
                None
            } else {
                previous_unsettled
            };
        result?;
        let current = self.owner_snapshot().ok_or_else(refused)?;
        if current.revisions.owner_generation != expected.revisions.owner_generation
            || current.revisions.source_generation != expected.revisions.source_generation
            || current.revisions.login_generation != expected.revisions.login_generation
        {
            return Err(refused());
        }
        Ok(())
    }
}
