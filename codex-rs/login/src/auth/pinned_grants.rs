//! Preserve upstream account-grant eligibility under an admitted source owner.
use super::owner_pin::OwnerPin;
use super::*;

impl OwnerPin {
    /// Verify authority at the physical read boundary; never resolve a provider
    /// or accept another cached/durable login while an observer is suspended.
    pub(super) async fn read_bound_persisted_tokens(
        self: &Arc<Self>,
    ) -> std::io::Result<Option<TokenData>> {
        self.cached_auth().map_err(std::io::Error::other)?;
        if self.renewal.is_host()
            || self.source.auth_credentials_store_mode == AuthCredentialsStoreMode::Ephemeral
        {
            return Ok(None);
        }
        let (auth, expected) = self.snapshot().map_err(std::io::Error::other)?;
        let Some(auth @ CodexAuth::Chatgpt(_)) = auth else {
            return Ok(None);
        };
        let tokens = auth.get_token_data()?;
        if tokens.login_owner_id.as_ref().is_none_or(String::is_empty) {
            return Ok(None);
        }
        let pin = Arc::clone(self);
        let originator = codex_otel::auth_storage::AuthStorageOriginator::current();
        let stored = codex_protocol::host_auth_task::run_blocking_call(move || {
            originator.sync_scope(|| -> std::io::Result<Option<TokenData>> {
                // In-process native commit -> external authority -> cached owner.
                // Cross-process changes are observed at each typed read boundary;
                // they are not continuously excluded by this manager's lease.
                let _commit = auth_storage_commit_guard()?;
                let authority = pin
                    .source
                    .external_auth
                    .read()
                    .map_err(|_| std::io::Error::other("account authority unavailable"))?;
                if !pin.renewal.matches_provider(&authority) {
                    return Ok(None);
                }
                let cached = pin
                    .source
                    .inner
                    .read()
                    .map_err(|_| std::io::Error::other("account cache unavailable"))?;
                if pin.source.owner_snapshot_locked(&cached) != expected {
                    return Ok(None);
                }
                let stored = create_auth_storage(
                    pin.source.codex_home.clone(),
                    pin.source.auth_credentials_store_mode,
                    pin.source.keyring_backend_kind,
                )
                .load_account_grant()
                .map_err(|_| std::io::Error::other("Account credential storage is unavailable."))?;
                Ok(stored
                    .filter(|auth| auth.resolved_mode() == AuthMode::Chatgpt)
                    .and_then(|auth| auth.tokens)
                    .filter(|stored| account_grant::same_account_login(&tokens, stored)))
            })
        })
        .await
        .map_err(|_| std::io::Error::other("Account credential read custody unavailable."))??;
        self.cached_auth().map_err(std::io::Error::other)?;
        Ok(stored)
    }

    pub(super) async fn read_bound_account_grant_tokens(
        self: &Arc<Self>,
    ) -> std::io::Result<Option<TokenData>> {
        let stored = self.read_bound_persisted_tokens().await?;
        let Some(auth @ CodexAuth::Chatgpt(_)) =
            self.cached_auth().map_err(std::io::Error::other)?
        else {
            return Ok(None);
        };
        let cached = auth.get_token_data()?;
        Ok(stored.filter(|tokens| account_grant::eligible_account_grant(tokens, &cached)))
    }

    pub(super) async fn account_login_is_current(
        self: &Arc<Self>,
        expected: &CodexAuth,
    ) -> std::io::Result<bool> {
        let Some(current) = self.cached_auth().map_err(std::io::Error::other)? else {
            return Ok(false);
        };
        if !matches!(expected, CodexAuth::Chatgpt(_)) {
            return Ok(AuthManager::auths_equal_for_refresh(
                Some(&current),
                Some(expected),
            ));
        }
        let expected = expected.get_token_data()?;
        if expected
            .login_owner_id
            .as_ref()
            .is_none_or(String::is_empty)
            || !matches!(current, CodexAuth::Chatgpt(_))
            || !account_grant::same_account_login(&expected, &current.get_token_data()?)
        {
            return Ok(false);
        }
        let stored = self.read_bound_persisted_tokens().await?;
        Ok(stored
            .as_ref()
            .is_some_and(|stored| account_grant::same_account_login(&expected, stored)))
    }
}
