//! Retained command credential envelope. Process selection and renewal wiring
//! remain a separate required port; this module does not invoke a command.
use super::CodexAuth;
use serde::Deserialize;
use serde::Serialize;
use sha2::Digest;
use std::io;

#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Envelope {
    pub version: u32,
    pub source: String,
    pub binding: String,
    pub account: String,
    pub principal: String,
    pub generation: u64,
    pub access_token: String,
    pub expires_at: i64,
    pub fingerprint: String,
}

impl Envelope {
    fn usable(&self) -> bool {
        self.expires_at > chrono::Utc::now().timestamp()
    }

    pub(super) fn auth(&self) -> io::Result<CodexAuth> {
        let fingerprint = format!("{:x}", sha2::Sha256::digest(self.access_token.as_bytes()));
        if self.version != 1
            || self.generation == 0
            || [&self.source, &self.binding, &self.account, &self.principal]
                .iter()
                .any(|value| value.trim().is_empty())
            || self.fingerprint != fingerprint
            || !self.usable()
        {
            return Err(io::Error::other("invalid or expired command credentials"));
        }
        let expiration = crate::token_data::parse_jwt_expiration(&self.access_token)
            .map_err(|_| io::Error::other("invalid credential expiry"))?;
        if expiration.map(|value| value.timestamp()) != Some(self.expires_at) {
            return Err(io::Error::other("command expiry mismatch"));
        }
        let claims = crate::token_data::parse_chatgpt_jwt_claims(&self.access_token)
            .map_err(|_| io::Error::other("invalid credential claims"))?;
        if claims.chatgpt_account_id.as_deref() != Some(self.account.as_str())
            || claims.chatgpt_user_id.as_deref() != Some(self.principal.as_str())
        {
            return Err(io::Error::other("command principal mismatch"));
        }
        CodexAuth::from_external_chatgpt_tokens(
            &self.access_token,
            &self.account,
            /*chatgpt_plan_type*/ None,
        )
        .map_err(|_| io::Error::other("invalid external credential"))
    }
}
