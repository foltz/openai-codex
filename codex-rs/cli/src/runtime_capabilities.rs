//! Token-free CLI schema inventory. Behavioral qualification is separate.
use crate::MultitoolCli;
use clap::CommandFactory;
use std::ffi::OsString;

pub(super) fn report(args: Vec<OsString>) -> Result<serde_json::Value, clap::Error> {
    let command = MultitoolCli::command();
    let matches = command.clone().try_get_matches_from(args)?;
    let parsed_command = matches.subcommand_name().map(str::to_owned);
    let mut has_remote_override = false;
    let mut unsafe_overrides = false;
    let mut level = Some(&matches);
    while let Some(current) = level {
        has_remote_override |= ["remote", "remote_auth_token_env", "exec_server_url"]
            .iter()
            .any(|key| current.try_get_raw(key).ok().flatten().is_some());
        unsafe_overrides |= current
            .try_get_many::<String>("raw_overrides")
            .ok()
            .flatten()
            .is_some_and(|values| {
                values.into_iter().any(|value| {
                    let key = value
                        .split('=')
                        .next()
                        .unwrap_or_default()
                        .trim()
                        .split('.')
                        .next()
                        .unwrap_or_default();
                    matches!(
                        key,
                        "codex_home"
                            | "sqlite_home"
                            | "cli_auth_credentials_store"
                            | "forced_login_method"
                            | "forced_chatgpt_workspace_id"
                            | "managed_auth_policy"
                    )
                })
            });
        level = current.subcommand().map(|(_, values)| values);
    }
    let commands: Vec<_> = command.get_subcommands().map(|subcommand| serde_json::json!({"name":subcommand.get_name(), "aliases":subcommand.get_all_aliases().collect::<Vec<_>>()})).collect();
    let options: Vec<_> = command.get_arguments().map(|argument| serde_json::json!({"long":argument.get_long(),"short":argument.get_short(),"value":argument.get_action().takes_values()})).collect();
    Ok(
        serde_json::json!({"version":1,"capabilities":["cache-auth-observation","command-chatgpt-auth","same-image-unix-peer"],"commands":commands,"options":options,"features":codex_features::FEATURES.iter().map(|feature|feature.key).collect::<Vec<_>>(),"parsedCommand":parsed_command,"hasRemoteOverride":has_remote_override,"unsafeOverrides":unsafe_overrides}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    fn inventory(args: &[&str]) -> serde_json::Value {
        report(args.iter().map(OsString::from).collect()).unwrap()
    }
    #[test]
    fn inventory_is_closed_metadata_and_includes_managed_command() {
        let value = inventory(&["codex", "--runtime-capabilities"]);
        assert_eq!(value["version"], 1);
        assert_eq!(
            value["capabilities"],
            serde_json::json!([
                "cache-auth-observation",
                "command-chatgpt-auth",
                "same-image-unix-peer"
            ])
        );
        assert!(
            value["commands"]
                .as_array()
                .unwrap()
                .iter()
                .any(|row| row["name"] == "__managed-auth-transition")
        );
        assert_eq!(value["unsafeOverrides"], false);
        assert_eq!(value["hasRemoteOverride"], false);
    }
    #[test]
    fn inventory_reports_unsafe_config_without_exposing_values() {
        let value = inventory(&[
            "codex",
            "--runtime-capabilities",
            "-c",
            "codex_home='/synthetic-secret'",
        ]);
        assert_eq!(value["unsafeOverrides"], true);
        assert!(!value.to_string().contains("synthetic-secret"));
    }
}
