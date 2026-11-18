#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ProcessState {
    pub(crate) has_exited: bool,
    pub(crate) exit_code: Option<i32>,
    pub(crate) failure_message: Option<String>,
    pub(crate) sandbox_denied: bool,
    pub(crate) sandbox_cleanup: Option<codex_protocol::exec_output::SandboxCleanupSummary>,
}

impl ProcessState {
    pub(crate) fn exited(&self, exit_code: Option<i32>) -> Self {
        Self {
            has_exited: true,
            exit_code,
            failure_message: self.failure_message.clone(),
            sandbox_denied: self.sandbox_denied,
            sandbox_cleanup: self.sandbox_cleanup,
        }
    }

    pub(crate) fn failed(&self, message: String) -> Self {
        Self {
            has_exited: true,
            exit_code: self.exit_code,
            failure_message: Some(message),
            sandbox_denied: self.sandbox_denied,
            sandbox_cleanup: self.sandbox_cleanup,
        }
    }
}
