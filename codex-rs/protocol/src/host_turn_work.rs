//! Non-serialized host custody carried with an in-process submission.

/// A host's independently counted submission. Core binds the generated turn
/// ID before executing the submission, so nested work can derive authority
/// before the caller receives its reply. Rejection drops the lease; a started
/// turn transfers it to host-owned terminal/loop evidence instead.
///
/// The queued operation owns this value. Cancelling a caller after enqueue
/// cannot release account work that Core may still start. Implementations must
/// not expose credentials in Debug or treat request completion as turn cleanup.
pub trait HostTurnWork: std::fmt::Debug + Send {
    fn bind_submission(&mut self, turn_id: &str);
    fn retain_until_terminal(self: Box<Self>);
}

/// Legacy task-start operations carried with the same in-process custody as
/// regular turn input. No wire representation or alternate request is added.
#[derive(Debug)]
pub enum HostTurnAction {
    Compact,
    Review(crate::protocol::ReviewRequest),
}
