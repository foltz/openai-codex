//! Non-serialized host custody for finite provider auth tasks.
//! Implementations own registration, counted work and the original join.
use std::future::Future;
use std::pin::Pin;

pub type AuthTaskFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
pub type AuthTaskObservation =
    Pin<Box<dyn Future<Output = std::result::Result<(), AuthTaskFailure>> + Send + 'static>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthTaskFailure {
    Refused,
    Unavailable,
    Cancelled,
    Panicked,
}
impl AuthTaskFailure {
    pub fn error(self) -> crate::error::CodexErr {
        crate::error::CodexErr::Fatal(format!("provider auth task custody failed: {self:?}"))
    }
}

/// Must capture/derive before dispatch and retain the original join independently of observers.
/// The future owns no credential output: its private caller channel owns that result.
pub trait AuthTaskSpawner: std::fmt::Debug + Send + Sync {
    fn spawn(
        &self,
        task: AuthTaskFuture,
    ) -> std::result::Result<AuthTaskObservation, AuthTaskFailure>;
}
