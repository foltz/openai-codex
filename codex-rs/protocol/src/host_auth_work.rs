//! Non-serialized host custody for finite provider auth tasks.
//! Implementations own registration, counted work and the original join.
use std::future::Future;
use std::pin::Pin;

/// A physical synchronous original. Its output stays in a private caller channel.
pub type AuthTaskBlocking = Box<dyn FnOnce() + Send + 'static>;

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
    /// Derive and register BEFORE the physical blocking task is dispatched.
    /// Never implement this as an abortable async wrapper around an unowned
    /// blocking join. Unsupported hosts must refuse without invoking the body.
    fn spawn_blocking(
        &self,
        task: AuthTaskBlocking,
    ) -> std::result::Result<AuthTaskObservation, AuthTaskFailure> {
        drop(task);
        Err(AuthTaskFailure::Unavailable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn legacy_host_refuses_physical_blocking_custody_without_invoking_body() {
        #[derive(Debug)]
        struct Legacy;
        impl AuthTaskSpawner for Legacy {
            fn spawn(&self, _: AuthTaskFuture) -> Result<AuthTaskObservation, AuthTaskFailure> {
                Err(AuthTaskFailure::Refused)
            }
        }
        let polled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let invoked = polled.clone();
        assert_eq!(
            Legacy
                .spawn_blocking(Box::new(
                    move || invoked.store(true, std::sync::atomic::Ordering::SeqCst)
                ))
                .err(),
            Some(AuthTaskFailure::Unavailable)
        );
        assert!(!polled.load(std::sync::atomic::Ordering::SeqCst));
    }
}
