//! Queue admission for the account projection coupled to a managed commit.

use codex_app_server_protocol::AccountUpdatedNotification;
use codex_app_server_protocol::ServerNotification;
use tokio::sync::mpsc;
use tokio::time::Instant;

use super::OutgoingEnvelope;
use super::OutgoingMessageSender;
use super::timestamped_server_notification;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AccountProjectionReservationError {
    Closed,
    TimedOut,
}

/// One queue slot, not a socket-write or client-delivery acknowledgement.
/// Dropping an unused reservation releases capacity without publishing.
pub(crate) struct AccountProjectionReservation(mpsc::OwnedPermit<OutgoingEnvelope>);

impl AccountProjectionReservation {
    /// Consume only inside the successful terminal commit, before reopening
    /// account work. No await, callback, or auth lookup occurs here.
    pub(crate) fn publish(self, notification: AccountUpdatedNotification) {
        self.0.send(OutgoingEnvelope::Broadcast {
            message: timestamped_server_notification(ServerNotification::AccountUpdated(
                notification,
            )),
        });
    }
}

impl OutgoingMessageSender {
    /// Reserve outside coordinator/auth locks. Payload and timestamp are bound
    /// only when the terminal commit consumes the reservation.
    pub(crate) async fn reserve_account_projection_until(
        &self,
        deadline: Instant,
    ) -> Result<AccountProjectionReservation, AccountProjectionReservationError> {
        if Instant::now() >= deadline {
            return Err(AccountProjectionReservationError::TimedOut);
        }
        match tokio::time::timeout_at(deadline, self.sender.clone().reserve_owned()).await {
            Ok(Ok(permit)) => Ok(AccountProjectionReservation(permit)),
            Ok(Err(_)) => Err(AccountProjectionReservationError::Closed),
            Err(_) => Err(AccountProjectionReservationError::TimedOut),
        }
    }
}

#[cfg(test)]
#[path = "account_projection_tests.rs"]
mod tests;
