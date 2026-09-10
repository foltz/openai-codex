use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

/// The supervisor acknowledges a generation only after the previous websocket
/// runtime and all of its connection/client workers have stopped.
#[derive(Clone)]
pub(super) struct AuthCycleReset {
    pub(super) requested: Arc<watch::Sender<u64>>,
    pub(super) completed: watch::Receiver<u64>,
}

pub(super) const AUTH_CYCLE_RESET_TIMEOUT: Duration = Duration::from_secs(5);

impl AuthCycleReset {
    pub(super) fn new() -> (Self, watch::Sender<u64>) {
        let (requested, _) = watch::channel(0_u64);
        let (completed_tx, completed) = watch::channel(0_u64);
        (
            Self {
                requested: Arc::new(requested),
                completed,
            },
            completed_tx,
        )
    }

    pub(super) async fn reset(&self) -> io::Result<()> {
        let mut requested_generation = None;
        self.requested.send_if_modified(|generation| {
            let Some(next) = generation.checked_add(1) else {
                return false;
            };
            *generation = next;
            requested_generation = Some(next);
            true
        });
        let generation = requested_generation
            .ok_or_else(|| io::Error::other("remote control auth generation exhausted"))?;
        let mut completed = self.completed.clone();
        tokio::time::timeout(AUTH_CYCLE_RESET_TIMEOUT, async {
            loop {
                if *completed.borrow_and_update() >= generation {
                    return Ok(());
                }
                completed.changed().await.map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "remote control stopped before acknowledging auth reset",
                    )
                })?;
            }
        })
        .await
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                "remote control did not retire its prior auth cycle before the deadline",
            )
        })?
    }
}
