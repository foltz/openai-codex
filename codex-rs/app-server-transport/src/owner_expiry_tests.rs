use super::*;
use pretty_assertions::assert_eq;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

struct DropFlag(Arc<AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// (b) work: one unit (send) per second, forever.
async fn sender_loop(sent: Arc<AtomicUsize>, dropped: Arc<AtomicBool>) {
    let _flag = DropFlag(dropped);
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        sent.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test(start_paused = true)]
async fn async_task_stops_within_one_unit_and_hands_back_its_owner() {
    let expired = CancellationToken::new();
    // The external shutdown owner; the task only holds a handle.
    let custody = OwnerCustody::new(expired.clone());
    let expiry = custody.expiry();
    let sent = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let expiry = expiry.clone();
        let work = sender_loop(Arc::clone(&sent), Arc::clone(&dropped));
        async move { expiry.run("outbound_router", work).await }
    });

    tokio::time::sleep(Duration::from_millis(5_500)).await;
    expired.cancel();
    let joined = task.await;

    // Returned cooperatively: not aborted, no output, owner handed back.
    assert!(matches!(joined, Ok(Ok(None))));
    assert_eq!(sent.load(Ordering::SeqCst), 5);
    assert_eq!(expiry.returned_tasks(), vec!["outbound_router"]);
    tokio::time::advance(Duration::from_secs(600)).await;
    tokio::task::yield_now().await;
    assert_eq!(sent.load(Ordering::SeqCst), 5, "no unit after expiry");
    assert!(
        !dropped.load(Ordering::SeqCst),
        "owner retained, not dropped"
    );
    drop(custody);
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test(start_paused = true)]
async fn work_finishing_before_expiry_returns_its_output() {
    let custody = OwnerCustody::new(CancellationToken::new());
    let expiry = custody.expiry();
    let output = expiry
        .run("acceptor", async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            7
        })
        .await;
    assert_eq!(output, Ok(Some(7)));
    assert!(expiry.returned_tasks().is_empty());
}

#[tokio::test(start_paused = true)]
async fn already_expired_work_is_never_polled() {
    let expired = CancellationToken::new();
    expired.cancel();
    let custody = OwnerCustody::new(expired);
    let expiry = custody.expiry();
    let polled = Arc::new(AtomicBool::new(false));
    let observed = Arc::clone(&polled);
    let output = expiry
        .run("connection_cleanup", async move {
            observed.store(true, Ordering::SeqCst);
        })
        .await;
    assert_eq!(output, Ok(None));
    assert!(!polled.load(Ordering::SeqCst));
    assert_eq!(expiry.returned_tasks(), vec!["connection_cleanup"]);
}

#[test]
fn blocking_work_is_reported_incomplete_until_it_actually_finishes() {
    let expiry = OwnerExpiry::never();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let handle = std::thread::spawn(move || {
        // Stands in for a thread blocked in read(2).
        let _ = release_rx.recv();
    });
    expiry.register_blocking("stdin_reader", handle);
    expiry.expired_token().cancel();

    // Expiry does not make blocking work stopped.
    assert_eq!(expiry.incomplete_blocking(), vec!["stdin_reader"]);

    drop(release_tx);
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !expiry.incomplete_blocking().is_empty() && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(expiry.incomplete_blocking().is_empty());
}

#[test]
fn blocking_states_report_incomplete_until_joined_then_the_last_note() {
    let expiry = OwnerExpiry::never();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let (noted_tx, noted_rx) = std::sync::mpsc::channel::<()>();
    let handle = std::thread::spawn({
        let expiry = expiry.clone();
        move || {
            let _ = release_rx.recv();
            expiry.note_blocking("stdout_writer", "cancelled: output abandoned");
            let _ = noted_tx.send(());
        }
    });
    expiry.register_blocking("stdout_writer", handle);
    // Still running: incomplete, whatever it may later note.
    assert_eq!(
        expiry.blocking_states(),
        vec![("stdout_writer", "incomplete")]
    );

    drop(release_tx);
    noted_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("thread noted its end");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !expiry.incomplete_blocking().is_empty() && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(
        expiry.blocking_states(),
        vec![("stdout_writer", "cancelled: output abandoned")]
    );
}

#[test]
fn abandoned_output_counts_saturate_and_ignore_zero() {
    let expiry = OwnerExpiry::never();
    expiry.record_abandoned(0, 0);
    assert_eq!(expiry.abandoned_output(), AbandonedOutput::default());
    expiry.record_abandoned(1, 10);
    expiry.record_abandoned(u64::MAX, u64::MAX);
    assert_eq!(
        expiry.abandoned_output(),
        AbandonedOutput {
            messages: u64::MAX,
            bytes: u64::MAX,
        }
    );
}

#[test]
fn dropped_output_is_abandoned_until_the_recipient_positively_closes() {
    let expiry = OwnerExpiry::never();
    // No positive close (e.g. stdin EOF only, or SIGTERM): abandoned.
    expiry.record_dropped_output(1, 100);
    assert_eq!(
        expiry.abandoned_output(),
        AbandonedOutput {
            messages: 1,
            bytes: 100,
        }
    );
    assert_eq!(
        expiry.undeliverable_after_disconnect(),
        AbandonedOutput::default()
    );

    // EPIPE/POLLHUP: the recipient is gone, so later drops are undeliverable.
    expiry.mark_recipient_closed();
    expiry.record_dropped_output(2, 50);
    assert_eq!(
        expiry.abandoned_output(),
        AbandonedOutput {
            messages: 1,
            bytes: 100,
        }
    );
    assert_eq!(
        expiry.undeliverable_after_disconnect(),
        AbandonedOutput {
            messages: 2,
            bytes: 50,
        }
    );
}

/// (b) work that is pending until its gate opens; counts every poll.
struct Gated {
    polls: Arc<AtomicUsize>,
    gate: tokio::sync::oneshot::Receiver<()>,
}

impl Future for Gated {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        std::pin::Pin::new(&mut self.gate).poll(cx).map(|_| ())
    }
}

#[tokio::test(start_paused = true)]
async fn work_is_never_polled_after_the_deadline_even_before_the_timer_task_runs() {
    // The token is never cancelled: it models the expiry timer task that has
    // not been polled yet although the latched deadline has passed.
    let custody = OwnerCustody::new(CancellationToken::new());
    let expiry = custody.expiry();
    let deadline = expiry
        .arm(Instant::now() + Duration::from_secs(35))
        .expect("the custody owner is live");
    let polls = Arc::new(AtomicUsize::new(0));
    let (open_gate, gate) = tokio::sync::oneshot::channel();
    let mut run = std::pin::pin!(expiry.run(
        "outbound_router",
        Gated {
            polls: Arc::clone(&polls),
            gate,
        },
    ));
    let first = std::future::poll_fn(|cx| std::task::Poll::Ready(run.as_mut().poll(cx))).await;
    assert!(first.is_pending());
    assert_eq!(polls.load(Ordering::SeqCst), 1);

    tokio::time::advance(Duration::from_secs(35)).await;
    assert!(Instant::now() >= deadline);
    // The work is ready to make progress, and the token is still live.
    open_gate.send(()).expect("work still owns its gate");
    assert!(!expiry.expired_token().is_cancelled());
    assert!(expiry.is_expired(), "the clock decides expiry");

    let second = std::future::poll_fn(|cx| std::task::Poll::Ready(run.as_mut().poll(cx))).await;
    assert_eq!(second, std::task::Poll::Ready(Ok(None)));
    assert_eq!(
        polls.load(Ordering::SeqCst),
        1,
        "never polled past the deadline"
    );
    assert_eq!(expiry.returned_tasks(), vec!["outbound_router"]);
}

#[tokio::test(start_paused = true)]
async fn armed_deadline_is_latched_once_and_never_extended() {
    let custody = OwnerCustody::new(CancellationToken::new());
    let expiry = custody.expiry();
    let first = Instant::now() + Duration::from_secs(35);
    let shared = expiry.clone();
    assert_eq!(shared.arm(first), Ok(first));
    // A later arm through any clone never replaces the latched deadline.
    assert_eq!(expiry.arm(first + Duration::from_secs(60)), Ok(first));
    assert_eq!(shared.deadline(), Some(first));
}

#[test]
fn panicked_blocking_thread_is_incomplete_not_finished() {
    let expiry = OwnerExpiry::never();
    let (unwinding_tx, unwinding_rx) = std::sync::mpsc::channel::<()>();
    let handle = std::thread::spawn(move || {
        // Dropped while the panic unwinds: the gate for the test below.
        let _unwinding = unwinding_tx;
        panic!("stdin reader panicked before leaving a note");
    });
    assert!(unwinding_rx.recv().is_err(), "the thread is unwinding");
    // Wait for the real thread end (no sleep): after this an is_finished
    // check alone would have reported the thread as finished.
    while !handle.is_finished() {
        std::thread::yield_now();
    }
    expiry.register_blocking("stdin_reader", handle);

    assert_eq!(expiry.incomplete_blocking(), vec!["stdin_reader"]);
    assert_eq!(expiry.running_blocking(), Vec::<&str>::new());
    assert_eq!(expiry.blocking_states(), vec![("stdin_reader", "panicked")]);
}

#[test]
fn a_note_before_a_panic_is_not_proof_of_a_normal_end() {
    let expiry = OwnerExpiry::never();
    let handle = std::thread::spawn({
        let expiry = expiry.clone();
        move || {
            expiry.note_blocking("stdout_writer", "cancelled: output abandoned");
            panic!("stdout writer panicked after noting");
        }
    });
    while !handle.is_finished() {
        std::thread::yield_now();
    }
    expiry.register_blocking("stdout_writer", handle);

    assert_eq!(expiry.incomplete_blocking(), vec!["stdout_writer"]);
    assert_eq!(
        expiry.blocking_states(),
        vec![("stdout_writer", "panicked")]
    );
}

/// Retained work whose future captures handles to its own owner's custody
/// (as the stdio connection captures `refusal_owner` / `abandoned_sink`).
struct ProbeWork {
    _handles: (OwnerExpiry, OwnerExpiry),
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}

impl Future for ProbeWork {
    type Output = ();

    fn poll(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        std::task::Poll::Pending
    }
}

impl Drop for ProbeWork {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test(start_paused = true)]
async fn retained_work_holding_its_owner_handles_is_released_exactly_once_by_the_owner() {
    let expired = CancellationToken::new();
    let custody = OwnerCustody::new(expired.clone());
    let expiry = custody.expiry();
    let deadline = expiry
        .arm(Instant::now() + Duration::from_secs(35))
        .expect("the custody owner is live");
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let work = ProbeWork {
        _handles: (expiry.clone(), expiry.clone()),
        polls: Arc::clone(&polls),
        drops: Arc::clone(&drops),
    };

    // Polled once before the deadline, then retained unpolled at expiry.
    assert_eq!(expiry.run("stdio_connection", work).await, Ok(None));
    assert_eq!(Instant::now(), deadline);
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert_eq!(expiry.returned_tasks(), vec!["stdio_connection"]);
    assert_eq!(drops.load(Ordering::SeqCst), 0, "retained, not released");

    // No cycle: the custody owner is the only strong owner of the store,
    // even though the retained work holds two handles to it.
    let store = Arc::downgrade(&custody._retained);
    assert_eq!(store.strong_count(), 1);
    tokio::time::advance(Duration::from_secs(600)).await;
    tokio::task::yield_now().await;
    assert_eq!(polls.load(Ordering::SeqCst), 1, "never polled again");

    // Dropping the external owner releases the work exactly once.
    drop(custody);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(store.upgrade().is_none(), "nothing keeps the store alive");
    assert_eq!(polls.load(Ordering::SeqCst), 1, "no poll on release");
    // Handles outlive the owner harmlessly (reporting state only).
    assert_eq!(expiry.returned_tasks(), vec!["stdio_connection"]);
    drop(expiry);
    assert_eq!(drops.load(Ordering::SeqCst), 1, "released exactly once");
}

#[tokio::test(start_paused = true)]
async fn ownerless_handle_refuses_arming_and_driving_bounded_work() {
    let expiry = OwnerExpiry::never();
    let deadline = Instant::now() + Duration::from_secs(35);

    // Arming is refused before anything is armed.
    assert_eq!(expiry.arm(deadline), Err(NoCustodyOwner));
    assert_eq!(expiry.deadline(), None);
    assert!(!expiry.is_expired());

    // Driving is refused before the work is admitted or polled, and the
    // refusal is reported; nothing is silently dropped at a later expiry.
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let work = ProbeWork {
        _handles: (expiry.clone(), expiry.clone()),
        polls: Arc::clone(&polls),
        drops: Arc::clone(&drops),
    };
    assert_eq!(
        expiry.run("stdio_connection", work).await,
        Err(NoCustodyOwner)
    );
    assert_eq!(polls.load(Ordering::SeqCst), 0, "never admitted or driven");
    assert_eq!(drops.load(Ordering::SeqCst), 1, "refused, not retained");
    assert_eq!(expiry.refused_tasks(), vec!["stdio_connection"]);
    assert!(expiry.returned_tasks().is_empty());
}

#[tokio::test(start_paused = true)]
async fn handle_whose_owner_was_dropped_refuses_bounded_work() {
    let custody = OwnerCustody::new(CancellationToken::new());
    let expiry = custody.expiry();
    drop(custody);
    assert_eq!(
        expiry.arm(Instant::now() + Duration::from_secs(35)),
        Err(NoCustodyOwner)
    );
    assert_eq!(
        expiry.run("outbound_router", async {}).await,
        Err(NoCustodyOwner)
    );
    assert_eq!(expiry.refused_tasks(), vec!["outbound_router"]);
}

#[tokio::test(start_paused = true)]
async fn run_nested_inside_retained_work_does_not_keep_its_owner_alive() {
    let custody = OwnerCustody::new(CancellationToken::new());
    let expiry = custody.expiry();
    let deadline = expiry
        .arm(Instant::now() + Duration::from_secs(35))
        .expect("the custody owner is live");
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    // Like a not-admitted refusal running inside the stdio connection: an
    // admitted inner run is still pending when the outer work expires.
    let inner = ProbeWork {
        _handles: (expiry.clone(), expiry.clone()),
        polls: Arc::clone(&polls),
        drops: Arc::clone(&drops),
    };
    let outer = {
        let expiry = expiry.clone();
        async move {
            let _ = expiry.run("not_admitted_refusal", inner).await;
        }
    };
    assert_eq!(expiry.run("stdio_connection", outer).await, Ok(None));
    assert_eq!(Instant::now(), deadline);
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    let store = Arc::downgrade(&custody._retained);
    assert_eq!(
        store.strong_count(),
        1,
        "the nested run holds no strong owner"
    );

    drop(custody);
    assert_eq!(drops.load(Ordering::SeqCst), 1, "released exactly once");
    assert!(store.upgrade().is_none());
    assert_eq!(polls.load(Ordering::SeqCst), 1);
}

/// Counts wake-ups of the driver.
struct WakeCounter(AtomicUsize);

impl std::task::Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

async fn owner_drop_while_pending_stops_the_driver(arm: Option<Duration>) {
    let custody = OwnerCustody::new(CancellationToken::new());
    let expiry = custody.expiry();
    if let Some(far) = arm {
        expiry
            .arm(Instant::now() + far)
            .expect("the custody owner is live");
    }
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let work = ProbeWork {
        _handles: (expiry.clone(), expiry.clone()),
        polls: Arc::clone(&polls),
        drops: Arc::clone(&drops),
    };
    // Driven by hand: no task is spawned.
    let wakes = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let waker = std::task::Waker::from(Arc::clone(&wakes));
    let mut cx = std::task::Context::from_waker(&waker);
    let mut run = std::pin::pin!(expiry.run("stdio_connection", work));
    assert!(run.as_mut().poll(&mut cx).is_pending());
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert_eq!(wakes.0.load(Ordering::SeqCst), 0);

    // The caller drops its custody while the work is still pending.
    drop(custody);
    assert!(
        wakes.0.load(Ordering::SeqCst) > 0,
        "the owner's release wakes the driver"
    );
    assert_eq!(run.as_mut().poll(&mut cx), std::task::Poll::Ready(Ok(None)));
    assert_eq!(polls.load(Ordering::SeqCst), 1, "no further poll");
    assert_eq!(drops.load(Ordering::SeqCst), 1, "released exactly once");
    assert_eq!(expiry.stopped_by_owner_release(), vec!["stdio_connection"]);
    assert!(expiry.returned_tasks().is_empty());
    assert!(expiry.owner_released());
}

#[tokio::test(start_paused = true)]
async fn owner_dropped_while_work_is_pending_without_a_deadline_stops_the_driver() {
    owner_drop_while_pending_stops_the_driver(None).await;
}

#[tokio::test(start_paused = true)]
async fn owner_dropped_while_work_is_pending_before_a_far_deadline_stops_the_driver() {
    owner_drop_while_pending_stops_the_driver(Some(Duration::from_secs(3_600))).await;
}

#[test]
fn blocking_panic_payload_is_released_outside_reporting_lock_and_failure_sticks() {
    struct Reenter {
        expiry: OwnerExpiry,
        released: Arc<AtomicBool>,
        unlocked: Arc<AtomicBool>,
    }
    impl Drop for Reenter {
        fn drop(&mut self) {
            let unlocked = self.expiry.custody.try_lock().is_ok();
            self.unlocked.store(unlocked, Ordering::SeqCst);
            if unlocked {
                assert_eq!(self.expiry.blocking_states(), vec![("panic", "panicked")]);
                self.expiry.note_blocking("panic", "payload released");
            }
            self.released.store(true, Ordering::SeqCst);
        }
    }
    let expiry = OwnerExpiry::never();
    let released = Arc::new(AtomicBool::new(false));
    let unlocked = Arc::new(AtomicBool::new(false));
    let payload = Reenter {
        expiry: expiry.clone(),
        released: released.clone(),
        unlocked: unlocked.clone(),
    };
    let handle = std::thread::spawn(move || std::panic::panic_any(payload));
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !handle.is_finished() && std::time::Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert!(handle.is_finished());
    expiry.register_blocking("panic", handle);
    assert_eq!(expiry.incomplete_blocking(), vec!["panic"]);
    assert!(released.load(Ordering::SeqCst));
    assert!(
        unlocked.load(Ordering::SeqCst),
        "panic payload must not drop under custody lock"
    );
    assert!(expiry.running_blocking().is_empty());
    assert_eq!(expiry.blocking_states(), vec![("panic", "panicked")]);
    assert_eq!(expiry.incomplete_blocking(), vec!["panic"]);
}
