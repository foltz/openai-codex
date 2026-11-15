use super::*;
use pretty_assertions::assert_eq;

#[derive(Debug, Default)]
struct RecordingSink {
    deadlines: Mutex<Vec<Instant>>,
}

impl CleanupDeadlineSink for RecordingSink {
    fn tighten(&self, deadline: Instant) {
        self.deadlines
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(deadline);
    }
}

#[test]
fn a_scope_only_tightens_and_reaches_later_registrations() {
    let scope = CleanupDeadlineScope::new();
    let early: Arc<RecordingSink> = Arc::new(RecordingSink::default());
    let early_sink: Arc<dyn CleanupDeadlineSink> = early.clone();
    scope.register(&early_sink);

    let first = Instant::now() + Duration::from_secs(10);
    scope.tighten(first);
    // A later deadline never relaxes the latched one.
    scope.tighten(first + Duration::from_secs(10));
    assert_eq!(scope.latched(), Some(first));

    let late: Arc<RecordingSink> = Arc::new(RecordingSink::default());
    let late_sink: Arc<dyn CleanupDeadlineSink> = late.clone();
    scope.register(&late_sink);

    assert_eq!(*early.deadlines.lock().expect("lock"), vec![first, first]);
    assert_eq!(*late.deadlines.lock().expect("lock"), vec![first]);
}

#[test]
fn dropped_custodies_are_not_retained_by_the_scope() {
    let scope = CleanupDeadlineScope::new();
    {
        let sink: Arc<dyn CleanupDeadlineSink> = Arc::new(RecordingSink::default());
        scope.register(&sink);
    }
    scope.tighten(Instant::now());
    assert_eq!(scope.state.lock().expect("lock").sinks.len(), 0);
}

#[test]
fn only_the_linux_sandbox_launcher_gets_custody() {
    assert!(
        SandboxCleanupCustody::for_launch(SandboxType::None, None, None)
            .expect("custody")
            .is_none()
    );
    let linux =
        SandboxCleanupCustody::for_launch(SandboxType::LinuxSeccomp, None, None).expect("custody");
    assert_eq!(linux.is_some(), cfg!(target_os = "linux"));
}

#[cfg(target_os = "linux")]
#[test]
fn scope_tightening_reaches_a_live_custody() {
    let scope = CleanupDeadlineScope::new();
    let custody = SandboxCleanupCustody::for_launch(
        SandboxType::LinuxSeccomp,
        Some(Instant::now() + Duration::from_secs(60)),
        Some(&scope),
    )
    .expect("custody")
    .expect("linux custody");
    let retirement = Instant::now() + Duration::from_secs(1);
    scope.tighten(retirement);
    assert_eq!(custody.deadline(), Some(retirement));
}
