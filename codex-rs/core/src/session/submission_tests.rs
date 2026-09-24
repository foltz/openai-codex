use super::*;
use codex_protocol::protocol::Op;
use pretty_assertions::assert_eq;

fn submission(id: &str) -> Submission {
    Submission {
        id: id.to_owned(),
        op: Op::Shutdown,
        root_turn_id: None,
        trace: None,
        parent_turn_id: None,
    }
}

#[test]
fn contended_idle_commit_refuses_without_running_commit_and_can_retry() {
    let (raw, _receiver) = async_channel::bounded(1);
    let sender = SubmissionSender::from(raw);
    let guard = sender.closed.lock().unwrap();
    assert_eq!(
        sender.with_idle_admission(|| -> Result<(), ()> {
            panic!("contended claim must not commit")
        }),
        Err(IdleAdmissionError::Busy)
    );
    drop(guard);
    assert_eq!(sender.with_idle_admission(|| Ok::<_, ()>(())), Ok(Ok(())));
}

#[tokio::test]
async fn private_shutdown_never_completes_public_work_with_the_same_id() {
    let (raw, receiver) = async_channel::bounded(2);
    let sender = SubmissionSender::from(raw);
    let control = sender.dispatch_control();
    sender.send_shutdown(submission("same-id")).await.unwrap();
    sender.send(submission("same-id")).await.unwrap();
    receiver.recv().await.unwrap();
    drop(control.begin());
    assert_eq!(
        sender.with_idle_admission(|| Ok::<_, ()>(())),
        Err(IdleAdmissionError::Outstanding)
    );
    receiver.recv().await.unwrap();
    drop(control.begin());
    assert_eq!(sender.with_idle_admission(|| Ok::<_, ()>(())), Ok(Ok(())));
}

#[tokio::test]
async fn cancelled_dispatch_releases_its_public_accounting_once() {
    let (raw, receiver) = async_channel::bounded(1);
    let sender = SubmissionSender::from(raw);
    let control = sender.dispatch_control();
    sender.send(submission("cancelled-dispatch")).await.unwrap();
    let (entered, ready) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        receiver.recv().await.unwrap();
        let _dispatch = control.begin();
        entered.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    ready.await.unwrap();
    assert_eq!(
        sender.with_idle_admission(|| Ok::<_, ()>(())),
        Err(IdleAdmissionError::Outstanding)
    );
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(sender.with_idle_admission(|| Ok::<_, ()>(())), Ok(Ok(())));
    assert_eq!(sender.with_idle_admission(|| Ok::<_, ()>(())), Ok(Ok(())));
}

#[tokio::test]
async fn commitment_rejects_pending_public_send_but_allows_private_shutdown() {
    let (raw, receiver) = async_channel::bounded(1);
    let sender = SubmissionSender::from(raw);
    sender.send(submission("before")).await.unwrap();
    let pending = sender.send(submission("rejected"));
    tokio::pin!(pending);
    assert!(futures::poll!(pending.as_mut()).is_pending());
    sender.close_admission();
    assert_eq!(receiver.recv().await.unwrap().id, "before");
    assert_eq!(pending.await, Err(()));
    sender.send_shutdown(submission("internal")).await.unwrap();
    assert_eq!(receiver.recv().await.unwrap().id, "internal");
    assert!(receiver.try_recv().is_err());
    assert_eq!(sender.send(submission("late")).await, Err(()));
}

#[tokio::test]
async fn completed_public_send_linearizes_before_commitment() {
    let (raw, receiver) = async_channel::bounded(2);
    let sender = SubmissionSender::from(raw);
    let clone = sender.clone();
    clone.send(submission("accepted")).await.unwrap();
    sender.close_admission();
    assert_eq!(clone.send(submission("late")).await, Err(()));
    sender.send_shutdown(submission("internal")).await.unwrap();
    assert_eq!(receiver.recv().await.unwrap().id, "accepted");
    assert_eq!(receiver.recv().await.unwrap().id, "internal");
    assert!(receiver.try_recv().is_err());
}

#[tokio::test(start_paused = true)]
async fn accepted_non_task_work_blocks_idle_claim_until_dispatch_finishes() {
    let (raw, receiver) = async_channel::bounded(1);
    let sender = SubmissionSender::from(raw);
    let control = sender.dispatch_control();
    let mut work = submission("non-task");
    work.op = Op::CleanBackgroundTerminals;
    sender.send(work).await.unwrap();
    assert_eq!(
        sender.with_idle_admission(|| Ok::<_, ()>(())),
        Err(IdleAdmissionError::Outstanding)
    );
    let _received = receiver.recv().await.unwrap();
    // Receiving is not dispatch completion: there is deliberately no guard
    // yet, and the queue is empty while the accepted operation remains owed.
    assert!(receiver.is_empty());
    assert_eq!(
        sender.with_idle_admission(|| Ok::<_, ()>(())),
        Err(IdleAdmissionError::Outstanding)
    );
    let dispatch = control.begin();
    assert_eq!(
        sender.with_idle_admission(|| Ok::<_, ()>(())),
        Err(IdleAdmissionError::Outstanding)
    );
    drop(dispatch);
    assert_eq!(sender.with_idle_admission(|| Ok::<_, ()>(())), Ok(Ok(())));
    assert_eq!(sender.send(submission("late")).await, Err(()));
}
