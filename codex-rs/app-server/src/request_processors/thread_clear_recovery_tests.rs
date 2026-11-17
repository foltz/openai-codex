use super::*;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn unavailable_storage_proves_support_but_never_absence() {
    assert_eq!(
        read_recovery(None, ThreadClearRecoveryReadParams::default())
            .await
            .unwrap(),
        ThreadClearRecoveryReadResponse {
            contract_version: 1,
            durability: ThreadClearRecoveryDurability::Unavailable,
            observation: ThreadClearRecoveryObservation::Support,
        }
    );
    assert!(
        read_recovery(
            None,
            ThreadClearRecoveryReadParams {
                successor_thread_id: Some(ThreadId::new().to_string()),
            }
        )
        .await
        .is_err()
    );
}

#[test]
fn recovery_validation_refuses_ambiguous_or_unsupported_context() {
    let context = ThreadClearRecoveryContext {
        contract_version: 1,
        predecessor_thread_id: Some(ThreadId::new().to_string()),
    };
    assert!(validate_recovery(&context, Some(ThreadStartSource::Clear), &None).is_ok());
    assert!(validate_recovery(&context, Some(ThreadStartSource::Startup), &None).is_err());
    assert!(
        validate_recovery(
            &context,
            Some(ThreadStartSource::Clear),
            &context.predecessor_thread_id
        )
        .is_err()
    );
    assert!(
        validate_recovery(
            &ThreadClearRecoveryContext {
                contract_version: 2,
                ..context.clone()
            },
            Some(ThreadStartSource::Clear),
            &None
        )
        .is_err()
    );
    assert!(
        validate_recovery(
            &ThreadClearRecoveryContext {
                predecessor_thread_id: Some("invalid".into()),
                ..context
            },
            Some(ThreadStartSource::Clear),
            &None
        )
        .is_err()
    );
}
