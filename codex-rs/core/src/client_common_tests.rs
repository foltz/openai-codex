use codex_api::OpenAiVerbosity;
use codex_api::ResponsesApiRequest;
use codex_api::TextControls;
use codex_api::create_text_param_for_request;
use codex_models_manager::model_info::model_info_from_slug;
use codex_protocol::config_types::ServiceTier;
use codex_protocol::models::FunctionCallOutputPayload;
use codex_protocol::models::ImageDetail;
use codex_protocol::models::ImageReference;
use pretty_assertions::assert_eq;
use serde_json::value::RawValue;
use std::sync::Arc;

use super::*;

fn empty_tools() -> Arc<RawValue> {
    Arc::from(RawValue::from_string("[]".to_string()).expect("valid tool JSON"))
}

fn prompt_with_image_outputs(detail: Option<ImageDetail>) -> Prompt {
    Prompt {
        input: vec![
            ResponseItem::Message {
                status: None,
                encrypted_content: None,
                id: None,
                role: "user".to_string(),
                content: vec![
                    ContentItem::InputText {
                        text: "Describe this image.".to_string(),
                    },
                    ContentItem::InputImage {
                        image: ImageReference::Inline {
                            image_url: "https://example.com/image.png".to_string(),
                        },
                        detail,
                    },
                ],
                phase: None,
                internal_chat_message_metadata_passthrough: None,
            },
            ResponseItem::FunctionCallOutput {
                id: None,
                call_id: Some("function-call".to_string()),
                name: None,
                namespace: None,
                output: FunctionCallOutputPayload::from_content_items(vec![
                    FunctionCallOutputContentItem::InputImage {
                        image: ImageReference::Inline {
                            image_url: "data:image/png;base64,function".to_string(),
                        },
                        detail,
                    },
                ]),
                internal_chat_message_metadata_passthrough: None,
            },
            ResponseItem::CustomToolCallOutput {
                id: None,
                call_id: "custom-call".to_string(),
                name: None,
                output: FunctionCallOutputPayload::from_content_items(vec![
                    FunctionCallOutputContentItem::InputImage {
                        image: ImageReference::Inline {
                            image_url: "data:image/png;base64,custom".to_string(),
                        },
                        detail,
                    },
                ]),
                internal_chat_message_metadata_passthrough: None,
            },
        ],
        ..Default::default()
    }
}

#[test_case::test_case(Some(ImageDetail::Original), Some(ImageDetail::High); "original")]
#[test_case::test_case(Some(ImageDetail::High), Some(ImageDetail::High); "high")]
#[test_case::test_case(Some(ImageDetail::Auto), Some(ImageDetail::Auto); "auto")]
#[test_case::test_case(Some(ImageDetail::Low), Some(ImageDetail::Low); "low")]
#[test_case::test_case(None, None; "unspecified")]
fn request_copies_project_image_details_for_receiving_model(
    detail: Option<ImageDetail>,
    unsupported_detail: Option<ImageDetail>,
) {
    let prompt = prompt_with_image_outputs(detail);
    let original = prompt.input.clone();
    let mut model_info = model_info_from_slug("gpt-5.4");
    model_info.use_responses_lite = false;
    model_info.supports_image_detail_original = true;
    assert_eq!(
        prompt.get_formatted_input_for_request(&model_info),
        original
    );

    model_info.supports_image_detail_original = false;
    assert_eq!(
        prompt.get_formatted_input_for_request(&model_info),
        prompt_with_image_outputs(unsupported_detail).input
    );

    model_info.use_responses_lite = true;
    for supports_original in [false, true] {
        model_info.supports_image_detail_original = supports_original;
        assert_eq!(
            prompt.get_formatted_input_for_request(&model_info),
            prompt_with_image_outputs(/*detail*/ None).input
        );
    }
    assert_eq!(prompt.input, original);
}

#[test]
fn serializes_text_verbosity_when_set() {
    let input: Vec<ResponseItem> = vec![];
    let req = ResponsesApiRequest {
        model: "gpt-5.4".to_string(),
        input,
        tools: Some(empty_tools().into()),
        tool_choice: "auto".to_string(),
        parallel_tool_calls: true,
        reasoning: None,
        store: false,
        stream: true,
        stream_options: None,
        include: vec![],
        prompt_cache_key: None,
        prompt_cache_options: None,
        service_tier: None,
        text: Some(TextControls {
            verbosity: Some(OpenAiVerbosity::Low),
            format: None,
        }),
        client_metadata: None,
        access_programs: None,
    };

    let v = serde_json::to_value(&req).expect("json");
    assert_eq!(
        v.get("text")
            .and_then(|t| t.get("verbosity"))
            .and_then(|s| s.as_str()),
        Some("low")
    );
}

#[test]
fn serializes_text_schema_with_strict_format() {
    let input: Vec<ResponseItem> = vec![];
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "answer": {"type": "string"}
        },
        "required": ["answer"],
    });
    let text_controls = create_text_param_for_request(
        /*verbosity*/ None,
        &Some(schema.clone()),
        /*output_schema_strict*/ true,
    )
    .expect("text controls");

    let req = ResponsesApiRequest {
        model: "gpt-5.4".to_string(),
        input,
        tools: Some(empty_tools().into()),
        tool_choice: "auto".to_string(),
        parallel_tool_calls: true,
        reasoning: None,
        store: false,
        stream: true,
        stream_options: None,
        include: vec![],
        prompt_cache_key: None,
        prompt_cache_options: None,
        service_tier: None,
        text: Some(text_controls),
        client_metadata: None,
        access_programs: None,
    };

    let v = serde_json::to_value(&req).expect("json");
    let text = v.get("text").expect("text field");
    assert!(text.get("verbosity").is_none());
    let format = text.get("format").expect("format field");

    assert_eq!(
        format.get("name"),
        Some(&serde_json::Value::String("codex_output_schema".into()))
    );
    assert_eq!(
        format.get("type"),
        Some(&serde_json::Value::String("json_schema".into()))
    );
    assert_eq!(format.get("strict"), Some(&serde_json::Value::Bool(true)));
    assert_eq!(format.get("schema"), Some(&schema));
}

#[test]
fn serializes_text_schema_with_non_strict_format() {
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "answer": {"type": "string"},
            "rationale": {"type": "string"}
        },
        "required": ["answer"],
        "additionalProperties": false
    });
    let text_controls = create_text_param_for_request(
        /*verbosity*/ None,
        &Some(schema.clone()),
        /*output_schema_strict*/ false,
    )
    .expect("text controls");

    let format = text_controls.format.expect("format field");
    assert!(!format.strict);
    assert_eq!(format.schema, schema);
}

#[test]
fn omits_text_when_not_set() {
    let input: Vec<ResponseItem> = vec![];
    let req = ResponsesApiRequest {
        model: "gpt-5.4".to_string(),
        input,
        tools: Some(empty_tools().into()),
        tool_choice: "auto".to_string(),
        parallel_tool_calls: true,
        reasoning: None,
        store: false,
        stream: true,
        stream_options: None,
        include: vec![],
        prompt_cache_key: None,
        prompt_cache_options: None,
        service_tier: None,
        text: None,
        client_metadata: None,
        access_programs: None,
    };

    let v = serde_json::to_value(&req).expect("json");
    assert!(v.get("text").is_none());
}

#[test]
fn serializes_flex_service_tier_when_set() {
    let req = ResponsesApiRequest {
        model: "gpt-5.4".to_string(),
        input: vec![],
        tools: Some(empty_tools().into()),
        tool_choice: "auto".to_string(),
        parallel_tool_calls: true,
        reasoning: None,
        store: false,
        stream: true,
        stream_options: None,
        include: vec![],
        prompt_cache_key: None,
        prompt_cache_options: None,
        service_tier: Some(ServiceTier::Flex.to_string()),
        text: None,
        client_metadata: None,
        access_programs: None,
    };

    let v = serde_json::to_value(&req).expect("json");
    assert_eq!(
        v.get("service_tier").and_then(|tier| tier.as_str()),
        Some("flex")
    );
}

fn finish_test_stream(pump: Option<ProviderTaskCompletion>) -> ResponseStream {
    let (_send, rx_event) = mpsc::channel(1);
    ResponseStream {
        routing_affinity: None,
        rx_event,
        interrupt: None,
        consumer_dropped: CancellationToken::new(),
        pump,
    }
}

#[tokio::test]
async fn unmanaged_finish_does_not_change_drop_or_cancellation_semantics() {
    let mut stream = finish_test_stream(None);
    let cancellation = stream.consumer_dropped.clone();
    stream.finish().await.unwrap();
    assert!(!cancellation.is_cancelled());
    drop(stream);
    assert!(cancellation.is_cancelled());
}

#[tokio::test]
async fn cancelled_finish_observer_retains_the_same_original_pump() {
    let (send, receive) = oneshot::channel();
    let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let child_finished = finished.clone();
    let original = tokio::spawn(async move {
        receive.await.unwrap();
        child_finished.store(true, std::sync::atomic::Ordering::SeqCst);
    });
    let observation = Box::pin(async move {
        original
            .await
            .map_err(|_| codex_model_provider::AuthTaskFailure::Unavailable)
    });
    let mut stream = finish_test_stream(Some(ProviderTaskCompletion::Pending(observation)));
    let mut first = Box::pin(stream.finish());
    assert!(futures::poll!(first.as_mut()).is_pending());
    drop(first);
    assert!(stream.consumer_dropped.is_cancelled());
    assert!(!finished.load(std::sync::atomic::Ordering::SeqCst));
    send.send(()).unwrap();
    stream.finish().await.unwrap();
    assert!(finished.load(std::sync::atomic::Ordering::SeqCst));
    stream.finish().await.unwrap();
}

#[tokio::test]
async fn managed_finish_replays_original_panic_as_failure() {
    let original = tokio::spawn(async {
        panic!("synthetic stream pump panic");
    });
    let observation = Box::pin(async move {
        original.await.map_err(|error| {
            assert!(error.is_panic());
            codex_model_provider::AuthTaskFailure::Panicked
        })
    });
    let mut stream = finish_test_stream(Some(ProviderTaskCompletion::Pending(observation)));
    for _ in 0..2 {
        assert_eq!(
            stream.finish().await.unwrap_err().to_string(),
            codex_model_provider::AuthTaskFailure::Panicked
                .error()
                .to_string()
        );
    }
}

#[tokio::test]
async fn managed_finish_releases_a_sender_blocked_after_the_terminal_event() {
    let (send, rx_event) = mpsc::channel(1);
    send.send(Ok(ResponseEvent::OutputTextDelta("queued".into())))
        .await
        .unwrap();
    let original = tokio::spawn(async move {
        assert!(
            send.send(Ok(ResponseEvent::OutputTextDelta("blocked".into())))
                .await
                .is_err()
        );
    });
    let observation = Box::pin(async move {
        original
            .await
            .map_err(|_| codex_model_provider::AuthTaskFailure::Unavailable)
    });
    let mut stream = ResponseStream {
        routing_affinity: None,
        rx_event,
        interrupt: None,
        consumer_dropped: CancellationToken::new(),
        pump: Some(ProviderTaskCompletion::Pending(observation)),
    };
    stream.finish().await.unwrap();
}
