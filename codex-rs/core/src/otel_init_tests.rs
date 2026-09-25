use super::*;
use crate::config::ConfigBuilder;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn preparation_uses_configured_exporter_and_analytics_policy() {
    let home = tempfile::tempdir().unwrap();
    let mut config = ConfigBuilder::default()
        .codex_home(home.path().to_path_buf())
        .build()
        .await
        .unwrap();
    config.otel.exporter = Kind::None;
    config.otel.trace_exporter = Kind::None;
    config.otel.metrics_exporter = Kind::Statsig;
    for (override_enabled, default_enabled, expected_enabled) in [
        (None, false, false),
        (None, true, true),
        (Some(false), true, false),
        (Some(true), false, true),
    ] {
        config.analytics_enabled = override_enabled;
        let settings = provider_settings(
            &config,
            "test-version",
            Some("test-service"),
            default_enabled,
        );
        assert_eq!(
            matches!(settings.metrics_exporter, OtelExporter::Statsig),
            expected_enabled
        );
        assert_eq!(
            (
                settings.service_name.as_str(),
                settings.service_version.as_str()
            ),
            ("test-service", "test-version")
        );
    }
    config.analytics_enabled = Some(false);
    let candidate = prepare_provider(&config, "test-version", Some("test-service"), true).unwrap();
    assert!(
        candidate.begin_retirement().is_none(),
        "disabled preparation owns no exporters"
    );

    config.otel.trace_exporter = Kind::OtlpHttp {
        endpoint: "http://127.0.0.1:9/v1/traces".to_owned(),
        headers: [("x-test-credential".to_owned(), "synthetic-only".to_owned())].into(),
        protocol: Protocol::Binary,
        tls: None,
    };
    let settings = provider_settings(&config, "test-version", Some("test-service"), true);
    let OtelExporter::OtlpHttp {
        endpoint,
        headers,
        protocol,
        tls,
    } = settings.trace_exporter
    else {
        panic!("configured trace exporter");
    };
    assert_eq!(endpoint, "http://127.0.0.1:9/v1/traces");
    assert_eq!(
        headers.get("x-test-credential").map(String::as_str),
        Some("synthetic-only")
    );
    assert!(matches!(protocol, OtelHttpProtocol::Binary));
    assert!(tls.is_none());
}
