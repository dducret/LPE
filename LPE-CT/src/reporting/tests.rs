use super::{
    build_digest_report, default_reporting_settings, filter_quarantine_for_domain,
    filter_quarantine_for_mailbox, load_digest_report, normalize_reporting_settings,
    DigestDomainDefault, DigestUserOverride,
};
use crate::smtp::QuarantineSummary;
use serde_json::Value;
use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

fn temp_dir(name: &str) -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let path = std::env::temp_dir().join(format!("lpe-ct-reporting-{name}-{suffix}"));
    fs::create_dir_all(path.join("policy")).unwrap();
    path
}

fn sample_item(trace_id: &str, mail_from: &str, rcpt_to: &[&str]) -> QuarantineSummary {
    QuarantineSummary {
        trace_id: trace_id.to_string(),
        queue: "quarantine".to_string(),
        direction: "inbound".to_string(),
        status: "quarantined".to_string(),
        received_at: "unix:10".to_string(),
        peer: "203.0.113.10".to_string(),
        helo: "mx.example.test".to_string(),
        mail_from: mail_from.to_string(),
        rcpt_to: rcpt_to.iter().map(|value| (*value).to_string()).collect(),
        subject: "Subject".to_string(),
        internet_message_id: None,
        reason: None,
        spam_score: 5.5,
        security_score: 3.0,
        reputation_score: -2,
        dnsbl_hits: Vec::new(),
        auth_summary: Value::Null,
        magika_summary: None,
        magika_decision: None,
        remote_message_ref: None,
        route_target: None,
        decision_summary: None,
    }
}

#[test]
fn reporting_defaults_are_normalized() {
    let mut settings = default_reporting_settings();
    settings.digest_interval_minutes = 0;
    settings.digest_max_items = 0;
    settings.history_retention_days = 0;
    settings.digest_report_retention_days = 0;
    normalize_reporting_settings(&mut settings);
    assert_eq!(settings.digest_interval_minutes, 360);
    assert_eq!(settings.digest_max_items, 25);
    assert_eq!(settings.history_retention_days, 30);
    assert_eq!(settings.digest_report_retention_days, 14);
    assert!(settings.next_digest_run_at.is_some());
}

#[test]
fn reporting_normalization_deduplicates_domain_defaults_and_overrides() {
    let mut settings = default_reporting_settings();
    settings.domain_defaults = vec![
        DigestDomainDefault {
            domain: "Example.com".to_string(),
            recipients: vec!["Ops@example.com".to_string(), "ops@example.com".to_string()],
        },
        DigestDomainDefault {
            domain: "example.com".to_string(),
            recipients: vec!["audit@example.com".to_string()],
        },
    ];
    settings.user_overrides = vec![
        DigestUserOverride {
            mailbox: "Alice@example.com".to_string(),
            recipient: "Ops@example.com".to_string(),
            enabled: true,
        },
        DigestUserOverride {
            mailbox: "alice@example.com".to_string(),
            recipient: "ops@example.com".to_string(),
            enabled: false,
        },
    ];

    normalize_reporting_settings(&mut settings);

    assert_eq!(settings.domain_defaults.len(), 1);
    assert_eq!(settings.domain_defaults[0].domain, "example.com");
    assert_eq!(
        settings.domain_defaults[0].recipients,
        vec!["ops@example.com"]
    );
    assert_eq!(settings.user_overrides.len(), 1);
    assert_eq!(settings.user_overrides[0].mailbox, "alice@example.com");
    assert_eq!(settings.user_overrides[0].recipient, "ops@example.com");
    assert!(settings.user_overrides[0].enabled);
}

#[test]
fn domain_filter_matches_sender_and_recipient_domains() {
    let items = vec![
        sample_item("a", "sender@example.com", &["dest@other.test"]),
        sample_item("b", "sender@other.test", &["dest@example.com"]),
        sample_item("c", "sender@else.test", &["dest@else.test"]),
    ];
    let filtered = filter_quarantine_for_domain(&items, "example.com", 25);
    assert_eq!(filtered.len(), 2);
}

#[test]
fn mailbox_filter_matches_sender_and_recipient_mailboxes() {
    let items = vec![
        sample_item("a", "alice@example.com", &["dest@other.test"]),
        sample_item("b", "sender@other.test", &["alice@example.com"]),
        sample_item("c", "sender@else.test", &["dest@else.test"]),
    ];
    let filtered = filter_quarantine_for_mailbox(&items, "alice@example.com", 25);
    assert_eq!(filtered.len(), 2);
}

#[test]
fn digest_report_enriches_status_and_domain_counts_and_persists_artifact() {
    let spool = temp_dir("digest");
    let mut inbound = sample_item("a", "alice@example.com", &["dest@other.test"]);
    inbound.reason = Some("dmarc reject".to_string());
    inbound.status = "quarantined".to_string();
    inbound.received_at = "unix:20".to_string();

    let mut outbound = sample_item("b", "sender@other.test", &["dest@example.com"]);
    outbound.direction = "outbound".to_string();
    outbound.status = "failed".to_string();
    outbound.received_at = "unix:30".to_string();
    outbound.reason = Some("blocked attachment".to_string());

    let detail = build_digest_report(
        &spool,
        "unix:40",
        "domain-default",
        "example.com",
        "ops@example.com",
        vec![outbound.clone(), inbound.clone()],
    )
    .unwrap();

    assert_eq!(detail.summary.item_count, 2);
    assert_eq!(detail.summary.inbound_count, 1);
    assert_eq!(detail.summary.outbound_count, 1);
    assert!(detail
        .status_counts
        .iter()
        .any(|entry| entry.key == "failed" && entry.count == 1));
    assert!(detail
        .status_counts
        .iter()
        .any(|entry| entry.key == "quarantined" && entry.count == 1));
    assert!(detail
        .domain_counts
        .iter()
        .any(|entry| entry.key == "example.com"));

    let persisted = load_digest_report(&spool, &detail.summary.report_id)
        .unwrap()
        .unwrap();
    assert_eq!(persisted.summary.report_id, detail.summary.report_id);
    assert_eq!(persisted.items.len(), 2);
}
