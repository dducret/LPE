use super::{
    apply_authentication_scores, classify_inbound_message, compose_rfc822_message, delete_trace,
    dkim_disposition, dnsbl_query_name, encode_quoted_printable, evaluate_greylisting,
    finalize_policy_decision, handle_smtp_command, handle_smtp_session, initialize_spool,
    load_antivirus_providers, load_bayespam_corpus, load_reputation_score, load_trace_details,
    parse_antivirus_output, parse_peer_ip, persist_message, postfix_style_mail_log_line,
    process_outbound_handoff, receive_message, receive_message_with_validator, release_trace,
    resolve_outbound_route, retry_after_seconds, retry_trace, score_bayespam,
    smtp_starttls_acceptor_for_paths, spf_disposition, stable_key_id, summarize_dkim,
    summarize_dmarc, summarize_spf, train_bayespam, unix_now, update_reputation, write_smtp,
    AcceptedDomainConfig, AntivirusProviderConfig, AntivirusProviderDecision, AuthSummary,
    AuthenticationAssessment, BayesLabel, DecisionTraceEntry, DkimDisposition, FilterAction,
    GreylistEntry, OutboundRoutingRule, OutboundThrottleRule, ParsedSmtpPath, QueuedMessage,
    RuntimeConfig, SmtpCommandOutcome, SmtpPathError, SmtpPathKind, SmtpTransaction,
    SpfDisposition, TransportAuditEvent, TransportDsnReport, TransportRouteDecision,
    TransportTechnicalStatus, TransportThrottleStatus, BAYESPAM_MIN_SCORING_TOKENS,
    DEFAULT_GREYLIST_DELAY_SECONDS, MAX_SMTP_COMMAND_LINE_LEN, MAX_SMTP_RCPT_PER_TRANSACTION,
};
use crate::env_test_lock;
use axum::{routing::post, Json, Router};
use email_auth::{dkim::DkimResult, dmarc::Disposition as DmarcDisposition, spf::SpfResult};
use lpe_domain::{
    InboundDeliveryRequest, InboundDeliveryResponse, OutboundMessageHandoffRequest,
    TransportDeliveryStatus, TransportRecipient,
};
use lpe_magika::{DetectionSource, Detector, MagikaDetection, Validator};
use serde_json::json;
use std::{
    io::{BufReader as StdIoBufReader, Cursor},
    net::IpAddr,
    net::SocketAddr,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context as TaskContext, Poll},
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};
use tokio_rustls::{
    rustls::{pki_types::ServerName, ClientConfig, RootCertStore},
    TlsConnector,
};
use uuid::Uuid;

#[test]
fn postfix_style_mail_log_line_keeps_operator_correlation_fields() {
    let event = TransportAuditEvent {
        timestamp: "unix:1700000000".to_string(),
        trace_id: "trace-123".to_string(),
        direction: "inbound".to_string(),
        queue: "sent".to_string(),
        status: "sent".to_string(),
        peer: "203.0.113.10:25".to_string(),
        mail_from: "sender@example.net".to_string(),
        rcpt_to: vec!["user@example.test".to_string()],
        subject: "hello\r\nbad".to_string(),
        internet_message_id: Some("mid@example.net".to_string()),
        reason: Some("core delivery accepted".to_string()),
        route_target: Some("mx.example.test".to_string()),
        remote_message_ref: Some("250 2.0.0 queued".to_string()),
        spam_score: 0.0,
        security_score: 0.0,
        reputation_score: 0,
        dnsbl_hits: Vec::new(),
        auth_summary: json!({"spf":"pass"}),
        magika_summary: None,
        magika_decision: None,
        technical_status: Some(json!({"detail":"250 2.0.0 ok"})),
        dsn: Some(json!({"status":"2.0.0"})),
        throttle: None,
        message_size_bytes: Some(42),
        decision_trace: Vec::new(),
    };

    let line = postfix_style_mail_log_line(&event);

    assert!(line.contains("lpe-ct/smtp["));
    assert!(line.contains("trace-123:"));
    assert!(line.contains("direction=inbound"));
    assert!(line.contains("status=sent"));
    assert!(line.contains("from=<sender@example.net>"));
    assert!(line.contains("to=<user@example.test>"));
    assert!(line.contains("message-id=<mid@example.net>"));
    assert!(line.contains("dsn=2.0.0"));
    assert!(line.contains("subject=\"hello bad\""));
    assert!(!line.contains('\n'));
    assert!(!line.contains('\r'));
}

const TEST_STARTTLS_CERT: &str = r#"-----BEGIN CERTIFICATE-----
MIICwzCCAaugAwIBAgIJAJuoO4jMAtuIMA0GCSqGSIb3DQEBCwUAMBQxEjAQBgNV
BAMTCWxvY2FsaG9zdDAeFw0yNDAxMDEwMDAwMDBaFw0zNjAxMDEwMDAwMDBaMBQx
EjAQBgNVBAMTCWxvY2FsaG9zdDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoC
ggEBANfmEnGwg+Tpia0A8yWhRyaVZPVG+Jkz9NrWpC7nmxmAJAnINjgdRdxAlnpL
v+3bomPjdvylkIIwbnUciIV+vXT5lzwRIwBnGbAX65zMLNfgTKX4Lfq/Tve19WD5
LBBricGcgXdCOQUFLOo/TsG7i+A8pf2bi5k7rNnKSS4NaLD76UPq7mx6VnJ8T6Rx
9GZPIvCQV1WYhkMT67H5o1emMNs025fjksbV/5onGw613mCtXOXrqNkV1ciebeh4
Ng5LerupzuUBLCzXbpczhveGuSDPjS1ciaqJ9auTTyBVHM7IEfPxa4lsULWezh1G
9J4ZhlmNttZKT72dSVDBmRaJh70CAwEAAaMYMBYwFAYDVR0RBA0wC4IJbG9jYWxo
b3N0MA0GCSqGSIb3DQEBCwUAA4IBAQBVBJ9FbRtJxsFNOHGorrwKx5vIhZa2bfk8
cTEqteUCpv6s7C0kURdAGe8Ljm87Wi2/GLcSzS6CEiN7IiPpQJohTmAFVyDx4HQY
a2b232lX28qUgwgptXCLjHpBYi9a8i1a3T3b5bOno3dA1fB87ktj4AphsyEaZ9fj
s6Uk7PVtHdGm/s0v2RKNqQmxk97b0RmLLjHG/uQdo1c3QK3yjnRxDAKyyD+M2/IX
qjRNTd1pSZBVFB3myZXaazG+hJDfQvYTMbNjqjF3I3rRUT+Jcp2Mr3ATYf0TCeNO
YO6eB+mUriJFwZ9gvz3C9oRfY+krdODdUK/6JJSK0Lr4kX4GJVi3
-----END CERTIFICATE-----"#;

const TEST_STARTTLS_KEY: &str = r#"-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDX5hJxsIPk6Ymt
APMloUcmlWT1RviZM/Ta1qQu55sZgCQJyDY4HUXcQJZ6S7/t26Jj43b8pZCCMG51
HIiFfr10+Zc8ESMAZxmwF+uczCzX4Eyl+C36v073tfVg+SwQa4nBnIF3QjkFBSzq
P07Bu4vgPKX9m4uZO6zZykkuDWiw++lD6u5selZyfE+kcfRmTyLwkFdVmIZDE+ux
+aNXpjDbNNuX45LG1f+aJxsOtd5grVzl66jZFdXInm3oeDYOS3q7qc7lASws126X
M4b3hrkgz40tXImqifWrk08gVRzOyBHz8WuJbFC1ns4dRvSeGYZZjbbWSk+9nUlQ
wZkWiYe9AgMBAAECggEACpTQGppYHIQFp2EAibuZzR5NUGgmDvwo6ADVEyduxpUt
Lv2NCrsEjYLs3RmRUosNLnAbiM5kgrz07PB1EHXhuzXwX5VHbeGftK23cnvfRsVL
fGbpefyeVi2o1RPhQPzER6TwA3RPbxuN0/0+UuhqNpdCW2egM+Zk1le/tm4Zz3Ky
JgbJhynYemOMc4/2OCcPpyEssaLZGnTlwbzGbR0EidS4ekfd1lfPYU3Kk4KfgifN
jFdkAr4kkqrCvlIuY5mwPzDgK1sNgEACHGgPJC3k3lYsdo8tWqjdn4qPlVvBHk9s
7yBJczo/guZfzZbG2n3YgSj++rw1piz4N39tI8dZ0QKBgQDbIeO/7wY8C2YJUuOM
aueusm30aeWdiPdd3W0jUOE1gDsHHwTUzGUU6DwxVuZRs8/k45pm76Pp/28wy7AX
CMOy4NlnWe/7pFW9sOb/ZV4cJf7632XJJ0UEWd2lSqWDxhxKjfrFEdJmiTEgpJHT
+suobWWiO5+JfYdEVaOS8aM4IwKBgQD8OOhLBuh+zrJDM45DoRSUlg70Ek+K5P2c
7EHjBi0Zai2ccTD7+BQB4QHgXnMspy53/1+TWxYo7omqseiVJ/SBVDBqZN9jR1mm
+84AD13FIXdm44MIaotgOw9RNqe/w3J0TkZOMnHYjD0BAFAH/Sa747+AYapsy4N/
XVZmziVOnwKBgF6MV803n7QGowcA2ad7dO1+lUyw6F65eynn4TAstI81/cIL0zTR
4AdOULJlMUktUVUME1G4sjvDd8FREXBO2slylLswJgiolkobav/lR97TUhoCi9Nn
+zJuZ+DqvVGHCCvu6LVhBCwzo5vXBgi1nGvWj9SY7zQOkm+cl9BOLEOLAoGBAKTK
faM/gToQzFGx5ppzLSIjpON87zF9ieI0TpwI1gCL6f8TyYBnRpMvsu0oaLHdDTRj
ystZMPJPX+0Bzkdd0peJLRTmkTmpTX8XeDF72LVKt1um/F7MVgHqtIhIYHOfPDGX
TsIanV1xyw3TaXa+xMbv95fmt9XbZjAaCLCksaVbAoGAQ1PE6Kqty08g4rF2weQQ
AJ0FxI7ZrH2cf/SAlGG+B60qVuhnRfBFiJRSBPET8ty9pSqAR+bNwURUln82/d6P
7rEifWa5MXwng2tG+DOIBrirfb0cpnTpdzvDOQNmJucsOQ2Us9nHrhHovYygRLpO
pzqAuzRp69VoxDpO6hdx/Qc=
-----END PRIVATE KEY-----"#;

fn temp_dir(label: &str) -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let dir = std::env::temp_dir().join(format!("lpe-ct-{label}-{suffix}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn runtime_config(primary_upstream: String, core_delivery_base_url: String) -> RuntimeConfig {
    RuntimeConfig {
        primary_upstream,
        secondary_upstream: String::new(),
        outbound_ehlo_name: "mx.lpe.example".to_string(),
        core_delivery_base_url,
        mutual_tls_required: false,
        fallback_to_hold_queue: false,
        drain_mode: false,
        quarantine_enabled: true,
        greylisting_enabled: false,
        greylist_delay_seconds: DEFAULT_GREYLIST_DELAY_SECONDS,
        antivirus_enabled: false,
        antivirus_fail_closed: true,
        antivirus_provider_chain: vec!["takeri".to_string()],
        antivirus_providers: load_antivirus_providers(&["takeri".to_string()]),
        bayespam_enabled: true,
        bayespam_auto_learn: true,
        bayespam_score_weight: 6.0,
        bayespam_min_token_length: 3,
        bayespam_max_tokens: 256,
        require_spf: true,
        require_dkim_alignment: false,
        require_dmarc_enforcement: true,
        defer_on_auth_tempfail: true,
        dnsbl_enabled: false,
        dnsbl_zones: Vec::new(),
        reputation_enabled: true,
        reputation_quarantine_threshold: -4,
        reputation_reject_threshold: -8,
        spam_quarantine_threshold: 5.0,
        spam_reject_threshold: 9.0,
        max_message_size_mb: 16,
        max_concurrent_sessions: 250,
        routing_rules: Vec::new(),
        throttle_enabled: false,
        throttle_rules: Vec::new(),
        address_policy: crate::transport_policy::AddressPolicyConfig::default(),
        recipient_verification: crate::transport_policy::RecipientVerificationConfig {
            enabled: false,
            fail_closed: true,
            cache_ttl_seconds: 300,
            local_db: crate::storage::LocalDbConfig::default(),
        },
        attachment_policy: crate::transport_policy::AttachmentPolicyConfig::default(),
        dkim: crate::dkim_signing::DkimConfig {
            enabled: false,
            headers: vec!["from".to_string()],
            over_sign: true,
            expiration_seconds: None,
            keys: Vec::new(),
        },
        local_db: crate::storage::LocalDbConfig::default(),
        accepted_domains: Vec::new(),
    }
}

fn plaintext_inbound_store(core_delivery_base_url: String) -> Arc<Mutex<crate::DashboardState>> {
    let mut state = crate::default_state();
    state.relay.primary_upstream = "127.0.0.1:9".to_string();
    state.relay.secondary_upstream.clear();
    state.relay.outbound_ehlo_name = "mx.lpe.example".to_string();
    state.relay.core_delivery_base_url = core_delivery_base_url;
    state.local_data_stores.dedicated_postgres.enabled = false;
    state.policies.greylisting_enabled = true;
    state.policies.dnsbl_enabled = true;
    state.policies.require_spf = true;
    state.policies.require_dmarc_enforcement = true;
    state.policies.defer_on_auth_tempfail = true;
    state.policies.bayespam_enabled = false;
    state.policies.reputation_enabled = false;
    state.policies.antivirus_enabled = false;
    state.policies.recipient_verification.enabled = false;
    state.accepted_domains = vec![crate::AcceptedDomain {
        id: "test-domain-lpe".to_string(),
        domain: "l-p-e.ch".to_string(),
        destination_server: "core-lpe".to_string(),
        verification_type: "bridge".to_string(),
        rbl_checks: false,
        spf_checks: false,
        greylisting: false,
        accept_null_reverse_path: true,
        verified: true,
    }];
    Arc::new(Mutex::new(state))
}

fn runtime_store_with_accepted_domains(
    domains: &[(&str, bool)],
) -> Arc<Mutex<crate::DashboardState>> {
    let mut state = crate::default_state();
    state.relay.primary_upstream = "127.0.0.1:9".to_string();
    state.relay.secondary_upstream.clear();
    state.relay.outbound_ehlo_name = "mx.lpe.example".to_string();
    state.relay.core_delivery_base_url = "http://127.0.0.1:9".to_string();
    state.policies.recipient_verification.enabled = false;
    state.accepted_domains = domains
        .iter()
        .enumerate()
        .map(|(index, (domain, verified))| crate::AcceptedDomain {
            id: format!("test-domain-{index}"),
            domain: domain.to_ascii_lowercase(),
            destination_server: "core-delivery".to_string(),
            verification_type: "dynamic".to_string(),
            rbl_checks: true,
            spf_checks: true,
            greylisting: true,
            accept_null_reverse_path: true,
            verified: *verified,
        })
        .collect();
    Arc::new(Mutex::new(state))
}

#[test]
fn recipient_domain_acceptance_is_exact_case_insensitive_and_verified() {
    let mut config = runtime_config("127.0.0.1:9".to_string(), "http://127.0.0.1:9".to_string());
    assert!(!super::recipient_domain_is_accepted(
        &config,
        "test@example.net"
    ));

    config.accepted_domains = vec![AcceptedDomainConfig {
        domain: "l-p-e.ch".to_string(),
        rbl_checks: true,
        spf_checks: true,
        greylisting: true,
        accept_null_reverse_path: true,
        verified: true,
    }];

    assert!(super::recipient_domain_is_accepted(
        &config,
        "test@l-p-e.ch"
    ));
    assert!(super::recipient_domain_is_accepted(
        &config,
        "Test@L-P-E.CH"
    ));
    assert!(!super::recipient_domain_is_accepted(
        &config,
        "relay-test@example.net"
    ));
    assert!(!super::recipient_domain_is_accepted(
        &config,
        "test@mail.l-p-e.ch"
    ));

    config.accepted_domains[0].verified = false;
    assert!(!super::recipient_domain_is_accepted(
        &config,
        "test@l-p-e.ch"
    ));
}

#[test]
fn smtp_path_parser_ignores_mail_parameters() {
    assert_eq!(
        super::parse_smtp_path(
            "<sender@example.test> SIZE=2048",
            SmtpPathKind::MailFrom,
            4096
        )
        .unwrap(),
        ParsedSmtpPath {
            address: "sender@example.test".to_string(),
            declared_size: Some(2048)
        }
    );
    assert_eq!(
        super::parse_smtp_path("<> SIZE=2048", SmtpPathKind::MailFrom, 4096).unwrap(),
        ParsedSmtpPath {
            address: String::new(),
            declared_size: Some(2048)
        }
    );
    assert_eq!(
        super::parse_smtp_path(
            "sender@example.test SIZE=2048",
            SmtpPathKind::MailFrom,
            4096
        )
        .unwrap_err(),
        SmtpPathError::MalformedPath
    );
    assert_eq!(
        super::parse_smtp_path(
            "<sender@example.test> BODY=8BITMIME",
            SmtpPathKind::MailFrom,
            4096
        )
        .unwrap_err(),
        SmtpPathError::UnsupportedParameter("BODY".to_string())
    );
    assert_eq!(
        super::parse_smtp_path("<bad", SmtpPathKind::RcptTo, 4096).unwrap_err(),
        SmtpPathError::MalformedPath
    );
    assert_eq!(
        super::parse_smtp_path("<bad>", SmtpPathKind::RcptTo, 4096).unwrap_err(),
        SmtpPathError::InvalidAddress
    );
    assert_eq!(
        super::parse_smtp_path(
            "<sender@example.test> SIZE=999999999999",
            SmtpPathKind::MailFrom,
            4096
        )
        .unwrap_err(),
        SmtpPathError::SizeTooLarge
    );
}

#[tokio::test]
async fn smtp_mail_from_rejects_malformed_paths_unsupported_params_and_size_overflow() {
    let client = reqwest::Client::new();
    let mut reader = BufReader::new(tokio::io::empty());
    let mut writer = Vec::new();
    let mut transaction = SmtpTransaction::default();
    let dashboard_store = runtime_store_with_accepted_domains(&[("l-p-e.ch", true)]);
    let spool = temp_dir("mail-from-hardening");
    let peer = "127.0.0.1:25".parse().unwrap();

    for command in [
        "MAIL FROM:probe@example.net",
        "MAIL FROM:<bad",
        "MAIL FROM:<sender@example.test> BODY=8BITMIME",
        "MAIL FROM:<sender@example.test> SMTPUTF8",
        "MAIL FROM:<sender@example.test> SIZE=999999999999",
        "MAIL FROM:<sender@example.test> SIZE=1024",
    ] {
        handle_smtp_command(
            &client,
            &mut reader,
            &mut writer,
            &dashboard_store,
            &spool,
            peer,
            &mut transaction,
            command,
            false,
        )
        .await
        .unwrap();
    }

    let transcript = String::from_utf8(writer).unwrap();
    assert!(transcript.contains("501 malformed MAIL FROM path\r\n"));
    assert!(transcript.contains("555 MAIL FROM parameter not supported (BODY)\r\n"));
    assert!(transcript.contains("555 MAIL FROM parameter not supported (SMTPUTF8)\r\n"));
    assert!(transcript.contains("552 message size exceeds configured maximum\r\n"));
    assert!(transcript.ends_with("250 sender accepted\r\n"));
    assert_eq!(transaction.mail_from, "sender@example.test");
}

#[tokio::test]
async fn smtp_rcpt_to_rejects_malformed_paths_and_unsupported_params() {
    let client = reqwest::Client::new();
    let mut reader = BufReader::new(tokio::io::empty());
    let mut writer = Vec::new();
    let mut transaction = SmtpTransaction::default();
    let dashboard_store = runtime_store_with_accepted_domains(&[("l-p-e.ch", true)]);
    let spool = temp_dir("rcpt-to-hardening");
    let peer = "127.0.0.1:25".parse().unwrap();

    handle_smtp_command(
        &client,
        &mut reader,
        &mut writer,
        &dashboard_store,
        &spool,
        peer,
        &mut transaction,
        "MAIL FROM:<sender@example.test>",
        false,
    )
    .await
    .unwrap();
    for command in [
        "RCPT TO:postmaster@l-p-e.ch",
        "RCPT TO:<postmaster@l-p-e.ch> NOTIFY=SUCCESS",
        "RCPT TO:<postmaster@l-p-e.ch> ORCPT=rfc822;probe@example.net",
        "RCPT TO:<bad@l-p-e.ch",
        "RCPT TO:<postmaster@l-p-e.ch>",
    ] {
        handle_smtp_command(
            &client,
            &mut reader,
            &mut writer,
            &dashboard_store,
            &spool,
            peer,
            &mut transaction,
            command,
            false,
        )
        .await
        .unwrap();
    }

    let transcript = String::from_utf8(writer).unwrap();
    assert!(transcript.contains("501 malformed RCPT TO path\r\n"));
    assert!(transcript.contains("555 RCPT TO parameter not supported (NOTIFY)\r\n"));
    assert!(transcript.contains("555 RCPT TO parameter not supported (ORCPT)\r\n"));
    assert!(transcript.ends_with("250 recipient accepted\r\n"));
    assert_eq!(transaction.rcpt_to, vec!["postmaster@l-p-e.ch".to_string()]);
}

#[tokio::test]
async fn smtp_rcpt_to_enforces_transaction_recipient_limit() {
    let client = reqwest::Client::new();
    let mut reader = BufReader::new(tokio::io::empty());
    let mut writer = Vec::new();
    let mut transaction = SmtpTransaction::default();
    let dashboard_store = runtime_store_with_accepted_domains(&[("l-p-e.ch", true)]);
    let spool = temp_dir("rcpt-limit");
    let peer = "127.0.0.1:25".parse().unwrap();

    handle_smtp_command(
        &client,
        &mut reader,
        &mut writer,
        &dashboard_store,
        &spool,
        peer,
        &mut transaction,
        "MAIL FROM:<sender@example.test>",
        false,
    )
    .await
    .unwrap();
    for index in 0..=MAX_SMTP_RCPT_PER_TRANSACTION {
        handle_smtp_command(
            &client,
            &mut reader,
            &mut writer,
            &dashboard_store,
            &spool,
            peer,
            &mut transaction,
            &format!("RCPT TO:<user{index}@l-p-e.ch>"),
            false,
        )
        .await
        .unwrap();
    }

    let transcript = String::from_utf8(writer).unwrap();
    assert_eq!(
        transcript.matches("250 recipient accepted").count(),
        MAX_SMTP_RCPT_PER_TRANSACTION
    );
    assert!(transcript.ends_with("452 too many recipients\r\n"));
    assert_eq!(transaction.rcpt_to.len(), MAX_SMTP_RCPT_PER_TRANSACTION);
}

#[tokio::test]
async fn smtp_long_command_line_returns_line_length_error() {
    let client = reqwest::Client::new();
    let mut reader = BufReader::new(tokio::io::empty());
    let mut writer = Vec::new();
    let mut transaction = SmtpTransaction::default();
    let dashboard_store = runtime_store_with_accepted_domains(&[("l-p-e.ch", true)]);
    let spool = temp_dir("long-command");
    let command = format!("NOOP {}", "A".repeat(MAX_SMTP_COMMAND_LINE_LEN));

    handle_smtp_command(
        &client,
        &mut reader,
        &mut writer,
        &dashboard_store,
        &spool,
        "127.0.0.1:25".parse().unwrap(),
        &mut transaction,
        &command,
        false,
    )
    .await
    .unwrap();

    assert_eq!(
        String::from_utf8(writer).unwrap(),
        "500 command line too long\r\n"
    );
}

#[tokio::test]
async fn smtp_command_sequence_requires_mail_and_recipient_before_data() {
    let client = reqwest::Client::new();
    let mut reader = BufReader::new(tokio::io::empty());
    let mut writer = Vec::new();
    let mut transaction = SmtpTransaction::default();
    let dashboard_store = runtime_store_with_accepted_domains(&[("l-p-e.ch", true)]);
    let spool = temp_dir("command-sequence");
    let peer = "127.0.0.1:25".parse().unwrap();

    handle_smtp_command(
        &client,
        &mut reader,
        &mut writer,
        &dashboard_store,
        &spool,
        peer,
        &mut transaction,
        "DATA",
        false,
    )
    .await
    .unwrap();
    handle_smtp_command(
        &client,
        &mut reader,
        &mut writer,
        &dashboard_store,
        &spool,
        peer,
        &mut transaction,
        "RCPT TO:<test@l-p-e.ch>",
        false,
    )
    .await
    .unwrap();
    handle_smtp_command(
        &client,
        &mut reader,
        &mut writer,
        &dashboard_store,
        &spool,
        peer,
        &mut transaction,
        "MAIL FROM:<sender@example.test>",
        false,
    )
    .await
    .unwrap();
    handle_smtp_command(
        &client,
        &mut reader,
        &mut writer,
        &dashboard_store,
        &spool,
        peer,
        &mut transaction,
        "DATA",
        false,
    )
    .await
    .unwrap();

    assert_eq!(
        String::from_utf8(writer).unwrap(),
        concat!(
            "503 sender and recipient required\r\n",
            "503 send MAIL FROM first\r\n",
            "250 sender accepted\r\n",
            "503 sender and recipient required\r\n"
        )
    );
}

#[tokio::test]
async fn smtp_rcpt_accepts_configured_domain_and_rejects_external_relay_domain() {
    let client = reqwest::Client::new();
    let mut reader = BufReader::new(tokio::io::empty());
    let mut writer = Vec::new();
    let mut transaction = SmtpTransaction::default();
    let dashboard_store = runtime_store_with_accepted_domains(&[("l-p-e.ch", true)]);
    let spool = temp_dir("accepted-domain-spool");

    handle_smtp_command(
        &client,
        &mut reader,
        &mut writer,
        &dashboard_store,
        &spool,
        "127.0.0.1:25".parse().unwrap(),
        &mut transaction,
        "MAIL FROM:<smtp-test@l-p-e.ch>",
        false,
    )
    .await
    .unwrap();
    handle_smtp_command(
        &client,
        &mut reader,
        &mut writer,
        &dashboard_store,
        &spool,
        "127.0.0.1:25".parse().unwrap(),
        &mut transaction,
        "RCPT TO:<test@l-p-e.ch>",
        false,
    )
    .await
    .unwrap();
    handle_smtp_command(
        &client,
        &mut reader,
        &mut writer,
        &dashboard_store,
        &spool,
        "127.0.0.1:25".parse().unwrap(),
        &mut transaction,
        "RCPT TO:<relay-test@example.net>",
        false,
    )
    .await
    .unwrap();
    handle_smtp_command(
        &client,
        &mut reader,
        &mut writer,
        &dashboard_store,
        &spool,
        "127.0.0.1:25".parse().unwrap(),
        &mut transaction,
        "RCPT TO:<test@sdic.ch>",
        false,
    )
    .await
    .unwrap();

    let transcript = String::from_utf8(writer).unwrap();
    assert!(transcript.contains("250 sender accepted\r\n"));
    assert!(transcript.contains("250 recipient accepted\r\n"));
    assert!(transcript.contains("550 recipient domain is not accepted by this sorting center\r\n"));
    assert_eq!(
        transcript
            .matches("550 recipient domain is not accepted by this sorting center")
            .count(),
        2
    );
    assert_eq!(transaction.rcpt_to, vec!["test@l-p-e.ch".to_string()]);
}

#[tokio::test]
async fn smtp_null_reverse_path_is_controlled_per_recipient_domain() {
    let client = reqwest::Client::new();
    let mut reader = BufReader::new(tokio::io::empty());
    let mut writer = Vec::new();
    let mut transaction = SmtpTransaction::default();
    let dashboard_store =
        runtime_store_with_accepted_domains(&[("l-p-e.ch", true), ("blocked.example", true)]);
    {
        let mut state = dashboard_store.lock().unwrap();
        state
            .accepted_domains
            .iter_mut()
            .find(|domain| domain.domain == "blocked.example")
            .unwrap()
            .accept_null_reverse_path = false;
    }
    let spool = temp_dir("null-reverse-path-domain-policy");
    let peer = "127.0.0.1:25".parse().unwrap();

    handle_smtp_command(
        &client,
        &mut reader,
        &mut writer,
        &dashboard_store,
        &spool,
        peer,
        &mut transaction,
        "MAIL FROM:<>",
        false,
    )
    .await
    .unwrap();
    handle_smtp_command(
        &client,
        &mut reader,
        &mut writer,
        &dashboard_store,
        &spool,
        peer,
        &mut transaction,
        "RCPT TO:<dsn@l-p-e.ch>",
        false,
    )
    .await
    .unwrap();
    handle_smtp_command(
        &client,
        &mut reader,
        &mut writer,
        &dashboard_store,
        &spool,
        peer,
        &mut transaction,
        "RCPT TO:<dsn@blocked.example>",
        false,
    )
    .await
    .unwrap();

    let transcript = String::from_utf8(writer).unwrap();
    assert!(transcript.contains("250 sender accepted\r\n"));
    assert!(transcript.contains("250 recipient accepted\r\n"));
    assert!(transcript.contains("550 recipient domain does not accept null reverse-path\r\n"));
    assert!(transaction.mail_from_seen);
    assert_eq!(transaction.mail_from, "");
    assert_eq!(transaction.rcpt_to, vec!["dsn@l-p-e.ch".to_string()]);
}

#[tokio::test]
async fn smtp_session_accepts_lpe_domain_and_rejects_external_relay_domain() {
    let spool = temp_dir("accepted-domain-session-spool");
    initialize_spool(&spool).unwrap();
    let dashboard_store = runtime_store_with_accepted_domains(&[("l-p-e.ch", true)]);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server_spool = spool.clone();
    let server = tokio::spawn(async move {
        let (stream, peer) = listener.accept().await.unwrap();
        handle_smtp_session(stream, peer, dashboard_store, server_spool, None)
            .await
            .unwrap();
    });

    let stream = TcpStream::connect(address).await.unwrap();
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "220 mx.lpe.example ESMTP ready\r\n");

    writer
        .write_all(b"EHLO validator.l-p-e.ch\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "250-mx.lpe.example\r\n");
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "250 SIZE 67108864\r\n");

    writer
        .write_all(b"MAIL FROM:<smtp-test@l-p-e.ch>\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "250 sender accepted\r\n");

    writer
        .write_all(b"RCPT TO:<test@l-p-e.ch>\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "250 recipient accepted\r\n");

    writer.write_all(b"RSET\r\n").await.unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "250 reset\r\n");

    writer
        .write_all(b"MAIL FROM:<relay-test@example.com>\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "250 sender accepted\r\n");

    writer
        .write_all(b"RCPT TO:<relay-test@example.net>\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(
        line,
        "550 recipient domain is not accepted by this sorting center\r\n"
    );

    writer.write_all(b"QUIT\r\n").await.unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "221 bye\r\n");

    server.await.unwrap();
}

#[tokio::test]
async fn smtp_data_accepts_plaintext_for_local_domain_and_hands_to_core() {
    let _guard = env_test_lock();
    std::env::set_var(
        "LPE_INTEGRATION_SHARED_SECRET",
        "0123456789abcdef0123456789abcdef",
    );
    let spool = temp_dir("smtp-data-accept");
    initialize_spool(&spool).unwrap();
    let captured = Arc::new(Mutex::new(None::<InboundDeliveryRequest>));
    let core_base_url = spawn_dummy_core(captured.clone()).await;
    let dashboard_store = plaintext_inbound_store(core_base_url);
    let client = reqwest::Client::new();
    let mut reader = BufReader::new(
            b"From: Sender <smtp-test@l-p-e.ch>\r\nMessage-ID: <codex-smtp-test-1777566333254@l-p-e.ch>\r\nSubject: Inbound\r\n\r\nBody\r\n.\r\n"
                .as_slice(),
        );
    let mut writer = Vec::new();
    let mut transaction = SmtpTransaction::default();
    let peer: SocketAddr = "127.0.0.1:25".parse().unwrap();

    handle_smtp_command(
        &client,
        &mut reader,
        &mut writer,
        &dashboard_store,
        &spool,
        peer,
        &mut transaction,
        "MAIL FROM:<smtp-test@l-p-e.ch>",
        false,
    )
    .await
    .unwrap();
    handle_smtp_command(
        &client,
        &mut reader,
        &mut writer,
        &dashboard_store,
        &spool,
        peer,
        &mut transaction,
        "RCPT TO:<test@l-p-e.ch>",
        false,
    )
    .await
    .unwrap();
    handle_smtp_command(
        &client,
        &mut reader,
        &mut writer,
        &dashboard_store,
        &spool,
        peer,
        &mut transaction,
        "DATA",
        false,
    )
    .await
    .unwrap();

    let transcript = String::from_utf8(writer).unwrap();
    assert!(transcript.contains("250 sender accepted\r\n"));
    assert!(transcript.contains("250 recipient accepted\r\n"));
    assert!(transcript.contains("354 end with <CRLF>.<CRLF>\r\n"));
    assert!(transcript.contains("250 queued as lpe-ct-in-"));
    assert!(!transcript.contains("451 "));
    let request = captured.lock().unwrap().clone().unwrap();
    assert_eq!(request.mail_from, "smtp-test@l-p-e.ch");
    assert_eq!(request.rcpt_to, vec!["test@l-p-e.ch".to_string()]);
    assert_eq!(request.subject, "Inbound");
    assert_eq!(
        request.internet_message_id.as_deref(),
        Some("<codex-smtp-test-1777566333254@l-p-e.ch>")
    );
    std::env::remove_var("LPE_INTEGRATION_SHARED_SECRET");
}

#[tokio::test]
async fn inbound_delivery_keeps_durable_spool_custody_until_core_accepts() {
    let _guard = env_test_lock();
    std::env::set_var(
        "LPE_INTEGRATION_SHARED_SECRET",
        "0123456789abcdef0123456789abcdef",
    );
    let spool = temp_dir("inbound-custody-before-core-accept");
    initialize_spool(&spool).unwrap();
    let observed_spool_custody = Arc::new(Mutex::new(false));
    let core_base_url =
        spawn_custody_asserting_core(spool.clone(), observed_spool_custody.clone()).await;
    let config = runtime_config("127.0.0.1:9".to_string(), core_base_url);

    let message = receive_message(
        &spool,
        &config,
        "203.0.113.10:25".to_string(),
        "mx.example.test".to_string(),
        "sender@example.test".to_string(),
        vec!["dest@example.test".to_string()],
        b"From: sender@example.test\r\nSubject: Custody\r\n\r\nBody\r\n".to_vec(),
    )
    .await
    .unwrap();

    assert!(*observed_spool_custody.lock().unwrap());
    assert_eq!(message.status, "sent");
    assert!(!spool
        .join("incoming")
        .join(format!("{}.json", message.id))
        .exists());
    assert!(spool
        .join("sent")
        .join(format!("{}.json", message.id))
        .exists());
    let audit =
        std::fs::read_to_string(spool.join("policy").join("transport-audit.jsonl")).unwrap();
    assert!(audit.contains("\"queue\":\"sent\""));
    assert!(audit.contains("\"status\":\"sent\""));
    std::env::remove_var("LPE_INTEGRATION_SHARED_SECRET");
}

#[tokio::test]
async fn smtp_data_accepts_null_reverse_path_for_dsn_delivery() {
    let _guard = env_test_lock();
    std::env::set_var(
        "LPE_INTEGRATION_SHARED_SECRET",
        "0123456789abcdef0123456789abcdef",
    );
    let spool = temp_dir("smtp-data-null-reverse-path");
    initialize_spool(&spool).unwrap();
    let captured = Arc::new(Mutex::new(None::<InboundDeliveryRequest>));
    let core_base_url = spawn_dummy_core(captured.clone()).await;
    let dashboard_store = plaintext_inbound_store(core_base_url);
    let client = reqwest::Client::new();
    let mut reader = BufReader::new(
            b"From: Mail Delivery System <mailer-daemon@example.test>\r\nSubject: Delivery Status Notification\r\n\r\nDelivery failed\r\n.\r\n"
                .as_slice(),
        );
    let mut writer = Vec::new();
    let mut transaction = SmtpTransaction::default();
    let peer: SocketAddr = "127.0.0.1:25".parse().unwrap();

    handle_smtp_command(
        &client,
        &mut reader,
        &mut writer,
        &dashboard_store,
        &spool,
        peer,
        &mut transaction,
        "MAIL FROM:<>",
        false,
    )
    .await
    .unwrap();
    handle_smtp_command(
        &client,
        &mut reader,
        &mut writer,
        &dashboard_store,
        &spool,
        peer,
        &mut transaction,
        "RCPT TO:<test@l-p-e.ch>",
        false,
    )
    .await
    .unwrap();
    handle_smtp_command(
        &client,
        &mut reader,
        &mut writer,
        &dashboard_store,
        &spool,
        peer,
        &mut transaction,
        "DATA",
        false,
    )
    .await
    .unwrap();

    let transcript = String::from_utf8(writer).unwrap();
    assert!(transcript.contains("250 sender accepted\r\n"));
    assert!(transcript.contains("250 recipient accepted\r\n"));
    assert!(transcript.contains("250 queued as lpe-ct-in-"));
    let request = captured.lock().unwrap().clone().unwrap();
    assert_eq!(request.mail_from, "");
    assert_eq!(request.rcpt_to, vec!["test@l-p-e.ch".to_string()]);
    assert_eq!(request.subject, "Delivery Status Notification");
    std::env::remove_var("LPE_INTEGRATION_SHARED_SECRET");
}

#[tokio::test]
async fn smtp_data_defers_with_trace_when_core_delivery_is_unavailable() {
    let _guard = env_test_lock();
    std::env::set_var(
        "LPE_INTEGRATION_SHARED_SECRET",
        "0123456789abcdef0123456789abcdef",
    );
    let spool = temp_dir("smtp-data-core-defer");
    initialize_spool(&spool).unwrap();
    let dashboard_store = plaintext_inbound_store("http://127.0.0.1:9".to_string());
    let client = reqwest::Client::new();
    let mut reader = BufReader::new(
        b"From: Sender <smtp-test@l-p-e.ch>\r\nSubject: Inbound\r\n\r\nBody\r\n.\r\n".as_slice(),
    );
    let mut writer = Vec::new();
    let mut transaction = SmtpTransaction::default();
    let peer: SocketAddr = "127.0.0.1:25".parse().unwrap();

    handle_smtp_command(
        &client,
        &mut reader,
        &mut writer,
        &dashboard_store,
        &spool,
        peer,
        &mut transaction,
        "MAIL FROM:<smtp-test@l-p-e.ch>",
        false,
    )
    .await
    .unwrap();
    handle_smtp_command(
        &client,
        &mut reader,
        &mut writer,
        &dashboard_store,
        &spool,
        peer,
        &mut transaction,
        "RCPT TO:<test@l-p-e.ch>",
        false,
    )
    .await
    .unwrap();
    handle_smtp_command(
        &client,
        &mut reader,
        &mut writer,
        &dashboard_store,
        &spool,
        peer,
        &mut transaction,
        "DATA",
        false,
    )
    .await
    .unwrap();

    let transcript = String::from_utf8(writer).unwrap();
    assert!(
        transcript.contains("451 core final delivery temporarily unavailable (trace lpe-ct-in-")
    );
    assert!(transcript.contains(")\r\n"));
    assert!(spool.join("deferred").read_dir().unwrap().next().is_some());
    assert!(spool.join("bounces").read_dir().unwrap().next().is_none());
    std::env::remove_var("LPE_INTEGRATION_SHARED_SECRET");
}

#[tokio::test]
async fn inbound_bridge_failure_keeps_deferred_custody_with_audit() {
    let _guard = env_test_lock();
    std::env::set_var(
        "LPE_INTEGRATION_SHARED_SECRET",
        "0123456789abcdef0123456789abcdef",
    );
    let spool = temp_dir("inbound-bridge-failure-custody");
    initialize_spool(&spool).unwrap();
    let config = runtime_config("127.0.0.1:9".to_string(), "http://127.0.0.1:9".to_string());

    let message = receive_message(
        &spool,
        &config,
        "203.0.113.10:25".to_string(),
        "mx.example.test".to_string(),
        "sender@example.test".to_string(),
        vec!["dest@example.test".to_string()],
        b"From: sender@example.test\r\nSubject: Deferred custody\r\n\r\nBody\r\n".to_vec(),
    )
    .await
    .unwrap();

    assert_eq!(message.status, "deferred");
    assert!(!spool
        .join("incoming")
        .join(format!("{}.json", message.id))
        .exists());
    assert!(spool
        .join("deferred")
        .join(format!("{}.json", message.id))
        .exists());
    assert!(spool.join("bounces").read_dir().unwrap().next().is_none());
    let audit =
        std::fs::read_to_string(spool.join("policy").join("transport-audit.jsonl")).unwrap();
    assert!(audit.contains("\"queue\":\"deferred\""));
    assert!(audit.contains("\"status\":\"deferred\""));
    assert!(audit.contains("core-delivery"));
    std::env::remove_var("LPE_INTEGRATION_SHARED_SECRET");
}

#[tokio::test]
async fn accepted_inbound_spool_custody_survives_restart_before_core_delivery() {
    let spool = temp_dir("inbound-restart-before-core-delivery");
    initialize_spool(&spool).unwrap();
    let message = inbound_test_message(
        "trace-inbound-restart-1",
        "incoming",
        "Restart before core delivery",
    );
    persist_message(&spool, "incoming", &message).await.unwrap();

    initialize_spool(&spool).unwrap();
    let recovered = load_trace_details(&spool, &message.id).unwrap().unwrap();

    assert_eq!(recovered.queue, "incoming");
    assert_eq!(recovered.status, "incoming");
    assert_eq!(recovered.direction, "inbound");
    assert_eq!(recovered.mail_from, "sender@example.test");
    assert_eq!(recovered.rcpt_to, vec!["dest@example.test".to_string()]);
    assert!(spool
        .join("incoming")
        .join(format!("{}.json", message.id))
        .exists());
}

#[tokio::test]
async fn smtp_unknown_local_recipient_core_rejection_defers_without_backscatter_bounce() {
    let _guard = env_test_lock();
    std::env::set_var(
        "LPE_INTEGRATION_SHARED_SECRET",
        "0123456789abcdef0123456789abcdef",
    );
    let spool = temp_dir("smtp-data-core-recipient-reject");
    initialize_spool(&spool).unwrap();
    let captured = Arc::new(Mutex::new(None::<InboundDeliveryRequest>));
    let capture = captured.clone();
    let router = Router::new().route(
        "/internal/lpe-ct/inbound-deliveries",
        post(move |Json(request): Json<InboundDeliveryRequest>| {
            let capture = capture.clone();
            async move {
                *capture.lock().unwrap() = Some(request);
                Json(InboundDeliveryResponse {
                    accepted: false,
                    delivered_mailboxes: Vec::new(),
                    detail: Some("unknown local recipient".to_string()),
                })
            }
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let dashboard_store = plaintext_inbound_store(format!("http://{address}"));
    let client = reqwest::Client::new();
    let mut reader = BufReader::new(
        b"From: Sender <smtp-test@l-p-e.ch>\r\nSubject: Unknown recipient\r\n\r\nBody\r\n.\r\n"
            .as_slice(),
    );
    let mut writer = Vec::new();
    let mut transaction = SmtpTransaction::default();
    let peer: SocketAddr = "127.0.0.1:25".parse().unwrap();

    handle_smtp_command(
        &client,
        &mut reader,
        &mut writer,
        &dashboard_store,
        &spool,
        peer,
        &mut transaction,
        "MAIL FROM:<smtp-test@l-p-e.ch>",
        false,
    )
    .await
    .unwrap();
    handle_smtp_command(
        &client,
        &mut reader,
        &mut writer,
        &dashboard_store,
        &spool,
        peer,
        &mut transaction,
        "RCPT TO:<definitely-not-a-real-user-260502@l-p-e.ch>",
        false,
    )
    .await
    .unwrap();
    handle_smtp_command(
        &client,
        &mut reader,
        &mut writer,
        &dashboard_store,
        &spool,
        peer,
        &mut transaction,
        "DATA",
        false,
    )
    .await
    .unwrap();

    let transcript = String::from_utf8(writer).unwrap();
    assert!(transcript.contains("250 recipient accepted\r\n"));
    assert!(
        transcript.contains("451 core final delivery temporarily unavailable (trace lpe-ct-in-")
    );
    assert!(spool.join("deferred").read_dir().unwrap().next().is_some());
    assert!(spool.join("bounces").read_dir().unwrap().next().is_none());
    assert_eq!(
        captured.lock().unwrap().clone().unwrap().rcpt_to,
        vec!["definitely-not-a-real-user-260502@l-p-e.ch".to_string()]
    );
    std::env::remove_var("LPE_INTEGRATION_SHARED_SECRET");
}

#[tokio::test]
async fn smtp_data_rejects_with_policy_reason_and_trace() {
    let spool = temp_dir("smtp-data-policy-reject");
    initialize_spool(&spool).unwrap();
    let dashboard_store = plaintext_inbound_store("http://127.0.0.1:9".to_string());
    {
        let mut state = dashboard_store.lock().unwrap();
        state.policies.require_spf = false;
        state.policies.require_dmarc_enforcement = false;
        state.policies.defer_on_auth_tempfail = false;
        state.policies.spam_reject_threshold = 0.0;
    }
    let client = reqwest::Client::new();
    let mut reader = BufReader::new(
        b"From: Sender <smtp-test@l-p-e.ch>\r\nSubject: Inbound\r\n\r\nBody\r\n.\r\n".as_slice(),
    );
    let mut writer = Vec::new();
    let mut transaction = SmtpTransaction::default();
    let peer: SocketAddr = "127.0.0.1:25".parse().unwrap();

    handle_smtp_command(
        &client,
        &mut reader,
        &mut writer,
        &dashboard_store,
        &spool,
        peer,
        &mut transaction,
        "MAIL FROM:<smtp-test@l-p-e.ch>",
        false,
    )
    .await
    .unwrap();
    handle_smtp_command(
        &client,
        &mut reader,
        &mut writer,
        &dashboard_store,
        &spool,
        peer,
        &mut transaction,
        "RCPT TO:<test@l-p-e.ch>",
        false,
    )
    .await
    .unwrap();
    handle_smtp_command(
        &client,
        &mut reader,
        &mut writer,
        &dashboard_store,
        &spool,
        peer,
        &mut transaction,
        "DATA",
        false,
    )
    .await
    .unwrap();

    let transcript = String::from_utf8(writer).unwrap();
    assert!(transcript.contains("354 end with <CRLF>.<CRLF>\r\n"));
    assert!(transcript.contains(
            "554 message rejected by perimeter policy: spam score 0.0 reached reject threshold 0.0 (trace lpe-ct-in-"
        ));
    assert!(spool
        .join("quarantine")
        .read_dir()
        .unwrap()
        .next()
        .is_some());
}

fn training_message(subject: &str, body: &str) -> QueuedMessage {
    QueuedMessage {
        id: format!("trace-{}", stable_key_id(&(subject, body))),
        direction: "inbound".to_string(),
        received_at: "unix:1".to_string(),
        peer: "192.0.2.10:25".to_string(),
        helo: "mx.example.test".to_string(),
        mail_from: "sender@example.test".to_string(),
        rcpt_to: vec!["dest@example.test".to_string()],
        status: "incoming".to_string(),
        relay_error: None,
        magika_summary: None,
        magika_decision: None,
        spam_score: 0.0,
        security_score: 0.0,
        reputation_score: 0,
        dnsbl_hits: Vec::new(),
        auth_summary: AuthSummary::default(),
        decision_trace: Vec::new(),
        remote_message_ref: None,
        technical_status: None,
        dsn: None,
        route: None,
        throttle: None,
        data: format!("Subject: {subject}\r\n\r\n{body}").into_bytes(),
    }
}

#[derive(Default)]
struct CountingWriter {
    writes: Vec<Vec<u8>>,
}

impl tokio::io::AsyncWrite for CountingWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut TaskContext<'_>,
        data: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.writes.push(data.to_vec());
        Poll::Ready(Ok(data.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn smtp_write_emits_reply_and_crlf_in_one_write() {
    let mut writer = CountingWriter::default();

    write_smtp(&mut writer, "220 ready to start TLS")
        .await
        .unwrap();

    assert_eq!(writer.writes, vec![b"220 ready to start TLS\r\n".to_vec()]);
}

#[tokio::test]
async fn smtp_ehlo_advertises_starttls_when_tls_is_available() {
    let client = reqwest::Client::new();
    let mut reader = BufReader::new(tokio::io::empty());
    let mut writer = Vec::new();
    let mut transaction = SmtpTransaction::default();
    let dashboard_store = runtime_store_with_accepted_domains(&[]);

    handle_smtp_command(
        &client,
        &mut reader,
        &mut writer,
        &dashboard_store,
        std::path::Path::new("spool"),
        "127.0.0.1:25".parse().unwrap(),
        &mut transaction,
        "EHLO mx.example.test",
        true,
    )
    .await
    .unwrap();

    assert_eq!(
        String::from_utf8(writer).unwrap(),
        "250-mx.lpe.example\r\n250-STARTTLS\r\n250 SIZE 67108864\r\n"
    );
}

#[tokio::test]
async fn smtp_ehlo_does_not_advertise_starttls_without_tls_config() {
    let client = reqwest::Client::new();
    let mut reader = BufReader::new(tokio::io::empty());
    let mut writer = Vec::new();
    let mut transaction = SmtpTransaction::default();
    let dashboard_store = runtime_store_with_accepted_domains(&[]);

    handle_smtp_command(
        &client,
        &mut reader,
        &mut writer,
        &dashboard_store,
        std::path::Path::new("spool"),
        "127.0.0.1:25".parse().unwrap(),
        &mut transaction,
        "EHLO mx.example.test",
        false,
    )
    .await
    .unwrap();

    assert_eq!(
        String::from_utf8(writer).unwrap(),
        "250-mx.lpe.example\r\n250 SIZE 67108864\r\n"
    );
}

#[tokio::test]
async fn smtp_public_ingress_does_not_advertise_or_accept_auth() {
    let client = reqwest::Client::new();
    let mut reader = BufReader::new(tokio::io::empty());
    let mut writer = Vec::new();
    let mut transaction = SmtpTransaction::default();
    let dashboard_store = runtime_store_with_accepted_domains(&[]);

    handle_smtp_command(
        &client,
        &mut reader,
        &mut writer,
        &dashboard_store,
        std::path::Path::new("spool"),
        "127.0.0.1:25".parse().unwrap(),
        &mut transaction,
        "EHLO mx.example.test",
        false,
    )
    .await
    .unwrap();
    handle_smtp_command(
        &client,
        &mut reader,
        &mut writer,
        &dashboard_store,
        std::path::Path::new("spool"),
        "127.0.0.1:25".parse().unwrap(),
        &mut transaction,
        "AUTH PLAIN",
        false,
    )
    .await
    .unwrap();

    let transcript = String::from_utf8(writer).unwrap();
    assert!(!transcript.to_ascii_uppercase().contains("250-AUTH"));
    assert!(!transcript.to_ascii_uppercase().contains("250 AUTH"));
    assert!(transcript.ends_with("502 AUTH not available on public SMTP ingress\r\n"));
}

#[test]
fn smtp_starttls_acceptor_rejects_invalid_tls_config() {
    let missing = temp_dir("missing-starttls-pems").join("missing.pem");

    assert!(smtp_starttls_acceptor_for_paths(
        Some(missing.display().to_string()),
        Some(missing.display().to_string()),
    )
    .is_err());
}

#[tokio::test]
async fn smtp_starttls_requires_ehlo_or_helo_first() {
    let client = reqwest::Client::new();
    let mut reader = BufReader::new(tokio::io::empty());
    let mut writer = Vec::new();
    let mut transaction = SmtpTransaction::default();
    let dashboard_store = runtime_store_with_accepted_domains(&[]);

    let outcome = handle_smtp_command(
        &client,
        &mut reader,
        &mut writer,
        &dashboard_store,
        std::path::Path::new("spool"),
        "127.0.0.1:25".parse().unwrap(),
        &mut transaction,
        "STARTTLS",
        true,
    )
    .await
    .unwrap();

    assert!(matches!(outcome, SmtpCommandOutcome::Continue));
    assert_eq!(
        String::from_utf8(writer).unwrap(),
        "503 send EHLO or HELO first\r\n"
    );
}

#[tokio::test]
async fn smtp_ehlo_does_not_advertise_starttls_after_tls_upgrade() {
    let client = reqwest::Client::new();
    let mut reader = BufReader::new(tokio::io::empty());
    let mut writer = Vec::new();
    let mut transaction = SmtpTransaction::default();
    let dashboard_store = runtime_store_with_accepted_domains(&[]);

    handle_smtp_command(
        &client,
        &mut reader,
        &mut writer,
        &dashboard_store,
        std::path::Path::new("spool"),
        "127.0.0.1:25".parse().unwrap(),
        &mut transaction,
        "EHLO mx.example.test",
        false,
    )
    .await
    .unwrap();

    assert_eq!(
        String::from_utf8(writer).unwrap(),
        "250-mx.lpe.example\r\n250 SIZE 67108864\r\n"
    );
}

async fn read_test_smtp_reply<R>(reader: &mut BufReader<R>) -> String
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut reply = String::new();
    loop {
        let mut line = String::new();
        let bytes = reader.read_line(&mut line).await.unwrap();
        assert_ne!(bytes, 0, "SMTP session closed before reply completed");
        let is_last = line.as_bytes().get(3).copied() != Some(b'-');
        reply.push_str(&line);
        if is_last {
            return reply;
        }
    }
}

#[tokio::test]
async fn smtp_starttls_upgrades_to_tls_after_ready_reply() {
    let _guard = env_test_lock();
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    std::env::set_var(
        "LPE_INTEGRATION_SHARED_SECRET",
        "0123456789abcdef0123456789abcdef",
    );
    let spool = temp_dir("starttls-session-spool");
    initialize_spool(&spool).unwrap();
    let tls_dir = temp_dir("starttls-pems");
    let cert_path = tls_dir.join("cert.pem");
    let key_path = tls_dir.join("key.pem");
    std::fs::write(&cert_path, TEST_STARTTLS_CERT).unwrap();
    std::fs::write(&key_path, TEST_STARTTLS_KEY).unwrap();
    let starttls = smtp_starttls_acceptor_for_paths(
        Some(cert_path.display().to_string()),
        Some(key_path.display().to_string()),
    )
    .unwrap()
    .expect("test TLS certificate should enable STARTTLS");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let captured = Arc::new(Mutex::new(None::<InboundDeliveryRequest>));
    let core_base_url = spawn_dummy_core(captured.clone()).await;
    let dashboard_store = plaintext_inbound_store(core_base_url);
    let server = tokio::spawn(async move {
        let (stream, peer) = listener.accept().await.unwrap();
        handle_smtp_session(stream, peer, dashboard_store, spool, Some(starttls))
            .await
            .unwrap();
    });

    let stream = TcpStream::connect(address).await.unwrap();
    let mut reader = BufReader::new(stream);
    assert_eq!(
        read_test_smtp_reply(&mut reader).await,
        "220 mx.lpe.example ESMTP ready\r\n"
    );
    reader
        .get_mut()
        .write_all(b"EHLO mx.example.test\r\n")
        .await
        .unwrap();
    let ehlo = read_test_smtp_reply(&mut reader).await;
    assert_eq!(
        ehlo,
        "250-mx.lpe.example\r\n250-STARTTLS\r\n250 SIZE 67108864\r\n"
    );

    reader.get_mut().write_all(b"STARTTLS\r\n").await.unwrap();
    assert_eq!(
        read_test_smtp_reply(&mut reader).await,
        "220 ready to start TLS\r\n"
    );

    let stream = reader.into_inner();
    let mut cert_reader = StdIoBufReader::new(Cursor::new(TEST_STARTTLS_CERT.as_bytes()));
    let certificate = rustls_pemfile::certs(&mut cert_reader)
        .next()
        .expect("test certificate should be present")
        .unwrap();
    let mut root_store = RootCertStore::empty();
    root_store.add(certificate).unwrap();
    let client_config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(client_config));
    let server_name = ServerName::try_from("localhost").unwrap().to_owned();
    let tls_stream = connector.connect(server_name, stream).await.unwrap();
    let mut tls_reader = BufReader::new(tls_stream);

    tls_reader
        .get_mut()
        .write_all(b"MAIL FROM:<internet-check@external.example>\r\n")
        .await
        .unwrap();
    assert_eq!(
        read_test_smtp_reply(&mut tls_reader).await,
        "503 send EHLO or HELO after STARTTLS first\r\n"
    );

    tls_reader
        .get_mut()
        .write_all(b"EHLO secure.example.test\r\n")
        .await
        .unwrap();
    assert_eq!(
        read_test_smtp_reply(&mut tls_reader).await,
        "250-mx.lpe.example\r\n250 SIZE 67108864\r\n"
    );
    tls_reader
        .get_mut()
        .write_all(b"MAIL FROM:<internet-check@external.example>\r\n")
        .await
        .unwrap();
    assert_eq!(
        read_test_smtp_reply(&mut tls_reader).await,
        "250 sender accepted\r\n"
    );
    tls_reader
        .get_mut()
        .write_all(b"RCPT TO:<test@l-p-e.ch>\r\n")
        .await
        .unwrap();
    assert_eq!(
        read_test_smtp_reply(&mut tls_reader).await,
        "250 recipient accepted\r\n"
    );
    tls_reader.get_mut().write_all(b"DATA\r\n").await.unwrap();
    assert_eq!(
        read_test_smtp_reply(&mut tls_reader).await,
        "354 end with <CRLF>.<CRLF>\r\n"
    );
    tls_reader
            .get_mut()
            .write_all(
                b"From: Internet Check <internet-check@external.example>\r\nMessage-ID: <starttls-inbound@external.example>\r\nSubject: STARTTLS inbound\r\n\r\nBody over TLS\r\n.\r\n",
            )
            .await
            .unwrap();
    assert!(read_test_smtp_reply(&mut tls_reader)
        .await
        .starts_with("250 queued as lpe-ct-in-"));
    tls_reader.get_mut().write_all(b"QUIT\r\n").await.unwrap();
    assert_eq!(read_test_smtp_reply(&mut tls_reader).await, "221 bye\r\n");

    server.await.unwrap();
    let request = captured.lock().unwrap().clone().unwrap();
    assert_eq!(request.mail_from, "internet-check@external.example");
    assert_eq!(request.rcpt_to, vec!["test@l-p-e.ch".to_string()]);
    assert_eq!(request.subject, "STARTTLS inbound");
    std::env::remove_var("LPE_CT_PUBLIC_TLS_CERT_PATH");
    std::env::remove_var("LPE_CT_PUBLIC_TLS_KEY_PATH");
    std::env::remove_var("LPE_INTEGRATION_SHARED_SECRET");
}

#[derive(Debug, Clone)]
struct FakeDetector {
    detection: Result<MagikaDetection, String>,
}

impl Detector for FakeDetector {
    fn detect(&self, _source: DetectionSource<'_>) -> anyhow::Result<MagikaDetection> {
        self.detection.clone().map_err(anyhow::Error::msg)
    }
}

#[tokio::test]
async fn outbound_handoff_relays_message() {
    let spool = temp_dir("outbound-relay");
    initialize_spool(&spool).unwrap();
    let captured = Arc::new(Mutex::new(String::new()));
    let captured_commands = Arc::new(Mutex::new(Vec::<String>::new()));
    let smtp_address = spawn_dummy_smtp_with_profile(DummySmtpProfile {
        captured: Some(captured.clone()),
        captured_commands: Some(captured_commands.clone()),
        ..DummySmtpProfile::default()
    })
    .await;

    let response = process_outbound_handoff(
        &spool,
        &runtime_config(smtp_address.clone(), "http://127.0.0.1:9".to_string()),
        OutboundMessageHandoffRequest {
            queue_id: Uuid::new_v4(),
            message_id: Uuid::new_v4(),
            account_id: Uuid::new_v4(),
            from_address: "sender@example.test".to_string(),
            from_display: Some("Sender".to_string()),
            sender_address: None,
            sender_display: None,
            sender_authorization_kind: "self".to_string(),
            to: vec![TransportRecipient {
                address: "dest@example.test".to_string(),
                display_name: Some("Dest".to_string()),
            }],
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: "Relay test".to_string(),
            body_text: "Body".to_string(),
            body_html_sanitized: None,
            internet_message_id: Some("<relay@test>".to_string()),
            attempt_count: 0,
            last_attempt_error: None,
        },
    )
    .await
    .unwrap();

    assert_eq!(response.status, TransportDeliveryStatus::Relayed);
    assert_eq!(
        response
            .route
            .as_ref()
            .and_then(|route| route.rule_id.as_deref()),
        None
    );
    assert_eq!(
        response
            .route
            .as_ref()
            .and_then(|route| route.relay_target.as_deref()),
        Some(smtp_address.as_str())
    );
    assert_eq!(
        response
            .technical
            .as_ref()
            .and_then(|status| status.smtp_code),
        Some(250)
    );
    assert!(spool
        .join("sent")
        .join(format!("{}.json", response.trace_id))
        .exists());
    let raw = captured.lock().unwrap().clone();
    assert!(raw.contains("Subject: Relay test"));
    assert!(raw.contains("Content-Type: text/plain; charset=utf-8"));
    assert!(raw.contains("Content-Transfer-Encoding: quoted-printable"));
    assert_eq!(
        captured_commands
            .lock()
            .unwrap()
            .first()
            .map(String::as_str),
        Some("EHLO mx.lpe.example")
    );
}

#[tokio::test]
async fn outbound_handoff_replay_after_relay_reuses_sent_custody_without_second_relay() {
    let spool = temp_dir("outbound-replay-sent-custody");
    initialize_spool(&spool).unwrap();
    let captured_commands = Arc::new(Mutex::new(Vec::<String>::new()));
    let smtp_address = spawn_dummy_smtp_with_profile(DummySmtpProfile {
        captured_commands: Some(captured_commands.clone()),
        ..DummySmtpProfile::default()
    })
    .await;
    let config = runtime_config(smtp_address, "http://127.0.0.1:9".to_string());
    let request = outbound_request("Replay sent custody");

    let first = process_outbound_handoff(&spool, &config, request.clone())
        .await
        .unwrap();
    let second = process_outbound_handoff(&spool, &config, request)
        .await
        .unwrap();

    assert_eq!(first.status, TransportDeliveryStatus::Relayed);
    assert_eq!(second.status, TransportDeliveryStatus::Relayed);
    assert_eq!(second.trace_id, first.trace_id);
    assert_eq!(count_queue_json_files(&spool, "sent"), 1);
    assert_eq!(
        captured_commands
            .lock()
            .unwrap()
            .iter()
            .filter(|command| command.as_str() == "DATA")
            .count(),
        1
    );
    let audit =
        std::fs::read_to_string(spool.join("policy").join("transport-audit.jsonl")).unwrap();
    assert!(audit.contains("handoff-replay-suppressed"));
}

#[tokio::test]
async fn outbound_sent_replay_after_restart_preserves_remote_reference_without_second_relay() {
    let spool = temp_dir("outbound-replay-sent-after-restart");
    initialize_spool(&spool).unwrap();
    let captured_commands = Arc::new(Mutex::new(Vec::<String>::new()));
    let smtp_address = spawn_dummy_smtp_with_profile(DummySmtpProfile {
        captured_commands: Some(captured_commands.clone()),
        ..DummySmtpProfile::default()
    })
    .await;
    let request = outbound_request("Replay sent after restart");
    let first = process_outbound_handoff(
        &spool,
        &runtime_config(smtp_address, "http://127.0.0.1:9".to_string()),
        request.clone(),
    )
    .await
    .unwrap();
    let first_remote_ref = first.remote_message_ref.clone();

    initialize_spool(&spool).unwrap();
    let second = process_outbound_handoff(
        &spool,
        &runtime_config("127.0.0.1:9".to_string(), "http://127.0.0.1:9".to_string()),
        request,
    )
    .await
    .unwrap();

    assert_eq!(first.status, TransportDeliveryStatus::Relayed);
    assert_eq!(second.status, TransportDeliveryStatus::Relayed);
    assert_eq!(second.trace_id, first.trace_id);
    assert_eq!(second.remote_message_ref, first_remote_ref);
    assert_eq!(count_queue_json_files(&spool, "sent"), 1);
    assert_eq!(count_queue_json_files(&spool, "outbound"), 0);
    assert_eq!(count_queue_json_files(&spool, "deferred"), 0);
    assert_eq!(
        captured_commands
            .lock()
            .unwrap()
            .iter()
            .filter(|command| command.as_str() == "DATA")
            .count(),
        1
    );
}

#[tokio::test]
async fn terminal_outbound_custody_queues_do_not_regress_after_restart() {
    let cases = [
        (
            "bounces",
            "bounced",
            TransportDeliveryStatus::Bounced,
            Some("remote-bounce-ref"),
        ),
        ("held", "held", TransportDeliveryStatus::Failed, None),
        (
            "quarantine",
            "quarantined",
            TransportDeliveryStatus::Quarantined,
            None,
        ),
    ];

    for (queue, message_status, expected_status, remote_ref) in cases {
        let spool = temp_dir(&format!("outbound-terminal-restart-{queue}"));
        initialize_spool(&spool).unwrap();
        let request = outbound_request(&format!("Terminal restart {queue}"));
        let trace_id = format!("lpe-ct-out-{}", request.queue_id);
        let message = outbound_terminal_test_message(&trace_id, message_status, remote_ref);
        persist_message(&spool, queue, &message).await.unwrap();

        initialize_spool(&spool).unwrap();
        let response = process_outbound_handoff(
            &spool,
            &runtime_config("127.0.0.1:9".to_string(), "http://127.0.0.1:9".to_string()),
            request,
        )
        .await
        .unwrap();

        assert_eq!(response.status, expected_status);
        assert_eq!(response.trace_id, trace_id);
        assert_eq!(response.remote_message_ref, remote_ref.map(str::to_string));
        assert!(spool.join(queue).join(format!("{trace_id}.json")).exists());
        assert_eq!(count_queue_json_files(&spool, "outbound"), 0);
        assert_eq!(count_queue_json_files(&spool, "deferred"), 0);
    }
}

#[tokio::test]
#[ignore = "env-sensitive"]
async fn smtp_session_rejects_when_ha_role_is_standby() {
    let _guard = env_test_lock();
    let spool = temp_dir("smtp-standby");
    initialize_spool(&spool).unwrap();
    let role_file = spool.join("ha-role");
    std::fs::write(&role_file, b"standby\n").unwrap();
    std::env::set_var("LPE_CT_HA_ROLE_FILE", &role_file);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let spool_for_server = spool.clone();
    let server = tokio::spawn(async move {
        let (stream, peer) = listener.accept().await.unwrap();
        handle_smtp_session(
            stream,
            peer,
            runtime_store_with_accepted_domains(&[]),
            spool_for_server,
            None,
        )
        .await
        .unwrap();
    });

    let client = TcpStream::connect(address).await.unwrap();
    let mut reader = BufReader::new(client);
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    assert!(line.starts_with("421 node role standby"));

    server.await.unwrap();
    std::env::remove_var("LPE_CT_HA_ROLE_FILE");
}

#[tokio::test]
async fn outbound_handoff_quarantines_message() {
    let spool = temp_dir("outbound-quarantine");
    initialize_spool(&spool).unwrap();

    let response = process_outbound_handoff(
        &spool,
        &runtime_config("127.0.0.1:9".to_string(), "http://127.0.0.1:9".to_string()),
        OutboundMessageHandoffRequest {
            queue_id: Uuid::new_v4(),
            message_id: Uuid::new_v4(),
            account_id: Uuid::new_v4(),
            from_address: "sender@example.test".to_string(),
            from_display: None,
            sender_address: None,
            sender_display: None,
            sender_authorization_kind: "self".to_string(),
            to: vec![TransportRecipient {
                address: "dest@example.test".to_string(),
                display_name: None,
            }],
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: "[quarantine] Test".to_string(),
            body_text: "Body".to_string(),
            body_html_sanitized: None,
            internet_message_id: None,
            attempt_count: 0,
            last_attempt_error: None,
        },
    )
    .await
    .unwrap();

    assert_eq!(response.status, TransportDeliveryStatus::Quarantined);
    assert!(spool
        .join("quarantine")
        .join(format!("{}.json", response.trace_id))
        .exists());
}

#[tokio::test]
async fn outbound_handoff_bounces_on_permanent_rcpt_failure() {
    let spool = temp_dir("outbound-bounce");
    initialize_spool(&spool).unwrap();
    let smtp_address = spawn_dummy_smtp_with_profile(DummySmtpProfile {
        rcpt_reply: "550 5.1.1 user unknown".to_string(),
        ..DummySmtpProfile::default()
    })
    .await;

    let response = process_outbound_handoff(
        &spool,
        &runtime_config(smtp_address.clone(), "http://127.0.0.1:9".to_string()),
        outbound_request("Bounce test"),
    )
    .await
    .unwrap();

    assert_eq!(response.status, TransportDeliveryStatus::Bounced);
    assert_eq!(
        response.dsn.as_ref().map(|dsn| dsn.status.as_str()),
        Some("5.1.1")
    );
    assert_eq!(
        response
            .technical
            .as_ref()
            .and_then(|status| status.smtp_code),
        Some(550)
    );
    assert!(spool
        .join("bounces")
        .join(format!("{}.json", response.trace_id))
        .exists());
}

#[tokio::test]
async fn outbound_handoff_defers_when_local_throttle_hits() {
    let spool = temp_dir("outbound-throttle");
    initialize_spool(&spool).unwrap();
    let smtp_address = spawn_dummy_smtp(Arc::new(Mutex::new(String::new()))).await;
    let mut config = runtime_config(smtp_address, "http://127.0.0.1:9".to_string());
    config.throttle_enabled = true;
    config.throttle_rules = vec![OutboundThrottleRule {
        id: "recipient-domain".to_string(),
        scope: "recipient-domain".to_string(),
        recipient_domain: None,
        sender_domain: None,
        max_messages: 1,
        window_seconds: 300,
        retry_after_seconds: 120,
    }];

    let first = process_outbound_handoff(&spool, &config, outbound_request("First"))
        .await
        .unwrap();
    let second = process_outbound_handoff(&spool, &config, outbound_request("Second"))
        .await
        .unwrap();

    assert_eq!(first.status, TransportDeliveryStatus::Relayed);
    assert_eq!(second.status, TransportDeliveryStatus::Deferred);
    assert_eq!(
        second
            .throttle
            .as_ref()
            .map(|throttle| throttle.retry_after_seconds),
        Some(120)
    );
    assert_eq!(
        second.retry.as_ref().map(|retry| retry.policy.as_str()),
        Some("throttle")
    );
}

#[tokio::test]
async fn outbound_handoff_replay_for_deferred_message_does_not_duplicate_custody() {
    let spool = temp_dir("outbound-replay-deferred-custody");
    initialize_spool(&spool).unwrap();
    let smtp_address = spawn_dummy_smtp_with_profile(DummySmtpProfile {
        final_reply: "451 4.4.1 try again later".to_string(),
        ..DummySmtpProfile::default()
    })
    .await;
    let config = runtime_config(smtp_address, "http://127.0.0.1:9".to_string());
    let request = outbound_request("Replay deferred custody");

    let first = process_outbound_handoff(&spool, &config, request.clone())
        .await
        .unwrap();
    let second = process_outbound_handoff(&spool, &config, request)
        .await
        .unwrap();

    assert_eq!(first.status, TransportDeliveryStatus::Deferred);
    assert_eq!(second.status, TransportDeliveryStatus::Deferred);
    assert_eq!(second.trace_id, first.trace_id);
    assert!(second.retry.is_some());
    assert_eq!(count_queue_json_files(&spool, "deferred"), 1);
    assert_eq!(count_queue_json_files(&spool, "outbound"), 0);
    assert_eq!(count_queue_json_files(&spool, "sent"), 0);
    let audit =
        std::fs::read_to_string(spool.join("policy").join("transport-audit.jsonl")).unwrap();
    assert!(audit.contains("handoff-replay-suppressed"));
}

#[tokio::test]
async fn outbound_handoff_uses_matching_routing_rule() {
    let spool = temp_dir("outbound-routing");
    initialize_spool(&spool).unwrap();
    let smtp_address = spawn_dummy_smtp(Arc::new(Mutex::new(String::new()))).await;
    let mut config = runtime_config("127.0.0.1:9".to_string(), "http://127.0.0.1:9".to_string());
    config.routing_rules = vec![OutboundRoutingRule {
        id: "example-route".to_string(),
        sender_domain: None,
        recipient_domain: Some("example.test".to_string()),
        relay_target: smtp_address.clone(),
    }];

    let response = process_outbound_handoff(&spool, &config, outbound_request("Routed"))
        .await
        .unwrap();

    assert_eq!(response.status, TransportDeliveryStatus::Relayed);
    assert_eq!(
        response
            .route
            .as_ref()
            .and_then(|route| route.rule_id.as_deref()),
        Some("example-route")
    );
    assert_eq!(
        response
            .route
            .as_ref()
            .and_then(|route| route.relay_target.as_deref()),
        Some(smtp_address.as_str())
    );
}

#[test]
fn outbound_route_without_smart_host_uses_direct_mx_default() {
    let config = runtime_config(String::new(), "http://127.0.0.1:9".to_string());
    let route = resolve_outbound_route(&config, &outbound_request("Direct MX"));

    assert_eq!(route.rule_id, None);
    assert_eq!(route.relay_target, None);
    assert_eq!(route.queue, "outbound");
}

#[tokio::test]
#[ignore = "env-sensitive"]
async fn outbound_handoff_delivers_accepted_domain_locally_without_direct_mx() {
    let _guard = env_test_lock();
    let spool = temp_dir("outbound-local-domain");
    initialize_spool(&spool).unwrap();
    std::env::set_var(
        "LPE_INTEGRATION_SHARED_SECRET",
        "0123456789abcdef0123456789abcdef",
    );
    let captured = Arc::new(Mutex::new(None::<InboundDeliveryRequest>));
    let core_base_url = spawn_dummy_core(captured.clone()).await;
    let mut config = runtime_config(String::new(), core_base_url);
    config.accepted_domains = vec![AcceptedDomainConfig {
        domain: "l-p-e.ch".to_string(),
        rbl_checks: false,
        spf_checks: false,
        greylisting: false,
        accept_null_reverse_path: true,
        verified: true,
    }];
    let mut request = outbound_request("Microsoft Outlook Test Message");
    request.from_address = "test@l-p-e.ch".to_string();
    request.to = vec![TransportRecipient {
        address: "test@l-p-e.ch".to_string(),
        display_name: None,
    }];

    let response = process_outbound_handoff(&spool, &config, request)
        .await
        .unwrap();

    assert_eq!(response.status, TransportDeliveryStatus::Relayed);
    assert_eq!(
        response
            .route
            .as_ref()
            .and_then(|route| route.relay_target.as_deref()),
        Some("local-core")
    );
    assert_eq!(
        response
            .technical
            .as_ref()
            .map(|status| status.phase.as_str()),
        Some("local-delivery")
    );
    let delivered = captured.lock().unwrap().clone().unwrap();
    assert_eq!(delivered.mail_from, "test@l-p-e.ch");
    assert_eq!(delivered.rcpt_to, vec!["test@l-p-e.ch".to_string()]);
    assert_eq!(delivered.subject, "Microsoft Outlook Test Message");
    assert!(spool
        .join("sent")
        .join(format!("{}.json", response.trace_id))
        .exists());
    std::env::remove_var("LPE_INTEGRATION_SHARED_SECRET");
}

#[tokio::test]
#[ignore = "env-sensitive"]
async fn inbound_message_posts_to_core_delivery_api() {
    let _guard = env_test_lock();
    let spool = temp_dir("inbound-delivery");
    initialize_spool(&spool).unwrap();
    std::env::set_var(
        "LPE_INTEGRATION_SHARED_SECRET",
        "0123456789abcdef0123456789abcdef",
    );
    let captured = Arc::new(Mutex::new(None::<InboundDeliveryRequest>));
    let core_base_url = spawn_dummy_core(captured.clone()).await;

    let message = receive_message(
        &spool,
        &runtime_config("127.0.0.1:9".to_string(), core_base_url),
        "127.0.0.1:2525".to_string(),
        "example.test".to_string(),
        "sender@example.test".to_string(),
        vec!["dest@example.test".to_string()],
        b"From: Sender <sender@example.test>\r\nSubject: Inbound\r\n\r\nBody".to_vec(),
    )
    .await
    .unwrap();

    assert_eq!(message.status, "sent");
    assert!(spool
        .join("sent")
        .join(format!("{}.json", message.id))
        .exists());
    let request = captured.lock().unwrap().clone().unwrap();
    assert_eq!(request.subject, "Inbound");
    assert_eq!(request.body_text, "Body");
    assert_eq!(request.rcpt_to, vec!["dest@example.test".to_string()]);
    std::env::remove_var("LPE_INTEGRATION_SHARED_SECRET");
}

#[test]
fn inbound_mismatch_is_rejected_before_delivery() {
    let validator = Validator::new(
        FakeDetector {
            detection: Ok(MagikaDetection {
                label: "exe".to_string(),
                mime_type: "application/x-msdownload".to_string(),
                description: "Executable".to_string(),
                group: "binary".to_string(),
                extensions: vec!["exe".to_string()],
                score: Some(0.99),
            }),
        },
        0.80,
    );
    let mime = concat!(
        "Content-Type: multipart/mixed; boundary=\"abc\"\r\n",
        "\r\n",
        "--abc\r\n",
        "Content-Type: application/pdf; name=\"invoice.pdf\"\r\n",
        "Content-Disposition: attachment; filename=\"invoice.pdf\"\r\n",
        "\r\n",
        "%PDF-1.7\r\n",
        "--abc--\r\n"
    );

    let outcome = classify_inbound_message(&validator, mime.as_bytes()).unwrap();
    assert!(matches!(outcome, super::InboundMagikaOutcome::Reject(_)));
}

#[tokio::test]
async fn inbound_magika_failure_is_quarantined() {
    let spool = temp_dir("inbound-quarantine-magika");
    initialize_spool(&spool).unwrap();
    let validator = Validator::new(
        FakeDetector {
            detection: Err("binary unavailable".to_string()),
        },
        0.80,
    );

    let message = receive_message_with_validator(
        &validator,
        &spool,
        &runtime_config("127.0.0.1:9".to_string(), "http://127.0.0.1:9".to_string()),
        "127.0.0.1:2525".to_string(),
        "example.test".to_string(),
        "sender@example.test".to_string(),
        vec!["dest@example.test".to_string()],
        concat!(
            "Content-Type: multipart/mixed; boundary=\"abc\"\r\n",
            "\r\n",
            "--abc\r\n",
            "Content-Type: application/pdf; name=\"invoice.pdf\"\r\n",
            "Content-Disposition: attachment; filename=\"invoice.pdf\"\r\n",
            "\r\n",
            "%PDF-1.7\r\n",
            "--abc--\r\n"
        )
        .as_bytes()
        .to_vec(),
    )
    .await
    .unwrap();

    assert_eq!(message.status, "quarantined");
    assert!(message
        .magika_summary
        .as_deref()
        .unwrap_or_default()
        .contains("Magika validation failed"));
    assert!(spool
        .join("quarantine")
        .join(format!("{}.json", message.id))
        .exists());
}

#[test]
fn outbound_handoff_builds_multipart_alternative_when_html_is_present() {
    let raw = String::from_utf8(compose_rfc822_message(&OutboundMessageHandoffRequest {
        queue_id: Uuid::nil(),
        message_id: Uuid::nil(),
        account_id: Uuid::nil(),
        from_address: "sender@example.test".to_string(),
        from_display: None,
        sender_address: None,
        sender_display: None,
        sender_authorization_kind: "self".to_string(),
        to: vec![TransportRecipient {
            address: "dest@example.test".to_string(),
            display_name: None,
        }],
        cc: Vec::new(),
        bcc: Vec::new(),
        subject: "HTML".to_string(),
        body_text: "Plain body".to_string(),
        body_html_sanitized: Some("<p>HTML body</p>".to_string()),
        internet_message_id: None,
        attempt_count: 0,
        last_attempt_error: None,
    }))
    .unwrap();

    assert!(raw.contains("Content-Type: multipart/alternative;"));
    assert!(raw.contains("Content-Type: text/plain; charset=utf-8"));
    assert!(raw.contains("Content-Type: text/html; charset=utf-8"));
    assert!(!raw.contains("\r\nBcc:"));
}

#[test]
fn outbound_handoff_emits_sender_header_for_delegated_sender() {
    let mut request = outbound_request("Delegated");
    request.sender_address = Some("delegate@other.test".to_string());
    request.sender_display = Some("Delegate".to_string());

    let raw = String::from_utf8(compose_rfc822_message(&request)).unwrap();

    assert!(raw.contains("From: Sender <sender@example.test>"));
    assert!(raw.contains("Sender: Delegate <delegate@other.test>"));
}

#[test]
fn quoted_printable_encoder_handles_utf8_and_line_breaks() {
    let encoded = encode_quoted_printable("Bonjour équipe\nHTML");
    assert!(encoded.contains("=C3=A9"));
    assert!(encoded.contains("\r\n"));
}

#[tokio::test]
#[ignore = "env-sensitive"]
async fn inbound_message_keeps_non_utf8_raw_bytes() {
    let _guard = env_test_lock();
    let spool = temp_dir("inbound-non-utf8");
    initialize_spool(&spool).unwrap();
    std::env::set_var(
        "LPE_INTEGRATION_SHARED_SECRET",
        "0123456789abcdef0123456789abcdef",
    );
    let captured = Arc::new(Mutex::new(None::<InboundDeliveryRequest>));
    let core_base_url = spawn_dummy_core(captured.clone()).await;
    let validator = Validator::new(
        FakeDetector {
            detection: Ok(MagikaDetection {
                label: "bin".to_string(),
                mime_type: "application/octet-stream".to_string(),
                description: "Binary".to_string(),
                group: "binary".to_string(),
                extensions: vec!["bin".to_string()],
                score: Some(0.99),
            }),
        },
        0.80,
    );
    let mut raw = b"From: Sender <sender@example.test>\r\nSubject: Binary\r\nContent-Type: multipart/mixed; boundary=\"b1\"\r\n\r\n--b1\r\nContent-Type: text/plain; charset=utf-8\r\n\r\nVisible body\r\n--b1\r\nContent-Type: application/octet-stream\r\nContent-Disposition: attachment; filename=\"blob.bin\"\r\n\r\n".to_vec();
    raw.extend_from_slice(&[0xff, 0xfe, 0x00, 0x41]);
    raw.extend_from_slice(b"\r\n--b1--\r\n");

    let message = receive_message_with_validator(
        &validator,
        &spool,
        &runtime_config("127.0.0.1:9".to_string(), core_base_url),
        "127.0.0.1:2525".to_string(),
        "example.test".to_string(),
        "sender@example.test".to_string(),
        vec!["dest@example.test".to_string()],
        raw.clone(),
    )
    .await
    .unwrap();

    assert_eq!(message.status, "sent");
    let request = captured.lock().unwrap().clone().unwrap();
    assert_eq!(request.body_text, "Visible body");
    assert_eq!(request.raw_message, raw);
    std::env::remove_var("LPE_INTEGRATION_SHARED_SECRET");
}

#[tokio::test]
async fn greylisting_defers_first_triplet_then_allows_after_release_window() {
    let spool = temp_dir("greylisting");
    initialize_spool(&spool).unwrap();
    let config = runtime_config("127.0.0.1:9".to_string(), "http://127.0.0.1:9".to_string());
    let ip: IpAddr = "192.0.2.45".parse().unwrap();
    let rcpt = vec!["dest@example.test".to_string()];

    let first = evaluate_greylisting(&spool, &config, ip, "sender@example.test", &rcpt)
        .await
        .unwrap();
    assert!(first.unwrap().contains("greylisted triplet"));

    let key = stable_key_id(&(
        ip,
        "sender@example.test".to_string(),
        "dest@example.test".to_string(),
    ));
    let path = spool.join("greylist").join(format!("{key}.json"));
    let mut entry: GreylistEntry =
        serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    entry.release_after_unix = unix_now().saturating_sub(1);
    std::fs::write(&path, serde_json::to_string_pretty(&entry).unwrap()).unwrap();

    let second = evaluate_greylisting(&spool, &config, ip, "sender@example.test", &rcpt)
        .await
        .unwrap();
    assert!(second.is_none());
}

#[tokio::test]
async fn reputation_score_penalizes_quarantine_and_rejects() {
    let spool = temp_dir("reputation");
    initialize_spool(&spool).unwrap();
    let config = runtime_config("127.0.0.1:9".to_string(), "http://127.0.0.1:9".to_string());
    let mut message = QueuedMessage {
        id: "trace-1".to_string(),
        direction: "inbound".to_string(),
        received_at: "unix:1".to_string(),
        peer: "192.0.2.10:25".to_string(),
        helo: "mx.example.test".to_string(),
        mail_from: "sender@example.test".to_string(),
        rcpt_to: vec!["dest@example.test".to_string()],
        status: "incoming".to_string(),
        relay_error: None,
        magika_summary: None,
        magika_decision: None,
        spam_score: 0.0,
        security_score: 0.0,
        reputation_score: 0,
        dnsbl_hits: Vec::new(),
        auth_summary: AuthSummary::default(),
        decision_trace: Vec::new(),
        remote_message_ref: None,
        technical_status: None,
        dsn: None,
        route: None,
        throttle: None,
        data: b"Subject: test\r\n\r\nbody".to_vec(),
    };

    update_reputation(&spool, &config, &message, FilterAction::Accept)
        .await
        .unwrap();
    update_reputation(&spool, &config, &message, FilterAction::Quarantine)
        .await
        .unwrap();
    message.id = "trace-2".to_string();
    update_reputation(&spool, &config, &message, FilterAction::Reject)
        .await
        .unwrap();

    let score = load_reputation_score(
        &spool,
        &config,
        parse_peer_ip(&message.peer),
        &message.mail_from,
    )
    .await
    .unwrap();
    assert_eq!(score, -4);
}

#[tokio::test]
async fn bayespam_learns_tokens_and_scores_spammy_message() {
    let spool = temp_dir("bayespam-train");
    initialize_spool(&spool).unwrap();
    let config = runtime_config("127.0.0.1:9".to_string(), "http://127.0.0.1:9".to_string());

    train_bayespam(
        &spool,
        &config,
        &training_message("Weekly report", "meeting agenda project status"),
        BayesLabel::Ham,
    )
    .await
    .unwrap();
    train_bayespam(
        &spool,
        &config,
        &training_message("Cheap pills", "cheap pills winner casino bonus pills"),
        BayesLabel::Spam,
    )
    .await
    .unwrap();

    let corpus = load_bayespam_corpus(&spool, &config).await.unwrap();
    assert_eq!(corpus.ham_messages, 1);
    assert_eq!(corpus.spam_messages, 1);
    assert!(corpus.spam_tokens.contains_key("cheap"));

    let score = score_bayespam(
        &spool,
        &config,
        "Cheap pills offer",
        "casino bonus cheap pills now",
        "sender@example.test",
        "mx.example.test",
    )
    .await
    .unwrap()
    .unwrap();

    assert!(score.probability > 0.80);
    assert!(score.contribution > 3.0);
}

#[tokio::test]
async fn bayespam_requires_enough_content_evidence_before_contributing() {
    let spool = temp_dir("bayespam-short-message");
    initialize_spool(&spool).unwrap();
    let config = runtime_config("127.0.0.1:9".to_string(), "http://127.0.0.1:9".to_string());

    train_bayespam(
        &spool,
        &config,
        &training_message("Weekly report", "meeting agenda project status"),
        BayesLabel::Ham,
    )
    .await
    .unwrap();
    train_bayespam(
        &spool,
        &config,
        &training_message("Test to Infomaniak", "test infomaniak"),
        BayesLabel::Spam,
    )
    .await
    .unwrap();

    let score = score_bayespam(
        &spool,
        &config,
        "Test to Infomaniak",
        "test",
        "test@l-p-e.ch",
        "lpe-core",
    )
    .await
    .unwrap()
    .unwrap();

    assert!(score.matched_tokens < BAYESPAM_MIN_SCORING_TOKENS);
    assert_eq!(score.contribution, 0.0);
}

#[tokio::test]
async fn outbound_handoff_quarantines_on_bayespam_score() {
    let spool = temp_dir("outbound-bayespam");
    initialize_spool(&spool).unwrap();
    let mut config = runtime_config("127.0.0.1:9".to_string(), "http://127.0.0.1:9".to_string());
    config.spam_quarantine_threshold = 4.0;

    train_bayespam(
        &spool,
        &config,
        &training_message("Project update", "meeting notes roadmap delivery"),
        BayesLabel::Ham,
    )
    .await
    .unwrap();
    train_bayespam(
        &spool,
        &config,
        &training_message("Cheap pills", "cheap pills winner casino bonus pills"),
        BayesLabel::Spam,
    )
    .await
    .unwrap();

    let mut request = outbound_request("Cheap pills now");
    request.body_text = "cheap pills winner casino bonus".to_string();
    let response = process_outbound_handoff(&spool, &config, request)
        .await
        .unwrap();

    assert_eq!(response.status, TransportDeliveryStatus::Quarantined);
    assert!(response
        .detail
        .as_deref()
        .unwrap_or_default()
        .contains("bayespam score"));
}

#[tokio::test]
async fn outbound_handoff_rejects_blocked_delegated_sender() {
    let spool = temp_dir("outbound-blocked-delegate");
    initialize_spool(&spool).unwrap();
    let mut config = runtime_config("127.0.0.1:9".to_string(), "http://127.0.0.1:9".to_string());
    config.address_policy.block_senders = vec!["other.test".to_string()];
    let mut request = outbound_request("Delegated block");
    request.sender_address = Some("delegate@other.test".to_string());
    request.sender_display = Some("Delegate".to_string());

    let response = process_outbound_handoff(&spool, &config, request)
        .await
        .unwrap();

    assert_eq!(response.status, TransportDeliveryStatus::Failed);
    assert!(response
        .detail
        .as_deref()
        .unwrap_or_default()
        .contains("sender delegate@other.test matched block list entry other.test"));
}

#[tokio::test]
async fn retry_trace_clears_stale_execution_state_and_appends_audit() {
    let spool = temp_dir("trace-retry");
    initialize_spool(&spool).unwrap();
    let config = runtime_config("127.0.0.1:9".to_string(), "http://127.0.0.1:9".to_string());
    let message = QueuedMessage {
        id: "trace-retry-1".to_string(),
        direction: "outbound".to_string(),
        received_at: "unix:1".to_string(),
        peer: "lpe-core".to_string(),
        helo: "lpe-core".to_string(),
        mail_from: "sender@example.test".to_string(),
        rcpt_to: vec!["dest@example.test".to_string()],
        status: "held".to_string(),
        relay_error: Some("remote relay failed".to_string()),
        magika_summary: None,
        magika_decision: None,
        spam_score: 0.0,
        security_score: 0.0,
        reputation_score: 0,
        dnsbl_hits: Vec::new(),
        auth_summary: AuthSummary::default(),
        decision_trace: Vec::new(),
        remote_message_ref: Some("remote-ref".to_string()),
        technical_status: Some(TransportTechnicalStatus {
            phase: "data".to_string(),
            smtp_code: Some(451),
            enhanced_code: Some("4.4.1".to_string()),
            remote_host: Some("mx.example.test:25".to_string()),
            detail: Some("temporary relay failure".to_string()),
        }),
        dsn: Some(TransportDsnReport {
            action: "delayed".to_string(),
            status: "4.4.1".to_string(),
            diagnostic_code: Some("smtp; temporary relay failure".to_string()),
            remote_mta: Some("mx.example.test".to_string()),
        }),
        route: Some(TransportRouteDecision {
            rule_id: Some("primary".to_string()),
            relay_target: Some("mx.example.test:25".to_string()),
            queue: "outbound".to_string(),
        }),
        throttle: Some(TransportThrottleStatus {
            scope: "sender".to_string(),
            key: "sender@example.test".to_string(),
            limit: 1,
            window_seconds: 60,
            retry_after_seconds: 45,
        }),
        data: b"Subject: Retry\r\n\r\nbody".to_vec(),
    };
    persist_message(&spool, "held", &message).await.unwrap();

    let result = retry_trace(&spool, &config, &message.id)
        .await
        .unwrap()
        .unwrap();
    let details = load_trace_details(&spool, &message.id).unwrap().unwrap();
    let audit =
        std::fs::read_to_string(spool.join("policy").join("transport-audit.jsonl")).unwrap();

    assert_eq!(result.from_queue, "held");
    assert_eq!(result.to_queue, "outbound");
    assert_eq!(details.queue, "outbound");
    assert_eq!(details.status, "outbound");
    assert!(details.reason.is_none());
    assert!(details.remote_message_ref.is_none());
    assert!(details.technical_status.is_none());
    assert!(details.dsn.is_none());
    assert!(details.route.is_none());
    assert!(details.throttle.is_none());
    assert!(details
        .decision_trace
        .iter()
        .any(|entry| entry.stage == "operator-action" && entry.outcome == "retry"));
    assert!(audit.contains("\"trace_id\":\"trace-retry-1\""));
    assert!(audit.contains("\"queue\":\"outbound\""));
    assert!(audit.contains("\"status\":\"outbound\""));
}

#[tokio::test]
async fn release_trace_moves_quarantined_inbound_back_to_incoming_and_appends_audit() {
    let spool = temp_dir("trace-release");
    initialize_spool(&spool).unwrap();
    let config = runtime_config("127.0.0.1:9".to_string(), "http://127.0.0.1:9".to_string());
    let message = QueuedMessage {
        id: "trace-release-1".to_string(),
        direction: "inbound".to_string(),
        received_at: "unix:1".to_string(),
        peer: "203.0.113.10:25".to_string(),
        helo: "mx.example.test".to_string(),
        mail_from: "sender@example.test".to_string(),
        rcpt_to: vec!["dest@example.test".to_string()],
        status: "quarantined".to_string(),
        relay_error: Some("policy quarantine".to_string()),
        magika_summary: None,
        magika_decision: None,
        spam_score: 7.0,
        security_score: 4.0,
        reputation_score: -3,
        dnsbl_hits: vec!["zen.spamhaus.org".to_string()],
        auth_summary: AuthSummary::default(),
        decision_trace: Vec::new(),
        remote_message_ref: Some("remote-ref".to_string()),
        technical_status: Some(TransportTechnicalStatus {
            phase: "data".to_string(),
            smtp_code: Some(554),
            enhanced_code: Some("5.7.1".to_string()),
            remote_host: Some("mx.example.test:25".to_string()),
            detail: Some("quarantined by policy".to_string()),
        }),
        dsn: Some(TransportDsnReport {
            action: "failed".to_string(),
            status: "5.7.1".to_string(),
            diagnostic_code: Some("smtp; quarantined by policy".to_string()),
            remote_mta: Some("mx.example.test".to_string()),
        }),
        route: Some(TransportRouteDecision {
            rule_id: Some("primary".to_string()),
            relay_target: Some("mx.example.test:25".to_string()),
            queue: "incoming".to_string(),
        }),
        throttle: Some(TransportThrottleStatus {
            scope: "recipient-domain".to_string(),
            key: "example.test".to_string(),
            limit: 10,
            window_seconds: 60,
            retry_after_seconds: 30,
        }),
        data: b"Subject: Release\r\n\r\nbody".to_vec(),
    };
    persist_message(&spool, "quarantine", &message)
        .await
        .unwrap();

    let result = release_trace(&spool, &config, &message.id)
        .await
        .unwrap()
        .unwrap();
    let details = load_trace_details(&spool, &message.id).unwrap().unwrap();
    let audit =
        std::fs::read_to_string(spool.join("policy").join("transport-audit.jsonl")).unwrap();

    assert_eq!(result.from_queue, "quarantine");
    assert_eq!(result.to_queue, "incoming");
    assert_eq!(details.queue, "incoming");
    assert_eq!(details.status, "incoming");
    assert!(details.reason.is_none());
    assert!(details.remote_message_ref.is_none());
    assert!(details.technical_status.is_none());
    assert!(details.dsn.is_none());
    assert!(details.route.is_none());
    assert!(details.throttle.is_none());
    assert!(details
        .decision_trace
        .iter()
        .any(|entry| entry.stage == "operator-action" && entry.outcome == "release"));
    assert!(audit.contains("\"trace_id\":\"trace-release-1\""));
    assert!(audit.contains("\"queue\":\"incoming\""));
    assert!(audit.contains("\"status\":\"incoming\""));
}

#[tokio::test]
async fn rejected_quarantine_trace_recovers_from_spool_until_operator_delete() {
    let spool = temp_dir("trace-reject-delete-recovery");
    initialize_spool(&spool).unwrap();
    let config = runtime_config("127.0.0.1:9".to_string(), "http://127.0.0.1:9".to_string());
    let message = QueuedMessage {
        id: "trace-reject-delete-1".to_string(),
        direction: "inbound".to_string(),
        received_at: "unix:1".to_string(),
        peer: "203.0.113.10:25".to_string(),
        helo: "mx.example.test".to_string(),
        mail_from: "sender@example.test".to_string(),
        rcpt_to: vec!["dest@example.test".to_string()],
        status: "rejected".to_string(),
        relay_error: Some("perimeter policy reject".to_string()),
        magika_summary: None,
        magika_decision: None,
        spam_score: 10.0,
        security_score: 6.0,
        reputation_score: -8,
        dnsbl_hits: Vec::new(),
        auth_summary: AuthSummary::default(),
        decision_trace: vec![DecisionTraceEntry {
            stage: "final-policy".to_string(),
            outcome: "reject".to_string(),
            detail: "perimeter policy reject".to_string(),
        }],
        remote_message_ref: None,
        technical_status: None,
        dsn: None,
        route: None,
        throttle: None,
        data: b"Subject: Rejected\r\n\r\nbody".to_vec(),
    };
    persist_message(&spool, "quarantine", &message)
        .await
        .unwrap();

    let recovered = load_trace_details(&spool, &message.id).unwrap().unwrap();
    assert_eq!(recovered.queue, "quarantine");
    assert_eq!(recovered.status, "rejected");
    assert_eq!(recovered.reason.as_deref(), Some("perimeter policy reject"));

    let result = delete_trace(&spool, &config, &message.id)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(result.from_queue, "quarantine");
    assert_eq!(result.status, "deleted");
    assert!(!spool
        .join("quarantine")
        .join("trace-reject-delete-1.json")
        .exists());
    let audit =
        std::fs::read_to_string(spool.join("policy").join("transport-audit.jsonl")).unwrap();
    assert!(audit.contains("\"trace_id\":\"trace-reject-delete-1\""));
    assert!(audit.contains("\"queue\":\"deleted\""));
    assert!(audit.contains("\"status\":\"deleted\""));
}

#[tokio::test]
async fn quarantine_release_reject_delete_recovers_across_node_replacement() {
    let spool = temp_dir("quarantine-node-replacement");
    initialize_spool(&spool).unwrap();
    let replacement_config =
        runtime_config("127.0.0.1:9".to_string(), "http://127.0.0.1:9".to_string());

    let mut release = inbound_test_message(
        "trace-quarantine-release-node-1",
        "quarantined",
        "Release after node replacement",
    );
    release.relay_error = Some("policy quarantine".to_string());
    persist_message(&spool, "quarantine", &release)
        .await
        .unwrap();

    initialize_spool(&spool).unwrap();
    let release_result = release_trace(&spool, &replacement_config, &release.id)
        .await
        .unwrap()
        .unwrap();
    let released = load_trace_details(&spool, &release.id).unwrap().unwrap();
    assert_eq!(release_result.from_queue, "quarantine");
    assert_eq!(release_result.to_queue, "incoming");
    assert_eq!(released.queue, "incoming");
    assert_eq!(released.status, "incoming");

    let mut rejected = inbound_test_message(
        "trace-quarantine-reject-node-1",
        "rejected",
        "Reject after node replacement",
    );
    rejected.relay_error = Some("perimeter policy reject".to_string());
    rejected.decision_trace.push(DecisionTraceEntry {
        stage: "final-policy".to_string(),
        outcome: "reject".to_string(),
        detail: "perimeter policy reject".to_string(),
    });
    persist_message(&spool, "quarantine", &rejected)
        .await
        .unwrap();

    initialize_spool(&spool).unwrap();
    let recovered = load_trace_details(&spool, &rejected.id).unwrap().unwrap();
    assert_eq!(recovered.queue, "quarantine");
    assert_eq!(recovered.status, "rejected");
    assert_eq!(recovered.reason.as_deref(), Some("perimeter policy reject"));

    let delete_result = delete_trace(&spool, &replacement_config, &rejected.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(delete_result.from_queue, "quarantine");
    assert_eq!(delete_result.status, "deleted");
    assert!(!spool
        .join("quarantine")
        .join(format!("{}.json", rejected.id))
        .exists());
}

#[tokio::test]
async fn delete_trace_removes_held_queue_items() {
    let spool = temp_dir("trace-delete-held");
    initialize_spool(&spool).unwrap();
    let config = runtime_config("127.0.0.1:9".to_string(), "http://127.0.0.1:9".to_string());
    let message = QueuedMessage {
        id: "trace-delete-1".to_string(),
        direction: "outbound".to_string(),
        received_at: "unix:1".to_string(),
        peer: "lpe-core".to_string(),
        helo: "lpe-core".to_string(),
        mail_from: "sender@example.test".to_string(),
        rcpt_to: vec!["dest@example.test".to_string()],
        status: "held".to_string(),
        relay_error: Some("awaiting review".to_string()),
        magika_summary: None,
        magika_decision: None,
        spam_score: 0.0,
        security_score: 0.0,
        reputation_score: 0,
        dnsbl_hits: Vec::new(),
        auth_summary: AuthSummary::default(),
        decision_trace: Vec::new(),
        remote_message_ref: None,
        technical_status: None,
        dsn: None,
        route: None,
        throttle: None,
        data: b"Subject: Delete\r\n\r\nbody".to_vec(),
    };
    persist_message(&spool, "held", &message).await.unwrap();

    let result = delete_trace(&spool, &config, &message.id)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(result.from_queue, "held");
    assert_eq!(result.to_queue, "held");
    assert_eq!(result.status, "deleted");
    assert_eq!(result.detail, "trace deleted from held");
    assert!(!spool.join("held").join("trace-delete-1.json").exists());
    let audit =
        std::fs::read_to_string(spool.join("policy").join("transport-audit.jsonl")).unwrap();
    assert!(audit.contains("\"trace_id\":\"trace-delete-1\""));
    assert!(audit.contains("\"queue\":\"deleted\""));
    assert!(audit.contains("\"status\":\"deleted\""));
}

#[tokio::test]
async fn delete_trace_rejects_sent_history_items() {
    let spool = temp_dir("trace-delete-sent-conflict");
    initialize_spool(&spool).unwrap();
    let config = runtime_config("127.0.0.1:9".to_string(), "http://127.0.0.1:9".to_string());
    let message = QueuedMessage {
        id: "trace-delete-sent-1".to_string(),
        direction: "outbound".to_string(),
        received_at: "unix:1".to_string(),
        peer: "lpe-core".to_string(),
        helo: "lpe-core".to_string(),
        mail_from: "sender@example.test".to_string(),
        rcpt_to: vec!["dest@example.test".to_string()],
        status: "sent".to_string(),
        relay_error: None,
        magika_summary: None,
        magika_decision: None,
        spam_score: 0.0,
        security_score: 0.0,
        reputation_score: 0,
        dnsbl_hits: Vec::new(),
        auth_summary: AuthSummary::default(),
        decision_trace: Vec::new(),
        remote_message_ref: None,
        technical_status: None,
        dsn: None,
        route: None,
        throttle: None,
        data: b"Subject: Sent\r\n\r\nbody".to_vec(),
    };
    persist_message(&spool, "sent", &message).await.unwrap();

    let result = delete_trace(&spool, &config, &message.id)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(result.from_queue, "sent");
    assert!(result.to_queue.is_empty());
    assert_eq!(result.status, "sent");
    assert_eq!(
        result.detail,
        "only active queue custody traces can be deleted"
    );
    assert!(spool.join("sent").join("trace-delete-sent-1.json").exists());
}

#[test]
fn takeri_provider_loads_with_default_command_and_args() {
    let providers = load_antivirus_providers(&["takeri".to_string()]);
    assert_eq!(providers.len(), 1);
    assert_eq!(providers[0].id, "takeri");
    assert_eq!(
        providers[0].command,
        "/opt/lpe-ct/bin/Shuhari-CyberForge-CLI"
    );
    assert_eq!(
        providers[0].args,
        vec!["takeri".to_string(), "scan".to_string()]
    );
}

#[test]
fn antivirus_output_parser_detects_takeri_infections_and_suspicious_files() {
    let provider = AntivirusProviderConfig {
        id: "takeri".to_string(),
        display_name: "takeri".to_string(),
        command: "/opt/lpe-ct/bin/Shuhari-CyberForge-CLI".to_string(),
        args: vec!["takeri".to_string(), "scan".to_string()],
        infected_markers: vec![
            "status: infected".to_string(),
            "infected files detected".to_string(),
            "infected files:".to_string(),
            "critical: malware detected".to_string(),
        ],
        suspicious_markers: vec![
            "status: suspicious".to_string(),
            "suspicious files:".to_string(),
        ],
        clean_markers: vec![
            "status: clean".to_string(),
            "no threats detected".to_string(),
        ],
    };

    let infected = parse_antivirus_output(
        &provider,
        "-------Scan Summary-------\nInfected files: 1\nSuspicious files: 0\n",
        "",
        Some(0),
    )
    .unwrap();
    assert_eq!(infected.decision, AntivirusProviderDecision::Infected);

    let suspicious = parse_antivirus_output(
        &provider,
        "-------Scan Result-------\nStatus: SUSPICIOUS\n",
        "",
        Some(0),
    )
    .unwrap();
    assert_eq!(suspicious.decision, AntivirusProviderDecision::Suspicious);

    let clean = parse_antivirus_output(&provider, "No threats detected.\n", "", Some(0)).unwrap();
    assert_eq!(clean.decision, AntivirusProviderDecision::Clean);
}

#[test]
fn antivirus_output_parser_ignores_negative_takeri_markers() {
    let provider = AntivirusProviderConfig {
        id: "takeri".to_string(),
        display_name: "takeri".to_string(),
        command: "/opt/lpe-ct/bin/Shuhari-CyberForge-CLI".to_string(),
        args: vec!["takeri".to_string(), "scan".to_string()],
        infected_markers: vec![
            "status: infected".to_string(),
            "infected files detected".to_string(),
            "infected files:".to_string(),
            "critical: malware detected".to_string(),
        ],
        suspicious_markers: vec![
            "status: suspicious".to_string(),
            "suspicious files:".to_string(),
        ],
        clean_markers: vec![
            "status: clean".to_string(),
            "no threats detected".to_string(),
        ],
    };

    let clean = parse_antivirus_output(
            &provider,
            "-------Scan Summary-------\nCritical: Malware detected: false\nInfected files: 0\nSuspicious files: 0\n",
            "",
            Some(0),
        )
        .unwrap();
    assert_eq!(clean.decision, AntivirusProviderDecision::Clean);

    let infected = parse_antivirus_output(
        &provider,
        "-------Scan Summary-------\nCritical: Malware detected: true\n",
        "",
        Some(0),
    )
    .unwrap();
    assert_eq!(infected.decision, AntivirusProviderDecision::Infected);
}

#[test]
fn auth_summary_uses_structured_outcomes() {
    assert_eq!(summarize_spf(&SpfResult::Pass), "pass");
    assert_eq!(
        summarize_spf(&SpfResult::Fail {
            explanation: Some("policy".to_string())
        }),
        "fail (policy)"
    );
    assert_eq!(
        summarize_dkim(
            &[DkimResult::Pass {
                domain: "example.test".to_string(),
                selector: "s1".to_string(),
                testing: false,
            }],
            true,
        ),
        "pass (aligned)"
    );
    assert_eq!(summarize_dmarc(DmarcDisposition::Reject), "reject");
    assert_eq!(
        spf_disposition(&SpfResult::SoftFail),
        SpfDisposition::SoftFail
    );
    assert_eq!(dkim_disposition(&[DkimResult::None]), DkimDisposition::None);
}

#[test]
fn auth_tempfail_is_detected_for_defer_logic() {
    let assessment = AuthenticationAssessment {
        spf: SpfDisposition::TempError,
        dkim: DkimDisposition::None,
        dkim_aligned: false,
        spf_aligned: false,
        dmarc: DmarcDisposition::None,
        from_domain: "example.test".to_string(),
        spf_domain: "example.test".to_string(),
    };
    assert!(assessment.has_temporary_failure());
}

#[test]
fn auth_score_application_penalizes_failures_and_alignment_gaps() {
    let assessment = AuthenticationAssessment {
        spf: SpfDisposition::Fail,
        dkim: DkimDisposition::PermFail,
        dkim_aligned: false,
        spf_aligned: false,
        dmarc: DmarcDisposition::Quarantine,
        from_domain: "from.example.test".to_string(),
        spf_domain: "bounce.example.test".to_string(),
    };
    let mut spam_score = 0.0;
    let mut security_score = 0.0;
    let mut trace = Vec::new();

    apply_authentication_scores(
        &assessment,
        &mut spam_score,
        &mut security_score,
        &mut trace,
    );

    assert!(spam_score >= 4.5);
    assert!(security_score >= 5.0);
    assert!(trace.iter().any(|entry| entry.stage == "spf-alignment"));
    assert!(trace.iter().any(|entry| entry.stage == "dkim-alignment"));
}

fn auth_policy_config() -> RuntimeConfig {
    let mut config = runtime_config("127.0.0.1:9".to_string(), "http://127.0.0.1:9".to_string());
    config.spam_reject_threshold = 100.0;
    config.spam_quarantine_threshold = 100.0;
    config.reputation_enabled = false;
    config.defer_on_auth_tempfail = false;
    config
}

fn decide_auth_policy(
    config: &RuntimeConfig,
    assessment: &AuthenticationAssessment,
) -> (FilterAction, Option<String>, Vec<DecisionTraceEntry>) {
    let mut trace = Vec::new();
    let (action, reason) = finalize_policy_decision(
        config,
        Some(assessment),
        0.0,
        0.0,
        0,
        &mut trace,
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    (action, reason, trace)
}

#[test]
fn strict_dmarc_rejects_spoofed_local_from_without_aligned_auth() {
    let config = auth_policy_config();
    let assessment = AuthenticationAssessment {
        spf: SpfDisposition::Fail,
        dkim: DkimDisposition::None,
        dkim_aligned: false,
        spf_aligned: false,
        dmarc: DmarcDisposition::Reject,
        from_domain: "l-p-e.ch".to_string(),
        spf_domain: "l-p-e.ch".to_string(),
    };

    let (action, reason, trace) = decide_auth_policy(&config, &assessment);

    assert_eq!(action, FilterAction::Reject);
    assert_eq!(
        reason.as_deref(),
        Some("DMARC policy requested reject; SPF failed and no aligned DKIM signature passed")
    );
    assert!(trace.iter().any(|entry| {
        entry.stage == "policy-trigger"
            && entry.outcome == "reject"
            && entry.detail == "DMARC policy requested reject"
    }));
}

#[test]
fn external_domain_without_rejecting_dmarc_is_accepted_by_auth_policy() {
    let config = auth_policy_config();
    let assessment = AuthenticationAssessment {
        spf: SpfDisposition::None,
        dkim: DkimDisposition::None,
        dkim_aligned: false,
        spf_aligned: false,
        dmarc: DmarcDisposition::None,
        from_domain: "external.example".to_string(),
        spf_domain: "external.example".to_string(),
    };

    let (action, reason, _) = decide_auth_policy(&config, &assessment);

    assert_eq!(action, FilterAction::Accept);
    assert_eq!(reason, None);
}

#[test]
fn aligned_spf_pass_accepts_message_under_dmarc() {
    let config = auth_policy_config();
    let assessment = AuthenticationAssessment {
        spf: SpfDisposition::Pass,
        dkim: DkimDisposition::None,
        dkim_aligned: false,
        spf_aligned: true,
        dmarc: DmarcDisposition::Pass,
        from_domain: "sender.example".to_string(),
        spf_domain: "sender.example".to_string(),
    };

    let (action, reason, _) = decide_auth_policy(&config, &assessment);

    assert_eq!(action, FilterAction::Accept);
    assert_eq!(reason, None);
}

#[test]
fn aligned_dkim_pass_compensates_for_spf_fail() {
    let config = auth_policy_config();
    let assessment = AuthenticationAssessment {
        spf: SpfDisposition::Fail,
        dkim: DkimDisposition::Pass,
        dkim_aligned: true,
        spf_aligned: false,
        dmarc: DmarcDisposition::Pass,
        from_domain: "sender.example".to_string(),
        spf_domain: "bounce.example".to_string(),
    };

    let (action, reason, _) = decide_auth_policy(&config, &assessment);

    assert_eq!(action, FilterAction::Accept);
    assert_eq!(reason, None);
}

#[test]
fn retry_backoff_grows_with_attempt_count_and_caps() {
    assert_eq!(retry_after_seconds(300, 0), 300);
    assert_eq!(retry_after_seconds(300, 1), 600);
    assert_eq!(retry_after_seconds(300, 3), 2400);
    assert_eq!(retry_after_seconds(300, 9), 3600);
}

#[test]
fn dnsbl_query_name_reverses_ipv4_and_ipv6_addresses() {
    let ipv4: IpAddr = "203.0.113.7".parse().unwrap();
    assert_eq!(
        dnsbl_query_name(ipv4, "zen.spamhaus.org"),
        "7.113.0.203.zen.spamhaus.org"
    );

    let ipv6: IpAddr = "2001:db8::1".parse().unwrap();
    assert!(dnsbl_query_name(ipv6, "dnsbl.example.test").ends_with(".dnsbl.example.test"));
}

async fn spawn_dummy_smtp(captured: Arc<Mutex<String>>) -> String {
    spawn_dummy_smtp_with_profile(DummySmtpProfile {
        captured: Some(captured),
        ..DummySmtpProfile::default()
    })
    .await
}

#[derive(Clone, Default)]
struct DummySmtpProfile {
    captured: Option<Arc<Mutex<String>>>,
    captured_commands: Option<Arc<Mutex<Vec<String>>>>,
    rcpt_reply: String,
    final_reply: String,
}

async fn spawn_dummy_smtp_with_profile(profile: DummySmtpProfile) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        handle_dummy_smtp(stream, profile).await;
    });
    address.to_string()
}

async fn handle_dummy_smtp(stream: TcpStream, profile: DummySmtpProfile) {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    writer.write_all(b"220 dummy\r\n").await.unwrap();

    let mut line = String::new();
    loop {
        line.clear();
        if reader.read_line(&mut line).await.unwrap() == 0 {
            break;
        }
        let trimmed = line.trim_end().to_string();
        if let Some(captured_commands) = &profile.captured_commands {
            captured_commands.lock().unwrap().push(trimmed.clone());
        }
        if trimmed == "DATA" {
            writer.write_all(b"354 data\r\n").await.unwrap();
            let mut data = String::new();
            loop {
                line.clear();
                reader.read_line(&mut line).await.unwrap();
                if line == ".\r\n" {
                    break;
                }
                data.push_str(&line);
            }
            if let Some(captured) = &profile.captured {
                *captured.lock().unwrap() = data;
            }
            let final_reply = if profile.final_reply.is_empty() {
                "250 stored".to_string()
            } else {
                profile.final_reply.clone()
            };
            writer
                .write_all(format!("{final_reply}\r\n").as_bytes())
                .await
                .unwrap();
        } else if trimmed == "QUIT" {
            writer.write_all(b"221 bye\r\n").await.unwrap();
            break;
        } else if trimmed.starts_with("RCPT TO:") && !profile.rcpt_reply.is_empty() {
            writer
                .write_all(format!("{}\r\n", profile.rcpt_reply).as_bytes())
                .await
                .unwrap();
        } else {
            writer.write_all(b"250 ok\r\n").await.unwrap();
        }
    }
}

fn outbound_request(subject: &str) -> OutboundMessageHandoffRequest {
    OutboundMessageHandoffRequest {
        queue_id: Uuid::new_v4(),
        message_id: Uuid::new_v4(),
        account_id: Uuid::new_v4(),
        from_address: "sender@example.test".to_string(),
        from_display: Some("Sender".to_string()),
        sender_address: None,
        sender_display: None,
        sender_authorization_kind: "self".to_string(),
        to: vec![TransportRecipient {
            address: "dest@example.test".to_string(),
            display_name: Some("Dest".to_string()),
        }],
        cc: Vec::new(),
        bcc: Vec::new(),
        subject: subject.to_string(),
        body_text: "Body".to_string(),
        body_html_sanitized: None,
        internet_message_id: Some(format!("<{}@test>", subject.to_ascii_lowercase())),
        attempt_count: 0,
        last_attempt_error: None,
    }
}

fn inbound_test_message(id: &str, status: &str, subject: &str) -> QueuedMessage {
    QueuedMessage {
        id: id.to_string(),
        direction: "inbound".to_string(),
        received_at: "unix:1".to_string(),
        peer: "203.0.113.10:25".to_string(),
        helo: "mx.example.test".to_string(),
        mail_from: "sender@example.test".to_string(),
        rcpt_to: vec!["dest@example.test".to_string()],
        status: status.to_string(),
        relay_error: None,
        magika_summary: None,
        magika_decision: None,
        spam_score: 0.0,
        security_score: 0.0,
        reputation_score: 0,
        dnsbl_hits: Vec::new(),
        auth_summary: AuthSummary::default(),
        decision_trace: vec![DecisionTraceEntry {
            stage: "ingress".to_string(),
            outcome: "accepted".to_string(),
            detail: "message accepted by SMTP edge and persisted to the incoming spool".to_string(),
        }],
        remote_message_ref: None,
        technical_status: None,
        dsn: None,
        route: None,
        throttle: None,
        data: format!("From: sender@example.test\r\nSubject: {subject}\r\n\r\nBody\r\n")
            .into_bytes(),
    }
}

fn outbound_terminal_test_message(
    id: &str,
    status: &str,
    remote_message_ref: Option<&str>,
) -> QueuedMessage {
    QueuedMessage {
        id: id.to_string(),
        direction: "outbound".to_string(),
        received_at: "unix:1".to_string(),
        peer: "lpe-core".to_string(),
        helo: "lpe-core".to_string(),
        mail_from: "sender@example.test".to_string(),
        rcpt_to: vec!["dest@example.test".to_string()],
        status: status.to_string(),
        relay_error: Some(format!("terminal {status} custody")),
        magika_summary: None,
        magika_decision: None,
        spam_score: 0.0,
        security_score: 0.0,
        reputation_score: 0,
        dnsbl_hits: Vec::new(),
        auth_summary: AuthSummary::default(),
        decision_trace: vec![DecisionTraceEntry {
            stage: "outbound-relay".to_string(),
            outcome: status.to_string(),
            detail: format!("terminal {status} custody"),
        }],
        remote_message_ref: remote_message_ref.map(str::to_string),
        technical_status: None,
        dsn: None,
        route: Some(TransportRouteDecision {
            rule_id: None,
            relay_target: Some("mx.example.test:25".to_string()),
            queue: status.to_string(),
        }),
        throttle: None,
        data: b"From: sender@example.test\r\nSubject: Terminal\r\n\r\nBody\r\n".to_vec(),
    }
}

async fn spawn_dummy_core(captured: Arc<Mutex<Option<InboundDeliveryRequest>>>) -> String {
    async fn accept(
        axum::extract::State(captured): axum::extract::State<
            Arc<Mutex<Option<InboundDeliveryRequest>>>,
        >,
        Json(request): Json<InboundDeliveryRequest>,
    ) -> Json<InboundDeliveryResponse> {
        *captured.lock().unwrap() = Some(request.clone());
        Json(InboundDeliveryResponse {
            accepted: true,
            delivered_mailboxes: request.rcpt_to.clone(),
            detail: None,
        })
    }

    let router = Router::new()
        .route("/internal/lpe-ct/inbound-deliveries", post(accept))
        .with_state(captured);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    format!("http://{}", address)
}

async fn spawn_custody_asserting_core(
    spool: PathBuf,
    observed_spool_custody: Arc<Mutex<bool>>,
) -> String {
    async fn accept(
        axum::extract::State((spool, observed_spool_custody)): axum::extract::State<(
            PathBuf,
            Arc<Mutex<bool>>,
        )>,
        Json(request): Json<InboundDeliveryRequest>,
    ) -> Json<InboundDeliveryResponse> {
        let incoming_path = spool
            .join("incoming")
            .join(format!("{}.json", request.trace_id));
        *observed_spool_custody.lock().unwrap() = incoming_path.exists();
        Json(InboundDeliveryResponse {
            accepted: true,
            delivered_mailboxes: request.rcpt_to.clone(),
            detail: None,
        })
    }

    let router = Router::new()
        .route("/internal/lpe-ct/inbound-deliveries", post(accept))
        .with_state((spool, observed_spool_custody));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    format!("http://{}", address)
}

fn count_queue_json_files(spool: &Path, queue: &str) -> usize {
    std::fs::read_dir(spool.join(queue))
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().and_then(|value| value.to_str()) == Some("json"))
        .count()
}

#[tokio::test]
#[ignore = "benchmark"]
async fn benchmark_relay_hot_path() {
    let spool = temp_dir("relay-bench");
    initialize_spool(&spool).unwrap();
    let start = Instant::now();
    for index in 0..25 {
        let smtp_address = spawn_dummy_smtp(Arc::new(Mutex::new(String::new()))).await;
        let mut config = runtime_config(smtp_address, "http://127.0.0.1:9".to_string());
        config.bayespam_enabled = false;
        config.reputation_enabled = false;
        config.require_spf = false;
        config.require_dmarc_enforcement = false;
        config.defer_on_auth_tempfail = false;
        let response = process_outbound_handoff(
            &spool,
            &config,
            outbound_request(&format!("Relay benchmark {index}")),
        )
        .await
        .unwrap();
        assert_eq!(response.status, TransportDeliveryStatus::Relayed);
    }
    let elapsed = start.elapsed();

    println!(
        "BENCH lpe-ct outbound_relay total={:?} avg_per_iter_us={} iterations=25",
        elapsed,
        elapsed.as_micros() / 25
    );
}
