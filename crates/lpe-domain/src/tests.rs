use super::{
    MailboxDisplayName, MailboxNameError, MailboxNamePolicy, MailboxPath, MailboxSegment,
    OutboundMessageHandoffRequest, OutboundMessageHandoffResponse, SmtpSubmissionRequest,
    TransportDeliveryStatus, TransportDsnReport, TransportRecipient, TransportRetryAdvice,
    TransportRouteDecision, TransportTechnicalStatus, TransportThrottleStatus,
};
use uuid::Uuid;

#[test]
fn transport_delivery_status_serializes_as_lowercase() {
    let value = serde_json::to_string(&TransportDeliveryStatus::Deferred).unwrap();
    assert_eq!(value, "\"deferred\"");
}

#[test]
fn outbound_envelope_recipients_include_bcc() {
    let request = OutboundMessageHandoffRequest {
        queue_id: Uuid::nil(),
        message_id: Uuid::nil(),
        account_id: Uuid::nil(),
        from_address: "sender@example.test".to_string(),
        from_display: None,
        sender_address: None,
        sender_display: None,
        sender_authorization_kind: "self".to_string(),
        to: vec![TransportRecipient {
            address: "to@example.test".to_string(),
            display_name: None,
        }],
        cc: vec![TransportRecipient {
            address: "cc@example.test".to_string(),
            display_name: None,
        }],
        bcc: vec![TransportRecipient {
            address: "bcc@example.test".to_string(),
            display_name: None,
        }],
        subject: "subject".to_string(),
        body_text: "body".to_string(),
        body_html_sanitized: None,
        internet_message_id: None,
        attempt_count: 0,
        last_attempt_error: None,
    };

    assert_eq!(
        request.envelope_recipients(),
        vec![
            "to@example.test".to_string(),
            "cc@example.test".to_string(),
            "bcc@example.test".to_string()
        ]
    );
}

#[test]
fn outbound_handoff_response_carries_structured_transport_details() {
    let response = OutboundMessageHandoffResponse {
        queue_id: Uuid::nil(),
        status: TransportDeliveryStatus::Deferred,
        trace_id: "trace-1".to_string(),
        detail: Some("rate limit reached".to_string()),
        remote_message_ref: Some("remote-42".to_string()),
        retry: Some(TransportRetryAdvice {
            retry_after_seconds: 120,
            policy: "throttle".to_string(),
            reason: Some("tenant quota".to_string()),
        }),
        dsn: Some(TransportDsnReport {
            action: "delayed".to_string(),
            status: "4.7.1".to_string(),
            diagnostic_code: Some("smtp; 451 4.7.1 throttled".to_string()),
            remote_mta: Some("mx1.example.test".to_string()),
        }),
        technical: Some(TransportTechnicalStatus {
            phase: "rcpt-to".to_string(),
            smtp_code: Some(451),
            enhanced_code: Some("4.7.1".to_string()),
            remote_host: Some("mx1.example.test".to_string()),
            detail: Some("recipient domain throttled".to_string()),
        }),
        route: Some(TransportRouteDecision {
            rule_id: Some("domain-example".to_string()),
            relay_target: Some("smtp://mx1.example.test:25".to_string()),
            queue: "deferred".to_string(),
        }),
        throttle: Some(TransportThrottleStatus {
            scope: "recipient-domain".to_string(),
            key: "example.test".to_string(),
            limit: 20,
            window_seconds: 60,
            retry_after_seconds: 120,
        }),
    };

    let json = serde_json::to_value(&response).unwrap();
    assert_eq!(json["status"], "deferred");
    assert_eq!(json["retry"]["retry_after_seconds"], 120);
    assert_eq!(json["dsn"]["status"], "4.7.1");
    assert_eq!(json["route"]["queue"], "deferred");
    assert_eq!(json["throttle"]["scope"], "recipient-domain");
}

#[test]
fn smtp_submission_request_serializes_raw_message_as_base64() {
    let request = SmtpSubmissionRequest {
        trace_id: "trace-1".to_string(),
        helo: "client.example.test".to_string(),
        peer: "203.0.113.10:53544".to_string(),
        account_id: Uuid::nil(),
        account_email: "alice@example.test".to_string(),
        account_display_name: "Alice".to_string(),
        mail_from: "alice@example.test".to_string(),
        rcpt_to: vec!["bob@example.test".to_string()],
        raw_message: b"Subject: hi\r\n\r\nbody".to_vec(),
    };

    let json = serde_json::to_value(&request).unwrap();
    assert_eq!(json["raw_message"], "U3ViamVjdDogaGkNCg0KYm9keQ==");
}

#[test]
fn mailbox_display_name_accepts_ascii_names() {
    let name = MailboxDisplayName::new("Projects").unwrap();
    assert_eq!(name.as_str(), "Projects");
    assert_eq!(name.canonical_key().as_str(), "projects");
}

#[test]
fn mailbox_display_name_normalizes_cafe_to_nfc_collision_key() {
    let composed = MailboxDisplayName::new("Café").unwrap();
    let decomposed = MailboxDisplayName::new("Cafe\u{301}").unwrap();

    assert_eq!(composed.as_str(), "Café");
    assert_eq!(decomposed.as_str(), "Café");
    assert!(composed
        .canonical_key()
        .collides_with(&decomposed.canonical_key()));
}

#[test]
fn mailbox_display_name_accepts_emoji_names() {
    let name = MailboxDisplayName::new("📁 Projects").unwrap();
    assert_eq!(name.as_str(), "📁 Projects");
}

#[test]
fn mailbox_display_name_accepts_japanese_names() {
    let name = MailboxDisplayName::new("案件").unwrap();
    assert_eq!(name.as_str(), "案件");
}

#[test]
fn mailbox_display_name_accepts_arabic_names_without_controls() {
    let name = MailboxDisplayName::new("مشاريع").unwrap();
    assert_eq!(name.as_str(), "مشاريع");
}

#[test]
fn mailbox_display_name_accepts_hebrew_names_without_controls() {
    let name = MailboxDisplayName::new("משימות").unwrap();
    assert_eq!(name.as_str(), "משימות");
}

#[test]
fn mailbox_display_name_rejects_control_characters() {
    assert_eq!(
        MailboxDisplayName::new("Projects\n2026").unwrap_err(),
        MailboxNameError::ContainsControl
    );
}

#[test]
fn mailbox_path_rejects_empty_segments() {
    assert_eq!(
        MailboxPath::parse("Projects//2026").unwrap_err(),
        MailboxNameError::EmptySegment
    );
}

#[test]
fn mailbox_list_pattern_percent_matches_one_hierarchy_level() {
    assert!(MailboxNamePolicy::list_pattern_matches("Projects", "%"));
    assert!(MailboxNamePolicy::list_pattern_matches(
        "Projects/2026",
        "Projects/%"
    ));
    assert!(!MailboxNamePolicy::list_pattern_matches(
        "Projects/2026/Q1",
        "Projects/%"
    ));
    assert!(!MailboxNamePolicy::list_pattern_matches(
        "Projects/2026",
        "%"
    ));
}

#[test]
fn mailbox_list_pattern_star_matches_recursively() {
    assert!(MailboxNamePolicy::list_pattern_matches(
        "Projects/2026/Q1",
        "Projects/*"
    ));
    assert!(MailboxNamePolicy::list_pattern_matches(
        "Projects/2026/Q1",
        "*"
    ));
}

#[test]
fn mailbox_list_pattern_matches_unicode_after_decoding() {
    assert!(MailboxNamePolicy::list_pattern_matches(
        "案件/顧客",
        "案件/%"
    ));
    assert!(!MailboxNamePolicy::list_pattern_matches(
        "案件/顧客/Q1",
        "案件/%"
    ));
    assert!(MailboxNamePolicy::list_pattern_matches(
        "Café",
        "CAFE\u{301}"
    ));
    assert!(MailboxNamePolicy::list_pattern_matches("Straße", "STRASSE"));
}

#[test]
fn mailbox_segment_rejects_delimiter_in_segment_names() {
    assert_eq!(
        MailboxSegment::new("Projects/2026").unwrap_err(),
        MailboxNameError::ContainsDelimiter
    );
}

#[test]
fn mailbox_display_name_rejects_unsafe_invisible_characters() {
    assert_eq!(
        MailboxDisplayName::new("Safe\u{200d}Name").unwrap_err(),
        MailboxNameError::ContainsUnsafeInvisible
    );
}

#[test]
fn mailbox_display_name_rejects_bidi_controls() {
    assert_eq!(
        MailboxDisplayName::new("Reports\u{202e}2026").unwrap_err(),
        MailboxNameError::ContainsUnsafeInvisible
    );
}

#[test]
fn mailbox_display_name_rejects_mixed_script_confusables() {
    assert_eq!(
        MailboxDisplayName::new("p\u{430}yp\u{430}l").unwrap_err(),
        MailboxNameError::ContainsMixedScriptConfusable
    );
}

#[test]
fn mailbox_canonical_key_collides_for_whole_script_confusables() {
    let latin = MailboxNamePolicy::canonical_key("paypal");
    let cyrillic = MailboxNamePolicy::canonical_key("\u{440}\u{430}\u{443}\u{440}\u{430}\u{04cf}");

    assert!(latin.collides_with(&cyrillic));
}

#[test]
fn mailbox_display_name_rejects_reserved_name_spoofing() {
    assert_eq!(
        MailboxDisplayName::new("ІNBOX").unwrap_err(),
        MailboxNameError::ReservedName
    );
    assert_eq!(
        MailboxSegment::new("Sent Items").unwrap_err(),
        MailboxNameError::ReservedName
    );
    assert_eq!(
        MailboxSegment::new("Spam").unwrap_err(),
        MailboxNameError::ReservedName
    );
    assert_eq!(
        MailboxSegment::new("Archive").unwrap_err(),
        MailboxNameError::ReservedName
    );
    assert_eq!(
        MailboxNamePolicy::system_role_for_display_name("Deleted Items"),
        Some("trash")
    );
}

#[test]
fn canonical_system_display_names_are_standard_backend_names() {
    assert_eq!(
        MailboxNamePolicy::canonical_system_display_name("inbox"),
        Some("INBOX")
    );
    assert_eq!(
        MailboxNamePolicy::canonical_system_display_name("sent"),
        Some("Sent")
    );
    assert_eq!(
        MailboxNamePolicy::canonical_system_display_name("drafts"),
        Some("Drafts")
    );
    assert_eq!(
        MailboxNamePolicy::canonical_system_display_name("trash"),
        Some("Trash")
    );
    assert_eq!(
        MailboxNamePolicy::canonical_system_display_name("junk"),
        Some("Junk")
    );
    assert_eq!(
        MailboxNamePolicy::canonical_system_display_name("archive"),
        Some("Archive")
    );
}
