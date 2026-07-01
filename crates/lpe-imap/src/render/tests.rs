use super::{
    mailbox_name_matches, render_fetch_response, render_flags, render_list_flags,
    render_mailbox_name, FetchAttributes, FetchItem,
};
use lpe_storage::{ImapEmail, ImapMimePart, JmapMailbox};
use uuid::Uuid;

#[test]
fn reserved_mailbox_name_matching_is_role_bound() {
    assert!(mailbox_name_matches("Courrier entrant", "inbox", "INBOX"));
    assert!(!mailbox_name_matches("INBOX", "custom", "INBOX"));
    assert!(!mailbox_name_matches("Sent Items", "custom", "Sent Items"));
    assert!(mailbox_name_matches("Projects", "custom", "projects"));
}

#[test]
fn special_use_flags_are_role_based_for_localized_names() {
    let mailbox = JmapMailbox {
        id: Uuid::new_v4(),
        parent_id: None,
        role: "sent".to_string(),
        name: "Gesendet".to_string(),
        sort_order: 20,
        modseq: 1,
        total_emails: 0,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    };

    assert_eq!(render_mailbox_name(&mailbox), "Gesendet");
    assert_eq!(
        render_list_flags(&mailbox.role, false),
        "(\\HasNoChildren \\Sent)"
    );
    assert_eq!(
        render_list_flags("archive", false),
        "(\\HasNoChildren \\Archive)"
    );
    assert_eq!(render_list_flags("junk", false), "(\\HasNoChildren \\Junk)");
    for role in [
        "outbox",
        "conversation_history",
        "rss_feeds",
        "sync_issues",
        "conflicts",
        "local_failures",
        "server_failures",
    ] {
        assert_eq!(render_list_flags(role, false), "(\\HasNoChildren)");
    }
}

#[test]
fn render_flags_projects_atom_safe_keywords() {
    let email = ImapEmail {
        id: Uuid::new_v4(),
        uid: 1,
        modseq: 1,
        thread_id: Uuid::new_v4(),
        mailbox_id: Uuid::new_v4(),
        mailbox_role: "inbox".to_string(),
        mailbox_name: "INBOX".to_string(),
        received_at: "2026-05-03T10:00:00Z".to_string(),
        sent_at: None,
        from_address: String::new(),
        from_display: None,
        to: Vec::new(),
        cc: Vec::new(),
        bcc: Vec::new(),
        subject: "Message".to_string(),
        preview: String::new(),
        body_text: String::new(),
        body_html_sanitized: None,
        unread: false,
        flagged: false,
        deleted: false,
        keywords: vec!["ProjectX".to_string(), "Red Category".to_string()],
        has_attachments: false,
        size_octets: 0,
        internet_message_id: None,
        delivery_status: "stored".to_string(),
        mime_parts: Vec::new(),
    };

    assert_eq!(render_flags(&email, "INBOX"), "\\Seen ProjectX");
}

#[test]
fn fetch_envelope_uses_parseable_sender_fallback() {
    let email = ImapEmail {
        id: Uuid::new_v4(),
        uid: 1,
        modseq: 1,
        thread_id: Uuid::new_v4(),
        mailbox_id: Uuid::new_v4(),
        mailbox_role: "inbox".to_string(),
        mailbox_name: "INBOX".to_string(),
        received_at: "2026-05-03T10:00:00Z".to_string(),
        sent_at: None,
        from_address: String::new(),
        from_display: None,
        to: Vec::new(),
        cc: Vec::new(),
        bcc: Vec::new(),
        subject: "Delivery report".to_string(),
        preview: String::new(),
        body_text: "Body".to_string(),
        body_html_sanitized: None,
        unread: true,
        flagged: false,
        deleted: false,
        keywords: Vec::new(),
        has_attachments: false,
        size_octets: 4,
        internet_message_id: None,
        delivery_status: "stored".to_string(),
        mime_parts: Vec::new(),
    };

    let response = String::from_utf8(
        render_fetch_response(
            1,
            &email,
            &FetchAttributes {
                items: vec![FetchItem::Envelope],
                mark_seen: false,
            },
        )
        .unwrap(),
    )
    .unwrap();

    assert!(response.contains("(NIL NIL \"unknown\" \"localhost\")"));
    assert!(!response
        .contains("ENVELOPE (\"03 May 2026 10:00:00 +0000\" \"Delivery report\" NIL NIL NIL"));
}

#[test]
fn fetch_header_does_not_duplicate_address_as_display_name() {
    let mut email = ImapEmail {
        id: Uuid::new_v4(),
        uid: 1,
        modseq: 1,
        thread_id: Uuid::new_v4(),
        mailbox_id: Uuid::new_v4(),
        mailbox_role: "inbox".to_string(),
        mailbox_name: "INBOX".to_string(),
        received_at: "2026-05-03T10:00:00Z".to_string(),
        sent_at: None,
        from_address: "sender@example.test".to_string(),
        from_display: Some("sender@example.test".to_string()),
        to: Vec::new(),
        cc: Vec::new(),
        bcc: Vec::new(),
        subject: "Message".to_string(),
        preview: String::new(),
        body_text: "Body".to_string(),
        body_html_sanitized: None,
        unread: true,
        flagged: false,
        deleted: false,
        keywords: Vec::new(),
        has_attachments: false,
        size_octets: 4,
        internet_message_id: None,
        delivery_status: "stored".to_string(),
        mime_parts: Vec::new(),
    };
    email.to.push(lpe_storage::JmapEmailAddress {
        address: "recipient@example.test".to_string(),
        display_name: Some("recipient@example.test".to_string()),
    });

    let response = String::from_utf8(
        render_fetch_response(
            1,
            &email,
            &FetchAttributes {
                items: vec![FetchItem::BodySection(super::BodySectionFetch {
                    peek: true,
                    section: "HEADER".to_string(),
                    partial: None,
                    response_label: None,
                })],
                mark_seen: false,
            },
        )
        .unwrap(),
    )
    .unwrap();

    assert!(response.contains("From: sender@example.test"));
    assert!(response.contains("To: recipient@example.test"));
    assert!(!response.contains("sender@example.test <sender@example.test>"));
    assert!(!response.contains("recipient@example.test <recipient@example.test>"));
}

#[test]
fn body_peek_fetch_response_uses_body_label() {
    let email = ImapEmail {
        id: Uuid::new_v4(),
        uid: 1,
        modseq: 1,
        thread_id: Uuid::new_v4(),
        mailbox_id: Uuid::new_v4(),
        mailbox_role: "inbox".to_string(),
        mailbox_name: "INBOX".to_string(),
        received_at: "2026-05-03T10:00:00Z".to_string(),
        sent_at: None,
        from_address: "sender@example.test".to_string(),
        from_display: None,
        to: Vec::new(),
        cc: Vec::new(),
        bcc: Vec::new(),
        subject: "Message".to_string(),
        preview: String::new(),
        body_text: "Body".to_string(),
        body_html_sanitized: None,
        unread: true,
        flagged: false,
        deleted: false,
        keywords: Vec::new(),
        has_attachments: false,
        size_octets: 4,
        internet_message_id: None,
        delivery_status: "stored".to_string(),
        mime_parts: Vec::new(),
    };

    let response = String::from_utf8(
        render_fetch_response(
            1,
            &email,
            &FetchAttributes {
                items: vec![FetchItem::BodySection(super::BodySectionFetch {
                    peek: true,
                    section: "HEADER.FIELDS (FROM TO SUBJECT)".to_string(),
                    partial: None,
                    response_label: None,
                })],
                mark_seen: false,
            },
        )
        .unwrap(),
    )
    .unwrap();

    assert!(response.contains("BODY[HEADER.FIELDS (FROM TO SUBJECT)]"));
    assert!(!response.contains("BODY.PEEK["));
}

#[test]
fn bodystructure_wraps_alternative_body_in_mixed_when_attachments_exist() {
    let email = ImapEmail {
        id: Uuid::new_v4(),
        uid: 1,
        modseq: 1,
        thread_id: Uuid::new_v4(),
        mailbox_id: Uuid::new_v4(),
        mailbox_role: "inbox".to_string(),
        mailbox_name: "INBOX".to_string(),
        received_at: "2026-05-03T10:00:00Z".to_string(),
        sent_at: None,
        from_address: "sender@example.test".to_string(),
        from_display: None,
        to: Vec::new(),
        cc: Vec::new(),
        bcc: Vec::new(),
        subject: "Message".to_string(),
        preview: String::new(),
        body_text: "Plain".to_string(),
        body_html_sanitized: Some("<p>HTML</p>".to_string()),
        unread: true,
        flagged: false,
        deleted: false,
        keywords: Vec::new(),
        has_attachments: true,
        size_octets: 4,
        internet_message_id: None,
        delivery_status: "stored".to_string(),
        mime_parts: vec![ImapMimePart {
            part_path: "attachment.1".to_string(),
            content_type: "image/png".to_string(),
            content_disposition: Some("inline".to_string()),
            content_id: Some("logo@example.test".to_string()),
            file_name: Some("logo.png".to_string()),
            transfer_encoding: Some("base64".to_string()),
            charset_name: None,
            size_octets: 128,
        }],
    };

    let response = String::from_utf8(
        render_fetch_response(
            1,
            &email,
            &FetchAttributes {
                items: vec![FetchItem::BodyStructure],
                mark_seen: false,
            },
        )
        .unwrap(),
    )
    .unwrap();

    assert!(response.contains("\"ALTERNATIVE\""));
    assert!(response.contains("\"MIXED\""));
    assert!(response.contains("\"INLINE\""));
    assert!(response.contains("\"logo@example.test\""));
    assert!(response.contains("\"IMAGE\" \"PNG\""));
    assert!(response.contains("\"NAME\" \"logo.png\""));
    assert!(response.contains("NIL \"BASE64\" 128 NIL"));
    assert!(response.contains("\"FILENAME\" \"logo.png\""));
}
