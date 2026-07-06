use super::*;
use crate::mapi::properties::default_wlink_group_uuid;
use lpe_storage::{
    AccessibleContact, CollaborationCollection, CollaborationRights, JmapEmailAddress,
    JmapEmailMailboxState,
};

fn exchange_builtin_excluded_folder_roles() -> Vec<String> {
    [
        "trash",
        "junk",
        "drafts",
        "outbox",
        "conflicts",
        "local_failures",
        "server_failures",
        "sync_issues",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn test_mailbox(id: Uuid) -> JmapMailbox {
    JmapMailbox {
        id,
        parent_id: None,
        role: "inbox".to_string(),
        name: "Inbox".to_string(),
        sort_order: 0,
        modseq: 1,
        total_emails: 3,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    }
}

fn test_email(id: Uuid, mailbox_id: Uuid, subject: &str) -> JmapEmail {
    JmapEmail {
        id,
        thread_id: Uuid::from_u128(0x12121212_1212_4212_8212_121212121212),
        mailbox_id,
        mailbox_role: "inbox".to_string(),
        mailbox_name: "Inbox".to_string(),
        modseq: 2,
        mailbox_ids: vec![mailbox_id],
        mailbox_states: vec![JmapEmailMailboxState {
            mailbox_id,
            role: "inbox".to_string(),
            name: "Inbox".to_string(),
            modseq: 2,
            unread: false,
            flagged: false,
            followup_flag_status: "none".to_string(),
            followup_icon: 0,
            todo_item_flags: 0,
            followup_request: String::new(),
            followup_start_at: None,
            followup_due_at: None,
            followup_completed_at: None,
            reminder_set: false,
            reminder_at: None,
            reminder_dismissed_at: None,
            swapped_todo_store_id: None,
            swapped_todo_data: None,
            categories: Vec::new(),
            draft: false,
        }],
        received_at: "2026-05-20T12:00:00Z".to_string(),
        sent_at: None,
        from_address: "alice@example.test".to_string(),
        from_display: Some("Alice".to_string()),
        sender_address: None,
        sender_display: None,
        sender_authorization_kind: "self".to_string(),
        submitted_by_account_id: Uuid::nil(),
        to: Vec::new(),
        cc: Vec::new(),
        bcc: Vec::new(),
        subject: subject.to_string(),
        preview: subject.to_string(),
        body_text: subject.to_string(),
        body_html_sanitized: None,
        unread: false,
        flagged: false,
        followup_flag_status: "none".to_string(),
        followup_icon: 0,
        todo_item_flags: 0,
        followup_request: String::new(),
        followup_start_at: None,
        followup_due_at: None,
        followup_completed_at: None,
        reminder_set: false,
        reminder_at: None,
        reminder_dismissed_at: None,
        swapped_todo_store_id: None,
        swapped_todo_data: None,
        categories: Vec::new(),
        has_attachments: false,
        size_octets: 42,
        internet_message_id: None,
        mime_blob_ref: None,
        delivery_status: "stored".to_string(),
    }
}

#[test]
fn content_table_window_emails_reuses_wider_window_slice() {
    let mailbox_id = Uuid::from_u128(0x44444444_4444_4444_8444_444444444444);
    let first_id = Uuid::from_u128(0x55555555_5555_4555_8555_555555555555);
    let second_id = Uuid::from_u128(0x66666666_6666_4666_8666_666666666666);
    let third_id = Uuid::from_u128(0x77777777_7777_4777_8777_777777777777);
    crate::mapi::identity::remember_mapi_identity(
        mailbox_id,
        crate::mapi::identity::INBOX_FOLDER_ID,
    );
    crate::mapi::identity::remember_mapi_identity(
        first_id,
        crate::mapi::identity::mapi_store_id(101),
    );
    crate::mapi::identity::remember_mapi_identity(
        second_id,
        crate::mapi::identity::mapi_store_id(102),
    );
    crate::mapi::identity::remember_mapi_identity(
        third_id,
        crate::mapi::identity::mapi_store_id(103),
    );
    let snapshot = MapiMailStoreSnapshot::new(
        vec![test_mailbox(mailbox_id)],
        vec![
            test_email(first_id, mailbox_id, "First"),
            test_email(second_id, mailbox_id, "Second"),
            test_email(third_id, mailbox_id, "Third"),
        ],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_content_windows(vec![MapiContentTableWindow {
        folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
        view_signature: 42,
        offset: 0,
        total: 3,
        message_ids: vec![first_id, second_id, third_id],
    }]);

    let (total, emails) = snapshot
        .content_table_window_emails(crate::mapi::identity::INBOX_FOLDER_ID, 42, 1, 2)
        .expect("wider window should satisfy subrange");

    assert_eq!(total, 3);
    assert_eq!(emails.len(), 2);
    assert_eq!(emails[0].subject, "Second");
    assert_eq!(emails[1].subject, "Third");
}

#[test]
fn content_table_window_emails_skips_insufficient_containing_window() {
    let mailbox_id = Uuid::from_u128(0x44444444_4444_4444_8444_444444444445);
    let first_id = Uuid::from_u128(0x55555555_5555_4555_8555_555555555556);
    let second_id = Uuid::from_u128(0x66666666_6666_4666_8666_666666666667);
    let third_id = Uuid::from_u128(0x77777777_7777_4777_8777_777777777778);
    crate::mapi::identity::remember_mapi_identity(
        mailbox_id,
        crate::mapi::identity::INBOX_FOLDER_ID,
    );
    crate::mapi::identity::remember_mapi_identity(
        first_id,
        crate::mapi::identity::mapi_store_id(104),
    );
    crate::mapi::identity::remember_mapi_identity(
        second_id,
        crate::mapi::identity::mapi_store_id(105),
    );
    crate::mapi::identity::remember_mapi_identity(
        third_id,
        crate::mapi::identity::mapi_store_id(106),
    );
    let snapshot = MapiMailStoreSnapshot::new(
        vec![test_mailbox(mailbox_id)],
        vec![
            test_email(first_id, mailbox_id, "First"),
            test_email(second_id, mailbox_id, "Second"),
            test_email(third_id, mailbox_id, "Third"),
        ],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_content_windows(vec![
        MapiContentTableWindow {
            folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
            view_signature: 42,
            offset: 0,
            total: 4,
            message_ids: vec![first_id, second_id],
        },
        MapiContentTableWindow {
            folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
            view_signature: 42,
            offset: 1,
            total: 4,
            message_ids: vec![second_id, third_id],
        },
    ]);

    let (total, emails) = snapshot
        .content_table_window_emails(crate::mapi::identity::INBOX_FOLDER_ID, 42, 1, 2)
        .expect("later sufficient window should satisfy subrange");

    assert_eq!(total, 4);
    assert_eq!(emails.len(), 2);
    assert_eq!(emails[0].subject, "Second");
    assert_eq!(emails[1].subject, "Third");
}

#[test]
fn content_table_window_emails_containing_skips_incomplete_window() {
    let mailbox_id = Uuid::from_u128(0x44444444_4444_4444_8444_444444444446);
    let first_id = Uuid::from_u128(0x55555555_5555_4555_8555_555555555557);
    let second_id = Uuid::from_u128(0x66666666_6666_4666_8666_666666666668);
    let third_id = Uuid::from_u128(0x77777777_7777_4777_8777_777777777779);
    let missing_id = Uuid::from_u128(0x88888888_8888_4888_8888_888888888889);
    crate::mapi::identity::remember_mapi_identity(
        mailbox_id,
        crate::mapi::identity::INBOX_FOLDER_ID,
    );
    crate::mapi::identity::remember_mapi_identity(
        first_id,
        crate::mapi::identity::mapi_store_id(107),
    );
    crate::mapi::identity::remember_mapi_identity(
        second_id,
        crate::mapi::identity::mapi_store_id(108),
    );
    crate::mapi::identity::remember_mapi_identity(
        third_id,
        crate::mapi::identity::mapi_store_id(109),
    );
    let snapshot = MapiMailStoreSnapshot::new(
        vec![test_mailbox(mailbox_id)],
        vec![
            test_email(first_id, mailbox_id, "First"),
            test_email(second_id, mailbox_id, "Second"),
            test_email(third_id, mailbox_id, "Third"),
        ],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_content_windows(vec![
        MapiContentTableWindow {
            folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
            view_signature: 42,
            offset: 0,
            total: 4,
            message_ids: vec![first_id, missing_id],
        },
        MapiContentTableWindow {
            folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
            view_signature: 42,
            offset: 1,
            total: 4,
            message_ids: vec![second_id, third_id],
        },
    ]);

    let (offset, total, emails) = snapshot
        .content_table_window_emails_containing(crate::mapi::identity::INBOX_FOLDER_ID, 42, 1)
        .expect("later complete window should satisfy position");

    assert_eq!(offset, 1);
    assert_eq!(total, 4);
    assert_eq!(emails.len(), 2);
    assert_eq!(emails[0].subject, "Second");
    assert_eq!(emails[1].subject, "Third");
}

#[test]
fn content_table_window_emails_containing_prefers_boundary_window() {
    let mailbox_id = Uuid::from_u128(0x44444444_4444_4444_8444_444444448888);
    let first_id = Uuid::from_u128(0x55555555_5555_4555_8555_555555558888);
    let second_id = Uuid::from_u128(0x66666666_6666_4666_8666_666666668888);
    let third_id = Uuid::from_u128(0x77777777_7777_4777_8777_777777778888);
    crate::mapi::identity::remember_mapi_identity(
        mailbox_id,
        crate::mapi::identity::INBOX_FOLDER_ID,
    );
    crate::mapi::identity::remember_mapi_identity(
        first_id,
        crate::mapi::identity::mapi_store_id(111),
    );
    crate::mapi::identity::remember_mapi_identity(
        second_id,
        crate::mapi::identity::mapi_store_id(112),
    );
    crate::mapi::identity::remember_mapi_identity(
        third_id,
        crate::mapi::identity::mapi_store_id(113),
    );
    let snapshot = MapiMailStoreSnapshot::new(
        vec![test_mailbox(mailbox_id)],
        vec![
            test_email(first_id, mailbox_id, "First"),
            test_email(second_id, mailbox_id, "Second"),
            test_email(third_id, mailbox_id, "Third"),
        ],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_content_windows(vec![
        MapiContentTableWindow {
            folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
            view_signature: 42,
            offset: 0,
            total: 3,
            message_ids: vec![first_id, second_id],
        },
        MapiContentTableWindow {
            folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
            view_signature: 42,
            offset: 2,
            total: 3,
            message_ids: vec![third_id],
        },
    ]);

    let (offset, total, emails) = snapshot
        .content_table_window_emails_containing(crate::mapi::identity::INBOX_FOLDER_ID, 42, 2)
        .expect("boundary window should satisfy position");

    assert_eq!(offset, 2);
    assert_eq!(total, 3);
    assert_eq!(emails.len(), 1);
    assert_eq!(emails[0].subject, "Third");
}

#[test]
fn content_table_window_emails_containing_prefers_longer_tail_window() {
    let mailbox_id = Uuid::from_u128(0x44444444_4444_4444_8444_444444449999);
    let first_id = Uuid::from_u128(0x55555555_5555_4555_8555_555555559999);
    let second_id = Uuid::from_u128(0x66666666_6666_4666_8666_666666669999);
    let third_id = Uuid::from_u128(0x77777777_7777_4777_8777_777777779999);
    crate::mapi::identity::remember_mapi_identity(
        mailbox_id,
        crate::mapi::identity::INBOX_FOLDER_ID,
    );
    crate::mapi::identity::remember_mapi_identity(
        first_id,
        crate::mapi::identity::mapi_store_id(114),
    );
    crate::mapi::identity::remember_mapi_identity(
        second_id,
        crate::mapi::identity::mapi_store_id(115),
    );
    crate::mapi::identity::remember_mapi_identity(
        third_id,
        crate::mapi::identity::mapi_store_id(116),
    );
    let snapshot = MapiMailStoreSnapshot::new(
        vec![test_mailbox(mailbox_id)],
        vec![
            test_email(first_id, mailbox_id, "First"),
            test_email(second_id, mailbox_id, "Second"),
            test_email(third_id, mailbox_id, "Third"),
        ],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_content_windows(vec![
        MapiContentTableWindow {
            folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
            view_signature: 42,
            offset: 0,
            total: 3,
            message_ids: vec![first_id, second_id, third_id],
        },
        MapiContentTableWindow {
            folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
            view_signature: 42,
            offset: 1,
            total: 3,
            message_ids: vec![second_id],
        },
    ]);

    let (offset, total, emails) = snapshot
        .content_table_window_emails_containing(crate::mapi::identity::INBOX_FOLDER_ID, 42, 1)
        .expect("longer complete window should satisfy position");

    assert_eq!(offset, 0);
    assert_eq!(total, 3);
    assert_eq!(emails.len(), 3);
    assert_eq!(emails[1].subject, "Second");
    assert_eq!(emails[2].subject, "Third");
}

#[test]
fn content_table_total_survives_total_only_window_without_rows() {
    let mailbox_id = Uuid::from_u128(0x44444444_4444_4444_8444_444444444447);
    crate::mapi::identity::remember_mapi_identity(
        mailbox_id,
        crate::mapi::identity::INBOX_FOLDER_ID,
    );
    let snapshot = MapiMailStoreSnapshot::new(
        vec![test_mailbox(mailbox_id)],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_content_windows(vec![MapiContentTableWindow {
        folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
        view_signature: 42,
        offset: 0,
        total: 2,
        message_ids: Vec::new(),
    }]);

    assert_eq!(
        snapshot.content_table_total(crate::mapi::identity::INBOX_FOLDER_ID, 42),
        Some(2)
    );
}

#[test]
fn advertised_special_mailbox_roles_have_reserved_mapi_counters() {
    let cases = [
        (
            "sync_issues",
            crate::mapi::identity::SYNC_ISSUES_FOLDER_COUNTER,
        ),
        ("conflicts", crate::mapi::identity::CONFLICTS_FOLDER_COUNTER),
        (
            "local_failures",
            crate::mapi::identity::LOCAL_FAILURES_FOLDER_COUNTER,
        ),
        (
            "server_failures",
            crate::mapi::identity::SERVER_FAILURES_FOLDER_COUNTER,
        ),
        ("junk", crate::mapi::identity::JUNK_FOLDER_COUNTER),
        ("rss_feeds", crate::mapi::identity::RSS_FEEDS_FOLDER_COUNTER),
        ("archive", crate::mapi::identity::ARCHIVE_FOLDER_COUNTER),
        (
            "conversation_history",
            crate::mapi::identity::CONVERSATION_HISTORY_FOLDER_COUNTER,
        ),
    ];

    for (role, counter) in cases {
        assert_eq!(reserved_folder_counter_for_role(role), Some(counter));
    }
}

#[test]
fn inbox_associated_configs_do_not_emit_synthetic_defaults() {
    let snapshot = MapiMailStoreSnapshot::empty();
    let messages =
        snapshot.associated_config_messages_for_folder(crate::mapi::identity::INBOX_FOLDER_ID);

    assert!(messages.is_empty());
    for suppressed_id in [
        OUTLOOK_INBOX_ACCOUNT_PREFS_CONFIG_ID,
        OUTLOOK_INBOX_EAS_CONFIG_ID,
        OUTLOOK_INBOX_MESSAGE_LIST_SETTINGS_CONFIG_ID,
        OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_ID,
    ] {
        assert!(snapshot
            .associated_config_message_for_id(suppressed_id)
            .is_none());
        assert!(!snapshot.associated_config_identity_matches_folder(
            crate::mapi::identity::INBOX_FOLDER_ID,
            suppressed_id
        ));
    }
    for exact_virtual_id in [
        OUTLOOK_INBOX_ELC_CONFIG_ID,
        OUTLOOK_INBOX_UMOLK_USER_OPTIONS_CONFIG_ID,
        OUTLOOK_INBOX_SHARING_CONFIGURATION_ID,
        OUTLOOK_INBOX_SHARING_INDEX_ID,
        OUTLOOK_INBOX_AGGREGATION_ID,
    ] {
        let message = snapshot
            .associated_config_message_for_id(exact_virtual_id)
            .expect("exact virtual Inbox FAI row should open by MID");
        assert_eq!(message.id, exact_virtual_id);
        assert!(snapshot.associated_config_identity_matches_folder(
            crate::mapi::identity::INBOX_FOLDER_ID,
            exact_virtual_id
        ));
    }
    assert!(snapshot
        .associated_config_message_for_id(OUTLOOK_INBOX_RULE_ORGANIZER_CONFIG_ID)
        .is_none());
    assert!(!snapshot.associated_config_identity_matches_folder(
        crate::mapi::identity::INBOX_FOLDER_ID,
        OUTLOOK_INBOX_RULE_ORGANIZER_CONFIG_ID
    ));

    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let persisted_id = Uuid::from_u128(0x6d617069_6561_7343_8000_000000000002);
    let duplicate_id = Uuid::from_u128(0x6d617069_6561_7343_8000_000000000003);
    crate::mapi::identity::remember_mapi_identity(
        persisted_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 71,
        ),
    );
    crate::mapi::identity::remember_mapi_identity(
        duplicate_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 72,
        ),
    );
    let persisted = MapiMailStoreSnapshot::empty().with_associated_configs(vec![
        crate::store::MapiAssociatedConfigRecord {
            id: persisted_id,
            account_id,
            folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
            message_class: OUTLOOK_INBOX_EAS_CONFIG_CLASS.to_string(),
            subject: "Client EAS config".to_string(),
            properties_json: serde_json::json!({}),
        },
        crate::store::MapiAssociatedConfigRecord {
            id: duplicate_id,
            account_id,
            folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
            message_class: OUTLOOK_INBOX_EAS_CONFIG_CLASS.to_string(),
            subject: "Duplicate EAS config".to_string(),
            properties_json: serde_json::json!({}),
        },
    ]);

    let persisted_messages =
        persisted.associated_config_messages_for_folder(crate::mapi::identity::INBOX_FOLDER_ID);
    assert_eq!(
        persisted_messages
            .iter()
            .filter(|message| message.message_class == OUTLOOK_INBOX_EAS_CONFIG_CLASS)
            .count(),
        1
    );
    assert_eq!(
        persisted_messages
            .iter()
            .find(|message| message.message_class == OUTLOOK_INBOX_EAS_CONFIG_CLASS)
            .map(|message| message.subject.as_str()),
        Some("Client EAS config")
    );
}

#[test]
fn inbox_associated_config_bootstrap_persists_no_outlook_defaults() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    log_outlook_inbox_associated_config_bootstrap(account_id, &[], &[], &[]);
}

#[test]
fn empty_inbox_compact_named_view_placeholder_is_suppressed() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let stale_id = Uuid::from_u128(0x6d617069_696e_4e76_8000_000000000077);
    crate::mapi::identity::remember_mapi_identity(
        stale_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 77,
        ),
    );
    let snapshot = MapiMailStoreSnapshot::empty().with_associated_configs(vec![
        crate::store::MapiAssociatedConfigRecord {
            id: stale_id,
            account_id,
            folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
            message_class: OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_CLASS.to_string(),
            subject: "Compact".to_string(),
            properties_json: serde_json::json!({}),
        },
    ]);

    let messages =
        snapshot.associated_config_messages_for_folder(crate::mapi::identity::INBOX_FOLDER_ID);
    assert!(messages
        .iter()
        .all(|message| { message.message_class != OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_CLASS }));
    assert!(!snapshot.associated_config_identity_matches_folder(
        crate::mapi::identity::INBOX_FOLDER_ID,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 77,
        )
    ));
}

#[test]
fn empty_persisted_umolk_placeholder_does_not_shadow_exact_modeled_row() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let stale_id = Uuid::from_u128(0x6d617069_756d_6f6c_8000_000000000001);
    crate::mapi::identity::remember_mapi_identity(
        stale_id,
        crate::mapi::identity::mapi_store_id(0x7fff_ffff_fffa),
    );
    let snapshot = MapiMailStoreSnapshot::empty().with_associated_configs(vec![
        crate::store::MapiAssociatedConfigRecord {
            id: stale_id,
            account_id,
            folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
            message_class: OUTLOOK_INBOX_UMOLK_USER_OPTIONS_CONFIG_CLASS.to_string(),
            subject: OUTLOOK_INBOX_UMOLK_USER_OPTIONS_CONFIG_CLASS.to_string(),
            properties_json: serde_json::json!({}),
        },
    ]);

    let messages =
        snapshot.associated_config_messages_for_folder(crate::mapi::identity::INBOX_FOLDER_ID);
    assert!(messages
        .iter()
        .all(|message| message.message_class != OUTLOOK_INBOX_UMOLK_USER_OPTIONS_CONFIG_CLASS));
    let modeled = snapshot
        .associated_config_message_for_id(crate::mapi::identity::mapi_store_id(0x7fff_ffff_fffa))
        .expect("exact modeled UMOLK row");
    assert_eq!(
        modeled.message_class,
        OUTLOOK_INBOX_UMOLK_USER_OPTIONS_CONFIG_CLASS
    );
    assert_eq!(
        modeled.subject,
        OUTLOOK_INBOX_UMOLK_USER_OPTIONS_CONFIG_CLASS
    );
    assert_eq!(
        modeled.properties_json["0x7c070102"]["value"],
        serde_json::json!("3c786d6c2f3e")
    );
    assert!(snapshot.associated_config_identity_matches_folder(
        crate::mapi::identity::INBOX_FOLDER_ID,
        crate::mapi::identity::mapi_store_id(0x7fff_ffff_fffa)
    ));
}

#[test]
fn associated_config_sync_messages_use_persisted_rows_before_narrow_defaults() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let persisted_umolk_id = Uuid::from_u128(0x6d617069_756d_6f6c_8000_000000000002);
    let persisted_named_view_id = Uuid::from_u128(0x6d617069_696e_4e76_8000_000000000002);
    let persisted_account_prefs_id = Uuid::from_u128(0x6d617069_6163_6350_8000_000000000002);
    for (offset, id) in [
        persisted_umolk_id,
        persisted_named_view_id,
        persisted_account_prefs_id,
    ]
    .into_iter()
    .enumerate()
    {
        crate::mapi::identity::remember_mapi_identity(
            id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 132 + offset as u64,
            ),
        );
    }
    let snapshot = MapiMailStoreSnapshot::empty().with_associated_configs(vec![
        crate::store::MapiAssociatedConfigRecord {
            id: persisted_umolk_id,
            account_id,
            folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
            message_class: OUTLOOK_INBOX_UMOLK_USER_OPTIONS_CONFIG_CLASS.to_string(),
            subject: "Persisted UMOLK".to_string(),
            properties_json: serde_json::json!({}),
        },
        crate::store::MapiAssociatedConfigRecord {
            id: persisted_named_view_id,
            account_id,
            folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
            message_class: OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_CLASS.to_string(),
            subject: "Persisted Compact".to_string(),
            properties_json: serde_json::json!({}),
        },
        crate::store::MapiAssociatedConfigRecord {
            id: persisted_account_prefs_id,
            account_id,
            folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
            message_class: OUTLOOK_INBOX_ACCOUNT_PREFS_CONFIG_CLASS.to_string(),
            subject: "Persisted AccountPrefs".to_string(),
            properties_json: serde_json::json!({}),
        },
    ]);

    let table_messages =
        snapshot.associated_config_messages_for_folder(crate::mapi::identity::INBOX_FOLDER_ID);
    let sync_messages =
        snapshot.associated_config_sync_messages_for_folder(crate::mapi::identity::INBOX_FOLDER_ID);

    assert_eq!(sync_messages, table_messages);
    assert_eq!(
        sync_messages
            .iter()
            .find(|message| {
                message.message_class == OUTLOOK_INBOX_UMOLK_USER_OPTIONS_CONFIG_CLASS
            })
            .map(|message| message.canonical_id),
        Some(persisted_umolk_id)
    );
    assert_eq!(
        sync_messages
            .iter()
            .find(|message| message.message_class == OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_CLASS)
            .map(|message| message.canonical_id),
        Some(persisted_named_view_id)
    );
    assert_eq!(
        sync_messages
            .iter()
            .find(|message| message.message_class == OUTLOOK_INBOX_ACCOUNT_PREFS_CONFIG_CLASS)
            .map(|message| message.canonical_id),
        Some(persisted_account_prefs_id)
    );
    assert!(!sync_messages
        .iter()
        .any(|message| is_outlook_inbox_virtual_only_associated_config_id(message.id)));
}

#[test]
fn empty_rule_organizer_placeholder_is_not_modeled_state() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let empty = crate::store::MapiAssociatedConfigRecord {
        id: Uuid::from_u128(0x6d617069_7275_6c65_8000_000000000001),
        account_id,
        folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
        message_class: OUTLOOK_INBOX_RULE_ORGANIZER_CONFIG_CLASS.to_string(),
        subject: OUTLOOK_INBOX_RULE_ORGANIZER_CONFIG_CLASS.to_string(),
        properties_json: serde_json::json!({}),
    };
    let non_empty = crate::store::MapiAssociatedConfigRecord {
        properties_json: serde_json::json!({
            OUTLOOK_RULE_ORGANIZER_BINARY_6802_JSON_KEY: {
                "type": "binary",
                "value": "0102"
            }
        }),
        ..empty.clone()
    };

    assert!(is_empty_outlook_rule_organizer_placeholder(&empty));
    assert!(!is_empty_outlook_rule_organizer_placeholder(&non_empty));
}

#[test]
fn associated_configs_keep_outlook_migration_markers_visible() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let kept_id = Uuid::from_u128(0x6d617069_6b65_6570_8000_000000000001);
    let migration_id = Uuid::from_u128(0x6d617069_6472_6f70_8000_000000000001);
    crate::mapi::identity::remember_mapi_identity(
        kept_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 81,
        ),
    );
    crate::mapi::identity::remember_mapi_identity(
        migration_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 82,
        ),
    );
    let snapshot = MapiMailStoreSnapshot::empty().with_associated_configs(vec![
        crate::store::MapiAssociatedConfigRecord {
            id: kept_id,
            account_id,
            folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
            message_class: OUTLOOK_INBOX_MESSAGE_LIST_SETTINGS_CONFIG_CLASS.to_string(),
            subject: OUTLOOK_INBOX_MESSAGE_LIST_SETTINGS_CONFIG_CLASS.to_string(),
            properties_json: serde_json::json!({}),
        },
        crate::store::MapiAssociatedConfigRecord {
            id: migration_id,
            account_id,
            folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
            message_class: "IPM.Microsoft.PendingChange.MigrateFlags".to_string(),
            subject: "IPM.Microsoft.PendingChange.MigrateFlags".to_string(),
            properties_json: serde_json::json!({}),
        },
    ]);

    let messages =
        snapshot.associated_config_messages_for_folder(crate::mapi::identity::INBOX_FOLDER_ID);

    assert!(messages.iter().any(|message| {
        message.message_class == OUTLOOK_INBOX_MESSAGE_LIST_SETTINGS_CONFIG_CLASS
    }));
    assert!(messages
        .iter()
        .any(|message| message.message_class == "IPM.Microsoft.PendingChange.MigrateFlags"));
}

#[test]
fn quick_step_settings_include_default_custom_action_without_duplicate() {
    let snapshot = MapiMailStoreSnapshot::empty();
    let messages = snapshot.associated_config_messages_for_folder(
        crate::mapi::identity::QUICK_STEP_SETTINGS_FOLDER_ID,
    );

    assert_eq!(
        messages
            .iter()
            .filter(|message| message.message_class == OUTLOOK_QUICK_STEP_CUSTOM_ACTION_CLASS)
            .count(),
        1
    );
    assert_eq!(
        snapshot
            .associated_config_message_for_id(OUTLOOK_QUICK_STEP_CUSTOM_ACTION_ID)
            .map(|message| message.message_class),
        Some(OUTLOOK_QUICK_STEP_CUSTOM_ACTION_CLASS.to_string())
    );
    assert_eq!(
        snapshot
            .associated_config_message_for_folder_and_source_key_id(
                crate::mapi::identity::QUICK_STEP_SETTINGS_FOLDER_ID,
                OUTLOOK_QUICK_STEP_CUSTOM_ACTION_ID,
            )
            .map(|message| message.message_class),
        Some(OUTLOOK_QUICK_STEP_CUSTOM_ACTION_CLASS.to_string())
    );
    assert!(snapshot.associated_config_identity_matches_folder(
        crate::mapi::identity::QUICK_STEP_SETTINGS_FOLDER_ID,
        OUTLOOK_QUICK_STEP_CUSTOM_ACTION_ID
    ));
    assert!(!snapshot.associated_config_identity_matches_folder(
        crate::mapi::identity::INBOX_FOLDER_ID,
        OUTLOOK_QUICK_STEP_CUSTOM_ACTION_ID
    ));
    assert!(is_outlook_quick_step_default_associated_config_id(
        OUTLOOK_QUICK_STEP_CUSTOM_ACTION_ID
    ));

    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let persisted_id = Uuid::from_u128(0x6d617069_7173_4361_8000_000000000002);
    crate::mapi::identity::remember_mapi_identity(
        persisted_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 171,
        ),
    );
    let persisted = MapiMailStoreSnapshot::empty().with_associated_configs(vec![
        crate::store::MapiAssociatedConfigRecord {
            id: persisted_id,
            account_id,
            folder_id: crate::mapi::identity::QUICK_STEP_SETTINGS_FOLDER_ID,
            message_class: OUTLOOK_QUICK_STEP_CUSTOM_ACTION_CLASS.to_string(),
            subject: "Client custom action".to_string(),
            properties_json: serde_json::json!({}),
        },
    ]);

    let persisted_messages = persisted.associated_config_messages_for_folder(
        crate::mapi::identity::QUICK_STEP_SETTINGS_FOLDER_ID,
    );
    assert_eq!(
        persisted_messages
            .iter()
            .filter(|message| message.message_class == OUTLOOK_QUICK_STEP_CUSTOM_ACTION_CLASS)
            .count(),
        1
    );
    assert_eq!(
        persisted_messages
            .iter()
            .find(|message| message.message_class == OUTLOOK_QUICK_STEP_CUSTOM_ACTION_CLASS)
            .map(|message| message.subject.as_str()),
        Some("Client custom action")
    );
}

#[test]
fn contacts_include_default_osc_contact_sync_without_duplicate() {
    let snapshot = MapiMailStoreSnapshot::empty();

    for (folder_id, sync_message_id, timestamp_message_id) in [(
        crate::mapi::identity::CONTACTS_FOLDER_ID,
        OUTLOOK_CONTACTS_OSC_CONTACT_SYNC_ID,
        OUTLOOK_CONTACTS_CONTACT_LINK_TIMESTAMP_ID,
    )] {
        let messages = snapshot.associated_config_messages_for_folder(folder_id);
        assert_eq!(
            messages
                .iter()
                .filter(|message| message.message_class == OUTLOOK_CONTACT_SYNC_CONFIG_CLASS)
                .count(),
            1
        );
        assert_eq!(
            messages
                .iter()
                .filter(|message| {
                    message.message_class == OUTLOOK_CONTACT_LINK_TIMESTAMP_CONFIG_CLASS
                })
                .count(),
            1
        );
        assert_eq!(
            snapshot
                .associated_config_message_for_id(sync_message_id)
                .map(|message| message.message_class),
            Some(OUTLOOK_CONTACT_SYNC_CONFIG_CLASS.to_string())
        );
        assert_eq!(
            snapshot
                .associated_config_message_for_id(timestamp_message_id)
                .map(|message| message.message_class),
            Some(OUTLOOK_CONTACT_LINK_TIMESTAMP_CONFIG_CLASS.to_string())
        );
        assert_eq!(
            snapshot
                .associated_config_message_for_folder_and_source_key_id(
                    folder_id,
                    timestamp_message_id
                )
                .map(|message| message.message_class),
            Some(OUTLOOK_CONTACT_LINK_TIMESTAMP_CONFIG_CLASS.to_string())
        );
        assert!(snapshot.associated_config_identity_matches_folder(folder_id, sync_message_id));
        assert!(snapshot.associated_config_identity_matches_folder(folder_id, timestamp_message_id));
        assert!(!snapshot.associated_config_identity_matches_folder(
            crate::mapi::identity::INBOX_FOLDER_ID,
            timestamp_message_id
        ));
    }

    let messages = snapshot
        .associated_config_messages_for_folder(crate::mapi::identity::SUGGESTED_CONTACTS_FOLDER_ID);
    assert_eq!(
        messages
            .iter()
            .filter(|message| message.message_class == OUTLOOK_CONTACT_SYNC_CONFIG_CLASS)
            .count(),
        0
    );
    assert_eq!(
        messages
            .iter()
            .filter(|message| {
                message.message_class == OUTLOOK_CONTACT_LINK_TIMESTAMP_CONFIG_CLASS
            })
            .count(),
        1
    );
    assert_eq!(
        snapshot
            .associated_config_message_for_id(OUTLOOK_SUGGESTED_CONTACTS_OSC_CONTACT_SYNC_ID)
            .map(|message| message.message_class),
        None
    );
    assert_eq!(
        snapshot
            .associated_config_message_for_id(OUTLOOK_SUGGESTED_CONTACTS_CONTACT_LINK_TIMESTAMP_ID)
            .map(|message| message.message_class),
        Some(OUTLOOK_CONTACT_LINK_TIMESTAMP_CONFIG_CLASS.to_string())
    );
}

#[test]
fn dynamic_contact_folder_includes_default_osc_contact_sync() {
    let folder_id = crate::mapi::identity::mapi_store_id(0x4e);
    let collection = CollaborationCollection {
        id: "outlook-log-dynamic-contacts".to_string(),
        kind: "contacts".to_string(),
        display_name: "Contacts".to_string(),
        owner_account_id: Uuid::from_u128(0x4e),
        owner_email: "owner@example.test".to_string(),
        owner_display_name: "Owner".to_string(),
        is_owned: true,
        rights: CollaborationRights {
            may_read: true,
            may_write: true,
            may_delete: true,
            may_share: true,
        },
    };
    crate::mapi::identity::remember_mapi_identity(
        collaboration_folder_identity_canonical_id(
            MapiCollaborationFolderKind::Contacts,
            &collection,
        )
        .unwrap(),
        folder_id,
    );
    let snapshot = MapiMailStoreSnapshot::new(
        Vec::new(),
        Vec::new(),
        Vec::new(),
        vec![collection],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    let message_id = outlook_dynamic_contact_sync_config_id(folder_id).unwrap();
    let timestamp_message_id = outlook_dynamic_contact_link_timestamp_config_id(folder_id).unwrap();
    let messages = snapshot.associated_config_messages_for_folder(folder_id);

    assert_eq!(
        messages
            .iter()
            .filter(|message| message.message_class == OUTLOOK_CONTACT_SYNC_CONFIG_CLASS)
            .count(),
        1
    );
    assert_eq!(
        messages
            .iter()
            .filter(|message| message.message_class == OUTLOOK_CONTACT_LINK_TIMESTAMP_CONFIG_CLASS)
            .count(),
        1
    );
    assert_eq!(
        snapshot
            .associated_config_message_for_id(message_id)
            .map(|message| (message.folder_id, message.message_class)),
        Some((folder_id, OUTLOOK_CONTACT_SYNC_CONFIG_CLASS.to_string()))
    );
    assert_eq!(
        snapshot
            .associated_config_message_for_id(timestamp_message_id)
            .map(|message| (message.folder_id, message.message_class)),
        Some((
            folder_id,
            OUTLOOK_CONTACT_LINK_TIMESTAMP_CONFIG_CLASS.to_string()
        ))
    );
    assert_eq!(
        snapshot
            .associated_config_message_for_folder_and_source_key_id(folder_id, message_id)
            .map(|message| message.message_class),
        Some(OUTLOOK_CONTACT_SYNC_CONFIG_CLASS.to_string())
    );
    assert!(snapshot.associated_config_identity_matches_folder(folder_id, message_id));
    assert!(snapshot.associated_config_identity_matches_folder(folder_id, timestamp_message_id));
    assert!(!snapshot.associated_config_identity_matches_folder(
        crate::mapi::identity::INBOX_FOLDER_ID,
        timestamp_message_id
    ));
    assert!(is_outlook_contact_default_associated_config_id(message_id));
    assert!(is_outlook_contact_default_associated_config_id(
        timestamp_message_id
    ));
}

#[test]
fn mailbox_backed_contact_folder_includes_default_osc_contact_sync() {
    let folder_id = crate::mapi::identity::mapi_store_id(0x55);
    let mailbox_id = Uuid::parse_str("aaaaaaaa-7777-4111-8111-aaaaaaaaaaaa").unwrap();
    crate::mapi::identity::remember_mapi_identity(mailbox_id, folder_id);
    let snapshot = MapiMailStoreSnapshot::new(
        vec![JmapMailbox {
            id: mailbox_id,
            parent_id: None,
            role: String::new(),
            name: "Quick Contacts".to_string(),
            sort_order: 0,
            modseq: 1,
            total_emails: 0,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        }],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    let message_id = outlook_dynamic_contact_sync_config_id(folder_id).unwrap();
    let messages = snapshot.associated_config_messages_for_folder(folder_id);

    assert_eq!(
        messages
            .iter()
            .filter(|message| message.message_class == OUTLOOK_CONTACT_SYNC_CONFIG_CLASS)
            .count(),
        1
    );
    assert_eq!(
        snapshot
            .associated_config_message_for_id(message_id)
            .map(|message| (message.folder_id, message.message_class)),
        Some((folder_id, OUTLOOK_CONTACT_SYNC_CONFIG_CLASS.to_string()))
    );
    assert!(snapshot.associated_config_identity_matches_folder(folder_id, message_id));
}

#[test]
fn mailbox_backed_suggested_contacts_includes_default_osc_contact_sync() {
    let folder_id = crate::mapi::identity::mapi_store_id(0x54);
    let mailbox_id = Uuid::parse_str("aaaaaaaa-7777-4111-8111-aaaaaaaaaaab").unwrap();
    crate::mapi::identity::remember_mapi_identity(mailbox_id, folder_id);
    let snapshot = MapiMailStoreSnapshot::new(
        vec![JmapMailbox {
            id: mailbox_id,
            parent_id: None,
            role: String::new(),
            name: "Suggested Contacts".to_string(),
            sort_order: 0,
            modseq: 1,
            total_emails: 0,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        }],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    let message_id = outlook_dynamic_contact_sync_config_id(folder_id).unwrap();

    assert_eq!(
        snapshot
            .associated_config_message_for_id(message_id)
            .map(|message| (message.folder_id, message.message_class)),
        Some((folder_id, OUTLOOK_CONTACT_SYNC_CONFIG_CLASS.to_string()))
    );
    assert!(snapshot.associated_config_identity_matches_folder(folder_id, message_id));
}

#[test]
fn associated_config_identity_only_placeholder_does_not_open_without_backing_message() {
    let object_id = crate::mapi::identity::mapi_store_id(
        crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 901,
    );
    let snapshot = MapiMailStoreSnapshot::empty().with_associated_config_identity_ids(vec![
        MapiAssociatedConfigIdentity {
            canonical_id: Uuid::from_u128(0xaabbccdd_0000_0000_0000_000000000001),
            object_id,
        },
    ]);

    assert!(!snapshot.associated_config_identity_matches_folder(
        crate::mapi::identity::INBOX_FOLDER_ID,
        object_id
    ));
    assert!(!snapshot.associated_config_identity_matches_folder(
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
        object_id
    ));
}

#[test]
fn modeled_virtual_associated_config_identity_opens_via_dynamic_id() {
    let object_id = crate::mapi::identity::mapi_store_id(
        crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 902,
    );
    let canonical_id = Uuid::from_u128(0x6d617069_756d_6f6c_8000_000000000902);
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    crate::mapi::identity::remember_mapi_identity(canonical_id, object_id);
    let snapshot = MapiMailStoreSnapshot::empty()
        .with_associated_config_identity_ids(vec![MapiAssociatedConfigIdentity {
            canonical_id,
            object_id,
        }])
        .with_associated_configs(vec![crate::store::MapiAssociatedConfigRecord {
            id: canonical_id,
            account_id,
            folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
            message_class: OUTLOOK_INBOX_UMOLK_USER_OPTIONS_CONFIG_CLASS.to_string(),
            subject: "Persisted UMOLK".to_string(),
            properties_json: serde_json::json!({
                "0x7c060003": {"type": "u32", "value": 4}
            }),
        }]);

    assert_eq!(
        snapshot
            .associated_config_message_for_identity_id(object_id)
            .map(|message| (message.folder_id, message.message_class)),
        Some((
            crate::mapi::identity::INBOX_FOLDER_ID,
            OUTLOOK_INBOX_UMOLK_USER_OPTIONS_CONFIG_CLASS.to_string()
        ))
    );
    assert!(snapshot.associated_config_identity_matches_folder(
        crate::mapi::identity::INBOX_FOLDER_ID,
        object_id
    ));
}

#[test]
fn empty_conversation_action_settings_exposes_default_table_row_only() {
    let snapshot = MapiMailStoreSnapshot::empty();

    assert!(snapshot.conversation_action_messages().is_empty());

    let messages = snapshot.conversation_action_table_messages();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].id, OUTLOOK_DEFAULT_CONVERSATION_ACTION_ID);
    assert_eq!(
        messages[0].folder_id,
        crate::mapi::identity::CONVERSATION_ACTION_SETTINGS_FOLDER_ID
    );
    assert_eq!(messages[0].action.subject, "IPM.ConversationAction");
    assert_eq!(
        snapshot
            .conversation_action_table_message_for_id(OUTLOOK_DEFAULT_CONVERSATION_ACTION_ID)
            .map(|message| message.action.subject),
        Some("IPM.ConversationAction".to_string())
    );
    assert!(snapshot
        .conversation_action_message_for_id(OUTLOOK_DEFAULT_CONVERSATION_ACTION_ID)
        .is_none());
}

#[test]
fn common_views_projects_default_named_views_and_shortcuts_for_table_only() {
    let snapshot = MapiMailStoreSnapshot::empty();
    assert!(snapshot.navigation_shortcut_messages().is_empty());
    assert_eq!(snapshot.common_views_messages().count(), 0);
    let messages = snapshot.common_views_table_messages().collect::<Vec<_>>();

    assert_eq!(messages.len(), 19);
    assert_eq!(
        messages
            .iter()
            .filter(|message| matches!(message, MapiCommonViewsMessage::NavigationShortcut(_)))
            .count(),
        17
    );
    let named_views = messages
        .iter()
        .filter_map(|message| match message {
            MapiCommonViewsMessage::NamedView(view) => Some(view),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(named_views.len(), 2);
    assert!(named_views
        .iter()
        .any(|view| view.name == "Compact" && view.view_flags == 14_745_605));
    assert!(named_views
        .iter()
        .any(|view| view.name == "Sent To" && view.view_flags == 15_269_893));
    assert!(named_views.iter().all(|view| view.view_type == 8));
    assert!(snapshot
        .navigation_shortcut_table_message_for_id(0)
        .is_none());
    assert!(snapshot
        .navigation_shortcut_table_message_for_id(
            OUTLOOK_COMMON_VIEWS_DEFAULT_NAVIGATION_SHORTCUT_ID
        )
        .is_some());
    assert!(snapshot
        .navigation_shortcut_table_message_for_id(
            OUTLOOK_COMMON_VIEWS_DEFAULT_SENT_NAVIGATION_SHORTCUT_ID
        )
        .is_some());
    assert!(snapshot
        .navigation_shortcut_table_message_for_id(
            OUTLOOK_COMMON_VIEWS_DEFAULT_TRASH_NAVIGATION_SHORTCUT_ID
        )
        .is_some());
    assert!(snapshot
        .navigation_shortcut_table_message_for_id(
            OUTLOOK_COMMON_VIEWS_DEFAULT_CALENDAR_GROUP_HEADER_ID
        )
        .is_some());
    assert!(snapshot
        .navigation_shortcut_table_message_for_id(
            OUTLOOK_COMMON_VIEWS_DEFAULT_CALENDAR_NAVIGATION_SHORTCUT_ID
        )
        .is_some());
    assert!(snapshot
        .navigation_shortcut_table_message_for_id(
            OUTLOOK_COMMON_VIEWS_DEFAULT_CONTACTS_GROUP_HEADER_ID
        )
        .is_some());
    assert!(snapshot
        .navigation_shortcut_table_message_for_id(
            OUTLOOK_COMMON_VIEWS_DEFAULT_CONTACTS_NAVIGATION_SHORTCUT_ID
        )
        .is_some());
    assert!(snapshot
        .navigation_shortcut_table_message_for_id(
            OUTLOOK_COMMON_VIEWS_DEFAULT_TASKS_GROUP_HEADER_ID
        )
        .is_some());
    assert!(snapshot
        .navigation_shortcut_table_message_for_id(
            OUTLOOK_COMMON_VIEWS_DEFAULT_TASKS_NAVIGATION_SHORTCUT_ID
        )
        .is_some());
    assert!(snapshot
        .navigation_shortcut_table_message_for_id(
            OUTLOOK_COMMON_VIEWS_DEFAULT_NOTES_GROUP_HEADER_ID
        )
        .is_some());
    assert!(snapshot
        .navigation_shortcut_table_message_for_id(
            OUTLOOK_COMMON_VIEWS_DEFAULT_NOTES_NAVIGATION_SHORTCUT_ID
        )
        .is_some());
    assert!(snapshot
        .navigation_shortcut_table_message_for_id(
            OUTLOOK_COMMON_VIEWS_DEFAULT_JOURNAL_GROUP_HEADER_ID
        )
        .is_some());
    assert!(snapshot
        .navigation_shortcut_table_message_for_id(
            OUTLOOK_COMMON_VIEWS_DEFAULT_JOURNAL_NAVIGATION_SHORTCUT_ID
        )
        .is_some());
    for named_view in named_views {
        assert!(snapshot
            .common_view_named_view_message_for_id(named_view.id)
            .is_some());
    }
}

#[test]
fn default_folder_named_views_use_folder_family_names() {
    let snapshot = MapiMailStoreSnapshot::empty();

    for (folder_id, expected_name) in [
        (crate::mapi::identity::INBOX_FOLDER_ID, "Compact"),
        (crate::mapi::identity::CALENDAR_FOLDER_ID, "Calendar"),
        (crate::mapi::identity::TASKS_FOLDER_ID, "Tasks"),
        (crate::mapi::identity::NOTES_FOLDER_ID, "Notes"),
        (crate::mapi::identity::JOURNAL_FOLDER_ID, "Journal"),
    ] {
        let view = snapshot
            .default_folder_named_view_message(folder_id, OUTLOOK_DEFAULT_FOLDER_NAMED_VIEW_ID)
            .expect("default folder named view");
        assert_eq!(view.folder_id, folder_id);
        assert_eq!(view.name, expected_name);
        assert_eq!(view.view_flags, 14_745_605);
        assert_eq!(view.view_type, 8);
    }
}

#[test]
fn common_views_skips_search_folder_definition_without_protocol_blob() {
    let definition_id = Uuid::from_u128(0xaaaaaaaa_1111_4111_8111_aaaaaaaaaaaa);
    let snapshot = MapiMailStoreSnapshot::empty().with_search_folder_definitions(vec![
        SearchFolderDefinition {
            id: definition_id,
            account_id: Uuid::nil(),
            role: "reminders".to_string(),
            display_name: "Reminders".to_string(),
            definition_kind: "exchange_builtin".to_string(),
            result_object_kind: "mixed".to_string(),
            scope_json: serde_json::json!({"scope": "top_of_personal_folders"}),
            restriction_json: serde_json::json!({"kind": "exchange_reminders"}),
            excluded_folder_roles: exchange_builtin_excluded_folder_roles(),
            is_builtin: true,
        },
    ]);

    assert!(snapshot
        .common_views_table_messages()
        .all(|message| !matches!(message, MapiCommonViewsMessage::SearchFolderDefinition(_))));
}

#[test]
fn common_views_projects_search_folder_definition_with_protocol_blob() {
    let definition_id = Uuid::from_u128(0xbbbbbbbb_1111_4111_8111_bbbbbbbbbbbb);
    let mut definition_blob = vec![
        0x04, 0x10, 0x00, 0x00, 0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
    ];
    definition_blob.extend_from_slice(&1u32.to_le_bytes());
    definition_blob.push(0xAA);
    definition_blob.extend_from_slice(&0u32.to_le_bytes());
    definition_blob.push(0xBB);
    definition_blob.extend_from_slice(&0u32.to_le_bytes());
    let snapshot = MapiMailStoreSnapshot::empty().with_search_folder_definitions(vec![
        SearchFolderDefinition {
            id: definition_id,
            account_id: Uuid::nil(),
            role: "reminders".to_string(),
            display_name: "Reminders".to_string(),
            definition_kind: "exchange_builtin".to_string(),
            result_object_kind: "mixed".to_string(),
            scope_json: serde_json::json!({"scope": "top_of_personal_folders"}),
            restriction_json: serde_json::json!({
                "kind": "exchange_reminders",
                "pidTagSearchFolderDefinition": BASE64_STANDARD.encode(&definition_blob)
            }),
            excluded_folder_roles: exchange_builtin_excluded_folder_roles(),
            is_builtin: true,
        },
    ]);

    assert!(snapshot
        .common_views_table_messages()
        .any(|message| matches!(message, MapiCommonViewsMessage::SearchFolderDefinition(_))));
}

#[test]
fn common_views_preserves_persisted_navigation_shortcuts() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let persisted_id = Uuid::from_u128(0x6d617069_776c_416c_8000_000000000001);
    crate::mapi::identity::remember_mapi_identity(
        persisted_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 72,
        ),
    );
    let snapshot = MapiMailStoreSnapshot::empty().with_navigation_shortcuts(vec![
        MapiNavigationShortcutRecord {
            id: persisted_id,
            account_id,
            subject: "Alpha".to_string(),
            target_folder_id: Some(crate::mapi::identity::SENT_FOLDER_ID),
            shortcut_type: 0,
            flags: 0,
            save_stamp: 0,
            section: 1,
            ordinal: 1,
            group_header_id: None,
            group_name: "Mail".to_string(),
        },
    ]);

    let messages = snapshot.common_views_table_messages().collect::<Vec<_>>();
    let shortcut = messages
        .iter()
        .find_map(|message| match message {
            MapiCommonViewsMessage::NavigationShortcut(shortcut) if shortcut.subject == "Alpha" => {
                Some(shortcut)
            }
            _ => None,
        })
        .expect("persisted shortcut");
    assert_eq!(shortcut.subject, "Alpha");
    assert_eq!(shortcut.group_header_id, Some(default_wlink_group_uuid()));
    assert_eq!(shortcut.group_name, OUTLOOK_MAIL_FAVORITES_GROUP_NAME);
    assert_eq!(messages.len(), 6);
    assert_eq!(
        messages
            .iter()
            .filter(|message| matches!(
                message,
                MapiCommonViewsMessage::NavigationShortcut(shortcut)
                    if shortcut.shortcut_type == 4
                        && shortcut.subject == OUTLOOK_MAIL_FAVORITES_GROUP_NAME
            ))
            .count(),
        1
    );
    assert!(snapshot
        .navigation_shortcut_table_message_for_id(0)
        .is_none());
    assert!(snapshot.common_view_named_view_message_for_id(0).is_none());
}

#[test]
fn common_views_deduplicates_repeated_persisted_navigation_shortcuts() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let inbox_first_id = Uuid::from_u128(0x6d617069_776c_496e_8000_000000000010);
    let inbox_duplicate_id = Uuid::from_u128(0x6d617069_776c_496e_8000_000000000011);
    let sent_id = Uuid::from_u128(0x6d617069_776c_5365_8000_000000000010);
    for (offset, id) in [inbox_first_id, inbox_duplicate_id, sent_id]
        .into_iter()
        .enumerate()
    {
        crate::mapi::identity::remember_mapi_identity(
            id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 80 + offset as u64,
            ),
        );
    }
    let group_uuid = Uuid::from_u128(0x5ba943d8_daaa_462c_a63e_9136f65c8681);
    let snapshot = MapiMailStoreSnapshot::empty().with_navigation_shortcuts(vec![
        MapiNavigationShortcutRecord {
            id: inbox_first_id,
            account_id,
            subject: "Pinned Inbox".to_string(),
            target_folder_id: Some(crate::mapi::identity::INBOX_FOLDER_ID),
            shortcut_type: 0,
            flags: 0,
            save_stamp: 0,
            section: 1,
            ordinal: 127,
            group_header_id: Some(group_uuid),
            group_name: "Mail".to_string(),
        },
        MapiNavigationShortcutRecord {
            id: inbox_duplicate_id,
            account_id,
            subject: "Pinned Inbox".to_string(),
            target_folder_id: Some(crate::mapi::identity::INBOX_FOLDER_ID),
            shortcut_type: 0,
            flags: 0,
            save_stamp: 0,
            section: 1,
            ordinal: 127,
            group_header_id: Some(group_uuid),
            group_name: "Mail".to_string(),
        },
        MapiNavigationShortcutRecord {
            id: sent_id,
            account_id,
            subject: "Sent".to_string(),
            target_folder_id: Some(crate::mapi::identity::SENT_FOLDER_ID),
            shortcut_type: 0,
            flags: 0,
            save_stamp: 0,
            section: 1,
            ordinal: 191,
            group_header_id: Some(group_uuid),
            group_name: "Mail".to_string(),
        },
    ]);

    let shortcuts = snapshot.navigation_shortcut_messages();
    assert_eq!(shortcuts.len(), 2);
    assert_eq!(shortcuts[0].canonical_id, inbox_first_id);
    assert!(!shortcuts.iter().any(|shortcut| {
        shortcut.shortcut_type == 4
            && shortcut.subject == "Mail"
            && shortcut.group_header_id == Some(group_uuid)
    }));
    let table_messages = snapshot.common_views_table_messages().collect::<Vec<_>>();
    assert_eq!(
        table_messages
            .iter()
            .filter(|message| matches!(
                message,
                MapiCommonViewsMessage::NavigationShortcut(shortcut)
                    if shortcut.subject == "Pinned Inbox"
                        && shortcut.target_folder_id
                            == Some(crate::mapi::identity::INBOX_FOLDER_ID)
            ))
            .count(),
        1
    );
}

#[test]
fn common_views_materializes_mail_group_header_for_custom_persisted_favorite_links() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let inbox_id = Uuid::from_u128(0x6d617069_776c_496e_8000_000000000020);
    crate::mapi::identity::remember_mapi_identity(
        inbox_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 91,
        ),
    );
    let group_uuid = default_wlink_group_uuid();
    let snapshot = MapiMailStoreSnapshot::empty().with_navigation_shortcuts(vec![
        MapiNavigationShortcutRecord {
            id: inbox_id,
            account_id,
            subject: "Pinned Inbox".to_string(),
            target_folder_id: Some(crate::mapi::identity::INBOX_FOLDER_ID),
            shortcut_type: 0,
            flags: 0x0010_8000,
            save_stamp: 0,
            section: 1,
            ordinal: 127,
            group_header_id: Some(group_uuid),
            group_name: "Mail".to_string(),
        },
    ]);

    let shortcuts = snapshot.navigation_shortcut_messages();
    assert!(!shortcuts.iter().any(|shortcut| {
        shortcut.id == OUTLOOK_COMMON_VIEWS_DEFAULT_MAIL_GROUP_HEADER_ID
            && shortcut.shortcut_type == 4
            && shortcut.subject == OUTLOOK_MAIL_FAVORITES_GROUP_NAME
            && shortcut.group_header_id == Some(group_uuid)
    }));
    assert!(!snapshot.common_views_messages().any(|message| matches!(
        message,
        MapiCommonViewsMessage::NavigationShortcut(shortcut)
            if shortcut.shortcut_type == 4
                && shortcut.subject == OUTLOOK_MAIL_FAVORITES_GROUP_NAME
    )));
    assert!(snapshot
        .common_views_table_messages()
        .any(|message| matches!(
            message,
            MapiCommonViewsMessage::NavigationShortcut(shortcut)
                if shortcut.shortcut_type == 4
                    && shortcut.subject == OUTLOOK_MAIL_FAVORITES_GROUP_NAME
        )));
    assert_eq!(
        snapshot
            .common_views_table_messages()
            .filter(|message| matches!(
                message,
                MapiCommonViewsMessage::NavigationShortcut(shortcut)
                    if shortcut.shortcut_type == 4
                        && shortcut.subject == OUTLOOK_MAIL_FAVORITES_GROUP_NAME
            ))
            .count(),
        1
    );
}

#[test]
fn common_views_projects_persisted_default_mail_favorites_in_startup_table() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let inbox_id = Uuid::from_u128(0x6d617069_776c_496e_8000_000000000030);
    let sent_id = Uuid::from_u128(0x6d617069_776c_5365_8000_000000000030);
    let trash_id = Uuid::from_u128(0x6d617069_776c_5472_8000_000000000030);
    for (offset, id) in [inbox_id, sent_id, trash_id].into_iter().enumerate() {
        crate::mapi::identity::remember_mapi_identity(
            id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 92 + offset as u64,
            ),
        );
    }
    let snapshot = MapiMailStoreSnapshot::empty().with_navigation_shortcuts(vec![
        MapiNavigationShortcutRecord {
            id: inbox_id,
            account_id,
            subject: "Inbox".to_string(),
            target_folder_id: Some(crate::mapi::identity::INBOX_FOLDER_ID),
            shortcut_type: 0,
            flags: 0x0010_8000,
            save_stamp: 0,
            section: 1,
            ordinal: 127,
            group_header_id: None,
            group_name: "Mail".to_string(),
        },
        MapiNavigationShortcutRecord {
            id: sent_id,
            account_id,
            subject: "Sent".to_string(),
            target_folder_id: Some(crate::mapi::identity::SENT_FOLDER_ID),
            shortcut_type: 0,
            flags: 0x0010_8000,
            save_stamp: 0,
            section: 1,
            ordinal: 191,
            group_header_id: None,
            group_name: "Mail".to_string(),
        },
        MapiNavigationShortcutRecord {
            id: trash_id,
            account_id,
            subject: "Trash".to_string(),
            target_folder_id: Some(crate::mapi::identity::TRASH_FOLDER_ID),
            shortcut_type: 0,
            flags: 0x0010_8000,
            save_stamp: 0,
            section: 1,
            ordinal: 223,
            group_header_id: None,
            group_name: "Mail".to_string(),
        },
    ]);

    assert_eq!(snapshot.navigation_shortcut_messages().len(), 3);
    let table_messages = snapshot.common_views_table_messages().collect::<Vec<_>>();
    assert_eq!(table_messages.len(), 6);
    assert_eq!(
        table_messages
            .iter()
            .filter(|message| matches!(message, MapiCommonViewsMessage::NavigationShortcut(_)))
            .count(),
        4
    );
    assert!(table_messages.iter().any(|message| matches!(
        message,
        MapiCommonViewsMessage::NavigationShortcut(shortcut)
            if shortcut.shortcut_type == 4
                && shortcut.subject == OUTLOOK_MAIL_FAVORITES_GROUP_NAME
    )));
    assert!(table_messages.iter().any(|message| matches!(
        message,
        MapiCommonViewsMessage::NavigationShortcut(shortcut)
            if shortcut.subject == "Inbox"
                && shortcut.target_folder_id == Some(crate::mapi::identity::INBOX_FOLDER_ID)
    )));
}

#[test]
fn common_views_projects_supported_module_shortcuts_in_startup_table() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let first_calendar_id = Uuid::from_u128(0x6d617069_776c_4361_8000_000000000020);
    let second_calendar_id = Uuid::from_u128(0x6d617069_776c_4361_8000_000000000021);
    for (offset, id) in [first_calendar_id, second_calendar_id]
        .into_iter()
        .enumerate()
    {
        crate::mapi::identity::remember_mapi_identity(
            id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 90 + offset as u64,
            ),
        );
    }

    let snapshot = MapiMailStoreSnapshot::empty().with_navigation_shortcuts(vec![
        MapiNavigationShortcutRecord {
            id: first_calendar_id,
            account_id,
            subject: "Calendar".to_string(),
            target_folder_id: Some(crate::mapi::identity::CALENDAR_FOLDER_ID),
            shortcut_type: 0,
            flags: 0,
            save_stamp: 0,
            section: 1,
            ordinal: 255,
            group_header_id: Some(default_wlink_group_uuid()),
            group_name: "Mail".to_string(),
        },
        MapiNavigationShortcutRecord {
            id: second_calendar_id,
            account_id,
            subject: "Calendar".to_string(),
            target_folder_id: Some(crate::mapi::identity::CALENDAR_FOLDER_ID),
            shortcut_type: 0,
            flags: 0,
            save_stamp: 0,
            section: 1,
            ordinal: 511,
            group_header_id: Some(Uuid::from_u128(0x5ba943d8_daaa_462c_a63e_9136f65c8681)),
            group_name: "My Calendars".to_string(),
        },
    ]);

    assert_eq!(snapshot.navigation_shortcut_messages().len(), 1);
    assert_eq!(
        snapshot
            .navigation_shortcut_messages()
            .first()
            .and_then(|shortcut| shortcut.target_folder_id),
        Some(crate::mapi::identity::CALENDAR_FOLDER_ID)
    );
    assert!(snapshot
        .navigation_shortcut_message_for_id(crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 90
        ))
        .is_some());
    assert!(snapshot
        .navigation_shortcut_table_message_for_id(crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 90
        ))
        .is_some());
    let table_messages = snapshot.common_views_table_messages().collect::<Vec<_>>();
    assert_eq!(
        table_messages
            .iter()
            .filter(|message| matches!(
                message,
                MapiCommonViewsMessage::NavigationShortcut(shortcut)
                    if shortcut.target_folder_id
                        == Some(crate::mapi::identity::CALENDAR_FOLDER_ID)
            ))
            .count(),
        1
    );
}

#[test]
fn snapshot_projects_canonical_mailbox_message_and_attachment_ids() {
    let mailbox_id = Uuid::parse_str("44444444-4444-4444-4444-444444444444").unwrap();
    let message_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap();
    let attachment_id = Uuid::parse_str("abababab-abab-abab-abab-abababababab").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        mailbox_id,
        crate::mapi::identity::mapi_store_id(17),
    );
    crate::mapi::identity::remember_mapi_identity(
        message_id,
        crate::mapi::identity::mapi_store_id(18),
    );
    let mailbox = JmapMailbox {
        id: mailbox_id,
        parent_id: None,
        role: "custom".to_string(),
        name: "RCA Sync".to_string(),
        sort_order: 10,
        modseq: 40,
        total_emails: 1,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    };
    let email = JmapEmail {
        id: message_id,
        thread_id: Uuid::parse_str("12121212-1212-1212-1212-121212121212").unwrap(),
        mailbox_id,
        mailbox_role: "custom".to_string(),
        mailbox_name: "RCA Sync".to_string(),
        modseq: 41,
        mailbox_ids: vec![mailbox_id],
        mailbox_states: vec![JmapEmailMailboxState {
            mailbox_id,
            role: "custom".to_string(),
            name: "RCA Sync".to_string(),
            modseq: 41,
            unread: false,
            flagged: false,
            followup_flag_status: "none".to_string(),
            followup_icon: 0,
            todo_item_flags: 0,
            followup_request: String::new(),
            followup_start_at: None,
            followup_due_at: None,
            followup_completed_at: None,
            reminder_set: false,
            reminder_at: None,
            reminder_dismissed_at: None,
            swapped_todo_store_id: None,
            swapped_todo_data: None,
            categories: Vec::new(),
            draft: false,
        }],
        received_at: "2026-05-03T12:00:00Z".to_string(),
        sent_at: None,
        from_address: "alice@example.test".to_string(),
        from_display: Some("Alice".to_string()),
        sender_address: None,
        sender_display: None,
        sender_authorization_kind: "self".to_string(),
        submitted_by_account_id: Uuid::nil(),
        to: vec![JmapEmailAddress {
            address: "bob@example.test".to_string(),
            display_name: Some("Bob".to_string()),
        }],
        cc: Vec::new(),
        bcc: Vec::new(),
        subject: "Hello".to_string(),
        preview: "Hello".to_string(),
        body_text: "Hello".to_string(),
        body_html_sanitized: None,
        unread: false,
        flagged: false,
        followup_flag_status: "none".to_string(),
        followup_icon: 0,
        todo_item_flags: 0,
        followup_request: String::new(),
        followup_start_at: None,
        followup_due_at: None,
        followup_completed_at: None,
        reminder_set: false,
        reminder_at: None,
        reminder_dismissed_at: None,
        swapped_todo_store_id: None,
        swapped_todo_data: None,
        categories: Vec::new(),
        has_attachments: true,
        size_octets: 42,
        internet_message_id: None,
        mime_blob_ref: None,
        delivery_status: "stored".to_string(),
    };
    let attachment = ActiveSyncAttachment {
        id: attachment_id,
        message_id,
        file_name: "brief.pdf".to_string(),
        media_type: "application/pdf".to_string(),
        disposition: None,
        content_id: None,
        size_octets: 5,
        file_reference: "attachment-ref".to_string(),
    };

    let snapshot = MapiMailStoreSnapshot::new(
        vec![mailbox],
        vec![email],
        vec![(message_id, vec![attachment])],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );

    assert_eq!(snapshot.folders().len(), 1);
    assert_eq!(snapshot.messages().len(), 1);
    assert_eq!(snapshot.messages()[0].canonical_id, message_id);
    assert_eq!(snapshot.messages()[0].folder_id, snapshot.folders()[0].id);
    assert_eq!(
        snapshot.messages()[0].attachments[0].canonical_id,
        attachment_id
    );
    assert_eq!(snapshot.messages()[0].attachments[0].attach_num, 0);
}

#[test]
fn snapshot_projects_outlook_contact_books_into_fixed_mapi_folders() {
    let account_id = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap();
    let rights = CollaborationRights {
        may_read: true,
        may_write: true,
        may_delete: true,
        may_share: false,
    };
    let cases = [
        (
            "suggested_contacts",
            "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "Suggested Contacts",
            crate::mapi::identity::SUGGESTED_CONTACTS_FOLDER_ID,
        ),
        (
            "quick_contacts",
            "cccccccc-cccc-cccc-cccc-cccccccccccc",
            "Quick Contacts",
            crate::mapi::identity::QUICK_CONTACTS_FOLDER_ID,
        ),
        (
            "im_contact_list",
            "dddddddd-dddd-dddd-dddd-dddddddddddd",
            "IM Contact List",
            crate::mapi::identity::IM_CONTACT_LIST_FOLDER_ID,
        ),
    ];
    let collections = cases
        .iter()
        .map(
            |(collection_id, _, display_name, _)| CollaborationCollection {
                id: (*collection_id).to_string(),
                kind: "contacts".to_string(),
                owner_account_id: account_id,
                owner_email: "alice@example.test".to_string(),
                owner_display_name: "Alice".to_string(),
                display_name: (*display_name).to_string(),
                is_owned: true,
                rights: rights.clone(),
            },
        )
        .collect::<Vec<_>>();
    let contacts = cases
        .iter()
        .enumerate()
        .map(|(index, (collection_id, contact_id, _, _))| {
            let contact_id = Uuid::parse_str(contact_id).unwrap();
            crate::mapi::identity::remember_mapi_identity(
                contact_id,
                crate::mapi::identity::mapi_store_id(92 + index as u64),
            );
            AccessibleContact {
                id: contact_id,
                collection_id: (*collection_id).to_string(),
                owner_account_id: account_id,
                owner_email: "alice@example.test".to_string(),
                owner_display_name: "Alice".to_string(),
                rights: rights.clone(),
                name: "Outlook Contact".to_string(),
                role: String::new(),
                email: "contact@example.test".to_string(),
                phone: String::new(),
                team: String::new(),
                notes: String::new(),
                ..Default::default()
            }
        })
        .collect::<Vec<_>>();

    let snapshot = MapiMailStoreSnapshot::new(
        Vec::new(),
        Vec::new(),
        Vec::new(),
        collections,
        Vec::new(),
        Vec::new(),
        contacts,
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );

    for (_, contact_id, _, folder_id) in cases {
        assert!(snapshot
            .collaboration_folders()
            .iter()
            .any(|folder| folder.id == folder_id));
        assert_eq!(
            snapshot.contacts_for_folder(folder_id)[0].canonical_id,
            Uuid::parse_str(contact_id).unwrap()
        );
    }

    let definition_id = Uuid::parse_str("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        definition_id,
        crate::mapi::identity::mapi_store_id(95),
    );
    let snapshot = snapshot.with_search_folder_definitions(vec![SearchFolderDefinition {
        id: definition_id,
        account_id,
        role: "contacts_search".to_string(),
        display_name: "Contacts Search".to_string(),
        definition_kind: "exchange_builtin".to_string(),
        result_object_kind: "contact".to_string(),
        scope_json: serde_json::json!({"scope": "contacts_folders"}),
        restriction_json: serde_json::json!({"kind": "exchange_contacts_search"}),
        excluded_folder_roles: Vec::new(),
        is_builtin: true,
    }]);
    assert_eq!(snapshot.contacts_search_results().len(), 3);
    assert!(snapshot
        .contact_for_id(
            crate::mapi::identity::CONTACTS_SEARCH_FOLDER_ID,
            crate::mapi::identity::mapi_store_id(92)
        )
        .is_some());
}

#[test]
fn collaboration_folder_identity_requests_cover_custom_and_shared_collections() {
    let owner_id = Uuid::parse_str("99999999-9999-4999-8999-999999999999").unwrap();
    let custom_calendar_id = Uuid::parse_str("aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa").unwrap();
    let rights = CollaborationRights {
        may_read: true,
        may_write: true,
        may_delete: true,
        may_share: true,
    };
    let contact_collections = vec![CollaborationCollection {
        id: format!("shared-contacts-{owner_id}"),
        kind: "contacts".to_string(),
        owner_account_id: owner_id,
        owner_email: "owner@example.test".to_string(),
        owner_display_name: "Owner".to_string(),
        display_name: "Owner Contacts".to_string(),
        is_owned: false,
        rights: rights.clone(),
    }];
    let calendar_collections = vec![
        CollaborationCollection {
            id: custom_calendar_id.to_string(),
            kind: "calendar".to_string(),
            owner_account_id: owner_id,
            owner_email: "owner@example.test".to_string(),
            owner_display_name: "Owner".to_string(),
            display_name: "Custom".to_string(),
            is_owned: true,
            rights: rights.clone(),
        },
        CollaborationCollection {
            id: format!("shared-calendar-{owner_id}"),
            kind: "calendar".to_string(),
            owner_account_id: owner_id,
            owner_email: "owner@example.test".to_string(),
            owner_display_name: "Owner".to_string(),
            display_name: "Owner Calendar".to_string(),
            is_owned: false,
            rights: rights.clone(),
        },
    ];
    let task_collections = vec![CollaborationCollection {
        id: format!("shared-tasks-{owner_id}"),
        kind: "tasks".to_string(),
        owner_account_id: owner_id,
        owner_email: "owner@example.test".to_string(),
        owner_display_name: "Owner".to_string(),
        display_name: "Owner Tasks".to_string(),
        is_owned: false,
        rights,
    }];

    let requests = collaboration_folder_identity_requests(
        &contact_collections,
        &calendar_collections,
        &task_collections,
    );
    let canonical_ids = requests
        .iter()
        .map(|request| request.canonical_id)
        .collect::<Vec<_>>();

    assert_eq!(requests.len(), 4);
    assert!(requests
        .iter()
        .all(|request| request.object_kind == MapiIdentityObjectKind::Mailbox));
    assert_eq!(
        canonical_ids
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>()
            .len(),
        4
    );
    assert!(!canonical_ids.contains(&owner_id));
    assert!(!canonical_ids.contains(&custom_calendar_id));
}

#[test]
fn snapshot_uses_allocated_identities_for_custom_and_shared_collaboration_folders() {
    let owner_id = Uuid::parse_str("99999999-9999-4999-8999-999999999999").unwrap();
    let rights = CollaborationRights {
        may_read: true,
        may_write: true,
        may_delete: true,
        may_share: true,
    };
    let contact_collection = CollaborationCollection {
        id: format!("shared-contacts-{owner_id}"),
        kind: "contacts".to_string(),
        owner_account_id: owner_id,
        owner_email: "owner@example.test".to_string(),
        owner_display_name: "Owner".to_string(),
        display_name: "Owner Contacts".to_string(),
        is_owned: false,
        rights: rights.clone(),
    };
    let calendar_collection = CollaborationCollection {
        id: format!("shared-calendar-{owner_id}"),
        kind: "calendar".to_string(),
        owner_account_id: owner_id,
        owner_email: "owner@example.test".to_string(),
        owner_display_name: "Owner".to_string(),
        display_name: "Owner Calendar".to_string(),
        is_owned: false,
        rights: rights.clone(),
    };
    let task_collection = CollaborationCollection {
        id: format!("shared-tasks-{owner_id}"),
        kind: "tasks".to_string(),
        owner_account_id: owner_id,
        owner_email: "owner@example.test".to_string(),
        owner_display_name: "Owner".to_string(),
        display_name: "Owner Tasks".to_string(),
        is_owned: false,
        rights,
    };
    let cases = [
        (
            MapiCollaborationFolderKind::Contacts,
            &contact_collection,
            crate::mapi::identity::mapi_store_id(201),
        ),
        (
            MapiCollaborationFolderKind::Calendar,
            &calendar_collection,
            crate::mapi::identity::mapi_store_id(202),
        ),
        (
            MapiCollaborationFolderKind::Task,
            &task_collection,
            crate::mapi::identity::mapi_store_id(203),
        ),
    ];
    for (kind, collection, object_id) in cases {
        let canonical_id = collaboration_folder_identity_canonical_id(kind, collection).unwrap();
        crate::mapi::identity::remember_mapi_identity(canonical_id, object_id);
    }

    let snapshot = MapiMailStoreSnapshot::new(
        Vec::new(),
        Vec::new(),
        Vec::new(),
        vec![contact_collection],
        vec![calendar_collection],
        vec![task_collection],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    let folder_ids = snapshot
        .collaboration_folders()
        .iter()
        .map(|folder| folder.id)
        .collect::<Vec<_>>();

    assert!(folder_ids.contains(&crate::mapi::identity::mapi_store_id(201)));
    assert!(folder_ids.contains(&crate::mapi::identity::mapi_store_id(202)));
    assert!(folder_ids.contains(&crate::mapi::identity::mapi_store_id(203)));
    assert_eq!(
        folder_ids
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>()
            .len(),
        3
    );
}

#[test]
fn snapshot_projects_canonical_notes_and_journal_into_default_mapi_folders() {
    let note_id = Uuid::parse_str("51515151-5151-5151-5151-515151515151").unwrap();
    let journal_id = Uuid::parse_str("61616161-6161-6161-6161-616161616161").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        note_id,
        crate::mapi::identity::mapi_store_id(90),
    );
    crate::mapi::identity::remember_mapi_identity(
        journal_id,
        crate::mapi::identity::mapi_store_id(91),
    );

    let snapshot = MapiMailStoreSnapshot::new(
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_notes_and_journal(
        vec![ClientNote {
            id: note_id,
            title: "Sticky note".to_string(),
            body_text: "Remember Outlook content tables".to_string(),
            color: "yellow".to_string(),
            categories_json: "[]".to_string(),
            created_at: "2026-05-19T12:00:00Z".to_string(),
            updated_at: "2026-05-19T12:30:00Z".to_string(),
        }],
        vec![JournalEntry {
            id: journal_id,
            subject: "Support call".to_string(),
            body_text: "Call notes".to_string(),
            entry_type: "phone-call".to_string(),
            message_class: "IPM.Activity".to_string(),
            starts_at: Some("2026-05-19T13:00:00Z".to_string()),
            ends_at: Some("2026-05-19T13:15:00Z".to_string()),
            occurred_at: None,
            companies_json: "[]".to_string(),
            contacts_json: "[]".to_string(),
            created_at: "2026-05-19T12:55:00Z".to_string(),
            updated_at: "2026-05-19T13:15:00Z".to_string(),
        }],
    );

    let notes = snapshot.notes_for_folder(crate::mapi::identity::NOTES_FOLDER_ID);
    let journal = snapshot.journal_entries_for_folder(crate::mapi::identity::JOURNAL_FOLDER_ID);
    assert_eq!(notes.len(), 1);
    assert_eq!(notes[0].id, crate::mapi::identity::mapi_store_id(90));
    assert_eq!(notes[0].folder_id, crate::mapi::identity::NOTES_FOLDER_ID);
    assert_eq!(journal.len(), 1);
    assert_eq!(journal[0].id, crate::mapi::identity::mapi_store_id(91));
    assert_eq!(
        journal[0].folder_id,
        crate::mapi::identity::JOURNAL_FOLDER_ID
    );
}

#[test]
fn snapshot_carries_persisted_search_folder_definitions() {
    let definition_id = Uuid::parse_str("aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        definition_id,
        crate::mapi::identity::mapi_store_id(96),
    );
    let definition = SearchFolderDefinition {
        id: definition_id,
        account_id: Uuid::parse_str("bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb").unwrap(),
        role: "reminders".to_string(),
        display_name: "Reminders".to_string(),
        definition_kind: "exchange_builtin".to_string(),
        result_object_kind: "mixed".to_string(),
        scope_json: serde_json::json!({"scope": "top_of_personal_folders"}),
        restriction_json: serde_json::json!({"kind": "exchange_reminders"}),
        excluded_folder_roles: exchange_builtin_excluded_folder_roles(),
        is_builtin: true,
    };
    let snapshot = MapiMailStoreSnapshot::new(
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_search_folder_definitions(vec![definition]);

    let reminders = snapshot
        .search_folder_definition_for_role("reminders")
        .expect("persisted reminders definition");
    assert_eq!(reminders.definition_kind, "exchange_builtin");
    assert_eq!(reminders.result_object_kind, "mixed");
    assert_eq!(
        reminders.excluded_folder_roles,
        exchange_builtin_excluded_folder_roles()
    );
    assert!(snapshot
        .search_folder_definition_for_role("todo_search")
        .is_none());
}

#[test]
fn snapshot_resolves_tracked_mail_processing_by_advertised_folder_id() {
    let definition = SearchFolderDefinition {
        id: Uuid::parse_str("aaaaaaaa-1212-4111-8111-aaaaaaaaaaaa").unwrap(),
        account_id: Uuid::parse_str("bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb").unwrap(),
        role: "tracked_mail_processing".to_string(),
        display_name: "Tracked Mail Processing".to_string(),
        definition_kind: "exchange_builtin".to_string(),
        result_object_kind: "message".to_string(),
        scope_json: serde_json::json!({"scope": "top_of_personal_folders"}),
        restriction_json: serde_json::json!({"kind": "exchange_tracked_mail_processing"}),
        excluded_folder_roles: exchange_builtin_excluded_folder_roles(),
        is_builtin: true,
    };
    let snapshot = MapiMailStoreSnapshot::new(
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_search_folder_definitions(vec![definition]);

    let definition = snapshot
        .search_folder_definition_for_folder_id(
            crate::mapi::identity::TRACKED_MAIL_PROCESSING_FOLDER_ID,
        )
        .expect("tracked mail processing definition");

    assert_eq!(definition.role, "tracked_mail_processing");
    assert!(definition.is_builtin);
}

#[test]
fn snapshot_projects_user_saved_search_folder_as_mapi_folder() {
    let definition_id = Uuid::parse_str("aaaaaaaa-2222-4111-8111-aaaaaaaaaaaa").unwrap();
    let folder_id = crate::mapi::identity::mapi_store_id(122);
    crate::mapi::identity::remember_mapi_identity(definition_id, folder_id);
    let snapshot = MapiMailStoreSnapshot::new(
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_search_folder_definitions(vec![SearchFolderDefinition {
        id: definition_id,
        account_id: Uuid::parse_str("bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb").unwrap(),
        role: "custom".to_string(),
        display_name: "Unread from Alice".to_string(),
        definition_kind: "user_saved".to_string(),
        result_object_kind: "message".to_string(),
        scope_json: serde_json::json!({"scope": "top_of_personal_folders"}),
        restriction_json: serde_json::json!({"kind": "text", "query": "alice"}),
        excluded_folder_roles: vec!["trash".to_string()],
        is_builtin: false,
    }]);

    let folder = snapshot
        .folders()
        .iter()
        .find(|folder| folder.canonical_id == definition_id)
        .expect("user search folder projected");
    assert_eq!(folder.id, folder_id);
    assert_eq!(folder.mailbox.name, "Unread from Alice");
    assert_eq!(folder.mailbox.role, "__mapi_search_folder_message");
}

#[test]
fn snapshot_deduplicates_user_saved_search_folder_projection_by_name() {
    let first_id = Uuid::parse_str("aaaaaaaa-3333-4111-8111-aaaaaaaaaaaa").unwrap();
    let second_id = Uuid::parse_str("aaaaaaaa-4444-4111-8111-aaaaaaaaaaaa").unwrap();
    let first_folder_id = crate::mapi::identity::mapi_store_id(0x7FFF_1000_0123);
    let second_folder_id = crate::mapi::identity::mapi_store_id(0x7FFF_1000_0124);
    crate::mapi::identity::remember_mapi_identity(first_id, first_folder_id);
    crate::mapi::identity::remember_mapi_identity(second_id, second_folder_id);

    let account_id = Uuid::parse_str("bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb").unwrap();
    let duplicate_name = "Categories Rename Search Folder";
    let definition = |id| SearchFolderDefinition {
        id,
        account_id,
        role: "custom".to_string(),
        display_name: duplicate_name.to_string(),
        definition_kind: "user_saved".to_string(),
        result_object_kind: "message".to_string(),
        scope_json: serde_json::json!({"scope": "folders"}),
        restriction_json: serde_json::json!({"kind": "mapi_bounded"}),
        excluded_folder_roles: Vec::new(),
        is_builtin: false,
    };
    let snapshot = MapiMailStoreSnapshot::empty()
        .with_search_folder_definitions(vec![definition(first_id), definition(second_id)]);

    let projected = snapshot
        .folders()
        .into_iter()
        .filter(|folder| folder.mailbox.name == duplicate_name)
        .collect::<Vec<_>>();
    assert_eq!(projected.len(), 1);
    assert_eq!(projected[0].id, first_folder_id);
    assert_eq!(
        snapshot
            .user_saved_search_folder_definition_by_display_name(duplicate_name, "message")
            .map(|definition| definition.id),
        Some(first_id)
    );
}

#[test]
fn snapshot_ignores_blank_mapi_bounded_user_saved_search_folder() {
    let definition_id = Uuid::parse_str("aaaaaaaa-3434-4111-8111-aaaaaaaaaaaa").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        definition_id,
        crate::mapi::identity::mapi_store_id(0x7FFF_1000_0125),
    );
    let account_id = Uuid::parse_str("bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb").unwrap();
    let snapshot = MapiMailStoreSnapshot::empty().with_search_folder_definitions(vec![
        SearchFolderDefinition {
            id: definition_id,
            account_id,
            role: "custom".to_string(),
            display_name: "Categories Rename Search Folder".to_string(),
            definition_kind: "user_saved".to_string(),
            result_object_kind: "message".to_string(),
            scope_json: serde_json::json!({
                "kind": "mapi_bounded",
                "scope": "folders",
                "folderIds": [],
                "folderRoles": ["inbox"],
                "recursive": true
            }),
            restriction_json: serde_json::json!({
                "kind": "mapi_bounded",
                "all": []
            }),
            excluded_folder_roles: Vec::new(),
            is_builtin: false,
        },
    ]);

    assert!(snapshot
        .folders()
        .iter()
        .all(|folder| folder.canonical_id != definition_id));
    assert!(snapshot
        .user_saved_search_folder_definition_by_display_name(
            "Categories Rename Search Folder",
            "message"
        )
        .is_none());
}

#[test]
fn snapshot_projects_canonical_tasks_into_todo_search_results() {
    let account_id = Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").unwrap();
    let task_id = Uuid::parse_str("11111111-2222-4333-8444-555555555555").unwrap();
    let definition_id = Uuid::parse_str("99999999-9999-4999-8999-999999999999").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        task_id,
        crate::mapi::identity::mapi_store_id(97),
    );
    crate::mapi::identity::remember_mapi_identity(
        definition_id,
        crate::mapi::identity::mapi_store_id(98),
    );
    let rights = CollaborationRights {
        may_read: true,
        may_write: true,
        may_delete: true,
        may_share: false,
    };
    let task_list_id = Uuid::parse_str("12121212-3434-4565-8787-909090909090").unwrap();
    let task = ClientTask {
        id: task_id,
        owner_account_id: account_id,
        owner_email: "alice@example.test".to_string(),
        owner_display_name: "Alice".to_string(),
        is_owned: true,
        rights: rights.clone(),
        task_list_id,
        task_list_sort_order: 0,
        title: "Follow up".to_string(),
        description: String::new(),
        status: "needs-action".to_string(),
        due_at: Some("2026-05-21T09:00:00Z".to_string()),
        completed_at: None,
        recurrence_rule: String::new(),
        sort_order: 0,
        updated_at: "2026-05-20T09:00:00Z".to_string(),
    };
    let snapshot = MapiMailStoreSnapshot::new(
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        vec![CollaborationCollection {
            id: "default".to_string(),
            kind: "tasks".to_string(),
            owner_account_id: account_id,
            owner_email: "alice@example.test".to_string(),
            owner_display_name: "Alice".to_string(),
            display_name: "Tasks".to_string(),
            is_owned: true,
            rights,
        }],
        Vec::new(),
        Vec::new(),
        vec![task],
        Vec::new(),
    )
    .with_search_folder_definitions(vec![SearchFolderDefinition {
        id: definition_id,
        account_id,
        role: "todo_search".to_string(),
        display_name: "To-Do".to_string(),
        definition_kind: "exchange_builtin".to_string(),
        result_object_kind: "mixed".to_string(),
        scope_json: serde_json::json!({"scope": "top_of_personal_folders"}),
        restriction_json: serde_json::json!({"kind": "exchange_todo"}),
        excluded_folder_roles: exchange_builtin_excluded_folder_roles(),
        is_builtin: true,
    }]);

    assert_eq!(snapshot.todo_search_results().len(), 1);
    assert!(snapshot
        .task_for_id(
            crate::mapi::identity::TODO_SEARCH_FOLDER_ID,
            crate::mapi::identity::mapi_store_id(97)
        )
        .is_some());
}

#[test]
fn snapshot_projects_followup_mail_into_todo_search_results() {
    let account_id = Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").unwrap();
    let mailbox_id = Uuid::parse_str("44444444-4444-4444-8444-444444444444").unwrap();
    let message_id = Uuid::parse_str("55555555-5555-4555-8555-555555555555").unwrap();
    let definition_id = Uuid::parse_str("99999999-9999-4999-8999-999999999999").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        mailbox_id,
        crate::mapi::identity::mapi_store_id(18),
    );
    crate::mapi::identity::remember_mapi_identity(
        message_id,
        crate::mapi::identity::mapi_store_id(19),
    );
    crate::mapi::identity::remember_mapi_identity(
        definition_id,
        crate::mapi::identity::mapi_store_id(20),
    );
    let mailbox = JmapMailbox {
        id: mailbox_id,
        parent_id: None,
        role: "inbox".to_string(),
        name: "Inbox".to_string(),
        sort_order: 0,
        modseq: 1,
        total_emails: 1,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    };
    let email = JmapEmail {
        id: message_id,
        thread_id: Uuid::parse_str("12121212-1212-4212-8212-121212121212").unwrap(),
        mailbox_id,
        mailbox_role: "inbox".to_string(),
        mailbox_name: "Inbox".to_string(),
        modseq: 2,
        mailbox_ids: vec![mailbox_id],
        mailbox_states: vec![JmapEmailMailboxState {
            mailbox_id,
            role: "inbox".to_string(),
            name: "Inbox".to_string(),
            modseq: 2,
            unread: false,
            flagged: true,
            followup_flag_status: "flagged".to_string(),
            followup_icon: 6,
            todo_item_flags: 8,
            followup_request: "Follow up".to_string(),
            followup_start_at: None,
            followup_due_at: None,
            followup_completed_at: None,
            reminder_set: false,
            reminder_at: None,
            reminder_dismissed_at: None,
            swapped_todo_store_id: None,
            swapped_todo_data: None,
            categories: Vec::new(),
            draft: false,
        }],
        received_at: "2026-05-20T12:00:00Z".to_string(),
        sent_at: None,
        from_address: "alice@example.test".to_string(),
        from_display: Some("Alice".to_string()),
        sender_address: None,
        sender_display: None,
        sender_authorization_kind: "self".to_string(),
        submitted_by_account_id: account_id,
        to: Vec::new(),
        cc: Vec::new(),
        bcc: Vec::new(),
        subject: "Flagged mail".to_string(),
        preview: "Flagged mail".to_string(),
        body_text: "Flagged mail".to_string(),
        body_html_sanitized: None,
        unread: false,
        flagged: true,
        followup_flag_status: "flagged".to_string(),
        followup_icon: 6,
        todo_item_flags: 8,
        followup_request: "Follow up".to_string(),
        followup_start_at: None,
        followup_due_at: None,
        followup_completed_at: None,
        reminder_set: false,
        reminder_at: None,
        reminder_dismissed_at: None,
        swapped_todo_store_id: None,
        swapped_todo_data: None,
        categories: Vec::new(),
        has_attachments: false,
        size_octets: 42,
        internet_message_id: None,
        mime_blob_ref: None,
        delivery_status: "stored".to_string(),
    };
    let snapshot = MapiMailStoreSnapshot::new(
        vec![mailbox],
        vec![email],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_search_folder_definitions(vec![SearchFolderDefinition {
        id: definition_id,
        account_id,
        role: "todo_search".to_string(),
        display_name: "To-Do".to_string(),
        definition_kind: "exchange_builtin".to_string(),
        result_object_kind: "mixed".to_string(),
        scope_json: serde_json::json!({"scope": "top_of_personal_folders"}),
        restriction_json: serde_json::json!({"kind": "exchange_todo"}),
        excluded_folder_roles: exchange_builtin_excluded_folder_roles(),
        is_builtin: true,
    }]);

    assert_eq!(snapshot.todo_search_messages().len(), 1);
    let message_id = snapshot.todo_search_messages()[0].id;
    assert!(snapshot.todo_search_message_for_id(message_id).is_some());
}

#[test]
fn snapshot_projects_swapped_todo_mail_into_tracked_mail_processing_results() {
    let account_id = Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").unwrap();
    let mailbox_id = Uuid::parse_str("44444444-4444-4444-8444-444444444444").unwrap();
    let message_id = Uuid::parse_str("66666666-6666-4666-8666-666666666666").unwrap();
    let store_id = Uuid::parse_str("77777777-7777-4777-8777-777777777777").unwrap();
    let definition_id = Uuid::parse_str("88888888-8888-4888-8888-888888888888").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        mailbox_id,
        crate::mapi::identity::mapi_store_id(20),
    );
    crate::mapi::identity::remember_mapi_identity(
        message_id,
        crate::mapi::identity::mapi_store_id(21),
    );
    crate::mapi::identity::remember_mapi_identity(
        definition_id,
        crate::mapi::identity::mapi_store_id(22),
    );
    let mailbox = JmapMailbox {
        id: mailbox_id,
        parent_id: None,
        role: "sent".to_string(),
        name: "Sent".to_string(),
        sort_order: 0,
        modseq: 1,
        total_emails: 1,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    };
    let email = JmapEmail {
        id: message_id,
        thread_id: Uuid::parse_str("12121212-1212-4212-8212-121212121212").unwrap(),
        mailbox_id,
        mailbox_role: "sent".to_string(),
        mailbox_name: "Sent".to_string(),
        modseq: 2,
        mailbox_ids: vec![mailbox_id],
        mailbox_states: vec![JmapEmailMailboxState {
            mailbox_id,
            role: "sent".to_string(),
            name: "Sent".to_string(),
            modseq: 2,
            unread: false,
            flagged: false,
            followup_flag_status: "none".to_string(),
            followup_icon: 0,
            todo_item_flags: 0,
            followup_request: String::new(),
            followup_start_at: None,
            followup_due_at: None,
            followup_completed_at: None,
            reminder_set: false,
            reminder_at: None,
            reminder_dismissed_at: None,
            swapped_todo_store_id: Some(store_id),
            swapped_todo_data: Some(vec![9, 8, 7]),
            categories: Vec::new(),
            draft: false,
        }],
        received_at: "2026-05-20T12:00:00Z".to_string(),
        sent_at: Some("2026-05-20T12:00:00Z".to_string()),
        from_address: "alice@example.test".to_string(),
        from_display: Some("Alice".to_string()),
        sender_address: None,
        sender_display: None,
        sender_authorization_kind: "self".to_string(),
        submitted_by_account_id: account_id,
        to: Vec::new(),
        cc: Vec::new(),
        bcc: Vec::new(),
        subject: "Tracked mail".to_string(),
        preview: "Tracked mail".to_string(),
        body_text: "Tracked mail".to_string(),
        body_html_sanitized: None,
        unread: false,
        flagged: false,
        followup_flag_status: "none".to_string(),
        followup_icon: 0,
        todo_item_flags: 0,
        followup_request: String::new(),
        followup_start_at: None,
        followup_due_at: None,
        followup_completed_at: None,
        reminder_set: false,
        reminder_at: None,
        reminder_dismissed_at: None,
        swapped_todo_store_id: Some(store_id),
        swapped_todo_data: Some(vec![9, 8, 7]),
        categories: Vec::new(),
        has_attachments: false,
        size_octets: 42,
        internet_message_id: None,
        mime_blob_ref: None,
        delivery_status: "stored".to_string(),
    };
    let snapshot = MapiMailStoreSnapshot::new(
        vec![mailbox],
        vec![email],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_search_folder_definitions(vec![SearchFolderDefinition {
        id: definition_id,
        account_id,
        role: "tracked_mail_processing".to_string(),
        display_name: "Tracked Mail Processing".to_string(),
        definition_kind: "exchange_builtin".to_string(),
        result_object_kind: "message".to_string(),
        scope_json: serde_json::json!({"scope": "top_of_personal_folders"}),
        restriction_json: serde_json::json!({"kind": "exchange_tracked_mail_processing"}),
        excluded_folder_roles: exchange_builtin_excluded_folder_roles(),
        is_builtin: true,
    }]);

    assert_eq!(snapshot.tracked_mail_processing_messages().len(), 1);
    assert!(snapshot
        .tracked_mail_processing_message_for_id(crate::mapi::identity::mapi_store_id(21))
        .is_some());
}

#[test]
fn snapshot_projects_reminders_as_underlying_calendar_and_task_links() {
    let account_id = Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").unwrap();
    let mailbox_id = Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap();
    let message_id = Uuid::parse_str("11112222-3333-4444-8555-666677778888").unwrap();
    let excluded_message_id = Uuid::parse_str("11112222-3333-4444-8555-666677778889").unwrap();
    let event_id = Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap();
    let task_id = Uuid::parse_str("33333333-3333-4333-8333-333333333333").unwrap();
    let search_definition_id = Uuid::parse_str("44444444-4444-4444-8444-444444444444").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        message_id,
        crate::mapi::identity::mapi_store_id(97),
    );
    crate::mapi::identity::remember_mapi_identity(
        excluded_message_id,
        crate::mapi::identity::mapi_store_id(101),
    );
    crate::mapi::identity::remember_mapi_identity(
        event_id,
        crate::mapi::identity::mapi_store_id(98),
    );
    crate::mapi::identity::remember_mapi_identity(
        task_id,
        crate::mapi::identity::mapi_store_id(99),
    );
    crate::mapi::identity::remember_mapi_identity(
        search_definition_id,
        crate::mapi::identity::mapi_store_id(100),
    );
    let rights = CollaborationRights {
        may_read: true,
        may_write: true,
        may_delete: true,
        may_share: false,
    };
    let event = AccessibleEvent {
        id: event_id,
        uid: "event-uid".to_string(),
        collection_id: "default".to_string(),
        owner_account_id: account_id,
        owner_email: "alice@example.test".to_string(),
        owner_display_name: "Alice".to_string(),
        rights: rights.clone(),
        date: "2026-05-21".to_string(),
        time: "09:00".to_string(),
        time_zone: "UTC".to_string(),
        duration_minutes: 30,
        all_day: false,
        status: "confirmed".to_string(),
        sequence: 0,
        recurrence_rule: String::new(),
        recurrence_json: "{}".to_string(),
        recurrence_exceptions_json: "[]".to_string(),
        title: "Standup".to_string(),
        location: "Room 1".to_string(),
        organizer_json: "{}".to_string(),
        attendees: String::new(),
        attendees_json: "[]".to_string(),
        notes: String::new(),
        body_html: String::new(),
    };
    let task_list_id = Uuid::parse_str("12121212-3434-4565-8787-909090909090").unwrap();
    let task = ClientTask {
        id: task_id,
        owner_account_id: account_id,
        owner_email: "alice@example.test".to_string(),
        owner_display_name: "Alice".to_string(),
        is_owned: true,
        rights: rights.clone(),
        task_list_id,
        task_list_sort_order: 0,
        title: "Follow up".to_string(),
        description: String::new(),
        status: "needs-action".to_string(),
        due_at: Some("2026-05-21T12:00:00Z".to_string()),
        completed_at: None,
        recurrence_rule: String::new(),
        sort_order: 0,
        updated_at: "2026-05-20T09:00:00Z".to_string(),
    };
    let mailbox = JmapMailbox {
        id: mailbox_id,
        parent_id: None,
        role: "inbox".to_string(),
        name: "Inbox".to_string(),
        sort_order: 10,
        modseq: 1,
        total_emails: 1,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    };
    let email = JmapEmail {
        id: message_id,
        thread_id: Uuid::parse_str("99999999-9999-4999-8999-999999999999").unwrap(),
        mailbox_id,
        mailbox_role: "inbox".to_string(),
        mailbox_name: "Inbox".to_string(),
        modseq: 2,
        mailbox_ids: vec![mailbox_id],
        mailbox_states: vec![JmapEmailMailboxState {
            mailbox_id,
            role: "inbox".to_string(),
            name: "Inbox".to_string(),
            modseq: 2,
            unread: false,
            flagged: true,
            followup_flag_status: "flagged".to_string(),
            followup_icon: 6,
            todo_item_flags: 8,
            followup_request: "Follow up".to_string(),
            followup_start_at: None,
            followup_due_at: Some("2026-05-21T17:00:00Z".to_string()),
            followup_completed_at: None,
            reminder_set: true,
            reminder_at: Some("2026-05-21T16:45:00Z".to_string()),
            reminder_dismissed_at: None,
            swapped_todo_store_id: None,
            swapped_todo_data: None,
            categories: Vec::new(),
            draft: false,
        }],
        received_at: "2026-05-20T12:00:00Z".to_string(),
        sent_at: None,
        from_address: "alice@example.test".to_string(),
        from_display: Some("Alice".to_string()),
        sender_address: None,
        sender_display: None,
        sender_authorization_kind: "self".to_string(),
        submitted_by_account_id: account_id,
        to: Vec::new(),
        cc: Vec::new(),
        bcc: Vec::new(),
        subject: "Mail reminder".to_string(),
        preview: "Mail reminder".to_string(),
        body_text: "Mail reminder".to_string(),
        body_html_sanitized: None,
        unread: false,
        flagged: true,
        followup_flag_status: "flagged".to_string(),
        followup_icon: 6,
        todo_item_flags: 8,
        followup_request: "Follow up".to_string(),
        followup_start_at: None,
        followup_due_at: Some("2026-05-21T17:00:00Z".to_string()),
        followup_completed_at: None,
        reminder_set: true,
        reminder_at: Some("2026-05-21T16:45:00Z".to_string()),
        reminder_dismissed_at: None,
        swapped_todo_store_id: None,
        swapped_todo_data: None,
        categories: Vec::new(),
        has_attachments: false,
        size_octets: 42,
        internet_message_id: None,
        mime_blob_ref: None,
        delivery_status: "stored".to_string(),
    };
    let mut excluded_email = email.clone();
    excluded_email.id = excluded_message_id;
    excluded_email.mailbox_role = "drafts".to_string();
    excluded_email.mailbox_name = "Drafts".to_string();
    excluded_email.mailbox_states[0].role = "drafts".to_string();
    excluded_email.mailbox_states[0].name = "Drafts".to_string();
    let snapshot = MapiMailStoreSnapshot::new(
        vec![mailbox],
        vec![email, excluded_email],
        Vec::new(),
        Vec::new(),
        vec![CollaborationCollection {
            id: "default".to_string(),
            kind: "calendar".to_string(),
            owner_account_id: account_id,
            owner_email: "alice@example.test".to_string(),
            owner_display_name: "Alice".to_string(),
            display_name: "Calendar".to_string(),
            is_owned: true,
            rights: rights.clone(),
        }],
        vec![CollaborationCollection {
            id: "default".to_string(),
            kind: "tasks".to_string(),
            owner_account_id: account_id,
            owner_email: "alice@example.test".to_string(),
            owner_display_name: "Alice".to_string(),
            display_name: "Tasks".to_string(),
            is_owned: true,
            rights,
        }],
        Vec::new(),
        vec![event],
        vec![task],
        Vec::new(),
    )
    .with_search_folder_definitions(vec![SearchFolderDefinition {
        id: search_definition_id,
        account_id,
        role: "reminders".to_string(),
        display_name: "Reminders".to_string(),
        definition_kind: "exchange_builtin".to_string(),
        result_object_kind: "mixed".to_string(),
        scope_json: serde_json::json!({"scope": "top_of_personal_folders"}),
        restriction_json: serde_json::json!({"kind": "exchange_reminders"}),
        excluded_folder_roles: exchange_builtin_excluded_folder_roles(),
        is_builtin: true,
    }])
    .with_reminders(vec![
        ClientReminder {
            source_type: "mail".to_string(),
            source_id: excluded_message_id,
            occurrence_start_at: None,
            title: "Draft reminder".to_string(),
            due_at: Some("2026-05-21T17:00:00Z".to_string()),
            reminder_at: "2026-05-21T16:45:00Z".to_string(),
            dismissed_at: None,
            completed_at: None,
            status: "pending".to_string(),
        },
        ClientReminder {
            source_type: "mail".to_string(),
            source_id: message_id,
            occurrence_start_at: None,
            title: "Mail reminder".to_string(),
            due_at: Some("2026-05-21T17:00:00Z".to_string()),
            reminder_at: "2026-05-21T16:45:00Z".to_string(),
            dismissed_at: None,
            completed_at: None,
            status: "pending".to_string(),
        },
        ClientReminder {
            source_type: "calendar".to_string(),
            source_id: event_id,
            occurrence_start_at: None,
            title: "Standup".to_string(),
            due_at: Some("2026-05-21T09:30:00Z".to_string()),
            reminder_at: "2026-05-21T09:00:00Z".to_string(),
            dismissed_at: None,
            completed_at: None,
            status: "pending".to_string(),
        },
        ClientReminder {
            source_type: "task".to_string(),
            source_id: task_id,
            occurrence_start_at: None,
            title: "Follow up".to_string(),
            due_at: Some("2026-05-21T12:00:00Z".to_string()),
            reminder_at: "2026-05-21T11:45:00Z".to_string(),
            dismissed_at: None,
            completed_at: None,
            status: "pending".to_string(),
        },
    ]);

    assert_eq!(snapshot.reminder_events().len(), 1);
    assert_eq!(snapshot.reminder_tasks().len(), 1);
    assert_eq!(snapshot.reminder_messages().len(), 1);
    assert!(snapshot
        .reminder_message_for_id(crate::mapi::identity::mapi_store_id(101))
        .is_none());
    assert!(snapshot
        .event_for_id(
            crate::mapi::identity::REMINDERS_FOLDER_ID,
            crate::mapi::identity::mapi_store_id(98)
        )
        .is_some());
    assert!(snapshot
        .task_for_id(
            crate::mapi::identity::REMINDERS_FOLDER_ID,
            crate::mapi::identity::mapi_store_id(99)
        )
        .is_some());
}

#[test]
fn snapshot_projects_computed_delegate_freebusy_messages() {
    let message_id = Uuid::parse_str("56565656-5656-4656-8656-565656565656").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        message_id,
        crate::mapi::identity::mapi_store_id(610),
    );
    let snapshot = MapiMailStoreSnapshot::empty().with_delegate_freebusy_messages(vec![
        DelegateFreeBusyMessageObject {
            id: message_id,
            account_id: Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").unwrap(),
            owner_account_id: Uuid::parse_str("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb").unwrap(),
            owner_email: "owner@example.test".to_string(),
            message_kind: "freebusy".to_string(),
            subject: "owner@example.test: busy".to_string(),
            body_text: "busy from 2026-05-26T08:00:00Z to 2026-05-26T09:00:00Z".to_string(),
            starts_at: Some("2026-05-26T08:00:00Z".to_string()),
            ends_at: Some("2026-05-26T09:00:00Z".to_string()),
            busy_status: Some("busy".to_string()),
            payload_json: "{}".to_string(),
            updated_at: "2026-05-26T08:00:00Z".to_string(),
        },
    ]);

    assert_eq!(snapshot.delegate_freebusy_messages().len(), 1);
    let projected_id = snapshot.delegate_freebusy_messages()[0].id;
    assert!(snapshot
        .delegate_freebusy_message_for_id(projected_id)
        .is_some());
}
