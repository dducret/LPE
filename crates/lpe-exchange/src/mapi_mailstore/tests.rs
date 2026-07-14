use super::*;
use lpe_storage::{JmapEmailAddress, JmapEmailMailboxState};

fn wire_id_bytes(object_id: u64) -> [u8; 8] {
    crate::mapi::identity::wire_id_bytes_from_object_id(object_id).unwrap()
}

#[test]
fn message_change_number_excludes_bcc_recipients() {
    let mut email = test_email();
    let baseline = canonical_message_change_number(&email);
    email.bcc.push(JmapEmailAddress {
        address: "secret@example.test".to_string(),
        display_name: Some("Secret".to_string()),
    });

    assert_eq!(canonical_message_change_number(&email), baseline);

    email.cc.push(JmapEmailAddress {
        address: "visible@example.test".to_string(),
        display_name: None,
    });
    email.mailbox_states[0].modseq += 1;
    assert_ne!(canonical_message_change_number(&email), baseline);
}

#[test]
fn message_change_number_tracks_per_folder_membership_state() {
    let mut email = test_email();
    let baseline = canonical_message_change_number(&email);
    let archive_id = Uuid::parse_str("44444444-4444-4444-4444-444444444444").unwrap();

    email.mailbox_ids.push(archive_id);
    email.mailbox_states.push(JmapEmailMailboxState {
        mailbox_id: archive_id,
        role: String::new(),
        name: "Archive".to_string(),
        modseq: 43,
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
    });
    let with_archive = canonical_message_change_number(&email);
    assert_ne!(with_archive, baseline);

    email
        .mailbox_states
        .iter_mut()
        .find(|state| state.mailbox_id == archive_id)
        .unwrap()
        .unread = true;
    email
        .mailbox_states
        .iter_mut()
        .find(|state| state.mailbox_id == archive_id)
        .unwrap()
        .modseq += 1;
    assert_ne!(canonical_message_change_number(&email), with_archive);
}

#[test]
fn canonical_change_numbers_fit_mapi_globcnt() {
    let mailbox = JmapMailbox {
        id: Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap(),
        parent_id: None,
        role: "inbox".to_string(),
        name: "Inbox".to_string(),
        sort_order: 40,
        modseq: 42,
        total_emails: 1,
        unread_emails: 1,
        size_octets: 0,
        is_subscribed: true,
    };
    let email = test_email();

    for change_number in [
        canonical_folder_change_number(&mailbox),
        canonical_message_change_number(&email),
    ] {
        assert!(change_number > 0);
        assert!(change_number <= 0x0000_FFFF_FFFF_FFFF);
        assert_eq!(
            crate::mapi::identity::global_counter_from_globcnt(&globcnt_bytes(change_number)),
            Some(change_number)
        );
    }
}

#[test]
fn source_and_change_keys_are_stable_replica_scoped_values() {
    let id = Uuid::parse_str("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee").unwrap();
    crate::mapi::identity::remember_mapi_identity(id, crate::mapi::identity::mapi_store_id(42));
    let source_key = source_key_for_uuid(&id);
    let change_key = change_key_for_change_number(42);

    assert_eq!(STORE_REPLICA_GUID[7] & 0xf0, 0x40);
    assert_eq!(STORE_REPLICA_GUID[8] & 0xc0, 0x80);
    assert_eq!(source_key.len(), 22);
    assert_eq!(change_key.len(), 22);
    assert_eq!(&source_key[16..22], &[0, 0, 0, 0, 0, 42]);
    assert_eq!(&change_key[16..22], &[0, 0, 0, 0, 0, 42]);
    assert!(source_key.starts_with(&STORE_REPLICA_GUID));
    assert!(change_key.starts_with(&STORE_REPLICA_GUID));
    assert_eq!(source_key, source_key_for_uuid(&id));
}

#[test]
fn store_id_change_numbers_use_global_counter() {
    let store_id = crate::mapi::identity::mapi_store_id(42);
    let change_number = change_number_for_store_id(store_id);
    let change_key = change_key_for_change_number(change_number);

    assert_eq!(change_number, 42);
    assert_eq!(
        &source_key_for_store_id(store_id)[16..22],
        &change_key[16..22]
    );
}

#[test]
fn hierarchy_change_numbers_do_not_collapse_low_modseq_special_folders() {
    let drafts = JmapMailbox {
        id: Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap(),
        parent_id: None,
        role: "drafts".to_string(),
        name: "Drafts".to_string(),
        sort_order: 30,
        modseq: 1,
        total_emails: 0,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    };
    let trash = JmapMailbox {
        id: Uuid::parse_str("44444444-4444-4444-4444-444444444444").unwrap(),
        parent_id: None,
        role: "trash".to_string(),
        name: "Trash".to_string(),
        sort_order: 50,
        modseq: 1,
        total_emails: 0,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    };

    assert_eq!(
        canonical_hierarchy_change_number(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID, &drafts),
        crate::mapi::identity::DRAFTS_FOLDER_COUNTER
    );
    assert_eq!(
        canonical_hierarchy_change_number(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID, &trash),
        crate::mapi::identity::TRASH_FOLDER_COUNTER
    );
}

#[test]
fn hierarchy_change_numbers_stay_in_folder_counter_domain() {
    let inbox = JmapMailbox {
        id: Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap(),
        parent_id: None,
        role: "inbox".to_string(),
        name: "Inbox".to_string(),
        sort_order: 10,
        modseq: 51,
        total_emails: 7,
        unread_emails: 7,
        size_octets: 0,
        is_subscribed: true,
    };

    assert_eq!(
        canonical_hierarchy_change_number(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID, &inbox),
        crate::mapi::identity::INBOX_FOLDER_COUNTER
    );
}

#[test]
fn special_folder_source_key_matches_projected_folder_id() {
    let mailbox_id = Uuid::parse_str("bbbbbbbb-cccc-4ddd-8eee-ffffffffffff").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        mailbox_id,
        crate::mapi::identity::mapi_store_id(0x1234),
    );
    let mailbox = JmapMailbox {
        id: mailbox_id,
        parent_id: None,
        role: "inbox".to_string(),
        name: "Inbox".to_string(),
        sort_order: 40,
        modseq: 42,
        total_emails: 0,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    };

    assert_eq!(
        source_key_for_mailbox_folder(&mailbox),
        source_key_for_store_id(crate::mapi::identity::INBOX_FOLDER_ID)
    );
}

#[test]
fn predecessor_change_list_uses_sized_change_xid() {
    let change_key = change_key_for_change_number(42);
    let predecessor_list = predecessor_change_list(42);

    assert_eq!(predecessor_list.len(), 1 + change_key.len());
    assert_eq!(predecessor_list[0], change_key.len() as u8);
    assert_eq!(&predecessor_list[1..], change_key.as_slice());
}

#[test]
fn unchanged_object_keeps_source_key_and_changed_object_advances_change_number() {
    let mut email = test_email();
    crate::mapi::identity::remember_mapi_identity(
        email.id,
        crate::mapi::identity::mapi_store_id(50),
    );
    let source_key = source_key_for_uuid(&email.id);
    let baseline_change_number = canonical_message_change_number(&email);

    email.subject = "Client-local stale subject".to_string();
    assert_eq!(source_key_for_uuid(&email.id), source_key);
    assert_eq!(
        canonical_message_change_number(&email),
        baseline_change_number
    );

    email.modseq = email.modseq.saturating_add(1);
    email.mailbox_states[0].modseq = email.modseq;
    let changed_change_number = canonical_message_change_number(&email);
    assert_eq!(source_key_for_uuid(&email.id), source_key);
    assert!(changed_change_number > baseline_change_number);
    assert_eq!(
        &change_key_for_change_number(changed_change_number)[16..22],
        &globcnt_bytes(changed_change_number)
    );
}

#[test]
fn canonical_message_change_number_uses_membership_modseq_without_bcc_leakage() {
    let mut email = test_email();
    email.has_attachments = true;
    let baseline = canonical_message_change_number(&email);

    email.bcc.push(JmapEmailAddress {
        address: "hidden@example.test".to_string(),
        display_name: None,
    });
    assert_eq!(canonical_message_change_number(&email), baseline);

    email.mailbox_states[0].modseq = email.mailbox_states[0].modseq.saturating_add(1);
    assert_ne!(canonical_message_change_number(&email), baseline);
}

#[test]
fn sync_manifest_serializes_variable_strings_with_fast_transfer_lengths() {
    let mailbox_id = Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap();
    let email_id = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        mailbox_id,
        crate::mapi::identity::mapi_store_id(5),
    );
    crate::mapi::identity::remember_mapi_identity(
        email_id,
        crate::mapi::identity::mapi_store_id(50),
    );
    let mailbox = JmapMailbox {
        id: mailbox_id,
        parent_id: None,
        role: "inbox".to_string(),
        name: "Inbox".to_string(),
        sort_order: 40,
        modseq: 42,
        total_emails: 1,
        unread_emails: 1,
        size_octets: 0,
        is_subscribed: true,
    };
    let email = test_email();
    let buffer = sync_manifest_buffer_with_attachments(
        0x02,
        0x0100,
        0,
        &[],
        crate::mapi::identity::ROOT_FOLDER_ID,
        &[mailbox],
        &[email],
        &[],
        &[],
        1,
    );

    assert_variable_property(&buffer, PID_TAG_DISPLAY_NAME_W, &utf16z("Inbox"));
    assert_variable_property(&buffer, PID_TAG_SUBJECT_W, &utf16z("Hello"));
    assert_variable_property(&buffer, PID_TAG_NORMALIZED_SUBJECT_A, b"Hello\0");
    assert_i32_property(&buffer, PID_TAG_ACCESS, MAPI_FOLDER_ACCESS as i32);
    assert_absent_property(&buffer, 0x3FE0_0102);
    assert_absent_property(&buffer, 0x3FE1_0102);
    assert_absent_property(&buffer, 0x0E27_0102);
}

#[test]
fn sync_manifest_serializes_content_message_header_in_fixed_order() {
    let email_id = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        email_id,
        crate::mapi::identity::mapi_store_id(50),
    );
    let email = test_email();
    let buffer = sync_manifest_buffer_with_attachments(
        0x01,
        SYNC_FLAG_NORMAL,
        SYNC_EXTRA_FLAG_EID | SYNC_EXTRA_FLAG_MESSAGE_SIZE | SYNC_EXTRA_FLAG_CHANGE_NUMBER,
        &[],
        crate::mapi::identity::INBOX_FOLDER_ID,
        &[],
        &[email],
        &[],
        &[],
        1,
    );

    assert_tag_order(
        &buffer,
        &[
            INCR_SYNC_CHG,
            PID_TAG_SOURCE_KEY,
            PID_TAG_LAST_MODIFICATION_TIME,
            PID_TAG_CHANGE_KEY,
            PID_TAG_PREDECESSOR_CHANGE_LIST,
            PID_TAG_ASSOCIATED,
            PID_TAG_MID,
            PID_TAG_MESSAGE_SIZE,
            PID_TAG_CHANGE_NUMBER,
            INCR_SYNC_MESSAGE,
            PID_TAG_MESSAGE_FLAGS,
        ],
    );
    assert_bool_property(&buffer, PID_TAG_ASSOCIATED, false);
    assert_i32_property(&buffer, PID_TAG_MESSAGE_SIZE, 42);
    assert_change_number_property(
        &buffer,
        PID_TAG_CHANGE_NUMBER,
        canonical_message_change_number(&test_email()),
    );
}

#[test]
fn microsoft_oxcfxics_content_sync_uses_recipient_markers() {
    let email_id = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        email_id,
        crate::mapi::identity::mapi_store_id(50),
    );
    let mut email = test_email();
    email.cc.push(JmapEmailAddress {
        address: "carol@example.test".to_string(),
        display_name: Some("Carol".to_string()),
    });
    let buffer = sync_manifest_buffer_with_attachments(
        SYNC_TYPE_CONTENTS,
        SYNC_FLAG_NORMAL,
        0,
        &[],
        crate::mapi::identity::INBOX_FOLDER_ID,
        &[],
        &[email],
        &[],
        &[],
        1,
    );

    assert_tag_order(
        &buffer,
        &[
            PID_TAG_SUBJECT_W,
            START_RECIP,
            PID_TAG_RECIPIENT_TYPE,
            PID_TAG_DISPLAY_NAME_W,
            PID_TAG_EMAIL_ADDRESS_W,
            END_TO_RECIP,
            INCR_SYNC_STATE_BEGIN,
        ],
    );
    assert_eq!(
        buffer
            .windows(START_RECIP.to_le_bytes().len())
            .filter(|window| *window == START_RECIP.to_le_bytes())
            .count(),
        2
    );
    assert_i32_property(&buffer, PID_TAG_RECIPIENT_TYPE, 1);
    assert_variable_property_present(&buffer, PID_TAG_DISPLAY_NAME_W, &utf16z("Bob"));
    assert_variable_property_present(
        &buffer,
        PID_TAG_EMAIL_ADDRESS_W,
        &utf16z("bob@example.test"),
    );
    assert_variable_property_present(&buffer, PID_TAG_DISPLAY_NAME_W, &utf16z("Carol"));
    assert_variable_property_present(
        &buffer,
        PID_TAG_EMAIL_ADDRESS_W,
        &utf16z("carol@example.test"),
    );
}

#[test]
fn microsoft_oxcfxics_content_sync_uses_attachment_markers() {
    let email_id = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        email_id,
        crate::mapi::identity::mapi_store_id(50),
    );
    let mut email = test_email();
    email.has_attachments = true;
    email.size_octets = 1024;
    let attachment = AttachmentSyncFact {
        id: Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap(),
        file_reference: "blob-ref".to_string(),
        file_name: "agenda.txt".to_string(),
        media_type: "text/plain".to_string(),
        size_octets: 12,
        embedded_message_blob: None,
    };
    let attachment_facts = [MessageAttachmentSyncFacts {
        message_id: email_id,
        attachments: vec![attachment],
    }];
    let buffer = sync_manifest_buffer_with_attachments(
        SYNC_TYPE_CONTENTS,
        SYNC_FLAG_NORMAL,
        0,
        &[],
        crate::mapi::identity::INBOX_FOLDER_ID,
        &[],
        &[email],
        &attachment_facts,
        &[],
        1,
    );

    assert_tag_order(
        &buffer,
        &[
            PID_TAG_SUBJECT_W,
            START_RECIP,
            END_TO_RECIP,
            NEW_ATTACH,
            PID_TAG_ATTACH_NUM,
            PID_TAG_ATTACH_ENCODING,
            PID_TAG_RENDERING_POSITION,
            PID_TAG_ATTACH_SIZE,
            PID_TAG_ATTACH_METHOD,
            PID_TAG_ATTACH_RENDERING,
            PID_TAG_ATTACH_FLAGS,
            PID_TAG_ATTACHMENT_HIDDEN,
            PID_TAG_ATTACH_FILENAME_W,
            PID_TAG_ATTACH_LONG_FILENAME_W,
            PID_TAG_ATTACH_MIME_TAG_W,
            END_ATTACH,
            INCR_SYNC_STATE_BEGIN,
        ],
    );
    assert_i32_property(&buffer, PID_TAG_ATTACH_NUM, 0);
    assert_i32_property(&buffer, PID_TAG_ATTACH_SIZE, 12);
    assert_i32_property(&buffer, PID_TAG_ATTACH_METHOD, ATTACH_BY_VALUE);
    assert_variable_property_present(&buffer, PID_TAG_ATTACH_FILENAME_W, &utf16z("agenda.txt"));
    assert_variable_property_present(
        &buffer,
        PID_TAG_ATTACH_LONG_FILENAME_W,
        &utf16z("agenda.txt"),
    );
    assert_variable_property_present(&buffer, PID_TAG_ATTACH_MIME_TAG_W, &utf16z("text/plain"));
}

#[test]
fn microsoft_oxcfxics_content_sync_uses_embedded_message_markers() {
    let email_id = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        email_id,
        crate::mapi::identity::mapi_store_id(50),
    );
    let mut email = test_email();
    email.has_attachments = true;
    let attachment = AttachmentSyncFact {
            id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
            file_reference: "embedded-ref".to_string(),
            file_name: "Embedded child.msg".to_string(),
            media_type: "application/vnd.ms-outlook".to_string(),
            size_octets: 512,
            embedded_message_blob: Some(
                b"LPE-MAPI-EMBEDDED-MESSAGE\0Subject:Saved child\r\nBody-Length:10\r\nChild body\r\nHtml-Length:0\r\n"
                    .to_vec(),
            ),
        };
    let attachment_facts = [MessageAttachmentSyncFacts {
        message_id: email_id,
        attachments: vec![attachment],
    }];
    let buffer = sync_manifest_buffer_with_attachments(
        SYNC_TYPE_CONTENTS,
        SYNC_FLAG_NORMAL,
        0,
        &[],
        crate::mapi::identity::INBOX_FOLDER_ID,
        &[],
        &[email],
        &attachment_facts,
        &[],
        1,
    );

    assert_tag_sequence(
        &buffer,
        &[
            NEW_ATTACH,
            PID_TAG_ATTACH_NUM,
            PID_TAG_ATTACH_METHOD,
            START_EMBED,
            PID_TAG_MESSAGE_CLASS_W,
            PID_TAG_SUBJECT_W,
            PID_TAG_BODY_W,
            END_EMBED,
            END_ATTACH,
            INCR_SYNC_STATE_BEGIN,
        ],
    );
    assert_i32_property(&buffer, PID_TAG_ATTACH_METHOD, ATTACH_EMBEDDED_MESSAGE);
    assert_variable_property_present(&buffer, PID_TAG_MESSAGE_CLASS_W, &utf16z("IPM.Note"));
    assert_variable_property_present(&buffer, PID_TAG_SUBJECT_W, &utf16z("Saved child"));
    assert_variable_property_present(&buffer, PID_TAG_BODY_W, &utf16z("Child body"));
}

#[test]
fn microsoft_oxcfxics_fast_transfer_copy_messages_uses_message_markers() {
    let email = test_email();
    let buffer = fast_transfer_message_list_buffer_with_attachments(&[email], &[]);

    assert_tag_sequence(
        &buffer,
        &[
            START_MESSAGE,
            PID_TAG_SUBJECT_W,
            PID_TAG_BODY_W,
            END_MESSAGE,
        ],
    );
    assert!(!buffer.starts_with(b"LPE-MAPI-FASTTRANSFER\0"));
    assert_variable_property_present(&buffer, PID_TAG_SUBJECT_W, &utf16z("Hello"));
    assert_variable_property_present(&buffer, PID_TAG_BODY_W, &utf16z("Hello body"));
}

#[test]
fn microsoft_oxcfxics_fast_transfer_copy_fai_uses_fai_message_marker() {
    let canonical_id = Uuid::parse_str("99999999-9999-9999-9999-999999999990").unwrap();
    let item_id = crate::mapi::identity::mapi_store_id(90);
    crate::mapi::identity::remember_mapi_identity(canonical_id, item_id);
    let special = SpecialMessageSyncFact {
        folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
        item_id,
        canonical_id,
        associated: true,
        subject: "Outlook Inbox view state".to_string(),
        body_text: "Client view payload".to_string(),
        message_class: "IPM.Configuration.MessageListSettings".to_string(),
        last_modified_filetime: filetime_from_rfc3339_utc("2026-05-19T10:00:00Z"),
        message_size: 19,
        read_state: None,
        named_properties: vec![(
            0x7C08_0102,
            SpecialMessagePropertyValue::Binary(b"view-extra".to_vec()),
        )],
    };
    let buffer = fast_transfer_manifest_buffer_with_special_objects(
        crate::mapi::identity::INBOX_FOLDER_ID,
        &[special],
    );

    assert_tag_sequence(
        &buffer,
        &[
            START_FAI_MSG,
            PID_TAG_PARENT_SOURCE_KEY,
            PID_TAG_SOURCE_KEY,
            PID_TAG_ASSOCIATED,
            PID_TAG_MID,
            PID_TAG_MESSAGE_CLASS_W,
            PID_TAG_BODY_W,
            0x7C08_0102,
            END_MESSAGE,
        ],
    );
    assert!(!buffer.starts_with(b"LPE-MAPI-FASTTRANSFER\0"));
    assert_bool_property(&buffer, PID_TAG_ASSOCIATED, true);
    assert_variable_property_present(
        &buffer,
        PID_TAG_SUBJECT_W,
        &utf16z("Outlook Inbox view state"),
    );
    assert_variable_property_present(&buffer, PID_TAG_BODY_W, &utf16z("Client view payload"));
    assert_variable_property_present(&buffer, 0x7C08_0102, b"view-extra");
}

#[test]
fn microsoft_oxcfxics_fast_transfer_copy_folder_uses_top_folder_markers() {
    let mailbox_id = Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        mailbox_id,
        crate::mapi::identity::INBOX_FOLDER_ID,
    );
    let mailbox = JmapMailbox {
        id: mailbox_id,
        parent_id: None,
        role: "inbox".to_string(),
        name: "Inbox".to_string(),
        sort_order: 40,
        modseq: 42,
        total_emails: 1,
        unread_emails: 1,
        size_octets: 0,
        is_subscribed: true,
    };
    let email = test_email();
    let buffer = fast_transfer_top_folder_buffer_with_attachments(
        crate::mapi::identity::INBOX_FOLDER_ID,
        &[mailbox],
        &[email],
        &[],
    );

    assert_tag_sequence(
        &buffer,
        &[
            START_TOP_FLD,
            PID_TAG_CONTAINER_CLASS_W,
            PID_TAG_CONTENT_COUNT,
            PID_TAG_CONTENT_UNREAD_COUNT,
            PID_TAG_ACCESS,
            PID_TAG_SUBFOLDERS,
            START_MESSAGE,
            PID_TAG_SUBJECT_W,
            END_MESSAGE,
            END_FOLDER,
        ],
    );
    assert!(!buffer.starts_with(b"LPE-MAPI-FASTTRANSFER\0"));
    assert_variable_property_present(&buffer, PID_TAG_CONTAINER_CLASS_W, &utf16z("IPF.Note"));
    assert_variable_property_present(&buffer, PID_TAG_SUBJECT_W, &utf16z("Hello"));
}

#[test]
fn microsoft_oxcfxics_fast_transfer_copy_folder_uses_subfolder_markers() {
    let parent_id = Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap();
    let child_id = Uuid::parse_str("44444444-4444-4444-4444-444444444444").unwrap();
    let child_folder_id = crate::mapi::identity::mapi_store_id(600);
    crate::mapi::identity::remember_mapi_identity(
        parent_id,
        crate::mapi::identity::INBOX_FOLDER_ID,
    );
    crate::mapi::identity::remember_mapi_identity(child_id, child_folder_id);
    let parent = JmapMailbox {
        id: parent_id,
        parent_id: None,
        role: "inbox".to_string(),
        name: "Inbox".to_string(),
        sort_order: 40,
        modseq: 42,
        total_emails: 1,
        unread_emails: 1,
        size_octets: 0,
        is_subscribed: true,
    };
    let child = JmapMailbox {
        id: child_id,
        parent_id: Some(parent_id),
        role: String::new(),
        name: "Project".to_string(),
        sort_order: 50,
        modseq: 43,
        total_emails: 1,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    };
    let parent_email = test_email();
    let mut child_email = test_email();
    child_email.id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    child_email.mailbox_id = child_id;
    child_email.mailbox_role.clear();
    child_email.mailbox_name = "Project".to_string();
    child_email.mailbox_ids = vec![child_id];
    child_email.mailbox_states[0].mailbox_id = child_id;
    child_email.mailbox_states[0].role.clear();
    child_email.mailbox_states[0].name = "Project".to_string();
    child_email.subject = "Child message".to_string();
    let buffer = fast_transfer_top_folder_buffer_with_attachments(
        crate::mapi::identity::INBOX_FOLDER_ID,
        &[parent, child],
        &[parent_email, child_email],
        &[],
    );

    assert_tag_sequence(
        &buffer,
        &[
            START_TOP_FLD,
            START_MESSAGE,
            PID_TAG_SUBJECT_W,
            END_MESSAGE,
            START_SUB_FLD,
            PID_TAG_FOLDER_ID,
            PID_TAG_DISPLAY_NAME_W,
            PID_TAG_PARENT_FOLDER_ID,
            START_MESSAGE,
            PID_TAG_SUBJECT_W,
            END_MESSAGE,
            END_FOLDER,
        ],
    );
    assert_variable_property_present(&buffer, PID_TAG_DISPLAY_NAME_W, &utf16z("Project"));
    assert_variable_property_present(&buffer, PID_TAG_SUBJECT_W, &utf16z("Child message"));
}

#[test]
fn hierarchy_transfer_keeps_subfolders_optional_property() {
    let mailbox_id = Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        mailbox_id,
        crate::mapi::identity::mapi_store_id(5),
    );
    let mailbox = JmapMailbox {
        id: mailbox_id,
        parent_id: None,
        role: "inbox".to_string(),
        name: "Inbox".to_string(),
        sort_order: 40,
        modseq: 42,
        total_emails: 1,
        unread_emails: 1,
        size_octets: 0,
        is_subscribed: true,
    };
    let buffer = sync_manifest_buffer_with_attachments(
        0x02,
        0x0100,
        0,
        &[],
        crate::mapi::identity::ROOT_FOLDER_ID,
        &[mailbox],
        &[],
        &[],
        &[],
        1,
    );

    let subfolders = PID_TAG_SUBFOLDERS.to_le_bytes();
    let offset = buffer
        .windows(subfolders.len())
        .position(|window| window == subfolders)
        .expect("subfolders property is present");
    assert_eq!(&buffer[offset + 4..offset + 6], &0u16.to_le_bytes());
    assert_eq!(
        &buffer[offset + 6..offset + 10],
        &INCR_SYNC_STATE_BEGIN.to_le_bytes()
    );
}

#[test]
fn root_hierarchy_transfer_ipm_subtree_reports_virtual_children() {
    let mailbox = virtual_special_mailbox(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID)
        .expect("virtual IPM subtree folder");
    let buffer = sync_manifest_buffer_with_attachments(
        SYNC_TYPE_HIERARCHY,
        0,
        0,
        &[],
        crate::mapi::identity::ROOT_FOLDER_ID,
        &[mailbox],
        &[],
        &[],
        &[],
        1,
    );

    let summary = decode_hierarchy_transfer_debug_summary(&buffer).unwrap();
    let row = summary.rows.first().expect("IPM subtree folder row");

    assert_eq!(row.display_name, "Top of Information Store");
    assert_eq!(row.subfolders, Some(true));
}

#[test]
fn hierarchy_transfer_debug_decoder_summarizes_serialized_stream() {
    let mailbox_id = Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        mailbox_id,
        crate::mapi::identity::mapi_store_id(5),
    );
    let mailbox = JmapMailbox {
        id: mailbox_id,
        parent_id: None,
        role: "inbox".to_string(),
        name: "Inbox".to_string(),
        sort_order: 40,
        modseq: 42,
        total_emails: 1,
        unread_emails: 1,
        size_octets: 0,
        is_subscribed: true,
    };
    let buffer = sync_manifest_buffer_with_attachments(
        0x02,
        0x0100,
        0,
        &[PID_TAG_CONTENT_COUNT, PID_TAG_CONTENT_UNREAD_COUNT],
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
        &[mailbox],
        &[],
        &[],
        &[],
        1,
    );

    let summary = decode_hierarchy_transfer_debug_summary(&buffer).unwrap();

    assert_eq!(summary.folder_change_count, 1);
    assert!(summary.final_state_present);
    assert_eq!(
            format_marker_tags(&summary.marker_tags),
            "IncrSyncChg:0x40120003,IncrSyncStateBegin:0x403a0003,IncrSyncStateEnd:0x403b0003,IncrSyncEnd:0x40140003"
        );
    assert!(summary.stream_end_marker_seen);
    assert_eq!(summary.parent_before_child_violations, 0);
    assert_eq!(summary.zero_length_parent_source_key_count, 1);
    assert_eq!(summary.nonzero_parent_source_key_count, 0);
    assert_eq!(summary.source_key_lengths, vec![22]);
    assert_eq!(summary.change_key_lengths, vec![22]);
    assert_eq!(
        summary.final_state_property_tags,
        vec![META_TAG_IDSET_GIVEN, META_TAG_CNSET_SEEN]
    );
    assert!(summary.final_state_expected_property_order_ok);
    assert_eq!(summary.final_state_property_lengths, vec![30, 30]);
    assert_eq!(summary.final_state_idset_given_len, 30);
    assert_eq!(summary.final_state_cnset_seen_len, 30);
    assert_eq!(summary.final_state_idset_given_counters, vec![5]);
    assert_eq!(summary.final_state_cnset_seen_counters, vec![5]);
    assert!(summary.final_state_idset_given_includes_all_expected_folder_source_counters);
    assert!(summary.final_state_cnset_seen_includes_all_expected_folder_change_counters);
    assert_eq!(summary.first_folder_name(), "Inbox");
    assert_eq!(summary.last_folder_name(), "Inbox");
    assert!(summary
        .final_state_idset_given_summary
        .as_deref()
        .unwrap()
        .contains("ranges=5"));
    assert!(summary
        .final_state_cnset_seen_summary
        .as_deref()
        .unwrap()
        .contains("ranges=5"));
    assert!(summary.emitted_property_tags.contains(&PID_TAG_SOURCE_KEY));
    assert!(summary
        .emitted_property_tags
        .contains(&PID_TAG_PARENT_SOURCE_KEY));
    assert!(summary.emitted_property_tags.contains(&PID_TAG_CHANGE_KEY));
    assert_eq!(summary.rows.len(), 1);
    assert_eq!(summary.rows[0].display_name, "Inbox");
    assert_eq!(summary.rows[0].container_class, "IPF.Note");
    assert!(summary.rows[0]
        .property_tags
        .contains(&PID_TAG_CONTAINER_CLASS_W));
    assert_eq!(summary.rows[0].folder_id, None);
    assert_eq!(summary.rows[0].source_key_len, 22);
    assert_eq!(summary.rows[0].parent_source_key_len, 0);
    assert!(hierarchy_identity_properties_before_display_name(
        &summary.rows[0].property_tags
    ));
    assert!(summary.rows[0].missing_core_property_tags.is_empty());

    let validation =
        hierarchy_semantic_validation(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID, &summary);
    assert_eq!(validation.semantic_flags, "ok");
    assert_eq!(
        validation.sync_root_source_counter,
        crate::mapi::identity::IPM_SUBTREE_FOLDER_COUNTER
    );
    assert_eq!(
        validation.sync_root_change_counter,
        crate::mapi::identity::IPM_SUBTREE_FOLDER_COUNTER
    );
    assert!(!validation.sync_root_row_present);
    assert!(!validation.sync_root_counter_in_final_idset);
    assert!(!validation.sync_root_counter_in_final_cnset);
    assert!(validation.root_inclusive_idset_given_delta_bytes >= 0);
    assert!(validation.root_inclusive_cnset_seen_delta_bytes >= 0);
    assert!(validation
        .root_inclusive_idset_given_summary
        .contains("ranges=4-5"));
    assert!(validation
        .root_inclusive_cnset_seen_summary
        .contains("ranges=4-5"));
    assert_eq!(validation.top_level_row_count, 1);
    assert_eq!(validation.nested_row_count, 0);
    assert_eq!(validation.rows_without_folder_id, 1);
    assert_eq!(validation.rows_missing_core_property_count, 0);
    assert_eq!(validation.rows_with_content_counts_present, 0);
    assert_eq!(validation.rows_with_folder_type_present, 1);
    assert_eq!(validation.rows_with_access_present, 1);
    assert!(validation.idset_missing_source_counters.is_empty());
    assert!(validation.idset_extra_source_counters.is_empty());
    assert!(validation.cnset_missing_change_counters.is_empty());
    assert!(validation.cnset_extra_change_counters.is_empty());
    assert_eq!(validation.top_level_row_names, "Inbox");
    assert!(validation.rows_missing_core_property_names.is_empty());
}

#[test]
fn ipm_hierarchy_transfer_excludes_sync_root_and_zeros_direct_child_parent_key() {
    let sync_root = virtual_special_mailbox(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID)
        .expect("virtual IPM subtree folder");
    let inbox = virtual_special_mailbox(crate::mapi::identity::INBOX_FOLDER_ID)
        .expect("virtual Inbox folder");
    let buffer = sync_manifest_buffer_with_attachments(
        SYNC_TYPE_HIERARCHY,
        0x0100,
        0,
        &[],
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
        &[sync_root, inbox],
        &[],
        &[],
        &[],
        1,
    );

    let summary = decode_hierarchy_transfer_debug_summary(&buffer).unwrap();

    // [MS-OXCFXICS] 2.2.4.3.9: only descendants are folderChange
    // elements, and a zero-length parent source key identifies a direct child.
    assert_eq!(summary.folder_change_count, 1);
    assert_eq!(summary.first_folder_name(), "Inbox");
    assert!(summary
        .rows
        .iter()
        .all(|row| { row.folder_id != Some(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID) }));
    assert_eq!(summary.rows[0].parent_source_key_len, 0);
    assert_eq!(summary.final_state_idset_given_counters, vec![5]);
    assert_eq!(summary.final_state_cnset_seen_counters, vec![5]);
}

#[test]
fn hierarchy_parent_source_key_role_matches_microsoft_ics_root_child_rule() {
    assert_eq!(
        hierarchy_parent_source_key_role(
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            true,
        ),
        "sync_root_child_zero_length"
    );
    assert_eq!(
        hierarchy_parent_source_key_role(
            crate::mapi::identity::SYNC_ISSUES_FOLDER_ID,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            false,
        ),
        "nested_child_source_key"
    );
    assert_eq!(
        hierarchy_parent_source_key_role(
            crate::mapi::identity::SYNC_ISSUES_FOLDER_ID,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            true,
        ),
        "unexpected_zero_parent_source_key"
    );
}

#[test]
fn hierarchy_microsoft_payload_comparison_matches_documented_folder_change_rules() {
    let mailbox_id = Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        mailbox_id,
        crate::mapi::identity::mapi_store_id(5),
    );
    let mailbox = JmapMailbox {
        id: mailbox_id,
        parent_id: None,
        role: "inbox".to_string(),
        name: "Inbox".to_string(),
        sort_order: 40,
        modseq: 42,
        total_emails: 1,
        unread_emails: 1,
        size_octets: 0,
        is_subscribed: true,
    };
    let requested_property_tags = [PID_TAG_CONTENT_COUNT, PID_TAG_CONTENT_UNREAD_COUNT];
    let buffer = sync_manifest_buffer_with_attachments(
        SYNC_TYPE_HIERARCHY,
        SYNC_FLAG_NO_FOREIGN_IDENTIFIERS,
        0,
        &requested_property_tags,
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
        &[mailbox],
        &[],
        &[],
        &[],
        1,
    );
    let summary = decode_hierarchy_transfer_debug_summary(&buffer).unwrap();

    let comparison = hierarchy_microsoft_payload_comparison(
        SYNC_FLAG_NO_FOREIGN_IDENTIFIERS,
        0,
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
        &requested_property_tags,
        &summary,
    );

    assert!(comparison.required_missing_row_names.is_empty());
    assert!(!comparison.folder_id_expected);
    assert!(comparison.folder_id_presence_mismatch_rows.is_empty());
    assert!(comparison.parent_folder_id_expected_by_no_foreign_identifiers);
    assert!(!comparison.parent_folder_id_recommended_by_eid);
    assert!(comparison.parent_folder_id_missing_required_rows.is_empty());
    assert!(comparison
        .optional_property_tags
        .contains(&PID_TAG_CONTAINER_CLASS_W));
    assert!(comparison
        .optional_property_tags
        .contains(&PID_TAG_SUBFOLDERS));
    assert!(!comparison
        .optional_property_tags
        .contains(&PID_TAG_PARENT_SOURCE_KEY));
    assert!(comparison
        .requested_excluded_property_present_tags
        .is_empty());
    assert!(comparison.final_state_exact_property_sequence);
    assert!(comparison.final_state_missing_property_tags.is_empty());
    assert!(comparison.final_state_extra_property_tags.is_empty());
    assert!(comparison
        .final_state_idset_missing_source_counters
        .is_empty());
    assert!(comparison
        .final_state_idset_extra_source_counters
        .is_empty());
    assert!(comparison
        .final_state_cnset_missing_change_counters
        .is_empty());
    assert!(comparison
        .final_state_cnset_extra_change_counters
        .is_empty());
}

#[test]
fn hierarchy_transfer_omits_targeted_optional_properties_but_keeps_required_outlook_shape() {
    let mailbox_id = Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        mailbox_id,
        crate::mapi::identity::mapi_store_id(5),
    );
    let mailbox = JmapMailbox {
        id: mailbox_id,
        parent_id: None,
        role: "inbox".to_string(),
        name: "Inbox".to_string(),
        sort_order: 40,
        modseq: 42,
        total_emails: 1,
        unread_emails: 1,
        size_octets: 0,
        is_subscribed: true,
    };
    let email = test_email();
    let buffer = sync_manifest_buffer_with_final_state(
        Uuid::nil(),
        SYNC_TYPE_HIERARCHY,
        SYNC_FLAG_NO_FOREIGN_IDENTIFIERS,
        0,
        &[],
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
        std::slice::from_ref(&mailbox),
        &[],
        &[],
        &[],
        std::slice::from_ref(&mailbox),
        std::slice::from_ref(&mailbox),
        &[],
        &[],
        std::slice::from_ref(&email),
        &[],
        1,
    );

    let summary = decode_hierarchy_transfer_debug_summary(&buffer).unwrap();
    let row = summary.rows.first().expect("folder row");

    assert!(summary
        .emitted_property_tags
        .contains(&PID_TAG_LOCAL_COMMIT_TIME_MAX));
    assert!(summary
        .emitted_property_tags
        .contains(&PID_TAG_DELETED_COUNT_TOTAL));
    assert!(!summary
        .emitted_property_tags
        .contains(&PID_TAG_CHANGE_NUMBER));
    assert!(!summary
        .emitted_property_tags
        .contains(&PID_TAG_CONTENT_COUNT));
    assert!(!summary
        .emitted_property_tags
        .contains(&PID_TAG_CONTENT_UNREAD_COUNT));
    assert!(summary
        .emitted_property_tags
        .contains(&PID_TAG_CONTAINER_CLASS_W));
    assert!(row.local_commit_time_max.is_some());
    assert_eq!(row.deleted_count_total, Some(0));
    assert_eq!(row.change_number, None);
    assert_eq!(row.content_count, None);
    assert_eq!(row.content_unread_count, None);
    assert!(row.missing_core_property_tags.is_empty());
    assert!(row.property_tags.contains(&PID_TAG_PARENT_FOLDER_ID));
    assert_eq!(
        row.parent_folder_id,
        Some(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID)
    );
    assert!(row.property_tags.contains(&PID_TAG_CONTAINER_CLASS_W));
    assert_eq!(row.container_class, "IPF.Note");
    assert!(row.property_tags.contains(&PID_TAG_SUBFOLDERS));
    assert_eq!(
        summary.final_state_property_tags,
        vec![META_TAG_IDSET_GIVEN, META_TAG_CNSET_SEEN]
    );
    assert!(summary.final_state_expected_property_order_ok);
    assert!(summary.final_state_idset_given_includes_all_expected_folder_source_counters);
    assert!(summary.final_state_cnset_seen_includes_all_expected_folder_change_counters);
}

#[test]
fn hierarchy_transfer_debug_summary_tracks_emitted_ipm_final_state_counters() {
    let folder_ids = [
        crate::mapi::identity::INBOX_FOLDER_ID,
        crate::mapi::identity::DRAFTS_FOLDER_ID,
        crate::mapi::identity::OUTBOX_FOLDER_ID,
        crate::mapi::identity::SENT_FOLDER_ID,
        crate::mapi::identity::TRASH_FOLDER_ID,
        crate::mapi::identity::CONTACTS_FOLDER_ID,
        crate::mapi::identity::SUGGESTED_CONTACTS_FOLDER_ID,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
        crate::mapi::identity::JOURNAL_FOLDER_ID,
        crate::mapi::identity::NOTES_FOLDER_ID,
        crate::mapi::identity::TASKS_FOLDER_ID,
        crate::mapi::identity::SYNC_ISSUES_FOLDER_ID,
        crate::mapi::identity::CONFLICTS_FOLDER_ID,
        crate::mapi::identity::LOCAL_FAILURES_FOLDER_ID,
        crate::mapi::identity::SERVER_FAILURES_FOLDER_ID,
        crate::mapi::identity::JUNK_FOLDER_ID,
        crate::mapi::identity::RSS_FEEDS_FOLDER_ID,
        crate::mapi::identity::ARCHIVE_FOLDER_ID,
    ];
    let expected_folder_count = folder_ids.len();
    let mut mailboxes = folder_ids
        .into_iter()
        .map(|folder_id| virtual_special_mailbox(folder_id).expect("virtual folder"))
        .collect::<Vec<_>>();
    let conversation_history_id = Uuid::parse_str("73737373-7373-4373-8373-737373737373").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        conversation_history_id,
        crate::mapi::identity::CONVERSATION_HISTORY_FOLDER_ID,
    );
    mailboxes.push(JmapMailbox {
        id: conversation_history_id,
        parent_id: None,
        role: "conversation_history".to_string(),
        name: "Conversation History".to_string(),
        sort_order: 0,
        modseq: 37,
        total_emails: 0,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    });
    let buffer = sync_manifest_buffer_with_attachments(
        SYNC_TYPE_HIERARCHY,
        0,
        0,
        &[],
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
        &mailboxes,
        &[],
        &[],
        &[],
        1,
    );

    let summary = decode_hierarchy_transfer_debug_summary(&buffer).unwrap();

    assert_eq!(summary.folder_change_count, expected_folder_count + 1);
    assert_eq!(summary.zero_length_parent_source_key_count, 16);
    assert_eq!(summary.nonzero_parent_source_key_count, 3);
    assert!(summary.final_state_idset_given_includes_all_expected_folder_source_counters);
    assert!(summary.final_state_cnset_seen_includes_all_expected_folder_change_counters);
    assert_eq!(summary.first_folder_name(), "Inbox");
    assert_eq!(summary.last_folder_name(), "Server Failures");

    let validation =
        hierarchy_semantic_validation(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID, &summary);
    assert_eq!(validation.semantic_flags, "ok");
    assert_eq!(validation.top_level_row_count, 16);
    assert_eq!(validation.nested_row_count, 3);
    assert_eq!(
        validation.rows_without_folder_id,
        summary.folder_change_count
    );
    assert_eq!(validation.rows_missing_core_property_count, 0);
    assert!(validation.root_inclusive_idset_given_delta_bytes >= 0);
    assert!(validation.root_inclusive_cnset_seen_delta_bytes >= 0);
    assert!(validation
        .root_inclusive_idset_given_summary
        .contains("ranges=4-8"));
    assert!(validation
        .top_level_row_names
        .starts_with("Inbox,Drafts,Outbox"));
    assert!(validation
        .top_level_row_names
        .contains("Conversation History"));
}

#[test]
fn default_folder_hierarchy_membership_summary_tracks_top_level_ipm_folders() {
    let folder_ids = [
        crate::mapi::identity::INBOX_FOLDER_ID,
        crate::mapi::identity::DRAFTS_FOLDER_ID,
        crate::mapi::identity::OUTBOX_FOLDER_ID,
        crate::mapi::identity::SENT_FOLDER_ID,
        crate::mapi::identity::TRASH_FOLDER_ID,
        crate::mapi::identity::CONTACTS_FOLDER_ID,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
        crate::mapi::identity::JOURNAL_FOLDER_ID,
        crate::mapi::identity::NOTES_FOLDER_ID,
        crate::mapi::identity::TASKS_FOLDER_ID,
    ];
    let mailboxes = folder_ids
        .into_iter()
        .map(|folder_id| virtual_special_mailbox(folder_id).expect("virtual folder"))
        .collect::<Vec<_>>();
    let buffer = sync_manifest_buffer_with_attachments(
        SYNC_TYPE_HIERARCHY,
        0,
        0,
        &[],
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
        &mailboxes,
        &[],
        &[],
        &[],
        1,
    );

    let summary = default_folder_hierarchy_membership_summary(
        SYNC_TYPE_HIERARCHY,
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
        &buffer,
    );

    assert!(summary.contains(&format!(
        "inbox:fid=0x{:016x};row_present=true",
        crate::mapi::identity::INBOX_FOLDER_ID
    )));
    assert!(summary.contains(&format!(
        "calendar:fid=0x{:016x};row_present=true",
        crate::mapi::identity::CALENDAR_FOLDER_ID
    )));
    assert!(summary.contains(&format!(
        "contacts:fid=0x{:016x};row_present=true",
        crate::mapi::identity::CONTACTS_FOLDER_ID
    )));
    assert!(summary.contains("parent_source_key_expected=true"));
    assert!(summary.contains("parent_source_key_len=0"));
    assert!(summary.contains("idset_present=true"));
    assert!(summary.contains("cnset_present=true"));
}

#[test]
fn hierarchy_transfer_without_eid_omits_folder_id_but_keeps_parent_folder_id() {
    let mailbox_id = Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        mailbox_id,
        crate::mapi::identity::mapi_store_id(5),
    );
    let mailbox = JmapMailbox {
        id: mailbox_id,
        parent_id: None,
        role: "inbox".to_string(),
        name: "Inbox".to_string(),
        sort_order: 40,
        modseq: 42,
        total_emails: 1,
        unread_emails: 1,
        size_octets: 0,
        is_subscribed: true,
    };
    let buffer = sync_manifest_buffer_with_attachments(
        0x02,
        0x0100,
        0,
        &[],
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
        &[mailbox],
        &[],
        &[],
        &[],
        1,
    );

    let summary = decode_hierarchy_transfer_debug_summary(&buffer).unwrap();

    assert_eq!(summary.rows.len(), 1);
    assert_eq!(summary.rows[0].folder_id, None);
    assert_eq!(
        summary.rows[0].parent_folder_id,
        Some(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID)
    );
    assert!(!summary.emitted_property_tags.contains(&PID_TAG_FOLDER_ID));
}

#[test]
fn hierarchy_transfer_includes_folder_id_with_eid_extra_flag() {
    let mailbox_id = Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        mailbox_id,
        crate::mapi::identity::mapi_store_id(5),
    );
    let mailbox = JmapMailbox {
        id: mailbox_id,
        parent_id: None,
        role: "inbox".to_string(),
        name: "Inbox".to_string(),
        sort_order: 40,
        modseq: 42,
        total_emails: 1,
        unread_emails: 1,
        size_octets: 0,
        is_subscribed: true,
    };
    let buffer = sync_manifest_buffer_with_attachments(
        0x02,
        0x0100,
        SYNC_EXTRA_FLAG_EID,
        &[],
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
        &[mailbox],
        &[],
        &[],
        &[],
        1,
    );

    let summary = decode_hierarchy_transfer_debug_summary(&buffer).unwrap();

    assert_eq!(summary.rows.len(), 1);
    assert_eq!(
        summary.rows[0].folder_id,
        Some(crate::mapi::identity::INBOX_FOLDER_ID)
    );
    assert!(summary.emitted_property_tags.contains(&PID_TAG_FOLDER_ID));
}

#[test]
fn hierarchy_transfer_calendar_includes_account_scoped_entry_id() {
    let account_id = Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").unwrap();
    let mailbox = virtual_special_mailbox(crate::mapi::identity::CALENDAR_FOLDER_ID)
        .expect("virtual calendar folder");
    let entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        account_id,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    )
    .unwrap();
    let buffer = sync_manifest_buffer_with_final_state(
        account_id,
        SYNC_TYPE_HIERARCHY,
        0,
        0,
        &[],
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
        std::slice::from_ref(&mailbox),
        &[],
        &[],
        &[],
        std::slice::from_ref(&mailbox),
        std::slice::from_ref(&mailbox),
        &[],
        &[],
        &[],
        &[],
        1,
    );

    assert_variable_property(&buffer, PID_TAG_ENTRY_ID, &entry_id);
    assert_variable_property(
        &buffer,
        PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W,
        &utf16z("IPM.Appointment"),
    );
}

#[test]
fn hierarchy_transfer_respects_entry_id_exclusion() {
    let account_id = Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").unwrap();
    let mailbox = virtual_special_mailbox(crate::mapi::identity::CALENDAR_FOLDER_ID)
        .expect("virtual calendar folder");
    let buffer = sync_manifest_buffer_with_final_state(
        account_id,
        SYNC_TYPE_HIERARCHY,
        0,
        0,
        &[PID_TAG_ENTRY_ID],
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
        std::slice::from_ref(&mailbox),
        &[],
        &[],
        &[],
        std::slice::from_ref(&mailbox),
        std::slice::from_ref(&mailbox),
        &[],
        &[],
        &[],
        &[],
        1,
    );

    assert_absent_property(&buffer, PID_TAG_ENTRY_ID);
}

#[test]
fn hierarchy_transfer_respects_default_post_message_class_exclusion() {
    let account_id = Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").unwrap();
    let mailbox = virtual_special_mailbox(crate::mapi::identity::CALENDAR_FOLDER_ID)
        .expect("virtual calendar folder");
    let buffer = sync_manifest_buffer_with_final_state(
        account_id,
        SYNC_TYPE_HIERARCHY,
        0,
        0,
        &[PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W],
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
        std::slice::from_ref(&mailbox),
        &[],
        &[],
        &[],
        std::slice::from_ref(&mailbox),
        std::slice::from_ref(&mailbox),
        &[],
        &[],
        &[],
        &[],
        1,
    );

    assert_absent_property(&buffer, PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W);
    assert_variable_property(
        &buffer,
        PID_TAG_CONTAINER_CLASS_W,
        &utf16z("IPF.Appointment"),
    );
}

#[test]
fn hierarchy_transfer_respects_default_post_message_class_string8_exclusion() {
    let account_id = Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").unwrap();
    let mailbox = virtual_special_mailbox(crate::mapi::identity::CALENDAR_FOLDER_ID)
        .expect("virtual calendar folder");
    let buffer = sync_manifest_buffer_with_final_state(
        account_id,
        SYNC_TYPE_HIERARCHY,
        0,
        0,
        &[0x36E5_001E],
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
        std::slice::from_ref(&mailbox),
        &[],
        &[],
        &[],
        std::slice::from_ref(&mailbox),
        std::slice::from_ref(&mailbox),
        &[],
        &[],
        &[],
        &[],
        1,
    );

    assert_absent_property(&buffer, PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W);
    assert_variable_property(
        &buffer,
        PID_TAG_CONTAINER_CLASS_W,
        &utf16z("IPF.Appointment"),
    );
}

#[test]
fn hierarchy_transfer_omits_custom_sync_root_and_projects_children() {
    let root_id = Uuid::parse_str("33333333-3333-3333-3333-333333333334").unwrap();
    let child_id = Uuid::parse_str("33333333-3333-3333-3333-333333333335").unwrap();
    let root_folder_id = crate::mapi::identity::mapi_store_id(100);
    let child_folder_id = crate::mapi::identity::mapi_store_id(101);
    crate::mapi::identity::remember_mapi_identity(root_id, root_folder_id);
    crate::mapi::identity::remember_mapi_identity(child_id, child_folder_id);
    let root = JmapMailbox {
        id: root_id,
        parent_id: None,
        role: "custom".to_string(),
        name: "Project".to_string(),
        sort_order: 40,
        modseq: 42,
        total_emails: 0,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    };
    let child = JmapMailbox {
        id: child_id,
        parent_id: Some(root_id),
        role: "custom".to_string(),
        name: "Archive".to_string(),
        sort_order: 40,
        modseq: 43,
        total_emails: 0,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    };
    let buffer = sync_manifest_buffer_with_attachments(
        0x02,
        0x0100,
        0,
        &[],
        root_folder_id,
        &[child, root],
        &[],
        &[],
        &[],
        1,
    );

    let summary = decode_hierarchy_transfer_debug_summary(&buffer).unwrap();

    assert_eq!(summary.rows.len(), 1);
    assert_eq!(summary.rows[0].display_name, "Archive");
    assert_eq!(summary.rows[0].folder_id, None);
    assert_eq!(summary.rows[0].parent_folder_id, Some(root_folder_id));
    assert_eq!(summary.rows[0].parent_source_key_len, 0);
}

#[test]
fn content_sync_manifest_includes_special_folder_message_objects() {
    let canonical_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap();
    let item_id = crate::mapi::identity::mapi_store_id(99);
    crate::mapi::identity::remember_mapi_identity(canonical_id, item_id);
    let special = SpecialMessageSyncFact {
        folder_id: crate::mapi::identity::NOTES_FOLDER_ID,
        item_id,
        canonical_id,
        associated: false,
        subject: "Sticky".to_string(),
        body_text: "Remember this".to_string(),
        message_class: "IPM.StickyNote".to_string(),
        last_modified_filetime: filetime_from_rfc3339_utc("2026-05-19T10:00:00Z"),
        message_size: 19,
        read_state: None,
        named_properties: vec![(0x8B00_0003, SpecialMessagePropertyValue::I32(3))],
    };
    let buffer = sync_manifest_buffer_with_special_objects_and_final_state(
        Uuid::nil(),
        SYNC_TYPE_CONTENTS,
        SYNC_FLAG_NORMAL,
        SYNC_EXTRA_FLAG_EID,
        &[],
        crate::mapi::identity::NOTES_FOLDER_ID,
        &[],
        &[],
        &[],
        &[special.clone()],
        &[],
        &[],
        &[],
        &[],
        &[],
        &[special],
        &[],
        &[],
        1,
    );

    assert!(contains_bytes(&buffer, &INCR_SYNC_CHG.to_le_bytes()));
    assert!(contains_bytes(&buffer, &INCR_SYNC_MESSAGE.to_le_bytes()));
    assert!(contains_bytes(&buffer, &wire_id_bytes(item_id)));
    assert!(contains_bytes(&buffer, &utf16z("IPM.StickyNote")));
    assert!(contains_bytes(&buffer, &utf16z("Remember this")));
    assert!(contains_bytes(&buffer, &0x8B00_0003u32.to_le_bytes()));
}

#[test]
fn microsoft_oxcfxics_content_sync_progress_markers_follow_progress_flag_example() {
    let mut email = test_email();
    email.subject = "Progress message".to_string();
    email.size_octets = 56;
    crate::mapi::identity::remember_mapi_identity(
        email.id,
        crate::mapi::identity::mapi_store_id(56),
    );
    let buffer = sync_manifest_buffer_with_special_objects_and_final_state(
        Uuid::nil(),
        SYNC_TYPE_CONTENTS,
        SYNC_FLAG_NORMAL | SYNC_FLAG_PROGRESS,
        SYNC_EXTRA_FLAG_EID | SYNC_EXTRA_FLAG_MESSAGE_SIZE,
        &[],
        crate::mapi::identity::INBOX_FOLDER_ID,
        &[],
        std::slice::from_ref(&email),
        &[],
        &[],
        &[],
        &[],
        &[],
        std::slice::from_ref(&email),
        &[],
        &[],
        std::slice::from_ref(&email),
        &[],
        1,
    );

    assert_tag_order(
        &buffer,
        &[
            INCR_SYNC_PROGRESS_MODE,
            0x0000_0102,
            INCR_SYNC_PROGRESS_PER_MSG,
            0x0000_0003,
            0x0000_000B,
            INCR_SYNC_CHG,
            INCR_SYNC_MESSAGE,
            INCR_SYNC_STATE_BEGIN,
            INCR_SYNC_STATE_END,
            INCR_SYNC_END,
        ],
    );
    let progress_offset = buffer
        .windows(4)
        .position(|window| window == 0x0000_0102u32.to_le_bytes())
        .unwrap();
    assert_eq!(
        u32::from_le_bytes(
            buffer[progress_offset + 4..progress_offset + 8]
                .try_into()
                .unwrap()
        ),
        32
    );
    assert_eq!(
        u32::from_le_bytes(
            buffer[progress_offset + 24..progress_offset + 28]
                .try_into()
                .unwrap()
        ),
        1
    );
    assert_eq!(
        u64::from_le_bytes(
            buffer[progress_offset + 32..progress_offset + 40]
                .try_into()
                .unwrap()
        ),
        56
    );
}

#[test]
fn content_sync_manifest_starts_fai_message_before_item_properties() {
    let canonical_id = Uuid::parse_str("99999999-9999-9999-9999-999999999997").unwrap();
    let item_id = crate::mapi::identity::mapi_store_id(97);
    crate::mapi::identity::remember_mapi_identity(canonical_id, item_id);
    let special = SpecialMessageSyncFact {
        folder_id: crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
        item_id,
        canonical_id,
        associated: true,
        subject: "Calendar".to_string(),
        body_text: String::new(),
        message_class: "IPM.Microsoft.WunderBar.Link".to_string(),
        last_modified_filetime: filetime_from_rfc3339_utc("2026-05-19T10:00:00Z"),
        message_size: 0,
        read_state: None,
        named_properties: Vec::new(),
    };
    let buffer = sync_manifest_buffer_with_special_objects_and_final_state(
        Uuid::nil(),
        SYNC_TYPE_CONTENTS,
        SYNC_FLAG_FAI,
        SYNC_EXTRA_FLAG_EID | SYNC_EXTRA_FLAG_MESSAGE_SIZE | SYNC_EXTRA_FLAG_CHANGE_NUMBER,
        &[],
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
        &[],
        &[],
        &[],
        std::slice::from_ref(&special),
        &[],
        &[],
        &[],
        &[],
        &[],
        std::slice::from_ref(&special),
        &[],
        &[],
        1,
    );

    assert_tag_order(
        &buffer,
        &[INCR_SYNC_CHG, INCR_SYNC_MESSAGE, PID_TAG_PARENT_SOURCE_KEY],
    );
    let summary = decode_content_transfer_fai_debug_summary(&buffer).unwrap();
    assert_eq!(summary.fai_items.len(), 1);
    let item = &summary.fai_items[0];
    let message_start = item.message_start_marker_offset.unwrap();
    let property_start = item.property_list_start_offset.unwrap();
    assert!(item.item_start_offset < message_start);
    assert!(message_start < property_start);
    assert!(property_start < item.item_end_offset);
    assert_eq!(item.item_id, Some(item_id));
    assert_eq!(item.associated, Some(true));
    assert_eq!(item.subject, "Calendar");
    assert_eq!(item.message_class, "IPM.Microsoft.WunderBar.Link");
    assert!(item.source_key_len > 0);
    assert!(item.parent_source_key_len > 0);
}

#[test]
fn content_sync_manifest_unicode_fai_uses_unicode_normalized_subject() {
    let canonical_id = Uuid::parse_str("99999999-9999-9999-9999-999999999996").unwrap();
    let item_id = crate::mapi::identity::mapi_store_id(96);
    crate::mapi::identity::remember_mapi_identity(canonical_id, item_id);
    let special = SpecialMessageSyncFact {
        folder_id: crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
        item_id,
        canonical_id,
        associated: true,
        subject: "Calendar".to_string(),
        body_text: String::new(),
        message_class: "IPM.Microsoft.WunderBar.Link".to_string(),
        last_modified_filetime: filetime_from_rfc3339_utc("2026-05-19T10:00:00Z"),
        message_size: 128,
        read_state: None,
        named_properties: Vec::new(),
    };
    let buffer = sync_manifest_buffer_with_special_objects_and_final_state(
        Uuid::nil(),
        SYNC_TYPE_CONTENTS,
        SYNC_FLAG_UNICODE | SYNC_FLAG_FAI,
        SYNC_EXTRA_FLAG_EID | SYNC_EXTRA_FLAG_MESSAGE_SIZE | SYNC_EXTRA_FLAG_CHANGE_NUMBER,
        &[],
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
        &[],
        &[],
        &[],
        std::slice::from_ref(&special),
        &[],
        &[],
        &[],
        &[],
        &[],
        std::slice::from_ref(&special),
        &[],
        &[],
        1,
    );

    assert_variable_property(&buffer, PID_TAG_NORMALIZED_SUBJECT_W, &utf16z("Calendar"));
    assert_absent_property(&buffer, PID_TAG_NORMALIZED_SUBJECT_A);
    let summary = decode_content_transfer_fai_debug_summary(&buffer).unwrap();
    assert_eq!(summary.fai_items.len(), 1);
    assert!(summary.fai_items[0]
        .property_tags
        .contains(&PID_TAG_NORMALIZED_SUBJECT_W));
}

#[test]
fn content_sync_manifest_applies_property_excludes_to_special_objects() {
    let canonical_id = Uuid::parse_str("99999999-9999-9999-9999-999999999998").unwrap();
    let item_id = crate::mapi::identity::mapi_store_id(98);
    crate::mapi::identity::remember_mapi_identity(canonical_id, item_id);
    let special = SpecialMessageSyncFact {
        folder_id: crate::mapi::identity::CALENDAR_FOLDER_ID,
        item_id,
        canonical_id,
        associated: false,
        subject: "Kept subject".to_string(),
        body_text: "Filtered body".to_string(),
        message_class: "IPM.Appointment".to_string(),
        last_modified_filetime: filetime_from_rfc3339_utc("2026-05-19T10:00:00Z"),
        message_size: 19,
        read_state: None,
        named_properties: vec![(0x8205_0003, SpecialMessagePropertyValue::I32(2))],
    };
    let excluded_property_tags = [
        PID_TAG_MESSAGE_CLASS_W,
        PID_TAG_BODY_W,
        PID_TAG_MESSAGE_SIZE,
        0x8205_0003,
    ];
    let buffer = sync_manifest_buffer_with_special_objects_and_final_state(
        Uuid::nil(),
        SYNC_TYPE_CONTENTS,
        SYNC_FLAG_NORMAL,
        SYNC_EXTRA_FLAG_EID,
        &excluded_property_tags,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
        &[],
        &[],
        &[],
        &[special.clone()],
        &[],
        &[],
        &[],
        &[],
        &[],
        &[special],
        &[],
        &[],
        1,
    );

    assert!(contains_bytes(&buffer, &utf16z("Kept subject")));
    assert!(!contains_bytes(&buffer, &utf16z("IPM.Appointment")));
    assert!(!contains_bytes(&buffer, &utf16z("Filtered body")));
    assert!(!contains_bytes(
        &buffer,
        &PID_TAG_MESSAGE_SIZE.to_le_bytes()
    ));
    assert!(!contains_bytes(&buffer, &0x8205_0003u32.to_le_bytes()));
}

#[test]
fn content_sync_manifest_applies_string8_property_excludes_to_special_objects() {
    let canonical_id = Uuid::parse_str("99999999-9999-9999-9999-999999999995").unwrap();
    let item_id = crate::mapi::identity::mapi_store_id(95);
    crate::mapi::identity::remember_mapi_identity(canonical_id, item_id);
    let special = SpecialMessageSyncFact {
        folder_id: crate::mapi::identity::CALENDAR_FOLDER_ID,
        item_id,
        canonical_id,
        associated: false,
        subject: "Kept subject".to_string(),
        body_text: String::new(),
        message_class: "IPM.Appointment".to_string(),
        last_modified_filetime: filetime_from_rfc3339_utc("2026-05-19T10:00:00Z"),
        message_size: 19,
        read_state: None,
        named_properties: Vec::new(),
    };
    let buffer = sync_manifest_buffer_with_special_objects_and_final_state(
        Uuid::nil(),
        SYNC_TYPE_CONTENTS,
        SYNC_FLAG_NORMAL,
        SYNC_EXTRA_FLAG_EID,
        &[0x001A_001E],
        crate::mapi::identity::CALENDAR_FOLDER_ID,
        &[],
        &[],
        &[],
        std::slice::from_ref(&special),
        &[],
        &[],
        &[],
        &[],
        &[],
        std::slice::from_ref(&special),
        &[],
        &[],
        1,
    );

    assert!(contains_bytes(&buffer, &utf16z("Kept subject")));
    assert!(!contains_bytes(&buffer, &utf16z("IPM.Appointment")));
}

#[test]
fn content_sync_manifest_applies_string8_property_includes_to_special_objects() {
    let canonical_id = Uuid::parse_str("99999999-9999-9999-9999-999999999994").unwrap();
    let item_id = crate::mapi::identity::mapi_store_id(94);
    crate::mapi::identity::remember_mapi_identity(canonical_id, item_id);
    let special = SpecialMessageSyncFact {
        folder_id: crate::mapi::identity::CALENDAR_FOLDER_ID,
        item_id,
        canonical_id,
        associated: false,
        subject: "Filtered subject".to_string(),
        body_text: String::new(),
        message_class: "IPM.Appointment".to_string(),
        last_modified_filetime: filetime_from_rfc3339_utc("2026-05-19T10:00:00Z"),
        message_size: 19,
        read_state: None,
        named_properties: Vec::new(),
    };
    let buffer = sync_manifest_buffer_with_special_objects_and_final_state(
        Uuid::nil(),
        SYNC_TYPE_CONTENTS,
        SYNC_FLAG_NORMAL | 0x0080,
        SYNC_EXTRA_FLAG_EID,
        &[0x001A_001E],
        crate::mapi::identity::CALENDAR_FOLDER_ID,
        &[],
        &[],
        &[],
        std::slice::from_ref(&special),
        &[],
        &[],
        &[],
        &[],
        &[],
        std::slice::from_ref(&special),
        &[],
        &[],
        1,
    );

    assert!(!contains_bytes(&buffer, &utf16z("Filtered subject")));
    assert!(contains_bytes(&buffer, &utf16z("IPM.Appointment")));
}

#[test]
fn content_sync_manifest_respects_normal_and_fai_scope_flags() {
    let normal_canonical_id = Uuid::parse_str("99999999-9999-9999-9999-999999999997").unwrap();
    let associated_canonical_id = Uuid::parse_str("99999999-9999-9999-9999-999999999996").unwrap();
    let normal_item_id = crate::mapi::identity::mapi_store_id(97);
    let associated_item_id = crate::mapi::identity::mapi_store_id(96);
    crate::mapi::identity::remember_mapi_identity(normal_canonical_id, normal_item_id);
    crate::mapi::identity::remember_mapi_identity(associated_canonical_id, associated_item_id);
    let normal_object = SpecialMessageSyncFact {
        folder_id: crate::mapi::identity::CALENDAR_FOLDER_ID,
        item_id: normal_item_id,
        canonical_id: normal_canonical_id,
        associated: false,
        subject: "Normal appointment".to_string(),
        body_text: String::new(),
        message_class: "IPM.Appointment".to_string(),
        last_modified_filetime: filetime_from_rfc3339_utc("2026-05-19T10:00:00Z"),
        message_size: 19,
        read_state: None,
        named_properties: Vec::new(),
    };
    let associated_object = SpecialMessageSyncFact {
        folder_id: crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
        item_id: associated_item_id,
        canonical_id: associated_canonical_id,
        associated: true,
        subject: "Associated view".to_string(),
        body_text: String::new(),
        message_class: "IPM.Microsoft.WunderBar.Link".to_string(),
        last_modified_filetime: filetime_from_rfc3339_utc("2026-05-19T10:00:00Z"),
        message_size: 19,
        read_state: None,
        named_properties: Vec::new(),
    };
    let email = test_email();
    crate::mapi::identity::remember_mapi_identity(
        email.id,
        crate::mapi::identity::mapi_store_id(95),
    );
    let default_mixed_buffer = sync_manifest_buffer_with_special_objects_and_final_state(
        Uuid::nil(),
        SYNC_TYPE_CONTENTS,
        0,
        SYNC_EXTRA_FLAG_EID,
        &[],
        crate::mapi::identity::CALENDAR_FOLDER_ID,
        &[],
        std::slice::from_ref(&email),
        &[],
        &[normal_object.clone(), associated_object.clone()],
        &[],
        &[],
        &[],
        std::slice::from_ref(&email),
        &[],
        &[normal_object.clone(), associated_object.clone()],
        &[],
        &[],
        1,
    );
    assert!(contains_bytes(&default_mixed_buffer, &utf16z("Hello")));
    assert!(contains_bytes(
        &default_mixed_buffer,
        &utf16z("Normal appointment")
    ));
    assert!(!contains_bytes(
        &default_mixed_buffer,
        &utf16z("Associated view")
    ));

    let default_fai_only_buffer = sync_manifest_buffer_with_special_objects_and_final_state(
        Uuid::nil(),
        SYNC_TYPE_CONTENTS,
        0,
        SYNC_EXTRA_FLAG_EID,
        &[],
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
        &[],
        &[],
        &[],
        std::slice::from_ref(&associated_object),
        &[],
        &[],
        &[],
        &[],
        &[],
        std::slice::from_ref(&associated_object),
        &[],
        &[],
        1,
    );
    assert!(contains_bytes(
        &default_fai_only_buffer,
        &utf16z("Associated view")
    ));

    let buffer = sync_manifest_buffer_with_special_objects_and_final_state(
        Uuid::nil(),
        SYNC_TYPE_CONTENTS,
        SYNC_FLAG_FAI,
        SYNC_EXTRA_FLAG_EID,
        &[],
        crate::mapi::identity::CALENDAR_FOLDER_ID,
        &[],
        &[email],
        &[],
        &[normal_object.clone(), associated_object.clone()],
        &[],
        &[],
        &[],
        &[],
        &[],
        &[normal_object, associated_object],
        &[],
        &[],
        1,
    );

    assert!(!contains_bytes(&buffer, &utf16z("Hello")));
    assert!(!contains_bytes(&buffer, &utf16z("Normal appointment")));
    assert!(!contains_bytes(&buffer, &wire_id_bytes(normal_item_id)));
    assert!(contains_bytes(&buffer, &utf16z("Associated view")));
    assert!(contains_bytes(&buffer, &wire_id_bytes(associated_item_id)));
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    needle.is_empty()
        || haystack
            .windows(needle.len())
            .any(|window| window == needle)
}

#[test]
fn hierarchy_sync_omits_content_activity_count_properties() {
    let mailbox_id = Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        mailbox_id,
        crate::mapi::identity::mapi_store_id(5),
    );
    let mailbox = JmapMailbox {
        id: mailbox_id,
        parent_id: None,
        role: "inbox".to_string(),
        name: "Inbox".to_string(),
        sort_order: 40,
        modseq: 42,
        total_emails: 0,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    };
    let email = test_email();
    let buffer = sync_manifest_buffer_with_final_state(
        Uuid::nil(),
        0x02,
        0x0100,
        0,
        &[
            PID_TAG_FOLDER_TYPE,
            PID_TAG_CONTENT_COUNT,
            PID_TAG_CONTENT_UNREAD_COUNT,
            PID_TAG_ACCESS,
        ],
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
        std::slice::from_ref(&mailbox),
        &[],
        &[],
        &[],
        std::slice::from_ref(&mailbox),
        std::slice::from_ref(&mailbox),
        &[],
        &[],
        std::slice::from_ref(&email),
        &[],
        1,
    );

    let summary =
        decode_hierarchy_transfer_debug_summary(&buffer).expect("hierarchy transfer debug");
    let row = summary.rows.first().expect("folder row");
    assert_eq!(row.content_count, None);
    assert_eq!(row.content_unread_count, None);
}

#[test]
fn hierarchy_sync_excluded_properties_are_not_reintroduced_as_stable_folder_facts() {
    let mailbox_id = Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        mailbox_id,
        crate::mapi::identity::mapi_store_id(5),
    );
    let mailbox = JmapMailbox {
        id: mailbox_id,
        parent_id: None,
        role: "inbox".to_string(),
        name: "Inbox".to_string(),
        sort_order: 40,
        modseq: 42,
        total_emails: 0,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    };
    let email = test_email();
    let buffer = sync_manifest_buffer_with_final_state(
        Uuid::nil(),
        0x02,
        0x0100,
        0,
        &[
            PID_TAG_FOLDER_TYPE,
            PID_TAG_CONTENT_COUNT,
            PID_TAG_CONTENT_UNREAD_COUNT,
            PID_TAG_ACCESS,
        ],
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
        std::slice::from_ref(&mailbox),
        &[],
        &[],
        &[],
        std::slice::from_ref(&mailbox),
        std::slice::from_ref(&mailbox),
        &[],
        &[],
        std::slice::from_ref(&email),
        &[],
        1,
    );

    let summary =
        decode_hierarchy_transfer_debug_summary(&buffer).expect("hierarchy transfer debug");
    let row = summary.rows.first().expect("folder row");
    assert_eq!(row.content_count, None);
    assert_eq!(row.content_unread_count, None);
    assert_eq!(row.folder_type, None);
    assert_eq!(row.access, None);
    assert_eq!(row.subfolders, Some(false));
}

#[test]
fn final_sync_state_separates_object_idset_from_change_cnset() {
    let token = final_sync_state_stream(
        0x02,
        &[
            crate::mapi::identity::mapi_store_id(5),
            crate::mapi::identity::mapi_store_id(7),
            crate::mapi::identity::mapi_store_id(8),
        ],
        &[10, 12],
    );
    let mut expected_idset = STORE_REPLICA_GUID.to_vec();
    expected_idset.push(GLOBSET_RANGE_COMMAND);
    expected_idset.extend_from_slice(&globcnt_bytes(5));
    expected_idset.extend_from_slice(&globcnt_bytes(5));
    expected_idset.push(GLOBSET_RANGE_COMMAND);
    expected_idset.extend_from_slice(&globcnt_bytes(7));
    expected_idset.extend_from_slice(&globcnt_bytes(8));
    expected_idset.push(GLOBSET_END_COMMAND);
    let mut expected_cnset = STORE_REPLICA_GUID.to_vec();
    expected_cnset.push(GLOBSET_RANGE_COMMAND);
    expected_cnset.extend_from_slice(&globcnt_bytes(10));
    expected_cnset.extend_from_slice(&globcnt_bytes(10));
    expected_cnset.push(GLOBSET_RANGE_COMMAND);
    expected_cnset.extend_from_slice(&globcnt_bytes(12));
    expected_cnset.extend_from_slice(&globcnt_bytes(12));
    expected_cnset.push(GLOBSET_END_COMMAND);

    assert_variable_property(&token, META_TAG_IDSET_GIVEN, &expected_idset);
    assert_variable_property(&token, META_TAG_CNSET_SEEN, &expected_cnset);
}

#[test]
fn replguid_globset_parser_decodes_push_singleton_client_state() {
    let mut globset = STORE_REPLICA_GUID.to_vec();
    globset.push(6);
    globset.extend_from_slice(&globcnt_bytes(0xbad397870262));
    globset.push(GLOBSET_END_COMMAND);

    assert_eq!(
        replguid_globset_counters(&globset).unwrap(),
        vec![0xbad397870262]
    );
    let summary = replguid_globset_debug_summary(&globset);
    assert!(summary.contains("range_count=1"));
    assert!(summary.contains("ranges=205417943073378"));
    assert!(summary.contains("parse_error="));
    assert!(!summary.contains("unsupported_command"));
}

#[test]
fn replguid_globset_parser_decodes_common_stack_range_and_bitmask() {
    let mut range_globset = STORE_REPLICA_GUID.to_vec();
    range_globset.push(5);
    range_globset.extend_from_slice(&[0, 0, 0, 0, 0]);
    range_globset.push(GLOBSET_RANGE_COMMAND);
    range_globset.push(7);
    range_globset.push(9);
    range_globset.push(GLOBSET_POP_COMMAND);
    range_globset.push(GLOBSET_END_COMMAND);
    assert_eq!(
        replguid_globset_counters(&range_globset).unwrap(),
        vec![7, 8, 9]
    );

    let mut bitmask_globset = STORE_REPLICA_GUID.to_vec();
    bitmask_globset.push(5);
    bitmask_globset.extend_from_slice(&[0, 0, 0, 0, 0]);
    bitmask_globset.push(GLOBSET_BITMASK_COMMAND);
    bitmask_globset.push(1);
    bitmask_globset.push(0b0000_1011);
    bitmask_globset.push(GLOBSET_POP_COMMAND);
    bitmask_globset.push(GLOBSET_END_COMMAND);
    assert_eq!(
        replguid_globset_counters(&bitmask_globset).unwrap(),
        vec![1, 2, 3, 5]
    );
}

#[test]
fn hierarchy_and_content_cnsets_replay_in_globcnt_order() {
    let hierarchy = final_sync_state_stream(
        0x02,
        &[crate::mapi::identity::mapi_store_id(7)],
        &[12, 10, 11],
    );
    let content = final_sync_state_stream(
        0x01,
        &[crate::mapi::identity::mapi_store_id(50)],
        &[22, 20, 21],
    );
    let expected_hierarchy_cnset = replguid_idset_from_counters(&[10, 11, 12]);
    let expected_content_cnset = replguid_idset_from_counters(&[20, 21, 22]);
    let empty_cnset = replguid_idset_from_counters(&[]);

    assert_variable_property(&hierarchy, META_TAG_CNSET_SEEN, &expected_hierarchy_cnset);
    assert_variable_property(&content, META_TAG_CNSET_SEEN, &expected_content_cnset);
    assert_variable_property(&content, META_TAG_CNSET_SEEN_FAI, &empty_cnset);
    assert_variable_property(&content, META_TAG_CNSET_READ, &expected_content_cnset);
}

#[test]
fn content_sync_state_keeps_normal_and_fai_cnsets_separate() {
    let token = final_content_sync_state_stream(
        &[
            crate::mapi::identity::mapi_store_id(50),
            crate::mapi::identity::mapi_store_id(70),
        ],
        &[20],
        &[30],
        &[20],
    );

    assert_variable_property(
        &token,
        META_TAG_CNSET_SEEN,
        &replguid_idset_from_counters(&[20]),
    );
    assert_variable_property(
        &token,
        META_TAG_CNSET_SEEN_FAI,
        &replguid_idset_from_counters(&[30]),
    );
    assert_variable_property(
        &token,
        META_TAG_CNSET_READ,
        &replguid_idset_from_counters(&[20]),
    );
}

#[test]
fn deleted_idset_uses_replid_globset_ranges() {
    let idset = replid_idset_from_object_ids(&[
        crate::mapi::identity::mapi_store_id(3),
        crate::mapi::identity::mapi_store_id(4),
        crate::mapi::identity::mapi_store_id(8),
    ]);

    let mut expected = (crate::mapi::identity::STORE_REPLICA_ID as u16)
        .to_le_bytes()
        .to_vec();
    expected.push(GLOBSET_RANGE_COMMAND);
    expected.extend_from_slice(&globcnt_bytes(3));
    expected.extend_from_slice(&globcnt_bytes(4));
    expected.push(GLOBSET_RANGE_COMMAND);
    expected.extend_from_slice(&globcnt_bytes(8));
    expected.extend_from_slice(&globcnt_bytes(8));
    expected.push(GLOBSET_END_COMMAND);

    assert_eq!(idset, expected);
}

fn assert_variable_property(buffer: &[u8], property_tag: u32, value: &[u8]) {
    let tag = property_tag.to_le_bytes();
    let offset = buffer
        .windows(tag.len())
        .position(|window| window == tag)
        .expect("property tag is present");
    let length = u32::from_le_bytes(buffer[offset + 4..offset + 8].try_into().unwrap());
    assert_eq!(length as usize, value.len());
    assert_eq!(&buffer[offset + 8..offset + 8 + value.len()], value);
}

fn assert_variable_property_present(buffer: &[u8], property_tag: u32, value: &[u8]) {
    let mut expected = property_tag.to_le_bytes().to_vec();
    expected.extend_from_slice(&(value.len() as u32).to_le_bytes());
    expected.extend_from_slice(value);
    assert!(buffer
        .windows(expected.len())
        .any(|window| window == expected));
}

fn assert_i32_property(buffer: &[u8], property_tag: u32, value: i32) {
    let tag = property_tag.to_le_bytes();
    let offset = buffer
        .windows(tag.len())
        .position(|window| window == tag)
        .expect("property tag is present");
    assert_eq!(
        i32::from_le_bytes(buffer[offset + 4..offset + 8].try_into().unwrap()),
        value
    );
}

fn assert_absent_property(buffer: &[u8], property_tag: u32) {
    let tag = property_tag.to_le_bytes();
    assert!(!buffer.windows(tag.len()).any(|window| window == tag));
}

fn assert_bool_property(buffer: &[u8], property_tag: u32, value: bool) {
    let tag = property_tag.to_le_bytes();
    let offset = buffer
        .windows(tag.len())
        .position(|window| window == tag)
        .expect("property tag is present");
    let expected = if value { [1, 0] } else { [0, 0] };
    assert_eq!(&buffer[offset + 4..offset + 6], &expected);
}

fn assert_change_number_property(buffer: &[u8], property_tag: u32, change_number: u64) {
    let tag = property_tag.to_le_bytes();
    let offset = buffer
        .windows(tag.len())
        .position(|window| window == tag)
        .expect("property tag is present");
    let value = crate::mapi::identity::object_id_from_wire_id(&buffer[offset + 4..offset + 12])
        .and_then(crate::mapi::identity::global_counter_from_store_id)
        .expect("change number is encoded as an internal CN structure");
    assert_eq!(value, change_number);
}

fn assert_tag_order(buffer: &[u8], tags: &[u32]) {
    let mut previous = None;
    for tag in tags {
        let tag_bytes = tag.to_le_bytes();
        let offset = buffer
            .windows(tag_bytes.len())
            .position(|window| window == tag_bytes)
            .expect("tag is present");
        if let Some(previous) = previous {
            assert!(previous < offset);
        }
        previous = Some(offset);
    }
}

fn assert_tag_sequence(buffer: &[u8], tags: &[u32]) {
    let mut search_offset = 0;
    for tag in tags {
        let tag_bytes = tag.to_le_bytes();
        let relative_offset = buffer[search_offset..]
            .windows(tag_bytes.len())
            .position(|window| window == tag_bytes)
            .expect("tag is present after previous tag");
        search_offset += relative_offset + tag_bytes.len();
    }
}

fn utf16z(value: &str) -> Vec<u8> {
    value
        .encode_utf16()
        .flat_map(u16::to_le_bytes)
        .chain([0, 0])
        .collect()
}

fn test_email() -> JmapEmail {
    JmapEmail {
        id: Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap(),
        thread_id: Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap(),
        mailbox_id: Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap(),
        mailbox_role: "inbox".to_string(),
        mailbox_name: "Inbox".to_string(),
        modseq: 42,
        mailbox_ids: vec![Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap()],
        mailbox_states: vec![JmapEmailMailboxState {
            mailbox_id: Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap(),
            role: "inbox".to_string(),
            name: "Inbox".to_string(),
            modseq: 42,
            unread: true,
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
        received_at: "2026-05-06T12:00:00Z".to_string(),
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
        body_text: "Hello body".to_string(),
        body_html_sanitized: None,
        unread: true,
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
        internet_message_id: Some("<message@example.test>".to_string()),
        mime_blob_ref: None,
        delivery_status: "stored".to_string(),
    }
}
