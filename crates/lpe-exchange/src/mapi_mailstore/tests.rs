use super::*;
use lpe_storage::{JmapEmailAddress, JmapEmailMailboxState};

#[test]
fn property_filters_match_ptyp_unspecified_by_property_id() {
    let unspecified_body = PID_TAG_BODY_W & 0xFFFF_0000;

    assert!(property_tag_matches(unspecified_body, PID_TAG_BODY_W));
    assert!(!property_tag_matches(unspecified_body, PID_TAG_SUBJECT_W));
    assert!(content_property_in_scope(
        SYNC_TYPE_CONTENTS,
        SYNC_FLAG_NORMAL | 0x0080,
        &[unspecified_body],
        PID_TAG_BODY_W,
    ));
    assert!(!content_property_in_scope(
        SYNC_TYPE_CONTENTS,
        SYNC_FLAG_NORMAL,
        &[unspecified_body],
        PID_TAG_BODY_W,
    ));
}

#[test]
fn content_sync_child_collections_follow_property_filters_independently() {
    assert_eq!(
        content_sync_message_children(SYNC_TYPE_CONTENTS, SYNC_FLAG_NORMAL, &[]),
        FastTransferMessageChildren::all(),
    );
    assert_eq!(
        content_sync_message_children(
            SYNC_TYPE_CONTENTS,
            SYNC_FLAG_NORMAL,
            &[PID_TAG_MESSAGE_RECIPIENTS],
        ),
        FastTransferMessageChildren::new(false, true),
    );
    assert_eq!(
        content_sync_message_children(
            SYNC_TYPE_CONTENTS,
            SYNC_FLAG_NORMAL | 0x0080,
            &[PID_TAG_MESSAGE_RECIPIENTS],
        ),
        FastTransferMessageChildren::new(true, false),
    );
}

fn wire_id_bytes(object_id: u64) -> [u8; 8] {
    crate::mapi::identity::wire_id_bytes_from_object_id(object_id).unwrap()
}

#[test]
fn rfc3339_filetime_accepts_postgresql_microseconds_and_preserves_100ns_ticks() {
    let whole_second = filetime_from_rfc3339_utc("2026-07-17T10:00:00Z");
    assert_ne!(whole_second, 0);
    assert_eq!(
        filetime_from_rfc3339_utc("2026-07-17T10:00:00.000000Z"),
        whole_second
    );
    assert_eq!(
        filetime_from_rfc3339_utc("2026-07-17T10:00:00.123456Z"),
        whole_second + 1_234_560
    );
    assert_eq!(
        filetime_from_rfc3339_utc("2026-07-17T10:00:00.1234567Z"),
        whole_second + 1_234_567
    );
    assert_eq!(
        filetime_from_rfc3339_utc("2026-07-17T10:00:00.12345678Z"),
        whole_second + 1_234_567
    );
    assert_eq!(
        filetime_from_rfc3339_utc("2026-07-17T10:00:00.123456789Z"),
        whole_second + 1_234_567
    );
    assert_eq!(filetime_from_rfc3339_utc("2026-02-30T10:00:00Z"), 0);
    assert_eq!(filetime_from_rfc3339_utc("2026-07-17T10:00:00Zjunk"), 0);
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
fn hierarchy_state_keeps_deleted_items_fid_distinct_from_its_server_cn() {
    let mut deleted_items = virtual_special_mailbox(crate::mapi::identity::TRASH_FOLDER_ID)
        .expect("virtual Deleted Items folder");
    // Fresh 0.5.0 run 202607181515 persisted the reserved Deleted Items FID
    // at counter 8 and its independently allocated server CN at counter 47.
    deleted_items.modseq = 47;

    let buffer = sync_manifest_buffer_with_attachments(
        SYNC_TYPE_HIERARCHY,
        0x0100,
        0,
        &[],
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
        &[deleted_items],
        &[],
        &[],
        &[],
        1,
    );
    let summary = decode_hierarchy_transfer_debug_summary(&buffer).unwrap();

    // [MS-OXCFXICS] sections 2.2.1.1.1 and 2.2.1.1.2 carry object IDs
    // and change numbers in separate state properties.
    assert_eq!(summary.final_state_idset_given_counters, vec![8]);
    assert_eq!(summary.final_state_cnset_seen_counters, vec![47]);
}

#[test]
fn hierarchy_download_keeps_imported_change_key_and_predecessor_lineage() {
    let mut deleted_items = virtual_special_mailbox(crate::mapi::identity::TRASH_FOLDER_ID)
        .expect("virtual Deleted Items folder");
    // The JMAP mailbox modseq is not the MAPI hierarchy CN once the folder
    // has a durable imported version.
    deleted_items.modseq = 47;
    let imported_change_key = vec![
        0x51, 0xa1, 0x66, 0x72, 0x14, 0x93, 0x5c, 0x48, 0xaa, 0x14, 0xe7, 0xdc, 0xb0, 0x5e, 0x0d,
        0xa6, 0x00, 0x00, 0x04, 0x15,
    ];
    let mut imported_predecessor_change_list = vec![imported_change_key.len() as u8];
    imported_predecessor_change_list.extend_from_slice(&imported_change_key);
    imported_predecessor_change_list.extend_from_slice(&predecessor_change_list(47));
    let version = crate::mapi_store::MapiFolderVersion {
        folder_id: crate::mapi::identity::TRASH_FOLDER_ID,
        change_number: 58,
        change_key: imported_change_key.clone(),
        predecessor_change_list: imported_predecessor_change_list.clone(),
        last_modification_time: filetime_from_change_number(58),
    };
    let mailboxes = [deleted_items];

    let buffer = sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions(
        Uuid::nil(),
        SYNC_TYPE_HIERARCHY,
        0x0100,
        0,
        &[],
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
        &mailboxes,
        &[],
        &[],
        &[],
        &[],
        &mailboxes,
        &mailboxes,
        &[],
        &[],
        &[],
        &[],
        &[],
        std::slice::from_ref(&version),
        1,
    );
    let binary_property = |property_tag: u32| {
        let tag = property_tag.to_le_bytes();
        let offset = buffer
            .windows(tag.len())
            .position(|window| window == tag)
            .unwrap();
        let length = u32::from_le_bytes(buffer[offset + 4..offset + 8].try_into().unwrap());
        buffer[offset + 8..offset + 8 + length as usize].to_vec()
    };
    let summary = decode_hierarchy_transfer_debug_summary(&buffer).unwrap();

    assert_eq!(binary_property(PID_TAG_CHANGE_KEY), imported_change_key);
    assert_eq!(
        binary_property(PID_TAG_PREDECESSOR_CHANGE_LIST),
        imported_predecessor_change_list
    );
    assert_eq!(summary.final_state_idset_given_counters, vec![8]);
    assert_eq!(summary.final_state_cnset_seen_counters, vec![58]);
    // [MS-OXCFXICS] sections 2.2.1.1.2, 2.2.1.2.7, and 3.1.5.3:
    // imported foreign ChangeKeys remain foreign XIDs, while CnsetSeen
    // records the server's locally assigned change number.
    assert!(summary.final_state_cnset_seen_includes_all_expected_folder_change_counters);

    let (selected, _) = select_download_manifest_for_client_state(
        SYNC_TYPE_HIERARCHY,
        0x0100,
        &buffer,
        &initial_sync_state_stream(SYNC_TYPE_HIERARCHY),
        &[DownloadChangeFact {
            object_id: version.folder_id,
            change_number: version.change_number,
            associated: false,
            source_key: source_key_for_store_id(version.folder_id),
        }],
        &[],
    )
    .expect("imported ChangeKey must not replace the server CN used for selection");
    let selected_summary = decode_hierarchy_transfer_debug_summary(&selected).unwrap();
    assert_eq!(selected_summary.folder_change_count, 1);
    assert_eq!(selected_summary.final_state_cnset_seen_counters, vec![58]);
}

#[test]
fn hierarchy_change_numbers_use_distinct_persisted_folder_versions() {
    let drafts = JmapMailbox {
        id: Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap(),
        parent_id: None,
        role: "drafts".to_string(),
        name: "Drafts".to_string(),
        sort_order: 30,
        modseq: 45,
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
        modseq: 47,
        total_emails: 0,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    };

    assert_eq!(
        canonical_hierarchy_change_number(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID, &drafts),
        45
    );
    assert_eq!(
        canonical_hierarchy_change_number(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID, &trash),
        47
    );
}

#[test]
fn hierarchy_change_number_uses_projected_mailbox_modseq() {
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
        51
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
    let mut email = test_email();
    email.modseq = 7;
    email.mailbox_states[0].modseq = 7;
    let durable_object_id = crate::mapi::identity::mapi_store_id(65_668);
    let durable_change_number = 65_669;
    let durable_source_key = source_key_for_store_id(durable_object_id);
    let durable_change_key = change_key_for_change_number(durable_change_number);
    let durable_predecessor_change_list = predecessor_change_list(durable_change_number);
    let durable_last_modification_time = 133_983_180_000_000_000;
    let durable_fact = NormalMessageSyncFact {
        canonical_id: email.id,
        object_id: durable_object_id,
        source_key: durable_source_key.clone(),
        change_number: durable_change_number,
        change_key: durable_change_key.clone(),
        predecessor_change_list: durable_predecessor_change_list.clone(),
        last_modification_time: durable_last_modification_time,
    };
    let buffer = sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts(
        Uuid::nil(),
        SYNC_TYPE_CONTENTS,
        SYNC_FLAG_NORMAL,
        SYNC_EXTRA_FLAG_EID | SYNC_EXTRA_FLAG_MESSAGE_SIZE | SYNC_EXTRA_FLAG_CHANGE_NUMBER,
        &[],
        &[],
        crate::mapi::identity::INBOX_FOLDER_ID,
        &[],
        std::slice::from_ref(&email),
        &[],
        std::slice::from_ref(&durable_fact),
        &[],
        &[],
        &[],
        &[],
        std::slice::from_ref(&email),
        &[],
        std::slice::from_ref(&durable_fact),
        &[],
        std::slice::from_ref(&email),
        &[],
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
    assert_change_number_property(&buffer, PID_TAG_CHANGE_NUMBER, durable_change_number);
    let mid_offset = buffer
        .windows(4)
        .position(|window| window == PID_TAG_MID.to_le_bytes())
        .expect("MID is present");
    assert_eq!(
        &buffer[mid_offset + 4..mid_offset + 12],
        &crate::mapi::identity::wire_id_bytes_from_object_id(durable_object_id).unwrap(),
    );
    assert_i64_property(
        &buffer,
        PID_TAG_LAST_MODIFICATION_TIME,
        durable_last_modification_time as i64,
    );
    assert_variable_property(&buffer, PID_TAG_SOURCE_KEY, &durable_source_key);
    assert_variable_property(&buffer, PID_TAG_CHANGE_KEY, &durable_change_key);
    assert_variable_property(
        &buffer,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        &durable_predecessor_change_list,
    );
    assert_variable_property(
        &buffer,
        META_TAG_CNSET_SEEN,
        &replguid_idset_from_counters(&[durable_change_number]),
    );
    let change_facts = download_change_facts_with_normal_message_sync_facts(
        SYNC_TYPE_CONTENTS,
        SYNC_FLAG_NORMAL,
        crate::mapi::identity::INBOX_FOLDER_ID,
        &[],
        std::slice::from_ref(&email),
        &[],
        std::slice::from_ref(&durable_fact),
        &[],
        &[],
    );
    assert_eq!(change_facts.len(), 1);
    assert_eq!(change_facts[0].object_id, durable_object_id);
    assert_eq!(change_facts[0].change_number, durable_change_number);
    assert_eq!(change_facts[0].source_key, durable_source_key);

    // [MS-OXCFXICS] sections 3.2.5.1 and 3.2.5.3: the final state advances
    // exactly by the differences emitted in this download.
    let (selected, final_state) = select_download_manifest_for_client_state(
        SYNC_TYPE_CONTENTS,
        SYNC_FLAG_NORMAL,
        &buffer,
        &initial_sync_state_stream(SYNC_TYPE_CONTENTS),
        &change_facts,
        &[],
    )
    .expect("select the unseen durable message version");
    assert_change_number_property(&selected, PID_TAG_CHANGE_NUMBER, durable_change_number);
    assert_variable_property(
        &final_state,
        META_TAG_CNSET_SEEN,
        &replguid_idset_from_counters(&[durable_change_number]),
    );
    assert_tag_sequence(
        &final_state,
        &[
            INCR_SYNC_STATE_BEGIN,
            META_TAG_CNSET_SEEN,
            META_TAG_CNSET_SEEN_FAI,
            META_TAG_IDSET_GIVEN,
            META_TAG_CNSET_READ,
            INCR_SYNC_STATE_END,
        ],
    );

    let (selected_again, final_state_again) = select_download_manifest_for_client_state(
        SYNC_TYPE_CONTENTS,
        SYNC_FLAG_NORMAL,
        &buffer,
        &final_state,
        &change_facts,
        &[],
    )
    .expect("recognize the durable message version from the returned state");
    let mut state_only = final_state.clone();
    state_only.extend_from_slice(&INCR_SYNC_END.to_le_bytes());
    assert_eq!(selected_again, state_only);
    assert_eq!(final_state_again, final_state);
}

#[test]
fn content_download_selection_emits_unseen_durable_inbox_change_after_completed_sync() {
    let mut previously_seen = test_email();
    previously_seen.id = Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaa111").unwrap();
    previously_seen.thread_id = Uuid::parse_str("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbb111").unwrap();
    previously_seen.subject = "Previously synchronized".to_string();
    previously_seen.received_at = "2026-08-03T19:00:00Z".to_string();

    let mut arrived_after_sync = test_email();
    arrived_after_sync.id = Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaa112").unwrap();
    arrived_after_sync.thread_id = Uuid::parse_str("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbb112").unwrap();
    arrived_after_sync.subject = "Arrived after completed sync".to_string();
    arrived_after_sync.received_at = "2026-08-03T21:31:00Z".to_string();

    let previously_seen_fact = NormalMessageSyncFact {
        canonical_id: previously_seen.id,
        object_id: crate::mapi::identity::mapi_store_id(0x4001),
        source_key: source_key_for_store_id(crate::mapi::identity::mapi_store_id(0x4001)),
        change_number: 70_001,
        change_key: change_key_for_change_number(70_001),
        predecessor_change_list: predecessor_change_list(70_001),
        last_modification_time: filetime_from_change_number(70_001),
    };
    let arrived_after_sync_fact = NormalMessageSyncFact {
        canonical_id: arrived_after_sync.id,
        object_id: crate::mapi::identity::mapi_store_id(0x4002),
        source_key: source_key_for_store_id(crate::mapi::identity::mapi_store_id(0x4002)),
        change_number: 70_002,
        change_key: change_key_for_change_number(70_002),
        predecessor_change_list: predecessor_change_list(70_002),
        last_modification_time: filetime_from_change_number(70_002),
    };
    let manifest_for = |emails: &[JmapEmail], facts: &[NormalMessageSyncFact]| {
        sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts(
            Uuid::nil(),
            SYNC_TYPE_CONTENTS,
            SYNC_FLAG_NORMAL,
            SYNC_EXTRA_FLAG_CHANGE_NUMBER,
            &[],
            &[],
            crate::mapi::identity::INBOX_FOLDER_ID,
            &[],
            emails,
            &[],
            facts,
            &[],
            &[],
            &[],
            &[],
            emails,
            &[],
            facts,
            &[],
            emails,
            &[],
            &[],
            &[],
            1,
        )
    };

    let previously_seen_manifest = manifest_for(
        std::slice::from_ref(&previously_seen),
        std::slice::from_ref(&previously_seen_fact),
    );
    let previously_seen_facts = download_change_facts_with_normal_message_sync_facts(
        SYNC_TYPE_CONTENTS,
        SYNC_FLAG_NORMAL,
        crate::mapi::identity::INBOX_FOLDER_ID,
        &[],
        std::slice::from_ref(&previously_seen),
        &[],
        std::slice::from_ref(&previously_seen_fact),
        &[],
        &[],
    );
    let (_, completed_state) = select_download_manifest_for_client_state(
        SYNC_TYPE_CONTENTS,
        SYNC_FLAG_NORMAL,
        &previously_seen_manifest,
        &initial_sync_state_stream(SYNC_TYPE_CONTENTS),
        &previously_seen_facts,
        &[],
    )
    .expect("complete the initial Inbox download");

    let emails = [previously_seen, arrived_after_sync];
    let facts = [previously_seen_fact, arrived_after_sync_fact.clone()];
    let full_manifest = manifest_for(&emails, &facts);
    let full_facts = download_change_facts_with_normal_message_sync_facts(
        SYNC_TYPE_CONTENTS,
        SYNC_FLAG_NORMAL,
        crate::mapi::identity::INBOX_FOLDER_ID,
        &[],
        &emails,
        &[],
        &facts,
        &[],
        &[],
    );

    // [MS-OXCFXICS] section 3.2.5.3: only a normal message whose server CN
    // is absent from the uploaded CnsetSeen is downloaded, and the final
    // CnsetSeen is the union of that uploaded state and emitted changes.
    let (selected, final_state) = select_download_manifest_for_client_state(
        SYNC_TYPE_CONTENTS,
        SYNC_FLAG_NORMAL,
        &full_manifest,
        &completed_state,
        &full_facts,
        &[],
    )
    .expect("select the Inbox change absent from the completed state");

    assert!(contains_bytes(
        &selected,
        &utf16z("Arrived after completed sync")
    ));
    assert!(!contains_bytes(
        &selected,
        &utf16z("Previously synchronized")
    ));
    assert_change_number_property(&selected, PID_TAG_CHANGE_NUMBER, 70_002);
    assert_variable_property(
        &final_state,
        META_TAG_CNSET_SEEN,
        &replguid_idset_from_counters(&[70_001, 70_002]),
    );
}

#[test]
fn microsoft_oxcfxics_content_sync_emits_sender_and_delivery_identity_properties() {
    let mut email = test_email();
    crate::mapi::identity::remember_mapi_identity(
        email.id,
        crate::mapi::identity::mapi_store_id(50),
    );
    email.sender_display = Some("Relay Sender".to_string());
    email.sender_address = Some("relay@example.test".to_string());
    let expected_delivery_time = filetime_from_rfc3339_utc(&email.received_at);
    let buffer = sync_manifest_buffer_with_attachments(
        SYNC_TYPE_CONTENTS,
        SYNC_FLAG_NORMAL | SYNC_FLAG_UNICODE,
        0,
        &[],
        crate::mapi::identity::INBOX_FOLDER_ID,
        &[],
        std::slice::from_ref(&email),
        &[],
        &[],
        1,
    );

    assert_i64_property(
        &buffer,
        PID_TAG_MESSAGE_DELIVERY_TIME,
        expected_delivery_time as i64,
    );
    assert_variable_property(&buffer, PID_TAG_SENDER_NAME_W, &utf16z("Relay Sender"));
    assert_variable_property(&buffer, PID_TAG_SENDER_ADDRESS_TYPE_W, &utf16z("SMTP"));
    assert_variable_property(
        &buffer,
        PID_TAG_SENDER_EMAIL_ADDRESS_W,
        &utf16z("relay@example.test"),
    );
    assert_variable_property(
        &buffer,
        PID_TAG_SENDER_SMTP_ADDRESS_W,
        &utf16z("relay@example.test"),
    );
    assert_variable_property(
        &buffer,
        PID_TAG_SENDER_ENTRY_ID,
        &crate::mapi::properties::sender_entry_id(&email),
    );
    assert_variable_property(
        &buffer,
        PID_TAG_SENDER_SEARCH_KEY,
        &crate::mapi::properties::smtp_search_key("relay@example.test"),
    );
    assert_variable_property(&buffer, PID_TAG_SENT_REPRESENTING_NAME_W, &utf16z("Alice"));
    assert_variable_property(
        &buffer,
        PID_TAG_SENT_REPRESENTING_ADDRESS_TYPE_W,
        &utf16z("SMTP"),
    );
    assert_variable_property(
        &buffer,
        PID_TAG_SENT_REPRESENTING_EMAIL_ADDRESS_W,
        &utf16z("alice@example.test"),
    );
    assert_variable_property(
        &buffer,
        PID_TAG_SENT_REPRESENTING_SMTP_ADDRESS_W,
        &utf16z("alice@example.test"),
    );
    assert_variable_property(
        &buffer,
        PID_TAG_SENT_REPRESENTING_ENTRY_ID,
        &crate::mapi::properties::sent_representing_entry_id(&email),
    );
    assert_variable_property(
        &buffer,
        PID_TAG_SENT_REPRESENTING_SEARCH_KEY,
        &crate::mapi::properties::smtp_search_key("alice@example.test"),
    );
    assert_variable_property(&buffer, PID_TAG_MESSAGE_CLASS_W, &utf16z("IPM.Note"));
    assert_variable_property(
        &buffer,
        META_TAG_CNSET_READ,
        &replguid_idset_from_counters(&[]),
    );

    let excluded_property_tags = [
        PID_TAG_MESSAGE_DELIVERY_TIME,
        PID_TAG_SENDER_NAME_W,
        PID_TAG_SENDER_ADDRESS_TYPE_W,
        PID_TAG_SENDER_EMAIL_ADDRESS_W,
        PID_TAG_SENDER_SMTP_ADDRESS_W,
        PID_TAG_SENDER_ENTRY_ID,
        PID_TAG_SENDER_SEARCH_KEY,
        PID_TAG_SENT_REPRESENTING_NAME_W,
        PID_TAG_SENT_REPRESENTING_ADDRESS_TYPE_W,
        PID_TAG_SENT_REPRESENTING_EMAIL_ADDRESS_W,
        PID_TAG_SENT_REPRESENTING_SMTP_ADDRESS_W,
        PID_TAG_SENT_REPRESENTING_ENTRY_ID,
        PID_TAG_SENT_REPRESENTING_SEARCH_KEY,
        PID_TAG_MESSAGE_CLASS_W,
    ];
    let excluded = sync_manifest_buffer_with_attachments(
        SYNC_TYPE_CONTENTS,
        SYNC_FLAG_NORMAL | SYNC_FLAG_UNICODE,
        0,
        &excluded_property_tags,
        crate::mapi::identity::INBOX_FOLDER_ID,
        &[],
        std::slice::from_ref(&email),
        &[],
        &[],
        1,
    );
    for property_tag in excluded_property_tags {
        assert_absent_property(&excluded, property_tag);
    }
}

#[test]
fn microsoft_oxcfxics_order_by_delivery_time_sorts_newest_message_changes_first() {
    let mut oldest = test_email();
    oldest.id = Uuid::parse_str("11111111-1111-1111-1111-111111111151").unwrap();
    oldest.thread_id = Uuid::parse_str("22222222-2222-2222-2222-222222222251").unwrap();
    oldest.subject = "Delivery 09:00".to_string();
    oldest.received_at = "2026-07-20T09:00:00Z".to_string();
    oldest.modseq = 93;
    oldest.mailbox_states[0].modseq = 93;

    let mut newest = test_email();
    newest.id = Uuid::parse_str("11111111-1111-1111-1111-111111111153").unwrap();
    newest.thread_id = Uuid::parse_str("22222222-2222-2222-2222-222222222253").unwrap();
    newest.subject = "Delivery 12:00".to_string();
    newest.received_at = "2026-07-20T12:00:00Z".to_string();
    newest.modseq = 91;
    newest.mailbox_states[0].modseq = 91;

    let mut middle = test_email();
    middle.id = Uuid::parse_str("11111111-1111-1111-1111-111111111152").unwrap();
    middle.thread_id = Uuid::parse_str("22222222-2222-2222-2222-222222222252").unwrap();
    middle.subject = "Delivery 10:30".to_string();
    middle.received_at = "2026-07-20T10:30:00Z".to_string();
    middle.modseq = 92;
    middle.mailbox_states[0].modseq = 92;

    for (counter, email) in [51, 53, 52].into_iter().zip([&oldest, &newest, &middle]) {
        crate::mapi::identity::remember_mapi_identity(
            email.id,
            crate::mapi::identity::mapi_store_id(counter),
        );
    }

    // Outlook trace 202607202136 used SynchronizationFlags 0xA139 and
    // SynchronizationExtraFlags 0x0000000D (Eid | CN | OrderByDeliveryTime).
    let buffer = sync_manifest_buffer_with_attachments(
        SYNC_TYPE_CONTENTS,
        0xA139,
        0x0000_000D,
        &[],
        crate::mapi::identity::INBOX_FOLDER_ID,
        &[],
        &[oldest, newest, middle],
        &[],
        &[],
        94,
    );

    let subject_offset = |subject: &str| {
        let value = utf16z(subject);
        let mut encoded = PID_TAG_SUBJECT_W.to_le_bytes().to_vec();
        encoded.extend_from_slice(&(value.len() as u32).to_le_bytes());
        encoded.extend_from_slice(&value);
        buffer
            .windows(encoded.len())
            .position(|window| window == encoded)
            .expect("subject property is present in the messageChange sequence")
    };

    let newest_offset = subject_offset("Delivery 12:00");
    let middle_offset = subject_offset("Delivery 10:30");
    let oldest_offset = subject_offset("Delivery 09:00");
    assert!(
        newest_offset < middle_offset && middle_offset < oldest_offset,
        "[MS-OXCFXICS] 3.2.5.9.1.1 requires newest-to-oldest messageChange ordering"
    );
}

#[test]
fn microsoft_oxcfxics_order_by_delivery_time_interleaves_normal_and_fai_changes() {
    let mut email = test_email();
    email.id = Uuid::parse_str("11111111-1111-4111-8111-111111111261").unwrap();
    email.thread_id = Uuid::parse_str("22222222-2222-4222-8222-222222222261").unwrap();
    email.subject = "Mail middle".to_string();
    email.received_at = "2026-07-20T11:00:00Z".to_string();
    crate::mapi::identity::remember_mapi_identity(
        email.id,
        crate::mapi::identity::mapi_store_id(261),
    );
    let newest = SpecialMessageSyncFact {
        folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
        item_id: crate::mapi::identity::mapi_store_id(263),
        canonical_id: Uuid::parse_str("33333333-3333-4333-8333-333333333263").unwrap(),
        associated: true,
        subject: "FAI newest".to_string(),
        body_text: Some(String::new()),
        message_class: "IPM.Configuration.Test".to_string(),
        last_modified_filetime: filetime_from_rfc3339_utc("2026-07-20T12:00:00Z"),
        message_size: 64,
        read_state: None,
        recipients: Vec::new(),
        named_properties: Vec::new(),
        named_property_definitions: Default::default(),
    };
    let oldest = SpecialMessageSyncFact {
        folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
        item_id: crate::mapi::identity::mapi_store_id(262),
        canonical_id: Uuid::parse_str("33333333-3333-4333-8333-333333333262").unwrap(),
        associated: true,
        subject: "FAI oldest".to_string(),
        body_text: Some(String::new()),
        message_class: "IPM.Configuration.Test".to_string(),
        last_modified_filetime: filetime_from_rfc3339_utc("2026-07-20T10:00:00Z"),
        message_size: 64,
        read_state: None,
        recipients: Vec::new(),
        named_properties: Vec::new(),
        named_property_definitions: Default::default(),
    };
    let specials = [oldest, newest];
    let buffer = sync_manifest_buffer_with_special_objects_and_final_state(
        Uuid::nil(),
        SYNC_TYPE_CONTENTS,
        0xA139,
        0x0000_000D,
        &[],
        crate::mapi::identity::INBOX_FOLDER_ID,
        &[],
        std::slice::from_ref(&email),
        &[],
        &specials,
        &[],
        &[],
        &[],
        std::slice::from_ref(&email),
        &[],
        &specials,
        std::slice::from_ref(&email),
        &[],
        264,
    );
    let subject_offset = |subject: &str| {
        let value = utf16z(subject);
        let mut encoded = PID_TAG_SUBJECT_W.to_le_bytes().to_vec();
        encoded.extend_from_slice(&(value.len() as u32).to_le_bytes());
        encoded.extend_from_slice(&value);
        buffer
            .windows(encoded.len())
            .position(|window| window == encoded)
            .expect("subject property is present in the messageChange sequence")
    };

    assert!(
        subject_offset("FAI newest") < subject_offset("Mail middle")
            && subject_offset("Mail middle") < subject_offset("FAI oldest"),
        "OrderByDeliveryTime applies to the complete normal/FAI messageChange sequence"
    );
}

#[test]
fn microsoft_oxcfxics_order_by_delivery_time_uses_calendar_delivery_property() {
    let appointment =
        |counter: u64, canonical_id: &str, subject: &str, delivery: &str, modified: &str| {
            SpecialMessageSyncFact {
                folder_id: crate::mapi::identity::CALENDAR_FOLDER_ID,
                item_id: crate::mapi::identity::mapi_store_id(counter),
                canonical_id: Uuid::parse_str(canonical_id).unwrap(),
                associated: false,
                subject: subject.to_string(),
                body_text: Some(String::new()),
                message_class: "IPM.Appointment".to_string(),
                last_modified_filetime: filetime_from_rfc3339_utc(modified),
                message_size: 128,
                read_state: None,
                recipients: Vec::new(),
                named_properties: vec![(
                    PID_TAG_MESSAGE_DELIVERY_TIME,
                    SpecialMessagePropertyValue::I64(filetime_from_rfc3339_utc(delivery) as i64),
                )],
                named_property_definitions: Default::default(),
            }
        };
    let appointments = [
        appointment(
            271,
            "44444444-4444-4444-8444-444444444271",
            "Appointment 09:00",
            "2026-07-20T09:00:00Z",
            "2026-07-20T13:00:00Z",
        ),
        appointment(
            273,
            "44444444-4444-4444-8444-444444444273",
            "Appointment 12:00",
            "2026-07-20T12:00:00Z",
            "2026-07-20T08:00:00Z",
        ),
        appointment(
            272,
            "44444444-4444-4444-8444-444444444272",
            "Appointment 10:30",
            "2026-07-20T10:30:00Z",
            "2026-07-20T11:00:00Z",
        ),
    ];
    let buffer = sync_manifest_buffer_with_special_objects_and_final_state(
        Uuid::nil(),
        SYNC_TYPE_CONTENTS,
        0xA139,
        0x0000_000D,
        &[],
        crate::mapi::identity::CALENDAR_FOLDER_ID,
        &[],
        &[],
        &[],
        &appointments,
        &[],
        &[],
        &[],
        &[],
        &[],
        &appointments,
        &[],
        &[],
        274,
    );
    let subject_offset = |subject: &str| {
        let value = utf16z(subject);
        let mut encoded = PID_TAG_SUBJECT_W.to_le_bytes().to_vec();
        encoded.extend_from_slice(&(value.len() as u32).to_le_bytes());
        encoded.extend_from_slice(&value);
        buffer
            .windows(encoded.len())
            .position(|window| window == encoded)
            .expect("appointment subject is present in the messageChange sequence")
    };

    assert!(
        subject_offset("Appointment 12:00") < subject_offset("Appointment 10:30")
            && subject_offset("Appointment 10:30") < subject_offset("Appointment 09:00"),
        "PidTagMessageDeliveryTime takes precedence over LastModificationTime"
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

    let message_start = buffer
        .windows(4)
        .position(|window| window == INCR_SYNC_MESSAGE.to_le_bytes())
        .expect("normal message content marker");
    let recipient_start = buffer[message_start..]
        .windows(4)
        .position(|window| window == START_RECIP.to_le_bytes())
        .map(|offset| message_start + offset)
        .expect("normal message recipient marker");
    let root_message_content = &buffer[message_start..recipient_start];
    for identity_tag in [
        PID_TAG_ENTRY_ID,
        PID_TAG_PARENT_ENTRY_ID,
        PID_TAG_INSTANCE_KEY,
    ] {
        assert!(
            !root_message_content
                .windows(4)
                .any(|window| window == identity_tag.to_le_bytes()),
            "ICS root message content must omit provider-local identity 0x{identity_tag:08x}"
        );
    }

    assert_tag_sequence(
        &buffer,
        &[
            PID_TAG_SUBJECT_W,
            START_RECIP,
            PID_TAG_ROWID,
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
    let first_recipient = [
        START_RECIP.to_le_bytes(),
        PID_TAG_ROWID.to_le_bytes(),
        0_i32.to_le_bytes(),
        PID_TAG_RECIPIENT_TYPE.to_le_bytes(),
        1_i32.to_le_bytes(),
    ]
    .concat();
    let second_recipient = [
        START_RECIP.to_le_bytes(),
        PID_TAG_ROWID.to_le_bytes(),
        1_i32.to_le_bytes(),
        PID_TAG_RECIPIENT_TYPE.to_le_bytes(),
        2_i32.to_le_bytes(),
    ]
    .concat();
    assert!(
        contains_bytes(&buffer, &first_recipient),
        "MS-OXCFXICS 2.2.4.3.23 requires Rowid first in the To recipient"
    );
    assert!(
        contains_bytes(&buffer, &second_recipient),
        "MS-OXCFXICS 2.2.4.3.23 requires a distinct Rowid first in the Cc recipient"
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
fn meeting_response_fast_transfer_uses_ics_organizer_and_responder_rows() {
    let mut email = test_email();
    crate::mapi::identity::remember_mapi_identity(
        email.id,
        crate::mapi::identity::mapi_store_id(50),
    );
    email.to = vec![JmapEmailAddress {
        address: "stale-rfc-recipient@example.test".to_string(),
        display_name: Some("Stale RFC recipient".to_string()),
    }];
    email.cc = vec![JmapEmailAddress {
        address: "stale-rfc-cc@example.test".to_string(),
        display_name: None,
    }];
    email.calendar_meeting_response = Some(lpe_storage::CalendarMeetingResponse {
        method: "REPLY".to_string(),
        transport_attachment_id: None,
        server_processed: false,
        organizer: Some(lpe_storage::CalendarMeetingIdentity {
            email: "organizer@example.test".to_string(),
            display_name: "Organizer".to_string(),
        }),
        attendee_email: "responder@example.test".to_string(),
        attendee_name: "Responder".to_string(),
        partstat: "accepted".to_string(),
        uid: "recipient-projection@example.test".to_string(),
        response_sent_at: Some("2026-08-24T18:00:00Z".to_string()),
        meeting_start: None,
        meeting_end: None,
        meeting_location: None,
        meeting_sequence: None,
        proposed_start: None,
        proposed_end: None,
        original_start: None,
        original_end: None,
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

    let first_start = buffer
        .windows(4)
        .position(|window| window == START_RECIP.to_le_bytes())
        .expect("organizer StartRecip");
    let second_end = buffer[first_start..]
        .windows(4)
        .enumerate()
        .filter(|(_, window)| *window == END_TO_RECIP.to_le_bytes())
        .nth(1)
        .map(|(offset, _)| first_start + offset + 4)
        .expect("responder EndToRecip");
    let recipient_collection = &buffer[first_start..second_end];

    assert_eq!(
        recipient_collection
            .windows(4)
            .filter(|window| *window == START_RECIP.to_le_bytes())
            .count(),
        2
    );
    let organizer_prefix = [
        START_RECIP.to_le_bytes(),
        PID_TAG_ROWID.to_le_bytes(),
        0_i32.to_le_bytes(),
        PID_TAG_RECIPIENT_TYPE.to_le_bytes(),
        1_i32.to_le_bytes(),
        PID_TAG_RECIPIENT_FLAGS.to_le_bytes(),
        3_i32.to_le_bytes(),
        PID_TAG_RECIPIENT_ORDER.to_le_bytes(),
        0_i32.to_le_bytes(),
        PID_TAG_RECIPIENT_TRACK_STATUS.to_le_bytes(),
        0_i32.to_le_bytes(),
    ]
    .concat();
    let responder_prefix = [
        START_RECIP.to_le_bytes(),
        PID_TAG_ROWID.to_le_bytes(),
        1_i32.to_le_bytes(),
        PID_TAG_RECIPIENT_TYPE.to_le_bytes(),
        1_i32.to_le_bytes(),
        PID_TAG_RECIPIENT_FLAGS.to_le_bytes(),
        1_i32.to_le_bytes(),
        PID_TAG_RECIPIENT_ORDER.to_le_bytes(),
        1_i32.to_le_bytes(),
        PID_TAG_RECIPIENT_TRACK_STATUS.to_le_bytes(),
        3_i32.to_le_bytes(),
    ]
    .concat();
    assert!(contains_bytes(recipient_collection, &organizer_prefix));
    assert!(contains_bytes(recipient_collection, &responder_prefix));
    assert_variable_property_present(
        recipient_collection,
        PID_TAG_SMTP_ADDRESS_W,
        &utf16z("organizer@example.test"),
    );
    assert_variable_property_present(
        recipient_collection,
        PID_TAG_SMTP_ADDRESS_W,
        &utf16z("responder@example.test"),
    );
    assert_variable_property_present(
        recipient_collection,
        PID_TAG_SEARCH_KEY,
        b"SMTP:ORGANIZER@EXAMPLE.TEST\0",
    );
    assert_eq!(
        recipient_collection
            .windows(PID_TAG_ENTRY_ID.to_le_bytes().len())
            .filter(|window| *window == PID_TAG_ENTRY_ID.to_le_bytes())
            .count(),
        2
    );
    assert_eq!(
        recipient_collection
            .windows(PID_TAG_RECIPIENT_ENTRY_ID.to_le_bytes().len())
            .filter(|window| *window == PID_TAG_RECIPIENT_ENTRY_ID.to_le_bytes())
            .count(),
        2
    );
    assert!(!contains_bytes(
        recipient_collection,
        &utf16z("stale-rfc-recipient@example.test")
    ));
    assert!(!contains_bytes(
        recipient_collection,
        &utf16z("stale-rfc-cc@example.test")
    ));
}

#[test]
fn meeting_request_fast_transfer_preserves_attendee_type_status_and_order() {
    let mut email = test_email();
    crate::mapi::identity::remember_mapi_identity(
        email.id,
        crate::mapi::identity::mapi_store_id(50),
    );
    email.calendar_meeting_request = Some(lpe_storage::CalendarMeetingRequest {
        uid: "request-recipient-projection@example.test".to_string(),
        transport_attachment_id: None,
        organizer: Some(lpe_storage::CalendarMeetingIdentity {
            email: "organizer@example.test".to_string(),
            display_name: "Organizer".to_string(),
        }),
        attendees: vec![
            lpe_storage::CalendarMeetingAttendee {
                email: "required@example.test".to_string(),
                display_name: "Required".to_string(),
                cutype: "INDIVIDUAL".to_string(),
                role: "REQ-PARTICIPANT".to_string(),
                partstat: "accepted".to_string(),
                rsvp: true,
            },
            lpe_storage::CalendarMeetingAttendee {
                email: "optional@example.test".to_string(),
                display_name: "Optional".to_string(),
                cutype: "INDIVIDUAL".to_string(),
                role: "OPT-PARTICIPANT".to_string(),
                partstat: "tentative".to_string(),
                rsvp: false,
            },
            lpe_storage::CalendarMeetingAttendee {
                email: "room@example.test".to_string(),
                display_name: "Room".to_string(),
                cutype: "ROOM".to_string(),
                role: "NON-PARTICIPANT".to_string(),
                partstat: "declined".to_string(),
                rsvp: false,
            },
        ],
        response_requested: true,
        sent_at: Some("2026-08-24T18:00:00Z".to_string()),
        meeting_start: "2026-08-25T08:00:00Z".to_string(),
        meeting_end: "2026-08-25T09:00:00Z".to_string(),
        meeting_location: None,
        meeting_sequence: 1,
        intended_busy_status: 2,
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

    assert_eq!(
        buffer
            .windows(4)
            .filter(|window| *window == START_RECIP.to_le_bytes())
            .count(),
        4
    );
    for (row_id, recipient_type, flags, status) in [
        (0_i32, 1_i32, 3_i32, 0_i32),
        (1, 1, 1, 3),
        (2, 2, 1, 2),
        (3, 3, 1, 4),
    ] {
        let prefix = [
            START_RECIP.to_le_bytes(),
            PID_TAG_ROWID.to_le_bytes(),
            row_id.to_le_bytes(),
            PID_TAG_RECIPIENT_TYPE.to_le_bytes(),
            recipient_type.to_le_bytes(),
            PID_TAG_RECIPIENT_FLAGS.to_le_bytes(),
            flags.to_le_bytes(),
            PID_TAG_RECIPIENT_ORDER.to_le_bytes(),
            row_id.to_le_bytes(),
            PID_TAG_RECIPIENT_TRACK_STATUS.to_le_bytes(),
            status.to_le_bytes(),
        ]
        .concat();
        assert!(contains_bytes(&buffer, &prefix));
    }
}

#[test]
fn microsoft_oxcfxics_calendar_content_sync_replaces_recipient_collection_with_organizer() {
    let entry_id = vec![
        0x00, 0x00, 0x00, 0x00, 0xdc, 0xa7, 0x40, 0xc8, 0xc0, 0x42, 0x10, 0x1a, 0xb4, 0xb9, 0x08,
        0x00, 0x2b, 0x2f, 0xe1, 0x82, 0x01, 0x00, 0x00, 0x00,
    ];
    let appointment = SpecialMessageSyncFact {
        folder_id: crate::mapi::identity::CALENDAR_FOLDER_ID,
        item_id: crate::mapi::identity::mapi_store_id(321),
        canonical_id: Uuid::parse_str("51515151-5151-4151-9151-515151515151").unwrap(),
        associated: false,
        subject: "Calendar organizer roundtrip".to_string(),
        body_text: Some(String::new()),
        message_class: "IPM.Appointment".to_string(),
        last_modified_filetime: filetime_from_rfc3339_utc("2026-08-10T20:38:16Z"),
        message_size: 128,
        read_state: None,
        recipients: vec![
            SpecialMessageRecipientSyncFact {
                row_id: 0,
                recipient_type: 1,
                recipient_flags: 3,
                track_status: 0,
                display_type_ex: 0x4000_0000,
                address_type: "EX".to_string(),
                email_address:
                    "/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn=test-l-p-e-ch"
                        .to_string(),
                smtp_address: "test@l-p-e.ch".to_string(),
                display_name: "test".to_string(),
                entry_id: entry_id.clone(),
            },
            SpecialMessageRecipientSyncFact {
                row_id: 1,
                recipient_type: 1,
                recipient_flags: 1,
                track_status: 3,
                display_type_ex: 0x4000_0000,
                address_type: "EX".to_string(),
                email_address:
                    "/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn=alice-example-test"
                        .to_string(),
                smtp_address: "alice@example.test".to_string(),
                display_name: "Alice".to_string(),
                entry_id: entry_id.clone(),
            },
        ],
        named_properties: vec![
            (
                PID_TAG_ENTRY_ID,
                SpecialMessagePropertyValue::Binary(vec![0x11; 70]),
            ),
            (
                PID_TAG_PARENT_ENTRY_ID,
                SpecialMessagePropertyValue::Binary(vec![0x22; 46]),
            ),
            (
                PID_TAG_INSTANCE_KEY,
                SpecialMessagePropertyValue::Binary(vec![0x33; 22]),
            ),
        ],
        named_property_definitions: Default::default(),
    };
    let appointments = [appointment];
    let buffer = sync_manifest_buffer_with_special_objects_and_final_state(
        Uuid::nil(),
        SYNC_TYPE_CONTENTS,
        0xA139,
        0x0000_000D,
        &[],
        crate::mapi::identity::CALENDAR_FOLDER_ID,
        &[],
        &[],
        &[],
        &appointments,
        &[],
        &[],
        &[],
        &[],
        &[],
        &appointments,
        &[],
        &[],
        322,
    );

    assert_eq!(
        buffer
            .windows(START_RECIP.to_le_bytes().len())
            .filter(|window| *window == START_RECIP.to_le_bytes())
            .count(),
        2
    );
    let message_start = buffer
        .windows(4)
        .position(|window| window == INCR_SYNC_MESSAGE.to_le_bytes())
        .expect("Calendar message content marker");
    let recipient_start = buffer[message_start..]
        .windows(4)
        .position(|window| window == START_RECIP.to_le_bytes())
        .map(|offset| message_start + offset)
        .expect("Calendar recipient marker");
    let root_message_content = &buffer[message_start..recipient_start];
    for identity_tag in [
        PID_TAG_ENTRY_ID,
        PID_TAG_PARENT_ENTRY_ID,
        PID_TAG_INSTANCE_KEY,
    ] {
        assert!(
            !root_message_content
                .windows(4)
                .any(|window| window == identity_tag.to_le_bytes()),
            "Calendar ICS root must omit provider-local identity 0x{identity_tag:08x}"
        );
    }
    // The two PidTagEntryId values below belong only to recipient rows.
    assert_eq!(
        buffer
            .windows(PID_TAG_ENTRY_ID.to_le_bytes().len())
            .filter(|window| *window == PID_TAG_ENTRY_ID.to_le_bytes())
            .count(),
        2
    );
    assert_tag_sequence(
        &buffer,
        &[
            META_TAG_FX_DEL_PROP,
            START_RECIP,
            PID_TAG_ROWID,
            PID_TAG_RECIPIENT_TYPE,
            PID_TAG_RECIPIENT_FLAGS,
            PID_TAG_RECIPIENT_ORDER,
            PID_TAG_RECIPIENT_TRACK_STATUS,
            PID_TAG_ADDRESS_TYPE_W,
            PID_TAG_EMAIL_ADDRESS_W,
            PID_TAG_SMTP_ADDRESS_W,
            PID_TAG_ENTRY_ID,
            PID_TAG_RECIPIENT_ENTRY_ID,
            END_TO_RECIP,
            META_TAG_FX_DEL_PROP,
        ],
    );
    assert_i32_property(&buffer, PID_TAG_RECIPIENT_TYPE, 1);
    assert_i32_property(&buffer, PID_TAG_RECIPIENT_FLAGS, 3);
    assert_i32_property(&buffer, PID_TAG_RECIPIENT_TRACK_STATUS, 0);
    let accepted_attendee_row = [
        START_RECIP.to_le_bytes(),
        PID_TAG_ROWID.to_le_bytes(),
        1_i32.to_le_bytes(),
        PID_TAG_RECIPIENT_TYPE.to_le_bytes(),
        1_i32.to_le_bytes(),
        PID_TAG_RECIPIENT_FLAGS.to_le_bytes(),
        1_i32.to_le_bytes(),
        PID_TAG_RECIPIENT_ORDER.to_le_bytes(),
        1_i32.to_le_bytes(),
        PID_TAG_RECIPIENT_TRACK_STATUS.to_le_bytes(),
        3_i32.to_le_bytes(),
    ]
    .concat();
    assert!(buffer
        .windows(accepted_attendee_row.len())
        .any(|window| window == accepted_attendee_row));
    assert_variable_property_present(&buffer, PID_TAG_ADDRESS_TYPE_W, &utf16z("EX"));
    assert_variable_property_present(
        &buffer,
        PID_TAG_EMAIL_ADDRESS_W,
        &utf16z("/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn=test-l-p-e-ch"),
    );
    assert_variable_property_present(&buffer, PID_TAG_SMTP_ADDRESS_W, &utf16z("test@l-p-e.ch"));
    assert_variable_property_present(&buffer, PID_TAG_ENTRY_ID, &entry_id);
    assert_variable_property_present(&buffer, PID_TAG_RECIPIENT_ENTRY_ID, &entry_id);
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
    let buffer =
        fast_transfer_message_list_buffer_with_attachments(std::slice::from_ref(&email), &[]);

    assert_tag_sequence(
        &buffer,
        &[
            START_MESSAGE,
            PID_TAG_MESSAGE_DELIVERY_TIME,
            PID_TAG_SENDER_NAME_W,
            PID_TAG_SENDER_ADDRESS_TYPE_W,
            PID_TAG_SENDER_EMAIL_ADDRESS_W,
            PID_TAG_SENDER_SMTP_ADDRESS_W,
            PID_TAG_SENDER_ENTRY_ID,
            PID_TAG_SENDER_SEARCH_KEY,
            PID_TAG_SENT_REPRESENTING_NAME_W,
            PID_TAG_SENT_REPRESENTING_ADDRESS_TYPE_W,
            PID_TAG_SENT_REPRESENTING_EMAIL_ADDRESS_W,
            PID_TAG_SENT_REPRESENTING_SMTP_ADDRESS_W,
            PID_TAG_SENT_REPRESENTING_ENTRY_ID,
            PID_TAG_SENT_REPRESENTING_SEARCH_KEY,
            PID_TAG_MESSAGE_CLASS_W,
            PID_TAG_SUBJECT_W,
            PID_TAG_BODY_W,
            END_MESSAGE,
        ],
    );
    assert!(!buffer.starts_with(b"LPE-MAPI-FASTTRANSFER\0"));
    assert_i64_property(
        &buffer,
        PID_TAG_MESSAGE_DELIVERY_TIME,
        filetime_from_rfc3339_utc("2026-05-06T12:00:00Z") as i64,
    );
    assert_variable_property_present(&buffer, PID_TAG_SENDER_NAME_W, &utf16z("Alice"));
    assert_variable_property_present(&buffer, PID_TAG_SENDER_ADDRESS_TYPE_W, &utf16z("SMTP"));
    assert_variable_property_present(
        &buffer,
        PID_TAG_SENDER_EMAIL_ADDRESS_W,
        &utf16z("alice@example.test"),
    );
    assert_variable_property_present(
        &buffer,
        PID_TAG_SENDER_SMTP_ADDRESS_W,
        &utf16z("alice@example.test"),
    );
    assert_variable_property_present(
        &buffer,
        PID_TAG_SENDER_ENTRY_ID,
        &crate::mapi::properties::sender_entry_id(&email),
    );
    assert_variable_property_present(
        &buffer,
        PID_TAG_SENDER_SEARCH_KEY,
        &crate::mapi::properties::smtp_search_key("alice@example.test"),
    );
    assert_variable_property_present(&buffer, PID_TAG_SENT_REPRESENTING_NAME_W, &utf16z("Alice"));
    assert_variable_property_present(
        &buffer,
        PID_TAG_SENT_REPRESENTING_ADDRESS_TYPE_W,
        &utf16z("SMTP"),
    );
    assert_variable_property_present(
        &buffer,
        PID_TAG_SENT_REPRESENTING_EMAIL_ADDRESS_W,
        &utf16z("alice@example.test"),
    );
    assert_variable_property_present(
        &buffer,
        PID_TAG_SENT_REPRESENTING_SMTP_ADDRESS_W,
        &utf16z("alice@example.test"),
    );
    assert_variable_property_present(
        &buffer,
        PID_TAG_SENT_REPRESENTING_ENTRY_ID,
        &crate::mapi::properties::sent_representing_entry_id(&email),
    );
    assert_variable_property_present(
        &buffer,
        PID_TAG_SENT_REPRESENTING_SEARCH_KEY,
        &crate::mapi::properties::smtp_search_key("alice@example.test"),
    );
    assert_variable_property_present(&buffer, PID_TAG_MESSAGE_CLASS_W, &utf16z("IPM.Note"));
    assert_variable_property_present(&buffer, PID_TAG_SUBJECT_W, &utf16z("Hello"));
    assert_variable_property_present(&buffer, PID_TAG_BODY_W, &utf16z("Hello body"));
}

#[test]
fn fast_transfer_keeps_distinct_from_and_sender_identity_families() {
    let mut email = test_email();
    email.from_display = Some("Meeting Organizer".to_string());
    email.from_address = "organizer@example.test".to_string();
    email.sender_display = Some("Transport Agent".to_string());
    email.sender_address = Some("agent@example.test".to_string());

    let buffer = fast_transfer_message_content_buffer_with_attachments(
        &email,
        &[],
        None,
        FastTransferDirectPropertyFilter::CopyToExcluding(&[]),
        FastTransferMessageChildren::new(false, false),
    );
    assert_variable_property_present(&buffer, PID_TAG_SENDER_NAME_W, &utf16z("Transport Agent"));
    assert_variable_property_present(
        &buffer,
        PID_TAG_SENDER_EMAIL_ADDRESS_W,
        &utf16z("agent@example.test"),
    );
    assert_variable_property_present(
        &buffer,
        PID_TAG_SENT_REPRESENTING_NAME_W,
        &utf16z("Meeting Organizer"),
    );
    assert_variable_property_present(
        &buffer,
        PID_TAG_SENT_REPRESENTING_EMAIL_ADDRESS_W,
        &utf16z("organizer@example.test"),
    );
}

#[test]
fn fast_transfer_named_property_boolean_tracks_plain_categorized_and_rss_mail() {
    const PID_TAG_HAS_NAMED_PROPERTIES: u32 = 0x664A_000B;

    let plain = test_email();
    let mut categorized = plain.clone();
    categorized.categories = vec!["Customer".to_string()];
    let mut rss = plain.clone();
    rss.mailbox_role = "rss_feeds".to_string();
    rss.mailbox_name = "RSS Feeds".to_string();

    for (email, expected) in [(plain, false), (categorized, true), (rss, true)] {
        let copy_to = fast_transfer_message_content_buffer_with_attachments(
            &email,
            &[],
            None,
            FastTransferDirectPropertyFilter::CopyToExcluding(&[]),
            FastTransferMessageChildren::new(false, false),
        );
        let contents_sync = sync_manifest_buffer_with_attachments(
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
        assert_bool_property(&copy_to, PID_TAG_HAS_NAMED_PROPERTIES, expected);
        assert_bool_property(&contents_sync, PID_TAG_HAS_NAMED_PROPERTIES, expected);
    }
}

#[test]
fn meeting_request_fast_transfer_projects_actionable_properties() {
    const PID_TAG_HAS_NAMED_PROPERTIES: u32 = 0x664A_000B;
    const PID_TAG_START_DATE: u32 = 0x0060_0040;
    const PID_TAG_END_DATE: u32 = 0x0061_0040;
    const PID_TAG_REPLY_REQUESTED: u32 = 0x0C17_000B;
    const PID_TAG_RESPONSE_REQUESTED: u32 = 0x0063_000B;
    const PID_TAG_PROCESSED: u32 = 0x7D01_000B;
    const PID_LID_APPOINTMENT_START_WHOLE_TAG: u32 = 0x820D_0040;
    const PID_LID_LOCATION_W_TAG: u32 = 0x8208_001F;
    const PID_LID_GLOBAL_OBJECT_ID_TAG: u32 = 0x8001_0102;
    const PID_LID_CLEAN_GLOBAL_OBJECT_ID_TAG: u32 = 0x8002_0102;
    const PSETID_APPOINTMENT_GUID: [u8; 16] = [
        0x02, 0x20, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x46,
    ];
    const PSETID_MEETING_GUID: [u8; 16] = [
        0x90, 0xDA, 0xD8, 0x6E, 0x0B, 0x45, 0x1B, 0x10, 0x98, 0xDA, 0x00, 0xAA, 0x00, 0x3F, 0x13,
        0x05,
    ];

    let mut email = test_email();
    crate::mapi::identity::remember_mapi_identity(
        email.id,
        crate::mapi::identity::mapi_store_id(50),
    );
    email.calendar_invitation = true;
    email.calendar_meeting_request = Some(lpe_storage::CalendarMeetingRequest {
        uid: "mapi-goid:040000008200e00074c5b7101a82e00807ea0818c08470cd9e31dd01000000000000000010000000ecff8aec00ce584390f914bf6a87f955".to_string(),
        transport_attachment_id: None,
        organizer: None,
        attendees: Vec::new(),
        response_requested: true,
        sent_at: Some("2026-08-23T18:00:00Z".to_string()),
        meeting_start: "2026-08-24T06:30:00Z".to_string(),
        meeting_end: "2026-08-24T07:00:00Z".to_string(),
        meeting_location: Some("Les Planches".to_string()),
        meeting_sequence: 2,
        intended_busy_status: 2,
    });
    let copy_to = fast_transfer_message_content_buffer_with_attachments(
        &email,
        &[],
        None,
        FastTransferDirectPropertyFilter::CopyToExcluding(&[]),
        FastTransferMessageChildren::new(false, false),
    );
    let contents_sync = sync_manifest_buffer_with_attachments(
        SYNC_TYPE_CONTENTS,
        SYNC_FLAG_NORMAL | SYNC_FLAG_UNICODE,
        0,
        &[],
        crate::mapi::identity::INBOX_FOLDER_ID,
        &[],
        &[email.clone()],
        &[],
        &[],
        1,
    );
    let start = filetime_from_rfc3339_utc("2026-08-24T06:30:00Z") as i64;
    let end = filetime_from_rfc3339_utc("2026-08-24T07:00:00Z") as i64;
    let goid =
        match crate::mapi::properties::email_property_value(&email, PID_LID_GLOBAL_OBJECT_ID_TAG) {
            Some(crate::mapi::properties::MapiValue::Binary(value)) => value,
            value => panic!("expected request GlobalObjectId, got {value:?}"),
        };
    let mut encoded_goid = (goid.len() as u32).to_le_bytes().to_vec();
    encoded_goid.extend_from_slice(&goid);
    let clean_goid = match crate::mapi::properties::email_property_value(
        &email,
        PID_LID_CLEAN_GLOBAL_OBJECT_ID_TAG,
    ) {
        Some(crate::mapi::properties::MapiValue::Binary(value)) => value,
        value => panic!("expected request CleanGlobalObjectId, got {value:?}"),
    };
    assert_ne!(goid, clean_goid);
    let mut encoded_clean_goid = (clean_goid.len() as u32).to_le_bytes().to_vec();
    encoded_clean_goid.extend_from_slice(&clean_goid);
    let mut encoded_location = (utf16z("Les Planches").len() as u32).to_le_bytes().to_vec();
    encoded_location.extend_from_slice(&utf16z("Les Planches"));

    for buffer in [&copy_to, &contents_sync] {
        assert_bool_property(buffer, PID_TAG_HAS_NAMED_PROPERTIES, true);
        assert_variable_property_present(
            buffer,
            PID_TAG_MESSAGE_CLASS_W,
            &utf16z("IPM.Schedule.Meeting.Request"),
        );
        assert_i64_property(buffer, PID_TAG_START_DATE, start);
        assert_i64_property(buffer, PID_TAG_END_DATE, end);
        assert_bool_property(buffer, PID_TAG_REPLY_REQUESTED, true);
        assert_bool_property(buffer, PID_TAG_RESPONSE_REQUESTED, true);
        assert_absent_property(buffer, PID_TAG_PROCESSED);
        assert_named_lid_property(
            buffer,
            PID_LID_APPOINTMENT_START_WHOLE_TAG,
            PSETID_APPOINTMENT_GUID,
            0x820D,
            &start.to_le_bytes(),
        );
        assert_named_lid_property(
            buffer,
            PID_LID_LOCATION_W_TAG,
            PSETID_APPOINTMENT_GUID,
            0x8208,
            &encoded_location,
        );
        assert_named_lid_property(
            buffer,
            PID_LID_GLOBAL_OBJECT_ID_TAG,
            PSETID_MEETING_GUID,
            0x0003,
            &encoded_goid,
        );
        assert_named_lid_property(
            buffer,
            PID_LID_CLEAN_GLOBAL_OBJECT_ID_TAG,
            PSETID_MEETING_GUID,
            0x0023,
            &encoded_clean_goid,
        );
    }

    let response_only = fast_transfer_message_content_buffer_with_attachments(
        &email,
        &[],
        None,
        FastTransferDirectPropertyFilter::CopyPropertiesIncluding(&[PID_TAG_RESPONSE_REQUESTED]),
        FastTransferMessageChildren::new(false, false),
    );
    assert_bool_property(&response_only, PID_TAG_RESPONSE_REQUESTED, true);
    assert_absent_property(&response_only, PID_TAG_START_DATE);
    assert_absent_property(&response_only, PID_LID_LOCATION_W_TAG);
}

#[test]
fn meeting_response_subject_relationship_matches_copy_to_and_contents_sync() {
    const PID_TAG_HAS_NAMED_PROPERTIES: u32 = 0x664A_000B;
    const PID_TAG_SUBJECT_PREFIX_W: u32 = 0x003D_001F;

    for (method, partstat, prefix) in [
        ("REPLY", "accepted", "Accepted: "),
        ("REPLY", "declined", "Declined: "),
        ("COUNTER", "tentative", "New Time Proposed: "),
    ] {
        let email = meeting_response_subject_test_email(method, partstat, prefix);
        let copy_to = fast_transfer_message_content_buffer_with_attachments(
            &email,
            &[],
            None,
            FastTransferDirectPropertyFilter::CopyToExcluding(&[]),
            FastTransferMessageChildren::new(false, false),
        );
        let contents_sync = sync_manifest_buffer_with_attachments(
            SYNC_TYPE_CONTENTS,
            SYNC_FLAG_NORMAL | SYNC_FLAG_UNICODE,
            0,
            &[],
            crate::mapi::identity::INBOX_FOLDER_ID,
            &[],
            &[email],
            &[],
            &[],
            1,
        );
        for buffer in [&copy_to, &contents_sync] {
            assert_bool_property(buffer, PID_TAG_HAS_NAMED_PROPERTIES, true);
            assert_variable_property_present(buffer, PID_TAG_SUBJECT_PREFIX_W, &utf16z(prefix));
            assert_variable_property_present(
                buffer,
                PID_TAG_NORMALIZED_SUBJECT_W,
                &utf16z("Probe 10"),
            );
        }
    }
}

#[test]
fn meeting_response_fast_transfer_projects_counter_proposal_named_properties() {
    const PID_TAG_HAS_NAMED_PROPERTIES: u32 = 0x664A_000B;
    const PID_TAG_SUBJECT_PREFIX_W: u32 = 0x003D_001F;
    const PID_LID_APPOINTMENT_COUNTER_PROPOSAL_TAG: u32 = 0x8257_000B;
    const PID_LID_APPOINTMENT_PROPOSED_START_WHOLE_TAG: u32 = 0x8250_0040;
    const PID_LID_APPOINTMENT_PROPOSED_END_WHOLE_TAG: u32 = 0x8251_0040;
    const PID_LID_IS_SILENT_TAG: u32 = 0x81E6_000B;
    const PSETID_APPOINTMENT_GUID: [u8; 16] = [
        0x02, 0x20, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x46,
    ];
    const PSETID_MEETING_GUID: [u8; 16] = [
        0x90, 0xDA, 0xD8, 0x6E, 0x0B, 0x45, 0x1B, 0x10, 0x98, 0xDA, 0x00, 0xAA, 0x00, 0x3F, 0x13,
        0x05,
    ];

    let mut email = test_email();
    crate::mapi::identity::remember_mapi_identity(
        email.id,
        crate::mapi::identity::mapi_store_id(51),
    );
    email.subject = "New Time Proposed: Probe 10".to_string();
    email.calendar_meeting_response = Some(lpe_storage::CalendarMeetingResponse {
        method: "COUNTER".to_string(),
        transport_attachment_id: None,
        server_processed: false,
        organizer: None,
        attendee_email: "denis.ducret@sdic.ch".to_string(),
        attendee_name: "Denis Ducret".to_string(),
        partstat: "declined".to_string(),
        uid: "probe-10@example.test".to_string(),
        response_sent_at: Some("2026-08-24T05:44:30Z".to_string()),
        meeting_start: Some("2026-08-24T06:30:00Z".to_string()),
        meeting_end: Some("2026-08-24T07:00:00Z".to_string()),
        meeting_location: Some("Les Planches".to_string()),
        meeting_sequence: Some(2),
        proposed_start: Some("2026-08-24T07:30:00Z".to_string()),
        proposed_end: Some("2026-08-24T08:00:00Z".to_string()),
        original_start: Some("2026-08-24T06:30:00Z".to_string()),
        original_end: Some("2026-08-24T07:00:00Z".to_string()),
    });
    let copy_to = fast_transfer_message_content_buffer_with_attachments(
        &email,
        &[],
        None,
        FastTransferDirectPropertyFilter::CopyToExcluding(&[]),
        FastTransferMessageChildren::new(false, false),
    );
    let contents_sync = sync_manifest_buffer_with_attachments(
        SYNC_TYPE_CONTENTS,
        SYNC_FLAG_NORMAL | SYNC_FLAG_UNICODE,
        0,
        &[],
        crate::mapi::identity::INBOX_FOLDER_ID,
        &[],
        &[email],
        &[],
        &[],
        1,
    );
    let proposed_start = filetime_from_rfc3339_utc("2026-08-24T07:30:00Z") as i64;
    let proposed_end = filetime_from_rfc3339_utc("2026-08-24T08:00:00Z") as i64;

    for buffer in [&copy_to, &contents_sync] {
        assert_bool_property(buffer, PID_TAG_HAS_NAMED_PROPERTIES, true);
        assert_variable_property_present(
            buffer,
            PID_TAG_MESSAGE_CLASS_W,
            &utf16z("IPM.Schedule.Meeting.Resp.Tent"),
        );
        assert_variable_property_present(
            buffer,
            PID_TAG_SUBJECT_PREFIX_W,
            &utf16z("New Time Proposed: "),
        );
        assert_variable_property_present(buffer, PID_TAG_NORMALIZED_SUBJECT_W, &utf16z("Probe 10"));
        assert_named_lid_property(
            buffer,
            PID_LID_APPOINTMENT_COUNTER_PROPOSAL_TAG,
            PSETID_APPOINTMENT_GUID,
            0x8257,
            &[1, 0],
        );
        assert_named_lid_property(
            buffer,
            PID_LID_APPOINTMENT_PROPOSED_START_WHOLE_TAG,
            PSETID_APPOINTMENT_GUID,
            0x8250,
            &proposed_start.to_le_bytes(),
        );
        assert_named_lid_property(
            buffer,
            PID_LID_APPOINTMENT_PROPOSED_END_WHOLE_TAG,
            PSETID_APPOINTMENT_GUID,
            0x8251,
            &proposed_end.to_le_bytes(),
        );
        assert_named_lid_property(
            buffer,
            PID_LID_IS_SILENT_TAG,
            PSETID_MEETING_GUID,
            0x0004,
            &[0, 0],
        );
    }
}

#[test]
fn fast_transfer_copy_properties_filters_message_identity_properties() {
    let email = test_email();
    let buffer = fast_transfer_message_content_buffer_with_attachments(
        &email,
        &[],
        None,
        FastTransferDirectPropertyFilter::CopyPropertiesIncluding(&[
            PID_TAG_MESSAGE_DELIVERY_TIME,
            PID_TAG_SENDER_NAME_W,
        ]),
        FastTransferMessageChildren::new(false, false),
    );

    assert_i64_property(
        &buffer,
        PID_TAG_MESSAGE_DELIVERY_TIME,
        filetime_from_rfc3339_utc(&email.received_at) as i64,
    );
    assert_variable_property(&buffer, PID_TAG_SENDER_NAME_W, &utf16z("Alice"));
    assert_absent_property(&buffer, PID_TAG_SENDER_ADDRESS_TYPE_W);
    assert_absent_property(&buffer, PID_TAG_SENDER_EMAIL_ADDRESS_W);
    assert_absent_property(&buffer, PID_TAG_SENT_REPRESENTING_NAME_W);
    assert_absent_property(&buffer, PID_TAG_SENT_REPRESENTING_ADDRESS_TYPE_W);
    assert_absent_property(&buffer, PID_TAG_SENT_REPRESENTING_EMAIL_ADDRESS_W);
    assert_absent_property(&buffer, PID_TAG_MESSAGE_CLASS_W);
    assert_absent_property(&buffer, PID_TAG_SUBJECT_W);
    assert_absent_property(&buffer, PID_TAG_BODY_W);
}

#[test]
fn direct_fast_transfer_uses_persisted_normal_message_identity_properties() {
    let email = test_email();
    let durable_identity = crate::store::MapiIdentityRecord {
        object_kind: crate::store::MapiIdentityObjectKind::Message,
        canonical_id: email.id,
        object_id: crate::mapi::identity::mapi_store_id(0x1234),
        change_number: 0x5678,
        source_key: vec![0x11; 22],
        change_key: vec![0x22; 22],
        predecessor_change_list: vec![0x16, 0x33, 0x44, 0x55],
        last_modification_time: 133_987_654_321_000_000,
    };
    let identity_property_tags = [
        PID_TAG_SOURCE_KEY,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_LAST_MODIFICATION_TIME,
    ];

    let copy_to = fast_transfer_message_content_buffer_with_attachments(
        &email,
        &[],
        Some(&durable_identity),
        FastTransferDirectPropertyFilter::CopyToExcluding(&[]),
        FastTransferMessageChildren::new(false, false),
    );
    assert_variable_property(&copy_to, PID_TAG_SOURCE_KEY, &durable_identity.source_key);
    assert_variable_property(&copy_to, PID_TAG_CHANGE_KEY, &durable_identity.change_key);
    assert_variable_property(
        &copy_to,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        &durable_identity.predecessor_change_list,
    );
    assert_i64_property(
        &copy_to,
        PID_TAG_LAST_MODIFICATION_TIME,
        durable_identity.last_modification_time as i64,
    );
    assert_absent_property(&copy_to, PID_TAG_MID);
    assert_absent_property(&copy_to, PID_TAG_CHANGE_NUMBER);

    let copy_properties = fast_transfer_message_content_buffer_with_attachments(
        &email,
        &[],
        Some(&durable_identity),
        FastTransferDirectPropertyFilter::CopyPropertiesIncluding(&identity_property_tags),
        FastTransferMessageChildren::new(false, false),
    );
    assert_variable_property(
        &copy_properties,
        PID_TAG_SOURCE_KEY,
        &durable_identity.source_key,
    );
    assert_variable_property(
        &copy_properties,
        PID_TAG_CHANGE_KEY,
        &durable_identity.change_key,
    );
    assert_variable_property(
        &copy_properties,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        &durable_identity.predecessor_change_list,
    );
    assert_i64_property(
        &copy_properties,
        PID_TAG_LAST_MODIFICATION_TIME,
        durable_identity.last_modification_time as i64,
    );

    let excluded_change_key = fast_transfer_message_content_buffer_with_attachments(
        &email,
        &[],
        Some(&durable_identity),
        FastTransferDirectPropertyFilter::CopyToExcluding(&[PID_TAG_CHANGE_KEY]),
        FastTransferMessageChildren::new(false, false),
    );
    assert_variable_property(
        &excluded_change_key,
        PID_TAG_SOURCE_KEY,
        &durable_identity.source_key,
    );
    assert_absent_property(&excluded_change_key, PID_TAG_CHANGE_KEY);
}

#[test]
fn microsoft_oxcfxics_fast_transfer_copy_fai_uses_message_content_root() {
    let mailbox_id = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap();
    let canonical_id = Uuid::parse_str("99999999-9999-9999-9999-999999999990").unwrap();
    let item_id = crate::mapi::identity::mapi_store_id(90);
    let persisted_search_key = vec![
        0xff, 0xee, 0xdd, 0xcc, 0xbb, 0xaa, 0x99, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11,
        0x00,
    ];
    crate::mapi::identity::remember_mapi_identity(canonical_id, item_id);
    let entry_id = crate::mapi::identity::message_entry_id_from_object_ids(
        mailbox_id,
        crate::mapi::identity::INBOX_FOLDER_ID,
        item_id,
    )
    .unwrap();
    let parent_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        mailbox_id,
        crate::mapi::identity::INBOX_FOLDER_ID,
    )
    .unwrap();
    let special = SpecialMessageSyncFact {
        folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
        item_id,
        canonical_id,
        associated: true,
        subject: "Outlook Inbox view state".to_string(),
        body_text: Some("Client view payload".to_string()),
        message_class: "IPM.Configuration.MessageListSettings".to_string(),
        last_modified_filetime: filetime_from_rfc3339_utc("2026-05-19T10:00:00Z"),
        message_size: 19,
        read_state: None,
        recipients: Vec::new(),
        named_properties: vec![
            (
                PID_TAG_SEARCH_KEY,
                SpecialMessagePropertyValue::Binary(persisted_search_key.clone()),
            ),
            (
                0x7C08_0102,
                SpecialMessagePropertyValue::Binary(b"view-extra".to_vec()),
            ),
        ],
        named_property_definitions: Default::default(),
    };
    let buffer = fast_transfer_message_content_buffer_with_special_object(
        Some(&entry_id),
        Some(&parent_entry_id),
        &special,
        0x00,
        FastTransferDirectPropertyFilter::All,
        FastTransferMessageChildren::all(),
    );

    assert_tag_sequence(
        &buffer,
        &[
            PID_TAG_SOURCE_KEY,
            PID_TAG_MESSAGE_CLASS_W,
            PID_TAG_BODY_W,
            0x7C08_0102,
        ],
    );
    assert!(buffer.starts_with(&PID_TAG_SOURCE_KEY.to_le_bytes()));
    assert_absent_property(&buffer, PID_TAG_PARENT_SOURCE_KEY);
    assert!(!buffer
        .windows(4)
        .any(|window| window == FastTransferMarker::StartFAIMsg.as_u32().to_le_bytes()));
    assert!(!buffer
        .windows(4)
        .any(|window| window == END_MESSAGE.to_le_bytes()));
    assert!(!buffer.starts_with(b"LPE-MAPI-FASTTRANSFER\0"));
    assert!(!buffer
        .windows(4)
        .any(|window| window == PID_TAG_ASSOCIATED.to_le_bytes()));
    assert!(!buffer
        .windows(4)
        .any(|window| window == PID_TAG_MID.to_le_bytes()));
    assert_variable_property_present(&buffer, PID_TAG_ENTRY_ID, &entry_id);
    assert_variable_property_present(&buffer, PID_TAG_PARENT_ENTRY_ID, &parent_entry_id);
    assert_i32_property(&buffer, PID_TAG_MESSAGE_FLAGS, MSGFLAG_FAI as i32);
    assert_variable_property_present(
        &buffer,
        PID_TAG_SUBJECT_W,
        &utf16z("Outlook Inbox view state"),
    );
    assert_variable_property_present(
        &buffer,
        PID_TAG_NORMALIZED_SUBJECT_A,
        b"Outlook Inbox view state\0",
    );
    assert!(!buffer
        .windows(4)
        .any(|window| window == PID_TAG_NORMALIZED_SUBJECT_W.to_le_bytes()));
    assert_variable_property_present(&buffer, PID_TAG_BODY_W, &utf16z("Client view payload"));
    assert_variable_property_present(&buffer, 0x7C08_0102, b"view-extra");
    let mut encoded_search_key = PID_TAG_SEARCH_KEY.to_le_bytes().to_vec();
    encoded_search_key.extend_from_slice(&(persisted_search_key.len() as u32).to_le_bytes());
    encoded_search_key.extend_from_slice(&persisted_search_key);
    assert_eq!(
        buffer
            .windows(encoded_search_key.len())
            .filter(|window| *window == encoded_search_key)
            .count(),
        1,
        "CopyTo must preserve exactly one existing SearchKey"
    );

    let outlook_buffer = fast_transfer_message_content_buffer_with_special_object(
        Some(&entry_id),
        Some(&parent_entry_id),
        &special,
        0x09,
        FastTransferDirectPropertyFilter::All,
        FastTransferMessageChildren::all(),
    );
    let empty_message_children = [
        0x03, 0x00, 0x16, 0x40, // MetaTagFXDelProp.
        0x0D, 0x00, 0x12, 0x0E, // PidTagMessageRecipients.
        0x03, 0x00, 0x16, 0x40, // MetaTagFXDelProp.
        0x0D, 0x00, 0x13, 0x0E, // PidTagMessageAttachments.
    ];
    assert!(outlook_buffer.ends_with(&empty_message_children));
    assert_i32_property(&outlook_buffer, PID_TAG_MESSAGE_FLAGS, MSGFLAG_FAI as i32);
    assert_variable_property_present(
        &outlook_buffer,
        PID_TAG_NORMALIZED_SUBJECT_W,
        &utf16z("Outlook Inbox view state"),
    );
    assert!(!outlook_buffer
        .windows(4)
        .any(|window| window == PID_TAG_NORMALIZED_SUBJECT_A.to_le_bytes()));

    let no_children_buffer = fast_transfer_message_content_buffer_with_special_object(
        Some(&entry_id),
        Some(&parent_entry_id),
        &special,
        0x09,
        FastTransferDirectPropertyFilter::All,
        FastTransferMessageChildren::new(false, false),
    );
    assert!(!no_children_buffer
        .windows(4)
        .any(|window| window == 0x4016_0003u32.to_le_bytes()));

    let persisted_parent_source_key =
        crate::mapi::identity::source_key_for_object_id(crate::mapi::identity::INBOX_FOLDER_ID);
    let mut special_with_persisted_parent = special.clone();
    special_with_persisted_parent.named_properties.push((
        PID_TAG_PARENT_SOURCE_KEY,
        SpecialMessagePropertyValue::Binary(persisted_parent_source_key.clone()),
    ));
    let persisted_parent_buffer = fast_transfer_message_content_buffer_with_special_object(
        Some(&entry_id),
        Some(&parent_entry_id),
        &special_with_persisted_parent,
        0x09,
        FastTransferDirectPropertyFilter::All,
        FastTransferMessageChildren::all(),
    );
    assert_variable_property_present(
        &persisted_parent_buffer,
        PID_TAG_PARENT_SOURCE_KEY,
        &persisted_parent_source_key,
    );

    let mut normal = special;
    normal.associated = false;
    let normal_buffer = fast_transfer_message_content_buffer_with_special_object(
        None,
        Some(&parent_entry_id),
        &normal,
        0x09,
        FastTransferDirectPropertyFilter::All,
        FastTransferMessageChildren::all(),
    );
    assert!(!normal_buffer
        .windows(4)
        .any(|window| window == PID_TAG_ASSOCIATED.to_le_bytes()));
    assert!(!normal_buffer
        .windows(4)
        .any(|window| window == PID_TAG_PARENT_SOURCE_KEY.to_le_bytes()));
    assert_i32_property(&normal_buffer, PID_TAG_MESSAGE_FLAGS, MSGFLAG_READ as i32);
}

#[test]
fn outlook_fai_copyto_generates_a_mapiuid_search_key() {
    let mailbox_id = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap();
    let canonical_id = Uuid::parse_str("ec2adc4b-4cc5-65fc-dcad-11588e3a88c6").unwrap();
    let item_id = crate::mapi::identity::mapi_store_id(602);
    crate::mapi::identity::remember_mapi_identity(canonical_id, item_id);
    let entry_id = crate::mapi::identity::message_entry_id_from_object_ids(
        mailbox_id,
        crate::mapi::identity::INBOX_FOLDER_ID,
        item_id,
    )
    .unwrap();
    let parent_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        mailbox_id,
        crate::mapi::identity::INBOX_FOLDER_ID,
    )
    .unwrap();
    let special = SpecialMessageSyncFact {
        folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
        item_id,
        canonical_id,
        associated: true,
        subject: "IPM.Configuration.MessageListSettings".to_string(),
        body_text: None,
        message_class: "IPM.Configuration.MessageListSettings".to_string(),
        last_modified_filetime: filetime_from_rfc3339_utc("2026-07-26T11:09:21Z"),
        message_size: 998,
        read_state: None,
        recipients: Vec::new(),
        named_properties: vec![
            (
                PID_TAG_MESSAGE_FLAGS,
                SpecialMessagePropertyValue::U32(0x0000_0449),
            ),
            (0x7C06_0003, SpecialMessagePropertyValue::U32(0)),
        ],
        named_property_definitions: Default::default(),
    };

    let buffer = fast_transfer_message_content_buffer_with_special_object(
        Some(&entry_id),
        Some(&parent_entry_id),
        &special,
        0x09,
        FastTransferDirectPropertyFilter::All,
        FastTransferMessageChildren::all(),
    );

    // Microsoft MAPIUID is the 16-byte message search identity used by
    // PidTagSearchKey. The 202607261508 Outlook collector instead received
    // LPE's 22-byte SourceKey XID in this direct CopyTo projection.
    assert_variable_property_present(&buffer, PID_TAG_SEARCH_KEY, canonical_id.as_bytes());
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
        vec![META_TAG_CNSET_SEEN, META_TAG_IDSET_GIVEN]
    );
    assert!(summary.final_state_expected_property_order_ok);
    assert_eq!(summary.final_state_property_lengths, vec![30, 30]);
    assert_eq!(summary.final_state_idset_given_len, 30);
    assert_eq!(summary.final_state_cnset_seen_len, 30);
    assert_eq!(summary.final_state_idset_given_counters, vec![5]);
    assert_eq!(summary.final_state_cnset_seen_counters, vec![42]);
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
        .contains("ranges=42"));
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
        .contains("ranges=4,42"));
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
fn hierarchy_download_selection_uses_uploaded_empty_client_state() {
    let inbox = virtual_special_mailbox(crate::mapi::identity::INBOX_FOLDER_ID)
        .expect("virtual Inbox folder");
    let manifest = sync_manifest_buffer_with_attachments(
        SYNC_TYPE_HIERARCHY,
        0x0101,
        0x0000_0001,
        &[
            PID_TAG_FOLDER_TYPE,
            PID_TAG_CONTENT_COUNT,
            PID_TAG_CONTENT_UNREAD_COUNT,
            PID_TAG_MESSAGE_SIZE,
            PID_TAG_ACCESS,
            0x3FE0_0102, // PidTagMappingSignature
            PID_TAG_RECORD_KEY,
            0x0E27_0102, // PidTagOrdinalMost
        ],
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
        std::slice::from_ref(&inbox),
        &[],
        &[],
        &[],
        1,
    );

    let (selected, final_state) = select_download_manifest_for_client_state(
        SYNC_TYPE_HIERARCHY,
        0x0101,
        &manifest,
        &initial_sync_state_stream(SYNC_TYPE_HIERARCHY),
        &[DownloadChangeFact {
            object_id: crate::mapi::identity::INBOX_FOLDER_ID,
            change_number: canonical_hierarchy_change_number(
                crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
                &inbox,
            ),
            associated: false,
            source_key: source_key_for_store_id(crate::mapi::identity::INBOX_FOLDER_ID),
        }],
        &[],
    )
    .expect("select hierarchy baseline from empty client state");

    let summary = decode_hierarchy_transfer_debug_summary(&selected).unwrap();
    assert_eq!(summary.folder_change_count, 1);
    assert_eq!(summary.first_folder_name(), "Inbox");
    assert_eq!(summary.final_state_idset_given_counters, vec![5]);
    assert_eq!(summary.final_state_cnset_seen_counters, vec![5]);
    assert!(selected.ends_with(&INCR_SYNC_END.to_le_bytes()));
    assert!(!final_state.ends_with(&INCR_SYNC_END.to_le_bytes()));
}

#[test]
fn hierarchy_download_selection_preserves_foreign_replica_and_uses_local_cnset() {
    let inbox = virtual_special_mailbox(crate::mapi::identity::INBOX_FOLDER_ID)
        .expect("virtual Inbox folder");
    let object_counter =
        crate::mapi::identity::global_counter_from_store_id(crate::mapi::identity::INBOX_FOLDER_ID)
            .expect("local Inbox GLOBCNT");
    let change_number =
        canonical_hierarchy_change_number(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID, &inbox);
    let foreign_guid = [0x11; 16];
    assert!(foreign_guid < STORE_REPLICA_GUID);
    let replguid_singletons = |replicas: &[([u8; 16], u64)]| {
        let mut value = Vec::new();
        for (guid, counter) in replicas {
            value.extend_from_slice(guid);
            value.push(GLOBSET_RANGE_COMMAND);
            value.extend_from_slice(&globcnt_bytes(*counter));
            value.extend_from_slice(&globcnt_bytes(*counter));
            value.push(GLOBSET_END_COMMAND);
        }
        value
    };
    let idset_given = replguid_singletons(&[
        (foreign_guid, object_counter),
        (STORE_REPLICA_GUID, object_counter),
    ]);
    let foreign_cnset_seen = replguid_singletons(&[(foreign_guid, change_number)]);
    let expected_cnset_seen = replguid_singletons(&[
        (foreign_guid, change_number),
        (STORE_REPLICA_GUID, change_number),
    ]);
    let client_state = sync_state_stream_with_uploaded_property(
        SYNC_TYPE_HIERARCHY,
        &initial_sync_state_stream(SYNC_TYPE_HIERARCHY),
        META_TAG_IDSET_GIVEN,
        &idset_given,
    );
    let client_state = sync_state_stream_with_uploaded_property(
        SYNC_TYPE_HIERARCHY,
        &client_state,
        META_TAG_CNSET_SEEN,
        &foreign_cnset_seen,
    );
    let manifest = sync_manifest_buffer_with_attachments(
        SYNC_TYPE_HIERARCHY,
        0x0101,
        0,
        &[],
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
        std::slice::from_ref(&inbox),
        &[],
        &[],
        &[],
        1,
    );
    let facts = [DownloadChangeFact {
        object_id: crate::mapi::identity::INBOX_FOLDER_ID,
        change_number,
        associated: false,
        source_key: source_key_for_store_id(crate::mapi::identity::INBOX_FOLDER_ID),
    }];

    // [MS-OXCFXICS] sections 2.2.2.4.2 and 3.2.5.3: XIDs with the
    // same GLOBCNT remain distinct by REPLGUID, and the local replica's
    // CnsetSeen determines whether the local server change is downloaded.
    let (selected, final_state) = select_download_manifest_for_client_state(
        SYNC_TYPE_HIERARCHY,
        0x0101,
        &manifest,
        &client_state,
        &facts,
        &[],
    )
    .expect("select change unseen by the local replica set");
    assert_eq!(
        decode_hierarchy_transfer_debug_summary(&selected)
            .unwrap()
            .folder_change_count,
        1
    );
    assert_variable_property(&final_state, META_TAG_IDSET_GIVEN, &idset_given);
    assert_variable_property(&final_state, META_TAG_CNSET_SEEN, &expected_cnset_seen);

    let (selected_again, final_state_again) = select_download_manifest_for_client_state(
        SYNC_TYPE_HIERARCHY,
        0x0101,
        &manifest,
        &final_state,
        &facts,
        &[],
    )
    .expect("select change already seen by the local replica set");
    assert_eq!(
        decode_hierarchy_transfer_debug_summary(&selected_again)
            .unwrap()
            .folder_change_count,
        0
    );
    assert_variable_property(&final_state_again, META_TAG_IDSET_GIVEN, &idset_given);
    assert_variable_property(
        &final_state_again,
        META_TAG_CNSET_SEEN,
        &expected_cnset_seen,
    );
}

#[test]
fn hierarchy_download_no_deletions_keeps_missing_id_without_tombstone() {
    let missing_id = crate::mapi::identity::mapi_store_id(0x1234);
    let client_idset = replguid_idset_from_counters(&[0x1234]);
    let client_state = sync_state_stream_with_uploaded_property(
        SYNC_TYPE_HIERARCHY,
        &initial_sync_state_stream(SYNC_TYPE_HIERARCHY),
        META_TAG_IDSET_GIVEN,
        &client_idset,
    );
    let manifest = sync_manifest_buffer_with_attachments(
        SYNC_TYPE_HIERARCHY,
        0x0002, // NoDeletions
        0,
        &[],
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
        &[],
        &[],
        &[],
        &[],
        1,
    );

    // [MS-OXCFXICS] section 3.2.5.3: NoDeletions suppresses deletion
    // output, so a client ID missing from the current scope stays in IdsetGiven.
    let (selected, final_state) = select_download_manifest_for_client_state(
        SYNC_TYPE_HIERARCHY,
        0x0002,
        &manifest,
        &client_state,
        &[],
        &[],
    )
    .expect("select hierarchy with NoDeletions");
    assert_absent_property(&selected, META_TAG_IDSET_DELETED);
    assert_variable_property(&final_state, META_TAG_IDSET_GIVEN, &client_idset);
    assert_eq!(
        crate::mapi::identity::global_counter_from_store_id(missing_id),
        Some(0x1234)
    );
}

#[test]
fn hierarchy_download_emits_explicit_tombstone_absent_from_client_idset() {
    let deleted_id = crate::mapi::identity::mapi_store_id(0x2345);
    let expected_tombstone = replid_idset_from_object_ids(&[deleted_id]);
    let manifest = sync_manifest_buffer_with_attachments(
        SYNC_TYPE_HIERARCHY,
        0x0100,
        0,
        &[],
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
        &[],
        &[],
        &[],
        &[deleted_id],
        1,
    );

    // [MS-OXCFXICS] section 3.2.5.3: an explicit hierarchy deletion is
    // emitted even when that folder was absent from the uploaded IdsetGiven.
    let (selected, final_state) = select_download_manifest_for_client_state(
        SYNC_TYPE_HIERARCHY,
        0x0100,
        &manifest,
        &initial_sync_state_stream(SYNC_TYPE_HIERARCHY),
        &[],
        &[],
    )
    .expect("select explicit hierarchy tombstone");
    let deletion_offset = selected
        .windows(4)
        .position(|bytes| bytes == INCR_SYNC_DEL.to_le_bytes())
        .expect("IncrSyncDel marker");
    assert_eq!(
        u32::from_le_bytes(
            selected[deletion_offset + 4..deletion_offset + 8]
                .try_into()
                .unwrap()
        ),
        0x67E5_0102,
        "[MS-OXCFXICS] section 2.2.1.3.1 MetaTagIdsetDeleted"
    );
    assert_variable_property_present(&selected, META_TAG_IDSET_DELETED, &expected_tombstone);
    assert_variable_property(&final_state, META_TAG_IDSET_GIVEN, &[]);
    let summary = decode_hierarchy_transfer_debug_summary(&selected)
        .expect("decode hierarchy stream containing IncrSyncDel");
    assert!(summary.stream_end_marker_seen);
}

#[test]
fn hierarchy_download_rejects_malformed_client_globset() {
    let mut malformed_cnset = STORE_REPLICA_GUID.to_vec();
    malformed_cnset.push(GLOBSET_RANGE_COMMAND);
    let client_state = sync_state_stream_with_uploaded_property(
        SYNC_TYPE_HIERARCHY,
        &initial_sync_state_stream(SYNC_TYPE_HIERARCHY),
        META_TAG_CNSET_SEEN,
        &malformed_cnset,
    );
    let manifest = sync_manifest_buffer_with_attachments(
        SYNC_TYPE_HIERARCHY,
        0x0100,
        0,
        &[],
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
        &[],
        &[],
        &[],
        &[],
        1,
    );

    // [MS-OXCFXICS] section 2.2.2.4.2: every REPLGUID is followed by a
    // complete GLOBSET; a truncated range is not a valid client state.
    let error = select_download_manifest_for_client_state(
        SYNC_TYPE_HIERARCHY,
        0x0100,
        &manifest,
        &client_state,
        &[],
        &[],
    )
    .expect_err("reject truncated client GLOBSET");
    assert_eq!(error, "truncated GLOBSET range low value");
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
        vec![META_TAG_CNSET_SEEN, META_TAG_IDSET_GIVEN]
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
fn hierarchy_transfer_inbox_includes_calendar_identification_entry_id() {
    let account_id = Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").unwrap();
    let inbox = virtual_special_mailbox(crate::mapi::identity::INBOX_FOLDER_ID)
        .expect("virtual inbox folder");
    let calendar_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
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
        std::slice::from_ref(&inbox),
        &[],
        &[],
        &[],
        std::slice::from_ref(&inbox),
        std::slice::from_ref(&inbox),
        &[],
        &[],
        &[],
        &[],
        1,
    );

    // [MS-OXOSFLD] section 2.2.3 and [MS-OXCFXICS] section 2.2.4.3.5:
    // the owner Inbox folderChange carries its Calendar identification property.
    assert_variable_property(&buffer, 0x36D0_0102, &calendar_entry_id);
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
        body_text: Some("Remember this".to_string()),
        message_class: "IPM.StickyNote".to_string(),
        last_modified_filetime: filetime_from_rfc3339_utc("2026-05-19T10:00:00Z"),
        message_size: 19,
        read_state: None,
        recipients: Vec::new(),
        named_properties: vec![(0x8B00_0003, SpecialMessagePropertyValue::I32(3))],
        named_property_definitions: Default::default(),
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
        body_text: None,
        message_class: "IPM.Microsoft.WunderBar.Link".to_string(),
        last_modified_filetime: filetime_from_rfc3339_utc("2026-05-19T10:00:00Z"),
        message_size: 0,
        read_state: None,
        recipients: Vec::new(),
        named_properties: vec![(
            PID_TAG_MESSAGE_FLAGS,
            SpecialMessagePropertyValue::I32(0x09),
        )],
        named_property_definitions: Default::default(),
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
    assert_eq!(summary.fai_items[0].message_flags, Some(0x49));
    assert_eq!(
        buffer
            .windows(4)
            .filter(|window| *window == PID_TAG_MESSAGE_FLAGS.to_le_bytes())
            .count(),
        1
    );
    assert!(!buffer
        .windows(4)
        .any(|window| window == PID_TAG_BODY_W.to_le_bytes()));
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
fn fai_foreign_source_key_identity_is_used_by_selected_and_full_idset_given() {
    let canonical_id = Uuid::parse_str("99999999-9999-4999-8999-999999999397").unwrap();
    let item_id = crate::mapi::identity::mapi_store_id(397);
    crate::mapi::identity::remember_mapi_identity(canonical_id, item_id);
    let foreign_guid = [0x31; 16];
    let foreign_counter = 0x0000_1020_3040_5060;
    let mut foreign_source_key = foreign_guid.to_vec();
    foreign_source_key.extend_from_slice(&globcnt_bytes(foreign_counter));
    assert_ne!(foreign_guid, STORE_REPLICA_GUID);
    assert_ne!(
        crate::mapi::identity::global_counter_from_store_id(item_id),
        Some(foreign_counter)
    );
    let special = SpecialMessageSyncFact {
        folder_id: crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
        item_id,
        canonical_id,
        associated: true,
        subject: "Foreign persisted FAI view".to_string(),
        body_text: Some(String::new()),
        message_class: "IPM.Microsoft.FolderDesign.NamedView".to_string(),
        last_modified_filetime: filetime_from_rfc3339_utc("2026-07-20T21:36:00Z"),
        message_size: 256,
        read_state: None,
        recipients: Vec::new(),
        named_properties: vec![(
            PID_TAG_SOURCE_KEY,
            SpecialMessagePropertyValue::Binary(foreign_source_key.clone()),
        )],
        named_property_definitions: Default::default(),
    };
    let manifest = sync_manifest_buffer_with_special_objects_and_final_state(
        Uuid::nil(),
        SYNC_TYPE_CONTENTS,
        SYNC_FLAG_FAI,
        SYNC_EXTRA_FLAG_EID | SYNC_EXTRA_FLAG_CHANGE_NUMBER,
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
    let facts = download_change_facts(
        SYNC_TYPE_CONTENTS,
        SYNC_FLAG_FAI,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
        &[],
        &[],
        &[],
        std::slice::from_ref(&special),
        &[],
    );
    let (_, selected_state) = select_download_manifest_for_client_state(
        SYNC_TYPE_CONTENTS,
        SYNC_FLAG_FAI,
        &manifest,
        &initial_sync_state_stream(SYNC_TYPE_CONTENTS),
        &facts,
        &[],
    )
    .expect("select FAI with its persisted foreign SourceKey");
    let mut expected_idset_given = foreign_guid.to_vec();
    expected_idset_given.push(GLOBSET_RANGE_COMMAND);
    expected_idset_given.extend_from_slice(&globcnt_bytes(foreign_counter));
    expected_idset_given.extend_from_slice(&globcnt_bytes(foreign_counter));
    expected_idset_given.push(GLOBSET_END_COMMAND);

    // [MS-OXCFXICS] sections 2.2.1.1.1, 2.2.1.2.5, 2.2.2.4.2,
    // and 3.2.5.3: change.Id is the GID carried by the emitted SourceKey;
    // IdsetGivenC adds that exact REPLGUID/GLOBCNT identity.
    assert_variable_property(&selected_state, META_TAG_IDSET_GIVEN, &expected_idset_given);
    assert_variable_property(&manifest, PID_TAG_SOURCE_KEY, &foreign_source_key);
    assert_variable_property(&manifest, META_TAG_IDSET_GIVEN, &expected_idset_given);
}

#[test]
fn normal_message_no_foreign_identifiers_uses_local_source_key_for_selection() {
    let canonical_id = Uuid::parse_str("99999999-9999-4999-8999-999999999398").unwrap();
    let object_id = crate::mapi::identity::mapi_store_id(398);
    let foreign_guid = [0x31; 16];
    let foreign_counter = 0x0000_1020_3040_5061;
    let mut foreign_source_key = foreign_guid.to_vec();
    foreign_source_key.extend_from_slice(&globcnt_bytes(foreign_counter));
    crate::mapi::identity::remember_mapi_identity_with_source_key(
        canonical_id,
        object_id,
        Some(foreign_source_key.clone()),
    );
    let mut email = test_email();
    email.id = canonical_id;
    let local_source_key = source_key_for_store_id(object_id);
    let local_counter = crate::mapi::identity::global_counter_from_store_id(object_id).unwrap();
    let replguid_singleton = |replica_guid: [u8; 16], counter: u64| {
        let mut value = replica_guid.to_vec();
        value.push(GLOBSET_RANGE_COMMAND);
        value.extend_from_slice(&globcnt_bytes(counter));
        value.extend_from_slice(&globcnt_bytes(counter));
        value.push(GLOBSET_END_COMMAND);
        value
    };
    let local_idset = replguid_singleton(STORE_REPLICA_GUID, local_counter);
    let foreign_idset = replguid_singleton(foreign_guid, foreign_counter);
    let no_foreign_flags = SYNC_FLAG_NORMAL | SYNC_FLAG_NO_FOREIGN_IDENTIFIERS;
    let manifest = sync_manifest_buffer_with_attachments(
        SYNC_TYPE_CONTENTS,
        no_foreign_flags,
        0,
        &[],
        crate::mapi::identity::INBOX_FOLDER_ID,
        &[],
        std::slice::from_ref(&email),
        &[],
        &[],
        1,
    );
    let facts = download_change_facts(
        SYNC_TYPE_CONTENTS,
        no_foreign_flags,
        crate::mapi::identity::INBOX_FOLDER_ID,
        &[],
        std::slice::from_ref(&email),
        &[],
        &[],
        &[],
    );
    let (_, selected_state) = select_download_manifest_for_client_state(
        SYNC_TYPE_CONTENTS,
        no_foreign_flags,
        &manifest,
        &initial_sync_state_stream(SYNC_TYPE_CONTENTS),
        &facts,
        &[],
    )
    .expect("select normal message with a local NoForeignIdentifiers SourceKey");

    assert_ne!(local_source_key, foreign_source_key);
    assert_variable_property(&manifest, PID_TAG_SOURCE_KEY, &local_source_key);
    assert_variable_property(&manifest, META_TAG_IDSET_GIVEN, &local_idset);
    assert_variable_property(&selected_state, META_TAG_IDSET_GIVEN, &local_idset);

    let preserved_manifest = sync_manifest_buffer_with_attachments(
        SYNC_TYPE_CONTENTS,
        SYNC_FLAG_NORMAL,
        0,
        &[],
        crate::mapi::identity::INBOX_FOLDER_ID,
        &[],
        std::slice::from_ref(&email),
        &[],
        &[],
        1,
    );
    assert_variable_property(&preserved_manifest, PID_TAG_SOURCE_KEY, &foreign_source_key);
    assert_variable_property(&preserved_manifest, META_TAG_IDSET_GIVEN, &foreign_idset);
}

#[test]
fn microsoft_oxcfxics_fai_content_sync_delimits_empty_child_collections_before_next_marker() {
    let first_canonical_id = Uuid::parse_str("99999999-9999-9999-9999-999999999395").unwrap();
    let second_canonical_id = Uuid::parse_str("99999999-9999-9999-9999-999999999396").unwrap();
    let first_item_id = crate::mapi::identity::mapi_store_id(395);
    let second_item_id = crate::mapi::identity::mapi_store_id(396);
    crate::mapi::identity::remember_mapi_identity(first_canonical_id, first_item_id);
    crate::mapi::identity::remember_mapi_identity(second_canonical_id, second_item_id);
    let final_property_tag = 0x7C06_0003;
    let first_property_value = 3i32;
    let second_property_value = 4i32;
    let specials = [
        SpecialMessageSyncFact {
            folder_id: crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
            item_id: first_item_id,
            canonical_id: first_canonical_id,
            associated: true,
            subject: "Compact".to_string(),
            body_text: Some(String::new()),
            message_class: "IPM.Microsoft.FolderDesign.NamedView".to_string(),
            last_modified_filetime: filetime_from_rfc3339_utc("2026-05-19T10:00:00Z"),
            message_size: 2_048,
            read_state: None,
            recipients: Vec::new(),
            named_properties: vec![(
                final_property_tag,
                SpecialMessagePropertyValue::I32(first_property_value),
            )],
            named_property_definitions: Default::default(),
        },
        SpecialMessageSyncFact {
            folder_id: crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
            item_id: second_item_id,
            canonical_id: second_canonical_id,
            associated: true,
            subject: "Sent To".to_string(),
            body_text: Some(String::new()),
            message_class: "IPM.Microsoft.FolderDesign.NamedView".to_string(),
            last_modified_filetime: filetime_from_rfc3339_utc("2026-05-19T10:01:00Z"),
            message_size: 1_984,
            read_state: None,
            recipients: Vec::new(),
            named_properties: vec![(
                final_property_tag,
                SpecialMessagePropertyValue::I32(second_property_value),
            )],
            named_property_definitions: Default::default(),
        },
    ];
    let buffer = sync_manifest_buffer_with_special_objects_and_final_state(
        Uuid::nil(),
        SYNC_TYPE_CONTENTS,
        SYNC_FLAG_FAI | SYNC_FLAG_PROGRESS,
        SYNC_EXTRA_FLAG_EID | SYNC_EXTRA_FLAG_MESSAGE_SIZE | SYNC_EXTRA_FLAG_CHANGE_NUMBER,
        &[],
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
        &[],
        &[],
        &[],
        &specials,
        &[],
        &[],
        &[],
        &[],
        &[],
        &specials,
        &[],
        &[],
        1,
    );

    // [MS-OXCFXICS] sections 2.2.4.3.12 and 3.2.5.10: included empty child
    // collections are delimited with MetaTagFXDelProp before the next marker;
    // a null property tag is not an item terminator in the FastTransfer grammar.
    let empty_message_children = [
        META_TAG_FX_DEL_PROP.to_le_bytes(),
        PID_TAG_MESSAGE_RECIPIENTS.to_le_bytes(),
        META_TAG_FX_DEL_PROP.to_le_bytes(),
        PID_TAG_MESSAGE_ATTACHMENTS.to_le_bytes(),
    ]
    .concat();
    let first_expected_boundary = [
        final_property_tag.to_le_bytes().as_slice(),
        first_property_value.to_le_bytes().as_slice(),
        empty_message_children.as_slice(),
        INCR_SYNC_PROGRESS_PER_MSG.to_le_bytes().as_slice(),
    ]
    .concat();
    let second_expected_boundary = [
        final_property_tag.to_le_bytes().as_slice(),
        second_property_value.to_le_bytes().as_slice(),
        empty_message_children.as_slice(),
        INCR_SYNC_STATE_BEGIN.to_le_bytes().as_slice(),
    ]
    .concat();
    assert!(
        contains_bytes(&buffer, &first_expected_boundary),
        "first FAI message must delimit its empty child collections before IncrSyncProgressPerMsg"
    );
    assert!(
        contains_bytes(&buffer, &second_expected_boundary),
        "last FAI message must delimit its empty child collections before IncrSyncStateBegin"
    );
}

#[test]
fn content_sync_manifest_unicode_fai_uses_unicode_subject_and_fai_message_flag() {
    let canonical_id = Uuid::parse_str("99999999-9999-9999-9999-999999999996").unwrap();
    let item_id = crate::mapi::identity::mapi_store_id(96);
    crate::mapi::identity::remember_mapi_identity(canonical_id, item_id);
    let special = SpecialMessageSyncFact {
        folder_id: crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
        item_id,
        canonical_id,
        associated: true,
        subject: "Calendar".to_string(),
        body_text: Some(String::new()),
        message_class: "IPM.Microsoft.WunderBar.Link".to_string(),
        last_modified_filetime: filetime_from_rfc3339_utc("2026-05-19T10:00:00Z"),
        message_size: 128,
        read_state: None,
        recipients: Vec::new(),
        named_properties: Vec::new(),
        named_property_definitions: Default::default(),
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
    assert_i32_property(&buffer, PID_TAG_MESSAGE_FLAGS, MSGFLAG_FAI as i32);
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
        body_text: Some("Filtered body".to_string()),
        message_class: "IPM.Appointment".to_string(),
        last_modified_filetime: filetime_from_rfc3339_utc("2026-05-19T10:00:00Z"),
        message_size: 19,
        read_state: None,
        recipients: Vec::new(),
        named_properties: vec![(0x8205_0003, SpecialMessagePropertyValue::I32(2))],
        named_property_definitions: Default::default(),
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
        body_text: Some(String::new()),
        message_class: "IPM.Appointment".to_string(),
        last_modified_filetime: filetime_from_rfc3339_utc("2026-05-19T10:00:00Z"),
        message_size: 19,
        read_state: None,
        recipients: Vec::new(),
        named_properties: Vec::new(),
        named_property_definitions: Default::default(),
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
        body_text: Some(String::new()),
        message_class: "IPM.Appointment".to_string(),
        last_modified_filetime: filetime_from_rfc3339_utc("2026-05-19T10:00:00Z"),
        message_size: 19,
        read_state: None,
        recipients: Vec::new(),
        named_properties: Vec::new(),
        named_property_definitions: Default::default(),
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
        body_text: Some(String::new()),
        message_class: "IPM.Appointment".to_string(),
        last_modified_filetime: filetime_from_rfc3339_utc("2026-05-19T10:00:00Z"),
        message_size: 19,
        read_state: None,
        recipients: Vec::new(),
        named_properties: Vec::new(),
        named_property_definitions: Default::default(),
    };
    let associated_object = SpecialMessageSyncFact {
        folder_id: crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
        item_id: associated_item_id,
        canonical_id: associated_canonical_id,
        associated: true,
        subject: "Associated view".to_string(),
        body_text: Some(String::new()),
        message_class: "IPM.Microsoft.WunderBar.Link".to_string(),
        last_modified_filetime: filetime_from_rfc3339_utc("2026-05-19T10:00:00Z"),
        message_size: 19,
        read_state: None,
        recipients: Vec::new(),
        named_properties: Vec::new(),
        named_property_definitions: Default::default(),
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

#[tokio::test]
async fn scoped_final_sync_state_uses_the_durable_inbox_counter() {
    let replica_guid = uuid::Uuid::from_u128(0x11223344_5566_7788_99aa_bbccddeeff00);
    let mut requests = Vec::new();
    let mut records = Vec::new();
    for counter in crate::mapi::identity::ROOT_FOLDER_COUNTER
        ..crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER
    {
        let canonical_id = uuid::Uuid::from_u128(counter as u128 + 1);
        let object_id = crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + counter - 1,
        );
        requests.push(crate::store::MapiIdentityRequest {
            object_kind: crate::store::MapiIdentityObjectKind::Mailbox,
            canonical_id,
            reserved_global_counter: Some(counter),
            source_key: None,
        });
        records.push(crate::store::MapiIdentityRecord {
            object_kind: crate::store::MapiIdentityObjectKind::Mailbox,
            canonical_id,
            object_id,
            change_number: 1,
            source_key: Vec::new(),
            change_key: Vec::new(),
            predecessor_change_list: Vec::new(),
            last_modification_time: 0,
        });
    }
    let codec = crate::mapi::identity::MapiIdentityCodec::from_special_folder_identity_records(
        replica_guid,
        &requests,
        &records,
    )
    .unwrap();
    let (token, idset, idset_counters) =
        crate::mapi::identity::with_current_mapi_identity_codec(codec, async {
            let idset = replguid_idset_from_object_ids(&[crate::mapi::identity::INBOX_FOLDER_ID]);
            let token = final_sync_state_stream(
                SYNC_TYPE_HIERARCHY,
                &[crate::mapi::identity::INBOX_FOLDER_ID],
                &[],
            );
            let idset_counters = replguid_globset_counters(&idset).unwrap();
            (token, idset, idset_counters)
        })
        .await;

    let durable_inbox_counter = crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER
        + crate::mapi::identity::INBOX_FOLDER_COUNTER
        - 1;
    assert_eq!(&idset[..16], replica_guid.as_bytes());
    assert_eq!(idset_counters, vec![durable_inbox_counter]);
    assert_variable_property(&token, META_TAG_IDSET_GIVEN, &idset);
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
fn hierarchy_and_content_cnsets_replay_in_globcnt_order_without_read_state_changes() {
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
    assert_variable_property(&content, META_TAG_CNSET_READ, &empty_cnset);
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
    assert_tag_sequence(
        &token,
        &[
            INCR_SYNC_STATE_BEGIN,
            META_TAG_CNSET_SEEN,
            META_TAG_CNSET_SEEN_FAI,
            META_TAG_IDSET_GIVEN,
            META_TAG_CNSET_READ,
            INCR_SYNC_STATE_END,
        ],
    );
}

#[test]
fn sync_state_writers_match_exchange_download_and_upload_orders() {
    let content_download = sync_state_stream_from_raw_properties(
        SYNC_TYPE_CONTENTS,
        b"given",
        b"seen",
        b"fai",
        b"read",
    );
    assert_tag_sequence(
        &content_download,
        &[
            INCR_SYNC_STATE_BEGIN,
            META_TAG_CNSET_SEEN,
            META_TAG_CNSET_SEEN_FAI,
            META_TAG_IDSET_GIVEN,
            META_TAG_CNSET_READ,
            INCR_SYNC_STATE_END,
        ],
    );

    let hierarchy_download =
        sync_state_stream_from_raw_properties(SYNC_TYPE_HIERARCHY, b"given", b"seen", &[], &[]);
    assert_tag_sequence(
        &hierarchy_download,
        &[
            INCR_SYNC_STATE_BEGIN,
            META_TAG_CNSET_SEEN,
            META_TAG_IDSET_GIVEN,
            INCR_SYNC_STATE_END,
        ],
    );

    let content_upload =
        upload_sync_state_stream_from_raw_properties(SYNC_TYPE_CONTENTS, b"seen", b"fai", b"read");
    assert_tag_sequence(
        &content_upload,
        &[
            INCR_SYNC_STATE_BEGIN,
            META_TAG_CNSET_SEEN,
            META_TAG_CNSET_SEEN_FAI,
            META_TAG_CNSET_READ,
            INCR_SYNC_STATE_END,
        ],
    );
    assert_absent_property(&content_upload, META_TAG_IDSET_GIVEN);
}

#[test]
fn special_message_headers_and_final_cnsets_share_durable_change_numbers() {
    let normal_canonical_id = Uuid::parse_str("81818181-8181-4181-8181-818181818181").unwrap();
    let fai_canonical_id = Uuid::parse_str("82828282-8282-4282-8282-828282828282").unwrap();
    let normal_item_id = crate::mapi::identity::mapi_store_id(81);
    let fai_item_id = crate::mapi::identity::mapi_store_id(82);
    let normal_change_number = 501;
    let fai_change_number = 777;
    assert_ne!(
        normal_change_number,
        change_number_for_store_id(normal_item_id)
    );
    assert_ne!(fai_change_number, change_number_for_store_id(fai_item_id));
    let normal = SpecialMessageSyncFact {
        folder_id: crate::mapi::identity::CALENDAR_FOLDER_ID,
        item_id: normal_item_id,
        canonical_id: normal_canonical_id,
        associated: false,
        subject: "Durable normal Event".to_string(),
        body_text: Some(String::new()),
        message_class: "IPM.Appointment".to_string(),
        last_modified_filetime: filetime_from_rfc3339_utc("2026-07-15T10:11:00Z"),
        message_size: 64,
        read_state: None,
        recipients: Vec::new(),
        named_properties: vec![
            (
                PID_TAG_CHANGE_NUMBER,
                SpecialMessagePropertyValue::U64(normal_change_number),
            ),
            (
                PID_TAG_CHANGE_KEY,
                SpecialMessagePropertyValue::Binary(change_key_for_change_number(
                    normal_change_number,
                )),
            ),
            (
                PID_TAG_PREDECESSOR_CHANGE_LIST,
                SpecialMessagePropertyValue::Binary(predecessor_change_list(normal_change_number)),
            ),
        ],
        named_property_definitions: Default::default(),
    };
    let fai = SpecialMessageSyncFact {
        folder_id: crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
        item_id: fai_item_id,
        canonical_id: fai_canonical_id,
        associated: true,
        subject: "Durable FAI".to_string(),
        body_text: Some(String::new()),
        message_class: "IPM.Microsoft.WunderBar.Link".to_string(),
        last_modified_filetime: filetime_from_rfc3339_utc("2026-07-15T10:12:00Z"),
        message_size: 64,
        read_state: None,
        recipients: Vec::new(),
        named_properties: vec![
            (
                PID_TAG_CHANGE_NUMBER,
                SpecialMessagePropertyValue::U64(fai_change_number),
            ),
            (
                PID_TAG_CHANGE_KEY,
                SpecialMessagePropertyValue::Binary(change_key_for_change_number(
                    fai_change_number,
                )),
            ),
            (
                PID_TAG_PREDECESSOR_CHANGE_LIST,
                SpecialMessagePropertyValue::Binary(predecessor_change_list(fai_change_number)),
            ),
        ],
        named_property_definitions: Default::default(),
    };
    let special_objects = [normal.clone(), fai];
    let buffer = sync_manifest_buffer_with_special_objects_and_final_state(
        Uuid::nil(),
        SYNC_TYPE_CONTENTS,
        SYNC_FLAG_NORMAL | SYNC_FLAG_FAI,
        SYNC_EXTRA_FLAG_EID | SYNC_EXTRA_FLAG_CHANGE_NUMBER,
        &[],
        crate::mapi::identity::CALENDAR_FOLDER_ID,
        &[],
        &[],
        &[],
        &special_objects,
        &[],
        &[],
        &[],
        &[],
        &[],
        &special_objects,
        &[],
        &[],
        1,
    );

    let summary = decode_content_transfer_fai_debug_summary(&buffer).unwrap();
    assert_eq!(summary.fai_items.len(), 1);
    let fai_header = summary
        .fai_items
        .iter()
        .find(|item| item.item_id == Some(fai_item_id))
        .unwrap();
    assert_eq!(fai_header.change_number, Some(fai_change_number));
    assert!(fai_header.change_number_in_final_cnset_fai);
    let normal_buffer = sync_manifest_buffer_with_special_objects_and_final_state(
        Uuid::nil(),
        SYNC_TYPE_CONTENTS,
        SYNC_FLAG_NORMAL,
        SYNC_EXTRA_FLAG_EID | SYNC_EXTRA_FLAG_CHANGE_NUMBER,
        &[],
        crate::mapi::identity::CALENDAR_FOLDER_ID,
        &[],
        &[],
        &[],
        std::slice::from_ref(&normal),
        &[],
        &[],
        &[],
        &[],
        &[],
        std::slice::from_ref(&normal),
        &[],
        &[],
        1,
    );
    assert_change_number_property(&normal_buffer, PID_TAG_CHANGE_NUMBER, normal_change_number);
    assert_variable_property(
        &buffer,
        META_TAG_CNSET_SEEN,
        &replguid_idset_from_counters(&[normal_change_number]),
    );
    assert_variable_property(
        &buffer,
        META_TAG_CNSET_SEEN_FAI,
        &replguid_idset_from_counters(&[fai_change_number]),
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

fn assert_i64_property(buffer: &[u8], property_tag: u32, value: i64) {
    let tag = property_tag.to_le_bytes();
    let offset = buffer
        .windows(tag.len())
        .position(|window| window == tag)
        .expect("property tag is present");
    assert_eq!(
        i64::from_le_bytes(buffer[offset + 4..offset + 12].try_into().unwrap()),
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

fn assert_named_lid_property(
    buffer: &[u8],
    property_tag: u32,
    guid: [u8; 16],
    lid: u32,
    encoded_value: &[u8],
) {
    let mut property_info = property_tag.to_le_bytes().to_vec();
    property_info.extend_from_slice(&guid);
    property_info.push(0x00);
    property_info.extend_from_slice(&lid.to_le_bytes());
    let offset = buffer
        .windows(property_info.len())
        .position(|window| window == property_info)
        .expect("named LID property and definition are present")
        + property_info.len();
    assert_eq!(&buffer[offset..offset + encoded_value.len()], encoded_value);
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
        calendar_invitation: false,
        calendar_meeting_request: None,
        calendar_meeting_response: None,
        size_octets: 42,
        internet_message_id: Some("<message@example.test>".to_string()),
        mime_blob_ref: None,
        delivery_status: "stored".to_string(),
    }
}

fn meeting_response_subject_test_email(method: &str, partstat: &str, prefix: &str) -> JmapEmail {
    let mut email = test_email();
    email.subject = format!("{prefix}Probe 10");
    email.calendar_meeting_response = Some(lpe_storage::CalendarMeetingResponse {
        method: method.to_string(),
        transport_attachment_id: None,
        server_processed: false,
        organizer: None,
        attendee_email: "denis.ducret@sdic.ch".to_string(),
        attendee_name: "Denis Ducret".to_string(),
        partstat: partstat.to_string(),
        uid: "probe-10@example.test".to_string(),
        response_sent_at: Some("2026-08-24T05:44:30Z".to_string()),
        meeting_start: Some("2026-08-24T06:30:00Z".to_string()),
        meeting_end: Some("2026-08-24T07:00:00Z".to_string()),
        meeting_location: Some("Les Planches".to_string()),
        meeting_sequence: Some(2),
        proposed_start: (method == "COUNTER").then(|| "2026-08-24T07:30:00Z".to_string()),
        proposed_end: (method == "COUNTER").then(|| "2026-08-24T08:00:00Z".to_string()),
        original_start: Some("2026-08-24T06:30:00Z".to_string()),
        original_end: Some("2026-08-24T07:00:00Z".to_string()),
    });
    email
}
