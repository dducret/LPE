use lpe_storage::{JmapEmail, JmapMailbox};
use uuid::Uuid;

pub(crate) use crate::mapi::identity::STORE_REPLICA_GUID;

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
const INCR_SYNC_CHG: u32 = 0x4012_0003;
const INCR_SYNC_DEL: u32 = 0x4013_0003;
const INCR_SYNC_END: u32 = 0x4014_0003;
const INCR_SYNC_READ: u32 = 0x402F_0003;
const INCR_SYNC_STATE_BEGIN: u32 = 0x403A_0003;
const INCR_SYNC_STATE_END: u32 = 0x403B_0003;
const PID_TAG_DISPLAY_NAME_W: u32 = 0x3001_001F;
const PID_TAG_CONTENT_COUNT: u32 = 0x3602_0003;
const PID_TAG_CONTENT_UNREAD_COUNT: u32 = 0x3603_0003;
const PID_TAG_SUBFOLDERS: u32 = 0x360A_000B;
const PID_TAG_FOLDER_TYPE: u32 = 0x3601_0003;
const PID_TAG_SUBJECT_W: u32 = 0x0037_001F;
const PID_TAG_NORMALIZED_SUBJECT_A: u32 = 0x0E1D_001E;
const PID_TAG_CONTAINER_CLASS_W: u32 = 0x3613_001F;
const PID_TAG_MESSAGE_FLAGS: u32 = 0x0E07_0003;
const PID_TAG_MESSAGE_SIZE: u32 = 0x0E08_0003;
const PID_TAG_LAST_MODIFICATION_TIME: u32 = 0x3008_0040;
const PID_TAG_ACCESS_BINARY: u32 = 0x0FF4_0102;
const PID_TAG_MAPPING_SIGNATURE: u32 = 0x3FE0_0102;
const PID_TAG_RECORD_KEY: u32 = 0x3FE1_0102;
const PID_TAG_ORDINAL_MOST: u32 = 0x0E27_0102;
const PID_TAG_FLAG_STATUS: u32 = 0x1090_0003;
const PID_TAG_SOURCE_KEY: u32 = 0x65E0_0102;
const PID_TAG_PARENT_SOURCE_KEY: u32 = 0x65E1_0102;
const PID_TAG_CHANGE_KEY: u32 = 0x65E2_0102;
const PID_TAG_PREDECESSOR_CHANGE_LIST: u32 = 0x65E3_0102;
const PID_TAG_LOCAL_COMMIT_TIME_MAX: u32 = 0x670A_0040;
const PID_TAG_DELETED_COUNT_TOTAL: u32 = 0x670B_0003;
const PID_TAG_MID: u32 = 0x674A_0014;
const PID_TAG_FOLDER_ID: u32 = 0x6748_0014;
const PID_TAG_PARENT_FOLDER_ID: u32 = 0x6749_0014;
const PID_TAG_CHANGE_NUMBER: u32 = 0x67A4_0014;
const META_TAG_IDSET_GIVEN: u32 = 0x4017_0102;
const META_TAG_IDSET_DELETED: u32 = 0x4018_0102;
const META_TAG_CNSET_SEEN: u32 = 0x6796_0102;
const META_TAG_CNSET_SEEN_FAI: u32 = 0x67DA_0102;
const META_TAG_CNSET_READ: u32 = 0x67D2_0102;
const GLOBSET_RANGE_COMMAND: u8 = 0x52;
const GLOBSET_END_COMMAND: u8 = 0x00;
const WINDOWS_UNIX_EPOCH_OFFSET_SECONDS: i64 = 11_644_473_600;
const FILETIME_TICKS_PER_SECOND: u64 = 10_000_000;
const FILETIME_2026_01_01: u64 =
    (WINDOWS_UNIX_EPOCH_OFFSET_SECONDS as u64 + 1_767_225_600) * FILETIME_TICKS_PER_SECOND;
const VIRTUAL_SPECIAL_MAILBOX_UUID_PREFIX: u128 = 0x4c50455f_4d415049_0000_0000_0000_0000;
const VIRTUAL_SPECIAL_MAILBOX_UUID_MASK: u128 = 0xffff_ffff_ffff_ffff_0000_0000_0000_0000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AttachmentSyncFact {
    pub(crate) id: Uuid,
    pub(crate) file_reference: String,
    pub(crate) file_name: String,
    pub(crate) media_type: String,
    pub(crate) size_octets: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MessageAttachmentSyncFacts {
    pub(crate) message_id: Uuid,
    pub(crate) attachments: Vec<AttachmentSyncFact>,
}

pub(crate) fn canonical_folder_change_number(mailbox: &JmapMailbox) -> u64 {
    stable_hash64([
        mailbox.id.as_bytes().as_slice(),
        mailbox.role.as_bytes(),
        mailbox.name.as_bytes(),
        &mailbox.sort_order.to_le_bytes(),
        &mailbox.total_emails.to_le_bytes(),
        &mailbox.unread_emails.to_le_bytes(),
    ])
}

pub(crate) fn canonical_message_change_number(email: &JmapEmail) -> u64 {
    canonical_message_change_number_with_attachments(email, &[])
}

pub(crate) fn canonical_message_change_number_with_attachments(
    email: &JmapEmail,
    attachments: &[AttachmentSyncFact],
) -> u64 {
    let mut hash = FNV_OFFSET;
    hash = hash_bytes(hash, email.id.as_bytes());
    hash = hash_bytes(hash, email.thread_id.as_bytes());
    hash = hash_bytes(hash, email.mailbox_id.as_bytes());
    hash = hash_bytes(hash, email.mailbox_role.as_bytes());
    hash = hash_bytes(hash, email.mailbox_name.as_bytes());
    hash = hash_bytes(hash, email.received_at.as_bytes());
    hash = hash_bytes(
        hash,
        email.sent_at.as_deref().unwrap_or_default().as_bytes(),
    );
    hash = hash_bytes(hash, email.from_address.as_bytes());
    hash = hash_bytes(
        hash,
        email.from_display.as_deref().unwrap_or_default().as_bytes(),
    );
    hash = hash_bytes(hash, email.subject.as_bytes());
    hash = hash_bytes(hash, email.preview.as_bytes());
    hash = hash_bytes(hash, email.body_text.as_bytes());
    hash = hash_bytes(
        hash,
        &[
            email.unread as u8,
            email.flagged as u8,
            email.has_attachments as u8,
        ],
    );
    hash = hash_bytes(hash, &email.size_octets.to_le_bytes());
    hash = hash_bytes(
        hash,
        email
            .internet_message_id
            .as_deref()
            .unwrap_or_default()
            .as_bytes(),
    );
    for recipient in email.to.iter().chain(email.cc.iter()) {
        hash = hash_bytes(hash, recipient.address.as_bytes());
        hash = hash_bytes(
            hash,
            recipient
                .display_name
                .as_deref()
                .unwrap_or_default()
                .as_bytes(),
        );
    }
    let mut attachments = attachments.iter().collect::<Vec<_>>();
    attachments.sort_by(|left, right| {
        left.file_name
            .cmp(&right.file_name)
            .then(left.media_type.cmp(&right.media_type))
            .then(left.id.cmp(&right.id))
    });
    for attachment in attachments {
        hash = hash_bytes(hash, attachment.id.as_bytes());
        hash = hash_bytes(hash, attachment.file_reference.as_bytes());
        hash = hash_bytes(hash, attachment.file_name.as_bytes());
        hash = hash_bytes(hash, attachment.media_type.as_bytes());
        hash = hash_bytes(hash, &attachment.size_octets.to_le_bytes());
    }
    hash.max(1)
}

pub(crate) fn source_key_for_uuid(id: &Uuid) -> Vec<u8> {
    let object_id =
        crate::mapi::identity::mapped_mapi_object_id(id).expect("MAPI identity mapping missing");
    crate::mapi::identity::source_key_for_object_id(object_id)
}

pub(crate) fn source_key_for_store_id(store_id: u64) -> Vec<u8> {
    crate::mapi::identity::source_key_for_object_id(store_id)
}

pub(crate) fn source_key_for_mailbox_folder(mailbox: &JmapMailbox) -> Vec<u8> {
    let fallback = crate::mapi::identity::mapped_mapi_object_id(&mailbox.id)
        .expect("MAPI folder identity mapping missing");
    source_key_for_store_id(mapi_folder_id_for_mailbox(mailbox, fallback))
}

pub(crate) fn source_key_for_mailbox_role(mailbox_id: &Uuid, role: &str) -> Vec<u8> {
    let folder_id = match role {
        "inbox" => crate::mapi::identity::INBOX_FOLDER_ID,
        "drafts" => crate::mapi::identity::DRAFTS_FOLDER_ID,
        "outbox" => crate::mapi::identity::OUTBOX_FOLDER_ID,
        "sent" => crate::mapi::identity::SENT_FOLDER_ID,
        "trash" => crate::mapi::identity::TRASH_FOLDER_ID,
        "contacts" => crate::mapi::identity::CONTACTS_FOLDER_ID,
        "calendar" => crate::mapi::identity::CALENDAR_FOLDER_ID,
        "journal" => crate::mapi::identity::JOURNAL_FOLDER_ID,
        "notes" => crate::mapi::identity::NOTES_FOLDER_ID,
        "tasks" => crate::mapi::identity::TASKS_FOLDER_ID,
        "reminders" => crate::mapi::identity::REMINDERS_FOLDER_ID,
        _ => crate::mapi::identity::mapped_mapi_object_id(mailbox_id)
            .expect("MAPI folder identity mapping missing"),
    };
    source_key_for_store_id(folder_id)
}

pub(crate) fn virtual_special_mailbox(folder_id: u64) -> Option<JmapMailbox> {
    let (role, name, sort_order, _, _) = virtual_special_folder_metadata(folder_id)?;
    Some(JmapMailbox {
        id: virtual_special_mailbox_id(folder_id),
        parent_id: None,
        role: role.to_string(),
        name: name.to_string(),
        sort_order,
        total_emails: 0,
        unread_emails: 0,
        is_subscribed: true,
    })
}

pub(crate) fn change_key_for_change_number(change_number: u64) -> Vec<u8> {
    crate::mapi::identity::change_key_for_change_number(change_number)
}

pub(crate) fn predecessor_change_list(change_number: u64) -> Vec<u8> {
    let change_key = change_key_for_change_number(change_number);
    let mut list = Vec::with_capacity(1 + change_key.len());
    list.push(change_key.len() as u8);
    list.extend_from_slice(&change_key);
    list
}

pub(crate) fn filetime_from_rfc3339_utc(value: &str) -> u64 {
    parse_rfc3339_utc_seconds(value)
        .map(|seconds| {
            (seconds + WINDOWS_UNIX_EPOCH_OFFSET_SECONDS).max(0) as u64 * FILETIME_TICKS_PER_SECOND
        })
        .unwrap_or_default()
}

pub(crate) fn filetime_from_change_number(change_number: u64) -> u64 {
    FILETIME_2026_01_01 + (change_number % 31_536_000) * FILETIME_TICKS_PER_SECOND
}

fn parse_rfc3339_utc_seconds(value: &str) -> Option<i64> {
    let bytes = value.as_bytes();
    if bytes.len() < 20 || bytes.get(4) != Some(&b'-') || bytes.get(7) != Some(&b'-') {
        return None;
    }
    if bytes.get(10) != Some(&b'T') || bytes.get(13) != Some(&b':') {
        return None;
    }
    if bytes.get(16) != Some(&b':') || bytes.get(19) != Some(&b'Z') {
        return None;
    }
    let year = parse_digits(bytes.get(0..4)?)? as i32;
    let month = parse_digits(bytes.get(5..7)?)? as i32;
    let day = parse_digits(bytes.get(8..10)?)? as i32;
    let hour = parse_digits(bytes.get(11..13)?)? as i64;
    let minute = parse_digits(bytes.get(14..16)?)? as i64;
    let second = parse_digits(bytes.get(17..19)?)? as i64;
    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || minute > 59
        || second > 60
    {
        return None;
    }
    Some(days_from_civil(year, month, day) * 86_400 + hour * 3_600 + minute * 60 + second)
}

fn parse_digits(bytes: &[u8]) -> Option<u32> {
    bytes.iter().try_fold(0u32, |value, byte| {
        byte.is_ascii_digit()
            .then_some(value * 10 + u32::from(byte - b'0'))
    })
}

fn days_from_civil(mut year: i32, month: i32, day: i32) -> i64 {
    year -= i32::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month_position = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * month_position + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    i64::from(era * 146_097 + day_of_era - 719_468)
}

pub(crate) fn sync_state_token_with_attachments(
    sync_type: u8,
    folder_id: u64,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    attachment_facts: &[MessageAttachmentSyncFacts],
) -> Vec<u8> {
    final_sync_state_stream(
        sync_type,
        &sync_state_object_ids(sync_type, folder_id, mailboxes, emails),
        &sync_state_change_numbers(sync_type, folder_id, mailboxes, emails, attachment_facts),
    )
}

#[cfg(test)]
pub(crate) fn sync_manifest_buffer_with_attachments(
    sync_type: u8,
    sync_flags: u16,
    sync_extra_flags: u32,
    sync_property_tags: &[u32],
    folder_id: u64,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    attachment_facts: &[MessageAttachmentSyncFacts],
    deleted_message_ids: &[u64],
    final_change_sequence: u64,
) -> Vec<u8> {
    sync_manifest_buffer_with_final_state(
        sync_type,
        sync_flags,
        sync_extra_flags,
        sync_property_tags,
        folder_id,
        mailboxes,
        emails,
        attachment_facts,
        deleted_message_ids,
        mailboxes,
        mailboxes,
        emails,
        attachment_facts,
        emails,
        attachment_facts,
        final_change_sequence,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn sync_manifest_buffer_with_final_state(
    sync_type: u8,
    sync_flags: u16,
    sync_extra_flags: u32,
    sync_property_tags: &[u32],
    folder_id: u64,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    attachment_facts: &[MessageAttachmentSyncFacts],
    deleted_message_ids: &[u64],
    parent_context_mailboxes: &[JmapMailbox],
    state_mailboxes: &[JmapMailbox],
    state_emails: &[JmapEmail],
    state_attachment_facts: &[MessageAttachmentSyncFacts],
    aggregate_emails: &[JmapEmail],
    aggregate_attachment_facts: &[MessageAttachmentSyncFacts],
    _final_change_sequence: u64,
) -> Vec<u8> {
    let mut buffer = Vec::new();
    let sync_root_folder_id = folder_id;
    let excluded_property_tags = if sync_flags & 0x0080 == 0 {
        sync_property_tags
    } else {
        &[]
    };
    let mut folders = mailboxes.iter().collect::<Vec<_>>();
    folders.sort_by(|left, right| {
        hierarchy_sort_depth(
            sync_type,
            sync_root_folder_id,
            left,
            parent_context_mailboxes,
        )
        .cmp(&hierarchy_sort_depth(
            sync_type,
            sync_root_folder_id,
            right,
            parent_context_mailboxes,
        ))
        .then(hierarchy_folder_sort_order(left).cmp(&hierarchy_folder_sort_order(right)))
        .then(left.name.cmp(&right.name))
        .then(left.id.cmp(&right.id))
    });
    for mailbox in folders {
        let folder_id = mapi_folder_id_for_mailbox(mailbox, folder_id);
        let parent_folder_id = mapi_folder_parent_id_for_mailbox(mailbox, parent_context_mailboxes);
        let change_number = canonical_hierarchy_change_number(sync_root_folder_id, mailbox);
        let source_key = source_key_for_store_id(folder_id);
        let parent_source_key = if sync_type == 0x02 && parent_folder_id == sync_root_folder_id {
            Vec::new()
        } else {
            source_key_for_store_id(parent_folder_id)
        };
        let container_class = mapi_folder_message_class(mailbox);
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            request_rop_id = "0x70",
            sync_type = format_args!("0x{sync_type:02x}"),
            folder_id = format_args!("0x{folder_id:016x}"),
            parent_folder_id = format_args!("0x{parent_folder_id:016x}"),
            source_key_len = source_key.len(),
            parent_source_key_len = parent_source_key.len(),
            display_name = %mailbox.name,
            container_class,
            change_number,
            "rca debug mapi hierarchy row"
        );
        write_u32(&mut buffer, INCR_SYNC_CHG);
        write_binary_property(&mut buffer, PID_TAG_PARENT_SOURCE_KEY, &parent_source_key);
        write_binary_property(&mut buffer, PID_TAG_SOURCE_KEY, &source_key);
        write_u32(&mut buffer, PID_TAG_LAST_MODIFICATION_TIME);
        write_i64(
            &mut buffer,
            filetime_from_change_number(change_number) as i64,
        );
        write_binary_property(
            &mut buffer,
            PID_TAG_CHANGE_KEY,
            &change_key_for_change_number(change_number),
        );
        write_binary_property(
            &mut buffer,
            PID_TAG_PREDECESSOR_CHANGE_LIST,
            &predecessor_change_list(change_number),
        );
        write_utf16_property(&mut buffer, PID_TAG_DISPLAY_NAME_W, &mailbox.name);
        if sync_type != 0x02 || sync_extra_flags & 0x0000_0001 != 0 {
            write_u32(&mut buffer, PID_TAG_FOLDER_ID);
            write_i64(&mut buffer, folder_id as i64);
        }
        if sync_type != 0x02 || sync_flags & 0x0100 != 0 || sync_extra_flags & 0x0000_0001 != 0 {
            write_u32(&mut buffer, PID_TAG_PARENT_FOLDER_ID);
            write_i64(&mut buffer, parent_folder_id as i64);
        }
        write_u32(&mut buffer, PID_TAG_CHANGE_NUMBER);
        write_i64(&mut buffer, change_number as i64);
        if !property_tag_excluded(excluded_property_tags, PID_TAG_CONTENT_COUNT) {
            write_i32_property(
                &mut buffer,
                PID_TAG_CONTENT_COUNT,
                mailbox.total_emails.min(i32::MAX as u32) as i32,
            );
        }
        if !property_tag_excluded(excluded_property_tags, PID_TAG_CONTENT_UNREAD_COUNT) {
            write_i32_property(
                &mut buffer,
                PID_TAG_CONTENT_UNREAD_COUNT,
                mailbox.unread_emails.min(i32::MAX as u32) as i32,
            );
        }
        if !property_tag_excluded(excluded_property_tags, PID_TAG_FOLDER_TYPE) {
            write_i32_property(&mut buffer, PID_TAG_FOLDER_TYPE, mapi_folder_type(mailbox));
        }
        if !property_tag_excluded(excluded_property_tags, PID_TAG_LOCAL_COMMIT_TIME_MAX) {
            write_u32(&mut buffer, PID_TAG_LOCAL_COMMIT_TIME_MAX);
            write_i64(
                &mut buffer,
                local_commit_time_max(mailbox, aggregate_emails, aggregate_attachment_facts) as i64,
            );
        }
        if !property_tag_excluded(excluded_property_tags, PID_TAG_DELETED_COUNT_TOTAL) {
            write_i32_property(&mut buffer, PID_TAG_DELETED_COUNT_TOTAL, 0);
        }
        if !property_tag_excluded(excluded_property_tags, PID_TAG_MESSAGE_SIZE) {
            write_i32_property(&mut buffer, PID_TAG_MESSAGE_SIZE, 0);
        }
        if !property_tag_excluded(excluded_property_tags, PID_TAG_ACCESS_BINARY) {
            write_binary_property(&mut buffer, PID_TAG_ACCESS_BINARY, &source_key);
        }
        if !property_tag_excluded(excluded_property_tags, PID_TAG_MAPPING_SIGNATURE) {
            write_binary_property(&mut buffer, PID_TAG_MAPPING_SIGNATURE, &source_key);
        }
        if !property_tag_excluded(excluded_property_tags, PID_TAG_RECORD_KEY) {
            write_binary_property(&mut buffer, PID_TAG_RECORD_KEY, &source_key);
        }
        if !property_tag_excluded(excluded_property_tags, PID_TAG_ORDINAL_MOST) {
            write_binary_property(&mut buffer, PID_TAG_ORDINAL_MOST, &source_key);
        }
        write_bool_property(
            &mut buffer,
            PID_TAG_SUBFOLDERS,
            mapi_folder_has_subfolders(mailbox, parent_context_mailboxes),
        );
        write_utf16_property(&mut buffer, PID_TAG_CONTAINER_CLASS_W, container_class);
    }

    let mut messages = emails.iter().collect::<Vec<_>>();
    messages.sort_by(|left, right| {
        left.received_at
            .cmp(&right.received_at)
            .then(left.subject.cmp(&right.subject))
            .then(left.id.cmp(&right.id))
    });
    for email in messages {
        let attachments = attachments_for_message(email.id, attachment_facts);
        let change_number = canonical_message_change_number_with_attachments(email, attachments);
        let source_key = source_key_for_uuid(&email.id);
        write_u32(&mut buffer, INCR_SYNC_CHG);
        write_u32(&mut buffer, PID_TAG_MID);
        write_i64(
            &mut buffer,
            crate::mapi::identity::mapped_mapi_object_id(&email.id).unwrap_or(0) as i64,
        );
        write_binary_property(&mut buffer, PID_TAG_SOURCE_KEY, &source_key);
        write_u32(&mut buffer, PID_TAG_CHANGE_NUMBER);
        write_i64(&mut buffer, change_number as i64);
        write_u32(&mut buffer, PID_TAG_MESSAGE_FLAGS);
        write_i32(&mut buffer, canonical_message_flags(email) as i32);
        write_u32(&mut buffer, PID_TAG_FLAG_STATUS);
        write_i32(&mut buffer, canonical_flag_status(email) as i32);
        write_utf16_property(&mut buffer, PID_TAG_SUBJECT_W, &email.subject);
        write_string8_property(&mut buffer, PID_TAG_NORMALIZED_SUBJECT_A, &email.subject);
        write_binary_property(
            &mut buffer,
            PID_TAG_CHANGE_KEY,
            &change_key_for_change_number(change_number),
        );
        write_binary_property(
            &mut buffer,
            PID_TAG_PREDECESSOR_CHANGE_LIST,
            &predecessor_change_list(change_number),
        );
        write_visible_recipient_facts(&mut buffer, email);
        buffer.extend_from_slice(&(attachments.len().min(u16::MAX as usize) as u16).to_le_bytes());
        let mut attachments = attachments.iter().collect::<Vec<_>>();
        attachments.sort_by(|left, right| {
            left.file_name
                .cmp(&right.file_name)
                .then(left.media_type.cmp(&right.media_type))
                .then(left.id.cmp(&right.id))
        });
        for attachment in attachments.into_iter().take(u16::MAX as usize) {
            write_prefixed_bytes(&mut buffer, attachment.file_name.as_bytes());
            write_prefixed_bytes(&mut buffer, attachment.media_type.as_bytes());
            buffer.extend_from_slice(&attachment.size_octets.to_le_bytes());
            write_prefixed_bytes(&mut buffer, attachment.file_reference.as_bytes());
        }
    }

    if !deleted_message_ids.is_empty() {
        write_u32(&mut buffer, INCR_SYNC_DEL);
        write_binary_property(
            &mut buffer,
            META_TAG_IDSET_DELETED,
            &replid_idset_from_object_ids(deleted_message_ids),
        );
    }

    if sync_type != 0x02 {
        for email in emails.iter().filter(|email| !email.unread) {
            write_u32(&mut buffer, INCR_SYNC_READ);
            write_u32(&mut buffer, PID_TAG_MID);
            write_i64(
                &mut buffer,
                crate::mapi::identity::mapped_mapi_object_id(&email.id).unwrap_or(0) as i64,
            );
        }
    }

    buffer.extend_from_slice(&sync_state_token_with_attachments(
        sync_type,
        folder_id,
        state_mailboxes,
        state_emails,
        state_attachment_facts,
    ));
    write_u32(&mut buffer, INCR_SYNC_END);
    buffer
}

fn mapi_folder_id_for_mailbox(mailbox: &JmapMailbox, fallback: u64) -> u64 {
    match mailbox.role.as_str() {
        "__mapi_ipm_subtree" => crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
        "__mapi_deferred_action" => crate::mapi::identity::DEFERRED_ACTION_FOLDER_ID,
        "__mapi_spooler_queue" => crate::mapi::identity::SPOOLER_QUEUE_FOLDER_ID,
        "inbox" => crate::mapi::identity::INBOX_FOLDER_ID,
        "drafts" => crate::mapi::identity::DRAFTS_FOLDER_ID,
        "outbox" => crate::mapi::identity::OUTBOX_FOLDER_ID,
        "sent" => crate::mapi::identity::SENT_FOLDER_ID,
        "trash" => crate::mapi::identity::TRASH_FOLDER_ID,
        "__mapi_common_views" => crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
        "__mapi_schedule" => crate::mapi::identity::SCHEDULE_FOLDER_ID,
        "__mapi_search" => crate::mapi::identity::SEARCH_FOLDER_ID,
        "__mapi_views" => crate::mapi::identity::VIEWS_FOLDER_ID,
        "__mapi_shortcuts" => crate::mapi::identity::SHORTCUTS_FOLDER_ID,
        "contacts" => crate::mapi::identity::CONTACTS_FOLDER_ID,
        "calendar" => crate::mapi::identity::CALENDAR_FOLDER_ID,
        "journal" => crate::mapi::identity::JOURNAL_FOLDER_ID,
        "notes" => crate::mapi::identity::NOTES_FOLDER_ID,
        "tasks" => crate::mapi::identity::TASKS_FOLDER_ID,
        "reminders" => crate::mapi::identity::REMINDERS_FOLDER_ID,
        _ => crate::mapi::identity::mapped_mapi_object_id(&mailbox.id).unwrap_or(fallback),
    }
}

fn mapi_folder_parent_id_for_mailbox(mailbox: &JmapMailbox, mailboxes: &[JmapMailbox]) -> u64 {
    match mailbox.role.as_str() {
        "__mapi_ipm_subtree"
        | "__mapi_deferred_action"
        | "__mapi_spooler_queue"
        | "__mapi_common_views"
        | "__mapi_schedule"
        | "__mapi_search"
        | "__mapi_views"
        | "__mapi_shortcuts" => crate::mapi::identity::ROOT_FOLDER_ID,
        "journal" | "notes" | "tasks" | "reminders" => crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
        _ => mailbox
            .parent_id
            .and_then(|parent_id| mailboxes.iter().find(|candidate| candidate.id == parent_id))
            .map(|parent| {
                let fallback = crate::mapi::identity::mapped_mapi_object_id(&parent.id)
                    .unwrap_or(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID);
                mapi_folder_id_for_mailbox(parent, fallback)
            })
            .unwrap_or(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID),
    }
}

fn mapi_folder_message_class(mailbox: &JmapMailbox) -> &'static str {
    virtual_special_folder_metadata(mapi_folder_id_for_mailbox(mailbox, 0))
        .map(|(_, _, _, _, message_class)| message_class)
        .unwrap_or(match mailbox.role.as_str() {
            "contacts" => "IPF.Contact",
            "calendar" => "IPF.Appointment",
            "journal" => "IPF.Journal",
            "notes" => "IPF.StickyNote",
            "tasks" => "IPF.Task",
            "reminders" => "IPF.Note",
            _ => "IPF.Note",
        })
}

fn mapi_folder_has_subfolders(mailbox: &JmapMailbox, mailboxes: &[JmapMailbox]) -> bool {
    let folder_id = mapi_folder_id_for_mailbox(mailbox, 0);
    mailboxes
        .iter()
        .any(|candidate| mapi_folder_parent_id_for_mailbox(candidate, mailboxes) == folder_id)
}

fn hierarchy_sort_depth(
    sync_type: u8,
    sync_root_folder_id: u64,
    mailbox: &JmapMailbox,
    mailboxes: &[JmapMailbox],
) -> u8 {
    if sync_type != 0x02 {
        return 0;
    }
    let Some(mut parent_id) = mailbox.parent_id else {
        return 0;
    };
    let mut depth = 1u8;
    while let Some(parent) = mailboxes.iter().find(|candidate| candidate.id == parent_id) {
        if mapi_folder_id_for_mailbox(parent, sync_root_folder_id) == sync_root_folder_id {
            break;
        }
        let Some(next_parent_id) = parent.parent_id else {
            break;
        };
        parent_id = next_parent_id;
        depth = depth.saturating_add(1);
    }
    depth
}

fn hierarchy_folder_sort_order(mailbox: &JmapMailbox) -> i32 {
    virtual_special_folder_metadata(mapi_folder_id_for_mailbox(mailbox, 0))
        .map(|(_, _, sort_order, _, _)| sort_order)
        .unwrap_or(i32::MAX)
}

fn virtual_special_mailbox_id(folder_id: u64) -> Uuid {
    Uuid::from_u128(VIRTUAL_SPECIAL_MAILBOX_UUID_PREFIX | u128::from(folder_id))
}

fn is_virtual_special_mailbox(mailbox: &JmapMailbox) -> bool {
    mailbox.id.as_u128() & VIRTUAL_SPECIAL_MAILBOX_UUID_MASK == VIRTUAL_SPECIAL_MAILBOX_UUID_PREFIX
}

fn virtual_special_folder_metadata(
    folder_id: u64,
) -> Option<(&'static str, &'static str, i32, u64, &'static str)> {
    match folder_id {
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID => Some((
            "__mapi_ipm_subtree",
            "Top of Information Store",
            0,
            crate::mapi::identity::ROOT_FOLDER_ID,
            "IPF.Note",
        )),
        crate::mapi::identity::DEFERRED_ACTION_FOLDER_ID => Some((
            "__mapi_deferred_action",
            "Deferred Action",
            1,
            crate::mapi::identity::ROOT_FOLDER_ID,
            "IPF.Root",
        )),
        crate::mapi::identity::SPOOLER_QUEUE_FOLDER_ID => Some((
            "__mapi_spooler_queue",
            "Spooler Queue",
            2,
            crate::mapi::identity::ROOT_FOLDER_ID,
            "IPF.Root",
        )),
        crate::mapi::identity::INBOX_FOLDER_ID => Some((
            "inbox",
            "Inbox",
            20,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Note",
        )),
        crate::mapi::identity::DRAFTS_FOLDER_ID => Some((
            "drafts",
            "Drafts",
            25,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Note",
        )),
        crate::mapi::identity::OUTBOX_FOLDER_ID => Some((
            "outbox",
            "Outbox",
            30,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Note",
        )),
        crate::mapi::identity::SENT_FOLDER_ID => Some((
            "sent",
            "Sent Items",
            40,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Note",
        )),
        crate::mapi::identity::TRASH_FOLDER_ID => Some((
            "trash",
            "Deleted Items",
            50,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Note",
        )),
        crate::mapi::identity::CONTACTS_FOLDER_ID => Some((
            "contacts",
            "Contacts",
            55,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Contact",
        )),
        crate::mapi::identity::CALENDAR_FOLDER_ID => Some((
            "calendar",
            "Calendar",
            57,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Appointment",
        )),
        crate::mapi::identity::JOURNAL_FOLDER_ID => Some((
            "journal",
            "Journal",
            58,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Journal",
        )),
        crate::mapi::identity::NOTES_FOLDER_ID => Some((
            "notes",
            "Notes",
            59,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.StickyNote",
        )),
        crate::mapi::identity::TASKS_FOLDER_ID => Some((
            "tasks",
            "Tasks",
            60,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Task",
        )),
        crate::mapi::identity::REMINDERS_FOLDER_ID => Some((
            "reminders",
            "Reminders",
            61,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Note",
        )),
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID => Some((
            "__mapi_common_views",
            "Common Views",
            70,
            crate::mapi::identity::ROOT_FOLDER_ID,
            "IPF.Root",
        )),
        crate::mapi::identity::SCHEDULE_FOLDER_ID => Some((
            "__mapi_schedule",
            "Schedule",
            80,
            crate::mapi::identity::ROOT_FOLDER_ID,
            "IPF.Root",
        )),
        crate::mapi::identity::SEARCH_FOLDER_ID => Some((
            "__mapi_search",
            "Search",
            90,
            crate::mapi::identity::ROOT_FOLDER_ID,
            "IPF.Root",
        )),
        crate::mapi::identity::VIEWS_FOLDER_ID => Some((
            "__mapi_views",
            "Views",
            100,
            crate::mapi::identity::ROOT_FOLDER_ID,
            "IPF.Root",
        )),
        crate::mapi::identity::SHORTCUTS_FOLDER_ID => Some((
            "__mapi_shortcuts",
            "Shortcuts",
            110,
            crate::mapi::identity::ROOT_FOLDER_ID,
            "IPF.Root",
        )),
        _ => None,
    }
}

fn property_tag_excluded(excluded_property_tags: &[u32], property_tag: u32) -> bool {
    excluded_property_tags.contains(&property_tag)
}

fn mapi_folder_type(mailbox: &JmapMailbox) -> i32 {
    if mailbox.role == "__mapi_search" {
        2
    } else {
        1
    }
}

fn local_commit_time_max(
    mailbox: &JmapMailbox,
    emails: &[JmapEmail],
    attachment_facts: &[MessageAttachmentSyncFacts],
) -> u64 {
    emails
        .iter()
        .filter(|email| email.mailbox_id == mailbox.id)
        .map(|email| {
            let attachments = attachments_for_message(email.id, attachment_facts);
            filetime_from_change_number(canonical_message_change_number_with_attachments(
                email,
                attachments,
            ))
        })
        .max()
        .unwrap_or(0)
}

fn sync_state_object_ids(
    sync_type: u8,
    folder_id: u64,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
) -> Vec<u64> {
    if sync_type == 0x02 {
        mailboxes
            .iter()
            .map(|mailbox| mapi_folder_id_for_mailbox(mailbox, folder_id))
            .collect()
    } else {
        emails
            .iter()
            .filter_map(|email| crate::mapi::identity::mapped_mapi_object_id(&email.id))
            .collect()
    }
}

fn sync_state_change_numbers(
    sync_type: u8,
    folder_id: u64,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    attachment_facts: &[MessageAttachmentSyncFacts],
) -> Vec<u64> {
    if sync_type == 0x02 {
        mailboxes
            .iter()
            .map(|mailbox| canonical_hierarchy_change_number(folder_id, mailbox))
            .collect()
    } else {
        emails
            .iter()
            .map(|email| {
                canonical_message_change_number_with_attachments(
                    email,
                    attachments_for_message(email.id, attachment_facts),
                )
            })
            .collect()
    }
}

fn canonical_hierarchy_change_number(sync_root_folder_id: u64, mailbox: &JmapMailbox) -> u64 {
    let folder_id = mapi_folder_id_for_mailbox(mailbox, sync_root_folder_id);
    if is_virtual_special_mailbox(mailbox) {
        folder_id
    } else {
        canonical_folder_change_number(mailbox)
    }
}

pub(crate) fn final_sync_state_stream(
    sync_type: u8,
    object_ids: &[u64],
    change_numbers: &[u64],
) -> Vec<u8> {
    let idset_given = replguid_idset_from_object_ids(object_ids);
    let cnset_seen = replguid_idset_from_counters(change_numbers);
    let mut token = Vec::new();
    write_u32(&mut token, INCR_SYNC_STATE_BEGIN);
    write_binary_property(&mut token, META_TAG_IDSET_GIVEN, &idset_given);
    write_binary_property(&mut token, META_TAG_CNSET_SEEN, &cnset_seen);
    if sync_type == 0x01 {
        write_binary_property(&mut token, META_TAG_CNSET_SEEN_FAI, &cnset_seen);
        write_binary_property(&mut token, META_TAG_CNSET_READ, &cnset_seen);
    }
    write_u32(&mut token, INCR_SYNC_STATE_END);
    token
}

pub(crate) fn fast_transfer_manifest_buffer_with_attachments(
    folder_id: u64,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    attachment_facts: &[MessageAttachmentSyncFacts],
) -> Vec<u8> {
    let mut buffer = b"LPE-MAPI-FASTTRANSFER\0".to_vec();
    buffer.extend_from_slice(&folder_id.to_le_bytes());
    buffer.extend_from_slice(&(mailboxes.len().min(u32::MAX as usize) as u32).to_le_bytes());
    buffer.extend_from_slice(&(emails.len().min(u32::MAX as usize) as u32).to_le_bytes());

    let mut folders = mailboxes.iter().collect::<Vec<_>>();
    folders.sort_by(|left, right| left.name.cmp(&right.name).then(left.id.cmp(&right.id)));
    for mailbox in folders {
        let change_number = canonical_folder_change_number(mailbox);
        write_prefixed_bytes(&mut buffer, &source_key_for_mailbox_folder(mailbox));
        buffer.extend_from_slice(&change_number.to_le_bytes());
        write_prefixed_bytes(&mut buffer, mailbox.role.as_bytes());
        write_prefixed_bytes(&mut buffer, mailbox.name.as_bytes());
    }

    let mut messages = emails.iter().collect::<Vec<_>>();
    messages.sort_by(|left, right| {
        left.received_at
            .cmp(&right.received_at)
            .then(left.subject.cmp(&right.subject))
            .then(left.id.cmp(&right.id))
    });
    for email in messages {
        let attachments = attachments_for_message(email.id, attachment_facts);
        let change_number = canonical_message_change_number_with_attachments(email, attachments);
        write_prefixed_bytes(&mut buffer, &source_key_for_uuid(&email.id));
        buffer.extend_from_slice(&change_number.to_le_bytes());
        buffer.extend_from_slice(&canonical_message_flags(email).to_le_bytes());
        buffer.extend_from_slice(&canonical_flag_status(email).to_le_bytes());
        write_prefixed_bytes(&mut buffer, email.subject.as_bytes());
        write_prefixed_bytes(&mut buffer, email.body_text.as_bytes());
        write_prefixed_bytes(&mut buffer, email.from_address.as_bytes());
        write_prefixed_bytes(
            &mut buffer,
            email.from_display.as_deref().unwrap_or_default().as_bytes(),
        );
        write_visible_recipient_facts(&mut buffer, email);
        buffer.extend_from_slice(&(attachments.len().min(u16::MAX as usize) as u16).to_le_bytes());
        let mut attachments = attachments.iter().collect::<Vec<_>>();
        attachments.sort_by(|left, right| {
            left.file_name
                .cmp(&right.file_name)
                .then(left.media_type.cmp(&right.media_type))
                .then(left.id.cmp(&right.id))
        });
        for attachment in attachments.into_iter().take(u16::MAX as usize) {
            write_prefixed_bytes(&mut buffer, attachment.file_name.as_bytes());
            write_prefixed_bytes(&mut buffer, attachment.media_type.as_bytes());
            buffer.extend_from_slice(&attachment.size_octets.to_le_bytes());
            write_prefixed_bytes(&mut buffer, attachment.file_reference.as_bytes());
        }
    }

    buffer
}

pub(crate) fn canonical_message_flags(email: &JmapEmail) -> u32 {
    let mut flags = 0u32;
    if !email.unread {
        flags |= 0x0000_0001;
    }
    if email.has_attachments {
        flags |= 0x0000_0010;
    }
    flags
}

pub(crate) fn canonical_flag_status(email: &JmapEmail) -> u32 {
    if email.flagged {
        2
    } else {
        0
    }
}

fn write_visible_recipient_facts(buffer: &mut Vec<u8>, email: &JmapEmail) {
    let recipient_count = email
        .to
        .len()
        .saturating_add(email.cc.len())
        .min(u16::MAX as usize);
    buffer.extend_from_slice(&(recipient_count as u16).to_le_bytes());

    let visible_recipients = email
        .to
        .iter()
        .map(|recipient| (1u8, recipient))
        .chain(email.cc.iter().map(|recipient| (2u8, recipient)));
    for (recipient_type, recipient) in visible_recipients.take(u16::MAX as usize) {
        buffer.push(recipient_type);
        write_prefixed_bytes(buffer, recipient.address.as_bytes());
        write_prefixed_bytes(
            buffer,
            recipient
                .display_name
                .as_deref()
                .unwrap_or_default()
                .as_bytes(),
        );
    }
}

fn attachments_for_message(
    message_id: Uuid,
    attachment_facts: &[MessageAttachmentSyncFacts],
) -> &[AttachmentSyncFact] {
    attachment_facts
        .iter()
        .find(|facts| facts.message_id == message_id)
        .map(|facts| facts.attachments.as_slice())
        .unwrap_or_default()
}

pub(crate) fn local_replica_id_range(
    account_id: Uuid,
    requested: u32,
    sequence: u64,
) -> (u64, u32) {
    let count = requested.clamp(1, 1_024);
    let seed = stable_hash64([account_id.as_bytes().as_slice(), &sequence.to_le_bytes()]);
    ((seed & 0x0000_FFFF_FFFF_FFFF).max(0x100), count)
}

fn stable_hash64<'a>(parts: impl IntoIterator<Item = &'a [u8]>) -> u64 {
    let mut hash = FNV_OFFSET;
    for part in parts {
        hash = hash_bytes(hash, part);
        hash = hash_bytes(hash, &[0]);
    }
    hash.max(1)
}

fn hash_bytes(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

fn write_prefixed_bytes(buffer: &mut Vec<u8>, bytes: &[u8]) {
    buffer.extend_from_slice(&(bytes.len().min(u16::MAX as usize) as u16).to_le_bytes());
    buffer.extend_from_slice(&bytes[..bytes.len().min(u16::MAX as usize)]);
}

fn write_u32(buffer: &mut Vec<u8>, value: u32) {
    buffer.extend_from_slice(&value.to_le_bytes());
}

fn write_i32(buffer: &mut Vec<u8>, value: i32) {
    buffer.extend_from_slice(&value.to_le_bytes());
}

fn write_i64(buffer: &mut Vec<u8>, value: i64) {
    buffer.extend_from_slice(&value.to_le_bytes());
}

fn write_i32_property(buffer: &mut Vec<u8>, property_tag: u32, value: i32) {
    write_u32(buffer, property_tag);
    write_i32(buffer, value);
}

fn write_bool_property(buffer: &mut Vec<u8>, property_tag: u32, value: bool) {
    write_u32(buffer, property_tag);
    buffer.extend_from_slice(&(value as u16).to_le_bytes());
}

fn write_binary_property(buffer: &mut Vec<u8>, property_tag: u32, value: &[u8]) {
    write_u32(buffer, property_tag);
    write_u32(buffer, value.len().min(u32::MAX as usize) as u32);
    buffer.extend_from_slice(value);
}

fn write_utf16_property(buffer: &mut Vec<u8>, property_tag: u32, value: &str) {
    write_u32(buffer, property_tag);
    let mut bytes = value
        .encode_utf16()
        .flat_map(u16::to_le_bytes)
        .collect::<Vec<_>>();
    bytes.extend_from_slice(&0u16.to_le_bytes());
    write_u32(buffer, bytes.len().min(u32::MAX as usize) as u32);
    buffer.extend_from_slice(&bytes);
}

fn write_string8_property(buffer: &mut Vec<u8>, property_tag: u32, value: &str) {
    write_u32(buffer, property_tag);
    let mut bytes = value.as_bytes().to_vec();
    bytes.push(0);
    write_u32(buffer, bytes.len().min(u32::MAX as usize) as u32);
    buffer.extend_from_slice(&bytes);
}

fn replguid_idset_from_object_ids(ids: &[u64]) -> Vec<u8> {
    let counters = ids
        .iter()
        .filter_map(|id| crate::mapi::identity::global_counter_from_store_id(*id))
        .collect::<Vec<_>>();
    replguid_idset_from_counters(&counters)
}

fn replguid_idset_from_counters(counters: &[u64]) -> Vec<u8> {
    let mut idset = STORE_REPLICA_GUID.to_vec();
    let mut counters = counters.to_vec();
    counters.sort_unstable();
    counters.dedup();
    write_globset_ranges(&mut idset, &coalesced_ranges(&counters));
    idset
}

fn replid_idset_from_object_ids(ids: &[u64]) -> Vec<u8> {
    let mut counters = ids
        .iter()
        .filter_map(|id| crate::mapi::identity::global_counter_from_store_id(*id))
        .collect::<Vec<_>>();
    counters.sort_unstable();
    counters.dedup();

    let mut idset = Vec::new();
    idset.extend_from_slice(&(crate::mapi::identity::STORE_REPLICA_ID as u16).to_le_bytes());
    write_globset_ranges(&mut idset, &coalesced_ranges(&counters));
    idset
}

fn coalesced_ranges(counters: &[u64]) -> Vec<(u64, u64)> {
    let mut ranges = Vec::new();
    let Some((&first, rest)) = counters.split_first() else {
        return ranges;
    };
    let mut low = first;
    let mut high = first;
    for &counter in rest {
        if counter == high.saturating_add(1) {
            high = counter;
        } else {
            ranges.push((low, high));
            low = counter;
            high = counter;
        }
    }
    ranges.push((low, high));
    ranges
}

fn write_globset_ranges(buffer: &mut Vec<u8>, ranges: &[(u64, u64)]) {
    for &(low, high) in ranges {
        buffer.push(GLOBSET_RANGE_COMMAND);
        buffer.extend_from_slice(&globcnt_bytes(low.max(1)));
        buffer.extend_from_slice(&globcnt_bytes(high.max(low).max(1)));
    }
    buffer.push(GLOBSET_END_COMMAND);
}

fn globcnt_bytes(value: u64) -> [u8; 6] {
    crate::mapi::identity::globcnt_bytes(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use lpe_storage::{JmapEmailAddress, JmapEmailMailboxState};

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
        assert_ne!(canonical_message_change_number(&email), baseline);
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
            total_emails: 0,
            unread_emails: 0,
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
    fn attachment_facts_advance_message_change_without_bcc_leakage() {
        let mut email = test_email();
        email.has_attachments = true;
        let baseline = canonical_message_change_number_with_attachments(
            &email,
            &[AttachmentSyncFact {
                id: Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap(),
                file_reference: "attachment:one".to_string(),
                file_name: "first.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                size_octets: 10,
            }],
        );

        email.bcc.push(JmapEmailAddress {
            address: "hidden@example.test".to_string(),
            display_name: None,
        });
        assert_eq!(
            canonical_message_change_number_with_attachments(
                &email,
                &[AttachmentSyncFact {
                    id: Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap(),
                    file_reference: "attachment:one".to_string(),
                    file_name: "first.pdf".to_string(),
                    media_type: "application/pdf".to_string(),
                    size_octets: 10,
                }],
            ),
            baseline
        );

        assert_ne!(
            canonical_message_change_number_with_attachments(
                &email,
                &[AttachmentSyncFact {
                    id: Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap(),
                    file_reference: "attachment:one".to_string(),
                    file_name: "renamed.pdf".to_string(),
                    media_type: "application/pdf".to_string(),
                    size_octets: 10,
                }],
            ),
            baseline
        );
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
            total_emails: 1,
            unread_emails: 1,
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
        let mailbox_source_key = source_key_for_uuid(&mailbox_id);
        assert_variable_property(&buffer, PID_TAG_ACCESS_BINARY, &mailbox_source_key);
        assert_variable_property(&buffer, PID_TAG_MAPPING_SIGNATURE, &mailbox_source_key);
        assert_variable_property(&buffer, PID_TAG_RECORD_KEY, &mailbox_source_key);
        assert_variable_property(&buffer, PID_TAG_ORDINAL_MOST, &mailbox_source_key);
    }

    #[test]
    fn sync_manifest_serializes_fast_transfer_boolean_values_as_two_bytes() {
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
            total_emails: 1,
            unread_emails: 1,
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
            &PID_TAG_CONTAINER_CLASS_W.to_le_bytes()
        );
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
            mailbox_ids: vec![Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap()],
            mailbox_states: vec![JmapEmailMailboxState {
                mailbox_id: Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap(),
                role: "inbox".to_string(),
                name: "Inbox".to_string(),
                unread: true,
                flagged: false,
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
            has_attachments: false,
            size_octets: 42,
            internet_message_id: Some("<message@example.test>".to_string()),
            mime_blob_ref: None,
            delivery_status: "stored".to_string(),
        }
    }
}
