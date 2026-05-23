use std::collections::BTreeSet;

use lpe_storage::{JmapEmail, JmapMailbox};
use uuid::Uuid;

pub(crate) use crate::mapi::identity::STORE_REPLICA_GUID;
use crate::mapi::wire::{FastTransferMarker, MapiSyncType};

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
const INCR_SYNC_CHG: u32 = FastTransferMarker::IncrSyncChg.as_u32();
const INCR_SYNC_DEL: u32 = FastTransferMarker::IncrSyncDel.as_u32();
const INCR_SYNC_END: u32 = FastTransferMarker::IncrSyncEnd.as_u32();
const INCR_SYNC_MESSAGE: u32 = FastTransferMarker::IncrSyncMessage.as_u32();
const INCR_SYNC_READ: u32 = FastTransferMarker::IncrSyncRead.as_u32();
const INCR_SYNC_STATE_BEGIN: u32 = FastTransferMarker::IncrSyncStateBegin.as_u32();
const INCR_SYNC_STATE_END: u32 = FastTransferMarker::IncrSyncStateEnd.as_u32();
const PID_TAG_DISPLAY_NAME_W: u32 = 0x3001_001F;
const PID_TAG_CONTENT_COUNT: u32 = 0x3602_0003;
const PID_TAG_CONTENT_UNREAD_COUNT: u32 = 0x3603_0003;
const PID_TAG_SUBFOLDERS: u32 = 0x360A_000B;
const PID_TAG_FOLDER_TYPE: u32 = 0x3601_0003;
const PID_TAG_SUBJECT_W: u32 = 0x0037_001F;
const PID_TAG_NORMALIZED_SUBJECT_A: u32 = 0x0E1D_001E;
const PID_TAG_BODY_W: u32 = 0x1000_001F;
const PID_TAG_CONTAINER_CLASS_W: u32 = 0x3613_001F;
const PID_TAG_MESSAGE_FLAGS: u32 = 0x0E07_0003;
const PID_TAG_MESSAGE_SIZE: u32 = 0x0E08_0003;
const PID_TAG_LAST_MODIFICATION_TIME: u32 = 0x3008_0040;
const PID_TAG_ACCESS: u32 = 0x0FF4_0003;
const PID_TAG_ASSOCIATED: u32 = 0x67AA_000B;
const PID_TAG_FLAG_STATUS: u32 = 0x1090_0003;
const MAPI_ACCESS_MODIFY: u32 = 0x0000_0001;
const MAPI_ACCESS_READ: u32 = 0x0000_0002;
const MAPI_ACCESS_DELETE: u32 = 0x0000_0004;
const MAPI_ACCESS_CREATE_HIERARCHY: u32 = 0x0000_0008;
const MAPI_ACCESS_CREATE_CONTENTS: u32 = 0x0000_0010;
const MAPI_ACCESS_CREATE_ASSOCIATED: u32 = 0x0000_0020;
const MAPI_FOLDER_ACCESS: u32 = MAPI_ACCESS_MODIFY
    | MAPI_ACCESS_READ
    | MAPI_ACCESS_DELETE
    | MAPI_ACCESS_CREATE_HIERARCHY
    | MAPI_ACCESS_CREATE_CONTENTS
    | MAPI_ACCESS_CREATE_ASSOCIATED;
const MSGFLAG_READ: u32 = 0x0000_0001;
const MSGFLAG_HASATTACH: u32 = 0x0000_0010;
const FOLLOWUP_FLAGGED: u32 = 0x0000_0002;
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
const META_TAG_IDSET_READ: u32 = 0x402D_0102;
const META_TAG_IDSET_UNREAD: u32 = 0x402E_0102;
const META_TAG_CNSET_SEEN: u32 = 0x6796_0102;
const META_TAG_CNSET_SEEN_FAI: u32 = 0x67DA_0102;
const META_TAG_CNSET_READ: u32 = 0x67D2_0102;
const SYNC_TYPE_CONTENTS: u8 = MapiSyncType::Contents.as_u8();
const SYNC_TYPE_HIERARCHY: u8 = MapiSyncType::Hierarchy.as_u8();
const SYNC_FLAG_NO_FOREIGN_IDENTIFIERS: u16 = 0x0100;
const SYNC_EXTRA_FLAG_EID: u32 = 0x0000_0001;
const GLOBSET_RANGE_COMMAND: u8 = 0x52;
const GLOBSET_END_COMMAND: u8 = 0x00;
const WINDOWS_UNIX_EPOCH_OFFSET_SECONDS: i64 = 11_644_473_600;
const FILETIME_TICKS_PER_SECOND: u64 = 10_000_000;
const FILETIME_2026_01_01: u64 =
    (WINDOWS_UNIX_EPOCH_OFFSET_SECONDS as u64 + 1_767_225_600) * FILETIME_TICKS_PER_SECOND;
const VIRTUAL_SPECIAL_MAILBOX_UUID_PREFIX: u128 = 0x4c50455f_4d415049_0000_0000_0000_0000;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SpecialMessageSyncFact {
    pub(crate) folder_id: u64,
    pub(crate) item_id: u64,
    pub(crate) canonical_id: Uuid,
    pub(crate) associated: bool,
    pub(crate) subject: String,
    pub(crate) body_text: String,
    pub(crate) message_class: String,
    pub(crate) last_modified_filetime: u64,
    pub(crate) message_size: i64,
    pub(crate) named_properties: Vec<(u32, SpecialMessagePropertyValue)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SpecialMessagePropertyValue {
    Binary(Vec<u8>),
    I32(i32),
    String(String),
    MultiString(Vec<String>),
    Time(String),
}

pub(crate) fn canonical_folder_change_number(mailbox: &JmapMailbox) -> u64 {
    canonical_modseq_change_number(mailbox.modseq)
}

pub(crate) fn canonical_message_change_number(email: &JmapEmail) -> u64 {
    canonical_message_change_number_with_attachments(email, &[])
}

pub(crate) fn canonical_message_change_number_with_attachments(
    email: &JmapEmail,
    _attachments: &[AttachmentSyncFact],
) -> u64 {
    canonical_modseq_change_number(
        email
            .mailbox_states
            .iter()
            .map(|state| state.modseq)
            .chain(std::iter::once(email.modseq))
            .max()
            .unwrap_or(1),
    )
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
        modseq: change_number_for_store_id(folder_id),
        total_emails: 0,
        unread_emails: 0,
        is_subscribed: true,
    })
}

pub(crate) fn change_key_for_change_number(change_number: u64) -> Vec<u8> {
    crate::mapi::identity::change_key_for_change_number(change_number)
}

pub(crate) fn change_number_for_store_id(store_id: u64) -> u64 {
    crate::mapi::identity::global_counter_from_store_id(store_id).unwrap_or_else(|| {
        crate::mapi::identity::global_counter_from_globcnt(&globcnt_bytes(store_id)).unwrap_or(1)
    })
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

pub(crate) fn sync_state_token_with_special_objects(
    sync_type: u8,
    folder_id: u64,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    attachment_facts: &[MessageAttachmentSyncFacts],
    special_objects: &[SpecialMessageSyncFact],
) -> Vec<u8> {
    let mut object_ids = sync_state_object_ids(sync_type, folder_id, mailboxes, emails);
    let mut change_numbers =
        sync_state_change_numbers(sync_type, folder_id, mailboxes, emails, attachment_facts);
    if sync_type == SYNC_TYPE_CONTENTS {
        object_ids.extend(special_objects.iter().map(|object| object.item_id));
        change_numbers.extend(
            special_objects
                .iter()
                .map(|object| change_number_for_store_id(object.item_id)),
        );
    }
    final_sync_state_stream(sync_type, &object_ids, &change_numbers)
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

#[allow(clippy::too_many_arguments, dead_code)]
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
    sync_manifest_buffer_with_special_objects_and_final_state(
        sync_type,
        sync_flags,
        sync_extra_flags,
        sync_property_tags,
        folder_id,
        mailboxes,
        emails,
        attachment_facts,
        &[],
        deleted_message_ids,
        parent_context_mailboxes,
        state_mailboxes,
        state_emails,
        state_attachment_facts,
        &[],
        aggregate_emails,
        aggregate_attachment_facts,
        _final_change_sequence,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn sync_manifest_buffer_with_special_objects_and_final_state(
    sync_type: u8,
    sync_flags: u16,
    sync_extra_flags: u32,
    sync_property_tags: &[u32],
    folder_id: u64,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    attachment_facts: &[MessageAttachmentSyncFacts],
    special_objects: &[SpecialMessageSyncFact],
    deleted_message_ids: &[u64],
    parent_context_mailboxes: &[JmapMailbox],
    state_mailboxes: &[JmapMailbox],
    state_emails: &[JmapEmail],
    state_attachment_facts: &[MessageAttachmentSyncFacts],
    state_special_objects: &[SpecialMessageSyncFact],
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
    if sync_type == SYNC_TYPE_HIERARCHY {
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
            if folder_id == sync_root_folder_id {
                continue;
            }
            let parent_folder_id =
                mapi_folder_parent_id_for_mailbox(mailbox, parent_context_mailboxes);
            let change_number = canonical_hierarchy_change_number(sync_root_folder_id, mailbox);
            let source_key = source_key_for_store_id(folder_id);
            let parent_source_key = if parent_folder_id == crate::mapi::identity::ROOT_FOLDER_ID
                || parent_folder_id == sync_root_folder_id
            {
                Vec::new()
            } else {
                source_key_for_store_id(parent_folder_id)
            };
            let container_class = mapi_folder_message_class(mailbox);
            let (content_count, content_unread_count, content_count_source) =
                folder_content_counts(folder_id, mailbox, mailboxes, aggregate_emails);
            let local_commit_time_max = local_commit_time_max(
                folder_id,
                mailboxes,
                aggregate_emails,
                aggregate_attachment_facts,
            );
            let local_commit_time_max_present = local_commit_time_max != 0
                && !property_tag_excluded(excluded_property_tags, PID_TAG_LOCAL_COMMIT_TIME_MAX);
            let hierarchy_core_folder_facts_forced = sync_type == SYNC_TYPE_HIERARCHY;
            let display_name = mapi_folder_display_name(mailbox);
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
                display_name,
                container_class,
                change_number,
                mailbox_content_count = mailbox.total_emails,
                mailbox_unread_count = mailbox.unread_emails,
                computed_content_count = content_count,
                computed_unread_count = content_unread_count,
                content_count_source,
                content_count_excluded =
                    property_tag_excluded(excluded_property_tags, PID_TAG_CONTENT_COUNT),
                content_unread_count_excluded =
                    property_tag_excluded(excluded_property_tags, PID_TAG_CONTENT_UNREAD_COUNT),
                local_commit_time_max,
                local_commit_time_max_present,
                deleted_count_total_present = false,
                folder_type_forced_by_experiment = hierarchy_core_folder_facts_forced,
                access_forced_by_experiment = hierarchy_core_folder_facts_forced,
                aggregate_email_count = aggregate_emails.len(),
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
            write_utf16_property(&mut buffer, PID_TAG_DISPLAY_NAME_W, display_name);
            write_u32(&mut buffer, PID_TAG_FOLDER_ID);
            write_i64(&mut buffer, folder_id as i64);
            if sync_type != SYNC_TYPE_HIERARCHY
                || sync_flags & 0x0100 != 0
                || sync_extra_flags & SYNC_EXTRA_FLAG_EID != 0
            {
                write_u32(&mut buffer, PID_TAG_PARENT_FOLDER_ID);
                write_i64(&mut buffer, parent_folder_id as i64);
            }
            if !property_tag_excluded(excluded_property_tags, PID_TAG_CONTAINER_CLASS_W) {
                write_utf16_property(&mut buffer, PID_TAG_CONTAINER_CLASS_W, container_class);
            }
            if !property_tag_excluded(excluded_property_tags, PID_TAG_CONTENT_COUNT)
                || hierarchy_core_folder_facts_forced
            {
                write_i32_property(&mut buffer, PID_TAG_CONTENT_COUNT, content_count);
            }
            if !property_tag_excluded(excluded_property_tags, PID_TAG_CONTENT_UNREAD_COUNT)
                || hierarchy_core_folder_facts_forced
            {
                write_i32_property(
                    &mut buffer,
                    PID_TAG_CONTENT_UNREAD_COUNT,
                    content_unread_count,
                );
            }
            if !property_tag_excluded(excluded_property_tags, PID_TAG_FOLDER_TYPE)
                || hierarchy_core_folder_facts_forced
            {
                write_i32_property(&mut buffer, PID_TAG_FOLDER_TYPE, mapi_folder_type(mailbox));
            }
            if !property_tag_excluded(excluded_property_tags, PID_TAG_MESSAGE_SIZE) {
                write_i32_property(&mut buffer, PID_TAG_MESSAGE_SIZE, 0);
            }
            if !property_tag_excluded(excluded_property_tags, PID_TAG_ACCESS)
                || hierarchy_core_folder_facts_forced
            {
                write_i32_property(&mut buffer, PID_TAG_ACCESS, MAPI_FOLDER_ACCESS as i32);
            }
            if local_commit_time_max_present {
                write_u32(&mut buffer, PID_TAG_LOCAL_COMMIT_TIME_MAX);
                write_i64(&mut buffer, local_commit_time_max as i64);
            }
            write_bool_property(
                &mut buffer,
                PID_TAG_SUBFOLDERS,
                mapi_folder_has_subfolders(mailbox, parent_context_mailboxes),
            );
        }
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
        write_bool_property(&mut buffer, PID_TAG_ASSOCIATED, false);
        if sync_extra_flags & SYNC_EXTRA_FLAG_EID != 0 {
            write_u32(&mut buffer, PID_TAG_MID);
            write_i64(
                &mut buffer,
                crate::mapi::identity::mapped_mapi_object_id(&email.id).unwrap_or(0) as i64,
            );
        }
        if sync_extra_flags & 0x0000_0002 != 0 {
            write_i32_property(&mut buffer, PID_TAG_MESSAGE_SIZE, 0);
        }
        if sync_extra_flags & 0x0000_0004 != 0 {
            write_u32(&mut buffer, PID_TAG_CHANGE_NUMBER);
            write_i64(&mut buffer, change_number as i64);
        }
        write_u32(&mut buffer, INCR_SYNC_MESSAGE);
        write_binary_property(
            &mut buffer,
            PID_TAG_PARENT_SOURCE_KEY,
            &source_key_for_store_id(folder_id),
        );
        if content_property_in_scope(
            sync_type,
            sync_flags,
            sync_property_tags,
            PID_TAG_MESSAGE_FLAGS,
        ) {
            write_u32(&mut buffer, PID_TAG_MESSAGE_FLAGS);
            write_i32(&mut buffer, canonical_message_flags(email) as i32);
        }
        if content_property_in_scope(
            sync_type,
            sync_flags,
            sync_property_tags,
            PID_TAG_FLAG_STATUS,
        ) {
            write_u32(&mut buffer, PID_TAG_FLAG_STATUS);
            write_i32(&mut buffer, canonical_flag_status(email) as i32);
        }
        let subject_in_scope =
            content_property_in_scope(sync_type, sync_flags, sync_property_tags, PID_TAG_SUBJECT_W);
        if subject_in_scope {
            write_utf16_property(&mut buffer, PID_TAG_SUBJECT_W, &email.subject);
        }
        if content_property_in_scope(
            sync_type,
            sync_flags,
            sync_property_tags,
            PID_TAG_NORMALIZED_SUBJECT_A,
        ) {
            write_string8_property(&mut buffer, PID_TAG_NORMALIZED_SUBJECT_A, &email.subject);
        }
        if content_property_in_scope(sync_type, sync_flags, sync_property_tags, PID_TAG_BODY_W) {
            write_utf16_property(&mut buffer, PID_TAG_BODY_W, &email.body_text);
        }
        if subject_in_scope {
            if sync_type == SYNC_TYPE_CONTENTS && !sync_property_tags.is_empty() {
                buffer.extend_from_slice(&0u16.to_le_bytes());
                buffer.extend_from_slice(&0u16.to_le_bytes());
            } else {
                write_visible_recipient_facts(&mut buffer, email);
                buffer.extend_from_slice(
                    &(attachments.len().min(u16::MAX as usize) as u16).to_le_bytes(),
                );
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
        }
    }

    let mut special_objects = special_objects.iter().collect::<Vec<_>>();
    special_objects.sort_by(|left, right| {
        left.folder_id
            .cmp(&right.folder_id)
            .then(left.subject.cmp(&right.subject))
            .then(left.canonical_id.cmp(&right.canonical_id))
    });
    for object in special_objects {
        let change_number = change_number_for_store_id(object.item_id);
        write_u32(&mut buffer, INCR_SYNC_CHG);
        write_binary_property(
            &mut buffer,
            PID_TAG_SOURCE_KEY,
            &source_key_for_store_id(object.item_id),
        );
        write_u32(&mut buffer, PID_TAG_LAST_MODIFICATION_TIME);
        write_i64(&mut buffer, object.last_modified_filetime as i64);
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
        write_bool_property(&mut buffer, PID_TAG_ASSOCIATED, object.associated);
        if sync_extra_flags & SYNC_EXTRA_FLAG_EID != 0 {
            write_u32(&mut buffer, PID_TAG_MID);
            write_i64(&mut buffer, object.item_id as i64);
        }
        if sync_extra_flags & 0x0000_0002 != 0 {
            write_i32_property(
                &mut buffer,
                PID_TAG_MESSAGE_SIZE,
                object.message_size as i32,
            );
        }
        if sync_extra_flags & 0x0000_0004 != 0 {
            write_u32(&mut buffer, PID_TAG_CHANGE_NUMBER);
            write_i64(&mut buffer, change_number as i64);
        }
        write_u32(&mut buffer, INCR_SYNC_MESSAGE);
        write_binary_property(
            &mut buffer,
            PID_TAG_PARENT_SOURCE_KEY,
            &source_key_for_store_id(object.folder_id),
        );
        if content_property_in_scope(
            sync_type,
            sync_flags,
            sync_property_tags,
            PID_TAG_MESSAGE_FLAGS,
        ) {
            write_i32_property(&mut buffer, PID_TAG_MESSAGE_FLAGS, MSGFLAG_READ as i32);
        }
        if content_property_in_scope(sync_type, sync_flags, sync_property_tags, PID_TAG_SUBJECT_W) {
            write_utf16_property(&mut buffer, PID_TAG_SUBJECT_W, &object.subject);
        }
        if content_property_in_scope(
            sync_type,
            sync_flags,
            sync_property_tags,
            PID_TAG_NORMALIZED_SUBJECT_A,
        ) {
            write_string8_property(&mut buffer, PID_TAG_NORMALIZED_SUBJECT_A, &object.subject);
        }
        write_utf16_property(&mut buffer, 0x001A_001F, &object.message_class);
        write_utf16_property(&mut buffer, 0x1000_001F, &object.body_text);
        write_i32_property(
            &mut buffer,
            PID_TAG_MESSAGE_SIZE,
            object.message_size as i32,
        );
        for (tag, value) in &object.named_properties {
            write_special_message_property(&mut buffer, *tag, value);
        }
        buffer.extend_from_slice(&0u16.to_le_bytes());
        buffer.extend_from_slice(&0u16.to_le_bytes());
    }

    if !deleted_message_ids.is_empty() {
        write_u32(&mut buffer, INCR_SYNC_DEL);
        write_binary_property(
            &mut buffer,
            META_TAG_IDSET_DELETED,
            &replid_idset_from_object_ids(deleted_message_ids),
        );
    }

    if sync_type == SYNC_TYPE_CONTENTS && sync_flags & 0x0008 != 0 {
        let read_message_ids = emails
            .iter()
            .filter(|email| !email.unread)
            .filter_map(|email| crate::mapi::identity::mapped_mapi_object_id(&email.id))
            .collect::<Vec<_>>();
        let unread_message_ids = emails
            .iter()
            .filter(|email| email.unread)
            .filter_map(|email| crate::mapi::identity::mapped_mapi_object_id(&email.id))
            .collect::<Vec<_>>();
        if !read_message_ids.is_empty() || !unread_message_ids.is_empty() {
            write_u32(&mut buffer, INCR_SYNC_READ);
            if !read_message_ids.is_empty() {
                write_binary_property(
                    &mut buffer,
                    META_TAG_IDSET_READ,
                    &replid_idset_from_object_ids(&read_message_ids),
                );
            }
            if !unread_message_ids.is_empty() {
                write_binary_property(
                    &mut buffer,
                    META_TAG_IDSET_UNREAD,
                    &replid_idset_from_object_ids(&unread_message_ids),
                );
            }
        }
    }

    buffer.extend_from_slice(&sync_state_token_with_special_objects(
        sync_type,
        folder_id,
        state_mailboxes,
        state_emails,
        state_attachment_facts,
        state_special_objects,
    ));
    write_u32(&mut buffer, INCR_SYNC_END);
    buffer
}

pub(crate) fn log_hierarchy_transfer_debug(
    sync_type: u8,
    sync_flags: u16,
    sync_extra_flags: u32,
    folder_id: u64,
    requested_property_tags: &[u32],
    transfer_buffer: &[u8],
) {
    if sync_type != SYNC_TYPE_HIERARCHY || !tracing::enabled!(tracing::Level::INFO) {
        return;
    }

    match decode_hierarchy_transfer_debug_summary(transfer_buffer) {
        Ok(summary) => {
            tracing::info!(
                rca_debug = true,
                adapter = "mapi",
                endpoint = "emsmdb",
                request_rop_id = "0x70",
                sync_type = format_args!("0x{sync_type:02x}"),
                folder_id = format_args!("0x{folder_id:016x}"),
                transfer_buffer_bytes = transfer_buffer.len(),
                hierarchy_decode_status = "ok",
                marker_count = summary.marker_tags.len(),
                marker_sequence = %format_marker_tags(&summary.marker_tags),
                fast_transfer_property_count = summary.property_count,
                stream_end_marker_seen = summary.stream_end_marker_seen,
                folder_change_count = summary.folder_change_count,
                final_state_present = summary.final_state_present,
                parent_before_child_violations = summary.parent_before_child_violations,
                zero_length_parent_source_key_count = summary.zero_length_parent_source_key_count,
                source_key_lengths = %format_usize_list(&summary.source_key_lengths),
                change_key_lengths = %format_usize_list(&summary.change_key_lengths),
                final_state_property_tags = %format_property_tags(&summary.final_state_property_tags),
                final_state_property_names = %format_property_tag_names(&summary.final_state_property_tags),
                final_state_property_lengths = %format_usize_list(&summary.final_state_property_lengths),
                final_state_expected_property_order_ok =
                    summary.final_state_expected_property_order_ok,
                final_state_idset_given = %summary.final_state_idset_given_summary.as_deref().unwrap_or_default(),
                final_state_cnset_seen = %summary.final_state_cnset_seen_summary.as_deref().unwrap_or_default(),
                emitted_property_tags = %format_property_tags(&summary.emitted_property_tags),
                requested_property_tags = %format_property_tags(requested_property_tags),
                property_tags_filter_mode = hierarchy_property_filter_mode(sync_flags, requested_property_tags),
                "rca debug mapi hierarchy transfer stream"
            );
            log_hierarchy_final_state_debug(sync_type, folder_id, &summary);
            log_hierarchy_microsoft_payload_comparison(
                sync_type,
                sync_flags,
                sync_extra_flags,
                folder_id,
                requested_property_tags,
                &summary,
            );
        }
        Err(error) => tracing::warn!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            request_rop_id = "0x70",
            sync_type = format_args!("0x{sync_type:02x}"),
            folder_id = format_args!("0x{folder_id:016x}"),
            transfer_buffer_bytes = transfer_buffer.len(),
            hierarchy_decode_status = "error",
            hierarchy_decode_error = %error,
            requested_property_tags = %format_property_tags(requested_property_tags),
            property_tags_filter_mode = hierarchy_property_filter_mode(sync_flags, requested_property_tags),
            "rca debug mapi hierarchy transfer stream"
        ),
    }
}

pub(crate) fn log_hierarchy_get_buffer_payload_summary(
    sync_type: u8,
    folder_id: u64,
    transfer_status: &str,
    transfer_buffer: &[u8],
) {
    if sync_type != SYNC_TYPE_HIERARCHY || !tracing::enabled!(tracing::Level::INFO) {
        return;
    }

    match decode_hierarchy_transfer_debug_summary(transfer_buffer) {
        Ok(summary) => {
            tracing::info!(
                rca_debug = true,
                adapter = "mapi",
                endpoint = "emsmdb",
                request_type = "Execute",
                request_rop_id = "0x4e",
                sync_type = format_args!("0x{sync_type:02x}"),
                folder_id = format_args!("0x{folder_id:016x}"),
                transfer_status,
                transfer_buffer_bytes = transfer_buffer.len(),
                marker_count = summary.marker_tags.len(),
                marker_sequence = %format_marker_tags(&summary.marker_tags),
                fast_transfer_property_count = summary.property_count,
                stream_end_marker_seen = summary.stream_end_marker_seen,
                final_state_idset_given_bytes = summary.final_state_idset_given_len,
                final_state_cnset_seen_bytes = summary.final_state_cnset_seen_len,
                final_state_expected_property_order_ok =
                    summary.final_state_expected_property_order_ok,
                folder_change_count = summary.folder_change_count,
                zero_parent_count = summary.zero_length_parent_source_key_count,
                nonzero_parent_count = summary.nonzero_parent_source_key_count,
                first_folder_name = %summary.first_folder_name(),
                last_folder_name = %summary.last_folder_name(),
                parent_before_child_violations = summary.parent_before_child_violations,
                final_state_idset_given_includes_all_expected_folder_source_key_counters =
                    summary.final_state_idset_given_includes_all_expected_folder_source_counters,
                final_state_cnset_seen_includes_all_expected_folder_change_counters =
                    summary.final_state_cnset_seen_includes_all_expected_folder_change_counters,
                "rca debug mapi hierarchy get buffer payload summary"
            );
            log_hierarchy_semantic_validation(sync_type, folder_id, transfer_status, &summary);
        }
        Err(error) => tracing::warn!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            request_type = "Execute",
            request_rop_id = "0x4e",
            sync_type = format_args!("0x{sync_type:02x}"),
            folder_id = format_args!("0x{folder_id:016x}"),
            transfer_status,
            transfer_buffer_bytes = transfer_buffer.len(),
            hierarchy_decode_status = "error",
            hierarchy_decode_error = %error,
            "rca debug mapi hierarchy get buffer payload summary"
        ),
    }
}

pub(crate) fn hierarchy_transfer_close_summary(
    sync_type: u8,
    folder_id: u64,
    transfer_buffer: &[u8],
) -> String {
    if sync_type != SYNC_TYPE_HIERARCHY {
        return String::new();
    }
    let Ok(summary) = decode_hierarchy_transfer_debug_summary(transfer_buffer) else {
        return "hierarchy_debug=parse_error".to_string();
    };
    let validation = hierarchy_semantic_validation(folder_id, &summary);
    format!(
        "first={};last={};root_first={};root_index={};root_name={};root_folder={};root_parent={};root_parent_sk={};root_type={};root_access={};root_subfolders={};parent_before_child={};semantic={};idset_missing={};cnset_missing={};final_state_order={}",
        summary.first_folder_name(),
        summary.last_folder_name(),
        validation.sync_root_row_index == 1,
        validation.sync_root_row_index,
        validation.sync_root_display_name,
        validation.sync_root_folder_id,
        validation.sync_root_parent_folder_id,
        validation.sync_root_parent_source_key_len,
        validation.sync_root_folder_type,
        validation.sync_root_access,
        validation.sync_root_subfolders,
        validation.parent_before_child_violations,
        validation.semantic_flags,
        format_counter_list(&validation.idset_missing_source_counters),
        format_counter_list(&validation.cnset_missing_change_counters),
        summary.final_state_expected_property_order_ok,
    )
}

fn log_hierarchy_semantic_validation(
    sync_type: u8,
    folder_id: u64,
    transfer_status: &str,
    summary: &HierarchyTransferDebugSummary,
) {
    let validation = hierarchy_semantic_validation(folder_id, summary);
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        request_type = "Execute",
        request_rop_id = "0x4e",
        sync_type = format_args!("0x{sync_type:02x}"),
        folder_id = format_args!("0x{folder_id:016x}"),
        transfer_status,
        completed = transfer_status == "0x0003",
        semantic_flags = %validation.semantic_flags,
        sync_root_source_counter = validation.sync_root_source_counter,
        sync_root_change_counter = validation.sync_root_change_counter,
        sync_root_row_present = validation.sync_root_row_present,
        sync_root_row_index = validation.sync_root_row_index,
        sync_root_row_first = validation.sync_root_row_index == 1,
        sync_root_display_name = %validation.sync_root_display_name,
        sync_root_folder_id = %validation.sync_root_folder_id,
        sync_root_parent_folder_id = %validation.sync_root_parent_folder_id,
        sync_root_parent_source_key_len = validation.sync_root_parent_source_key_len,
        sync_root_folder_type = validation.sync_root_folder_type,
        sync_root_access = validation.sync_root_access,
        sync_root_subfolders = validation.sync_root_subfolders,
        sync_root_counter_in_final_idset = validation.sync_root_counter_in_final_idset,
        sync_root_counter_in_final_cnset = validation.sync_root_counter_in_final_cnset,
        first_row_name = %validation.first_row_name,
        first_row_folder_id = %validation.first_row_folder_id,
        first_row_parent_folder_id = %validation.first_row_parent_folder_id,
        parent_before_child_violations = validation.parent_before_child_violations,
        root_inclusive_idset_given_bytes = validation.root_inclusive_idset_given_len,
        root_inclusive_cnset_seen_bytes = validation.root_inclusive_cnset_seen_len,
        root_inclusive_idset_given_delta_bytes = validation.root_inclusive_idset_given_delta_bytes,
        root_inclusive_cnset_seen_delta_bytes = validation.root_inclusive_cnset_seen_delta_bytes,
        root_inclusive_idset_given = %validation.root_inclusive_idset_given_summary,
        root_inclusive_cnset_seen = %validation.root_inclusive_cnset_seen_summary,
        top_level_row_count = validation.top_level_row_count,
        nested_row_count = validation.nested_row_count,
        rows_without_folder_id = validation.rows_without_folder_id,
        rows_missing_core_property_count = validation.rows_missing_core_property_count,
        rows_with_content_counts_present = validation.rows_with_content_counts_present,
        rows_with_folder_type_present = validation.rows_with_folder_type_present,
        rows_with_access_present = validation.rows_with_access_present,
        idset_missing_source_counters = %format_counter_list(&validation.idset_missing_source_counters),
        idset_extra_source_counters = %format_counter_list(&validation.idset_extra_source_counters),
        cnset_missing_change_counters = %format_counter_list(&validation.cnset_missing_change_counters),
        cnset_extra_change_counters = %format_counter_list(&validation.cnset_extra_change_counters),
        top_level_row_names = %validation.top_level_row_names,
        rows_missing_core_property_names = %validation.rows_missing_core_property_names,
        "rca debug mapi hierarchy semantic validation"
    );
}

fn log_hierarchy_final_state_debug(
    sync_type: u8,
    folder_id: u64,
    summary: &HierarchyTransferDebugSummary,
) {
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        request_rop_id = "0x70",
        sync_type = format_args!("0x{sync_type:02x}"),
        folder_id = format_args!("0x{folder_id:016x}"),
        final_metatag_idset_given = %summary.final_state_idset_given_summary.as_deref().unwrap_or_default(),
        final_metatag_cnset_seen = %summary.final_state_cnset_seen_summary.as_deref().unwrap_or_default(),
        final_metatag_idset_given_bytes = summary.final_state_idset_given_len,
        final_metatag_cnset_seen_bytes = summary.final_state_cnset_seen_len,
        final_state_expected_folder_counter_count = summary.folder_change_count,
        final_state_folder_change_count = summary.folder_change_count,
        final_metatag_idset_given_counter_count = summary.final_state_idset_given_counters.len(),
        final_metatag_cnset_seen_counter_count = summary.final_state_cnset_seen_counters.len(),
        final_state_expected_property_order_ok = summary.final_state_expected_property_order_ok,
        final_metatag_idset_given_includes_all_expected_folder_source_key_counters =
            summary.final_state_idset_given_includes_all_expected_folder_source_counters,
        final_metatag_cnset_seen_includes_all_expected_folder_change_counters =
            summary.final_state_cnset_seen_includes_all_expected_folder_change_counters,
        "rca debug mapi hierarchy final state"
    );
}

fn log_hierarchy_microsoft_payload_comparison(
    sync_type: u8,
    sync_flags: u16,
    sync_extra_flags: u32,
    folder_id: u64,
    requested_property_tags: &[u32],
    summary: &HierarchyTransferDebugSummary,
) {
    let comparison = hierarchy_microsoft_payload_comparison(
        sync_flags,
        sync_extra_flags,
        folder_id,
        requested_property_tags,
        summary,
    );
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        request_rop_id = "0x70",
        sync_type = format_args!("0x{sync_type:02x}"),
        folder_id = format_args!("0x{folder_id:016x}"),
        exchange_folder_change_required_missing_row_count =
            comparison.required_missing_row_names.len(),
        exchange_folder_change_required_missing_rows =
            %comparison.required_missing_row_names.join(","),
        exchange_folder_id_expected_by_eid = comparison.folder_id_expected_by_eid,
        exchange_folder_id_presence_mismatch_count =
            comparison.folder_id_presence_mismatch_rows.len(),
        exchange_folder_id_presence_mismatch_rows =
            %comparison.folder_id_presence_mismatch_rows.join(","),
        exchange_parent_folder_id_expected_by_no_foreign_identifiers =
            comparison.parent_folder_id_expected_by_no_foreign_identifiers,
        exchange_parent_folder_id_recommended_by_eid =
            comparison.parent_folder_id_recommended_by_eid,
        exchange_parent_folder_id_missing_required_count =
            comparison.parent_folder_id_missing_required_rows.len(),
        exchange_parent_folder_id_missing_required_rows =
            %comparison.parent_folder_id_missing_required_rows.join(","),
        exchange_optional_property_tags = %format_property_tags(&comparison.optional_property_tags),
        exchange_optional_property_names =
            %format_property_tag_names(&comparison.optional_property_tags),
        exchange_requested_excluded_property_present_tags =
            %format_property_tags(&comparison.requested_excluded_property_present_tags),
        exchange_requested_excluded_property_present_names =
            %format_property_tag_names(&comparison.requested_excluded_property_present_tags),
        exchange_final_state_exact_property_sequence =
            comparison.final_state_exact_property_sequence,
        exchange_final_state_missing_property_tags =
            %format_property_tags(&comparison.final_state_missing_property_tags),
        exchange_final_state_missing_property_names =
            %format_property_tag_names(&comparison.final_state_missing_property_tags),
        exchange_final_state_extra_property_tags =
            %format_property_tags(&comparison.final_state_extra_property_tags),
        exchange_final_state_extra_property_names =
            %format_property_tag_names(&comparison.final_state_extra_property_tags),
        exchange_final_state_idset_missing_source_counters =
            %format_counter_list(&comparison.final_state_idset_missing_source_counters),
        exchange_final_state_idset_extra_source_counters =
            %format_counter_list(&comparison.final_state_idset_extra_source_counters),
        exchange_final_state_cnset_missing_change_counters =
            %format_counter_list(&comparison.final_state_cnset_missing_change_counters),
        exchange_final_state_cnset_extra_change_counters =
            %format_counter_list(&comparison.final_state_cnset_extra_change_counters),
        "rca debug mapi hierarchy microsoft payload comparison"
    );
}

fn hierarchy_property_filter_mode(
    sync_flags: u16,
    requested_property_tags: &[u32],
) -> &'static str {
    if requested_property_tags.is_empty() {
        "none"
    } else if sync_flags & 0x0080 == 0 {
        "exclude"
    } else {
        "only-specified"
    }
}

#[derive(Debug, PartialEq, Eq)]
struct HierarchyMicrosoftPayloadComparison {
    required_missing_row_names: Vec<String>,
    folder_id_expected_by_eid: bool,
    folder_id_presence_mismatch_rows: Vec<String>,
    parent_folder_id_expected_by_no_foreign_identifiers: bool,
    parent_folder_id_recommended_by_eid: bool,
    parent_folder_id_missing_required_rows: Vec<String>,
    optional_property_tags: Vec<u32>,
    requested_excluded_property_present_tags: Vec<u32>,
    final_state_exact_property_sequence: bool,
    final_state_missing_property_tags: Vec<u32>,
    final_state_extra_property_tags: Vec<u32>,
    final_state_idset_missing_source_counters: Vec<u64>,
    final_state_idset_extra_source_counters: Vec<u64>,
    final_state_cnset_missing_change_counters: Vec<u64>,
    final_state_cnset_extra_change_counters: Vec<u64>,
}

fn hierarchy_microsoft_payload_comparison(
    sync_flags: u16,
    sync_extra_flags: u32,
    sync_root_folder_id: u64,
    requested_property_tags: &[u32],
    summary: &HierarchyTransferDebugSummary,
) -> HierarchyMicrosoftPayloadComparison {
    let folder_id_expected_by_eid = sync_extra_flags & SYNC_EXTRA_FLAG_EID != 0;
    let parent_folder_id_expected_by_no_foreign_identifiers =
        sync_flags & SYNC_FLAG_NO_FOREIGN_IDENTIFIERS != 0;
    let parent_folder_id_recommended_by_eid = folder_id_expected_by_eid;
    let required_tags = microsoft_folder_change_required_tags();
    let mut optional_property_tags = BTreeSet::new();
    let mut requested_excluded_property_present_tags = BTreeSet::new();
    let mut required_missing_row_names = Vec::new();
    let mut folder_id_presence_mismatch_rows = Vec::new();
    let mut parent_folder_id_missing_required_rows = Vec::new();

    for row in &summary.rows {
        if !required_tags
            .iter()
            .all(|required| row.property_tags.contains(required))
        {
            required_missing_row_names.push(row.display_name.clone());
        }

        if row.property_tags.contains(&PID_TAG_FOLDER_ID) != folder_id_expected_by_eid {
            folder_id_presence_mismatch_rows.push(row.display_name.clone());
        }

        if parent_folder_id_expected_by_no_foreign_identifiers
            && !row.property_tags.contains(&PID_TAG_PARENT_FOLDER_ID)
        {
            parent_folder_id_missing_required_rows.push(row.display_name.clone());
        }

        for tag in &row.property_tags {
            if !required_tags.contains(tag)
                && *tag != PID_TAG_FOLDER_ID
                && *tag != PID_TAG_PARENT_FOLDER_ID
            {
                optional_property_tags.insert(*tag);
            }
            if requested_property_tags.contains(tag)
                && hierarchy_property_filter_mode(sync_flags, requested_property_tags) == "exclude"
            {
                requested_excluded_property_present_tags.insert(*tag);
            }
        }
    }

    let expected_final_state_tags = [META_TAG_IDSET_GIVEN, META_TAG_CNSET_SEEN];
    let final_state_missing_property_tags = expected_final_state_tags
        .iter()
        .copied()
        .filter(|tag| !summary.final_state_property_tags.contains(tag))
        .collect::<Vec<_>>();
    let final_state_extra_property_tags = summary
        .final_state_property_tags
        .iter()
        .copied()
        .filter(|tag| !expected_final_state_tags.contains(tag))
        .collect::<Vec<_>>();
    let expected_source_counters = summary
        .rows
        .iter()
        .filter_map(|row| row.source_counter)
        .collect::<Vec<_>>();
    let mut expected_change_counters = summary
        .rows
        .iter()
        .filter_map(|row| row.change_counter)
        .collect::<Vec<_>>();
    expected_change_counters.push(change_number_for_store_id(sync_root_folder_id));

    HierarchyMicrosoftPayloadComparison {
        required_missing_row_names,
        folder_id_expected_by_eid,
        folder_id_presence_mismatch_rows,
        parent_folder_id_expected_by_no_foreign_identifiers,
        parent_folder_id_recommended_by_eid,
        parent_folder_id_missing_required_rows,
        optional_property_tags: optional_property_tags.into_iter().collect(),
        requested_excluded_property_present_tags: requested_excluded_property_present_tags
            .into_iter()
            .collect(),
        final_state_exact_property_sequence: summary.final_state_property_tags.as_slice()
            == expected_final_state_tags.as_slice(),
        final_state_missing_property_tags,
        final_state_extra_property_tags,
        final_state_idset_missing_source_counters: counter_difference(
            &expected_source_counters,
            &summary.final_state_idset_given_counters,
        ),
        final_state_idset_extra_source_counters: counter_difference(
            &summary.final_state_idset_given_counters,
            &expected_source_counters,
        ),
        final_state_cnset_missing_change_counters: counter_difference(
            &expected_change_counters,
            &summary.final_state_cnset_seen_counters,
        ),
        final_state_cnset_extra_change_counters: counter_difference(
            &summary.final_state_cnset_seen_counters,
            &expected_change_counters,
        ),
    }
}

fn microsoft_folder_change_required_tags() -> [u32; 6] {
    [
        PID_TAG_PARENT_SOURCE_KEY,
        PID_TAG_SOURCE_KEY,
        PID_TAG_LAST_MODIFICATION_TIME,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_DISPLAY_NAME_W,
    ]
}

#[derive(Debug, PartialEq, Eq)]
struct HierarchySemanticValidation {
    sync_root_source_counter: u64,
    sync_root_change_counter: u64,
    sync_root_row_present: bool,
    sync_root_row_index: usize,
    sync_root_display_name: String,
    sync_root_folder_id: String,
    sync_root_parent_folder_id: String,
    sync_root_parent_source_key_len: usize,
    sync_root_folder_type: i32,
    sync_root_access: i32,
    sync_root_subfolders: bool,
    sync_root_counter_in_final_idset: bool,
    sync_root_counter_in_final_cnset: bool,
    first_row_name: String,
    first_row_folder_id: String,
    first_row_parent_folder_id: String,
    parent_before_child_violations: usize,
    root_inclusive_idset_given_len: usize,
    root_inclusive_cnset_seen_len: usize,
    root_inclusive_idset_given_delta_bytes: isize,
    root_inclusive_cnset_seen_delta_bytes: isize,
    root_inclusive_idset_given_summary: String,
    root_inclusive_cnset_seen_summary: String,
    top_level_row_count: usize,
    nested_row_count: usize,
    rows_without_folder_id: usize,
    rows_missing_core_property_count: usize,
    rows_with_content_counts_present: usize,
    rows_with_folder_type_present: usize,
    rows_with_access_present: usize,
    idset_missing_source_counters: Vec<u64>,
    idset_extra_source_counters: Vec<u64>,
    cnset_missing_change_counters: Vec<u64>,
    cnset_extra_change_counters: Vec<u64>,
    top_level_row_names: String,
    rows_missing_core_property_names: String,
    semantic_flags: String,
}

fn hierarchy_semantic_validation(
    sync_root_folder_id: u64,
    summary: &HierarchyTransferDebugSummary,
) -> HierarchySemanticValidation {
    let sync_root_source_counter =
        crate::mapi::identity::global_counter_from_store_id(sync_root_folder_id)
            .unwrap_or_else(|| change_number_for_store_id(sync_root_folder_id));
    let sync_root_change_counter = change_number_for_store_id(sync_root_folder_id);
    let expected_source_counters = summary
        .rows
        .iter()
        .filter_map(|row| row.source_counter)
        .collect::<Vec<_>>();
    let mut expected_change_counters = summary
        .rows
        .iter()
        .filter_map(|row| row.change_counter)
        .collect::<Vec<_>>();
    if !expected_change_counters.contains(&sync_root_change_counter) {
        expected_change_counters.push(sync_root_change_counter);
    }
    let top_level_rows = summary
        .rows
        .iter()
        .filter(|row| row.parent_source_key_len == 0)
        .collect::<Vec<_>>();
    let rows_missing_core_properties = summary
        .rows
        .iter()
        .filter(|row| !row.missing_core_property_tags.is_empty())
        .collect::<Vec<_>>();
    let sync_root_row = summary
        .rows
        .iter()
        .find(|row| row.source_counter == Some(sync_root_source_counter));
    let first_row = summary.rows.first();

    let idset_missing_source_counters = counter_difference(
        &expected_source_counters,
        &summary.final_state_idset_given_counters,
    );
    let idset_extra_source_counters = counter_difference(
        &summary.final_state_idset_given_counters,
        &expected_source_counters,
    );
    let cnset_missing_change_counters = counter_difference(
        &expected_change_counters,
        &summary.final_state_cnset_seen_counters,
    );
    let cnset_extra_change_counters = counter_difference(
        &summary.final_state_cnset_seen_counters,
        &expected_change_counters,
    );
    let root_inclusive_idset_given = root_inclusive_idset(
        &summary.final_state_idset_given_counters,
        sync_root_source_counter,
    );
    let root_inclusive_cnset_seen = root_inclusive_idset(
        &summary.final_state_cnset_seen_counters,
        sync_root_change_counter,
    );
    let mut semantic_flags = Vec::new();
    if !summary.stream_end_marker_seen {
        semantic_flags.push("missing_stream_end");
    }
    if !summary.final_state_present {
        semantic_flags.push("missing_final_state");
    }
    if !summary.final_state_expected_property_order_ok {
        semantic_flags.push("final_state_order");
    }
    if !idset_missing_source_counters.is_empty() {
        semantic_flags.push("idset_missing_source");
    }
    if !cnset_missing_change_counters.is_empty() {
        semantic_flags.push("cnset_missing_change");
    }
    if !rows_missing_core_properties.is_empty() {
        semantic_flags.push("row_missing_core");
    }
    if summary.parent_before_child_violations > 0 {
        semantic_flags.push("parent_before_child");
    }
    if top_level_rows.is_empty() {
        semantic_flags.push("no_top_level_rows");
    }

    HierarchySemanticValidation {
        sync_root_source_counter,
        sync_root_change_counter,
        sync_root_row_present: expected_source_counters.contains(&sync_root_source_counter),
        sync_root_row_index: sync_root_row.map(|row| row.row_index).unwrap_or_default(),
        sync_root_display_name: sync_root_row
            .map(|row| row.display_name.clone())
            .unwrap_or_default(),
        sync_root_folder_id: sync_root_row
            .and_then(|row| row.folder_id.map(format_u64_hex))
            .unwrap_or_default(),
        sync_root_parent_folder_id: sync_root_row
            .and_then(|row| row.parent_folder_id.map(format_u64_hex))
            .unwrap_or_default(),
        sync_root_parent_source_key_len: sync_root_row
            .map(|row| row.parent_source_key_len)
            .unwrap_or_default(),
        sync_root_folder_type: sync_root_row
            .and_then(|row| row.folder_type)
            .unwrap_or_default(),
        sync_root_access: sync_root_row.and_then(|row| row.access).unwrap_or_default(),
        sync_root_subfolders: sync_root_row
            .and_then(|row| row.subfolders)
            .unwrap_or_default(),
        sync_root_counter_in_final_idset: summary
            .final_state_idset_given_counters
            .contains(&sync_root_source_counter),
        sync_root_counter_in_final_cnset: summary
            .final_state_cnset_seen_counters
            .contains(&sync_root_change_counter),
        first_row_name: first_row
            .map(|row| row.display_name.clone())
            .unwrap_or_default(),
        first_row_folder_id: first_row
            .and_then(|row| row.folder_id.map(format_u64_hex))
            .unwrap_or_default(),
        first_row_parent_folder_id: first_row
            .and_then(|row| row.parent_folder_id.map(format_u64_hex))
            .unwrap_or_default(),
        parent_before_child_violations: summary.parent_before_child_violations,
        root_inclusive_idset_given_len: root_inclusive_idset_given.len(),
        root_inclusive_cnset_seen_len: root_inclusive_cnset_seen.len(),
        root_inclusive_idset_given_delta_bytes: root_inclusive_idset_given.len() as isize
            - summary.final_state_idset_given_len as isize,
        root_inclusive_cnset_seen_delta_bytes: root_inclusive_cnset_seen.len() as isize
            - summary.final_state_cnset_seen_len as isize,
        root_inclusive_idset_given_summary: format_replguid_globset_debug(
            &root_inclusive_idset_given,
        ),
        root_inclusive_cnset_seen_summary: format_replguid_globset_debug(
            &root_inclusive_cnset_seen,
        ),
        top_level_row_count: top_level_rows.len(),
        nested_row_count: summary.rows.len().saturating_sub(top_level_rows.len()),
        rows_without_folder_id: summary
            .rows
            .iter()
            .filter(|row| row.folder_id.is_none())
            .count(),
        rows_missing_core_property_count: rows_missing_core_properties.len(),
        rows_with_content_counts_present: summary
            .rows
            .iter()
            .filter(|row| row.content_count.is_some() || row.content_unread_count.is_some())
            .count(),
        rows_with_folder_type_present: summary
            .rows
            .iter()
            .filter(|row| row.folder_type.is_some())
            .count(),
        rows_with_access_present: summary
            .rows
            .iter()
            .filter(|row| row.access.is_some())
            .count(),
        idset_missing_source_counters,
        idset_extra_source_counters,
        cnset_missing_change_counters,
        cnset_extra_change_counters,
        top_level_row_names: top_level_rows
            .iter()
            .map(|row| row.display_name.as_str())
            .collect::<Vec<_>>()
            .join(","),
        rows_missing_core_property_names: rows_missing_core_properties
            .iter()
            .map(|row| row.display_name.as_str())
            .collect::<Vec<_>>()
            .join(","),
        semantic_flags: if semantic_flags.is_empty() {
            "ok".to_string()
        } else {
            semantic_flags.join(",")
        },
    }
}

fn root_inclusive_idset(existing_counters: &[u64], root_counter: u64) -> Vec<u8> {
    let mut counters = existing_counters.to_vec();
    counters.push(root_counter);
    replguid_idset_from_counters(&counters)
}

fn counter_difference(left: &[u64], right: &[u64]) -> Vec<u64> {
    let right = right.iter().copied().collect::<BTreeSet<_>>();
    left.iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .difference(&right)
        .copied()
        .collect()
}

fn format_counter_list(counters: &[u64]) -> String {
    counters
        .iter()
        .map(|counter| counter.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

#[derive(Debug, Default, PartialEq, Eq)]
struct HierarchyTransferDebugSummary {
    marker_tags: Vec<u32>,
    property_count: usize,
    stream_end_marker_seen: bool,
    folder_change_count: usize,
    final_state_present: bool,
    parent_before_child_violations: usize,
    zero_length_parent_source_key_count: usize,
    nonzero_parent_source_key_count: usize,
    source_key_lengths: Vec<usize>,
    change_key_lengths: Vec<usize>,
    final_state_property_tags: Vec<u32>,
    final_state_property_lengths: Vec<usize>,
    final_state_idset_given_len: usize,
    final_state_cnset_seen_len: usize,
    final_state_idset_given_summary: Option<String>,
    final_state_cnset_seen_summary: Option<String>,
    final_state_idset_given_counters: Vec<u64>,
    final_state_cnset_seen_counters: Vec<u64>,
    final_state_expected_property_order_ok: bool,
    final_state_idset_given_includes_all_expected_folder_source_counters: bool,
    final_state_cnset_seen_includes_all_expected_folder_change_counters: bool,
    emitted_property_tags: Vec<u32>,
    rows: Vec<HierarchyTransferRowDebug>,
}

impl HierarchyTransferDebugSummary {
    fn first_folder_name(&self) -> &str {
        self.rows
            .first()
            .map(|row| row.display_name.as_str())
            .unwrap_or_default()
    }

    fn last_folder_name(&self) -> &str {
        self.rows
            .last()
            .map(|row| row.display_name.as_str())
            .unwrap_or_default()
    }
}

#[derive(Default)]
struct HierarchyTransferFolderDebug {
    source_key: Option<Vec<u8>>,
    parent_source_key: Option<Vec<u8>>,
    change_key: Option<Vec<u8>>,
    predecessor_change_list: Option<Vec<u8>>,
    display_name: Option<String>,
    container_class: Option<String>,
    folder_id: Option<u64>,
    parent_folder_id: Option<u64>,
    last_modification_time: Option<u64>,
    change_number: Option<u64>,
    content_count: Option<i32>,
    content_unread_count: Option<i32>,
    folder_type: Option<i32>,
    local_commit_time_max: Option<u64>,
    deleted_count_total: Option<i32>,
    message_size: Option<i32>,
    access: Option<i32>,
    subfolders: Option<bool>,
    property_tags: Vec<u32>,
}

#[derive(Debug, PartialEq, Eq)]
struct HierarchyTransferRowDebug {
    row_index: usize,
    display_name: String,
    container_class: String,
    folder_id: Option<u64>,
    parent_folder_id: Option<u64>,
    source_key_len: usize,
    parent_source_key_len: usize,
    change_key_len: usize,
    source_counter: Option<u64>,
    change_counter: Option<u64>,
    predecessor_change_list_len: usize,
    last_modification_time: Option<u64>,
    change_number: Option<u64>,
    content_count: Option<i32>,
    content_unread_count: Option<i32>,
    folder_type: Option<i32>,
    local_commit_time_max: Option<u64>,
    deleted_count_total: Option<i32>,
    message_size: Option<i32>,
    access: Option<i32>,
    subfolders: Option<bool>,
    source_key_hex: String,
    parent_source_key_hex: String,
    change_key_hex: String,
    property_tags: Vec<u32>,
    missing_core_property_tags: Vec<u32>,
}

struct FastTransferDebugProperty {
    tag: u32,
    value: Vec<u8>,
    next_offset: usize,
}

fn decode_hierarchy_transfer_debug_summary(
    bytes: &[u8],
) -> Result<HierarchyTransferDebugSummary, String> {
    let mut offset = 0;
    let mut current_folder: Option<HierarchyTransferFolderDebug> = None;
    let mut seen_source_keys = Vec::<Vec<u8>>::new();
    let mut emitted_property_tags = BTreeSet::new();
    let mut summary = HierarchyTransferDebugSummary::default();
    let mut in_final_state = false;

    while offset < bytes.len() {
        let tag = read_debug_u32(bytes, offset)?;
        if hierarchy_debug_marker(tag) {
            summary.marker_tags.push(tag);
            match tag {
                INCR_SYNC_CHG => {
                    if let Some(folder) = current_folder.take() {
                        finish_hierarchy_debug_folder(folder, &mut seen_source_keys, &mut summary);
                    }
                    current_folder = Some(HierarchyTransferFolderDebug::default());
                }
                INCR_SYNC_STATE_BEGIN => {
                    if let Some(folder) = current_folder.take() {
                        finish_hierarchy_debug_folder(folder, &mut seen_source_keys, &mut summary);
                    }
                    summary.final_state_present = true;
                    in_final_state = true;
                }
                INCR_SYNC_STATE_END => {
                    in_final_state = false;
                }
                INCR_SYNC_END => {
                    if let Some(folder) = current_folder.take() {
                        finish_hierarchy_debug_folder(folder, &mut seen_source_keys, &mut summary);
                    }
                    summary.stream_end_marker_seen = true;
                    offset += 4;
                    if offset != bytes.len() {
                        return Err("trailing bytes after IncrSyncEnd".into());
                    }
                    break;
                }
                _ => unreachable!(),
            }
            offset += 4;
            continue;
        }

        let property = parse_debug_fast_transfer_property(bytes, offset)?;
        offset = property.next_offset;
        summary.property_count += 1;
        emitted_property_tags.insert(property.tag);

        if in_final_state && current_folder.is_none() {
            collect_final_state_debug_property(&property, &mut summary);
        }

        if let Some(folder) = current_folder.as_mut() {
            folder.property_tags.push(property.tag);
            match property.tag {
                PID_TAG_PARENT_SOURCE_KEY => folder.parent_source_key = Some(property.value),
                PID_TAG_SOURCE_KEY => folder.source_key = Some(property.value),
                PID_TAG_CHANGE_KEY => folder.change_key = Some(property.value),
                PID_TAG_PREDECESSOR_CHANGE_LIST => {
                    folder.predecessor_change_list = Some(property.value)
                }
                PID_TAG_DISPLAY_NAME_W => {
                    folder.display_name = decode_debug_utf16z(&property.value)
                }
                PID_TAG_CONTAINER_CLASS_W => {
                    folder.container_class = decode_debug_utf16z(&property.value)
                }
                PID_TAG_FOLDER_ID => folder.folder_id = decode_debug_u64(&property.value),
                PID_TAG_PARENT_FOLDER_ID => {
                    folder.parent_folder_id = decode_debug_u64(&property.value)
                }
                PID_TAG_LAST_MODIFICATION_TIME => {
                    folder.last_modification_time = decode_debug_u64(&property.value)
                }
                PID_TAG_CHANGE_NUMBER => folder.change_number = decode_debug_u64(&property.value),
                PID_TAG_CONTENT_COUNT => folder.content_count = decode_debug_i32(&property.value),
                PID_TAG_CONTENT_UNREAD_COUNT => {
                    folder.content_unread_count = decode_debug_i32(&property.value)
                }
                PID_TAG_FOLDER_TYPE => folder.folder_type = decode_debug_i32(&property.value),
                PID_TAG_LOCAL_COMMIT_TIME_MAX => {
                    folder.local_commit_time_max = decode_debug_u64(&property.value)
                }
                PID_TAG_DELETED_COUNT_TOTAL => {
                    folder.deleted_count_total = decode_debug_i32(&property.value)
                }
                PID_TAG_MESSAGE_SIZE => folder.message_size = decode_debug_i32(&property.value),
                PID_TAG_ACCESS => folder.access = decode_debug_i32(&property.value),
                PID_TAG_SUBFOLDERS => folder.subfolders = decode_debug_bool(&property.value),
                _ => {}
            }
        } else if !in_final_state {
            return Err(format!(
                "property 0x{:08x} appears outside folderChange or final state",
                property.tag
            ));
        }
    }

    if let Some(folder) = current_folder.take() {
        finish_hierarchy_debug_folder(folder, &mut seen_source_keys, &mut summary);
    }
    summary.emitted_property_tags = emitted_property_tags.into_iter().collect();
    finalize_hierarchy_debug_summary(&mut summary);
    Ok(summary)
}

fn collect_final_state_debug_property(
    property: &FastTransferDebugProperty,
    summary: &mut HierarchyTransferDebugSummary,
) {
    summary.final_state_property_tags.push(property.tag);
    summary
        .final_state_property_lengths
        .push(property.value.len());
    match property.tag {
        META_TAG_IDSET_GIVEN => {
            summary.final_state_idset_given_len = property.value.len();
            summary.final_state_idset_given_summary =
                Some(format_replguid_globset_debug(&property.value));
            summary.final_state_idset_given_counters =
                replguid_globset_counters(&property.value).unwrap_or_default();
        }
        META_TAG_CNSET_SEEN => {
            summary.final_state_cnset_seen_len = property.value.len();
            summary.final_state_cnset_seen_summary =
                Some(format_replguid_globset_debug(&property.value));
            summary.final_state_cnset_seen_counters =
                replguid_globset_counters(&property.value).unwrap_or_default();
        }
        _ => {}
    }
}

fn finalize_hierarchy_debug_summary(summary: &mut HierarchyTransferDebugSummary) {
    summary.final_state_expected_property_order_ok = summary.final_state_property_tags.as_slice()
        == [META_TAG_IDSET_GIVEN, META_TAG_CNSET_SEEN].as_slice();
    let source_counters = summary
        .rows
        .iter()
        .filter_map(|row| row.source_counter)
        .collect::<Vec<_>>();
    let change_counters = summary
        .rows
        .iter()
        .filter_map(|row| row.change_counter)
        .collect::<Vec<_>>();
    summary.final_state_idset_given_includes_all_expected_folder_source_counters =
        source_counters.len() == summary.folder_change_count
            && counters_include_all(&summary.final_state_idset_given_counters, &source_counters);
    summary.final_state_cnset_seen_includes_all_expected_folder_change_counters =
        change_counters.len() == summary.folder_change_count
            && counters_include_all(&summary.final_state_cnset_seen_counters, &change_counters);
}

fn counters_include_all(haystack: &[u64], needles: &[u64]) -> bool {
    let haystack = haystack.iter().copied().collect::<BTreeSet<_>>();
    needles.iter().all(|counter| haystack.contains(counter))
}

fn finish_hierarchy_debug_folder(
    folder: HierarchyTransferFolderDebug,
    seen_source_keys: &mut Vec<Vec<u8>>,
    summary: &mut HierarchyTransferDebugSummary,
) {
    summary.folder_change_count += 1;
    let parent_source_key_present = folder.parent_source_key.is_some();
    let parent_source_key = folder.parent_source_key.unwrap_or_default();
    if !parent_source_key.is_empty() {
        summary.nonzero_parent_source_key_count += 1;
        if !hierarchy_debug_known_parent_source_key(&parent_source_key)
            && !seen_source_keys
                .iter()
                .any(|source_key| source_key.as_slice() == parent_source_key.as_slice())
        {
            summary.parent_before_child_violations += 1;
        }
    } else if parent_source_key_present {
        summary.zero_length_parent_source_key_count += 1;
    }
    let source_key = folder.source_key.unwrap_or_default();
    if !source_key.is_empty() {
        summary.source_key_lengths.push(source_key.len());
        seen_source_keys.push(source_key.clone());
    }
    let change_key = folder.change_key.unwrap_or_default();
    if !change_key.is_empty() {
        summary.change_key_lengths.push(change_key.len());
    }
    let predecessor_change_list = folder.predecessor_change_list.unwrap_or_default();
    let missing_core_property_tags = missing_hierarchy_core_property_tags(&folder.property_tags);
    let parent_source_key_index =
        property_position(&folder.property_tags, PID_TAG_PARENT_SOURCE_KEY);
    let source_key_index = property_position(&folder.property_tags, PID_TAG_SOURCE_KEY);
    let last_modification_time_index =
        property_position(&folder.property_tags, PID_TAG_LAST_MODIFICATION_TIME);
    let change_key_index = property_position(&folder.property_tags, PID_TAG_CHANGE_KEY);
    let predecessor_change_list_index =
        property_position(&folder.property_tags, PID_TAG_PREDECESSOR_CHANGE_LIST);
    let display_name_index = property_position(&folder.property_tags, PID_TAG_DISPLAY_NAME_W);
    let container_class_index = property_position(&folder.property_tags, PID_TAG_CONTAINER_CLASS_W);
    let subfolders_index = property_position(&folder.property_tags, PID_TAG_SUBFOLDERS);
    let identity_properties_before_display_name =
        hierarchy_identity_properties_before_display_name(&folder.property_tags);
    let row = HierarchyTransferRowDebug {
        row_index: summary.folder_change_count,
        display_name: folder.display_name.unwrap_or_default(),
        container_class: folder.container_class.unwrap_or_default(),
        folder_id: folder.folder_id,
        parent_folder_id: folder.parent_folder_id,
        source_key_len: source_key.len(),
        parent_source_key_len: parent_source_key.len(),
        change_key_len: change_key.len(),
        source_counter: counter_from_xid(&source_key),
        change_counter: counter_from_xid(&change_key),
        predecessor_change_list_len: predecessor_change_list.len(),
        last_modification_time: folder.last_modification_time,
        change_number: folder.change_number,
        content_count: folder.content_count,
        content_unread_count: folder.content_unread_count,
        folder_type: folder.folder_type,
        local_commit_time_max: folder.local_commit_time_max,
        deleted_count_total: folder.deleted_count_total,
        message_size: folder.message_size,
        access: folder.access,
        subfolders: folder.subfolders,
        source_key_hex: format_debug_hex(&source_key),
        parent_source_key_hex: format_debug_hex(&parent_source_key),
        change_key_hex: format_debug_hex(&change_key),
        property_tags: folder.property_tags,
        missing_core_property_tags,
    };
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        request_rop_id = "0x70",
        row_index = row.row_index,
        display_name = %row.display_name,
        container_class = %row.container_class,
        folder_id = row.folder_id.map(format_u64_hex).unwrap_or_default(),
        parent_folder_id = row.parent_folder_id.map(format_u64_hex).unwrap_or_default(),
        source_key_len = row.source_key_len,
        parent_source_key_len = row.parent_source_key_len,
        change_key_len = row.change_key_len,
        predecessor_change_list_len = row.predecessor_change_list_len,
        last_modification_time = row.last_modification_time.unwrap_or_default(),
        change_number = row.change_number.unwrap_or_default(),
        change_number_present = row.change_number.is_some(),
        content_count = row.content_count.unwrap_or_default(),
        content_count_present = row.content_count.is_some(),
        content_unread_count = row.content_unread_count.unwrap_or_default(),
        content_unread_count_present = row.content_unread_count.is_some(),
        folder_type = row.folder_type.unwrap_or_default(),
        folder_type_present = row.folder_type.is_some(),
        local_commit_time_max = row.local_commit_time_max.unwrap_or_default(),
        local_commit_time_max_present = row.local_commit_time_max.is_some(),
        deleted_count_total = row.deleted_count_total.unwrap_or_default(),
        deleted_count_total_present = row.deleted_count_total.is_some(),
        message_size = row.message_size.unwrap_or_default(),
        message_size_present = row.message_size.is_some(),
        access = row.access.unwrap_or_default(),
        access_present = row.access.is_some(),
        subfolders = row.subfolders.unwrap_or_default(),
        subfolders_present = row.subfolders.is_some(),
        source_key_hex = %row.source_key_hex,
        parent_source_key_hex = %row.parent_source_key_hex,
        change_key_hex = %row.change_key_hex,
        property_count = row.property_tags.len(),
        first_property_tag = %row.property_tags.first().map(|tag| format!("0x{tag:08x}")).unwrap_or_default(),
        first_property_name = row.property_tags.first().map(|tag| property_tag_debug_name(*tag)).unwrap_or_default(),
        last_property_tag = %row.property_tags.last().map(|tag| format!("0x{tag:08x}")).unwrap_or_default(),
        last_property_name = row.property_tags.last().map(|tag| property_tag_debug_name(*tag)).unwrap_or_default(),
        parent_source_key_property_index = parent_source_key_index,
        source_key_property_index = source_key_index,
        last_modification_time_property_index = last_modification_time_index,
        change_key_property_index = change_key_index,
        predecessor_change_list_property_index = predecessor_change_list_index,
        display_name_property_index = display_name_index,
        container_class_property_index = container_class_index,
        subfolders_property_index = subfolders_index,
        identity_properties_before_display_name,
        emitted_property_tags = %format_property_tags(&row.property_tags),
        emitted_property_names = %format_property_tag_names(&row.property_tags),
        missing_core_property_tags = %format_property_tags(&row.missing_core_property_tags),
        missing_core_property_names = %format_property_tag_names(&row.missing_core_property_tags),
        "rca debug mapi hierarchy transfer row semantics"
    );
    summary.rows.push(row);
}

fn missing_hierarchy_core_property_tags(property_tags: &[u32]) -> Vec<u32> {
    [
        PID_TAG_PARENT_SOURCE_KEY,
        PID_TAG_SOURCE_KEY,
        PID_TAG_LAST_MODIFICATION_TIME,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_DISPLAY_NAME_W,
        PID_TAG_SUBFOLDERS,
    ]
    .into_iter()
    .filter(|tag| !property_tags.contains(tag))
    .collect()
}

fn property_position(property_tags: &[u32], property_tag: u32) -> usize {
    property_tags
        .iter()
        .position(|tag| *tag == property_tag)
        .map(|index| index + 1)
        .unwrap_or_default()
}

fn hierarchy_identity_properties_before_display_name(property_tags: &[u32]) -> bool {
    let display_name = property_position(property_tags, PID_TAG_DISPLAY_NAME_W);
    if display_name == 0 {
        return false;
    }
    [
        PID_TAG_PARENT_SOURCE_KEY,
        PID_TAG_SOURCE_KEY,
        PID_TAG_LAST_MODIFICATION_TIME,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
    ]
    .into_iter()
    .all(|tag| {
        let position = property_position(property_tags, tag);
        position != 0 && position < display_name
    })
}

fn decode_debug_i32(bytes: &[u8]) -> Option<i32> {
    (bytes.len() == 4).then(|| i32::from_le_bytes(bytes.try_into().unwrap()))
}

fn decode_debug_u64(bytes: &[u8]) -> Option<u64> {
    (bytes.len() == 8).then(|| u64::from_le_bytes(bytes.try_into().unwrap()))
}

fn decode_debug_bool(bytes: &[u8]) -> Option<bool> {
    (bytes.len() == 2).then(|| u16::from_le_bytes(bytes.try_into().unwrap()) != 0)
}

fn decode_debug_utf16z(bytes: &[u8]) -> Option<String> {
    if bytes.len() % 2 != 0 {
        return None;
    }
    let mut units = bytes
        .chunks_exact(2)
        .map(|chunk| u16::from_le_bytes(chunk.try_into().unwrap()))
        .collect::<Vec<_>>();
    if units.last() == Some(&0) {
        units.pop();
    }
    String::from_utf16(&units).ok()
}

fn format_debug_hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join("")
}

fn format_u64_hex(value: u64) -> String {
    format!("0x{value:016x}")
}

fn format_property_tag_names(tags: &[u32]) -> String {
    tags.iter()
        .map(|tag| property_tag_debug_name(*tag))
        .collect::<Vec<_>>()
        .join(",")
}

fn property_tag_debug_name(tag: u32) -> &'static str {
    match tag {
        PID_TAG_DISPLAY_NAME_W => "PidTagDisplayName",
        PID_TAG_CONTENT_COUNT => "PidTagContentCount",
        PID_TAG_CONTENT_UNREAD_COUNT => "PidTagContentUnreadCount",
        PID_TAG_SUBFOLDERS => "PidTagSubfolders",
        PID_TAG_FOLDER_TYPE => "PidTagFolderType",
        PID_TAG_CONTAINER_CLASS_W => "PidTagContainerClass",
        PID_TAG_MESSAGE_SIZE => "PidTagMessageSize",
        PID_TAG_LAST_MODIFICATION_TIME => "PidTagLastModificationTime",
        PID_TAG_ACCESS => "PidTagAccess",
        PID_TAG_SOURCE_KEY => "PidTagSourceKey",
        PID_TAG_PARENT_SOURCE_KEY => "PidTagParentSourceKey",
        PID_TAG_CHANGE_KEY => "PidTagChangeKey",
        PID_TAG_PREDECESSOR_CHANGE_LIST => "PidTagPredecessorChangeList",
        PID_TAG_LOCAL_COMMIT_TIME_MAX => "PidTagLocalCommitTimeMax",
        PID_TAG_DELETED_COUNT_TOTAL => "PidTagDeletedCountTotal",
        PID_TAG_FOLDER_ID => "PidTagFolderId",
        PID_TAG_PARENT_FOLDER_ID => "PidTagParentFolderId",
        PID_TAG_CHANGE_NUMBER => "PidTagChangeNumber",
        META_TAG_IDSET_GIVEN => "MetaTagIdsetGiven",
        META_TAG_IDSET_READ => "MetaTagIdsetRead",
        META_TAG_IDSET_UNREAD => "MetaTagIdsetUnread",
        META_TAG_CNSET_SEEN => "MetaTagCnsetSeen",
        META_TAG_CNSET_SEEN_FAI => "MetaTagCnsetSeenFAI",
        META_TAG_CNSET_READ => "MetaTagCnsetRead",
        _ => "unknown",
    }
}

fn hierarchy_debug_known_parent_source_key(source_key: &[u8]) -> bool {
    source_key == source_key_for_store_id(crate::mapi::identity::ROOT_FOLDER_ID).as_slice()
        || source_key
            == source_key_for_store_id(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID).as_slice()
}

fn hierarchy_debug_marker(tag: u32) -> bool {
    matches!(
        tag,
        INCR_SYNC_CHG | INCR_SYNC_STATE_BEGIN | INCR_SYNC_STATE_END | INCR_SYNC_END
    )
}

fn fast_transfer_marker_debug_name(tag: u32) -> &'static str {
    match tag {
        INCR_SYNC_CHG => "IncrSyncChg",
        INCR_SYNC_STATE_BEGIN => "IncrSyncStateBegin",
        INCR_SYNC_STATE_END => "IncrSyncStateEnd",
        INCR_SYNC_END => "IncrSyncEnd",
        _ => "unknown",
    }
}

fn parse_debug_fast_transfer_property(
    bytes: &[u8],
    offset: usize,
) -> Result<FastTransferDebugProperty, String> {
    let tag = read_debug_u32(bytes, offset)?;
    let property_type = tag & 0x0000_FFFF;
    let value_start = offset + 4;
    let (value_start, value_len) = match property_type {
        0x0002 => (value_start, 2),
        0x0003 => (value_start, 4),
        0x000B => (value_start, 2),
        0x0014 | 0x0040 => (value_start, 8),
        0x001E | 0x001F | 0x0102 => {
            let len = read_debug_u32(bytes, value_start)? as usize;
            (value_start + 4, len)
        }
        _ => {
            return Err(format!(
                "unsupported FastTransfer property type in 0x{tag:08x}"
            ))
        }
    };
    let value = read_debug_slice(bytes, value_start, value_len)?.to_vec();
    Ok(FastTransferDebugProperty {
        tag,
        value,
        next_offset: value_start + value_len,
    })
}

fn read_debug_u32(bytes: &[u8], offset: usize) -> Result<u32, String> {
    let slice = read_debug_slice(bytes, offset, 4)?;
    Ok(u32::from_le_bytes(slice.try_into().unwrap()))
}

fn read_debug_slice(bytes: &[u8], offset: usize, len: usize) -> Result<&[u8], String> {
    bytes
        .get(offset..offset.saturating_add(len))
        .ok_or_else(|| format!("FastTransfer atom at offset {offset} overruns stream"))
}

fn format_usize_list(values: &[usize]) -> String {
    values
        .iter()
        .map(usize::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn format_property_tags(tags: &[u32]) -> String {
    tags.iter()
        .map(|tag| format!("0x{tag:08x}"))
        .collect::<Vec<_>>()
        .join(",")
}

pub(crate) fn replguid_globset_debug_summary(value: &[u8]) -> String {
    format_replguid_globset_debug(value)
}

pub(crate) fn final_sync_state_debug_summary(value: &[u8]) -> String {
    match decode_hierarchy_transfer_debug_summary(value) {
        Ok(summary) => format!(
            "bytes={};property_tags={};expected_order={};idset={};cnset={}",
            value.len(),
            format_property_tags(&summary.final_state_property_tags),
            summary.final_state_expected_property_order_ok,
            summary
                .final_state_idset_given_summary
                .as_deref()
                .unwrap_or("missing"),
            summary
                .final_state_cnset_seen_summary
                .as_deref()
                .unwrap_or("missing")
        ),
        Err(error) => format!(
            "bytes={};preview={};parse_error={error}",
            value.len(),
            format_debug_hex(&value[..value.len().min(32)])
        ),
    }
}

fn format_marker_tags(tags: &[u32]) -> String {
    tags.iter()
        .map(|tag| format!("{}:0x{tag:08x}", fast_transfer_marker_debug_name(*tag)))
        .collect::<Vec<_>>()
        .join(",")
}

fn format_replguid_globset_debug(value: &[u8]) -> String {
    let preview = format_debug_hex(&value[..value.len().min(32)]);
    let Some(replica_guid) = value.get(..16) else {
        return format!(
            "bytes={};preview={preview};parse_error=missing_replica_guid",
            value.len()
        );
    };

    let mut offset = 16;
    let mut ranges: Vec<(u64, u64)> = Vec::new();
    let mut parse_error = "";
    let mut saw_end = false;
    while offset < value.len() {
        let command = value[offset];
        offset += 1;
        match command {
            GLOBSET_END_COMMAND => {
                saw_end = true;
                if offset != value.len() {
                    parse_error = "trailing_bytes_after_end";
                }
                break;
            }
            GLOBSET_RANGE_COMMAND => {
                let Some(low) = value.get(offset..offset + 6) else {
                    parse_error = "truncated_range_low";
                    break;
                };
                let Some(high) = value.get(offset + 6..offset + 12) else {
                    parse_error = "truncated_range_high";
                    break;
                };
                let low =
                    crate::mapi::identity::global_counter_from_globcnt(low).unwrap_or_default();
                let high =
                    crate::mapi::identity::global_counter_from_globcnt(high).unwrap_or_default();
                ranges.push((low, high));
                offset += 12;
            }
            _ => {
                parse_error = "unsupported_command";
                break;
            }
        }
    }
    if !saw_end && parse_error.is_empty() {
        parse_error = "missing_end_command";
    }

    let range_summary = ranges
        .iter()
        .take(8)
        .map(|(low, high)| {
            if low == high {
                low.to_string()
            } else {
                format!("{low}-{high}")
            }
        })
        .collect::<Vec<_>>()
        .join(",");

    format!(
        "bytes={};replica_guid={};range_count={};ranges={range_summary};preview={preview};parse_error={parse_error}",
        value.len(),
        format_debug_hex(replica_guid),
        ranges.len()
    )
}

fn replguid_globset_counters(value: &[u8]) -> Result<Vec<u64>, String> {
    if value.len() < 17 {
        return Err("missing_replica_guid".to_string());
    }
    if value[..16] != STORE_REPLICA_GUID {
        return Err("unexpected_replica_guid".to_string());
    }

    let mut offset = 16;
    let mut counters = BTreeSet::new();
    while offset < value.len() {
        let command = value[offset];
        offset += 1;
        match command {
            GLOBSET_END_COMMAND => {
                if offset == value.len() {
                    return Ok(counters.into_iter().collect());
                }
                return Err("trailing_bytes_after_end".to_string());
            }
            GLOBSET_RANGE_COMMAND => {
                let low = value
                    .get(offset..offset + 6)
                    .and_then(crate::mapi::identity::global_counter_from_globcnt)
                    .ok_or_else(|| "truncated_or_invalid_range_low".to_string())?;
                let high: u64 = value
                    .get(offset + 6..offset + 12)
                    .and_then(crate::mapi::identity::global_counter_from_globcnt)
                    .ok_or_else(|| "truncated_or_invalid_range_high".to_string())?;
                for counter in low..=high.max(low) {
                    counters.insert(counter);
                }
                offset += 12;
            }
            _ => return Err("unsupported_command".to_string()),
        }
    }
    Err("missing_end_command".to_string())
}

fn counter_from_xid(value: &[u8]) -> Option<u64> {
    if value.len() != 22 || value[..16] != STORE_REPLICA_GUID {
        return None;
    }
    crate::mapi::identity::global_counter_from_globcnt(value.get(16..22)?)
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
        "suggested_contacts" => crate::mapi::identity::SUGGESTED_CONTACTS_FOLDER_ID,
        "quick_contacts" => crate::mapi::identity::QUICK_CONTACTS_FOLDER_ID,
        "im_contact_list" => crate::mapi::identity::IM_CONTACT_LIST_FOLDER_ID,
        "contacts_search" => crate::mapi::identity::CONTACTS_SEARCH_FOLDER_ID,
        "document_libraries" => crate::mapi::identity::DOCUMENT_LIBRARIES_FOLDER_ID,
        "sync_issues" => crate::mapi::identity::SYNC_ISSUES_FOLDER_ID,
        "conflicts" => crate::mapi::identity::CONFLICTS_FOLDER_ID,
        "local_failures" => crate::mapi::identity::LOCAL_FAILURES_FOLDER_ID,
        "server_failures" => crate::mapi::identity::SERVER_FAILURES_FOLDER_ID,
        "junk" => crate::mapi::identity::JUNK_FOLDER_ID,
        "rss_feeds" => crate::mapi::identity::RSS_FEEDS_FOLDER_ID,
        "tracked_mail_processing" => crate::mapi::identity::TRACKED_MAIL_PROCESSING_FOLDER_ID,
        "todo_search" => crate::mapi::identity::TODO_SEARCH_FOLDER_ID,
        "conversation_action_settings" => {
            crate::mapi::identity::CONVERSATION_ACTION_SETTINGS_FOLDER_ID
        }
        "archive" => crate::mapi::identity::ARCHIVE_FOLDER_ID,
        "conversation_history" => crate::mapi::identity::CONVERSATION_HISTORY_FOLDER_ID,
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
        | "__mapi_shortcuts"
        | "__mapi_freebusy_data" => crate::mapi::identity::ROOT_FOLDER_ID,
        "journal"
        | "notes"
        | "tasks"
        | "reminders"
        | "suggested_contacts"
        | "quick_contacts"
        | "im_contact_list"
        | "contacts_search"
        | "document_libraries"
        | "sync_issues"
        | "junk"
        | "rss_feeds"
        | "tracked_mail_processing"
        | "todo_search"
        | "conversation_action_settings"
        | "archive"
        | "conversation_history" => crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
        "conflicts" | "local_failures" | "server_failures" => {
            crate::mapi::identity::SYNC_ISSUES_FOLDER_ID
        }
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
            "reminders" => "Outlook.Reminder",
            "suggested_contacts" => "IPF.Contact",
            "quick_contacts" => "IPF.Contact.MOC.QuickContacts",
            "im_contact_list" => "IPF.Contact.MOC.ImContactList",
            "contacts_search" => "IPF.Contact",
            "document_libraries" => "IPF.ShortcutFolder",
            "rss_feeds" => "IPF.Note.OutlookHomepage",
            "todo_search" => "IPF.Task",
            "conversation_action_settings" => "IPF.Configuration",
            _ => "IPF.Note",
        })
}

fn mapi_folder_display_name(mailbox: &JmapMailbox) -> &str {
    virtual_special_folder_metadata(mapi_folder_id_for_mailbox(mailbox, 0))
        .map(|(_, name, _, _, _)| name)
        .unwrap_or(&mailbox.name)
}

fn mapi_folder_has_subfolders(mailbox: &JmapMailbox, mailboxes: &[JmapMailbox]) -> bool {
    let folder_id = mapi_folder_id_for_mailbox(mailbox, 0);
    mailboxes
        .iter()
        .any(|candidate| mapi_folder_parent_id_for_mailbox(candidate, mailboxes) == folder_id)
}

fn folder_content_counts(
    folder_id: u64,
    mailbox: &JmapMailbox,
    mailboxes: &[JmapMailbox],
    aggregate_emails: &[JmapEmail],
) -> (i32, i32, &'static str) {
    if aggregate_emails.is_empty() {
        return (
            mailbox.total_emails.min(i32::MAX as u32) as i32,
            mailbox.unread_emails.min(i32::MAX as u32) as i32,
            "mailbox",
        );
    }

    let mut total = 0u32;
    let mut unread = 0u32;
    for unread_in_folder in aggregate_emails
        .iter()
        .filter_map(|email| email_unread_in_manifest_folder(email, folder_id, mailboxes))
    {
        total = total.saturating_add(1);
        if unread_in_folder {
            unread = unread.saturating_add(1);
        }
    }

    (
        total.min(i32::MAX as u32) as i32,
        unread.min(i32::MAX as u32) as i32,
        "snapshot",
    )
}

fn email_unread_in_manifest_folder(
    email: &JmapEmail,
    folder_id: u64,
    mailboxes: &[JmapMailbox],
) -> Option<bool> {
    if let Some((role, _, _, _, _)) = virtual_special_folder_metadata(folder_id) {
        if role.starts_with("__mapi_") {
            return None;
        }
        return email
            .mailbox_states
            .iter()
            .find(|state| state.role == role)
            .map(|state| state.unread)
            .or_else(|| (email.mailbox_role == role).then_some(email.unread));
    }

    mailboxes
        .iter()
        .find(|mailbox| {
            let Some(mapped_folder_id) = crate::mapi::identity::mapped_mapi_object_id(&mailbox.id)
            else {
                return false;
            };
            mapi_folder_id_for_mailbox(mailbox, mapped_folder_id) == folder_id
        })
        .and_then(|mailbox| {
            email
                .mailbox_states
                .iter()
                .find(|state| state.mailbox_id == mailbox.id)
                .map(|state| state.unread)
                .or_else(|| (email.mailbox_id == mailbox.id).then_some(email.unread))
        })
}

fn hierarchy_sort_depth(
    sync_type: u8,
    sync_root_folder_id: u64,
    mailbox: &JmapMailbox,
    mailboxes: &[JmapMailbox],
) -> u8 {
    if sync_type != SYNC_TYPE_HIERARCHY {
        return 0;
    }
    if mapi_folder_id_for_mailbox(mailbox, 0) == sync_root_folder_id {
        return 0;
    }
    let mut parent_folder_id = mapi_folder_parent_id_for_mailbox(mailbox, mailboxes);
    if parent_folder_id == sync_root_folder_id {
        return 0;
    }

    let mut depth = 1u8;
    let mut visited = BTreeSet::new();
    while parent_folder_id != 0 && visited.insert(parent_folder_id) {
        let Some(next_parent_folder_id) =
            mapi_parent_folder_id_for_folder_id(parent_folder_id, mailboxes)
        else {
            break;
        };
        if next_parent_folder_id == sync_root_folder_id {
            break;
        }
        parent_folder_id = next_parent_folder_id;
        depth = depth.saturating_add(1);
    }
    depth
}

fn mapi_parent_folder_id_for_folder_id(folder_id: u64, mailboxes: &[JmapMailbox]) -> Option<u64> {
    if folder_id == crate::mapi::identity::ROOT_FOLDER_ID {
        return None;
    }
    if let Some((_, _, _, parent_folder_id, _)) = virtual_special_folder_metadata(folder_id) {
        return Some(parent_folder_id);
    }
    mailboxes
        .iter()
        .find(|mailbox| {
            let fallback = crate::mapi::identity::mapped_mapi_object_id(&mailbox.id)
                .unwrap_or(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID);
            mapi_folder_id_for_mailbox(mailbox, fallback) == folder_id
        })
        .map(|mailbox| mapi_folder_parent_id_for_mailbox(mailbox, mailboxes))
}

fn hierarchy_folder_sort_order(mailbox: &JmapMailbox) -> i32 {
    virtual_special_folder_metadata(mapi_folder_id_for_mailbox(mailbox, 0))
        .map(|(_, _, sort_order, _, _)| sort_order)
        .unwrap_or(i32::MAX)
}

fn virtual_special_mailbox_id(folder_id: u64) -> Uuid {
    Uuid::from_u128(VIRTUAL_SPECIAL_MAILBOX_UUID_PREFIX | u128::from(folder_id))
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
            "Outlook.Reminder",
        )),
        crate::mapi::identity::SUGGESTED_CONTACTS_FOLDER_ID => Some((
            "suggested_contacts",
            "Suggested Contacts",
            62,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Contact",
        )),
        crate::mapi::identity::QUICK_CONTACTS_FOLDER_ID => Some((
            "quick_contacts",
            "Quick Contacts",
            63,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Contact.MOC.QuickContacts",
        )),
        crate::mapi::identity::IM_CONTACT_LIST_FOLDER_ID => Some((
            "im_contact_list",
            "IM Contact List",
            64,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Contact.MOC.ImContactList",
        )),
        crate::mapi::identity::CONTACTS_SEARCH_FOLDER_ID => Some((
            "contacts_search",
            "Contacts Search",
            65,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Contact",
        )),
        crate::mapi::identity::DOCUMENT_LIBRARIES_FOLDER_ID => Some((
            "document_libraries",
            "Document Libraries",
            66,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.ShortcutFolder",
        )),
        crate::mapi::identity::SYNC_ISSUES_FOLDER_ID => Some((
            "sync_issues",
            "Sync Issues",
            67,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Note",
        )),
        crate::mapi::identity::CONFLICTS_FOLDER_ID => Some((
            "conflicts",
            "Conflicts",
            68,
            crate::mapi::identity::SYNC_ISSUES_FOLDER_ID,
            "IPF.Note",
        )),
        crate::mapi::identity::LOCAL_FAILURES_FOLDER_ID => Some((
            "local_failures",
            "Local Failures",
            69,
            crate::mapi::identity::SYNC_ISSUES_FOLDER_ID,
            "IPF.Note",
        )),
        crate::mapi::identity::SERVER_FAILURES_FOLDER_ID => Some((
            "server_failures",
            "Server Failures",
            70,
            crate::mapi::identity::SYNC_ISSUES_FOLDER_ID,
            "IPF.Note",
        )),
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID => Some((
            "__mapi_common_views",
            "Common Views",
            80,
            crate::mapi::identity::ROOT_FOLDER_ID,
            "IPF.Root",
        )),
        crate::mapi::identity::SCHEDULE_FOLDER_ID => Some((
            "__mapi_schedule",
            "Schedule",
            90,
            crate::mapi::identity::ROOT_FOLDER_ID,
            "IPF.Root",
        )),
        crate::mapi::identity::SEARCH_FOLDER_ID => Some((
            "__mapi_search",
            "Search",
            100,
            crate::mapi::identity::ROOT_FOLDER_ID,
            "IPF.Root",
        )),
        crate::mapi::identity::VIEWS_FOLDER_ID => Some((
            "__mapi_views",
            "Views",
            110,
            crate::mapi::identity::ROOT_FOLDER_ID,
            "IPF.Root",
        )),
        crate::mapi::identity::SHORTCUTS_FOLDER_ID => Some((
            "__mapi_shortcuts",
            "Shortcuts",
            120,
            crate::mapi::identity::ROOT_FOLDER_ID,
            "IPF.Root",
        )),
        crate::mapi::identity::JUNK_FOLDER_ID => Some((
            "junk",
            "Junk E-mail",
            130,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Note",
        )),
        crate::mapi::identity::RSS_FEEDS_FOLDER_ID => Some((
            "rss_feeds",
            "RSS Feeds",
            140,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Note.OutlookHomepage",
        )),
        crate::mapi::identity::TRACKED_MAIL_PROCESSING_FOLDER_ID => Some((
            "tracked_mail_processing",
            "Tracked Mail Processing",
            150,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Note",
        )),
        crate::mapi::identity::TODO_SEARCH_FOLDER_ID => Some((
            "todo_search",
            "To-Do",
            160,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Task",
        )),
        crate::mapi::identity::CONVERSATION_ACTION_SETTINGS_FOLDER_ID => Some((
            "conversation_action_settings",
            "Conversation Action Settings",
            170,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Configuration",
        )),
        crate::mapi::identity::ARCHIVE_FOLDER_ID => Some((
            "archive",
            "Archive",
            180,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Note",
        )),
        crate::mapi::identity::FREEBUSY_DATA_FOLDER_ID => Some((
            "__mapi_freebusy_data",
            "FreeBusy Data",
            190,
            crate::mapi::identity::ROOT_FOLDER_ID,
            "IPF.Root",
        )),
        crate::mapi::identity::CONVERSATION_HISTORY_FOLDER_ID => Some((
            "conversation_history",
            "Conversation History",
            200,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Note",
        )),
        _ => None,
    }
}

fn property_tag_excluded(excluded_property_tags: &[u32], property_tag: u32) -> bool {
    excluded_property_tags.contains(&property_tag)
}

fn content_property_in_scope(
    sync_type: u8,
    sync_flags: u16,
    sync_property_tags: &[u32],
    property_tag: u32,
) -> bool {
    if sync_type != SYNC_TYPE_CONTENTS || sync_property_tags.is_empty() {
        return true;
    }
    if sync_flags & 0x0080 != 0 {
        sync_property_tags.contains(&property_tag)
    } else {
        !sync_property_tags.contains(&property_tag)
    }
}

fn mapi_folder_type(mailbox: &JmapMailbox) -> i32 {
    if mailbox.role == "__mapi_ipm_subtree" {
        0
    } else if mailbox.role == "__mapi_search" {
        2
    } else {
        1
    }
}

fn local_commit_time_max(
    folder_id: u64,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    attachment_facts: &[MessageAttachmentSyncFacts],
) -> u64 {
    emails
        .iter()
        .filter(|email| email_unread_in_manifest_folder(email, folder_id, mailboxes).is_some())
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
    if sync_type == SYNC_TYPE_HIERARCHY {
        mailboxes
            .iter()
            .filter_map(|mailbox| {
                let object_id = mapi_folder_id_for_mailbox(mailbox, folder_id);
                (object_id != folder_id).then_some(object_id)
            })
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
    if sync_type == SYNC_TYPE_HIERARCHY {
        let mut change_numbers = vec![change_number_for_store_id(folder_id)];
        change_numbers.extend(mailboxes
            .iter()
            .filter_map(|mailbox| {
                let object_id = mapi_folder_id_for_mailbox(mailbox, folder_id);
                (object_id != folder_id).then_some(canonical_hierarchy_change_number(
                    folder_id, mailbox,
                ))
            })
        );
        change_numbers
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
    change_number_for_store_id(folder_id)
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
    if sync_type == SYNC_TYPE_CONTENTS {
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
        flags |= MSGFLAG_READ;
    }
    if email.has_attachments {
        flags |= MSGFLAG_HASATTACH;
    }
    flags
}

pub(crate) fn canonical_flag_status(email: &JmapEmail) -> u32 {
    match email.followup_flag_status.as_str() {
        "complete" => 1,
        "flagged" => FOLLOWUP_FLAGGED,
        _ if email.flagged => FOLLOWUP_FLAGGED,
        _ => 0,
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

fn canonical_modseq_change_number(modseq: u64) -> u64 {
    modseq.clamp(1, 0x0000_FFFF_FFFF_FFFF)
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

fn write_multi_string_property(buffer: &mut Vec<u8>, property_tag: u32, values: &[String]) {
    write_u32(buffer, property_tag);
    write_u32(buffer, values.len().min(u32::MAX as usize) as u32);
    for value in values.iter().take(u32::MAX as usize) {
        let mut bytes = value
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect::<Vec<_>>();
        bytes.extend_from_slice(&0u16.to_le_bytes());
        write_u32(buffer, bytes.len().min(u32::MAX as usize) as u32);
        buffer.extend_from_slice(&bytes);
    }
}

fn write_special_message_property(
    buffer: &mut Vec<u8>,
    property_tag: u32,
    value: &SpecialMessagePropertyValue,
) {
    match value {
        SpecialMessagePropertyValue::Binary(value) => {
            write_binary_property(buffer, property_tag, value)
        }
        SpecialMessagePropertyValue::I32(value) => write_i32_property(buffer, property_tag, *value),
        SpecialMessagePropertyValue::String(value) => {
            write_utf16_property(buffer, property_tag, value)
        }
        SpecialMessagePropertyValue::MultiString(values) => {
            write_multi_string_property(buffer, property_tag, values)
        }
        SpecialMessagePropertyValue::Time(value) => {
            write_u32(buffer, property_tag);
            write_i64(buffer, filetime_from_rfc3339_utc(value) as i64);
        }
    }
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
            is_subscribed: true,
        };

        assert_eq!(
            canonical_hierarchy_change_number(
                crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
                &drafts
            ),
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
            0,
            0x0000_0007,
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
        assert_eq!(summary.final_state_property_lengths, vec![30, 43]);
        assert_eq!(summary.final_state_idset_given_len, 30);
        assert_eq!(summary.final_state_cnset_seen_len, 43);
        assert_eq!(summary.final_state_idset_given_counters, vec![5]);
        assert_eq!(summary.final_state_cnset_seen_counters, vec![4, 42]);
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
            .contains("ranges=4,42"));
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
        assert_eq!(
            summary.rows[0].folder_id,
            Some(crate::mapi::identity::INBOX_FOLDER_ID)
        );
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
        assert!(validation.sync_root_counter_in_final_cnset);
        assert!(validation.root_inclusive_idset_given_delta_bytes >= 0);
        assert_eq!(validation.root_inclusive_cnset_seen_delta_bytes, 0);
        assert!(validation
            .root_inclusive_idset_given_summary
            .contains("ranges=4-5"));
        assert!(validation
            .root_inclusive_cnset_seen_summary
            .contains("ranges=4,42"));
        assert_eq!(validation.top_level_row_count, 1);
        assert_eq!(validation.nested_row_count, 0);
        assert_eq!(validation.rows_without_folder_id, 0);
        assert_eq!(validation.rows_missing_core_property_count, 0);
        assert_eq!(validation.rows_with_content_counts_present, 1);
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
        assert!(!comparison.folder_id_expected_by_eid);
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
            is_subscribed: true,
        };
        let email = test_email();
        let buffer = sync_manifest_buffer_with_final_state(
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
        assert!(!summary
            .emitted_property_tags
            .contains(&PID_TAG_DELETED_COUNT_TOTAL));
        assert!(!summary
            .emitted_property_tags
            .contains(&PID_TAG_CHANGE_NUMBER));
        assert!(summary
            .emitted_property_tags
            .contains(&PID_TAG_CONTAINER_CLASS_W));
        assert!(row.local_commit_time_max.is_some());
        assert_eq!(row.deleted_count_total, None);
        assert_eq!(row.change_number, None);
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
            crate::mapi::identity::CONVERSATION_HISTORY_FOLDER_ID,
        ];
        let expected_folder_count = folder_ids.len();
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

        let summary = decode_hierarchy_transfer_debug_summary(&buffer).unwrap();

        assert_eq!(summary.folder_change_count, expected_folder_count);
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
        assert_eq!(validation.rows_without_folder_id, 0);
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
    fn hierarchy_transfer_omits_folder_id_without_eid_extra_flag() {
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
        assert_eq!(
            summary.rows[0].folder_id,
            Some(crate::mapi::identity::INBOX_FOLDER_ID)
        );
        assert!(summary.emitted_property_tags.contains(&PID_TAG_FOLDER_ID));
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
            named_properties: vec![(0x8B00_0003, SpecialMessagePropertyValue::I32(3))],
        };
        let buffer = sync_manifest_buffer_with_special_objects_and_final_state(
            SYNC_TYPE_CONTENTS,
            0,
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
        assert!(contains_bytes(&buffer, &item_id.to_le_bytes()));
        assert!(contains_bytes(&buffer, &utf16z("IPM.StickyNote")));
        assert!(contains_bytes(&buffer, &utf16z("Remember this")));
        assert!(contains_bytes(&buffer, &0x8B00_0003u32.to_le_bytes()));
    }

    fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
        needle.is_empty()
            || haystack
                .windows(needle.len())
                .any(|window| window == needle)
    }

    #[test]
    fn hierarchy_sync_honors_excluded_count_properties() {
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
            is_subscribed: true,
        };
        let email = test_email();
        let buffer = sync_manifest_buffer_with_final_state(
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
        assert_eq!(row.content_count, Some(1));
        assert_eq!(row.content_unread_count, Some(1));
    }

    #[test]
    fn hierarchy_sync_excluded_count_properties_do_not_force_other_properties() {
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
            is_subscribed: true,
        };
        let email = test_email();
        let buffer = sync_manifest_buffer_with_final_state(
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
        assert_eq!(row.content_count, Some(1));
        assert_eq!(row.content_unread_count, Some(1));
        assert_eq!(row.folder_type, Some(1));
        assert_eq!(row.access, Some(MAPI_FOLDER_ACCESS as i32));
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

        assert_variable_property(&hierarchy, META_TAG_CNSET_SEEN, &expected_hierarchy_cnset);
        assert_variable_property(&content, META_TAG_CNSET_SEEN, &expected_content_cnset);
        assert_variable_property(&content, META_TAG_CNSET_SEEN_FAI, &expected_content_cnset);
        assert_variable_property(&content, META_TAG_CNSET_READ, &expected_content_cnset);
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
}
