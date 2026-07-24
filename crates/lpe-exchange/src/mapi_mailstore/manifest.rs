use super::special_message::{
    special_message_access, special_message_access_level, special_message_change_key,
    special_message_change_number, special_message_flags, special_message_has_attachments,
    special_message_parent_source_key, special_message_predecessor_change_list,
    special_message_property_is_ics_identity, special_message_property_is_server_projected,
    special_message_search_key, special_message_status, special_message_sync_source_key,
    write_special_message_property, PID_TAG_HAS_ATTACHMENTS, PID_TAG_MESSAGE_STATUS,
};
use super::*;

const OWNER_INBOX_SPECIAL_FOLDER_ENTRY_IDS: [(u32, u64); 7] = [
    (0x36D0_0102, crate::mapi::identity::CALENDAR_FOLDER_ID),
    (0x36D1_0102, crate::mapi::identity::CONTACTS_FOLDER_ID),
    (0x36D2_0102, crate::mapi::identity::JOURNAL_FOLDER_ID),
    (0x36D3_0102, crate::mapi::identity::NOTES_FOLDER_ID),
    (0x36D4_0102, crate::mapi::identity::TASKS_FOLDER_ID),
    (0x36D5_0102, crate::mapi::identity::REMINDERS_FOLDER_ID),
    (0x36D7_0102, crate::mapi::identity::DRAFTS_FOLDER_ID),
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AttachmentSyncFact {
    pub(crate) id: Uuid,
    pub(crate) file_reference: String,
    pub(crate) file_name: String,
    pub(crate) media_type: String,
    pub(crate) size_octets: u64,
    pub(crate) embedded_message_blob: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MessageAttachmentSyncFacts {
    pub(crate) message_id: Uuid,
    pub(crate) attachments: Vec<AttachmentSyncFact>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct FaiContentSyncDebugContext<'a> {
    pub(crate) mailbox: &'a str,
    pub(crate) tenant: &'a str,
    pub(crate) account: &'a str,
    pub(crate) mapi_request_id: &'a str,
    pub(crate) request_rop_id: &'a str,
    pub(crate) checkpoint_kind: &'a str,
    pub(crate) active_transfer_selection: &'a str,
}

impl Default for FaiContentSyncDebugContext<'_> {
    fn default() -> Self {
        Self {
            mailbox: "",
            tenant: "",
            account: "",
            mapi_request_id: "",
            request_rop_id: "0x70",
            checkpoint_kind: "",
            active_transfer_selection: "",
        }
    }
}

fn normalized_subject_tag(sync_flags: u16) -> u32 {
    if sync_flags & SYNC_FLAG_UNICODE != 0 {
        PID_TAG_NORMALIZED_SUBJECT_W
    } else {
        PID_TAG_NORMALIZED_SUBJECT_A
    }
}

fn email_delivery_sort_time(email: &JmapEmail, attachments: &[AttachmentSyncFact]) -> u64 {
    parse_rfc3339_utc_filetime(&email.received_at).unwrap_or_else(|| {
        filetime_from_change_number(canonical_message_change_number_with_attachments(
            email,
            attachments,
        ))
    })
}

fn special_message_delivery_sort_time(object: &SpecialMessageSyncFact) -> u64 {
    object
        .named_properties
        .iter()
        .find_map(|(tag, value)| {
            (canonical_property_storage_tag(*tag) == PID_TAG_MESSAGE_DELIVERY_TIME)
                .then(|| match value {
                    SpecialMessagePropertyValue::I64(value) => u64::try_from(*value).ok(),
                    SpecialMessagePropertyValue::U64(value) => Some(*value),
                    SpecialMessagePropertyValue::Time(value) => parse_rfc3339_utc_filetime(value),
                    _ => None,
                })
                .flatten()
        })
        .unwrap_or(object.last_modified_filetime)
}

fn write_normalized_subject_property(buffer: &mut Vec<u8>, property_tag: u32, subject: &str) {
    // [MS-OXCFXICS] sections 3.2.5.8.1.1 and 3.2.5.9.1.1: canonical
    // Unicode strings remain Unicode when the synchronization advertises Unicode.
    if property_tag == PID_TAG_NORMALIZED_SUBJECT_W {
        write_utf16_property(buffer, property_tag, subject);
    } else {
        write_string8_property(buffer, property_tag, subject);
    }
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
    if let Some(source_key) = crate::mapi::identity::mapped_mapi_source_key(id) {
        return source_key;
    }
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
        size_octets: 0,
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
    parse_rfc3339_utc_filetime(value).unwrap_or_default()
}

pub(crate) fn filetime_from_change_number(change_number: u64) -> u64 {
    FILETIME_2026_01_01 + (change_number % 31_536_000) * WINDOWS_FILETIME_TICKS_PER_SECOND
}

fn parse_rfc3339_utc_filetime(value: &str) -> Option<u64> {
    let bytes = value.as_bytes();
    if bytes.len() < 20
        || bytes.get(4) != Some(&b'-')
        || bytes.get(7) != Some(&b'-')
        || bytes.get(10) != Some(&b'T')
        || bytes.get(13) != Some(&b':')
        || bytes.get(16) != Some(&b':')
    {
        return None;
    }
    let fractional_ticks = match bytes.get(19..) {
        Some(b"Z") => 0,
        Some(suffix)
            if suffix.first() == Some(&b'.')
                && suffix.last() == Some(&b'Z')
                && (3..=11).contains(&suffix.len()) =>
        {
            let fraction = suffix.get(1..suffix.len() - 1)?;
            parse_digits(fraction)?;
            let retained_precision = fraction.len().min(7);
            let retained_digits = parse_digits(fraction.get(..retained_precision)?)? as u64;
            retained_digits.checked_mul(10u64.pow(7u32.checked_sub(retained_precision as u32)?))?
        }
        _ => return None,
    };
    let year = parse_digits(bytes.get(0..4)?)? as i32;
    let month = parse_digits(bytes.get(5..7)?)? as i32;
    let day = parse_digits(bytes.get(8..10)?)? as i32;
    let hour = parse_digits(bytes.get(11..13)?)? as i64;
    let minute = parse_digits(bytes.get(14..16)?)? as i64;
    let second = parse_digits(bytes.get(17..19)?)? as i64;
    if !(1..=12).contains(&month)
        || !(1..=days_in_month(year, month)?).contains(&day)
        || hour > 23
        || minute > 59
        || second > 60
    {
        return None;
    }
    let unix_seconds = days_from_civil(i64::from(year), i64::from(month), i64::from(day)) * 86_400
        + hour * 3_600
        + minute * 60
        + second;
    windows_filetime_from_signed_unix_seconds(unix_seconds).checked_add(fractional_ticks)
}

fn days_in_month(year: i32, month: i32) -> Option<i32> {
    Some(match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        2 => 28,
        _ => return None,
    })
}

fn parse_digits(bytes: &[u8]) -> Option<u32> {
    bytes.iter().try_fold(0u32, |value, byte| {
        byte.is_ascii_digit()
            .then_some(value * 10 + u32::from(byte - b'0'))
    })
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
    sync_flags: u16,
    folder_id: u64,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    attachment_facts: &[MessageAttachmentSyncFacts],
    special_objects: &[SpecialMessageSyncFact],
) -> Vec<u8> {
    if sync_type != SYNC_TYPE_CONTENTS {
        return final_sync_state_stream(
            sync_type,
            &sync_state_object_ids(sync_type, folder_id, mailboxes, emails),
            &sync_state_change_numbers(sync_type, folder_id, mailboxes, emails, attachment_facts),
        );
    }
    let scoped_emails = if content_sync_includes_normal(sync_type, sync_flags) {
        emails
    } else {
        &[]
    };
    let mut source_key_identities = scoped_emails
        .iter()
        .filter_map(|email| {
            let object_id = crate::mapi::identity::mapped_mapi_object_id(&email.id)?;
            Some((source_key_for_uuid(&email.id), object_id))
        })
        .collect::<Vec<_>>();
    let normal_change_numbers = sync_state_change_numbers(
        sync_type,
        folder_id,
        mailboxes,
        scoped_emails,
        attachment_facts,
    );
    let default_include_associated =
        default_content_sync_includes_associated(scoped_emails, special_objects);
    let scoped_special_objects = special_objects
        .iter()
        .filter(|object| {
            content_sync_includes_associated(
                sync_type,
                sync_flags,
                object.associated,
                default_include_associated,
            )
        })
        .collect::<Vec<_>>();
    source_key_identities.extend(scoped_special_objects.iter().map(|object| {
        (
            special_message_sync_source_key(object, sync_flags),
            object.item_id,
        )
    }));
    let mut normal_change_numbers = normal_change_numbers;
    // [MS-OXCFXICS] sections 2.2.1.1.2, 2.2.1.1.3, and 3.2.5.3 require
    // the final CnsetSeen/CnsetSeenFAI to contain the CN of each downloaded
    // normal/FAI change. Reuse the same durable CN as the object header.
    normal_change_numbers.extend(
        scoped_special_objects
            .iter()
            .filter(|object| !object.associated)
            .map(|object| special_message_change_number(object)),
    );
    let fai_change_numbers = scoped_special_objects
        .iter()
        .filter(|object| object.associated)
        .map(|object| special_message_change_number(object))
        .collect::<Vec<_>>();
    // [MS-OXCFXICS] sections 2.2.1.1.1, 2.2.1.2.5, 2.2.2.4.2,
    // and 3.2.5.3: the complete state uses the GID from each SourceKey that
    // this synchronization emits, including persisted foreign replicas.
    sync_state_stream_from_raw_properties(
        SYNC_TYPE_CONTENTS,
        &super::client_state::replguid_idset_from_source_keys(
            source_key_identities
                .iter()
                .map(|(source_key, object_id)| (source_key.as_slice(), *object_id)),
        ),
        &replguid_idset_from_counters(&normal_change_numbers),
        &replguid_idset_from_counters(&fai_change_numbers),
        &replguid_idset_from_counters(&normal_change_numbers),
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
        Uuid::nil(),
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
    mailbox_guid: Uuid,
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
        mailbox_guid,
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
    mailbox_guid: Uuid,
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
    sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions(
        mailbox_guid,
        sync_type,
        sync_flags,
        sync_extra_flags,
        sync_property_tags,
        folder_id,
        mailboxes,
        emails,
        attachment_facts,
        special_objects,
        deleted_message_ids,
        parent_context_mailboxes,
        state_mailboxes,
        state_emails,
        state_attachment_facts,
        state_special_objects,
        aggregate_emails,
        aggregate_attachment_facts,
        &[],
        _final_change_sequence,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions(
    mailbox_guid: Uuid,
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
    folder_versions: &[crate::mapi_store::MapiFolderVersion],
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
            // [MS-OXCFXICS] 2.2.4.3.9: hierarchySync contains descendant
            // folderChange elements, never the synchronization root itself.
            if folder_id == sync_root_folder_id {
                continue;
            }
            let parent_folder_id =
                mapi_folder_parent_id_for_mailbox(mailbox, parent_context_mailboxes);
            let folder_version = folder_versions
                .iter()
                .find(|version| version.folder_id == folder_id);
            let change_number = folder_version
                .map(|version| version.change_number)
                .unwrap_or_else(|| canonical_hierarchy_change_number(sync_root_folder_id, mailbox));
            let change_key = folder_version
                .map(|version| version.change_key.clone())
                .unwrap_or_else(|| change_key_for_change_number(change_number));
            let predecessor_change_list = folder_version
                .map(|version| version.predecessor_change_list.clone())
                .unwrap_or_else(|| predecessor_change_list(change_number));
            let last_modification_time = folder_version
                .map(|version| version.last_modification_time)
                .unwrap_or_else(|| filetime_from_change_number(change_number));
            let source_key = source_key_for_store_id(folder_id);
            let parent_source_key = if parent_folder_id == crate::mapi::identity::ROOT_FOLDER_ID
                || parent_folder_id == sync_root_folder_id
            {
                Vec::new()
            } else {
                source_key_for_store_id(parent_folder_id)
            };
            let parent_source_key_role = hierarchy_parent_source_key_role(
                parent_folder_id,
                sync_root_folder_id,
                parent_source_key.is_empty(),
            );
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
            let deleted_count_total_present =
                !property_tag_excluded(excluded_property_tags, PID_TAG_DELETED_COUNT_TOTAL);
            let content_count_present = sync_type != SYNC_TYPE_HIERARCHY
                && !property_tag_excluded(excluded_property_tags, PID_TAG_CONTENT_COUNT);
            let content_unread_count_present = sync_type != SYNC_TYPE_HIERARCHY
                && !property_tag_excluded(excluded_property_tags, PID_TAG_CONTENT_UNREAD_COUNT);
            let owner_inbox_special_folder_entry_id_count =
                if folder_id == crate::mapi::identity::INBOX_FOLDER_ID {
                    OWNER_INBOX_SPECIAL_FOLDER_ENTRY_IDS
                        .iter()
                        .filter(|(tag, _)| !property_tag_excluded(excluded_property_tags, *tag))
                        .count()
                } else {
                    0
                };
            let display_name = mapi_folder_display_name(mailbox);
            tracing::info!(
                rca_debug = true,
                adapter = "mapi",
                endpoint = "emsmdb",
                request_rop_id = "0x70",
                sync_type = format_args!("0x{sync_type:02x}"),
                folder_id = format_args!("0x{folder_id:016x}"),
                parent_folder_id = format_args!("0x{parent_folder_id:016x}"),
                sync_root_folder_id = format_args!("0x{sync_root_folder_id:016x}"),
                source_key_len = source_key.len(),
                parent_source_key_len = parent_source_key.len(),
                parent_source_key_role,
                microsoft_ics_parent_source_key_rule_ok = parent_source_key_role
                    != "unexpected_zero_parent_source_key"
                    && parent_source_key_role != "unexpected_nonzero_sync_root_child",
                display_name,
                container_class,
                change_number,
                mailbox_content_count = mailbox.total_emails,
                mailbox_unread_count = mailbox.unread_emails,
                computed_content_count = content_count,
                computed_unread_count = content_unread_count,
                content_count_source,
                content_count_present,
                content_unread_count_present,
                local_commit_time_max,
                local_commit_time_max_present,
                deleted_count_total_present,
                owner_inbox_special_folder_entry_id_count,
                owner_inbox_calendar_entry_id_present = owner_inbox_special_folder_entry_id_count
                    > 0
                    && !property_tag_excluded(excluded_property_tags, 0x36D0_0102),
                folder_type_excluded =
                    property_tag_excluded(excluded_property_tags, PID_TAG_FOLDER_TYPE),
                access_excluded = property_tag_excluded(excluded_property_tags, PID_TAG_ACCESS),
                aggregate_email_count = aggregate_emails.len(),
                "rca debug mapi hierarchy row"
            );
            write_u32(&mut buffer, INCR_SYNC_CHG);
            write_binary_property(&mut buffer, PID_TAG_PARENT_SOURCE_KEY, &parent_source_key);
            write_binary_property(&mut buffer, PID_TAG_SOURCE_KEY, &source_key);
            write_u32(&mut buffer, PID_TAG_LAST_MODIFICATION_TIME);
            write_i64(&mut buffer, last_modification_time as i64);
            write_binary_property(&mut buffer, PID_TAG_CHANGE_KEY, &change_key);
            write_binary_property(
                &mut buffer,
                PID_TAG_PREDECESSOR_CHANGE_LIST,
                &predecessor_change_list,
            );
            if !property_tag_excluded(excluded_property_tags, PID_TAG_ENTRY_ID) {
                if let Some(entry_id) = crate::mapi::identity::folder_entry_id_from_object_id(
                    hierarchy_entry_id_mailbox_guid(mailbox, mailbox_guid),
                    folder_id,
                ) {
                    write_binary_property(&mut buffer, PID_TAG_ENTRY_ID, &entry_id);
                }
            }
            write_utf16_property(&mut buffer, PID_TAG_DISPLAY_NAME_W, display_name);
            // [MS-OXCFXICS] section 2.2.4.3.5: PidTagFolderId is present
            // in folderChange if and only if SynchronizationExtraFlags.Eid.
            if sync_extra_flags & SYNC_EXTRA_FLAG_EID != 0 {
                write_u32(&mut buffer, PID_TAG_FOLDER_ID);
                write_object_id(&mut buffer, folder_id);
            }
            if sync_type != SYNC_TYPE_HIERARCHY
                || sync_flags & 0x0100 != 0
                || sync_extra_flags & SYNC_EXTRA_FLAG_EID != 0
            {
                write_u32(&mut buffer, PID_TAG_PARENT_FOLDER_ID);
                write_object_id(&mut buffer, parent_folder_id);
            }
            if !property_tag_excluded(excluded_property_tags, PID_TAG_CONTAINER_CLASS_W) {
                write_utf16_property(&mut buffer, PID_TAG_CONTAINER_CLASS_W, container_class);
            }
            if folder_id == crate::mapi::identity::INBOX_FOLDER_ID {
                // [MS-OXOSFLD] section 2.2.3 stores these identification
                // properties on the owner's Inbox. [MS-OXCFXICS] section
                // 2.2.4.3.5 carries unfiltered folder properties in folderChange.
                for (property_tag, special_folder_id) in OWNER_INBOX_SPECIAL_FOLDER_ENTRY_IDS {
                    if !property_tag_excluded(excluded_property_tags, property_tag) {
                        let entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
                            mailbox_guid,
                            special_folder_id,
                        )
                        .expect("special folders use valid MAPI folder IDs");
                        write_binary_property(&mut buffer, property_tag, &entry_id);
                    }
                }
            }
            if container_class == "IPF.Appointment"
                && !property_tag_excluded(
                    excluded_property_tags,
                    PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W,
                )
            {
                write_utf16_property(
                    &mut buffer,
                    PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W,
                    "IPM.Appointment",
                );
            }
            if content_count_present {
                write_i32_property(&mut buffer, PID_TAG_CONTENT_COUNT, content_count);
            }
            if content_unread_count_present {
                write_i32_property(
                    &mut buffer,
                    PID_TAG_CONTENT_UNREAD_COUNT,
                    content_unread_count,
                );
            }
            if !property_tag_excluded(excluded_property_tags, PID_TAG_FOLDER_TYPE) {
                write_i32_property(&mut buffer, PID_TAG_FOLDER_TYPE, mapi_folder_type(mailbox));
            }
            if !property_tag_excluded(excluded_property_tags, PID_TAG_MESSAGE_SIZE) {
                write_i32_property(&mut buffer, PID_TAG_MESSAGE_SIZE, 0);
            }
            if !property_tag_excluded(excluded_property_tags, PID_TAG_ACCESS) {
                write_i32_property(&mut buffer, PID_TAG_ACCESS, MAPI_FOLDER_ACCESS as i32);
            }
            if local_commit_time_max_present {
                write_u32(&mut buffer, PID_TAG_LOCAL_COMMIT_TIME_MAX);
                write_i64(&mut buffer, local_commit_time_max as i64);
            }
            if deleted_count_total_present {
                write_i32_property(&mut buffer, PID_TAG_DELETED_COUNT_TOTAL, 0);
            }
            write_bool_property(
                &mut buffer,
                PID_TAG_SUBFOLDERS,
                mapi_folder_has_subfolders(mailbox, parent_context_mailboxes),
            );
        }
    }

    let mut messages = emails
        .iter()
        .filter(|_| content_sync_includes_normal(sync_type, sync_flags))
        .collect::<Vec<_>>();
    messages.sort_by(|left, right| {
        left.received_at
            .cmp(&right.received_at)
            .then(left.subject.cmp(&right.subject))
            .then(left.id.cmp(&right.id))
    });
    let default_include_associated =
        default_content_sync_includes_associated(emails, special_objects);
    let mut special_objects = special_objects
        .iter()
        .filter(|object| {
            content_sync_includes_associated(
                sync_type,
                sync_flags,
                object.associated,
                default_include_associated,
            )
        })
        .collect::<Vec<_>>();
    special_objects.sort_by(|left, right| {
        left.folder_id
            .cmp(&right.folder_id)
            .then(left.subject.cmp(&right.subject))
            .then(left.canonical_id.cmp(&right.canonical_id))
    });
    if sync_type == SYNC_TYPE_CONTENTS && sync_flags & SYNC_FLAG_PROGRESS != 0 {
        write_content_sync_progress_mode(&mut buffer, &messages, &special_objects);
    }
    let mut message_changes = Vec::with_capacity(messages.len() + special_objects.len());
    for email in messages {
        let attachments = attachments_for_message(email.id, attachment_facts);
        let delivery_sort_time = email_delivery_sort_time(email, attachments);
        let mut buffer = Vec::new();
        let change_number = canonical_message_change_number_with_attachments(email, attachments);
        let message_size = email.size_octets.min(i32::MAX as i64) as i32;
        let source_key = source_key_for_uuid(&email.id);
        if sync_type == SYNC_TYPE_CONTENTS && sync_flags & SYNC_FLAG_PROGRESS != 0 {
            write_content_sync_progress_per_message(&mut buffer, message_size, false);
        }
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
            write_object_id(
                &mut buffer,
                crate::mapi::identity::mapped_mapi_object_id(&email.id).unwrap_or(0),
            );
        }
        if sync_extra_flags & SYNC_EXTRA_FLAG_MESSAGE_SIZE != 0 {
            write_i32_property(&mut buffer, PID_TAG_MESSAGE_SIZE, message_size);
        }
        if sync_extra_flags & SYNC_EXTRA_FLAG_CHANGE_NUMBER != 0 {
            write_u32(&mut buffer, PID_TAG_CHANGE_NUMBER);
            write_change_number(&mut buffer, change_number);
        }
        write_u32(&mut buffer, INCR_SYNC_MESSAGE);
        write_binary_property(
            &mut buffer,
            PID_TAG_PARENT_SOURCE_KEY,
            &source_key_for_store_id(folder_id),
        );
        if content_property_in_scope(sync_type, sync_flags, sync_property_tags, PID_TAG_ENTRY_ID) {
            if let Some(entry_id) = crate::mapi::identity::message_entry_id_from_object_ids(
                mailbox_guid,
                folder_id,
                crate::mapi::identity::mapped_mapi_object_id(&email.id).unwrap_or(0),
            ) {
                write_binary_property(&mut buffer, PID_TAG_ENTRY_ID, &entry_id);
            }
        }
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
        let normalized_subject_tag = normalized_subject_tag(sync_flags);
        if content_property_in_scope(
            sync_type,
            sync_flags,
            sync_property_tags,
            normalized_subject_tag,
        ) {
            write_normalized_subject_property(&mut buffer, normalized_subject_tag, &email.subject);
        }
        if content_property_in_scope(sync_type, sync_flags, sync_property_tags, PID_TAG_BODY_W) {
            write_utf16_property(&mut buffer, PID_TAG_BODY_W, &email.body_text);
        }
        write_fast_transfer_message_children(
            &mut buffer,
            content_sync_message_children(sync_type, sync_flags, sync_property_tags),
            Some(email),
            attachments,
        );
        let original_order = message_changes.len();
        message_changes.push((delivery_sort_time, original_order, buffer));
    }

    for object in &special_objects {
        let attachments = attachments_for_message(object.canonical_id, attachment_facts);
        let delivery_sort_time = special_message_delivery_sort_time(object);
        let mut buffer = Vec::new();
        let change_number = special_message_change_number(object);
        // [MS-OXCFXICS] section 3.2.5.9.1.1: NoForeignIdentifiers requires
        // local replica identifiers even when the canonical object retains an
        // imported SourceKey for a synchronization without that flag.
        let no_foreign_identifiers = sync_flags & SYNC_FLAG_NO_FOREIGN_IDENTIFIERS != 0;
        let source_key = special_message_sync_source_key(object, sync_flags);
        let parent_source_key = if no_foreign_identifiers {
            source_key_for_store_id(object.folder_id)
        } else {
            special_message_parent_source_key(object)
        };
        let change_key = special_message_change_key(object);
        let predecessor_change_list = special_message_predecessor_change_list(object);
        if sync_type == SYNC_TYPE_CONTENTS && sync_flags & SYNC_FLAG_PROGRESS != 0 {
            write_content_sync_progress_per_message(
                &mut buffer,
                object.message_size.clamp(0, i64::from(i32::MAX)) as i32,
                object.associated,
            );
        }
        write_u32(&mut buffer, INCR_SYNC_CHG);
        write_binary_property(&mut buffer, PID_TAG_SOURCE_KEY, &source_key);
        write_u32(&mut buffer, PID_TAG_LAST_MODIFICATION_TIME);
        write_i64(&mut buffer, object.last_modified_filetime as i64);
        write_binary_property(&mut buffer, PID_TAG_CHANGE_KEY, &change_key);
        write_binary_property(
            &mut buffer,
            PID_TAG_PREDECESSOR_CHANGE_LIST,
            &predecessor_change_list,
        );
        write_bool_property(&mut buffer, PID_TAG_ASSOCIATED, object.associated);
        if sync_extra_flags & SYNC_EXTRA_FLAG_EID != 0 {
            write_u32(&mut buffer, PID_TAG_MID);
            write_object_id(&mut buffer, object.item_id);
        }
        if sync_extra_flags & SYNC_EXTRA_FLAG_MESSAGE_SIZE != 0 {
            write_i32_property(
                &mut buffer,
                PID_TAG_MESSAGE_SIZE,
                object.message_size as i32,
            );
        }
        if sync_extra_flags & SYNC_EXTRA_FLAG_CHANGE_NUMBER != 0 {
            write_u32(&mut buffer, PID_TAG_CHANGE_NUMBER);
            write_change_number(&mut buffer, change_number);
        }
        write_u32(&mut buffer, INCR_SYNC_MESSAGE);
        write_binary_property(&mut buffer, PID_TAG_PARENT_SOURCE_KEY, &parent_source_key);
        if content_property_in_scope(sync_type, sync_flags, sync_property_tags, PID_TAG_ENTRY_ID) {
            if let Some(entry_id) = crate::mapi::identity::message_entry_id_from_object_ids(
                mailbox_guid,
                object.folder_id,
                object.item_id,
            ) {
                write_binary_property(&mut buffer, PID_TAG_ENTRY_ID, &entry_id);
            }
        }
        if object.associated
            && content_property_in_scope(
                sync_type,
                sync_flags,
                sync_property_tags,
                PID_TAG_RECORD_KEY,
            )
        {
            write_binary_property(&mut buffer, PID_TAG_RECORD_KEY, &source_key);
        }
        if object.associated
            && content_property_in_scope(
                sync_type,
                sync_flags,
                sync_property_tags,
                PID_TAG_SEARCH_KEY,
            )
        {
            write_binary_property(
                &mut buffer,
                PID_TAG_SEARCH_KEY,
                &special_message_search_key(object),
            );
        }
        // [MS-OXCMSG] section 2.2.1.1 requires these server-owned
        // properties on every Message object. [MS-OXCFXICS] section 4.5
        // includes PidTagAccessLevel in a content synchronization download.
        if content_property_in_scope(sync_type, sync_flags, sync_property_tags, PID_TAG_ACCESS) {
            write_i32_property(
                &mut buffer,
                PID_TAG_ACCESS,
                special_message_access(object) as i32,
            );
        }
        if content_property_in_scope(
            sync_type,
            sync_flags,
            sync_property_tags,
            PID_TAG_ACCESS_LEVEL,
        ) {
            write_i32_property(
                &mut buffer,
                PID_TAG_ACCESS_LEVEL,
                special_message_access_level(object) as i32,
            );
        }
        if content_property_in_scope(
            sync_type,
            sync_flags,
            sync_property_tags,
            PID_TAG_HAS_ATTACHMENTS,
        ) {
            write_bool_property(
                &mut buffer,
                PID_TAG_HAS_ATTACHMENTS,
                special_message_has_attachments(object),
            );
        }
        if content_property_in_scope(
            sync_type,
            sync_flags,
            sync_property_tags,
            PID_TAG_MESSAGE_STATUS,
        ) {
            write_i32_property(
                &mut buffer,
                PID_TAG_MESSAGE_STATUS,
                special_message_status(object) as i32,
            );
        }
        if content_property_in_scope(
            sync_type,
            sync_flags,
            sync_property_tags,
            PID_TAG_MESSAGE_FLAGS,
        ) {
            // [MS-OXCMSG] section 2.2.1.6: mfFAI identifies an FAI message.
            let message_flags = special_message_flags(object);
            write_i32_property(&mut buffer, PID_TAG_MESSAGE_FLAGS, message_flags as i32);
        }
        let subject_in_scope =
            content_property_in_scope(sync_type, sync_flags, sync_property_tags, PID_TAG_SUBJECT_W);
        if subject_in_scope {
            write_utf16_property(&mut buffer, PID_TAG_SUBJECT_W, &object.subject);
        }
        let normalized_subject_tag = normalized_subject_tag(sync_flags);
        if content_property_in_scope(
            sync_type,
            sync_flags,
            sync_property_tags,
            normalized_subject_tag,
        ) {
            write_normalized_subject_property(&mut buffer, normalized_subject_tag, &object.subject);
        }
        if content_property_in_scope(
            sync_type,
            sync_flags,
            sync_property_tags,
            PID_TAG_MESSAGE_CLASS_W,
        ) {
            write_utf16_property(&mut buffer, PID_TAG_MESSAGE_CLASS_W, &object.message_class);
        }
        if content_property_in_scope(sync_type, sync_flags, sync_property_tags, PID_TAG_BODY_W) {
            if let Some(body_text) = &object.body_text {
                write_utf16_property(&mut buffer, PID_TAG_BODY_W, body_text);
            }
        }
        if content_property_in_scope(
            sync_type,
            sync_flags,
            sync_property_tags,
            PID_TAG_MESSAGE_SIZE,
        ) {
            write_i32_property(
                &mut buffer,
                PID_TAG_MESSAGE_SIZE,
                object.message_size as i32,
            );
        }
        for (tag, value) in &object.named_properties {
            if !special_message_property_is_ics_identity(*tag)
                && !special_message_property_is_server_projected(*tag)
                && *tag != PID_TAG_MESSAGE_FLAGS
                && content_property_in_scope(sync_type, sync_flags, sync_property_tags, *tag)
            {
                write_special_message_property(&mut buffer, object, *tag, value);
            }
        }
        write_fast_transfer_message_children(
            &mut buffer,
            content_sync_message_children(sync_type, sync_flags, sync_property_tags),
            None,
            attachments,
        );
        let original_order = message_changes.len();
        message_changes.push((delivery_sort_time, original_order, buffer));
    }

    if sync_extra_flags & SYNC_EXTRA_FLAG_ORDER_BY_DELIVERY_TIME != 0 {
        // [MS-OXCFXICS] section 3.2.5.9.1.1: sort the complete sequence of
        // normal and FAI messageChange elements from newest to oldest by
        // PidTagMessageDeliveryTime, falling back to LastModificationTime.
        message_changes
            .sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
    }
    for (_, _, message_change) in message_changes {
        buffer.extend_from_slice(&message_change);
    }

    if !deleted_message_ids.is_empty() {
        write_u32(&mut buffer, INCR_SYNC_DEL);
        write_binary_property(
            &mut buffer,
            META_TAG_IDSET_DELETED,
            &replid_idset_from_object_ids(deleted_message_ids),
        );
    }

    buffer.extend_from_slice(&sync_state_token_with_special_objects(
        sync_type,
        sync_flags,
        folder_id,
        state_mailboxes,
        state_emails,
        state_attachment_facts,
        state_special_objects,
    ));
    write_u32(&mut buffer, INCR_SYNC_END);
    buffer
}
