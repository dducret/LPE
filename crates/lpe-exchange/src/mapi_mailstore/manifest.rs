use super::*;

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
    pub(crate) read_state: Option<bool>,
    pub(crate) named_properties: Vec<(u32, SpecialMessagePropertyValue)>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SpecialMessagePropertyValue {
    Binary(Vec<u8>),
    Bool(bool),
    Guid([u8; 16]),
    I32(i32),
    I64(i64),
    U32(u32),
    U64(u64),
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
    parse_rfc3339_utc_seconds(value)
        .map(windows_filetime_from_signed_unix_seconds)
        .unwrap_or_default()
}

pub(crate) fn filetime_from_change_number(change_number: u64) -> u64 {
    FILETIME_2026_01_01 + (change_number % 31_536_000) * WINDOWS_FILETIME_TICKS_PER_SECOND
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
    Some(
        days_from_civil(i64::from(year), i64::from(month), i64::from(day)) * 86_400
            + hour * 3_600
            + minute * 60
            + second,
    )
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
    let normal_object_ids = sync_state_object_ids(sync_type, folder_id, mailboxes, scoped_emails);
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
    let mut object_ids = normal_object_ids;
    object_ids.extend(scoped_special_objects.iter().map(|object| object.item_id));
    let mut normal_change_numbers = normal_change_numbers;
    normal_change_numbers.extend(
        scoped_special_objects
            .iter()
            .filter(|object| !object.associated)
            .map(|object| change_number_for_store_id(object.item_id)),
    );
    let fai_change_numbers = scoped_special_objects
        .iter()
        .filter(|object| object.associated)
        .map(|object| change_number_for_store_id(object.item_id))
        .collect::<Vec<_>>();
    final_content_sync_state_stream(
        &object_ids,
        &normal_change_numbers,
        &fai_change_numbers,
        &normal_change_numbers,
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
            let change_number = canonical_hierarchy_change_number(sync_root_folder_id, mailbox);
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
            if !property_tag_excluded(excluded_property_tags, PID_TAG_ENTRY_ID) {
                if let Some(entry_id) = crate::mapi::identity::folder_entry_id_from_object_id(
                    hierarchy_entry_id_mailbox_guid(mailbox, mailbox_guid),
                    folder_id,
                ) {
                    write_binary_property(&mut buffer, PID_TAG_ENTRY_ID, &entry_id);
                }
            }
            write_utf16_property(&mut buffer, PID_TAG_DISPLAY_NAME_W, display_name);
            write_u32(&mut buffer, PID_TAG_FOLDER_ID);
            write_object_id(&mut buffer, folder_id);
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
    for email in messages {
        let attachments = attachments_for_message(email.id, attachment_facts);
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
        if subject_in_scope && (sync_type != SYNC_TYPE_CONTENTS || sync_property_tags.is_empty()) {
            write_fast_transfer_visible_recipients(&mut buffer, email);
            write_fast_transfer_attachments(&mut buffer, attachments);
        }
    }

    for object in &special_objects {
        let change_number = change_number_for_store_id(object.item_id);
        if sync_type == SYNC_TYPE_CONTENTS && sync_flags & SYNC_FLAG_PROGRESS != 0 {
            write_content_sync_progress_per_message(
                &mut buffer,
                object.message_size.clamp(0, i64::from(i32::MAX)) as i32,
                object.associated,
            );
        }
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
        write_binary_property(
            &mut buffer,
            PID_TAG_PARENT_SOURCE_KEY,
            &source_key_for_store_id(object.folder_id),
        );
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
            write_binary_property(
                &mut buffer,
                PID_TAG_RECORD_KEY,
                &source_key_for_store_id(object.item_id),
            );
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
                &source_key_for_store_id(object.item_id),
            );
        }
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
        if content_property_in_scope(
            sync_type,
            sync_flags,
            sync_property_tags,
            PID_TAG_MESSAGE_CLASS_W,
        ) {
            write_utf16_property(&mut buffer, PID_TAG_MESSAGE_CLASS_W, &object.message_class);
        }
        if content_property_in_scope(sync_type, sync_flags, sync_property_tags, PID_TAG_BODY_W) {
            write_utf16_property(&mut buffer, PID_TAG_BODY_W, &object.body_text);
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
            if content_property_in_scope(sync_type, sync_flags, sync_property_tags, *tag) {
                write_special_message_property(&mut buffer, *tag, value);
            }
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
        let mut read_message_ids = emails
            .iter()
            .filter(|email| !email.unread)
            .filter_map(|email| crate::mapi::identity::mapped_mapi_object_id(&email.id))
            .collect::<Vec<_>>();
        read_message_ids.extend(
            special_objects
                .iter()
                .filter(|object| object.read_state == Some(true))
                .map(|object| object.item_id),
        );
        let mut unread_message_ids = emails
            .iter()
            .filter(|email| email.unread)
            .filter_map(|email| crate::mapi::identity::mapped_mapi_object_id(&email.id))
            .collect::<Vec<_>>();
        unread_message_ids.extend(
            special_objects
                .iter()
                .filter(|object| object.read_state == Some(false))
                .map(|object| object.item_id),
        );
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
