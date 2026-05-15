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
const PID_TAG_SUBJECT_W: u32 = 0x0037_001F;
const PID_TAG_NORMALIZED_SUBJECT_A: u32 = 0x0E1D_001E;
const PID_TAG_MESSAGE_CLASS_W: u32 = 0x001A_001F;
const PID_TAG_MESSAGE_FLAGS: u32 = 0x0E07_0003;
const PID_TAG_FLAG_STATUS: u32 = 0x1090_0003;
const PID_TAG_SOURCE_KEY: u32 = 0x65E0_0102;
const PID_TAG_PARENT_SOURCE_KEY: u32 = 0x65E1_0102;
const PID_TAG_CHANGE_KEY: u32 = 0x65E2_0102;
const PID_TAG_PREDECESSOR_CHANGE_LIST: u32 = 0x65E3_0102;
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

pub(crate) fn change_key_for_change_number(change_number: u64) -> Vec<u8> {
    crate::mapi::identity::change_key_for_change_number(change_number)
}

pub(crate) fn predecessor_change_list(change_number: u64) -> Vec<u8> {
    let change_number = change_number.max(1);
    let mut list = Vec::with_capacity(16);
    list.extend_from_slice(&(change_number.saturating_sub(1)).to_le_bytes());
    list.extend_from_slice(&change_number.to_le_bytes());
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
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    attachment_facts: &[MessageAttachmentSyncFacts],
) -> Vec<u8> {
    let max_change = mailboxes
        .iter()
        .map(canonical_folder_change_number)
        .chain(emails.iter().map(|email| {
            canonical_message_change_number_with_attachments(
                email,
                attachments_for_message(email.id, attachment_facts),
            )
        }))
        .max()
        .unwrap_or(1);
    final_sync_state_stream(sync_type, max_change)
}

pub(crate) fn sync_manifest_buffer_with_attachments(
    sync_type: u8,
    folder_id: u64,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    attachment_facts: &[MessageAttachmentSyncFacts],
    deleted_message_ids: &[u64],
    final_change_sequence: u64,
) -> Vec<u8> {
    let mut buffer = Vec::new();
    let mut folders = mailboxes.iter().collect::<Vec<_>>();
    folders.sort_by(|left, right| left.name.cmp(&right.name).then(left.id.cmp(&right.id)));
    for mailbox in folders {
        let change_number = canonical_folder_change_number(mailbox);
        let source_key = source_key_for_uuid(&mailbox.id);
        let folder_id = mapi_folder_id_for_mailbox(mailbox, folder_id);
        write_u32(&mut buffer, INCR_SYNC_CHG);
        write_u32(&mut buffer, PID_TAG_FOLDER_ID);
        write_i64(&mut buffer, folder_id as i64);
        write_u32(&mut buffer, PID_TAG_PARENT_FOLDER_ID);
        write_i64(
            &mut buffer,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID as i64,
        );
        write_binary_property(&mut buffer, PID_TAG_SOURCE_KEY, &source_key);
        write_binary_property(&mut buffer, PID_TAG_PARENT_SOURCE_KEY, &[]);
        write_u32(&mut buffer, PID_TAG_CHANGE_NUMBER);
        write_i64(&mut buffer, change_number as i64);
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
        write_i32_property(
            &mut buffer,
            PID_TAG_CONTENT_COUNT,
            mailbox.total_emails.min(i32::MAX as u32) as i32,
        );
        write_i32_property(
            &mut buffer,
            PID_TAG_CONTENT_UNREAD_COUNT,
            mailbox.unread_emails.min(i32::MAX as u32) as i32,
        );
        write_bool_property(&mut buffer, PID_TAG_SUBFOLDERS, false);
        write_utf16_property(&mut buffer, PID_TAG_DISPLAY_NAME_W, &mailbox.name);
        write_utf16_property(
            &mut buffer,
            PID_TAG_MESSAGE_CLASS_W,
            mapi_folder_message_class(mailbox),
        );
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

    buffer.extend_from_slice(&final_sync_state_stream(
        sync_type,
        final_change_sequence.max(1),
    ));
    write_u32(&mut buffer, INCR_SYNC_END);
    buffer
}

fn mapi_folder_id_for_mailbox(mailbox: &JmapMailbox, fallback: u64) -> u64 {
    match mailbox.role.as_str() {
        "inbox" => crate::mapi::identity::INBOX_FOLDER_ID,
        "drafts" => crate::mapi::identity::DRAFTS_FOLDER_ID,
        "outbox" => crate::mapi::identity::OUTBOX_FOLDER_ID,
        "sent" => crate::mapi::identity::SENT_FOLDER_ID,
        "trash" => crate::mapi::identity::TRASH_FOLDER_ID,
        "contacts" => crate::mapi::identity::CONTACTS_FOLDER_ID,
        "calendar" => crate::mapi::identity::CALENDAR_FOLDER_ID,
        _ => crate::mapi::identity::mapped_mapi_object_id(&mailbox.id).unwrap_or(fallback),
    }
}

fn mapi_folder_message_class(mailbox: &JmapMailbox) -> &'static str {
    match mailbox.role.as_str() {
        "contacts" => "IPF.Contact",
        "calendar" => "IPF.Appointment",
        _ => "IPF.Note",
    }
}

pub(crate) fn final_sync_state_stream(sync_type: u8, max_change: u64) -> Vec<u8> {
    let idset = replguid_idset_from_change(max_change);
    let mut token = Vec::new();
    write_u32(&mut token, INCR_SYNC_STATE_BEGIN);
    write_binary_property(&mut token, META_TAG_IDSET_GIVEN, &idset);
    write_binary_property(&mut token, META_TAG_CNSET_SEEN, &idset);
    if sync_type == 0x01 {
        write_binary_property(&mut token, META_TAG_CNSET_SEEN_FAI, &idset);
        write_binary_property(&mut token, META_TAG_CNSET_READ, &idset);
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
        write_prefixed_bytes(&mut buffer, &source_key_for_uuid(&mailbox.id));
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

fn replguid_idset_from_change(change: u64) -> Vec<u8> {
    let mut idset = STORE_REPLICA_GUID.to_vec();
    write_globset_ranges(&mut idset, &[(1, change.max(1))]);
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
    let mut bytes = [0; 6];
    bytes.copy_from_slice(&value.to_le_bytes()[..6]);
    bytes
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
        let id = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
        crate::mapi::identity::remember_mapi_identity(id, crate::mapi::identity::mapi_store_id(42));
        let source_key = source_key_for_uuid(&id);
        let change_key = change_key_for_change_number(42);

        assert_eq!(STORE_REPLICA_GUID[7] & 0xf0, 0x40);
        assert_eq!(STORE_REPLICA_GUID[8] & 0xc0, 0x80);
        assert!(source_key.starts_with(&STORE_REPLICA_GUID));
        assert!(change_key.starts_with(&STORE_REPLICA_GUID));
        assert_eq!(source_key, source_key_for_uuid(&id));
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
    }

    #[test]
    fn sync_manifest_serializes_fast_transfer_boolean_values_as_u16() {
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
            crate::mapi::identity::ROOT_FOLDER_ID,
            &[mailbox],
            &[],
            &[],
            &[],
            1,
        );

        let subfolders = PID_TAG_SUBFOLDERS.to_le_bytes();
        let display_name = PID_TAG_DISPLAY_NAME_W.to_le_bytes();
        let offset = buffer
            .windows(subfolders.len())
            .position(|window| window == subfolders)
            .expect("subfolders property is present");
        assert_eq!(&buffer[offset + 4..offset + 6], &[0, 0]);
        assert_eq!(&buffer[offset + 6..offset + 10], &display_name);
    }

    #[test]
    fn final_sync_state_uses_replguid_globset_ranges() {
        let token = final_sync_state_stream(0x02, 32);
        let mut expected = STORE_REPLICA_GUID.to_vec();
        expected.push(GLOBSET_RANGE_COMMAND);
        expected.extend_from_slice(&globcnt_bytes(1));
        expected.extend_from_slice(&globcnt_bytes(32));
        expected.push(GLOBSET_END_COMMAND);

        assert_variable_property(&token, META_TAG_IDSET_GIVEN, &expected);
        assert_variable_property(&token, META_TAG_CNSET_SEEN, &expected);
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
