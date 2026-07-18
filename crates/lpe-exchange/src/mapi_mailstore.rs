use std::collections::BTreeSet;

use lpe_domain::{
    crypto::hex_lower, days_from_civil, windows_filetime_from_signed_unix_seconds,
    WINDOWS_FILETIME_TICKS_PER_SECOND, WINDOWS_UNIX_EPOCH_OFFSET_SECONDS,
};
use lpe_storage::{JmapEmail, JmapMailbox};
use uuid::Uuid;

mod diagnostics;
mod folders;
mod manifest;
#[cfg(test)]
mod tests;

pub(crate) use folders::*;

#[cfg(test)]
pub(crate) use manifest::sync_manifest_buffer_with_attachments;
#[cfg(test)]
pub(crate) use manifest::sync_manifest_buffer_with_final_state;
#[cfg(test)]
pub(crate) use manifest::sync_manifest_buffer_with_special_objects_and_final_state;
pub(crate) use manifest::{
    canonical_folder_change_number, canonical_message_change_number,
    canonical_message_change_number_with_attachments, change_key_for_change_number,
    change_number_for_store_id, filetime_from_change_number, filetime_from_rfc3339_utc,
    predecessor_change_list, source_key_for_mailbox_folder, source_key_for_mailbox_role,
    source_key_for_store_id, source_key_for_uuid, special_message_source_key,
    sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions,
    sync_state_token_with_attachments, sync_state_token_with_special_objects,
    virtual_special_mailbox, AttachmentSyncFact, FaiContentSyncDebugContext,
    MessageAttachmentSyncFacts, SpecialMessagePropertyValue, SpecialMessageSyncFact,
};

#[cfg(test)]
pub(crate) use diagnostics::{
    decode_content_transfer_fai_debug_summary, decode_hierarchy_transfer_debug_summary,
    fai_debug_state_origin, format_marker_tags, hierarchy_identity_properties_before_display_name,
    hierarchy_microsoft_payload_comparison, hierarchy_semantic_validation,
    ContentTransferFaiDebugSummary, ContentTransferFaiItemDebug,
};
pub(crate) use diagnostics::{
    default_folder_hierarchy_membership_summary, final_sync_state_debug_summary,
    hierarchy_parent_source_key_role, hierarchy_transfer_close_summary, log_fai_content_sync_debug,
    log_hierarchy_get_buffer_payload_summary, log_hierarchy_transfer_debug,
    replguid_globset_counters, replguid_globset_debug_summary,
};

pub(crate) use crate::mapi::identity::STORE_REPLICA_GUID;
use crate::mapi::properties::canonical_property_storage_tag;
use crate::mapi::wire::{FastTransferMarker, MapiSyncType};

const INCR_SYNC_CHG: u32 = FastTransferMarker::IncrSyncChg.as_u32();
const INCR_SYNC_DEL: u32 = FastTransferMarker::IncrSyncDel.as_u32();
const INCR_SYNC_END: u32 = FastTransferMarker::IncrSyncEnd.as_u32();
const INCR_SYNC_MESSAGE: u32 = FastTransferMarker::IncrSyncMessage.as_u32();
const INCR_SYNC_READ: u32 = FastTransferMarker::IncrSyncRead.as_u32();
const INCR_SYNC_STATE_BEGIN: u32 = FastTransferMarker::IncrSyncStateBegin.as_u32();
const INCR_SYNC_STATE_END: u32 = FastTransferMarker::IncrSyncStateEnd.as_u32();
const INCR_SYNC_PROGRESS_MODE: u32 = FastTransferMarker::IncrSyncProgressMode.as_u32();
const INCR_SYNC_PROGRESS_PER_MSG: u32 = FastTransferMarker::IncrSyncProgressPerMsg.as_u32();
const NEW_ATTACH: u32 = FastTransferMarker::NewAttach.as_u32();
const START_EMBED: u32 = FastTransferMarker::StartEmbed.as_u32();
const END_EMBED: u32 = FastTransferMarker::EndEmbed.as_u32();
const START_RECIP: u32 = FastTransferMarker::StartRecip.as_u32();
const END_TO_RECIP: u32 = FastTransferMarker::EndToRecip.as_u32();
const START_TOP_FLD: u32 = FastTransferMarker::StartTopFld.as_u32();
const START_SUB_FLD: u32 = FastTransferMarker::StartSubFld.as_u32();
const END_FOLDER: u32 = FastTransferMarker::EndFolder.as_u32();
const START_MESSAGE: u32 = FastTransferMarker::StartMessage.as_u32();
const END_MESSAGE: u32 = FastTransferMarker::EndMessage.as_u32();
const END_ATTACH: u32 = FastTransferMarker::EndAttach.as_u32();
const PID_TAG_DISPLAY_NAME_W: u32 = 0x3001_001F;
const PID_TAG_EMAIL_ADDRESS_W: u32 = 0x3003_001F;
const PID_TAG_CONTENT_COUNT: u32 = 0x3602_0003;
const PID_TAG_CONTENT_UNREAD_COUNT: u32 = 0x3603_0003;
const PID_TAG_SUBFOLDERS: u32 = 0x360A_000B;
const PID_TAG_FOLDER_TYPE: u32 = 0x3601_0003;
const PID_TAG_MESSAGE_CLASS_W: u32 = 0x001A_001F;
const PID_TAG_SUBJECT_W: u32 = 0x0037_001F;
const PID_TAG_NORMALIZED_SUBJECT_A: u32 = 0x0E1D_001E;
const PID_TAG_NORMALIZED_SUBJECT_W: u32 = 0x0E1D_001F;
const PID_TAG_BODY_W: u32 = 0x1000_001F;
const PID_TAG_CONTAINER_CLASS_W: u32 = 0x3613_001F;
const PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W: u32 = 0x36E5_001F;
const PID_TAG_MESSAGE_FLAGS: u32 = 0x0E07_0003;
const PID_TAG_MESSAGE_SIZE: u32 = 0x0E08_0003;
const PID_TAG_RECIPIENT_TYPE: u32 = 0x0C15_0003;
const PID_TAG_ATTACH_SIZE: u32 = 0x0E20_0003;
const PID_TAG_ATTACH_NUM: u32 = 0x0E21_0003;
const PID_TAG_ENTRY_ID: u32 = 0x0FFF_0102;
const PID_TAG_RECORD_KEY: u32 = 0x0FF9_0102;
const PID_TAG_SEARCH_KEY: u32 = 0x300B_0102;
const PID_TAG_ATTACH_ENCODING: u32 = 0x3702_0102;
const PID_TAG_ATTACH_FILENAME_W: u32 = 0x3704_001F;
const PID_TAG_ATTACH_METHOD: u32 = 0x3705_0003;
const PID_TAG_ATTACH_LONG_FILENAME_W: u32 = 0x3707_001F;
const PID_TAG_ATTACH_RENDERING: u32 = 0x3709_0102;
const PID_TAG_RENDERING_POSITION: u32 = 0x370B_0003;
const PID_TAG_ATTACH_MIME_TAG_W: u32 = 0x370E_001F;
const PID_TAG_ATTACH_FLAGS: u32 = 0x3714_0003;
const PID_TAG_ATTACHMENT_HIDDEN: u32 = 0x7FFE_000B;
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
const MSGFLAG_UNMODIFIED: u32 = 0x0000_0002;
const MSGFLAG_UNSENT: u32 = 0x0000_0008;
const MSGFLAG_HASATTACH: u32 = 0x0000_0010;
const MSGFLAG_FAI: u32 = 0x0000_0040;
const ATTACH_BY_VALUE: i32 = 1;
const ATTACH_EMBEDDED_MESSAGE: i32 = 5;
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
const META_TAG_IDSET_GIVEN: u32 = 0x4017_0003;
const META_TAG_IDSET_GIVEN_BINARY: u32 = 0x4017_0102;
const META_TAG_IDSET_DELETED: u32 = 0x4018_0102;
const META_TAG_IDSET_READ: u32 = 0x402D_0102;
const META_TAG_IDSET_UNREAD: u32 = 0x402E_0102;
const META_TAG_CNSET_SEEN: u32 = 0x6796_0102;
const META_TAG_CNSET_SEEN_FAI: u32 = 0x67DA_0102;
const META_TAG_CNSET_READ: u32 = 0x67D2_0102;
const SYNC_TYPE_CONTENTS: u8 = MapiSyncType::Contents.as_u8();
const SYNC_TYPE_HIERARCHY: u8 = MapiSyncType::Hierarchy.as_u8();
const SYNC_FLAG_FAI: u16 = 0x0010;
const SYNC_FLAG_NORMAL: u16 = 0x0020;
const SYNC_FLAG_UNICODE: u16 = 0x0001;
const SYNC_FLAG_NO_FOREIGN_IDENTIFIERS: u16 = 0x0100;
const SYNC_FLAG_PROGRESS: u16 = 0x8000;
const SYNC_EXTRA_FLAG_EID: u32 = 0x0000_0001;
// [MS-OXCFXICS] section 2.2.3.2.1.1.1: MessageSize is bit 0x02;
// bit 0x08 is OrderByDeliveryTime.
const SYNC_EXTRA_FLAG_MESSAGE_SIZE: u32 = 0x0000_0002;
const SYNC_EXTRA_FLAG_CHANGE_NUMBER: u32 = 0x0000_0004;
const GLOBSET_RANGE_COMMAND: u8 = 0x52;
const GLOBSET_BITMASK_COMMAND: u8 = 0x42;
const GLOBSET_POP_COMMAND: u8 = 0x50;
const GLOBSET_END_COMMAND: u8 = 0x00;
const FILETIME_2026_01_01: u64 =
    (WINDOWS_UNIX_EPOCH_OFFSET_SECONDS + 1_767_225_600) * WINDOWS_FILETIME_TICKS_PER_SECOND;
const VIRTUAL_SPECIAL_MAILBOX_UUID_PREFIX: u128 = 0x4c50455f_4d415049_0000_0000_0000_0000;

fn property_tag_excluded(excluded_property_tags: &[u32], property_tag: u32) -> bool {
    property_tag_requested(excluded_property_tags, property_tag)
}

fn property_tag_requested(requested_property_tags: &[u32], property_tag: u32) -> bool {
    let property_tag = canonical_property_storage_tag(property_tag);
    requested_property_tags
        .iter()
        .any(|tag| canonical_property_storage_tag(*tag) == property_tag)
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
        property_tag_requested(sync_property_tags, property_tag)
    } else {
        !property_tag_requested(sync_property_tags, property_tag)
    }
}

fn content_sync_includes_normal(sync_type: u8, sync_flags: u16) -> bool {
    sync_type != SYNC_TYPE_CONTENTS
        || sync_flags & (SYNC_FLAG_NORMAL | SYNC_FLAG_FAI) == 0
        || sync_flags & SYNC_FLAG_NORMAL != 0
}

fn default_content_sync_includes_associated(
    emails: &[JmapEmail],
    special_objects: &[SpecialMessageSyncFact],
) -> bool {
    emails.is_empty()
        && special_objects.iter().all(|object| object.associated)
        && special_objects.iter().any(|object| object.associated)
}

fn content_sync_includes_associated(
    sync_type: u8,
    sync_flags: u16,
    associated: bool,
    default_include_associated: bool,
) -> bool {
    if sync_type != SYNC_TYPE_CONTENTS {
        return true;
    }
    if sync_flags & (SYNC_FLAG_NORMAL | SYNC_FLAG_FAI) == 0 {
        return associated == default_include_associated;
    }
    if associated {
        sync_flags & SYNC_FLAG_FAI != 0
    } else {
        sync_flags & SYNC_FLAG_NORMAL != 0
    }
}

fn mapi_folder_type(mailbox: &JmapMailbox) -> i32 {
    if mailbox.role == "__mapi_search" || mailbox.role.starts_with("__mapi_search_folder_") {
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
            .map(|mailbox| mapi_folder_id_for_mailbox(mailbox, folder_id))
            .filter(|object_id| *object_id != folder_id)
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
        let mut change_numbers = BTreeSet::new();
        change_numbers.extend(mailboxes.iter().filter_map(|mailbox| {
            let object_id = mapi_folder_id_for_mailbox(mailbox, folder_id);
            (object_id != folder_id).then(|| canonical_hierarchy_change_number(folder_id, mailbox))
        }));
        change_numbers.into_iter().collect()
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

pub(crate) fn canonical_hierarchy_change_number(
    _sync_root_folder_id: u64,
    mailbox: &JmapMailbox,
) -> u64 {
    canonical_folder_change_number(mailbox)
}

pub(crate) fn final_sync_state_stream(
    sync_type: u8,
    object_ids: &[u64],
    change_numbers: &[u64],
) -> Vec<u8> {
    if sync_type == SYNC_TYPE_CONTENTS {
        return final_content_sync_state_stream(object_ids, change_numbers, &[], change_numbers);
    }
    final_sync_state_stream_with_cnsets(sync_type, object_ids, change_numbers, &[], &[])
}

pub(crate) fn initial_sync_state_stream(sync_type: u8) -> Vec<u8> {
    final_sync_state_stream(sync_type, &[], &[])
}

pub(crate) fn sync_state_stream_with_uploaded_property(
    sync_type: u8,
    current_state: &[u8],
    property_tag: u32,
    value: &[u8],
) -> Vec<u8> {
    if !value.is_empty() && replguid_globset_counters(value).is_err() {
        return current_state.to_vec();
    }
    let normalized_property_tag = match property_tag {
        META_TAG_IDSET_GIVEN | META_TAG_IDSET_GIVEN_BINARY => META_TAG_IDSET_GIVEN,
        tag => tag,
    };
    let idset_given = if normalized_property_tag == META_TAG_IDSET_GIVEN {
        value.to_vec()
    } else {
        sync_state_property_value(current_state, META_TAG_IDSET_GIVEN).unwrap_or_default()
    };
    let cnset_seen = if normalized_property_tag == META_TAG_CNSET_SEEN {
        value.to_vec()
    } else {
        sync_state_property_value(current_state, META_TAG_CNSET_SEEN).unwrap_or_default()
    };
    if sync_type != SYNC_TYPE_CONTENTS {
        return sync_state_stream_from_raw_properties(
            sync_type,
            &idset_given,
            &cnset_seen,
            &[],
            &[],
        );
    }
    let cnset_seen_fai = if normalized_property_tag == META_TAG_CNSET_SEEN_FAI {
        value.to_vec()
    } else {
        sync_state_property_value(current_state, META_TAG_CNSET_SEEN_FAI).unwrap_or_default()
    };
    let cnset_read = if normalized_property_tag == META_TAG_CNSET_READ {
        value.to_vec()
    } else {
        sync_state_property_value(current_state, META_TAG_CNSET_READ).unwrap_or_default()
    };
    sync_state_stream_from_raw_properties(
        sync_type,
        &idset_given,
        &cnset_seen,
        &cnset_seen_fai,
        &cnset_read,
    )
}

pub(crate) fn upload_sync_state_stream_with_uploaded_property(
    sync_type: u8,
    current_state: &[u8],
    property_tag: u32,
    value: &[u8],
) -> Vec<u8> {
    if !value.is_empty() && replguid_globset_counters(value).is_err() {
        return current_state.to_vec();
    }
    let normalized_property_tag = match property_tag {
        META_TAG_IDSET_GIVEN | META_TAG_IDSET_GIVEN_BINARY => META_TAG_IDSET_GIVEN,
        tag => tag,
    };
    // [MS-OXCFXICS] section 3.2.5.2.1: MetaTagIdsetGiven is ignored
    // during upload and is not included in the final upload state.
    if normalized_property_tag == META_TAG_IDSET_GIVEN {
        return current_state.to_vec();
    }
    let cnset_seen = if normalized_property_tag == META_TAG_CNSET_SEEN {
        value.to_vec()
    } else {
        sync_state_property_value(current_state, META_TAG_CNSET_SEEN).unwrap_or_default()
    };
    let cnset_seen_fai = if normalized_property_tag == META_TAG_CNSET_SEEN_FAI {
        value.to_vec()
    } else {
        sync_state_property_value(current_state, META_TAG_CNSET_SEEN_FAI).unwrap_or_default()
    };
    let cnset_read = if normalized_property_tag == META_TAG_CNSET_READ {
        value.to_vec()
    } else {
        sync_state_property_value(current_state, META_TAG_CNSET_READ).unwrap_or_default()
    };
    upload_sync_state_stream_from_raw_properties(
        sync_type,
        &cnset_seen,
        &cnset_seen_fai,
        &cnset_read,
    )
}

pub(crate) fn upload_sync_state_stream_from_sets(
    sync_type: u8,
    normal_change_numbers: &[u64],
    fai_change_numbers: &[u64],
    read_change_numbers: &[u64],
) -> Vec<u8> {
    upload_sync_state_stream_from_raw_properties(
        sync_type,
        &replguid_idset_from_counters(normal_change_numbers),
        &replguid_idset_from_counters(fai_change_numbers),
        &replguid_idset_from_counters(read_change_numbers),
    )
}

fn final_content_sync_state_stream(
    object_ids: &[u64],
    normal_change_numbers: &[u64],
    fai_change_numbers: &[u64],
    read_change_numbers: &[u64],
) -> Vec<u8> {
    final_sync_state_stream_with_cnsets(
        SYNC_TYPE_CONTENTS,
        object_ids,
        normal_change_numbers,
        fai_change_numbers,
        read_change_numbers,
    )
}

fn sync_state_stream_from_raw_properties(
    sync_type: u8,
    idset_given: &[u8],
    cnset_seen: &[u8],
    cnset_seen_fai: &[u8],
    cnset_read: &[u8],
) -> Vec<u8> {
    let mut token = Vec::new();
    write_u32(&mut token, INCR_SYNC_STATE_BEGIN);
    write_binary_property(&mut token, META_TAG_IDSET_GIVEN, idset_given);
    write_binary_property(&mut token, META_TAG_CNSET_SEEN, cnset_seen);
    if sync_type == SYNC_TYPE_CONTENTS {
        write_binary_property(&mut token, META_TAG_CNSET_SEEN_FAI, cnset_seen_fai);
        write_binary_property(&mut token, META_TAG_CNSET_READ, cnset_read);
    }
    write_u32(&mut token, INCR_SYNC_STATE_END);
    token
}

fn upload_sync_state_stream_from_raw_properties(
    sync_type: u8,
    cnset_seen: &[u8],
    cnset_seen_fai: &[u8],
    cnset_read: &[u8],
) -> Vec<u8> {
    let mut token = Vec::new();
    write_u32(&mut token, INCR_SYNC_STATE_BEGIN);
    write_binary_property(&mut token, META_TAG_CNSET_SEEN, cnset_seen);
    if sync_type == SYNC_TYPE_CONTENTS {
        write_binary_property(&mut token, META_TAG_CNSET_SEEN_FAI, cnset_seen_fai);
        write_binary_property(&mut token, META_TAG_CNSET_READ, cnset_read);
    }
    write_u32(&mut token, INCR_SYNC_STATE_END);
    token
}

fn final_sync_state_stream_with_cnsets(
    sync_type: u8,
    object_ids: &[u64],
    normal_change_numbers: &[u64],
    fai_change_numbers: &[u64],
    read_change_numbers: &[u64],
) -> Vec<u8> {
    let idset_given = replguid_idset_from_object_ids(object_ids);
    let cnset_seen = replguid_idset_from_counters(normal_change_numbers);
    let mut token = Vec::new();
    write_u32(&mut token, INCR_SYNC_STATE_BEGIN);
    write_binary_property(&mut token, META_TAG_IDSET_GIVEN, &idset_given);
    write_binary_property(&mut token, META_TAG_CNSET_SEEN, &cnset_seen);
    if sync_type == SYNC_TYPE_CONTENTS {
        write_binary_property(
            &mut token,
            META_TAG_CNSET_SEEN_FAI,
            &replguid_idset_from_counters(fai_change_numbers),
        );
        write_binary_property(
            &mut token,
            META_TAG_CNSET_READ,
            &replguid_idset_from_counters(read_change_numbers),
        );
    }
    write_u32(&mut token, INCR_SYNC_STATE_END);
    token
}

fn sync_state_property_value(state: &[u8], property_tag: u32) -> Option<Vec<u8>> {
    let mut offset = 0usize;
    if read_sync_state_u32(state, &mut offset)? != INCR_SYNC_STATE_BEGIN {
        return None;
    }
    while offset + 4 <= state.len() {
        let tag = read_sync_state_u32(state, &mut offset)?;
        if tag == INCR_SYNC_STATE_END {
            return None;
        }
        let len = read_sync_state_u32(state, &mut offset)? as usize;
        let end = offset.checked_add(len)?;
        if end > state.len() {
            return None;
        }
        let normalized_tag = match tag {
            META_TAG_IDSET_GIVEN | META_TAG_IDSET_GIVEN_BINARY => META_TAG_IDSET_GIVEN,
            tag => tag,
        };
        if normalized_tag == property_tag {
            return Some(state[offset..end].to_vec());
        }
        offset = end;
    }
    None
}

fn read_sync_state_u32(bytes: &[u8], offset: &mut usize) -> Option<u32> {
    let end = offset.checked_add(4)?;
    let value = u32::from_le_bytes(bytes.get(*offset..end)?.try_into().ok()?);
    *offset = end;
    Some(value)
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

pub(crate) fn fast_transfer_message_list_buffer_with_attachments(
    emails: &[JmapEmail],
    attachment_facts: &[MessageAttachmentSyncFacts],
) -> Vec<u8> {
    let mut buffer = Vec::new();
    let mut messages = emails.iter().collect::<Vec<_>>();
    messages.sort_by(|left, right| {
        left.received_at
            .cmp(&right.received_at)
            .then(left.subject.cmp(&right.subject))
            .then(left.id.cmp(&right.id))
    });
    for email in messages {
        let attachments = attachments_for_message(email.id, attachment_facts);
        write_u32(&mut buffer, START_MESSAGE);
        write_fast_transfer_message_content(&mut buffer, email, attachments);
        write_u32(&mut buffer, END_MESSAGE);
    }
    buffer
}

pub(crate) fn fast_transfer_message_content_buffer_with_attachments(
    email: &JmapEmail,
    attachment_facts: &[MessageAttachmentSyncFacts],
) -> Vec<u8> {
    let mut buffer = Vec::new();
    write_fast_transfer_message_content(
        &mut buffer,
        email,
        attachments_for_message(email.id, attachment_facts),
    );
    buffer
}

fn write_fast_transfer_message_content(
    buffer: &mut Vec<u8>,
    email: &JmapEmail,
    attachments: &[AttachmentSyncFact],
) {
    write_utf16_property(buffer, PID_TAG_SUBJECT_W, &email.subject);
    write_utf16_property(buffer, PID_TAG_BODY_W, &email.body_text);
    write_fast_transfer_visible_recipients(buffer, email);
    write_fast_transfer_attachments(buffer, attachments);
}

pub(crate) fn fast_transfer_top_folder_buffer_with_attachments(
    folder_id: u64,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    attachment_facts: &[MessageAttachmentSyncFacts],
) -> Vec<u8> {
    let mut buffer = Vec::new();
    if let Some(mailbox) = mailboxes.iter().find(|mailbox| {
        let fallback = crate::mapi::identity::mapped_mapi_object_id(&mailbox.id).unwrap_or(0);
        mapi_folder_id_for_mailbox(mailbox, fallback) == folder_id
    }) {
        write_fast_transfer_folder_content(
            &mut buffer,
            folder_id,
            mailbox,
            mailboxes,
            emails,
            attachment_facts,
            true,
        );
    } else {
        write_u32(&mut buffer, START_TOP_FLD);
        write_u32(&mut buffer, END_FOLDER);
    }
    buffer
}

fn write_fast_transfer_folder_content(
    buffer: &mut Vec<u8>,
    folder_id: u64,
    mailbox: &JmapMailbox,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    attachment_facts: &[MessageAttachmentSyncFacts],
    top_folder: bool,
) {
    write_u32(
        buffer,
        if top_folder {
            START_TOP_FLD
        } else {
            START_SUB_FLD
        },
    );
    write_fast_transfer_folder_properties(
        buffer, folder_id, mailbox, mailboxes, emails, top_folder,
    );
    let folder_messages = fast_transfer_emails_for_folder(folder_id, mailboxes, emails);
    buffer.extend_from_slice(&fast_transfer_message_list_buffer_with_attachments(
        &folder_messages,
        attachment_facts,
    ));
    for child in fast_transfer_child_mailboxes(folder_id, mailboxes) {
        let child_folder_id = mapi_folder_id_for_mailbox(
            child,
            crate::mapi::identity::mapped_mapi_object_id(&child.id).unwrap_or(0),
        );
        write_fast_transfer_folder_content(
            buffer,
            child_folder_id,
            child,
            mailboxes,
            emails,
            attachment_facts,
            false,
        );
    }
    write_u32(buffer, END_FOLDER);
}

fn write_fast_transfer_folder_properties(
    buffer: &mut Vec<u8>,
    folder_id: u64,
    mailbox: &JmapMailbox,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    top_folder: bool,
) {
    if !top_folder {
        write_u32(buffer, PID_TAG_FOLDER_ID);
        write_object_id(buffer, folder_id);
        write_utf16_property(
            buffer,
            PID_TAG_DISPLAY_NAME_W,
            mapi_folder_display_name(mailbox),
        );
        write_u32(buffer, PID_TAG_PARENT_FOLDER_ID);
        write_object_id(
            buffer,
            mapi_folder_parent_id_for_mailbox(mailbox, mailboxes),
        );
    }
    let (content_count, unread_count, _) =
        folder_content_counts(folder_id, mailbox, mailboxes, emails);
    write_utf16_property(
        buffer,
        PID_TAG_CONTAINER_CLASS_W,
        mapi_folder_message_class(mailbox),
    );
    write_i32_property(buffer, PID_TAG_CONTENT_COUNT, content_count);
    write_i32_property(buffer, PID_TAG_CONTENT_UNREAD_COUNT, unread_count);
    write_i32_property(buffer, PID_TAG_ACCESS, MAPI_FOLDER_ACCESS as i32);
    write_bool_property(
        buffer,
        PID_TAG_SUBFOLDERS,
        mapi_folder_has_subfolders(mailbox, mailboxes),
    );
}

fn fast_transfer_child_mailboxes<'a>(
    folder_id: u64,
    mailboxes: &'a [JmapMailbox],
) -> Vec<&'a JmapMailbox> {
    let mut children = mailboxes
        .iter()
        .filter(|mailbox| {
            let fallback = crate::mapi::identity::mapped_mapi_object_id(&mailbox.id).unwrap_or(0);
            mapi_folder_id_for_mailbox(mailbox, fallback) != folder_id
                && mapi_folder_parent_id_for_mailbox(mailbox, mailboxes) == folder_id
        })
        .collect::<Vec<_>>();
    children.sort_by(|left, right| {
        mapi_folder_display_name(left)
            .cmp(mapi_folder_display_name(right))
            .then(left.id.cmp(&right.id))
    });
    children
}

fn fast_transfer_emails_for_folder(
    folder_id: u64,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
) -> Vec<JmapEmail> {
    let mut messages = emails
        .iter()
        .filter(|email| fast_transfer_email_matches_folder(email, folder_id, mailboxes))
        .cloned()
        .collect::<Vec<_>>();
    messages.sort_by(|left, right| {
        left.received_at
            .cmp(&right.received_at)
            .then(left.subject.cmp(&right.subject))
            .then(left.id.cmp(&right.id))
    });
    messages
}

fn fast_transfer_email_matches_folder(
    email: &JmapEmail,
    folder_id: u64,
    mailboxes: &[JmapMailbox],
) -> bool {
    if let Some((role, _, _, _, _)) = virtual_special_folder_metadata(folder_id) {
        if role.starts_with("__mapi_") {
            return false;
        }
        return email.mailbox_states.iter().any(|state| state.role == role)
            || email.mailbox_role == role;
    }

    mailboxes
        .iter()
        .find(|mailbox| {
            let fallback = crate::mapi::identity::mapped_mapi_object_id(&mailbox.id).unwrap_or(0);
            mapi_folder_id_for_mailbox(mailbox, fallback) == folder_id
        })
        .is_some_and(|mailbox| {
            email
                .mailbox_states
                .iter()
                .any(|state| state.mailbox_id == mailbox.id)
                || email.mailbox_id == mailbox.id
        })
}

pub(crate) fn fast_transfer_message_content_buffer_with_special_object(
    folder_id: u64,
    object: &SpecialMessageSyncFact,
) -> Vec<u8> {
    let mut buffer = Vec::new();
    write_fast_transfer_special_message_content(&mut buffer, folder_id, object);
    buffer
}

fn write_fast_transfer_special_message_content(
    buffer: &mut Vec<u8>,
    folder_id: u64,
    object: &SpecialMessageSyncFact,
) {
    let source_key = manifest::special_message_source_key(object);
    let change_key = manifest::special_message_change_key(object);
    let predecessor_change_list = manifest::special_message_predecessor_change_list(object);
    write_binary_property(
        buffer,
        PID_TAG_PARENT_SOURCE_KEY,
        &source_key_for_store_id(folder_id),
    );
    write_binary_property(buffer, PID_TAG_SOURCE_KEY, &source_key);
    write_u32(buffer, PID_TAG_LAST_MODIFICATION_TIME);
    write_i64(buffer, object.last_modified_filetime as i64);
    write_binary_property(buffer, PID_TAG_CHANGE_KEY, &change_key);
    write_binary_property(
        buffer,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        &predecessor_change_list,
    );
    write_bool_property(buffer, PID_TAG_ASSOCIATED, object.associated);
    write_u32(buffer, PID_TAG_MID);
    write_object_id(buffer, object.item_id);
    write_i32_property(buffer, PID_TAG_MESSAGE_FLAGS, MSGFLAG_READ as i32);
    write_utf16_property(buffer, PID_TAG_SUBJECT_W, &object.subject);
    write_string8_property(buffer, PID_TAG_NORMALIZED_SUBJECT_A, &object.subject);
    write_utf16_property(buffer, PID_TAG_MESSAGE_CLASS_W, &object.message_class);
    write_utf16_property(buffer, PID_TAG_BODY_W, &object.body_text);
    write_i32_property(buffer, PID_TAG_MESSAGE_SIZE, object.message_size as i32);
    for (tag, value) in &object.named_properties {
        if !manifest::special_message_property_is_sync_identity(*tag) {
            write_special_message_property(buffer, *tag, value);
        }
    }
}

pub(crate) fn canonical_message_flags(email: &JmapEmail) -> u32 {
    let mut flags = MSGFLAG_UNMODIFIED;
    if !email.unread {
        flags |= MSGFLAG_READ;
    }
    if email
        .mailbox_states
        .iter()
        .find(|state| state.mailbox_id == email.mailbox_id)
        .is_some_and(|state| state.draft)
    {
        // [MS-OXCMSG] 2.2.1.6: mfUnsent identifies a message that is still
        // being composed and is treated as a Draft Message object.
        flags |= MSGFLAG_UNSENT;
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

fn write_fast_transfer_visible_recipients(buffer: &mut Vec<u8>, email: &JmapEmail) {
    let visible_recipients = email
        .to
        .iter()
        .map(|recipient| (1i32, recipient))
        .chain(email.cc.iter().map(|recipient| (2i32, recipient)));
    for (recipient_type, recipient) in visible_recipients {
        write_u32(buffer, START_RECIP);
        write_i32_property(buffer, PID_TAG_RECIPIENT_TYPE, recipient_type);
        write_utf16_property(
            buffer,
            PID_TAG_DISPLAY_NAME_W,
            recipient
                .display_name
                .as_deref()
                .unwrap_or(&recipient.address),
        );
        write_utf16_property(buffer, PID_TAG_EMAIL_ADDRESS_W, &recipient.address);
        write_u32(buffer, END_TO_RECIP);
    }
}

fn write_fast_transfer_attachments(buffer: &mut Vec<u8>, attachments: &[AttachmentSyncFact]) {
    let mut attachments = attachments.iter().collect::<Vec<_>>();
    attachments.sort_by(|left, right| {
        left.file_name
            .cmp(&right.file_name)
            .then(left.media_type.cmp(&right.media_type))
            .then(left.id.cmp(&right.id))
    });
    for (attach_num, attachment) in attachments.into_iter().enumerate().take(i32::MAX as usize) {
        let embedded_message = attachment_sync_fact_is_embedded_message(attachment);
        let attach_method = if embedded_message {
            ATTACH_EMBEDDED_MESSAGE
        } else {
            ATTACH_BY_VALUE
        };
        write_u32(buffer, NEW_ATTACH);
        write_i32_property(buffer, PID_TAG_ATTACH_NUM, attach_num as i32);
        write_binary_property(buffer, PID_TAG_ATTACH_ENCODING, &[]);
        write_i32_property(buffer, PID_TAG_RENDERING_POSITION, -1);
        write_i32_property(
            buffer,
            PID_TAG_ATTACH_SIZE,
            attachment.size_octets.min(i32::MAX as u64) as i32,
        );
        write_i32_property(buffer, PID_TAG_ATTACH_METHOD, attach_method);
        write_binary_property(buffer, PID_TAG_ATTACH_RENDERING, &[]);
        write_i32_property(buffer, PID_TAG_ATTACH_FLAGS, 0);
        write_bool_property(buffer, PID_TAG_ATTACHMENT_HIDDEN, false);
        write_utf16_property(buffer, PID_TAG_ATTACH_FILENAME_W, &attachment.file_name);
        write_utf16_property(
            buffer,
            PID_TAG_ATTACH_LONG_FILENAME_W,
            &attachment.file_name,
        );
        write_utf16_property(buffer, PID_TAG_ATTACH_MIME_TAG_W, &attachment.media_type);
        if embedded_message {
            write_fast_transfer_embedded_message(buffer, attachment);
        }
        write_u32(buffer, END_ATTACH);
    }
}

pub(crate) fn attachment_sync_fact_is_embedded_message(attachment: &AttachmentSyncFact) -> bool {
    attachment
        .media_type
        .trim()
        .eq_ignore_ascii_case("application/vnd.ms-outlook")
        || attachment
            .file_name
            .trim()
            .to_ascii_lowercase()
            .ends_with(".msg")
}

fn write_fast_transfer_embedded_message(buffer: &mut Vec<u8>, attachment: &AttachmentSyncFact) {
    let embedded = embedded_message_properties_from_attachment(attachment);
    write_u32(buffer, START_EMBED);
    write_utf16_property(buffer, PID_TAG_MESSAGE_CLASS_W, "IPM.Note");
    write_utf16_property(buffer, PID_TAG_SUBJECT_W, &embedded.subject);
    if let Some(body) = embedded.body_text.as_deref() {
        write_utf16_property(buffer, PID_TAG_BODY_W, body);
    }
    write_u32(buffer, END_EMBED);
}

struct EmbeddedMessageFastTransferProperties {
    subject: String,
    body_text: Option<String>,
}

fn embedded_message_properties_from_attachment(
    attachment: &AttachmentSyncFact,
) -> EmbeddedMessageFastTransferProperties {
    let mut properties = attachment
        .embedded_message_blob
        .as_deref()
        .map(embedded_message_properties_from_blob)
        .unwrap_or_else(|| EmbeddedMessageFastTransferProperties {
            subject: embedded_message_subject_from_file_name(&attachment.file_name),
            body_text: None,
        });
    if properties.subject.is_empty() {
        properties.subject = embedded_message_subject_from_file_name(&attachment.file_name);
    }
    properties
}

fn embedded_message_properties_from_blob(blob: &[u8]) -> EmbeddedMessageFastTransferProperties {
    let text = String::from_utf8_lossy(
        blob.strip_prefix(b"LPE-MAPI-EMBEDDED-MESSAGE\0")
            .unwrap_or(blob),
    );
    let subject = text
        .split_once("Subject:")
        .and_then(|(_, rest)| rest.split_once("\r\n").map(|(value, _)| value))
        .map(str::trim)
        .unwrap_or_default()
        .to_string();
    let body_text = text
        .split_once("Body-Length:")
        .and_then(|(_, rest)| rest.split_once("\r\n").map(|(_, body)| body))
        .map(|body| {
            body.split_once("\r\nHtml-Length:")
                .map(|(value, _)| value)
                .unwrap_or(body)
        })
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    EmbeddedMessageFastTransferProperties { subject, body_text }
}

fn embedded_message_subject_from_file_name(file_name: &str) -> String {
    file_name
        .trim()
        .strip_suffix(".msg")
        .filter(|value| !value.is_empty())
        .unwrap_or("Embedded message")
        .to_string()
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

fn write_object_id(buffer: &mut Vec<u8>, value: u64) {
    if let Some(bytes) = crate::mapi::identity::wire_id_bytes_from_object_id(value) {
        buffer.extend_from_slice(&bytes);
    } else {
        buffer.extend_from_slice(&value.to_le_bytes());
    }
}

fn write_change_number(buffer: &mut Vec<u8>, change_number: u64) {
    write_object_id(buffer, crate::mapi::identity::mapi_store_id(change_number));
}

fn write_i32_property(buffer: &mut Vec<u8>, property_tag: u32, value: i32) {
    write_u32(buffer, property_tag);
    write_i32(buffer, value);
}

fn write_content_sync_progress_mode(
    buffer: &mut Vec<u8>,
    messages: &[&JmapEmail],
    special_objects: &[&SpecialMessageSyncFact],
) {
    let fai_message_count = special_objects
        .iter()
        .filter(|object| object.associated)
        .count()
        .min(u32::MAX as usize) as u32;
    let fai_message_total_size = special_objects
        .iter()
        .filter(|object| object.associated)
        .map(|object| object.message_size.max(0) as u64)
        .sum::<u64>();
    let normal_special_count = special_objects
        .iter()
        .filter(|object| !object.associated)
        .count();
    let normal_message_count = messages
        .len()
        .saturating_add(normal_special_count)
        .min(u32::MAX as usize) as u32;
    let normal_message_total_size = messages
        .iter()
        .map(|message| message.size_octets.max(0) as u64)
        .sum::<u64>()
        .saturating_add(
            special_objects
                .iter()
                .filter(|object| !object.associated)
                .map(|object| object.message_size.max(0) as u64)
                .sum::<u64>(),
        );
    let mut progress = Vec::with_capacity(32);
    progress.extend_from_slice(&0u16.to_le_bytes());
    progress.extend_from_slice(&0u16.to_le_bytes());
    progress.extend_from_slice(&fai_message_count.to_le_bytes());
    progress.extend_from_slice(&fai_message_total_size.to_le_bytes());
    progress.extend_from_slice(&normal_message_count.to_le_bytes());
    progress.extend_from_slice(&0u32.to_le_bytes());
    progress.extend_from_slice(&normal_message_total_size.to_le_bytes());

    write_u32(buffer, INCR_SYNC_PROGRESS_MODE);
    write_binary_property(buffer, 0x0000_0102, &progress);
}

fn write_content_sync_progress_per_message(
    buffer: &mut Vec<u8>,
    message_size: i32,
    associated: bool,
) {
    write_u32(buffer, INCR_SYNC_PROGRESS_PER_MSG);
    write_i32_property(buffer, 0x0000_0003, message_size);
    write_bool_property(buffer, 0x0000_000B, associated);
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
        SpecialMessagePropertyValue::Bool(value) => {
            write_bool_property(buffer, property_tag, *value)
        }
        SpecialMessagePropertyValue::Guid(value) => {
            write_u32(buffer, property_tag);
            buffer.extend_from_slice(value);
        }
        SpecialMessagePropertyValue::I32(value) => write_i32_property(buffer, property_tag, *value),
        SpecialMessagePropertyValue::I64(value) => {
            write_u32(buffer, property_tag);
            write_i64(buffer, *value);
        }
        SpecialMessagePropertyValue::U32(value) => {
            write_u32(buffer, property_tag);
            write_u32(buffer, *value);
        }
        SpecialMessagePropertyValue::U64(value) => {
            write_u32(buffer, property_tag);
            write_i64(buffer, *value as i64);
        }
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
    if counters.is_empty() {
        return Vec::new();
    }
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
