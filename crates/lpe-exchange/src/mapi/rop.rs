use super::properties::*;
use super::session::*;
use super::sync::*;
use super::tables::*;
use super::wire::{MapiError, MapiPropertyType, MapiRestrictionType, RopId};
use super::*;
use sha2::{Digest, Sha256};

pub(in crate::mapi) fn split_rop_buffer(buffer: &[u8]) -> Option<(&[u8], &[u8])> {
    if let Some(payload) = rpc_header_ext_payload(buffer) {
        return split_rop_payload_spec(payload);
    }
    split_rop_payload_best_effort(buffer)
}

fn split_rop_payload_best_effort(buffer: &[u8]) -> Option<(&[u8], &[u8])> {
    let spec = split_rop_payload_spec(buffer);
    let legacy = split_rop_payload_legacy(buffer);
    match (spec, legacy) {
        (Some(spec), Some(legacy)) => {
            if spec.1.len() % 4 == 0 && legacy.1.len() % 4 != 0 {
                Some(spec)
            } else {
                Some(legacy)
            }
        }
        (Some(spec), None) => Some(spec),
        (None, Some(legacy)) => Some(legacy),
        (None, None) => None,
    }
}

pub(in crate::mapi) fn split_rop_payload_spec(buffer: &[u8]) -> Option<(&[u8], &[u8])> {
    if buffer.len() < 2 {
        return None;
    }
    let rop_size = u16::from_le_bytes([buffer[0], buffer[1]]) as usize;
    if rop_size < 2 || buffer.len() < rop_size {
        return None;
    }
    Some((&buffer[2..rop_size], &buffer[rop_size..]))
}

pub(in crate::mapi) fn split_rop_payload_legacy(buffer: &[u8]) -> Option<(&[u8], &[u8])> {
    if buffer.len() < 2 {
        return None;
    }
    let rop_size = u16::from_le_bytes([buffer[0], buffer[1]]) as usize;
    if buffer.len() < 2 + rop_size {
        return None;
    }
    Some((&buffer[2..2 + rop_size], &buffer[2 + rop_size..]))
}

pub(in crate::mapi) fn is_rpc_header_ext_rop_buffer(buffer: &[u8]) -> bool {
    rpc_header_ext_payload(buffer).is_some()
}

pub(in crate::mapi) fn rpc_header_ext_payload(buffer: &[u8]) -> Option<&[u8]> {
    if buffer.len() < 10 {
        return None;
    }
    let version = u16::from_le_bytes([buffer[0], buffer[1]]);
    let flags = u16::from_le_bytes([buffer[2], buffer[3]]);
    let size = u16::from_le_bytes([buffer[4], buffer[5]]) as usize;
    let size_actual = u16::from_le_bytes([buffer[6], buffer[7]]) as usize;
    if version != 0 || size == 0 || size > size_actual || buffer.len() < 8 + size {
        return None;
    }
    // The RCA bootstrap uses an uncompressed, unobfuscated RPC_HEADER_EXT payload
    // with the Last flag. Compression and XOR obfuscation are handled later.
    if flags & !0x0004 != 0 {
        return None;
    }
    let payload = &buffer[8..8 + size];
    split_rop_payload_spec(payload)?;
    Some(payload)
}

pub(in crate::mapi) fn rpc_header_ext_rop_buffer(payload: Vec<u8>) -> Vec<u8> {
    let size = payload.len().min(u16::MAX as usize) as u16;
    let mut buffer = Vec::with_capacity(8 + payload.len());
    buffer.extend_from_slice(&0u16.to_le_bytes());
    buffer.extend_from_slice(&0x0004u16.to_le_bytes());
    buffer.extend_from_slice(&size.to_le_bytes());
    buffer.extend_from_slice(&size.to_le_bytes());
    buffer.extend_from_slice(&payload);
    buffer
}

pub(in crate::mapi) fn rop_logon_response_body(
    principal: &AccountPrincipal,
    request: &RopRequest,
) -> Vec<u8> {
    let output_handle_index = request.output_handle_index.unwrap_or(0);
    let logon_flags = request.payload.first().copied().unwrap_or(0x01) & 0x07 | 0x01;
    let mut response = Vec::new();
    response.push(0xFE);
    response.push(output_handle_index);
    write_u32(&mut response, 0);
    response.push(logon_flags);
    for folder_id in PRIVATE_LOGON_SPECIAL_FOLDER_IDS {
        write_object_id(&mut response, folder_id);
    }
    response.push(0x07);
    response.extend_from_slice(&principal.account_id.to_bytes_le());
    response.extend_from_slice(&1u16.to_le_bytes());
    response.extend_from_slice(&mapi_mailstore::STORE_REPLICA_GUID);
    let now = SystemTime::now();
    response.extend_from_slice(&logon_time_bytes(now));
    write_u64(&mut response, gwart_time_marker(now));
    write_u32(&mut response, 0);
    response
}

pub(in crate::mapi) fn rop_public_folder_logon_response_body(
    principal: &AccountPrincipal,
    request: &RopRequest,
) -> Vec<u8> {
    let output_handle_index = request.output_handle_index.unwrap_or(0);
    let logon_flags = request.payload.first().copied().unwrap_or(0) & 0x07 & !0x01;
    let mut response = Vec::new();
    response.push(0xFE);
    response.push(output_handle_index);
    write_u32(&mut response, 0);
    response.push(logon_flags);
    for folder_id in PUBLIC_LOGON_SPECIAL_FOLDER_IDS {
        write_object_id(&mut response, folder_id);
    }
    response.push(0x00);
    response.extend_from_slice(&principal.tenant_id.to_bytes_le());
    response.extend_from_slice(&STORE_REPLICA_ID.to_le_bytes()[..2]);
    response.extend_from_slice(&mapi_mailstore::STORE_REPLICA_GUID);
    let now = SystemTime::now();
    response.extend_from_slice(&logon_time_bytes(now));
    write_u64(&mut response, gwart_time_marker(now));
    write_u32(&mut response, 0);
    response
}

pub(in crate::mapi) fn gwart_time_marker(now: SystemTime) -> u64 {
    now.duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs()
        .max(1)
}

pub(in crate::mapi) fn logon_time_bytes(now: SystemTime) -> [u8; 8] {
    let duration = now
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO);
    let seconds = duration.as_secs();
    let days = (seconds / 86_400) as i64;
    let seconds_of_day = seconds % 86_400;
    let hour = (seconds_of_day / 3_600) as u8;
    let minute = ((seconds_of_day % 3_600) / 60) as u8;
    let second = (seconds_of_day % 60) as u8;
    let day_of_week = ((days + 4).rem_euclid(7)) as u8;
    let (year, month, day) = civil_from_unix_days(days);
    let year = (year as u16).to_le_bytes();
    [
        second,
        minute,
        hour,
        day_of_week,
        day,
        month,
        year[0],
        year[1],
    ]
}

pub(in crate::mapi) fn civil_from_unix_days(days: i64) -> (i32, u8, u8) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = year + i64::from(month <= 2);
    (year as i32, month as u8, day as u8)
}

pub(in crate::mapi) fn rop_open_folder_response(request: &RopRequest, is_ghosted: bool) -> Vec<u8> {
    let mut response = vec![0x02, request.output_handle_index.unwrap_or(0)];
    write_u32(&mut response, 0);
    response.push(0);
    response.push(u8::from(is_ghosted));
    response
}

pub(in crate::mapi) fn rop_open_message_response(
    request: &RopRequest,
    subject: &str,
    recipient_count: usize,
) -> Vec<u8> {
    let mut response = vec![0x03, request.output_handle_index.unwrap_or(0)];
    write_u32(&mut response, 0);
    response.push(0);
    write_typed_string(&mut response, "");
    write_typed_string(&mut response, subject);
    response.extend_from_slice(&(recipient_count.min(u16::MAX as usize) as u16).to_le_bytes());
    response.extend_from_slice(&0u16.to_le_bytes());
    response.push(0);
    response
}

pub(in crate::mapi) fn rop_open_embedded_message_response(
    request: &RopRequest,
    message_id: u64,
    subject: &str,
    recipient_count: usize,
) -> Vec<u8> {
    let mut response = vec![0x46, request.output_handle_index.unwrap_or(0)];
    write_u32(&mut response, 0);
    response.push(0);
    write_object_id(&mut response, message_id);
    response.push(0);
    write_typed_string(&mut response, "");
    write_typed_string(&mut response, subject);
    response.extend_from_slice(&(recipient_count.min(u16::MAX as usize) as u16).to_le_bytes());
    response.extend_from_slice(&0u16.to_le_bytes());
    response.push(0);
    response
}

pub(in crate::mapi) fn rop_reload_cached_information_response(
    request: &RopRequest,
    object: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u8> {
    let (subject, recipient_count) = match object {
        Some(MapiObject::Message {
            folder_id,
            message_id,
            saved_email,
            ..
        }) => match message_for_id(*folder_id, *message_id, mailboxes, emails)
            .or_else(|| {
                search_folder_message_for_id(snapshot, *folder_id, *message_id)
                    .map(|message| &message.email)
            })
            .or(saved_email.as_ref().map(|saved| &saved.email))
        {
            Some(email) => (email.subject.clone(), message_recipients(email).len()),
            None => {
                return rop_error_response(0x10, request.response_handle_index(), 0x8004_010F);
            }
        },
        Some(MapiObject::PendingMessage {
            properties,
            recipients,
            ..
        }) => (
            pending_text_property(
                properties,
                &[PID_TAG_SUBJECT_W, PID_TAG_NORMALIZED_SUBJECT_W],
            ),
            recipients.len(),
        ),
        Some(MapiObject::PendingContact { properties, .. }) => (
            pending_text_property(
                properties,
                &[
                    PID_TAG_DISPLAY_NAME_W,
                    PID_TAG_SUBJECT_W,
                    PID_TAG_NORMALIZED_SUBJECT_W,
                ],
            ),
            0,
        ),
        Some(MapiObject::PendingEvent { properties, .. }) => (
            pending_text_property(
                properties,
                &[
                    PID_TAG_SUBJECT_W,
                    PID_TAG_NORMALIZED_SUBJECT_W,
                    PID_TAG_DISPLAY_NAME_W,
                ],
            ),
            0,
        ),
        Some(MapiObject::PendingTask { properties, .. }) => (
            pending_text_property(
                properties,
                &[
                    PID_TAG_SUBJECT_W,
                    PID_TAG_NORMALIZED_SUBJECT_W,
                    PID_TAG_DISPLAY_NAME_W,
                ],
            ),
            0,
        ),
        Some(MapiObject::PendingNote { properties, .. }) => (
            pending_text_property(
                properties,
                &[
                    PID_TAG_SUBJECT_W,
                    PID_TAG_NORMALIZED_SUBJECT_W,
                    PID_TAG_DISPLAY_NAME_W,
                ],
            ),
            0,
        ),
        Some(MapiObject::PendingJournalEntry { properties, .. }) => (
            pending_text_property(
                properties,
                &[
                    PID_TAG_SUBJECT_W,
                    PID_TAG_NORMALIZED_SUBJECT_W,
                    PID_TAG_DISPLAY_NAME_W,
                ],
            ),
            0,
        ),
        _ => return rop_error_response(0x10, request.response_handle_index(), 0x0000_04B9),
    };

    let mut response = vec![0x10, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(0);
    write_typed_string(&mut response, "");
    write_typed_string(&mut response, &subject);
    response.extend_from_slice(&(recipient_count.min(u16::MAX as usize) as u16).to_le_bytes());
    response.extend_from_slice(&0u16.to_le_bytes());
    response.push(0);
    response
}

pub(in crate::mapi) fn rop_message_status_response(
    request: &RopRequest,
    old_status: u32,
) -> Vec<u8> {
    let mut response = vec![
        RopId::SetMessageStatus.as_u8(),
        request.response_handle_index(),
    ];
    write_u32(&mut response, 0);
    write_u32(&mut response, old_status);
    response
}

pub(in crate::mapi) fn rop_create_folder_response(
    request: &RopRequest,
    folder_id: u64,
    existing: bool,
) -> Vec<u8> {
    let mut response = vec![0x1C, request.output_handle_index.unwrap_or(0)];
    write_u32(&mut response, 0);
    write_object_id(&mut response, folder_id);
    response.push(existing as u8);
    if existing {
        response.push(0);
    }
    response
}

pub(in crate::mapi) fn rop_get_hierarchy_table_response(
    request: &RopRequest,
    row_count: u32,
) -> Vec<u8> {
    let mut response = vec![0x04, request.output_handle_index.unwrap_or(0)];
    write_u32(&mut response, 0);
    write_u32(&mut response, row_count);
    response
}

pub(in crate::mapi) fn rop_get_contents_table_response(
    request: &RopRequest,
    row_count: u32,
) -> Vec<u8> {
    let mut response = vec![0x05, request.output_handle_index.unwrap_or(0)];
    write_u32(&mut response, 0);
    write_u32(&mut response, row_count);
    response
}

pub(in crate::mapi) fn rop_get_attachment_table_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x21, request.output_handle_index.unwrap_or(0)];
    write_u32(&mut response, 0);
    response
}

pub(in crate::mapi) fn rop_open_attachment_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x22, request.output_handle_index.unwrap_or(0)];
    write_u32(&mut response, 0);
    response
}

pub(in crate::mapi) fn rop_create_attachment_response(
    request: &RopRequest,
    attach_num: u32,
) -> Vec<u8> {
    let mut response = vec![0x23, request.output_handle_index.unwrap_or(0)];
    write_u32(&mut response, 0);
    write_u32(&mut response, attach_num);
    response
}

pub(in crate::mapi) fn rop_open_stream_response(
    request: &RopRequest,
    stream_size: usize,
) -> Vec<u8> {
    let mut response = vec![0x2B, request.output_handle_index.unwrap_or(0)];
    write_u32(&mut response, 0);
    write_u32(&mut response, stream_size.min(u32::MAX as usize) as u32);
    response
}

pub(in crate::mapi) fn rop_read_stream_response(
    request: &RopRequest,
    stream: &mut MapiObject,
) -> Vec<u8> {
    let input_handle_index = request.input_handle_index().unwrap_or(0);
    let MapiObject::AttachmentStream { data, position, .. } = stream else {
        return rop_error_response(0x2C, input_handle_index, 0x8004_010F);
    };
    let requested = request
        .read_byte_count()
        .unwrap_or(0)
        .min(u16::MAX as usize);
    let end = position.saturating_add(requested).min(data.len());
    let chunk = data[*position..end].to_vec();
    *position = end;

    let mut response = vec![0x2C, input_handle_index];
    write_u32(&mut response, 0);
    response.extend_from_slice(&(chunk.len() as u16).to_le_bytes());
    response.extend_from_slice(&chunk);
    response
}

pub(in crate::mapi) fn rop_seek_stream_response(
    request: &RopRequest,
    stream: &mut MapiObject,
) -> Vec<u8> {
    let input_handle_index = request.input_handle_index().unwrap_or(0);
    let MapiObject::AttachmentStream { data, position, .. } = stream else {
        return rop_error_response(0x2E, input_handle_index, 0x8004_010F);
    };
    let Some(offset) = request.stream_seek_offset() else {
        return rop_error_response(0x2E, input_handle_index, 0x8007_0057);
    };
    let base = match request.stream_seek_origin() {
        Some(0) => 0i64,
        Some(1) => *position as i64,
        Some(2) => data.len() as i64,
        _ => return rop_error_response(0x2E, input_handle_index, 0x8007_0057),
    };
    let Some(new_position) = base.checked_add(offset) else {
        return rop_error_response(0x2E, input_handle_index, 0x8007_0057);
    };
    if new_position < 0 {
        return rop_error_response(0x2E, input_handle_index, 0x8007_0057);
    }
    let new_position = new_position as usize;
    *position = new_position;

    let mut response = vec![0x2E, input_handle_index];
    write_u32(&mut response, 0);
    write_u64(&mut response, new_position as u64);
    response
}

pub(in crate::mapi) fn rop_write_stream_response(request: &RopRequest, written: usize) -> Vec<u8> {
    let mut response = vec![request.rop_id, request.response_handle_index()];
    write_u32(&mut response, 0);
    if request.rop_id == 0xA3 {
        write_u32(&mut response, written.min(u32::MAX as usize) as u32);
    } else {
        response.extend_from_slice(&(written.min(u16::MAX as usize) as u16).to_le_bytes());
    }
    response
}

pub(in crate::mapi) fn rop_copy_to_stream_response(
    request: &RopRequest,
    read: usize,
    written: usize,
) -> Vec<u8> {
    let mut response = vec![0x3A, request.response_handle_index()];
    write_u32(&mut response, 0);
    write_u64(&mut response, read as u64);
    write_u64(&mut response, written as u64);
    response
}

pub(in crate::mapi) fn rop_get_stream_size_response(
    request: &RopRequest,
    stream_size: usize,
) -> Vec<u8> {
    let mut response = vec![0x5E, request.response_handle_index()];
    write_u32(&mut response, 0);
    write_u32(&mut response, stream_size.min(u32::MAX as usize) as u32);
    response
}

pub(in crate::mapi) fn rop_set_read_flags_response(
    request: &RopRequest,
    partial_completion: bool,
) -> Vec<u8> {
    let mut response = vec![0x66, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(partial_completion as u8);
    response
}

pub(in crate::mapi) fn rop_long_term_id_from_id_response(request: &RopRequest) -> Vec<u8> {
    let Some(object_id) = request.long_term_source_object_id() else {
        return rop_error_response(0x43, request.response_handle_index(), 0x8004_010F);
    };
    let Some(long_term_id) = long_term_id_from_object_id(object_id) else {
        return rop_error_response(0x43, request.response_handle_index(), 0x8004_010F);
    };
    let mut response = vec![0x43, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.extend_from_slice(&long_term_id);
    response
}

pub(in crate::mapi) fn rop_id_from_long_term_id_response(
    request: &RopRequest,
    replica_guid_aliases: &[[u8; 16]],
) -> Vec<u8> {
    let Some(long_term_id) = request.long_term_id() else {
        return rop_error_response(0x44, request.response_handle_index(), 0x8004_0102);
    };
    let Some(object_id) = crate::mapi::identity::object_id_from_long_term_id_with_replica_guids(
        long_term_id,
        replica_guid_aliases,
    )
    .or_else(|| stale_special_folder_object_id_from_long_term_id(long_term_id)) else {
        return rop_error_response(0x44, request.response_handle_index(), 0x8004_010F);
    };
    let mut response = vec![0x44, request.response_handle_index()];
    write_u32(&mut response, 0);
    write_object_id(&mut response, object_id);
    response
}

pub(in crate::mapi) fn rop_get_per_user_long_term_ids_response(
    request: &RopRequest,
    long_term_ids: &[[u8; 24]],
) -> Vec<u8> {
    let mut response = vec![0x60, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.extend_from_slice(&(long_term_ids.len().min(u16::MAX as usize) as u16).to_le_bytes());
    for long_term_id in long_term_ids.iter().take(u16::MAX as usize) {
        response.extend_from_slice(long_term_id);
    }
    response
}

pub(in crate::mapi) fn rop_get_per_user_guid_response(
    request: &RopRequest,
    database_guid: &[u8; 16],
) -> Vec<u8> {
    let mut response = vec![0x61, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.extend_from_slice(database_guid);
    response
}

pub(in crate::mapi) fn rop_read_per_user_information_response(
    request: &RopRequest,
    stream: &[u8],
) -> Vec<u8> {
    let offset = request.per_user_data_offset() as usize;
    let max_size = request.per_user_max_data_size();
    let max_size = if max_size == 0 {
        u16::MAX as usize
    } else {
        max_size as usize
    };
    if offset > stream.len() {
        return rop_error_response(0x63, request.response_handle_index(), 0x8007_0057);
    }
    let end = offset.saturating_add(max_size).min(stream.len());
    let chunk = &stream[offset..end];
    let mut response = vec![0x63, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push((end >= stream.len()) as u8);
    response.extend_from_slice(&(chunk.len().min(u16::MAX as usize) as u16).to_le_bytes());
    response.extend_from_slice(chunk);
    response
}

pub(in crate::mapi) fn rop_write_per_user_information_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x64, request.response_handle_index()];
    write_u32(&mut response, 0);
    response
}

fn stale_special_folder_object_id_from_long_term_id(long_term_id: &[u8]) -> Option<u64> {
    if long_term_id.len() != 24 || long_term_id[22..24] != [0, 0] {
        return None;
    }
    let global_counter = crate::mapi::identity::global_counter_from_globcnt(&long_term_id[16..22])?;
    let object_id = crate::mapi::identity::mapi_store_id(global_counter);
    is_advertised_special_folder(object_id).then_some(object_id)
}

fn stale_special_folder_object_id_from_short_id(bytes: &[u8]) -> Option<u64> {
    if bytes.len() != 8 {
        return None;
    }
    let leading_replid = u16::from_le_bytes(bytes[..2].try_into().ok()?);
    let trailing_replid = u16::from_le_bytes(bytes[6..8].try_into().ok()?);
    let candidates = [
        (
            leading_replid,
            crate::mapi::identity::global_counter_from_globcnt(&bytes[2..8]),
        ),
        (
            trailing_replid,
            crate::mapi::identity::global_counter_from_globcnt(&bytes[..6]),
        ),
        (
            leading_replid,
            global_counter_from_little_endian_globcnt(&bytes[2..8]),
        ),
        (
            trailing_replid,
            global_counter_from_little_endian_globcnt(&bytes[..6]),
        ),
    ];
    candidates
        .into_iter()
        .filter(|(replica_id, _)| *replica_id != 0)
        .filter_map(|(_, counter)| counter)
        .map(crate::mapi::identity::mapi_store_id)
        .find(|object_id| is_advertised_special_folder(*object_id))
        .or_else(|| advertised_virtual_object_id_from_bare_little_endian_short_id(bytes))
        .or_else(|| dynamic_object_id_from_bare_little_endian_short_id(bytes))
}

fn advertised_virtual_object_id_from_bare_little_endian_short_id(bytes: &[u8]) -> Option<u64> {
    if bytes.len() != 8 || bytes[6..8] != [0, 0] {
        return None;
    }
    let counter = global_counter_from_little_endian_globcnt(&bytes[..6])?;
    let object_id = crate::mapi::identity::mapi_store_id(counter);
    (counter >= crate::mapi::identity::SYNC_ISSUES_FOLDER_COUNTER
        && is_advertised_special_folder(object_id))
    .then_some(object_id)
}

fn dynamic_object_id_from_bare_little_endian_short_id(bytes: &[u8]) -> Option<u64> {
    if bytes.len() != 8 || bytes[6..8] != [0, 0] {
        return None;
    }
    let counter = global_counter_from_little_endian_globcnt(&bytes[..6])?;
    (counter >= crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER)
        .then(|| crate::mapi::identity::mapi_store_id(counter))
}

fn global_counter_from_little_endian_globcnt(bytes: &[u8]) -> Option<u64> {
    let bytes: [u8; 6] = bytes.try_into().ok()?;
    let counter = u64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], 0, 0,
    ]);
    (counter != 0).then_some(counter)
}

pub(in crate::mapi) fn rop_get_address_types_response(request: &RopRequest) -> Vec<u8> {
    let address_types = b"EX\0SMTP\0";
    let mut response = vec![0x49, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.extend_from_slice(&2u16.to_le_bytes());
    response.extend_from_slice(&(address_types.len() as u16).to_le_bytes());
    response.extend_from_slice(address_types);
    response
}

pub(in crate::mapi) fn rop_transport_send_success_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x4A, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(1);
    response
}

pub(in crate::mapi) fn rop_options_data_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x6F, request.input_handle_index().unwrap_or(0)];
    write_u32(&mut response, 0);
    response.push(1);
    write_u16(&mut response, 0);
    write_u16(&mut response, 0);
    response
}

pub(in crate::mapi) fn rop_partial_completion_response(
    rop_id: u8,
    handle_index: u8,
    partial_completion: bool,
) -> Vec<u8> {
    let mut response = vec![rop_id, handle_index];
    write_u32(&mut response, 0);
    response.push(partial_completion as u8);
    response
}

pub(in crate::mapi) fn rop_set_columns_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x12, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(0);
    response
}

pub(in crate::mapi) fn rop_sort_table_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x13, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(0);
    response
}

pub(in crate::mapi) fn rop_expand_row_success_response(
    request: &RopRequest,
    expanded_row_count: usize,
    rows: Vec<Vec<u8>>,
) -> Vec<u8> {
    let mut response = vec![0x59, request.response_handle_index()];
    write_u32(&mut response, 0);
    write_u32(
        &mut response,
        expanded_row_count.min(u32::MAX as usize) as u32,
    );
    write_u16(&mut response, rows.len().min(u16::MAX as usize) as u16);
    for row in rows.into_iter().take(u16::MAX as usize) {
        write_standard_property_row(&mut response, &row);
    }
    response
}

pub(in crate::mapi) fn rop_collapse_row_success_response(
    request: &RopRequest,
    collapsed_row_count: usize,
) -> Vec<u8> {
    let mut response = vec![0x5A, request.response_handle_index()];
    write_u32(&mut response, 0);
    write_u32(
        &mut response,
        collapsed_row_count.min(u32::MAX as usize) as u32,
    );
    response
}

pub(in crate::mapi) fn rop_get_collapse_state_success_response(
    request: &RopRequest,
    collapse_state: &[u8],
) -> Vec<u8> {
    let mut response = vec![0x6B, request.response_handle_index()];
    write_u32(&mut response, 0);
    write_u16(
        &mut response,
        collapse_state.len().min(u16::MAX as usize) as u16,
    );
    response.extend_from_slice(&collapse_state[..collapse_state.len().min(u16::MAX as usize)]);
    response
}

pub(in crate::mapi) fn rop_set_collapse_state_success_response(
    request: &RopRequest,
    bookmark: &[u8],
) -> Vec<u8> {
    let mut response = vec![0x6C, request.response_handle_index()];
    write_u32(&mut response, 0);
    write_u16(&mut response, bookmark.len().min(u16::MAX as usize) as u16);
    response.extend_from_slice(&bookmark[..bookmark.len().min(u16::MAX as usize)]);
    response
}

pub(in crate::mapi) fn rop_restrict_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x14, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(0);
    response
}

pub(in crate::mapi) fn rop_create_message_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x06, request.output_handle_index.unwrap_or(0)];
    write_u32(&mut response, 0);
    response.push(0);
    response
}

pub(in crate::mapi) fn rop_set_properties_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![request.rop_id, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.extend_from_slice(&0u16.to_le_bytes());
    response
}

pub(in crate::mapi) fn rop_set_properties_problem_response(
    request: &RopRequest,
    problems: &[(usize, u32, u32)],
) -> Vec<u8> {
    let mut response = vec![request.rop_id, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.extend_from_slice(&(problems.len().min(u16::MAX as usize) as u16).to_le_bytes());
    for (index, property_tag, error_code) in problems.iter().take(u16::MAX as usize) {
        response.extend_from_slice(&(*index as u16).to_le_bytes());
        write_u32(&mut response, *property_tag);
        write_u32(&mut response, *error_code);
    }
    response
}

pub(in crate::mapi) fn rop_delete_properties_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![request.rop_id, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.extend_from_slice(&0u16.to_le_bytes());
    response
}

pub(in crate::mapi) fn rop_simple_success_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![request.rop_id, request.response_handle_index()];
    write_u32(&mut response, 0);
    response
}

pub(in crate::mapi) fn rop_get_search_criteria_response(
    request: &RopRequest,
    restriction: &[u8],
    folder_ids: &[u64],
    search_flags: u32,
) -> Vec<u8> {
    let mut response = vec![0x31, request.response_handle_index()];
    write_u32(&mut response, 0);
    if request.get_search_criteria_include_restriction() {
        response
            .extend_from_slice(&(restriction.len().min(u16::MAX as usize) as u16).to_le_bytes());
        response.extend_from_slice(&restriction[..restriction.len().min(u16::MAX as usize)]);
    } else {
        response.extend_from_slice(&0u16.to_le_bytes());
    }
    response.push(0);
    if request.get_search_criteria_include_folders() {
        response.extend_from_slice(&(folder_ids.len().min(u16::MAX as usize) as u16).to_le_bytes());
        for folder_id in folder_ids.iter().take(u16::MAX as usize) {
            write_object_id(&mut response, *folder_id);
        }
    } else {
        response.extend_from_slice(&0u16.to_le_bytes());
    }
    write_u32(&mut response, search_flags);
    response
}

pub(in crate::mapi) fn rop_upload_state_success_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![request.rop_id, request.input_handle_index().unwrap_or(0)];
    write_u32(&mut response, 0);
    response
}

pub(in crate::mapi) fn rop_fast_transfer_put_buffer_response(
    request: &RopRequest,
    used_size: usize,
) -> Vec<u8> {
    let mut response = vec![request.rop_id, request.input_handle_index().unwrap_or(0)];
    write_u32(&mut response, 0);
    if request.rop_id == RopId::FastTransferDestinationPutBufferExtended.as_u8() {
        write_u32(&mut response, used_size.min(u32::MAX as usize) as u32);
    } else {
        response.push(0);
        write_u16(&mut response, used_size.min(u16::MAX as usize) as u16);
    }
    response
}

pub(in crate::mapi) fn rop_save_changes_message_response(
    request: &RopRequest,
    message_id: u64,
) -> Vec<u8> {
    let mut response = vec![0x0C, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(request.input_handle_index().unwrap_or(0));
    write_object_id(&mut response, message_id);
    response
}

pub(in crate::mapi) fn rop_get_properties_list_response(
    request: &RopRequest,
    object: Option<&MapiObject>,
) -> Vec<u8> {
    let Some(object) = object else {
        return rop_error_response(0x09, request.response_handle_index(), 0x8004_0102);
    };
    let tags = match object {
        MapiObject::Logon => default_store_property_tags(),
        MapiObject::Folder {
            folder_id: ROOT_FOLDER_ID | INBOX_FOLDER_ID,
            ..
        } => default_folder_property_tags_with_identity(),
        MapiObject::Attachment { .. }
        | MapiObject::PendingAttachment { .. }
        | MapiObject::SavedAttachment { .. } => default_attachment_columns(),
        MapiObject::Contact { .. } | MapiObject::PendingContact { .. } => {
            default_contact_property_tags()
        }
        MapiObject::Event { .. } | MapiObject::PendingEvent { .. } => default_event_property_tags(),
        MapiObject::Task { .. } | MapiObject::PendingTask { .. } => default_task_property_tags(),
        MapiObject::Note { .. } | MapiObject::PendingNote { .. } => default_note_property_tags(),
        MapiObject::JournalEntry { .. } | MapiObject::PendingJournalEntry { .. } => {
            default_journal_entry_property_tags()
        }
        MapiObject::ConversationAction { .. } | MapiObject::PendingConversationAction { .. } => {
            default_conversation_action_property_tags()
        }
        MapiObject::Message { .. }
        | MapiObject::AssociatedConfig { .. }
        | MapiObject::PublicFolderItem { .. }
        | MapiObject::PendingAssociatedMessage { .. }
        | MapiObject::PendingMessage { .. } => default_message_property_tags(),
        _ => default_folder_property_tags(),
    };
    let mut response = vec![0x09, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.extend_from_slice(&(tags.len() as u16).to_le_bytes());
    for tag in tags {
        write_u32(&mut response, tag);
    }
    response
}

#[allow(dead_code)]
pub(in crate::mapi) fn rop_get_properties_specific_response(
    request: &RopRequest,
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u8> {
    rop_get_properties_specific_response_with_custom(
        request,
        object,
        principal,
        mailboxes,
        emails,
        snapshot,
        &HashMap::new(),
    )
}

pub(in crate::mapi) fn rop_get_properties_specific_response_with_custom(
    request: &RopRequest,
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    custom_values: &HashMap<u32, Vec<u8>>,
) -> Vec<u8> {
    let mut response = vec![0x07, request.input_handle_index().unwrap_or(0)];
    write_u32(&mut response, 0);
    let columns = request.property_tags();
    let mut unsupported_tags = unsupported_specific_property_tags(
        object, principal, mailboxes, emails, snapshot, &columns,
    );
    unsupported_tags.retain(|tag| !custom_values.contains_key(tag));
    let size_limited_tags = size_limited_specific_property_tags(
        request,
        object,
        principal,
        mailboxes,
        emails,
        snapshot,
        &columns,
        custom_values,
    );
    let row = match object {
        Some(MapiObject::Logon) => {
            log_get_properties_specific_debug(
                request, object, principal, &columns, mailboxes, emails, snapshot,
            );
            let supported_columns = columns
                .iter()
                .copied()
                .filter(|tag| !unsupported_tags.contains(tag))
                .collect::<Vec<_>>();
            write_logon_property_row(&mut response, principal, &supported_columns);
            return response;
        }
        Some(MapiObject::Message {
            folder_id,
            message_id,
            saved_email,
            ..
        }) => {
            let Some(_email) = message_for_id(*folder_id, *message_id, mailboxes, emails)
                .or_else(|| {
                    search_folder_message_for_id(snapshot, *folder_id, *message_id)
                        .map(|message| &message.email)
                })
                .or(saved_email.as_ref().map(|saved| &saved.email))
            else {
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    mailbox = %principal.email,
                    request_type = "Execute",
                    request_rop_id = "0x07",
                    object_kind = "message",
                    folder_id = %format!("{folder_id:#018x}"),
                    message_id = %format!("{message_id:#018x}"),
                    requested_property_tag_count = columns.len(),
                    requested_property_tags = %columns
                        .iter()
                        .map(|tag| format!("{tag:#010x}"))
                        .collect::<Vec<_>>()
                        .join(","),
                    base_email_count = emails.len(),
                    failure_reason = "message_identity_not_visible_for_getprops",
                    "rca debug mapi get properties specific"
                );
                return rop_error_response(
                    0x07,
                    request.input_handle_index().unwrap_or(0),
                    0x8004_010F,
                );
            };
            serialize_object_property_row_with_custom(
                object,
                principal,
                mailboxes,
                emails,
                snapshot,
                &columns,
                custom_values,
            )
        }
        Some(MapiObject::PendingMessage { properties, .. }) => {
            serialize_pending_message_row(principal, properties, &columns)
        }
        Some(MapiObject::PendingAssociatedMessage { properties, .. }) => {
            serialize_pending_associated_message_row(principal, properties, &columns)
        }
        Some(MapiObject::Contact {
            folder_id,
            contact_id,
        }) => {
            let Some(_contact) = snapshot.contact_for_id(*folder_id, *contact_id) else {
                return rop_error_response(
                    0x07,
                    request.input_handle_index().unwrap_or(0),
                    0x8004_010F,
                );
            };
            serialize_object_property_row_with_custom(
                object,
                principal,
                mailboxes,
                emails,
                snapshot,
                &columns,
                custom_values,
            )
        }
        Some(MapiObject::PendingContact { properties, .. }) => {
            serialize_pending_contact_row(principal, properties, &columns)
        }
        Some(MapiObject::Event {
            folder_id,
            event_id,
        }) => {
            let Some(_event) = snapshot.event_for_id(*folder_id, *event_id) else {
                return rop_error_response(
                    0x07,
                    request.input_handle_index().unwrap_or(0),
                    0x8004_010F,
                );
            };
            serialize_object_property_row_with_custom(
                object,
                principal,
                mailboxes,
                emails,
                snapshot,
                &columns,
                custom_values,
            )
        }
        Some(MapiObject::PendingEvent { properties, .. }) => {
            serialize_pending_event_row(principal, properties, &columns)
        }
        Some(MapiObject::PendingTask { properties, .. }) => {
            serialize_pending_task_row(principal, properties, &columns)
        }
        Some(MapiObject::PendingNote { properties, .. }) => {
            serialize_pending_note_row(principal, properties, &columns)
        }
        Some(MapiObject::PendingJournalEntry { properties, .. }) => {
            serialize_pending_journal_entry_row(principal, properties, &columns)
        }
        Some(MapiObject::PendingConversationAction { properties, .. }) => {
            serialize_pending_conversation_action_row(properties, &columns)
        }
        Some(MapiObject::PendingNavigationShortcut { properties, .. }) => {
            serialize_pending_navigation_shortcut_row(properties, principal, &columns)
        }
        Some(MapiObject::Task { folder_id, task_id }) => {
            let Some(_task) = snapshot.task_for_id(*folder_id, *task_id) else {
                return rop_error_response(
                    0x07,
                    request.input_handle_index().unwrap_or(0),
                    0x8004_010F,
                );
            };
            serialize_object_property_row_with_custom(
                object,
                principal,
                mailboxes,
                emails,
                snapshot,
                &columns,
                custom_values,
            )
        }
        Some(MapiObject::Note { folder_id, note_id }) => {
            let Some(_note) = snapshot.note_for_id(*folder_id, *note_id) else {
                return rop_error_response(
                    0x07,
                    request.input_handle_index().unwrap_or(0),
                    0x8004_010F,
                );
            };
            serialize_object_property_row_with_custom(
                object,
                principal,
                mailboxes,
                emails,
                snapshot,
                &columns,
                custom_values,
            )
        }
        Some(MapiObject::JournalEntry {
            folder_id,
            journal_entry_id,
        }) => {
            let Some(_entry) = snapshot.journal_entry_for_id(*folder_id, *journal_entry_id) else {
                return rop_error_response(
                    0x07,
                    request.input_handle_index().unwrap_or(0),
                    0x8004_010F,
                );
            };
            serialize_object_property_row_with_custom(
                object,
                principal,
                mailboxes,
                emails,
                snapshot,
                &columns,
                custom_values,
            )
        }
        Some(MapiObject::NavigationShortcut {
            folder_id,
            shortcut_id,
        }) => {
            let Some(message) = snapshot
                .navigation_shortcut_table_message_for_id(*shortcut_id)
                .filter(|message| message.folder_id == *folder_id)
            else {
                return rop_error_response(
                    0x07,
                    request.input_handle_index().unwrap_or(0),
                    0x8004_010F,
                );
            };
            serialize_navigation_shortcut_row(&message, Some(principal), &columns)
        }
        Some(MapiObject::CommonViewNamedView { folder_id, view_id }) => {
            let Some(message) = snapshot.named_view_message_for_folder_and_id(*folder_id, *view_id)
            else {
                return rop_error_response(
                    0x07,
                    request.input_handle_index().unwrap_or(0),
                    0x8004_010F,
                );
            };
            serialize_common_view_named_view_row_with_mailbox_guid(
                &message,
                principal.account_id,
                &columns,
            )
        }
        Some(MapiObject::SearchFolderDefinitionMessage {
            folder_id,
            message_id,
        }) => {
            let Some(definition) = (folder_id == &COMMON_VIEWS_FOLDER_ID)
                .then(|| {
                    snapshot.common_views_table_messages().find_map(|message| {
                        if let crate::mapi_store::MapiCommonViewsMessage::SearchFolderDefinition(
                            definition,
                        ) = message
                        {
                            (crate::mapi::identity::mapped_mapi_object_id(&definition.id)
                                == Some(*message_id))
                            .then_some(definition)
                        } else {
                            None
                        }
                    })
                })
                .flatten()
            else {
                return rop_error_response(
                    0x07,
                    request.input_handle_index().unwrap_or(0),
                    0x8004_010F,
                );
            };
            serialize_search_folder_definition_row_with_mailbox_guid(
                &definition,
                principal.account_id,
                &columns,
            )
        }
        Some(MapiObject::AssociatedConfig {
            folder_id,
            config_id,
            saved_message,
            ..
        }) => {
            let Some(message) = snapshot
                .associated_config_message_for_id(*config_id)
                .or_else(|| saved_message.clone())
                .filter(|message| message.folder_id == *folder_id)
            else {
                return rop_error_response(
                    0x07,
                    request.input_handle_index().unwrap_or(0),
                    0x8004_010F,
                );
            };
            serialize_associated_config_row_with_mailbox_guid(
                &message,
                principal.account_id,
                &columns,
            )
        }
        Some(MapiObject::ConversationAction {
            folder_id,
            conversation_action_id,
            ..
        }) => {
            let Some(message) = snapshot
                .conversation_action_table_message_for_id(*conversation_action_id)
                .filter(|message| message.folder_id == *folder_id)
            else {
                return rop_error_response(
                    0x07,
                    request.input_handle_index().unwrap_or(0),
                    0x8004_010F,
                );
            };
            serialize_conversation_action_row(&message, &columns)
        }
        Some(MapiObject::DelegateFreeBusyMessage {
            folder_id,
            message_id,
        }) => {
            let Some(message) = snapshot
                .delegate_freebusy_message_for_id(*message_id)
                .filter(|message| message.folder_id == *folder_id)
            else {
                return rop_error_response(
                    0x07,
                    request.input_handle_index().unwrap_or(0),
                    0x8004_010F,
                );
            };
            serialize_delegate_freebusy_row(message, &columns)
        }
        Some(MapiObject::RecoverableItem { folder_id, item_id }) => {
            let Some(item) = snapshot.recoverable_item_for_id(*folder_id, *item_id) else {
                return rop_error_response(
                    0x07,
                    request.input_handle_index().unwrap_or(0),
                    0x8004_010F,
                );
            };
            serialize_recoverable_item_row(item, &columns)
        }
        Some(MapiObject::PublicFolderItem {
            folder_id, item_id, ..
        }) => {
            if snapshot
                .public_folder_item_for_id(*folder_id, *item_id)
                .is_none()
            {
                return rop_error_response(
                    0x07,
                    request.input_handle_index().unwrap_or(0),
                    0x8004_010F,
                );
            };
            serialize_object_property_row_with_custom(
                object,
                principal,
                mailboxes,
                emails,
                snapshot,
                &columns,
                custom_values,
            )
        }
        Some(MapiObject::Folder {
            folder_id,
            properties,
        }) => serialize_session_folder_row(
            *folder_id, properties, principal, mailboxes, snapshot, &columns,
        ),
        Some(MapiObject::Attachment {
            folder_id,
            message_id,
            attach_num,
        }) => {
            if snapshot
                .attachment_for_message(*folder_id, *message_id, *attach_num)
                .is_none()
            {
                return rop_error_response(
                    0x07,
                    request.input_handle_index().unwrap_or(0),
                    0x8004_010F,
                );
            }
            serialize_object_property_row_with_custom(
                object,
                principal,
                mailboxes,
                emails,
                snapshot,
                &columns,
                custom_values,
            )
        }
        Some(MapiObject::PendingAttachment {
            attach_num,
            properties,
            data,
            ..
        }) => serialize_pending_attachment_row(*attach_num, properties, data, &columns),
        Some(MapiObject::SavedAttachment {
            attach_num,
            file_reference,
            file_name,
            media_type,
            disposition,
            content_id,
            size_octets,
            ..
        }) => serialize_saved_attachment_row(
            *attach_num,
            file_reference,
            file_name,
            media_type,
            disposition.as_deref(),
            content_id.as_deref(),
            *size_octets,
            &columns,
        ),
        _ => {
            let folder_id = object
                .and_then(MapiObject::folder_id)
                .unwrap_or(ROOT_FOLDER_ID);
            folder_row_for_id(folder_id, mailboxes)
                .map(|mailbox| {
                    serialize_folder_row_with_context(
                        mailbox,
                        mailboxes,
                        &columns,
                        principal.account_id,
                    )
                })
                .or_else(|| {
                    snapshot
                        .collaboration_folder_for_id(folder_id)
                        .map(|folder| {
                            serialize_collaboration_folder_row_with_context(
                                folder,
                                &columns,
                                associated_folder_message_count(folder_id, snapshot),
                            )
                        })
                })
                .unwrap_or_else(|| {
                    serialize_special_folder_row(folder_id, mailboxes, &columns, Some(principal))
                })
        }
    };
    log_get_properties_specific_debug(
        request, object, principal, &columns, mailboxes, emails, snapshot,
    );
    if unsupported_tags.is_empty()
        && size_limited_tags.is_empty()
        && !columns
            .iter()
            .any(|tag| get_properties_specific_typed_value_tag(object, *tag).is_some())
    {
        write_standard_property_row(&mut response, &row);
    } else {
        write_flagged_property_row(
            &mut response,
            object,
            principal,
            mailboxes,
            emails,
            snapshot,
            &columns,
            &unsupported_tags,
            &size_limited_tags,
        );
    }
    response
}

fn serialize_object_property_row_with_custom(
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    columns: &[u32],
    custom_values: &HashMap<u32, Vec<u8>>,
) -> Vec<u8> {
    let mut row = Vec::new();
    for tag in columns {
        if let Some(value) = custom_values.get(tag) {
            row.extend_from_slice(value);
        } else {
            row.extend_from_slice(&serialize_object_property(
                object,
                principal,
                mailboxes,
                emails,
                snapshot,
                get_properties_specific_value_tag(object, *tag),
            ));
        }
    }
    row
}

fn unsupported_specific_property_tags(
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    columns: &[u32],
) -> Vec<u32> {
    columns
        .iter()
        .copied()
        .filter(|tag| {
            property_is_unsupported_for_object(object, principal, *tag)
                || fallback_default_specific_property(
                    object, principal, mailboxes, emails, snapshot, *tag,
                )
        })
        .collect()
}

fn size_limited_specific_property_tags(
    request: &RopRequest,
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    columns: &[u32],
    custom_values: &HashMap<u32, Vec<u8>>,
) -> Vec<u32> {
    let size_limit = request_property_size_limit(request);
    if size_limit == 0 {
        return Vec::new();
    }
    columns
        .iter()
        .copied()
        .filter(|tag| {
            let value_len = custom_values.get(tag).map(Vec::len).unwrap_or_else(|| {
                serialize_object_property(
                    object,
                    principal,
                    mailboxes,
                    emails,
                    snapshot,
                    get_properties_specific_value_tag(object, *tag),
                )
                .len()
            });
            value_len > size_limit
        })
        .collect()
}

fn request_property_size_limit(request: &RopRequest) -> usize {
    request
        .payload
        .get(..2)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u16::from_le_bytes)
        .map(usize::from)
        .unwrap_or(0)
}

fn fallback_default_specific_property(
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    tag: u32,
) -> bool {
    if !matches!(
        object,
        Some(MapiObject::Logon | MapiObject::PublicFolderLogon)
            | Some(MapiObject::Folder { .. })
            | Some(MapiObject::AssociatedConfig { .. } | MapiObject::CommonViewNamedView { .. })
            | None
    ) {
        return false;
    }
    let tag = get_properties_specific_value_tag(object, tag);
    let encoded = serialize_object_property(object, principal, mailboxes, emails, snapshot, tag);
    let mut default_value = Vec::new();
    write_property_default(&mut default_value, tag);
    encoded == default_value && !modeled_zero_or_default_property(object, tag)
}

fn flagged_property_error_code(
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    tag: u32,
) -> u32 {
    if property_is_unsupported_for_object(object, principal, tag) {
        0x8004_0102
    } else if fallback_default_specific_property(
        object, principal, mailboxes, emails, snapshot, tag,
    ) {
        ROP_ERROR_NOT_FOUND
    } else if property_tag_debug_name(tag) == "unknown" {
        0x8004_0102
    } else {
        0x8004_0102
    }
}

fn write_flagged_property_row(
    response: &mut Vec<u8>,
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    columns: &[u32],
    unsupported_tags: &[u32],
    size_limited_tags: &[u32],
) {
    response.push(1);
    for tag in columns {
        if size_limited_tags.contains(tag) {
            if let Some((_value_tag, property_type)) =
                get_properties_specific_typed_value_tag(object, *tag)
            {
                write_u16(response, property_type);
            }
            write_flagged_property_error(response, 0x8007_000E);
        } else if unsupported_tags.contains(tag) {
            write_flagged_property_error(
                response,
                flagged_property_error_code(object, principal, mailboxes, emails, snapshot, *tag),
            );
        } else if let Some((value_tag, property_type)) =
            get_properties_specific_typed_value_tag(object, *tag)
        {
            write_u16(response, property_type);
            response.push(0);
            response.extend_from_slice(&serialize_object_property(
                object, principal, mailboxes, emails, snapshot, value_tag,
            ));
        } else {
            response.push(0);
            response.extend_from_slice(&serialize_object_property(
                object, principal, mailboxes, emails, snapshot, *tag,
            ));
        }
    }
}

fn write_flagged_property_error(response: &mut Vec<u8>, error_code: u32) {
    response.push(0x0A);
    write_u32(response, error_code);
}

fn log_get_properties_specific_debug(
    request: &RopRequest,
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    columns: &[u32],
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) {
    let mut defaulted_tags = Vec::new();
    let mut intentional_default_tags = Vec::new();
    let mut fallback_default_tags = Vec::new();
    for tag in columns {
        if property_is_unsupported_for_object(object, principal, *tag) {
            continue;
        }
        let value = serialize_object_property(object, principal, mailboxes, emails, snapshot, *tag);
        let mut default_value = Vec::new();
        write_property_default(&mut default_value, *tag);
        if value == default_value {
            defaulted_tags.push(*tag);
            if modeled_zero_or_default_property(object, *tag) {
                intentional_default_tags.push(*tag);
            } else {
                fallback_default_tags.push(*tag);
            }
        }
    }
    let flagged_error_tags =
        unsupported_specific_property_tags(object, principal, mailboxes, emails, snapshot, columns);
    let (object_kind, folder_id, item_id) = mapi_object_debug_fields(object);
    let default_folder_mappings = default_folder_property_mappings_for_debug(columns);
    let returned_property_value_shapes = format_property_value_shapes_for_debug(
        object,
        principal,
        columns,
        mailboxes,
        emails,
        snapshot,
        &flagged_error_tags,
    );
    let outlook_bootstrap_getprops = is_outlook_logon_bootstrap_getprops(object, columns);
    let outlook_bootstrap_property_details = if outlook_bootstrap_getprops {
        format_outlook_logon_bootstrap_property_details(principal, columns)
    } else {
        String::new()
    };
    let outlook_bootstrap_row_shape = if outlook_bootstrap_getprops {
        outlook_logon_bootstrap_row_shape(principal, columns)
    } else {
        OutlookLogonBootstrapRowShape::default()
    };
    let ipm_configuration_getprops_contract = format_ipm_configuration_getprops_contract(
        object,
        columns,
        snapshot,
        &fallback_default_tags,
    );
    let folder_type_getprops_contract =
        format_folder_type_getprops_contract(object, principal, columns, mailboxes, snapshot);
    let message_body_getprops_contract =
        format_message_body_getprops_contract(object, columns, mailboxes, emails, snapshot);
    let default_view_entry_id_decoding = format_default_view_entry_id_decoding(
        object, principal, columns, mailboxes, emails, snapshot,
    );
    let common_view_descriptor_getprops_contract =
        format_common_view_descriptor_getprops_contract(object, principal, columns, snapshot);
    let message = "rca debug mapi get properties specific";
    tracing::debug!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = "0x07",
        input_handle_index = request.input_handle_index().unwrap_or(0),
        response_handle_index = request.response_handle_index(),
        object_kind = object_kind,
        folder_id = %folder_id,
        item_id = %item_id,
        requested_property_tag_count = columns.len(),
        requested_property_tags = %format_property_tags_for_debug(columns),
        requested_property_names = %format_property_names_for_debug(columns),
        returned_property_tag_count = columns.len().saturating_sub(flagged_error_tags.len()),
        returned_property_tags = %format_returned_property_tags_for_debug(columns, &flagged_error_tags),
        zero_or_default_property_tag_count = defaulted_tags.len(),
        zero_or_default_property_tags = %format_property_tags_for_debug(&defaulted_tags),
        intentional_zero_or_default_property_tag_count = intentional_default_tags.len(),
        intentional_zero_or_default_property_tags = %format_property_tags_for_debug(&intentional_default_tags),
        fallback_default_property_tag_count = fallback_default_tags.len(),
        fallback_default_property_tags = %format_property_tags_for_debug(&fallback_default_tags),
        unsupported_property_tag_count = flagged_error_tags.len(),
        unsupported_property_tags = %format_property_tags_for_debug(&flagged_error_tags),
        default_ipm_folder_mapping_count = default_folder_mappings.len(),
        default_ipm_folder_mappings = %default_folder_mappings.join(","),
        response_property_row_kind = %property_row_kind_for_debug(
            object,
            principal,
            mailboxes,
            emails,
            snapshot,
            columns,
        ),
        unsupported_property_errors = %format_property_errors_for_debug(
            object,
            principal,
            mailboxes,
            emails,
            snapshot,
            &flagged_error_tags
        ),
        returned_property_value_shapes = %returned_property_value_shapes,
        ipm_configuration_getprops_contract = %ipm_configuration_getprops_contract,
        folder_type_getprops_contract = %folder_type_getprops_contract,
        message_body_getprops_contract = %message_body_getprops_contract,
        default_view_entry_id_decoding = %default_view_entry_id_decoding,
        common_view_descriptor_getprops_contract = %common_view_descriptor_getprops_contract,
        outlook_bootstrap_getprops = outlook_bootstrap_getprops,
        outlook_bootstrap_estimated_rop_payload_bytes =
            outlook_bootstrap_row_shape.estimated_rop_payload_bytes,
        outlook_bootstrap_property_row_bytes = outlook_bootstrap_row_shape.property_row_bytes,
        outlook_bootstrap_icon_row_bytes = outlook_bootstrap_row_shape.icon_row_bytes,
        outlook_bootstrap_non_icon_row_bytes = outlook_bootstrap_row_shape.non_icon_row_bytes,
        outlook_bootstrap_property_details = %outlook_bootstrap_property_details,
        message = message,
    );
    log_common_view_descriptor_getprops_summary(principal, request, object, columns, snapshot);
    log_calendar_default_folder_lookup_debug(
        object,
        principal,
        columns,
        mailboxes,
        emails,
        snapshot,
        &flagged_error_tags,
    );
}

fn log_common_view_descriptor_getprops_summary(
    principal: &AccountPrincipal,
    request: &RopRequest,
    object: Option<&MapiObject>,
    columns: &[u32],
    snapshot: &MapiMailStoreSnapshot,
) {
    let Some(MapiObject::CommonViewNamedView { folder_id, view_id }) = object else {
        return;
    };
    if !common_view_descriptor_property_requested(columns) {
        return;
    }
    let Some(message) = snapshot
        .common_view_named_view_message_for_id(*view_id)
        .or_else(|| snapshot.default_folder_named_view_message(*folder_id, *view_id))
    else {
        tracing::warn!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            mailbox = %principal.email,
            request_type = "Execute",
            request_rop_id = "0x07",
            input_handle_index = request.input_handle_index().unwrap_or(0),
            folder_id = %format!("0x{folder_id:016x}"),
            view_message_id = %format!("0x{view_id:016x}"),
            requested_property_tags = %format_property_tags_for_debug(columns),
            ms_oxcfg_reference = "MS-OXOCFG 2.2.6, 2.2.6.1, 2.2.6.3",
            message = "rca debug outlook view descriptor getprops missing view message",
        );
        return;
    };

    let definition = outlook_folder_view_definition(message.folder_id, &message.name);
    let descriptor = view_descriptor_binary(&definition);
    let descriptor_strings = view_descriptor_strings(&definition);
    let descriptor_string_bytes = utf16le_bytes(&descriptor_strings);
    let descriptor_columns = view_descriptor_debug_property_tags(&descriptor);
    let requested_required = format_requested_view_descriptor_contract(columns);
    let response_values =
        format_common_view_descriptor_response_values(principal.account_id, &message, columns);
    let descriptor_strings_terminators = descriptor_strings
        .chars()
        .filter(|value| *value == '\n')
        .count();

    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = "0x07",
        input_handle_index = request.input_handle_index().unwrap_or(0),
        response_handle_index = request.response_handle_index(),
        folder_id = %format!("0x{folder_id:016x}"),
        view_message_id = %format!("0x{view_id:016x}"),
        view_name = %message.name,
        view_message_class = "IPM.Microsoft.FolderDesign.NamedView",
        requested_property_tags = %format_property_tags_for_debug(columns),
        requested_view_descriptor_contract = %requested_required,
        requested_view_descriptor_response_values = %response_values,
        ms_oxcfg_reference = "MS-OXOCFG 2.2.6, 2.2.6.1, 2.2.6.3",
        descriptor_version = 8u32,
        descriptor_name_present = !message.name.is_empty(),
        descriptor_binary_bytes = descriptor.len(),
        descriptor_binary_sha256_16 = %sha256_hex_prefix(&descriptor, 16),
        descriptor_binary_preview = %hex_preview_for_debug(&descriptor, 96),
        descriptor_column_count = descriptor_columns.len(),
        descriptor_column_tags = %format_property_tags_for_debug(&descriptor_columns),
        descriptor_strings_utf16_bytes = descriptor_string_bytes.len(),
        descriptor_strings_sha256_16 = %sha256_hex_prefix(&descriptor_string_bytes, 16),
        descriptor_strings_utf16_preview = %hex_preview_for_debug(&descriptor_string_bytes, 96),
        descriptor_strings_terminators,
        descriptor_strings_starts_with_terminator = descriptor_strings.starts_with('\n'),
        descriptor_strings_ends_with_terminator = descriptor_strings.ends_with('\n'),
        message = "rca debug outlook view descriptor getprops",
    );
}

fn common_view_descriptor_property_requested(columns: &[u32]) -> bool {
    columns.iter().any(|tag| {
        matches!(
            canonical_property_storage_tag(*tag),
            PID_TAG_VIEW_DESCRIPTOR_BINARY
                | OUTLOOK_COMMON_VIEW_DESCRIPTOR_BINARY_6835
                | OUTLOOK_COMMON_VIEW_DESCRIPTOR_STRINGS_683C
                | PID_TAG_VIEW_DESCRIPTOR_STRINGS_W
                | PID_TAG_VIEW_DESCRIPTOR_NAME_W
                | PID_TAG_VIEW_DESCRIPTOR_VERSION
                | PID_TAG_VIEW_DESCRIPTOR_VERSION_CANONICAL
                | OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B
        )
    })
}

fn format_requested_view_descriptor_contract(columns: &[u32]) -> String {
    let mut parts = Vec::new();
    for (name, tags) in [
        (
            "version",
            &[
                PID_TAG_VIEW_DESCRIPTOR_VERSION,
                PID_TAG_VIEW_DESCRIPTOR_VERSION_CANONICAL,
            ][..],
        ),
        ("name", &[PID_TAG_VIEW_DESCRIPTOR_NAME_W][..]),
        (
            "binary",
            &[
                PID_TAG_VIEW_DESCRIPTOR_BINARY,
                OUTLOOK_COMMON_VIEW_DESCRIPTOR_BINARY_6835,
            ][..],
        ),
        (
            "strings",
            &[
                PID_TAG_VIEW_DESCRIPTOR_STRINGS_W,
                OUTLOOK_COMMON_VIEW_DESCRIPTOR_STRINGS_683C,
            ][..],
        ),
    ] {
        parts.push(format!(
            "{name}={}",
            columns.iter().any(|column| {
                let storage_tag = canonical_property_storage_tag(*column);
                tags.iter().any(|tag| storage_tag == *tag)
            })
        ));
    }
    parts.join(";")
}

fn utf16le_bytes(value: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(value.encode_utf16().count() * 2);
    for unit in value.encode_utf16() {
        bytes.extend_from_slice(&unit.to_le_bytes());
    }
    bytes
}

fn sha256_hex_prefix(bytes: &[u8], hex_chars: usize) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let hex = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    hex.chars().take(hex_chars).collect()
}

fn format_common_view_descriptor_getprops_contract(
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    columns: &[u32],
    snapshot: &MapiMailStoreSnapshot,
) -> String {
    let Some(MapiObject::CommonViewNamedView { folder_id, view_id }) = object else {
        return String::new();
    };
    let descriptor_requested = columns.iter().any(|tag| {
        matches!(
            canonical_property_storage_tag(*tag),
            PID_TAG_VIEW_DESCRIPTOR_BINARY
                | OUTLOOK_COMMON_VIEW_DESCRIPTOR_BINARY_6835
                | OUTLOOK_COMMON_VIEW_DESCRIPTOR_STRINGS_683C
                | PID_TAG_VIEW_DESCRIPTOR_STRINGS_W
                | OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B
        )
    });
    if !descriptor_requested {
        return String::new();
    }
    let message = snapshot
        .common_view_named_view_message_for_id(*view_id)
        .or_else(|| snapshot.default_folder_named_view_message(*folder_id, *view_id));
    let Some(message) = message else {
        return format!(
            "found=false;folder_id=0x{folder_id:016x};view_id=0x{view_id:016x};requested_descriptor_tags={}",
            format_property_tags_for_debug(columns)
        );
    };
    let definition = outlook_folder_view_definition(message.folder_id, &message.name);
    let descriptor = view_descriptor_binary(&definition);
    let descriptor_columns = view_descriptor_debug_property_tags(&descriptor);
    let descriptor_strings = view_descriptor_strings(&definition);
    let response_values =
        format_common_view_descriptor_response_values(principal.account_id, &message, columns);
    let target = default_view_message_entry_id_target(
        &crate::mapi::identity::message_entry_id_from_object_ids(
            principal.account_id,
            *folder_id,
            *view_id,
        )
        .unwrap_or_default(),
    )
    .map(|(target_folder_id, target_message_id)| {
        format!("folder_id=0x{target_folder_id:016x};message_id=0x{target_message_id:016x}")
    })
    .unwrap_or_else(|| "decode=not_message_entry_id".to_string());

    format!(
        "found=true;folder_id=0x{folder_id:016x};view_id=0x{view_id:016x};view_name={};\
         requested_descriptor_tags={};descriptor_bytes={};descriptor_strings_utf16_bytes={};\
         descriptor_column_count={};descriptor_column_tags={};response_values={};\
         descriptor_entry_id_target={target}",
        message.name,
        format_property_tags_for_debug(columns),
        descriptor.len(),
        descriptor_strings.encode_utf16().count() * 2,
        descriptor_columns.len(),
        format_property_tags_for_debug(&descriptor_columns),
        response_values
    )
}

fn format_common_view_descriptor_response_values(
    account_id: uuid::Uuid,
    message: &crate::mapi_store::MapiCommonViewNamedViewMessage,
    columns: &[u32],
) -> String {
    columns
        .iter()
        .filter_map(|tag| {
            if !common_view_descriptor_property_requested(&[*tag]) {
                return None;
            }
            let storage_tag = canonical_property_storage_tag(*tag);
            let value = common_view_named_view_property_value(message, account_id, storage_tag)?;
            Some(format!(
                "{tag:#010x}:{}:{}",
                property_tag_debug_name(storage_tag),
                view_descriptor_value_shape_for_debug(&value)
            ))
        })
        .collect::<Vec<_>>()
        .join("|")
}

fn view_descriptor_value_shape_for_debug(value: &MapiValue) -> String {
    match value {
        MapiValue::Binary(bytes) => format!(
            "binary_bytes={};sha256_16={};preview={}",
            bytes.len(),
            sha256_hex_prefix(bytes, 16),
            hex_preview_for_debug(bytes, 64)
        ),
        MapiValue::String(value) => {
            let bytes = utf16le_bytes(value);
            format!(
                "string_chars={};utf16_bytes={};sha256_16={};preview={}",
                value.chars().count(),
                bytes.len(),
                sha256_hex_prefix(&bytes, 16),
                text_preview_for_debug(value, 48)
            )
        }
        MapiValue::U32(value) => format!("u32={value}"),
        value => mapi_value_shape_for_debug(value),
    }
}

fn view_descriptor_debug_property_tags(descriptor: &[u8]) -> Vec<u32> {
    view_descriptor_all_property_tags(descriptor)
}

fn format_default_view_entry_id_decoding(
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    columns: &[u32],
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> String {
    columns
        .iter()
        .filter(|tag| canonical_property_storage_tag(**tag) == PID_TAG_DEFAULT_VIEW_ENTRY_ID)
        .map(|tag| {
            let encoded =
                serialize_object_property(object, principal, mailboxes, emails, snapshot, *tag);
            let mut cursor = Cursor::new(&encoded);
            match parse_mapi_property_value(&mut cursor, *tag) {
                Ok(MapiValue::Binary(entry_id)) => {
                    match default_view_message_entry_id_target(&entry_id) {
                        Some((folder_id, message_id)) => format!(
                            "{tag:#010x}:bytes={}:folder_id={folder_id:#018x};message_id={message_id:#018x}",
                            entry_id.len()
                        ),
                        None => format!(
                            "{tag:#010x}:bytes={}:decode=not_message_entry_id",
                            entry_id.len()
                        ),
                    }
                }
                Ok(value) => format!("{tag:#010x}:unexpected_value={value:?}"),
                Err(error) => format!("{tag:#010x}:decode_error={error}"),
            }
        })
        .collect::<Vec<_>>()
        .join("|")
}

fn default_view_message_entry_id_target(entry_id: &[u8]) -> Option<(u64, u64)> {
    if entry_id.len() != 70
        || entry_id[0..4] != [0, 0, 0, 0]
        || entry_id[20..22] != 0x0007u16.to_le_bytes()
        || entry_id[44..46] != [0, 0]
        || entry_id[68..70] != [0, 0]
    {
        return None;
    }
    let folder_counter = crate::mapi::identity::global_counter_from_globcnt(&entry_id[38..44])?;
    let message_counter = crate::mapi::identity::global_counter_from_globcnt(&entry_id[62..68])?;
    Some((
        crate::mapi::identity::mapi_store_id(folder_counter),
        crate::mapi::identity::mapi_store_id(message_counter),
    ))
}

fn format_message_body_getprops_contract(
    object: Option<&MapiObject>,
    columns: &[u32],
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> String {
    if !columns.iter().any(|tag| is_message_body_debug_tag(*tag)) {
        return String::new();
    }
    let Some(MapiObject::Message {
        folder_id,
        message_id,
        saved_email,
        ..
    }) = object
    else {
        return String::new();
    };

    let (source, email) =
        if let Some(email) = message_for_id(*folder_id, *message_id, mailboxes, emails) {
            ("mailbox", Some(email))
        } else {
            (
                "search_folder",
                search_folder_message_for_id(snapshot, *folder_id, *message_id)
                    .map(|message| &message.email),
            )
        };
    let saved_email = saved_email.as_ref().map(|saved| &saved.email);
    let (source, email) = match email.or(saved_email) {
        Some(email) if saved_email.is_some_and(|saved| saved.id == email.id) => {
            ("saved_handle", Some(email))
        }
        Some(email) => (source, Some(email)),
        None => (source, None),
    };
    let Some(email) = email else {
        return format!(
            "message_found=false;folder_id={folder_id:#018x};message_id={message_id:#018x};requested_body_tags={}",
            format_property_tags_for_debug(
                &columns
                    .iter()
                    .copied()
                    .filter(|tag| is_message_body_debug_tag(*tag))
                    .collect::<Vec<_>>()
            )
        );
    };

    let body_text_chars = email.body_text.chars().count();
    let body_html_bytes = email
        .body_html_sanitized
        .as_deref()
        .map(str::len)
        .unwrap_or_default();
    format!(
        "message_found=true;source={source};folder_id={folder_id:#018x};message_id={message_id:#018x};subject_chars={};body_text_chars={body_text_chars};body_text_empty={};body_html_bytes={body_html_bytes};body_html_empty={};native_body={};has_attachments={};size_octets={};requested_body_tags={}",
        email.subject.chars().count(),
        email.body_text.trim().is_empty(),
        email.body_html_sanitized
            .as_deref()
            .map(str::trim)
            .unwrap_or("")
            .is_empty(),
        native_body_format(email),
        email.has_attachments,
        email.size_octets,
        format_property_tags_for_debug(
            &columns
                .iter()
                .copied()
                .filter(|tag| is_message_body_debug_tag(*tag))
                .collect::<Vec<_>>()
        )
    )
}

fn is_message_body_debug_tag(tag: u32) -> bool {
    matches!(
        canonical_property_storage_tag(tag),
        PID_TAG_BODY_STRING8
            | PID_TAG_BODY_W
            | PID_TAG_RTF_COMPRESSED
            | PID_TAG_BODY_HTML_W
            | PID_TAG_HTML_BINARY
            | PID_TAG_NATIVE_BODY
            | PID_TAG_RTF_IN_SYNC
    )
}

fn format_folder_type_getprops_contract(
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    columns: &[u32],
    mailboxes: &[JmapMailbox],
    snapshot: &MapiMailStoreSnapshot,
) -> String {
    if !columns
        .iter()
        .any(|tag| canonical_property_storage_tag(*tag) == PID_TAG_FOLDER_TYPE)
    {
        return String::new();
    }
    let Some(MapiObject::Folder {
        folder_id,
        properties,
    }) = object
    else {
        return String::new();
    };

    let mailbox = folder_row_for_id(*folder_id, mailboxes);
    let collaboration_folder = snapshot.collaboration_folder_for_id(*folder_id);
    let public_folder = snapshot.public_folder_for_id(*folder_id);
    let search_folder_found = snapshot
        .search_folder_definition_for_folder_id(*folder_id)
        .is_some();
    let advertised_special_folder = is_advertised_special_folder(*folder_id);

    let handle_value = properties
        .get(&PID_TAG_FOLDER_TYPE)
        .cloned()
        .and_then(MapiValue::into_u32);
    let (property_source, returned_value) = if search_folder_found {
        ("search_folder_definition", Some(FOLDER_SEARCH))
    } else if handle_value.is_some() {
        ("opened_handle", handle_value)
    } else if let Some(mailbox) = mailbox {
        (
            "mailbox",
            mailbox_property_value_with_context_for_account(
                mailbox,
                mailboxes,
                PID_TAG_FOLDER_TYPE,
                principal.account_id,
            )
            .and_then(MapiValue::into_u32),
        )
    } else if let Some(folder) = collaboration_folder {
        (
            "collaboration_folder",
            collaboration_folder_property_value(folder, PID_TAG_FOLDER_TYPE)
                .and_then(MapiValue::into_u32),
        )
    } else if let Some(folder) = public_folder {
        (
            "public_folder",
            public_folder_property_value(folder, PID_TAG_FOLDER_TYPE).and_then(MapiValue::into_u32),
        )
    } else {
        (
            "special_folder_fallback",
            special_folder_property_value(*folder_id, PID_TAG_FOLDER_TYPE, principal.account_id)
                .and_then(MapiValue::into_u32),
        )
    };
    let (expected_kind, expected_value) =
        expected_folder_type_for_debug(*folder_id, mailbox, search_folder_found);

    let mut issues = Vec::new();
    if returned_value.is_none() {
        issues.push("missing_folder_type");
    }
    if returned_value
        .map(|value| !matches!(value, FOLDER_ROOT | FOLDER_GENERIC | FOLDER_SEARCH))
        .unwrap_or(false)
    {
        issues.push("invalid_folder_type_value");
    }
    if let (Some(returned), Some(expected)) = (returned_value, expected_value) {
        if returned != expected {
            issues.push("folder_type_mismatch");
        }
    }
    if *folder_id == INBOX_FOLDER_ID && mailbox.is_none() {
        issues.push("inbox_without_loaded_mailbox");
    }
    if *folder_id == INBOX_FOLDER_ID && property_source == "special_folder_fallback" {
        issues.push("inbox_answered_from_special_fallback");
    }
    if property_source == "special_folder_fallback" && !advertised_special_folder {
        issues.push("non_advertised_special_fallback");
    }

    format!(
        "folder_id=0x{folder_id:016x};mailbox_folder_found={};collaboration_folder_found={};public_folder_found={};search_folder_definition_found={};advertised_special_folder={};property_source={property_source};returned_value={};returned_kind={};expected_value={};expected_kind={expected_kind};issues={}",
        mailbox.is_some(),
        collaboration_folder.is_some(),
        public_folder.is_some(),
        search_folder_found,
        advertised_special_folder,
        returned_value
            .map(|value| value.to_string())
            .unwrap_or_else(|| "missing".to_string()),
        returned_value
            .map(folder_type_kind_for_debug)
            .unwrap_or("missing"),
        expected_value
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unknown".to_string()),
        issues.join("|")
    )
}

fn expected_folder_type_for_debug(
    folder_id: u64,
    mailbox: Option<&JmapMailbox>,
    search_folder_found: bool,
) -> (&'static str, Option<u32>) {
    if matches!(folder_id, ROOT_FOLDER_ID | PUBLIC_FOLDERS_ROOT_FOLDER_ID) {
        return ("root", Some(FOLDER_ROOT));
    }
    if search_folder_found
        || advertised_special_search_folder_for_debug(folder_id)
        || mailbox
            .map(|mailbox| {
                mailbox.role == "__mapi_search" || mailbox.role.starts_with("__mapi_search_folder_")
            })
            .unwrap_or(false)
    {
        return ("search", Some(FOLDER_SEARCH));
    }
    if mailbox.is_some() || is_advertised_special_folder(folder_id) {
        return ("generic", Some(FOLDER_GENERIC));
    }
    ("unknown", None)
}

fn advertised_special_search_folder_for_debug(folder_id: u64) -> bool {
    matches!(
        folder_id,
        SEARCH_FOLDER_ID
            | CONTACTS_SEARCH_FOLDER_ID
            | REMINDERS_FOLDER_ID
            | TRACKED_MAIL_PROCESSING_FOLDER_ID
            | TODO_SEARCH_FOLDER_ID
    )
}

fn folder_type_kind_for_debug(value: u32) -> &'static str {
    match value {
        FOLDER_ROOT => "root",
        FOLDER_GENERIC => "generic",
        FOLDER_SEARCH => "search",
        _ => "invalid",
    }
}

fn format_ipm_configuration_getprops_contract(
    object: Option<&MapiObject>,
    columns: &[u32],
    snapshot: &MapiMailStoreSnapshot,
    fallback_tags: &[u32],
) -> String {
    let Some(MapiObject::AssociatedConfig {
        folder_id,
        config_id,
        saved_message,
    }) = object
    else {
        return String::new();
    };
    let Some(message) = snapshot
        .associated_config_message_for_id(*config_id)
        .or_else(|| saved_message.clone())
        .filter(|message| message.folder_id == *folder_id)
    else {
        return format!("found=false;folder_id=0x{folder_id:016x};config_id=0x{config_id:016x}");
    };
    if !message.message_class.starts_with("IPM.Configuration.") {
        return String::new();
    }
    let datatypes = associated_config_property_value(&message, PID_TAG_ROAMING_DATATYPES)
        .and_then(|value| value.into_u32());
    let requested_stream_tags = columns
        .iter()
        .copied()
        .filter(|tag| {
            matches!(
                *tag,
                PID_TAG_ROAMING_DICTIONARY | PID_TAG_ROAMING_XML_STREAM
            )
        })
        .collect::<Vec<_>>();
    let missing_requested_streams = requested_stream_tags
        .iter()
        .copied()
        .filter(|tag| associated_config_property_value(&message, *tag).is_none())
        .collect::<Vec<_>>();
    let undocumented_0e0b = format_associated_config_0e0b_debug(columns, &message, fallback_tags);
    format!(
        "found=true;folder_id=0x{folder_id:016x};config_id=0x{config_id:016x};class={};datatypes={};has_dictionary={};has_xml={};requested_streams={};missing_requested_streams={};fallback_tags={};undocumented_0e0b={}",
        message.message_class,
        datatypes
            .map(|value| format!("0x{value:08x}"))
            .unwrap_or_else(|| "missing".to_string()),
        associated_config_property_value(&message, PID_TAG_ROAMING_DICTIONARY).is_some(),
        associated_config_property_value(&message, PID_TAG_ROAMING_XML_STREAM).is_some(),
        format_property_tags_for_debug(&requested_stream_tags),
        format_property_tags_for_debug(&missing_requested_streams),
        format_property_tags_for_debug(fallback_tags),
        undocumented_0e0b
    )
}

fn format_associated_config_0e0b_debug(
    columns: &[u32],
    message: &crate::mapi_store::MapiAssociatedConfigMessage,
    fallback_tags: &[u32],
) -> String {
    if !columns.contains(&OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B) {
        return "requested=false".to_string();
    }
    let properties = mapi_properties_from_json(&message.properties_json);
    let mut property_json_tags = properties.keys().copied().collect::<Vec<_>>();
    property_json_tags.sort_unstable();
    let stored_value = properties.get(&OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B);
    let semantic_value =
        associated_config_property_value(message, OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B);
    let semantic_shape = semantic_value
        .as_ref()
        .map(mapi_value_shape_for_debug)
        .unwrap_or_else(|| "missing".to_string());
    format!(
        "requested=true;public_ms_oxprops_name=unmapped;stored={};stored_shape={};semantic_shape={};fallback_default={};property_json_tags={}",
        stored_value.is_some(),
        stored_value
            .map(mapi_value_shape_for_debug)
            .unwrap_or_else(|| "missing".to_string()),
        semantic_shape,
        fallback_tags.contains(&OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B),
        format_property_tags_for_debug(&property_json_tags)
    )
}

fn log_calendar_default_folder_lookup_debug(
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    columns: &[u32],
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    unsupported_tags: &[u32],
) {
    if !columns
        .iter()
        .any(|tag| canonical_property_storage_tag(*tag) == PID_TAG_IPM_APPOINTMENT_ENTRY_ID)
    {
        return;
    }
    let (object_kind, folder_id, _item_id) = mapi_object_debug_fields(object);
    let lookup_location = match object {
        Some(MapiObject::Folder {
            folder_id: INBOX_FOLDER_ID,
            ..
        }) => "inbox_primary",
        Some(MapiObject::Folder {
            folder_id: ROOT_FOLDER_ID,
            ..
        }) => "root_fallback",
        Some(MapiObject::Logon) => "store_logon",
        Some(MapiObject::PublicFolderLogon) => "public_folder_logon",
        Some(MapiObject::Folder { .. }) => "other_folder",
        _ => "other_object",
    };
    let unsupported = unsupported_tags
        .iter()
        .any(|tag| canonical_property_storage_tag(*tag) == PID_TAG_IPM_APPOINTMENT_ENTRY_ID);
    let entry_id = special_folder_identification_property_value(
        principal.account_id,
        PID_TAG_IPM_APPOINTMENT_ENTRY_ID,
    )
    .and_then(|value| match value {
        MapiValue::Binary(bytes) => Some(bytes),
        _ => None,
    })
    .unwrap_or_default();
    let inbox_entry_id = entry_id.clone();
    let root_fallback_entry_id = special_folder_identification_property_value(
        principal.account_id,
        PID_TAG_IPM_APPOINTMENT_ENTRY_ID,
    )
    .and_then(|value| match value {
        MapiValue::Binary(bytes) => Some(bytes),
        _ => None,
    })
    .unwrap_or_default();
    let decoded_folder_id = crate::mapi::identity::object_id_from_folder_entry_id(&entry_id);
    let root_fallback_decoded_folder_id =
        crate::mapi::identity::object_id_from_folder_entry_id(&root_fallback_entry_id);
    let calendar_collection = snapshot.collaboration_folder_for_id(CALENDAR_FOLDER_ID);
    let returned_value_shape = format_property_value_shapes_for_debug(
        object,
        principal,
        &[PID_TAG_IPM_APPOINTMENT_ENTRY_ID],
        mailboxes,
        emails,
        snapshot,
        unsupported_tags,
    );
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = "0x07",
        object_kind,
        folder_id = %folder_id,
        microsoft_documented_lookup_order = "GetReceiveFolder(Inbox), Inbox.GetProps(PR_IPM_APPOINTMENT_ENTRYID), root fallback",
        lookup_location,
        lookup_asked_inbox =
            matches!(object, Some(MapiObject::Folder { folder_id: INBOX_FOLDER_ID, .. })),
        lookup_asked_root =
            matches!(object, Some(MapiObject::Folder { folder_id: ROOT_FOLDER_ID, .. })),
        property_tag = "0x36d00102",
        property_name = "PidTagIpmAppointmentEntryId",
        property_returned = !unsupported,
        entry_id_bytes = entry_id.len(),
        entry_id_preview = %hex_preview_for_debug(&entry_id, 24),
        inbox_entry_id_bytes = inbox_entry_id.len(),
        inbox_entry_id_preview = %hex_preview_for_debug(&inbox_entry_id, 24),
        root_fallback_entry_id_bytes = root_fallback_entry_id.len(),
        root_fallback_entry_id_preview = %hex_preview_for_debug(&root_fallback_entry_id, 24),
        root_fallback_matches_inbox = root_fallback_entry_id == inbox_entry_id,
        decoded_folder_id = %decoded_folder_id
            .map(|folder_id| format!("0x{folder_id:016x}"))
            .unwrap_or_default(),
        root_fallback_decoded_folder_id = %root_fallback_decoded_folder_id
            .map(|folder_id| format!("0x{folder_id:016x}"))
            .unwrap_or_default(),
        decoded_folder_is_calendar = decoded_folder_id == Some(CALENDAR_FOLDER_ID),
        root_fallback_decoded_folder_is_calendar =
            root_fallback_decoded_folder_id == Some(CALENDAR_FOLDER_ID),
        expected_calendar_folder_id = "0x0000000000100001",
        calendar_folder_projected = calendar_collection.is_some(),
        calendar_collection_id =
            calendar_collection.map(|folder| folder.collection.id.as_str()).unwrap_or(""),
        calendar_collection_name =
            calendar_collection.map(|folder| folder.collection.display_name.as_str()).unwrap_or(""),
        returned_property_value_shape = %returned_value_shape,
        message = "rca debug mapi calendar default folder lookup"
    );
}

#[derive(Default)]
struct OutlookLogonBootstrapRowShape {
    estimated_rop_payload_bytes: usize,
    property_row_bytes: usize,
    icon_row_bytes: usize,
    non_icon_row_bytes: usize,
}

fn outlook_logon_bootstrap_row_shape(
    principal: &AccountPrincipal,
    columns: &[u32],
) -> OutlookLogonBootstrapRowShape {
    let mut shape = OutlookLogonBootstrapRowShape::default();
    for tag in columns {
        if logon_property_value(principal, *tag).is_none() {
            continue;
        }
        let value = serialize_logon_row(principal, &[*tag]);
        shape.property_row_bytes += value.len();
        if matches!(
            *tag,
            PID_TAG_SERVER_CONNECTED_ICON | PID_TAG_SERVER_ACCOUNT_ICON
        ) {
            shape.icon_row_bytes += value.len();
        } else {
            shape.non_icon_row_bytes += value.len();
        }
    }
    shape.estimated_rop_payload_bytes = shape.property_row_bytes + 7;
    shape
}

fn is_outlook_logon_bootstrap_getprops(object: Option<&MapiObject>, columns: &[u32]) -> bool {
    const OUTLOOK_BOOTSTRAP_LOGON_PROPS: [u32; 9] = [
        PID_TAG_MAILBOX_OWNER_NAME_W,
        PID_TAG_MAILBOX_OWNER_ENTRY_ID,
        PID_TAG_SERVER_TYPE_DISPLAY_NAME_W,
        PID_TAG_SERVER_CONNECTED_ICON,
        PID_TAG_SERVER_ACCOUNT_ICON,
        PID_TAG_PRIVATE,
        PID_TAG_OUTLOOK_STORE_STATE,
        PID_TAG_USER_GUID,
        PID_TAG_MAX_SUBMIT_MESSAGE_SIZE,
    ];
    const OUTLOOK_BOOTSTRAP_LOGON_EXTENSION_PROPS: [u32; 3] = [
        PID_TAG_RESOURCE_FLAGS,
        PID_TAG_USER_ENTRY_ID,
        PID_TAG_IPM_PUBLIC_FOLDERS_ENTRY_ID,
    ];
    const REQUIRED_OUTLOOK_BOOTSTRAP_LOGON_PROPS: [u32; 8] = [
        PID_TAG_MAILBOX_OWNER_NAME_W,
        PID_TAG_MAILBOX_OWNER_ENTRY_ID,
        PID_TAG_SERVER_TYPE_DISPLAY_NAME_W,
        PID_TAG_SERVER_CONNECTED_ICON,
        PID_TAG_SERVER_ACCOUNT_ICON,
        PID_TAG_PRIVATE,
        PID_TAG_OUTLOOK_STORE_STATE,
        PID_TAG_USER_GUID,
    ];

    matches!(object, Some(MapiObject::Logon))
        && columns.len() >= REQUIRED_OUTLOOK_BOOTSTRAP_LOGON_PROPS.len()
        && columns.len()
            <= OUTLOOK_BOOTSTRAP_LOGON_PROPS.len() + OUTLOOK_BOOTSTRAP_LOGON_EXTENSION_PROPS.len()
        && REQUIRED_OUTLOOK_BOOTSTRAP_LOGON_PROPS
            .iter()
            .all(|expected| columns.contains(expected))
        && columns.iter().all(|tag| {
            OUTLOOK_BOOTSTRAP_LOGON_PROPS.contains(tag)
                || OUTLOOK_BOOTSTRAP_LOGON_EXTENSION_PROPS.contains(tag)
        })
}

fn format_outlook_logon_bootstrap_property_details(
    principal: &AccountPrincipal,
    columns: &[u32],
) -> String {
    columns
        .iter()
        .filter_map(|tag| {
            let value = logon_property_value(principal, *tag)?;
            let detail = match (*tag, value) {
                (PID_TAG_MAILBOX_OWNER_ENTRY_ID, MapiValue::Binary(bytes)) => {
                    format_mailbox_owner_entry_id_details(&bytes)
                }
                (
                    PID_TAG_SERVER_CONNECTED_ICON | PID_TAG_SERVER_ACCOUNT_ICON,
                    MapiValue::Binary(bytes),
                ) => format_ico_header_details(&bytes),
                (PID_TAG_USER_GUID, MapiValue::Binary(bytes)) => {
                    format!(
                        "user_guid_bytes={};user_guid_hex={}",
                        bytes.len(),
                        hex_preview_for_debug(&bytes, bytes.len())
                    )
                }
                (PID_TAG_OUTLOOK_STORE_STATE, MapiValue::U32(value)) => {
                    format!("outlook_store_state={value:#010x}")
                }
                (PID_TAG_PRIVATE, MapiValue::Bool(value)) => format!("private={value}"),
                (PID_TAG_MAX_SUBMIT_MESSAGE_SIZE, MapiValue::U32(value)) => {
                    format!("max_submit_message_size_kb={value}")
                }
                (PID_TAG_MESSAGE_SIZE_EXTENDED, MapiValue::I64(value)) => {
                    format!("message_size_extended_octets={value}")
                }
                (
                    PID_TAG_PROHIBIT_RECEIVE_QUOTA
                    | PID_TAG_PROHIBIT_SEND_QUOTA
                    | PID_TAG_STORAGE_QUOTA_LIMIT,
                    MapiValue::U32(value),
                ) => format!("quota_limit_kb={value}"),
                (
                    PID_TAG_MAILBOX_OWNER_NAME_W | PID_TAG_SERVER_TYPE_DISPLAY_NAME_W,
                    MapiValue::String(value),
                ) => {
                    format!(
                        "string_chars={};string_preview={}",
                        value.chars().count(),
                        text_preview_for_debug(&value, 32)
                    )
                }
                (_, value) => mapi_value_shape_for_debug(&value),
            };
            Some(format!(
                "{tag:#010x}:{}:{detail}",
                property_tag_debug_name(*tag)
            ))
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn format_mailbox_owner_entry_id_details(bytes: &[u8]) -> String {
    if bytes.len() < 28 {
        return format!(
            "permanent_entry_id_len={};parse_error=too_short",
            bytes.len()
        );
    }

    let id_type = bytes[0];
    let reserved_1 = bytes[1];
    let reserved_2 = bytes[2];
    let reserved_3 = bytes[3];
    let provider_uid = &bytes[4..20];
    let reserved_4 = u32::from_le_bytes(bytes[20..24].try_into().unwrap());
    let display_type = u32::from_le_bytes(bytes[24..28].try_into().unwrap());
    let dn_bytes = &bytes[28..];
    let null_terminated = dn_bytes.last().copied() == Some(0);
    let dn_payload = if null_terminated {
        &dn_bytes[..dn_bytes.len().saturating_sub(1)]
    } else {
        dn_bytes
    };
    let distinguished_name = String::from_utf8_lossy(dn_payload);

    format!(
        "permanent_entry_id_len={};id_type={id_type:#04x};r1={reserved_1:#04x};r2={reserved_2:#04x};r3={reserved_3:#04x};provider_uid={};provider_uid_matches_nspi={};r4={reserved_4:#010x};display_type={display_type:#010x};dn_len={};dn_null_terminated={null_terminated};dn_preview={}",
        bytes.len(),
        hex_preview_for_debug(provider_uid, provider_uid.len()),
        provider_uid == NSPI_PERMANENT_ENTRY_ID_PROVIDER_UID,
        dn_payload.len(),
        text_preview_for_debug(&distinguished_name, 96),
    )
}

fn format_ico_header_details(bytes: &[u8]) -> String {
    if bytes.len() < 22 {
        return format!("ico_len={};parse_error=too_short", bytes.len());
    }

    let reserved = u16::from_le_bytes(bytes[0..2].try_into().unwrap());
    let image_type = u16::from_le_bytes(bytes[2..4].try_into().unwrap());
    let image_count = u16::from_le_bytes(bytes[4..6].try_into().unwrap());
    let width = bytes[6];
    let height = bytes[7];
    let color_count = bytes[8];
    let planes = u16::from_le_bytes(bytes[10..12].try_into().unwrap());
    let bit_count = u16::from_le_bytes(bytes[12..14].try_into().unwrap());
    let image_size = u32::from_le_bytes(bytes[14..18].try_into().unwrap());
    let image_offset = u32::from_le_bytes(bytes[18..22].try_into().unwrap());
    let length_matches_directory = image_offset
        .checked_add(image_size)
        .is_some_and(|expected| expected as usize == bytes.len());

    format!(
        "ico_len={};reserved={reserved:#06x};type={image_type:#06x};count={image_count};width={width};height={height};color_count={color_count};planes={planes};bit_count={bit_count};image_size={image_size};image_offset={image_offset};length_matches_directory={length_matches_directory}",
        bytes.len(),
    )
}

fn property_is_unsupported_for_object(
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    tag: u32,
) -> bool {
    if canonical_property_storage_tag(tag) == OUTLOOK_UNDOCUMENTED_FOLDER_BINARY_120C {
        return false;
    }
    let value_tag = get_properties_specific_value_tag(object, tag);
    if MapiPropertyTag::new(value_tag).property_type().is_none() {
        return true;
    }
    if matches!(
        object,
        Some(MapiObject::Note { .. } | MapiObject::PendingNote { .. })
    ) && matches!(
        canonical_property_storage_tag(value_tag),
        PID_LID_NOTE_HEIGHT_TAG | PID_LID_NOTE_WIDTH_TAG | PID_LID_NOTE_X_TAG | PID_LID_NOTE_Y_TAG
    ) {
        return true;
    }
    matches!(object, Some(MapiObject::Logon))
        && logon_property_value(principal, value_tag).is_none()
}

fn get_properties_specific_value_tag(object: Option<&MapiObject>, tag: u32) -> u32 {
    get_properties_specific_typed_value_tag(object, tag)
        .map(|(value_tag, _property_type)| value_tag)
        .unwrap_or(tag)
}

fn get_properties_specific_typed_value_tag(
    object: Option<&MapiObject>,
    tag: u32,
) -> Option<(u32, u16)> {
    let requested_type = MapiPropertyTag::new(tag).property_type_code();
    if requested_type != 0x0000 && requested_type != 0x0001 {
        return None;
    }
    let property_id = tag & 0xFFFF_0000;
    let value_tag = match property_id {
        0x001A_0000 => PID_TAG_MESSAGE_CLASS_W,
        0x0037_0000 => PID_TAG_SUBJECT_W,
        0x0E07_0000 => PID_TAG_MESSAGE_FLAGS,
        0x0E1D_0000 => PID_TAG_NORMALIZED_SUBJECT_W,
        0x1000_0000 => PID_TAG_BODY_W,
        0x1013_0000 => PID_TAG_BODY_HTML_W,
        0x3001_0000 => PID_TAG_DISPLAY_NAME_W,
        _ => get_properties_specific_candidate_tags(object)
            .into_iter()
            .find(|candidate| (*candidate & 0xFFFF_0000) == property_id)?,
    };
    Some((value_tag, (value_tag & 0xFFFF) as u16))
}

fn get_properties_specific_candidate_tags(object: Option<&MapiObject>) -> Vec<u32> {
    match object {
        Some(MapiObject::Logon) => default_store_property_tags(),
        Some(MapiObject::PublicFolderLogon) => vec![PID_TAG_PRIVATE],
        Some(MapiObject::Contact { .. } | MapiObject::PendingContact { .. }) => {
            default_contact_property_tags()
        }
        Some(MapiObject::Event { .. } | MapiObject::PendingEvent { .. }) => {
            default_event_property_tags()
        }
        Some(MapiObject::Task { .. } | MapiObject::PendingTask { .. }) => {
            default_task_property_tags()
        }
        Some(MapiObject::Note { .. } | MapiObject::PendingNote { .. }) => {
            default_note_property_tags()
        }
        Some(MapiObject::JournalEntry { .. } | MapiObject::PendingJournalEntry { .. }) => {
            default_journal_entry_property_tags()
        }
        Some(MapiObject::Attachment { .. })
        | Some(MapiObject::PendingAttachment { .. })
        | Some(MapiObject::SavedAttachment { .. }) => default_attachment_columns(),
        Some(
            MapiObject::Message { .. }
            | MapiObject::AssociatedConfig { .. }
            | MapiObject::PublicFolderItem { .. }
            | MapiObject::PendingAssociatedMessage { .. }
            | MapiObject::PendingMessage { .. },
        ) => default_message_property_tags(),
        Some(
            MapiObject::ConversationAction { .. } | MapiObject::PendingConversationAction { .. },
        ) => default_conversation_action_property_tags(),
        _ => default_folder_property_tags(),
    }
}

fn modeled_zero_or_default_property(object: Option<&MapiObject>, tag: u32) -> bool {
    let storage_tag = canonical_property_storage_tag(tag);
    match object {
        Some(MapiObject::Logon) => matches!(
            tag,
            PID_TAG_PRIVATE
                | PID_TAG_OUTLOOK_STORE_STATE
                | PID_TAG_RESOURCE_FLAGS
                | PID_TAG_PST_PATH_W
                | PID_TAG_SERVER_CONNECTED_ICON
                | PID_TAG_SERVER_ACCOUNT_ICON
        ),
        Some(MapiObject::PublicFolderLogon) => matches!(tag, PID_TAG_PRIVATE),
        Some(MapiObject::AssociatedConfig { .. }) => matches!(
            storage_tag,
            PID_TAG_INSTANCE_NUM
                | PID_TAG_MESSAGE_FLAGS
                | PID_TAG_MESSAGE_STATUS
                | PID_TAG_ACCESS_LEVEL
                | PID_TAG_SENT_MAIL_SVR_EID
                | PID_TAG_ASSOCIATED
                | OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B
        ),
        Some(MapiObject::CommonViewNamedView { .. }) => matches!(
            storage_tag,
            OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B
                | OUTLOOK_COMMON_VIEW_DESCRIPTOR_BINARY_6835
                | PID_TAG_VIEW_DESCRIPTOR_BINARY
        ),
        Some(MapiObject::DelegateFreeBusyMessage { .. }) => matches!(
            storage_tag,
            PID_TAG_VIEW_DESCRIPTOR_VIEW_MODE
                | OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B
                | 0x6842_000B
                | 0x6843_000B
                | 0x6844_101F
                | 0x6845_1102
                | 0x684A_101F
                | 0x684B_000B
                | 0x686B_1003
                | 0x686D_000B
                | 0x686E_000B
                | 0x686F_000B
                | 0x6870_1102
                | 0x6871_1003
                | 0x6872_001F
        ),
        Some(MapiObject::Folder { folder_id, .. }) => {
            is_acl_member_name_property_tag(tag)
                || is_modeled_empty_special_folder_class_property(*folder_id, storage_tag)
                || matches!(
                    storage_tag,
                    PID_TAG_CONTENT_COUNT
                        | PID_TAG_CONTENT_UNREAD_COUNT
                        | PID_TAG_DELETED_COUNT_TOTAL
                        | PID_TAG_SUBFOLDERS
                        | PID_TAG_PARENT_FOLDER_ID
                        | PID_TAG_PARENT_SOURCE_KEY
                        | PID_TAG_FOLDER_TYPE
                        | PID_TAG_ARCHIVE_TAG
                        | PID_TAG_POLICY_TAG
                        | PID_TAG_RETENTION_PERIOD
                        | PID_TAG_RETENTION_FLAGS
                        | PID_TAG_ARCHIVE_PERIOD
                        | PID_TAG_FOLDER_FORM_FLAGS
                        | PID_TAG_FOLDER_WEBVIEWINFO
                        | PID_TAG_FOLDER_XVIEWINFO_E
                        | OUTLOOK_UNDOCUMENTED_FOLDER_BINARY_120C
                        | PID_TAG_FOLDER_VIEWS_ONLY
                        | PID_TAG_DEFAULT_FORM_NAME_W
                        | PID_TAG_DEFAULT_VIEW_ENTRY_ID
                        | PID_TAG_FOLDER_FORM_STORAGE
                        | PID_TAG_FOLDER_VIEWLIST_FLAGS
                )
        }
        None => {
            is_acl_member_name_property_tag(tag)
                || matches!(
                    storage_tag,
                    PID_TAG_CONTENT_COUNT
                        | PID_TAG_CONTENT_UNREAD_COUNT
                        | PID_TAG_DELETED_COUNT_TOTAL
                        | PID_TAG_SUBFOLDERS
                        | PID_TAG_PARENT_FOLDER_ID
                        | PID_TAG_PARENT_SOURCE_KEY
                        | PID_TAG_FOLDER_TYPE
                        | PID_TAG_ARCHIVE_TAG
                        | PID_TAG_POLICY_TAG
                        | PID_TAG_RETENTION_PERIOD
                        | PID_TAG_RETENTION_FLAGS
                        | PID_TAG_ARCHIVE_PERIOD
                        | PID_TAG_FOLDER_FORM_FLAGS
                        | PID_TAG_FOLDER_WEBVIEWINFO
                        | PID_TAG_FOLDER_XVIEWINFO_E
                        | OUTLOOK_UNDOCUMENTED_FOLDER_BINARY_120C
                        | PID_TAG_FOLDER_VIEWS_ONLY
                        | PID_TAG_DEFAULT_FORM_NAME_W
                        | PID_TAG_DEFAULT_VIEW_ENTRY_ID
                        | PID_TAG_FOLDER_FORM_STORAGE
                        | PID_TAG_FOLDER_VIEWLIST_FLAGS
                )
        }
        _ => false,
    }
}

fn is_modeled_empty_special_folder_class_property(folder_id: u64, storage_tag: u32) -> bool {
    matches!(
        folder_id,
        ROOT_FOLDER_ID
            | DEFERRED_ACTION_FOLDER_ID
            | SPOOLER_QUEUE_FOLDER_ID
            | COMMON_VIEWS_FOLDER_ID
            | VIEWS_FOLDER_ID
    ) && matches!(
        storage_tag,
        PID_TAG_CONTAINER_CLASS_W
            | PID_TAG_MESSAGE_CLASS_W
            | PID_TAG_DEFAULT_POST_MESSAGE_CLASS_STRING8
            | PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W
    )
}

fn property_row_kind_for_debug(
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    columns: &[u32],
) -> &'static str {
    if !unsupported_specific_property_tags(object, principal, mailboxes, emails, snapshot, columns)
        .is_empty()
    {
        "flagged"
    } else {
        "standard"
    }
}

fn format_property_errors_for_debug(
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    tags: &[u32],
) -> String {
    tags.iter()
        .map(|tag| {
            format!(
                "{tag:#010x}:{}:{:#010x}",
                property_tag_debug_name(*tag),
                flagged_property_error_code(object, principal, mailboxes, emails, snapshot, *tag)
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn format_property_value_shapes_for_debug(
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    columns: &[u32],
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    unsupported_tags: &[u32],
) -> String {
    columns
        .iter()
        .map(|tag| {
            let name = property_tag_debug_name(*tag);
            if unsupported_tags.contains(tag) {
                return format!("{tag:#010x}:{name}:unsupported");
            }
            let encoded =
                serialize_object_property(object, principal, mailboxes, emails, snapshot, *tag);
            let mut default_value = Vec::new();
            write_property_default(&mut default_value, *tag);
            let default_kind = if encoded == default_value {
                if modeled_zero_or_default_property(object, *tag) {
                    ":default=intentional"
                } else {
                    ":default=fallback"
                }
            } else {
                ""
            };
            let semantic_shape =
                semantic_property_shape_for_debug(object, principal, snapshot, *tag)
                    .map(|shape| format!(":{shape}"))
                    .unwrap_or_default();
            format!(
                "{tag:#010x}:{name}:row_bytes={}{}:row_hex={}{}",
                encoded.len(),
                semantic_shape,
                hex_preview_for_debug(&encoded, 16),
                default_kind
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn semantic_property_shape_for_debug(
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    snapshot: &MapiMailStoreSnapshot,
    tag: u32,
) -> Option<String> {
    match object {
        Some(MapiObject::Logon) => logon_property_value(principal, tag)
            .as_ref()
            .map(mapi_value_shape_for_debug),
        Some(MapiObject::PublicFolderLogon) => {
            (tag == PID_TAG_PRIVATE).then(|| mapi_value_shape_for_debug(&MapiValue::Bool(false)))
        }
        Some(MapiObject::Folder { .. }) => {
            special_folder_identification_property_value(principal.account_id, tag)
                .as_ref()
                .map(mapi_value_shape_for_debug)
        }
        Some(MapiObject::AssociatedConfig {
            folder_id,
            config_id,
            saved_message,
        }) => snapshot
            .associated_config_message_for_id(*config_id)
            .or_else(|| saved_message.clone())
            .filter(|message| message.folder_id == *folder_id)
            .and_then(|message| {
                associated_config_property_value_with_mailbox_guid(
                    &message,
                    principal.account_id,
                    tag,
                )
            })
            .as_ref()
            .map(mapi_value_shape_for_debug),
        _ => None,
    }
}

fn mapi_value_shape_for_debug(value: &MapiValue) -> String {
    match value {
        MapiValue::Bool(value) => format!("bool={value}"),
        MapiValue::I16(value) => format!("i16={value}"),
        MapiValue::I32(value) => format!("i32={value}"),
        MapiValue::I64(value) => format!("i64={value}"),
        MapiValue::F64(value) => format!("f64={}", f64::from_bits(*value)),
        MapiValue::U32(value) => format!("u32={value}"),
        MapiValue::U64(value) => format!("u64={value}"),
        MapiValue::String(value) => format!(
            "string:chars={}:preview={}",
            value.chars().count(),
            text_preview_for_debug(value, 32)
        ),
        MapiValue::Binary(value) => {
            format!(
                "binary:bytes={}:preview={}",
                value.len(),
                hex_preview_for_debug(value, 16)
            )
        }
        MapiValue::Guid(value) => format!("guid={}", hex_preview_for_debug(value, value.len())),
        MapiValue::Error(value) => format!("error={value:#010x}"),
        MapiValue::MultiI16(values) => format!("multi_i16:count={}", values.len()),
        MapiValue::MultiI32(values) => format!("multi_i32:count={}", values.len()),
        MapiValue::MultiI64(values) => format!("multi_i64:count={}", values.len()),
        MapiValue::MultiString(values) => format!("multi_string:count={}", values.len()),
        MapiValue::MultiBinary(values) => format!("multi_binary:count={}", values.len()),
        MapiValue::MultiGuid(values) => format!("multi_guid:count={}", values.len()),
    }
}

fn text_preview_for_debug(value: &str, max_chars: usize) -> String {
    value
        .chars()
        .take(max_chars)
        .map(|ch| match ch {
            ',' | ';' | '\n' | '\r' | '\t' => ' ',
            _ => ch,
        })
        .collect()
}

fn hex_preview_for_debug(bytes: &[u8], max_bytes: usize) -> String {
    let mut preview = bytes
        .iter()
        .take(max_bytes)
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    if bytes.len() > max_bytes {
        preview.push_str("...");
    }
    preview
}

fn mapi_object_debug_fields(object: Option<&MapiObject>) -> (&'static str, String, String) {
    match object {
        Some(MapiObject::Logon) => ("logon", String::new(), String::new()),
        Some(MapiObject::PublicFolderLogon) => {
            ("public_folder_logon", String::new(), String::new())
        }
        Some(MapiObject::Folder { folder_id, .. }) => {
            ("folder", format!("{folder_id:#018x}"), String::new())
        }
        Some(MapiObject::Message {
            folder_id,
            message_id,
            ..
        }) => (
            "message",
            format!("{folder_id:#018x}"),
            format!("{message_id:#018x}"),
        ),
        Some(MapiObject::Contact {
            folder_id,
            contact_id,
        }) => (
            "contact",
            format!("{folder_id:#018x}"),
            format!("{contact_id:#018x}"),
        ),
        Some(MapiObject::Event {
            folder_id,
            event_id,
        }) => (
            "event",
            format!("{folder_id:#018x}"),
            format!("{event_id:#018x}"),
        ),
        Some(MapiObject::Task { folder_id, task_id }) => (
            "task",
            format!("{folder_id:#018x}"),
            format!("{task_id:#018x}"),
        ),
        Some(MapiObject::Note { folder_id, note_id }) => (
            "note",
            format!("{folder_id:#018x}"),
            format!("{note_id:#018x}"),
        ),
        Some(MapiObject::JournalEntry {
            folder_id,
            journal_entry_id,
        }) => (
            "journal_entry",
            format!("{folder_id:#018x}"),
            format!("{journal_entry_id:#018x}"),
        ),
        Some(MapiObject::ConversationAction {
            folder_id,
            conversation_action_id,
        }) => (
            "conversation_action",
            format!("{folder_id:#018x}"),
            format!("{conversation_action_id:#018x}"),
        ),
        Some(MapiObject::NavigationShortcut {
            folder_id,
            shortcut_id,
        }) => (
            "navigation_shortcut",
            format!("{folder_id:#018x}"),
            format!("{shortcut_id:#018x}"),
        ),
        Some(MapiObject::CommonViewNamedView { folder_id, view_id }) => (
            "common_view_named_view",
            format!("{folder_id:#018x}"),
            format!("{view_id:#018x}"),
        ),
        Some(MapiObject::SearchFolderDefinitionMessage {
            folder_id,
            message_id,
        }) => (
            "search_folder_definition_message",
            format!("{folder_id:#018x}"),
            format!("{message_id:#018x}"),
        ),
        Some(MapiObject::AssociatedConfig {
            folder_id,
            config_id,
            ..
        }) => (
            "associated_config",
            format!("{folder_id:#018x}"),
            format!("{config_id:#018x}"),
        ),
        Some(MapiObject::DelegateFreeBusyMessage {
            folder_id,
            message_id,
        }) => (
            "delegate_freebusy_message",
            format!("{folder_id:#018x}"),
            format!("{message_id:#018x}"),
        ),
        Some(MapiObject::RecoverableItem { folder_id, item_id }) => (
            "recoverable_item",
            format!("{folder_id:#018x}"),
            format!("{item_id:#018x}"),
        ),
        Some(MapiObject::PublicFolderItem {
            folder_id, item_id, ..
        }) => (
            "public_folder_item",
            format!("{folder_id:#018x}"),
            format!("{item_id:#018x}"),
        ),
        Some(MapiObject::PendingMessage { folder_id, .. }) => (
            "pending_message",
            format!("{folder_id:#018x}"),
            String::new(),
        ),
        Some(MapiObject::PendingAssociatedMessage { folder_id, .. }) => (
            "pending_associated_message",
            format!("{folder_id:#018x}"),
            String::new(),
        ),
        Some(MapiObject::PendingContact { folder_id, .. }) => (
            "pending_contact",
            format!("{folder_id:#018x}"),
            String::new(),
        ),
        Some(MapiObject::PendingEvent { folder_id, .. }) => {
            ("pending_event", format!("{folder_id:#018x}"), String::new())
        }
        Some(MapiObject::PendingTask { folder_id, .. }) => {
            ("pending_task", format!("{folder_id:#018x}"), String::new())
        }
        Some(MapiObject::PendingNote { folder_id, .. }) => {
            ("pending_note", format!("{folder_id:#018x}"), String::new())
        }
        Some(MapiObject::PendingJournalEntry { folder_id, .. }) => (
            "pending_journal_entry",
            format!("{folder_id:#018x}"),
            String::new(),
        ),
        Some(MapiObject::PendingConversationAction { folder_id, .. }) => (
            "pending_conversation_action",
            format!("{folder_id:#018x}"),
            String::new(),
        ),
        Some(MapiObject::PendingNavigationShortcut { folder_id, .. }) => (
            "pending_navigation_shortcut",
            format!("{folder_id:#018x}"),
            String::new(),
        ),
        Some(MapiObject::HierarchyTable { folder_id, .. }) => (
            "hierarchy_table",
            format!("{folder_id:#018x}"),
            String::new(),
        ),
        Some(MapiObject::ContentsTable { folder_id, .. }) => (
            "contents_table",
            format!("{folder_id:#018x}"),
            String::new(),
        ),
        Some(MapiObject::AttachmentTable {
            folder_id,
            message_id,
            ..
        }) => (
            "attachment_table",
            format!("{folder_id:#018x}"),
            format!("{message_id:#018x}"),
        ),
        Some(MapiObject::PermissionTable { folder_id, .. }) => (
            "permission_table",
            format!("{folder_id:#018x}"),
            String::new(),
        ),
        Some(MapiObject::RuleTable { folder_id, .. }) => {
            ("rule_table", format!("{folder_id:#018x}"), String::new())
        }
        Some(MapiObject::Attachment {
            folder_id,
            message_id,
            attach_num,
        }) => (
            "attachment",
            format!("{folder_id:#018x}"),
            format!("{message_id:#018x}/{}", attach_num),
        ),
        Some(MapiObject::PendingAttachment {
            folder_id,
            message_id,
            attach_num,
            ..
        }) => (
            "pending_attachment",
            format!("{folder_id:#018x}"),
            format!("{message_id:#018x}/{}", attach_num),
        ),
        Some(MapiObject::SavedAttachment {
            folder_id,
            message_id,
            attach_num,
            ..
        }) => (
            "saved_attachment",
            format!("{folder_id:#018x}"),
            format!("{message_id:#018x}/{}", attach_num),
        ),
        Some(MapiObject::AttachmentStream { .. }) => {
            ("attachment_stream", String::new(), String::new())
        }
        Some(MapiObject::NotificationSubscription { .. }) => {
            ("notification_subscription", String::new(), String::new())
        }
        Some(MapiObject::SynchronizationSource {
            folder_id,
            sync_type,
            ..
        }) => (
            "synchronization_source",
            format!("{folder_id:#018x}"),
            format!("{sync_type:#04x}"),
        ),
        Some(MapiObject::SynchronizationCollector {
            folder_id,
            checkpoint_kind,
            ..
        }) => (
            "synchronization_collector",
            format!("{folder_id:#018x}"),
            format!("{checkpoint_kind:?}"),
        ),
        Some(MapiObject::FastTransferDestination {
            folder_id,
            target_handle,
            ..
        }) => (
            "fast_transfer_destination",
            format!("{folder_id:#018x}"),
            format!("target_handle={target_handle}"),
        ),
        None => ("unknown", String::new(), String::new()),
    }
}

fn format_returned_property_tags_for_debug(columns: &[u32], unsupported_tags: &[u32]) -> String {
    let returned = columns
        .iter()
        .copied()
        .filter(|tag| !unsupported_tags.contains(tag))
        .collect::<Vec<_>>();
    format_property_tags_for_debug(&returned)
}

fn format_property_tags_for_debug(tags: &[u32]) -> String {
    tags.iter()
        .map(|tag| format!("{tag:#010x}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn format_property_names_for_debug(tags: &[u32]) -> String {
    tags.iter()
        .map(|tag| property_tag_debug_name(*tag))
        .collect::<Vec<_>>()
        .join(",")
}

fn property_tag_debug_name(tag: u32) -> &'static str {
    match tag {
        PID_TAG_DISPLAY_NAME_W => "PidTagDisplayName",
        PID_TAG_ENTRY_ID => "PidTagEntryId",
        PID_TAG_RECORD_KEY => "PidTagRecordKey",
        PID_TAG_SEARCH_KEY => "PidTagSearchKey",
        PID_TAG_CREATOR_NAME_W => "PidTagCreatorName",
        PID_TAG_CREATOR_ENTRY_ID => "PidTagCreatorEntryId",
        PID_TAG_LAST_MODIFIER_NAME_W => "PidTagLastModifierName",
        PID_TAG_LAST_MODIFIER_ENTRY_ID => "PidTagLastModifierEntryId",
        PID_TAG_CREATION_TIME => "PidTagCreationTime",
        PID_TAG_SOURCE_KEY => "PidTagSourceKey",
        PID_TAG_PARENT_SOURCE_KEY => "PidTagParentSourceKey",
        PID_TAG_PARENT_ENTRY_ID => "PidTagParentEntryId",
        PID_TAG_FOLDER_ID => "PidTagFolderId",
        PID_TAG_PARENT_FOLDER_ID => "PidTagParentFolderId",
        PID_TAG_INSTANCE_KEY => "PidTagInstanceKey",
        PID_TAG_FOLDER_TYPE => "PidTagFolderType",
        OUTLOOK_UNDOCUMENTED_FOLDER_BINARY_120C => "OutlookUndocumentedFolderBinary120C",
        PID_TAG_MESSAGE_CLASS_W | PID_TAG_MESSAGE_CLASS_STRING8 => "PidTagMessageClass",
        PID_TAG_ORIGINAL_MESSAGE_CLASS_W => "PidTagOriginalMessageClass",
        PID_TAG_IMPORTANCE => "PidTagImportance",
        PID_TAG_SENT_REPRESENTING_NAME_W => "PidTagSentRepresentingName",
        PID_TAG_SENT_REPRESENTING_ENTRY_ID => "PidTagSentRepresentingEntryId",
        PID_TAG_SENT_REPRESENTING_ADDRESS_TYPE_W => "PidTagSentRepresentingAddressType",
        PID_TAG_SENT_REPRESENTING_EMAIL_ADDRESS_W => "PidTagSentRepresentingEmailAddress",
        PID_TAG_SENT_REPRESENTING_SMTP_ADDRESS_W => "PidTagSentRepresentingSmtpAddress",
        PID_TAG_VIEW_DESCRIPTOR_CLSID => "PidTagViewDescriptorCLSID",
        PID_TAG_VIEW_DESCRIPTOR_FLAGS => "PidTagViewDescriptorFlags",
        OUTLOOK_COMMON_VIEW_DESCRIPTOR_BINARY_6835 => "OutlookCommonViewDescriptorBinary6835",
        PID_TAG_VIEW_DESCRIPTOR_VERSION => "PidTagViewDescriptorVersion",
        OUTLOOK_COMMON_VIEW_DESCRIPTOR_STRINGS_683C => "OutlookCommonViewDescriptorStrings683C",
        PID_TAG_VIEW_DESCRIPTOR_FOLDER_TYPE => "PidTagViewDescriptorFolderType",
        PID_TAG_VIEW_DESCRIPTOR_VIEW_MODE => "PidTagViewDescriptorViewMode",
        PID_TAG_VIEW_DESCRIPTOR_BINARY => "PidTagViewDescriptorBinary",
        PID_TAG_VIEW_DESCRIPTOR_STRINGS_W => "PidTagViewDescriptorStrings",
        PID_TAG_VIEW_DESCRIPTOR_NAME_W => "PidTagViewDescriptorName",
        PID_TAG_VIEW_DESCRIPTOR_VERSION_CANONICAL => "PidTagViewDescriptorVersionCanonical",
        PID_TAG_WLINK_GROUP_HEADER_ID => "PidTagWlinkGroupHeaderId",
        PID_TAG_WLINK_SAVE_STAMP => "PidTagWlinkSaveStamp",
        PID_TAG_WLINK_TYPE => "PidTagWlinkType",
        PID_TAG_WLINK_FLAGS => "PidTagWlinkFlags",
        PID_TAG_WLINK_ORDINAL => "PidTagWlinkOrdinal",
        PID_TAG_WLINK_ENTRY_ID => "PidTagWlinkEntryId",
        PID_TAG_WLINK_RECORD_KEY => "PidTagWlinkRecordKey",
        PID_TAG_WLINK_STORE_ENTRY_ID => "PidTagWlinkStoreEntryId",
        PID_TAG_WLINK_FOLDER_TYPE => "PidTagWlinkFolderType",
        PID_TAG_WLINK_GROUP_CLSID => "PidTagWlinkGroupClsid",
        PID_TAG_WLINK_GROUP_NAME_W => "PidTagWlinkGroupName",
        PID_TAG_WLINK_SECTION => "PidTagWlinkSection",
        PID_TAG_WLINK_CALENDAR_COLOR => "PidTagWlinkCalendarColor",
        PID_TAG_WLINK_ADDRESS_BOOK_EID => "PidTagWlinkAddressBookEid",
        PID_TAG_WLINK_CLIENT_ID => "PidTagWlinkClientId",
        PID_TAG_WLINK_ADDRESS_BOOK_STORE_EID => "PidTagWlinkAddressBookStoreEid",
        PID_TAG_WLINK_RO_GROUP_TYPE => "PidTagWlinkRoGroupType",
        OUTLOOK_STALE_SHARING_LOCAL_FOLDER_ID_TAG => {
            "OutlookStaleSharingCalendarGroupEntryAssociatedLocalFolderId"
        }
        OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B => "OutlookAssociatedConfigBinary0E0B",
        PID_NAME_CONTENT_CLASS_W_TAG => "PidNameContentClass",
        PID_NAME_CONTENT_TYPE_W_TAG => "PidNameContentType",
        PID_TAG_MESSAGE_STATUS => "PidTagMessageStatus",
        PID_TAG_CONTENT_COUNT => "PidTagContentCount",
        PID_TAG_ASSOCIATED_CONTENT_COUNT => "PidTagAssociatedContentCount",
        PID_TAG_CONTAINER_CLASS_W => "PidTagContainerClass",
        PID_TAG_CONTENT_UNREAD_COUNT => "PidTagContentUnreadCount",
        PID_TAG_SUBFOLDERS => "PidTagSubfolders",
        PID_TAG_IPM_SUBTREE_ENTRY_ID => "PidTagIpmSubtreeEntryId",
        PID_TAG_IPM_OUTBOX_ENTRY_ID => "PidTagIpmOutboxEntryId",
        PID_TAG_IPM_WASTEBASKET_ENTRY_ID => "PidTagIpmWastebasketEntryId",
        PID_TAG_IPM_SENTMAIL_ENTRY_ID => "PidTagIpmSentMailEntryId",
        PID_TAG_VIEWS_ENTRY_ID => "PidTagViewsEntryId",
        PID_TAG_COMMON_VIEWS_ENTRY_ID => "PidTagCommonViewsEntryId",
        PID_TAG_FINDER_ENTRY_ID => "PidTagFinderEntryId",
        PID_TAG_IPM_ARCHIVE_ENTRY_ID => "PidTagIpmArchiveEntryId",
        PID_TAG_IPM_APPOINTMENT_ENTRY_ID => "PidTagIpmAppointmentEntryId",
        PID_TAG_IPM_CONTACT_ENTRY_ID => "PidTagIpmContactEntryId",
        PID_TAG_IPM_JOURNAL_ENTRY_ID => "PidTagIpmJournalEntryId",
        PID_TAG_IPM_NOTE_ENTRY_ID => "PidTagIpmNoteEntryId",
        PID_TAG_IPM_TASK_ENTRY_ID => "PidTagIpmTaskEntryId",
        PID_TAG_REM_ONLINE_ENTRY_ID => "PidTagRemOnlineEntryId",
        PID_TAG_REM_OFFLINE_ENTRY_ID => "PidTagRemOfflineEntryId",
        PID_TAG_IPM_DRAFTS_ENTRY_ID => "PidTagIpmDraftsEntryId",
        PID_TAG_ADDITIONAL_REN_ENTRY_IDS => "PidTagAdditionalRenEntryIds",
        PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX => "PidTagAdditionalRenEntryIdsEx",
        PID_TAG_FREE_BUSY_ENTRY_IDS => "PidTagFreeBusyEntryIds",
        PID_TAG_EMAIL_ADDRESS_W => "PidTagEmailAddress",
        PID_TAG_SMTP_ADDRESS_W => "PidTagSmtpAddress",
        PID_TAG_OBJECT_TYPE => "PidTagObjectType",
        PID_TAG_DISPLAY_TYPE => "PidTagDisplayType",
        PID_TAG_DISPLAY_TYPE_EX => "PidTagDisplayTypeEx",
        PID_TAG_ADDRESS_BOOK_DISPLAY_NAME_PRINTABLE_W => "PidTagAddressBookDisplayNamePrintable",
        PID_TAG_SEND_INTERNET_ENCODING => "PidTagSendInternetEncoding",
        PID_TAG_RECIPIENT_DISPLAY_NAME_W => "PidTagRecipientDisplayName",
        PID_TAG_RECIPIENT_ENTRY_ID => "PidTagRecipientEntryId",
        PID_TAG_RECIPIENT_FLAGS => "PidTagRecipientFlags",
        PID_TAG_RECIPIENT_ORDER => "PidTagRecipientOrder",
        PID_TAG_RECIPIENT_TRACK_STATUS => "PidTagRecipientTrackStatus",
        OUTLOOK_RECIPIENT_5FDE => "OutlookRecipient5FDE",
        PID_TAG_SENDER_ADDRESS_TYPE_W => "PidTagSenderAddressType",
        PID_TAG_SENDER_NAME_W => "PidTagSenderName",
        PID_TAG_SENDER_EMAIL_ADDRESS_W => "PidTagSenderEmailAddress",
        PID_TAG_SENDER_SMTP_ADDRESS_W => "PidTagSenderSmtpAddress",
        PID_TAG_CLIENT_SUBMIT_TIME => "PidTagClientSubmitTime",
        PID_TAG_MESSAGE_DELIVERY_TIME => "PidTagMessageDeliveryTime",
        PID_TAG_DISPLAY_BCC_W => "PidTagDisplayBcc",
        PID_TAG_DISPLAY_CC_W => "PidTagDisplayCc",
        PID_TAG_DISPLAY_TO_W => "PidTagDisplayTo",
        PID_TAG_SUBJECT_W => "PidTagSubject",
        PID_TAG_SUBJECT_PREFIX_W => "PidTagSubjectPrefix",
        PID_TAG_NORMALIZED_SUBJECT_W => "PidTagNormalizedSubject",
        PID_TAG_TRANSPORT_MESSAGE_HEADERS_W => "PidTagTransportMessageHeaders",
        PID_TAG_BODY_STRING8 | PID_TAG_BODY_W => "PidTagBody",
        PID_TAG_RTF_COMPRESSED => "PidTagRtfCompressed",
        PID_TAG_BODY_HTML_W => "PidTagBodyHtml",
        PID_TAG_HTML_BINARY => "PidTagHtml",
        PID_TAG_RTF_IN_SYNC => "PidTagRtfInSync",
        PID_TAG_NATIVE_BODY => "PidTagNativeBody",
        PID_TAG_HAS_ATTACHMENTS => "PidTagHasAttachments",
        PID_TAG_TRUST_SENDER => "PidTagTrustSender",
        PID_TAG_HAS_NAMED_PROPERTIES => "PidTagHasNamedProperties",
        PID_TAG_MESSAGE_FLAGS => "PidTagMessageFlags",
        PID_TAG_MESSAGE_SIZE => "PidTagMessageSize",
        PID_TAG_READ => "PidTagRead",
        PID_TAG_INTERNET_CODEPAGE => "PidTagInternetCodepage",
        PID_TAG_MESSAGE_LOCALE_ID => "PidTagMessageLocaleId",
        PID_TAG_LOCALE_ID => "PidTagLocaleId",
        PID_TAG_INTERNET_MESSAGE_ID_W => "PidTagInternetMessageId",
        PID_TAG_EXTENDED_RULE_MESSAGE_ACTIONS => "PidTagExtendedRuleMessageActions",
        PID_TAG_FLAG_STATUS => "PidTagFlagStatus",
        PID_TAG_SWAPPED_TODO_STORE => "PidTagSwappedToDoStore",
        PID_TAG_LAST_MODIFICATION_TIME => "PidTagLastModificationTime",
        PID_TAG_SERIALIZED_REPLID_GUID_MAP => "PidTagSerializedReplidGuidMap",
        PID_TAG_RESOURCE_FLAGS => "PidTagResourceFlags",
        PID_TAG_USER_ENTRY_ID => "PidTagUserEntryId",
        PID_TAG_MAILBOX_OWNER_ENTRY_ID => "PidTagMailboxOwnerEntryId",
        PID_TAG_MAILBOX_OWNER_NAME_W => "PidTagMailboxOwnerName",
        PID_TAG_ASSOCIATED_SHARING_PROVIDER => "PidTagAssociatedSharingProvider",
        PID_TAG_IPM_PUBLIC_FOLDERS_ENTRY_ID => "PidTagIpmPublicFoldersEntryId",
        PID_TAG_SERVER_TYPE_DISPLAY_NAME_W => "PidTagServerTypeDisplayName",
        PID_TAG_SERVER_CONNECTED_ICON => "PidTagServerConnectedIcon",
        PID_TAG_SERVER_ACCOUNT_ICON => "PidTagServerAccountIcon",
        PID_TAG_OUTLOOK_STORE_STATE => "OutlookStoreState",
        PID_TAG_PRIVATE => "PidTagPrivate",
        PID_TAG_USER_GUID => "PidTagUserGuid",
        PID_TAG_MESSAGE_SIZE_EXTENDED => "PidTagMessageSizeExtended",
        PID_TAG_PROHIBIT_RECEIVE_QUOTA => "PidTagProhibitReceiveQuota",
        PID_TAG_MAX_SUBMIT_MESSAGE_SIZE => "PidTagMaxSubmitMessageSize",
        PID_TAG_PROHIBIT_SEND_QUOTA => "PidTagProhibitSendQuota",
        PID_TAG_STORAGE_QUOTA_LIMIT => "PidTagStorageQuotaLimit",
        PID_TAG_EXTENDED_RULE_SIZE_LIMIT => "PidTagExtendedRuleSizeLimit",
        PID_TAG_PST_PATH_W => "PidTagPstPath",
        PID_TAG_ATTACH_NUM => "PidTagAttachNumber",
        PID_TAG_ATTACH_EXTENSION_W => "PidTagAttachExtension",
        PID_TAG_ATTACH_FILENAME_W => "PidTagAttachFilename",
        PID_TAG_ATTACH_METHOD => "PidTagAttachMethod",
        PID_TAG_ATTACH_LONG_FILENAME_W => "PidTagAttachLongFilename",
        PID_TAG_ATTACH_RENDERING => "PidTagAttachRendering",
        PID_TAG_RENDERING_POSITION => "PidTagRenderingPosition",
        PID_TAG_ATTACH_MIME_TAG_W => "PidTagAttachMimeTag",
        PID_TAG_ATTACH_CONTENT_ID_W => "PidTagAttachContentId",
        PID_TAG_ATTACH_FLAGS => "PidTagAttachFlags",
        PID_TAG_ATTACHMENT_LINK_ID => "PidTagAttachmentLinkId",
        PID_TAG_ATTACHMENT_FLAGS => "PidTagAttachmentFlags",
        PID_TAG_ATTACHMENT_HIDDEN => "PidTagAttachmentHidden",
        PID_TAG_LOCAL_COMMIT_TIME_MAX => "PidTagLocalCommitTimeMax",
        PID_TAG_DELETED_COUNT_TOTAL => "PidTagDeletedCountTotal",
        PID_TAG_DEFAULT_POST_MESSAGE_CLASS_STRING8 | PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W => {
            "PidTagDefaultPostMessageClass"
        }
        PID_TAG_DEFAULT_FORM_NAME_W => "PidTagDefaultFormName",
        PID_TAG_DEFAULT_VIEW_ENTRY_ID => "PidTagDefaultViewEntryId",
        PID_TAG_FOLDER_FORM_FLAGS => "PidTagFolderFormFlags",
        PID_TAG_FOLDER_WEBVIEWINFO => "PidTagFolderWebViewInfo",
        PID_TAG_FOLDER_XVIEWINFO_E => "PidTagFolderXViewInfoE",
        PID_TAG_FOLDER_VIEWS_ONLY => "PidTagFolderViewsOnly",
        PID_TAG_FOLDER_FORM_STORAGE => "PidTagFolderFormStorage",
        PID_TAG_EXTENDED_FOLDER_FLAGS => "PidTagExtendedFolderFlags",
        PID_TAG_SEARCH_FOLDER_ID => "PidTagSearchFolderId",
        PID_TAG_SEARCH_FOLDER_STORAGE_TYPE => "PidTagSearchFolderStorageType",
        PID_TAG_SEARCH_FOLDER_EFP_FLAGS => "PidTagSearchFolderEfpFlags",
        PID_TAG_SEARCH_FOLDER_DEFINITION => "PidTagSearchFolderDefinition",
        PID_TAG_ARCHIVE_TAG => "PidTagArchiveTag",
        PID_TAG_POLICY_TAG => "PidTagPolicyTag",
        PID_TAG_RETENTION_PERIOD => "PidTagRetentionPeriod",
        PID_TAG_RETENTION_FLAGS => "PidTagRetentionFlags",
        PID_TAG_ARCHIVE_PERIOD => "PidTagArchivePeriod",
        PID_TAG_RIGHTS => "PidTagRights",
        PID_TAG_FOLDER_VIEWLIST_FLAGS => "PidTagFolderViewListFlags",
        PID_TAG_SENT_MAIL_SVR_EID => "PidTagSentMailSvrEID",
        tag if is_acl_member_name_property_tag(tag) => "PidTagMemberName",
        PID_LID_PERCENT_COMPLETE_TAG => "PidLidPercentComplete",
        PID_LID_LOCATION_W_TAG => "PidLidLocation",
        PID_LID_APPOINTMENT_DURATION_TAG => "PidLidAppointmentDuration",
        PID_LID_APPOINTMENT_START_WHOLE_TAG => "PidLidAppointmentStartWhole",
        PID_LID_APPOINTMENT_END_WHOLE_TAG => "PidLidAppointmentEndWhole",
        PID_LID_BUSY_STATUS_TAG => "PidLidBusyStatus",
        PID_LID_APPOINTMENT_SUB_TYPE_TAG => "PidLidAppointmentSubType",
        PID_LID_APPOINTMENT_STATE_FLAGS_TAG => "PidLidAppointmentStateFlags",
        PID_LID_TIME_ZONE_STRUCT_TAG => "PidLidTimeZoneStruct",
        PID_LID_TIME_ZONE_DESCRIPTION_W_TAG => "PidLidTimeZoneDescription",
        PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_START_DISPLAY_TAG => {
            "PidLidAppointmentTimeZoneDefinitionStartDisplay"
        }
        PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_END_DISPLAY_TAG => {
            "PidLidAppointmentTimeZoneDefinitionEndDisplay"
        }
        PID_TAG_CHANGE_KEY => "PidTagChangeKey",
        PID_TAG_ACCESS => "PidTagAccess",
        PID_TAG_ACCESS_LEVEL => "PidTagAccessLevel",
        PID_TAG_CONVERSATION_TOPIC_W => "PidTagConversationTopic",
        PID_TAG_CONVERSATION_INDEX => "PidTagConversationIndex",
        PID_TAG_ROAMING_DATATYPES => "PidTagRoamingDatatypes",
        PID_TAG_ROAMING_DICTIONARY => "PidTagRoamingDictionary",
        PID_TAG_ROAMING_XML_STREAM => "PidTagRoamingXmlStream",
        0x7C09_0102 => "PidTagRoamingBinary",
        0x685D_0003 => "OutlookConfigurationStamp",
        tag if debug_property_id_matches(tag, PID_TAG_VIEW_DESCRIPTOR_CLSID) => {
            "PidTagViewDescriptorCLSID"
        }
        tag if debug_property_id_matches(tag, PID_TAG_VIEW_DESCRIPTOR_FLAGS) => {
            "PidTagViewDescriptorFlags"
        }
        tag if debug_property_id_matches(tag, PID_TAG_VIEW_DESCRIPTOR_VERSION) => {
            "PidTagViewDescriptorVersion"
        }
        tag if debug_property_id_matches(tag, PID_TAG_VIEW_DESCRIPTOR_VERSION_CANONICAL) => {
            "PidTagViewDescriptorVersionCanonical"
        }
        tag if debug_property_id_matches(tag, PID_TAG_VIEW_DESCRIPTOR_NAME_W) => {
            "PidTagViewDescriptorName"
        }
        tag if debug_property_id_matches(tag, PID_TAG_VIEW_DESCRIPTOR_STRINGS_W) => {
            "PidTagViewDescriptorStrings"
        }
        tag if debug_property_id_matches(tag, PID_TAG_VIEW_DESCRIPTOR_FOLDER_TYPE) => {
            "PidTagViewDescriptorFolderType"
        }
        tag if debug_property_id_matches(tag, PID_TAG_VIEW_DESCRIPTOR_VIEW_MODE) => {
            "PidTagViewDescriptorViewMode"
        }
        tag if debug_property_id_matches(tag, PID_TAG_WLINK_GROUP_HEADER_ID) => {
            "PidTagWlinkGroupHeaderId"
        }
        tag if debug_property_id_matches(tag, PID_TAG_WLINK_SAVE_STAMP) => "PidTagWlinkSaveStamp",
        tag if debug_property_id_matches(tag, PID_TAG_WLINK_TYPE) => "PidTagWlinkType",
        tag if debug_property_id_matches(tag, PID_TAG_WLINK_FLAGS) => "PidTagWlinkFlags",
        tag if debug_property_id_matches(tag, PID_TAG_WLINK_ORDINAL) => "PidTagWlinkOrdinal",
        tag if debug_property_id_matches(tag, PID_TAG_WLINK_ENTRY_ID) => "PidTagWlinkEntryId",
        tag if debug_property_id_matches(tag, PID_TAG_WLINK_RECORD_KEY) => "PidTagWlinkRecordKey",
        tag if debug_property_id_matches(tag, PID_TAG_WLINK_STORE_ENTRY_ID) => {
            "PidTagWlinkStoreEntryId"
        }
        tag if debug_property_id_matches(tag, PID_TAG_WLINK_FOLDER_TYPE) => "PidTagWlinkFolderType",
        tag if debug_property_id_matches(tag, PID_TAG_WLINK_GROUP_CLSID) => "PidTagWlinkGroupClsid",
        tag if debug_property_id_matches(tag, PID_TAG_WLINK_GROUP_NAME_W) => "PidTagWlinkGroupName",
        tag if debug_property_id_matches(tag, PID_TAG_WLINK_SECTION) => "PidTagWlinkSection",
        tag if debug_property_id_matches(tag, PID_TAG_WLINK_CALENDAR_COLOR) => {
            "PidTagWlinkCalendarColor"
        }
        tag if debug_property_id_matches(tag, PID_TAG_WLINK_ADDRESS_BOOK_EID) => {
            "PidTagWlinkAddressBookEid"
        }
        tag if debug_property_id_matches(tag, PID_TAG_WLINK_CLIENT_ID) => "PidTagWlinkClientId",
        tag if debug_property_id_matches(tag, PID_TAG_WLINK_ADDRESS_BOOK_STORE_EID) => {
            "PidTagWlinkAddressBookStoreEid"
        }
        tag if debug_property_id_matches(tag, PID_TAG_WLINK_RO_GROUP_TYPE) => {
            "PidTagWlinkRoGroupType"
        }
        tag if debug_property_id_matches(tag, PID_TAG_USER_GUID) => "PidTagUserGuid",
        PID_TAG_OST_OSTID => "PR_OST_OSTID",
        _ => "unknown",
    }
}

fn debug_property_id_matches(tag: u32, known_tag: u32) -> bool {
    tag & 0xffff_0000 == known_tag & 0xffff_0000
}

fn default_folder_property_mappings_for_debug(tags: &[u32]) -> Vec<String> {
    tags.iter()
        .filter_map(|tag| default_folder_property_mapping_for_debug(*tag))
        .collect()
}

fn default_folder_property_mapping_for_debug(tag: u32) -> Option<String> {
    let (name, folder_id) = match canonical_property_storage_tag(tag) {
        PID_TAG_IPM_SUBTREE_ENTRY_ID => ("IPM Subtree", IPM_SUBTREE_FOLDER_ID),
        PID_TAG_IPM_OUTBOX_ENTRY_ID => ("Outbox", OUTBOX_FOLDER_ID),
        PID_TAG_IPM_WASTEBASKET_ENTRY_ID => ("Deleted Items", TRASH_FOLDER_ID),
        PID_TAG_IPM_SENTMAIL_ENTRY_ID => ("Sent Items", SENT_FOLDER_ID),
        PID_TAG_VIEWS_ENTRY_ID => ("Personal Views", VIEWS_FOLDER_ID),
        PID_TAG_COMMON_VIEWS_ENTRY_ID => ("Common Views", COMMON_VIEWS_FOLDER_ID),
        PID_TAG_FINDER_ENTRY_ID => ("Finder", SEARCH_FOLDER_ID),
        PID_TAG_IPM_ARCHIVE_ENTRY_ID => ("Archive", ARCHIVE_FOLDER_ID),
        PID_TAG_IPM_APPOINTMENT_ENTRY_ID => ("Calendar", CALENDAR_FOLDER_ID),
        PID_TAG_IPM_CONTACT_ENTRY_ID => ("Contacts", CONTACTS_FOLDER_ID),
        PID_TAG_IPM_JOURNAL_ENTRY_ID => ("Journal", JOURNAL_FOLDER_ID),
        PID_TAG_IPM_NOTE_ENTRY_ID => ("Notes", NOTES_FOLDER_ID),
        PID_TAG_IPM_TASK_ENTRY_ID => ("Tasks", TASKS_FOLDER_ID),
        PID_TAG_REM_ONLINE_ENTRY_ID => ("Reminders", REMINDERS_FOLDER_ID),
        PID_TAG_REM_OFFLINE_ENTRY_ID => ("Reminders", REMINDERS_FOLDER_ID),
        PID_TAG_IPM_DRAFTS_ENTRY_ID => ("Drafts", DRAFTS_FOLDER_ID),
        _ => return None,
    };
    Some(format!(
        "{tag:#010x}:{name}:folder_id={folder_id:#018x}:source_key={}",
        format_bytes_hex(&mapi_mailstore::source_key_for_store_id(folder_id))
    ))
}

fn format_bytes_hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join("")
}

pub(in crate::mapi) fn rop_get_properties_all_response(
    request: &RopRequest,
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u8> {
    let Some(object) = object else {
        return rop_error_response(0x08, request.input_handle_index().unwrap_or(0), 0x8004_0102);
    };
    if let MapiObject::Event {
        folder_id,
        event_id,
    } = object
    {
        if snapshot.event_for_id(*folder_id, *event_id).is_none() {
            return rop_error_response(
                0x08,
                request.input_handle_index().unwrap_or(0),
                0x8004_010F,
            );
        }
    }
    let mut response = vec![0x08, request.input_handle_index().unwrap_or(0)];
    write_u32(&mut response, 0);
    let size_limit = request_property_size_limit(request);
    let want_unicode = request_get_properties_all_want_unicode(request);
    let tags = match object {
        MapiObject::Logon => default_store_property_tags(),
        MapiObject::Folder {
            folder_id: ROOT_FOLDER_ID | INBOX_FOLDER_ID,
            ..
        } => default_folder_property_tags_with_identity(),
        MapiObject::Attachment { .. }
        | MapiObject::PendingAttachment { .. }
        | MapiObject::SavedAttachment { .. } => default_attachment_columns(),
        MapiObject::Message { .. }
        | MapiObject::PublicFolderItem { .. }
        | MapiObject::PendingMessage { .. } => default_message_property_tags(),
        MapiObject::Contact { .. } | MapiObject::PendingContact { .. } => {
            default_contact_property_tags()
        }
        MapiObject::Event { .. } | MapiObject::PendingEvent { .. } => default_event_property_tags(),
        MapiObject::Task { .. } | MapiObject::PendingTask { .. } => default_task_property_tags(),
        MapiObject::Note { .. } | MapiObject::PendingNote { .. } => default_note_property_tags(),
        MapiObject::JournalEntry { .. } | MapiObject::PendingJournalEntry { .. } => {
            default_journal_entry_property_tags()
        }
        MapiObject::ConversationAction { .. } | MapiObject::PendingConversationAction { .. } => {
            default_conversation_action_property_tags()
        }
        _ => default_folder_property_tags(),
    };
    response.extend_from_slice(&(tags.len() as u16).to_le_bytes());
    for tag in tags {
        let tag = get_properties_all_response_tag(tag, want_unicode);
        let value =
            serialize_object_property(Some(object), principal, mailboxes, emails, snapshot, tag);
        if size_limit != 0 && value.len() > size_limit {
            write_u32(&mut response, property_error_tag(tag));
            write_u32(&mut response, 0x8007_000E);
        } else {
            write_u32(&mut response, tag);
            response.extend_from_slice(&value);
        }
    }
    response
}

fn request_get_properties_all_want_unicode(request: &RopRequest) -> bool {
    request
        .payload
        .get(2..4)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u16::from_le_bytes)
        .unwrap_or(1)
        != 0
}

fn get_properties_all_response_tag(property_tag: u32, want_unicode: bool) -> u32 {
    if want_unicode {
        return property_tag;
    }
    match MapiPropertyTag::new(property_tag).property_type() {
        Some(MapiPropertyType::String) => (property_tag & 0xFFFF_0000) | 0x001E,
        Some(MapiPropertyType::MultipleString) => (property_tag & 0xFFFF_0000) | 0x101E,
        _ => property_tag,
    }
}

fn property_error_tag(property_tag: u32) -> u32 {
    (property_tag & 0xFFFF_0000) | 0x000A
}

fn default_folder_property_tags_with_identity() -> Vec<u32> {
    let mut tags = default_folder_property_tags();
    tags.extend(default_folder_identity_property_tags());
    tags
}

pub(in crate::mapi) fn rop_get_valid_attachments_response(
    request: &RopRequest,
    object: Option<&MapiObject>,
    snapshot: &MapiMailStoreSnapshot,
    pending_attachment_deletions: &HashSet<(u64, u64, u32)>,
) -> Vec<u8> {
    let (folder_id, message_id) = match object {
        Some(MapiObject::Message {
            folder_id,
            message_id,
            ..
        })
        | Some(MapiObject::Event {
            folder_id,
            event_id: message_id,
        }) => (*folder_id, *message_id),
        _ => return rop_error_response(0x52, request.response_handle_index(), 0x0000_04B9),
    };
    if matches!(object, Some(MapiObject::Event { .. }))
        && snapshot.event_for_id(folder_id, message_id).is_none()
    {
        return rop_error_response(0x52, request.response_handle_index(), 0x8004_010F);
    }
    let attachments = snapshot
        .attachments_for_message(folder_id, message_id)
        .unwrap_or_default();
    let valid_attachments = attachments
        .iter()
        .filter(|attachment| {
            !pending_attachment_deletions.contains(&(folder_id, message_id, attachment.attach_num))
        })
        .collect::<Vec<_>>();
    let mut response = vec![0x52, request.response_handle_index()];
    write_u32(&mut response, 0);
    response
        .extend_from_slice(&(valid_attachments.len().min(u16::MAX as usize) as u16).to_le_bytes());
    for attachment in valid_attachments.iter().take(u16::MAX as usize) {
        write_u32(&mut response, attachment.attach_num);
    }
    response
}

pub(in crate::mapi) fn rop_get_property_ids_from_names_response(
    request: &RopRequest,
    property_ids: &[u16],
) -> Vec<u8> {
    let mut response = vec![0x56, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.extend_from_slice(&(property_ids.len().min(u16::MAX as usize) as u16).to_le_bytes());
    for property_id in property_ids.iter().take(u16::MAX as usize) {
        response.extend_from_slice(&property_id.to_le_bytes());
    }
    response
}

pub(in crate::mapi) fn rop_get_names_from_property_ids_response(
    request: &RopRequest,
    session: &MapiSession,
) -> Vec<u8> {
    let property_ids = request.property_ids();
    let mut response = vec![0x55, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.extend_from_slice(&(property_ids.len().min(u16::MAX as usize) as u16).to_le_bytes());
    for property_id in property_ids.iter().take(u16::MAX as usize) {
        write_named_property(&mut response, &session.property_name_for_id(*property_id));
    }
    response
}

pub(in crate::mapi) fn rop_query_named_properties_response(
    request: &RopRequest,
    session: &MapiSession,
) -> Vec<u8> {
    let properties = session.named_properties_for_query(request.named_property_query_guid());
    let mut response = vec![0x5F, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.extend_from_slice(&(properties.len().min(u16::MAX as usize) as u16).to_le_bytes());
    for (property_id, _property) in properties.iter().take(u16::MAX as usize) {
        response.extend_from_slice(&property_id.to_le_bytes());
    }
    for (_property_id, property) in properties.iter().take(u16::MAX as usize) {
        write_named_property(&mut response, property);
    }
    response
}

pub(in crate::mapi) fn serialize_object_property(
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    tag: u32,
) -> Vec<u8> {
    match object {
        Some(MapiObject::Logon) => serialize_logon_row(principal, &[tag]),
        Some(MapiObject::PublicFolderLogon) if tag == PID_TAG_PRIVATE => {
            let mut row = Vec::new();
            write_mapi_value(&mut row, tag, &MapiValue::Bool(false));
            row
        }
        Some(MapiObject::Message {
            folder_id,
            message_id,
            saved_email,
            ..
        }) => message_for_id(*folder_id, *message_id, mailboxes, emails)
            .or_else(|| {
                search_folder_message_for_id(snapshot, *folder_id, *message_id)
                    .map(|message| &message.email)
            })
            .or(saved_email.as_ref().map(|saved| &saved.email))
            .map(|email| serialize_message_row(email, &[tag]))
            .unwrap_or_else(|| {
                let mut value = Vec::new();
                write_property_default(&mut value, tag);
                value
            }),
        Some(MapiObject::PendingMessage { properties, .. }) => {
            serialize_pending_message_row(principal, properties, &[tag])
        }
        Some(MapiObject::PendingAssociatedMessage { properties, .. }) => {
            serialize_pending_associated_message_row(principal, properties, &[tag])
        }
        Some(MapiObject::Contact {
            folder_id,
            contact_id,
        }) => snapshot
            .contact_for_id(*folder_id, *contact_id)
            .map(|contact| {
                serialize_contact_row(&contact.contact, contact.id, contact.folder_id, &[tag])
            })
            .unwrap_or_else(|| {
                let mut value = Vec::new();
                write_property_default(&mut value, tag);
                value
            }),
        Some(MapiObject::PendingContact { properties, .. }) => {
            serialize_pending_contact_row(principal, properties, &[tag])
        }
        Some(MapiObject::Event {
            folder_id,
            event_id,
        }) => snapshot
            .event_for_id(*folder_id, *event_id)
            .map(|event| {
                serialize_event_row_with_attachments(
                    &event.event,
                    event.id,
                    event.folder_id,
                    !event.attachments.is_empty(),
                    &[tag],
                )
            })
            .unwrap_or_else(|| {
                let mut value = Vec::new();
                write_property_default(&mut value, tag);
                value
            }),
        Some(MapiObject::PendingEvent { properties, .. }) => {
            serialize_pending_event_row(principal, properties, &[tag])
        }
        Some(MapiObject::PendingTask { properties, .. }) => {
            serialize_pending_task_row(principal, properties, &[tag])
        }
        Some(MapiObject::PendingNote { properties, .. }) => {
            serialize_pending_note_row(principal, properties, &[tag])
        }
        Some(MapiObject::PendingJournalEntry { properties, .. }) => {
            serialize_pending_journal_entry_row(principal, properties, &[tag])
        }
        Some(MapiObject::PendingConversationAction { properties, .. }) => {
            serialize_pending_conversation_action_row(properties, &[tag])
        }
        Some(MapiObject::PendingNavigationShortcut { properties, .. }) => {
            serialize_pending_navigation_shortcut_row(properties, principal, &[tag])
        }
        Some(MapiObject::Task { folder_id, task_id }) => snapshot
            .task_for_id(*folder_id, *task_id)
            .map(|task| serialize_task_row(&task.task, task.id, task.folder_id, &[tag]))
            .unwrap_or_else(|| {
                let mut value = Vec::new();
                write_property_default(&mut value, tag);
                value
            }),
        Some(MapiObject::Note { folder_id, note_id }) => snapshot
            .note_for_id(*folder_id, *note_id)
            .map(|note| serialize_note_row(&note.note, note.id, note.folder_id, &[tag]))
            .unwrap_or_else(|| {
                let mut value = Vec::new();
                write_property_default(&mut value, tag);
                value
            }),
        Some(MapiObject::JournalEntry {
            folder_id,
            journal_entry_id,
        }) => snapshot
            .journal_entry_for_id(*folder_id, *journal_entry_id)
            .map(|entry| {
                serialize_journal_entry_row(&entry.entry, entry.id, entry.folder_id, &[tag])
            })
            .unwrap_or_else(|| {
                let mut value = Vec::new();
                write_property_default(&mut value, tag);
                value
            }),
        Some(MapiObject::NavigationShortcut {
            folder_id,
            shortcut_id,
        }) => snapshot
            .navigation_shortcut_table_message_for_id(*shortcut_id)
            .filter(|message| message.folder_id == *folder_id)
            .map(|message| serialize_navigation_shortcut_row(&message, Some(principal), &[tag]))
            .unwrap_or_else(|| {
                let mut value = Vec::new();
                write_property_default(&mut value, tag);
                value
            }),
        Some(MapiObject::CommonViewNamedView { folder_id, view_id }) => snapshot
            .named_view_message_for_folder_and_id(*folder_id, *view_id)
            .map(|message| {
                serialize_common_view_named_view_row_with_mailbox_guid(
                    &message,
                    principal.account_id,
                    &[tag],
                )
            })
            .unwrap_or_else(|| {
                let mut value = Vec::new();
                write_property_default(&mut value, tag);
                value
            }),
        Some(MapiObject::AssociatedConfig {
            folder_id,
            config_id,
            saved_message,
            ..
        }) => snapshot
            .associated_config_message_for_id(*config_id)
            .or_else(|| saved_message.clone())
            .filter(|message| message.folder_id == *folder_id)
            .map(|message| {
                serialize_associated_config_row_with_mailbox_guid(
                    &message,
                    principal.account_id,
                    &[tag],
                )
            })
            .unwrap_or_else(|| {
                let mut value = Vec::new();
                write_property_default(&mut value, tag);
                value
            }),
        Some(MapiObject::ConversationAction {
            folder_id,
            conversation_action_id,
            ..
        }) => snapshot
            .conversation_action_table_message_for_id(*conversation_action_id)
            .filter(|message| message.folder_id == *folder_id)
            .map(|message| serialize_conversation_action_row(&message, &[tag]))
            .unwrap_or_else(|| {
                let mut value = Vec::new();
                write_property_default(&mut value, tag);
                value
            }),
        Some(MapiObject::DelegateFreeBusyMessage {
            folder_id,
            message_id,
        }) => snapshot
            .delegate_freebusy_message_for_id(*message_id)
            .filter(|message| message.folder_id == *folder_id)
            .map(|message| serialize_delegate_freebusy_row(message, &[tag]))
            .unwrap_or_else(|| {
                let mut value = Vec::new();
                write_property_default(&mut value, tag);
                value
            }),
        Some(MapiObject::RecoverableItem { folder_id, item_id }) => snapshot
            .recoverable_item_for_id(*folder_id, *item_id)
            .map(|item| serialize_recoverable_item_row(item, &[tag]))
            .unwrap_or_else(|| {
                let mut value = Vec::new();
                write_property_default(&mut value, tag);
                value
            }),
        Some(MapiObject::PublicFolderItem {
            folder_id, item_id, ..
        }) => snapshot
            .public_folder_item_for_id(*folder_id, *item_id)
            .map(|item| serialize_public_folder_item_row(item, &[tag]))
            .unwrap_or_else(|| {
                let mut value = Vec::new();
                write_property_default(&mut value, tag);
                value
            }),
        Some(MapiObject::Folder {
            folder_id,
            properties,
        }) => serialize_session_folder_row(
            *folder_id,
            properties,
            principal,
            mailboxes,
            snapshot,
            &[tag],
        ),
        Some(MapiObject::Attachment {
            folder_id,
            message_id,
            attach_num,
        }) => snapshot
            .attachment_for_message(*folder_id, *message_id, *attach_num)
            .map(|attachment| serialize_attachment_row(attachment, &[tag]))
            .unwrap_or_else(|| {
                let mut value = Vec::new();
                write_property_default(&mut value, tag);
                value
            }),
        Some(MapiObject::PendingAttachment {
            attach_num,
            properties,
            data,
            ..
        }) => serialize_pending_attachment_row(*attach_num, properties, data, &[tag]),
        Some(MapiObject::SavedAttachment {
            attach_num,
            file_reference,
            file_name,
            media_type,
            disposition,
            content_id,
            size_octets,
            ..
        }) => serialize_saved_attachment_row(
            *attach_num,
            file_reference,
            file_name,
            media_type,
            disposition.as_deref(),
            content_id.as_deref(),
            *size_octets,
            &[tag],
        ),
        _ => {
            let folder_id = object
                .and_then(MapiObject::folder_id)
                .unwrap_or(ROOT_FOLDER_ID);
            folder_row_for_id(folder_id, mailboxes)
                .map(|mailbox| {
                    serialize_folder_row_with_context(
                        mailbox,
                        mailboxes,
                        &[tag],
                        principal.account_id,
                    )
                })
                .or_else(|| {
                    snapshot
                        .collaboration_folder_for_id(folder_id)
                        .map(|folder| {
                            serialize_collaboration_folder_row_with_context(
                                folder,
                                &[tag],
                                associated_folder_message_count(folder_id, snapshot),
                            )
                        })
                })
                .unwrap_or_else(|| {
                    serialize_special_folder_row(folder_id, mailboxes, &[tag], Some(principal))
                })
        }
    }
}

fn serialize_session_folder_row(
    folder_id: u64,
    properties: &HashMap<u32, MapiValue>,
    principal: &AccountPrincipal,
    mailboxes: &[JmapMailbox],
    snapshot: &MapiMailStoreSnapshot,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        let storage_tag = canonical_property_storage_tag(*column);
        if storage_tag == PID_TAG_FOLDER_TYPE
            && snapshot
                .search_folder_definition_for_folder_id(folder_id)
                .is_some()
        {
            write_mapi_value(&mut row, *column, &MapiValue::U32(FOLDER_SEARCH));
            continue;
        }

        if let Some(value) = properties
            .get(&storage_tag)
            .or_else(|| properties.get(column))
        {
            write_mapi_value(&mut row, *column, value);
            continue;
        }

        if (is_advertised_special_folder(folder_id)
            || snapshot
                .search_folder_definition_for_folder_id(folder_id)
                .is_some())
            && storage_tag == PID_TAG_CONTENT_COUNT
        {
            write_mapi_value(
                &mut row,
                *column,
                &MapiValue::U32(folder_message_count(folder_id, mailboxes, &[], snapshot)),
            );
            continue;
        }

        if let Some(definition) = snapshot.search_folder_definition_for_folder_id(folder_id) {
            if let Some(value) = search_folder_definition_property_value(
                definition,
                folder_id,
                *column,
                principal.account_id,
            ) {
                write_mapi_value(&mut row, *column, &value);
                continue;
            }
        }

        if folder_id == IPM_SUBTREE_FOLDER_ID && storage_tag == PID_TAG_OST_OSTID {
            write_mapi_value(
                &mut row,
                *column,
                &MapiValue::Binary(ipm_subtree_ost_ostid(principal)),
            );
            continue;
        }

        if matches!(folder_id, ROOT_FOLDER_ID | INBOX_FOLDER_ID) {
            if let Some(value) =
                special_folder_identification_property_value(principal.account_id, *column)
            {
                write_mapi_value(&mut row, *column, &value);
                continue;
            }
        }

        let value = folder_row_for_id(folder_id, mailboxes)
            .map(|mailbox| {
                serialize_folder_row_with_context(
                    mailbox,
                    mailboxes,
                    &[*column],
                    principal.account_id,
                )
            })
            .or_else(|| {
                snapshot
                    .collaboration_folder_for_id(folder_id)
                    .map(|folder| {
                        serialize_collaboration_folder_row_with_context(
                            folder,
                            &[*column],
                            associated_folder_message_count(folder_id, snapshot),
                        )
                    })
            })
            .or_else(|| {
                snapshot
                    .public_folder_for_id(folder_id)
                    .map(|folder| serialize_public_folder_row(folder, &[*column]))
            })
            .unwrap_or_else(|| {
                serialize_special_folder_row(folder_id, mailboxes, &[*column], Some(principal))
            });
        row.extend_from_slice(&value);
    }
    row
}

pub(in crate::mapi) fn rop_get_receive_folder_response(
    request: &RopRequest,
    folder_id: u64,
    response_message_class: &str,
) -> Vec<u8> {
    let mut response = vec![0x27, request.response_handle_index()];
    write_u32(&mut response, 0);
    write_object_id(&mut response, folder_id);
    response.extend_from_slice(response_message_class.as_bytes());
    response.push(0);
    response
}

pub(in crate::mapi) fn valid_receive_folder_message_class(message_class: &str) -> bool {
    let len = message_class.len() + 1;
    len <= 255
        && !message_class.starts_with('.')
        && !message_class.ends_with('.')
        && !message_class.contains("..")
        && message_class
            .bytes()
            .all(|byte| (0x20..=0x7E).contains(&byte))
}

#[derive(Clone, Copy)]
struct ReceiveFolderEntry {
    message_class: &'static str,
    folder_id: u64,
}

const RECEIVE_FOLDER_ENTRIES: &[ReceiveFolderEntry] = &[
    ReceiveFolderEntry {
        message_class: "IPM.Appointment",
        folder_id: CALENDAR_FOLDER_ID,
    },
    ReceiveFolderEntry {
        message_class: "IPM.Note",
        folder_id: INBOX_FOLDER_ID,
    },
    ReceiveFolderEntry {
        message_class: "IPM",
        folder_id: INBOX_FOLDER_ID,
    },
];

fn receive_folder_entry_matches(entry: ReceiveFolderEntry, message_class: &str) -> bool {
    if entry.message_class.is_empty() {
        return true;
    }
    if message_class.len() < entry.message_class.len()
        || !message_class.as_bytes()[..entry.message_class.len()]
            .eq_ignore_ascii_case(entry.message_class.as_bytes())
    {
        return false;
    }
    message_class.len() == entry.message_class.len()
        || message_class.as_bytes().get(entry.message_class.len()) == Some(&b'.')
}

fn receive_folder_entry_for_message_class(message_class: &str) -> ReceiveFolderEntry {
    if message_class.is_empty() {
        return ReceiveFolderEntry {
            message_class: "",
            folder_id: INBOX_FOLDER_ID,
        };
    }
    RECEIVE_FOLDER_ENTRIES
        .iter()
        .copied()
        .filter(|entry| receive_folder_entry_matches(*entry, message_class))
        .max_by_key(|entry| entry.message_class.len())
        .unwrap_or(ReceiveFolderEntry {
            message_class: "",
            folder_id: INBOX_FOLDER_ID,
        })
}

pub(in crate::mapi) fn explicit_receive_folder_message_class(message_class: &str) -> &'static str {
    receive_folder_entry_for_message_class(message_class).message_class
}

pub(in crate::mapi) fn receive_folder_id_for_message_class(message_class: &str) -> u64 {
    receive_folder_entry_for_message_class(message_class).folder_id
}

pub(in crate::mapi) fn rop_get_receive_folder_table_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x68, request.response_handle_index()];
    write_u32(&mut response, 0);
    write_u32(&mut response, RECEIVE_FOLDER_ENTRIES.len() as u32);
    for entry in RECEIVE_FOLDER_ENTRIES {
        let mut row = Vec::new();
        write_mapi_value(
            &mut row,
            PID_TAG_FOLDER_ID,
            &MapiValue::U64(entry.folder_id),
        );
        write_mapi_value(
            &mut row,
            PID_TAG_MESSAGE_CLASS_STRING8,
            &MapiValue::String(entry.message_class.to_string()),
        );
        write_mapi_value(
            &mut row,
            PID_TAG_LAST_MODIFICATION_TIME,
            &MapiValue::U64(crate::mapi_mailstore::filetime_from_change_number(
                crate::mapi_mailstore::change_number_for_store_id(entry.folder_id),
            )),
        );
        write_standard_property_row(&mut response, &row);
    }
    response
}

pub(in crate::mapi) fn rop_get_transport_folder_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x6D, request.input_handle_index().unwrap_or(0)];
    write_u32(&mut response, 0);
    write_object_id(&mut response, OUTBOX_FOLDER_ID);
    response
}

pub(in crate::mapi) fn rop_get_store_state_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x7B, request.response_handle_index()];
    write_u32(&mut response, 0);
    write_u32(&mut response, 0);
    response
}

pub(in crate::mapi) fn rop_get_owning_servers_response(
    request: &RopRequest,
    servers: &[String],
) -> Vec<u8> {
    let mut response = vec![0x42, request.response_handle_index()];
    write_u32(&mut response, 0);
    let server_count = servers.len().min(u16::MAX as usize) as u16;
    write_u16(&mut response, server_count);
    write_u16(&mut response, server_count);
    for server in servers.iter().take(usize::from(server_count)) {
        response.extend_from_slice(server.as_bytes());
        response.push(0);
    }
    response
}

pub(in crate::mapi) fn rop_public_folder_is_ghosted_response(
    request: &RopRequest,
    is_ghosted: bool,
) -> Vec<u8> {
    let mut response = vec![0x45, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(u8::from(is_ghosted));
    if is_ghosted {
        write_u16(&mut response, 0);
        write_u16(&mut response, 0);
    }
    response
}

pub(in crate::mapi) fn rop_reset_table_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x81, request.response_handle_index()];
    write_u32(&mut response, 0);
    response
}

pub(in crate::mapi) fn folder_row_for_id(
    folder_id: u64,
    mailboxes: &[JmapMailbox],
) -> Option<&JmapMailbox> {
    mailboxes.iter().find(|mailbox| {
        mapi_folder_id(mailbox) == folder_id
            || mailbox.role == role_for_folder_id(folder_id).unwrap_or_default()
    })
}

pub(in crate::mapi) const ROP_ERROR_NOT_SUPPORTED: u32 = MapiError::InvalidParameter.as_u32();
pub(in crate::mapi) const ROP_ERROR_NOT_FOUND: u32 = MapiError::NotFound.as_u32();

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::mapi) struct RopResponseError {
    pub(in crate::mapi) rop_id: u8,
    pub(in crate::mapi) handle_index: u8,
    pub(in crate::mapi) error_code: u32,
}

impl RopResponseError {
    pub(in crate::mapi) fn serialize(self) -> Vec<u8> {
        let mut response = vec![self.rop_id, self.handle_index];
        write_u32(&mut response, self.error_code);
        response
    }
}

pub(in crate::mapi) fn unsupported_rop_response(rop_id: u8, handle_index: u8) -> Vec<u8> {
    let known_unsupported_name = RopId::known_unsupported_name(rop_id);
    tracing::warn!(
        adapter = "mapi",
        enum_name = "RopId",
        raw_value = rop_id,
        known_unsupported = known_unsupported_name.is_some(),
        known_unsupported_name = known_unsupported_name.unwrap_or(""),
        "unsupported MAPI ROP response"
    );
    rop_error_response(rop_id, handle_index, ROP_ERROR_NOT_SUPPORTED)
}

pub(in crate::mapi) fn rop_copy_to_null_destination_response(request: &RopRequest) -> Vec<u8> {
    rop_property_copy_null_destination_response(0x39, request)
}

pub(in crate::mapi) fn rop_copy_properties_null_destination_response(
    request: &RopRequest,
) -> Vec<u8> {
    rop_property_copy_null_destination_response(0x67, request)
}

pub(in crate::mapi) fn rop_copy_properties_success_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x67, request.response_handle_index()];
    write_u32(&mut response, 0);
    write_u16(&mut response, 0);
    response
}

fn rop_property_copy_null_destination_response(rop_id: u8, request: &RopRequest) -> Vec<u8> {
    let mut response = vec![rop_id, request.response_handle_index()];
    write_u32(&mut response, 0x0000_0503);
    write_u32(
        &mut response,
        request.output_handle_index().map(u32::from).unwrap_or(0),
    );
    response
}

pub(in crate::mapi) fn rop_error_response(
    rop_id: u8,
    handle_index: u8,
    error_code: u32,
) -> Vec<u8> {
    RopResponseError {
        rop_id,
        handle_index,
        error_code,
    }
    .serialize()
}

pub(in crate::mapi) fn rop_parse_error_response() -> Vec<u8> {
    rop_error_response(0, 0, ROP_ERROR_NOT_SUPPORTED)
}

pub(in crate::mapi) fn rop_handle_index_error_response(request: &RopRequest) -> Vec<u8> {
    rop_error_response(
        request.rop_id,
        request.response_handle_index(),
        ROP_ERROR_NOT_FOUND,
    )
}

pub(in crate::mapi) fn rop_buffer_with_response(
    response: Vec<u8>,
    output_handles: &[u32],
) -> Vec<u8> {
    let mut buffer = Vec::new();
    buffer.extend_from_slice(&(response.len() as u16).to_le_bytes());
    buffer.extend_from_slice(&response);
    for handle in output_handles {
        buffer.extend_from_slice(&handle.to_le_bytes());
    }
    buffer
}

pub(in crate::mapi) fn rop_buffer_with_response_spec(
    response: Vec<u8>,
    output_handles: &[u32],
) -> Vec<u8> {
    let mut buffer = Vec::new();
    let rop_size = response.len().saturating_add(2).min(u16::MAX as usize) as u16;
    buffer.extend_from_slice(&rop_size.to_le_bytes());
    buffer.extend_from_slice(&response);
    for handle in output_handles {
        buffer.extend_from_slice(&handle.to_le_bytes());
    }
    buffer
}

pub(in crate::mapi) fn rop_buffer_too_small_response(
    size_needed: usize,
    request_buffers: &[u8],
    handle_table: &[u8],
) -> Vec<u8> {
    let mut response = Vec::with_capacity(3 + request_buffers.len());
    response.push(0xFF);
    response.extend_from_slice(&(size_needed.min(u16::MAX as usize) as u16).to_le_bytes());
    response.extend_from_slice(request_buffers);

    let mut buffer = Vec::with_capacity(2 + response.len() + handle_table.len());
    let rop_size = response.len().saturating_add(2).min(u16::MAX as usize) as u16;
    buffer.extend_from_slice(&rop_size.to_le_bytes());
    buffer.extend_from_slice(&response);
    buffer.extend_from_slice(handle_table);
    buffer
}

#[allow(dead_code)]
pub(in crate::mapi) fn rop_backoff_response(
    logon_id: u8,
    duration_ms: u32,
    backoff_rops: &[(u8, u32)],
    additional_data: &[u8],
) -> Vec<u8> {
    let mut response = Vec::new();
    response.push(0xF9);
    response.push(logon_id);
    response.extend_from_slice(&duration_ms.to_le_bytes());
    response.push(backoff_rops.len().min(u8::MAX as usize) as u8);
    for (rop_id, duration_ms) in backoff_rops.iter().take(u8::MAX as usize) {
        response.push(*rop_id);
        response.extend_from_slice(&duration_ms.to_le_bytes());
    }
    response
        .extend_from_slice(&(additional_data.len().min(u16::MAX as usize) as u16).to_le_bytes());
    response.extend_from_slice(&additional_data[..additional_data.len().min(u16::MAX as usize)]);
    response
}

pub(in crate::mapi) fn write_u32(body: &mut Vec<u8>, value: u32) {
    body.extend_from_slice(&value.to_le_bytes());
}

pub(in crate::mapi) fn write_u16(body: &mut Vec<u8>, value: u16) {
    body.extend_from_slice(&value.to_le_bytes());
}

pub(in crate::mapi) fn write_u16_prefixed_bytes(body: &mut Vec<u8>, value: &[u8]) {
    body.extend_from_slice(&(value.len() as u16).to_le_bytes());
    body.extend_from_slice(value);
}

pub(in crate::mapi) fn read_u16_prefixed_string(bytes: &[u8], offset: usize) -> Option<String> {
    let size_bytes = bytes.get(offset..offset + 2)?;
    let size = u16::from_le_bytes(size_bytes.try_into().ok()?) as usize;
    let value = bytes.get(offset + 2..offset + 2 + size)?;
    Some(String::from_utf8_lossy(value).into_owned())
}

pub(in crate::mapi) fn write_u64(body: &mut Vec<u8>, value: u64) {
    body.extend_from_slice(&value.to_le_bytes());
}

pub(in crate::mapi) fn write_object_id(body: &mut Vec<u8>, value: u64) {
    if let Some(bytes) = crate::mapi::identity::wire_id_bytes_from_object_id(value) {
        body.extend_from_slice(&bytes);
    } else {
        write_u64(body, value);
    }
}

pub(in crate::mapi) fn write_utf16z(body: &mut Vec<u8>, value: &str) {
    for unit in value.encode_utf16() {
        body.extend_from_slice(&unit.to_le_bytes());
    }
    body.extend_from_slice(&0u16.to_le_bytes());
}

pub(in crate::mapi) fn write_typed_string(body: &mut Vec<u8>, value: &str) {
    if value.is_empty() {
        body.push(0x01);
    } else {
        body.push(0x04);
        write_utf16z(body, value);
    }
}

pub(in crate::mapi) struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    pub(in crate::mapi) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    pub(in crate::mapi) fn read_u32(&mut self) -> Result<u32> {
        let bytes = self.read_bytes(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub(in crate::mapi) fn read_i32(&mut self) -> Result<i32> {
        let bytes = self.read_bytes(4)?;
        Ok(i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub(in crate::mapi) fn read_i64(&mut self) -> Result<i64> {
        let bytes = self.read_bytes(8)?;
        Ok(i64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    pub(in crate::mapi) fn read_u16(&mut self) -> Result<u16> {
        let bytes = self.read_bytes(2)?;
        Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    pub(in crate::mapi) fn read_u8(&mut self) -> Result<u8> {
        let bytes = self.read_bytes(1)?;
        Ok(bytes[0])
    }

    pub(in crate::mapi) fn read_bytes(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(len)
            .ok_or_else(|| anyhow!("request body offset overflow"))?;
        let bytes = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| anyhow!("request body is truncated"))?;
        self.position = end;
        Ok(bytes)
    }

    pub(in crate::mapi) fn read_ascii_z(&mut self) -> Result<String> {
        let start = self.position;
        while self.remaining() > 0 {
            if self.bytes[self.position] == 0 {
                let bytes = &self.bytes[start..self.position];
                self.position += 1;
                return Ok(String::from_utf8_lossy(bytes).into_owned());
            }
            self.position += 1;
        }
        Err(anyhow!("unterminated ASCII string"))
    }

    pub(in crate::mapi) fn read_utf16z(&mut self) -> Result<String> {
        let mut units = Vec::new();
        loop {
            let unit = self.read_u16()?;
            if unit == 0 {
                return String::from_utf16(&units)
                    .map_err(|_| anyhow!("invalid UTF-16 string in restriction"));
            }
            units.push(unit);
        }
    }

    pub(in crate::mapi) fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.position)
    }

    pub(in crate::mapi) fn remaining_is_zero_padding(&self) -> bool {
        let remaining = self.remaining();
        remaining <= 2 && self.bytes[self.position..].iter().all(|byte| *byte == 0)
    }

    pub(in crate::mapi) fn position(&self) -> usize {
        self.position
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) struct RopRequest {
    pub(in crate::mapi) rop_id: u8,
    pub(in crate::mapi) input_handle_index: Option<u8>,
    pub(in crate::mapi) output_handle_index: Option<u8>,
    pub(in crate::mapi) payload: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) enum TypedRopRequest {
    Release(RopInputOnlyRequest),
    OpenFolder(RopOpenFolderRequest),
    OpenMessage(RopOpenMessageRequest),
    OpenTable(RopOpenTableRequest),
    CreateMessage(RopCreateMessageRequest),
    SaveChangesMessage(RopSaveChangesMessageRequest),
    OpenEmbeddedMessage(RopOpenEmbeddedMessageRequest),
    SetColumns(RopSetColumnsRequest),
    Restrict(RopRestrictionRequest),
    QueryRows(RopQueryRowsRequest),
    Logon(RopLogonRequest),
    SupportedRaw(RopSupportedRawRequest),
    Unsupported(RopUnsupportedRequest),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) struct RopInputOnlyRequest {
    pub(in crate::mapi) rop_id: u8,
    pub(in crate::mapi) input_handle_index: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) struct RopOpenFolderRequest {
    pub(in crate::mapi) input_handle_index: u8,
    pub(in crate::mapi) output_handle_index: u8,
    pub(in crate::mapi) folder_id: u64,
    pub(in crate::mapi) open_mode_flags: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) struct RopOpenMessageRequest {
    pub(in crate::mapi) input_handle_index: u8,
    pub(in crate::mapi) output_handle_index: u8,
    pub(in crate::mapi) folder_id: u64,
    pub(in crate::mapi) open_mode_flags: u8,
    pub(in crate::mapi) message_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) struct RopOpenTableRequest {
    pub(in crate::mapi) rop_id: u8,
    pub(in crate::mapi) input_handle_index: u8,
    pub(in crate::mapi) output_handle_index: u8,
    pub(in crate::mapi) table_flags: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) struct RopCreateMessageRequest {
    pub(in crate::mapi) input_handle_index: u8,
    pub(in crate::mapi) output_handle_index: u8,
    pub(in crate::mapi) folder_id: u64,
    pub(in crate::mapi) associated_flag: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) struct RopSaveChangesMessageRequest {
    pub(in crate::mapi) response_handle_index: u8,
    pub(in crate::mapi) input_handle_index: u8,
    pub(in crate::mapi) save_flags: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) struct RopOpenEmbeddedMessageRequest {
    pub(in crate::mapi) input_handle_index: u8,
    pub(in crate::mapi) output_handle_index: u8,
    pub(in crate::mapi) code_page_id: u16,
    pub(in crate::mapi) open_mode_flags: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) struct RopSetColumnsRequest {
    pub(in crate::mapi) input_handle_index: u8,
    pub(in crate::mapi) flags: u8,
    pub(in crate::mapi) property_tags: Vec<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) struct RopRestrictionRequest {
    pub(in crate::mapi) rop_id: u8,
    pub(in crate::mapi) input_handle_index: u8,
    pub(in crate::mapi) flags: u8,
    pub(in crate::mapi) restriction: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) struct RopQueryRowsRequest {
    pub(in crate::mapi) input_handle_index: u8,
    pub(in crate::mapi) flags: u8,
    pub(in crate::mapi) forward_read: bool,
    pub(in crate::mapi) row_count: u16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) struct RopLogonRequest {
    pub(in crate::mapi) output_handle_index: u8,
    pub(in crate::mapi) logon_flags: u8,
    pub(in crate::mapi) prefix: Vec<u8>,
    pub(in crate::mapi) essdn: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) struct RopSupportedRawRequest {
    pub(in crate::mapi) rop_id: u8,
    pub(in crate::mapi) input_handle_index: Option<u8>,
    pub(in crate::mapi) output_handle_index: Option<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) struct RopUnsupportedRequest {
    pub(in crate::mapi) rop_id: u8,
    pub(in crate::mapi) input_handle_index: Option<u8>,
    pub(in crate::mapi) reserved: bool,
}

impl TypedRopRequest {
    pub(in crate::mapi) fn rop_id(&self) -> u8 {
        match self {
            Self::Release(request) => request.rop_id,
            Self::OpenFolder(_) => 0x02,
            Self::OpenMessage(_) => 0x03,
            Self::OpenTable(request) => request.rop_id,
            Self::CreateMessage(_) => 0x06,
            Self::SaveChangesMessage(_) => 0x0C,
            Self::OpenEmbeddedMessage(_) => 0x46,
            Self::SetColumns(_) => 0x12,
            Self::Restrict(request) => request.rop_id,
            Self::QueryRows(_) => 0x15,
            Self::Logon(_) => 0xFE,
            Self::SupportedRaw(request) => request.rop_id,
            Self::Unsupported(request) => request.rop_id,
        }
    }

    pub(in crate::mapi) fn unsupported_is_terminal(&self) -> bool {
        matches!(self, Self::Unsupported(_))
    }
}

impl RopRequest {
    pub(in crate::mapi) fn typed(&self) -> TypedRopRequest {
        match RopId::from_u8(self.rop_id) {
            Some(RopId::Release) => TypedRopRequest::Release(RopInputOnlyRequest {
                rop_id: self.rop_id,
                input_handle_index: self.input_handle_index.unwrap_or(0),
            }),
            Some(RopId::OpenFolder) => TypedRopRequest::OpenFolder(RopOpenFolderRequest {
                input_handle_index: self.input_handle_index.unwrap_or(0),
                output_handle_index: self.output_handle_index.unwrap_or(0),
                folder_id: self.folder_id().unwrap_or(0),
                open_mode_flags: self.payload.get(8).copied().unwrap_or(0),
            }),
            Some(RopId::OpenMessage) => TypedRopRequest::OpenMessage(RopOpenMessageRequest {
                input_handle_index: self.input_handle_index.unwrap_or(0),
                output_handle_index: self.output_handle_index.unwrap_or(0),
                folder_id: self.folder_id().unwrap_or(0),
                open_mode_flags: self.payload.get(8).copied().unwrap_or(0),
                message_id: self.message_id().unwrap_or(0),
            }),
            Some(
                RopId::GetHierarchyTable | RopId::GetContentsTable | RopId::GetAttachmentTable,
            ) => TypedRopRequest::OpenTable(RopOpenTableRequest {
                rop_id: self.rop_id,
                input_handle_index: self.input_handle_index.unwrap_or(0),
                output_handle_index: self.output_handle_index.unwrap_or(0),
                table_flags: self.payload.first().copied().unwrap_or(0),
            }),
            Some(RopId::CreateMessage) => TypedRopRequest::CreateMessage(RopCreateMessageRequest {
                input_handle_index: self.input_handle_index.unwrap_or(0),
                output_handle_index: self.output_handle_index.unwrap_or(0),
                folder_id: self.folder_id().unwrap_or(0),
                associated_flag: self.payload.get(8).copied().unwrap_or(0),
            }),
            Some(RopId::SaveChangesMessage) => {
                TypedRopRequest::SaveChangesMessage(RopSaveChangesMessageRequest {
                    response_handle_index: self.output_handle_index.unwrap_or(0),
                    input_handle_index: self.input_handle_index.unwrap_or(0),
                    save_flags: self.payload.first().copied().unwrap_or(0),
                })
            }
            Some(RopId::OpenEmbeddedMessage) => {
                TypedRopRequest::OpenEmbeddedMessage(RopOpenEmbeddedMessageRequest {
                    input_handle_index: self.input_handle_index.unwrap_or(0),
                    output_handle_index: self.output_handle_index.unwrap_or(0),
                    code_page_id: self
                        .payload
                        .get(..2)
                        .and_then(|bytes| bytes.try_into().ok())
                        .map(u16::from_le_bytes)
                        .unwrap_or(0),
                    open_mode_flags: self.payload.get(2).copied().unwrap_or(0),
                })
            }
            Some(RopId::SetColumns) => TypedRopRequest::SetColumns(RopSetColumnsRequest {
                input_handle_index: self.input_handle_index.unwrap_or(0),
                flags: self.payload.first().copied().unwrap_or(0),
                property_tags: self.property_tags(),
            }),
            Some(RopId::Restrict | RopId::FindRow) => {
                let size = self
                    .payload
                    .get(1..3)
                    .and_then(|bytes| bytes.try_into().ok())
                    .map(u16::from_le_bytes)
                    .map(usize::from)
                    .unwrap_or(0);
                TypedRopRequest::Restrict(RopRestrictionRequest {
                    rop_id: self.rop_id,
                    input_handle_index: self.input_handle_index.unwrap_or(0),
                    flags: self.payload.first().copied().unwrap_or(0),
                    restriction: self.payload.get(3..3 + size).unwrap_or_default().to_vec(),
                })
            }
            Some(RopId::QueryRows) => TypedRopRequest::QueryRows(RopQueryRowsRequest {
                input_handle_index: self.input_handle_index.unwrap_or(0),
                flags: self.payload.first().copied().unwrap_or(0),
                forward_read: self.query_forward_read(),
                row_count: self.query_row_count().unwrap_or(0).min(u16::MAX as usize) as u16,
            }),
            Some(RopId::Logon) => {
                let essdn_size = self
                    .payload
                    .get(9..11)
                    .and_then(|bytes| bytes.try_into().ok())
                    .map(u16::from_le_bytes)
                    .map(usize::from)
                    .unwrap_or(0);
                TypedRopRequest::Logon(RopLogonRequest {
                    output_handle_index: self.output_handle_index.unwrap_or(0),
                    logon_flags: self.payload.first().copied().unwrap_or(0),
                    prefix: self.payload.get(1..9).unwrap_or_default().to_vec(),
                    essdn: self
                        .payload
                        .get(11..11 + essdn_size)
                        .unwrap_or_default()
                        .to_vec(),
                })
            }
            Some(rop_id) if rop_id.is_supported_by_dispatch() => {
                TypedRopRequest::SupportedRaw(RopSupportedRawRequest {
                    rop_id: self.rop_id,
                    input_handle_index: self.input_handle_index,
                    output_handle_index: self.output_handle_index,
                })
            }
            _ => TypedRopRequest::Unsupported(RopUnsupportedRequest {
                rop_id: self.rop_id,
                input_handle_index: self.input_handle_index,
                reserved: rop_id_is_reserved(self.rop_id),
            }),
        }
    }

    pub(in crate::mapi) fn input_handle_index(&self) -> Option<u8> {
        self.input_handle_index
    }

    pub(in crate::mapi) fn output_handle_index(&self) -> Option<u8> {
        self.output_handle_index
    }

    pub(in crate::mapi) fn response_handle_index(&self) -> u8 {
        if matches!(
            self.rop_id,
            0x02 | 0x03
                | 0x04
                | 0x05
                | 0x06
                | 0x0C
                | 0x11
                | 0x1C
                | 0x21
                | 0x22
                | 0x2B
                | 0x25
                | 0x29
                | 0x3B
                | 0x3E
                | 0x3F
                | 0x46
                | 0x4B
                | 0x4C
                | 0x4D
                | 0x53
                | 0x69
                | 0x70
                | 0x72
                | 0x7E
                | 0x82
        ) {
            return self.output_handle_index.unwrap_or(0);
        }
        self.input_handle_index
            .unwrap_or(self.output_handle_index.unwrap_or(0))
    }

    pub(in crate::mapi) fn folder_id(&self) -> Option<u64> {
        if !matches!(
            RopId::from_u8(self.rop_id),
            Some(RopId::OpenFolder | RopId::OpenMessage | RopId::CreateMessage)
        ) {
            return None;
        }
        let bytes = self.payload.get(..8)?;
        crate::mapi::identity::object_id_from_wire_id(bytes)
    }

    pub(in crate::mapi) fn create_message_associated(&self) -> bool {
        matches!(RopId::from_u8(self.rop_id), Some(RopId::CreateMessage))
            && self.payload.get(8).is_some_and(|flag| *flag != 0)
    }

    pub(in crate::mapi) fn abort_submit_folder_id(&self) -> Option<u64> {
        if !matches!(RopId::from_u8(self.rop_id), Some(RopId::AbortSubmit)) {
            return None;
        }
        let bytes = self.payload.get(..8)?;
        crate::mapi::identity::object_id_from_wire_id(bytes)
    }

    pub(in crate::mapi) fn abort_submit_message_id(&self) -> Option<u64> {
        if !matches!(RopId::from_u8(self.rop_id), Some(RopId::AbortSubmit)) {
            return None;
        }
        let bytes = self.payload.get(8..16)?;
        crate::mapi::identity::object_id_from_wire_id(bytes)
            .or_else(|| bytes.try_into().ok().map(u64::from_le_bytes))
    }

    pub(in crate::mapi) fn long_term_source_object_id(&self) -> Option<u64> {
        if !matches!(RopId::from_u8(self.rop_id), Some(RopId::LongTermIdFromId)) {
            return None;
        }
        let bytes = self.payload.get(..8)?;
        crate::mapi::identity::object_id_from_wire_id(bytes)
            .or_else(|| crate::mapi::identity::object_id_from_trailing_replid_wire_id(bytes))
            .or_else(|| stale_special_folder_object_id_from_short_id(bytes))
    }

    pub(in crate::mapi) fn long_term_source_id_bytes(&self) -> Option<&[u8]> {
        self.payload.get(..8)
    }

    pub(in crate::mapi) fn public_folder_probe_object_id(&self) -> Option<u64> {
        if !matches!(
            RopId::from_u8(self.rop_id),
            Some(RopId::GetOwningServers | RopId::PublicFolderIsGhosted)
        ) {
            return None;
        }
        let bytes = self.payload.get(..8)?;
        crate::mapi::identity::object_id_from_wire_id(bytes)
            .or_else(|| crate::mapi::identity::object_id_from_trailing_replid_wire_id(bytes))
    }

    pub(in crate::mapi) fn modify_permissions_count(&self) -> Option<u16> {
        if self.rop_id != 0x40 {
            return None;
        }
        let bytes = self.payload.get(1..3)?;
        Some(u16::from_le_bytes(bytes.try_into().ok()?))
    }

    pub(in crate::mapi) fn modify_rules_count(&self) -> Option<u16> {
        if self.rop_id != 0x41 {
            return None;
        }
        let bytes = self.payload.get(1..3)?;
        Some(u16::from_le_bytes(bytes.try_into().ok()?))
    }

    pub(in crate::mapi) fn modify_rules_rows(&self) -> Result<Vec<ModifyRulesRow>> {
        if self.rop_id != 0x41 {
            return Ok(Vec::new());
        }
        let count = self.modify_rules_count().unwrap_or(0) as usize;
        let mut cursor = Cursor::new(self.payload.get(3..).unwrap_or_default());
        let mut rows = Vec::with_capacity(count);
        for _ in 0..count {
            let flags = cursor.read_u8()?;
            let property_count = cursor.read_u16()? as usize;
            let mut properties = HashMap::new();
            for _ in 0..property_count {
                let (property_tag, value) = parse_tagged_property(&mut cursor)?;
                properties.insert(property_tag, value);
            }
            rows.push(ModifyRulesRow { flags, properties });
        }
        Ok(rows)
    }

    pub(in crate::mapi) fn modify_permissions_rows(&self) -> Result<Vec<ModifyRulesRow>> {
        if self.rop_id != 0x40 {
            return Ok(Vec::new());
        }
        let count = self.modify_permissions_count().unwrap_or(0) as usize;
        let mut cursor = Cursor::new(self.payload.get(3..).unwrap_or_default());
        let mut rows = Vec::with_capacity(count);
        for _ in 0..count {
            let flags = cursor.read_u8()?;
            let property_count = cursor.read_u16()? as usize;
            let mut properties = HashMap::new();
            for _ in 0..property_count {
                let (property_tag, value) = parse_tagged_property(&mut cursor)?;
                properties.insert(property_tag, value);
            }
            rows.push(ModifyRulesRow { flags, properties });
        }
        Ok(rows)
    }

    pub(in crate::mapi) fn notification_types(&self) -> Option<u16> {
        if self.rop_id != 0x29 {
            return None;
        }
        let bytes = self.payload.get(..2)?;
        Some(u16::from_le_bytes(bytes.try_into().ok()?))
    }

    pub(in crate::mapi) fn notification_want_whole_store(&self) -> Option<bool> {
        if self.rop_id != 0x29 {
            return None;
        }
        let offset = if self.notification_types()? & 0x0400 != 0 {
            3
        } else {
            2
        };
        Some(self.payload.get(offset).copied()? != 0)
    }

    pub(in crate::mapi) fn notification_folder_id(&self) -> Option<u64> {
        if self.rop_id != 0x29 || self.notification_want_whole_store()? {
            return None;
        }
        let offset = if self.notification_types()? & 0x0400 != 0 {
            4
        } else {
            3
        };
        let bytes = self.payload.get(offset..offset + 8)?;
        crate::mapi::identity::object_id_from_wire_id(bytes)
    }

    pub(in crate::mapi) fn message_id(&self) -> Option<u64> {
        if !matches!(RopId::from_u8(self.rop_id), Some(RopId::OpenMessage)) {
            return None;
        }
        let bytes = self.payload.get(9..17)?;
        crate::mapi::identity::object_id_from_wire_id(bytes)
    }

    pub(in crate::mapi) fn row_id(&self) -> Option<u32> {
        let bytes = self.payload.get(..4)?;
        Some(u32::from_le_bytes(bytes.try_into().ok()?))
    }

    pub(in crate::mapi) fn read_recipients_reserved(&self) -> Option<u16> {
        if !matches!(RopId::from_u8(self.rop_id), Some(RopId::ReadRecipients)) {
            return None;
        }
        let bytes = self.payload.get(4..6)?;
        Some(u16::from_le_bytes(bytes.try_into().ok()?))
    }

    pub(in crate::mapi) fn attach_num(&self) -> Option<u32> {
        let bytes = if self.rop_id == 0x24 {
            self.payload.get(..4)?
        } else {
            self.payload.get(1..5)?
        };
        Some(u32::from_le_bytes(bytes.try_into().ok()?))
    }

    pub(in crate::mapi) fn stream_property_tag(&self) -> Option<u32> {
        let bytes = self.payload.get(..4)?;
        Some(u32::from_le_bytes(bytes.try_into().ok()?))
    }

    pub(in crate::mapi) fn stream_open_mode(&self) -> Option<u8> {
        self.payload.get(4).copied()
    }

    pub(in crate::mapi) fn read_byte_count(&self) -> Option<usize> {
        let bytes = self.payload.get(..2)?;
        let byte_count = u16::from_le_bytes(bytes.try_into().ok()?);
        if byte_count == 0xBABE {
            let maximum = self.payload.get(2..6)?;
            let maximum = u32::from_le_bytes(maximum.try_into().ok()?);
            return Some((maximum as usize).min(u16::MAX as usize));
        }
        Some(usize::from(byte_count))
    }

    pub(in crate::mapi) fn stream_write_data(&self) -> &[u8] {
        let size = self
            .payload
            .get(..2)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .map(usize::from)
            .unwrap_or(0);
        self.payload.get(2..2 + size).unwrap_or_default()
    }

    pub(in crate::mapi) fn stream_seek_origin(&self) -> Option<u8> {
        self.payload.first().copied()
    }

    pub(in crate::mapi) fn stream_seek_offset(&self) -> Option<i64> {
        let bytes = self.payload.get(1..9)?;
        Some(i64::from_le_bytes(bytes.try_into().ok()?))
    }

    pub(in crate::mapi) fn stream_size(&self) -> Option<u64> {
        let bytes = self.payload.get(..8)?;
        Some(u64::from_le_bytes(bytes.try_into().ok()?))
    }

    pub(in crate::mapi) fn read_flags(&self) -> Option<u8> {
        match self.rop_id {
            0x11 => self.payload.first().copied(),
            0x66 => self.payload.get(1).copied(),
            _ => None,
        }
    }

    pub(in crate::mapi) fn want_asynchronous(&self) -> Option<u8> {
        if matches!(RopId::from_u8(self.rop_id), Some(RopId::SetReadFlags)) {
            self.payload.first().copied()
        } else {
            None
        }
    }

    pub(in crate::mapi) fn sync_type(&self) -> u8 {
        self.payload.first().copied().unwrap_or(0)
    }

    pub(in crate::mapi) fn sync_send_options(&self) -> u8 {
        self.payload.get(1).copied().unwrap_or(0)
    }

    pub(in crate::mapi) fn sync_flags(&self) -> u16 {
        self.payload
            .get(2..4)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .unwrap_or(0)
    }

    pub(in crate::mapi) fn sync_extra_flags(&self) -> u32 {
        let restriction_size = self
            .payload
            .get(4..6)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .unwrap_or(0) as usize;
        self.payload
            .get(6 + restriction_size..10 + restriction_size)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u32::from_le_bytes)
            .unwrap_or(0)
    }

    pub(in crate::mapi) fn sync_property_tags(&self) -> Vec<u32> {
        let restriction_size = self
            .payload
            .get(4..6)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .unwrap_or(0) as usize;
        let count_offset = 10 + restriction_size;
        let count = self
            .payload
            .get(count_offset..count_offset + 2)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .unwrap_or(0) as usize;
        self.payload
            .get(count_offset + 2..)
            .unwrap_or_default()
            .chunks_exact(4)
            .take(count)
            .map(|bytes| u32::from_le_bytes(bytes.try_into().unwrap_or_default()))
            .collect()
    }

    pub(in crate::mapi) fn fast_transfer_buffer_size(&self) -> usize {
        let requested = self
            .payload
            .get(..2)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .unwrap_or(u16::MAX);
        if requested == 0xBABE {
            return self
                .payload
                .get(2..4)
                .and_then(|bytes| bytes.try_into().ok())
                .map(u16::from_le_bytes)
                .map(usize::from)
                .unwrap_or(u16::MAX as usize);
        }
        usize::from(requested)
    }

    pub(in crate::mapi) fn stream_data(&self) -> &[u8] {
        let Some(size_bytes) = self.payload.get(..4) else {
            return &[];
        };
        let size = u32::from_le_bytes([size_bytes[0], size_bytes[1], size_bytes[2], size_bytes[3]])
            as usize;
        self.payload.get(4..4 + size).unwrap_or_default()
    }

    pub(in crate::mapi) fn fast_transfer_upload_data(&self) -> &[u8] {
        if !matches!(
            RopId::from_u8(self.rop_id),
            Some(
                RopId::FastTransferDestinationPutBuffer
                    | RopId::FastTransferDestinationPutBufferExtended
            )
        ) {
            return &[];
        }
        let Some(size_bytes) = self.payload.get(..2) else {
            return &[];
        };
        let size = u16::from_le_bytes([size_bytes[0], size_bytes[1]]) as usize;
        self.payload.get(2..2 + size).unwrap_or_default()
    }

    pub(in crate::mapi) fn upload_state_property_tag(&self) -> Option<u32> {
        self.payload
            .get(..4)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u32::from_le_bytes)
    }

    pub(in crate::mapi) fn upload_state_transfer_size(&self) -> Option<u32> {
        self.payload
            .get(4..8)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u32::from_le_bytes)
    }

    pub(in crate::mapi) fn import_message_id(&self) -> Option<u64> {
        if !matches!(
            RopId::from_u8(self.rop_id),
            Some(RopId::SynchronizationImportMessageChange)
        ) {
            return None;
        }
        self.import_property_values()
            .ok()?
            .into_iter()
            .find_map(|(tag, value)| match (tag, value) {
                (PID_TAG_SOURCE_KEY, MapiValue::Binary(bytes)) => {
                    crate::mapi::identity::object_id_from_source_key(&bytes)
                }
                _ => None,
            })
    }

    pub(in crate::mapi) fn import_flag(&self) -> Option<u8> {
        self.payload.first().copied()
    }

    pub(in crate::mapi) fn import_property_values(&self) -> Result<Vec<(u32, MapiValue)>> {
        let property_payload = self
            .payload
            .get(1..)
            .ok_or_else(|| anyhow!("missing import property payload"))?;
        let mut cursor = Cursor::new(property_payload);
        let property_value_count = cursor.read_u16()? as usize;
        let mut values = Vec::with_capacity(property_value_count);
        for _ in 0..property_value_count {
            values.push(parse_tagged_property(&mut cursor)?);
        }
        Ok(values)
    }

    pub(in crate::mapi) fn import_hierarchy_values(
        &self,
    ) -> Result<(Vec<(u32, MapiValue)>, Vec<(u32, MapiValue)>)> {
        let mut cursor = Cursor::new(self.payload.as_slice());
        let hierarchy_count = cursor.read_u16()? as usize;
        let mut hierarchy_values = Vec::with_capacity(hierarchy_count);
        for _ in 0..hierarchy_count {
            hierarchy_values.push(parse_tagged_property(&mut cursor)?);
        }
        let property_count = cursor.read_u16()? as usize;
        let mut property_values = Vec::with_capacity(property_count);
        for _ in 0..property_count {
            property_values.push(parse_tagged_property(&mut cursor)?);
        }
        Ok((hierarchy_values, property_values))
    }

    pub(in crate::mapi) fn import_delete_message_ids(&self) -> Vec<u64> {
        if !matches!(
            RopId::from_u8(self.rop_id),
            Some(RopId::SynchronizationImportDeletes)
        ) {
            return Vec::new();
        }
        let count = self
            .payload
            .get(1..3)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .unwrap_or(0) as usize;
        self.payload
            .get(3..)
            .unwrap_or_default()
            .chunks_exact(8)
            .take(count)
            .filter_map(crate::mapi::identity::object_id_from_wire_id)
            .collect()
    }

    pub(in crate::mapi) fn import_delete_hard_delete(&self) -> bool {
        self.payload.first().is_some_and(|flags| flags & 0x02 != 0)
    }

    pub(in crate::mapi) fn fast_transfer_message_ids(&self) -> Vec<u64> {
        if !matches!(
            RopId::from_u8(self.rop_id),
            Some(RopId::FastTransferSourceCopyMessages)
        ) {
            return Vec::new();
        }
        let count = self
            .payload
            .get(..2)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .map(usize::from)
            .unwrap_or(0);
        self.payload
            .get(2..)
            .unwrap_or_default()
            .chunks_exact(8)
            .take(count)
            .filter_map(crate::mapi::identity::object_id_from_wire_id)
            .collect()
    }

    pub(in crate::mapi) fn import_move(&self) -> Option<(u64, u64)> {
        if !matches!(
            RopId::from_u8(self.rop_id),
            Some(RopId::SynchronizationImportMessageMove)
        ) {
            return None;
        }
        let mut cursor = Cursor::new(&self.payload);
        let source_folder_id_size = cursor.read_u32().ok()? as usize;
        let source_folder_id = cursor.read_bytes(source_folder_id_size).ok()?;
        let source_folder_id = crate::mapi::identity::object_id_from_wire_id(source_folder_id)?;
        let source_message_id_size = cursor.read_u32().ok()? as usize;
        let source_message_id = cursor.read_bytes(source_message_id_size).ok()?;
        let source_message_id = crate::mapi::identity::object_id_from_wire_id(source_message_id)?;
        Some((source_folder_id, source_message_id))
    }

    pub(in crate::mapi) fn import_read_state_changes(&self) -> Vec<(u64, bool)> {
        if !matches!(
            RopId::from_u8(self.rop_id),
            Some(RopId::SynchronizationImportReadStateChanges)
        ) {
            return Vec::new();
        }
        let size = self
            .payload
            .get(..2)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .unwrap_or(0) as usize;
        if self.payload.len() == 2 + size.saturating_mul(9) {
            return self.payload[2..]
                .chunks_exact(9)
                .filter_map(|chunk| {
                    crate::mapi::identity::object_id_from_wire_id(&chunk[..8])
                        .map(|message_id| (message_id, chunk[8] == 0))
                })
                .collect();
        }
        let mut cursor = Cursor::new(self.payload.get(2..2 + size).unwrap_or_default());
        let mut changes = Vec::new();
        while cursor.remaining() >= 3 {
            let Ok(message_id_size) = cursor.read_u16().map(usize::from) else {
                break;
            };
            let Ok(message_id_bytes) = cursor.read_bytes(message_id_size) else {
                break;
            };
            let Ok(mark_as_read) = cursor.read_u8() else {
                break;
            };
            if let Some(message_id) =
                crate::mapi::identity::object_id_from_wire_id(message_id_bytes)
                    .or_else(|| crate::mapi::identity::object_id_from_source_key(message_id_bytes))
            {
                changes.push((message_id, mark_as_read == 0));
            }
        }
        changes
    }

    pub(in crate::mapi) fn local_replica_midset_deleted(&self) -> &[u8] {
        self.payload.as_slice()
    }

    pub(in crate::mapi) fn search_criteria_restriction_bytes(&self) -> Option<&[u8]> {
        if !matches!(RopId::from_u8(self.rop_id), Some(RopId::SetSearchCriteria)) {
            return None;
        }
        let size = u16::from_le_bytes(self.payload.get(..2)?.try_into().ok()?) as usize;
        self.payload.get(2..2 + size)
    }

    pub(in crate::mapi) fn search_criteria_folder_ids(&self) -> Option<Vec<u64>> {
        if !matches!(RopId::from_u8(self.rop_id), Some(RopId::SetSearchCriteria)) {
            return None;
        }
        let size = u16::from_le_bytes(self.payload.get(..2)?.try_into().ok()?) as usize;
        let count_offset = 2 + size;
        let count = u16::from_le_bytes(
            self.payload
                .get(count_offset..count_offset + 2)?
                .try_into()
                .ok()?,
        ) as usize;
        let ids_offset = count_offset + 2;
        self.payload
            .get(ids_offset..ids_offset + count * 8)?
            .chunks_exact(8)
            .map(crate::mapi::identity::object_id_from_wire_id)
            .collect()
    }

    pub(in crate::mapi) fn search_criteria_flags(&self) -> Option<u32> {
        if !matches!(RopId::from_u8(self.rop_id), Some(RopId::SetSearchCriteria)) {
            return None;
        }
        let size = u16::from_le_bytes(self.payload.get(..2)?.try_into().ok()?) as usize;
        let count_offset = 2 + size;
        let count = u16::from_le_bytes(
            self.payload
                .get(count_offset..count_offset + 2)?
                .try_into()
                .ok()?,
        ) as usize;
        let flags_offset = count_offset + 2 + count * 8;
        Some(u32::from_le_bytes(
            self.payload
                .get(flags_offset..flags_offset + 4)?
                .try_into()
                .ok()?,
        ))
    }

    pub(in crate::mapi) fn get_search_criteria_include_restriction(&self) -> bool {
        matches!(RopId::from_u8(self.rop_id), Some(RopId::GetSearchCriteria))
            && self.payload.get(1).copied().unwrap_or(0) != 0
    }

    pub(in crate::mapi) fn get_search_criteria_use_unicode(&self) -> bool {
        matches!(RopId::from_u8(self.rop_id), Some(RopId::GetSearchCriteria))
            && self.payload.first().copied().unwrap_or(0) != 0
    }

    pub(in crate::mapi) fn get_search_criteria_include_folders(&self) -> bool {
        matches!(RopId::from_u8(self.rop_id), Some(RopId::GetSearchCriteria))
            && self.payload.get(2).copied().unwrap_or(0) != 0
    }

    pub(in crate::mapi) fn receive_folder_message_class(&self) -> Option<&str> {
        let bytes = self.payload.strip_suffix(&[0])?;
        std::str::from_utf8(bytes).ok()
    }

    pub(in crate::mapi) fn set_receive_folder_id(&self) -> Option<u64> {
        if !matches!(RopId::from_u8(self.rop_id), Some(RopId::SetReceiveFolder)) {
            return None;
        }
        crate::mapi::identity::object_id_from_wire_id(self.payload.get(..8)?)
    }

    pub(in crate::mapi) fn set_receive_folder_message_class(&self) -> Option<&str> {
        if !matches!(RopId::from_u8(self.rop_id), Some(RopId::SetReceiveFolder)) {
            return None;
        }
        let bytes = self.payload.get(8..)?.strip_suffix(&[0])?;
        std::str::from_utf8(bytes).ok()
    }

    pub(in crate::mapi) fn local_replica_id_count(&self) -> u32 {
        self.payload
            .get(..4)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u32::from_le_bytes)
            .unwrap_or(1)
    }

    pub(in crate::mapi) fn long_term_id(&self) -> Option<&[u8]> {
        self.payload.get(..24)
    }

    pub(in crate::mapi) fn per_user_folder_object_id(&self) -> Option<u64> {
        if !matches!(
            RopId::from_u8(self.rop_id),
            Some(RopId::ReadPerUserInformation | RopId::WritePerUserInformation)
        ) {
            return None;
        }
        crate::mapi::identity::object_id_from_long_term_id(self.payload.get(..24)?)
    }

    pub(in crate::mapi) fn per_user_data_offset(&self) -> u32 {
        self.payload
            .get(25..29)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u32::from_le_bytes)
            .unwrap_or(0)
    }

    pub(in crate::mapi) fn per_user_max_data_size(&self) -> u16 {
        self.payload
            .get(29..31)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .unwrap_or(0)
    }

    pub(in crate::mapi) fn per_user_has_finished(&self) -> bool {
        self.payload.get(24).copied().unwrap_or(0) != 0
    }

    pub(in crate::mapi) fn per_user_write_data(&self) -> &[u8] {
        let size = self.per_user_max_data_size() as usize;
        self.payload.get(31..31 + size).unwrap_or_default()
    }

    pub(in crate::mapi) fn message_ids(&self) -> Vec<u64> {
        if !matches!(
            RopId::from_u8(self.rop_id),
            Some(RopId::DeleteMessages | RopId::HardDeleteMessages | RopId::SetReadFlags)
        ) {
            return Vec::new();
        }
        let (count_offset, ids_offset) = (2, 4);
        let Some(count_bytes) = self.payload.get(count_offset..count_offset + 2) else {
            return Vec::new();
        };
        let count = u16::from_le_bytes([count_bytes[0], count_bytes[1]]) as usize;
        self.payload[ids_offset..]
            .chunks_exact(8)
            .take(count)
            .filter_map(crate::mapi::identity::object_id_from_wire_id)
            .collect()
    }

    pub(in crate::mapi) fn delete_messages_want_asynchronous(&self) -> Option<u8> {
        if !matches!(
            RopId::from_u8(self.rop_id),
            Some(RopId::DeleteMessages | RopId::HardDeleteMessages)
        ) {
            return None;
        }
        self.payload.first().copied()
    }

    pub(in crate::mapi) fn delete_messages_notify_non_read(&self) -> Option<u8> {
        if !matches!(
            RopId::from_u8(self.rop_id),
            Some(RopId::DeleteMessages | RopId::HardDeleteMessages)
        ) {
            return None;
        }
        self.payload.get(1).copied()
    }

    pub(in crate::mapi) fn status_message_id(&self) -> Option<u64> {
        if !matches!(
            RopId::from_u8(self.rop_id),
            Some(RopId::GetMessageStatus | RopId::SetMessageStatus)
        ) {
            return None;
        }
        let bytes = self.payload.get(..8)?;
        crate::mapi::identity::object_id_from_wire_id(bytes)
    }

    pub(in crate::mapi) fn message_status_flags(&self) -> u32 {
        self.payload
            .get(8..12)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u32::from_le_bytes)
            .unwrap_or(0)
    }

    pub(in crate::mapi) fn message_status_mask(&self) -> u32 {
        self.payload
            .get(12..16)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u32::from_le_bytes)
            .unwrap_or(0)
    }

    pub(in crate::mapi) fn reload_cached_information_reserved(&self) -> Option<u16> {
        if !matches!(
            RopId::from_u8(self.rop_id),
            Some(RopId::ReloadCachedInformation)
        ) {
            return None;
        }
        let bytes = self.payload.get(..2)?;
        Some(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    pub(in crate::mapi) fn create_folder_type(&self) -> u8 {
        self.payload.first().copied().unwrap_or(0)
    }

    pub(in crate::mapi) fn create_folder_open_existing(&self) -> bool {
        self.payload
            .get(2)
            .is_some_and(|open_existing| *open_existing != 0)
    }

    pub(in crate::mapi) fn create_folder_reserved(&self) -> u8 {
        self.payload.get(3).copied().unwrap_or(0)
    }

    pub(in crate::mapi) fn create_folder_display_name(&self) -> String {
        read_u16_prefixed_string(&self.payload, 4).unwrap_or_default()
    }

    pub(in crate::mapi) fn delete_folder_flags(&self) -> Option<u8> {
        if !matches!(RopId::from_u8(self.rop_id), Some(RopId::DeleteFolder)) {
            return None;
        }
        self.payload.first().copied()
    }

    pub(in crate::mapi) fn delete_folder_id(&self) -> Option<u64> {
        if !matches!(RopId::from_u8(self.rop_id), Some(RopId::DeleteFolder)) {
            return None;
        }
        let bytes = self.payload.get(1..9)?;
        crate::mapi::identity::object_id_from_wire_id(bytes)
    }

    pub(in crate::mapi) fn move_copy_message_ids(&self) -> Vec<u64> {
        if !matches!(RopId::from_u8(self.rop_id), Some(RopId::MoveCopyMessages)) {
            return Vec::new();
        }
        let Some(count_bytes) = self.payload.get(..2) else {
            return Vec::new();
        };
        let count = u16::from_le_bytes([count_bytes[0], count_bytes[1]]) as usize;
        self.payload[2..]
            .chunks_exact(8)
            .take(count)
            .filter_map(crate::mapi::identity::object_id_from_wire_id)
            .collect()
    }

    pub(in crate::mapi) fn move_copy_want_copy(&self) -> bool {
        let count = self
            .payload
            .get(..2)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .unwrap_or(0) as usize;
        self.payload
            .get(2 + count * 8 + 1)
            .is_some_and(|want_copy| *want_copy != 0)
    }

    pub(in crate::mapi) fn move_copy_want_asynchronous(&self) -> Option<u8> {
        let count = self
            .payload
            .get(..2)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .unwrap_or(0) as usize;
        self.payload.get(2 + count * 8).copied()
    }

    pub(in crate::mapi) fn move_copy_want_copy_raw(&self) -> Option<u8> {
        let count = self
            .payload
            .get(..2)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .unwrap_or(0) as usize;
        self.payload.get(2 + count * 8 + 1).copied()
    }

    pub(in crate::mapi) fn folder_move_copy_folder_id(&self) -> Option<u64> {
        if !matches!(
            RopId::from_u8(self.rop_id),
            Some(RopId::MoveFolder | RopId::CopyFolder)
        ) {
            return None;
        }
        let offset = if matches!(RopId::from_u8(self.rop_id), Some(RopId::CopyFolder)) {
            3
        } else {
            2
        };
        let bytes = self.payload.get(offset..offset + 8)?;
        crate::mapi::identity::object_id_from_wire_id(bytes)
    }

    pub(in crate::mapi) fn folder_move_copy_want_asynchronous(&self) -> Option<u8> {
        if !matches!(
            RopId::from_u8(self.rop_id),
            Some(RopId::MoveFolder | RopId::CopyFolder)
        ) {
            return None;
        }
        self.payload.first().copied()
    }

    pub(in crate::mapi) fn folder_move_copy_want_recursive(&self) -> Option<u8> {
        if !matches!(RopId::from_u8(self.rop_id), Some(RopId::CopyFolder)) {
            return None;
        }
        self.payload.get(1).copied()
    }

    pub(in crate::mapi) fn folder_move_copy_use_unicode(&self) -> Option<u8> {
        if !matches!(
            RopId::from_u8(self.rop_id),
            Some(RopId::MoveFolder | RopId::CopyFolder)
        ) {
            return None;
        }
        let offset = if matches!(RopId::from_u8(self.rop_id), Some(RopId::CopyFolder)) {
            2
        } else {
            1
        };
        self.payload.get(offset).copied()
    }

    pub(in crate::mapi) fn folder_move_copy_display_name(&self) -> String {
        if !matches!(
            RopId::from_u8(self.rop_id),
            Some(RopId::MoveFolder | RopId::CopyFolder)
        ) {
            return String::new();
        }
        let unicode_offset = if matches!(RopId::from_u8(self.rop_id), Some(RopId::CopyFolder)) {
            2
        } else {
            1
        };
        let Some(use_unicode) = self.payload.get(unicode_offset) else {
            return String::new();
        };
        let name_offset = unicode_offset + 1 + 8;
        let Some(name_bytes) = self.payload.get(name_offset..) else {
            return String::new();
        };
        if *use_unicode == 0 {
            let end = name_bytes
                .iter()
                .position(|byte| *byte == 0)
                .unwrap_or(name_bytes.len());
            String::from_utf8_lossy(&name_bytes[..end]).into_owned()
        } else {
            let mut units = Vec::new();
            for bytes in name_bytes.chunks_exact(2) {
                let unit = u16::from_le_bytes([bytes[0], bytes[1]]);
                if unit == 0 {
                    break;
                }
                units.push(unit);
            }
            String::from_utf16_lossy(&units)
        }
    }

    pub(in crate::mapi) fn empty_folder_want_asynchronous(&self) -> Option<u8> {
        if !matches!(
            RopId::from_u8(self.rop_id),
            Some(RopId::EmptyFolder | RopId::HardDeleteMessagesAndSubfolders)
        ) {
            return None;
        }
        self.payload.first().copied()
    }

    pub(in crate::mapi) fn empty_folder_want_delete_associated(&self) -> Option<u8> {
        if !matches!(
            RopId::from_u8(self.rop_id),
            Some(RopId::EmptyFolder | RopId::HardDeleteMessagesAndSubfolders)
        ) {
            return None;
        }
        self.payload.get(1).copied()
    }

    pub(in crate::mapi) fn move_copy_target_handle(&self, input_handles: &[u32]) -> Option<u32> {
        input_handles
            .get(self.output_handle_index? as usize)
            .copied()
            .filter(|handle| *handle != u32::MAX)
    }

    pub(in crate::mapi) fn copy_to_want_asynchronous(&self) -> Option<u8> {
        if !matches!(RopId::from_u8(self.rop_id), Some(RopId::CopyTo)) {
            return None;
        }
        self.payload.first().copied()
    }

    pub(in crate::mapi) fn copy_to_want_subobjects(&self) -> Option<u8> {
        if !matches!(RopId::from_u8(self.rop_id), Some(RopId::CopyTo)) {
            return None;
        }
        self.payload.get(1).copied()
    }

    pub(in crate::mapi) fn copy_to_excluded_property_tags(&self) -> Vec<u32> {
        if !matches!(RopId::from_u8(self.rop_id), Some(RopId::CopyTo)) {
            return Vec::new();
        }
        let Some(count_bytes) = self.payload.get(3..5) else {
            return Vec::new();
        };
        let count = u16::from_le_bytes([count_bytes[0], count_bytes[1]]) as usize;
        self.payload
            .get(5..)
            .unwrap_or_default()
            .chunks_exact(4)
            .take(count)
            .map(|bytes| u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
            .collect()
    }

    pub(in crate::mapi) fn copy_properties_want_asynchronous(&self) -> Option<u8> {
        if !matches!(RopId::from_u8(self.rop_id), Some(RopId::CopyProperties)) {
            return None;
        }
        self.payload.first().copied()
    }

    pub(in crate::mapi) fn copy_properties_property_tags(&self) -> Vec<u32> {
        if !matches!(RopId::from_u8(self.rop_id), Some(RopId::CopyProperties)) {
            return Vec::new();
        }
        let Some(count_bytes) = self.payload.get(2..4) else {
            return Vec::new();
        };
        let count = u16::from_le_bytes([count_bytes[0], count_bytes[1]]) as usize;
        self.payload
            .get(4..)
            .unwrap_or_default()
            .chunks_exact(4)
            .take(count)
            .map(|bytes| u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
            .collect()
    }

    pub(in crate::mapi) fn query_row_count(&self) -> Option<usize> {
        let bytes = self.payload.get(2..4)?;
        Some(u16::from_le_bytes(bytes.try_into().ok()?) as usize)
    }

    pub(in crate::mapi) fn query_no_advance(&self) -> bool {
        self.payload.first().is_some_and(|flags| flags & 0x01 != 0)
    }

    pub(in crate::mapi) fn query_forward_read(&self) -> bool {
        self.payload
            .get(1)
            .map(|forward| *forward != 0)
            .unwrap_or(true)
    }

    pub(in crate::mapi) fn restriction(&self) -> Result<Option<MapiRestriction>> {
        let Some(size_bytes) = self.payload.get(1..3) else {
            return Ok(None);
        };
        let size = u16::from_le_bytes([size_bytes[0], size_bytes[1]]) as usize;
        if size == 0 {
            return Ok(None);
        }
        let bytes = self
            .payload
            .get(3..3 + size)
            .ok_or_else(|| anyhow!("restriction data is truncated"))?;
        parse_mapi_restriction(bytes).map(Some)
    }

    pub(in crate::mapi) fn find_origin(&self) -> Option<u8> {
        let size = u16::from_le_bytes(self.payload.get(1..3)?.try_into().ok()?) as usize;
        self.payload.get(3 + size).copied()
    }

    pub(in crate::mapi) fn find_backward(&self) -> bool {
        self.payload.first().is_some_and(|flags| flags & 0x01 != 0)
    }

    pub(in crate::mapi) fn bookmark(&self) -> &[u8] {
        let size = self
            .payload
            .get(..2)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .unwrap_or(0) as usize;
        self.payload.get(2..2 + size).unwrap_or_default()
    }

    pub(in crate::mapi) fn bookmark_row_count(&self) -> Option<i32> {
        let size = u16::from_le_bytes(self.payload.get(..2)?.try_into().ok()?) as usize;
        let bytes = self.payload.get(2 + size..6 + size)?;
        Some(i32::from_le_bytes(bytes.try_into().ok()?))
    }

    pub(in crate::mapi) fn bookmark_want_row_moved_count(&self) -> bool {
        let Some(size) = self
            .payload
            .get(..2)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .map(usize::from)
        else {
            return false;
        };
        self.payload.get(6 + size).is_some_and(|want| *want != 0)
    }

    pub(in crate::mapi) fn seek_origin(&self) -> Option<u8> {
        self.payload.first().copied()
    }

    pub(in crate::mapi) fn seek_row_count(&self) -> Option<i32> {
        let bytes = self.payload.get(1..5)?;
        Some(i32::from_le_bytes(bytes.try_into().ok()?))
    }

    pub(in crate::mapi) fn want_row_moved_count(&self) -> bool {
        self.payload.get(5).is_some_and(|want| *want != 0)
    }

    pub(in crate::mapi) fn fractional_position(&self) -> Option<(u32, u32)> {
        let numerator = u32::from_le_bytes(self.payload.get(..4)?.try_into().ok()?);
        let denominator = u32::from_le_bytes(self.payload.get(4..8)?.try_into().ok()?);
        Some((numerator, denominator))
    }

    pub(in crate::mapi) fn sort_orders(&self) -> Vec<MapiSortOrder> {
        let Some(count_bytes) = self.payload.get(1..3) else {
            return Vec::new();
        };
        let count = u16::from_le_bytes([count_bytes[0], count_bytes[1]]) as usize;
        self.payload
            .get(7..)
            .unwrap_or_default()
            .chunks_exact(5)
            .take(count)
            .map(|bytes| MapiSortOrder {
                property_tag: u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
                order: bytes[4],
            })
            .collect()
    }

    pub(in crate::mapi) fn sort_category_count(&self) -> u16 {
        self.payload
            .get(3..5)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .unwrap_or(0)
    }

    pub(in crate::mapi) fn sort_expanded_count(&self) -> u16 {
        self.payload
            .get(5..7)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .unwrap_or(0)
    }

    pub(in crate::mapi) fn category_id(&self) -> Option<u64> {
        let offset = match RopId::from_u8(self.rop_id) {
            Some(RopId::ExpandRow) => 2,
            Some(RopId::CollapseRow) => 0,
            _ => return None,
        };
        self.payload
            .get(offset..offset + 8)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u64::from_le_bytes)
    }

    pub(in crate::mapi) fn expand_max_row_count(&self) -> usize {
        self.payload
            .get(..2)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .map(usize::from)
            .unwrap_or(0)
    }

    pub(in crate::mapi) fn collapse_state(&self) -> &[u8] {
        let size = self
            .payload
            .get(..2)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .map(usize::from)
            .unwrap_or(0);
        self.payload.get(2..2 + size).unwrap_or_default()
    }

    pub(in crate::mapi) fn collapse_state_row_id(&self) -> Option<u64> {
        if self.rop_id != 0x6B {
            return None;
        }
        self.payload
            .get(..8)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u64::from_le_bytes)
    }

    pub(in crate::mapi) fn collapse_state_row_instance_number(&self) -> u32 {
        self.payload
            .get(8..12)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u32::from_le_bytes)
            .unwrap_or(0)
    }

    pub(in crate::mapi) fn property_tags(&self) -> Vec<u32> {
        let start = match self.rop_id {
            0x07 => 4,
            0x0B | 0x0E | 0x7A => 2,
            _ => 3,
        };
        if self.payload.len() < start {
            return Vec::new();
        }
        let count_offset = start - 2;
        let count = u16::from_le_bytes([self.payload[count_offset], self.payload[count_offset + 1]])
            as usize;
        self.payload[start..]
            .chunks_exact(4)
            .take(count)
            .map(|bytes| u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
            .collect()
    }

    pub(in crate::mapi) fn property_ids(&self) -> Vec<u16> {
        let Some(count_bytes) = self.payload.get(..2) else {
            return Vec::new();
        };
        let count = u16::from_le_bytes([count_bytes[0], count_bytes[1]]) as usize;
        self.payload
            .get(2..)
            .unwrap_or_default()
            .chunks_exact(2)
            .take(count)
            .map(|bytes| u16::from_le_bytes([bytes[0], bytes[1]]))
            .collect()
    }

    pub(in crate::mapi) fn named_property_create(&self) -> bool {
        self.payload.first().is_some_and(|flags| *flags == 0x02)
    }

    pub(in crate::mapi) fn named_property_names(&self) -> Result<Vec<MapiNamedProperty>> {
        let Some(count_bytes) = self.payload.get(1..3) else {
            return Ok(Vec::new());
        };
        let count = u16::from_le_bytes([count_bytes[0], count_bytes[1]]) as usize;
        let mut cursor = Cursor::new(
            self.payload
                .get(3..)
                .ok_or_else(|| anyhow!("missing named property payload"))?,
        );
        let mut properties = Vec::with_capacity(count);
        for _ in 0..count {
            properties.push(parse_named_property(&mut cursor)?);
        }
        Ok(properties)
    }

    pub(in crate::mapi) fn named_property_query_guid(&self) -> Option<[u8; 16]> {
        if self.payload.get(1).copied().unwrap_or_default() == 0 {
            return None;
        }
        self.payload.get(2..18)?.try_into().ok()
    }

    pub(in crate::mapi) fn property_values(&self) -> Result<Vec<(u32, MapiValue)>> {
        let Some(size_bytes) = self.payload.get(..2) else {
            return Ok(Vec::new());
        };
        let property_value_size = u16::from_le_bytes([size_bytes[0], size_bytes[1]]) as usize;
        if property_value_size < 2 {
            return Err(anyhow!("invalid property value size"));
        }
        let Some(count_bytes) = self.payload.get(2..4) else {
            return Err(anyhow!("missing property value count"));
        };
        let count = u16::from_le_bytes([count_bytes[0], count_bytes[1]]) as usize;
        let value_bytes = self
            .payload
            .get(4..4 + property_value_size - 2)
            .ok_or_else(|| anyhow!("truncated property values"))?;
        let mut cursor = Cursor::new(value_bytes);
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            values.push(parse_tagged_property(&mut cursor)?);
        }
        Ok(values)
    }

    pub(in crate::mapi) fn modify_recipients(
        &self,
        principal: &AccountPrincipal,
        address_book_entries: &[ExchangeAddressBookEntry],
    ) -> Result<Vec<PendingRecipientChange>> {
        let Some(count_bytes) = self.payload.get(..2) else {
            return Ok(Vec::new());
        };
        let column_count = u16::from_le_bytes([count_bytes[0], count_bytes[1]]) as usize;
        let columns_end = 2 + column_count * 4;
        let columns = self
            .payload
            .get(2..columns_end)
            .ok_or_else(|| anyhow!("truncated recipient columns"))?
            .chunks_exact(4)
            .map(|bytes| u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
            .collect::<Vec<_>>();
        let row_count_bytes = self
            .payload
            .get(columns_end..columns_end + 2)
            .ok_or_else(|| anyhow!("missing recipient row count"))?;
        let row_count = u16::from_le_bytes([row_count_bytes[0], row_count_bytes[1]]) as usize;
        let mut cursor = Cursor::new(
            self.payload
                .get(columns_end + 2..)
                .ok_or_else(|| anyhow!("missing recipient rows"))?,
        );
        let mut changes = Vec::with_capacity(row_count);
        for _ in 0..row_count {
            let row_id = cursor.read_u32()?;
            let recipient_type = cursor.read_u8()?;
            let row_size = cursor.read_u16()? as usize;
            if row_size == 0 {
                changes.push(PendingRecipientChange::Delete(row_id));
                continue;
            }
            let row = cursor.read_bytes(row_size)?;
            changes.push(PendingRecipientChange::Upsert(parse_pending_recipient_row(
                row_id,
                recipient_type,
                &columns,
                row,
                principal,
                address_book_entries,
            )?));
        }
        Ok(changes)
    }
}

pub(in crate::mapi) fn parse_pending_recipient_row(
    row_id: u32,
    fallback_recipient_type: u8,
    columns: &[u32],
    row: &[u8],
    principal: &AccountPrincipal,
    address_book_entries: &[ExchangeAddressBookEntry],
) -> Result<PendingRecipient> {
    if let Ok(recipient) = parse_wrapped_pending_recipient_row(
        row_id,
        fallback_recipient_type,
        columns,
        row,
        principal,
        address_book_entries,
    ) {
        return Ok(recipient);
    }

    parse_simple_pending_recipient_row(row_id, fallback_recipient_type, columns, row)
}

fn parse_simple_pending_recipient_row(
    row_id: u32,
    fallback_recipient_type: u8,
    columns: &[u32],
    row: &[u8],
) -> Result<PendingRecipient> {
    let mut cursor = Cursor::new(row);
    let mut values = HashMap::new();
    for column in columns {
        values.insert(
            canonical_property_storage_tag(*column),
            parse_property_value_for_tag(&mut cursor, *column)?,
        );
    }
    let recipient_type = values
        .get(&PID_TAG_RECIPIENT_TYPE)
        .and_then(MapiValue::as_i64)
        .and_then(|value| u8::try_from(value).ok())
        .unwrap_or(fallback_recipient_type);
    let recipient_type = normalize_recipient_type(recipient_type)?;
    let address =
        optional_mapi_value_text(&values, &[PID_TAG_SMTP_ADDRESS_W, PID_TAG_EMAIL_ADDRESS_W])
            .and_then(super::properties::normalize_mapi_submit_address)
            .ok_or_else(|| {
                anyhow!(
                    "recipient address is required;row_format=simple;columns={}",
                    format_property_tags_for_debug(columns)
                )
            })?;
    let display_name = recipient_display_name_from_values(&values)
        .filter(|value| !value.eq_ignore_ascii_case(&address));

    Ok(PendingRecipient {
        row_id,
        recipient_type,
        address,
        display_name,
    })
}

fn parse_wrapped_pending_recipient_row(
    row_id: u32,
    fallback_recipient_type: u8,
    columns: &[u32],
    row: &[u8],
    principal: &AccountPrincipal,
    address_book_entries: &[ExchangeAddressBookEntry],
) -> Result<PendingRecipient> {
    let mut cursor = Cursor::new(row);
    let recipient_flags = cursor.read_u16()?;
    let address_type = recipient_flags & 0x0007;
    let unicode_strings = recipient_flags & 0x0200 != 0;

    let x500_dn = if address_type == 0x01 {
        let _address_prefix_used = cursor.read_u8()?;
        let _display_type = cursor.read_u8()?;
        Some(cursor.read_ascii_z()?).filter(|value| !value.is_empty())
    } else if matches!(address_type, 0x06 | 0x07) {
        let entry_id_size = cursor.read_u16()? as usize;
        let _entry_id = cursor.read_bytes(entry_id_size)?;
        let search_key_size = cursor.read_u16()? as usize;
        let _search_key = cursor.read_bytes(search_key_size)?;
        None
    } else {
        None
    };

    if address_type == 0x00 && recipient_flags & 0x8000 != 0 {
        let _address_type = cursor.read_ascii_z()?;
    }

    let email_address = if recipient_flags & 0x0008 != 0 {
        Some(read_recipient_string(&mut cursor, unicode_strings)?)
    } else {
        None
    };
    let display_name = if recipient_flags & 0x0010 != 0 {
        Some(read_recipient_string(&mut cursor, unicode_strings)?)
    } else {
        None
    };
    if recipient_flags & 0x0400 != 0 {
        let _simple_display_name = read_recipient_string(&mut cursor, unicode_strings)?;
    }
    if recipient_flags & 0x0020 != 0 {
        let _transmittable_display_name = read_recipient_string(&mut cursor, unicode_strings)?;
    }

    let recipient_column_count = cursor.read_u16()? as usize;
    if recipient_column_count > columns.len() {
        return Err(anyhow!(
            "recipient column count exceeds request column count"
        ));
    }
    let row_kind = cursor.read_u8()?;
    if row_kind != 0 {
        return Err(anyhow!("unsupported flagged recipient property row"));
    }

    let mut values = HashMap::new();
    for column in columns.iter().take(recipient_column_count) {
        values.insert(
            canonical_property_storage_tag(*column),
            parse_property_value_for_tag(&mut cursor, *column)?,
        );
    }

    let recipient_type = values
        .get(&PID_TAG_RECIPIENT_TYPE)
        .and_then(MapiValue::as_i64)
        .and_then(|value| u8::try_from(value).ok())
        .unwrap_or(fallback_recipient_type);
    let recipient_type = normalize_recipient_type(recipient_type)?;
    let address =
        optional_mapi_value_text(&values, &[PID_TAG_SMTP_ADDRESS_W, PID_TAG_EMAIL_ADDRESS_W])
            .or(email_address)
            .and_then(super::properties::normalize_mapi_submit_address)
            .or_else(|| {
                x500_dn
                    .as_deref()
                    .and_then(|dn| legacy_dn_recipient_address(dn, principal, address_book_entries))
            })
            .ok_or_else(|| {
                anyhow!(
                    "recipient address is required;row_format=wrapped;recipient_flags={recipient_flags:#06x};address_type={address_type:#04x};recipient_column_count={recipient_column_count};columns={}",
                    format_property_tags_for_debug(columns)
                )
            })?;
    let display_name = recipient_display_name_from_values(&values)
        .or(display_name)
        .filter(|value| !value.eq_ignore_ascii_case(&address));

    Ok(PendingRecipient {
        row_id,
        recipient_type,
        address,
        display_name,
    })
}

fn recipient_display_name_from_values(values: &HashMap<u32, MapiValue>) -> Option<String> {
    optional_mapi_value_text(
        values,
        &[
            PID_TAG_DISPLAY_NAME_W,
            PID_TAG_RECIPIENT_DISPLAY_NAME_W,
            PID_TAG_ADDRESS_BOOK_DISPLAY_NAME_PRINTABLE_W,
        ],
    )
}

fn normalize_recipient_type(recipient_type: u8) -> Result<u8> {
    let base_type = recipient_type & 0x0F;
    let flags = recipient_type & !0x0F;
    if matches!(base_type, 0x01..=0x03) && flags & !0x90 == 0 {
        Ok(base_type)
    } else {
        Err(anyhow!("invalid recipient type {recipient_type:#04x}"))
    }
}

fn legacy_dn_recipient_address(
    legacy_dn: &str,
    principal: &AccountPrincipal,
    address_book_entries: &[ExchangeAddressBookEntry],
) -> Option<String> {
    let legacy_dn = super::nspi::normalize_nspi_lookup_value(legacy_dn);
    let principal_entry = super::nspi::principal_address_book_entry(principal);
    std::iter::once(&principal_entry)
        .chain(address_book_entries.iter())
        .find(|entry| {
            legacy_dn_matches_entry(&legacy_dn, &super::nspi::nspi_entry_legacy_dn(entry))
                || legacy_dn_matches_entry(
                    &legacy_dn,
                    &super::nspi::nspi_entry_unprefixed_legacy_dn(entry),
                )
        })
        .map(|entry| lpe_storage::normalize_mailbox_email(&entry.email))
        .filter(|address| !address.is_empty())
}

fn legacy_dn_matches_entry(actual: &str, expected: &str) -> bool {
    let expected = expected.to_ascii_lowercase();
    actual == expected || actual == expected.trim_start_matches('/')
}

fn read_recipient_string(cursor: &mut Cursor<'_>, unicode: bool) -> Result<String> {
    if unicode {
        cursor.read_utf16z()
    } else {
        cursor.read_ascii_z()
    }
}

pub(in crate::mapi) fn optional_mapi_value_text(
    values: &HashMap<u32, MapiValue>,
    tags: &[u32],
) -> Option<String> {
    tags.iter()
        .find_map(|tag| values.get(tag).and_then(|value| value.clone().into_text()))
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

pub(in crate::mapi) fn parse_mapi_restriction(bytes: &[u8]) -> Result<MapiRestriction> {
    let mut cursor = Cursor::new(bytes);
    let restriction = parse_mapi_restriction_from(&mut cursor)?;
    if cursor.remaining() != 0 {
        return Err(anyhow!("restriction data has trailing bytes"));
    }
    Ok(restriction)
}

pub(in crate::mapi) fn parse_mapi_restriction_from(
    cursor: &mut Cursor<'_>,
) -> Result<MapiRestriction> {
    let restriction_type = cursor.read_u8()?;
    match MapiRestrictionType::from_u8(restriction_type) {
        Some(MapiRestrictionType::And) => {
            let count = cursor.read_u16()? as usize;
            let mut children = Vec::with_capacity(count);
            for _ in 0..count {
                children.push(parse_mapi_restriction_from(cursor)?);
            }
            Ok(MapiRestriction::And(children))
        }
        Some(MapiRestrictionType::Or) => {
            let count = cursor.read_u16()? as usize;
            let mut children = Vec::with_capacity(count);
            for _ in 0..count {
                children.push(parse_mapi_restriction_from(cursor)?);
            }
            Ok(MapiRestriction::Or(children))
        }
        Some(MapiRestrictionType::Not) => Ok(MapiRestriction::Not(Box::new(
            parse_mapi_restriction_from(cursor)?,
        ))),
        Some(MapiRestrictionType::Content) => {
            let fuzzy_level_low = cursor.read_u16()?;
            let fuzzy_level_high = cursor.read_u16()?;
            let property_tag = cursor.read_u32()?;
            let value = parse_tagged_property_value(cursor)?
                .into_text()
                .ok_or_else(|| anyhow!("content restriction requires a text value"))?;
            Ok(MapiRestriction::Content {
                property_tag,
                value,
                fuzzy_level_low,
                fuzzy_level_high,
            })
        }
        Some(MapiRestrictionType::Property) => {
            let relop = cursor.read_u8()?;
            let property_tag = cursor.read_u32()?;
            let value = parse_tagged_property_value(cursor)?;
            Ok(MapiRestriction::Property {
                relop,
                property_tag,
                value,
            })
        }
        Some(MapiRestrictionType::CompareProperties) => {
            let relop = cursor.read_u8()?;
            let left_property_tag = cursor.read_u32()?;
            let right_property_tag = cursor.read_u32()?;
            Ok(MapiRestriction::CompareProperties {
                relop,
                left_property_tag,
                right_property_tag,
            })
        }
        Some(MapiRestrictionType::Bitmask) => {
            let rel_bmr = cursor.read_u8()?;
            let property_tag = cursor.read_u32()?;
            let mask = cursor.read_u32()?;
            Ok(MapiRestriction::Bitmask {
                property_tag,
                mask,
                must_be_nonzero: rel_bmr != 0,
            })
        }
        Some(MapiRestrictionType::Size) => {
            let relop = cursor.read_u8()?;
            let property_tag = cursor.read_u32()?;
            let size = cursor.read_u32()?;
            Ok(MapiRestriction::Size {
                relop,
                property_tag,
                size,
            })
        }
        Some(MapiRestrictionType::Exist) => {
            let property_tag = cursor.read_u32()?;
            Ok(MapiRestriction::Exist { property_tag })
        }
        Some(MapiRestrictionType::SubObject) => {
            let subobject = cursor.read_u32()?;
            let child = parse_mapi_restriction_from(cursor)?;
            Ok(MapiRestriction::SubObject {
                subobject,
                child: Box::new(child),
            })
        }
        Some(MapiRestrictionType::Comment) => {
            let count = cursor.read_u8()? as usize;
            for _ in 0..count {
                parse_tagged_property(cursor)?;
            }
            match cursor.read_u8()? {
                0x00 => Ok(MapiRestriction::And(Vec::new())),
                0x01 => parse_mapi_restriction_from(cursor),
                _ => Err(anyhow!("comment restriction has invalid present flag")),
            }
        }
        Some(MapiRestrictionType::Count) => {
            let count = cursor.read_u32()?;
            let child = parse_mapi_restriction_from(cursor)?;
            Ok(MapiRestriction::Count {
                count,
                child: Box::new(child),
            })
        }
        _ => {
            tracing::warn!(
                adapter = "mapi",
                enum_name = "MapiRestrictionType",
                raw_value = restriction_type,
                "unsupported MAPI restriction type rejected at parser boundary"
            );
            Err(anyhow!("unsupported MAPI restriction type"))
        }
    }
}

pub(in crate::mapi) fn parse_tagged_property_value(cursor: &mut Cursor<'_>) -> Result<MapiValue> {
    parse_tagged_property(cursor).map(|(_property_tag, value)| value)
}

pub(in crate::mapi) fn parse_tagged_property(cursor: &mut Cursor<'_>) -> Result<(u32, MapiValue)> {
    let property_tag = cursor.read_u32()?;
    let value = parse_property_value_for_tag(cursor, property_tag)?;
    Ok((canonical_property_storage_tag(property_tag), value))
}

pub(in crate::mapi) fn parse_named_property(cursor: &mut Cursor<'_>) -> Result<MapiNamedProperty> {
    let kind = cursor.read_u8()?;
    let guid: [u8; 16] = cursor
        .read_bytes(16)?
        .try_into()
        .map_err(|_| anyhow!("invalid named property GUID"))?;
    let kind = match kind {
        0x00 => MapiNamedPropertyKind::Lid(cursor.read_u32()?),
        0x01 => {
            let name_size = cursor.read_u8()? as usize;
            let name_bytes = cursor.read_bytes(name_size)?;
            MapiNamedPropertyKind::Name(decode_utf16z_bytes(name_bytes))
        }
        _ => return Err(anyhow!("unsupported named property kind")),
    };
    Ok(MapiNamedProperty { guid, kind })
}

pub(in crate::mapi) fn decode_utf16z_bytes(bytes: &[u8]) -> String {
    String::from_utf16_lossy(
        &bytes
            .chunks_exact(2)
            .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
            .take_while(|unit| *unit != 0)
            .collect::<Vec<_>>(),
    )
}

pub(in crate::mapi) fn parse_property_value_for_tag(
    cursor: &mut Cursor<'_>,
    property_tag: u32,
) -> Result<MapiValue> {
    parse_mapi_property_value(cursor, property_tag)
}

pub(in crate::mapi) fn rop_id_is_reserved(rop_id: u8) -> bool {
    RopId::is_reserved(rop_id)
}

pub(in crate::mapi) fn read_rop_request(cursor: &mut Cursor<'_>) -> Result<RopRequest> {
    let rop_id = cursor.read_u8()?;
    let _logon_id = cursor.read_u8()?;
    match RopId::from_u8(rop_id) {
        Some(RopId::Release) => {
            let input_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: Vec::new(),
            })
        }
        Some(RopId::OpenFolder) => {
            let input_handle_index = cursor.read_u8()?;
            let output_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(cursor.read_bytes(8)?);
            payload.push(cursor.read_u8()?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: Some(output_handle_index),
                payload,
            })
        }
        Some(RopId::OpenMessage) => {
            let input_handle_index = cursor.read_u8()?;
            let output_handle_index = cursor.read_u8()?;
            let _code_page_id = cursor.read_u16()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(cursor.read_bytes(8)?);
            payload.push(cursor.read_u8()?);
            payload.extend_from_slice(cursor.read_bytes(8)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: Some(output_handle_index),
                payload,
            })
        }
        Some(RopId::GetHierarchyTable | RopId::GetContentsTable | RopId::GetAttachmentTable) => {
            let input_handle_index = cursor.read_u8()?;
            let output_handle_index = cursor.read_u8()?;
            let payload = vec![cursor.read_u8()?];
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: Some(output_handle_index),
                payload,
            })
        }
        Some(RopId::CreateMessage) => {
            let input_handle_index = cursor.read_u8()?;
            let output_handle_index = cursor.read_u8()?;
            let _code_page_id = cursor.read_u16()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(cursor.read_bytes(8)?);
            payload.push(cursor.read_u8()?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: Some(output_handle_index),
                payload,
            })
        }
        Some(RopId::OpenAttachment) => {
            let input_handle_index = cursor.read_u8()?;
            let output_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.push(cursor.read_u8()?);
            payload.extend_from_slice(&cursor.read_u32()?.to_le_bytes());
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: Some(output_handle_index),
                payload,
            })
        }
        Some(RopId::CreateAttachment) => {
            let input_handle_index = cursor.read_u8()?;
            let output_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: Some(output_handle_index),
                payload: Vec::new(),
            })
        }
        Some(RopId::DeleteAttachment) => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(&cursor.read_u32()?.to_le_bytes());
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::SaveChangesAttachment) => {
            let response_handle_index = cursor.read_u8()?;
            let input_handle_index = cursor.read_u8()?;
            let payload = vec![cursor.read_u8()?];
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: Some(response_handle_index),
                payload,
            })
        }
        Some(RopId::OpenEmbeddedMessage) => {
            let input_handle_index = cursor.read_u8()?;
            let output_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(&cursor.read_u16()?.to_le_bytes());
            payload.push(cursor.read_u8()?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: Some(output_handle_index),
                payload,
            })
        }
        Some(RopId::GetPropertiesAll) => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(&cursor.read_u16()?.to_le_bytes());
            payload.extend_from_slice(&cursor.read_u16()?.to_le_bytes());
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::OpenStream) => {
            let input_handle_index = cursor.read_u8()?;
            let output_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(&cursor.read_u32()?.to_le_bytes());
            payload.push(cursor.read_u8()?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: Some(output_handle_index),
                payload,
            })
        }
        Some(RopId::ReadStream) => {
            let input_handle_index = cursor.read_u8()?;
            let byte_count = cursor.read_u16()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(&byte_count.to_le_bytes());
            if byte_count == 0xBABE {
                payload.extend_from_slice(&cursor.read_u32()?.to_le_bytes());
            }
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::WriteStream | RopId::WriteAndCommitStream | RopId::WriteStreamExtended) => {
            let input_handle_index = cursor.read_u8()?;
            let size = cursor.read_u16()? as usize;
            let mut payload = Vec::new();
            payload.extend_from_slice(&(size as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(size)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::SeekStream) => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.push(cursor.read_u8()?);
            payload.extend_from_slice(cursor.read_bytes(8)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::SetStreamSize) => {
            let input_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: cursor.read_bytes(8)?.to_vec(),
            })
        }
        Some(RopId::CopyToStream) => {
            let source_handle_index = cursor.read_u8()?;
            let dest_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(source_handle_index),
                output_handle_index: Some(dest_handle_index),
                payload: cursor.read_bytes(8)?.to_vec(),
            })
        }
        Some(RopId::CopyTo) => {
            let source_handle_index = cursor.read_u8()?;
            let dest_handle_index = cursor.read_u8()?;
            let mut payload = vec![cursor.read_u8()?, cursor.read_u8()?, cursor.read_u8()?];
            let excluded_tag_count = cursor.read_u16()? as usize;
            payload.extend_from_slice(&(excluded_tag_count as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(excluded_tag_count * 4)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(source_handle_index),
                output_handle_index: Some(dest_handle_index),
                payload,
            })
        }
        Some(RopId::CopyProperties) => {
            let source_handle_index = cursor.read_u8()?;
            let dest_handle_index = cursor.read_u8()?;
            let mut payload = vec![cursor.read_u8()?, cursor.read_u8()?];
            let property_tag_count = cursor.read_u16()? as usize;
            payload.extend_from_slice(&(property_tag_count as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(property_tag_count * 4)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(source_handle_index),
                output_handle_index: Some(dest_handle_index),
                payload,
            })
        }
        Some(RopId::CloneStream) => {
            let input_handle_index = cursor.read_u8()?;
            let output_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: Some(output_handle_index),
                payload: Vec::new(),
            })
        }
        Some(RopId::RegisterNotification) => {
            let input_handle_index = cursor.read_u8()?;
            let output_handle_index = cursor.read_u8()?;
            let notification_types = cursor.read_u16()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(&notification_types.to_le_bytes());
            if notification_types & 0x0400 != 0 {
                payload.push(cursor.read_u8()?);
            }
            let want_whole_store = cursor.read_u8()?;
            payload.push(want_whole_store);
            if want_whole_store == 0 {
                payload.extend_from_slice(cursor.read_bytes(16)?);
            }
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: Some(output_handle_index),
                payload,
            })
        }
        Some(RopId::SetSearchCriteria) => {
            let input_handle_index = cursor.read_u8()?;
            let restriction_size = cursor.read_u16()? as usize;
            let mut payload = Vec::new();
            payload.extend_from_slice(&(restriction_size as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(restriction_size)?);
            let folder_id_count = cursor.read_u16()? as usize;
            payload.extend_from_slice(&(folder_id_count as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(folder_id_count * 8)?);
            payload.extend_from_slice(&cursor.read_u32()?.to_le_bytes());
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::GetSearchCriteria) => {
            let input_handle_index = cursor.read_u8()?;
            let payload = vec![cursor.read_u8()?, cursor.read_u8()?, cursor.read_u8()?];
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::Abort) => {
            let input_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: Vec::new(),
            })
        }
        Some(RopId::GetPermissionsTable | RopId::GetRulesTable) => {
            let input_handle_index = cursor.read_u8()?;
            let output_handle_index = cursor.read_u8()?;
            let payload = vec![cursor.read_u8()?];
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: Some(output_handle_index),
                payload,
            })
        }
        Some(RopId::ModifyPermissions) => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = vec![cursor.read_u8()?];
            let permissions_count = cursor.read_u16()? as usize;
            payload.extend_from_slice(&(permissions_count as u16).to_le_bytes());
            for _ in 0..permissions_count {
                payload.push(cursor.read_u8()?);
                let property_count = cursor.read_u16()? as usize;
                payload.extend_from_slice(&(property_count as u16).to_le_bytes());
                for _ in 0..property_count {
                    let (property_tag, value) = parse_tagged_property(cursor)?;
                    payload.extend_from_slice(&property_tag.to_le_bytes());
                    write_mapi_value(&mut payload, property_tag, &value);
                }
            }
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::ModifyRules) => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = vec![cursor.read_u8()?];
            let rules_count = cursor.read_u16()? as usize;
            payload.extend_from_slice(&(rules_count as u16).to_le_bytes());
            for _ in 0..rules_count {
                payload.push(cursor.read_u8()?);
                let property_count = cursor.read_u16()? as usize;
                payload.extend_from_slice(&(property_count as u16).to_le_bytes());
                for _ in 0..property_count {
                    let (property_tag, value) = parse_tagged_property(cursor)?;
                    payload.extend_from_slice(&property_tag.to_le_bytes());
                    write_mapi_value(&mut payload, property_tag, &value);
                }
            }
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::GetOwningServers | RopId::LongTermIdFromId | RopId::PublicFolderIsGhosted) => {
            let input_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: cursor.read_bytes(8)?.to_vec(),
            })
        }
        Some(RopId::IdFromLongTermId) => {
            let input_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: cursor.read_bytes(24)?.to_vec(),
            })
        }
        Some(RopId::Progress) => {
            let input_handle_index = cursor.read_u8()?;
            let payload = vec![cursor.read_u8()?];
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::EmptyFolder) => {
            let input_handle_index = cursor.read_u8()?;
            let payload = vec![cursor.read_u8()?, cursor.read_u8()?];
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::HardDeleteMessagesAndSubfolders) => {
            let input_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: vec![cursor.read_u8()?, cursor.read_u8()?],
            })
        }
        Some(RopId::CollapseRow) => {
            let input_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: cursor.read_bytes(8)?.to_vec(),
            })
        }
        Some(RopId::LockRegionStream | RopId::UnlockRegionStream) => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(cursor.read_bytes(8)?);
            payload.extend_from_slice(cursor.read_bytes(8)?);
            payload.extend_from_slice(&cursor.read_u32()?.to_le_bytes());
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::CommitStream | RopId::GetStreamSize) => {
            let input_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: Vec::new(),
            })
        }
        Some(RopId::SetReceiveFolder) => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(cursor.read_bytes(8)?);
            let message_class = cursor.read_ascii_z()?;
            payload.extend_from_slice(message_class.as_bytes());
            payload.push(0);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::GetPerUserLongTermIds) => {
            let input_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: cursor.read_bytes(16)?.to_vec(),
            })
        }
        Some(RopId::GetPerUserGuid) => {
            let input_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: cursor.read_bytes(24)?.to_vec(),
            })
        }
        Some(RopId::ReadPerUserInformation) => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(cursor.read_bytes(24)?);
            payload.push(cursor.read_u8()?);
            payload.extend_from_slice(&cursor.read_u32()?.to_le_bytes());
            payload.extend_from_slice(&cursor.read_u16()?.to_le_bytes());
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::WritePerUserInformation) => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(cursor.read_bytes(24)?);
            payload.push(cursor.read_u8()?);
            let data_offset = cursor.read_u32()?;
            payload.extend_from_slice(&data_offset.to_le_bytes());
            let data_size = cursor.read_u16()? as usize;
            payload.extend_from_slice(&(data_size as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(data_size)?);
            if data_offset == 0 && cursor.remaining() == 16 {
                payload.extend_from_slice(cursor.read_bytes(16)?);
            }
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::SetReadFlags) => {
            let input_handle_index = cursor.read_u8()?;
            let want_asynchronous = cursor.read_u8()?;
            let read_flags = cursor.read_u8()?;
            let message_id_count = cursor.read_u16()? as usize;
            let mut payload = vec![want_asynchronous, read_flags];
            payload.extend_from_slice(&(message_id_count as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(message_id_count * 8)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::GetCollapseState) => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(cursor.read_bytes(8)?);
            payload.extend_from_slice(&cursor.read_u32()?.to_le_bytes());
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::SetCollapseState) => {
            let input_handle_index = cursor.read_u8()?;
            let collapse_state_size = cursor.read_u16()? as usize;
            let mut payload = Vec::new();
            payload.extend_from_slice(&(collapse_state_size as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(collapse_state_size)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::SynchronizationConfigure) => {
            let input_handle_index = cursor.read_u8()?;
            let output_handle_index = cursor.read_u8()?;
            let sync_type = cursor.read_u8()?;
            let send_options = cursor.read_u8()?;
            let sync_flags = cursor.read_u16()?;
            let mut payload = vec![sync_type, send_options];
            payload.extend_from_slice(&sync_flags.to_le_bytes());
            if cursor.remaining() > 0 {
                let restriction_size = cursor.read_u16()? as usize;
                payload.extend_from_slice(&(restriction_size as u16).to_le_bytes());
                payload.extend_from_slice(cursor.read_bytes(restriction_size)?);
                payload.extend_from_slice(&cursor.read_u32()?.to_le_bytes());
                let property_tag_count = cursor.read_u16()? as usize;
                payload.extend_from_slice(&(property_tag_count as u16).to_le_bytes());
                payload.extend_from_slice(cursor.read_bytes(property_tag_count * 4)?);
            }
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: Some(output_handle_index),
                payload,
            })
        }
        Some(RopId::FastTransferSourceGetBuffer) => {
            let input_handle_index = cursor.read_u8()?;
            let buffer_size = cursor.read_u16()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(&buffer_size.to_le_bytes());
            if buffer_size == 0xBABE {
                payload.extend_from_slice(&cursor.read_u16()?.to_le_bytes());
            }
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::FastTransferSourceCopyMessages) => {
            let input_handle_index = cursor.read_u8()?;
            let output_handle_index = cursor.read_u8()?;
            let message_id_count = cursor.read_u16()? as usize;
            let mut payload = Vec::new();
            payload.extend_from_slice(&(message_id_count as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(message_id_count * 8)?);
            payload.push(cursor.read_u8()?);
            payload.push(cursor.read_u8()?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: Some(output_handle_index),
                payload,
            })
        }
        Some(RopId::FastTransferSourceCopyFolder) => {
            let input_handle_index = cursor.read_u8()?;
            let output_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.push(cursor.read_u8()?);
            payload.push(cursor.read_u8()?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: Some(output_handle_index),
                payload,
            })
        }
        Some(RopId::FastTransferSourceCopyTo | RopId::FastTransferSourceCopyProperties) => {
            let input_handle_index = cursor.read_u8()?;
            let output_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.push(cursor.read_u8()?);
            if matches!(
                RopId::from_u8(rop_id),
                Some(RopId::FastTransferSourceCopyTo)
            ) {
                payload.extend_from_slice(&cursor.read_u32()?.to_le_bytes());
            } else {
                payload.push(cursor.read_u8()?);
            }
            payload.push(cursor.read_u8()?);
            let property_tag_count = cursor.read_u16()? as usize;
            payload.extend_from_slice(&(property_tag_count as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(property_tag_count * 4)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: Some(output_handle_index),
                payload,
            })
        }
        Some(RopId::FastTransferDestinationConfigure) => {
            let input_handle_index = cursor.read_u8()?;
            let output_handle_index = cursor.read_u8()?;
            let payload = vec![cursor.read_u8()?, cursor.read_u8()?];
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: Some(output_handle_index),
                payload,
            })
        }
        Some(
            RopId::FastTransferDestinationPutBuffer
            | RopId::FastTransferDestinationPutBufferExtended,
        ) => {
            let input_handle_index = cursor.read_u8()?;
            let transfer_data_size = cursor.read_u16()? as usize;
            let mut payload = Vec::new();
            payload.extend_from_slice(&(transfer_data_size as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(transfer_data_size)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::SynchronizationUploadStateStreamBegin) => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(&cursor.read_u32()?.to_le_bytes());
            payload.extend_from_slice(&cursor.read_u32()?.to_le_bytes());
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::SynchronizationUploadStateStreamContinue) => {
            let input_handle_index = cursor.read_u8()?;
            let stream_size = cursor.read_u32()? as usize;
            let mut payload = Vec::new();
            payload.extend_from_slice(&(stream_size as u32).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(stream_size)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::SynchronizationUploadStateStreamEnd) => {
            let input_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: Vec::new(),
            })
        }
        Some(RopId::SynchronizationOpenCollector) => {
            let input_handle_index = cursor.read_u8()?;
            let output_handle_index = cursor.read_u8()?;
            let payload = vec![cursor.read_u8()?];
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: Some(output_handle_index),
                payload,
            })
        }
        Some(RopId::SynchronizationGetTransferState) => {
            let input_handle_index = cursor.read_u8()?;
            let output_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: Some(output_handle_index),
                payload: Vec::new(),
            })
        }
        Some(RopId::SynchronizationImportMessageChange) => {
            let input_handle_index = cursor.read_u8()?;
            let output_handle_index = cursor.read_u8()?;
            let start = cursor.position;
            cursor.read_u8()?;
            let property_value_count = cursor.read_u16()? as usize;
            for _ in 0..property_value_count {
                parse_tagged_property(cursor)?;
            }
            let end = cursor.position;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: Some(output_handle_index),
                payload: cursor.bytes[start..end].to_vec(),
            })
        }
        Some(RopId::SynchronizationImportHierarchyChange) => {
            let input_handle_index = cursor.read_u8()?;
            let start = cursor.position;
            let hierarchy_count = cursor.read_u16()? as usize;
            for _ in 0..hierarchy_count {
                parse_tagged_property(cursor)?;
            }
            let property_count = cursor.read_u16()? as usize;
            for _ in 0..property_count {
                parse_tagged_property(cursor)?;
            }
            let end = cursor.position;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: cursor.bytes[start..end].to_vec(),
            })
        }
        Some(RopId::SynchronizationImportDeletes) => {
            let input_handle_index = cursor.read_u8()?;
            let delete_flags = cursor.read_u8()?;
            let message_id_count = cursor.read_u16()? as usize;
            let mut payload = vec![delete_flags];
            payload.extend_from_slice(&(message_id_count as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(message_id_count * 8)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::SynchronizationImportMessageMove) => {
            let input_handle_index = cursor.read_u8()?;
            let start = cursor.position;
            let source_folder_id_size = cursor.read_u32()? as usize;
            cursor.read_bytes(source_folder_id_size)?;
            let source_message_id_size = cursor.read_u32()? as usize;
            cursor.read_bytes(source_message_id_size)?;
            let predecessor_change_list_size = cursor.read_u32()? as usize;
            cursor.read_bytes(predecessor_change_list_size)?;
            let destination_message_id_size = cursor.read_u32()? as usize;
            cursor.read_bytes(destination_message_id_size)?;
            let change_number_size = cursor.read_u32()? as usize;
            cursor.read_bytes(change_number_size)?;
            let end = cursor.position;
            let mut payload = Vec::new();
            payload.extend_from_slice(&cursor.bytes[start..end]);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::SynchronizationImportReadStateChanges) => {
            let input_handle_index = cursor.read_u8()?;
            let count_or_size = cursor.read_u16()? as usize;
            let mut payload = Vec::new();
            payload.extend_from_slice(&(count_or_size as u16).to_le_bytes());
            let compact_size = count_or_size.saturating_mul(9);
            if count_or_size > 0 && count_or_size <= 1024 && cursor.remaining() >= compact_size {
                payload.extend_from_slice(cursor.read_bytes(compact_size)?);
            } else {
                payload.extend_from_slice(cursor.read_bytes(count_or_size)?);
            }
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::SetLocalReplicaMidsetDeleted) => {
            let input_handle_index = cursor.read_u8()?;
            let size = cursor.read_u16()? as usize;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: cursor.read_bytes(size)?.to_vec(),
            })
        }
        Some(RopId::TellVersion) => {
            let input_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: cursor.read_bytes(6)?.to_vec(),
            })
        }
        Some(RopId::GetLocalReplicaIds) => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(&cursor.read_u32()?.to_le_bytes());
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(
            RopId::GetPropertiesList
            | RopId::GetStatus
            | RopId::QueryPosition
            | RopId::QueryColumnsAll
            | RopId::SetSpooler
            | RopId::TransportSend
            | RopId::GetValidAttachments
            | RopId::GetReceiveFolderTable
            | RopId::GetTransportFolder
            | RopId::GetStoreState
            | RopId::ResetTable,
        ) => {
            let input_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: Vec::new(),
            })
        }
        Some(RopId::OptionsData) => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            let address_type = cursor.read_ascii_z()?;
            payload.extend_from_slice(address_type.as_bytes());
            payload.push(0);
            payload.push(cursor.read_u8()?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::ReloadCachedInformation) => {
            let input_handle_index = cursor.read_u8()?;
            let reserved = cursor.read_u16()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: reserved.to_le_bytes().to_vec(),
            })
        }
        Some(RopId::SeekRow) => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.push(cursor.read_u8()?);
            payload.extend_from_slice(&cursor.read_u32()?.to_le_bytes());
            payload.push(cursor.read_u8()?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::SeekRowBookmark) => {
            let input_handle_index = cursor.read_u8()?;
            let bookmark_size = cursor.read_u16()? as usize;
            let mut payload = Vec::new();
            payload.extend_from_slice(&(bookmark_size as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(bookmark_size)?);
            payload.extend_from_slice(&cursor.read_i32()?.to_le_bytes());
            payload.push(cursor.read_u8()?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::SeekRowFractional) => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(&cursor.read_u32()?.to_le_bytes());
            payload.extend_from_slice(&cursor.read_u32()?.to_le_bytes());
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::GetMessageStatus) => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(cursor.read_bytes(8)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::SetMessageStatus) => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(cursor.read_bytes(8)?);
            payload.extend_from_slice(&cursor.read_u32()?.to_le_bytes());
            payload.extend_from_slice(&cursor.read_u32()?.to_le_bytes());
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::CreateBookmark) => {
            let input_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: Vec::new(),
            })
        }
        Some(RopId::CreateFolder) => {
            let input_handle_index = cursor.read_u8()?;
            let output_handle_index = cursor.read_u8()?;
            let folder_type = cursor.read_u8()?;
            let use_unicode_raw = cursor.read_u8()?;
            let use_unicode = use_unicode_raw != 0;
            let open_existing = cursor.read_u8()?;
            let reserved = cursor.read_u8()?;
            let display_name = if use_unicode {
                cursor.read_utf16z()?
            } else {
                cursor.read_ascii_z()?
            };
            let comment = if use_unicode {
                cursor.read_utf16z()?
            } else {
                cursor.read_ascii_z()?
            };
            let mut payload = vec![folder_type, use_unicode_raw, open_existing, reserved];
            write_u16_prefixed_bytes(&mut payload, display_name.as_bytes());
            write_u16_prefixed_bytes(&mut payload, comment.as_bytes());
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: Some(output_handle_index),
                payload,
            })
        }
        Some(RopId::DeleteFolder) => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = vec![cursor.read_u8()?];
            payload.extend_from_slice(cursor.read_bytes(8)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::DeleteMessages | RopId::HardDeleteMessages) => {
            let input_handle_index = cursor.read_u8()?;
            let want_asynchronous = cursor.read_u8()?;
            let notify_non_read = cursor.read_u8()?;
            let message_id_count = cursor.read_u16()? as usize;
            let mut payload = vec![want_asynchronous, notify_non_read];
            payload.extend_from_slice(&(message_id_count as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(message_id_count * 8)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::ExpandRow) => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(&cursor.read_u16()?.to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(8)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::SetProperties | RopId::SetPropertiesNoReplicate) => {
            let input_handle_index = cursor.read_u8()?;
            let property_value_size = cursor.read_u16()? as usize;
            let property_value_count = cursor.read_u16()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(&(property_value_size as u16).to_le_bytes());
            payload.extend_from_slice(&property_value_count.to_le_bytes());
            let values_size = property_value_size
                .checked_sub(2)
                .ok_or_else(|| anyhow!("invalid RopSetProperties value size"))?;
            payload.extend_from_slice(cursor.read_bytes(values_size)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::DeleteProperties | RopId::DeletePropertiesNoReplicate) => {
            let input_handle_index = cursor.read_u8()?;
            let property_tag_count = cursor.read_u16()? as usize;
            let mut payload = Vec::new();
            payload.extend_from_slice(&(property_tag_count as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(property_tag_count * 4)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::ReadRecipients) => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(&cursor.read_u32()?.to_le_bytes());
            payload.extend_from_slice(&cursor.read_u16()?.to_le_bytes());
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::SaveChangesMessage) => {
            let response_handle_index = cursor.read_u8()?;
            let input_handle_index = cursor.read_u8()?;
            let payload = vec![cursor.read_u8()?];
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: Some(response_handle_index),
                payload,
            })
        }
        Some(RopId::RemoveAllRecipients) => {
            let input_handle_index = cursor.read_u8()?;
            let _reserved = cursor.read_u32()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: Vec::new(),
            })
        }
        Some(RopId::ModifyRecipients) => {
            let input_handle_index = cursor.read_u8()?;
            let column_count = cursor.read_u16()? as usize;
            let mut payload = Vec::new();
            payload.extend_from_slice(&(column_count as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(column_count * 4)?);
            let row_count = cursor.read_u16()? as usize;
            payload.extend_from_slice(&(row_count as u16).to_le_bytes());
            for _ in 0..row_count {
                payload.extend_from_slice(&cursor.read_u32()?.to_le_bytes());
                payload.push(cursor.read_u8()?);
                let row_size = cursor.read_u16()? as usize;
                payload.extend_from_slice(&(row_size as u16).to_le_bytes());
                payload.extend_from_slice(cursor.read_bytes(row_size)?);
            }
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::SetMessageReadFlag) => {
            let response_handle_index = cursor.read_u8()?;
            let input_handle_index = cursor.read_u8()?;
            let payload = vec![cursor.read_u8()?];
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: Some(response_handle_index),
                payload,
            })
        }
        Some(RopId::SetColumns) => {
            let input_handle_index = cursor.read_u8()?;
            let set_columns_flags = cursor.read_u8()?;
            let property_tag_count = cursor.read_u16()? as usize;
            let mut payload = vec![set_columns_flags];
            payload.extend_from_slice(&(property_tag_count as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(property_tag_count * 4)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::SortTable) => {
            let input_handle_index = cursor.read_u8()?;
            let sort_table_flags = cursor.read_u8()?;
            let sort_order_count = cursor.read_u16()? as usize;
            let category_count = cursor.read_u16()?;
            let expanded_count = cursor.read_u16()?;
            let mut payload = vec![sort_table_flags];
            payload.extend_from_slice(&(sort_order_count as u16).to_le_bytes());
            payload.extend_from_slice(&category_count.to_le_bytes());
            payload.extend_from_slice(&expanded_count.to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(sort_order_count * 5)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::Restrict) => {
            let input_handle_index = cursor.read_u8()?;
            let restrict_flags = cursor.read_u8()?;
            let restriction_size = cursor.read_u16()? as usize;
            let mut payload = vec![restrict_flags];
            payload.extend_from_slice(&(restriction_size as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(restriction_size)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::GetPropertiesSpecific) => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(&cursor.read_u16()?.to_le_bytes());
            let first_count_or_want_unicode = cursor.read_u16()?;
            let property_tag_count = if first_count_or_want_unicode <= 1 && cursor.remaining() >= 2
            {
                let checkpoint = cursor.position;
                let count = cursor.read_u16()? as usize;
                if cursor.remaining() >= count.saturating_mul(4) {
                    count
                } else {
                    cursor.position = checkpoint;
                    first_count_or_want_unicode as usize
                }
            } else {
                first_count_or_want_unicode as usize
            };
            payload.extend_from_slice(&(property_tag_count as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(property_tag_count * 4)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::QueryRows) => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.push(cursor.read_u8()?);
            payload.push(cursor.read_u8()?);
            payload.extend_from_slice(&cursor.read_u16()?.to_le_bytes());
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::GetReceiveFolder) => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            while cursor.remaining() > 0 {
                let byte = cursor.read_u8()?;
                payload.push(byte);
                if byte == 0 {
                    break;
                }
            }
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::SubmitMessage) => {
            let input_handle_index = cursor.read_u8()?;
            let payload = vec![cursor.read_u8()?];
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::AbortSubmit) => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(cursor.read_bytes(8)?);
            payload.extend_from_slice(cursor.read_bytes(8)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::MoveCopyMessages) => {
            let source_handle_index = cursor.read_u8()?;
            let dest_handle_index = cursor.read_u8()?;
            let message_id_count = cursor.read_u16()? as usize;
            let mut payload = Vec::new();
            payload.extend_from_slice(&(message_id_count as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(message_id_count * 8)?);
            payload.push(cursor.read_u8()?);
            payload.push(cursor.read_u8()?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(source_handle_index),
                output_handle_index: Some(dest_handle_index),
                payload,
            })
        }
        Some(RopId::MoveFolder | RopId::CopyFolder) => {
            let source_handle_index = cursor.read_u8()?;
            let dest_handle_index = cursor.read_u8()?;
            let want_asynchronous = cursor.read_u8()?;
            let mut payload = vec![want_asynchronous];
            if matches!(RopId::from_u8(rop_id), Some(RopId::CopyFolder)) {
                payload.push(cursor.read_u8()?);
            }
            let use_unicode = cursor.read_u8()?;
            payload.push(use_unicode);
            payload.extend_from_slice(cursor.read_bytes(8)?);
            if use_unicode == 0 {
                let folder_name = cursor.read_ascii_z()?;
                payload.extend_from_slice(folder_name.as_bytes());
                payload.push(0);
            } else {
                let folder_name = cursor.read_utf16z()?;
                write_utf16z(&mut payload, &folder_name);
            }
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(source_handle_index),
                output_handle_index: Some(dest_handle_index),
                payload,
            })
        }
        Some(RopId::SpoolerLockMessage) => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(cursor.read_bytes(8)?);
            payload.push(cursor.read_u8()?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::TransportNewMail) => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(cursor.read_bytes(8)?);
            payload.extend_from_slice(cursor.read_bytes(8)?);
            let message_class = cursor.read_ascii_z()?;
            payload.extend_from_slice(message_class.as_bytes());
            payload.push(0);
            payload.extend_from_slice(&cursor.read_u32()?.to_le_bytes());
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::FindRow) => {
            let input_handle_index = cursor.read_u8()?;
            let find_row_flags = cursor.read_u8()?;
            let restriction_size = cursor.read_u16()? as usize;
            let mut payload = vec![find_row_flags];
            payload.extend_from_slice(&(restriction_size as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(restriction_size)?);
            payload.push(cursor.read_u8()?);
            let bookmark_size = cursor.read_u16()? as usize;
            payload.extend_from_slice(&(bookmark_size as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(bookmark_size)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::GetNamesFromPropertyIds) => {
            let input_handle_index = cursor.read_u8()?;
            let property_id_count = cursor.read_u16()? as usize;
            let mut payload = Vec::new();
            payload.extend_from_slice(&(property_id_count as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(property_id_count * 2)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::UpdateDeferredActionMessages) => {
            let input_handle_index = cursor.read_u8()?;
            let server_entry_id_size = cursor.read_u16()? as usize;
            let mut payload = Vec::new();
            payload.extend_from_slice(&(server_entry_id_size as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(server_entry_id_size)?);
            let client_entry_id_size = cursor.read_u16()? as usize;
            payload.extend_from_slice(&(client_entry_id_size as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(client_entry_id_size)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::GetPropertyIdsFromNames) => {
            let input_handle_index = cursor.read_u8()?;
            let flags = cursor.read_u8()?;
            let property_name_count = cursor.read_u16()? as usize;
            let mut payload = vec![flags];
            payload.extend_from_slice(&(property_name_count as u16).to_le_bytes());
            for _ in 0..property_name_count {
                let kind = cursor.read_u8()?;
                payload.push(kind);
                payload.extend_from_slice(cursor.read_bytes(16)?);
                match kind {
                    0x00 => payload.extend_from_slice(&cursor.read_u32()?.to_le_bytes()),
                    0x01 => {
                        let name_size = cursor.read_u8()? as usize;
                        payload.push(name_size as u8);
                        payload.extend_from_slice(cursor.read_bytes(name_size)?);
                    }
                    _ => return Err(anyhow!("unsupported named property kind")),
                }
            }
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::QueryNamedProperties) => {
            let input_handle_index = cursor.read_u8()?;
            let query_flags = cursor.read_u8()?;
            let has_guid = cursor.read_u8()?;
            let mut payload = vec![query_flags, has_guid];
            if has_guid != 0 {
                payload.extend_from_slice(cursor.read_bytes(16)?);
            }
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::FreeBookmark) => {
            let input_handle_index = cursor.read_u8()?;
            let bookmark_size = cursor.read_u16()? as usize;
            let mut payload = Vec::new();
            payload.extend_from_slice(&(bookmark_size as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(bookmark_size)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        Some(RopId::Logon) => {
            let output_handle_index = cursor.read_u8()?;
            let logon_flags = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.push(logon_flags);
            payload.extend_from_slice(cursor.read_bytes(4)?);
            payload.extend_from_slice(cursor.read_bytes(4)?);
            let essdn_size = cursor.read_u16()? as usize;
            payload.extend_from_slice(&(essdn_size as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(essdn_size)?);
            if logon_flags & 0x40 != 0 {
                payload.extend_from_slice(cursor.read_bytes(cursor.remaining())?);
            }
            Ok(RopRequest {
                rop_id,
                input_handle_index: None,
                output_handle_index: Some(output_handle_index),
                payload,
            })
        }
        _ => {
            let input_handle_index = if cursor.remaining() > 0 {
                Some(cursor.read_u8()?)
            } else {
                None
            };
            let payload = if rop_id_is_reserved(rop_id) {
                cursor.read_bytes(cursor.remaining())?.to_vec()
            } else {
                Vec::new()
            };
            Ok(RopRequest {
                rop_id,
                input_handle_index,
                output_handle_index: None,
                payload,
            })
        }
    }
}

pub(in crate::mapi) struct ModifyRulesRow {
    pub(in crate::mapi) flags: u8,
    pub(in crate::mapi) properties: HashMap<u32, MapiValue>,
}

#[allow(dead_code)]
pub(in crate::mapi) fn serialize_rop_request(request: &RopRequest) -> Result<Vec<u8>> {
    let mut buffer = vec![request.rop_id, 0];
    match request.typed() {
        TypedRopRequest::Release(request) => buffer.push(request.input_handle_index),
        TypedRopRequest::OpenFolder(request) => {
            buffer.push(request.input_handle_index);
            buffer.push(request.output_handle_index);
            buffer.extend_from_slice(
                &crate::mapi::identity::wire_id_bytes_from_object_id(request.folder_id)
                    .ok_or_else(|| anyhow!("invalid OpenFolder folder id"))?,
            );
            buffer.push(request.open_mode_flags);
        }
        TypedRopRequest::OpenMessage(request) => {
            buffer.push(request.input_handle_index);
            buffer.push(request.output_handle_index);
            write_u16(&mut buffer, 0);
            buffer.extend_from_slice(
                &crate::mapi::identity::wire_id_bytes_from_object_id(request.folder_id)
                    .ok_or_else(|| anyhow!("invalid OpenMessage folder id"))?,
            );
            buffer.push(request.open_mode_flags);
            buffer.extend_from_slice(
                &crate::mapi::identity::wire_id_bytes_from_object_id(request.message_id)
                    .ok_or_else(|| anyhow!("invalid OpenMessage message id"))?,
            );
        }
        TypedRopRequest::OpenTable(request) => {
            buffer.push(request.input_handle_index);
            buffer.push(request.output_handle_index);
            buffer.push(request.table_flags);
        }
        TypedRopRequest::CreateMessage(request) => {
            buffer.push(request.input_handle_index);
            buffer.push(request.output_handle_index);
            write_u16(&mut buffer, 0);
            buffer.extend_from_slice(
                &crate::mapi::identity::wire_id_bytes_from_object_id(request.folder_id)
                    .ok_or_else(|| anyhow!("invalid CreateMessage folder id"))?,
            );
            buffer.push(request.associated_flag);
        }
        TypedRopRequest::SaveChangesMessage(request) => {
            buffer.push(request.response_handle_index);
            buffer.push(request.input_handle_index);
            buffer.push(request.save_flags);
        }
        TypedRopRequest::OpenEmbeddedMessage(request) => {
            buffer.push(request.input_handle_index);
            buffer.push(request.output_handle_index);
            write_u16(&mut buffer, request.code_page_id);
            buffer.push(request.open_mode_flags);
        }
        TypedRopRequest::SetColumns(request) => {
            buffer.push(request.input_handle_index);
            buffer.push(request.flags);
            write_u16(
                &mut buffer,
                request.property_tags.len().min(u16::MAX as usize) as u16,
            );
            for property_tag in request.property_tags {
                write_u32(&mut buffer, property_tag);
            }
        }
        TypedRopRequest::Restrict(request) if request.rop_id == 0x14 => {
            buffer.push(request.input_handle_index);
            buffer.push(request.flags);
            write_u16(
                &mut buffer,
                request.restriction.len().min(u16::MAX as usize) as u16,
            );
            buffer.extend_from_slice(&request.restriction);
        }
        TypedRopRequest::QueryRows(request) => {
            buffer.push(request.input_handle_index);
            buffer.push(request.flags);
            buffer.push(request.forward_read as u8);
            write_u16(&mut buffer, request.row_count);
        }
        TypedRopRequest::Logon(request) => {
            buffer.push(request.output_handle_index);
            buffer.push(request.logon_flags);
            buffer.extend_from_slice(&request.prefix);
            write_u16(
                &mut buffer,
                request.essdn.len().min(u16::MAX as usize) as u16,
            );
            buffer.extend_from_slice(&request.essdn);
        }
        TypedRopRequest::SupportedRaw(request) => {
            return Err(anyhow!(
                "ROP 0x{:02X} request serialization is not typed yet",
                request.rop_id
            ));
        }
        TypedRopRequest::Restrict(request) => {
            return Err(anyhow!(
                "ROP 0x{:02X} request serialization is not typed yet",
                request.rop_id
            ));
        }
        TypedRopRequest::Unsupported(request) => {
            return Err(anyhow!(
                "unsupported ROP 0x{:02X} request serialization",
                request.rop_id
            ));
        }
    }
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::super::transport::MAPI_SESSION_MAX_AGE_SECONDS;
    use super::*;

    fn test_hex_bytes(value: &str) -> Vec<u8> {
        let hex = value
            .chars()
            .filter(|character| !character.is_ascii_whitespace())
            .collect::<String>();
        assert_eq!(hex.len() % 2, 0);
        (0..hex.len())
            .step_by(2)
            .map(|index| u8::from_str_radix(&hex[index..index + 2], 16).unwrap())
            .collect()
    }

    #[test]
    fn split_rop_buffer_accepts_microsoft_spec_framing_examples() {
        let empty = [0x02, 0x00];
        let (requests, handles) = split_rop_buffer(&empty).unwrap();
        assert!(requests.is_empty());
        assert!(handles.is_empty());

        let single = [
            0x09, 0x00, 0x15, 0x01, 0x01, 0x02, 0x01, 0xFF, 0x0F, 0x6D, 0x00, 0x00, 0x00, 0x56,
            0x00, 0x00, 0x00,
        ];
        let (requests, handles) = split_rop_buffer(&single).unwrap();
        assert_eq!(requests, &[0x15, 0x01, 0x01, 0x02, 0x01, 0xFF, 0x0F]);
        assert_eq!(handles, &[0x6D, 0x00, 0x00, 0x00, 0x56, 0x00, 0x00, 0x00]);

        let multiple = [
            0x14, 0x00, 0x02, 0x00, 0x00, 0x01, 0x01, 0x00, 0x59, 0x65, 0x73, 0x73, 0x69, 0x72,
            0x00, 0x04, 0x00, 0x01, 0x02, 0x04, 0x6E, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF,
        ];
        let (requests, handles) = split_rop_buffer(&multiple).unwrap();
        assert_eq!(
            requests,
            &[
                0x02, 0x00, 0x00, 0x01, 0x01, 0x00, 0x59, 0x65, 0x73, 0x73, 0x69, 0x72, 0x00, 0x04,
                0x00, 0x01, 0x02, 0x04,
            ]
        );
        assert_eq!(
            handles,
            &[0x6E, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,]
        );

        let release = [
            0x08, 0x00, 0x01, 0x00, 0x00, 0x01, 0x00, 0x01, 0x6F, 0x00, 0x00, 0x00, 0x6E, 0x00,
            0x00, 0x00,
        ];
        let (requests, handles) = split_rop_buffer(&release).unwrap();
        assert_eq!(requests, &[0x01, 0x00, 0x00, 0x01, 0x00, 0x01]);
        assert_eq!(handles, &[0x6F, 0x00, 0x00, 0x00, 0x6E, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn split_rop_buffer_preserves_legacy_framing_when_handle_table_is_valid() {
        let legacy = rop_buffer_with_response(vec![0x01, 0x00, 0x00], &[0x34]);
        let (requests, handles) = split_rop_buffer(&legacy).unwrap();

        assert_eq!(requests, &[0x01, 0x00, 0x00]);
        assert_eq!(handles, &[0x34, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn message_create_and_save_responses_match_microsoft_message_examples() {
        let create = RopRequest {
            rop_id: RopId::CreateMessage.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: Some(1),
            payload: Vec::new(),
        };
        assert_eq!(
            rop_create_message_response(&create),
            vec![0x06, 0x01, 0, 0, 0, 0, 0]
        );

        let save = RopRequest {
            rop_id: RopId::SaveChangesMessage.as_u8(),
            input_handle_index: Some(1),
            output_handle_index: Some(0),
            payload: vec![0x0A],
        };
        assert_eq!(
            rop_save_changes_message_response(&save, 0x3986_F000_0000_0101),
            vec![0x0C, 0x00, 0, 0, 0, 0, 0x01, 0x01, 0x01, 0x00, 0x00, 0x00, 0xF0, 0x86, 0x39,]
        );
    }

    #[test]
    fn attachment_create_and_save_responses_match_microsoft_message_examples() {
        let table = RopRequest {
            rop_id: RopId::GetAttachmentTable.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: Some(1),
            payload: vec![0x00],
        };
        assert_eq!(
            rop_get_attachment_table_response(&table),
            vec![0x21, 0x01, 0, 0, 0, 0]
        );

        let create = RopRequest {
            rop_id: RopId::CreateAttachment.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: Some(3),
            payload: Vec::new(),
        };
        assert_eq!(
            rop_create_attachment_response(&create, 1),
            vec![0x23, 0x03, 0, 0, 0, 0, 1, 0, 0, 0]
        );

        let save = RopRequest {
            rop_id: RopId::SaveChangesAttachment.as_u8(),
            input_handle_index: Some(1),
            output_handle_index: Some(2),
            payload: vec![0x0A],
        };
        assert_eq!(
            rop_simple_success_response(&save),
            vec![0x25, 0x02, 0, 0, 0, 0]
        );
    }

    #[test]
    fn microsoft_oxcmsg_core_request_examples_parse_expected_fields() {
        let create_message_golden = vec![
            0x06, 0x00, 0x00, 0x01, 0xFF, 0x0F, 0x01, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x79, 0x93,
            0x00,
        ];
        let mut create_message_cursor = Cursor::new(&create_message_golden);
        let create_message = read_rop_request(&mut create_message_cursor).unwrap();

        assert_eq!(
            RopId::from_u8(create_message.rop_id),
            Some(RopId::CreateMessage)
        );
        assert_eq!(create_message.input_handle_index, Some(0));
        assert_eq!(create_message.output_handle_index, Some(1));
        assert_eq!(
            create_message.payload,
            vec![0x01, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x79, 0x93, 0x00]
        );
        assert!(!create_message.create_message_associated());
        assert_eq!(create_message_cursor.remaining(), 0);

        let attachment_table_golden = vec![0x21, 0x00, 0x00, 0x01, 0x00];
        let mut attachment_table_cursor = Cursor::new(&attachment_table_golden);
        let attachment_table = read_rop_request(&mut attachment_table_cursor).unwrap();

        assert_eq!(
            attachment_table.typed(),
            TypedRopRequest::OpenTable(RopOpenTableRequest {
                rop_id: RopId::GetAttachmentTable.as_u8(),
                input_handle_index: 0,
                output_handle_index: 1,
                table_flags: 0,
            })
        );
        assert_eq!(
            serialize_rop_request(&attachment_table).unwrap(),
            attachment_table_golden
        );
        assert_eq!(attachment_table_cursor.remaining(), 0);

        let save_message_golden = vec![0x0C, 0x00, 0x00, 0x01, 0x0A];
        let mut save_message_cursor = Cursor::new(&save_message_golden);
        let save_message = read_rop_request(&mut save_message_cursor).unwrap();

        assert_eq!(
            save_message.typed(),
            TypedRopRequest::SaveChangesMessage(RopSaveChangesMessageRequest {
                response_handle_index: 0,
                input_handle_index: 1,
                save_flags: 0x0A,
            })
        );
        assert_eq!(
            serialize_rop_request(&save_message).unwrap(),
            save_message_golden
        );
        assert_eq!(save_message_cursor.remaining(), 0);
    }

    #[test]
    fn microsoft_oxcmsg_attachment_request_examples_parse_expected_fields() {
        for (golden, output_handle_index) in [
            (vec![0x23, 0x00, 0x00, 0x01], 1),
            (vec![0x23, 0x00, 0x00, 0x03], 3),
        ] {
            let mut cursor = Cursor::new(&golden);
            let request = read_rop_request(&mut cursor).unwrap();

            assert_eq!(
                RopId::from_u8(request.rop_id),
                Some(RopId::CreateAttachment)
            );
            assert_eq!(request.input_handle_index, Some(0));
            assert_eq!(request.output_handle_index, Some(output_handle_index));
            assert!(request.payload.is_empty());
            assert_eq!(cursor.remaining(), 0);
        }

        for (golden, response_handle_index, input_handle_index) in [
            (vec![0x25, 0x00, 0x01, 0x00, 0x0A], 1, 0),
            (vec![0x25, 0x00, 0x02, 0x01, 0x0A], 2, 1),
        ] {
            let mut cursor = Cursor::new(&golden);
            let request = read_rop_request(&mut cursor).unwrap();

            assert_eq!(
                RopId::from_u8(request.rop_id),
                Some(RopId::SaveChangesAttachment)
            );
            assert_eq!(request.input_handle_index, Some(input_handle_index));
            assert_eq!(request.output_handle_index, Some(response_handle_index));
            assert_eq!(request.payload, vec![0x0A]);
            assert_eq!(request.response_handle_index(), response_handle_index);
            assert_eq!(cursor.remaining(), 0);
        }
    }

    #[test]
    fn microsoft_oxcmsg_modify_recipients_example_parses_wrapped_recipient_row() {
        let golden = test_hex_bytes(
            "\
            0e00080c000300fe0f030000391f00ff391f00fe390300713a030005391f00f6\
            5f0300fd5f0300ff5f0300de5f0300df5f0201f75f010000000000012701\
            51065a00557365723200750073006500720032000000750073006500720032000000\
            0c0000060000000000000075007300650072003200000075007300650072003200\
            400073007a0066006b0075006b002d0064006f006d002e006500780074006500\
            730074002e006d006900630072006f0073006f00660074002e0063006f006d00\
            000000000000000000407500730065007200320000000100000000000000000000\
            00000000007c0000000000dca740c8c042101ab4b908002b2fe1820100000000\
            0000002f6f3d4669727374204f7267616e697a6174696f6e2f6f753d45786368\
            616e67652041646d696e6973747261746976652047726f757020284659444942\
            4f484632335350444c54292f636e3d526563697069656e74732f636e3d757365\
            723200",
        );
        let mut cursor = Cursor::new(&golden);
        let request = read_rop_request(&mut cursor).unwrap();

        assert_eq!(
            RopId::from_u8(request.rop_id),
            Some(RopId::ModifyRecipients)
        );
        assert_eq!(request.input_handle_index, Some(8));
        assert_eq!(request.output_handle_index, None);
        assert_eq!(request.property_tags().len(), 12);
        assert_eq!(
            request.property_tags(),
            vec![
                0x0FFE_0003,
                0x3900_0003,
                0x39FF_001F,
                0x39FE_001F,
                0x3A71_0003,
                0x3905_0003,
                0x5FF6_001F,
                0x5FFD_0003,
                0x5FFF_0003,
                0x5FDE_0003,
                0x5FDF_0003,
                0x5FF7_0102,
            ]
        );

        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id: Uuid::nil(),
            email: "sender@example.test".to_string(),
            display_name: "Sender".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };
        let changes = request.modify_recipients(&principal, &[]).unwrap();

        assert_eq!(changes.len(), 1);
        let PendingRecipientChange::Upsert(recipient) = &changes[0] else {
            panic!("MS-OXCMSG 4.7 row should upsert one recipient");
        };
        assert_eq!(recipient.recipient_type, 1);
        assert_eq!(recipient.address, "user2@szfkuk-dom.extest.microsoft.com");
        assert_eq!(recipient.display_name.as_deref(), Some("user2"));
        assert_eq!(cursor.remaining(), 0);
    }

    #[test]
    fn folder_create_and_hierarchy_table_responses_match_microsoft_folder_examples() {
        let create = RopRequest {
            rop_id: RopId::CreateFolder.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: Some(1),
            payload: Vec::new(),
        };
        assert_eq!(
            rop_create_folder_response(
                &create,
                crate::mapi::identity::mapi_store_id(0x0E91_5212),
                false,
            ),
            vec![0x1C, 0x01, 0, 0, 0, 0, 0x01, 0x00, 0x00, 0x00, 0x0E, 0x91, 0x52, 0x12, 0x00]
        );

        let hierarchy = RopRequest {
            rop_id: RopId::GetHierarchyTable.as_u8(),
            input_handle_index: Some(1),
            output_handle_index: Some(2),
            payload: vec![0x00],
        };
        assert_eq!(
            rop_get_hierarchy_table_response(&hierarchy, 21),
            vec![0x04, 0x02, 0, 0, 0, 0, 0x15, 0x00, 0x00, 0x00]
        );
    }

    #[test]
    fn folder_mutation_responses_match_microsoft_folder_examples() {
        for (rop_id, handle_index, expected) in [
            (
                RopId::DeleteFolder.as_u8(),
                1,
                vec![0x1D, 0x01, 0, 0, 0, 0, 0],
            ),
            (
                RopId::DeleteMessages.as_u8(),
                0,
                vec![0x1E, 0x00, 0, 0, 0, 0, 0],
            ),
            (
                RopId::MoveCopyMessages.as_u8(),
                0,
                vec![0x33, 0x00, 0, 0, 0, 0, 0],
            ),
            (
                RopId::MoveFolder.as_u8(),
                1,
                vec![0x35, 0x01, 0, 0, 0, 0, 0],
            ),
            (
                RopId::CopyFolder.as_u8(),
                0,
                vec![0x36, 0x00, 0, 0, 0, 0, 0],
            ),
        ] {
            let request = RopRequest {
                rop_id,
                input_handle_index: Some(handle_index),
                output_handle_index: None,
                payload: Vec::new(),
            };
            assert_eq!(
                rop_partial_completion_response(rop_id, request.response_handle_index(), false),
                expected
            );
        }

        let set_search = RopRequest {
            rop_id: RopId::SetSearchCriteria.as_u8(),
            input_handle_index: Some(1),
            output_handle_index: None,
            payload: Vec::new(),
        };
        assert_eq!(
            rop_simple_success_response(&set_search),
            vec![0x30, 0x01, 0, 0, 0, 0]
        );
    }

    #[test]
    fn microsoft_oxcfold_create_and_hierarchy_examples_parse_through_typed_parser() {
        let create_golden = vec![
            0x1C, 0x00, 0x00, 0x01, 0x01, 0x01, 0x00, 0x00, 0x46, 0x00, 0x6F, 0x00, 0x6C, 0x00,
            0x64, 0x00, 0x65, 0x00, 0x72, 0x00, 0x31, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        let mut create_cursor = Cursor::new(&create_golden);
        let create = read_rop_request(&mut create_cursor).unwrap();

        assert_eq!(RopId::from_u8(create.rop_id), Some(RopId::CreateFolder));
        assert_eq!(create.input_handle_index, Some(0));
        assert_eq!(create.output_handle_index, Some(1));
        assert_eq!(create.create_folder_type(), 1);
        assert!(!create.create_folder_open_existing());
        assert_eq!(create.create_folder_reserved(), 0);
        assert_eq!(create.create_folder_display_name(), "Folder1");
        assert_eq!(create.payload.get(13..15), Some(&[0x00, 0x00][..]));
        assert_eq!(create_cursor.remaining(), 0);

        let hierarchy_golden = vec![0x04, 0x00, 0x01, 0x02, 0x00];
        let mut hierarchy_cursor = Cursor::new(&hierarchy_golden);
        let hierarchy = read_rop_request(&mut hierarchy_cursor).unwrap();

        assert_eq!(
            hierarchy.typed(),
            TypedRopRequest::OpenTable(RopOpenTableRequest {
                rop_id: RopId::GetHierarchyTable.as_u8(),
                input_handle_index: 1,
                output_handle_index: 2,
                table_flags: 0,
            })
        );
        assert_eq!(serialize_rop_request(&hierarchy).unwrap(), hierarchy_golden);
        assert_eq!(hierarchy_cursor.remaining(), 0);
    }

    #[test]
    fn microsoft_oxcfold_folder_mutation_examples_parse_expected_fields() {
        let delete_folder_golden = vec![
            0x1D, 0x00, 0x01, 0x05, 0x01, 0x00, 0x00, 0x00, 0x0E, 0x8E, 0xDF, 0x36,
        ];
        let mut delete_folder_cursor = Cursor::new(&delete_folder_golden);
        let delete_folder = read_rop_request(&mut delete_folder_cursor).unwrap();

        assert_eq!(
            RopId::from_u8(delete_folder.rop_id),
            Some(RopId::DeleteFolder)
        );
        assert_eq!(delete_folder.input_handle_index, Some(1));
        assert_eq!(delete_folder.delete_folder_flags(), Some(0x05));
        assert_eq!(
            delete_folder.payload,
            vec![0x05, 0x01, 0x00, 0x00, 0x00, 0x0E, 0x8E, 0xDF, 0x36]
        );
        assert_eq!(delete_folder_cursor.remaining(), 0);

        let delete_messages_golden = vec![
            0x1E, 0x00, 0x00, 0x00, 0x01, 0x02, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0E, 0x8E, 0xF1,
            0x48, 0x01, 0x00, 0x00, 0x00, 0x0E, 0x8E, 0xC3, 0x02,
        ];
        let mut delete_messages_cursor = Cursor::new(&delete_messages_golden);
        let delete_messages = read_rop_request(&mut delete_messages_cursor).unwrap();

        assert_eq!(
            RopId::from_u8(delete_messages.rop_id),
            Some(RopId::DeleteMessages)
        );
        assert_eq!(delete_messages.input_handle_index, Some(0));
        assert_eq!(delete_messages.delete_messages_want_asynchronous(), Some(0));
        assert_eq!(delete_messages.delete_messages_notify_non_read(), Some(1));
        assert_eq!(&delete_messages.payload[..4], &[0x00, 0x01, 0x02, 0x00]);
        assert_eq!(delete_messages.payload.len(), 20);
        assert_eq!(delete_messages_cursor.remaining(), 0);

        let move_copy_messages_golden = vec![
            0x33, 0x00, 0x00, 0x01, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0E, 0x8E, 0xEC, 0x5D,
            0x00, 0x00,
        ];
        let mut move_copy_messages_cursor = Cursor::new(&move_copy_messages_golden);
        let move_copy_messages = read_rop_request(&mut move_copy_messages_cursor).unwrap();

        assert_eq!(
            RopId::from_u8(move_copy_messages.rop_id),
            Some(RopId::MoveCopyMessages)
        );
        assert_eq!(move_copy_messages.input_handle_index, Some(0));
        assert_eq!(move_copy_messages.output_handle_index, Some(1));
        assert_eq!(move_copy_messages.move_copy_want_asynchronous(), Some(0));
        assert_eq!(move_copy_messages.move_copy_want_copy_raw(), Some(0));
        assert!(!move_copy_messages.move_copy_want_copy());
        assert_eq!(
            move_copy_messages.payload,
            vec![0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0E, 0x8E, 0xEC, 0x5D, 0x00, 0x00]
        );
        assert_eq!(move_copy_messages_cursor.remaining(), 0);
    }

    #[test]
    fn microsoft_oxcfold_folder_move_copy_and_search_examples_parse_expected_fields() {
        let move_folder_golden = vec![
            0x35, 0x00, 0x01, 0x02, 0x01, 0x01, 0x01, 0x00, 0x00, 0x00, 0x0E, 0x8E, 0xDF, 0x36,
            0x46, 0x00, 0x6F, 0x00, 0x6C, 0x00, 0x64, 0x00, 0x65, 0x00, 0x72, 0x00, 0x31, 0x00,
            0x00, 0x00,
        ];
        let mut move_folder_cursor = Cursor::new(&move_folder_golden);
        let move_folder = read_rop_request(&mut move_folder_cursor).unwrap();

        assert_eq!(RopId::from_u8(move_folder.rop_id), Some(RopId::MoveFolder));
        assert_eq!(move_folder.input_handle_index, Some(1));
        assert_eq!(move_folder.output_handle_index, Some(2));
        assert_eq!(move_folder.folder_move_copy_want_asynchronous(), Some(1));
        assert_eq!(move_folder.folder_move_copy_use_unicode(), Some(1));
        assert_eq!(move_folder.folder_move_copy_display_name(), "Folder1");
        assert_eq!(move_folder_cursor.remaining(), 0);

        let copy_folder_golden = vec![
            0x36, 0x00, 0x00, 0x01, 0x01, 0x01, 0x01, 0x01, 0x00, 0x00, 0x00, 0x0E, 0x8E, 0xDF,
            0x36, 0x46, 0x00, 0x6F, 0x00, 0x6C, 0x00, 0x64, 0x00, 0x65, 0x00, 0x72, 0x00, 0x31,
            0x00, 0x00, 0x00,
        ];
        let mut copy_folder_cursor = Cursor::new(&copy_folder_golden);
        let copy_folder = read_rop_request(&mut copy_folder_cursor).unwrap();

        assert_eq!(RopId::from_u8(copy_folder.rop_id), Some(RopId::CopyFolder));
        assert_eq!(copy_folder.input_handle_index, Some(0));
        assert_eq!(copy_folder.output_handle_index, Some(1));
        assert_eq!(copy_folder.folder_move_copy_want_asynchronous(), Some(1));
        assert_eq!(copy_folder.folder_move_copy_want_recursive(), Some(1));
        assert_eq!(copy_folder.folder_move_copy_use_unicode(), Some(1));
        assert_eq!(copy_folder.folder_move_copy_display_name(), "Folder1");
        assert_eq!(copy_folder_cursor.remaining(), 0);

        let get_search_golden = vec![0x31, 0x00, 0x00, 0x01, 0x01, 0x00];
        let mut get_search_cursor = Cursor::new(&get_search_golden);
        let get_search = read_rop_request(&mut get_search_cursor).unwrap();

        assert_eq!(
            RopId::from_u8(get_search.rop_id),
            Some(RopId::GetSearchCriteria)
        );
        assert_eq!(get_search.input_handle_index, Some(0));
        assert!(get_search.get_search_criteria_use_unicode());
        assert!(get_search.get_search_criteria_include_restriction());
        assert!(!get_search.get_search_criteria_include_folders());
        assert_eq!(get_search_cursor.remaining(), 0);
    }

    #[test]
    fn microsoft_oxcfold_set_search_criteria_example_parses_scope_and_flags() {
        let golden = vec![
            0x30, 0x00, 0x01, 0x29, 0x01, 0x00, 0x02, 0x00, 0x00, 0x07, 0x00, 0x02, 0x03, 0x02,
            0x00, 0x01, 0x00, 0x1F, 0x00, 0x1A, 0x00, 0x1F, 0x00, 0x1A, 0x00, 0x49, 0x00, 0x50,
            0x00, 0x4D, 0x00, 0x2E, 0x00, 0x41, 0x00, 0x70, 0x00, 0x70, 0x00, 0x6F, 0x00, 0x69,
            0x00, 0x6E, 0x00, 0x74, 0x00, 0x6D, 0x00, 0x65, 0x00, 0x6E, 0x00, 0x74, 0x00, 0x00,
            0x00, 0x02, 0x03, 0x02, 0x00, 0x01, 0x00, 0x1F, 0x00, 0x1A, 0x00, 0x1F, 0x00, 0x1A,
            0x00, 0x49, 0x00, 0x50, 0x00, 0x4D, 0x00, 0x2E, 0x00, 0x43, 0x00, 0x6F, 0x00, 0x6E,
            0x00, 0x74, 0x00, 0x61, 0x00, 0x63, 0x00, 0x74, 0x00, 0x00, 0x00, 0x02, 0x03, 0x02,
            0x00, 0x01, 0x00, 0x1F, 0x00, 0x1A, 0x00, 0x1F, 0x00, 0x1A, 0x00, 0x49, 0x00, 0x50,
            0x00, 0x4D, 0x00, 0x2E, 0x00, 0x44, 0x00, 0x69, 0x00, 0x73, 0x00, 0x74, 0x00, 0x4C,
            0x00, 0x69, 0x00, 0x73, 0x00, 0x74, 0x00, 0x00, 0x00, 0x02, 0x03, 0x02, 0x00, 0x01,
            0x00, 0x1F, 0x00, 0x1A, 0x00, 0x1F, 0x00, 0x1A, 0x00, 0x49, 0x00, 0x50, 0x00, 0x4D,
            0x00, 0x2E, 0x00, 0x41, 0x00, 0x63, 0x00, 0x74, 0x00, 0x69, 0x00, 0x76, 0x00, 0x69,
            0x00, 0x74, 0x00, 0x79, 0x00, 0x00, 0x00, 0x02, 0x03, 0x02, 0x00, 0x01, 0x00, 0x1F,
            0x00, 0x1A, 0x00, 0x1F, 0x00, 0x1A, 0x00, 0x49, 0x00, 0x50, 0x00, 0x4D, 0x00, 0x2E,
            0x00, 0x53, 0x00, 0x74, 0x00, 0x69, 0x00, 0x63, 0x00, 0x6B, 0x00, 0x79, 0x00, 0x4E,
            0x00, 0x6F, 0x00, 0x74, 0x00, 0x65, 0x00, 0x00, 0x00, 0x02, 0x03, 0x00, 0x00, 0x01,
            0x00, 0x1F, 0x00, 0x1A, 0x00, 0x1F, 0x00, 0x1A, 0x00, 0x49, 0x00, 0x50, 0x00, 0x4D,
            0x00, 0x2E, 0x00, 0x54, 0x00, 0x61, 0x00, 0x73, 0x00, 0x6B, 0x00, 0x00, 0x00, 0x02,
            0x03, 0x02, 0x00, 0x01, 0x00, 0x1F, 0x00, 0x1A, 0x00, 0x1F, 0x00, 0x1A, 0x00, 0x49,
            0x00, 0x50, 0x00, 0x4D, 0x00, 0x2E, 0x00, 0x54, 0x00, 0x61, 0x00, 0x73, 0x00, 0x6B,
            0x00, 0x2E, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x04, 0x04, 0x03, 0x00, 0x17, 0x00,
            0x03, 0x00, 0x17, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x14, 0x88, 0x2A, 0x00, 0x02, 0x00,
        ];
        let mut cursor = Cursor::new(&golden);
        let request = read_rop_request(&mut cursor).unwrap();

        assert_eq!(
            RopId::from_u8(request.rop_id),
            Some(RopId::SetSearchCriteria)
        );
        assert_eq!(request.input_handle_index, Some(1));
        assert_eq!(request.payload.get(..2), Some(&[0x29, 0x01][..]));
        assert_eq!(request.payload.get(299..301), Some(&[0x01, 0x00][..]));
        assert_eq!(
            request.payload.get(301..309),
            Some(&[0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x88][..])
        );
        assert_eq!(
            request.payload.get(309..313),
            Some(&[0x2A, 0x00, 0x02, 0x00][..])
        );
        assert_eq!(request.payload.len(), 313);
        assert_eq!(cursor.remaining(), 0);
    }

    #[test]
    fn contents_table_responses_match_microsoft_table_examples() {
        let open = RopRequest {
            rop_id: RopId::GetContentsTable.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: Some(1),
            payload: vec![0],
        };
        assert_eq!(
            rop_get_contents_table_response(&open, 4),
            vec![0x05, 0x01, 0, 0, 0, 0, 4, 0, 0, 0]
        );

        let set_columns = RopRequest {
            rop_id: RopId::SetColumns.as_u8(),
            input_handle_index: Some(1),
            output_handle_index: None,
            payload: Vec::new(),
        };
        assert_eq!(
            rop_set_columns_response(&set_columns),
            vec![0x12, 0x01, 0, 0, 0, 0, 0]
        );

        let sort = RopRequest {
            rop_id: RopId::SortTable.as_u8(),
            input_handle_index: Some(1),
            output_handle_index: None,
            payload: Vec::new(),
        };
        assert_eq!(
            rop_sort_table_response(&sort),
            vec![0x13, 0x01, 0, 0, 0, 0, 0]
        );
    }

    #[test]
    fn expand_row_response_matches_microsoft_category_example() {
        let request = RopRequest {
            rop_id: RopId::ExpandRow.as_u8(),
            input_handle_index: Some(1),
            output_handle_index: None,
            payload: Vec::new(),
        };

        assert_eq!(
            rop_expand_row_success_response(&request, 3, Vec::new()),
            vec![0x59, 0x01, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0]
        );
    }

    #[test]
    fn microsoft_oxcrops_rop_buffer_request_examples_parse_expected_fields() {
        fn split_rop_buffer(buffer: &[u8]) -> (&[u8], &[u8]) {
            let rop_size = u16::from_le_bytes(buffer[0..2].try_into().unwrap()) as usize;
            (&buffer[2..rop_size], &buffer[rop_size..])
        }

        let empty = [0x02, 0x00];
        let (rops, handles) = split_rop_buffer(&empty);
        assert!(rops.is_empty());
        assert!(handles.is_empty());

        let single = [
            0x09, 0x00, 0x15, 0x01, 0x01, 0x02, 0x01, 0xFF, 0x0F, 0x6D, 0x00, 0x00, 0x00, 0x56,
            0x00, 0x00, 0x00,
        ];
        let (single_rops, single_handles) = split_rop_buffer(&single);
        let mut cursor = Cursor::new(single_rops);
        let query_rows = read_rop_request(&mut cursor).unwrap();
        assert_eq!(
            query_rows.typed(),
            TypedRopRequest::QueryRows(RopQueryRowsRequest {
                input_handle_index: 1,
                flags: 2,
                forward_read: true,
                row_count: 0x0FFF,
            })
        );
        let mut serialized_query_rows = serialize_rop_request(&query_rows).unwrap();
        // RopRequest does not retain LogonId; this assertion still verifies every modeled field.
        serialized_query_rows[1] = single_rops[1];
        assert_eq!(serialized_query_rows, single_rops);
        assert_eq!(cursor.remaining(), 0);
        assert_eq!(
            single_handles
                .chunks_exact(4)
                .map(|handle| u32::from_le_bytes(handle.try_into().unwrap()))
                .collect::<Vec<_>>(),
            vec![0x6D, 0x56]
        );

        let multiple = [
            0x14, 0x00, 0x02, 0x00, 0x00, 0x01, 0x01, 0x00, 0x59, 0x65, 0x73, 0x73, 0x69, 0x72,
            0x00, 0x04, 0x00, 0x01, 0x02, 0x04, 0x6E, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF,
        ];
        let (multiple_rops, multiple_handles) = split_rop_buffer(&multiple);
        let mut cursor = Cursor::new(multiple_rops);
        let open_folder = read_rop_request(&mut cursor).unwrap();
        let hierarchy_table = read_rop_request(&mut cursor).unwrap();
        assert_eq!(
            open_folder.typed(),
            TypedRopRequest::OpenFolder(RopOpenFolderRequest {
                input_handle_index: 0,
                output_handle_index: 1,
                folder_id: 0x5965_7373_6972_0001,
                open_mode_flags: 0,
            })
        );
        assert_eq!(
            hierarchy_table.typed(),
            TypedRopRequest::OpenTable(RopOpenTableRequest {
                rop_id: RopId::GetHierarchyTable.as_u8(),
                input_handle_index: 1,
                output_handle_index: 2,
                table_flags: 4,
            })
        );
        let mut serialized = serialize_rop_request(&open_folder).unwrap();
        serialized.extend_from_slice(&serialize_rop_request(&hierarchy_table).unwrap());
        assert_eq!(serialized, multiple_rops);
        assert_eq!(cursor.remaining(), 0);
        assert_eq!(
            multiple_handles
                .chunks_exact(4)
                .map(|handle| u32::from_le_bytes(handle.try_into().unwrap()))
                .collect::<Vec<_>>(),
            vec![0x6E, u32::MAX, u32::MAX]
        );

        let release_pair = [
            0x08, 0x00, 0x01, 0x00, 0x00, 0x01, 0x00, 0x01, 0x6F, 0x00, 0x00, 0x00, 0x6E, 0x00,
            0x00, 0x00,
        ];
        let (release_rops, release_handles) = split_rop_buffer(&release_pair);
        let mut cursor = Cursor::new(release_rops);
        let first_release = read_rop_request(&mut cursor).unwrap();
        let second_release = read_rop_request(&mut cursor).unwrap();
        assert_eq!(
            first_release.typed(),
            TypedRopRequest::Release(RopInputOnlyRequest {
                rop_id: RopId::Release.as_u8(),
                input_handle_index: 0,
            })
        );
        assert_eq!(
            second_release.typed(),
            TypedRopRequest::Release(RopInputOnlyRequest {
                rop_id: RopId::Release.as_u8(),
                input_handle_index: 1,
            })
        );
        let mut serialized = serialize_rop_request(&first_release).unwrap();
        serialized.extend_from_slice(&serialize_rop_request(&second_release).unwrap());
        assert_eq!(serialized, release_rops);
        assert_eq!(cursor.remaining(), 0);
        assert_eq!(
            release_handles
                .chunks_exact(4)
                .map(|handle| u32::from_le_bytes(handle.try_into().unwrap()))
                .collect::<Vec<_>>(),
            vec![0x6F, 0x6E]
        );
    }

    #[test]
    fn buffer_too_small_response_matches_microsoft_rop_layout() {
        let request = [
            0x03, 0x00, 0x00, 0x01, 0xFF, 0x0F, 0x01, 0x00, 0x15, 0x89, 0x00, 0x78, 0x27, 0x1E,
            0x03, 0x01, 0x00, 0x15, 0x89, 0x00, 0x78, 0x2F, 0xBB,
        ];
        let handles = [0x12, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF];

        let response = rop_buffer_too_small_response(0x002C, &request, &handles);

        assert_eq!(&response[..3], &[0x1C, 0x00, 0xFF]);
        assert_eq!(&response[3..5], &0x002Cu16.to_le_bytes());
        assert_eq!(&response[5..28], request.as_slice());
        assert_eq!(&response[28..], handles.as_slice());
    }

    #[test]
    fn backoff_response_matches_microsoft_logon_example() {
        let set_columns = RopRequest {
            rop_id: RopId::SetColumns.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: Vec::new(),
        };
        let mut response = rop_set_columns_response(&set_columns);
        response.extend_from_slice(&rop_backoff_response(0, 0x1234, &[], &[]));

        assert_eq!(
            rop_buffer_with_response_spec(response, &[0x28]),
            vec![
                0x12, 0x00, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF9, 0x00, 0x34, 0x12, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x28, 0x00, 0x00, 0x00,
            ]
        );
    }

    #[test]
    fn backoff_response_matches_microsoft_targeted_rop_example() {
        let open_folder = RopRequest {
            rop_id: RopId::OpenFolder.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: Some(1),
            payload: Vec::new(),
        };
        let mut response = rop_open_folder_response(&open_folder, false);
        response.extend_from_slice(&rop_backoff_response(0, 0, &[(0x1C, 0x0004_4F17)], &[]));

        assert_eq!(
            rop_buffer_with_response_spec(response, &[0x0A, 0x24]),
            vec![
                0x18, 0x00, 0x02, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF9, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x01, 0x1C, 0x17, 0x4F, 0x04, 0x00, 0x00, 0x00, 0x0A, 0x00, 0x00, 0x00,
                0x24, 0x00, 0x00, 0x00,
            ]
        );
    }

    #[test]
    fn get_properties_specific_returns_typed_value_for_unspecified_subject() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id: Uuid::from_u128(0xbbbbbbbb_bbbb_bbbb_bbbb_bbbbbbbbbbbb),
            email: "alice@example.test".to_string(),
            display_name: "Alice".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };
        let mailbox_id = Uuid::from_u128(0x11111111111111111111111111111111);
        let email_id = Uuid::from_u128(0x22222222222222222222222222222222);
        let message_id = crate::mapi::identity::mapi_store_id(0x2222);
        crate::mapi::identity::remember_mapi_identity(mailbox_id, INBOX_FOLDER_ID);
        crate::mapi::identity::remember_mapi_identity(email_id, message_id);
        let mailboxes = vec![JmapMailbox {
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
        }];
        let emails = vec![JmapEmail {
            id: email_id,
            thread_id: email_id,
            mailbox_ids: vec![mailbox_id],
            mailbox_states: Vec::new(),
            mailbox_id,
            mailbox_role: "inbox".to_string(),
            mailbox_name: "Inbox".to_string(),
            modseq: 7,
            received_at: "2026-06-07T19:56:00Z".to_string(),
            sent_at: None,
            from_address: "sender@example.test".to_string(),
            from_display: Some("Sender".to_string()),
            sender_address: None,
            sender_display: None,
            sender_authorization_kind: "author".to_string(),
            submitted_by_account_id: Uuid::nil(),
            to: Vec::new(),
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: "Hello".to_string(),
            preview: String::new(),
            body_text: "Body".to_string(),
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
            size_octets: 512,
            internet_message_id: Some("<hello@example.test>".to_string()),
            mime_blob_ref: None,
            delivery_status: "delivered".to_string(),
        }];
        let mut payload = Vec::new();
        write_u16(&mut payload, 4096);
        write_u16(&mut payload, 2);
        write_u32(&mut payload, PID_TAG_MESSAGE_FLAGS);
        write_u32(&mut payload, 0x0037_0001);
        let request = RopRequest {
            rop_id: RopId::GetPropertiesSpecific.as_u8(),
            input_handle_index: Some(3),
            output_handle_index: None,
            payload,
        };
        let object = MapiObject::Message {
            folder_id: INBOX_FOLDER_ID,
            message_id,
            saved_email: None,
            pending_properties: HashMap::new(),
        };

        let response = rop_get_properties_specific_response(
            &request,
            Some(&object),
            &principal,
            &mailboxes,
            &emails,
            &MapiMailStoreSnapshot::empty(),
        );

        assert_eq!(&response[..7], &[0x07, 0x03, 0, 0, 0, 0, 0x01]);
        assert_eq!(response[7], 0);
        assert_eq!(&response[12..15], &[0x1F, 0x00, 0]);
        assert_eq!(&response[15..], utf16z_bytes("Hello").as_slice());
    }

    #[test]
    fn get_properties_specific_resolves_unspecified_modeled_message_properties() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id: Uuid::from_u128(0xbbbbbbbb_bbbb_bbbb_bbbb_bbbbbbbbbbbb),
            email: "alice@example.test".to_string(),
            display_name: "Alice".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };
        let mailbox_id = Uuid::from_u128(0x11111111111111111111111111111111);
        let email_id = Uuid::from_u128(0x33333333333333333333333333333333);
        let message_id = crate::mapi::identity::mapi_store_id(0x3333);
        crate::mapi::identity::remember_mapi_identity(mailbox_id, INBOX_FOLDER_ID);
        crate::mapi::identity::remember_mapi_identity(email_id, message_id);
        let mailboxes = vec![JmapMailbox {
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
        }];
        let emails = vec![JmapEmail {
            id: email_id,
            thread_id: email_id,
            mailbox_ids: vec![mailbox_id],
            mailbox_states: Vec::new(),
            mailbox_id,
            mailbox_role: "inbox".to_string(),
            mailbox_name: "Inbox".to_string(),
            modseq: 7,
            received_at: "2026-06-07T19:56:00Z".to_string(),
            sent_at: None,
            from_address: "sender@example.test".to_string(),
            from_display: Some("Sender".to_string()),
            sender_address: None,
            sender_display: None,
            sender_authorization_kind: "author".to_string(),
            submitted_by_account_id: Uuid::nil(),
            to: Vec::new(),
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: "Unspecified".to_string(),
            preview: String::new(),
            body_text: "Body".to_string(),
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
            size_octets: 2048,
            internet_message_id: Some("<unspecified@example.test>".to_string()),
            mime_blob_ref: None,
            delivery_status: "delivered".to_string(),
        }];
        let mut payload = Vec::new();
        write_u16(&mut payload, 4096);
        write_u16(&mut payload, 3);
        write_u32(&mut payload, 0x0E1B_0000);
        write_u32(&mut payload, 0x0E08_0000);
        write_u32(&mut payload, 0x1035_0000);
        let request = RopRequest {
            rop_id: RopId::GetPropertiesSpecific.as_u8(),
            input_handle_index: Some(3),
            output_handle_index: None,
            payload,
        };
        let object = MapiObject::Message {
            folder_id: INBOX_FOLDER_ID,
            message_id,
            saved_email: None,
            pending_properties: HashMap::new(),
        };

        let response = rop_get_properties_specific_response(
            &request,
            Some(&object),
            &principal,
            &mailboxes,
            &emails,
            &MapiMailStoreSnapshot::empty(),
        );

        assert_eq!(&response[..7], &[0x07, 0x03, 0, 0, 0, 0, 0x01]);
        let mut offset = 7;
        assert_eq!(
            u16::from_le_bytes(response[offset..offset + 2].try_into().unwrap()),
            0x000B
        );
        offset += 2;
        assert_eq!(&response[offset..offset + 2], &[0, 1]);
        offset += 2;
        assert_eq!(
            u16::from_le_bytes(response[offset..offset + 2].try_into().unwrap()),
            0x0003
        );
        offset += 2;
        assert_eq!(response[offset], 0);
        offset += 1;
        assert_eq!(
            u32::from_le_bytes(response[offset..offset + 4].try_into().unwrap()),
            2048
        );
        offset += 4;
        assert_eq!(
            u16::from_le_bytes(response[offset..offset + 2].try_into().unwrap()),
            0x001F
        );
        offset += 2;
        assert_eq!(response[offset], 0);
        offset += 1;
        assert_eq!(
            &response[offset..],
            utf16z_bytes("<unspecified@example.test>").as_slice()
        );
    }

    #[test]
    fn get_properties_specific_returns_not_enough_memory_for_size_limited_value() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id: Uuid::nil(),
            email: "sender@example.test".to_string(),
            display_name: "Sender".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };
        let mut properties = HashMap::new();
        properties.insert(
            PID_TAG_SUBJECT_W,
            MapiValue::String("Large subject".to_string()),
        );
        let object = MapiObject::PendingMessage {
            folder_id: DRAFTS_FOLDER_ID,
            properties,
            recipients: Vec::new(),
        };
        let mut payload = Vec::new();
        write_u16(&mut payload, 4);
        write_u16(&mut payload, 1);
        write_u32(&mut payload, PID_TAG_SUBJECT_W);
        let request = RopRequest {
            rop_id: RopId::GetPropertiesSpecific.as_u8(),
            input_handle_index: Some(3),
            output_handle_index: None,
            payload,
        };

        let response = rop_get_properties_specific_response(
            &request,
            Some(&object),
            &principal,
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
        );

        assert_eq!(&response[..6], &[0x07, 0x03, 0, 0, 0, 0]);
        assert_eq!(response[6], 0x01);
        assert_eq!(response[7], 0x0A);
        assert_eq!(
            u32::from_le_bytes(response[8..12].try_into().unwrap()),
            0x8007_000E
        );
    }

    #[test]
    fn get_properties_specific_size_limit_preserves_unspecified_property_type() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id: Uuid::nil(),
            email: "sender@example.test".to_string(),
            display_name: "Sender".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };
        let mut properties = HashMap::new();
        properties.insert(
            PID_TAG_SUBJECT_W,
            MapiValue::String("Large subject".to_string()),
        );
        let object = MapiObject::PendingMessage {
            folder_id: DRAFTS_FOLDER_ID,
            properties,
            recipients: Vec::new(),
        };
        let mut payload = Vec::new();
        write_u16(&mut payload, 4);
        write_u16(&mut payload, 1);
        write_u32(&mut payload, 0x0037_0001);
        let request = RopRequest {
            rop_id: RopId::GetPropertiesSpecific.as_u8(),
            input_handle_index: Some(3),
            output_handle_index: None,
            payload,
        };

        let response = rop_get_properties_specific_response(
            &request,
            Some(&object),
            &principal,
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
        );

        assert_eq!(&response[..6], &[0x07, 0x03, 0, 0, 0, 0]);
        assert_eq!(response[6], 0x01);
        assert_eq!(&response[7..9], &0x001Fu16.to_le_bytes());
        assert_eq!(response[9], 0x0A);
        assert_eq!(
            u32::from_le_bytes(response[10..14].try_into().unwrap()),
            0x8007_000E
        );
    }

    #[test]
    pub(in crate::mapi) fn session_idle_expiry_follows_cookie_max_age() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10_000);
        let fresh = MapiSession {
            endpoint: MapiEndpoint::Emsmdb,
            tenant_id: Uuid::from_u128(0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa),
            account_id: Uuid::nil(),
            email: "user@example.test".to_string(),
            created_at: now,
            last_seen_at: now - Duration::from_secs(u64::from(MAPI_SESSION_MAX_AGE_SECONDS)),
            first_request_type: "Connect".to_string(),
            first_request_id: "test:1".to_string(),
            last_request_type: "Connect".to_string(),
            last_request_id: "test:1".to_string(),
            request_count: 1,
            execute_request_count: 0,
            next_handle: 1,
            handles: HashMap::new(),
            message_statuses: HashMap::new(),
            message_save_generations: HashMap::new(),
            message_handle_generations: HashMap::new(),
            pending_message_recipient_replacements: HashMap::new(),
            pending_message_attachments: HashMap::new(),
            pending_attachment_parent_messages: HashMap::new(),
            pending_attachment_deletions: HashSet::new(),
            pending_embedded_message_ids: HashMap::new(),
            pending_embedded_message_attachments: HashMap::new(),
            saved_embedded_messages: HashMap::new(),
            saved_search_folder_definitions: HashMap::new(),
            special_folder_aliases: HashMap::new(),
            deleted_advertised_special_folders: HashSet::new(),
            deleted_search_folder_definitions: HashSet::new(),
            named_properties: HashMap::new(),
            named_property_ids: HashMap::new(),
            next_named_property_id: FIRST_NAMED_PROPERTY_ID,
            next_local_replica_sequence: 1,
            notification_cursor: None,
            pending_notifications: VecDeque::new(),
            completed_execute_requests: HashMap::new(),
            completed_execute_request_order: VecDeque::new(),
            post_hierarchy_actions: PostHierarchyActionState::default(),
            inbox_associated_config_stream_handles: HashSet::new(),
            inbox_rule_organizer_stream_handles: HashSet::new(),
            logon_identity: None,
        };
        let stale = MapiSession {
            last_seen_at: now - Duration::from_secs(u64::from(MAPI_SESSION_MAX_AGE_SECONDS) + 1),
            ..fresh.clone()
        };

        assert!(!session_is_expired(&fresh, now));
        assert!(session_is_expired(&stale, now));
    }

    #[test]
    pub(in crate::mapi) fn logon_time_bytes_encode_valid_utc_calendar_fields() {
        let bytes = logon_time_bytes(SystemTime::UNIX_EPOCH + Duration::from_secs(1_778_046_495));

        assert_eq!(bytes, [15, 48, 5, 3, 6, 5, 0xEA, 0x07]);
    }

    #[test]
    pub(in crate::mapi) fn create_folder_private_response_stops_after_non_existing_flag() {
        let request = RopRequest {
            rop_id: RopId::CreateFolder.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: Some(1),
            payload: Vec::new(),
        };

        let created = rop_create_folder_response(&request, QUICK_STEP_SETTINGS_FOLDER_ID, false);
        assert_eq!(created.len(), 15);
        assert_eq!(created[14], 0);

        let existing = rop_create_folder_response(&request, QUICK_STEP_SETTINGS_FOLDER_ID, true);
        assert_eq!(existing.len(), 16);
        assert_eq!(existing[14], 1);
    }

    #[test]
    fn open_message_response_does_not_advertise_missing_recipient_rows() {
        let request = RopRequest {
            rop_id: RopId::OpenMessage.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: Some(2),
            payload: Vec::new(),
        };

        let response = rop_open_message_response(&request, "Subject", 3);

        assert_eq!(response[0], RopId::OpenMessage.as_u8());
        assert_eq!(response[1], 2);
        assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
        assert_eq!(&response[response.len() - 5..response.len() - 3], &[3, 0]);
        assert_eq!(&response[response.len() - 3..response.len() - 1], &[0, 0]);
        assert_eq!(response[response.len() - 1], 0);
    }

    #[test]
    fn microsoft_reload_cached_information_matches_open_message_shape() {
        let reload_request = RopRequest {
            rop_id: RopId::ReloadCachedInformation.as_u8(),
            input_handle_index: Some(2),
            output_handle_index: None,
            payload: Vec::new(),
        };
        let open_request = RopRequest {
            rop_id: RopId::OpenMessage.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: Some(2),
            payload: Vec::new(),
        };
        let object = MapiObject::PendingMessage {
            folder_id: INBOX_FOLDER_ID,
            properties: HashMap::from([(PID_TAG_SUBJECT_W, MapiValue::String("Subject".into()))]),
            recipients: vec![PendingRecipient {
                row_id: 0,
                recipient_type: 1,
                address: "alice@example.test".into(),
                display_name: Some("Alice".into()),
            }],
        };
        let snapshot = MapiMailStoreSnapshot::empty();

        let response = rop_reload_cached_information_response(
            &reload_request,
            Some(&object),
            &[],
            &[],
            &snapshot,
        );
        let open_response = rop_open_message_response(&open_request, "Subject", 1);

        assert_eq!(response[0], RopId::ReloadCachedInformation.as_u8());
        assert_eq!(response[1], 2);
        assert_eq!(&response[2..], &open_response[2..]);
        assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
        assert_eq!(&response[response.len() - 5..response.len() - 3], &[1, 0]);
        assert_eq!(&response[response.len() - 3..response.len() - 1], &[0, 0]);
        assert_eq!(response[response.len() - 1], 0);
    }

    #[test]
    fn modify_recipients_accepts_microsoft_message_example_columns() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
            email: "test@example.test".to_string(),
            display_name: "Test".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };
        let columns = [
            PID_TAG_OBJECT_TYPE,
            PID_TAG_DISPLAY_TYPE,
            PID_TAG_ADDRESS_BOOK_DISPLAY_NAME_PRINTABLE_W,
            PID_TAG_SMTP_ADDRESS_W,
            PID_TAG_SEND_INTERNET_ENCODING,
            PID_TAG_DISPLAY_TYPE_EX,
            PID_TAG_RECIPIENT_DISPLAY_NAME_W,
            PID_TAG_RECIPIENT_FLAGS,
            PID_TAG_RECIPIENT_TRACK_STATUS,
            OUTLOOK_RECIPIENT_5FDE,
            PID_TAG_RECIPIENT_ORDER,
            PID_TAG_RECIPIENT_ENTRY_ID,
        ];
        let mut row = Vec::new();
        write_u16(&mut row, 0x0651);
        row.push(b'Z');
        row.push(0);
        write_ascii_z(&mut row, "User2");
        write_utf16z(&mut row, "User2");
        write_utf16z(&mut row, "user2");
        write_u16(&mut row, columns.len() as u16);
        row.push(0);
        for (tag, value) in [
            (PID_TAG_OBJECT_TYPE, MapiValue::U32(6)),
            (PID_TAG_DISPLAY_TYPE, MapiValue::U32(0)),
            (
                PID_TAG_ADDRESS_BOOK_DISPLAY_NAME_PRINTABLE_W,
                MapiValue::String("user2".to_string()),
            ),
            (
                PID_TAG_SMTP_ADDRESS_W,
                MapiValue::String("user2@szfkuk-dom.extest.microsoft.com".to_string()),
            ),
            (PID_TAG_SEND_INTERNET_ENCODING, MapiValue::U32(0)),
            (PID_TAG_DISPLAY_TYPE_EX, MapiValue::U32(0x4000_0000)),
            (
                PID_TAG_RECIPIENT_DISPLAY_NAME_W,
                MapiValue::String("user2".to_string()),
            ),
            (PID_TAG_RECIPIENT_FLAGS, MapiValue::U32(1)),
            (PID_TAG_RECIPIENT_TRACK_STATUS, MapiValue::U32(0)),
            (OUTLOOK_RECIPIENT_5FDE, MapiValue::U32(0)),
            (PID_TAG_RECIPIENT_ORDER, MapiValue::U32(0)),
            (PID_TAG_RECIPIENT_ENTRY_ID, MapiValue::Binary(vec![0; 124])),
        ] {
            write_mapi_value(&mut row, tag, &value);
        }

        let recipient = parse_pending_recipient_row(0, 1, &columns, &row, &principal, &[]).unwrap();

        assert_eq!(recipient.row_id, 0);
        assert_eq!(recipient.recipient_type, 1);
        assert_eq!(recipient.address, "user2@szfkuk-dom.extest.microsoft.com");
        assert_eq!(recipient.display_name.as_deref(), Some("user2"));
    }

    #[test]
    pub(in crate::mapi) fn gwart_time_marker_uses_real_timestamp_and_stays_nonzero() {
        assert_eq!(
            gwart_time_marker(SystemTime::UNIX_EPOCH + Duration::from_secs(1_778_046_495)),
            1_778_046_495
        );
        assert_eq!(gwart_time_marker(SystemTime::UNIX_EPOCH), 1);
    }

    #[test]
    pub(in crate::mapi) fn property_debug_names_cover_recent_outlook_folder_probes() {
        assert_eq!(
            property_tag_debug_name(PID_TAG_LOCAL_COMMIT_TIME_MAX),
            "PidTagLocalCommitTimeMax"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_DELETED_COUNT_TOTAL),
            "PidTagDeletedCountTotal"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_CONTENT_UNREAD_COUNT),
            "PidTagContentUnreadCount"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_CONTENT_COUNT),
            "PidTagContentCount"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_PARENT_FOLDER_ID),
            "PidTagParentFolderId"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_SUBFOLDERS),
            "PidTagSubfolders"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_FOLDER_TYPE),
            "PidTagFolderType"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_CHANGE_KEY),
            "PidTagChangeKey"
        );
        assert_eq!(property_tag_debug_name(PID_TAG_ACCESS), "PidTagAccess");
        assert_eq!(
            property_tag_debug_name(PID_TAG_CONVERSATION_TOPIC_W),
            "PidTagConversationTopic"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_CONVERSATION_INDEX),
            "PidTagConversationIndex"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_MESSAGE_CLASS_W),
            "PidTagMessageClass"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_ORIGINAL_MESSAGE_CLASS_W),
            "PidTagOriginalMessageClass"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_ACCESS_LEVEL),
            "PidTagAccessLevel"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_SENDER_ADDRESS_TYPE_W),
            "PidTagSenderAddressType"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_SENDER_SMTP_ADDRESS_W),
            "PidTagSenderSmtpAddress"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_MESSAGE_STATUS),
            "PidTagMessageStatus"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_SEARCH_KEY),
            "PidTagSearchKey"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_DISPLAY_BCC_W),
            "PidTagDisplayBcc"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_DISPLAY_TO_W),
            "PidTagDisplayTo"
        );
        assert_eq!(property_tag_debug_name(PID_TAG_SUBJECT_W), "PidTagSubject");
        assert_eq!(
            property_tag_debug_name(PID_TAG_SUBJECT_PREFIX_W),
            "PidTagSubjectPrefix"
        );
        assert_eq!(property_tag_debug_name(PID_TAG_BODY_W), "PidTagBody");
        assert_eq!(
            property_tag_debug_name(PID_TAG_RTF_COMPRESSED),
            "PidTagRtfCompressed"
        );
        assert_eq!(property_tag_debug_name(PID_TAG_HTML_BINARY), "PidTagHtml");
        assert_eq!(
            property_tag_debug_name(PID_TAG_HAS_ATTACHMENTS),
            "PidTagHasAttachments"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_MESSAGE_FLAGS),
            "PidTagMessageFlags"
        );
        assert_eq!(property_tag_debug_name(PID_TAG_READ), "PidTagRead");
        assert_eq!(
            property_tag_debug_name(PID_TAG_TRANSPORT_MESSAGE_HEADERS_W),
            "PidTagTransportMessageHeaders"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_RTF_IN_SYNC),
            "PidTagRtfInSync"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_NATIVE_BODY),
            "PidTagNativeBody"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_INTERNET_CODEPAGE),
            "PidTagInternetCodepage"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_MESSAGE_LOCALE_ID),
            "PidTagMessageLocaleId"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_EXTENDED_RULE_MESSAGE_ACTIONS),
            "PidTagExtendedRuleMessageActions"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_INTERNET_MESSAGE_ID_W),
            "PidTagInternetMessageId"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_FLAG_STATUS),
            "PidTagFlagStatus"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_SWAPPED_TODO_STORE),
            "PidTagSwappedToDoStore"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_LAST_MODIFICATION_TIME),
            "PidTagLastModificationTime"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_VIEW_DESCRIPTOR_CLSID),
            "PidTagViewDescriptorCLSID"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_VIEW_DESCRIPTOR_FLAGS),
            "PidTagViewDescriptorFlags"
        );
        assert_eq!(
            property_tag_debug_name(OUTLOOK_COMMON_VIEW_DESCRIPTOR_BINARY_6835),
            "OutlookCommonViewDescriptorBinary6835"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_VIEW_DESCRIPTOR_VERSION),
            "PidTagViewDescriptorVersion"
        );
        assert_eq!(
            property_tag_debug_name(OUTLOOK_COMMON_VIEW_DESCRIPTOR_STRINGS_683C),
            "OutlookCommonViewDescriptorStrings683C"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_VIEW_DESCRIPTOR_FOLDER_TYPE),
            "PidTagViewDescriptorFolderType"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_VIEW_DESCRIPTOR_VIEW_MODE),
            "PidTagViewDescriptorViewMode"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_VIEW_DESCRIPTOR_BINARY),
            "PidTagViewDescriptorBinary"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_VIEW_DESCRIPTOR_STRINGS_W),
            "PidTagViewDescriptorStrings"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_VIEW_DESCRIPTOR_NAME_W),
            "PidTagViewDescriptorName"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_VIEW_DESCRIPTOR_VERSION_CANONICAL),
            "PidTagViewDescriptorVersionCanonical"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_WLINK_GROUP_HEADER_ID),
            "PidTagWlinkGroupHeaderId"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_WLINK_SAVE_STAMP),
            "PidTagWlinkSaveStamp"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_WLINK_TYPE),
            "PidTagWlinkType"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_WLINK_FLAGS),
            "PidTagWlinkFlags"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_WLINK_ORDINAL),
            "PidTagWlinkOrdinal"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_WLINK_ENTRY_ID),
            "PidTagWlinkEntryId"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_WLINK_RECORD_KEY),
            "PidTagWlinkRecordKey"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_WLINK_STORE_ENTRY_ID),
            "PidTagWlinkStoreEntryId"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_WLINK_FOLDER_TYPE),
            "PidTagWlinkFolderType"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_WLINK_GROUP_CLSID),
            "PidTagWlinkGroupClsid"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_WLINK_GROUP_NAME_W),
            "PidTagWlinkGroupName"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_WLINK_SECTION),
            "PidTagWlinkSection"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_WLINK_ADDRESS_BOOK_STORE_EID),
            "PidTagWlinkAddressBookStoreEid"
        );
        assert_eq!(
            property_tag_debug_name(OUTLOOK_STALE_SHARING_LOCAL_FOLDER_ID_TAG),
            "OutlookStaleSharingCalendarGroupEntryAssociatedLocalFolderId"
        );
        assert_eq!(
            property_tag_debug_name(OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B),
            "OutlookAssociatedConfigBinary0E0B"
        );
        assert_eq!(
            property_tag_debug_name(PID_NAME_CONTENT_CLASS_W_TAG),
            "PidNameContentClass"
        );
        assert_eq!(
            property_tag_debug_name(PID_NAME_CONTENT_TYPE_W_TAG),
            "PidNameContentType"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_ROAMING_DATATYPES),
            "PidTagRoamingDatatypes"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_ROAMING_DICTIONARY),
            "PidTagRoamingDictionary"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_ROAMING_XML_STREAM),
            "PidTagRoamingXmlStream"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W),
            "PidTagDefaultPostMessageClass"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_DEFAULT_POST_MESSAGE_CLASS_STRING8),
            "PidTagDefaultPostMessageClass"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_ADDITIONAL_REN_ENTRY_IDS),
            "PidTagAdditionalRenEntryIds"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX),
            "PidTagAdditionalRenEntryIdsEx"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_FREE_BUSY_ENTRY_IDS),
            "PidTagFreeBusyEntryIds"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_ASSOCIATED_SHARING_PROVIDER),
            "PidTagAssociatedSharingProvider"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_SEARCH_FOLDER_ID),
            "PidTagSearchFolderId"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_SEARCH_FOLDER_STORAGE_TYPE),
            "PidTagSearchFolderStorageType"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_SEARCH_FOLDER_EFP_FLAGS),
            "PidTagSearchFolderEfpFlags"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_SEARCH_FOLDER_DEFINITION),
            "PidTagSearchFolderDefinition"
        );
        assert_eq!(property_tag_debug_name(PID_TAG_PST_PATH_W), "PidTagPstPath");
        assert_eq!(
            property_tag_debug_name(PID_TAG_EXTENDED_RULE_SIZE_LIMIT),
            "PidTagExtendedRuleSizeLimit"
        );
        assert_eq!(
            property_tag_debug_name(PID_TAG_ATTACH_NUM),
            "PidTagAttachNumber"
        );
        assert_eq!(property_tag_debug_name(0x6707_001F), "PidTagUserGuid");
        assert_eq!(
            property_tag_debug_name(0x6842_000B),
            "PidTagWlinkGroupHeaderId"
        );
        assert_eq!(property_tag_debug_name(0x684A_101F), "PidTagWlinkFlags");
        assert_eq!(property_tag_debug_name(0x684B_000B), "PidTagWlinkOrdinal");
        assert_eq!(
            property_tag_debug_name(0x6841_001F),
            "PidTagViewDescriptorViewMode"
        );
    }

    #[test]
    pub(in crate::mapi) fn set_search_criteria_rejects_invalid_folder_id_scope() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&0u16.to_le_bytes());
        payload.extend_from_slice(&1u16.to_le_bytes());
        payload.extend_from_slice(&0u64.to_le_bytes());
        payload.extend_from_slice(&1u32.to_le_bytes());
        let request = RopRequest {
            rop_id: RopId::SetSearchCriteria.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload,
        };

        assert_eq!(request.search_criteria_folder_ids(), None);
    }

    #[test]
    fn logon_getprops_projects_extended_rule_size_limit() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
            email: "test@example.test".to_string(),
            display_name: "Test".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };
        let mut payload = Vec::new();
        payload.extend_from_slice(&4096u16.to_le_bytes());
        payload.extend_from_slice(&1u16.to_le_bytes());
        payload.extend_from_slice(&PID_TAG_EXTENDED_RULE_SIZE_LIMIT.to_le_bytes());
        let request = RopRequest {
            rop_id: RopId::GetPropertiesSpecific.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload,
        };

        let response = rop_get_properties_specific_response(
            &request,
            Some(&MapiObject::Logon),
            &principal,
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
        );

        assert_eq!(response[0], RopId::GetPropertiesSpecific.as_u8());
        assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
        assert_eq!(response[6], 0);
        assert_eq!(
            u32::from_le_bytes(response[7..11].try_into().unwrap()),
            35 * 1024
        );
    }

    #[test]
    fn associated_config_0e0b_debug_reports_stored_value_and_fallback() {
        let message = crate::mapi_store::MapiAssociatedConfigMessage {
            id: 0x7fff_ffff_fffb_0001,
            folder_id: INBOX_FOLDER_ID,
            canonical_id: Uuid::parse_str("11111111-2222-4333-8444-555555555555").unwrap(),
            message_class: "IPM.Configuration.AccountPrefs".to_string(),
            subject: "Account preferences".to_string(),
            properties_json: serde_json::json!({
                "0x0e0b0102": {"type": "binary", "value": "01020304"}
            }),
        };

        let summary = format_associated_config_0e0b_debug(
            &[OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B],
            &message,
            &[OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B],
        );

        assert!(summary.contains("requested=true"));
        assert!(summary.contains("public_ms_oxprops_name=unmapped"));
        assert!(summary.contains("stored=true"));
        assert!(summary.contains("stored_shape=binary:bytes=4:preview=01020304"));
        assert!(summary.contains("semantic_shape=binary:bytes=4:preview=01020304"));
        assert!(summary.contains("fallback_default=true"));
        assert!(summary.contains("property_json_tags=0x0e0b0102"));
    }

    #[test]
    fn associated_config_zero_metadata_defaults_are_intentional() {
        let object = MapiObject::AssociatedConfig {
            folder_id: INBOX_FOLDER_ID,
            config_id: 0x7fff_ffff_fffb_0001,
            saved_message: None,
        };

        assert!(modeled_zero_or_default_property(
            Some(&object),
            PID_TAG_MESSAGE_STATUS
        ));
        assert!(modeled_zero_or_default_property(
            Some(&object),
            PID_TAG_SENT_MAIL_SVR_EID
        ));
        assert!(modeled_zero_or_default_property(
            Some(&object),
            OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B
        ));
        assert!(!modeled_zero_or_default_property(
            Some(&object),
            PID_TAG_ORIGINAL_MESSAGE_CLASS_W
        ));
        assert!(!modeled_zero_or_default_property(
            Some(&object),
            0x801D_0003
        ));
        assert!(!modeled_zero_or_default_property(
            Some(&object),
            0x801D_0000
        ));
    }

    #[test]
    fn quick_step_custom_action_defaults_undocumented_0e0b_to_empty_binary() {
        let object = MapiObject::AssociatedConfig {
            folder_id: QUICK_STEP_SETTINGS_FOLDER_ID,
            config_id: crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF4),
            saved_message: None,
        };

        assert!(modeled_zero_or_default_property(
            Some(&object),
            OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B
        ));
    }

    #[test]
    fn associated_config_getprops_rejects_default_from_wrong_folder() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id: Uuid::nil(),
            email: "test@example.test".to_string(),
            display_name: "Test".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };
        let object = MapiObject::AssociatedConfig {
            folder_id: QUICK_STEP_SETTINGS_FOLDER_ID,
            config_id: crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFFC),
            saved_message: None,
        };
        let mut payload = Vec::new();
        payload.extend_from_slice(&4096u16.to_le_bytes());
        payload.extend_from_slice(&1u16.to_le_bytes());
        payload.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
        let request = RopRequest {
            rop_id: RopId::GetPropertiesSpecific.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload,
        };

        let response = rop_get_properties_specific_response(
            &request,
            Some(&object),
            &principal,
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
        );

        assert_eq!(response[0], RopId::GetPropertiesSpecific.as_u8());
        assert_eq!(
            u32::from_le_bytes(response[2..6].try_into().unwrap()),
            0x8004_010F
        );
    }

    #[test]
    fn folder_default_named_view_getprops_projects_message_class() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
            email: "test@example.test".to_string(),
            display_name: "Test".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };
        let object = MapiObject::CommonViewNamedView {
            folder_id: INBOX_FOLDER_ID,
            view_id: crate::mapi_store::OUTLOOK_DEFAULT_FOLDER_NAMED_VIEW_ID,
        };
        let mut payload = Vec::new();
        payload.extend_from_slice(&4096u16.to_le_bytes());
        payload.extend_from_slice(&1u16.to_le_bytes());
        payload.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
        let request = RopRequest {
            rop_id: RopId::GetPropertiesSpecific.as_u8(),
            input_handle_index: Some(3),
            output_handle_index: None,
            payload,
        };

        let response = rop_get_properties_specific_response(
            &request,
            Some(&object),
            &principal,
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
        );

        assert_eq!(response[0], RopId::GetPropertiesSpecific.as_u8());
        assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
        assert!(response.windows(2).any(|bytes| bytes == b"I\0"));
    }

    fn test_accessible_calendar_event(
        id: Uuid,
        account_id: Uuid,
        title: &str,
    ) -> lpe_storage::AccessibleEvent {
        lpe_storage::AccessibleEvent {
            id,
            uid: format!("uid-{id}"),
            collection_id: "default".to_string(),
            owner_account_id: account_id,
            owner_email: "test@example.test".to_string(),
            owner_display_name: "Test".to_string(),
            rights: lpe_storage::CollaborationRights {
                may_read: true,
                may_write: true,
                may_delete: true,
                may_share: false,
            },
            date: "2026-06-01".to_string(),
            time: "10:00".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 60,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: title.to_string(),
            location: String::new(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: "[]".to_string(),
            notes: String::new(),
            body_html: String::new(),
        }
    }

    fn contains_utf16(bytes: &[u8], value: &str) -> bool {
        let needle = value
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect::<Vec<_>>();
        bytes
            .windows(needle.len())
            .any(|window| window == needle.as_slice())
    }

    fn contains_ascii_z(bytes: &[u8], value: &str) -> bool {
        let mut needle = value.as_bytes().to_vec();
        needle.push(0);
        contains_bytes(bytes, &needle)
    }

    fn contains_bytes(bytes: &[u8], needle: &[u8]) -> bool {
        bytes.windows(needle.len()).any(|window| window == needle)
    }

    #[test]
    fn get_properties_all_honors_non_unicode_string_request() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id: Uuid::nil(),
            email: "sender@example.test".to_string(),
            display_name: "Sender".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };
        let mut properties = HashMap::new();
        properties.insert(
            PID_TAG_SUBJECT_W,
            MapiValue::String("String8 subject".to_string()),
        );
        properties.insert(
            PID_TAG_BODY_W,
            MapiValue::String("String8 body".to_string()),
        );
        let object = MapiObject::PendingMessage {
            folder_id: DRAFTS_FOLDER_ID,
            properties,
            recipients: Vec::new(),
        };
        let request = RopRequest {
            rop_id: RopId::GetPropertiesAll.as_u8(),
            input_handle_index: Some(2),
            output_handle_index: None,
            payload: [0x00, 0x10, 0x00, 0x00].to_vec(),
        };

        let response = rop_get_properties_all_response(
            &request,
            Some(&object),
            &principal,
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
        );

        assert_eq!(&response[..6], &[0x08, 0x02, 0, 0, 0, 0]);
        assert!(contains_bytes(
            &response,
            &((PID_TAG_SUBJECT_W & 0xFFFF_0000) | 0x001E).to_le_bytes()
        ));
        assert!(contains_bytes(
            &response,
            &((PID_TAG_BODY_W & 0xFFFF_0000) | 0x001E).to_le_bytes()
        ));
        assert!(contains_ascii_z(&response, "String8 subject"));
        assert!(contains_ascii_z(&response, "String8 body"));
    }

    #[test]
    fn get_properties_all_returns_error_tag_for_size_limited_value() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id: Uuid::nil(),
            email: "sender@example.test".to_string(),
            display_name: "Sender".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };
        let mut properties = HashMap::new();
        properties.insert(
            PID_TAG_SUBJECT_W,
            MapiValue::String("Oversized subject".to_string()),
        );
        let object = MapiObject::PendingMessage {
            folder_id: DRAFTS_FOLDER_ID,
            properties,
            recipients: Vec::new(),
        };
        let request = RopRequest {
            rop_id: RopId::GetPropertiesAll.as_u8(),
            input_handle_index: Some(2),
            output_handle_index: None,
            payload: [0x04, 0x00, 0x01, 0x00].to_vec(),
        };

        let response = rop_get_properties_all_response(
            &request,
            Some(&object),
            &principal,
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
        );
        let mut expected = property_error_tag(PID_TAG_SUBJECT_W).to_le_bytes().to_vec();
        expected.extend_from_slice(&0x8007_000E_u32.to_le_bytes());

        assert_eq!(&response[..6], &[0x08, 0x02, 0, 0, 0, 0]);
        assert!(contains_bytes(&response, &expected));
    }

    #[test]
    fn calendar_event_getprops_specific_projects_visible_event() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id: Uuid::from_u128(0x8181),
            email: "test@example.test".to_string(),
            display_name: "Test".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };
        let event_id = Uuid::from_u128(0x8182);
        crate::mapi::identity::remember_mapi_identity(
            event_id,
            crate::mapi::identity::mapi_store_id(0x8182),
        );
        let snapshot = MapiMailStoreSnapshot::new(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec![test_accessible_calendar_event(
                event_id,
                principal.account_id,
                "Projected event",
            )],
            Vec::new(),
            Vec::new(),
        );
        let object = MapiObject::Event {
            folder_id: CALENDAR_FOLDER_ID,
            event_id: crate::mapi::identity::mapi_store_id(0x8182),
        };
        let mut payload = Vec::new();
        payload.extend_from_slice(&4096u16.to_le_bytes());
        payload.extend_from_slice(&2u16.to_le_bytes());
        payload.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
        payload.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
        let request = RopRequest {
            rop_id: RopId::GetPropertiesSpecific.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload,
        };

        let response = rop_get_properties_specific_response(
            &request,
            Some(&object),
            &principal,
            &[],
            &[],
            &snapshot,
        );

        assert_eq!(response[0], RopId::GetPropertiesSpecific.as_u8());
        assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
        assert!(contains_utf16(&response, "IPM.Appointment"));
        assert!(contains_utf16(&response, "Projected event"));
    }

    #[test]
    fn calendar_event_getprops_all_rejects_missing_event_handle() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id: Uuid::nil(),
            email: "test@example.test".to_string(),
            display_name: "Test".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };
        let object = MapiObject::Event {
            folder_id: CALENDAR_FOLDER_ID,
            event_id: crate::mapi::identity::mapi_store_id(0x43),
        };
        let request = RopRequest {
            rop_id: RopId::GetPropertiesAll.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: Vec::new(),
        };

        let response = rop_get_properties_all_response(
            &request,
            Some(&object),
            &principal,
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
        );

        assert_eq!(response[0], RopId::GetPropertiesAll.as_u8());
        assert_eq!(
            u32::from_le_bytes(response[2..6].try_into().unwrap()),
            0x8004_010F
        );
    }

    #[test]
    fn calendar_event_get_valid_attachments_rejects_missing_event_handle() {
        let object = MapiObject::Event {
            folder_id: CALENDAR_FOLDER_ID,
            event_id: crate::mapi::identity::mapi_store_id(0x43),
        };
        let request = RopRequest {
            rop_id: RopId::GetValidAttachments.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: Vec::new(),
        };

        let response = rop_get_valid_attachments_response(
            &request,
            Some(&object),
            &MapiMailStoreSnapshot::empty(),
            &HashSet::new(),
        );

        assert_eq!(response[0], RopId::GetValidAttachments.as_u8());
        assert_eq!(
            u32::from_le_bytes(response[2..6].try_into().unwrap()),
            0x8004_010F
        );
    }

    #[test]
    fn conversation_action_getprops_rejects_default_from_wrong_folder() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id: Uuid::nil(),
            email: "test@example.test".to_string(),
            display_name: "Test".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };
        let object = MapiObject::ConversationAction {
            folder_id: COMMON_VIEWS_FOLDER_ID,
            conversation_action_id: crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF2),
        };
        let mut payload = Vec::new();
        payload.extend_from_slice(&4096u16.to_le_bytes());
        payload.extend_from_slice(&1u16.to_le_bytes());
        payload.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
        let request = RopRequest {
            rop_id: RopId::GetPropertiesSpecific.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload,
        };

        let response = rop_get_properties_specific_response(
            &request,
            Some(&object),
            &principal,
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
        );

        assert_eq!(response[0], RopId::GetPropertiesSpecific.as_u8());
        assert_eq!(
            u32::from_le_bytes(response[2..6].try_into().unwrap()),
            0x8004_010F
        );
    }

    #[test]
    fn delegate_freebusy_getprops_rejects_message_from_wrong_folder() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id: Uuid::nil(),
            email: "test@example.test".to_string(),
            display_name: "Test".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };
        let message_id = Uuid::parse_str("56565656-5656-4656-8656-565656565656").unwrap();
        crate::mapi::identity::remember_mapi_identity(
            message_id,
            crate::mapi::identity::mapi_store_id(610),
        );
        let snapshot = MapiMailStoreSnapshot::empty().with_delegate_freebusy_messages(vec![
            lpe_storage::DelegateFreeBusyMessageObject {
                id: message_id,
                account_id: Uuid::nil(),
                owner_account_id: Uuid::nil(),
                owner_email: "owner@example.test".to_string(),
                message_kind: "freebusy".to_string(),
                subject: "owner@example.test: busy".to_string(),
                body_text: "busy".to_string(),
                starts_at: None,
                ends_at: None,
                busy_status: None,
                payload_json: "{}".to_string(),
                updated_at: "2026-05-26T08:00:00Z".to_string(),
            },
        ]);
        let object = MapiObject::DelegateFreeBusyMessage {
            folder_id: INBOX_FOLDER_ID,
            message_id: snapshot.delegate_freebusy_messages()[0].id,
        };
        let mut payload = Vec::new();
        payload.extend_from_slice(&4096u16.to_le_bytes());
        payload.extend_from_slice(&1u16.to_le_bytes());
        payload.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
        let request = RopRequest {
            rop_id: RopId::GetPropertiesSpecific.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload,
        };

        let response = rop_get_properties_specific_response(
            &request,
            Some(&object),
            &principal,
            &[],
            &[],
            &snapshot,
        );

        assert_eq!(response[0], RopId::GetPropertiesSpecific.as_u8());
        assert_eq!(
            u32::from_le_bytes(response[2..6].try_into().unwrap()),
            0x8004_010F
        );
    }

    #[test]
    fn message_body_getprops_contract_reports_canonical_body_shape() {
        let mailbox_id = Uuid::parse_str("10101010-1010-1010-1010-101010101010").unwrap();
        let email_id = Uuid::parse_str("20202020-2020-2020-2020-202020202020").unwrap();
        crate::mapi::identity::remember_mapi_identity(mailbox_id, INBOX_FOLDER_ID);
        crate::mapi::identity::remember_mapi_identity(
            email_id,
            crate::mapi::identity::mapi_store_id(0x99),
        );
        let mailboxes = vec![JmapMailbox {
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
        }];
        let emails = vec![JmapEmail {
            id: email_id,
            thread_id: email_id,
            mailbox_ids: vec![mailbox_id],
            mailbox_states: Vec::new(),
            mailbox_id,
            mailbox_role: "inbox".to_string(),
            mailbox_name: "Inbox".to_string(),
            modseq: 7,
            received_at: "2026-06-07T19:56:00Z".to_string(),
            sent_at: None,
            from_address: "sender@example.test".to_string(),
            from_display: Some("Sender".to_string()),
            sender_address: None,
            sender_display: None,
            sender_authorization_kind: "author".to_string(),
            submitted_by_account_id: Uuid::nil(),
            to: vec![JmapEmailAddress {
                address: "test@example.test".to_string(),
                display_name: Some("Test".to_string()),
            }],
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: "Body check".to_string(),
            preview: "Plain body".to_string(),
            body_text: "Plain body".to_string(),
            body_html_sanitized: Some("<p>Plain body</p>".to_string()),
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
            size_octets: 512,
            internet_message_id: Some("<body-check@example.test>".to_string()),
            mime_blob_ref: None,
            delivery_status: "delivered".to_string(),
        }];
        let object = MapiObject::Message {
            folder_id: INBOX_FOLDER_ID,
            message_id: crate::mapi::identity::mapi_store_id(0x99),
            saved_email: None,
            pending_properties: HashMap::new(),
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
        );

        let contract = format_message_body_getprops_contract(
            Some(&object),
            &[
                PID_TAG_BODY_W,
                PID_TAG_RTF_COMPRESSED,
                PID_TAG_HTML_BINARY,
                PID_TAG_NATIVE_BODY,
            ],
            &mailboxes,
            &emails,
            &snapshot,
        );

        assert!(contract.contains("message_found=true"));
        assert!(contract.contains("source=mailbox"));
        assert!(contract.contains("subject_chars=10"));
        assert!(contract.contains("body_text_chars=10"));
        assert!(contract.contains("body_text_empty=false"));
        assert!(contract.contains("body_html_bytes=17"));
        assert!(contract.contains("native_body=3"));
        assert!(
            contract.contains("requested_body_tags=0x1000001f,0x10090102,0x10130102,0x10160003")
        );
    }

    #[test]
    fn microsoft_oxcdata_property_row_example_streams_oversized_body() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id: Uuid::from_u128(0x8181),
            email: "test@example.test".to_string(),
            display_name: "Test".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };
        let mailbox_id = Uuid::from_u128(0x9001);
        let email_id = Uuid::from_u128(0x9002);
        let message_id = crate::mapi::identity::mapi_store_id(0x9002);
        crate::mapi::identity::remember_mapi_identity(mailbox_id, INBOX_FOLDER_ID);
        crate::mapi::identity::remember_mapi_identity(email_id, message_id);
        let mailboxes = vec![JmapMailbox {
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
        }];
        let emails = vec![JmapEmail {
            id: email_id,
            thread_id: email_id,
            mailbox_ids: vec![mailbox_id],
            mailbox_states: Vec::new(),
            mailbox_id,
            mailbox_role: "inbox".to_string(),
            mailbox_name: "Inbox".to_string(),
            modseq: 7,
            received_at: "2026-06-07T19:56:00Z".to_string(),
            sent_at: None,
            from_address: "sender@example.test".to_string(),
            from_display: Some("Sender".to_string()),
            sender_address: None,
            sender_display: None,
            sender_authorization_kind: "author".to_string(),
            submitted_by_account_id: Uuid::nil(),
            to: Vec::new(),
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: "Hello".to_string(),
            preview: "Large body".to_string(),
            body_text: "Large body ".repeat(32),
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
            size_octets: 4096,
            internet_message_id: Some("<body-check@example.test>".to_string()),
            mime_blob_ref: None,
            delivery_status: "delivered".to_string(),
        }];
        let object = MapiObject::Message {
            folder_id: INBOX_FOLDER_ID,
            message_id,
            saved_email: None,
            pending_properties: HashMap::new(),
        };
        let mut payload = Vec::new();
        payload.extend_from_slice(&16u16.to_le_bytes());
        payload.extend_from_slice(&3u16.to_le_bytes());
        payload.extend_from_slice(&PID_TAG_MESSAGE_FLAGS.to_le_bytes());
        payload.extend_from_slice(&0x0037_0001u32.to_le_bytes());
        payload.extend_from_slice(&PID_TAG_BODY_W.to_le_bytes());
        let request = RopRequest {
            rop_id: RopId::GetPropertiesSpecific.as_u8(),
            input_handle_index: Some(2),
            output_handle_index: None,
            payload,
        };

        let response = rop_get_properties_specific_response(
            &request,
            Some(&object),
            &principal,
            &mailboxes,
            &emails,
            &MapiMailStoreSnapshot::empty(),
        );

        let mut expected_row = vec![0x00];
        expected_row.extend_from_slice(&0x13u32.to_le_bytes());
        expected_row.extend_from_slice(&0x001Fu16.to_le_bytes());
        expected_row.push(0x00);
        expected_row.extend_from_slice(&utf16z_bytes("Hello"));
        expected_row.push(0x0A);
        expected_row.extend_from_slice(&0x8007_000E_u32.to_le_bytes());

        assert_eq!(&response[..7], &[0x07, 0x02, 0, 0, 0, 0, 1]);
        assert_eq!(&response[7..], expected_row.as_slice());
    }

    #[test]
    fn saved_message_handle_getprops_uses_same_batch_email() {
        let account_id = Uuid::parse_str("10101010-1010-1010-1010-101010101010").unwrap();
        let email_id = Uuid::parse_str("20202020-2020-2020-2020-202020202020").unwrap();
        let message_id = crate::mapi::identity::mapi_store_id(0x99);
        let email = JmapEmail {
            id: email_id,
            thread_id: email_id,
            mailbox_ids: vec![account_id],
            mailbox_states: Vec::new(),
            mailbox_id: account_id,
            mailbox_role: "inbox".to_string(),
            mailbox_name: "Inbox".to_string(),
            modseq: 7,
            received_at: "2026-06-07T19:56:00Z".to_string(),
            sent_at: None,
            from_address: "sender@example.test".to_string(),
            from_display: Some("Sender".to_string()),
            sender_address: None,
            sender_display: None,
            sender_authorization_kind: "author".to_string(),
            submitted_by_account_id: account_id,
            to: Vec::new(),
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: "Saved batch".to_string(),
            preview: "Saved body".to_string(),
            body_text: "Saved body".to_string(),
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
            size_octets: 128,
            internet_message_id: Some("<saved-batch@example.test>".to_string()),
            mime_blob_ref: None,
            delivery_status: "delivered".to_string(),
        };
        let object = MapiObject::Message {
            folder_id: INBOX_FOLDER_ID,
            message_id,
            saved_email: Some(MapiSavedEmail { email }),
            pending_properties: HashMap::new(),
        };
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id,
            email: "test@example.test".to_string(),
            display_name: "test".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };
        let mut payload = Vec::new();
        payload.extend_from_slice(&4096u16.to_le_bytes());
        payload.extend_from_slice(&2u16.to_le_bytes());
        payload.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
        payload.extend_from_slice(&PID_TAG_BODY_W.to_le_bytes());
        let request = RopRequest {
            rop_id: RopId::GetPropertiesSpecific as u8,
            input_handle_index: Some(1),
            output_handle_index: None,
            payload,
        };

        let response = rop_get_properties_specific_response(
            &request,
            Some(&object),
            &principal,
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
        );

        assert_eq!(&response[..7], &[0x07, 0x01, 0, 0, 0, 0, 0]);
        assert!(response
            .windows(utf16z_bytes("Saved batch").len())
            .any(|window| window == utf16z_bytes("Saved batch").as_slice()));
        assert!(response
            .windows(utf16z_bytes("Saved body").len())
            .any(|window| window == utf16z_bytes("Saved body").as_slice()));
    }

    #[test]
    pub(in crate::mapi) fn persisted_message_getprops_returns_body_values() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
            email: "test@l-p-e.ch".to_string(),
            display_name: "test".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };
        let mailbox_id = Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap();
        crate::mapi::identity::remember_mapi_identity(mailbox_id, INBOX_FOLDER_ID);
        let email_id = Uuid::parse_str("99999999-9999-4999-8999-999999999999").unwrap();
        let message_id = crate::mapi::identity::mapi_store_id(0x99);
        crate::mapi::identity::remember_mapi_identity(email_id, message_id);
        let mailboxes = vec![JmapMailbox {
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
        }];
        let emails = vec![JmapEmail {
            id: email_id,
            thread_id: email_id,
            mailbox_ids: vec![mailbox_id],
            mailbox_states: Vec::new(),
            mailbox_id,
            mailbox_role: "inbox".to_string(),
            mailbox_name: "Inbox".to_string(),
            modseq: 7,
            received_at: "2026-06-07T19:56:00Z".to_string(),
            sent_at: None,
            from_address: "sender@example.test".to_string(),
            from_display: Some("Sender".to_string()),
            sender_address: None,
            sender_display: None,
            sender_authorization_kind: "author".to_string(),
            submitted_by_account_id: Uuid::nil(),
            to: Vec::new(),
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: "Body check".to_string(),
            preview: "Plain body".to_string(),
            body_text: "Plain body".to_string(),
            body_html_sanitized: Some("<p>Plain body</p>".to_string()),
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
            size_octets: 512,
            internet_message_id: Some("<body-check@example.test>".to_string()),
            mime_blob_ref: None,
            delivery_status: "delivered".to_string(),
        }];
        let object = MapiObject::Message {
            folder_id: INBOX_FOLDER_ID,
            message_id,
            saved_email: None,
            pending_properties: HashMap::new(),
        };
        let mut payload = Vec::new();
        payload.extend_from_slice(&4096u16.to_le_bytes());
        payload.extend_from_slice(&4u16.to_le_bytes());
        for tag in [
            PID_TAG_BODY_W,
            PID_TAG_RTF_COMPRESSED,
            PID_TAG_HTML_BINARY,
            PID_TAG_NATIVE_BODY,
        ] {
            payload.extend_from_slice(&tag.to_le_bytes());
        }
        let request = RopRequest {
            rop_id: RopId::GetPropertiesSpecific as u8,
            input_handle_index: Some(1),
            output_handle_index: None,
            payload,
        };

        let response = rop_get_properties_specific_response(
            &request,
            Some(&object),
            &principal,
            &mailboxes,
            &emails,
            &MapiMailStoreSnapshot::empty(),
        );

        assert_eq!(&response[..7], &[0x07, 0x01, 0, 0, 0, 0, 0]);
        assert!(response
            .windows(utf16z_bytes("Plain body").len())
            .any(|window| window == utf16z_bytes("Plain body").as_slice()));
        assert!(response
            .windows("<p>Plain body</p>".len())
            .any(|window| window == b"<p>Plain body</p>"));
        assert!(response.windows(5).any(|window| window == b"{\\rtf"));
        assert!(response
            .windows(4)
            .any(|window| window == 3u32.to_le_bytes()));
    }

    #[test]
    fn saved_associated_config_getprops_uses_same_batch_saved_message() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
            email: "test@l-p-e.ch".to_string(),
            display_name: "test".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };
        let config_id = crate::mapi::identity::mapi_store_id(0x4321);
        let object = MapiObject::AssociatedConfig {
            folder_id: CALENDAR_FOLDER_ID,
            config_id,
            saved_message: Some(crate::mapi_store::MapiAssociatedConfigMessage {
                id: config_id,
                folder_id: CALENDAR_FOLDER_ID,
                canonical_id: Uuid::parse_str("11111111-2222-4333-8444-555555555555").unwrap(),
                message_class: "IPM.Configuration.Calendar".to_string(),
                subject: "Calendar config".to_string(),
                properties_json: serde_json::json!({}),
            }),
        };
        let mut payload = Vec::new();
        payload.extend_from_slice(&4096u16.to_le_bytes());
        payload.extend_from_slice(&2u16.to_le_bytes());
        payload.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
        payload.extend_from_slice(&PID_TAG_CHANGE_KEY.to_le_bytes());
        let request = RopRequest {
            rop_id: RopId::GetPropertiesSpecific as u8,
            input_handle_index: Some(3),
            output_handle_index: None,
            payload,
        };

        let response = rop_get_properties_specific_response(
            &request,
            Some(&object),
            &principal,
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
        );

        assert_eq!(&response[..7], &[0x07, 0x03, 0, 0, 0, 0, 0]);
        assert!(response
            .windows(utf16z_bytes("IPM.Configuration.Calendar").len())
            .any(|window| window == utf16z_bytes("IPM.Configuration.Calendar").as_slice()));
        let expected_change_key = mapi_mailstore::change_key_for_change_number(
            mapi_mailstore::change_number_for_store_id(config_id),
        );
        assert!(response
            .windows(expected_change_key.len())
            .any(|window| window == expected_change_key.as_slice()));
    }

    #[test]
    fn property_row_kind_reports_fallback_defaults_as_flagged() {
        const UNKNOWN_FOLDER_INTEGER: u32 = 0x801D_0003;
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
            email: "test@l-p-e.ch".to_string(),
            display_name: "test".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };
        let object = MapiObject::Folder {
            folder_id: INBOX_FOLDER_ID,
            properties: HashMap::new(),
        };

        assert_eq!(
            property_row_kind_for_debug(
                Some(&object),
                &principal,
                &[],
                &[],
                &MapiMailStoreSnapshot::empty(),
                &[UNKNOWN_FOLDER_INTEGER],
            ),
            "flagged"
        );

        let mut payload = Vec::new();
        payload.extend_from_slice(&4096u16.to_le_bytes());
        payload.extend_from_slice(&1u16.to_le_bytes());
        payload.extend_from_slice(&UNKNOWN_FOLDER_INTEGER.to_le_bytes());
        let request = RopRequest {
            rop_id: RopId::GetPropertiesSpecific as u8,
            input_handle_index: Some(3),
            output_handle_index: None,
            payload,
        };

        let response = rop_get_properties_specific_response(
            &request,
            Some(&object),
            &principal,
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
        );

        assert_eq!(&response[..7], &[0x07, 0x03, 0, 0, 0, 0, 1]);
        assert_eq!(response[7], 0x0A);
        assert_eq!(
            u32::from_le_bytes(response[8..12].try_into().unwrap()),
            0x8004_010F
        );
    }

    #[test]
    fn undocumented_folder_binary_120c_returns_empty_binary() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
            email: "test@l-p-e.ch".to_string(),
            display_name: "test".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };
        let folder = MapiObject::Folder {
            folder_id: INBOX_FOLDER_ID,
            properties: HashMap::new(),
        };
        let mut payload = Vec::new();
        payload.extend_from_slice(&4096u16.to_le_bytes());
        payload.extend_from_slice(&1u16.to_le_bytes());
        payload.extend_from_slice(&OUTLOOK_UNDOCUMENTED_FOLDER_BINARY_120C.to_le_bytes());
        let request = RopRequest {
            rop_id: RopId::GetPropertiesSpecific as u8,
            input_handle_index: Some(1),
            output_handle_index: None,
            payload,
        };

        assert_eq!(
            property_tag_debug_name(OUTLOOK_UNDOCUMENTED_FOLDER_BINARY_120C),
            "OutlookUndocumentedFolderBinary120C"
        );
        assert!(!fallback_default_specific_property(
            Some(&folder),
            &principal,
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
            OUTLOOK_UNDOCUMENTED_FOLDER_BINARY_120C,
        ));

        let response = rop_get_properties_specific_response(
            &request,
            Some(&folder),
            &principal,
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
        );

        assert_eq!(&response[..7], &[0x07, 0x01, 0, 0, 0, 0, 0]);
        assert_eq!(&response[7..], &[0x00, 0x00]);

        for folder_id in [
            CALENDAR_FOLDER_ID,
            CONTACTS_FOLDER_ID,
            TASKS_FOLDER_ID,
            NOTES_FOLDER_ID,
            JOURNAL_FOLDER_ID,
        ] {
            let folder = MapiObject::Folder {
                folder_id,
                properties: HashMap::new(),
            };
            let mut default_view_payload = Vec::new();
            default_view_payload.extend_from_slice(&4096u16.to_le_bytes());
            default_view_payload.extend_from_slice(&1u16.to_le_bytes());
            default_view_payload.extend_from_slice(&PID_TAG_DEFAULT_VIEW_ENTRY_ID.to_le_bytes());
            let default_view_request = RopRequest {
                rop_id: RopId::GetPropertiesSpecific as u8,
                input_handle_index: Some(1),
                output_handle_index: None,
                payload: default_view_payload,
            };

            assert!(!fallback_default_specific_property(
                Some(&folder),
                &principal,
                &[],
                &[],
                &MapiMailStoreSnapshot::empty(),
                PID_TAG_DEFAULT_VIEW_ENTRY_ID,
            ));

            let response = rop_get_properties_specific_response(
                &default_view_request,
                Some(&folder),
                &principal,
                &[],
                &[],
                &MapiMailStoreSnapshot::empty(),
            );

            assert_eq!(&response[..7], &[0x07, 0x01, 0, 0, 0, 0, 0]);
            assert!(response.len() > 7);
        }

        let ipm_subtree = MapiObject::Folder {
            folder_id: IPM_SUBTREE_FOLDER_ID,
            properties: HashMap::new(),
        };
        let mut default_view_payload = Vec::new();
        default_view_payload.extend_from_slice(&4096u16.to_le_bytes());
        default_view_payload.extend_from_slice(&1u16.to_le_bytes());
        default_view_payload.extend_from_slice(&PID_TAG_DEFAULT_VIEW_ENTRY_ID.to_le_bytes());
        let default_view_request = RopRequest {
            rop_id: RopId::GetPropertiesSpecific as u8,
            input_handle_index: Some(1),
            output_handle_index: None,
            payload: default_view_payload,
        };

        assert!(!fallback_default_specific_property(
            Some(&ipm_subtree),
            &principal,
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
            PID_TAG_DEFAULT_VIEW_ENTRY_ID,
        ));

        let response = rop_get_properties_specific_response(
            &default_view_request,
            Some(&ipm_subtree),
            &principal,
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
        );

        assert_eq!(&response[..7], &[0x07, 0x01, 0, 0, 0, 0, 0]);
        assert_eq!(&response[7..], &[0x00, 0x00]);
    }

    #[test]
    fn fallback_property_errors_for_debug_match_wire_error_codes() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
            email: "test@l-p-e.ch".to_string(),
            display_name: "test".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };
        let folder = MapiObject::Folder {
            folder_id: INBOX_FOLDER_ID,
            properties: HashMap::new(),
        };
        let folder_error_tags = unsupported_specific_property_tags(
            Some(&folder),
            &principal,
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
            &[OUTLOOK_UNDOCUMENTED_FOLDER_BINARY_120C],
        );
        let folder_errors = format_property_errors_for_debug(
            Some(&folder),
            &principal,
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
            &folder_error_tags,
        );
        assert!(folder_errors.is_empty());

        let config = MapiObject::AssociatedConfig {
            folder_id: INBOX_FOLDER_ID,
            config_id: crate::mapi::identity::mapi_store_id(0x4322),
            saved_message: None,
        };
        assert!(fallback_default_specific_property(
            Some(&config),
            &principal,
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
            0x801D_0003,
        ));
        let missing_errors = format_property_errors_for_debug(
            Some(&config),
            &principal,
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
            &[0x801D_0003],
        );
        assert!(missing_errors.contains("0x801d0003:unknown:0x8004010f"));

        let unsupported_errors = format_property_errors_for_debug(
            Some(&config),
            &principal,
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
            &[0x801D_0000],
        );
        assert!(unsupported_errors.contains("0x801d0000:unknown:0x80040102"));
    }

    #[test]
    pub(in crate::mapi) fn folder_deleted_count_total_zero_is_modeled_not_fallback() {
        let folder = MapiObject::Folder {
            folder_id: COMMON_VIEWS_FOLDER_ID,
            properties: HashMap::new(),
        };

        assert!(modeled_zero_or_default_property(
            Some(&folder),
            PID_TAG_DELETED_COUNT_TOTAL
        ));
    }

    #[test]
    fn logon_empty_pst_path_is_modeled_not_fallback() {
        assert!(modeled_zero_or_default_property(
            Some(&MapiObject::Logon),
            PID_TAG_PST_PATH_W
        ));
    }

    #[test]
    fn folder_archive_policy_empty_defaults_are_modeled_not_fallback() {
        let folder = MapiObject::Folder {
            folder_id: INBOX_FOLDER_ID,
            properties: HashMap::new(),
        };

        for property_tag in [
            PID_TAG_ARCHIVE_TAG,
            PID_TAG_POLICY_TAG,
            PID_TAG_RETENTION_PERIOD,
            PID_TAG_RETENTION_FLAGS,
            PID_TAG_ARCHIVE_PERIOD,
        ] {
            assert!(modeled_zero_or_default_property(
                Some(&folder),
                property_tag
            ));
        }
    }

    #[test]
    fn folder_view_empty_defaults_are_modeled_not_fallback() {
        let folder = MapiObject::Folder {
            folder_id: INBOX_FOLDER_ID,
            properties: HashMap::new(),
        };

        for property_tag in [
            PID_TAG_FOLDER_FORM_FLAGS,
            PID_TAG_FOLDER_WEBVIEWINFO,
            PID_TAG_FOLDER_XVIEWINFO_E,
            PID_TAG_FOLDER_VIEWS_ONLY,
            PID_TAG_DEFAULT_FORM_NAME_W,
            PID_TAG_DEFAULT_VIEW_ENTRY_ID,
            PID_TAG_FOLDER_FORM_STORAGE,
            PID_TAG_ACL_MEMBER_NAME_W,
            0x6672_0102,
            PID_TAG_FOLDER_VIEWLIST_FLAGS,
        ] {
            assert!(modeled_zero_or_default_property(
                Some(&folder),
                property_tag
            ));
        }
    }

    #[test]
    fn empty_class_defaults_are_modeled_only_for_none_special_folders() {
        for folder_id in [
            ROOT_FOLDER_ID,
            DEFERRED_ACTION_FOLDER_ID,
            SPOOLER_QUEUE_FOLDER_ID,
            COMMON_VIEWS_FOLDER_ID,
            VIEWS_FOLDER_ID,
        ] {
            let folder = MapiObject::Folder {
                folder_id,
                properties: HashMap::new(),
            };

            for property_tag in [
                PID_TAG_CONTAINER_CLASS_W,
                PID_TAG_MESSAGE_CLASS_W,
                PID_TAG_DEFAULT_POST_MESSAGE_CLASS_STRING8,
                PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W,
            ] {
                assert!(modeled_zero_or_default_property(
                    Some(&folder),
                    property_tag
                ));
            }
        }

        let freebusy = MapiObject::Folder {
            folder_id: FREEBUSY_DATA_FOLDER_ID,
            properties: HashMap::new(),
        };

        for property_tag in [
            PID_TAG_CONTAINER_CLASS_W,
            PID_TAG_MESSAGE_CLASS_W,
            PID_TAG_DEFAULT_POST_MESSAGE_CLASS_STRING8,
            PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W,
        ] {
            assert!(!modeled_zero_or_default_property(
                Some(&freebusy),
                property_tag
            ));
        }

        let inbox = MapiObject::Folder {
            folder_id: INBOX_FOLDER_ID,
            properties: HashMap::new(),
        };

        for property_tag in [
            PID_TAG_CONTAINER_CLASS_W,
            PID_TAG_MESSAGE_CLASS_W,
            PID_TAG_DEFAULT_POST_MESSAGE_CLASS_STRING8,
            PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W,
        ] {
            assert!(!modeled_zero_or_default_property(
                Some(&inbox),
                property_tag
            ));
        }
    }

    #[test]
    fn root_folder_type_zero_is_modeled_not_fallback() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id: Uuid::nil(),
            email: "alice@example.test".to_string(),
            display_name: "Alice".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };
        let folder = MapiObject::Folder {
            folder_id: ROOT_FOLDER_ID,
            properties: HashMap::new(),
        };

        assert!(modeled_zero_or_default_property(
            Some(&folder),
            PID_TAG_FOLDER_TYPE
        ));
        assert!(!fallback_default_specific_property(
            Some(&folder),
            &principal,
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
            PID_TAG_FOLDER_TYPE
        ));
    }

    #[test]
    fn folder_type_getprops_contract_reports_loaded_inbox() {
        let account_id = Uuid::from_u128(0xbbbbbbbb_bbbb_bbbb_bbbb_bbbbbbbbbbbb);
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id,
            email: "alice@example.test".to_string(),
            display_name: "Alice".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };
        let inbox_id = Uuid::from_u128(0x1111);
        crate::mapi::identity::remember_mapi_identity(inbox_id, INBOX_FOLDER_ID);
        let inbox = JmapMailbox {
            id: inbox_id,
            parent_id: None,
            role: "inbox".to_string(),
            name: "INBOX".to_string(),
            sort_order: 0,
            modseq: 42,
            total_emails: 221,
            unread_emails: 17,
            size_octets: 0,
            is_subscribed: true,
        };
        let object = MapiObject::Folder {
            folder_id: INBOX_FOLDER_ID,
            properties: HashMap::new(),
        };

        let contract = format_folder_type_getprops_contract(
            Some(&object),
            &principal,
            &[PID_TAG_FOLDER_TYPE],
            &[inbox],
            &MapiMailStoreSnapshot::empty(),
        );

        assert!(contract.contains("mailbox_folder_found=true"));
        assert!(contract.contains("property_source=mailbox"));
        assert!(contract.contains("returned_value=1"));
        assert!(contract.contains("returned_kind=generic"));
        assert!(contract.contains("expected_kind=generic"));
        assert!(contract.ends_with("issues="));
    }

    #[test]
    fn folder_type_getprops_contract_flags_inbox_without_snapshot() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id: Uuid::from_u128(0xbbbbbbbb_bbbb_bbbb_bbbb_bbbbbbbbbbbb),
            email: "alice@example.test".to_string(),
            display_name: "Alice".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };
        let object = MapiObject::Folder {
            folder_id: INBOX_FOLDER_ID,
            properties: HashMap::new(),
        };

        let contract = format_folder_type_getprops_contract(
            Some(&object),
            &principal,
            &[PID_TAG_FOLDER_TYPE],
            &[],
            &MapiMailStoreSnapshot::empty(),
        );

        assert!(contract.contains("mailbox_folder_found=false"));
        assert!(contract.contains("property_source=special_folder_fallback"));
        assert!(contract.contains("returned_value=1"));
        assert!(contract
            .contains("issues=inbox_without_loaded_mailbox|inbox_answered_from_special_fallback"));
    }

    #[test]
    fn folder_type_getprops_contract_accepts_advertised_search_folder() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id: Uuid::from_u128(0xbbbbbbbb_bbbb_bbbb_bbbb_bbbbbbbbbbbb),
            email: "alice@example.test".to_string(),
            display_name: "Alice".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };
        let object = MapiObject::Folder {
            folder_id: CONTACTS_SEARCH_FOLDER_ID,
            properties: HashMap::from([(PID_TAG_FOLDER_TYPE, MapiValue::U32(FOLDER_SEARCH))]),
        };

        let contract = format_folder_type_getprops_contract(
            Some(&object),
            &principal,
            &[PID_TAG_FOLDER_TYPE],
            &[],
            &MapiMailStoreSnapshot::empty(),
        );

        assert!(contract.contains("advertised_special_folder=true"));
        assert!(contract.contains("property_source=opened_handle"));
        assert!(contract.contains("returned_value=2"));
        assert!(contract.contains("returned_kind=search"));
        assert!(contract.contains("expected_kind=search"));
        assert!(contract.ends_with("issues="));

        let finder_root = MapiObject::Folder {
            folder_id: SEARCH_FOLDER_ID,
            properties: HashMap::from([(PID_TAG_FOLDER_TYPE, MapiValue::U32(FOLDER_SEARCH))]),
        };
        let contract = format_folder_type_getprops_contract(
            Some(&finder_root),
            &principal,
            &[PID_TAG_FOLDER_TYPE],
            &[],
            &MapiMailStoreSnapshot::empty(),
        );

        assert!(contract.contains("folder_id=0x00000000000b0001"));
        assert!(contract.contains("returned_kind=search"));
        assert!(contract.contains("expected_kind=search"));
        assert!(contract.ends_with("issues="));
    }

    #[test]
    fn folder_type_getprops_contract_prefers_saved_search_definition() {
        let account_id = Uuid::from_u128(0xbbbbbbbb_bbbb_bbbb_bbbb_bbbbbbbbbbbb);
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id,
            email: "alice@example.test".to_string(),
            display_name: "Alice".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };
        let folder_id = crate::mapi::identity::mapi_store_id(0x165);
        let mailbox_id = Uuid::from_u128(0x165);
        crate::mapi::identity::remember_mapi_identity(mailbox_id, folder_id);
        let mailbox = JmapMailbox {
            id: mailbox_id,
            parent_id: None,
            role: String::new(),
            name: "People Search".to_string(),
            sort_order: 0,
            modseq: 42,
            total_emails: 0,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        };
        let snapshot = MapiMailStoreSnapshot::empty().with_search_folder_definitions(vec![
            lpe_storage::SearchFolderDefinition {
                id: mailbox_id,
                account_id,
                role: "contacts_search".to_string(),
                display_name: "People Search".to_string(),
                definition_kind: "exchange_builtin".to_string(),
                result_object_kind: "contact".to_string(),
                scope_json: serde_json::json!({"scope": "contacts"}),
                restriction_json: serde_json::json!({"kind": "contacts_search"}),
                excluded_folder_roles: Vec::new(),
                is_builtin: true,
            },
        ]);
        let object = MapiObject::Folder {
            folder_id,
            properties: HashMap::from([(PID_TAG_FOLDER_TYPE, MapiValue::U32(FOLDER_GENERIC))]),
        };

        let contract = format_folder_type_getprops_contract(
            Some(&object),
            &principal,
            &[PID_TAG_FOLDER_TYPE],
            &[mailbox],
            &snapshot,
        );

        assert!(contract.contains("search_folder_definition_found=true"));
        assert!(contract.contains("property_source=search_folder_definition"));
        assert!(contract.contains("returned_value=2"));
        assert!(contract.contains("returned_kind=search"));
        assert!(contract.contains("expected_kind=search"));
        assert!(contract.ends_with("issues="));
    }

    #[test]
    fn folder_type_getprops_contract_accepts_projected_search_folder_role() {
        let account_id = Uuid::from_u128(0xbbbbbbbb_bbbb_bbbb_bbbb_bbbbbbbbbbbb);
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id,
            email: "alice@example.test".to_string(),
            display_name: "Alice".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };
        let folder_id = crate::mapi::identity::mapi_store_id(0x195);
        let mailbox_id = Uuid::from_u128(0x195);
        crate::mapi::identity::remember_mapi_identity(mailbox_id, folder_id);
        let mailbox = JmapMailbox {
            id: mailbox_id,
            parent_id: None,
            role: "__mapi_search_folder_message".to_string(),
            name: "Categories Rename Search Folder".to_string(),
            sort_order: 0,
            modseq: 42,
            total_emails: 0,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        };
        let object = MapiObject::Folder {
            folder_id,
            properties: HashMap::from([(PID_TAG_FOLDER_TYPE, MapiValue::U32(FOLDER_SEARCH))]),
        };

        let contract = format_folder_type_getprops_contract(
            Some(&object),
            &principal,
            &[PID_TAG_FOLDER_TYPE],
            &[mailbox],
            &MapiMailStoreSnapshot::empty(),
        );

        assert!(contract.contains("returned_kind=search"));
        assert!(contract.contains("expected_kind=search"));
        assert!(contract.ends_with("issues="));
    }

    #[test]
    fn folder_getprops_returns_search_type_for_saved_search_definition() {
        let account_id = Uuid::from_u128(0xbbbbbbbb_bbbb_bbbb_bbbb_bbbbbbbbbbbb);
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id,
            email: "alice@example.test".to_string(),
            display_name: "Alice".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };
        let folder_id = crate::mapi::identity::mapi_store_id(0x168);
        let mailbox_id = Uuid::from_u128(0x168);
        crate::mapi::identity::remember_mapi_identity(mailbox_id, folder_id);
        let mailbox = JmapMailbox {
            id: mailbox_id,
            parent_id: None,
            role: String::new(),
            name: "Category Search".to_string(),
            sort_order: 0,
            modseq: 42,
            total_emails: 0,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        };
        let snapshot = MapiMailStoreSnapshot::empty().with_search_folder_definitions(vec![
            lpe_storage::SearchFolderDefinition {
                id: mailbox_id,
                account_id,
                role: "category_search".to_string(),
                display_name: "Category Search".to_string(),
                definition_kind: "exchange_builtin".to_string(),
                result_object_kind: "message".to_string(),
                scope_json: serde_json::json!({"scope": "mail"}),
                restriction_json: serde_json::json!({"kind": "category_search"}),
                excluded_folder_roles: Vec::new(),
                is_builtin: true,
            },
        ]);
        let object = MapiObject::Folder {
            folder_id,
            properties: HashMap::from([(PID_TAG_FOLDER_TYPE, MapiValue::U32(FOLDER_GENERIC))]),
        };

        let row = serialize_object_property(
            Some(&object),
            &principal,
            &[mailbox],
            &[],
            &snapshot,
            PID_TAG_FOLDER_TYPE,
        );

        assert_eq!(u32::from_le_bytes(row.try_into().unwrap()), FOLDER_SEARCH);
    }

    #[test]
    fn default_view_entry_id_debug_decodes_message_target_ids() {
        let entry_id = crate::mapi::identity::message_entry_id_from_object_ids(
            Uuid::from_u128(0xbbbbbbbb_bbbb_bbbb_bbbb_bbbbbbbbbbbb),
            INBOX_FOLDER_ID,
            crate::mapi_store::OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_ID,
        )
        .unwrap();

        assert_eq!(
            default_view_message_entry_id_target(&entry_id),
            Some((
                INBOX_FOLDER_ID,
                crate::mapi_store::OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_ID
            ))
        );
        assert_eq!(default_view_message_entry_id_target(&entry_id[..46]), None);
    }

    #[test]
    fn folder_getprops_projects_saved_search_definition_metadata() {
        let account_id = Uuid::from_u128(0xbbbbbbbb_bbbb_bbbb_bbbb_bbbbbbbbbbbb);
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id,
            email: "alice@example.test".to_string(),
            display_name: "Alice".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };
        let folder_id = crate::mapi::identity::mapi_store_id(0x1db);
        let definition_id = Uuid::from_u128(0x1db);
        crate::mapi::identity::remember_mapi_identity(definition_id, folder_id);
        let snapshot = MapiMailStoreSnapshot::empty().with_search_folder_definitions(vec![
            lpe_storage::SearchFolderDefinition {
                id: definition_id,
                account_id,
                role: "custom".to_string(),
                display_name: "Categories Rename Search Folder".to_string(),
                definition_kind: "user_saved".to_string(),
                result_object_kind: "message".to_string(),
                scope_json: serde_json::json!({"scope": "mail"}),
                restriction_json: serde_json::json!({"kind": "mapi_bounded", "all": []}),
                excluded_folder_roles: Vec::new(),
                is_builtin: false,
            },
        ]);
        let object = MapiObject::Folder {
            folder_id,
            properties: HashMap::new(),
        };

        assert_eq!(
            serialize_object_property(
                Some(&object),
                &principal,
                &[],
                &[],
                &snapshot,
                PID_TAG_DISPLAY_NAME_W,
            ),
            utf16z_bytes("Categories Rename Search Folder")
        );
        assert_eq!(
            serialize_object_property(
                Some(&object),
                &principal,
                &[],
                &[],
                &snapshot,
                PID_TAG_CONTAINER_CLASS_W,
            ),
            utf16z_bytes("IPF.Note")
        );
        assert_eq!(
            serialize_object_property(
                Some(&object),
                &principal,
                &[],
                &[],
                &snapshot,
                PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W,
            ),
            utf16z_bytes("IPM.Note")
        );
        let rights = serialize_object_property(
            Some(&object),
            &principal,
            &[],
            &[],
            &snapshot,
            PID_TAG_RIGHTS,
        );
        assert_eq!(
            u32::from_le_bytes(rights.try_into().unwrap()),
            MAPI_FOLDER_ACCESS
        );
        let mut expected_extended_flags = Vec::new();
        write_mapi_value(
            &mut expected_extended_flags,
            PID_TAG_EXTENDED_FOLDER_FLAGS,
            &MapiValue::Binary(extended_folder_flags()),
        );
        assert_eq!(
            serialize_object_property(
                Some(&object),
                &principal,
                &[],
                &[],
                &snapshot,
                PID_TAG_EXTENDED_FOLDER_FLAGS,
            ),
            expected_extended_flags
        );
    }

    #[test]
    pub(in crate::mapi) fn microsoft_get_message_status_response_uses_set_status_opcode() {
        let request = RopRequest {
            rop_id: RopId::GetMessageStatus.as_u8(),
            input_handle_index: Some(1),
            output_handle_index: None,
            payload: Vec::new(),
        };

        let response = rop_message_status_response(&request, 0);

        assert_eq!(
            response,
            vec![RopId::SetMessageStatus.as_u8(), 1, 0, 0, 0, 0, 0, 0, 0, 0]
        );
    }

    #[test]
    pub(in crate::mapi) fn microsoft_open_embedded_message_response_includes_message_id() {
        let request = RopRequest {
            rop_id: RopId::OpenEmbeddedMessage.as_u8(),
            input_handle_index: Some(3),
            output_handle_index: Some(4),
            payload: Vec::new(),
        };
        let message_id = crate::mapi::identity::mapi_store_id(0x44);

        let response = rop_open_embedded_message_response(&request, message_id, "Embedded", 0);

        assert_eq!(response[0], RopId::OpenEmbeddedMessage.as_u8());
        assert_eq!(response[1], 4);
        assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
        assert_eq!(response[6], 0);
        assert_eq!(
            crate::mapi::identity::object_id_from_wire_id(&response[7..15]),
            Some(message_id)
        );
        assert!(response
            .windows(utf16z_bytes("Embedded").len())
            .any(|window| window == utf16z_bytes("Embedded").as_slice()));
    }

    #[test]
    pub(in crate::mapi) fn restriction_parser_preserves_content_fuzzy_levels() {
        let mut restriction = vec![MapiRestrictionType::Content as u8];
        restriction.extend_from_slice(&0x0002u16.to_le_bytes());
        restriction.extend_from_slice(&0x0001u16.to_le_bytes());
        restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
        restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
        write_utf16z(&mut restriction, "IPM.Schedule");

        assert_eq!(
            parse_mapi_restriction(&restriction).unwrap(),
            MapiRestriction::Content {
                property_tag: PID_TAG_MESSAGE_CLASS_W,
                value: "IPM.Schedule".to_string(),
                fuzzy_level_low: 0x0002,
                fuzzy_level_high: 0x0001,
            }
        );
    }

    #[test]
    pub(in crate::mapi) fn restriction_parser_rejects_trailing_bytes() {
        let mut restriction = vec![MapiRestrictionType::Exist as u8];
        restriction.extend_from_slice(&PID_TAG_HAS_ATTACHMENTS.to_le_bytes());

        assert_eq!(
            parse_mapi_restriction(&restriction).unwrap(),
            MapiRestriction::Exist {
                property_tag: PID_TAG_HAS_ATTACHMENTS
            }
        );

        restriction.extend_from_slice(&[0xEE, 0xEE]);

        assert!(parse_mapi_restriction(&restriction).is_err());
    }

    #[test]
    pub(in crate::mapi) fn outlook_logon_bootstrap_details_use_valid_store_icons() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
            email: "test@l-p-e.ch".to_string(),
            display_name: "test".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };
        let columns = [
            PID_TAG_MAILBOX_OWNER_NAME_W,
            PID_TAG_MAILBOX_OWNER_ENTRY_ID,
            PID_TAG_SERVER_TYPE_DISPLAY_NAME_W,
            PID_TAG_SERVER_CONNECTED_ICON,
            PID_TAG_SERVER_ACCOUNT_ICON,
            PID_TAG_PRIVATE,
            PID_TAG_OUTLOOK_STORE_STATE,
            PID_TAG_USER_GUID,
            PID_TAG_MAX_SUBMIT_MESSAGE_SIZE,
        ];

        assert!(is_outlook_logon_bootstrap_getprops(
            Some(&MapiObject::Logon),
            &columns
        ));
        let details = format_outlook_logon_bootstrap_property_details(&principal, &columns);
        let row_shape = outlook_logon_bootstrap_row_shape(&principal, &columns);

        assert!(details.contains("provider_uid_matches_nspi=true"));
        assert!(details.contains("r4=0x00000001"));
        assert!(details.contains("dn_null_terminated=true"));
        assert!(details.contains("private=true"));
        assert!(details.contains("max_submit_message_size_kb=35840"));
        assert!(details.contains("ico_len=70"));
        assert!(details.contains("reserved=0x0000"));
        assert!(details.contains("type=0x0001"));
        assert!(details.contains("count=1"));
        assert!(details.contains("bit_count=32"));
        assert!(details.contains("length_matches_directory=true"));
        assert_eq!(row_shape.estimated_rop_payload_bytes, 297);
        assert_eq!(row_shape.property_row_bytes, 290);
        assert_eq!(row_shape.icon_row_bytes, 144);
        assert_eq!(row_shape.non_icon_row_bytes, 146);
    }

    #[test]
    fn contacts_search_getprops_content_count_matches_projected_results() {
        let account_id = Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap();
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id,
            email: "test@example.test".to_string(),
            display_name: "Test".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };
        let rights = lpe_storage::CollaborationRights {
            may_read: true,
            may_write: true,
            may_delete: true,
            may_share: true,
        };
        let collection = lpe_storage::CollaborationCollection {
            id: "default".to_string(),
            kind: "contacts".to_string(),
            owner_account_id: account_id,
            owner_email: principal.email.clone(),
            owner_display_name: principal.display_name.clone(),
            display_name: "Contacts".to_string(),
            is_owned: true,
            rights: rights.clone(),
        };
        let contact_id = Uuid::parse_str("71717171-7171-7171-7171-717171717171").unwrap();
        crate::mapi::identity::remember_mapi_identity(
            contact_id,
            crate::mapi::identity::mapi_store_id(67),
        );
        let contact = lpe_storage::AccessibleContact {
            id: contact_id,
            collection_id: collection.id.clone(),
            owner_account_id: account_id,
            owner_email: principal.email.clone(),
            owner_display_name: principal.display_name.clone(),
            rights,
            name: "Denis Ducret".to_string(),
            role: String::new(),
            email: "denis@example.test".to_string(),
            phone: String::new(),
            team: String::new(),
            notes: String::new(),
            ..Default::default()
        };
        let snapshot = MapiMailStoreSnapshot::new(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec![collection],
            Vec::new(),
            Vec::new(),
            vec![contact],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .with_search_folder_definitions(vec![lpe_storage::SearchFolderDefinition {
            id: Uuid::parse_str("34343434-3434-4434-8434-343434343402").unwrap(),
            account_id,
            role: "contacts_search".to_string(),
            display_name: "Contacts Search".to_string(),
            definition_kind: "exchange_builtin".to_string(),
            result_object_kind: "contact".to_string(),
            scope_json: serde_json::json!({"scope": "contacts"}),
            restriction_json: serde_json::json!({"kind": "contacts_search"}),
            excluded_folder_roles: Vec::new(),
            is_builtin: true,
        }]);
        let object = MapiObject::Folder {
            folder_id: CONTACTS_SEARCH_FOLDER_ID,
            properties: HashMap::new(),
        };
        let mut payload = Vec::new();
        payload.extend_from_slice(&4096u16.to_le_bytes());
        payload.extend_from_slice(&1u16.to_le_bytes());
        payload.extend_from_slice(&PID_TAG_CONTENT_COUNT.to_le_bytes());
        let request = RopRequest {
            rop_id: RopId::GetPropertiesSpecific as u8,
            input_handle_index: Some(1),
            output_handle_index: None,
            payload,
        };

        let response = rop_get_properties_specific_response(
            &request,
            Some(&object),
            &principal,
            &[],
            &[],
            &snapshot,
        );

        assert_eq!(&response[..7], &[0x07, 0x01, 0, 0, 0, 0, 0]);
        assert_eq!(u32::from_le_bytes(response[7..11].try_into().unwrap()), 1);
    }

    #[test]
    fn public_folder_replica_responses_match_microsoft_counter_shape() {
        let request = RopRequest {
            rop_id: 0x42,
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: Vec::new(),
        };

        assert_eq!(
            rop_get_owning_servers_response(&request, &["LPE-MBX-01".to_string()]),
            [
                0x42, 0x00, 0, 0, 0, 0, 1, 0, 1, 0, b'L', b'P', b'E', b'-', b'M', b'B', b'X', b'-',
                b'0', b'1', 0,
            ]
        );
        assert_eq!(
            rop_public_folder_is_ghosted_response(&request, true),
            [0x45, 0x00, 0, 0, 0, 0, 1, 0, 0, 0, 0]
        );
        assert_eq!(
            rop_public_folder_is_ghosted_response(&request, false),
            [0x45, 0x00, 0, 0, 0, 0, 0]
        );
    }

    #[test]
    pub(in crate::mapi) fn private_logon_places_exactly_13_folder_ids_before_response_flags() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
            email: "test@l-p-e.ch".to_string(),
            display_name: "test".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };
        let request = RopRequest {
            rop_id: 0xFE,
            input_handle_index: Some(0),
            output_handle_index: Some(1),
            payload: vec![0x01],
        };

        let response = rop_logon_response_body(&principal, &request);
        let response_flags_offset = 7 + PRIVATE_LOGON_SPECIAL_FOLDER_IDS.len() * 8;

        assert_eq!(PRIVATE_LOGON_SPECIAL_FOLDER_IDS.len(), 13);
        assert_eq!(response[response_flags_offset], 0x07);
        assert_eq!(
            &response[response_flags_offset + 1..response_flags_offset + 17],
            &principal.account_id.to_bytes_le()
        );
        assert_eq!(
            &response[7..response_flags_offset],
            PRIVATE_LOGON_SPECIAL_FOLDER_IDS
                .iter()
                .flat_map(|folder_id| {
                    crate::mapi::identity::wire_id_bytes_from_object_id(*folder_id)
                        .unwrap()
                        .to_vec()
                })
                .collect::<Vec<_>>()
                .as_slice()
        );
    }

    #[test]
    pub(in crate::mapi) fn long_term_id_from_id_accepts_outlook_and_emitted_counter_forms() {
        let canonical_id = crate::mapi::identity::CALENDAR_FOLDER_ID;
        let dynamic_id = crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 3,
        );
        let cases = [
            (
                crate::mapi::identity::wire_id_bytes_from_object_id(canonical_id)
                    .unwrap()
                    .to_vec(),
                canonical_id,
            ),
            (
                {
                    let mut bytes = Vec::new();
                    bytes.extend_from_slice(&crate::mapi::identity::globcnt_bytes(
                        crate::mapi::identity::CALENDAR_FOLDER_COUNTER,
                    ));
                    bytes.extend_from_slice(&1u16.to_le_bytes());
                    bytes
                },
                canonical_id,
            ),
            (
                {
                    let mut bytes = Vec::new();
                    bytes.extend_from_slice(&999u16.to_le_bytes());
                    bytes.extend_from_slice(&crate::mapi::identity::globcnt_bytes(
                        crate::mapi::identity::CALENDAR_FOLDER_COUNTER,
                    ));
                    bytes
                },
                canonical_id,
            ),
            (
                {
                    let mut bytes = crate::mapi::identity::globcnt_bytes(
                        crate::mapi::identity::CALENDAR_FOLDER_COUNTER,
                    )
                    .to_vec();
                    bytes.reverse();
                    bytes.extend_from_slice(&999u16.to_le_bytes());
                    bytes
                },
                canonical_id,
            ),
            (
                {
                    let mut bytes = Vec::new();
                    bytes.extend_from_slice(
                        &crate::mapi::identity::CONFLICTS_FOLDER_COUNTER.to_le_bytes()[..6],
                    );
                    bytes.extend_from_slice(&0u16.to_le_bytes());
                    bytes
                },
                crate::mapi::identity::CONFLICTS_FOLDER_ID,
            ),
            (
                {
                    let mut bytes = Vec::new();
                    bytes.extend_from_slice(
                        &crate::mapi::identity::global_counter_from_store_id(dynamic_id)
                            .unwrap()
                            .to_le_bytes()[..6],
                    );
                    bytes.extend_from_slice(&0u16.to_le_bytes());
                    bytes
                },
                dynamic_id,
            ),
        ];

        for (bytes, expected_id) in cases {
            let request = RopRequest {
                rop_id: 0x43,
                input_handle_index: Some(0),
                output_handle_index: None,
                payload: bytes,
            };
            let response = rop_long_term_id_from_id_response(&request);

            assert_eq!(&response[..6], &[0x43, 0x00, 0, 0, 0, 0]);
            assert_eq!(
                &response[6..30],
                &crate::mapi::identity::long_term_id_from_object_id(expected_id).unwrap()
            );
        }
    }

    #[test]
    pub(in crate::mapi) fn long_term_id_from_id_unmapped_values_return_ec_not_found() {
        for bytes in [[0; 8], [0xFF; 8], [0x01, 0, 0, 0, 0, 0, 0, 0]] {
            let request = RopRequest {
                rop_id: 0x43,
                input_handle_index: Some(0),
                output_handle_index: None,
                payload: bytes.to_vec(),
            };

            assert_eq!(
                rop_long_term_id_from_id_response(&request),
                vec![0x43, 0x00, 0x0F, 0x01, 0x04, 0x80]
            );
        }
    }

    #[test]
    pub(in crate::mapi) fn id_from_long_term_id_accepts_mailbox_guid_aliases_and_special_stale_guid(
    ) {
        let principal_guid = Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap();
        let aliases = [*principal_guid.as_bytes(), principal_guid.to_bytes_le()];
        let normal_id = crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 4,
        );
        let special_id = crate::mapi::identity::CALENDAR_FOLDER_ID;
        let mut aliased = crate::mapi::identity::long_term_id_from_object_id(normal_id).unwrap();
        aliased[..16].copy_from_slice(&principal_guid.to_bytes_le());
        let mut stale_special =
            crate::mapi::identity::long_term_id_from_object_id(special_id).unwrap();
        stale_special[..16].copy_from_slice(&[0xA5; 16]);
        let mut stale_normal =
            crate::mapi::identity::long_term_id_from_object_id(normal_id).unwrap();
        stale_normal[..16].copy_from_slice(&[0xA5; 16]);

        for (long_term_id, expected_id) in [(aliased, normal_id), (stale_special, special_id)] {
            let request = RopRequest {
                rop_id: 0x44,
                input_handle_index: Some(0),
                output_handle_index: None,
                payload: long_term_id.to_vec(),
            };
            let response = rop_id_from_long_term_id_response(&request, &aliases);

            assert_eq!(&response[..6], &[0x44, 0x00, 0, 0, 0, 0]);
            assert_eq!(
                &response[6..14],
                &crate::mapi::identity::wire_id_bytes_from_object_id(expected_id).unwrap()
            );
        }

        let request = RopRequest {
            rop_id: 0x44,
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: stale_normal.to_vec(),
        };
        assert_eq!(
            rop_id_from_long_term_id_response(&request, &aliases),
            vec![0x44, 0x00, 0x0F, 0x01, 0x04, 0x80]
        );
    }

    #[test]
    pub(in crate::mapi) fn ipm_subtree_ostid_read_prefers_session_client_write() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
            email: "test@l-p-e.ch".to_string(),
            display_name: "test".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };
        let client_ostid = vec![0x42; 40];
        let mut folder = MapiObject::Folder {
            folder_id: IPM_SUBTREE_FOLDER_ID,
            properties: HashMap::new(),
        };

        apply_mapi_property_values(
            Some(&mut folder),
            vec![(PID_TAG_OST_OSTID, MapiValue::Binary(client_ostid.clone()))],
        )
        .unwrap();
        let row = serialize_object_property(
            Some(&folder),
            &principal,
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
            PID_TAG_OST_OSTID,
        );

        assert_eq!(u16::from_le_bytes(row[0..2].try_into().unwrap()), 40);
        assert_eq!(&row[2..], client_ostid.as_slice());
    }

    #[test]
    pub(in crate::mapi) fn golden_open_folder_rop_round_trips_through_typed_parser() {
        let folder_id = crate::mapi::identity::mapi_store_id(0x1122_3344_5566);
        let mut golden = vec![0x02, 0x00, 0x00, 0x01];
        golden.extend_from_slice(
            &crate::mapi::identity::wire_id_bytes_from_object_id(folder_id).unwrap(),
        );
        golden.push(0x00);

        let mut cursor = Cursor::new(&golden);
        let request = read_rop_request(&mut cursor).unwrap();

        assert_eq!(
            request.typed(),
            TypedRopRequest::OpenFolder(RopOpenFolderRequest {
                input_handle_index: 0,
                output_handle_index: 1,
                folder_id,
                open_mode_flags: 0,
            })
        );
        assert_eq!(serialize_rop_request(&request).unwrap(), golden);
        assert_eq!(cursor.remaining(), 0);
    }

    #[test]
    pub(in crate::mapi) fn golden_set_columns_rop_round_trips_through_typed_parser() {
        let golden = vec![
            0x12, 0x00, 0x02, 0x00, 0x02, 0x00, 0x1F, 0x00, 0x37, 0x00, 0x03, 0x00, 0x0E, 0x0C,
        ];

        let mut cursor = Cursor::new(&golden);
        let request = read_rop_request(&mut cursor).unwrap();

        assert_eq!(
            request.typed(),
            TypedRopRequest::SetColumns(RopSetColumnsRequest {
                input_handle_index: 2,
                flags: 0,
                property_tags: vec![0x0037_001F, 0x0C0E_0003],
            })
        );
        assert_eq!(serialize_rop_request(&request).unwrap(), golden);
        assert_eq!(cursor.remaining(), 0);
    }

    #[test]
    fn microsoft_oxctabl_get_contents_table_example_round_trips_through_typed_parser() {
        let golden = vec![0x05, 0x00, 0x00, 0x01, 0x00];
        let mut cursor = Cursor::new(&golden);
        let request = read_rop_request(&mut cursor).unwrap();

        assert_eq!(
            request.typed(),
            TypedRopRequest::OpenTable(RopOpenTableRequest {
                rop_id: RopId::GetContentsTable.as_u8(),
                input_handle_index: 0,
                output_handle_index: 1,
                table_flags: 0,
            })
        );
        assert_eq!(serialize_rop_request(&request).unwrap(), golden);
        assert_eq!(cursor.remaining(), 0);
    }

    #[test]
    fn microsoft_oxctabl_set_columns_example_round_trips_through_typed_parser() {
        let golden = vec![
            0x12, 0x00, 0x01, 0x00, 0x06, 0x00, 0x14, 0x00, 0x48, 0x67, 0x14, 0x00, 0x4A, 0x67,
            0x14, 0x00, 0x4D, 0x67, 0x03, 0x00, 0x4E, 0x67, 0x1F, 0x00, 0x37, 0x00, 0x40, 0x00,
            0x06, 0x0E,
        ];

        let mut cursor = Cursor::new(&golden);
        let request = read_rop_request(&mut cursor).unwrap();

        assert_eq!(
            request.typed(),
            TypedRopRequest::SetColumns(RopSetColumnsRequest {
                input_handle_index: 1,
                flags: 0,
                property_tags: vec![
                    0x6748_0014,
                    0x674A_0014,
                    0x674D_0014,
                    0x674E_0003,
                    0x0037_001F,
                    0x0E06_0040,
                ],
            })
        );
        assert_eq!(serialize_rop_request(&request).unwrap(), golden);
        assert_eq!(cursor.remaining(), 0);
    }

    #[test]
    fn microsoft_oxctabl_sort_and_query_rows_examples_parse_through_typed_parser() {
        let sort_golden = vec![
            0x13, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x06, 0x0E,
            0x01,
        ];
        let mut sort_cursor = Cursor::new(&sort_golden);
        let sort = read_rop_request(&mut sort_cursor).unwrap();

        assert_eq!(RopId::from_u8(sort.rop_id), Some(RopId::SortTable));
        assert_eq!(sort.input_handle_index, Some(1));
        assert_eq!(sort.sort_orders().len(), 1);
        assert_eq!(sort.sort_orders()[0].property_tag, 0x0E06_0040);
        assert_eq!(sort.sort_orders()[0].order, 1);
        assert_eq!(sort.sort_category_count(), 0);
        assert_eq!(sort_cursor.remaining(), 0);

        let query_golden = vec![0x15, 0x00, 0x01, 0x00, 0x01, 0x32, 0x00];
        let mut query_cursor = Cursor::new(&query_golden);
        let query = read_rop_request(&mut query_cursor).unwrap();

        assert_eq!(
            query.typed(),
            TypedRopRequest::QueryRows(RopQueryRowsRequest {
                input_handle_index: 1,
                flags: 0,
                forward_read: true,
                row_count: 0x32,
            })
        );
        assert_eq!(serialize_rop_request(&query).unwrap(), query_golden);
        assert_eq!(query_cursor.remaining(), 0);
    }

    #[test]
    fn microsoft_oxctabl_category_examples_parse_expected_fields() {
        let sort_golden = vec![
            0x13, 0x00, 0x00, 0x00, 0x02, 0x00, 0x01, 0x00, 0x01, 0x00, 0x1F, 0x30, 0x08, 0x80,
            0x00, 0x40, 0x00, 0x06, 0x0E, 0x01,
        ];
        let mut sort_cursor = Cursor::new(&sort_golden);
        let sort = read_rop_request(&mut sort_cursor).unwrap();

        assert_eq!(RopId::from_u8(sort.rop_id), Some(RopId::SortTable));
        assert_eq!(sort.input_handle_index, Some(0));
        assert_eq!(sort.sort_category_count(), 1);
        assert_eq!(sort.sort_expanded_count(), 1);
        assert_eq!(sort.sort_orders().len(), 2);
        assert_eq!(sort.sort_orders()[0].property_tag, 0x8008_301F);
        assert_eq!(sort.sort_orders()[0].order, 0);
        assert_eq!(sort.sort_orders()[1].property_tag, 0x0E06_0040);
        assert_eq!(sort.sort_orders()[1].order, 1);
        assert_eq!(sort_cursor.remaining(), 0);

        let expand_golden = vec![
            0x59, 0x00, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0xF1, 0x88, 0xBD,
        ];
        let mut expand_cursor = Cursor::new(&expand_golden);
        let expand = read_rop_request(&mut expand_cursor).unwrap();

        assert_eq!(RopId::from_u8(expand.rop_id), Some(RopId::ExpandRow));
        assert_eq!(expand.input_handle_index, Some(1));
        assert_eq!(expand.expand_max_row_count(), 0);
        assert_eq!(expand.category_id(), Some(0xBD88_F100_0000_0001));
        assert_eq!(expand_cursor.remaining(), 0);

        let query_golden = vec![0x15, 0x00, 0x00, 0x00, 0x01, 0x32, 0x00];
        let mut query_cursor = Cursor::new(&query_golden);
        let query = read_rop_request(&mut query_cursor).unwrap();

        assert_eq!(
            query.typed(),
            TypedRopRequest::QueryRows(RopQueryRowsRequest {
                input_handle_index: 0,
                flags: 0,
                forward_read: true,
                row_count: 0x32,
            })
        );
        assert_eq!(serialize_rop_request(&query).unwrap(), query_golden);
        assert_eq!(query_cursor.remaining(), 0);
    }

    #[test]
    pub(in crate::mapi) fn expand_row_payload_never_decodes_as_message_ids() {
        let category_id = crate::mapi::identity::mapi_store_id(0x0102_0304_0506);
        let mut golden = vec![RopId::ExpandRow.as_u8(), 0x00, 0x00];
        golden.extend_from_slice(&1u16.to_le_bytes());
        golden.extend_from_slice(
            &crate::mapi::identity::wire_id_bytes_from_object_id(category_id).unwrap(),
        );

        let mut cursor = Cursor::new(&golden);
        let request = read_rop_request(&mut cursor).unwrap();

        assert_eq!(RopId::from_u8(request.rop_id), Some(RopId::ExpandRow));
        assert_eq!(request.message_ids(), Vec::<u64>::new());
        assert_eq!(cursor.remaining(), 0);
    }

    #[test]
    pub(in crate::mapi) fn sync_import_message_move_uses_length_prefixed_source_ids() {
        let source_folder_id = crate::mapi::identity::mapi_store_id(0x0102_0304_0507);
        let source_message_id = crate::mapi::identity::mapi_store_id(0x0102_0304_0508);
        let destination_message_id = crate::mapi::identity::mapi_store_id(0x0102_0304_0509);
        let change_number = crate::mapi::identity::mapi_store_id(0x0102_0304_0510);
        let source_folder_wire =
            crate::mapi::identity::wire_id_bytes_from_object_id(source_folder_id).unwrap();
        let source_message_wire =
            crate::mapi::identity::wire_id_bytes_from_object_id(source_message_id).unwrap();
        let destination_message_wire =
            crate::mapi::identity::wire_id_bytes_from_object_id(destination_message_id).unwrap();
        let change_number_wire =
            crate::mapi::identity::wire_id_bytes_from_object_id(change_number).unwrap();
        let predecessor_change_list = [0x01, 0x02, 0x03, 0x04];
        let mut golden = vec![RopId::SynchronizationImportMessageMove.as_u8(), 0x00, 0x00];
        for field in [
            source_folder_wire.as_slice(),
            source_message_wire.as_slice(),
            predecessor_change_list.as_slice(),
            destination_message_wire.as_slice(),
            change_number_wire.as_slice(),
        ] {
            golden.extend_from_slice(&(field.len() as u32).to_le_bytes());
            golden.extend_from_slice(field);
        }

        let mut cursor = Cursor::new(&golden);
        let request = read_rop_request(&mut cursor).unwrap();

        assert_eq!(
            RopId::from_u8(request.rop_id),
            Some(RopId::SynchronizationImportMessageMove)
        );
        assert_eq!(
            request.import_move(),
            Some((source_folder_id, source_message_id))
        );
        assert_eq!(cursor.remaining(), 0);

        let mut truncated = Cursor::new(&golden[..golden.len() - 1]);
        assert!(read_rop_request(&mut truncated).is_err());
    }

    #[test]
    pub(in crate::mapi) fn malformed_supported_rop_buffer_fails_without_partial_request() {
        let mut cursor = Cursor::new(&[0x02, 0x00, 0x00, 0x01, 0x88, 0x77]);

        assert!(read_rop_request(&mut cursor).is_err());
    }

    #[test]
    pub(in crate::mapi) fn supported_rop_uses_enum_classification_without_terminal_stop() {
        let mut cursor = Cursor::new(&[0x04, 0x00, 0x01, 0x02, 0x04]);
        let request = read_rop_request(&mut cursor).unwrap();

        assert_eq!(
            RopId::from_u8(request.rop_id),
            Some(RopId::GetHierarchyTable)
        );
        assert_eq!(request.typed().rop_id(), RopId::GetHierarchyTable.as_u8());
        assert!(!request.typed().unsupported_is_terminal());
        assert_eq!(request.input_handle_index(), Some(1));
        assert_eq!(request.output_handle_index, Some(2));
        assert_eq!(cursor.remaining(), 0);
    }

    #[test]
    pub(in crate::mapi) fn unsupported_rop_is_terminal_without_consuming_later_rop_bytes() {
        let mut cursor = Cursor::new(&[0xAA, 0x00, 0x03, 0x01, 0x00, 0x00]);
        let request = read_rop_request(&mut cursor).unwrap();

        assert_eq!(RopId::from_u8(request.rop_id), None);
        assert!(request.typed().unsupported_is_terminal());
        assert_eq!(request.input_handle_index(), Some(3));
        assert_eq!(cursor.remaining(), 3);
        assert!(serialize_rop_request(&request).is_err());
        assert_eq!(
            unsupported_rop_response(0xAA, request.response_handle_index()),
            vec![0xAA, 0x03, 0x02, 0x01, 0x04, 0x80]
        );
    }

    #[test]
    pub(in crate::mapi) fn malformed_handle_table_is_rejected() {
        assert!(read_handle_table(&[0x01, 0x02, 0x03]).is_err());
        assert_eq!(
            read_handle_table(&[0x6E, 0x00, 0x00, 0x00]).unwrap(),
            vec![0x6E]
        );
    }

    #[test]
    pub(in crate::mapi) fn invalid_input_handle_index_serializes_common_rop_error() {
        let request = RopRequest {
            rop_id: 0x04,
            input_handle_index: Some(7),
            output_handle_index: Some(1),
            payload: vec![0],
        };
        let handles = read_handle_table(&[0x6E, 0x00, 0x00, 0x00]).unwrap();

        assert_eq!(input_handle(&handles, &request), None);
        assert_eq!(
            rop_handle_index_error_response(&request),
            vec![0x04, 0x07, 0x0F, 0x01, 0x04, 0x80]
        );
    }

    #[test]
    pub(in crate::mapi) fn upload_state_success_response_uses_input_handle_index() {
        for rop_id in [0x75, 0x76, 0x77] {
            let request = RopRequest {
                rop_id,
                input_handle_index: Some(3),
                output_handle_index: Some(9),
                payload: Vec::new(),
            };

            assert_eq!(
                rop_upload_state_success_response(&request),
                vec![rop_id, 3, 0, 0, 0, 0]
            );
        }
    }

    #[test]
    pub(in crate::mapi) fn note_and_journal_message_handles_serialize_object_properties() {
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
                body_text: "Remember Outlook open-message reads".to_string(),
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
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id: Uuid::nil(),
            email: "test@example.test".to_string(),
            display_name: "Test".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };

        let note_object = MapiObject::Note {
            folder_id: NOTES_FOLDER_ID,
            note_id: crate::mapi::identity::mapi_store_id(90),
        };
        let journal_object = MapiObject::JournalEntry {
            folder_id: JOURNAL_FOLDER_ID,
            journal_entry_id: crate::mapi::identity::mapi_store_id(91),
        };
        let notes = snapshot.notes_for_folder(NOTES_FOLDER_ID);
        let journal_entries = snapshot.journal_entries_for_folder(JOURNAL_FOLDER_ID);

        assert_eq!(
            serialize_object_property(
                Some(&note_object),
                &principal,
                &[],
                &[],
                &snapshot,
                PID_TAG_MESSAGE_CLASS_W,
            ),
            serialize_note_row(
                &notes[0].note,
                crate::mapi::identity::mapi_store_id(90),
                NOTES_FOLDER_ID,
                &[PID_TAG_MESSAGE_CLASS_W],
            )
        );
        assert_eq!(
            serialize_object_property(
                Some(&journal_object),
                &principal,
                &[],
                &[],
                &snapshot,
                PID_TAG_SUBJECT_W,
            ),
            serialize_journal_entry_row(
                &journal_entries[0].entry,
                crate::mapi::identity::mapi_store_id(91),
                JOURNAL_FOLDER_ID,
                &[PID_TAG_SUBJECT_W],
            )
        );
    }

    #[test]
    pub(in crate::mapi) fn reserved_rop_is_terminal_and_uses_common_unsupported_response() {
        let mut cursor = Cursor::new(&[0x28, 0x00, 0x03, 0xAA]);
        let request = read_rop_request(&mut cursor).unwrap();

        assert!(request.typed().unsupported_is_terminal());
        assert_eq!(request.input_handle_index(), Some(3));
        assert_eq!(cursor.remaining(), 0);
        assert!(serialize_rop_request(&request).is_err());
        assert_eq!(
            unsupported_rop_response(0x28, request.response_handle_index()),
            vec![0x28, 0x03, 0x02, 0x01, 0x04, 0x80]
        );
    }
}
