use super::properties::*;
use super::session::*;
use super::sync::*;
use super::tables::*;
use super::*;

pub(in crate::mapi) fn split_rop_buffer(buffer: &[u8]) -> Option<(&[u8], &[u8])> {
    if let Some(payload) = rpc_header_ext_payload(buffer) {
        return split_rop_payload_spec(payload);
    }
    split_rop_payload_legacy(buffer)
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
        write_u64(&mut response, folder_id);
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

pub(in crate::mapi) fn rop_open_folder_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x02, request.output_handle_index.unwrap_or(0)];
    write_u32(&mut response, 0);
    response.push(0);
    response.push(0);
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
    response.extend_from_slice(&(recipient_count as u16).to_le_bytes());
    response.extend_from_slice(&0u16.to_le_bytes());
    response.push(0);
    response
}

pub(in crate::mapi) fn rop_reload_cached_information_response(
    request: &RopRequest,
    object: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
) -> Vec<u8> {
    let (subject, recipient_count) = match object {
        Some(MapiObject::Message {
            folder_id,
            message_id,
        }) => match message_for_id(*folder_id, *message_id, mailboxes, emails) {
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
        _ => return rop_error_response(0x10, request.response_handle_index(), 0x0000_04B9),
    };

    let mut response = vec![0x10, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(0);
    write_typed_string(&mut response, "");
    write_typed_string(&mut response, &subject);
    response.extend_from_slice(&(recipient_count as u16).to_le_bytes());
    response.extend_from_slice(&0u16.to_le_bytes());
    response.push(0);
    response
}

pub(in crate::mapi) fn rop_message_status_response(
    request: &RopRequest,
    old_status: u32,
) -> Vec<u8> {
    let mut response = vec![0x20, request.response_handle_index()];
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
    write_u64(&mut response, folder_id);
    response.push(existing as u8);
    response.push(0);
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

pub(in crate::mapi) fn rop_get_attachment_table_response(
    request: &RopRequest,
    row_count: u32,
) -> Vec<u8> {
    let mut response = vec![0x21, request.output_handle_index.unwrap_or(0)];
    write_u32(&mut response, 0);
    write_u32(&mut response, row_count);
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
    let Some(object_id) = request.object_id() else {
        return rop_error_response(0x43, request.response_handle_index(), 0x8004_0102);
    };
    let Some(long_term_id) = long_term_id_from_object_id(object_id) else {
        return rop_error_response(0x43, request.response_handle_index(), 0x8004_010F);
    };
    let mut response = vec![0x43, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.extend_from_slice(&long_term_id);
    response
}

pub(in crate::mapi) fn rop_id_from_long_term_id_response(request: &RopRequest) -> Vec<u8> {
    let Some(long_term_id) = request.long_term_id() else {
        return rop_error_response(0x44, request.response_handle_index(), 0x8004_0102);
    };
    let Some(object_id) = object_id_from_long_term_id(long_term_id) else {
        return rop_error_response(0x44, request.response_handle_index(), 0x8004_010F);
    };
    let mut response = vec![0x44, request.response_handle_index()];
    write_u32(&mut response, 0);
    write_u64(&mut response, object_id);
    response
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
    write_u16(&mut response, 0);
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

pub(in crate::mapi) fn rop_save_changes_message_response(
    request: &RopRequest,
    message_id: u64,
) -> Vec<u8> {
    let mut response = vec![0x0C, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(request.input_handle_index().unwrap_or(0));
    write_u64(&mut response, message_id);
    response
}

pub(in crate::mapi) fn rop_get_properties_list_response(
    request: &RopRequest,
    object: Option<&MapiObject>,
) -> Vec<u8> {
    let tags = match object {
        Some(
            MapiObject::Attachment { .. }
            | MapiObject::PendingAttachment { .. }
            | MapiObject::SavedAttachment { .. },
        ) => default_attachment_columns(),
        Some(MapiObject::Contact { .. }) | Some(MapiObject::PendingContact { .. }) => {
            default_contact_property_tags()
        }
        Some(MapiObject::Event { .. }) | Some(MapiObject::PendingEvent { .. }) => {
            default_event_property_tags()
        }
        Some(MapiObject::Message { .. }) | Some(MapiObject::PendingMessage { .. }) => {
            default_message_property_tags()
        }
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

pub(in crate::mapi) fn rop_get_properties_specific_response(
    request: &RopRequest,
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u8> {
    let mut response = vec![0x07, request.input_handle_index().unwrap_or(0)];
    write_u32(&mut response, 0);
    let columns = request.property_tags();
    let row = match object {
        Some(MapiObject::Logon) => serialize_logon_row(&columns),
        Some(MapiObject::Message {
            folder_id,
            message_id,
        }) => {
            let Some(email) = message_for_id(*folder_id, *message_id, mailboxes, emails) else {
                return rop_error_response(
                    0x07,
                    request.input_handle_index().unwrap_or(0),
                    0x8004_010F,
                );
            };
            serialize_message_row(email, &columns)
        }
        Some(MapiObject::PendingMessage { properties, .. }) => {
            serialize_pending_message_row(principal, properties, &columns)
        }
        Some(MapiObject::Contact {
            folder_id,
            contact_id,
        }) => {
            let Some(contact) = snapshot.contact_for_id(*folder_id, *contact_id) else {
                return rop_error_response(
                    0x07,
                    request.input_handle_index().unwrap_or(0),
                    0x8004_010F,
                );
            };
            serialize_contact_row(&contact.contact, contact.id, contact.folder_id, &columns)
        }
        Some(MapiObject::PendingContact { properties, .. }) => {
            serialize_pending_contact_row(principal, properties, &columns)
        }
        Some(MapiObject::Event {
            folder_id,
            event_id,
        }) => {
            let Some(event) = snapshot.event_for_id(*folder_id, *event_id) else {
                return rop_error_response(
                    0x07,
                    request.input_handle_index().unwrap_or(0),
                    0x8004_010F,
                );
            };
            serialize_event_row(&event.event, event.id, event.folder_id, &columns)
        }
        Some(MapiObject::PendingEvent { properties, .. }) => {
            serialize_pending_event_row(principal, properties, &columns)
        }
        Some(MapiObject::Folder {
            folder_id,
            properties,
        }) => serialize_session_folder_row(*folder_id, properties, mailboxes, snapshot, &columns),
        Some(MapiObject::Attachment {
            folder_id,
            message_id,
            attach_num,
        }) => {
            let Some(attachment) =
                snapshot.attachment_for_message(*folder_id, *message_id, *attach_num)
            else {
                return rop_error_response(
                    0x07,
                    request.input_handle_index().unwrap_or(0),
                    0x8004_010F,
                );
            };
            serialize_attachment_row(attachment, &columns)
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
            size_octets,
            ..
        }) => serialize_saved_attachment_row(
            *attach_num,
            file_reference,
            file_name,
            media_type,
            *size_octets,
            &columns,
        ),
        _ => {
            let folder_id = object
                .and_then(MapiObject::folder_id)
                .unwrap_or(ROOT_FOLDER_ID);
            folder_row_for_id(folder_id, mailboxes)
                .map(|mailbox| serialize_folder_row(mailbox, &columns))
                .or_else(|| {
                    snapshot
                        .collaboration_folder_for_id(folder_id)
                        .map(|folder| serialize_collaboration_folder_row(folder, &columns))
                })
                .unwrap_or_else(|| serialize_special_folder_row(folder_id, mailboxes, &columns))
        }
    };
    write_standard_property_row(&mut response, &row);
    response
}

pub(in crate::mapi) fn rop_get_properties_all_response(
    request: &RopRequest,
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u8> {
    let mut response = vec![0x08, request.input_handle_index().unwrap_or(0)];
    write_u32(&mut response, 0);
    let tags = match object {
        Some(
            MapiObject::Attachment { .. }
            | MapiObject::PendingAttachment { .. }
            | MapiObject::SavedAttachment { .. },
        ) => default_attachment_columns(),
        Some(MapiObject::Message { .. }) | Some(MapiObject::PendingMessage { .. }) => {
            default_message_property_tags()
        }
        _ => default_folder_property_tags(),
    };
    response.extend_from_slice(&(tags.len() as u16).to_le_bytes());
    for tag in tags {
        write_u32(&mut response, tag);
        let value = serialize_object_property(object, principal, mailboxes, emails, snapshot, tag);
        response.extend_from_slice(&value);
    }
    response
}

pub(in crate::mapi) fn rop_get_valid_attachments_response(
    request: &RopRequest,
    object: Option<&MapiObject>,
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u8> {
    let Some(MapiObject::Message {
        folder_id,
        message_id,
    }) = object
    else {
        return rop_error_response(0x52, request.response_handle_index(), 0x0000_04B9);
    };
    let attachments = snapshot
        .attachments_for_message(*folder_id, *message_id)
        .unwrap_or_default();
    let mut response = vec![0x52, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.extend_from_slice(&(attachments.len().min(u16::MAX as usize) as u16).to_le_bytes());
    for attachment in attachments.iter().take(u16::MAX as usize) {
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
        Some(MapiObject::Logon) => serialize_logon_row(&[tag]),
        Some(MapiObject::Message {
            folder_id,
            message_id,
        }) => message_for_id(*folder_id, *message_id, mailboxes, emails)
            .map(|email| serialize_message_row(email, &[tag]))
            .unwrap_or_else(|| {
                let mut value = Vec::new();
                write_property_default(&mut value, tag);
                value
            }),
        Some(MapiObject::PendingMessage { properties, .. }) => {
            serialize_pending_message_row(principal, properties, &[tag])
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
            .map(|event| serialize_event_row(&event.event, event.id, event.folder_id, &[tag]))
            .unwrap_or_else(|| {
                let mut value = Vec::new();
                write_property_default(&mut value, tag);
                value
            }),
        Some(MapiObject::PendingEvent { properties, .. }) => {
            serialize_pending_event_row(principal, properties, &[tag])
        }
        Some(MapiObject::Folder {
            folder_id,
            properties,
        }) => serialize_session_folder_row(*folder_id, properties, mailboxes, snapshot, &[tag]),
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
            size_octets,
            ..
        }) => serialize_saved_attachment_row(
            *attach_num,
            file_reference,
            file_name,
            media_type,
            *size_octets,
            &[tag],
        ),
        _ => {
            let folder_id = object
                .and_then(MapiObject::folder_id)
                .unwrap_or(ROOT_FOLDER_ID);
            folder_row_for_id(folder_id, mailboxes)
                .map(|mailbox| serialize_folder_row(mailbox, &[tag]))
                .or_else(|| {
                    snapshot
                        .collaboration_folder_for_id(folder_id)
                        .map(|folder| serialize_collaboration_folder_row(folder, &[tag]))
                })
                .unwrap_or_else(|| serialize_special_folder_row(folder_id, mailboxes, &[tag]))
        }
    }
}

fn serialize_session_folder_row(
    folder_id: u64,
    properties: &HashMap<u32, MapiValue>,
    mailboxes: &[JmapMailbox],
    snapshot: &MapiMailStoreSnapshot,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        let storage_tag = canonical_property_storage_tag(*column);
        if let Some(value) = properties
            .get(&storage_tag)
            .or_else(|| properties.get(column))
        {
            write_mapi_value(&mut row, *column, value);
            continue;
        }

        let value = folder_row_for_id(folder_id, mailboxes)
            .map(|mailbox| serialize_folder_row(mailbox, &[*column]))
            .or_else(|| {
                snapshot
                    .collaboration_folder_for_id(folder_id)
                    .map(|folder| serialize_collaboration_folder_row(folder, &[*column]))
            })
            .unwrap_or_else(|| serialize_special_folder_row(folder_id, mailboxes, &[*column]));
        row.extend_from_slice(&value);
    }
    row
}

pub(in crate::mapi) fn rop_get_receive_folder_response(
    request: &RopRequest,
    explicit_message_class: &str,
) -> Vec<u8> {
    let mut response = vec![0x27, request.response_handle_index()];
    write_u32(&mut response, 0);
    write_u64(&mut response, INBOX_FOLDER_ID);
    response.extend_from_slice(explicit_message_class.as_bytes());
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

pub(in crate::mapi) fn explicit_receive_folder_message_class(message_class: &str) -> &'static str {
    if message_class.eq_ignore_ascii_case("IPM.Note")
        || message_class
            .get(..9)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("IPM.Note."))
    {
        "IPM.Note"
    } else {
        ""
    }
}

pub(in crate::mapi) fn rop_get_receive_folder_table_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x68, request.response_handle_index()];
    write_u32(&mut response, 0);
    write_u32(&mut response, 1);
    response.push(0);
    write_u64(&mut response, INBOX_FOLDER_ID);
    write_utf16z(&mut response, "IPM.Note");
    write_u64(&mut response, 0);
    response
}

pub(in crate::mapi) fn rop_get_transport_folder_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x6D, request.input_handle_index().unwrap_or(0)];
    write_u32(&mut response, 0);
    write_u64(&mut response, OUTBOX_FOLDER_ID);
    response
}

pub(in crate::mapi) fn rop_get_store_state_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x7B, request.response_handle_index()];
    write_u32(&mut response, 0);
    write_u32(&mut response, 0);
    response
}

pub(in crate::mapi) fn rop_public_folder_is_ghosted_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x45, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(0);
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

pub(in crate::mapi) const ROP_ERROR_NOT_SUPPORTED: u32 = 0x8004_0102;
pub(in crate::mapi) const ROP_ERROR_NOT_FOUND: u32 = 0x8004_010F;

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
    rop_error_response(rop_id, handle_index, ROP_ERROR_NOT_SUPPORTED)
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
        match self.rop_id {
            0x01 => TypedRopRequest::Release(RopInputOnlyRequest {
                rop_id: self.rop_id,
                input_handle_index: self.input_handle_index.unwrap_or(0),
            }),
            0x02 => TypedRopRequest::OpenFolder(RopOpenFolderRequest {
                input_handle_index: self.input_handle_index.unwrap_or(0),
                output_handle_index: self.output_handle_index.unwrap_or(0),
                folder_id: self.folder_id().unwrap_or(0),
                open_mode_flags: self.payload.get(8).copied().unwrap_or(0),
            }),
            0x03 => TypedRopRequest::OpenMessage(RopOpenMessageRequest {
                input_handle_index: self.input_handle_index.unwrap_or(0),
                output_handle_index: self.output_handle_index.unwrap_or(0),
                folder_id: self.folder_id().unwrap_or(0),
                open_mode_flags: self.payload.get(8).copied().unwrap_or(0),
                message_id: self.message_id().unwrap_or(0),
            }),
            0x04 | 0x05 | 0x21 => TypedRopRequest::OpenTable(RopOpenTableRequest {
                rop_id: self.rop_id,
                input_handle_index: self.input_handle_index.unwrap_or(0),
                output_handle_index: self.output_handle_index.unwrap_or(0),
                table_flags: self.payload.first().copied().unwrap_or(0),
            }),
            0x06 => TypedRopRequest::CreateMessage(RopCreateMessageRequest {
                input_handle_index: self.input_handle_index.unwrap_or(0),
                output_handle_index: self.output_handle_index.unwrap_or(0),
                folder_id: self.folder_id().unwrap_or(0),
                associated_flag: self.payload.get(8).copied().unwrap_or(0),
            }),
            0x0C => TypedRopRequest::SaveChangesMessage(RopSaveChangesMessageRequest {
                response_handle_index: self.output_handle_index.unwrap_or(0),
                input_handle_index: self.input_handle_index.unwrap_or(0),
                save_flags: self.payload.first().copied().unwrap_or(0),
            }),
            0x12 => TypedRopRequest::SetColumns(RopSetColumnsRequest {
                input_handle_index: self.input_handle_index.unwrap_or(0),
                flags: self.payload.first().copied().unwrap_or(0),
                property_tags: self.property_tags(),
            }),
            0x14 | 0x4F => {
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
            0x15 => TypedRopRequest::QueryRows(RopQueryRowsRequest {
                input_handle_index: self.input_handle_index.unwrap_or(0),
                flags: self.payload.first().copied().unwrap_or(0),
                forward_read: self.query_forward_read(),
                row_count: self.query_row_count().unwrap_or(0).min(u16::MAX as usize) as u16,
            }),
            0xFE => {
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
            0x41 if self.modify_rules_count().unwrap_or(0) != 0 => {
                TypedRopRequest::Unsupported(RopUnsupportedRequest {
                    rop_id: self.rop_id,
                    input_handle_index: self.input_handle_index,
                    reserved: false,
                })
            }
            rop_id if rop_id_is_supported_by_dispatch(rop_id) => {
                TypedRopRequest::SupportedRaw(RopSupportedRawRequest {
                    rop_id,
                    input_handle_index: self.input_handle_index,
                    output_handle_index: self.output_handle_index,
                })
            }
            rop_id => TypedRopRequest::Unsupported(RopUnsupportedRequest {
                rop_id,
                input_handle_index: self.input_handle_index,
                reserved: rop_id_is_reserved(rop_id),
            }),
        }
    }

    pub(in crate::mapi) fn input_handle_index(&self) -> Option<u8> {
        self.input_handle_index
    }

    pub(in crate::mapi) fn response_handle_index(&self) -> u8 {
        if matches!(
            self.rop_id,
            0x0C | 0x11
                | 0x25
                | 0x29
                | 0x3B
                | 0x3E
                | 0x3F
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
        let bytes = self.payload.get(..8)?;
        Some(u64::from_le_bytes(bytes.try_into().ok()?))
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
        Some(u64::from_le_bytes(bytes.try_into().ok()?))
    }

    pub(in crate::mapi) fn message_id(&self) -> Option<u64> {
        let bytes = self.payload.get(9..17)?;
        Some(u64::from_le_bytes(bytes.try_into().ok()?))
    }

    pub(in crate::mapi) fn row_id(&self) -> Option<u32> {
        let bytes = self.payload.get(..4)?;
        Some(u32::from_le_bytes(bytes.try_into().ok()?))
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

    pub(in crate::mapi) fn sync_type(&self) -> u8 {
        self.payload.first().copied().unwrap_or(0)
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

    pub(in crate::mapi) fn import_message_id(&self) -> Option<u64> {
        let bytes = self.payload.get(..8)?;
        Some(u64::from_le_bytes(bytes.try_into().ok()?))
    }

    pub(in crate::mapi) fn import_property_values(&self) -> Result<Vec<(u32, MapiValue)>> {
        let property_payload = self
            .payload
            .get(8..)
            .ok_or_else(|| anyhow!("missing import property payload"))?;
        RopRequest {
            rop_id: 0x0A,
            input_handle_index: self.input_handle_index,
            output_handle_index: self.output_handle_index,
            payload: property_payload.to_vec(),
        }
        .property_values()
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
            .map(|bytes| u64::from_le_bytes(bytes.try_into().unwrap_or_default()))
            .collect()
    }

    pub(in crate::mapi) fn import_delete_hard_delete(&self) -> bool {
        self.payload.first().is_some_and(|flags| flags & 0x02 != 0)
    }

    pub(in crate::mapi) fn fast_transfer_message_ids(&self) -> Vec<u64> {
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
            .filter_map(|bytes| bytes.try_into().ok().map(u64::from_le_bytes))
            .collect()
    }

    pub(in crate::mapi) fn import_move(&self) -> Option<(u64, u64)> {
        let message_id = u64::from_le_bytes(self.payload.get(..8)?.try_into().ok()?);
        let target_folder_id = u64::from_le_bytes(self.payload.get(8..16)?.try_into().ok()?);
        Some((message_id, target_folder_id))
    }

    pub(in crate::mapi) fn import_read_state_changes(&self) -> Vec<(u64, bool)> {
        let count = self
            .payload
            .get(..2)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .unwrap_or(0) as usize;
        self.payload
            .get(2..)
            .unwrap_or_default()
            .chunks_exact(9)
            .take(count)
            .map(|bytes| {
                let message_id = u64::from_le_bytes(bytes[0..8].try_into().unwrap_or_default());
                let unread = bytes[8] == 0;
                (message_id, unread)
            })
            .collect()
    }

    pub(in crate::mapi) fn local_replica_midset_deleted(&self) -> &[u8] {
        self.payload.as_slice()
    }

    pub(in crate::mapi) fn receive_folder_message_class(&self) -> Option<&str> {
        let bytes = self.payload.strip_suffix(&[0])?;
        std::str::from_utf8(bytes).ok()
    }

    pub(in crate::mapi) fn local_replica_id_count(&self) -> u32 {
        self.payload
            .get(..4)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u32::from_le_bytes)
            .unwrap_or(1)
    }

    pub(in crate::mapi) fn object_id(&self) -> Option<u64> {
        let bytes = self.payload.get(..8)?;
        Some(u64::from_le_bytes(bytes.try_into().ok()?))
    }

    pub(in crate::mapi) fn long_term_id(&self) -> Option<&[u8]> {
        self.payload.get(..24)
    }

    pub(in crate::mapi) fn message_ids(&self) -> Vec<u64> {
        let Some(count_bytes) = self.payload.get(2..4) else {
            return Vec::new();
        };
        let count = u16::from_le_bytes([count_bytes[0], count_bytes[1]]) as usize;
        self.payload[4..]
            .chunks_exact(8)
            .take(count)
            .map(|bytes| u64::from_le_bytes(bytes.try_into().unwrap_or_default()))
            .collect()
    }

    pub(in crate::mapi) fn status_message_id(&self) -> Option<u64> {
        let bytes = self.payload.get(..8)?;
        Some(u64::from_le_bytes(bytes.try_into().ok()?))
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

    pub(in crate::mapi) fn create_folder_type(&self) -> u8 {
        self.payload.first().copied().unwrap_or(0)
    }

    pub(in crate::mapi) fn create_folder_open_existing(&self) -> bool {
        self.payload
            .get(2)
            .is_some_and(|open_existing| *open_existing != 0)
    }

    pub(in crate::mapi) fn create_folder_display_name(&self) -> String {
        read_u16_prefixed_string(&self.payload, 3).unwrap_or_default()
    }

    pub(in crate::mapi) fn delete_folder_id(&self) -> Option<u64> {
        let bytes = self.payload.get(1..9)?;
        Some(u64::from_le_bytes(bytes.try_into().ok()?))
    }

    pub(in crate::mapi) fn move_copy_message_ids(&self) -> Vec<u64> {
        let Some(count_bytes) = self.payload.get(..2) else {
            return Vec::new();
        };
        let count = u16::from_le_bytes([count_bytes[0], count_bytes[1]]) as usize;
        self.payload[2..]
            .chunks_exact(8)
            .take(count)
            .map(|bytes| u64::from_le_bytes(bytes.try_into().unwrap_or_default()))
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

    pub(in crate::mapi) fn move_copy_target_handle(&self, input_handles: &[u32]) -> Option<u32> {
        input_handles
            .get(self.output_handle_index? as usize)
            .copied()
            .filter(|handle| *handle != u32::MAX)
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

    pub(in crate::mapi) fn property_tags(&self) -> Vec<u32> {
        let start = match self.rop_id {
            0x07 => 4,
            0x0B | 0x7A => 2,
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

    pub(in crate::mapi) fn modify_recipients(&self) -> Result<Vec<PendingRecipientChange>> {
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
    let address =
        optional_mapi_value_text(&values, &[PID_TAG_SMTP_ADDRESS_W, PID_TAG_EMAIL_ADDRESS_W])
            .ok_or_else(|| anyhow!("recipient address is required"))?;
    let display_name = optional_mapi_value_text(&values, &[PID_TAG_DISPLAY_NAME_W])
        .filter(|value| !value.eq_ignore_ascii_case(&address));

    Ok(PendingRecipient {
        row_id,
        recipient_type,
        address,
        display_name,
    })
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
    parse_mapi_restriction_from(&mut cursor)
}

pub(in crate::mapi) fn parse_mapi_restriction_from(
    cursor: &mut Cursor<'_>,
) -> Result<MapiRestriction> {
    match cursor.read_u8()? {
        0x00 => {
            let count = cursor.read_u16()? as usize;
            let mut children = Vec::with_capacity(count);
            for _ in 0..count {
                children.push(parse_mapi_restriction_from(cursor)?);
            }
            Ok(MapiRestriction::And(children))
        }
        0x01 => {
            let count = cursor.read_u16()? as usize;
            let mut children = Vec::with_capacity(count);
            for _ in 0..count {
                children.push(parse_mapi_restriction_from(cursor)?);
            }
            Ok(MapiRestriction::Or(children))
        }
        0x02 => Ok(MapiRestriction::Not(Box::new(parse_mapi_restriction_from(
            cursor,
        )?))),
        0x03 => {
            let _fuzzy_level = cursor.read_u32()?;
            let property_tag = cursor.read_u32()?;
            let value = parse_tagged_property_value(cursor)?
                .into_text()
                .ok_or_else(|| anyhow!("content restriction requires a text value"))?;
            Ok(MapiRestriction::Content {
                property_tag,
                value,
            })
        }
        0x04 => {
            let relop = cursor.read_u8()?;
            let property_tag = cursor.read_u32()?;
            let value = parse_tagged_property_value(cursor)?;
            Ok(MapiRestriction::Property {
                relop,
                property_tag,
                value,
            })
        }
        0x06 => {
            let rel_bmr = cursor.read_u8()?;
            let property_tag = cursor.read_u32()?;
            let mask = cursor.read_u32()?;
            Ok(MapiRestriction::Bitmask {
                property_tag,
                mask,
                must_be_nonzero: rel_bmr != 0,
            })
        }
        0x07 => {
            let relop = cursor.read_u8()?;
            let property_tag = cursor.read_u32()?;
            let size = cursor.read_u32()?;
            Ok(MapiRestriction::Size {
                relop,
                property_tag,
                size,
            })
        }
        0x08 => {
            let property_tag = cursor.read_u32()?;
            Ok(MapiRestriction::Exist { property_tag })
        }
        _ => Err(anyhow!("unsupported MAPI restriction type")),
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
    matches!(
        rop_id,
        0x00 | 0x28 | 0x3C | 0x3D | 0x62 | 0x65 | 0x6A | 0x71
    )
}

pub(in crate::mapi) fn rop_id_is_supported_by_dispatch(rop_id: u8) -> bool {
    matches!(
        rop_id,
        0x01 | 0x02
            | 0x03
            | 0x04
            | 0x05
            | 0x06
            | 0x07
            | 0x08
            | 0x09
            | 0x0A
            | 0x0B
            | 0x0C
            | 0x0D
            | 0x0E
            | 0x0F
            | 0x10
            | 0x11
            | 0x12
            | 0x13
            | 0x14
            | 0x15
            | 0x16
            | 0x17
            | 0x18
            | 0x19
            | 0x1A
            | 0x1B
            | 0x1C
            | 0x1D
            | 0x1E
            | 0x1F
            | 0x20
            | 0x21
            | 0x22
            | 0x23
            | 0x24
            | 0x25
            | 0x26
            | 0x27
            | 0x29
            | 0x2B
            | 0x2C
            | 0x2D
            | 0x2E
            | 0x2F
            | 0x30
            | 0x31
            | 0x32
            | 0x33
            | 0x34
            | 0x35
            | 0x36
            | 0x37
            | 0x38
            | 0x3A
            | 0x3B
            | 0x3E
            | 0x3F
            | 0x40
            | 0x41
            | 0x42
            | 0x43
            | 0x44
            | 0x45
            | 0x47
            | 0x48
            | 0x49
            | 0x4A
            | 0x4B
            | 0x4C
            | 0x4D
            | 0x4E
            | 0x4F
            | 0x50
            | 0x51
            | 0x52
            | 0x53
            | 0x54
            | 0x55
            | 0x56
            | 0x57
            | 0x58
            | 0x59
            | 0x5A
            | 0x5B
            | 0x5C
            | 0x5D
            | 0x5E
            | 0x5F
            | 0x60
            | 0x61
            | 0x63
            | 0x64
            | 0x66
            | 0x68
            | 0x69
            | 0x6B
            | 0x6C
            | 0x6D
            | 0x6F
            | 0x70
            | 0x72
            | 0x73
            | 0x74
            | 0x75
            | 0x76
            | 0x77
            | 0x78
            | 0x79
            | 0x7A
            | 0x7B
            | 0x7E
            | 0x7F
            | 0x80
            | 0x81
            | 0x82
            | 0x86
            | 0x89
            | 0x90
            | 0x91
            | 0x92
            | 0x93
            | 0xA3
            | 0xFE
    )
}

pub(in crate::mapi) fn read_rop_request(cursor: &mut Cursor<'_>) -> Result<RopRequest> {
    let rop_id = cursor.read_u8()?;
    let _logon_id = cursor.read_u8()?;
    match rop_id {
        0x01 => {
            let input_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: Vec::new(),
            })
        }
        0x02 => {
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
        0x03 => {
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
        0x04 | 0x05 | 0x21 => {
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
        0x06 => {
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
        0x22 => {
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
        0x23 => {
            let input_handle_index = cursor.read_u8()?;
            let output_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: Some(output_handle_index),
                payload: Vec::new(),
            })
        }
        0x24 => {
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
        0x25 => {
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
        0x08 => {
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
        0x2B => {
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
        0x2C => {
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
        0x2D | 0x90 | 0xA3 => {
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
        0x2E => {
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
        0x2F => {
            let input_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: cursor.read_bytes(8)?.to_vec(),
            })
        }
        0x3A => {
            let source_handle_index = cursor.read_u8()?;
            let dest_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(source_handle_index),
                output_handle_index: Some(dest_handle_index),
                payload: cursor.read_bytes(8)?.to_vec(),
            })
        }
        0x3B => {
            let input_handle_index = cursor.read_u8()?;
            let output_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: Some(output_handle_index),
                payload: Vec::new(),
            })
        }
        0x29 => {
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
        0x30 => {
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
        0x31 => {
            let input_handle_index = cursor.read_u8()?;
            let payload = vec![cursor.read_u8()?, cursor.read_u8()?, cursor.read_u8()?];
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        0x38 => {
            let input_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: Vec::new(),
            })
        }
        0x3E | 0x3F => {
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
        0x40 => {
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
        0x41 => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = vec![cursor.read_u8()?];
            let rules_count = cursor.read_u16()? as usize;
            payload.extend_from_slice(&(rules_count as u16).to_le_bytes());
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        0x42 | 0x43 | 0x45 => {
            let input_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: cursor.read_bytes(8)?.to_vec(),
            })
        }
        0x44 => {
            let input_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: cursor.read_bytes(24)?.to_vec(),
            })
        }
        0x50 => {
            let input_handle_index = cursor.read_u8()?;
            let payload = vec![cursor.read_u8()?];
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        0x58 | 0x92 => {
            let input_handle_index = cursor.read_u8()?;
            let payload = vec![cursor.read_u8()?, cursor.read_u8()?];
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        0x59 => {
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
        0x5A => {
            let input_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: cursor.read_bytes(8)?.to_vec(),
            })
        }
        0x5B | 0x5C => {
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
        0x5D | 0x5E => {
            let input_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: Vec::new(),
            })
        }
        0x26 => {
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
        0x60 => {
            let input_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: cursor.read_bytes(16)?.to_vec(),
            })
        }
        0x61 => {
            let input_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: cursor.read_bytes(24)?.to_vec(),
            })
        }
        0x63 => {
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
        0x64 => {
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
        0x66 => {
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
        0x6B => {
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
        0x6C => {
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
        0x70 => {
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
        0x4E => {
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
        0x4B => {
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
        0x4C => {
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
        0x4D | 0x69 => {
            let input_handle_index = cursor.read_u8()?;
            let output_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.push(cursor.read_u8()?);
            if rop_id == 0x4D {
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
        0x53 => {
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
        0x54 => {
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
        0x75 => {
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
        0x76 => {
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
        0x77 => {
            let input_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: Vec::new(),
            })
        }
        0x7E | 0x82 => {
            let input_handle_index = cursor.read_u8()?;
            let output_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: Some(output_handle_index),
                payload: Vec::new(),
            })
        }
        0x72 => {
            let input_handle_index = cursor.read_u8()?;
            let output_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(cursor.read_bytes(8)?);
            let property_value_size = cursor.read_u16()? as usize;
            let property_value_count = cursor.read_u16()?;
            payload.extend_from_slice(&(property_value_size as u16).to_le_bytes());
            payload.extend_from_slice(&property_value_count.to_le_bytes());
            let values_size = property_value_size
                .checked_sub(2)
                .ok_or_else(|| anyhow!("invalid import property value size"))?;
            payload.extend_from_slice(cursor.read_bytes(values_size)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: Some(output_handle_index),
                payload,
            })
        }
        0x73 => {
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
        0x74 => {
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
        0x78 => {
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
        0x80 => {
            let input_handle_index = cursor.read_u8()?;
            let change_count = cursor.read_u16()? as usize;
            let mut payload = Vec::new();
            payload.extend_from_slice(&(change_count as u16).to_le_bytes());
            for _ in 0..change_count {
                payload.extend_from_slice(cursor.read_bytes(8)?);
                payload.push(cursor.read_u8()?);
            }
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        0x93 => {
            let input_handle_index = cursor.read_u8()?;
            let size = cursor.read_u16()? as usize;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: cursor.read_bytes(size)?.to_vec(),
            })
        }
        0x86 => {
            let input_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: cursor.read_bytes(6)?.to_vec(),
            })
        }
        0x7F => {
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
        0x09 | 0x16 | 0x17 | 0x37 | 0x47 | 0x4A | 0x52 | 0x68 | 0x6D | 0x7B | 0x81 => {
            let input_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: Vec::new(),
            })
        }
        0x6F => {
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
        0x10 => {
            let input_handle_index = cursor.read_u8()?;
            let _reserved = cursor.read_u16()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: Vec::new(),
            })
        }
        0x18 => {
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
        0x19 => {
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
        0x1A => {
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
        0x1F => {
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
        0x20 => {
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
        0x1B => {
            let input_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: Vec::new(),
            })
        }
        0x1C => {
            let input_handle_index = cursor.read_u8()?;
            let output_handle_index = cursor.read_u8()?;
            let folder_type = cursor.read_u8()?;
            let use_unicode = cursor.read_u8()? != 0;
            let open_existing = cursor.read_u8()?;
            let _reserved = cursor.read_u8()?;
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
            let mut payload = vec![folder_type, use_unicode as u8, open_existing];
            write_u16_prefixed_bytes(&mut payload, display_name.as_bytes());
            write_u16_prefixed_bytes(&mut payload, comment.as_bytes());
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: Some(output_handle_index),
                payload,
            })
        }
        0x1D => {
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
        0x1E | 0x91 => {
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
        0x0A | 0x79 => {
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
        0x0B | 0x7A => {
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
        0x0F => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(&cursor.read_u32()?.to_le_bytes());
            let _reserved = cursor.read_u16()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        0x0C => {
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
        0x0D => {
            let input_handle_index = cursor.read_u8()?;
            let _reserved = cursor.read_u32()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: Vec::new(),
            })
        }
        0x0E => {
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
        0x11 => {
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
        0x12 => {
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
        0x13 => {
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
        0x14 => {
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
        0x07 => {
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
        0x15 => {
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
        0x27 => {
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
        0x32 => {
            let input_handle_index = cursor.read_u8()?;
            let payload = vec![cursor.read_u8()?];
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        0x34 => {
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
        0x33 => {
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
        0x35 | 0x36 => {
            let source_handle_index = cursor.read_u8()?;
            let dest_handle_index = cursor.read_u8()?;
            let want_asynchronous = cursor.read_u8()?;
            let mut payload = vec![want_asynchronous];
            if rop_id == 0x36 {
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
        0x48 => {
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
        0x51 => {
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
        0x4F => {
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
        0x55 => {
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
        0x57 => {
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
        0x56 => {
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
        0x5F => {
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
        0x89 => {
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
        0xFE => {
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

#[allow(dead_code)]
pub(in crate::mapi) fn serialize_rop_request(request: &RopRequest) -> Result<Vec<u8>> {
    let mut buffer = vec![request.rop_id, 0];
    match request.typed() {
        TypedRopRequest::Release(request) => buffer.push(request.input_handle_index),
        TypedRopRequest::OpenFolder(request) => {
            buffer.push(request.input_handle_index);
            buffer.push(request.output_handle_index);
            write_u64(&mut buffer, request.folder_id);
            buffer.push(request.open_mode_flags);
        }
        TypedRopRequest::OpenMessage(request) => {
            buffer.push(request.input_handle_index);
            buffer.push(request.output_handle_index);
            write_u16(&mut buffer, 0);
            write_u64(&mut buffer, request.folder_id);
            buffer.push(request.open_mode_flags);
            write_u64(&mut buffer, request.message_id);
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
            write_u64(&mut buffer, request.folder_id);
            buffer.push(request.associated_flag);
        }
        TypedRopRequest::SaveChangesMessage(request) => {
            buffer.push(request.response_handle_index);
            buffer.push(request.input_handle_index);
            buffer.push(request.save_flags);
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

    #[test]
    pub(in crate::mapi) fn session_idle_expiry_follows_cookie_max_age() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10_000);
        let fresh = MapiSession {
            endpoint: MapiEndpoint::Emsmdb,
            tenant_id: Uuid::from_u128(0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa),
            account_id: Uuid::nil(),
            email: "user@example.test".to_string(),
            last_seen_at: now - Duration::from_secs(u64::from(MAPI_SESSION_MAX_AGE_SECONDS)),
            next_handle: 1,
            handles: HashMap::new(),
            message_statuses: HashMap::new(),
            named_properties: HashMap::new(),
            named_property_ids: HashMap::new(),
            next_named_property_id: FIRST_NAMED_PROPERTY_ID,
            next_local_replica_sequence: 1,
            notification_cursor: None,
            pending_notifications: VecDeque::new(),
            completed_execute_requests: HashMap::new(),
            completed_execute_request_order: VecDeque::new(),
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
    pub(in crate::mapi) fn gwart_time_marker_uses_real_timestamp_and_stays_nonzero() {
        assert_eq!(
            gwart_time_marker(SystemTime::UNIX_EPOCH + Duration::from_secs(1_778_046_495)),
            1_778_046_495
        );
        assert_eq!(gwart_time_marker(SystemTime::UNIX_EPOCH), 1);
    }

    #[test]
    pub(in crate::mapi) fn golden_open_folder_rop_round_trips_through_typed_parser() {
        let folder_id = 0x1122_3344_5566_7788u64;
        let mut golden = vec![0x02, 0x00, 0x00, 0x01];
        golden.extend_from_slice(&folder_id.to_le_bytes());
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
    pub(in crate::mapi) fn malformed_supported_rop_buffer_fails_without_partial_request() {
        let mut cursor = Cursor::new(&[0x02, 0x00, 0x00, 0x01, 0x88, 0x77]);

        assert!(read_rop_request(&mut cursor).is_err());
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
