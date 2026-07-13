use super::{
    default_folder_property_tags_with_identity, message_for_id, pending_text_property,
    search_folder_message_for_id,
};
use super::{
    rop_error_response, write_object_id, write_typed_string, write_u16, write_u32, write_u64,
    RopRequest,
};
use crate::mapi::identity::OUTBOX_FOLDER_ID;
use crate::mapi::identity::{INBOX_FOLDER_ID, ROOT_FOLDER_ID};
use crate::mapi::properties::*;
use crate::mapi::session::MapiObject;
use crate::mapi::tables::{
    default_attachment_columns, default_contact_property_tags,
    default_conversation_action_property_tags, default_event_property_tags,
    default_folder_property_tags, default_journal_entry_property_tags,
    default_message_property_tags, default_note_property_tags, default_store_property_tags,
    default_task_property_tags, message_recipients, serialize_recipient_row,
    write_standard_property_row,
};
use crate::mapi::wire::RopId;
use crate::mapi_store::MapiMailStoreSnapshot;
use lpe_storage::{JmapEmail, JmapMailbox};

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

pub(in crate::mapi) fn rop_open_message_response_with_recipients(
    request: &RopRequest,
    subject: &str,
    email: &JmapEmail,
) -> Vec<u8> {
    let recipients = message_recipients(email);
    let mut response = vec![0x03, request.output_handle_index.unwrap_or(0)];
    write_u32(&mut response, 0);
    response.push(0);
    write_typed_string(&mut response, "");
    write_typed_string(&mut response, subject);
    response.extend_from_slice(&(recipients.len().min(u16::MAX as usize) as u16).to_le_bytes());
    // [MS-OXCROPS] 2.2.6.1.2 permits an empty RecipientColumns array;
    // the intrinsic fields in each RecipientRow still carry its address and name.
    response.extend_from_slice(&0u16.to_le_bytes());
    response.push(recipients.len().min(u8::MAX as usize) as u8);
    for recipient in recipients.into_iter().take(u8::MAX as usize) {
        let row = serialize_recipient_row(recipient.address);
        response.push(recipient.recipient_type);
        response.extend_from_slice(&0x0FFFu16.to_le_bytes());
        response.extend_from_slice(&0u16.to_le_bytes());
        response.extend_from_slice(&(row.len().min(u16::MAX as usize) as u16).to_le_bytes());
        response.extend_from_slice(&row[..row.len().min(u16::MAX as usize)]);
    }
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

pub(in crate::mapi) fn rop_get_address_types_response(request: &RopRequest) -> Vec<u8> {
    let address_types = b"EX\0SMTP\0";
    let mut response = vec![0x49, request.input_handle_index().unwrap_or(0)];
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

pub(in crate::mapi) fn rop_set_read_flags_response(
    request: &RopRequest,
    partial_completion: bool,
) -> Vec<u8> {
    let mut response = vec![0x66, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(partial_completion as u8);
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
