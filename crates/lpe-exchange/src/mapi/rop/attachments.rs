use super::{rop_error_response, write_u32, RopRequest};
use crate::mapi::session::MapiObject;
use crate::mapi_store::MapiMailStoreSnapshot;
use std::collections::HashSet;

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
            ..
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
    rop_get_valid_attachment_numbers_response(
        request,
        &valid_attachments
            .into_iter()
            .map(|attachment| attachment.attach_num)
            .collect::<Vec<_>>(),
    )
}

pub(in crate::mapi) fn rop_get_valid_attachment_numbers_response(
    request: &RopRequest,
    attach_nums: &[u32],
) -> Vec<u8> {
    let mut response = vec![0x52, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.extend_from_slice(&(attach_nums.len().min(u16::MAX as usize) as u16).to_le_bytes());
    for attach_num in attach_nums.iter().take(u16::MAX as usize) {
        write_u32(&mut response, *attach_num);
    }
    response
}
