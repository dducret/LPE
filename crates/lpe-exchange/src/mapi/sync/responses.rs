use super::*;

pub(in crate::mapi) fn rop_synchronization_configure_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x70, request.output_handle_index.unwrap_or(0)];
    write_u32(&mut response, 0);
    response
}

pub(in crate::mapi) fn rop_fast_transfer_source_copy_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![request.rop_id, request.output_handle_index.unwrap_or(0)];
    write_u32(&mut response, 0);
    response
}

pub(in crate::mapi) fn rop_fast_transfer_source_get_buffer_response(
    request: &RopRequest,
    transfer_buffer: &[u8],
    transfer_position: &mut usize,
) -> Vec<u8> {
    let requested = request
        .fast_transfer_buffer_size()
        .clamp(1, u16::MAX as usize);
    let end = transfer_position
        .saturating_add(requested)
        .min(transfer_buffer.len());
    let chunk = transfer_buffer[*transfer_position..end].to_vec();
    *transfer_position = end;
    let done = *transfer_position >= transfer_buffer.len();
    let total_steps = transfer_buffer
        .len()
        .div_ceil(requested)
        .min(u16::MAX as usize) as u16;
    let completed_steps = if total_steps == 0 {
        0
    } else {
        (*transfer_position)
            .div_ceil(requested)
            .min(u16::MAX as usize) as u16
    };

    let mut response = vec![0x4E, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.extend_from_slice(&(if done { 0x0003u16 } else { 0x0001u16 }).to_le_bytes());
    response.extend_from_slice(&completed_steps.to_le_bytes());
    response.extend_from_slice(&total_steps.to_le_bytes());
    response.push(0);
    response.extend_from_slice(&(chunk.len().min(u16::MAX as usize) as u16).to_le_bytes());
    response.extend_from_slice(&chunk);
    response
}

pub(in crate::mapi) fn rop_synchronization_get_transfer_state_response(
    request: &RopRequest,
) -> Vec<u8> {
    let mut response = vec![0x82, request.output_handle_index.unwrap_or(0)];
    write_u32(&mut response, 0);
    response
}

pub(in crate::mapi) fn rop_synchronization_import_message_change_response(
    request: &RopRequest,
) -> Vec<u8> {
    let mut response = vec![0x72, request.output_handle_index.unwrap_or(0)];
    write_u32(&mut response, 0);
    write_object_id(&mut response, 0);
    response
}

pub(in crate::mapi) fn rop_synchronization_import_hierarchy_change_response(
    request: &RopRequest,
) -> Vec<u8> {
    let mut response = vec![0x73, request.response_handle_index()];
    write_u32(&mut response, 0);
    write_object_id(&mut response, 0);
    response
}

pub(in crate::mapi) fn rop_synchronization_import_message_move_response(
    request: &RopRequest,
) -> Vec<u8> {
    let mut response = vec![0x78, request.response_handle_index()];
    write_u32(&mut response, 0);
    write_object_id(&mut response, 0);
    response
}

pub(in crate::mapi) fn rop_get_local_replica_ids_response(
    request: &RopRequest,
    first_global_counter: u64,
) -> Vec<u8> {
    let mut response = vec![0x7F, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.extend_from_slice(&mapi_mailstore::STORE_REPLICA_GUID);
    response.extend_from_slice(&crate::mapi::identity::globcnt_bytes(first_global_counter));
    response
}
