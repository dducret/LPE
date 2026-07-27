use super::*;

pub(in crate::mapi) const ROP_ERROR_NOT_SUPPORTED: u32 = MapiError::NotSupported.as_u32();
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

pub(in crate::mapi) fn rop_id_is_reserved(rop_id: u8) -> bool {
    RopId::is_reserved(rop_id)
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
