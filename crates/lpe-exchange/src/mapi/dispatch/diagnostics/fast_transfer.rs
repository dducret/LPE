use crate::mapi::transport::hex_preview;

#[derive(Debug, PartialEq, Eq)]
pub(in crate::mapi::dispatch) struct FastTransferGetBufferResponseDebug {
    pub(in crate::mapi::dispatch) header_bytes: usize,
    pub(in crate::mapi::dispatch) rop_id: String,
    pub(in crate::mapi::dispatch) rop_id_matches: bool,
    pub(in crate::mapi::dispatch) handle_index: u8,
    pub(in crate::mapi::dispatch) return_value: String,
    pub(in crate::mapi::dispatch) transfer_status: String,
    pub(in crate::mapi::dispatch) transfer_status_matches_completed: bool,
    pub(in crate::mapi::dispatch) in_progress_count: u16,
    pub(in crate::mapi::dispatch) total_step_count: u16,
    pub(in crate::mapi::dispatch) reserved_byte: u8,
    pub(in crate::mapi::dispatch) reserved_zero: bool,
    pub(in crate::mapi::dispatch) transfer_buffer_size: u16,
    pub(in crate::mapi::dispatch) transfer_payload_bytes: usize,
    pub(in crate::mapi::dispatch) transfer_buffer_size_matches_payload: bool,
    pub(in crate::mapi::dispatch) transfer_payload_preview_hex: String,
    pub(in crate::mapi::dispatch) transfer_payload_tail_hex: String,
    pub(in crate::mapi::dispatch) parse_error: String,
}

pub(in crate::mapi::dispatch) fn summarize_fast_transfer_get_buffer_response(
    response: &[u8],
    completed: bool,
) -> FastTransferGetBufferResponseDebug {
    const HEADER_BYTES: usize = 15;
    if response.len() < HEADER_BYTES {
        return FastTransferGetBufferResponseDebug {
            header_bytes: HEADER_BYTES,
            rop_id: response
                .first()
                .map(|value| format!("0x{value:02x}"))
                .unwrap_or_default(),
            rop_id_matches: response.first() == Some(&0x4e),
            handle_index: response.get(1).copied().unwrap_or_default(),
            return_value: String::new(),
            transfer_status: String::new(),
            transfer_status_matches_completed: false,
            in_progress_count: 0,
            total_step_count: 0,
            reserved_byte: 0,
            reserved_zero: false,
            transfer_buffer_size: 0,
            transfer_payload_bytes: 0,
            transfer_buffer_size_matches_payload: false,
            transfer_payload_preview_hex: String::new(),
            transfer_payload_tail_hex: String::new(),
            parse_error: "truncated_get_buffer_response_header".to_string(),
        };
    }

    let return_value = u32::from_le_bytes(response[2..6].try_into().unwrap());
    let transfer_status = u16::from_le_bytes(response[6..8].try_into().unwrap());
    let in_progress_count = u16::from_le_bytes(response[8..10].try_into().unwrap());
    let total_step_count = u16::from_le_bytes(response[10..12].try_into().unwrap());
    let reserved_byte = response[12];
    let transfer_buffer_size = u16::from_le_bytes(response[13..15].try_into().unwrap());
    let transfer_payload = &response[HEADER_BYTES..];
    let tail_start = transfer_payload.len().saturating_sub(16);

    FastTransferGetBufferResponseDebug {
        header_bytes: HEADER_BYTES,
        rop_id: format!("0x{:02x}", response[0]),
        rop_id_matches: response[0] == 0x4e,
        handle_index: response[1],
        return_value: format!("0x{return_value:08x}"),
        transfer_status: format!("0x{transfer_status:04x}"),
        transfer_status_matches_completed: matches!(
            (completed, transfer_status),
            (true, 0x0003) | (false, 0x0001)
        ),
        in_progress_count,
        total_step_count,
        reserved_byte,
        reserved_zero: reserved_byte == 0,
        transfer_buffer_size,
        transfer_payload_bytes: transfer_payload.len(),
        transfer_buffer_size_matches_payload: transfer_buffer_size as usize
            == transfer_payload.len(),
        transfer_payload_preview_hex: hex_preview(transfer_payload, 32),
        transfer_payload_tail_hex: hex_preview(&transfer_payload[tail_start..], 16),
        parse_error: String::new(),
    }
}
