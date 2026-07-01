use super::*;

pub(super) fn append_unsupported_known_dispatch_response(
    rop_id: RopId,
    request: &RopRequest,
    responses: &mut Vec<u8>,
) {
    responses.extend_from_slice(&unsupported_known_rop_response(rop_id, request));
}

pub(super) fn append_unsupported_unknown_dispatch_response(
    request: &RopRequest,
    responses: &mut Vec<u8>,
) {
    responses.extend_from_slice(&unsupported_unknown_rop_response(request));
}

pub(super) fn unsupported_known_rop_response(rop_id: RopId, request: &RopRequest) -> Vec<u8> {
    unsupported_rop_response(rop_id.as_u8(), request.response_handle_index())
}

pub(super) fn unsupported_unknown_rop_response(request: &RopRequest) -> Vec<u8> {
    unsupported_rop_response(request.rop_id, request.response_handle_index())
}
