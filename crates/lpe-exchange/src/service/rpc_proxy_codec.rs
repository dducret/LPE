pub(super) fn read_le_u32(body: &[u8], offset: usize) -> Option<u32> {
    body.get(offset..offset + 4)
        .map(|bytes| u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

pub(super) fn rpc_proxy_push_ndr_byte_array(stub: &mut Vec<u8>, value: &[u8]) {
    push_le_u32(stub, value.len() as u32);
    push_le_u32(stub, 0);
    push_le_u32(stub, value.len() as u32);
    stub.extend_from_slice(value);
    while stub.len() % 4 != 0 {
        stub.push(0);
    }
}

pub(super) fn rpc_proxy_push_ndr_ascii_string(buffer: &mut Vec<u8>, value: &str) {
    let bytes = value.as_bytes();
    let count = bytes.len() as u32 + 1;
    push_le_u32(buffer, count);
    push_le_u32(buffer, 0);
    push_le_u32(buffer, count);
    buffer.extend_from_slice(bytes);
    buffer.push(0);
    while buffer.len() % 4 != 0 {
        buffer.push(0);
    }
}

pub(super) fn rpc_proxy_push_ndr_utf16_string(buffer: &mut Vec<u8>, value: &str) {
    let units: Vec<u16> = value.encode_utf16().chain(std::iter::once(0)).collect();
    push_le_u32(buffer, units.len() as u32);
    push_le_u32(buffer, 0);
    push_le_u32(buffer, units.len() as u32);
    for unit in units {
        buffer.extend_from_slice(&unit.to_le_bytes());
    }
    while buffer.len() % 4 != 0 {
        buffer.push(0);
    }
}

pub(super) fn push_le_u32(buffer: &mut Vec<u8>, value: u32) {
    buffer.extend_from_slice(&value.to_le_bytes());
}
