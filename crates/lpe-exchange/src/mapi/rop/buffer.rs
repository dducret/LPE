use anyhow::{anyhow, Result};

pub(in crate::mapi) struct Cursor<'a> {
    pub(in crate::mapi) bytes: &'a [u8],
    pub(in crate::mapi) position: usize,
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
