use super::*;

pub(super) struct NspiResolveNamesRequest {
    pub(super) requested_names: Vec<String>,
}

/// Parses the complete ResolveNames body before any canonical address-book
/// projection is read or allocated. [MS-OXCMAPIHTTP] section 2.2.5.14.1
/// defines each NameValues element as a null-terminated Unicode string; the
/// all-zero RCA bootstrap probe remains compatible.
pub(super) fn parse_nspi_resolve_names_request(request: &[u8]) -> Option<NspiResolveNamesRequest> {
    if !request.is_empty() && request.iter().all(|byte| *byte == 0) {
        return Some(NspiResolveNamesRequest {
            requested_names: Vec::new(),
        });
    }

    let mut cursor = Cursor::new(request);
    let _reserved = cursor.read_u32().ok()?;
    if cursor.read_u8().ok()? != 0 {
        cursor.read_bytes(36).ok()?;
    }
    if cursor.read_u8().ok()? != 0 {
        let count = cursor.read_u32().ok()? as usize;
        if count == 0 || count > 128 {
            return None;
        }
        cursor.read_bytes(count.checked_mul(4)?).ok()?;
    }
    let mut requested_names = Vec::new();
    if cursor.read_u8().ok()? != 0 {
        let count = cursor.read_u32().ok()? as usize;
        if count > 128 {
            return None;
        }
        for _ in 0..count {
            let value = cursor.read_utf16z().ok()?;
            requested_names.push(normalize_nspi_lookup_value(&value));
        }
    }
    let _reserved = cursor.read_u32().ok()?;
    (cursor.remaining() == 0).then_some(NspiResolveNamesRequest { requested_names })
}
