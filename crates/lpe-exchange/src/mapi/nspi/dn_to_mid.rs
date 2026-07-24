use super::*;

const NSPI_MAX_STRING_ARRAY_VALUES: usize = 100_000;

// MS-OXCMAPIHTTP 2.2.5.4.1 encodes DNToMId names as an ASCII string array,
// which is distinct from the ResolveNames request body. MS-OXNSPI 2.2.7.1
// permits up to 100,000 values and requires their original cardinality.
pub(super) fn parse_dn_to_mid_names(request: &[u8]) -> Option<Vec<String>> {
    let mut cursor = Cursor::new(request);
    let _reserved = cursor.read_u32().ok()?;
    let values = if cursor.read_u8().ok()? == 0 {
        Vec::new()
    } else {
        let count = cursor.read_u32().ok()? as usize;
        if count > NSPI_MAX_STRING_ARRAY_VALUES {
            return None;
        }
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            values.push(normalize_nspi_lookup_value(&cursor.read_ascii_z().ok()?));
        }
        values
    };
    let auxiliary_size = cursor.read_u32().ok()? as usize;
    cursor.read_bytes(auxiliary_size).ok()?;
    if cursor.remaining() != 0 {
        return None;
    }
    Some(values)
}
