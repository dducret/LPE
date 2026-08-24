const GLOBAL_OBJECT_ID_BYTE_ARRAY_ID: [u8; 16] = [
    0x04, 0x00, 0x00, 0x00, 0x82, 0x00, 0xE0, 0x00, 0x74, 0xC5, 0xB7, 0x10, 0x1A, 0x82, 0xE0, 0x08,
];
const GLOBAL_OBJECT_ID_FIXED_SIZE: usize = 40;
const THIRD_PARTY_GLOBAL_OBJECT_ID_PREFIX: &[u8; 12] = b"vCal-Uid\x01\0\0\0";

fn calendar_global_object_id_hex(value: &str) -> (&str, bool) {
    let prefixed = value
        .get(.."mapi-goid:".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("mapi-goid:"));
    if prefixed {
        (&value["mapi-goid:".len()..], true)
    } else {
        (value, false)
    }
}

/// Decodes a native iCalendar EncodedGlobalId only when it is a complete
/// GlobalObjectId structure. See [MS-OXCICAL] section 2.1.3.1.1.20.26 and
/// [MS-OXOCAL] section 2.2.1.27.
pub fn decode_calendar_global_object_id_uid(value: &str) -> Option<Vec<u8>> {
    let value = value.trim();
    let (encoded, prefixed) = calendar_global_object_id_hex(value);
    if encoded.len() < (GLOBAL_OBJECT_ID_FIXED_SIZE + 1) * 2
        || encoded.len() % 2 != 0
        || !encoded.as_bytes().iter().all(u8::is_ascii_hexdigit)
        // [MS-OXCICAL] section 2.1.3.1.1.20.26 product note <203>:
        // Outlook through 2019 recognizes the unprefixed ByteArrayID text
        // only in its documented uppercase form. The private mapi-goid:
        // marker already declares binary content, so its hex is case-neutral.
        || (!prefixed
            && encoded.get(..32) != Some("040000008200E00074C5B7101A82E008"))
    {
        return None;
    }
    let mut decoded = Vec::with_capacity(encoded.len() / 2);
    for pair in encoded.as_bytes().chunks_exact(2) {
        let pair = std::str::from_utf8(pair).ok()?;
        decoded.push(u8::from_str_radix(pair, 16).ok()?);
    }
    complete_global_object_id(&decoded).then_some(decoded)
}

/// Exports a complete GlobalObjectId as the external iCalendar UID form from
/// [MS-OXCICAL] section 2.1.3.1.1.20.26.
pub fn calendar_uid_from_global_object_id(value: &[u8]) -> Option<String> {
    if !complete_global_object_id(value) {
        return None;
    }
    let data = &value[GLOBAL_OBJECT_ID_FIXED_SIZE..];
    if let Some(uid) = data.strip_prefix(THIRD_PARTY_GLOBAL_OBJECT_ID_PREFIX) {
        let uid = std::str::from_utf8(uid).ok()?;
        if uid.is_empty()
            || uid
                .chars()
                .any(|character| matches!(character, '\0' | '\r' | '\n'))
        {
            return None;
        }
        return Some(uid.to_string());
    }

    let mut clean = value.to_vec();
    clean[16..20].fill(0);
    Some(clean.iter().map(|byte| format!("{byte:02X}")).collect())
}

/// Projects a canonical calendar UID into the external iCalendar UID form from
/// [MS-OXCICAL] section 2.1.3.1.1.20.26. Complete native GlobalObjectIds use
/// the clean uppercase EncodedGlobalId, while third-party GlobalObjectIds
/// recover their original vCal UID.
pub fn external_calendar_uid(value: &str) -> String {
    decode_calendar_global_object_id_uid(value)
        .and_then(|global_object_id| calendar_uid_from_global_object_id(&global_object_id))
        .unwrap_or_else(|| value.to_string())
}

fn complete_global_object_id(value: &[u8]) -> bool {
    if value.len() < GLOBAL_OBJECT_ID_FIXED_SIZE + 1
        || value[..GLOBAL_OBJECT_ID_BYTE_ARRAY_ID.len()] != GLOBAL_OBJECT_ID_BYTE_ARRAY_ID
        // [MS-OXOCAL] section 2.2.1.27: X is reserved and MUST be zero.
        || value[28..36].iter().any(|byte| *byte != 0)
    {
        return false;
    }
    let Some(size) = value[36..40]
        .try_into()
        .ok()
        .map(u32::from_le_bytes)
        .and_then(|size| usize::try_from(size).ok())
    else {
        return false;
    };
    size == value.len() - GLOBAL_OBJECT_ID_FIXED_SIZE
}

pub fn normalize_calendar_meeting_uid(value: &str) -> String {
    let value = value.trim();
    let (encoded, _) = calendar_global_object_id_hex(value);
    if decode_calendar_global_object_id_uid(value).is_some() {
        format!("mapi-goid:{}", encoded.to_ascii_lowercase())
    } else {
        value.to_string()
    }
}

pub(super) fn calendar_uid_has_occurrence_date(value: &str) -> bool {
    decode_calendar_global_object_id_uid(value)
        .is_some_and(|global_object_id| global_object_id[16..20].iter().any(|byte| *byte != 0))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn global_object_id(instance_date: [u8; 4], data: &[u8]) -> Vec<u8> {
        let mut value = GLOBAL_OBJECT_ID_BYTE_ARRAY_ID.to_vec();
        value.extend_from_slice(&instance_date);
        value.extend_from_slice(&0x01DD_319E_CD70_84C0u64.to_le_bytes());
        value.extend_from_slice(&0u64.to_le_bytes());
        value.extend_from_slice(&(data.len() as u32).to_le_bytes());
        value.extend_from_slice(data);
        value
    }

    #[test]
    fn external_uid_export_zeroes_native_occurrence_date() {
        let value = global_object_id([0x07, 0xEA, 0x08, 0x18], &[0xEC; 16]);
        let exported = calendar_uid_from_global_object_id(&value).unwrap();

        assert!(!exported.starts_with("mapi-goid:"));
        assert_eq!(&exported[..32], "040000008200E00074C5B7101A82E008");
        let decoded = decode_calendar_global_object_id_uid(&exported).unwrap();
        assert_eq!(&decoded[16..20], &[0, 0, 0, 0]);
        assert_eq!(&decoded[20..], &value[20..]);
    }

    #[test]
    fn external_uid_export_recovers_third_party_utf8_uid() {
        let mut data = THIRD_PARTY_GLOBAL_OBJECT_ID_PREFIX.to_vec();
        data.extend_from_slice("probe-7@example.test".as_bytes());
        let value = global_object_id([0, 0, 0, 0], &data);

        assert_eq!(
            calendar_uid_from_global_object_id(&value).as_deref(),
            Some("probe-7@example.test")
        );
    }

    #[test]
    fn external_uid_export_rejects_malformed_structure() {
        let mut value = global_object_id([0, 0, 0, 0], &[0xEC; 16]);
        value[28] = 1;
        assert_eq!(calendar_uid_from_global_object_id(&value), None);
    }

    #[test]
    fn external_calendar_uid_projects_canonical_native_and_third_party_values() {
        let native = global_object_id([0x07, 0xEA, 0x08, 0x18], &[0xEC; 16]);
        let canonical_native = format!(
            "mapi-goid:{}",
            native
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        );
        let expected_native = calendar_uid_from_global_object_id(&native).unwrap();
        assert_eq!(external_calendar_uid(&canonical_native), expected_native);
        let encoded_native = native
            .iter()
            .map(|byte| format!("{byte:02X}"))
            .collect::<String>();
        assert_eq!(external_calendar_uid(&encoded_native), expected_native);

        let mut third_party_data = THIRD_PARTY_GLOBAL_OBJECT_ID_PREFIX.to_vec();
        third_party_data.extend_from_slice(b"probe-10@example.test");
        let third_party = global_object_id([0, 0, 0, 0], &third_party_data);
        let canonical_third_party = format!(
            "mapi-goid:{}",
            third_party
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        );
        assert_eq!(
            external_calendar_uid(&canonical_third_party),
            "probe-10@example.test"
        );
        assert_eq!(
            external_calendar_uid("Opaque-Uid@Example.Test"),
            "Opaque-Uid@Example.Test"
        );
        assert_eq!(
            external_calendar_uid("mapi-goid:0011aabb"),
            "mapi-goid:0011aabb"
        );
    }
}
