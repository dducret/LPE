use super::*;

const MAX_FAST_TRANSFER_MULTI_VALUE_COUNT: usize = u16::MAX as usize;
const META_TAG_DN_PREFIX: u32 = 0x4008_001E;
const META_TAG_FX_DEL_PROP: u32 = 0x4016_0003;

#[derive(Debug)]
pub(in crate::mapi::dispatch) struct MissingFastTransferNamedProperty(
    pub(in crate::mapi::dispatch) MapiNamedProperty,
);

impl std::fmt::Display for MissingFastTransferNamedProperty {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("FastTransfer named property has no mailbox mapping")
    }
}

impl std::error::Error for MissingFastTransferNamedProperty {}

pub(in crate::mapi::dispatch) fn fast_transfer_property_values(
    session: &mut MapiSession,
    bytes: &[u8],
) -> Result<(Vec<(u32, MapiValue)>, usize)> {
    let mut cursor = Cursor::new(bytes);
    let mut values = Vec::new();
    while cursor.remaining() > 0 {
        let property_start = cursor.position();
        let Some(wire_property_tag) = read_fast_transfer_u32_if_complete(&mut cursor) else {
            return Ok((values, property_start));
        };
        if fast_transfer_marker_tag(wire_property_tag) {
            return Err(anyhow::anyhow!("unsupported FastTransfer marker"));
        }
        if MapiPropertyType::from_code((wire_property_tag & 0xFFFF) as u16).is_none() {
            return Err(anyhow::anyhow!("unsupported FastTransfer property type"));
        }
        let Some(property_tag) = resolve_fast_transfer_destination_property_tag(
            session,
            &mut cursor,
            wire_property_tag,
        )?
        else {
            return Ok((values, property_start));
        };
        let Some(value) = read_fast_transfer_property_value(&mut cursor, property_tag)? else {
            return Ok((values, property_start));
        };
        // [MS-OXCFXICS] section 2.2.4.1.5.6: MetaTagDnPrefix is
        // transport metadata and MUST be ignored by the destination.
        if wire_property_tag == META_TAG_DN_PREFIX {
            continue;
        }
        values.push((property_tag, value));
    }
    Ok((values, cursor.position()))
}

fn fast_transfer_marker_tag(tag: u32) -> bool {
    matches!(
        tag,
        0x4000_0003
            | 0x4001_0003
            | 0x4002_0003
            | 0x4003_0003
            | 0x4004_0003
            | 0x4009_0003
            | 0x400A_0003
            | 0x400B_0003
            | 0x400C_0003
            | 0x400D_0003
            | 0x400E_0003
            | 0x4010_0003
            | 0x4012_0003
            | 0x4013_0003
            | 0x4014_0003
            | 0x4015_0003
            | META_TAG_FX_DEL_PROP
            | 0x4018_0003
            | 0x402F_0003
            | 0x403A_0003
            | 0x403B_0003
            | 0x4074_000B
            | 0x4075_000B
            | 0x407B_0102
            | 0x407D_0003
    )
}

fn resolve_fast_transfer_destination_property_tag(
    session: &mut MapiSession,
    cursor: &mut Cursor<'_>,
    wire_property_tag: u32,
) -> Result<Option<u32>> {
    let wire_tag = MapiPropertyTag::new(wire_property_tag);
    if wire_tag.property_id() < 0x8000 {
        return Ok(Some(wire_property_tag));
    }

    // [MS-OXCFXICS] section 2.2.4.1: namedPropInfo follows the tag and
    // identifies the property independently of the source store's numeric ID.
    // Require an existing mailbox mapping (normally established through
    // GetIDsFromNames) so a transfer cannot create a session-only alias that
    // disappears on reconnect.
    if cursor.remaining() < 17 {
        return Ok(None);
    }
    let guid: [u8; 16] = cursor.read_bytes(16)?.try_into().unwrap();
    let kind = match cursor.read_u8()? {
        0x00 => {
            let Some(lid) = read_fast_transfer_u32_if_complete(cursor) else {
                return Ok(None);
            };
            MapiNamedPropertyKind::Lid(lid)
        }
        0x01 => {
            let Some(name) = read_fast_transfer_utf16z_if_complete(cursor)? else {
                return Ok(None);
            };
            MapiNamedPropertyKind::Name(name)
        }
        kind => {
            return Err(anyhow::anyhow!(
                "invalid FastTransfer named-property kind 0x{kind:02x}"
            ))
        }
    };
    let property = normalize_named_property(MapiNamedProperty { guid, kind });
    let property_id = if property.guid == PS_MAPI_GUID {
        let MapiNamedPropertyKind::Lid(lid) = property.kind else {
            return Err(anyhow::anyhow!(
                "PS_MAPI FastTransfer property must use a LID"
            ));
        };
        u16::try_from(lid).map_err(|_| anyhow::anyhow!("invalid PS_MAPI property LID"))?
    } else {
        session
            .named_properties
            .get(&property)
            .copied()
            .ok_or_else(|| anyhow::Error::new(MissingFastTransferNamedProperty(property)))?
    };
    let resolved_tag = (u32::from(property_id) << 16) | u32::from(wire_tag.property_type_code());
    Ok(Some(session.normalize_named_property_tag(resolved_tag)))
}

fn read_fast_transfer_property_value(
    cursor: &mut Cursor<'_>,
    property_tag: u32,
) -> Result<Option<MapiValue>> {
    match MapiPropertyType::from_code((property_tag & 0xFFFF) as u16) {
        Some(MapiPropertyType::Integer16) => Ok(read_fast_transfer_bytes_if_complete(cursor, 2)
            .map(|bytes| MapiValue::I16(i16::from_le_bytes(bytes.try_into().unwrap())))),
        Some(MapiPropertyType::Integer32) => Ok(read_fast_transfer_bytes_if_complete(cursor, 4)
            .map(|bytes| MapiValue::I32(i32::from_le_bytes(bytes.try_into().unwrap())))),
        Some(MapiPropertyType::Floating32 | MapiPropertyType::Floating64) => Err(anyhow::anyhow!(
            "unsupported FastTransfer floating-point property type"
        )),
        Some(MapiPropertyType::Boolean) => Ok(read_fast_transfer_bytes_if_complete(cursor, 2)
            .map(|bytes| MapiValue::Bool(u16::from_le_bytes(bytes.try_into().unwrap()) != 0))),
        Some(MapiPropertyType::Integer64) | Some(MapiPropertyType::Time) => {
            Ok(read_fast_transfer_bytes_if_complete(cursor, 8)
                .map(|bytes| MapiValue::I64(i64::from_le_bytes(bytes.try_into().unwrap()))))
        }
        Some(MapiPropertyType::String8) => {
            let Some(bytes) = read_fast_transfer_variable_bytes(cursor)? else {
                return Ok(None);
            };
            if bytes.is_empty() {
                return Err(anyhow::anyhow!("zero-length FastTransfer String8 value"));
            }
            Ok(Some(MapiValue::String(decode_fast_transfer_string8(
                &bytes,
            ))))
        }
        Some(MapiPropertyType::String) => {
            let Some(bytes) = read_fast_transfer_variable_bytes(cursor)? else {
                return Ok(None);
            };
            if bytes.is_empty() {
                return Err(anyhow::anyhow!("zero-length FastTransfer Unicode value"));
            }
            Ok(Some(MapiValue::String(decode_fast_transfer_utf16(&bytes)?)))
        }
        Some(MapiPropertyType::ServerId) => {
            let Some(bytes) = read_fast_transfer_server_id(cursor)? else {
                return Ok(None);
            };
            Ok(Some(MapiValue::Binary(bytes)))
        }
        Some(MapiPropertyType::Binary) => {
            Ok(read_fast_transfer_variable_bytes(cursor)?.map(MapiValue::Binary))
        }
        Some(MapiPropertyType::MultipleString8) => {
            let Some(values) = read_fast_transfer_multi_string(cursor, false)? else {
                return Ok(None);
            };
            Ok(Some(MapiValue::MultiString(values)))
        }
        Some(MapiPropertyType::MultipleString) => {
            let Some(values) = read_fast_transfer_multi_string(cursor, true)? else {
                return Ok(None);
            };
            Ok(Some(MapiValue::MultiString(values)))
        }
        Some(MapiPropertyType::Guid) => Ok(read_fast_transfer_bytes_if_complete(cursor, 16)
            .map(|bytes| MapiValue::Guid(bytes.try_into().unwrap()))),
        _ => Err(anyhow::anyhow!("unsupported FastTransfer property type")),
    }
}

fn read_fast_transfer_server_id(cursor: &mut Cursor<'_>) -> Result<Option<Vec<u8>>> {
    let start = cursor.position();
    let Some(length_bytes) = read_fast_transfer_bytes_if_complete(cursor, 2) else {
        return Ok(None);
    };
    let len = u16::from_le_bytes(length_bytes.try_into().unwrap()) as usize;
    let Some(bytes) = read_fast_transfer_bytes_if_complete(cursor, len) else {
        cursor.position = start;
        return Ok(None);
    };
    let valid = match bytes.first().copied() {
        Some(0x00) => true,
        Some(0x01) => bytes.len() == 21,
        _ => false,
    };
    if !valid {
        return Err(anyhow::anyhow!("invalid FastTransfer ServerId value"));
    }
    Ok(Some(bytes.to_vec()))
}

fn read_fast_transfer_multi_string(
    cursor: &mut Cursor<'_>,
    unicode: bool,
) -> Result<Option<Vec<String>>> {
    let start = cursor.position();
    let Some(count) = read_fast_transfer_u32_if_complete(cursor) else {
        return Ok(None);
    };
    let count = count as usize;
    if count > MAX_FAST_TRANSFER_MULTI_VALUE_COUNT {
        return Err(anyhow::anyhow!(
            "FastTransfer multivalue count exceeds limit"
        ));
    }
    if count > cursor.remaining() / 4 {
        cursor.position = start;
        return Ok(None);
    }
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        let Some(bytes) = read_fast_transfer_variable_bytes(cursor)? else {
            cursor.position = start;
            return Ok(None);
        };
        if bytes.is_empty() {
            return Err(anyhow::anyhow!("zero-length FastTransfer string element"));
        }
        values.push(if unicode {
            decode_fast_transfer_utf16(&bytes)?
        } else {
            decode_fast_transfer_string8(&bytes)
        });
    }
    Ok(Some(values))
}

fn read_fast_transfer_variable_bytes(cursor: &mut Cursor<'_>) -> Result<Option<Vec<u8>>> {
    let start = cursor.position();
    let Some(len) = read_fast_transfer_u32_if_complete(cursor) else {
        return Ok(None);
    };
    let len = len as usize;
    let Some(bytes) = read_fast_transfer_bytes_if_complete(cursor, len) else {
        cursor.position = start;
        return Ok(None);
    };
    Ok(Some(bytes.to_vec()))
}

fn read_fast_transfer_u32_if_complete(cursor: &mut Cursor<'_>) -> Option<u32> {
    read_fast_transfer_bytes_if_complete(cursor, 4)
        .map(|bytes| u32::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_fast_transfer_bytes_if_complete<'a>(
    cursor: &mut Cursor<'a>,
    len: usize,
) -> Option<&'a [u8]> {
    (cursor.remaining() >= len).then(|| {
        cursor
            .read_bytes(len)
            .expect("FastTransfer length was prevalidated")
    })
}

fn read_fast_transfer_utf16z_if_complete(cursor: &mut Cursor<'_>) -> Result<Option<String>> {
    let start = cursor.position();
    let remaining = &cursor.bytes[start..];
    let Some(end) = remaining
        .chunks_exact(2)
        .position(|unit| unit == [0, 0])
        .map(|units| units * 2)
    else {
        return Ok(None);
    };
    let bytes = cursor.read_bytes(end + 2)?;
    decode_fast_transfer_utf16(&bytes[..end]).map(Some)
}

fn decode_fast_transfer_string8(bytes: &[u8]) -> String {
    let trimmed = bytes.strip_suffix(&[0]).unwrap_or(bytes);
    String::from_utf8_lossy(trimmed).into_owned()
}

fn decode_fast_transfer_utf16(bytes: &[u8]) -> Result<String> {
    if bytes.len() % 2 != 0 {
        return Err(anyhow::anyhow!("odd UTF-16 FastTransfer string length"));
    }
    let mut units = bytes
        .chunks_exact(2)
        .map(|chunk| u16::from_le_bytes(chunk.try_into().unwrap()))
        .collect::<Vec<_>>();
    if units.last() == Some(&0) {
        units.pop();
    }
    Ok(String::from_utf16(&units)?)
}
