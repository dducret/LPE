use super::*;

pub(in crate::mapi) fn mapi_properties_to_json(
    properties: &HashMap<u32, MapiValue>,
) -> serde_json::Value {
    let mut values = serde_json::Map::new();
    for (tag, value) in properties {
        values.insert(format!("0x{tag:08x}"), mapi_value_to_json(value));
    }
    serde_json::Value::Object(values)
}

pub(in crate::mapi) fn mapi_properties_from_json(
    properties: &serde_json::Value,
) -> HashMap<u32, MapiValue> {
    properties
        .as_object()
        .map(|values| {
            values
                .iter()
                .filter_map(|(tag, value)| {
                    let tag = u32::from_str_radix(tag.trim_start_matches("0x"), 16).ok()?;
                    Some((tag, mapi_value_from_json(value)?))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn mapi_value_to_json(value: &MapiValue) -> serde_json::Value {
    match value {
        MapiValue::Bool(value) => serde_json::json!({"type": "bool", "value": value}),
        MapiValue::I16(value) => serde_json::json!({"type": "i16", "value": value}),
        MapiValue::I32(value) => serde_json::json!({"type": "i32", "value": value}),
        MapiValue::I64(value) => serde_json::json!({"type": "i64", "value": value}),
        MapiValue::U32(value) => serde_json::json!({"type": "u32", "value": value}),
        MapiValue::U64(value) => serde_json::json!({"type": "u64", "value": value}),
        MapiValue::F64(value) => {
            serde_json::json!({"type": "f64", "value": f64::from_bits(*value)})
        }
        MapiValue::String(value) => serde_json::json!({"type": "string", "value": value}),
        MapiValue::Binary(value) => {
            serde_json::json!({"type": "binary", "value": bytes_to_hex(value)})
        }
        MapiValue::Guid(value) => serde_json::json!({"type": "guid", "value": bytes_to_hex(value)}),
        MapiValue::Error(value) => serde_json::json!({"type": "error", "value": value}),
        MapiValue::MultiI16(values) => serde_json::json!({"type": "multi_i16", "value": values}),
        MapiValue::MultiI32(values) => serde_json::json!({"type": "multi_i32", "value": values}),
        MapiValue::MultiI64(values) => serde_json::json!({"type": "multi_i64", "value": values}),
        MapiValue::MultiString(values) => {
            serde_json::json!({"type": "multi_string", "value": values})
        }
        MapiValue::MultiBinary(values) => serde_json::json!({
            "type": "multi_binary",
            "value": values.iter().map(|value| bytes_to_hex(value)).collect::<Vec<_>>()
        }),
        MapiValue::MultiGuid(values) => serde_json::json!({
            "type": "multi_guid",
            "value": values.iter().map(|value| bytes_to_hex(value)).collect::<Vec<_>>()
        }),
    }
}

fn mapi_value_from_json(value: &serde_json::Value) -> Option<MapiValue> {
    let value_type = value.get("type")?.as_str()?;
    let value = value.get("value")?;
    match value_type {
        "bool" => Some(MapiValue::Bool(value.as_bool()?)),
        "i16" => Some(MapiValue::I16(value.as_i64()?.try_into().ok()?)),
        "i32" => Some(MapiValue::I32(value.as_i64()?.try_into().ok()?)),
        "i64" => Some(MapiValue::I64(value.as_i64()?)),
        "u32" => Some(MapiValue::U32(value.as_u64()?.try_into().ok()?)),
        "u64" => Some(MapiValue::U64(value.as_u64()?)),
        "f64" => Some(MapiValue::F64(value.as_f64()?.to_bits())),
        "string" => Some(MapiValue::String(value.as_str()?.to_string())),
        "binary" => Some(MapiValue::Binary(hex_to_bytes(value.as_str()?)?)),
        "guid" => Some(MapiValue::Guid(
            hex_to_bytes(value.as_str()?)?.try_into().ok()?,
        )),
        "error" => Some(MapiValue::Error(value.as_u64()?.try_into().ok()?)),
        "multi_i16" => Some(MapiValue::MultiI16(json_i64_values(value)?)),
        "multi_i32" => Some(MapiValue::MultiI32(json_i64_values(value)?)),
        "multi_i64" => Some(MapiValue::MultiI64(
            value
                .as_array()?
                .iter()
                .map(serde_json::Value::as_i64)
                .collect::<Option<Vec<_>>>()?,
        )),
        "multi_string" => Some(MapiValue::MultiString(
            value
                .as_array()?
                .iter()
                .map(|value| value.as_str().map(str::to_string))
                .collect::<Option<Vec<_>>>()?,
        )),
        "multi_binary" => Some(MapiValue::MultiBinary(json_hex_values(value)?)),
        "multi_guid" => Some(MapiValue::MultiGuid(
            json_hex_values(value)?
                .into_iter()
                .map(|value| value.try_into().ok())
                .collect::<Option<Vec<_>>>()?,
        )),
        _ => None,
    }
}

fn json_i64_values<T>(value: &serde_json::Value) -> Option<Vec<T>>
where
    T: TryFrom<i64>,
{
    value
        .as_array()?
        .iter()
        .map(|value| value.as_i64()?.try_into().ok())
        .collect()
}

fn json_hex_values(value: &serde_json::Value) -> Option<Vec<Vec<u8>>> {
    value
        .as_array()?
        .iter()
        .map(|value| hex_to_bytes(value.as_str()?))
        .collect()
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub(super) fn hex_to_bytes(value: &str) -> Option<Vec<u8>> {
    if value.len() % 2 != 0 {
        return None;
    }
    (0..value.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&value[index..index + 2], 16).ok())
        .collect()
}

pub(in crate::mapi) fn write_mapi_value(row: &mut Vec<u8>, property_tag: u32, value: &MapiValue) {
    match MapiPropertyTag::new(property_tag).property_type() {
        Some(MapiPropertyType::Integer16) => write_u16(
            row,
            value
                .clone()
                .into_u32()
                .and_then(|value| u16::try_from(value).ok())
                .unwrap_or_default(),
        ),
        Some(MapiPropertyType::Integer32) => {
            write_u32(row, value.clone().into_u32().unwrap_or_default())
        }
        Some(MapiPropertyType::Floating32) => {
            let value = match value {
                MapiValue::F64(value) if f64::from_bits(*value).is_finite() => {
                    f64::from_bits(*value) as f32
                }
                _ => 0.0,
            };
            row.extend_from_slice(&value.to_le_bytes());
        }
        Some(MapiPropertyType::Floating64) => {
            let value = match value {
                MapiValue::F64(value) if f64::from_bits(*value).is_finite() => {
                    f64::from_bits(*value)
                }
                _ => 0.0,
            };
            row.extend_from_slice(&value.to_le_bytes());
        }
        Some(MapiPropertyType::Error) => {
            write_u32(row, value.clone().into_u32().unwrap_or(0x8004_0102))
        }
        Some(MapiPropertyType::Boolean) => row.push(value.as_bool().unwrap_or_default() as u8),
        Some(MapiPropertyType::Integer64) | Some(MapiPropertyType::Time) => {
            let value = value.as_i64().unwrap_or_default().max(0) as u64;
            match property_tag {
                PID_TAG_FOLDER_ID | PID_TAG_PARENT_FOLDER_ID | PID_TAG_MID => {
                    write_object_id(row, value)
                }
                _ => write_u64(row, value),
            }
        }
        Some(MapiPropertyType::String8) => {
            write_ascii_z(row, &value.clone().into_text().unwrap_or_default())
        }
        Some(MapiPropertyType::String) => {
            write_utf16z(row, &value.clone().into_text().unwrap_or_default())
        }
        Some(MapiPropertyType::Guid) => match value {
            MapiValue::Guid(guid) => row.extend_from_slice(guid),
            _ => row.extend_from_slice(Uuid::nil().as_bytes()),
        },
        Some(MapiPropertyType::ServerId | MapiPropertyType::Binary) => match value {
            MapiValue::Binary(bytes) => write_rop_binary(row, bytes),
            _ => write_rop_binary(row, &[]),
        },
        Some(MapiPropertyType::MultipleInteger16) => match value {
            MapiValue::MultiI16(values) => write_multi_i16(row, values),
            _ => write_u32(row, 0),
        },
        Some(MapiPropertyType::MultipleInteger32) => match value {
            MapiValue::MultiI32(values) => write_multi_i32(row, values),
            _ => write_u32(row, 0),
        },
        Some(MapiPropertyType::MultipleInteger64) => match value {
            MapiValue::MultiI64(values) => write_multi_i64(row, values),
            _ => write_u32(row, 0),
        },
        Some(MapiPropertyType::MultipleString8) => match value {
            MapiValue::MultiString(values) => write_multi_string8(row, values),
            _ => write_u32(row, 0),
        },
        Some(MapiPropertyType::MultipleString) => match value {
            MapiValue::MultiString(values) => write_multi_string(row, values),
            _ => write_u32(row, 0),
        },
        Some(MapiPropertyType::MultipleGuid) => match value {
            MapiValue::MultiGuid(values) => write_multi_guid(row, values),
            _ => write_u32(row, 0),
        },
        Some(MapiPropertyType::MultipleBinary) => match value {
            MapiValue::MultiBinary(values) => write_multi_binary(row, values),
            _ => write_u32(row, 0),
        },
        None => write_property_default(row, property_tag),
    }
}

pub(in crate::mapi) fn parse_mapi_property_value(
    cursor: &mut Cursor<'_>,
    property_tag: u32,
) -> Result<MapiValue> {
    match MapiPropertyTag::new(property_tag).property_type() {
        Some(MapiPropertyType::Integer16) => Ok(MapiValue::I16(cursor.read_u16()? as i16)),
        Some(MapiPropertyType::Integer32) => Ok(MapiValue::I32(cursor.read_i32()?)),
        Some(MapiPropertyType::Floating32) => {
            let bytes = cursor.read_bytes(4)?;
            Ok(MapiValue::F64(
                (f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as f64).to_bits(),
            ))
        }
        Some(MapiPropertyType::Floating64) => {
            let bytes = cursor.read_bytes(8)?;
            Ok(MapiValue::F64(
                f64::from_le_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                ])
                .to_bits(),
            ))
        }
        Some(MapiPropertyType::Error) => Ok(MapiValue::Error(cursor.read_u32()?)),
        Some(MapiPropertyType::Boolean) => Ok(MapiValue::Bool(cursor.read_u8()? != 0)),
        Some(MapiPropertyType::Integer64) | Some(MapiPropertyType::Time) => {
            Ok(MapiValue::I64(cursor.read_i64()?))
        }
        Some(MapiPropertyType::String8) => Ok(MapiValue::String(cursor.read_ascii_z()?)),
        Some(MapiPropertyType::String) => Ok(MapiValue::String(cursor.read_utf16z()?)),
        Some(MapiPropertyType::Guid) => {
            let guid = cursor
                .read_bytes(16)?
                .try_into()
                .map_err(|_| anyhow!("invalid MAPI GUID property value"))?;
            Ok(MapiValue::Guid(guid))
        }
        Some(MapiPropertyType::ServerId | MapiPropertyType::Binary) => {
            let len = cursor.read_u16()? as usize;
            Ok(MapiValue::Binary(cursor.read_bytes(len)?.to_vec()))
        }
        Some(MapiPropertyType::MultipleInteger16) => {
            let count = cursor.read_u32()? as usize;
            let mut values = Vec::new();
            for _ in 0..count {
                values.push(cursor.read_u16()? as i16);
            }
            Ok(MapiValue::MultiI16(values))
        }
        Some(MapiPropertyType::MultipleInteger32) => {
            let count = cursor.read_u32()? as usize;
            let mut values = Vec::new();
            for _ in 0..count {
                values.push(cursor.read_i32()?);
            }
            Ok(MapiValue::MultiI32(values))
        }
        Some(MapiPropertyType::MultipleInteger64) => {
            let count = cursor.read_u32()? as usize;
            let mut values = Vec::new();
            for _ in 0..count {
                values.push(cursor.read_i64()?);
            }
            Ok(MapiValue::MultiI64(values))
        }
        Some(MapiPropertyType::MultipleString8) => {
            let count = cursor.read_u32()? as usize;
            let mut values = Vec::new();
            for _ in 0..count {
                values.push(cursor.read_ascii_z()?);
            }
            Ok(MapiValue::MultiString(values))
        }
        Some(MapiPropertyType::MultipleString) => {
            let count = cursor.read_u32()? as usize;
            let mut values = Vec::new();
            for _ in 0..count {
                values.push(cursor.read_utf16z()?);
            }
            Ok(MapiValue::MultiString(values))
        }
        Some(MapiPropertyType::MultipleGuid) => {
            let count = cursor.read_u32()? as usize;
            let mut values = Vec::new();
            for _ in 0..count {
                let guid = cursor
                    .read_bytes(16)?
                    .try_into()
                    .map_err(|_| anyhow!("invalid MAPI multivalue GUID property value"))?;
                values.push(guid);
            }
            Ok(MapiValue::MultiGuid(values))
        }
        Some(MapiPropertyType::MultipleBinary) => {
            let count = cursor.read_u32()? as usize;
            let mut values = Vec::new();
            for _ in 0..count {
                let len = cursor.read_u16()? as usize;
                values.push(cursor.read_bytes(len)?.to_vec());
            }
            Ok(MapiValue::MultiBinary(values))
        }
        None => {
            let tag = MapiPropertyTag::new(property_tag);
            let known_unsupported_name =
                MapiPropertyType::known_unsupported_name(tag.property_type_code());
            tracing::warn!(
                adapter = "mapi",
                enum_name = "MapiPropertyType",
                raw_value = tag.property_type_code(),
                property_id = tag.property_id(),
                known_unsupported = known_unsupported_name.is_some(),
                known_unsupported_name = known_unsupported_name.unwrap_or(""),
                "unsupported MAPI property type rejected at parser boundary"
            );
            Err(anyhow!(
                "unsupported MAPI property type {:#06X} for property id {:#06X}",
                tag.property_type_code(),
                tag.property_id()
            ))
        }
    }
}

pub(in crate::mapi) fn write_ascii_z(row: &mut Vec<u8>, value: &str) {
    row.extend(
        value
            .bytes()
            .map(|byte| if byte.is_ascii() { byte } else { b'?' }),
    );
    row.push(0);
}

pub(in crate::mapi) fn write_rop_binary(row: &mut Vec<u8>, value: &[u8]) {
    let len = value.len().min(u16::MAX as usize);
    write_u16(row, len as u16);
    row.extend_from_slice(&value[..len]);
}

pub(in crate::mapi) fn write_multi_i16(row: &mut Vec<u8>, values: &[i16]) {
    write_u32(row, values.len().min(u32::MAX as usize) as u32);
    for value in values.iter().take(u32::MAX as usize) {
        write_u16(row, *value as u16);
    }
}

pub(in crate::mapi) fn write_multi_i32(row: &mut Vec<u8>, values: &[i32]) {
    write_u32(row, values.len().min(u32::MAX as usize) as u32);
    for value in values.iter().take(u32::MAX as usize) {
        row.extend_from_slice(&value.to_le_bytes());
    }
}

pub(in crate::mapi) fn write_multi_i64(row: &mut Vec<u8>, values: &[i64]) {
    write_u32(row, values.len().min(u32::MAX as usize) as u32);
    for value in values.iter().take(u32::MAX as usize) {
        row.extend_from_slice(&value.to_le_bytes());
    }
}

pub(in crate::mapi) fn write_multi_string8(row: &mut Vec<u8>, values: &[String]) {
    write_u32(row, values.len().min(u32::MAX as usize) as u32);
    for value in values.iter().take(u32::MAX as usize) {
        write_ascii_z(row, value);
    }
}

pub(in crate::mapi) fn write_multi_string(row: &mut Vec<u8>, values: &[String]) {
    write_u32(row, values.len().min(u32::MAX as usize) as u32);
    for value in values.iter().take(u32::MAX as usize) {
        write_utf16z(row, value);
    }
}

pub(in crate::mapi) fn write_multi_guid(row: &mut Vec<u8>, values: &[[u8; 16]]) {
    write_u32(row, values.len().min(u32::MAX as usize) as u32);
    for value in values.iter().take(u32::MAX as usize) {
        row.extend_from_slice(value);
    }
}

pub(in crate::mapi) fn write_multi_binary(row: &mut Vec<u8>, values: &[Vec<u8>]) {
    write_u32(row, values.len().min(u32::MAX as usize) as u32);
    for value in values.iter().take(u32::MAX as usize) {
        write_rop_binary(row, value);
    }
}

pub(in crate::mapi) fn write_named_property(row: &mut Vec<u8>, property: &MapiNamedProperty) {
    match &property.kind {
        MapiNamedPropertyKind::Lid(lid) => {
            row.push(0x00);
            row.extend_from_slice(&property.guid);
            write_u32(row, *lid);
        }
        MapiNamedPropertyKind::Name(name) => {
            row.push(0x01);
            row.extend_from_slice(&property.guid);
            let mut name_bytes = name
                .encode_utf16()
                .flat_map(u16::to_le_bytes)
                .collect::<Vec<_>>();
            name_bytes.extend_from_slice(&0u16.to_le_bytes());
            let size = name_bytes.len().min(u8::MAX as usize);
            row.push(size as u8);
            row.extend_from_slice(&name_bytes[..size]);
        }
    }
}
