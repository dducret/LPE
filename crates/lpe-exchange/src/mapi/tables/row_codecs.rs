use super::*;

pub(in crate::mapi) fn write_standard_property_row(response: &mut Vec<u8>, values: &[u8]) {
    response.push(0);
    response.extend_from_slice(values);
}

pub(super) const QUERY_ROWS_MAX_PROPERTY_VALUE_BYTES: usize = 510;

pub(super) fn write_query_rows_property_row(
    response: &mut Vec<u8>,
    columns: &[u32],
    values: &[u8],
) {
    response.push(0);
    let mut offset = 0usize;
    for column in columns {
        match write_query_rows_property_value(response, *column, values, offset) {
            Some(next_offset) => offset = next_offset,
            None => {
                response.extend_from_slice(values.get(offset..).unwrap_or_default());
                return;
            }
        }
    }
    response.extend_from_slice(values.get(offset..).unwrap_or_default());
}

pub(in crate::mapi) fn query_rows_property_row_bytes(_columns: &[u32], values: &[u8]) -> Vec<u8> {
    standard_property_row_bytes(values)
}

pub(in crate::mapi) fn standard_property_row_bytes(values: &[u8]) -> Vec<u8> {
    let mut row = Vec::with_capacity(values.len().saturating_add(1));
    write_standard_property_row(&mut row, values);
    row
}

fn write_query_rows_property_value(
    response: &mut Vec<u8>,
    property_tag: u32,
    values: &[u8],
    offset: usize,
) -> Option<usize> {
    let property_type = MapiPropertyTag::new(property_tag).property_type()?;
    match property_type {
        MapiPropertyType::Integer16 => {
            write_fixed_query_rows_property_value(response, values, offset, 2)
        }
        MapiPropertyType::Integer32 | MapiPropertyType::Floating32 | MapiPropertyType::Error => {
            write_fixed_query_rows_property_value(response, values, offset, 4)
        }
        MapiPropertyType::Boolean => {
            write_fixed_query_rows_property_value(response, values, offset, 1)
        }
        MapiPropertyType::Floating64 | MapiPropertyType::Integer64 | MapiPropertyType::Time => {
            write_fixed_query_rows_property_value(response, values, offset, 8)
        }
        MapiPropertyType::Guid => {
            write_fixed_query_rows_property_value(response, values, offset, 16)
        }
        MapiPropertyType::String8 => write_query_rows_string8_value(response, values, offset),
        MapiPropertyType::String => write_query_rows_utf16_value(response, values, offset),
        MapiPropertyType::ServerId | MapiPropertyType::Binary => {
            write_query_rows_binary_value(response, values, offset)
        }
        MapiPropertyType::MultipleInteger16 => {
            write_counted_fixed_query_rows_property_values(response, values, offset, 2)
        }
        MapiPropertyType::MultipleInteger32 => {
            write_counted_fixed_query_rows_property_values(response, values, offset, 4)
        }
        MapiPropertyType::MultipleInteger64 => {
            write_counted_fixed_query_rows_property_values(response, values, offset, 8)
        }
        MapiPropertyType::MultipleGuid => {
            write_counted_fixed_query_rows_property_values(response, values, offset, 16)
        }
        MapiPropertyType::MultipleString8 => {
            write_counted_query_rows_string_values(response, values, offset, false)
        }
        MapiPropertyType::MultipleString => {
            write_counted_query_rows_string_values(response, values, offset, true)
        }
        MapiPropertyType::MultipleBinary => {
            write_counted_query_rows_binary_values(response, values, offset)
        }
    }
}

fn write_fixed_query_rows_property_value(
    response: &mut Vec<u8>,
    values: &[u8],
    offset: usize,
    size: usize,
) -> Option<usize> {
    let end = offset.checked_add(size)?;
    response.extend_from_slice(values.get(offset..end)?);
    Some(end)
}

fn write_query_rows_string8_value(
    response: &mut Vec<u8>,
    values: &[u8],
    offset: usize,
) -> Option<usize> {
    let remaining = values.get(offset..)?;
    let end = remaining
        .iter()
        .position(|byte| *byte == 0)
        .map(|position| offset + position + 1)
        .unwrap_or(values.len());
    let segment = values.get(offset..end)?;
    if segment.len() <= QUERY_ROWS_MAX_PROPERTY_VALUE_BYTES {
        response.extend_from_slice(segment);
    } else {
        response.extend_from_slice(&segment[..QUERY_ROWS_MAX_PROPERTY_VALUE_BYTES - 1]);
        response.push(0);
    }
    Some(end)
}

fn write_query_rows_utf16_value(
    response: &mut Vec<u8>,
    values: &[u8],
    offset: usize,
) -> Option<usize> {
    let remaining = values.get(offset..)?;
    let mut relative_end = remaining.len();
    let mut index = 0usize;
    while index + 1 < remaining.len() {
        if remaining[index] == 0 && remaining[index + 1] == 0 {
            relative_end = index + 2;
            break;
        }
        index += 2;
    }
    let end = offset.checked_add(relative_end)?;
    let segment = values.get(offset..end)?;
    if segment.len() <= QUERY_ROWS_MAX_PROPERTY_VALUE_BYTES {
        response.extend_from_slice(segment);
    } else {
        response.extend_from_slice(&segment[..QUERY_ROWS_MAX_PROPERTY_VALUE_BYTES - 2]);
        response.extend_from_slice(&0u16.to_le_bytes());
    }
    Some(end)
}

fn write_query_rows_binary_value(
    response: &mut Vec<u8>,
    values: &[u8],
    offset: usize,
) -> Option<usize> {
    let size_bytes = values.get(offset..offset + 2)?;
    let size = u16::from_le_bytes(size_bytes.try_into().ok()?) as usize;
    let value_offset = offset + 2;
    let end = value_offset.checked_add(size)?;
    let value = values.get(value_offset..end)?;
    let truncated_size = value.len().min(QUERY_ROWS_MAX_PROPERTY_VALUE_BYTES);
    response.extend_from_slice(&(truncated_size as u16).to_le_bytes());
    response.extend_from_slice(&value[..truncated_size]);
    Some(end)
}

fn write_counted_fixed_query_rows_property_values(
    response: &mut Vec<u8>,
    values: &[u8],
    offset: usize,
    value_size: usize,
) -> Option<usize> {
    let count_bytes = values.get(offset..offset + 4)?;
    let count = u32::from_le_bytes(count_bytes.try_into().ok()?) as usize;
    let size = 4usize.checked_add(count.checked_mul(value_size)?)?;
    write_fixed_query_rows_property_value(response, values, offset, size)
}

fn write_counted_query_rows_string_values(
    response: &mut Vec<u8>,
    values: &[u8],
    offset: usize,
    unicode: bool,
) -> Option<usize> {
    let count_bytes = values.get(offset..offset + 4)?;
    let count = u32::from_le_bytes(count_bytes.try_into().ok()?) as usize;
    response.extend_from_slice(count_bytes);
    let mut current = offset + 4;
    for _ in 0..count {
        current = if unicode {
            write_query_rows_utf16_value(response, values, current)?
        } else {
            write_query_rows_string8_value(response, values, current)?
        };
    }
    Some(current)
}

fn write_counted_query_rows_binary_values(
    response: &mut Vec<u8>,
    values: &[u8],
    offset: usize,
) -> Option<usize> {
    let count_bytes = values.get(offset..offset + 4)?;
    let count = u32::from_le_bytes(count_bytes.try_into().ok()?) as usize;
    response.extend_from_slice(count_bytes);
    let mut current = offset + 4;
    for _ in 0..count {
        current = write_query_rows_binary_value(response, values, current)?;
    }
    Some(current)
}

pub(in crate::mapi) fn write_property_default(row: &mut Vec<u8>, property_tag: u32) {
    match MapiPropertyTag::new(property_tag).property_type() {
        Some(MapiPropertyType::Integer16) => write_u16(row, 0),
        Some(MapiPropertyType::Integer32) | Some(MapiPropertyType::Error) => write_u32(row, 0),
        Some(MapiPropertyType::Floating32) => row.extend_from_slice(&0.0f32.to_le_bytes()),
        Some(MapiPropertyType::Floating64) => row.extend_from_slice(&0.0f64.to_le_bytes()),
        Some(MapiPropertyType::Boolean) => row.push(0),
        Some(MapiPropertyType::Integer64) | Some(MapiPropertyType::Time) => write_u64(row, 0),
        Some(MapiPropertyType::String8) => write_ascii_z(row, ""),
        Some(MapiPropertyType::String) => write_utf16z(row, ""),
        Some(MapiPropertyType::Guid) => row.extend_from_slice(Uuid::nil().as_bytes()),
        Some(MapiPropertyType::ServerId | MapiPropertyType::Binary) => write_rop_binary(row, &[]),
        Some(
            MapiPropertyType::MultipleInteger16
            | MapiPropertyType::MultipleInteger32
            | MapiPropertyType::MultipleInteger64
            | MapiPropertyType::MultipleString8
            | MapiPropertyType::MultipleString
            | MapiPropertyType::MultipleGuid
            | MapiPropertyType::MultipleBinary,
        ) => write_u32(row, 0),
        _ => write_u32(row, 0x8004_0102),
    }
}
