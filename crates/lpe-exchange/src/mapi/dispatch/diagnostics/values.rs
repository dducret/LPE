use super::*;

pub(in crate::mapi::dispatch) fn mapi_value_debug_string(
    properties: &HashMap<u32, MapiValue>,
    tag: u32,
) -> String {
    match properties.get(&tag) {
        Some(MapiValue::String(value)) => value.clone(),
        Some(value) => mapi_value_debug_shape(value),
        None => "missing".to_string(),
    }
}

pub(in crate::mapi::dispatch) fn mapi_value_debug_u32(
    properties: &HashMap<u32, MapiValue>,
    tag: u32,
) -> String {
    match properties.get(&tag) {
        Some(MapiValue::U32(value)) => value.to_string(),
        Some(MapiValue::I32(value)) => value.to_string(),
        Some(value) => mapi_value_debug_shape(value),
        None => "missing".to_string(),
    }
}

pub(in crate::mapi::dispatch) fn mapi_value_debug_bool(
    properties: &HashMap<u32, MapiValue>,
    tag: u32,
) -> String {
    match properties.get(&tag) {
        Some(MapiValue::Bool(value)) => value.to_string(),
        Some(value) => mapi_value_debug_shape(value),
        None => "missing".to_string(),
    }
}

pub(in crate::mapi::dispatch) fn mapi_value_debug_binary_decode(
    properties: &HashMap<u32, MapiValue>,
    tag: u32,
) -> String {
    match properties.get(&tag) {
        Some(MapiValue::Binary(bytes)) => {
            let decoded_folder_id = object_id_from_source_key(bytes)
                .or_else(|| object_id_from_folder_identifier_bytes(bytes));
            format!(
                "bytes={};decoded={}",
                bytes.len(),
                format_optional_folder_id(decoded_folder_id)
            )
        }
        Some(value) => mapi_value_debug_shape(value),
        None => "missing".to_string(),
    }
}

pub(in crate::mapi::dispatch) fn format_optional_folder_id(folder_id: Option<u64>) -> String {
    folder_id
        .map(|folder_id| format!("0x{folder_id:016x}"))
        .unwrap_or_default()
}

pub(in crate::mapi::dispatch) fn mapi_value_debug_shape(value: &MapiValue) -> String {
    match value {
        MapiValue::Bool(_) => "bool".to_string(),
        MapiValue::I16(_) => "i16".to_string(),
        MapiValue::I32(_) => "i32".to_string(),
        MapiValue::I64(_) => "i64".to_string(),
        MapiValue::F64(_) => "f64".to_string(),
        MapiValue::U32(_) => "u32".to_string(),
        MapiValue::U64(_) => "u64".to_string(),
        MapiValue::String(value) => format!("string:chars={}", value.chars().count()),
        MapiValue::Binary(value) => {
            format!(
                "binary:bytes={}:preview={}",
                value.len(),
                hex_preview(value, 32)
            )
        }
        MapiValue::Guid(_) => "guid".to_string(),
        MapiValue::Error(error) => format!("error:{error:#010x}"),
        MapiValue::MultiI16(value) => format!("multi_i16:count={}", value.len()),
        MapiValue::MultiI32(value) => format!("multi_i32:count={}", value.len()),
        MapiValue::MultiI64(value) => format!("multi_i64:count={}", value.len()),
        MapiValue::MultiString(value) => format!("multi_string:count={}", value.len()),
        MapiValue::MultiBinary(value) => format!("multi_binary:count={}", value.len()),
        MapiValue::MultiGuid(value) => format!("multi_guid:count={}", value.len()),
    }
}

pub(in crate::mapi::dispatch) fn mapi_value_debug_u32_from_value(value: &MapiValue) -> String {
    match value {
        MapiValue::U32(value) => format!("{value:#010x}"),
        MapiValue::I32(value) => format!("{value:#010x}"),
        _ => mapi_value_debug_shape(value),
    }
}

pub(in crate::mapi::dispatch) fn format_inbox_folder_type_getprops_response_context(
    response: &[u8],
) -> String {
    let return_value = response
        .get(2..6)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_le_bytes)
        .map(|value| format!("0x{value:08x}"))
        .unwrap_or_else(|| "truncated".to_string());
    let row_preview = if response.len() > 6 {
        hex_preview(&response[6..], 64)
    } else {
        String::new()
    };
    format!(
        "response_bytes={};return_value={};row_bytes={};row_preview={}",
        response.len(),
        return_value,
        response.len().saturating_sub(6),
        row_preview
    )
}

pub(in crate::mapi::dispatch) fn debug_context_or_none(context: &str) -> &str {
    if context.is_empty() {
        "none"
    } else {
        context
    }
}
