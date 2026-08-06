use super::*;

#[derive(Default)]
pub(in crate::mapi::dispatch) struct HierarchyResponseMetricSummary {
    pub(in crate::mapi::dispatch) has_conversation_action: bool,
    pub(in crate::mapi::dispatch) has_quick_step: bool,
}

fn parse_hierarchy_property_row_value(
    cursor: &mut Cursor<'_>,
    row_status: u8,
    property_tag: u32,
) -> Result<Option<MapiValue>> {
    // [MS-OXCDATA] sections 2.8.1.1, 2.8.1.2, and 2.11.5: a
    // FlaggedPropertyRow prefixes each value with its availability or error.
    match row_status {
        0x00 => parse_mapi_property_value(cursor, property_tag).map(Some),
        0x01 => match cursor.read_u8()? {
            0x00 => parse_mapi_property_value(cursor, property_tag).map(Some),
            0x01 => Ok(None),
            0x0A => Ok(Some(MapiValue::Error(cursor.read_u32()?))),
            flag => Err(anyhow!(
                "invalid flagged hierarchy property value {flag:#04x}"
            )),
        },
        status => Err(anyhow!(
            "invalid hierarchy property row status {status:#04x}"
        )),
    }
}

pub(in crate::mapi::dispatch) fn hierarchy_response_metric_summary(
    response: &[u8],
    selected_columns: &[u32],
) -> HierarchyResponseMetricSummary {
    let row_count = response
        .get(7..9)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u16::from_le_bytes)
        .unwrap_or(0) as usize;
    let mut cursor = Cursor::new(response.get(9..).unwrap_or_default());
    let mut summary = HierarchyResponseMetricSummary::default();

    for _ in 0..row_count {
        let row_status = match cursor.read_u8() {
            Ok(status) => status,
            Err(_) => break,
        };
        for column in selected_columns {
            let value = match parse_hierarchy_property_row_value(&mut cursor, row_status, *column) {
                Ok(Some(value)) => value,
                Ok(None) => continue,
                Err(_) => return summary,
            };
            if *column == PID_TAG_FOLDER_ID {
                match hierarchy_metric_folder_id(&value) {
                    Some(CONVERSATION_ACTION_SETTINGS_FOLDER_ID) => {
                        summary.has_conversation_action = true;
                    }
                    Some(QUICK_STEP_SETTINGS_FOLDER_ID) => {
                        summary.has_quick_step = true;
                    }
                    _ => {}
                }
            }
        }
    }

    summary
}

fn hierarchy_metric_folder_id(value: &MapiValue) -> Option<u64> {
    let raw = match value {
        MapiValue::I64(value) if *value >= 0 => *value as u64,
        MapiValue::U64(value) => *value,
        MapiValue::I32(value) if *value >= 0 => *value as u64,
        MapiValue::U32(value) => u64::from(*value),
        _ => return None,
    };
    let bytes = raw.to_le_bytes();
    crate::mapi::identity::object_id_from_wire_id(&bytes)
        .or_else(|| crate::mapi::identity::object_id_from_trailing_replid_wire_id(&bytes))
        .or(Some(raw))
}

pub(in crate::mapi::dispatch) fn format_hierarchy_query_rows_wire_summary(
    response: &[u8],
    selected_columns: &[u32],
    max_rows: usize,
) -> String {
    if selected_columns.is_empty() {
        return String::new();
    }
    let row_count = response
        .get(7..9)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u16::from_le_bytes)
        .unwrap_or(0) as usize;
    if row_count == 0 {
        return "total=0;decoded=0".to_string();
    }

    let mut cursor = Cursor::new(response.get(9..).unwrap_or_default());
    let decode_count = row_count.min(max_rows);
    let mut rows = Vec::new();
    for row_index in 0..decode_count {
        let row_status = match cursor.read_u8() {
            Ok(status) => status,
            Err(error) => {
                rows.push(format!("index={row_index};row_status=parse_error={error}"));
                break;
            }
        };
        let mut values = HashMap::new();
        let mut parse_error = String::new();
        for column in selected_columns {
            match parse_hierarchy_property_row_value(&mut cursor, row_status, *column) {
                Ok(Some(value)) => {
                    values.insert(*column, value);
                }
                Ok(None) => {}
                Err(error) => {
                    parse_error = format!("parse_error={error}");
                    break;
                }
            }
        }
        rows.push(format!(
            "index={row_index};row_status=0x{row_status:02x};id={};class={};name={};count={};type={};hidden={};subfolders={};{}",
            format_hierarchy_debug_folder_id(values.get(&PID_TAG_FOLDER_ID)),
            format_hierarchy_debug_string(values.get(&PID_TAG_CONTAINER_CLASS_W)),
            format_hierarchy_debug_string(values.get(&PID_TAG_DISPLAY_NAME_W)),
            format_hierarchy_debug_count(values.get(&PID_TAG_CONTENT_COUNT)),
            format_hierarchy_debug_count(values.get(&PID_TAG_FOLDER_TYPE)),
            format_hierarchy_debug_bool(values.get(&PID_TAG_ATTRIBUTE_HIDDEN)),
            format_hierarchy_debug_bool(values.get(&PID_TAG_SUBFOLDERS)),
            parse_error
        ));
        if !parse_error.is_empty() {
            break;
        }
    }

    format!(
        "total={row_count};decoded={};truncated={};remaining_bytes={};{}",
        rows.len(),
        row_count > rows.len(),
        cursor.remaining(),
        rows.join("|")
    )
}

fn format_hierarchy_debug_folder_id(value: Option<&MapiValue>) -> String {
    match value {
        Some(MapiValue::I64(value)) if *value >= 0 => {
            format_hierarchy_debug_wire_folder_id(*value as u64)
        }
        Some(MapiValue::U64(value)) => format_hierarchy_debug_wire_folder_id(*value),
        Some(MapiValue::I32(value)) if *value >= 0 => format!("0x{:016x}", *value as u64),
        Some(MapiValue::U32(value)) => format!("0x{:016x}", u64::from(*value)),
        Some(value) => mapi_value_debug_shape(value),
        None => "missing".to_string(),
    }
}

fn format_hierarchy_debug_wire_folder_id(value: u64) -> String {
    let bytes = value.to_le_bytes();
    crate::mapi::identity::object_id_from_wire_id(&bytes)
        .or_else(|| crate::mapi::identity::object_id_from_trailing_replid_wire_id(&bytes))
        .map(|folder_id| format!("0x{folder_id:016x}"))
        .unwrap_or_else(|| format!("0x{value:016x}"))
}

fn format_hierarchy_debug_string(value: Option<&MapiValue>) -> String {
    match value {
        Some(MapiValue::String(value)) => format_debug_text_value(value),
        Some(value) => mapi_value_debug_shape(value),
        None => "missing".to_string(),
    }
}

fn format_hierarchy_debug_count(value: Option<&MapiValue>) -> String {
    match value {
        Some(MapiValue::I32(value)) => value.to_string(),
        Some(MapiValue::U32(value)) => value.to_string(),
        Some(value) => mapi_value_debug_shape(value),
        None => "missing".to_string(),
    }
}

fn format_hierarchy_debug_bool(value: Option<&MapiValue>) -> String {
    match value {
        Some(MapiValue::Bool(value)) => value.to_string(),
        Some(value) => mapi_value_debug_shape(value),
        None => "missing".to_string(),
    }
}
