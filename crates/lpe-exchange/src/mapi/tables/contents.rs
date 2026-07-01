use super::*;
use lpe_domain::crypto::hex_lower;

#[derive(Clone)]
pub(super) struct CategorizedTableRow {
    pub(super) category_id: u64,
    pub(super) leaf_count: usize,
    pub(super) row: Vec<u8>,
    pub(super) leaf: bool,
}

pub(super) const TABLE_EXPANDED_CATEGORY: u32 = 0x0000_0003;
pub(super) const TABLE_COLLAPSED_CATEGORY: u32 = 0x0000_0004;

pub(super) fn category_id_for_value(folder_id: u64, property_tag: u32, value: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in folder_id
        .to_le_bytes()
        .into_iter()
        .chain(property_tag.to_le_bytes())
        .chain(value.as_bytes().iter().copied())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01B3);
    }
    hash | 0x8000_0000_0000_0000
}

fn category_values_for_email(email: &JmapEmail, property_tag: u32) -> Vec<String> {
    let storage_tag = canonical_property_storage_tag(property_tag);
    if named_property_id_matches(storage_tag, PID_NAME_KEYWORDS_TAG) {
        let values = email
            .categories
            .iter()
            .map(|category| category.trim())
            .filter(|category| !category.is_empty())
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        if values.is_empty() {
            vec![String::new()]
        } else {
            values
        }
    } else {
        match email_property_value(email, storage_tag) {
            Some(value) => category_values_from_mapi_value(value),
            None => vec![String::new()],
        }
    }
}

pub(super) fn category_values_from_mapi_value(value: MapiValue) -> Vec<String> {
    match value {
        MapiValue::MultiString(values) => {
            let values = values
                .into_iter()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .collect::<Vec<_>>();
            if values.is_empty() {
                vec![String::new()]
            } else {
                values
            }
        }
        value => vec![category_value_to_string(&value)],
    }
}

fn named_property_id_matches(left: u32, right: u32) -> bool {
    (left & 0xFFFF_0000) == (right & 0xFFFF_0000)
}

fn category_value_to_string(value: &MapiValue) -> String {
    match value {
        MapiValue::Bool(value) => value.to_string(),
        MapiValue::I16(value) => value.to_string(),
        MapiValue::I32(value) => value.to_string(),
        MapiValue::I64(value) => value.to_string(),
        MapiValue::F64(value) => f64::from_bits(*value).to_string(),
        MapiValue::U32(value) => value.to_string(),
        MapiValue::U64(value) => value.to_string(),
        MapiValue::String(value) => value.clone(),
        MapiValue::MultiString(values) => values.first().cloned().unwrap_or_default(),
        MapiValue::Binary(value) => format_bytes_hex(value),
        MapiValue::Guid(value) => format_bytes_hex(value),
        MapiValue::Error(value) => format!("{value:#010x}"),
        MapiValue::MultiI16(values) => values.first().map(i16::to_string).unwrap_or_default(),
        MapiValue::MultiI32(values) => values.first().map(i32::to_string).unwrap_or_default(),
        MapiValue::MultiI64(values) => values.first().map(i64::to_string).unwrap_or_default(),
        MapiValue::MultiBinary(values) => values
            .first()
            .map(|value| format_bytes_hex(value))
            .unwrap_or_default(),
        MapiValue::MultiGuid(values) => values
            .first()
            .map(|value| format_bytes_hex(value))
            .unwrap_or_default(),
    }
}

fn format_bytes_hex(bytes: &[u8]) -> String {
    hex_lower(bytes)
}

pub(super) fn categorized_email_rows(
    folder_id: u64,
    emails: Vec<&JmapEmail>,
    columns: &[u32],
    sort_orders: &[MapiSortOrder],
    expanded_count: u16,
    collapsed_categories: &HashSet<u64>,
) -> Vec<CategorizedTableRow> {
    let Some(category_sort) = sort_orders.first() else {
        return emails
            .into_iter()
            .map(|email| CategorizedTableRow {
                category_id: 0,
                leaf_count: 1,
                row: serialize_message_row(email, columns),
                leaf: true,
            })
            .collect();
    };
    let mut groups: Vec<(u64, String, Vec<(&JmapEmail, u32)>)> = Vec::new();
    for email in emails {
        for (instance, value) in category_values_for_email(email, category_sort.property_tag)
            .into_iter()
            .enumerate()
        {
            let category_id = category_id_for_value(folder_id, category_sort.property_tag, &value);
            let instance_num = instance.saturating_add(1).min(u32::MAX as usize) as u32;
            if let Some((_, _, rows)) = groups.iter_mut().find(|(id, _, _)| *id == category_id) {
                rows.push((email, instance_num));
            } else {
                groups.push((category_id, value, vec![(email, instance_num)]));
            }
        }
    }
    groups.sort_by(|left, right| {
        apply_sort_direction(
            compare_case_insensitive(&left.1, &right.1),
            category_sort.order,
        )
    });

    let mut rows = Vec::new();
    for (category_id, value, leaves) in groups {
        let expanded = expanded_count > 0 && !collapsed_categories.contains(&category_id);
        let unread_count = leaves.iter().filter(|(email, _)| email.unread).count();
        rows.push(CategorizedTableRow {
            category_id,
            leaf_count: leaves.len(),
            row: serialize_category_header_row(
                category_id,
                &value,
                leaves.len(),
                unread_count,
                category_sort.property_tag,
                expanded,
                columns,
            ),
            leaf: false,
        });
        if expanded {
            rows.extend(
                leaves
                    .into_iter()
                    .map(|(email, instance_num)| CategorizedTableRow {
                        category_id,
                        leaf_count: 1,
                        row: serialize_categorized_message_row(
                            email,
                            columns,
                            category_sort.property_tag,
                            &value,
                            instance_num,
                        ),
                        leaf: true,
                    }),
            );
        }
    }
    rows
}

fn serialize_category_header_row(
    category_id: u64,
    value: &str,
    leaf_count: usize,
    unread_count: usize,
    category_property_tag: u32,
    expanded: bool,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match canonical_property_storage_tag(*column) {
            PID_TAG_INST_ID => write_u64(&mut row, category_id),
            PID_TAG_INSTANCE_NUM => write_u32(&mut row, 0),
            PID_TAG_ROW_TYPE => write_u32(
                &mut row,
                if expanded {
                    TABLE_EXPANDED_CATEGORY
                } else {
                    TABLE_COLLAPSED_CATEGORY
                },
            ),
            PID_TAG_DEPTH => write_u32(&mut row, 0),
            PID_TAG_CONTENT_COUNT => write_u32(&mut row, leaf_count.min(u32::MAX as usize) as u32),
            PID_TAG_CONTENT_UNREAD_COUNT => {
                write_u32(&mut row, unread_count.min(u32::MAX as usize) as u32)
            }
            tag if tag == canonical_property_storage_tag(category_property_tag) => {
                write_category_instance_value(&mut row, *column, value)
            }
            _ => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(super) fn write_category_instance_value(row: &mut Vec<u8>, property_tag: u32, value: &str) {
    let value = match MapiPropertyTag::new(property_tag).property_type() {
        Some(MapiPropertyType::MultipleString | MapiPropertyType::MultipleString8) => {
            MapiValue::MultiString(vec![value.to_string()])
        }
        _ => MapiValue::String(value.to_string()),
    };
    write_mapi_value(row, property_tag, &value);
}
