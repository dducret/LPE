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
        MapiValue::Null => String::new(),
        MapiValue::Bool(value) => value.to_string(),
        MapiValue::I16(value) => value.to_string(),
        MapiValue::I32(value) => value.to_string(),
        MapiValue::I64(value) => value.to_string(),
        MapiValue::F64(value) => f64::from_bits(*value).to_string(),
        MapiValue::U32(value) => value.to_string(),
        MapiValue::U64(value) => value.to_string(),
        MapiValue::String(value) => value.clone(),
        MapiValue::MultiString(values) => values.first().cloned().unwrap_or_default(),
        MapiValue::Binary(value) => hex_lower(value),
        MapiValue::Guid(value) => hex_lower(value),
        MapiValue::Error(value) => format!("{value:#010x}"),
        MapiValue::MultiI16(values) => values.first().map(i16::to_string).unwrap_or_default(),
        MapiValue::MultiI32(values) => values.first().map(i32::to_string).unwrap_or_default(),
        MapiValue::MultiI64(values) => values.first().map(i64::to_string).unwrap_or_default(),
        MapiValue::MultiBinary(values) => values.first().map(hex_lower).unwrap_or_default(),
        MapiValue::MultiGuid(values) => values.first().map(hex_lower).unwrap_or_default(),
    }
}

pub(in crate::mapi) fn serialize_message_row(email: &JmapEmail, columns: &[u32]) -> Vec<u8> {
    serialize_message_row_with_table_instance(email, columns, 0, 0, None)
}

fn serialize_categorized_message_row(
    email: &JmapEmail,
    columns: &[u32],
    category_property_tag: u32,
    category_value: &str,
    instance_num: u32,
) -> Vec<u8> {
    serialize_message_row_with_table_instance(
        email,
        columns,
        instance_num,
        1,
        Some((category_property_tag, category_value)),
    )
}

fn serialize_message_row_with_table_instance(
    email: &JmapEmail,
    columns: &[u32],
    instance_num: u32,
    depth: u32,
    category_value: Option<(u32, &str)>,
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        let storage_tag = canonical_property_storage_tag(*column);
        if let Some((category_property_tag, value)) = category_value {
            if storage_tag == canonical_property_storage_tag(category_property_tag) {
                write_category_instance_value(&mut row, *column, value);
                continue;
            }
        }
        match *column {
            PID_TAG_FOLDER_ID | PID_TAG_PARENT_FOLDER_ID => {
                write_object_id(&mut row, mapi_folder_id_for_email(email))
            }
            PID_TAG_MID => write_object_id(&mut row, mapi_message_id(email)),
            PID_TAG_INST_ID => write_u64(&mut row, mapi_message_id(email)),
            PID_TAG_INSTANCE_NUM => write_u32(&mut row, instance_num),
            PID_TAG_ROW_TYPE => write_u32(&mut row, TABLE_LEAF_ROW),
            PID_TAG_DEPTH => write_u32(&mut row, depth),
            PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
                write_utf16z(&mut row, &email.subject)
            }
            PID_TAG_MESSAGE_CLASS_W | PID_TAG_ORIGINAL_MESSAGE_CLASS_W => {
                write_utf16z(&mut row, message_class_for_email(email))
            }
            PID_TAG_CREATION_TIME
            | PID_TAG_MESSAGE_DELIVERY_TIME
            | PID_TAG_LAST_MODIFICATION_TIME
            | PID_TAG_LOCAL_COMMIT_TIME => write_u64(
                &mut row,
                mapi_mailstore::filetime_from_rfc3339_utc(&email.received_at),
            ),
            PID_TAG_CLIENT_SUBMIT_TIME => {
                write_u64(&mut row, email_client_submit_time_filetime(email))
            }
            PID_TAG_ACCESS => write_u32(&mut row, MAPI_MESSAGE_ACCESS),
            PID_TAG_ACCESS_LEVEL => write_u32(&mut row, 1),
            PID_TAG_IMPORTANCE => write_u32(&mut row, 1),
            PID_TAG_PRIORITY | PID_TAG_SENSITIVITY => write_u32(&mut row, 0),
            PID_TAG_SUBJECT_PREFIX_W => write_utf16z(&mut row, ""),
            PID_TAG_MESSAGE_FLAGS => write_u32(&mut row, message_flags(email)),
            PID_TAG_READ => row.push((!email.unread) as u8),
            PID_TAG_MESSAGE_SIZE => {
                write_u32(&mut row, email.size_octets.clamp(0, u32::MAX as i64) as u32)
            }
            PID_TAG_MESSAGE_SIZE_EXTENDED => write_u64(&mut row, email.size_octets.max(0) as u64),
            PID_TAG_SENDER_NAME_W => write_utf16z(&mut row, email_sender_name(email)),
            PID_TAG_SENDER_ADDRESS_TYPE_W => write_utf16z(&mut row, "SMTP"),
            PID_TAG_SENDER_EMAIL_ADDRESS_W | PID_TAG_SENDER_SMTP_ADDRESS_W => {
                write_utf16z(&mut row, email_sender_address(email))
            }
            PID_TAG_SENT_REPRESENTING_NAME_W => {
                write_utf16z(&mut row, email_sent_representing_name(email))
            }
            PID_TAG_SENT_REPRESENTING_ENTRY_ID => {
                write_u16_prefixed_bytes(&mut row, &sent_representing_entry_id(email))
            }
            PID_TAG_SENT_REPRESENTING_ADDRESS_TYPE_W => write_utf16z(&mut row, "SMTP"),
            PID_TAG_SENT_REPRESENTING_EMAIL_ADDRESS_W
            | PID_TAG_SENT_REPRESENTING_SMTP_ADDRESS_W => {
                write_utf16z(&mut row, email_sent_representing_address(email))
            }
            PID_TAG_DISPLAY_TO_W => write_utf16z(&mut row, &display_to(email)),
            PID_TAG_DISPLAY_CC_W => write_utf16z(&mut row, &display_cc(email)),
            PID_TAG_DISPLAY_BCC_W => write_utf16z(&mut row, &display_bcc(email)),
            PID_TAG_HAS_ATTACHMENTS => row.push(email.has_attachments as u8),
            PID_TAG_RTF_IN_SYNC => row.push(0),
            PID_TAG_BODY_W => write_utf16z(&mut row, &email.body_text),
            PID_TAG_RTF_COMPRESSED => {
                write_u16_prefixed_bytes(&mut row, &uncompressed_rtf_body(&email.body_text))
            }
            PID_TAG_NATIVE_BODY => write_u32(&mut row, native_body_format(email)),
            PID_TAG_INTERNET_CODEPAGE => write_u32(&mut row, 65001),
            PID_TAG_MESSAGE_LOCALE_ID => write_u32(&mut row, 0x0409),
            PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &crate::mapi::identity::instance_key_for_object_id(mapi_message_id(email)),
            ),
            PID_TAG_INTERNET_MESSAGE_ID_W => {
                write_utf16z(&mut row, email.internet_message_id.as_deref().unwrap_or(""))
            }
            PID_NAME_CONTENT_CLASS_W_TAG => write_utf16z(&mut row, "urn:content-classes:message"),
            _ => match email_property_value(email, *column) {
                Some(value) => write_mapi_value(&mut row, *column, &value),
                None => write_property_default(&mut row, *column),
            },
        }
    }
    row
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
