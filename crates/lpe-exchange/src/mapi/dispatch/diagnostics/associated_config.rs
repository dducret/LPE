use super::super::*;

pub(in crate::mapi::dispatch) fn associated_config_debug_fields(
    session: &MapiSession,
    snapshot: &MapiMailStoreSnapshot,
    handle: u32,
) -> (String, String, String) {
    let Some(MapiObject::AssociatedConfig {
        folder_id,
        config_id,
        saved_message,
    }) = session.handles.get(&handle)
    else {
        return ("none".to_string(), "none".to_string(), "none".to_string());
    };
    snapshot
        .associated_config_message_for_id(*config_id)
        .or_else(|| saved_message.clone())
        .filter(|message| message.folder_id == *folder_id)
        .map(|message| {
            (
                format!("0x{:016x}", message.id),
                message.message_class,
                message.subject,
            )
        })
        .unwrap_or_else(|| {
            (
                format!("0x{config_id:016x}"),
                "missing".to_string(),
                "missing".to_string(),
            )
        })
}

pub(in crate::mapi::dispatch) fn associated_config_open_shape(
    message: &crate::mapi_store::MapiAssociatedConfigMessage,
) -> String {
    let actions_len =
        associated_config_binary_property_len(message, PID_TAG_EXTENDED_RULE_MESSAGE_ACTIONS);
    let condition_len =
        associated_config_binary_property_len(message, PID_TAG_EXTENDED_RULE_MESSAGE_CONDITION);
    let has_undocumented_0e0b =
        associated_config_property_value(message, OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B).is_some();
    format!(
        "class={};subject={};properties={};extended_rule_actions_len={};extended_rule_condition_len={};has_0e0b={}",
        message.message_class,
        message.subject,
        mapi_properties_from_json(&message.properties_json).len(),
        actions_len
            .map(|len| len.to_string())
            .unwrap_or_else(|| "missing".to_string()),
        condition_len
            .map(|len| len.to_string())
            .unwrap_or_else(|| "missing".to_string()),
        has_undocumented_0e0b
    )
}

fn associated_config_binary_property_len(
    message: &crate::mapi_store::MapiAssociatedConfigMessage,
    property_tag: u32,
) -> Option<usize> {
    match associated_config_property_value(message, property_tag) {
        Some(MapiValue::Binary(bytes)) => Some(bytes.len()),
        _ => None,
    }
}

pub(in crate::mapi::dispatch) fn format_inbox_associated_wire_row_summary(
    mailbox_guid: Uuid,
    folder_id: u64,
    associated: bool,
    position: usize,
    forward_read: bool,
    row_count: usize,
    sort_orders: &[MapiSortOrder],
    restriction: Option<&MapiRestriction>,
    columns: &[u32],
    snapshot: &MapiMailStoreSnapshot,
) -> String {
    if !associated || row_count == 0 || columns.is_empty() {
        return String::new();
    }
    let mut rows = debug_associated_table_rows(folder_id, snapshot, restriction, mailbox_guid);
    if rows.is_empty() {
        return String::new();
    }
    sort_debug_associated_table_rows(&mut rows, sort_orders, mailbox_guid);
    let selected = select_query_window(rows.len(), position, forward_read, row_count);
    let column_shape = columns
        .iter()
        .map(|tag| format!("0x{tag:08x}:type=0x{:04x}", *tag & 0x0000_ffff))
        .collect::<Vec<_>>()
        .join(",");
    let row_summaries = selected
        .iter()
        .map(|index| {
            let message = &rows[*index];
            let values = serialize_debug_associated_row(message, mailbox_guid, columns);
            let standard_row = standard_property_row_bytes(&values);
            let query_rows_row = query_rows_property_row_bytes(columns, &values);
            format!(
                "index={};id=0x{:016x};class={};status=0x{:02x};value_len={};standard_len={};query_rows_len={};value_preview={};standard_preview={};query_rows_preview={}",
                index,
                debug_associated_row_id(message),
                debug_associated_row_class(message),
                standard_row.first().copied().unwrap_or(0xff),
                values.len(),
                standard_row.len(),
                query_rows_row.len(),
                hex_preview(&values, 160),
                hex_preview(&standard_row, 160),
                hex_preview(&query_rows_row, 160)
            )
        })
        .collect::<Vec<_>>()
        .join("|");
    format!(
        "total={};position={};forward={};requested={};returned={};columns={};{}",
        rows.len(),
        position,
        forward_read,
        row_count,
        selected.len(),
        column_shape,
        row_summaries
    )
}

pub(in crate::mapi::dispatch) fn sort_associated_config_messages_for_debug(
    rows: &mut [crate::mapi_store::MapiAssociatedConfigMessage],
    sort_orders: &[MapiSortOrder],
) {
    if sort_orders.is_empty() {
        return;
    }
    rows.sort_by(|left, right| {
        for sort_order in sort_orders {
            let ordering = match sort_order.property_tag {
                PID_TAG_MESSAGE_CLASS_W => {
                    compare_case_insensitive(&left.message_class, &right.message_class)
                }
                PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
                    compare_case_insensitive(&left.subject, &right.subject)
                }
                PID_TAG_LAST_MODIFICATION_TIME | PID_TAG_MID => left.id.cmp(&right.id),
                _ => Ordering::Equal,
            };
            let ordering = apply_sort_direction(ordering, sort_order.order);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        left.id.cmp(&right.id)
    });
}

pub(in crate::mapi::dispatch) fn format_inbox_associated_config_summary(
    folder_id: u64,
    associated: bool,
    snapshot: &MapiMailStoreSnapshot,
) -> String {
    if !associated || folder_id != INBOX_FOLDER_ID {
        return String::new();
    }
    let messages = debug_associated_table_rows(folder_id, snapshot, None, Uuid::nil())
        .into_iter()
        .filter_map(|row| match row {
            DebugAssociatedTableRow::Config(message) => Some(message),
            DebugAssociatedTableRow::NamedView(_) => None,
        })
        .collect::<Vec<_>>();
    let mut parts = Vec::new();
    for message in &messages {
        let source_key = mapi_mailstore::source_key_for_store_id(message.id);
        let decoded_source_key = crate::mapi::identity::object_id_from_source_key(&source_key);
        parts.push(format!(
            "id=0x{:016x};source_key_id={};class={};subject={}",
            message.id,
            format_optional_folder_id(decoded_source_key),
            message.message_class,
            message.subject
        ));
    }
    format!("count={};{}", messages.len(), parts.join("|"))
}
