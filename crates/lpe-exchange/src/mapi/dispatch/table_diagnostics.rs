use super::*;

pub(super) fn format_debug_mapi_value(value: &MapiValue) -> String {
    match value {
        MapiValue::String(value) => format_debug_text_value(value),
        MapiValue::Binary(value) => {
            format!(
                "binary:bytes={}:preview={}",
                value.len(),
                hex_preview(value, 96)
            )
        }
        MapiValue::Bool(value) => value.to_string(),
        MapiValue::I16(value) => value.to_string(),
        MapiValue::I32(value) => value.to_string(),
        MapiValue::I64(value) => value.to_string(),
        MapiValue::F64(value) => f64::from_bits(*value).to_string(),
        MapiValue::U32(value) => value.to_string(),
        MapiValue::U64(value) => value.to_string(),
        MapiValue::Guid(value) => format!("guid:{}", hex_preview(value, 16)),
        MapiValue::Error(value) => format!("error:0x{value:08x}"),
        MapiValue::MultiI16(values) => format!("{values:?}"),
        MapiValue::MultiI32(values) => format!("{values:?}"),
        MapiValue::MultiI64(values) => format!("{values:?}"),
        MapiValue::MultiString(values) => format!("{values:?}"),
        MapiValue::MultiBinary(values) => format!("multi_binary:count={}", values.len()),
        MapiValue::MultiGuid(values) => format!("multi_guid:count={}", values.len()),
    }
}

pub(super) fn format_debug_text_value(value: &str) -> String {
    value.escape_debug().to_string()
}

pub(super) fn format_ipm_configuration_set_columns_contract(
    folder_id: u64,
    associated: bool,
    columns: &[u32],
) -> String {
    if !associated || folder_id != INBOX_FOLDER_ID {
        return String::new();
    }
    let missing = missing_debug_property_tags(
        &[
            PID_TAG_FOLDER_ID,
            PID_TAG_MID,
            PID_TAG_MESSAGE_CLASS_W,
            PID_TAG_ROAMING_DATATYPES,
        ],
        columns,
    );
    format!(
        "ms_oxocfg_required_columns_selected={};not_selected_required_columns={}",
        missing.is_empty(),
        missing
    )
}

pub(super) fn format_ipm_configuration_contract_summary(
    folder_id: u64,
    associated: bool,
    columns: &[u32],
    sort_orders: &[MapiSortOrder],
    snapshot: &MapiMailStoreSnapshot,
) -> String {
    if !associated || folder_id != INBOX_FOLDER_ID {
        return String::new();
    }
    let rows = snapshot
        .associated_config_messages_for_folder(folder_id)
        .into_iter()
        .filter(|message| {
            message.message_class.starts_with("IPM.Configuration.")
                && associated_config_visible_in_table(folder_id, None, message)
        })
        .collect::<Vec<_>>();
    let missing_columns = missing_debug_property_tags(
        &[
            PID_TAG_FOLDER_ID,
            PID_TAG_MID,
            PID_TAG_MESSAGE_CLASS_W,
            PID_TAG_ROAMING_DATATYPES,
        ],
        columns,
    );
    let sort_ok = sort_orders.len() >= 2
        && sort_orders[0].property_tag == PID_TAG_MESSAGE_CLASS_W
        && sort_orders[1].property_tag == PID_TAG_LAST_MODIFICATION_TIME;
    let row_summaries = rows
        .iter()
        .take(16)
        .map(format_ipm_configuration_row_contract)
        .collect::<Vec<_>>()
        .join("|");
    let issue_count = rows
        .iter()
        .filter(|message| !ipm_configuration_row_issues(message).is_empty())
        .count();
    format!(
        "rows={};not_selected_required_columns={};sort_by_message_class_then_lastmod={};row_issue_count={};{}",
        rows.len(),
        missing_columns,
        sort_ok,
        issue_count,
        row_summaries
    )
}

pub(super) fn missing_debug_property_tags(required: &[u32], present: &[u32]) -> String {
    required
        .iter()
        .copied()
        .filter(|tag| !debug_property_tag_present(*tag, present))
        .map(|tag| format!("0x{tag:08x}"))
        .collect::<Vec<_>>()
        .join(",")
}

pub(super) const OUTLOOK_VIEW_DESCRIPTOR_NAMED_STRING_PLACEHOLDER_TAG: u32 = 0x0000_101E;

fn debug_property_tag_present(required: u32, present: &[u32]) -> bool {
    if present.contains(&required) {
        return true;
    }
    if required == OUTLOOK_VIEW_DESCRIPTOR_NAMED_STRING_PLACEHOLDER_TAG {
        return true;
    }
    let required_id = required >> 16;
    let required_type = required & 0xFFFF;
    if matches!(required_type, 0x001E | 0x001F | 0x101E | 0x101F)
        && present.iter().copied().any(|tag| {
            let present_id = tag >> 16;
            let present_type = tag & 0xFFFF;
            present_id == required_id
                && matches!(
                    (required_type, present_type),
                    (0x001E | 0x001F, 0x001E | 0x001F) | (0x101E | 0x101F, 0x101E | 0x101F)
                )
        })
    {
        return true;
    }
    false
}

fn format_ipm_configuration_row_contract(
    message: &crate::mapi_store::MapiAssociatedConfigMessage,
) -> String {
    let datatypes = associated_config_property_value(message, PID_TAG_ROAMING_DATATYPES)
        .and_then(|value| value.into_u32());
    let has_dictionary_stream =
        associated_config_property_value(message, PID_TAG_ROAMING_DICTIONARY).is_some();
    let has_xml_stream =
        associated_config_property_value(message, PID_TAG_ROAMING_XML_STREAM).is_some();
    let has_binary_stream = associated_config_property_value(message, 0x7C09_0102).is_some();
    let message_flags = associated_config_property_value(message, PID_TAG_MESSAGE_FLAGS)
        .and_then(|value| value.into_u32());
    let last_modified = associated_config_property_value(message, PID_TAG_LAST_MODIFICATION_TIME)
        .map(|value| format_debug_mapi_value(&value))
        .unwrap_or_else(|| "missing".to_string());
    let associated_config_0e0b =
        associated_config_property_value(message, OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B)
            .as_ref()
            .map(format_debug_mapi_value)
            .unwrap_or_else(|| "missing".to_string());
    let issues = ipm_configuration_row_issues(message);
    format!(
        "id=0x{:016x};class={};message_flags={};last_modified={};datatypes={};has_dict={};has_xml={};has_binary={};associated_config_0e0b={};issues={}",
        message.id,
        message.message_class,
        message_flags
            .map(|value| format!("0x{value:08x}"))
            .unwrap_or_else(|| "missing".to_string()),
        last_modified,
        datatypes
            .map(|value| format!("0x{value:08x}"))
            .unwrap_or_else(|| "missing".to_string()),
        has_dictionary_stream,
        has_xml_stream,
        has_binary_stream,
        associated_config_0e0b,
        issues
    )
}

fn ipm_configuration_row_issues(
    message: &crate::mapi_store::MapiAssociatedConfigMessage,
) -> String {
    let datatypes = associated_config_property_value(message, PID_TAG_ROAMING_DATATYPES)
        .and_then(|value| value.into_u32());
    let has_dictionary_stream =
        associated_config_property_value(message, PID_TAG_ROAMING_DICTIONARY).is_some();
    let has_xml_stream =
        associated_config_property_value(message, PID_TAG_ROAMING_XML_STREAM).is_some();
    let mut issues = Vec::new();
    match datatypes {
        Some(value) => {
            let invalid_bits = value & !0x0000_0007;
            if invalid_bits != 0 {
                issues.push(format!("invalid_datatype_bits=0x{invalid_bits:08x}"));
            }
            if value & 0x0000_0001 != 0
                && associated_config_property_value(message, 0x7C09_0102).is_none()
            {
                issues.push("binary_bit_without_stream".to_string());
            }
            if value & 0x0000_0002 != 0 && !has_xml_stream {
                issues.push("xml_bit_without_stream".to_string());
            }
            if value & 0x0000_0004 != 0 && !has_dictionary_stream {
                issues.push("dictionary_bit_without_stream".to_string());
            }
        }
        None => issues.push("missing_roaming_datatypes".to_string()),
    }
    issues.join(",")
}

pub(super) fn request_restriction_bytes(request: &RopRequest) -> &[u8] {
    let Some(size_bytes) = request.payload.get(1..3) else {
        return &[];
    };
    let size = u16::from_le_bytes([size_bytes[0], size_bytes[1]]) as usize;
    request.payload.get(3..3 + size).unwrap_or_default()
}

pub(super) fn restriction_property_tags_from_request(request: &RopRequest) -> Vec<u32> {
    request
        .restriction()
        .ok()
        .flatten()
        .map(|restriction| {
            let mut tags = Vec::new();
            collect_restriction_property_tags(&restriction, &mut tags);
            tags
        })
        .unwrap_or_default()
}

pub(super) fn format_debug_restriction(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return String::new();
    }
    match parse_mapi_restriction(bytes) {
        Ok(restriction) => format_debug_parsed_restriction(&restriction),
        Err(error) => format!("parse_error={error}"),
    }
}

pub(super) fn format_debug_search_criteria_scope(request: &RopRequest) -> String {
    if request.rop_id != RopId::SetSearchCriteria.as_u8() {
        return String::new();
    }
    let Some(restriction_size_bytes) = request.payload.get(..2) else {
        return "parse=missing_restriction_size".to_string();
    };
    let Ok(restriction_size_bytes) = restriction_size_bytes.try_into() else {
        return "parse=invalid_restriction_size".to_string();
    };
    let restriction_size = u16::from_le_bytes(restriction_size_bytes) as usize;
    let Some(restriction_bytes) = request.payload.get(2..2 + restriction_size) else {
        return format!("parse=truncated_restriction;restriction_bytes={restriction_size}");
    };
    let count_offset = 2 + restriction_size;
    let Some(folder_count_bytes) = request.payload.get(count_offset..count_offset + 2) else {
        return format!("parse=missing_folder_count;restriction_bytes={restriction_size}");
    };
    let Ok(folder_count_bytes) = folder_count_bytes.try_into() else {
        return format!("parse=invalid_folder_count;restriction_bytes={restriction_size}");
    };
    let folder_count = u16::from_le_bytes(folder_count_bytes) as usize;
    let folder_ids_offset = count_offset + 2;
    let folder_ids_end = folder_ids_offset + folder_count * 8;
    let Some(folder_id_bytes) = request.payload.get(folder_ids_offset..folder_ids_end) else {
        return format!(
            "parse=truncated_folder_ids;restriction_bytes={restriction_size};folder_count={folder_count}"
        );
    };
    let flags_offset = folder_ids_end;
    let Some(flags_bytes) = request.payload.get(flags_offset..flags_offset + 4) else {
        return format!(
            "parse=missing_flags;restriction_bytes={restriction_size};folder_count={folder_count};raw_folder_ids={}",
            hex_preview(folder_id_bytes, 96)
        );
    };
    let Ok(flags_bytes) = flags_bytes.try_into() else {
        return format!(
            "parse=invalid_flags;restriction_bytes={restriction_size};folder_count={folder_count};raw_folder_ids={}",
            hex_preview(folder_id_bytes, 96)
        );
    };
    let folder_ids = folder_id_bytes
        .chunks_exact(8)
        .map(|bytes| {
            crate::mapi::identity::object_id_from_wire_id(bytes)
                .map(|folder_id| format!("0x{folder_id:016x}"))
                .unwrap_or_else(|| format!("invalid:{}", bytes_to_hex(bytes)))
        })
        .collect::<Vec<_>>()
        .join(",");
    let flags = u32::from_le_bytes(flags_bytes);
    let parse = if request.search_criteria_folder_ids().is_some() {
        "ok"
    } else {
        "invalid_folder_id"
    };
    format!(
        "parse={parse};restriction_bytes={restriction_size};restriction={};folder_count={folder_count};folder_ids={folder_ids};flags={flags:#010x};raw_folder_ids={}",
        format_debug_restriction(restriction_bytes),
        hex_preview(folder_id_bytes, 96)
    )
}

pub(super) fn format_debug_restriction_option(restriction: Option<&MapiRestriction>) -> String {
    restriction
        .map(format_debug_parsed_restriction)
        .unwrap_or_default()
}

pub(super) fn format_debug_restriction_property_tags(
    restriction: Option<&MapiRestriction>,
) -> String {
    let Some(restriction) = restriction else {
        return String::new();
    };
    let mut tags = Vec::new();
    collect_restriction_property_tags(restriction, &mut tags);
    format_debug_property_tags(&tags)
}

pub(super) fn format_debug_parsed_restriction(restriction: &MapiRestriction) -> String {
    match restriction {
        MapiRestriction::InvalidTableRestriction => "invalid".to_string(),
        MapiRestriction::And(children) => format!(
            "and({})",
            children
                .iter()
                .map(format_debug_parsed_restriction)
                .collect::<Vec<_>>()
                .join(",")
        ),
        MapiRestriction::Or(children) => format!(
            "or({})",
            children
                .iter()
                .map(format_debug_parsed_restriction)
                .collect::<Vec<_>>()
                .join(",")
        ),
        MapiRestriction::Not(child) => {
            format!("not({})", format_debug_parsed_restriction(child))
        }
        MapiRestriction::Count { count, child } => {
            format!(
                "count;count={count};child={}",
                format_debug_parsed_restriction(child)
            )
        }
        MapiRestriction::SubObject { subobject, child } => format!(
            "subobject;subobject=0x{subobject:08x};child={}",
            format_debug_parsed_restriction(child)
        ),
        MapiRestriction::Content {
            property_tag,
            value,
            fuzzy_level_low,
            fuzzy_level_high,
        } => format!(
            "content;property_tag=0x{property_tag:08x};fuzzy_low=0x{fuzzy_level_low:04x};fuzzy_high=0x{fuzzy_level_high:04x};value={}",
            format_debug_text_value(value)
        ),
        MapiRestriction::Property {
            relop,
            property_tag,
            value,
        } => format!(
            "property;relop=0x{relop:02x};property_tag=0x{property_tag:08x};value={}",
            format_debug_mapi_value(value)
        ),
        MapiRestriction::CompareProperties {
            relop,
            left_property_tag,
            right_property_tag,
        } => format!(
            "compare_properties;relop=0x{relop:02x};left_property_tag=0x{left_property_tag:08x};right_property_tag=0x{right_property_tag:08x}"
        ),
        MapiRestriction::Bitmask {
            property_tag,
            mask,
            must_be_nonzero,
        } => format!(
            "bitmask;property_tag=0x{property_tag:08x};mask=0x{mask:08x};must_be_nonzero={must_be_nonzero}"
        ),
        MapiRestriction::Size {
            relop,
            property_tag,
            size,
        } => {
            format!("size;relop=0x{relop:02x};property_tag=0x{property_tag:08x};size={size}")
        }
        MapiRestriction::Exist { property_tag } => {
            format!("exist;property_tag=0x{property_tag:08x}")
        }
    }
}

fn collect_restriction_property_tags(restriction: &MapiRestriction, tags: &mut Vec<u32>) {
    match restriction {
        MapiRestriction::InvalidTableRestriction => {}
        MapiRestriction::And(children) | MapiRestriction::Or(children) => {
            for child in children {
                collect_restriction_property_tags(child, tags);
            }
        }
        MapiRestriction::Not(child)
        | MapiRestriction::Count { child, .. }
        | MapiRestriction::SubObject { child, .. } => {
            collect_restriction_property_tags(child, tags)
        }
        MapiRestriction::Content { property_tag, .. }
        | MapiRestriction::Property { property_tag, .. }
        | MapiRestriction::Bitmask { property_tag, .. }
        | MapiRestriction::Size { property_tag, .. }
        | MapiRestriction::Exist { property_tag } => {
            if !tags.contains(property_tag) {
                tags.push(*property_tag);
            }
        }
        MapiRestriction::CompareProperties {
            left_property_tag,
            right_property_tag,
            ..
        } => {
            for property_tag in [left_property_tag, right_property_tag] {
                if !tags.contains(property_tag) {
                    tags.push(*property_tag);
                }
            }
        }
    }
}

#[derive(Clone)]
pub(super) enum DebugAssociatedTableRow {
    Config(crate::mapi_store::MapiAssociatedConfigMessage),
    NamedView(crate::mapi_store::MapiCommonViewNamedViewMessage),
}

pub(super) fn debug_associated_table_rows(
    folder_id: u64,
    snapshot: &MapiMailStoreSnapshot,
    restriction: Option<&MapiRestriction>,
    mailbox_guid: Uuid,
) -> Vec<DebugAssociatedTableRow> {
    let mut config_messages = snapshot.associated_config_messages_for_folder(folder_id);
    append_exact_virtual_inbox_debug_associated_config(
        folder_id,
        restriction,
        &mut config_messages,
    );
    let mut rows = config_messages
        .into_iter()
        .filter(|message| {
            restriction_matches_associated_config(restriction, message)
                && associated_config_visible_in_table(folder_id, restriction, message)
        })
        .map(DebugAssociatedTableRow::Config)
        .collect::<Vec<_>>();
    if let Some(message) = debug_default_folder_associated_named_view(snapshot, folder_id) {
        if restriction_matches_common_view_named_view(restriction, &message, mailbox_guid) {
            rows.push(DebugAssociatedTableRow::NamedView(message));
        }
    }
    rows
}

pub(super) fn sort_debug_associated_table_rows(
    rows: &mut [DebugAssociatedTableRow],
    sort_orders: &[MapiSortOrder],
    mailbox_guid: Uuid,
) {
    if sort_orders.is_empty() {
        return;
    }
    rows.sort_by(|left, right| {
        for sort_order in sort_orders {
            let ordering = compare_debug_mapi_values(
                debug_associated_row_property_value(left, mailbox_guid, sort_order.property_tag),
                debug_associated_row_property_value(right, mailbox_guid, sort_order.property_tag),
            );
            let ordering = apply_sort_direction(ordering, sort_order.order);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        debug_associated_row_id(left).cmp(&debug_associated_row_id(right))
    });
}

pub(super) fn debug_associated_row_property_value(
    message: &DebugAssociatedTableRow,
    mailbox_guid: Uuid,
    property_tag: u32,
) -> Option<MapiValue> {
    match message {
        DebugAssociatedTableRow::Config(message) => {
            associated_config_property_value_with_mailbox_guid(message, mailbox_guid, property_tag)
        }
        DebugAssociatedTableRow::NamedView(message) => {
            common_view_named_view_property_value(message, mailbox_guid, property_tag)
        }
    }
}

pub(super) fn debug_associated_row_id(message: &DebugAssociatedTableRow) -> u64 {
    match message {
        DebugAssociatedTableRow::Config(message) => message.id,
        DebugAssociatedTableRow::NamedView(message) => message.id,
    }
}

pub(super) fn debug_associated_row_class(message: &DebugAssociatedTableRow) -> &str {
    match message {
        DebugAssociatedTableRow::Config(message) => &message.message_class,
        DebugAssociatedTableRow::NamedView(_) => "IPM.Microsoft.FolderDesign.NamedView",
    }
}

pub(super) fn debug_associated_row_subject(message: &DebugAssociatedTableRow) -> &str {
    match message {
        DebugAssociatedTableRow::Config(message) => &message.subject,
        DebugAssociatedTableRow::NamedView(message) => &message.name,
    }
}

pub(super) fn serialize_debug_associated_row(
    message: &DebugAssociatedTableRow,
    mailbox_guid: Uuid,
    columns: &[u32],
) -> Vec<u8> {
    match message {
        DebugAssociatedTableRow::Config(message) => {
            serialize_associated_config_row_with_mailbox_guid(message, mailbox_guid, columns)
        }
        DebugAssociatedTableRow::NamedView(message) => {
            serialize_common_view_named_view_row_with_mailbox_guid(message, mailbox_guid, columns)
        }
    }
}

pub(super) fn debug_default_folder_associated_named_view(
    snapshot: &MapiMailStoreSnapshot,
    folder_id: u64,
) -> Option<crate::mapi_store::MapiCommonViewNamedViewMessage> {
    let container_class = snapshot
        .collaboration_folder_for_id(folder_id)
        .map(|folder| collaboration_folder_message_class(folder.kind))
        .or_else(|| advertised_special_folder_container_class(folder_id))?;
    default_view_supported_folder(folder_id, container_class)
        .then(|| {
            snapshot.default_folder_named_view_message(
                folder_id,
                crate::mapi_store::OUTLOOK_DEFAULT_FOLDER_NAMED_VIEW_ID,
            )
        })
        .flatten()
}

pub(super) fn format_inbox_fai_handoff_visibility_context(
    snapshot: &MapiMailStoreSnapshot,
    restriction: Option<&MapiRestriction>,
    account_id: Uuid,
) -> String {
    let unfiltered_rows = debug_associated_table_rows(INBOX_FOLDER_ID, snapshot, None, account_id);
    let current_rows =
        debug_associated_table_rows(INBOX_FOLDER_ID, snapshot, restriction, account_id);
    let prefix_restriction = MapiRestriction::Content {
        property_tag: PID_TAG_MESSAGE_CLASS_W,
        value: "IPM.Configuration.".to_string(),
        fuzzy_level_low: 0x0002,
        fuzzy_level_high: 0x0001,
    };
    let prefix_rows = debug_associated_table_rows(
        INBOX_FOLDER_ID,
        snapshot,
        Some(&prefix_restriction),
        account_id,
    );
    let advertised_default_view =
        debug_default_folder_associated_named_view(snapshot, INBOX_FOLDER_ID);
    let advertised_default_view_rows = advertised_default_view
        .as_ref()
        .map(|view| {
            format!(
                "id=0x{:016x};folder=0x{:016x};class=IPM.Microsoft.FolderDesign.NamedView;subject={}",
                view.id, view.folder_id, view.name
            )
        })
        .unwrap_or_default();
    format!(
        "advertised_default_view_folder_id=0x{INBOX_FOLDER_ID:016x};default_view_id={};current_restriction={};current_count={};current_rows={};unfiltered_count={};unfiltered_rows={};prefix_ipm_configuration_count={};prefix_ipm_configuration_rows={};exact_named_view_count={};exact_named_view_rows={}",
        advertised_default_view
            .as_ref()
            .map(|view| format!("0x{:016x}", view.id))
            .unwrap_or_else(|| "none".to_string()),
        restriction
            .map(format_debug_parsed_restriction)
            .unwrap_or_default(),
        current_rows.len(),
        format_debug_associated_row_list(&current_rows),
        unfiltered_rows.len(),
        format_debug_associated_row_list(&unfiltered_rows),
        prefix_rows.len(),
        format_debug_associated_row_list(&prefix_rows),
        usize::from(advertised_default_view.is_some()),
        advertised_default_view_rows
    )
}

fn format_debug_associated_row_list(rows: &[DebugAssociatedTableRow]) -> String {
    rows.iter()
        .map(|row| {
            format!(
                "id=0x{:016x};class={};subject={}",
                debug_associated_row_id(row),
                debug_associated_row_class(row),
                debug_associated_row_subject(row)
            )
        })
        .collect::<Vec<_>>()
        .join("|")
}

pub(super) fn format_inbox_hierarchy_query_context(
    object: Option<&MapiObject>,
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    snapshot: &MapiMailStoreSnapshot,
) -> Option<String> {
    let Some(MapiObject::HierarchyTable {
        folder_id,
        columns,
        position,
        sort_orders,
        restriction,
        deleted_advertised_special_folders,
        ..
    }) = object
    else {
        return None;
    };
    if *folder_id != INBOX_FOLDER_ID {
        return None;
    }
    let row_count = hierarchy_row_count_excluding_deleted(
        *folder_id,
        mailboxes,
        snapshot,
        deleted_advertised_special_folders,
    );
    Some(format!(
        "input_index={};position={};forward={};requested_rows={};columns={};sort={};restriction={};row_count={};expected_subfolders=false",
        request.input_handle_index().unwrap_or(0),
        position,
        request.query_forward_read(),
        request.query_row_count().unwrap_or(0),
        format_debug_property_tags(columns),
        format_debug_sort_orders(sort_orders),
        restriction
            .as_ref()
            .map(format_debug_parsed_restriction)
            .unwrap_or_default(),
        row_count
    ))
}

pub(super) fn format_inbox_associated_query_context(
    object: Option<&MapiObject>,
    request: &RopRequest,
    mailbox_guid: Uuid,
    snapshot: &MapiMailStoreSnapshot,
) -> Option<String> {
    let Some(MapiObject::ContentsTable {
        folder_id,
        associated,
        columns,
        position,
        restriction,
        sort_orders,
        ..
    }) = object
    else {
        return None;
    };
    if !*associated || *folder_id != INBOX_FOLDER_ID {
        return None;
    }
    let selected_columns = effective_contents_table_columns(*folder_id, *associated, columns);
    let requested_row_count = request.query_row_count().unwrap_or(0);
    Some(format!(
        "input_index={};position={};forward={};requested_rows={};columns={};sort={};window={};values={};wire={}",
        request.input_handle_index().unwrap_or(0),
        position,
        request.query_forward_read(),
        requested_row_count,
        format_debug_property_tags(&selected_columns),
        format_debug_sort_orders(sort_orders),
        format_inbox_associated_query_row_window(
            mailbox_guid,
            *position,
            request.query_forward_read(),
            requested_row_count,
            sort_orders,
            restriction.as_ref(),
            snapshot,
        ),
        format_outlook_query_row_values(
            mailbox_guid,
            *folder_id,
            *associated,
            *position,
            request.query_forward_read(),
            requested_row_count,
            sort_orders,
            restriction.as_ref(),
            &selected_columns,
            snapshot
        ),
        format_inbox_associated_wire_row_summary(
            mailbox_guid,
            *folder_id,
            *associated,
            *position,
            request.query_forward_read(),
            requested_row_count,
            sort_orders,
            restriction.as_ref(),
            &selected_columns,
            snapshot
        )
    ))
}

pub(super) fn format_post_fai_hierarchy_release_without_inbox_contents_context(
    object: Option<&MapiObject>,
    released_handle: Option<u32>,
    state: &PostHierarchyActionState,
    mailboxes: &[JmapMailbox],
    snapshot: &MapiMailStoreSnapshot,
) -> Option<String> {
    let Some(MapiObject::HierarchyTable {
        folder_id,
        columns,
        position,
        sort_orders,
        restriction,
        deleted_advertised_special_folders,
        ..
    }) = object
    else {
        return None;
    };
    if !state.inbox_associated_contents_table_observed
        || state.inbox_normal_contents_table_observed
        || !state.post_inbox_fai_handoff_logged
    {
        return None;
    }
    let computed_row_count = hierarchy_row_count_excluding_deleted(
        *folder_id,
        mailboxes,
        snapshot,
        deleted_advertised_special_folders,
    );
    let row_count = computed_row_count.max((*position).min(u32::MAX as usize) as u32);
    Some(format!(
        "handle={};folder=0x{folder_id:016x};role={};position={position};row_count={row_count};computed_row_count={computed_row_count};columns={};sort={};restriction={};last_associated_query={};last_hierarchy_table={};last_hierarchy_query={};recent_actions={};next_expected_client_step=open_inbox_normal_contents_table_or_sync_configure",
        format_optional_debug_handle(released_handle),
        debug_role_for_folder_id(*folder_id),
        format_debug_property_tags(columns),
        format_debug_sort_orders(sort_orders),
        restriction
            .as_ref()
            .map(format_debug_parsed_restriction)
            .unwrap_or_default(),
        debug_context_or_none(&state.last_inbox_associated_query_context),
        debug_context_or_none(&state.last_inbox_hierarchy_table_context),
        debug_context_or_none(&state.last_inbox_hierarchy_query_context),
        state.recent_probe_actions.join(">")
    ))
}

pub(super) fn format_inbox_associated_find_context(
    object: Option<&MapiObject>,
    request: &RopRequest,
    mailbox_guid: Uuid,
    snapshot: &MapiMailStoreSnapshot,
    response: &[u8],
) -> Option<String> {
    let Some(MapiObject::ContentsTable {
        folder_id,
        associated,
        columns,
        position,
        sort_orders,
        ..
    }) = object
    else {
        return None;
    };
    if !*associated || *folder_id != INBOX_FOLDER_ID {
        return None;
    }
    let selected_columns = effective_contents_table_columns(*folder_id, *associated, columns);
    Some(format!(
        "input_index={};origin={};backward={};found={};position={};columns={};sort={};restriction={};row={};values={};wire={};prefix_find_summary={}",
        request.input_handle_index().unwrap_or(0),
        request.find_origin().unwrap_or(0),
        request.find_backward(),
        response.get(7).copied().unwrap_or(0),
        position,
        format_debug_property_tags(&selected_columns),
        format_debug_sort_orders(sort_orders),
        format_debug_restriction(request_restriction_bytes(request)),
        format_inbox_associated_query_row_window(
            mailbox_guid,
            *position,
            true,
            1,
            sort_orders,
            None,
            snapshot
        ),
        format_outlook_query_row_values(
            mailbox_guid,
            *folder_id,
            *associated,
            *position,
            true,
            1,
            sort_orders,
            None,
            &selected_columns,
            snapshot
        ),
        format_inbox_associated_wire_row_summary(
            mailbox_guid,
            *folder_id,
            *associated,
            *position,
            true,
            1,
            sort_orders,
            None,
            &selected_columns,
            snapshot
        ),
        format_inbox_associated_prefix_find_summary(*position, sort_orders, snapshot)
    ))
}

fn format_inbox_associated_prefix_find_summary(
    position: usize,
    sort_orders: &[MapiSortOrder],
    snapshot: &MapiMailStoreSnapshot,
) -> String {
    let mut rows = snapshot.associated_config_messages_for_folder(INBOX_FOLDER_ID);
    rows.retain(|message| associated_config_visible_in_table(INBOX_FOLDER_ID, None, message));
    sort_associated_config_messages_for_debug(&mut rows, sort_orders);
    let first_class = rows
        .get(position)
        .map(|message| message.message_class.as_str())
        .unwrap_or("none");
    let classes = rows
        .iter()
        .map(|message| message.message_class.as_str())
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "first_class={};account_prefs_first={};available_classes={}",
        first_class,
        first_class == "IPM.Configuration.AccountPrefs",
        classes
    )
}

pub(super) fn inbox_associated_broad_findrow_matched(
    object: Option<&MapiObject>,
    request: &RopRequest,
    response: &[u8],
) -> bool {
    let Some(MapiObject::ContentsTable {
        folder_id,
        associated,
        ..
    }) = object
    else {
        return false;
    };
    *folder_id == INBOX_FOLDER_ID
        && *associated
        && response.get(7).copied().unwrap_or(0) != 0
        && request
            .restriction()
            .ok()
            .flatten()
            .as_ref()
            .is_some_and(is_broad_ipm_configuration_restriction)
}

fn is_broad_ipm_configuration_restriction(restriction: &MapiRestriction) -> bool {
    match restriction {
        MapiRestriction::Content {
            property_tag,
            value,
            fuzzy_level_low,
            ..
        } => {
            matches!(
                canonical_property_storage_tag(*property_tag),
                PID_TAG_MESSAGE_CLASS_W
            ) && value == "IPM.Configuration."
                && fuzzy_level_low & 0x0002 != 0
        }
        MapiRestriction::And(children) | MapiRestriction::Or(children) => {
            children.iter().any(is_broad_ipm_configuration_restriction)
        }
        MapiRestriction::Not(child)
        | MapiRestriction::Count { child, .. }
        | MapiRestriction::SubObject { child, .. } => is_broad_ipm_configuration_restriction(child),
        _ => false,
    }
}

pub(super) fn format_common_views_inbox_shortcut_context(
    object: Option<&MapiObject>,
    request: &RopRequest,
    account_id: Uuid,
    snapshot: &MapiMailStoreSnapshot,
) -> Option<String> {
    let Some(MapiObject::ContentsTable {
        folder_id,
        associated,
        columns,
        position,
        sort_orders,
        ..
    }) = object
    else {
        return None;
    };
    if !*associated || *folder_id != COMMON_VIEWS_FOLDER_ID {
        return None;
    }
    let selected_columns = effective_contents_table_columns(*folder_id, *associated, columns);
    let requested_row_count = request.query_row_count().unwrap_or(0);
    let mut rows = snapshot.common_views_table_messages().collect::<Vec<_>>();
    sort_common_views_messages(&mut rows, sort_orders);
    let selected = select_query_window(
        rows.len(),
        *position,
        request.query_forward_read(),
        requested_row_count,
    );
    let selected_inbox_indexes = selected
        .iter()
        .filter_map(|index| match &rows[*index] {
            crate::mapi_store::MapiCommonViewsMessage::NavigationShortcut(shortcut)
                if shortcut.target_folder_id == Some(INBOX_FOLDER_ID) =>
            {
                Some(index.to_string())
            }
            _ => None,
        })
        .collect::<Vec<_>>()
        .join(",");
    Some(format!(
        "input_index={};position={};requested_rows={};columns={};sort={};selected_inbox_indexes={};wlink_decoding={}",
        request.input_handle_index().unwrap_or(0),
        position,
        requested_row_count,
        format_debug_property_tags(&selected_columns),
        format_debug_sort_orders(sort_orders),
        if selected_inbox_indexes.is_empty() {
            "none".to_string()
        } else {
            selected_inbox_indexes
        },
        format_common_views_wlink_target_decoding(account_id, snapshot)
    ))
}

pub(super) fn format_common_views_wlink_target_decoding(
    account_id: Uuid,
    snapshot: &MapiMailStoreSnapshot,
) -> String {
    snapshot
        .common_views_table_messages()
        .filter_map(|message| match message {
            crate::mapi_store::MapiCommonViewsMessage::NavigationShortcut(shortcut) => {
                if shortcut.shortcut_type == 4 {
                    return None;
                }
                let entry_id = shortcut
                    .target_folder_id
                    .and_then(|folder_id| {
                        crate::mapi::identity::folder_entry_id_from_object_id(account_id, folder_id)
                    })
                    .unwrap_or_default();
                let entry_id_decoded =
                    crate::mapi::identity::object_id_from_folder_entry_id(&entry_id);
                let source_key = shortcut
                    .target_folder_id
                    .map(mapi_mailstore::source_key_for_store_id)
                    .unwrap_or_default();
                let source_key_decoded =
                    crate::mapi::identity::object_id_from_source_key(&source_key);
                let sharing_local_folder_id = navigation_shortcut_property_value(
                    &shortcut,
                    account_id,
                    PID_NAME_SHARING_CALENDAR_GROUP_ENTRY_ASSOCIATED_LOCAL_FOLDER_ID_TAG,
                );
                let sharing_local_folder_id_decoded = match &sharing_local_folder_id {
                    Some(MapiValue::Binary(bytes)) => {
                        crate::mapi::identity::object_id_from_folder_entry_id(bytes)
                    }
                    _ => None,
                };
                Some(format!(
                    "id=0x{:016x};subject={};target_folder={};entry_id_bytes={};entry_id_decoded={};entry_id_matches_inbox={};source_key_bytes={};source_key_decoded={};source_key_matches_inbox={};sharing_local_folder_id={};sharing_local_folder_id_decoded={};sharing_local_folder_id_matches_inbox={};expected_inbox=0x{INBOX_FOLDER_ID:016x}",
                    shortcut.id,
                    shortcut.subject,
                    shortcut
                        .target_folder_id
                        .map(|folder_id| format!("0x{folder_id:016x}"))
                        .unwrap_or_else(|| "none".to_string()),
                    entry_id.len(),
                    format_optional_folder_id(entry_id_decoded),
                    entry_id_decoded == Some(INBOX_FOLDER_ID),
                    source_key.len(),
                    format_optional_folder_id(source_key_decoded),
                    source_key_decoded == Some(INBOX_FOLDER_ID),
                    sharing_local_folder_id
                        .as_ref()
                        .map(mapi_value_debug_shape)
                        .unwrap_or_else(|| "missing".to_string()),
                    format_optional_folder_id(sharing_local_folder_id_decoded),
                    sharing_local_folder_id_decoded == Some(INBOX_FOLDER_ID)
                ))
            }
            crate::mapi_store::MapiCommonViewsMessage::NamedView(_) => None,
            crate::mapi_store::MapiCommonViewsMessage::SearchFolderDefinition(_) => None,
        })
        .collect::<Vec<_>>()
        .join("|")
}

pub(super) fn format_common_views_wlink_contract_summary(
    selected_columns: &[u32],
    snapshot: &MapiMailStoreSnapshot,
) -> String {
    if selected_columns.is_empty() {
        return String::new();
    }

    let link_rows = snapshot
        .common_views_table_messages()
        .filter(|message| {
            matches!(
                message,
                crate::mapi_store::MapiCommonViewsMessage::NavigationShortcut(shortcut)
                    if shortcut.shortcut_type != 4
            )
        })
        .count();
    let header_rows = snapshot
        .common_views_table_messages()
        .filter(|message| {
            matches!(
                message,
                crate::mapi_store::MapiCommonViewsMessage::NavigationShortcut(shortcut)
                    if shortcut.shortcut_type == 4
            )
        })
        .count();

    let required_link_columns = [
        PID_TAG_WLINK_ENTRY_ID,
        PID_TAG_WLINK_RECORD_KEY,
        PID_TAG_WLINK_STORE_ENTRY_ID,
        PID_TAG_WLINK_FOLDER_TYPE,
        PID_TAG_WLINK_GROUP_CLSID,
        PID_TAG_WLINK_GROUP_NAME_W,
        PID_TAG_WLINK_SECTION,
        PID_TAG_WLINK_ORDINAL,
        PID_TAG_WLINK_TYPE,
        PID_TAG_WLINK_FLAGS,
        PID_TAG_WLINK_SAVE_STAMP,
        PID_NAME_SHARING_CALENDAR_GROUP_ENTRY_ASSOCIATED_LOCAL_FOLDER_ID_TAG,
    ];
    let missing_required_link_columns = required_link_columns
        .iter()
        .copied()
        .filter(|tag| {
            !selected_columns
                .iter()
                .any(|column| property_ids_match(*column, *tag))
        })
        .collect::<Vec<_>>();
    let expected_link_default_columns = selected_columns
        .iter()
        .copied()
        .filter(|tag| common_views_link_row_expected_default(*tag))
        .collect::<Vec<_>>();

    format!(
        "link_rows={};header_rows={};not_selected_required_link_columns={};expected_link_default_columns={};note=calendar_wlink_fields_default_on_non_header_mail_shortcut_rows",
        link_rows,
        header_rows,
        format_debug_property_tags(&missing_required_link_columns),
        format_debug_property_tags(&expected_link_default_columns)
    )
}

pub(super) fn format_inbox_related_release_context(
    object: Option<&MapiObject>,
    handle: Option<u32>,
    state: &PostHierarchyActionState,
    snapshot: &MapiMailStoreSnapshot,
) -> Option<String> {
    match object {
        Some(MapiObject::ContentsTable {
            folder_id,
            associated,
            columns,
            position,
            sort_orders,
            restriction,
            ..
        }) if *folder_id == INBOX_FOLDER_ID || *folder_id == COMMON_VIEWS_FOLDER_ID => {
            let release_without_query_rows = *folder_id == INBOX_FOLDER_ID
                && !*associated
                && !columns.is_empty()
                && state.last_inbox_normal_contents_table_setcolumns_handle == handle
                && state.last_inbox_normal_contents_table_query_rows_handle != handle;
            Some(format!(
                "handle={};kind=contents_table;folder=0x{folder_id:016x};associated={};position={};columns={};column_support={};sort={};restriction={};view_handoff={};descriptor_behavior={};after_inbox_associated_query={};normal_contents_table_observed={};normal_setcolumns_observed={};normal_query_rows_observed={};visible_inbox_release_without_query_rows={};last_normal_setcolumns_handle={};last_normal_query_rows_handle={}",
                format_optional_debug_handle(handle),
                associated,
                position,
                format_debug_property_tags(columns),
                contents_table_column_support_summary(*associated, columns),
                format_debug_sort_orders(sort_orders),
                format_debug_restriction_option(restriction.as_ref()),
                format_outlook_view_handoff_table_contract(
                    *folder_id,
                    *associated,
                    columns,
                    snapshot,
                ),
                format_inbox_view_descriptor_set_columns_behavior_contract(
                    *folder_id,
                    *associated,
                    columns,
                    snapshot,
                ),
                state.inbox_associated_contents_table_observed,
                state.inbox_normal_contents_table_observed,
                state.inbox_normal_contents_table_setcolumns_observed,
                state.inbox_normal_contents_table_query_rows_observed,
                release_without_query_rows,
                format_optional_debug_handle(state.last_inbox_normal_contents_table_setcolumns_handle),
                format_optional_debug_handle(state.last_inbox_normal_contents_table_query_rows_handle)
            ))
        }
        Some(MapiObject::Folder { folder_id, .. }) if *folder_id == INBOX_FOLDER_ID => {
            Some(format!(
                "handle={};kind=folder;folder=0x{folder_id:016x};after_inbox_associated_query={};normal_contents_table_observed={};normal_setcolumns_observed={};normal_query_rows_observed={};last_normal_setcolumns_handle={};last_normal_query_rows_handle={}",
                format_optional_debug_handle(handle),
                state.inbox_associated_contents_table_observed,
                state.inbox_normal_contents_table_observed,
                state.inbox_normal_contents_table_setcolumns_observed,
                state.inbox_normal_contents_table_query_rows_observed,
                format_optional_debug_handle(state.last_inbox_normal_contents_table_setcolumns_handle),
                format_optional_debug_handle(state.last_inbox_normal_contents_table_query_rows_handle)
            ))
        }
        _ => None,
    }
}

#[derive(Default)]
pub(super) struct HierarchyResponseMetricSummary {
    pub(super) has_conversation_action: bool,
    pub(super) has_quick_step: bool,
}

pub(super) fn hierarchy_response_metric_summary(
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
        if cursor.read_u8().is_err() {
            break;
        }
        for column in selected_columns {
            let value = match parse_mapi_property_value(&mut cursor, *column) {
                Ok(value) => value,
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

pub(super) fn format_hierarchy_query_rows_wire_summary(
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
            match parse_mapi_property_value(&mut cursor, *column) {
                Ok(value) => {
                    values.insert(*column, value);
                }
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

fn append_exact_virtual_inbox_debug_associated_config(
    folder_id: u64,
    restriction: Option<&MapiRestriction>,
    messages: &mut Vec<crate::mapi_store::MapiAssociatedConfigMessage>,
) {
    if folder_id != INBOX_FOLDER_ID {
        return;
    }
    if restriction.is_none()
        || restriction.is_some_and(|restriction| {
            matches!(
                restriction,
                MapiRestriction::Property {
                    relop: 0x02,
                    property_tag: PID_TAG_MESSAGE_CLASS_W,
                    value: MapiValue::String(value),
                } | MapiRestriction::Content {
                    property_tag: PID_TAG_MESSAGE_CLASS_W,
                    value,
                    ..
                } if value.eq_ignore_ascii_case("IPM.Configuration.")
            )
        })
    {
        for message in crate::mapi_store::outlook_inbox_broad_startup_associated_config_defaults() {
            if !messages.iter().any(|existing| {
                existing
                    .message_class
                    .eq_ignore_ascii_case(&message.message_class)
            }) {
                messages.push(message);
            }
        }
    }
    let Some(message_class) = debug_exact_message_class_restriction_value(restriction) else {
        return;
    };
    let Some(message) =
        crate::mapi_store::outlook_inbox_exact_virtual_associated_config_for_message_class(
            message_class,
        )
    else {
        return;
    };
    if !messages.iter().any(|existing| {
        existing
            .message_class
            .eq_ignore_ascii_case(&message.message_class)
    }) {
        messages.push(message);
    }
}

fn debug_exact_message_class_restriction_value(
    restriction: Option<&MapiRestriction>,
) -> Option<&str> {
    match restriction? {
        MapiRestriction::Property {
            relop: 0x04,
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            value: MapiValue::String(value),
        }
        | MapiRestriction::Content {
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            value,
            ..
        } => Some(value.as_str()),
        _ => None,
    }
}

fn compare_debug_mapi_values(left: Option<MapiValue>, right: Option<MapiValue>) -> Ordering {
    match (left, right) {
        (Some(MapiValue::String(left)), Some(MapiValue::String(right))) => {
            compare_case_insensitive(&left, &right)
        }
        (Some(MapiValue::U64(left)), Some(MapiValue::U64(right))) => left.cmp(&right),
        (Some(MapiValue::I64(left)), Some(MapiValue::I64(right))) => left.cmp(&right),
        (Some(MapiValue::U32(left)), Some(MapiValue::U32(right))) => left.cmp(&right),
        (Some(MapiValue::I32(left)), Some(MapiValue::I32(right))) => left.cmp(&right),
        _ => Ordering::Equal,
    }
}
