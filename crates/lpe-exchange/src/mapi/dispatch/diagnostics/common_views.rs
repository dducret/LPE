use super::super::*;

pub(in crate::mapi::dispatch) fn common_views_saved_shortcut_summary(
    shortcut: &crate::mapi_store::MapiNavigationShortcutMessage,
    properties: &HashMap<u32, MapiValue>,
) -> String {
    let property_tags = properties.keys().copied().collect::<Vec<_>>();
    let entry_id = property_value_by_id(properties, PID_TAG_WLINK_ENTRY_ID);
    let entry_id_shape = entry_id
        .map(mapi_value_debug_shape)
        .unwrap_or_else(|| "missing".to_string());
    let entry_id_decoded = entry_id.and_then(|value| match value {
        MapiValue::Binary(bytes) => {
            crate::mapi::identity::object_id_from_folder_identifier_bytes(bytes).or_else(|| {
                bytes
                    .windows(46)
                    .find_map(crate::mapi::identity::object_id_from_folder_entry_id)
            })
        }
        _ => None,
    });
    let store_entry_id_shape = property_value_by_id(properties, PID_TAG_WLINK_STORE_ENTRY_ID)
        .map(mapi_value_debug_shape)
        .unwrap_or_else(|| "missing".to_string());
    format!(
        "subject={};type={};section={};ordinal={};target={};entry_id={};entry_id_decoded={};store_entry_id={};group_header_id={};group_name={};property_tags={}",
        shortcut.subject,
        shortcut.shortcut_type,
        shortcut.section,
        shortcut.ordinal,
        shortcut
            .target_folder_id
            .map(|folder_id| format!("0x{folder_id:016x}"))
            .unwrap_or_else(|| "none".to_string()),
        entry_id_shape,
        format_optional_folder_id(entry_id_decoded),
        store_entry_id_shape,
        shortcut
            .group_header_id
            .map(|group_id| group_id.to_string())
            .unwrap_or_else(|| "none".to_string()),
        shortcut.group_name,
        format_debug_property_tags(&property_tags)
    )
}

fn property_value_by_id(
    properties: &HashMap<u32, MapiValue>,
    property_tag: u32,
) -> Option<&MapiValue> {
    properties
        .iter()
        .find_map(|(tag, value)| property_ids_match(*tag, property_tag).then_some(value))
}

pub(in crate::mapi::dispatch) fn log_outlook_view_handoff(
    principal: &AccountPrincipal,
    request: &RopRequest,
    folder_id: u64,
    message_id: u64,
    output_handle: u32,
    message: &crate::mapi_store::MapiCommonViewNamedViewMessage,
    snapshot: &MapiMailStoreSnapshot,
) {
    let definition = outlook_folder_view_definition(message.folder_id, &message.name);
    let descriptor_binary = view_descriptor_binary(&definition);
    let descriptor_strings = view_descriptor_strings(&definition);
    let descriptor_summary = format_view_descriptor_binary_summary(&descriptor_binary);
    let descriptor_strings_chars = descriptor_strings.chars().count();
    let descriptor_strings_utf16_bytes = descriptor_strings.encode_utf16().count() * 2;
    let source = if folder_id == COMMON_VIEWS_FOLDER_ID {
        "common_views"
    } else {
        "folder_local_default"
    };
    let folder_local_default_visible_in_fai_table = folder_id != COMMON_VIEWS_FOLDER_ID
        && debug_default_folder_associated_named_view(snapshot, folder_id)
            .is_some_and(|view| view.id == message_id);
    let view_invariant_warnings = format_view_handoff_invariant_warnings(
        folder_id,
        message,
        &descriptor_binary,
        &descriptor_strings,
        folder_local_default_visible_in_fai_table,
    );

    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = "0x03",
        request_input_handle_index = request.input_handle_index().unwrap_or(0),
        request_output_handle_index = request.output_handle_index.unwrap_or(0),
        output_handle,
        view_source = source,
        view_folder_id = %format!("0x{folder_id:016x}"),
        view_message_id = %format!("0x{message_id:016x}"),
        opened_view_id = %format!("0x{:016x}", message.id),
        view_canonical_id = %format!("0x{:032x}", message.canonical_id),
        view_name = %message.name,
        view_message_class = "IPM.Microsoft.FolderDesign.NamedView",
        view_version = 8u32,
        view_flags = message.view_flags,
        view_type = message.view_type,
        view_entry_id_decoded_folder_id = %format!("0x{folder_id:016x}"),
        view_entry_id_decoded_message_id = %format!("0x{message_id:016x}"),
        folder_local_default_visible_in_fai_table,
        descriptor_binary_len = descriptor_binary.len(),
        descriptor_strings_chars,
        descriptor_strings_utf16_bytes,
        descriptor_summary = %descriptor_summary,
        associated_config_0e0b_shape = %format!(
            "same_as_descriptor_binary;bytes={}",
            descriptor_binary.len()
        ),
        required_view_descriptor_version_present = true,
        required_view_descriptor_name_present = !message.name.is_empty(),
        required_view_descriptor_binary_present = !descriptor_binary.is_empty(),
        required_view_descriptor_strings_present = !descriptor_strings.is_empty(),
        view_invariant_warnings = %view_invariant_warnings,
        "rca debug outlook view handoff"
    );

    if !view_invariant_warnings.is_empty() {
        tracing::warn!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            account_id = %principal.account_id,
            mailbox = %principal.email,
            request_type = "Execute",
            request_rop_id = "0x03",
            view_source = source,
            view_folder_id = %format!("0x{folder_id:016x}"),
            view_message_id = %format!("0x{message_id:016x}"),
            view_invariant_warnings = %view_invariant_warnings,
            descriptor_summary = %descriptor_summary,
            message = "rca debug outlook view handoff invariant warning",
        );
    }
}

fn format_view_handoff_invariant_warnings(
    folder_id: u64,
    message: &crate::mapi_store::MapiCommonViewNamedViewMessage,
    descriptor_binary: &[u8],
    descriptor_strings: &str,
    folder_local_default_visible_in_fai_table: bool,
) -> String {
    let mut warnings = Vec::new();
    if folder_id != COMMON_VIEWS_FOLDER_ID && !folder_local_default_visible_in_fai_table {
        warnings.push("folder_local_default_view_not_visible_in_associated_table");
    }
    if message.name.is_empty() {
        warnings.push("missing_view_descriptor_name");
    }
    if descriptor_strings.is_empty() {
        warnings.push("missing_view_descriptor_strings");
    }
    if descriptor_binary.len() < 60 {
        warnings.push("descriptor_binary_too_short");
    }
    if descriptor_binary
        .get(8..12)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_le_bytes)
        != Some(8)
    {
        warnings.push("descriptor_version_not_8");
    }
    if view_descriptor_column_count(descriptor_binary) == Some(0) {
        warnings.push("descriptor_has_no_columns");
    }
    warnings.join("|")
}

pub(in crate::mapi::dispatch) fn format_outlook_view_handoff_table_contract(
    folder_id: u64,
    associated: bool,
    columns: &[u32],
    snapshot: &MapiMailStoreSnapshot,
) -> String {
    if !is_outlook_folder_table_debug_target(folder_id) {
        return String::new();
    }
    let container_class = snapshot
        .collaboration_folder_for_id(folder_id)
        .map(|folder| collaboration_folder_message_class(folder.kind))
        .or_else(|| advertised_special_folder_container_class(folder_id));
    let common_views_default_id = container_class
        .and_then(|container_class| default_common_views_named_view_id(container_class, folder_id));
    let uses_common_views =
        folder_id == COMMON_VIEWS_FOLDER_ID || common_views_default_id.is_some();
    let expected_common_views_id = common_views_default_id
        .unwrap_or(crate::mapi_store::OUTLOOK_COMMON_VIEWS_COMPACT_NAMED_VIEW_ID);
    if associated && folder_id == COMMON_VIEWS_FOLDER_ID {
        let view = snapshot.common_view_named_view_message_for_id(expected_common_views_id);
        let descriptor_summary = view
            .as_ref()
            .map(|message| {
                let definition = outlook_folder_view_definition(message.folder_id, &message.name);
                let descriptor = view_descriptor_binary(&definition);
                format_view_descriptor_binary_summary(&descriptor)
            })
            .unwrap_or_default();
        let selected_view_name = view
            .as_ref()
            .map(|message| message.name.as_str())
            .unwrap_or("");
        return format!(
            "folder_local_default_supported=false;\
             folder_local_default_visible_in_fai_table=false;\
             associated_navigation_table=true;\
             advertised_default_view_folder_id=0x{COMMON_VIEWS_FOLDER_ID:016x};\
             selected_view_name={selected_view_name};\
             expected_view_message_id=0x{expected_common_views_id:016x};\
             selected_property_tag_count={};\
             descriptor_comparison=not_applicable_common_views_associated_table;\
             descriptor_summary={descriptor_summary}",
            columns.len()
        );
    }
    let (view_folder_id, expected_view_message_id, view) = if uses_common_views {
        (
            COMMON_VIEWS_FOLDER_ID,
            expected_common_views_id,
            snapshot.common_view_named_view_message_for_id(expected_common_views_id),
        )
    } else {
        (
            folder_id,
            crate::mapi_store::outlook_default_folder_named_view_id(folder_id),
            debug_default_folder_associated_named_view(snapshot, folder_id),
        )
    };
    let folder_local_default_view = if folder_id == COMMON_VIEWS_FOLDER_ID {
        None
    } else {
        container_class
            .filter(|container_class| default_view_supported_folder(folder_id, container_class))
            .and_then(|_| {
                snapshot.default_folder_named_view_message(
                    folder_id,
                    crate::mapi_store::outlook_default_folder_named_view_id(folder_id),
                )
            })
    };
    let folder_local_default_supported = folder_local_default_view.is_some();
    let exact_named_view_restriction = MapiRestriction::Property {
        relop: 0x04,
        property_tag: PID_TAG_MESSAGE_CLASS_W,
        value: MapiValue::String("IPM.Microsoft.FolderDesign.NamedView".to_string()),
    };
    let folder_local_default_visible_in_fai_table = folder_local_default_supported
        && debug_associated_table_rows(
            folder_id,
            snapshot,
            Some(&exact_named_view_restriction),
            Uuid::nil(),
        )
        .iter()
        .any(|row| {
            debug_associated_row_id(row)
                == crate::mapi_store::outlook_default_folder_named_view_id(folder_id)
        });
    let descriptor_summary = view
        .as_ref()
        .map(|message| {
            let definition = outlook_folder_view_definition(message.folder_id, &message.name);
            let descriptor = view_descriptor_binary(&definition);
            format_view_descriptor_binary_summary(&descriptor)
        })
        .unwrap_or_default();
    let selected_missing_descriptor_columns = (!associated)
        .then(|| {
            view.as_ref().map(|message| {
                let definition = outlook_folder_view_definition(message.folder_id, &message.name);
                let descriptor = view_descriptor_binary(&definition);
                let descriptor_columns = view_descriptor_property_tags(&descriptor);
                let comparable_columns = view_descriptor_comparable_selected_columns(columns);
                missing_debug_property_tags(&comparable_columns, &descriptor_columns)
            })
        })
        .flatten()
        .unwrap_or_default();
    let selected_view_name = view
        .as_ref()
        .map(|message| message.name.as_str())
        .unwrap_or("");
    format!(
        "folder_local_default_supported={folder_local_default_supported};\
         folder_local_default_visible_in_fai_table={folder_local_default_visible_in_fai_table};\
         advertised_default_view_folder_id=0x{view_folder_id:016x};\
         selected_view_name={selected_view_name};\
         expected_view_message_id=0x{:016x};selected_property_tag_count={};\
         selected_missing_descriptor_columns={selected_missing_descriptor_columns};\
         descriptor_summary={descriptor_summary}",
        expected_view_message_id,
        columns.len()
    )
}

pub(in crate::mapi::dispatch) fn format_outlook_view_descriptor_named_property_context(
    session: &MapiSession,
    folder_id: u64,
    snapshot: &MapiMailStoreSnapshot,
) -> String {
    let Some(view) = debug_advertised_default_named_view(snapshot, folder_id) else {
        return String::new();
    };
    let definition = outlook_folder_view_definition(view.folder_id, &view.name);
    let descriptor = view_descriptor_binary(&definition);
    let descriptor_columns = view_descriptor_property_tags(&descriptor);
    format_debug_named_property_context(session, &descriptor_columns)
}

pub(in crate::mapi::dispatch) fn outlook_view_descriptor_visible_property_tags(
    folder_id: u64,
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u32> {
    let Some(view) = debug_advertised_default_named_view(snapshot, folder_id) else {
        return Vec::new();
    };
    let definition = outlook_folder_view_definition(view.folder_id, &view.name);
    let descriptor = view_descriptor_binary(&definition);
    view_descriptor_property_tags(&descriptor)
}

pub(in crate::mapi::dispatch) fn format_inbox_view_descriptor_behavior_contract(
    folder_id: u64,
    associated: bool,
    position: usize,
    forward_read: bool,
    row_count: usize,
    sort_orders: &[MapiSortOrder],
    restriction: Option<&MapiRestriction>,
    columns: &[u32],
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> String {
    if associated || folder_id != INBOX_FOLDER_ID {
        return String::new();
    }
    let Some(message) = debug_advertised_default_named_view(snapshot, folder_id) else {
        return "default_view=missing".to_string();
    };
    let definition = outlook_folder_view_definition(message.folder_id, &message.name);
    let descriptor = view_descriptor_binary(&definition);
    let descriptor_columns = view_descriptor_property_tags(&descriptor);
    let mut rows = emails_for_folder(folder_id, mailboxes, emails);
    rows.retain(|email| restriction_matches_email(restriction, email));
    sort_emails(&mut rows, sort_orders);
    let selected = select_query_window(rows.len(), position, forward_read, row_count);
    let comparable_columns = view_descriptor_comparable_selected_columns(columns);
    let selected_missing_descriptor_columns =
        missing_debug_property_tags(&comparable_columns, &descriptor_columns);
    let sample_values = selected
        .iter()
        .take(3)
        .map(|index| {
            let email = rows[*index];
            let values = descriptor_columns
                .iter()
                .map(|tag| {
                    let value = normal_message_debug_property_value(email, *tag)
                        .map(|value| format_normal_message_debug_value(*tag, &value))
                        .unwrap_or_else(|| "default".to_string());
                    format!("0x{tag:08x}={value}")
                })
                .collect::<Vec<_>>()
                .join(",");
            format!(
                "index={index};mid=0x{:016x};{}",
                mapi_message_id(email),
                values
            )
        })
        .collect::<Vec<_>>()
        .join("|");
    let descriptor_column_projection = descriptor_columns
        .iter()
        .map(|tag| {
            let projected = selected
                .iter()
                .any(|index| normal_message_debug_property_value(rows[*index], *tag).is_some());
            format!("0x{tag:08x}:projected={projected}")
        })
        .collect::<Vec<_>>()
        .join(",");

    format!(
        "default_view_id=0x{:016x};view_name={};descriptor_summary={};\
         descriptor_columns={};selected_missing_descriptor_columns={};\
         total_rows={};position={};forward={};requested={};sampled={};\
         descriptor_column_projection={};sample_values={}",
        message.id,
        message.name,
        format_view_descriptor_binary_summary(&descriptor),
        format_debug_property_tags(&descriptor_columns),
        selected_missing_descriptor_columns,
        rows.len(),
        position,
        forward_read,
        row_count,
        selected.len().min(3),
        descriptor_column_projection,
        sample_values
    )
}

pub(in crate::mapi::dispatch) fn format_inbox_view_descriptor_set_columns_behavior_contract(
    folder_id: u64,
    associated: bool,
    columns: &[u32],
    snapshot: &MapiMailStoreSnapshot,
) -> String {
    if associated || folder_id != INBOX_FOLDER_ID {
        return String::new();
    }
    let Some(message) = debug_advertised_default_named_view(snapshot, folder_id) else {
        return "default_view=missing".to_string();
    };
    let definition = outlook_folder_view_definition(message.folder_id, &message.name);
    let descriptor = view_descriptor_binary(&definition);
    let descriptor_columns = view_descriptor_property_tags(&descriptor);
    let comparable_columns = view_descriptor_comparable_selected_columns(columns);
    let selected_missing_descriptor_columns =
        missing_debug_property_tags(&comparable_columns, &descriptor_columns);

    format!(
        "phase=setcolumns;default_view_id=0x{:016x};view_name={};\
         descriptor_summary={};descriptor_columns={};\
         selected_columns={};selected_missing_descriptor_columns={}",
        message.id,
        message.name,
        format_view_descriptor_binary_summary(&descriptor),
        format_debug_property_tags(&descriptor_columns),
        format_debug_property_tags(columns),
        selected_missing_descriptor_columns
    )
}

pub(in crate::mapi::dispatch) fn format_default_view_table_compatibility_contract(
    folder_id: u64,
    associated: bool,
    columns: &[u32],
    sort_orders: &[MapiSortOrder],
    restriction: Option<&MapiRestriction>,
    snapshot: &MapiMailStoreSnapshot,
) -> String {
    if associated {
        return String::new();
    }
    let Some(message) = debug_advertised_default_named_view(snapshot, folder_id) else {
        return "default_view=missing".to_string();
    };
    let definition = outlook_folder_view_definition(message.folder_id, &message.name);
    let descriptor = view_descriptor_binary(&definition);
    let descriptor_columns = view_descriptor_property_tags(&descriptor);
    let all_descriptor_columns = view_descriptor_all_property_tags(&descriptor);
    let descriptor_sort_column = descriptor
        .get(24..28)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_le_bytes)
        .and_then(|column| usize::try_from(column).ok());
    let descriptor_sort_tag =
        descriptor_sort_column.and_then(|index| all_descriptor_columns.get(index).copied());
    let table_primary_sort_tag = sort_orders.first().map(|sort| sort.property_tag);
    let descriptor_missing_from_table =
        normal_message_table_unsupported_columns_from_summary(&descriptor_columns);
    let descriptor_columns_not_selected = missing_debug_property_tags(&descriptor_columns, columns);
    let table_sort_matches_descriptor = descriptor_sort_tag
        .zip(table_primary_sort_tag)
        .is_some_and(|(expected, actual)| expected == actual);

    format!(
        "folder=0x{folder_id:016x};view_folder=0x{:016x};view=0x{:016x};\
         view_name={};descriptor_summary={};descriptor_columns_missing_from_table={};\
         descriptor_columns_not_selected={};\
         descriptor_sort_tag={};table_primary_sort_tag={};table_sort_matches_descriptor={};\
         table_sort_count={};table_restriction_present={}",
        message.folder_id,
        message.id,
        message.name,
        format_view_descriptor_binary_summary(&descriptor),
        descriptor_missing_from_table,
        descriptor_columns_not_selected,
        descriptor_sort_tag
            .map(|tag| format!("0x{tag:08x}"))
            .unwrap_or_else(|| "none".to_string()),
        table_primary_sort_tag
            .map(|tag| format!("0x{tag:08x}"))
            .unwrap_or_else(|| "none".to_string()),
        table_sort_matches_descriptor,
        sort_orders.len(),
        restriction.is_some()
    )
}

fn normal_message_table_unsupported_columns_from_summary(columns: &[u32]) -> String {
    let support = normal_message_table_column_support_summary(columns);
    let defaulted = support_field(&support, "defaulted");
    let named_or_dynamic = support_field(&support, "named_or_dynamic");
    [defaulted, named_or_dynamic]
        .into_iter()
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>()
        .join(",")
}

fn view_descriptor_comparable_selected_columns(columns: &[u32]) -> Vec<u32> {
    columns
        .iter()
        .copied()
        .filter(|tag| {
            !matches!(
                *tag,
                PID_TAG_FOLDER_ID | PID_TAG_MID | PID_TAG_INST_ID | PID_TAG_INSTANCE_NUM
            )
        })
        .collect()
}

fn support_field(summary: &str, name: &str) -> String {
    summary
        .split(';')
        .find_map(|field| field.strip_prefix(name)?.strip_prefix('='))
        .unwrap_or_default()
        .to_string()
}

pub(in crate::mapi::dispatch) fn warn_outlook_view_handoff_table_invariants(
    principal: &AccountPrincipal,
    request_rop_id: &str,
    folder_id: u64,
    associated: bool,
    view_handoff_table_contract: &str,
) {
    if !associated || folder_id == COMMON_VIEWS_FOLDER_ID {
        return;
    }
    let _ = (principal, request_rop_id, view_handoff_table_contract);
}

pub(in crate::mapi::dispatch) fn format_folder_local_default_view_fai_visibility_contract(
    folder_id: u64,
    snapshot: &MapiMailStoreSnapshot,
) -> Option<String> {
    if folder_id == COMMON_VIEWS_FOLDER_ID {
        return None;
    }
    let view = debug_default_folder_associated_named_view(snapshot, folder_id)?;
    if default_common_views_named_view_id(
        advertised_special_folder_container_class(folder_id)?,
        folder_id,
    )
    .is_some()
    {
        return None;
    }
    let rows = debug_associated_table_rows(folder_id, snapshot, None, Uuid::nil());
    let present = rows
        .iter()
        .any(|row| debug_associated_row_id(row) == view.id);
    Some(format!(
        "folder=0x{folder_id:016x};role={};view=0x{:016x};name={};expected=true;present={present};associated_row_count={}",
        debug_role_for_folder_id(folder_id),
        view.id,
        view.name,
        rows.len()
    ))
}

pub(in crate::mapi::dispatch) fn format_view_descriptor_binary_summary(
    descriptor: &[u8],
) -> String {
    let version = descriptor
        .get(8..12)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_le_bytes);
    let flags = descriptor
        .get(12..16)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_le_bytes);
    let column_count = view_descriptor_column_count(descriptor);
    let sort_column = descriptor
        .get(24..28)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_le_bytes);
    let group_count = descriptor
        .get(28..32)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_le_bytes);
    let category_sort = descriptor
        .get(32..36)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_le_bytes);
    let all_column_tags = view_descriptor_all_property_tags(descriptor);
    let visible_column_tags = view_descriptor_property_tags(descriptor);
    let expected_column_bytes = view_descriptor_column_payload_len(descriptor).unwrap_or(60);
    let restriction_bytes = descriptor.len().saturating_sub(expected_column_bytes);

    format!(
        "version={};ul_flags={};column_count={};sort_column={};group_count={};ul_cat_sort={};restriction_bytes={restriction_bytes};column_tags={};visible_column_tags={}",
        version
            .map(|value| value.to_string())
            .unwrap_or_else(|| "missing".to_string()),
        flags
            .map(|value| format!("0x{value:08x}"))
            .unwrap_or_else(|| "missing".to_string()),
        column_count
            .map(|value| value.to_string())
            .unwrap_or_else(|| "missing".to_string()),
        sort_column
            .map(|value| value.to_string())
            .unwrap_or_else(|| "missing".to_string()),
        group_count
            .map(|value| value.to_string())
            .unwrap_or_else(|| "missing".to_string()),
        category_sort
            .map(|value| format!("0x{value:08x}"))
            .unwrap_or_else(|| "missing".to_string()),
        format_debug_property_tags(&all_column_tags),
        format_debug_property_tags(&visible_column_tags)
    )
}

fn view_descriptor_column_count(descriptor: &[u8]) -> Option<u32> {
    descriptor
        .get(20..24)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_le_bytes)
}

fn view_descriptor_column_payload_len(descriptor: &[u8]) -> Option<usize> {
    let column_count = view_descriptor_column_count(descriptor)? as usize;
    let mut offset = 60usize;
    for _ in 0..column_count {
        let packet = descriptor.get(offset..offset + 36)?;
        let flags = u32::from_le_bytes([packet[12], packet[13], packet[14], packet[15]]);
        let kind = u32::from_le_bytes([packet[28], packet[29], packet[30], packet[31]]);
        offset = offset.checked_add(36)?;
        if flags & 0x0000_1000 == 0 {
            continue;
        }
        offset = offset.checked_add(16)?;
        if kind == 1 {
            let buffer_length = descriptor
                .get(offset..offset + 4)
                .and_then(|bytes| bytes.try_into().ok())
                .map(u32::from_le_bytes)? as usize;
            offset = offset.checked_add(4)?.checked_add(buffer_length)?;
        }
    }
    Some(offset)
}

fn view_descriptor_all_property_tags(descriptor: &[u8]) -> Vec<u32> {
    crate::mapi::properties::view_descriptor_all_property_tags(descriptor)
}

fn view_descriptor_property_tags(descriptor: &[u8]) -> Vec<u32> {
    crate::mapi::properties::view_descriptor_property_tags(descriptor)
}

pub(in crate::mapi::dispatch) fn is_outlook_folder_table_debug_target(folder_id: u64) -> bool {
    matches!(
        folder_id,
        INBOX_FOLDER_ID
            | DRAFTS_FOLDER_ID
            | SENT_FOLDER_ID
            | CONTACTS_FOLDER_ID
            | CALENDAR_FOLDER_ID
            | SUGGESTED_CONTACTS_FOLDER_ID
            | CONTACTS_SEARCH_FOLDER_ID
            | QUICK_CONTACTS_FOLDER_ID
            | IM_CONTACT_LIST_FOLDER_ID
            | TASKS_FOLDER_ID
            | NOTES_FOLDER_ID
            | JOURNAL_FOLDER_ID
            | JUNK_FOLDER_ID
            | COMMON_VIEWS_FOLDER_ID
            | QUICK_STEP_SETTINGS_FOLDER_ID
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn view_handoff_descriptor_summary_reports_outlook_view_shape() {
        let definition = outlook_mail_view_definition("Compact");
        let descriptor = view_descriptor_binary(&definition);
        let summary = format_view_descriptor_binary_summary(&descriptor);

        assert!(summary.contains("version=8"));
        assert!(summary.contains("column_count=11"));
        assert!(summary.contains("sort_column=8"));
        assert!(summary.contains("restriction_bytes=0"));
        assert!(summary.contains("column_tags=0x00040001"));
        assert!(summary.contains(
            "visible_column_tags=0x00170003,0x8503000b,0x001a001e,0x10900003,0x0e1b000b,0x0042001e,0x0037001e,0x0e060040,0x0e080003,0x9000101e"
        ));
        assert!(summary.contains("0x0e060040"));
    }

    #[test]
    fn common_views_table_contract_reports_common_views_named_view() {
        let snapshot = MapiMailStoreSnapshot::empty();
        let summary = format_outlook_view_handoff_table_contract(
            COMMON_VIEWS_FOLDER_ID,
            true,
            &[],
            &snapshot,
        );

        assert!(summary.contains("folder_local_default_supported=false"));
        assert!(summary.contains("advertised_default_view_folder_id=0x0000000000090001"));
        assert!(summary.contains("expected_view_message_id=0x7ffffffffff70001"));
        assert!(summary.contains("associated_navigation_table=true"));
        assert!(
            summary.contains("descriptor_comparison=not_applicable_common_views_associated_table")
        );
        assert!(summary.contains("descriptor_summary=version=8"));
        assert!(!summary.contains("selected_missing_descriptor_columns="));
    }

    #[test]
    fn default_view_table_compatibility_reports_visible_inbox_contract() {
        let snapshot = MapiMailStoreSnapshot::empty();
        let columns = [
            PID_TAG_IMPORTANCE,
            PID_LID_REMINDER_SET_TAG,
            PID_TAG_MESSAGE_CLASS_W,
            PID_TAG_FLAG_STATUS,
            PID_TAG_HAS_ATTACHMENTS,
            PID_TAG_SENT_REPRESENTING_NAME_W,
            PID_TAG_SUBJECT_W,
            PID_TAG_MESSAGE_DELIVERY_TIME,
            PID_TAG_MESSAGE_SIZE,
            PID_NAME_KEYWORDS_TAG,
        ];
        let sort_orders = [MapiSortOrder {
            property_tag: PID_TAG_MESSAGE_DELIVERY_TIME,
            order: 0,
        }];
        let restriction = MapiRestriction::Bitmask {
            property_tag: PID_TAG_MESSAGE_FLAGS,
            mask: MSGFLAG_READ,
            must_be_nonzero: false,
        };

        let summary = format_default_view_table_compatibility_contract(
            INBOX_FOLDER_ID,
            false,
            &columns,
            &sort_orders,
            Some(&restriction),
            &snapshot,
        );

        assert!(summary.contains("view_folder=0x0000000000050001"));
        assert!(summary.contains("view_name=Compact"));
        assert!(summary.contains("descriptor_columns_missing_from_table="));
        assert!(summary.contains("descriptor_sort_tag=0x0e060040"));
        assert!(summary.contains("table_primary_sort_tag=0x0e060040"));
        assert!(summary.contains("table_sort_matches_descriptor=true"));
        assert!(summary.contains("table_restriction_present=true"));
    }
}
