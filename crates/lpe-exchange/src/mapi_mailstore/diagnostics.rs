mod codec;
use codec::*;
pub(crate) use codec::{
    decode_content_transfer_fai_debug_summary, decode_hierarchy_transfer_debug_summary,
    final_sync_state_debug_summary, format_marker_tags, replguid_globset_counters,
    replguid_globset_debug_summary, ContentTransferFaiItemDebug,
};
#[cfg(test)]
pub(crate) use codec::{
    hierarchy_identity_properties_before_display_name, ContentTransferFaiDebugSummary,
};

use super::*;

pub(crate) fn hierarchy_parent_source_key_role(
    parent_folder_id: u64,
    sync_root_folder_id: u64,
    parent_source_key_empty: bool,
) -> &'static str {
    if parent_folder_id == sync_root_folder_id {
        if parent_source_key_empty {
            "sync_root_child_zero_length"
        } else {
            "emitted_sync_root_child_source_key"
        }
    } else if parent_source_key_empty {
        "unexpected_zero_parent_source_key"
    } else {
        "nested_child_source_key"
    }
}

pub(crate) fn log_hierarchy_transfer_debug(
    sync_type: u8,
    sync_flags: u16,
    sync_extra_flags: u32,
    folder_id: u64,
    requested_property_tags: &[u32],
    transfer_buffer: &[u8],
) {
    if sync_type != SYNC_TYPE_HIERARCHY || !tracing::enabled!(tracing::Level::INFO) {
        return;
    }

    match decode_hierarchy_transfer_debug_summary(transfer_buffer) {
        Ok(summary) => {
            tracing::info!(
                rca_debug = true,
                adapter = "mapi",
                endpoint = "emsmdb",
                request_rop_id = "0x70",
                sync_type = format_args!("0x{sync_type:02x}"),
                folder_id = format_args!("0x{folder_id:016x}"),
                transfer_buffer_bytes = transfer_buffer.len(),
                hierarchy_decode_status = "ok",
                marker_count = summary.marker_tags.len(),
                marker_sequence = %format_marker_tags(&summary.marker_tags),
                fast_transfer_property_count = summary.property_count,
                stream_end_marker_seen = summary.stream_end_marker_seen,
                folder_change_count = summary.folder_change_count,
                final_state_present = summary.final_state_present,
                parent_before_child_violations = summary.parent_before_child_violations,
                zero_length_parent_source_key_count = summary.zero_length_parent_source_key_count,
                source_key_lengths = %format_usize_list(&summary.source_key_lengths),
                change_key_lengths = %format_usize_list(&summary.change_key_lengths),
                final_state_property_tags = %format_property_tags(&summary.final_state_property_tags),
                final_state_property_names = %format_property_tag_names(&summary.final_state_property_tags),
                final_state_property_lengths = %format_usize_list(&summary.final_state_property_lengths),
                final_state_expected_property_order_ok =
                    summary.final_state_expected_property_order_ok,
                final_state_idset_given = %summary.final_state_idset_given_summary.as_deref().unwrap_or_default(),
                final_state_cnset_seen = %summary.final_state_cnset_seen_summary.as_deref().unwrap_or_default(),
                emitted_property_tags = %format_property_tags(&summary.emitted_property_tags),
                requested_property_tags = %format_property_tags(requested_property_tags),
                property_tags_filter_mode = hierarchy_property_filter_mode(sync_flags, requested_property_tags),
                "rca debug mapi hierarchy transfer stream"
            );
            log_hierarchy_final_state_debug(sync_type, folder_id, &summary);
            log_hierarchy_microsoft_payload_comparison(
                sync_type,
                sync_flags,
                sync_extra_flags,
                folder_id,
                requested_property_tags,
                &summary,
            );
        }
        Err(error) => tracing::warn!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            request_rop_id = "0x70",
            sync_type = format_args!("0x{sync_type:02x}"),
            folder_id = format_args!("0x{folder_id:016x}"),
            transfer_buffer_bytes = transfer_buffer.len(),
            hierarchy_decode_status = "error",
            hierarchy_decode_error = %error,
            requested_property_tags = %format_property_tags(requested_property_tags),
            property_tags_filter_mode = hierarchy_property_filter_mode(sync_flags, requested_property_tags),
            "rca debug mapi hierarchy transfer stream"
        ),
    }
}

pub(crate) fn log_fai_content_sync_debug(
    sync_type: u8,
    folder_id: u64,
    mailbox_guid: Uuid,
    special_objects: &[SpecialMessageSyncFact],
    transfer_buffer: &[u8],
    context: FaiContentSyncDebugContext<'_>,
) {
    if sync_type != SYNC_TYPE_CONTENTS
        || !matches!(
            folder_id,
            crate::mapi::identity::INBOX_FOLDER_ID | crate::mapi::identity::COMMON_VIEWS_FOLDER_ID
        )
        || !tracing::enabled!(tracing::Level::INFO)
    {
        return;
    }
    let folder_role = match folder_id {
        crate::mapi::identity::INBOX_FOLDER_ID => "inbox",
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID => "__mapi_common_views",
        _ => "",
    };
    match decode_content_transfer_fai_debug_summary(transfer_buffer) {
        Ok(summary) => {
            let item_count = summary.fai_items.len();
            let total_property_count = summary
                .fai_items
                .iter()
                .map(|item| item.property_tags.len())
                .sum::<usize>();
            let persisted_count = summary
                .fai_items
                .iter()
                .filter(|item| {
                    let item_id = item.item_id.unwrap_or_default();
                    let special_object = special_objects
                        .iter()
                        .find(|object| object.item_id == item_id);
                    fai_debug_state_origin(folder_id, special_object, item_id) == "sql_associated"
                })
                .count();
            let virtual_count = summary
                .fai_items
                .iter()
                .filter(|item| {
                    let item_id = item.item_id.unwrap_or_default();
                    fai_debug_state_origin(folder_id, None, item_id) == "mapi_virtual"
                })
                .count();
            let synthetic_count = item_count
                .saturating_sub(persisted_count)
                .saturating_sub(virtual_count);
            let item_order = format_fai_debug_item_order(&summary.fai_items);
            let first_item = summary.fai_items.first();
            let last_item = summary.fai_items.last();
            tracing::info!(
                rca_debug = true,
                adapter = "mapi",
                endpoint = "emsmdb",
                mailbox = %context.mailbox,
                tenant = %context.tenant,
                account = %context.account,
                mapi_request_id = %context.mapi_request_id,
                request_rop_id = %context.request_rop_id,
                folder_id = format_args!("0x{folder_id:016x}"),
                folder_role,
                folder_container_class = debug_container_class_for_fai_folder(folder_id),
                sync_type = format_args!("0x{sync_type:02x}"),
                checkpoint_kind = %context.checkpoint_kind,
                item_count,
                persisted_count,
                synthetic_count,
                virtual_count,
                total_transfer_bytes = transfer_buffer.len(),
                total_property_count,
                first_item_id = %first_item
                    .and_then(|item| item.item_id)
                    .map(format_u64_hex)
                    .unwrap_or_default(),
                first_item_class = %first_item.map(|item| item.message_class.as_str()).unwrap_or_default(),
                first_item_subject = %first_item.map(|item| item.subject.as_str()).unwrap_or_default(),
                last_item_id = %last_item
                    .and_then(|item| item.item_id)
                    .map(format_u64_hex)
                    .unwrap_or_default(),
                last_item_class = %last_item.map(|item| item.message_class.as_str()).unwrap_or_default(),
                last_item_subject = %last_item.map(|item| item.subject.as_str()).unwrap_or_default(),
                item_order = %item_order,
                final_cnset_fai = %summary.final_cnset_seen_fai_summary,
                active_transfer_selection = %context.active_transfer_selection,
                "rca debug mapi fai fasttransfer transfer summary"
            );
            for (index, item) in summary.fai_items.into_iter().enumerate() {
                let special_object = special_objects
                    .iter()
                    .find(|object| object.item_id == item.item_id.unwrap_or_default());
                let canonical_id = special_object
                    .map(|object| object.canonical_id.to_string())
                    .unwrap_or_default();
                let item_id = item.item_id.unwrap_or_default();
                let classification =
                    fai_debug_item_classification(folder_id, special_object, item_id);
                let state_origin = fai_debug_state_origin(folder_id, special_object, item_id);
                let source_repository = fai_debug_source_repository(folder_id, state_origin);
                let expected_entry_id_len =
                    crate::mapi::identity::message_entry_id_from_object_ids(
                        mailbox_guid,
                        folder_id,
                        item_id,
                    )
                    .map(|entry_id| entry_id.len())
                    .unwrap_or_default();
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    mailbox = %context.mailbox,
                    tenant = %context.tenant,
                    account = %context.account,
                    mapi_request_id = %context.mapi_request_id,
                    request_rop_id = %context.request_rop_id,
                    folder_id = format_args!("0x{folder_id:016x}"),
                    folder_role,
                    folder_container_class = debug_container_class_for_fai_folder(folder_id),
                    sync_type = format_args!("0x{sync_type:02x}"),
                    checkpoint_kind = %context.checkpoint_kind,
                    item_index_in_transfer = index,
                    item_count_in_transfer = item_count,
                    item_id = format_args!("0x{item_id:016x}"),
                    global_counter = item.global_counter.unwrap_or_default(),
                    change_number = item.change_number.unwrap_or_default(),
                    canonical_id,
                    sql_row_id = if state_origin == "sql_associated" { canonical_id.as_str() } else { "" },
                    jmap_id = "",
                    folder_canonical_id = "",
                    source_repository,
                    message_class = %item.message_class,
                    subject = %item.subject,
                    associated = item.associated.unwrap_or_default(),
                    classification,
                    state_origin,
                    source_key_hex = %item.source_key_hex,
                    source_key_len = item.source_key_len,
                    parent_source_key_hex = %item.parent_source_key_hex,
                    parent_source_key_len = item.parent_source_key_len,
                    entry_id_len = item.entry_id_len,
                    expected_entry_id_len,
                    record_key_len = item.record_key_len,
                    change_key_len = item.change_key_len,
                    predecessor_change_list_len = item.predecessor_change_list_len,
                    emitted_property_count = item.property_tags.len(),
                    emitted_property_tags = %format_property_tags(&item.property_tags),
                    emitted_property_names = %format_property_tag_names(&item.property_tags),
                    emitted_value_shapes = %format_property_value_shapes(&item.property_value_shapes),
                    final_cnset_fai = %summary.final_cnset_seen_fai_summary,
                    change_number_in_final_cnset_fai = item.change_number_in_final_cnset_fai,
                    transfer_item_start_offset = item.item_start_offset,
                    transfer_item_end_offset = item.item_end_offset,
                    transfer_item_byte_length = item.item_byte_len,
                    cumulative_transfer_bytes_before_item = item.item_start_offset,
                    cumulative_transfer_bytes_after_item = item.item_end_offset,
                    current_transfer_buffer_total_length = transfer_buffer.len(),
                    first_fai_item = index == 0,
                    last_fai_item = index + 1 == item_count,
                    message_start_marker_offset = item
                        .message_start_marker_offset
                        .map(|offset| offset.to_string())
                        .unwrap_or_default(),
                    message_end_marker_offset = item
                        .message_end_marker_offset
                        .map(|offset| offset.to_string())
                        .unwrap_or_default(),
                    property_list_start_offset = item
                        .property_list_start_offset
                        .map(|offset| offset.to_string())
                        .unwrap_or_default(),
                    property_list_end_offset = item
                        .property_list_end_offset
                        .map(|offset| offset.to_string())
                        .unwrap_or_default(),
                    attachment_marker_count = item.attachment_marker_count,
                    recipient_marker_count = item.recipient_marker_count,
                    fasttransfer_marker_summary =
                        %format_fai_fasttransfer_marker_summary(&item),
                    item_payload_preview_hex = %item.payload_preview_hex,
                    item_payload_tail_hex = %item.payload_tail_hex,
                    "rca debug mapi fai fasttransfer item boundary"
                );
                if folder_id == crate::mapi::identity::COMMON_VIEWS_FOLDER_ID {
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %context.mailbox,
                        tenant = %context.tenant,
                        account = %context.account,
                        mapi_request_id = %context.mapi_request_id,
                        request_rop_id = %context.request_rop_id,
                        folder_id = format_args!("0x{folder_id:016x}"),
                        folder_role,
                        item_id = format_args!("0x{item_id:016x}"),
                        global_counter = item.global_counter.unwrap_or_default(),
                        canonical_id,
                        message_class = %item.message_class,
                        subject = %item.subject,
                        associated_present = item.associated.is_some(),
                        associated_value = item.associated.unwrap_or_default(),
                        source_key_hex = %item.source_key_hex,
                        source_key_len = item.source_key_len,
                        parent_source_key_hex = %item.parent_source_key_hex,
                        parent_source_key_len = item.parent_source_key_len,
                        entry_id_len = item.entry_id_len,
                        expected_entry_id_len,
                        persisted = special_object.is_some()
                            && !crate::mapi_store::is_outlook_common_views_default_named_view_id(item_id)
                            && !crate::mapi_store::is_outlook_common_views_default_navigation_shortcut_id(item_id),
                        default = crate::mapi_store::is_outlook_common_views_default_named_view_id(item_id)
                            || crate::mapi_store::is_outlook_common_views_default_navigation_shortcut_id(item_id),
                        virtual_only = false,
                        classification,
                        state_origin,
                        change_number = item.change_number.unwrap_or_default(),
                        change_number_in_final_cnset_fai = item.change_number_in_final_cnset_fai,
                        final_cnset_fai = %summary.final_cnset_seen_fai_summary,
                        final_idset_given = %summary.final_idset_given_summary,
                        emitted_property_tags = %format_property_tags(&item.property_tags),
                        emitted_property_names = %format_property_tag_names(&item.property_tags),
                        value_shapes = %format_property_value_shapes(&item.property_value_shapes),
                        "rca debug mapi common views fai content sync item"
                    );
                } else {
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %context.mailbox,
                        tenant = %context.tenant,
                        account = %context.account,
                        mapi_request_id = %context.mapi_request_id,
                        request_rop_id = %context.request_rop_id,
                        folder_id = format_args!("0x{folder_id:016x}"),
                        folder_role,
                        item_id = format_args!("0x{item_id:016x}"),
                        global_counter = item.global_counter.unwrap_or_default(),
                        canonical_id,
                        message_class = %item.message_class,
                        subject = %item.subject,
                        associated_present = item.associated.is_some(),
                        associated_value = item.associated.unwrap_or_default(),
                        source_key_hex = %item.source_key_hex,
                        source_key_len = item.source_key_len,
                        parent_source_key_hex = %item.parent_source_key_hex,
                        parent_source_key_len = item.parent_source_key_len,
                        entry_id_len = item.entry_id_len,
                        expected_entry_id_len,
                        persisted = special_object.is_some()
                            && !crate::mapi_store::is_outlook_inbox_default_associated_config_id(
                                item_id
                            ),
                        default = crate::mapi_store::is_outlook_inbox_default_associated_config_id(
                            item_id
                        ),
                        virtual_only =
                            crate::mapi_store::is_outlook_inbox_virtual_only_associated_config_id(
                                item_id
                            ),
                        classification,
                        state_origin,
                        change_number = item.change_number.unwrap_or_default(),
                        change_number_in_final_cnset_fai = item.change_number_in_final_cnset_fai,
                        final_cnset_fai = %summary.final_cnset_seen_fai_summary,
                        final_idset_given = %summary.final_idset_given_summary,
                        emitted_property_tags = %format_property_tags(&item.property_tags),
                        emitted_property_names = %format_property_tag_names(&item.property_tags),
                        value_shapes = %format_property_value_shapes(&item.property_value_shapes),
                        "rca debug mapi inbox fai content sync item"
                    );
                }
            }
        }
        Err(error) => {
            if folder_id == crate::mapi::identity::COMMON_VIEWS_FOLDER_ID {
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    request_rop_id = "0x70",
                    folder_id = format_args!("0x{folder_id:016x}"),
                    folder_role,
                    transfer_buffer_bytes = transfer_buffer.len(),
                    parse_error = %error,
                    "rca debug mapi common views fai content sync parse error"
                );
            } else {
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    request_rop_id = "0x70",
                    folder_id = format_args!("0x{folder_id:016x}"),
                    folder_role,
                    transfer_buffer_bytes = transfer_buffer.len(),
                    parse_error = %error,
                    "rca debug mapi inbox fai content sync parse error"
                );
            }
        }
    }
}

fn fai_debug_item_classification(
    folder_id: u64,
    special_object: Option<&SpecialMessageSyncFact>,
    item_id: u64,
) -> &'static str {
    if folder_id == crate::mapi::identity::INBOX_FOLDER_ID {
        if crate::mapi_store::is_outlook_inbox_virtual_only_associated_config_id(item_id) {
            "virtual_only"
        } else if crate::mapi_store::is_outlook_inbox_default_associated_config_id(item_id) {
            "default"
        } else if special_object.is_some() {
            "persisted"
        } else {
            "unknown"
        }
    } else if folder_id == crate::mapi::identity::COMMON_VIEWS_FOLDER_ID {
        if crate::mapi_store::is_outlook_common_views_default_named_view_id(item_id) {
            "default_named_view"
        } else if crate::mapi_store::is_outlook_common_views_default_navigation_shortcut_id(item_id)
        {
            "default_navigation_shortcut"
        } else if special_object.is_some() {
            "persisted_navigation_shortcut"
        } else {
            "unknown"
        }
    } else {
        "unknown"
    }
}

pub(crate) fn fai_debug_state_origin(
    folder_id: u64,
    special_object: Option<&SpecialMessageSyncFact>,
    item_id: u64,
) -> &'static str {
    if folder_id == crate::mapi::identity::INBOX_FOLDER_ID {
        if crate::mapi_store::is_outlook_inbox_virtual_only_associated_config_id(item_id) {
            "mapi_virtual"
        } else if crate::mapi_store::is_outlook_inbox_default_associated_config_id(item_id)
            || crate::mapi_store::is_outlook_default_folder_named_view_id(item_id)
        {
            "mapi_synthetic_default"
        } else if special_object.is_some() {
            "sql_associated"
        } else {
            "unknown"
        }
    } else if folder_id == crate::mapi::identity::COMMON_VIEWS_FOLDER_ID {
        if crate::mapi_store::is_outlook_common_views_default_named_view_id(item_id)
            || crate::mapi_store::is_outlook_common_views_default_navigation_shortcut_id(item_id)
        {
            "mapi_synthetic_default"
        } else if special_object.is_some() {
            "sql_associated"
        } else {
            "unknown"
        }
    } else {
        "unknown"
    }
}

fn fai_debug_source_repository(folder_id: u64, state_origin: &str) -> &'static str {
    match (folder_id, state_origin) {
        (crate::mapi::identity::COMMON_VIEWS_FOLDER_ID, "sql_associated") => {
            "mapi_navigation_shortcuts"
        }
        (crate::mapi::identity::INBOX_FOLDER_ID, "sql_associated") => {
            "mapi_associated_config_messages"
        }
        (_, "mapi_synthetic_default") | (_, "mapi_virtual") => "mapi_store_projection",
        _ => "",
    }
}

fn debug_container_class_for_fai_folder(folder_id: u64) -> &'static str {
    match folder_id {
        crate::mapi::identity::INBOX_FOLDER_ID => "IPF.Note",
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID => "IPF.Note",
        _ => "",
    }
}

fn format_fai_fasttransfer_marker_summary(item: &ContentTransferFaiItemDebug) -> String {
    format!(
        "item_start={};message_start={};message_end={};property_start={};property_end={};recipients={};attachments={}",
        item.item_start_offset,
        item.message_start_marker_offset
            .map(|offset| offset.to_string())
            .unwrap_or_else(|| "missing".to_string()),
        item.message_end_marker_offset
            .map(|offset| offset.to_string())
            .unwrap_or_else(|| "missing".to_string()),
        item.property_list_start_offset
            .map(|offset| offset.to_string())
            .unwrap_or_else(|| "missing".to_string()),
        item.property_list_end_offset
            .map(|offset| offset.to_string())
            .unwrap_or_else(|| "missing".to_string()),
        item.recipient_marker_count,
        item.attachment_marker_count
    )
}

fn format_fai_debug_item_order(items: &[ContentTransferFaiItemDebug]) -> String {
    items
        .iter()
        .enumerate()
        .map(|(index, item)| {
            format!(
                "{}:{}:{}:{}:{}:{}",
                index,
                item.item_id.map(format_u64_hex).unwrap_or_default(),
                item.message_class,
                item.subject,
                item.item_start_offset,
                item.item_byte_len
            )
        })
        .collect::<Vec<_>>()
        .join("|")
}

pub(crate) fn log_hierarchy_get_buffer_payload_summary(
    sync_type: u8,
    folder_id: u64,
    transfer_status: &str,
    transfer_buffer: &[u8],
) {
    if sync_type != SYNC_TYPE_HIERARCHY || !tracing::enabled!(tracing::Level::INFO) {
        return;
    }

    match decode_hierarchy_transfer_debug_summary(transfer_buffer) {
        Ok(summary) => {
            tracing::info!(
                rca_debug = true,
                adapter = "mapi",
                endpoint = "emsmdb",
                request_type = "Execute",
                request_rop_id = "0x4e",
                sync_type = format_args!("0x{sync_type:02x}"),
                folder_id = format_args!("0x{folder_id:016x}"),
                transfer_status,
                transfer_buffer_bytes = transfer_buffer.len(),
                marker_count = summary.marker_tags.len(),
                marker_sequence = %format_marker_tags(&summary.marker_tags),
                fast_transfer_property_count = summary.property_count,
                stream_end_marker_seen = summary.stream_end_marker_seen,
                final_state_idset_given_bytes = summary.final_state_idset_given_len,
                final_state_cnset_seen_bytes = summary.final_state_cnset_seen_len,
                final_state_expected_property_order_ok =
                    summary.final_state_expected_property_order_ok,
                folder_change_count = summary.folder_change_count,
                zero_parent_count = summary.zero_length_parent_source_key_count,
                nonzero_parent_count = summary.nonzero_parent_source_key_count,
                first_folder_name = %summary.first_folder_name(),
                last_folder_name = %summary.last_folder_name(),
                parent_before_child_violations = summary.parent_before_child_violations,
                final_state_idset_given_includes_all_expected_folder_source_key_counters =
                    summary.final_state_idset_given_includes_all_expected_folder_source_counters,
                final_state_cnset_seen_includes_all_expected_folder_change_counters =
                    summary.final_state_cnset_seen_includes_all_expected_folder_change_counters,
                "rca debug mapi hierarchy get buffer payload summary"
            );
            log_hierarchy_semantic_validation(sync_type, folder_id, transfer_status, &summary);
        }
        Err(error) => tracing::warn!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            request_type = "Execute",
            request_rop_id = "0x4e",
            sync_type = format_args!("0x{sync_type:02x}"),
            folder_id = format_args!("0x{folder_id:016x}"),
            transfer_status,
            transfer_buffer_bytes = transfer_buffer.len(),
            hierarchy_decode_status = "error",
            hierarchy_decode_error = %error,
            "rca debug mapi hierarchy get buffer payload summary"
        ),
    }
}

pub(crate) fn hierarchy_transfer_close_summary(
    sync_type: u8,
    folder_id: u64,
    transfer_buffer: &[u8],
) -> String {
    if sync_type != SYNC_TYPE_HIERARCHY {
        return String::new();
    }
    let Ok(summary) = decode_hierarchy_transfer_debug_summary(transfer_buffer) else {
        return "hierarchy_debug=parse_error".to_string();
    };
    let validation = hierarchy_semantic_validation(folder_id, &summary);
    format!(
        "first={};last={};root_first={};root_index={};root_name={};root_folder={};root_parent={};root_parent_sk={};root_type={};root_access={};root_subfolders={};parent_before_child={};semantic={};idset_missing={};cnset_missing={};final_state_order={}",
        summary.first_folder_name(),
        summary.last_folder_name(),
        validation.sync_root_row_index == 1,
        validation.sync_root_row_index,
        validation.sync_root_display_name,
        validation.sync_root_folder_id,
        validation.sync_root_parent_folder_id,
        validation.sync_root_parent_source_key_len,
        validation.sync_root_folder_type,
        validation.sync_root_access,
        validation.sync_root_subfolders,
        validation.parent_before_child_violations,
        validation.semantic_flags,
        format_counter_list(&validation.idset_missing_source_counters),
        format_counter_list(&validation.cnset_missing_change_counters),
        summary.final_state_expected_property_order_ok,
    )
}

pub(crate) fn default_folder_hierarchy_membership_summary(
    sync_type: u8,
    sync_root_folder_id: u64,
    transfer_buffer: &[u8],
) -> String {
    if sync_type != SYNC_TYPE_HIERARCHY {
        return String::new();
    }
    let Ok(summary) = decode_hierarchy_transfer_debug_summary(transfer_buffer) else {
        return "hierarchy_debug=parse_error".to_string();
    };
    default_folder_hierarchy_membership_specs()
        .iter()
        .map(|(name, folder_id)| {
            let source_counter = crate::mapi::identity::global_counter_from_store_id(*folder_id)
                .unwrap_or_else(|| change_number_for_store_id(*folder_id));
            let change_counter = change_number_for_store_id(*folder_id);
            let row = summary
                .rows
                .iter()
                .find(|row| {
                    row.folder_id == Some(*folder_id)
                        || row.source_counter == Some(source_counter)
                });
            let row_present = row.is_some();
            let row_index = row.map(|row| row.row_index).unwrap_or_default();
            let parent_folder_matches = row
                .and_then(|row| row.parent_folder_id)
                .is_some_and(|parent| parent == sync_root_folder_id);
            let parent_source_key_expected =
                row.is_some_and(|row| row.parent_source_key_len == 0);
            let source_key_len = row.map(|row| row.source_key_len).unwrap_or_default();
            let parent_source_key_len = row.map(|row| row.parent_source_key_len).unwrap_or_default();
            let idset_present = summary.final_state_idset_given_counters.contains(&source_counter);
            let cnset_present = summary.final_state_cnset_seen_counters.contains(&change_counter);
            format!(
                "{name}:fid=0x{folder_id:016x};row_present={row_present};row_index={row_index};parent_folder_matches={parent_folder_matches};parent_source_key_expected={parent_source_key_expected};source_key_len={source_key_len};parent_source_key_len={parent_source_key_len};idset_present={idset_present};cnset_present={cnset_present}"
            )
        })
        .collect::<Vec<_>>()
        .join("|")
}

fn default_folder_hierarchy_membership_specs() -> [(&'static str, u64); 10] {
    [
        ("inbox", crate::mapi::identity::INBOX_FOLDER_ID),
        ("drafts", crate::mapi::identity::DRAFTS_FOLDER_ID),
        ("outbox", crate::mapi::identity::OUTBOX_FOLDER_ID),
        ("sent", crate::mapi::identity::SENT_FOLDER_ID),
        ("trash", crate::mapi::identity::TRASH_FOLDER_ID),
        ("calendar", crate::mapi::identity::CALENDAR_FOLDER_ID),
        ("contacts", crate::mapi::identity::CONTACTS_FOLDER_ID),
        ("journal", crate::mapi::identity::JOURNAL_FOLDER_ID),
        ("notes", crate::mapi::identity::NOTES_FOLDER_ID),
        ("tasks", crate::mapi::identity::TASKS_FOLDER_ID),
    ]
}

fn log_hierarchy_semantic_validation(
    sync_type: u8,
    folder_id: u64,
    transfer_status: &str,
    summary: &HierarchyTransferDebugSummary,
) {
    let validation = hierarchy_semantic_validation(folder_id, summary);
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        request_type = "Execute",
        request_rop_id = "0x4e",
        sync_type = format_args!("0x{sync_type:02x}"),
        folder_id = format_args!("0x{folder_id:016x}"),
        transfer_status,
        completed = transfer_status == "0x0003",
        semantic_flags = %validation.semantic_flags,
        sync_root_source_counter = validation.sync_root_source_counter,
        sync_root_change_counter = validation.sync_root_change_counter,
        sync_root_row_present = validation.sync_root_row_present,
        sync_root_row_index = validation.sync_root_row_index,
        sync_root_row_first = validation.sync_root_row_index == 1,
        sync_root_display_name = %validation.sync_root_display_name,
        sync_root_folder_id = %validation.sync_root_folder_id,
        sync_root_parent_folder_id = %validation.sync_root_parent_folder_id,
        sync_root_parent_source_key_len = validation.sync_root_parent_source_key_len,
        sync_root_folder_type = validation.sync_root_folder_type,
        sync_root_access = validation.sync_root_access,
        sync_root_subfolders = validation.sync_root_subfolders,
        sync_root_counter_in_final_idset = validation.sync_root_counter_in_final_idset,
        sync_root_counter_in_final_cnset = validation.sync_root_counter_in_final_cnset,
        first_row_name = %validation.first_row_name,
        first_row_folder_id = %validation.first_row_folder_id,
        first_row_parent_folder_id = %validation.first_row_parent_folder_id,
        parent_before_child_violations = validation.parent_before_child_violations,
        root_inclusive_idset_given_bytes = validation.root_inclusive_idset_given_len,
        root_inclusive_cnset_seen_bytes = validation.root_inclusive_cnset_seen_len,
        root_inclusive_idset_given_delta_bytes = validation.root_inclusive_idset_given_delta_bytes,
        root_inclusive_cnset_seen_delta_bytes = validation.root_inclusive_cnset_seen_delta_bytes,
        root_inclusive_idset_given = %validation.root_inclusive_idset_given_summary,
        root_inclusive_cnset_seen = %validation.root_inclusive_cnset_seen_summary,
        top_level_row_count = validation.top_level_row_count,
        nested_row_count = validation.nested_row_count,
        rows_without_folder_id = validation.rows_without_folder_id,
        rows_missing_core_property_count = validation.rows_missing_core_property_count,
        rows_with_content_counts_present = validation.rows_with_content_counts_present,
        rows_with_folder_type_present = validation.rows_with_folder_type_present,
        rows_with_access_present = validation.rows_with_access_present,
        idset_missing_source_counters = %format_counter_list(&validation.idset_missing_source_counters),
        idset_extra_source_counters = %format_counter_list(&validation.idset_extra_source_counters),
        cnset_missing_change_counters = %format_counter_list(&validation.cnset_missing_change_counters),
        cnset_extra_change_counters = %format_counter_list(&validation.cnset_extra_change_counters),
        top_level_row_names = %validation.top_level_row_names,
        rows_missing_core_property_names = %validation.rows_missing_core_property_names,
        "rca debug mapi hierarchy semantic validation"
    );
}

fn log_hierarchy_final_state_debug(
    sync_type: u8,
    folder_id: u64,
    summary: &HierarchyTransferDebugSummary,
) {
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        request_rop_id = "0x70",
        sync_type = format_args!("0x{sync_type:02x}"),
        folder_id = format_args!("0x{folder_id:016x}"),
        final_metatag_idset_given = %summary.final_state_idset_given_summary.as_deref().unwrap_or_default(),
        final_metatag_cnset_seen = %summary.final_state_cnset_seen_summary.as_deref().unwrap_or_default(),
        final_metatag_idset_given_bytes = summary.final_state_idset_given_len,
        final_metatag_cnset_seen_bytes = summary.final_state_cnset_seen_len,
        final_state_expected_folder_counter_count = summary.folder_change_count,
        final_state_folder_change_count = summary.folder_change_count,
        final_metatag_idset_given_counter_count = summary.final_state_idset_given_counters.len(),
        final_metatag_cnset_seen_counter_count = summary.final_state_cnset_seen_counters.len(),
        final_state_expected_property_order_ok = summary.final_state_expected_property_order_ok,
        final_metatag_idset_given_includes_all_expected_folder_source_key_counters =
            summary.final_state_idset_given_includes_all_expected_folder_source_counters,
        final_metatag_cnset_seen_includes_all_expected_folder_change_counters =
            summary.final_state_cnset_seen_includes_all_expected_folder_change_counters,
        "rca debug mapi hierarchy final state"
    );
}

fn log_hierarchy_microsoft_payload_comparison(
    sync_type: u8,
    sync_flags: u16,
    sync_extra_flags: u32,
    folder_id: u64,
    requested_property_tags: &[u32],
    summary: &HierarchyTransferDebugSummary,
) {
    let comparison = hierarchy_microsoft_payload_comparison(
        sync_flags,
        sync_extra_flags,
        folder_id,
        requested_property_tags,
        summary,
    );
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        request_rop_id = "0x70",
        sync_type = format_args!("0x{sync_type:02x}"),
        folder_id = format_args!("0x{folder_id:016x}"),
        exchange_folder_change_required_missing_row_count =
            comparison.required_missing_row_names.len(),
        exchange_folder_change_required_missing_rows =
            %comparison.required_missing_row_names.join(","),
        exchange_folder_id_expected = comparison.folder_id_expected,
        exchange_folder_id_presence_mismatch_count =
            comparison.folder_id_presence_mismatch_rows.len(),
        exchange_folder_id_presence_mismatch_rows =
            %comparison.folder_id_presence_mismatch_rows.join(","),
        exchange_parent_folder_id_expected_by_no_foreign_identifiers =
            comparison.parent_folder_id_expected_by_no_foreign_identifiers,
        exchange_parent_folder_id_recommended_by_eid =
            comparison.parent_folder_id_recommended_by_eid,
        exchange_parent_folder_id_missing_required_count =
            comparison.parent_folder_id_missing_required_rows.len(),
        exchange_parent_folder_id_missing_required_rows =
            %comparison.parent_folder_id_missing_required_rows.join(","),
        exchange_optional_property_tags = %format_property_tags(&comparison.optional_property_tags),
        exchange_optional_property_names =
            %format_property_tag_names(&comparison.optional_property_tags),
        exchange_requested_excluded_property_present_tags =
            %format_property_tags(&comparison.requested_excluded_property_present_tags),
        exchange_requested_excluded_property_present_names =
            %format_property_tag_names(&comparison.requested_excluded_property_present_tags),
        exchange_final_state_exact_property_sequence =
            comparison.final_state_exact_property_sequence,
        exchange_final_state_missing_property_tags =
            %format_property_tags(&comparison.final_state_missing_property_tags),
        exchange_final_state_missing_property_names =
            %format_property_tag_names(&comparison.final_state_missing_property_tags),
        exchange_final_state_extra_property_tags =
            %format_property_tags(&comparison.final_state_extra_property_tags),
        exchange_final_state_extra_property_names =
            %format_property_tag_names(&comparison.final_state_extra_property_tags),
        exchange_final_state_idset_missing_source_counters =
            %format_counter_list(&comparison.final_state_idset_missing_source_counters),
        exchange_final_state_idset_extra_source_counters =
            %format_counter_list(&comparison.final_state_idset_extra_source_counters),
        exchange_final_state_cnset_missing_change_counters =
            %format_counter_list(&comparison.final_state_cnset_missing_change_counters),
        exchange_final_state_cnset_extra_change_counters =
            %format_counter_list(&comparison.final_state_cnset_extra_change_counters),
        "rca debug mapi hierarchy microsoft payload comparison"
    );
}

fn hierarchy_property_filter_mode(
    sync_flags: u16,
    requested_property_tags: &[u32],
) -> &'static str {
    if requested_property_tags.is_empty() {
        "none"
    } else if sync_flags & 0x0080 == 0 {
        "exclude"
    } else {
        "only-specified"
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct HierarchyMicrosoftPayloadComparison {
    pub(crate) required_missing_row_names: Vec<String>,
    pub(crate) folder_id_expected: bool,
    pub(crate) folder_id_presence_mismatch_rows: Vec<String>,
    pub(crate) parent_folder_id_expected_by_no_foreign_identifiers: bool,
    pub(crate) parent_folder_id_recommended_by_eid: bool,
    pub(crate) parent_folder_id_missing_required_rows: Vec<String>,
    pub(crate) optional_property_tags: Vec<u32>,
    pub(crate) requested_excluded_property_present_tags: Vec<u32>,
    pub(crate) final_state_exact_property_sequence: bool,
    pub(crate) final_state_missing_property_tags: Vec<u32>,
    pub(crate) final_state_extra_property_tags: Vec<u32>,
    pub(crate) final_state_idset_missing_source_counters: Vec<u64>,
    pub(crate) final_state_idset_extra_source_counters: Vec<u64>,
    pub(crate) final_state_cnset_missing_change_counters: Vec<u64>,
    pub(crate) final_state_cnset_extra_change_counters: Vec<u64>,
}

pub(crate) fn hierarchy_microsoft_payload_comparison(
    sync_flags: u16,
    sync_extra_flags: u32,
    _sync_root_folder_id: u64,
    requested_property_tags: &[u32],
    summary: &HierarchyTransferDebugSummary,
) -> HierarchyMicrosoftPayloadComparison {
    // [MS-OXCFXICS] section 2.2.4.3.5: PidTagFolderId is present if and only if
    // the Eid synchronization extra flag is set.
    let folder_id_expected = sync_extra_flags & SYNC_EXTRA_FLAG_EID != 0;
    let parent_folder_id_expected_by_no_foreign_identifiers =
        sync_flags & SYNC_FLAG_NO_FOREIGN_IDENTIFIERS != 0;
    let parent_folder_id_recommended_by_eid = sync_extra_flags & SYNC_EXTRA_FLAG_EID != 0;
    let required_tags = microsoft_folder_change_required_tags();
    let mut optional_property_tags = BTreeSet::new();
    let mut requested_excluded_property_present_tags = BTreeSet::new();
    let mut required_missing_row_names = Vec::new();
    let mut folder_id_presence_mismatch_rows = Vec::new();
    let mut parent_folder_id_missing_required_rows = Vec::new();

    for row in &summary.rows {
        if !required_tags
            .iter()
            .all(|required| row.property_tags.contains(required))
        {
            required_missing_row_names.push(row.display_name.clone());
        }

        if row.property_tags.contains(&PID_TAG_FOLDER_ID) != folder_id_expected {
            folder_id_presence_mismatch_rows.push(row.display_name.clone());
        }

        if parent_folder_id_expected_by_no_foreign_identifiers
            && !row.property_tags.contains(&PID_TAG_PARENT_FOLDER_ID)
        {
            parent_folder_id_missing_required_rows.push(row.display_name.clone());
        }

        for tag in &row.property_tags {
            if !required_tags.contains(tag)
                && *tag != PID_TAG_FOLDER_ID
                && *tag != PID_TAG_PARENT_FOLDER_ID
            {
                optional_property_tags.insert(*tag);
            }
            if property_tag_requested(requested_property_tags, *tag)
                && hierarchy_property_filter_mode(sync_flags, requested_property_tags) == "exclude"
            {
                requested_excluded_property_present_tags.insert(*tag);
            }
        }
    }

    let expected_final_state_tags = [META_TAG_IDSET_GIVEN, META_TAG_CNSET_SEEN];
    let final_state_missing_property_tags = expected_final_state_tags
        .iter()
        .copied()
        .filter(|tag| !summary.final_state_property_tags.contains(tag))
        .collect::<Vec<_>>();
    let final_state_extra_property_tags = summary
        .final_state_property_tags
        .iter()
        .copied()
        .filter(|tag| !expected_final_state_tags.contains(tag))
        .collect::<Vec<_>>();
    let expected_source_counters = summary
        .rows
        .iter()
        .filter_map(|row| row.source_counter)
        .collect::<Vec<_>>();
    let expected_change_counters = summary
        .rows
        .iter()
        .filter_map(|row| row.change_counter)
        .collect::<Vec<_>>();

    HierarchyMicrosoftPayloadComparison {
        required_missing_row_names,
        folder_id_expected,
        folder_id_presence_mismatch_rows,
        parent_folder_id_expected_by_no_foreign_identifiers,
        parent_folder_id_recommended_by_eid,
        parent_folder_id_missing_required_rows,
        optional_property_tags: optional_property_tags.into_iter().collect(),
        requested_excluded_property_present_tags: requested_excluded_property_present_tags
            .into_iter()
            .collect(),
        final_state_exact_property_sequence: summary.final_state_property_tags.as_slice()
            == expected_final_state_tags.as_slice(),
        final_state_missing_property_tags,
        final_state_extra_property_tags,
        final_state_idset_missing_source_counters: counter_difference(
            &expected_source_counters,
            &summary.final_state_idset_given_counters,
        ),
        final_state_idset_extra_source_counters: counter_difference(
            &summary.final_state_idset_given_counters,
            &expected_source_counters,
        ),
        final_state_cnset_missing_change_counters: counter_difference(
            &expected_change_counters,
            &summary.final_state_cnset_seen_counters,
        ),
        final_state_cnset_extra_change_counters: counter_difference(
            &summary.final_state_cnset_seen_counters,
            &expected_change_counters,
        ),
    }
}

fn microsoft_folder_change_required_tags() -> [u32; 6] {
    [
        PID_TAG_PARENT_SOURCE_KEY,
        PID_TAG_SOURCE_KEY,
        PID_TAG_LAST_MODIFICATION_TIME,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_DISPLAY_NAME_W,
    ]
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct HierarchySemanticValidation {
    pub(crate) sync_root_source_counter: u64,
    pub(crate) sync_root_change_counter: u64,
    pub(crate) sync_root_row_present: bool,
    pub(crate) sync_root_row_index: usize,
    pub(crate) sync_root_display_name: String,
    pub(crate) sync_root_folder_id: String,
    pub(crate) sync_root_parent_folder_id: String,
    pub(crate) sync_root_parent_source_key_len: usize,
    pub(crate) sync_root_folder_type: i32,
    pub(crate) sync_root_access: i32,
    pub(crate) sync_root_subfolders: bool,
    pub(crate) sync_root_counter_in_final_idset: bool,
    pub(crate) sync_root_counter_in_final_cnset: bool,
    pub(crate) first_row_name: String,
    pub(crate) first_row_folder_id: String,
    pub(crate) first_row_parent_folder_id: String,
    pub(crate) parent_before_child_violations: usize,
    pub(crate) root_inclusive_idset_given_len: usize,
    pub(crate) root_inclusive_cnset_seen_len: usize,
    pub(crate) root_inclusive_idset_given_delta_bytes: isize,
    pub(crate) root_inclusive_cnset_seen_delta_bytes: isize,
    pub(crate) root_inclusive_idset_given_summary: String,
    pub(crate) root_inclusive_cnset_seen_summary: String,
    pub(crate) top_level_row_count: usize,
    pub(crate) nested_row_count: usize,
    pub(crate) rows_without_folder_id: usize,
    pub(crate) rows_missing_core_property_count: usize,
    pub(crate) rows_with_content_counts_present: usize,
    pub(crate) rows_with_folder_type_present: usize,
    pub(crate) rows_with_access_present: usize,
    pub(crate) idset_missing_source_counters: Vec<u64>,
    pub(crate) idset_extra_source_counters: Vec<u64>,
    pub(crate) cnset_missing_change_counters: Vec<u64>,
    pub(crate) cnset_extra_change_counters: Vec<u64>,
    pub(crate) top_level_row_names: String,
    pub(crate) rows_missing_core_property_names: String,
    pub(crate) semantic_flags: String,
}

pub(crate) fn hierarchy_semantic_validation(
    sync_root_folder_id: u64,
    summary: &HierarchyTransferDebugSummary,
) -> HierarchySemanticValidation {
    let sync_root_source_counter =
        crate::mapi::identity::global_counter_from_store_id(sync_root_folder_id)
            .unwrap_or_else(|| change_number_for_store_id(sync_root_folder_id));
    let sync_root_change_counter = change_number_for_store_id(sync_root_folder_id);
    let expected_source_counters = summary
        .rows
        .iter()
        .filter_map(|row| row.source_counter)
        .collect::<Vec<_>>();
    let expected_change_counters = summary
        .rows
        .iter()
        .filter_map(|row| row.change_counter)
        .collect::<Vec<_>>();
    let rows_missing_core_properties = summary
        .rows
        .iter()
        .filter(|row| !row.missing_core_property_tags.is_empty())
        .collect::<Vec<_>>();
    let sync_root_row = summary
        .rows
        .iter()
        .find(|row| row.source_counter == Some(sync_root_source_counter));
    let sync_root_source_key_hex = format_debug_hex(&source_key_for_store_id(sync_root_folder_id));
    let top_level_rows = summary
        .rows
        .iter()
        .filter(|row| {
            row.parent_folder_id == Some(sync_root_folder_id)
                || if sync_root_row.is_some() {
                    row.parent_source_key_hex == sync_root_source_key_hex
                } else {
                    row.parent_source_key_len == 0
                }
        })
        .collect::<Vec<_>>();
    let first_row = summary.rows.first();

    let idset_missing_source_counters = counter_difference(
        &expected_source_counters,
        &summary.final_state_idset_given_counters,
    );
    let idset_extra_source_counters = counter_difference(
        &summary.final_state_idset_given_counters,
        &expected_source_counters,
    );
    let cnset_missing_change_counters = counter_difference(
        &expected_change_counters,
        &summary.final_state_cnset_seen_counters,
    );
    let cnset_extra_change_counters = counter_difference(
        &summary.final_state_cnset_seen_counters,
        &expected_change_counters,
    );
    let root_inclusive_idset_given = root_inclusive_idset(
        &summary.final_state_idset_given_counters,
        sync_root_source_counter,
    );
    let root_inclusive_cnset_seen = root_inclusive_idset(
        &summary.final_state_cnset_seen_counters,
        sync_root_change_counter,
    );
    let mut semantic_flags = Vec::new();
    if !summary.stream_end_marker_seen {
        semantic_flags.push("missing_stream_end");
    }
    if !summary.final_state_present {
        semantic_flags.push("missing_final_state");
    }
    if !summary.final_state_expected_property_order_ok {
        semantic_flags.push("final_state_order");
    }
    if !idset_missing_source_counters.is_empty() {
        semantic_flags.push("idset_missing_source");
    }
    if !cnset_missing_change_counters.is_empty() {
        semantic_flags.push("cnset_missing_change");
    }
    if !rows_missing_core_properties.is_empty() {
        semantic_flags.push("row_missing_core");
    }
    if summary.parent_before_child_violations > 0 {
        semantic_flags.push("parent_before_child");
    }
    if top_level_rows.is_empty() {
        semantic_flags.push("no_top_level_rows");
    }

    HierarchySemanticValidation {
        sync_root_source_counter,
        sync_root_change_counter,
        sync_root_row_present: expected_source_counters.contains(&sync_root_source_counter),
        sync_root_row_index: sync_root_row.map(|row| row.row_index).unwrap_or_default(),
        sync_root_display_name: sync_root_row
            .map(|row| row.display_name.clone())
            .unwrap_or_default(),
        sync_root_folder_id: sync_root_row
            .and_then(|row| row.folder_id.map(format_u64_hex))
            .unwrap_or_default(),
        sync_root_parent_folder_id: sync_root_row
            .and_then(|row| row.parent_folder_id.map(format_u64_hex))
            .unwrap_or_default(),
        sync_root_parent_source_key_len: sync_root_row
            .map(|row| row.parent_source_key_len)
            .unwrap_or_default(),
        sync_root_folder_type: sync_root_row
            .and_then(|row| row.folder_type)
            .unwrap_or_default(),
        sync_root_access: sync_root_row.and_then(|row| row.access).unwrap_or_default(),
        sync_root_subfolders: sync_root_row
            .and_then(|row| row.subfolders)
            .unwrap_or_default(),
        sync_root_counter_in_final_idset: summary
            .final_state_idset_given_counters
            .contains(&sync_root_source_counter),
        sync_root_counter_in_final_cnset: summary
            .final_state_cnset_seen_counters
            .contains(&sync_root_change_counter),
        first_row_name: first_row
            .map(|row| row.display_name.clone())
            .unwrap_or_default(),
        first_row_folder_id: first_row
            .and_then(|row| row.folder_id.map(format_u64_hex))
            .unwrap_or_default(),
        first_row_parent_folder_id: first_row
            .and_then(|row| row.parent_folder_id.map(format_u64_hex))
            .unwrap_or_default(),
        parent_before_child_violations: summary.parent_before_child_violations,
        root_inclusive_idset_given_len: root_inclusive_idset_given.len(),
        root_inclusive_cnset_seen_len: root_inclusive_cnset_seen.len(),
        root_inclusive_idset_given_delta_bytes: root_inclusive_idset_given.len() as isize
            - summary.final_state_idset_given_len as isize,
        root_inclusive_cnset_seen_delta_bytes: root_inclusive_cnset_seen.len() as isize
            - summary.final_state_cnset_seen_len as isize,
        root_inclusive_idset_given_summary: format_replguid_globset_debug(
            &root_inclusive_idset_given,
        ),
        root_inclusive_cnset_seen_summary: format_replguid_globset_debug(
            &root_inclusive_cnset_seen,
        ),
        top_level_row_count: top_level_rows.len(),
        nested_row_count: summary.rows.len().saturating_sub(top_level_rows.len()),
        rows_without_folder_id: summary
            .rows
            .iter()
            .filter(|row| row.folder_id.is_none())
            .count(),
        rows_missing_core_property_count: rows_missing_core_properties.len(),
        rows_with_content_counts_present: summary
            .rows
            .iter()
            .filter(|row| row.content_count.is_some() || row.content_unread_count.is_some())
            .count(),
        rows_with_folder_type_present: summary
            .rows
            .iter()
            .filter(|row| row.folder_type.is_some())
            .count(),
        rows_with_access_present: summary
            .rows
            .iter()
            .filter(|row| row.access.is_some())
            .count(),
        idset_missing_source_counters,
        idset_extra_source_counters,
        cnset_missing_change_counters,
        cnset_extra_change_counters,
        top_level_row_names: top_level_rows
            .iter()
            .map(|row| row.display_name.as_str())
            .collect::<Vec<_>>()
            .join(","),
        rows_missing_core_property_names: rows_missing_core_properties
            .iter()
            .map(|row| row.display_name.as_str())
            .collect::<Vec<_>>()
            .join(","),
        semantic_flags: if semantic_flags.is_empty() {
            "ok".to_string()
        } else {
            semantic_flags.join(",")
        },
    }
}

fn root_inclusive_idset(existing_counters: &[u64], root_counter: u64) -> Vec<u8> {
    let mut counters = existing_counters.to_vec();
    counters.push(root_counter);
    replguid_idset_from_counters(&counters)
}

fn counter_difference(left: &[u64], right: &[u64]) -> Vec<u64> {
    let right = right.iter().copied().collect::<BTreeSet<_>>();
    left.iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .difference(&right)
        .copied()
        .collect()
}

fn format_counter_list(counters: &[u64]) -> String {
    counters
        .iter()
        .map(|counter| counter.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct HierarchyTransferDebugSummary {
    pub(crate) marker_tags: Vec<u32>,
    pub(crate) property_count: usize,
    pub(crate) stream_end_marker_seen: bool,
    pub(crate) folder_change_count: usize,
    pub(crate) final_state_present: bool,
    pub(crate) parent_before_child_violations: usize,
    pub(crate) zero_length_parent_source_key_count: usize,
    pub(crate) nonzero_parent_source_key_count: usize,
    pub(crate) source_key_lengths: Vec<usize>,
    pub(crate) change_key_lengths: Vec<usize>,
    pub(crate) final_state_property_tags: Vec<u32>,
    pub(crate) final_state_property_lengths: Vec<usize>,
    pub(crate) final_state_idset_given_len: usize,
    pub(crate) final_state_cnset_seen_len: usize,
    pub(crate) final_state_idset_given_summary: Option<String>,
    pub(crate) final_state_cnset_seen_summary: Option<String>,
    pub(crate) final_state_cnset_seen_fai_summary: Option<String>,
    pub(crate) final_state_cnset_read_summary: Option<String>,
    pub(crate) final_state_idset_given_counters: Vec<u64>,
    pub(crate) final_state_cnset_seen_counters: Vec<u64>,
    pub(crate) final_state_expected_property_order_ok: bool,
    pub(crate) final_state_idset_given_includes_all_expected_folder_source_counters: bool,
    pub(crate) final_state_cnset_seen_includes_all_expected_folder_change_counters: bool,
    pub(crate) emitted_property_tags: Vec<u32>,
    pub(crate) rows: Vec<HierarchyTransferRowDebug>,
}

impl HierarchyTransferDebugSummary {
    pub(crate) fn first_folder_name(&self) -> &str {
        self.rows
            .first()
            .map(|row| row.display_name.as_str())
            .unwrap_or_default()
    }

    pub(crate) fn last_folder_name(&self) -> &str {
        self.rows
            .last()
            .map(|row| row.display_name.as_str())
            .unwrap_or_default()
    }
}

#[derive(Default)]
pub(crate) struct HierarchyTransferFolderDebug {
    source_key: Option<Vec<u8>>,
    parent_source_key: Option<Vec<u8>>,
    change_key: Option<Vec<u8>>,
    predecessor_change_list: Option<Vec<u8>>,
    display_name: Option<String>,
    container_class: Option<String>,
    folder_id: Option<u64>,
    parent_folder_id: Option<u64>,
    last_modification_time: Option<u64>,
    change_number: Option<u64>,
    content_count: Option<i32>,
    content_unread_count: Option<i32>,
    folder_type: Option<i32>,
    local_commit_time_max: Option<u64>,
    deleted_count_total: Option<i32>,
    message_size: Option<i32>,
    access: Option<i32>,
    subfolders: Option<bool>,
    property_tags: Vec<u32>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct HierarchyTransferRowDebug {
    pub(crate) row_index: usize,
    pub(crate) display_name: String,
    pub(crate) container_class: String,
    pub(crate) folder_id: Option<u64>,
    pub(crate) parent_folder_id: Option<u64>,
    pub(crate) source_key_len: usize,
    pub(crate) parent_source_key_len: usize,
    pub(crate) change_key_len: usize,
    pub(crate) source_counter: Option<u64>,
    pub(crate) change_counter: Option<u64>,
    pub(crate) predecessor_change_list_len: usize,
    pub(crate) last_modification_time: Option<u64>,
    pub(crate) change_number: Option<u64>,
    pub(crate) content_count: Option<i32>,
    pub(crate) content_unread_count: Option<i32>,
    pub(crate) folder_type: Option<i32>,
    pub(crate) local_commit_time_max: Option<u64>,
    pub(crate) deleted_count_total: Option<i32>,
    pub(crate) message_size: Option<i32>,
    pub(crate) access: Option<i32>,
    pub(crate) subfolders: Option<bool>,
    pub(crate) source_key_hex: String,
    pub(crate) parent_source_key_hex: String,
    pub(crate) change_key_hex: String,
    pub(crate) property_tags: Vec<u32>,
    pub(crate) missing_core_property_tags: Vec<u32>,
}
