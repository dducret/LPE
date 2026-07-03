use super::super::*;

pub(in crate::mapi::dispatch) fn outlook_bootstrap_query_rows_phase(
    object: Option<&MapiObject>,
) -> Option<(&'static str, u64, bool)> {
    match object {
        Some(MapiObject::HierarchyTable { folder_id, .. })
            if matches!(
                *folder_id,
                ROOT_FOLDER_ID | IPM_SUBTREE_FOLDER_ID | SYNC_ISSUES_FOLDER_ID
            ) =>
        {
            Some(("hierarchy_table_query_rows_completed", *folder_id, false))
        }
        Some(MapiObject::ContentsTable {
            folder_id,
            associated,
            ..
        }) if *associated && *folder_id == COMMON_VIEWS_FOLDER_ID => Some((
            "common_views_associated_table_query_rows_completed",
            *folder_id,
            true,
        )),
        Some(MapiObject::ContentsTable {
            folder_id,
            associated,
            ..
        }) if *associated && *folder_id == INBOX_FOLDER_ID => Some((
            "inbox_associated_table_query_rows_completed",
            *folder_id,
            true,
        )),
        Some(MapiObject::ContentsTable {
            folder_id,
            associated,
            ..
        }) if !*associated && *folder_id == INBOX_FOLDER_ID => Some((
            "inbox_contents_table_query_rows_completed",
            *folder_id,
            false,
        )),
        _ => None,
    }
}

pub(in crate::mapi::dispatch) fn outlook_bootstrap_query_rows_total_count(
    object: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    mailbox_guid: Uuid,
) -> Option<u32> {
    let Some(
        MapiObject::HierarchyTable { folder_id, .. } | MapiObject::ContentsTable { folder_id, .. },
    ) = object
    else {
        return None;
    };
    match object {
        Some(MapiObject::HierarchyTable {
            deleted_advertised_special_folders,
            ..
        }) if matches!(
            *folder_id,
            ROOT_FOLDER_ID | IPM_SUBTREE_FOLDER_ID | SYNC_ISSUES_FOLDER_ID
        ) =>
        {
            Some(hierarchy_row_count_excluding_deleted(
                *folder_id,
                mailboxes,
                snapshot,
                deleted_advertised_special_folders,
            ))
        }
        Some(MapiObject::ContentsTable {
            associated,
            columns,
            restriction,
            ..
        }) if *associated => {
            if *folder_id == COMMON_VIEWS_FOLDER_ID
                && is_unrestricted_common_views_navigation_projection(columns, restriction)
            {
                Some(
                    snapshot
                        .common_views_table_messages()
                        .filter(|message| {
                            matches!(
                                message,
                                crate::mapi_store::MapiCommonViewsMessage::NavigationShortcut(_)
                            )
                        })
                        .count()
                        .min(u32::MAX as usize) as u32,
                )
            } else {
                Some(
                    restricted_associated_folder_message_count(
                        *folder_id,
                        snapshot,
                        restriction.as_ref(),
                        mailbox_guid,
                    )
                    .min(u32::MAX as usize) as u32,
                )
            }
        }
        Some(MapiObject::ContentsTable { associated, .. })
            if !*associated && *folder_id == INBOX_FOLDER_ID =>
        {
            Some(folder_message_count(
                *folder_id, mailboxes, emails, snapshot,
            ))
        }
        _ => None,
    }
}

pub(in crate::mapi::dispatch) fn log_outlook_contents_table_find_row(
    principal: &AccountPrincipal,
    request_id: &str,
    request: &RopRequest,
    object: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    selected_named_property_context: &str,
    snapshot: &MapiMailStoreSnapshot,
    response: &[u8],
) {
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
        return;
    };
    let response_return_value = rop_response_return_value(response);
    if !is_outlook_folder_table_debug_target(*folder_id) && response_return_value != 0x8004_010F {
        return;
    }

    let selected_columns = effective_contents_table_columns(*folder_id, *associated, columns);
    let restriction_property_tags = restriction_property_tags_from_request(request);
    let find_row_failure_candidate_summary = if response_return_value == 0x8004_010F && !*associated
    {
        format_normal_message_find_row_failure_candidates(
            *folder_id,
            *position,
            request.find_backward(),
            request,
            restriction.as_ref(),
            sort_orders,
            &selected_columns,
            &restriction_property_tags,
            mailboxes,
            emails,
        )
    } else {
        String::new()
    };
    let total_row_count = if *associated {
        associated_folder_message_count(*folder_id, snapshot)
    } else {
        folder_message_count(*folder_id, mailboxes, emails, snapshot)
    };
    let found_row_value_summary = if response.get(7).copied().unwrap_or(0) == 1 {
        format_outlook_query_row_values(
            principal.account_id,
            *folder_id,
            *associated,
            *position,
            true,
            1,
            sort_orders,
            restriction.as_ref(),
            &selected_columns,
            snapshot,
        )
    } else {
        String::new()
    };
    let found_wire_row_summary = if response.get(7).copied().unwrap_or(0) == 1 {
        format_inbox_associated_wire_row_summary(
            principal.account_id,
            *folder_id,
            *associated,
            *position,
            true,
            1,
            sort_orders,
            restriction.as_ref(),
            &selected_columns,
            snapshot,
        )
    } else {
        String::new()
    };
    let normal_message_find_row_summary = if !*associated {
        format_normal_message_query_row_summary(
            *folder_id,
            *associated,
            *position,
            true,
            total_row_count.min(5) as usize,
            sort_orders,
            restriction.as_ref(),
            &selected_columns,
            mailboxes,
            emails,
            snapshot,
        )
    } else {
        String::new()
    };
    let response_found = response.get(7).copied().unwrap_or(0);
    let view_handoff_table_contract = format_outlook_view_handoff_table_contract(
        *folder_id,
        *associated,
        &selected_columns,
        snapshot,
    );
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = "Execute",
        mapi_request_id = request_id,
        request_rop_id = "0x4f",
        request_input_handle_index = request.input_handle_index().unwrap_or(0),
        folder_id = %format!("0x{folder_id:016x}"),
        folder_role = debug_role_for_folder_id(*folder_id),
        associated,
        find_flags = %format!("0x{:02x}", request.payload.first().copied().unwrap_or(0)),
        find_origin = request.find_origin().unwrap_or(0),
        find_backward = request.find_backward(),
        restriction_bytes = request_restriction_bytes(request).len(),
        restriction_preview = %hex_preview(request_restriction_bytes(request), 96),
        restriction_decoded = %format_debug_restriction(request_restriction_bytes(request)),
        restriction_property_tags = %format_debug_property_tags(&restriction_property_tags),
        response_return_value = %format!("0x{response_return_value:08x}"),
        response_found,
        current_position = *position,
        table_total_row_count = total_row_count,
        table_has_restriction = restriction.is_some(),
        table_sort_order_count = sort_orders.len(),
        table_sort_orders = %format_debug_sort_orders(sort_orders),
        selected_column_source = if columns.is_empty() { "default" } else { "setcolumns" },
        selected_property_tag_count = selected_columns.len(),
        selected_property_tags = %format_debug_property_tags(&selected_columns),
        selected_named_property_context,
        inbox_associated_config_summary =
            %format_inbox_associated_config_summary(*folder_id, *associated, snapshot),
        ipm_configuration_contract_summary =
            %format_ipm_configuration_contract_summary(
                *folder_id,
                *associated,
                &selected_columns,
                sort_orders,
                snapshot
        ),
        find_row_value_summary = %found_row_value_summary,
        find_row_wire_summary = %found_wire_row_summary,
        normal_message_find_row_summary = %normal_message_find_row_summary,
        find_row_failure_candidate_summary = %find_row_failure_candidate_summary,
        view_handoff_table_contract = %view_handoff_table_contract,
        response_row_wire_preview = %if response_found == 1 {
            hex_preview(response.get(8..).unwrap_or_default(), 160)
        } else {
            String::new()
        },
        "rca debug outlook contents table find row"
    );
    warn_outlook_view_handoff_table_invariants(
        principal,
        "0x4f",
        *folder_id,
        *associated,
        &view_handoff_table_contract,
    );
    if response_return_value == 0
        && response_found == 1
        && found_row_value_summary.is_empty()
        && normal_message_find_row_summary.is_empty()
    {
        tracing::warn!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            account_id = %principal.account_id,
            mailbox = %principal.email,
            request_type = "Execute",
            request_rop_id = "0x4f",
            folder_id = %format!("0x{folder_id:016x}"),
            folder_role = debug_role_for_folder_id(*folder_id),
            associated,
            response_return_value = "0x00000000",
            response_found,
            selected_property_tags = %format_debug_property_tags(&selected_columns),
            message = "rca debug outlook contents table find row invariant warning: found row has no decoded row identity",
        );
    }
}

fn rop_response_return_value(response: &[u8]) -> u32 {
    response
        .get(2..6)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_le_bytes)
        .unwrap_or(0)
}

pub(in crate::mapi::dispatch) fn log_outlook_contents_table_open(
    principal: &AccountPrincipal,
    request_id: &str,
    request: &RopRequest,
    folder_id: u64,
    table_flags: u8,
    associated: bool,
    row_count: u32,
    output_handle: u32,
    snapshot: &MapiMailStoreSnapshot,
) {
    if !is_outlook_folder_table_debug_target(folder_id) {
        return;
    }

    let selected_columns = Vec::new();
    let view_handoff_table_contract = format_outlook_view_handoff_table_contract(
        folder_id,
        associated,
        &selected_columns,
        snapshot,
    );
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = "Execute",
        mapi_request_id = %request_id,
        request_rop_id = "0x05",
        request_input_handle_index = request.input_handle_index().unwrap_or(0),
        request_output_handle_index = request.output_handle_index.unwrap_or(0),
        output_handle,
        folder_id = %format!("0x{folder_id:016x}"),
        folder_role = debug_role_for_folder_id(folder_id),
        table_flags = %format!("0x{table_flags:02x}"),
        associated,
        row_count,
        selected_column_source = "none_before_setcolumns",
        selected_property_tag_count = 0,
        selected_property_tags = "",
        view_handoff_table_contract = %view_handoff_table_contract,
        "rca debug outlook contents table opened"
    );
    warn_outlook_view_handoff_table_invariants(
        principal,
        "0x05",
        folder_id,
        associated,
        &view_handoff_table_contract,
    );
}

pub(in crate::mapi::dispatch) fn log_outlook_contents_table_set_columns(
    principal: &AccountPrincipal,
    request_id: &str,
    request: &RopRequest,
    folder_id: u64,
    associated: bool,
    columns: &[u32],
    named_property_context: &str,
    snapshot: &MapiMailStoreSnapshot,
) {
    if !is_outlook_folder_table_debug_target(folder_id) {
        return;
    }

    let view_handoff_table_contract =
        format_outlook_view_handoff_table_contract(folder_id, associated, columns, snapshot);
    let inbox_view_descriptor_behavior_contract =
        format_inbox_view_descriptor_set_columns_behavior_contract(
            folder_id, associated, columns, snapshot,
        );
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = "Execute",
        mapi_request_id = %request_id,
        request_rop_id = "0x12",
        request_input_handle_index = request.input_handle_index().unwrap_or(0),
        folder_id = %format!("0x{folder_id:016x}"),
        folder_role = debug_role_for_folder_id(folder_id),
        associated,
        set_columns_flags = %format!("0x{:02x}", request.payload.first().copied().unwrap_or(0)),
        requested_property_tag_count = columns.len(),
        requested_property_tags = %format_debug_property_tags(columns),
        selected_named_property_context = %named_property_context,
        ipm_configuration_column_contract =
            %format_ipm_configuration_set_columns_contract(folder_id, associated, columns),
        view_handoff_table_contract = %view_handoff_table_contract,
        inbox_view_descriptor_behavior_contract = %inbox_view_descriptor_behavior_contract,
        "rca debug outlook contents table columns selected"
    );
    warn_outlook_view_handoff_table_invariants(
        principal,
        "0x12",
        folder_id,
        associated,
        &view_handoff_table_contract,
    );
}

pub(in crate::mapi::dispatch) fn log_outlook_contents_table_sort(
    principal: &AccountPrincipal,
    request: &RopRequest,
    object: Option<&MapiObject>,
    selected_named_property_context: &str,
    snapshot: &MapiMailStoreSnapshot,
) {
    let Some(MapiObject::ContentsTable {
        folder_id,
        associated,
        columns,
        position,
        sort_orders,
        ..
    }) = object
    else {
        return;
    };
    if !is_outlook_folder_table_debug_target(*folder_id) {
        return;
    }

    let selected_columns = effective_contents_table_columns(*folder_id, *associated, columns);
    let view_handoff_table_contract = format_outlook_view_handoff_table_contract(
        *folder_id,
        *associated,
        &selected_columns,
        snapshot,
    );
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = "0x13",
        request_input_handle_index = request.input_handle_index().unwrap_or(0),
        folder_id = %format!("0x{folder_id:016x}"),
        folder_role = debug_role_for_folder_id(*folder_id),
        associated,
        sort_flags = %format!("0x{:02x}", request.payload.first().copied().unwrap_or(0)),
        requested_sort_order_count = request.sort_orders().len(),
        requested_sort_orders = %format_debug_sort_orders(&request.sort_orders()),
        sort_category_count = request.sort_category_count(),
        sort_expanded_count = request.sort_expanded_count(),
        stored_sort_order_count = sort_orders.len(),
        stored_sort_orders = %format_debug_sort_orders(sort_orders),
        current_position = *position,
        selected_column_source = if columns.is_empty() { "default" } else { "setcolumns" },
        selected_property_tag_count = selected_columns.len(),
        selected_property_tags = %format_debug_property_tags(&selected_columns),
        selected_named_property_context,
        inbox_associated_config_summary =
            %format_inbox_associated_config_summary(*folder_id, *associated, snapshot),
        ipm_configuration_contract_summary =
            %format_ipm_configuration_contract_summary(
                *folder_id,
                *associated,
                &selected_columns,
                sort_orders,
                snapshot
            ),
        view_handoff_table_contract = %view_handoff_table_contract,
        "rca debug outlook contents table sorted"
    );
    warn_outlook_view_handoff_table_invariants(
        principal,
        "0x13",
        *folder_id,
        *associated,
        &view_handoff_table_contract,
    );
}

pub(in crate::mapi::dispatch) fn log_outlook_contents_table_restrict(
    principal: &AccountPrincipal,
    request: &RopRequest,
    object: Option<&MapiObject>,
    selected_named_property_context: &str,
    snapshot: &MapiMailStoreSnapshot,
) {
    let Some(MapiObject::ContentsTable {
        folder_id,
        associated,
        columns,
        position,
        restriction,
        ..
    }) = object
    else {
        return;
    };
    if !is_outlook_folder_table_debug_target(*folder_id) {
        return;
    }

    let selected_columns = effective_contents_table_columns(*folder_id, *associated, columns);
    let view_handoff_table_contract = format_outlook_view_handoff_table_contract(
        *folder_id,
        *associated,
        &selected_columns,
        snapshot,
    );
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = "0x14",
        request_input_handle_index = request.input_handle_index().unwrap_or(0),
        folder_id = %format!("0x{folder_id:016x}"),
        folder_role = debug_role_for_folder_id(*folder_id),
        associated,
        restrict_flags = %format!("0x{:02x}", request.payload.first().copied().unwrap_or(0)),
        restriction_bytes = request_restriction_bytes(request).len(),
        restriction_preview = %hex_preview(request_restriction_bytes(request), 96),
        restriction_decoded = %format_debug_restriction(request_restriction_bytes(request)),
        parsed_restriction_present = restriction.is_some(),
        current_position = *position,
        selected_column_source = if columns.is_empty() { "default" } else { "setcolumns" },
        selected_property_tag_count = selected_columns.len(),
        selected_property_tags = %format_debug_property_tags(&selected_columns),
        selected_named_property_context,
        inbox_associated_config_summary =
            %format_inbox_associated_config_summary(*folder_id, *associated, snapshot),
        ipm_configuration_contract_summary =
            %format_ipm_configuration_contract_summary(
                *folder_id,
                *associated,
                &selected_columns,
                &[],
                snapshot
            ),
        view_handoff_table_contract = %view_handoff_table_contract,
        "rca debug outlook contents table restricted"
    );
    warn_outlook_view_handoff_table_invariants(
        principal,
        "0x14",
        *folder_id,
        *associated,
        &view_handoff_table_contract,
    );
}

pub(in crate::mapi::dispatch) fn log_outlook_contents_table_query_rows(
    principal: &AccountPrincipal,
    request_id: &str,
    request: &RopRequest,
    object: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    selected_named_property_context: &str,
    snapshot: &MapiMailStoreSnapshot,
) {
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
        return;
    };
    if !is_outlook_folder_table_debug_target(*folder_id) {
        return;
    }

    let selected_columns = effective_contents_table_columns(*folder_id, *associated, columns);
    let total_row_count = if *associated
        && *folder_id == COMMON_VIEWS_FOLDER_ID
        && is_unrestricted_common_views_navigation_projection(&selected_columns, restriction)
    {
        snapshot
            .common_views_table_messages()
            .filter(|message| {
                matches!(
                    message,
                    crate::mapi_store::MapiCommonViewsMessage::NavigationShortcut(_)
                )
            })
            .count()
            .min(u32::MAX as usize) as u32
    } else if *associated {
        restricted_associated_folder_message_count(
            *folder_id,
            snapshot,
            restriction.as_ref(),
            principal.account_id,
        )
        .min(u32::MAX as usize) as u32
    } else {
        folder_message_count(*folder_id, mailboxes, emails, snapshot)
    };
    let requested_row_count = request.query_row_count().unwrap_or(0);
    let query_row_window_summary = format_outlook_query_row_window(
        *folder_id,
        *associated,
        *position,
        request.query_forward_read(),
        requested_row_count,
        sort_orders,
        restriction.as_ref(),
        &selected_columns,
        principal.account_id,
        snapshot,
    );
    let query_row_value_summary = format_outlook_query_row_values(
        principal.account_id,
        *folder_id,
        *associated,
        *position,
        request.query_forward_read(),
        requested_row_count,
        sort_orders,
        restriction.as_ref(),
        &selected_columns,
        snapshot,
    );
    let normal_message_query_row_summary = format_normal_message_query_row_summary(
        *folder_id,
        *associated,
        *position,
        request.query_forward_read(),
        requested_row_count,
        sort_orders,
        restriction.as_ref(),
        &selected_columns,
        mailboxes,
        emails,
        snapshot,
    );
    let inbox_associated_wire_row_summary = format_inbox_associated_wire_row_summary(
        principal.account_id,
        *folder_id,
        *associated,
        *position,
        request.query_forward_read(),
        requested_row_count,
        sort_orders,
        restriction.as_ref(),
        &selected_columns,
        snapshot,
    );
    let view_handoff_table_contract = format_outlook_view_handoff_table_contract(
        *folder_id,
        *associated,
        &selected_columns,
        snapshot,
    );
    let inbox_view_descriptor_behavior_contract = format_inbox_view_descriptor_behavior_contract(
        *folder_id,
        *associated,
        *position,
        request.query_forward_read(),
        requested_row_count,
        sort_orders,
        restriction.as_ref(),
        &selected_columns,
        mailboxes,
        emails,
        snapshot,
    );

    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = "Execute",
        mapi_request_id = %request_id,
        request_rop_id = "0x15",
        request_input_handle_index = request.input_handle_index().unwrap_or(0),
        folder_id = %format!("0x{folder_id:016x}"),
        folder_role = debug_role_for_folder_id(*folder_id),
        associated,
        requested_forward_read = request.query_forward_read(),
        requested_row_count = requested_row_count,
        current_position = *position,
        table_total_row_count = total_row_count,
        table_has_restriction = restriction.is_some(),
        table_restriction_decoded = %format_debug_restriction_option(restriction.as_ref()),
        table_restriction_property_tags =
            %format_debug_restriction_property_tags(restriction.as_ref()),
        table_sort_order_count = sort_orders.len(),
        table_sort_orders = %format_debug_sort_orders(sort_orders),
        selected_column_source = if columns.is_empty() { "default" } else { "setcolumns" },
        selected_property_tag_count = selected_columns.len(),
        selected_property_tags = %format_debug_property_tags(&selected_columns),
        selected_named_property_context,
        inbox_associated_config_summary =
            %format_inbox_associated_config_summary(*folder_id, *associated, snapshot),
        ipm_configuration_contract_summary =
            %format_ipm_configuration_contract_summary(
                *folder_id,
                *associated,
                &selected_columns,
                sort_orders,
                snapshot
        ),
        query_row_window_summary = %query_row_window_summary,
        query_row_value_summary = %query_row_value_summary,
        normal_message_query_row_summary = %normal_message_query_row_summary,
        inbox_associated_wire_row_summary = %inbox_associated_wire_row_summary,
        common_views_wlink_target_decoding = %if *folder_id == COMMON_VIEWS_FOLDER_ID && *associated {
            format_common_views_wlink_target_decoding(principal.account_id, snapshot)
        } else {
            String::new()
        },
        common_views_wlink_contract_summary = %if *folder_id == COMMON_VIEWS_FOLDER_ID && *associated {
            format_common_views_wlink_contract_summary(&selected_columns, snapshot)
        } else {
            String::new()
        },
        view_handoff_table_contract = %view_handoff_table_contract,
        inbox_view_descriptor_behavior_contract = %inbox_view_descriptor_behavior_contract,
        "rca debug outlook contents table query rows"
    );
    warn_outlook_view_handoff_table_invariants(
        principal,
        "0x15",
        *folder_id,
        *associated,
        &view_handoff_table_contract,
    );
}

pub(in crate::mapi::dispatch) fn log_outlook_contents_table_query_rows_response(
    principal: &AccountPrincipal,
    request_id: &str,
    request: &RopRequest,
    object: Option<&MapiObject>,
    response: &[u8],
    snapshot: &MapiMailStoreSnapshot,
    selected_named_property_context: &str,
    queried_position: usize,
) {
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
        return;
    };
    if !is_outlook_folder_table_debug_target(*folder_id) {
        return;
    }

    let response_origin = response.get(6).copied().unwrap_or(0xff);
    let selected_columns = effective_contents_table_columns(*folder_id, *associated, columns);
    let row_count = response
        .get(7..9)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u16::from_le_bytes)
        .unwrap_or(0);
    let response_origin_position = if request.query_forward_read() {
        queried_position
    } else {
        queried_position.saturating_sub(row_count as usize)
    };
    let associated_wire_row_summary = if *associated {
        format_inbox_associated_wire_row_summary(
            principal.account_id,
            *folder_id,
            *associated,
            queried_position,
            request.query_forward_read(),
            row_count as usize,
            sort_orders,
            restriction.as_ref(),
            &selected_columns,
            snapshot,
        )
    } else {
        String::new()
    };
    let response_row_payload_preview = response
        .get(9..)
        .map(|bytes| hex_preview(bytes, 160))
        .unwrap_or_default();
    let view_handoff_table_contract = format_outlook_view_handoff_table_contract(
        *folder_id,
        *associated,
        &selected_columns,
        snapshot,
    );
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        account_id = %principal.account_id,
        mailbox = %principal.email,
        mapi_request_id = %request_id,
        request_type = "Execute",
        request_rop_id = "0x15",
        request_input_handle_index = request.input_handle_index().unwrap_or(0),
        folder_id = %format!("0x{folder_id:016x}"),
        folder_role = debug_role_for_folder_id(*folder_id),
        associated,
        requested_forward_read = request.query_forward_read(),
        requested_row_count = request.query_row_count().unwrap_or(0),
        queried_position,
        response_origin_position,
        current_position_after = *position,
        response_origin = %format!("0x{response_origin:02x}"),
        response_origin_name = match response_origin {
            0x00 => "BOOKMARK_BEGINNING",
            0x01 => "BOOKMARK_CURRENT",
            0x02 => "BOOKMARK_END",
            _ => "unknown",
        },
        response_row_count = row_count,
        selected_column_source = if columns.is_empty() { "default" } else { "setcolumns" },
        selected_property_tag_count = selected_columns.len(),
        selected_property_tags = %format_debug_property_tags(&selected_columns),
        selected_named_property_context,
        response_payload_bytes = response.len(),
        response_row_payload_preview = %response_row_payload_preview,
        associated_wire_row_summary = %associated_wire_row_summary,
        view_handoff_table_contract = %view_handoff_table_contract,
        "rca debug outlook contents table query rows response"
    );
    warn_outlook_view_handoff_table_invariants(
        principal,
        "0x15.response",
        *folder_id,
        *associated,
        &view_handoff_table_contract,
    );
}

pub(in crate::mapi::dispatch) fn log_outlook_contents_table_seek_row(
    principal: &AccountPrincipal,
    request: &RopRequest,
    object: Option<&MapiObject>,
    selected_named_property_context: &str,
    snapshot: &MapiMailStoreSnapshot,
    before_position: Option<usize>,
    response: &[u8],
) {
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
        return;
    };
    if !is_outlook_folder_table_debug_target(*folder_id) {
        return;
    }

    let selected_columns = effective_contents_table_columns(*folder_id, *associated, columns);
    let view_handoff_table_contract = format_outlook_view_handoff_table_contract(
        *folder_id,
        *associated,
        &selected_columns,
        snapshot,
    );
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = "0x18",
        request_input_handle_index = request.input_handle_index().unwrap_or(0),
        folder_id = %format!("0x{folder_id:016x}"),
        folder_role = debug_role_for_folder_id(*folder_id),
        associated,
        seek_origin = request.seek_origin().unwrap_or(0),
        requested_row_count = request.seek_row_count().unwrap_or(0),
        want_row_moved_count = request.want_row_moved_count(),
        before_position = before_position.unwrap_or(*position),
        current_position = *position,
        response_sought_less = response.get(6).copied().unwrap_or(0),
        response_rows_sought = response
            .get(7..11)
            .and_then(|bytes| bytes.try_into().ok())
            .map(i32::from_le_bytes)
            .unwrap_or(0),
        table_has_restriction = restriction.is_some(),
        table_sort_order_count = sort_orders.len(),
        table_sort_orders = %format_debug_sort_orders(sort_orders),
        selected_column_source = if columns.is_empty() { "default" } else { "setcolumns" },
        selected_property_tag_count = selected_columns.len(),
        selected_property_tags = %format_debug_property_tags(&selected_columns),
        selected_named_property_context,
        inbox_associated_config_summary =
            %format_inbox_associated_config_summary(*folder_id, *associated, snapshot),
        ipm_configuration_contract_summary =
            %format_ipm_configuration_contract_summary(
                *folder_id,
                *associated,
                &selected_columns,
                sort_orders,
                snapshot
            ),
        view_handoff_table_contract = %view_handoff_table_contract,
        "rca debug outlook contents table seek row"
    );
    warn_outlook_view_handoff_table_invariants(
        principal,
        "0x18",
        *folder_id,
        *associated,
        &view_handoff_table_contract,
    );
}

pub(in crate::mapi::dispatch) fn log_outlook_hierarchy_table_query_rows_response(
    principal: &AccountPrincipal,
    request_id: &str,
    request: &RopRequest,
    object: Option<&MapiObject>,
    response: &[u8],
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    queried_position: usize,
) {
    let Some(MapiObject::HierarchyTable {
        folder_id,
        columns,
        position,
        ..
    }) = object
    else {
        return;
    };
    let response_origin = response.get(6).copied().unwrap_or(0xff);
    let row_count = response
        .get(7..9)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u16::from_le_bytes)
        .unwrap_or(0);
    let response_origin_position = if request.query_forward_read() {
        queried_position
    } else {
        queried_position.saturating_sub(row_count as usize)
    };
    let selected_columns = if columns.is_empty() {
        default_hierarchy_columns()
    } else {
        columns.clone()
    };
    let table_total_row_count =
        table_position_and_count(object, mailboxes, emails, snapshot, principal.account_id).1;
    let response_row_payload_preview = response
        .get(9..)
        .map(|bytes| hex_preview(bytes, 160))
        .unwrap_or_default();
    let hierarchy_wire_row_summary =
        format_hierarchy_query_rows_wire_summary(response, &selected_columns, 32);
    if *folder_id == IPM_SUBTREE_FOLDER_ID {
        let metric_summary = hierarchy_response_metric_summary(response, &selected_columns);
        record_mapi_outlook_view_ipm_subtree_hierarchy_query(
            u64::from(row_count),
            table_total_row_count as u64,
            metric_summary.has_conversation_action,
            metric_summary.has_quick_step,
        );
    }

    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        account_id = %principal.account_id,
        mailbox = %principal.email,
        mapi_request_id = request_id,
        request_type = "Execute",
        request_rop_id = "0x15",
        request_input_handle_index = request.input_handle_index().unwrap_or(0),
        folder_id = %format!("0x{folder_id:016x}"),
        folder_role = debug_role_for_folder_id(*folder_id),
        requested_forward_read = request.query_forward_read(),
        requested_row_count = request.query_row_count().unwrap_or(0),
        queried_position,
        response_origin_position,
        current_position_after = *position,
        response_origin = %format!("0x{response_origin:02x}"),
        response_origin_name = match response_origin {
            0x00 => "BOOKMARK_BEGINNING",
            0x01 => "BOOKMARK_CURRENT",
            0x02 => "BOOKMARK_END",
            _ => "unknown",
        },
        response_row_count = row_count,
        table_total_row_count,
        selected_column_source = if columns.is_empty() { "default" } else { "setcolumns" },
        selected_property_tag_count = selected_columns.len(),
        selected_property_tags = %format_debug_property_tags(&selected_columns),
        response_payload_bytes = response.len(),
        response_row_payload_preview = %response_row_payload_preview,
        hierarchy_wire_row_summary = %hierarchy_wire_row_summary,
        "rca debug outlook hierarchy table query rows response"
    );
}

pub(in crate::mapi::dispatch) fn log_mapi_query_position_debug(
    principal: &AccountPrincipal,
    request_id: &str,
    request: &RopRequest,
    object: Option<&MapiObject>,
    response: &[u8],
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) {
    if !object.is_some_and(is_table_object) {
        return;
    }
    let position = response
        .get(6..10)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_le_bytes)
        .unwrap_or(0);
    let row_count = response
        .get(10..14)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_le_bytes)
        .unwrap_or(0);
    let (
        associated,
        selected_columns,
        sort_order_count,
        restriction_present,
        restriction_decoded,
        restriction_property_tags,
        normal_message_query_position_summary,
        calendar_event_query_position_summary,
        calendar_view_descriptor_columns,
        calendar_view_descriptor_row_projection,
        inbox_view_descriptor_behavior_contract,
    ) = match object {
        Some(MapiObject::ContentsTable {
            folder_id,
            associated,
            columns,
            position: table_position,
            sort_orders,
            restriction,
            ..
        }) => {
            let effective_columns =
                effective_contents_table_columns(*folder_id, *associated, columns);
            let calendar_view_descriptor_columns =
                outlook_view_descriptor_visible_property_tags(*folder_id, snapshot);
            let calendar_view_descriptor_row_projection =
                format_calendar_event_query_position_summary(
                    *folder_id,
                    *associated,
                    *table_position,
                    row_count.min(5) as usize,
                    sort_orders,
                    restriction.as_ref(),
                    &calendar_view_descriptor_columns,
                    snapshot,
                );
            (
                Some(*associated),
                format_debug_property_tags(&effective_columns),
                sort_orders.len(),
                restriction.is_some(),
                format_debug_restriction_option(restriction.as_ref()),
                format_debug_restriction_property_tags(restriction.as_ref()),
                format_normal_message_query_row_summary(
                    *folder_id,
                    *associated,
                    *table_position,
                    true,
                    row_count.min(5) as usize,
                    sort_orders,
                    restriction.as_ref(),
                    &effective_columns,
                    mailboxes,
                    emails,
                    snapshot,
                ),
                format_calendar_event_query_position_summary(
                    *folder_id,
                    *associated,
                    *table_position,
                    row_count.min(5) as usize,
                    sort_orders,
                    restriction.as_ref(),
                    &effective_columns,
                    snapshot,
                ),
                format_debug_property_tags(&calendar_view_descriptor_columns),
                calendar_view_descriptor_row_projection,
                format_inbox_view_descriptor_behavior_contract(
                    *folder_id,
                    *associated,
                    *table_position,
                    true,
                    row_count.min(5) as usize,
                    sort_orders,
                    restriction.as_ref(),
                    &effective_columns,
                    mailboxes,
                    emails,
                    snapshot,
                ),
            )
        }
        _ => (
            None,
            String::new(),
            0,
            false,
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
        ),
    };
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = "Execute",
        mapi_request_id = %request_id,
        request_rop_id = "0x17",
        request_input_handle_index = request.input_handle_index().unwrap_or(0),
        object_kind = mapi_object_debug_kind(object),
        folder_id = %mapi_object_debug_folder_id(object),
        folder_role = object
            .and_then(MapiObject::folder_id)
            .map(debug_role_for_folder_id)
            .unwrap_or("none"),
        associated = associated
            .map(|value| value.to_string())
            .unwrap_or_else(|| "n/a".to_string()),
        position,
        row_count,
        selected_property_tags = %selected_columns,
        sort_order_count,
        restriction_present,
        restriction_decoded = %restriction_decoded,
        restriction_property_tags = %restriction_property_tags,
        normal_message_query_position_summary = %normal_message_query_position_summary,
        calendar_event_query_position_summary = %calendar_event_query_position_summary,
        calendar_view_descriptor_columns = %calendar_view_descriptor_columns,
        calendar_view_descriptor_row_projection = %calendar_view_descriptor_row_projection,
        inbox_view_descriptor_behavior_contract = %inbox_view_descriptor_behavior_contract,
        inbox_associated_config_summary = object
            .and_then(MapiObject::folder_id)
            .map(|folder_id| {
                format_inbox_associated_config_summary(
                    folder_id,
                    associated.unwrap_or(false),
                    snapshot,
                )
            })
            .unwrap_or_default(),
        "rca debug mapi query position"
    );
}
