use super::*;

pub(super) enum TableControlFlow {
    Continue,
    StopBatch,
}

fn is_outlook_default_view_setcolumns_diagnostic_target(folder_id: u64) -> bool {
    matches!(
        folder_id,
        CALENDAR_FOLDER_ID
            | CONTACTS_FOLDER_ID
            | NOTES_FOLDER_ID
            | TASKS_FOLDER_ID
            | JOURNAL_FOLDER_ID
    )
}

pub(super) fn is_status_or_bookmark_rop(rop_id: RopId) -> bool {
    matches!(
        rop_id,
        RopId::GetStoreState
            | RopId::Abort
            | RopId::Progress
            | RopId::ResetTable
            | RopId::FreeBookmark
    )
}

pub(super) fn append_status_or_bookmark_dispatch_response(
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    responses: &mut Vec<u8>,
) {
    match RopId::from_u8(request.rop_id) {
        Some(RopId::GetStoreState) => {
            append_store_state_response(handle_slots, request, responses);
        }
        Some(RopId::Abort | RopId::Progress | RopId::ResetTable) => {
            append_execute_status_response(session, handle_slots, request, responses);
        }
        Some(RopId::FreeBookmark) => {
            append_free_bookmark_response(session, handle_slots, request, responses);
        }
        _ => {}
    }
}

pub(super) fn is_table_control_rop(
    rop_id: RopId,
    session: &MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
) -> bool {
    match rop_id {
        RopId::SetColumns
        | RopId::SortTable
        | RopId::Restrict
        | RopId::QueryRows
        | RopId::GetStatus
        | RopId::QueryPosition
        | RopId::SeekRow
        | RopId::SeekRowBookmark
        | RopId::SeekRowFractional
        | RopId::CreateBookmark
        | RopId::QueryColumnsAll
        | RopId::CollapseRow
        | RopId::GetCollapseState
        | RopId::SetCollapseState
        | RopId::FindRow => true,
        RopId::ExpandRow => !matches!(
            input_object(session, handle_slots, request),
            Some(MapiObject::Folder { .. })
        ),
        _ => false,
    }
}

pub(super) fn append_table_control_dispatch_response(
    principal: &AccountPrincipal,
    request_id: &str,
    request_rop_names: &str,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) -> TableControlFlow {
    match RopId::from_u8(request.rop_id) {
        Some(RopId::SetColumns) => append_set_columns_response(
            principal,
            session,
            handle_slots,
            request,
            request_id,
            mailboxes,
            emails,
            snapshot,
            responses,
        ),
        Some(RopId::SortTable) => {
            append_sort_table_response(
                principal,
                session,
                handle_slots,
                request,
                request_id,
                snapshot,
                responses,
            );
            TableControlFlow::Continue
        }
        Some(RopId::Restrict) => append_restrict_table_control_response(
            principal,
            session,
            handle_slots,
            request,
            request_id,
            snapshot,
            responses,
        ),
        Some(RopId::QueryRows) => {
            append_query_rows_response(
                principal,
                session,
                handle_slots,
                request,
                request_id,
                request_rop_names,
                mailboxes,
                emails,
                snapshot,
                responses,
            );
            TableControlFlow::Continue
        }
        Some(
            RopId::GetStatus
            | RopId::QueryPosition
            | RopId::SeekRow
            | RopId::SeekRowBookmark
            | RopId::SeekRowFractional
            | RopId::CreateBookmark
            | RopId::QueryColumnsAll
            | RopId::CollapseRow
            | RopId::GetCollapseState
            | RopId::SetCollapseState
            | RopId::ExpandRow,
        ) => {
            append_table_control_response(
                principal,
                request_id,
                request_rop_names,
                session,
                handle_slots,
                request,
                mailboxes,
                emails,
                snapshot,
                responses,
            );
            TableControlFlow::Continue
        }
        Some(RopId::FindRow) => {
            append_find_row_response(
                principal,
                session,
                handle_slots,
                request,
                request_id,
                mailboxes,
                emails,
                snapshot,
                responses,
            );
            TableControlFlow::Continue
        }
        _ => TableControlFlow::Continue,
    }
}

pub(super) fn append_set_columns_response(
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    request_id: &str,
    mailboxes: &[lpe_storage::JmapMailbox],
    emails: &[lpe_storage::JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) -> TableControlFlow {
    let requested_columns = request.property_tags();
    let normalized_columns =
        normalize_table_property_tags_for_session(session, requested_columns.clone());
    let input_handle_value = input_handle(handle_slots, request);
    let normalized_named_property_context = (requested_columns != normalized_columns)
        .then(|| format_debug_named_property_context(session, &requested_columns))
        .unwrap_or_default();
    let selected_named_property_context =
        format_debug_named_property_context(session, &normalized_columns);
    let outlook_view_descriptor_named_property_context =
        match input_object(session, handle_slots, request) {
            Some(MapiObject::ContentsTable {
                folder_id,
                associated,
                ..
            }) if !*associated => {
                format_outlook_view_descriptor_named_property_context(session, *folder_id, snapshot)
            }
            _ => String::new(),
        };
    let mut inbox_normal_setcolumns_context = None;
    let mut outlook_default_view_setcolumns_context = None;
    let flow = match input_object_mut(session, handle_slots, request) {
        Some(MapiObject::HierarchyTable {
            folder_id,
            columns,
            columns_set,
            ..
        }) => {
            if !set_columns_request_is_valid(request) {
                columns.clear();
                *columns_set = false;
                responses.extend_from_slice(&rop_error_response(
                    0x12,
                    request.response_handle_index(),
                    0x8007_0057,
                ));
                return TableControlFlow::StopBatch;
            }
            let folder_id_value = *folder_id;
            *columns = normalized_columns.clone();
            *columns_set = true;
            let selected_columns = columns.clone();
            if folder_id_value == INBOX_FOLDER_ID {
                session.record_last_inbox_hierarchy_table_context(format!(
                    "set_columns_input_index={};set_columns={}",
                    request.input_handle_index().unwrap_or(0),
                    format_debug_property_tags(&selected_columns)
                ));
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    mailbox = %principal.email,
                    request_type = "Execute",
                    request_rop_id = "0x12",
                    folder_id = %format!("0x{folder_id_value:016x}"),
                    input_handle_index = request.input_handle_index().unwrap_or(0),
                    requested_columns = %format_debug_property_tags(&selected_columns),
                    message = "rca debug mapi inbox hierarchy set columns"
                );
            }
            responses.extend_from_slice(&set_columns_response(request));
            TableControlFlow::Continue
        }
        Some(MapiObject::AttachmentTable {
            columns,
            columns_set,
            ..
        }) => {
            if !set_columns_request_is_valid(request) {
                columns.clear();
                *columns_set = false;
                responses.extend_from_slice(&rop_error_response(
                    0x12,
                    request.response_handle_index(),
                    0x8007_0057,
                ));
                return TableControlFlow::StopBatch;
            }
            *columns = normalized_columns.clone();
            *columns_set = true;
            responses.extend_from_slice(&set_columns_response(request));
            TableControlFlow::Continue
        }
        Some(MapiObject::ContentsTable {
            folder_id,
            associated,
            columns,
            columns_set,
            restriction,
            sort_orders,
            ..
        }) => {
            if !set_columns_request_is_valid(request) {
                tracing::warn!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    mailbox = %principal.email,
                    request_type = "Execute",
                    request_rop_id = "0x12",
                    folder_id = %format!("0x{folder_id:016x}"),
                    associated = *associated,
                    requested_columns = %format_debug_property_tags(&request.property_tags()),
                    unknown_wire_type_columns =
                        %format_unknown_wire_type_property_tags(&request.property_tags()),
                    response_error = "0x80070057",
                    message = "rca debug mapi contents table set columns rejected",
                );
                columns.clear();
                *columns_set = false;
                responses.extend_from_slice(&rop_error_response(
                    0x12,
                    request.response_handle_index(),
                    0x8007_0057,
                ));
                return TableControlFlow::StopBatch;
            }
            *columns = normalized_columns.clone();
            *columns_set = true;
            if !normalized_named_property_context.is_empty() {
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    mailbox = %principal.email,
                    request_type = "Execute",
                    request_rop_id = "0x12",
                    folder_id = %format!("0x{folder_id:016x}"),
                    associated = *associated,
                    requested_columns = %format_debug_property_tags(&requested_columns),
                    normalized_columns = %format_debug_property_tags(columns),
                    named_property_context = %normalized_named_property_context,
                    message = "rca debug mapi contents table set columns normalized named property aliases",
                );
            }
            log_outlook_contents_table_set_columns(
                principal,
                request_id,
                request,
                *folder_id,
                *associated,
                columns,
                &selected_named_property_context,
                snapshot,
            );
            if *folder_id == INBOX_FOLDER_ID && !*associated {
                let row_count = folder_message_count(*folder_id, mailboxes, emails, snapshot);
                let view_handoff_table_contract = format_outlook_view_handoff_table_contract(
                    *folder_id,
                    *associated,
                    columns,
                    snapshot,
                );
                let descriptor_behavior =
                    format_inbox_view_descriptor_set_columns_behavior_contract(
                        *folder_id,
                        *associated,
                        columns,
                        snapshot,
                    );
                inbox_normal_setcolumns_context = Some((
                    input_handle_value,
                    format!(
                        "handle={};input_index={};row_count={};columns={};column_support={};normal_message_defaulted_column_detail={};named_properties={};view_handoff={};table_compatibility={};descriptor_behavior={}",
                        format_optional_debug_handle(input_handle_value),
                        request.input_handle_index().unwrap_or(0),
                        row_count,
                        format_debug_property_tags(columns),
                        normal_message_table_column_support_summary(columns),
                        normal_message_defaulted_column_detail(columns),
                        selected_named_property_context,
                        view_handoff_table_contract,
                        format_default_view_table_compatibility_contract(
                            *folder_id,
                            *associated,
                            columns,
                            sort_orders,
                            restriction.as_ref(),
                            snapshot,
                        ),
                        descriptor_behavior
                    ),
                ));
            }
            if is_outlook_default_view_setcolumns_diagnostic_target(*folder_id) && !*associated {
                let row_count = folder_message_count(*folder_id, mailboxes, emails, snapshot);
                outlook_default_view_setcolumns_context = Some((
                    *folder_id,
                    format!(
                    "handle={};input_index={};folder=0x{:016x};role={};row_count={};columns={};named_properties={};view_descriptor_named_properties={};view_handoff={}",
                    format_optional_debug_handle(input_handle_value),
                    request.input_handle_index().unwrap_or(0),
                    *folder_id,
                    debug_role_for_folder_id(*folder_id),
                    row_count,
                    format_debug_property_tags(columns),
                    selected_named_property_context,
                    outlook_view_descriptor_named_property_context,
                    format_outlook_view_handoff_table_contract(
                        *folder_id,
                        *associated,
                        columns,
                        snapshot,
                    )
                )));
            }
            responses.extend_from_slice(&set_columns_response(request));
            TableControlFlow::Continue
        }
        Some(MapiObject::PermissionTable {
            columns,
            columns_set,
            ..
        }) => {
            if !set_columns_request_is_valid(request) {
                columns.clear();
                *columns_set = false;
                responses.extend_from_slice(&rop_error_response(
                    0x12,
                    request.response_handle_index(),
                    0x8007_0057,
                ));
                return TableControlFlow::StopBatch;
            }
            *columns = normalized_columns.clone();
            *columns_set = true;
            responses.extend_from_slice(&set_columns_response(request));
            TableControlFlow::Continue
        }
        Some(MapiObject::RuleTable {
            columns,
            columns_set,
            ..
        }) => {
            if !set_columns_request_is_valid_for_rule_table(request) {
                tracing::warn!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    mailbox = %principal.email,
                    request_type = "Execute",
                    request_rop_id = "0x12",
                    requested_columns = %format_debug_property_tags(&request.property_tags()),
                    unknown_wire_type_columns =
                        %format_unknown_wire_type_property_tags(&request.property_tags()),
                    response_error = "0x80070057",
                    message = "rca debug mapi rule table set columns rejected",
                );
                columns.clear();
                *columns_set = false;
                responses.extend_from_slice(&rop_error_response(
                    0x12,
                    request.response_handle_index(),
                    0x8007_0057,
                ));
                return TableControlFlow::StopBatch;
            }
            *columns = normalized_columns.clone();
            *columns_set = true;
            responses.extend_from_slice(&set_columns_response(request));
            TableControlFlow::Continue
        }
        _ => {
            responses.extend_from_slice(&rop_error_response(
                0x12,
                request.response_handle_index(),
                0x8004_0102,
            ));
            TableControlFlow::Continue
        }
    };
    if let Some((handle, context)) = inbox_normal_setcolumns_context {
        session.record_inbox_normal_contents_table_setcolumns(handle, context.clone());
        session
            .record_outlook_view_failure_trace_event(format!("visible_inbox_setcolumns:{context}"));
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            mailbox = %principal.email,
            request_type = "Execute",
            mapi_request_id = %request_id,
            request_rop_id = "0x12",
            input_handle_index = request.input_handle_index().unwrap_or(0),
            input_handle_value = %format_optional_debug_handle(handle),
            setcolumns_context = %context,
            "rca debug mapi visible inbox setcolumns tracked"
        );
    }
    if let Some((folder_id, context)) = outlook_default_view_setcolumns_context {
        session.record_outlook_view_failure_trace_event(format!(
            "outlook_default_view_setcolumns:{context}"
        ));
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            mailbox = %principal.email,
            request_type = "Execute",
            request_rop_id = "0x12",
            input_handle_index = request.input_handle_index().unwrap_or(0),
            folder_id = %format!("0x{folder_id:016x}"),
            folder_role = debug_role_for_folder_id(folder_id),
            setcolumns_context = %context,
            "rca debug mapi outlook default view setcolumns tracked"
        );
    }
    flow
}

pub(super) fn append_sort_table_response(
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    request_id: &str,
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) {
    let sort_trace = match input_object(session, handle_slots, request) {
        Some(MapiObject::ContentsTable {
            folder_id,
            associated,
            columns,
            ..
        }) if *folder_id == INBOX_FOLDER_ID => Some(format!(
            "inbox_sort_table:request_id={request_id};handle={};associated={associated};columns={};sort={}",
            format_optional_debug_handle(input_handle(handle_slots, request)),
            format_debug_property_tags(columns),
            format_debug_sort_orders(&request.sort_orders())
        )),
        Some(MapiObject::ContentsTable {
            folder_id,
            associated,
            columns,
            ..
        }) if *folder_id == CALENDAR_FOLDER_ID && *associated => Some(
            format_calendar_associated_sort_trace(
                request_id,
                format_optional_debug_handle(input_handle(handle_slots, request)),
                columns,
                &request.sort_orders(),
                snapshot,
            ),
        ),
        _ => None,
    };
    match input_object_mut(session, handle_slots, request) {
        Some(MapiObject::ContentsTable {
            sort_orders,
            category_count,
            expanded_count,
            collapsed_categories,
            position,
            bookmarks,
            ..
        }) => {
            if !sort_table_request_is_valid(request) {
                *sort_orders = invalid_table_sort_orders();
                *category_count = 0;
                *expanded_count = 0;
                collapsed_categories.clear();
                *position = 0;
                bookmarks.clear();
                responses.extend_from_slice(&rop_error_response(
                    0x13,
                    request.response_handle_index(),
                    0x8007_0057,
                ));
                if let Some(trace) = sort_trace {
                    session.record_outlook_view_failure_trace_event(trace);
                }
                return;
            }
            *sort_orders = request.sort_orders();
            *category_count = request.sort_category_count();
            *expanded_count = request.sort_expanded_count();
            collapsed_categories.clear();
            *position = 0;
            bookmarks.clear();
            let selected_named_property_context = format_contents_table_named_property_context(
                session,
                input_object(session, handle_slots, request),
            );
            log_outlook_contents_table_sort(
                principal,
                request,
                input_object(session, handle_slots, request),
                &selected_named_property_context,
                snapshot,
            );
            responses.extend_from_slice(&sort_table_response(request));
        }
        _ => responses.extend_from_slice(&rop_error_response(
            0x13,
            request.response_handle_index(),
            0x8004_0102,
        )),
    }
    if let Some(trace) = sort_trace {
        session.record_outlook_view_failure_trace_event(trace);
    }
}

pub(super) fn format_calendar_associated_sort_trace(
    request_id: &str,
    handle: String,
    columns: &[u32],
    sort_orders: &[MapiSortOrder],
    snapshot: &MapiMailStoreSnapshot,
) -> String {
    format!(
        "calendar_associated_sort_table:request_id={request_id};handle={handle};associated=true;row_count={};columns={};sort={};next_expected_client_step=query_rows_on_calendar_associated_contents_table",
        associated_folder_message_count(CALENDAR_FOLDER_ID, snapshot),
        format_debug_property_tags(columns),
        format_debug_sort_orders(sort_orders)
    )
}

pub(super) fn append_restrict_response(
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    request_id: &str,
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) -> TableControlFlow {
    if !table_async_flags_are_valid(request) {
        if let Some(
            MapiObject::HierarchyTable {
                restriction,
                position,
                bookmarks,
                ..
            }
            | MapiObject::ContentsTable {
                restriction,
                position,
                bookmarks,
                ..
            },
        ) = input_object_mut(session, handle_slots, request)
        {
            *restriction = Some(MapiRestriction::InvalidTableRestriction);
            *position = 0;
            bookmarks.clear();
        }
        responses.extend_from_slice(&rop_error_response(
            0x14,
            request.response_handle_index(),
            0x8007_0057,
        ));
        return TableControlFlow::Continue;
    }

    let restrict_trace = match input_object(session, handle_slots, request) {
        Some(MapiObject::ContentsTable {
            folder_id,
            associated,
            columns,
            ..
        }) if *folder_id == INBOX_FOLDER_ID => Some(format!(
            "inbox_restrict:request_id={request_id};handle={};associated={associated};columns={};restriction_tags={}",
            format_optional_debug_handle(input_handle(handle_slots, request)),
            format_debug_property_tags(columns),
            request
                .restriction()
                .ok()
                .and_then(|restriction| restriction)
                .map(|restriction| {
                    format_debug_restriction_property_tags(Some(&restriction))
                })
                .unwrap_or_default()
        )),
        _ => None,
    };

    let flow = match input_object_mut(session, handle_slots, request) {
        Some(MapiObject::HierarchyTable {
            restriction,
            position,
            bookmarks,
            ..
        })
        | Some(MapiObject::ContentsTable {
            restriction,
            position,
            bookmarks,
            ..
        }) => match request.restriction() {
            Ok(parsed) => {
                *restriction = parsed;
                *position = 0;
                bookmarks.clear();
                let selected_named_property_context = format_contents_table_named_property_context(
                    session,
                    input_object(session, handle_slots, request),
                );
                log_outlook_contents_table_restrict(
                    principal,
                    request,
                    input_object(session, handle_slots, request),
                    &selected_named_property_context,
                    snapshot,
                );
                responses.extend_from_slice(&restrict_response(request));
                TableControlFlow::Continue
            }
            Err(_) => {
                *restriction = Some(MapiRestriction::InvalidTableRestriction);
                *position = 0;
                bookmarks.clear();
                responses.extend_from_slice(&rop_error_response(
                    0x14,
                    request.response_handle_index(),
                    0x8004_0102,
                ));
                TableControlFlow::StopBatch
            }
        },
        Some(MapiObject::RuleTable { position, .. }) => {
            *position = 0;
            responses.extend_from_slice(&restrict_response(request));
            TableControlFlow::Continue
        }
        _ => {
            responses.extend_from_slice(&rop_error_response(
                0x14,
                request.response_handle_index(),
                0x8004_0102,
            ));
            TableControlFlow::Continue
        }
    };

    if let Some(trace) = restrict_trace {
        session.record_outlook_view_failure_trace_event(trace);
    }

    flow
}

pub(super) fn append_restrict_table_control_response(
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    request_id: &str,
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) -> TableControlFlow {
    if !input_object(session, handle_slots, request).is_some_and(restrict_supported_on_object) {
        responses.extend_from_slice(&rop_error_response(
            0x14,
            request.response_handle_index(),
            0x8004_0102,
        ));
        return TableControlFlow::Continue;
    }
    append_restrict_response(
        principal,
        session,
        handle_slots,
        request,
        request_id,
        snapshot,
        responses,
    )
}

pub(super) fn append_query_rows_response(
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    request_id: &str,
    request_rop_names: &str,
    mailboxes: &[lpe_storage::JmapMailbox],
    emails: &[lpe_storage::JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) {
    let input_handle_value = input_handle(handle_slots, request);
    let query_object = input_object(session, handle_slots, request);
    let inbox_normal_query_rows_context = match query_object {
        Some(MapiObject::ContentsTable {
            folder_id,
            associated,
            columns,
            position,
            restriction,
            sort_orders,
            ..
        }) if *folder_id == INBOX_FOLDER_ID && !*associated => Some((
            input_handle_value,
            format!(
                "handle={};input_index={};position={};requested_forward_read={};requested_row_count={};columns={};column_support={};sort={};restriction={}",
                format_optional_debug_handle(input_handle_value),
                request.input_handle_index().unwrap_or(0),
                position,
                request.query_forward_read(),
                request.query_row_count().unwrap_or(0),
                format_debug_property_tags(columns),
                normal_message_table_column_support_summary(columns),
                format_debug_sort_orders(sort_orders),
                format_debug_restriction_option(restriction.as_ref())
            ),
        )),
        _ => None,
    };
    let calendar_normal_query_rows_context = match query_object {
        Some(MapiObject::ContentsTable {
            folder_id,
            associated,
            columns,
            position,
            restriction,
            sort_orders,
            ..
        }) if *folder_id == CALENDAR_FOLDER_ID && !*associated => Some((
            input_handle_value,
            format!(
                "handle={};input_index={};position={};requested_forward_read={};requested_row_count={};columns={};sort={};restriction={}",
                format_optional_debug_handle(input_handle_value),
                request.input_handle_index().unwrap_or(0),
                position,
                request.query_forward_read(),
                request.query_row_count().unwrap_or(0),
                format_debug_property_tags(columns),
                format_debug_sort_orders(sort_orders),
                format_debug_restriction_option(restriction.as_ref())
            ),
        )),
        _ => None,
    };
    let default_view_normal_query_rows_context = match query_object {
        Some(MapiObject::ContentsTable {
            folder_id,
            associated,
            columns,
            position,
            restriction,
            sort_orders,
            ..
        }) if !*associated => {
            let (_, _, container_class) = debug_open_folder_metadata(*folder_id, mailboxes);
            default_view_supported_folder(*folder_id, &container_class).then(|| {
                (
                    input_handle_value,
                    format!(
                        "handle={};input_index={};folder=0x{folder_id:016x};role={};container_class={container_class};position={};requested_forward_read={};requested_row_count={};columns={};sort={};restriction={}",
                        format_optional_debug_handle(input_handle_value),
                        request.input_handle_index().unwrap_or(0),
                        debug_role_for_folder_id(*folder_id),
                        position,
                        request.query_forward_read(),
                        request.query_row_count().unwrap_or(0),
                        format_debug_property_tags(columns),
                        format_debug_sort_orders(sort_orders),
                        format_debug_restriction_option(restriction.as_ref())
                    ),
                )
            })
        }
        _ => None,
    };
    let bootstrap_query_phase = outlook_bootstrap_query_rows_phase(query_object);
    let bootstrap_row_invariants = outlook_bootstrap_row_invariant_summaries(
        query_object,
        mailboxes,
        emails,
        snapshot,
        principal.account_id,
        request.query_forward_read(),
        request.query_row_count().unwrap_or(0),
    );
    let bootstrap_total_row_count = outlook_bootstrap_query_rows_total_count(
        query_object,
        mailboxes,
        emails,
        snapshot,
        principal.account_id,
    );
    let selected_named_property_context =
        format_contents_table_named_property_context(session, query_object);
    log_calendar_hierarchy_query_rows_contract(principal, query_object, snapshot);
    log_outlook_contents_table_query_rows(
        principal,
        request_id,
        request,
        query_object,
        mailboxes,
        emails,
        &selected_named_property_context,
        snapshot,
    );
    let inbox_associated_query_context = format_inbox_associated_query_context(
        input_object(session, handle_slots, request),
        request,
        principal.account_id,
        snapshot,
    );
    let common_views_inbox_shortcut_context = format_common_views_inbox_shortcut_context(
        query_object,
        request,
        principal.account_id,
        snapshot,
    );
    let inbox_hierarchy_query_context =
        format_inbox_hierarchy_query_context(query_object, request, mailboxes, snapshot);
    if let Some(context) = inbox_associated_query_context {
        session.record_last_inbox_associated_query_context(context);
    }
    if let Some(context) = common_views_inbox_shortcut_context {
        session.record_last_common_views_inbox_shortcut_context(context);
    }
    if let Some(context) = inbox_hierarchy_query_context {
        session.record_last_inbox_hierarchy_query_context(context.clone());
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            mailbox = %principal.email,
            request_type = "Execute",
            request_rop_id = "0x15",
            folder_id = %format!("0x{INBOX_FOLDER_ID:016x}"),
            query_context = %context,
            message = "rca debug mapi inbox hierarchy query rows"
        );
    }
    let smart_input_variant_context = apply_outlook_smart_input_variant_before_query_rows(
        session,
        handle_slots,
        request,
        request_id,
        request_rop_names,
    );
    if let Some(context) = smart_input_variant_context {
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            mailbox = %principal.email,
            request_type = "Execute",
            mapi_request_id = %request_id,
            request_rop_id = "0x15",
            outlook_smart_input_variant = %session.outlook_smart_input_variant,
            outlook_smart_input_variant_scope = "session",
            outlook_smart_input_variant_applied = true,
            outlook_smart_input_variant_context = %context,
            message = "rca debug mapi outlook smart input variant applied"
        );
    }
    let queried_position = input_object(session, handle_slots, request)
        .and_then(table_position)
        .unwrap_or(0);
    let response = query_rows_response(
        request,
        input_object_mut(session, handle_slots, request),
        mailboxes,
        emails,
        snapshot,
        principal.account_id,
    );
    log_outlook_contents_table_query_rows_response(
        principal,
        request_id,
        request,
        input_object(session, handle_slots, request),
        &response,
        snapshot,
        &selected_named_property_context,
        queried_position,
    );
    log_outlook_hierarchy_table_query_rows_response(
        principal,
        request_id,
        request,
        input_object(session, handle_slots, request),
        &response,
        mailboxes,
        emails,
        snapshot,
        queried_position,
    );
    if let Some(MapiObject::HierarchyTable {
        folder_id,
        columns,
        position,
        ..
    }) = input_object(session, handle_slots, request)
    {
        let row_count = response
            .get(7..9)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .unwrap_or(0);
        let response_origin = response.get(6).copied().unwrap_or(0xff);
        let selected_columns = if columns.is_empty() {
            default_hierarchy_columns()
        } else {
            columns.clone()
        };
        session.record_outlook_view_failure_trace_event(format!(
            "hierarchy_query_rows:request_id={request_id};folder=0x{folder_id:016x};role={};input_index={};handle={};queried_position={queried_position};current_position_after={position};requested_forward_read={};requested_row_count={};response_origin=0x{response_origin:02x};response_row_count={row_count};columns={};after_view_handoff={}",
            debug_role_for_folder_id(*folder_id),
            request.input_handle_index().unwrap_or(0),
            format_optional_debug_handle(input_handle_value),
            request.query_forward_read(),
            request.query_row_count().unwrap_or(0),
            format_debug_property_tags(&selected_columns),
            session
                .post_hierarchy_actions
                .outlook_view_failure_trace_events
                .iter()
                .any(|event| event.starts_with("view_handoff:"))
        ));
    }
    let mut inbox_associated_query_rows_returned_non_empty = false;
    if let Some(MapiObject::ContentsTable {
        folder_id,
        associated,
        columns,
        position,
        restriction,
        sort_orders,
        ..
    }) = input_object(session, handle_slots, request)
    {
        let row_count = response
            .get(7..9)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .unwrap_or(0);
        let response_origin = response.get(6).copied().unwrap_or(0xff);
        let folder_id = *folder_id;
        let associated = *associated;
        let position = *position;
        let columns = columns.clone();
        let sort_orders = sort_orders.clone();
        let restriction = restriction.clone();
        if folder_id == INBOX_FOLDER_ID && associated && row_count > 0 {
            inbox_associated_query_rows_returned_non_empty = true;
        }
        let query_context = format!(
            "phase=query_rows;request_id={request_id};request_rops={request_rop_names};input_index={};handle={};folder=0x{folder_id:016x};role={};associated={associated};queried_position={queried_position};current_position_after={position};requested_forward_read={};requested_row_count={};response_row_count={row_count};columns={};sort={};restriction={}",
            request.input_handle_index().unwrap_or(0),
            format_optional_debug_handle(input_handle(handle_slots, request)),
            debug_role_for_folder_id(folder_id),
            request.query_forward_read(),
            request.query_row_count().unwrap_or(0),
            format_debug_property_tags(&columns),
            format_debug_sort_orders(&sort_orders),
            format_debug_restriction_option(restriction.as_ref())
        );
        session.record_last_table_query_rows_context(query_context.clone());
        if folder_id == INBOX_FOLDER_ID && associated {
            if row_count > 0 {
                session.record_inbox_associated_non_empty_query_context(query_context.clone());
            } else if response_origin == 0x02
                && session
                    .post_hierarchy_actions
                    .inbox_associated_query_rows_returned_non_empty
            {
                session.record_inbox_associated_query_rows_reached_end(query_context.clone());
                if !session.post_hierarchy_actions.post_inbox_fai_handoff_logged
                    && !session
                        .post_hierarchy_actions
                        .inbox_associated_config_open_observed
                    && !session
                        .post_hierarchy_actions
                        .inbox_normal_contents_table_observed
                {
                    let handoff_context =
                        format_inbox_post_fai_handoff_context(&session.post_hierarchy_actions);
                    let live_handle_summary = format_live_handle_debug_summary(session);
                    record_mapi_outlook_view_inbox_fai_handoff_without_contents();
                    record_mapi_outlook_view_bootstrap_stall(1);
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %principal.email,
                        request_type = "Execute",
                        mapi_request_id = %request_id,
                        request_rop_id = "0x15",
                        request_rop_names = %request_rop_names,
                        input_handle_index = request.input_handle_index().unwrap_or(0),
                        input_handle_value = %format_optional_debug_handle(input_handle(handle_slots, request)),
                        query_rows_end_context = %query_context,
                        handoff_context = %handoff_context,
                        live_handle_summaries = %live_handle_summary,
                        config_open_observed = session
                            .post_hierarchy_actions
                            .inbox_associated_config_open_observed,
                        config_stream_open_observed = session
                            .post_hierarchy_actions
                            .inbox_associated_config_stream_open_observed,
                        config_stream_read_observed = session
                            .post_hierarchy_actions
                            .inbox_associated_config_stream_read_observed,
                        normal_contents_table_observed = session
                            .post_hierarchy_actions
                            .inbox_normal_contents_table_observed,
                        normal_query_rows_observed = session
                            .post_hierarchy_actions
                            .inbox_normal_contents_table_query_rows_observed,
                        next_expected_client_step =
                            "open_inbox_associated_config_message_or_normal_contents_table",
                        "rca debug mapi inbox associated fai exhausted without handoff"
                    );
                    session.mark_post_inbox_fai_handoff_logged();
                }
            }
        }
        if folder_id == COMMON_VIEWS_FOLDER_ID
            && associated
            && row_count == 0
            && response_origin == 0x02
            && !session
                .post_hierarchy_actions
                .post_common_views_handoff_logged
            && !session
                .post_hierarchy_actions
                .inbox_associated_contents_table_observed
            && !session
                .post_hierarchy_actions
                .inbox_normal_contents_table_observed
        {
            let live_handle_summary = format_live_handle_debug_summary(session);
            record_mapi_outlook_view_common_views_handoff_without_contents();
            session.record_outlook_view_failure_trace_event(format!(
                "common_views_exhausted_without_inbox_contents:{query_context}"
            ));
            tracing::info!(
                rca_debug = true,
                adapter = "mapi",
                endpoint = "emsmdb",
                mailbox = %principal.email,
                request_type = "Execute",
                mapi_request_id = %request_id,
                request_rop_id = "0x15",
                request_rop_names = %request_rop_names,
                input_handle_index = request.input_handle_index().unwrap_or(0),
                input_handle_value = %format_optional_debug_handle(input_handle(handle_slots, request)),
                query_rows_end_context = %query_context,
                common_views_inbox_shortcut_context =
                    %session.post_hierarchy_actions.last_common_views_inbox_shortcut_context,
                live_handle_summaries = %live_handle_summary,
                receive_folder_verification_passed = session
                    .post_hierarchy_actions
                    .receive_folder_verification_passed,
                inbox_associated_contents_table_observed = session
                    .post_hierarchy_actions
                    .inbox_associated_contents_table_observed,
                normal_contents_table_observed = session
                    .post_hierarchy_actions
                    .inbox_normal_contents_table_observed,
                next_expected_client_step =
                    "open_inbox_associated_or_normal_contents_table",
                "rca debug mapi common views exhausted without inbox contents"
            );
            session.mark_post_common_views_handoff_logged();
        }
    }
    if inbox_associated_query_rows_returned_non_empty {
        session.record_inbox_associated_query_rows_returned_non_empty();
    }
    responses.extend_from_slice(&response);
    if let Some((phase, folder_id, associated)) = bootstrap_query_phase {
        log_outlook_bootstrap_phase(
            principal,
            phase,
            "0x15",
            Some(folder_id),
            associated,
            bootstrap_total_row_count,
            Some(bootstrap_row_invariants.len() as u32),
            None,
            "",
        );
        for summary in bootstrap_row_invariants {
            log_outlook_bootstrap_row_invariant(principal, phase, folder_id, associated, &summary);
        }
    }
    if let Some((handle, context)) = inbox_normal_query_rows_context {
        session.record_inbox_normal_contents_table_query_rows(handle, context.clone());
        session
            .record_outlook_view_failure_trace_event(format!("visible_inbox_query_rows:{context}"));
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            mailbox = %principal.email,
            request_type = "Execute",
            mapi_request_id = %request_id,
            request_rop_id = "0x15",
            input_handle_index = request.input_handle_index().unwrap_or(0),
            input_handle_value = %format_optional_debug_handle(handle),
            query_rows_context = %context,
            "rca debug mapi visible inbox query rows tracked"
        );
    }
    if let Some((handle, context)) = calendar_normal_query_rows_context {
        session.record_calendar_normal_contents_table_query_rows(handle, context.clone());
        session.record_outlook_view_failure_trace_event(format!(
            "calendar_normal_query_rows:{context}"
        ));
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            mailbox = %principal.email,
            request_type = "Execute",
            mapi_request_id = %request_id,
            request_rop_id = "0x15",
            input_handle_index = request.input_handle_index().unwrap_or(0),
            input_handle_value = %format_optional_debug_handle(handle),
            query_rows_context = %context,
            "rca debug mapi calendar query rows tracked"
        );
    }
    if let Some((handle, context)) = default_view_normal_query_rows_context {
        session.record_default_view_normal_contents_table_query_rows(handle, context.clone());
        session.record_outlook_view_failure_trace_event(format!(
            "default_view_normal_query_rows:{context}"
        ));
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            mailbox = %principal.email,
            request_type = "Execute",
            mapi_request_id = %request_id,
            request_rop_id = "0x15",
            input_handle_index = request.input_handle_index().unwrap_or(0),
            input_handle_value = %format_optional_debug_handle(handle),
            query_rows_context = %context,
            "rca debug mapi default view normal query rows tracked"
        );
    }
}

pub(super) fn append_find_row_response(
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    request_id: &str,
    mailboxes: &[lpe_storage::JmapMailbox],
    emails: &[lpe_storage::JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) {
    let find_trace = match input_object(session, handle_slots, request) {
        Some(MapiObject::ContentsTable {
            folder_id,
            associated,
            columns,
            position,
            ..
        }) if *folder_id == INBOX_FOLDER_ID => Some((
            format_optional_debug_handle(input_handle(handle_slots, request)),
            *associated,
            *position,
            format_debug_property_tags(columns),
            format_debug_restriction(request_restriction_bytes(request)),
        )),
        _ => None,
    };
    let selected_named_property_context = format_contents_table_named_property_context(
        session,
        input_object(session, handle_slots, request),
    );
    let response = find_row_response(
        request,
        input_object_mut(session, handle_slots, request),
        mailboxes,
        emails,
        snapshot,
        principal.account_id,
    );
    log_outlook_contents_table_find_row(
        principal,
        request_id,
        request,
        input_object(session, handle_slots, request),
        mailboxes,
        emails,
        &selected_named_property_context,
        snapshot,
        &response,
    );
    if let Some(context) = format_inbox_associated_find_context(
        input_object(session, handle_slots, request),
        request,
        principal.account_id,
        snapshot,
        &response,
    ) {
        session.record_last_inbox_associated_find_context(context);
    }
    if inbox_associated_broad_findrow_matched(
        input_object(session, handle_slots, request),
        request,
        &response,
    ) {
        session.record_inbox_associated_broad_findrow(true);
    }
    if inbox_associated_exact_configuration_findrow_matched(
        input_object(session, handle_slots, request),
        request,
        &response,
    ) {
        session.record_inbox_associated_exact_findrow(true);
    }
    if matches!(
        input_object(session, handle_slots, request),
        Some(MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: true,
            ..
        })
    ) && response.get(7).copied().unwrap_or(0) == 1
    {
        session.record_inbox_associated_findrow_returned_content();
    }
    if let Some((handle, associated, position, columns, restriction)) = find_trace {
        let response_return_value = read_response_error_code(&response, 0).unwrap_or(0xffff_ffff);
        let response_found = response.get(7).copied().unwrap_or(0);
        session.record_outlook_view_failure_trace_event(format!(
            "inbox_find_row:request_id={request_id};handle={handle};associated={associated};position={position};columns={columns};request_restriction={restriction};response={response_return_value:#010x};found={response_found}"
        ));
    }
    responses.extend_from_slice(&response);
}
