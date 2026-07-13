use super::*;

pub(super) fn is_table_open_rop(rop_id: RopId) -> bool {
    matches!(
        rop_id,
        RopId::GetHierarchyTable | RopId::GetContentsTable | RopId::GetReceiveFolderTable
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn append_table_open_dispatch_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    request_id: &str,
    request_rop_names: &str,
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
    output_handles: &mut Vec<u32>,
) where
    S: ExchangeStore,
{
    match RopId::from_u8(request.rop_id) {
        Some(RopId::GetHierarchyTable | RopId::GetContentsTable) => {
            append_open_table_response(
                store,
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
                output_handles,
            )
            .await;
        }
        Some(RopId::GetReceiveFolderTable) => {
            append_receive_folder_table_dispatch_response(
                principal,
                session,
                handle_slots,
                request,
                responses,
            );
        }
        _ => {}
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn append_open_table_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    request_id: &str,
    request_rop_names: &str,
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
    output_handles: &mut Vec<u32>,
) where
    S: ExchangeStore,
{
    match RopId::from_u8(request.rop_id) {
        Some(RopId::GetHierarchyTable) => {
            if input_handle(handle_slots, request).is_none() {
                responses.extend_from_slice(&rop_handle_index_error_response(request));
                return;
            }
            if !hierarchy_table_flags_are_valid(request) {
                responses.extend_from_slice(&rop_error_response(
                    0x04,
                    request.output_handle_index.unwrap_or(0),
                    0x8004_0102,
                ));
                return;
            }
            let input_handle_value = input_handle(handle_slots, request);
            let input_debug_kind =
                mapi_object_debug_kind(input_object(session, handle_slots, request));
            let input_debug_folder =
                mapi_object_debug_folder_id(input_object(session, handle_slots, request));
            let input_debug_context =
                format_handle_lineage_context(input_object(session, handle_slots, request));
            let folder_id = input_object(session, handle_slots, request)
                .and_then(|object| object.folder_id())
                .unwrap_or(ROOT_FOLDER_ID);
            let handle = session.allocate_output_handle(
                request.output_handle_index,
                hierarchy_table_object(
                    folder_id,
                    session.deleted_advertised_special_folders.clone(),
                ),
            );
            let table_flags = request.payload.first().copied().unwrap_or(0);
            session.remember_table_notification_eligibility(handle, table_flags & 0x10 == 0);
            set_handle_slot(handle_slots, request.output_handle_index, handle);
            let row_count = if folder_id == PUBLIC_FOLDERS_ROOT_FOLDER_ID
                && snapshot.public_folders().is_empty()
            {
                store
                    .fetch_public_folder_trees(principal.account_id)
                    .await
                    .map(|trees| {
                        trees
                            .iter()
                            .filter(|tree| tree.root_folder_id.is_some())
                            .count()
                            .min(u32::MAX as usize) as u32
                    })
                    .unwrap_or(0)
            } else {
                hierarchy_row_count_excluding_deleted(
                    folder_id,
                    mailboxes,
                    snapshot,
                    &session.deleted_advertised_special_folders,
                )
            };
            responses.extend_from_slice(&get_hierarchy_table_response(request, row_count));
            let hierarchy_context = format!(
                "phase=open;request_id={request_id};request_rops={request_rop_names};input_index={};input_handle={};input_kind={input_debug_kind};input_folder={input_debug_folder};input_context={input_debug_context};output_index={};output_handle={handle};folder=0x{folder_id:016x};role={};row_count={row_count}",
                request.input_handle_index().unwrap_or(0),
                format_optional_debug_handle(input_handle_value),
                request.output_handle_index.unwrap_or(0),
                debug_role_for_folder_id(folder_id)
            );
            session.record_last_table_context(hierarchy_context.clone());
            session.record_outlook_view_failure_trace_event(format!(
                "hierarchy_table_open:{hierarchy_context}"
            ));
            tracing::info!(
                rca_debug = true,
                adapter = "mapi",
                endpoint = "emsmdb",
                mailbox = %principal.email,
                request_type = "Execute",
                mapi_request_id = %request_id,
                request_rop_id = "0x04",
                input_handle_index = request.input_handle_index().unwrap_or(0),
                input_handle_value = %format_optional_debug_handle(input_handle_value),
                input_object_kind = input_debug_kind,
                input_folder_id = %input_debug_folder,
                input_handle_context = %input_debug_context,
                output_handle_index = request.output_handle_index.unwrap_or(0),
                output_handle_value = handle,
                folder_id = %format!("0x{folder_id:016x}"),
                folder_role = debug_role_for_folder_id(folder_id),
                hierarchy_row_count = row_count,
                microsoft_reference = "MS-OXCFOLD 2.2.1.13/3.1.4.4.2.13; MS-OXCROPS 2.2.4.13",
                "rca debug mapi hierarchy table opened"
            );
            if folder_id == INBOX_FOLDER_ID {
                session.record_last_inbox_hierarchy_table_context(format!(
                    "input_index={};output_index={};output_handle={};row_count={row_count};expected_subfolders=false",
                    request.input_handle_index().unwrap_or(0),
                    request.output_handle_index.unwrap_or(0),
                    handle
                ));
                session.record_recent_probe_action(format!(
                    "GetHierarchyTable(in={},out={},row_count={row_count})",
                    request.input_handle_index().unwrap_or(0),
                    handle
                ));
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    mailbox = %principal.email,
                    request_type = "Execute",
                    request_rop_id = "0x04",
                    folder_id = %format!("0x{folder_id:016x}"),
                    input_handle_index = request.input_handle_index().unwrap_or(0),
                    output_handle_index = request.output_handle_index.unwrap_or(0),
                    output_handle_value = handle,
                    hierarchy_row_count = row_count,
                    expected_subfolders = false,
                    normal_contents_table_observed =
                        session.post_hierarchy_actions.inbox_normal_contents_table_observed,
                    associated_contents_table_observed =
                        session.post_hierarchy_actions.inbox_associated_contents_table_observed,
                    message = "rca debug mapi inbox hierarchy table opened"
                );
            }
            if matches!(folder_id, ROOT_FOLDER_ID | IPM_SUBTREE_FOLDER_ID) {
                log_outlook_bootstrap_phase(
                    principal,
                    "hierarchy_table_opened",
                    "0x04",
                    Some(folder_id),
                    false,
                    Some(row_count),
                    None,
                    Some(handle),
                    "",
                );
            }
            output_handles.push(handle);
        }
        Some(RopId::GetContentsTable) => {
            if input_handle(handle_slots, request).is_none() {
                responses.extend_from_slice(&rop_handle_index_error_response(request));
                return;
            }
            let Some(input_object) = input_object(session, handle_slots, request) else {
                responses.extend_from_slice(&rop_handle_index_error_response(request));
                return;
            };
            let folder_id = match input_object {
                MapiObject::Folder { folder_id, .. } => *folder_id,
                _ => {
                    responses.extend_from_slice(&rop_error_response(
                        0x05,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_0102,
                    ));
                    return;
                }
            };
            let table_flags = request.payload.first().copied().unwrap_or(0);
            if let Some(error) = contents_table_flags_error(
                table_flags,
                folder_id,
                snapshot.public_folder_for_id(folder_id).is_some(),
            ) {
                responses.extend_from_slice(&rop_error_response(
                    0x05,
                    request.output_handle_index.unwrap_or(0),
                    error,
                ));
                return;
            }
            let associated = table_flags & 0x02 != 0;
            let contents_folder_id = if table_flags & 0x80 != 0
                && folder_id == ROOT_FOLDER_ID
                && snapshot.public_folder_for_id(folder_id).is_none()
            {
                CONVERSATION_MEMBERS_CONTENTS_TABLE_ID
            } else {
                folder_id
            };
            if !snapshot
                .folder_access_for_principal(folder_id, principal.account_id)
                .map(|access| access.may_read)
                .unwrap_or(true)
            {
                responses.extend_from_slice(&rop_error_response(
                    0x05,
                    request.output_handle_index.unwrap_or(0),
                    0x8007_0005,
                ));
                return;
            }
            let (_, _, container_class) = debug_open_folder_metadata(contents_folder_id, mailboxes);
            let initial_sort = default_view_contents_table_initial_sort(
                contents_folder_id,
                associated,
                &container_class,
            );
            let handle = session.allocate_output_handle(
                request.output_handle_index,
                contents_table_object_with_default_view_sort(
                    contents_folder_id,
                    associated,
                    initial_sort.clone(),
                ),
            );
            session.remember_table_notification_eligibility(handle, table_flags & 0x10 == 0);
            set_handle_slot(handle_slots, request.output_handle_index, handle);
            let row_count = contents_table_open_row_count(
                contents_folder_id,
                associated,
                mailboxes,
                emails,
                snapshot,
            );
            log_outlook_contents_table_open(
                principal,
                request_id,
                request,
                contents_folder_id,
                table_flags,
                associated,
                row_count,
                handle,
                snapshot,
            );
            session.record_last_table_context(format!(
                "phase=open;request_id={request_id};request_rops={request_rop_names};input_index={};output_index={};handle={};folder=0x{contents_folder_id:016x};role={};associated={associated};table_flags=0x{table_flags:02x};row_count={row_count}",
                request.input_handle_index().unwrap_or(0),
                request.output_handle_index.unwrap_or(0),
                handle,
                debug_role_for_folder_id(contents_folder_id)
            ));
            if !associated && default_view_supported_folder(contents_folder_id, &container_class) {
                let initial_sort_summary = format_debug_sort_orders(&initial_sort);
                let context = format!(
                    "request_id={request_id};handle={handle};folder=0x{contents_folder_id:016x};role={};container_class={container_class};row_count={row_count};table_flags=0x{table_flags:02x};initial_sort={initial_sort_summary}",
                    debug_role_for_folder_id(contents_folder_id)
                );
                session.record_outlook_view_failure_trace_event(format!(
                    "default_view_normal_table_open:{context}"
                ));
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    mailbox = %principal.email,
                    request_type = "Execute",
                    mapi_request_id = %request_id,
                    request_rop_id = "0x05",
                    folder_id = format!("0x{contents_folder_id:016x}"),
                    folder_role = debug_role_for_folder_id(contents_folder_id),
                    container_class,
                    output_handle = handle,
                    row_count,
                    initial_sort = %initial_sort_summary,
                    default_view_normal_table_open = %context,
                    next_expected_client_step = "set_columns_or_query_rows_on_default_view_contents_table",
                    "rca debug mapi default view normal contents table opened"
                );
            }
            if contents_folder_id == CALENDAR_FOLDER_ID && !associated {
                session.record_outlook_view_failure_trace_event(format!(
                    "calendar_normal_table_open:request_id={request_id};handle={handle};row_count={row_count};flags=0x{table_flags:02x}"
                ));
            }
            if folder_id == INBOX_FOLDER_ID {
                session.record_outlook_view_failure_trace_event(format!(
                    "inbox_contents_table_open:request_id={request_id};handle={handle};associated={associated};row_count={row_count};flags=0x{table_flags:02x}"
                ));
                if associated {
                    session.record_inbox_associated_contents_table();
                } else {
                    session.record_inbox_normal_contents_table();
                    record_mapi_outlook_view_inbox_normal_contents_opened();
                    record_normal_inbox_table_lifecycle(
                        session,
                        "open",
                        request_id,
                        request_rop_names,
                        request.input_handle_index().unwrap_or(0),
                        Some(handle),
                        &format!(
                            "output_index={};folder=0x{contents_folder_id:016x};role={};row_count={row_count};table_flags=0x{table_flags:02x};initial_sort={}",
                            request.output_handle_index.unwrap_or(0),
                            debug_role_for_folder_id(contents_folder_id),
                            format_debug_sort_orders(&initial_sort)
                        ),
                    );
                }
                session.record_last_inbox_contents_table_context(format!(
                    "input_index={};output_index={};output_handle={};table_flags=0x{table_flags:02x};associated={associated};row_count={row_count}",
                    request.input_handle_index().unwrap_or(0),
                    request.output_handle_index.unwrap_or(0),
                    handle
                ));
                if !associated {
                    session.record_recent_probe_action(format!(
                        "GetContentsTable(in={},out={},associated=false,row_count={row_count})",
                        request.input_handle_index().unwrap_or(0),
                        handle
                    ));
                }
            }
            if associated && folder_id == COMMON_VIEWS_FOLDER_ID {
                log_outlook_bootstrap_phase(
                    principal,
                    "common_views_associated_table_opened",
                    "0x05",
                    Some(folder_id),
                    associated,
                    Some(row_count),
                    None,
                    Some(handle),
                    "",
                );
            } else if associated && folder_id == INBOX_FOLDER_ID {
                log_outlook_bootstrap_phase(
                    principal,
                    "inbox_associated_table_opened",
                    "0x05",
                    Some(folder_id),
                    associated,
                    Some(row_count),
                    None,
                    Some(handle),
                    "",
                );
            } else if !associated && folder_id == INBOX_FOLDER_ID {
                log_outlook_bootstrap_phase(
                    principal,
                    "inbox_contents_table_opened",
                    "0x05",
                    Some(folder_id),
                    associated,
                    Some(row_count),
                    None,
                    Some(handle),
                    "",
                );
            }
            responses.extend_from_slice(&get_contents_table_response(request, row_count));
            output_handles.push(handle);
        }
        _ => {}
    }
}

pub(super) fn contents_table_object_with_default_view_sort(
    folder_id: u64,
    associated: bool,
    sort_orders: Vec<MapiSortOrder>,
) -> MapiObject {
    let mut object = contents_table_object(folder_id, associated);
    if let MapiObject::ContentsTable {
        sort_orders: object_sort_orders,
        ..
    } = &mut object
    {
        *object_sort_orders = sort_orders;
    }
    object
}

pub(super) fn default_view_contents_table_initial_sort(
    folder_id: u64,
    associated: bool,
    container_class: &str,
) -> Vec<MapiSortOrder> {
    if associated || !default_view_supported_folder(folder_id, container_class) {
        return Vec::new();
    }
    let view_name = if folder_id == SENT_FOLDER_ID {
        "Sent To"
    } else {
        crate::mapi_store::outlook_default_folder_named_view_name(folder_id)
    };
    outlook_folder_view_sort_orders(folder_id, view_name)
}
