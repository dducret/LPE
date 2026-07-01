use super::*;

pub(super) fn is_notification_dispatch_rop(rop_id: RopId) -> bool {
    matches!(rop_id, RopId::RegisterNotification)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn append_notification_dispatch_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    request_id: &str,
    request_rop_names: &str,
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    responses: &mut Vec<u8>,
    output_handles: &mut Vec<u32>,
) where
    S: ExchangeStore,
{
    if matches!(
        RopId::from_u8(request.rop_id),
        Some(RopId::RegisterNotification)
    ) {
        append_register_notification_response(
            store,
            principal,
            request_id,
            request_rop_names,
            session,
            handle_slots,
            request,
            responses,
            output_handles,
        )
        .await;
    }
}

pub(super) async fn append_register_notification_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    request_id: &str,
    request_rop_names: &str,
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    responses: &mut Vec<u8>,
    output_handles: &mut Vec<u32>,
) where
    S: ExchangeStore,
{
    let registration = notification_registration_from_request(request);
    let input_handle_value = input_handle(handle_slots, request);
    let input_object = input_object(session, handle_slots, request);
    let input_object_kind = mapi_object_debug_kind(input_object);
    let input_folder_id = mapi_object_debug_folder_id(input_object);
    let input_context = format_handle_lineage_context(input_object);
    let notification_types = registration.notification_types;
    let notification_folder_id = registration.folder_id;
    if session.notification_cursor.is_none() {
        session.notification_cursor = store
            .fetch_mapi_notification_cursor(principal.account_id)
            .await
            .ok()
            .flatten();
    }
    let handle = session.allocate_output_handle(
        request.output_handle_index,
        MapiObject::NotificationSubscription { registration },
    );
    set_handle_slot(handle_slots, request.output_handle_index, handle);
    responses.extend_from_slice(&rop_register_notification_response(request));
    output_handles.push(handle);
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        mapi_request_id = request_id,
        request_rop_id = "0x29",
        request_rop_names = %request_rop_names,
        input_handle_index = request.input_handle_index().unwrap_or(0),
        input_handle_value = %format_optional_debug_handle(input_handle_value),
        input_object_kind = input_object_kind,
        input_folder_id = %input_folder_id,
        input_context = %input_context,
        output_handle_index = request.output_handle_index.unwrap_or(0),
        output_handle_value = handle,
        notification_types = %format!("0x{notification_types:04x}"),
        want_whole_store = notification_folder_id.is_none(),
        notification_folder_id = %notification_folder_id
            .map(|folder_id| format!("0x{folder_id:016x}"))
            .unwrap_or_else(|| "none".to_string()),
        notification_cursor_loaded = session.notification_cursor.is_some(),
        inbox_normal_contents_table_observed =
            session.post_hierarchy_actions.inbox_normal_contents_table_observed,
        inbox_normal_setcolumns_observed =
            session
                .post_hierarchy_actions
                .inbox_normal_contents_table_setcolumns_observed,
        inbox_normal_query_rows_observed =
            session
                .post_hierarchy_actions
                .inbox_normal_contents_table_query_rows_observed,
        last_normal_setcolumns_handle =
            %format_optional_debug_handle(session
                .post_hierarchy_actions
                .last_inbox_normal_contents_table_setcolumns_handle),
        last_normal_query_rows_handle =
            %format_optional_debug_handle(session
                .post_hierarchy_actions
                .last_inbox_normal_contents_table_query_rows_handle),
        recent_actions =
            %session.post_hierarchy_actions.recent_probe_actions.join(">"),
        "rca debug mapi register notification"
    );
}
