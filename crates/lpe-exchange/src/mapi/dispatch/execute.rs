use super::*;

const EXECUTE_ACTIVE_SESSION_RETRY_ATTEMPTS: usize = 50;
const EXECUTE_ACTIVE_SESSION_RETRY_DELAY_MS: u64 = 10;

pub(in crate::mapi) struct ExecuteRequest {
    pub(in crate::mapi) rop_buffer: Vec<u8>,
    pub(in crate::mapi) max_rop_out: u32,
}

pub(in crate::mapi) fn parse_execute_request(body: &[u8]) -> Result<ExecuteRequest> {
    let mut cursor = Cursor::new(body);
    let _flags = cursor.read_u32()?;
    let rop_buffer_size = cursor.read_u32()? as usize;
    let rop_buffer = cursor.read_bytes(rop_buffer_size)?.to_vec();
    let max_rop_out = cursor.read_u32()?;
    let auxiliary_buffer_size = cursor.read_u32()? as usize;
    let _auxiliary_buffer = cursor.read_bytes(auxiliary_buffer_size)?;
    Ok(ExecuteRequest {
        rop_buffer,
        max_rop_out,
    })
}

pub(super) async fn acquire_execute_active_session_request(
    session_id: &str,
) -> Option<ActiveSessionRequest> {
    for attempt in 0..EXECUTE_ACTIVE_SESSION_RETRY_ATTEMPTS {
        if let Some(active_request) = begin_active_session_request(session_id) {
            return Some(active_request);
        }
        if attempt + 1 < EXECUTE_ACTIVE_SESSION_RETRY_ATTEMPTS {
            tokio::time::sleep(std::time::Duration::from_millis(
                EXECUTE_ACTIVE_SESSION_RETRY_DELAY_MS,
            ))
            .await;
        }
    }
    None
}

pub(super) fn rop_buffer_is_store_independent_logon(rop_buffer: &[u8]) -> bool {
    let Some((requests, _handle_table)) = split_rop_buffer(rop_buffer) else {
        return false;
    };
    let mut cursor = Cursor::new(requests);
    let mut saw_request = false;
    while cursor.remaining() > 0 {
        let Ok(request) = read_rop_request(&mut cursor) else {
            return false;
        };
        if !matches!(RopId::from_u8(request.rop_id), Some(RopId::Logon)) {
            return false;
        }
        saw_request = true;
    }
    saw_request
}

pub(super) fn rop_buffer_is_store_independent_release_only(rop_buffer: &[u8]) -> bool {
    let Some((requests, _handle_table)) = split_rop_buffer(rop_buffer) else {
        return false;
    };
    let mut cursor = Cursor::new(requests);
    let mut saw_request = false;
    while cursor.remaining() > 0 {
        let Ok(request) = read_rop_request(&mut cursor) else {
            return false;
        };
        if !matches!(RopId::from_u8(request.rop_id), Some(RopId::Release)) {
            return false;
        }
        saw_request = true;
    }
    saw_request
}

pub(super) fn rop_buffer_is_store_independent_special_folder_getprops_probe(
    rop_buffer: &[u8],
    session: &MapiSession,
) -> bool {
    let Some((requests, handle_table)) = split_rop_buffer(rop_buffer) else {
        return false;
    };
    let Ok(handle_slots) = read_handle_table(handle_table) else {
        return false;
    };
    let mut opened_probe_folder_by_index = HashMap::new();
    let mut saw_open_folder = false;
    let mut saw_get_properties = false;
    let mut cursor = Cursor::new(requests);
    while cursor.remaining() > 0 {
        let Ok(request) = read_rop_request(&mut cursor) else {
            return false;
        };
        match RopId::from_u8(request.rop_id) {
            Some(RopId::Release) => {}
            Some(RopId::OpenFolder) => {
                let folder_id = session
                    .resolve_special_folder_alias(request.folder_id().unwrap_or(ROOT_FOLDER_ID));
                if !is_store_independent_special_folder(folder_id) {
                    return false;
                }
                opened_probe_folder_by_index
                    .insert(request.output_handle_index.unwrap_or(0), folder_id);
                saw_open_folder = true;
            }
            Some(RopId::GetPropertiesSpecific) => {
                let property_tags = request.property_tags();
                if property_tags.iter().copied().any(is_custom_property_tag) {
                    return false;
                }
                let input_handle_index = request.input_handle_index().unwrap_or(0);
                let opened_folder_id = opened_probe_folder_by_index
                    .get(&input_handle_index)
                    .copied();
                let existing_folder_id = input_handle(&handle_slots, &request)
                    .and_then(|handle| session.handles.get(&handle))
                    .and_then(MapiObject::folder_id);
                let Some(folder_id) = opened_folder_id.or(existing_folder_id) else {
                    return false;
                };
                if !is_store_independent_folder_getprops_probe(folder_id, &property_tags) {
                    return false;
                }
                saw_get_properties = true;
            }
            _ => return false,
        }
    }
    saw_open_folder && saw_get_properties
}

fn is_store_independent_folder_getprops_probe(folder_id: u64, property_tags: &[u32]) -> bool {
    is_store_independent_special_folder(folder_id)
        && !property_tags
            .iter()
            .any(|tag| strips_default_folder_identification_value_for_folder_id(folder_id, *tag))
}

fn is_store_independent_special_folder(folder_id: u64) -> bool {
    matches!(
        folder_id,
        ROOT_FOLDER_ID
            | COMMON_VIEWS_FOLDER_ID
            | SCHEDULE_FOLDER_ID
            | SEARCH_FOLDER_ID
            | VIEWS_FOLDER_ID
            | SHORTCUTS_FOLDER_ID
            | FREEBUSY_DATA_FOLDER_ID
    )
}

pub(super) fn rop_buffer_has_no_requests(rop_buffer: &[u8]) -> bool {
    split_rop_buffer(rop_buffer)
        .map(|(requests, _handle_table)| requests.is_empty())
        .unwrap_or(false)
}

pub(super) fn execute_success_rop_buffer(body: &[u8]) -> Option<&[u8]> {
    let mut cursor = Cursor::new(body);
    cursor.read_u32().ok()?;
    cursor.read_u32().ok()?;
    cursor.read_u32().ok()?;
    let rop_buffer_size = cursor.read_u32().ok()? as usize;
    cursor.read_bytes(rop_buffer_size).ok()
}

pub(super) fn apply_execute_max_rop_out(
    request_id: &str,
    request_rop_buffer: &[u8],
    response_rop_buffer: Vec<u8>,
    max_rop_out: u32,
) -> Vec<u8> {
    if max_rop_out == 0 || response_rop_buffer.len() <= max_rop_out as usize {
        return response_rop_buffer;
    }
    let Some((requests, handle_table)) = split_rop_buffer(request_rop_buffer) else {
        return response_rop_buffer;
    };
    let replacement =
        rop_buffer_too_small_response(response_rop_buffer.len(), requests, handle_table);
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        request_type = "Execute",
        mapi_request_id = request_id,
        max_rop_out,
        response_rop_buffer_size = response_rop_buffer.len(),
        replacement_rop_buffer_size = replacement.len(),
        "rca debug mapi execute max rop out exceeded"
    );
    replacement
}

pub(super) fn execute_response_handle_table(
    responses: &[u8],
    handle_slots: &[u32],
    output_handles: &[u32],
    response_handle_indexes: &[u8],
    echo_input_handle_table: bool,
    request_has_release: bool,
) -> Vec<u32> {
    if responses.is_empty() && !echo_input_handle_table {
        return Vec::new();
    }
    let mut handles = response_handle_table_with_released_handle_sentinel(
        handle_slots,
        output_handles,
        echo_input_handle_table,
        request_has_release && echo_input_handle_table && !responses.is_empty(),
    );
    if !responses.is_empty() {
        if let Some(max_response_handle_index) = response_handle_indexes.iter().copied().max() {
            let required_len = usize::from(max_response_handle_index) + 1;
            if handles.len() > required_len {
                handles.truncate(required_len);
            }
            while handles.len() < required_len {
                handles.push(u32::MAX);
            }
        }
    }
    handles
}

pub(super) fn parse_execute_rop_dispatch_input(
    rop_buffer: &[u8],
) -> Result<(&[u8], Vec<u32>, bool), Vec<u8>> {
    let Some((requests, handle_table)) = split_rop_buffer(rop_buffer) else {
        return Err(rop_buffer_with_response(rop_parse_error_response(), &[]));
    };
    let extended = is_rpc_header_ext_rop_buffer(rop_buffer);
    match read_handle_table(handle_table) {
        Ok(handle_slots) => Ok((requests, handle_slots, extended)),
        Err(_) => {
            let response = if extended {
                rop_buffer_with_response_spec(rop_parse_error_response(), &[])
            } else {
                rop_buffer_with_response(rop_parse_error_response(), &[])
            };
            Err(if extended {
                rpc_header_ext_rop_buffer(response)
            } else {
                response
            })
        }
    }
}

pub(super) fn record_execute_stream_batch_observation(
    principal: &AccountPrincipal,
    request_id: &str,
    request_rop_names: &str,
    request_handle_table_summary: &str,
    session: &mut MapiSession,
) {
    if request_rop_names != "SetProperties,OpenStream,SetStreamSize,WriteStream,CommitStream" {
        return;
    }
    let summary = format!(
        "request_id={request_id};request_rops={request_rop_names};handles={request_handle_table_summary}"
    );
    session.record_outlook_stream_batch_observed(summary.clone());
    session.record_outlook_view_failure_trace_event(format!("stream_batch_observed:{summary}"));
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        mapi_request_id = request_id,
        request_rop_names = %request_rop_names,
        input_handle_table_summary = %request_handle_table_summary,
        stream_batch_observed = true,
        "rca debug outlook stream batch observed"
    );
}

pub(super) fn read_next_execute_rop_request(
    cursor: &mut Cursor<'_>,
    responses: &mut Vec<u8>,
) -> Option<RopRequest> {
    if cursor.remaining_is_zero_padding() {
        return None;
    }
    match read_rop_request(cursor) {
        Ok(request) => Some(request),
        Err(_) => {
            responses.extend_from_slice(&rop_parse_error_response());
            None
        }
    }
}

pub(super) fn finalize_execute_rop_buffer(
    responses: Vec<u8>,
    handle_slots: &[u32],
    output_handles: &[u32],
    response_handle_indexes: &[u8],
    echo_input_handle_table: bool,
    request_has_release: bool,
    extended: bool,
) -> Vec<u8> {
    let response_handles = execute_response_handle_table(
        &responses,
        handle_slots,
        output_handles,
        response_handle_indexes,
        echo_input_handle_table,
        request_has_release,
    );
    let response = if extended {
        rop_buffer_with_response_spec(responses, &response_handles)
    } else {
        rop_buffer_with_response(responses, &response_handles)
    };
    if extended {
        rpc_header_ext_rop_buffer(response)
    } else {
        response
    }
}

pub(super) fn record_execute_sync_observations(
    session: &mut MapiSession,
    completed_hierarchy_sync: Option<(u64, String, String)>,
    content_sync_configure_observed: bool,
) {
    if let Some((
        sync_root_folder_id,
        get_buffer_summary,
        default_folder_hierarchy_membership_summary,
    )) = completed_hierarchy_sync
    {
        session.record_completed_hierarchy_sync(
            sync_root_folder_id,
            get_buffer_summary,
            default_folder_hierarchy_membership_summary,
        );
    }
    if content_sync_configure_observed {
        session.record_content_sync_configure();
    }
}

pub(super) fn abort_response(request: &RopRequest, input_object: Option<&MapiObject>) -> Vec<u8> {
    let result = match input_object {
        Some(MapiObject::HierarchyTable { .. } | MapiObject::ContentsTable { .. }) => 0x8004_0114,
        _ => 0x8004_0102,
    };
    rop_error_response(0x38, request.response_handle_index(), result)
}

pub(super) fn append_abort_response(
    request: &RopRequest,
    input_object: Option<&MapiObject>,
    responses: &mut Vec<u8>,
) {
    responses.extend_from_slice(&abort_response(request, input_object));
}

pub(super) fn progress_response(
    request: &RopRequest,
    input_object: Option<&MapiObject>,
) -> Vec<u8> {
    let result = if !matches!(request.payload.first().copied(), Some(0x00 | 0x01)) {
        0x8007_0057
    } else {
        match input_object {
            Some(MapiObject::HierarchyTable { .. } | MapiObject::ContentsTable { .. }) => {
                0x8004_0400
            }
            _ => 0x8004_0102,
        }
    };
    rop_error_response(0x50, request.response_handle_index(), result)
}

pub(super) fn append_progress_response(
    request: &RopRequest,
    input_object: Option<&MapiObject>,
    responses: &mut Vec<u8>,
) {
    responses.extend_from_slice(&progress_response(request, input_object));
}

pub(super) fn reset_table_response(request: &RopRequest, reset_succeeded: bool) -> Vec<u8> {
    if reset_succeeded {
        rop_reset_table_response(request)
    } else {
        rop_error_response(0x81, request.response_handle_index(), 0x8004_0102)
    }
}

pub(super) fn append_reset_table_response(
    request: &RopRequest,
    reset_succeeded: bool,
    responses: &mut Vec<u8>,
) {
    responses.extend_from_slice(&reset_table_response(request, reset_succeeded));
}

pub(super) fn append_execute_status_response(
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    responses: &mut Vec<u8>,
) {
    match RopId::from_u8(request.rop_id) {
        Some(RopId::Abort) => append_abort_response(
            request,
            input_object(session, handle_slots, request),
            responses,
        ),
        Some(RopId::Progress) => append_progress_response(
            request,
            input_object(session, handle_slots, request),
            responses,
        ),
        Some(RopId::ResetTable) => append_reset_table_response(
            request,
            input_object_mut(session, handle_slots, request).is_some_and(reset_table_state),
            responses,
        ),
        _ => {}
    }
}

pub(super) fn unknown_property_wire_type_response(
    principal: &AccountPrincipal,
    request: &RopRequest,
) -> Option<Vec<u8>> {
    if !matches!(request.rop_id, 0x07 | 0x0B | 0x7A)
        || property_tags_have_known_wire_types(&request.property_tags())
    {
        return None;
    }
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = %format!("{:#04x}", request.rop_id),
        input_handle_index = request.input_handle_index().unwrap_or(0),
        property_tags = %format_debug_property_tags(&request.property_tags()),
        failure_reason = "unknown_property_wire_type",
        "rca debug mapi property rop rejected"
    );
    Some(rop_error_response(
        request.rop_id,
        request.response_handle_index(),
        0x8004_0102,
    ))
}
