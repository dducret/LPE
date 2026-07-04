use super::super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi::dispatch) struct PostHierarchyReleaseDebugEvent {
    pub(in crate::mapi::dispatch) input_handle_index: u8,
    pub(in crate::mapi::dispatch) handle: String,
    pub(in crate::mapi::dispatch) object_kind: String,
    pub(in crate::mapi::dispatch) folder_id: String,
    pub(in crate::mapi::dispatch) remaining_before: usize,
    pub(in crate::mapi::dispatch) remaining_after: usize,
    pub(in crate::mapi::dispatch) logon_before_content_sync: bool,
}

pub(in crate::mapi::dispatch) fn format_post_hierarchy_release_kinds(
    events: &[PostHierarchyReleaseDebugEvent],
) -> String {
    events
        .iter()
        .map(|event| event.object_kind.as_str())
        .collect::<Vec<_>>()
        .join(",")
}

pub(in crate::mapi::dispatch) fn format_post_hierarchy_release_context(
    events: &[PostHierarchyReleaseDebugEvent],
) -> String {
    events
        .iter()
        .map(|event| {
            format!(
                "in={};handle={};kind={};folder={};before={};after={};logon_before_content={}",
                event.input_handle_index,
                event.handle,
                event.object_kind,
                event.folder_id,
                event.remaining_before,
                event.remaining_after,
                event.logon_before_content_sync
            )
        })
        .collect::<Vec<_>>()
        .join("|")
}

pub(in crate::mapi::dispatch) fn post_sync_release_flags(
    events: &[PostHierarchyReleaseDebugEvent],
) -> String {
    let mut logon = 0usize;
    let mut public_folder_logon = 0usize;
    let mut folder = 0usize;
    let mut message = 0usize;
    let mut contents_table = 0usize;
    let mut hierarchy_table = 0usize;
    let mut synchronization_source = 0usize;
    let mut synchronization_collector = 0usize;
    let mut notification_subscription = 0usize;
    for event in events {
        match event.object_kind.as_str() {
            "logon" => logon += 1,
            "public_folder_logon" => public_folder_logon += 1,
            "folder" => folder += 1,
            "message" => message += 1,
            "contents_table" => contents_table += 1,
            "hierarchy_table" => hierarchy_table += 1,
            "synchronization_source" => synchronization_source += 1,
            "synchronization_collector" => synchronization_collector += 1,
            "notification_subscription" => notification_subscription += 1,
            _ => {}
        }
    }
    format!(
        "logon={logon};public_folder_logon={public_folder_logon};folder={folder};message={message};contents_table={contents_table};hierarchy_table={hierarchy_table};synchronization_source={synchronization_source};synchronization_collector={synchronization_collector};notification_subscription={notification_subscription}"
    )
}

pub(in crate::mapi::dispatch) fn post_hierarchy_getprops_contract(
    request: &RopRequest,
    object: Option<&MapiObject>,
    property_response: &[u8],
) -> String {
    let property_tags = request.property_tags();
    let response = getprops_contract_response_summary(&property_tags, property_response);
    format!(
        "GetPropertiesSpecific({};probe={};in={};tags={};names={};returned_tags={};problem_tags={};zero_default_tags={};value_shapes={};response={})",
        post_hierarchy_object_contract(object),
        post_hierarchy_getprops_probe_kind(object, &property_tags),
        request.input_handle_index().unwrap_or(0),
        format_debug_property_tags(&property_tags),
        format_set_property_names_for_debug(&property_tags),
        response.returned_tags,
        response.problem_tags,
        response.zero_default_tags,
        response.value_shapes,
        response.result
    )
}

pub(in crate::mapi::dispatch) fn post_hierarchy_setprops_contract(
    request: &RopRequest,
    object: Option<&MapiObject>,
    probe: &SetPropertiesProbeRequest,
    response: &[u8],
) -> String {
    let default_folder_identification_values_stripped =
        default_folder_identification_values_stripped_by_safe_values(object, &probe.property_tags);
    let response_result = read_response_error_code(response, 0).unwrap_or(0xffff_ffff);
    let problem_count = set_properties_problem_count(response);
    let problem_tags = set_properties_problem_details_for_debug(response);
    format!(
        "{}({};probe={};in={};tags={};names={};values={};decoded_values={};problem_count={problem_count};problem_tags={};default_folder_entry_ids_touched={};write_mode={};response={response_result:#010x})",
        if request.rop_id == RopId::SetPropertiesNoReplicate.as_u8() {
            "SetPropertiesNoReplicate"
        } else {
            "SetProperties"
        },
        post_hierarchy_object_contract(object),
        post_hierarchy_setprops_probe_kind(object, &probe.property_tags),
        request.input_handle_index().unwrap_or(0),
        format_debug_property_tags(&probe.property_tags),
        format_set_property_names_for_debug(&probe.property_tags),
        probe.property_value_shapes,
        debug_context_or_none(&probe.default_folder_entry_id_values),
        debug_context_or_none(&problem_tags),
        probe
            .property_tags
            .iter()
            .copied()
            .any(is_default_folder_identification_property_tag),
        post_hierarchy_setprops_write_mode(
            object,
            response_result == 0,
            problem_count,
            default_folder_identification_values_stripped
        )
    )
}

pub(in crate::mapi::dispatch) fn log_set_properties_default_folder_response_debug(
    principal: &AccountPrincipal,
    request_id: &str,
    request: &RopRequest,
    object: Option<&MapiObject>,
    probe: &SetPropertiesProbeRequest,
    response: &[u8],
) {
    if !probe
        .property_tags
        .iter()
        .copied()
        .any(is_default_folder_identification_property_tag)
    {
        return;
    }
    let property_problem_details = set_properties_problem_details_for_debug(response);
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        mapi_request_id = request_id,
        request_rop_id = %rop_id_hex(request.rop_id),
        input_handle_index = request.input_handle_index().unwrap_or(0),
        response_handle_index = request.response_handle_index(),
        object_kind = mapi_object_debug_kind(object),
        folder_id = %mapi_object_debug_folder_id(object),
        property_tags = %format_debug_property_tags(&probe.property_tags),
        property_names = %format_set_property_names_for_debug(&probe.property_tags),
        request_default_folder_entry_id_values = %probe.default_folder_entry_id_values,
        response = %summarize_set_properties_probe_response(response, 0, probe),
        property_problem_details = %property_problem_details,
        "rca debug mapi default folder setprops response"
    );
}

pub(in crate::mapi::dispatch) fn post_hierarchy_open_folder_contract(
    folder_id: u64,
    result: &str,
) -> String {
    format!(
        "OpenFolder(folder=0x{folder_id:016x};role={};container={};probe={})->{}",
        debug_role_for_folder_id(folder_id),
        debug_container_class_for_folder_id(folder_id),
        post_hierarchy_folder_probe_kind(folder_id),
        result
    )
}

pub(in crate::mapi::dispatch) fn post_hierarchy_get_receive_folder_contract(
    message_class: &str,
    folder_id: u64,
) -> String {
    format!(
        "GetReceiveFolder(message_class={message_class};folder=0x{folder_id:016x};role={};container={};probe=receive_folder_probe)->ok",
        debug_role_for_folder_id(folder_id),
        debug_container_class_for_folder_id(folder_id)
    )
}

#[derive(Default)]
pub(in crate::mapi::dispatch) struct GetPropsContractResponseSummary {
    pub(in crate::mapi::dispatch) result: String,
    pub(in crate::mapi::dispatch) returned_tags: String,
    pub(in crate::mapi::dispatch) problem_tags: String,
    pub(in crate::mapi::dispatch) zero_default_tags: String,
    pub(in crate::mapi::dispatch) value_shapes: String,
}

pub(in crate::mapi::dispatch) fn getprops_contract_response_summary(
    property_tags: &[u32],
    response: &[u8],
) -> GetPropsContractResponseSummary {
    let result_code = read_response_error_code(response, 0);
    let mut summary = GetPropsContractResponseSummary {
        result: result_code
            .map(|code| format!("{code:#010x}"))
            .unwrap_or_else(|| "truncated".to_string()),
        value_shapes: get_properties_specific_response_values_for_debug(property_tags, response),
        ..GetPropsContractResponseSummary::default()
    };
    if result_code != Some(0) {
        return summary;
    }
    let row_shape = response.get(6).copied();
    if row_shape != Some(0) && row_shape != Some(1) {
        return summary;
    }
    let mut cursor = Cursor::new(response.get(7..).unwrap_or_default());
    let mut returned_tags = Vec::new();
    let mut problem_tags = Vec::new();
    let mut zero_default_tags = Vec::new();
    let mut value_shapes = Vec::new();
    for tag in property_tags {
        let storage_tag = canonical_property_storage_tag(*tag);
        if row_shape == Some(1) {
            let Ok(flag) = cursor.read_u8() else {
                problem_tags.push(format!(
                    "{storage_tag:#010x}:{}:truncated_flag",
                    set_property_debug_name(storage_tag)
                ));
                break;
            };
            if flag == 0x0A {
                let Ok(error) = cursor.read_u32() else {
                    problem_tags.push(format!(
                        "{storage_tag:#010x}:{}:truncated_error",
                        set_property_debug_name(storage_tag)
                    ));
                    break;
                };
                problem_tags.push(format!(
                    "{storage_tag:#010x}:{}:{error:#010x}",
                    set_property_debug_name(storage_tag)
                ));
                value_shapes.push(format!("{storage_tag:#010x}:error:{error:#010x}"));
                continue;
            }
        }
        match parse_property_value_for_tag(&mut cursor, *tag) {
            Ok(MapiValue::Error(error)) => {
                problem_tags.push(format!(
                    "{storage_tag:#010x}:{}:{error:#010x}",
                    set_property_debug_name(storage_tag)
                ));
                value_shapes.push(format!("{storage_tag:#010x}:error:{error:#010x}"));
            }
            Ok(value) => {
                returned_tags.push(format!("{storage_tag:#010x}"));
                if mapi_value_is_zero_or_default(&value) {
                    zero_default_tags.push(format!("{storage_tag:#010x}"));
                }
                value_shapes.push(format!(
                    "{storage_tag:#010x}:{}",
                    mapi_getprops_contract_value_debug(storage_tag, &value)
                ));
            }
            Err(error) => {
                problem_tags.push(format!(
                    "{storage_tag:#010x}:{}:parse_error={error}",
                    set_property_debug_name(storage_tag)
                ));
                break;
            }
        }
    }
    summary.returned_tags = returned_tags.join(",");
    summary.problem_tags = problem_tags.join(",");
    summary.zero_default_tags = zero_default_tags.join(",");
    summary.value_shapes = value_shapes.join(",");
    summary
}

fn mapi_getprops_contract_value_debug(storage_tag: u32, value: &MapiValue) -> String {
    match storage_tag {
        PID_TAG_ACCESS | PID_TAG_ACCESS_LEVEL => mapi_value_debug_u32_from_value(value),
        _ => mapi_value_debug_shape(value),
    }
}

fn set_properties_problem_count(response: &[u8]) -> usize {
    response
        .get(6..8)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u16::from_le_bytes)
        .map(usize::from)
        .unwrap_or(0)
}

fn set_properties_problem_details_for_debug(response: &[u8]) -> String {
    let mut cursor = Cursor::new(response.get(6..).unwrap_or_default());
    let Ok(problem_count) = cursor.read_u16() else {
        return "truncated".to_string();
    };
    let mut details = Vec::new();
    for _ in 0..problem_count {
        let Ok(index) = cursor.read_u16() else {
            details.push("truncated_index".to_string());
            break;
        };
        let Ok(property_tag) = cursor.read_u32() else {
            details.push(format!("index={index}:truncated_tag"));
            break;
        };
        let Ok(error_code) = cursor.read_u32() else {
            details.push(format!(
                "index={index}:tag={property_tag:#010x}:truncated_error"
            ));
            break;
        };
        details.push(format!(
            "index={index}:tag={property_tag:#010x}:name={}:error={error_code:#010x}",
            set_property_debug_name(property_tag)
        ));
    }
    details.join(",")
}

fn mapi_value_is_zero_or_default(value: &MapiValue) -> bool {
    match value {
        MapiValue::Bool(value) => !*value,
        MapiValue::I16(value) => *value == 0,
        MapiValue::I32(value) => *value == 0,
        MapiValue::I64(value) => *value == 0,
        MapiValue::U32(value) => *value == 0,
        MapiValue::U64(value) => *value == 0,
        MapiValue::F64(value) => *value == 0,
        MapiValue::String(value) => value.is_empty(),
        MapiValue::Binary(value) => value.is_empty(),
        MapiValue::Guid(value) => value.iter().all(|byte| *byte == 0),
        MapiValue::MultiI16(value) => value.is_empty(),
        MapiValue::MultiI32(value) => value.is_empty(),
        MapiValue::MultiI64(value) => value.is_empty(),
        MapiValue::MultiString(value) => value.is_empty(),
        MapiValue::MultiBinary(value) => value.is_empty(),
        MapiValue::MultiGuid(value) => value.is_empty(),
        MapiValue::Error(_) => false,
    }
}

fn post_hierarchy_object_contract(object: Option<&MapiObject>) -> String {
    match object {
        Some(MapiObject::Folder { folder_id, .. }) => format!(
            "kind=folder;folder=0x{folder_id:016x};role={};container={}",
            debug_role_for_folder_id(*folder_id),
            debug_container_class_for_folder_id(*folder_id)
        ),
        Some(object) => object
            .folder_id()
            .map(|folder_id| {
                format!(
                    "kind={};folder=0x{folder_id:016x};role={};container={}",
                    mapi_object_debug_kind(Some(object)),
                    debug_role_for_folder_id(folder_id),
                    debug_container_class_for_folder_id(folder_id)
                )
            })
            .unwrap_or_else(|| {
                format!("kind={};folder=none", mapi_object_debug_kind(Some(object)))
            }),
        None => "kind=none;folder=none".to_string(),
    }
}

fn post_hierarchy_getprops_probe_kind(
    object: Option<&MapiObject>,
    property_tags: &[u32],
) -> &'static str {
    if property_tags
        .iter()
        .copied()
        .any(is_default_folder_identification_property_tag)
        && matches!(
            object,
            Some(MapiObject::Logon)
                | Some(MapiObject::Folder {
                    folder_id: ROOT_FOLDER_ID,
                    ..
                })
        )
    {
        return "root_default_folder_bootstrap";
    }
    object
        .and_then(MapiObject::folder_id)
        .map(post_hierarchy_folder_probe_kind)
        .unwrap_or("generic_probe")
}

fn post_hierarchy_setprops_probe_kind(
    object: Option<&MapiObject>,
    property_tags: &[u32],
) -> &'static str {
    if property_tags
        .iter()
        .copied()
        .any(is_default_folder_identification_property_tag)
        && matches!(
            object,
            Some(MapiObject::Folder {
                folder_id: ROOT_FOLDER_ID,
                ..
            })
        )
    {
        return "root_default_folder_bootstrap";
    }
    object
        .and_then(MapiObject::folder_id)
        .map(post_hierarchy_folder_probe_kind)
        .unwrap_or("generic_probe")
}

fn post_hierarchy_folder_probe_kind(folder_id: u64) -> &'static str {
    match folder_id {
        CALENDAR_FOLDER_ID => "calendar_probe",
        INBOX_FOLDER_ID => "receive_folder_probe",
        ROOT_FOLDER_ID => "root_default_folder_bootstrap",
        _ => "generic_probe",
    }
}

fn post_hierarchy_setprops_write_mode(
    object: Option<&MapiObject>,
    response_ok: bool,
    problem_count: usize,
    default_folder_identification_values_stripped: bool,
) -> &'static str {
    if !response_ok || problem_count > 0 {
        return "rejected";
    }
    if default_folder_identification_values_stripped {
        return "ignored_canonical_projection";
    }
    match object {
        Some(MapiObject::Folder { .. })
        | Some(MapiObject::Message { .. })
        | Some(MapiObject::Contact { .. })
        | Some(MapiObject::Event { .. })
        | Some(MapiObject::Task { .. })
        | Some(MapiObject::Note { .. })
        | Some(MapiObject::JournalEntry { .. })
        | Some(MapiObject::ConversationAction { .. })
        | Some(MapiObject::NavigationShortcut { .. })
        | Some(MapiObject::AssociatedConfig { .. })
        | Some(MapiObject::DelegateFreeBusyMessage { .. })
        | Some(MapiObject::PublicFolderItem { .. })
        | Some(MapiObject::Attachment { .. }) => "persisted",
        Some(_) => "session_only",
        None => "missing_handle",
    }
}

pub(in crate::mapi::dispatch) fn format_inbox_open_loop_summary(
    state: &PostHierarchyActionState,
) -> Option<String> {
    if state.inbox_open_folder_probe_count < 2
        || state.inbox_folder_type_getprops_probe_count < 2
        || state.inbox_normal_contents_table_observed
    {
        return None;
    }
    Some(format!(
        "folder=0x{INBOX_FOLDER_ID:016x};open_folder_count={};folder_type_getprops_count={};normal_contents_table_observed={};normal_setcolumns_observed={};normal_query_rows_observed={};associated_contents_table_observed={};associated_findrow_returned_content={};associated_query_rows_returned_non_empty={};associated_query_rows_reached_end={};associated_config_open_observed={};associated_config_stream_open_observed={};associated_config_stream_read_observed={};rule_organizer_stream_read_observed={};next_debug_focus={};first_loop_transition={};last_open={};last_contents_table={};last_normal_setcolumns={};last_normal_query_position={};last_normal_query_rows={};last_associated_query={};last_associated_non_empty_query={};last_associated_end_query={};last_associated_find={};last_rule_organizer_stream={};last_common_views_inbox_shortcut={};last_inbox_notification_registration={};last_inbox_hierarchy_table={};last_inbox_hierarchy_query={};last_inbox_related_release={};last_folder_type_getprops={};recent_actions={}",
        state.inbox_open_folder_probe_count,
        state.inbox_folder_type_getprops_probe_count,
        state.inbox_normal_contents_table_observed,
        state.inbox_normal_contents_table_setcolumns_observed,
        state.inbox_normal_contents_table_query_rows_observed,
        state.inbox_associated_contents_table_observed,
        state.inbox_associated_findrow_returned_content,
        state.inbox_associated_query_rows_returned_non_empty,
        state.inbox_associated_query_rows_reached_end,
        state.inbox_associated_config_open_observed,
        state.inbox_associated_config_stream_open_observed,
        state.inbox_associated_config_stream_read_observed,
        state.inbox_rule_organizer_stream_read_observed,
        inbox_open_loop_next_debug_focus(state),
        debug_context_or_none(&state.first_inbox_loop_transition_context),
        debug_context_or_none(&state.last_inbox_open_folder_context),
        debug_context_or_none(&state.last_inbox_contents_table_context),
        debug_context_or_none(&state.last_inbox_normal_contents_table_setcolumns_context),
        debug_context_or_none(&state.last_inbox_normal_contents_table_query_position_context),
        debug_context_or_none(&state.last_inbox_normal_contents_table_query_rows_context),
        debug_context_or_none(&state.last_inbox_associated_query_context),
        debug_context_or_none(&state.last_inbox_associated_non_empty_query_context),
        debug_context_or_none(&state.last_inbox_associated_end_query_context),
        debug_context_or_none(&state.last_inbox_associated_find_context),
        debug_context_or_none(&state.last_inbox_rule_organizer_stream_context),
        debug_context_or_none(&state.last_common_views_inbox_shortcut_context),
        debug_context_or_none(&state.last_inbox_notification_registration_context),
        debug_context_or_none(&state.last_inbox_hierarchy_table_context),
        debug_context_or_none(&state.last_inbox_hierarchy_query_context),
        debug_context_or_none(&state.last_inbox_related_release_context),
        debug_context_or_none(&state.last_inbox_folder_type_getprops_context),
        state.recent_probe_actions.join(">")
    ))
}

fn inbox_open_loop_next_debug_focus(state: &PostHierarchyActionState) -> &'static str {
    if state.inbox_associated_contents_table_observed
        && !state.inbox_associated_config_open_observed
        && !state.inbox_normal_contents_table_observed
        && !state.last_inbox_hierarchy_query_context.is_empty()
    {
        "inbox_hierarchy_handoff"
    } else if !state
        .last_inbox_notification_registration_context
        .is_empty()
        && !state.last_common_views_inbox_shortcut_context.is_empty()
        && !state.inbox_associated_contents_table_observed
        && !state.inbox_normal_contents_table_observed
    {
        "post_common_views_notification_handoff"
    } else if !state.last_common_views_inbox_shortcut_context.is_empty()
        && !state.inbox_associated_contents_table_observed
        && !state.inbox_normal_contents_table_observed
    {
        "post_common_views_inbox_handoff"
    } else if state.inbox_associated_contents_table_observed
        && !state.inbox_associated_config_open_observed
        && !state.inbox_normal_contents_table_observed
    {
        "common_views_or_inbox_fai_handoff"
    } else if state.inbox_normal_contents_table_setcolumns_observed
        && !state
            .last_inbox_normal_contents_table_query_position_context
            .is_empty()
        && !state.inbox_normal_contents_table_query_rows_observed
    {
        "visible_inbox_query_rows_missing_after_query_position"
    } else {
        "inbox_open_folder_loop"
    }
}

pub(in crate::mapi::dispatch) fn inbox_post_fai_reopen_stall_observed(
    state: &PostHierarchyActionState,
) -> bool {
    state.post_inbox_fai_handoff_logged
        && state.inbox_associated_contents_table_observed
        && !state.inbox_normal_contents_table_observed
        && !state.last_inbox_related_release_context.is_empty()
}

pub(in crate::mapi::dispatch) fn format_post_fai_folder_type_probe_loop_context(
    state: &PostHierarchyActionState,
) -> Option<String> {
    if !state.post_inbox_fai_handoff_logged
        || !state.post_inbox_fai_reopen_logged
        || state.post_inbox_fai_folder_type_probe_loop_logged
        || state.inbox_normal_contents_table_observed
        || state.inbox_open_folder_probe_count < 2
        || state.inbox_folder_type_getprops_probe_count < 2
    {
        return None;
    }
    Some(format!(
        "folder=0x{INBOX_FOLDER_ID:016x};open_folder_count={};folder_type_getprops_count={};associated_contents_table_observed={};last_open={};last_folder_type_getprops={};last_associated_query={};last_associated_find={};last_inbox_related_release={};recent_actions={};next_expected_client_step=open_inbox_normal_contents_table_or_sync_configure",
        state.inbox_open_folder_probe_count,
        state.inbox_folder_type_getprops_probe_count,
        state.inbox_associated_contents_table_observed,
        debug_context_or_none(&state.last_inbox_open_folder_context),
        debug_context_or_none(&state.last_inbox_folder_type_getprops_context),
        debug_context_or_none(&state.last_inbox_associated_query_context),
        debug_context_or_none(&state.last_inbox_associated_find_context),
        debug_context_or_none(&state.last_inbox_related_release_context),
        state.recent_probe_actions.join(">")
    ))
}

pub(in crate::mapi) fn format_inbox_post_fai_handoff_context(
    state: &PostHierarchyActionState,
) -> String {
    format!(
        "normal_contents_table_observed={};normal_setcolumns_observed={};normal_query_rows_observed={};associated_contents_table_observed={};associated_findrow_returned_content={};associated_query_rows_returned_non_empty={};associated_query_rows_reached_end={};associated_config_open_observed={};associated_config_stream_open_observed={};associated_config_stream_read_observed={};rule_organizer_stream_read_observed={};first_loop_transition={};last_open={};last_contents_table={};last_normal_setcolumns={};last_normal_query_position={};last_normal_query_rows={};last_associated_query={};last_associated_non_empty_query={};last_associated_end_query={};last_associated_find={};last_rule_organizer_stream={};last_common_views_inbox_shortcut={};last_inbox_notification_registration={};last_inbox_hierarchy_table={};last_inbox_hierarchy_query={};last_inbox_related_release={};last_folder_type_getprops={};recent_actions={};next_expected_client_step=open_inbox_associated_config_message_or_normal_contents_table",
        state.inbox_normal_contents_table_observed,
        state.inbox_normal_contents_table_setcolumns_observed,
        state.inbox_normal_contents_table_query_rows_observed,
        state.inbox_associated_contents_table_observed,
        state.inbox_associated_findrow_returned_content,
        state.inbox_associated_query_rows_returned_non_empty,
        state.inbox_associated_query_rows_reached_end,
        state.inbox_associated_config_open_observed,
        state.inbox_associated_config_stream_open_observed,
        state.inbox_associated_config_stream_read_observed,
        state.inbox_rule_organizer_stream_read_observed,
        debug_context_or_none(&state.first_inbox_loop_transition_context),
        debug_context_or_none(&state.last_inbox_open_folder_context),
        debug_context_or_none(&state.last_inbox_contents_table_context),
        debug_context_or_none(&state.last_inbox_normal_contents_table_setcolumns_context),
        debug_context_or_none(&state.last_inbox_normal_contents_table_query_position_context),
        debug_context_or_none(&state.last_inbox_normal_contents_table_query_rows_context),
        debug_context_or_none(&state.last_inbox_associated_query_context),
        debug_context_or_none(&state.last_inbox_associated_non_empty_query_context),
        debug_context_or_none(&state.last_inbox_associated_end_query_context),
        debug_context_or_none(&state.last_inbox_associated_find_context),
        debug_context_or_none(&state.last_inbox_rule_organizer_stream_context),
        debug_context_or_none(&state.last_common_views_inbox_shortcut_context),
        debug_context_or_none(&state.last_inbox_notification_registration_context),
        debug_context_or_none(&state.last_inbox_hierarchy_table_context),
        debug_context_or_none(&state.last_inbox_hierarchy_query_context),
        debug_context_or_none(&state.last_inbox_related_release_context),
        debug_context_or_none(&state.last_inbox_folder_type_getprops_context),
        state.recent_probe_actions.join(">")
    )
}
