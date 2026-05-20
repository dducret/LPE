use super::notifications::*;
use super::permissions::*;
use super::properties::*;
use super::rop::*;
use super::session::*;
use super::store_adapter::*;
use super::sync::*;
use super::tables::*;
use super::transport::*;
use super::wire::RopId;
use super::*;
use crate::store::{MapiFolderPropertyValue, MapiSyncCheckpoint};

const HIERARCHY_SYNC_CURSOR_VERSION: u64 = 2;

pub(in crate::mapi) async fn execute_response<S, V>(
    store: &S,
    validator: &Validator<V>,
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    headers: &HeaderMap,
    body: &[u8],
    request_id: &str,
) -> Response
where
    S: ExchangeStore,
    V: Detector,
{
    let Some(session_id) = request_cookie(endpoint, headers) else {
        return execute_failure_response(request_id, 13, "missing MAPI session cookie", None);
    };
    if !request_sequence_cookie_matches(endpoint, headers, &session_id) {
        return execute_failure_response(
            request_id,
            6,
            "invalid MAPI request sequence cookie",
            None,
        );
    }
    let Some(_active_request) = begin_active_session_request(&session_id) else {
        return execute_failure_response(
            request_id,
            15,
            "MAPI session already has an active request",
            None,
        );
    };
    let Some(session) = get_session(&session_id) else {
        return execute_failure_response(request_id, 10, "MAPI session context not found", None);
    };
    if session.endpoint != endpoint
        || session.tenant_id != principal.tenant_id
        || session.account_id != principal.account_id
        || session.email != principal.email
    {
        return execute_failure_response(
            request_id,
            10,
            "MAPI authentication context changed",
            None,
        );
    }

    let execute = match parse_execute_request(body) {
        Ok(execute) => execute,
        Err(error) => {
            return execute_failure_response(
                request_id,
                4,
                &format!("invalid Execute request body: {error}"),
                Some(session_cookie(endpoint, &session_id, false)),
            );
        }
    };
    let Some(mut session) = remove_session(&session_id) else {
        return execute_failure_response(
            request_id,
            10,
            "MAPI session context not found",
            Some(session_cookie(endpoint, &session_id, false)),
        );
    };
    if !session_matches(&session, endpoint, principal) {
        return execute_failure_response(
            request_id,
            10,
            "MAPI authentication context changed",
            Some(session_cookie(endpoint, &session_id, false)),
        );
    }
    let rop_fingerprint = mapi_payload_fingerprint(&execute.rop_buffer);
    let request_debug = summarize_request_rop_buffer(&execute.rop_buffer);
    let hierarchy_completed_before_execute = session.hierarchy_sync_completed();
    if let Some(cached) = session.completed_execute_requests.get(request_id).cloned() {
        if cached.rop_fingerprint == rop_fingerprint {
            let post_hierarchy_observation =
                if endpoint == MapiEndpoint::Emsmdb && hierarchy_completed_before_execute {
                    session.record_execute_after_hierarchy_completion(&request_debug.ids)
                } else {
                    PostHierarchyExecuteObservation::default()
                };
            let cached_rop_buffer = execute_success_rop_buffer(&cached.response_body);
            log_execute_rop_debug(
                endpoint,
                principal,
                headers,
                &session_id,
                request_id,
                &request_debug,
                &execute.rop_buffer,
                cached_rop_buffer.unwrap_or_default(),
                &session,
                post_hierarchy_observation,
            );
            store_session(session_id.clone(), session);
            return mapi_response_with_cookies(
                "Execute",
                request_id,
                0,
                cached.response_body,
                session_context_cookies(endpoint, &session_id, false),
            );
        }
        store_session(session_id.clone(), session);
        return execute_failure_response(
            request_id,
            12,
            "reused MAPI Execute request id with a different ROP payload",
            Some(session_cookie(endpoint, &session_id, false)),
        );
    }

    let access_plan = plan_mapi_store_access(&session, &execute.rop_buffer);
    let snapshot =
        match load_mapi_store_for_access_plan(store, principal.account_id, &access_plan, 500).await
        {
            Ok(snapshot) => snapshot,
            Err(error) => {
                store_session(session_id.clone(), session);
                return execute_failure_response(
                    request_id,
                    4,
                    &format!("failed to load MAPI mail store view: {error}"),
                    Some(session_cookie(endpoint, &session_id, false)),
                );
            }
        };
    let mailboxes = snapshot.mailboxes();
    let emails = snapshot.emails();
    let rop_buffer = execute_rops(
        store,
        principal,
        &mut session,
        &mailboxes,
        &emails,
        &snapshot,
        validator,
        &execute.rop_buffer,
    )
    .await;
    let post_hierarchy_observation =
        if endpoint == MapiEndpoint::Emsmdb && hierarchy_completed_before_execute {
            session.record_execute_after_hierarchy_completion(&request_debug.ids)
        } else {
            PostHierarchyExecuteObservation::default()
        };
    log_execute_rop_debug(
        endpoint,
        principal,
        headers,
        &session_id,
        request_id,
        &request_debug,
        &execute.rop_buffer,
        &rop_buffer,
        &session,
        post_hierarchy_observation,
    );
    let response_body = execute_success_body(rop_buffer, Vec::new());
    cache_execute_response(&mut session, request_id, rop_fingerprint, &response_body);
    store_session(session_id.clone(), session);
    mapi_response_with_cookies(
        "Execute",
        request_id,
        0,
        response_body,
        session_context_cookies(endpoint, &session_id, false),
    )
}

pub(in crate::mapi) struct ExecuteRequest {
    rop_buffer: Vec<u8>,
}

pub(in crate::mapi) fn parse_execute_request(body: &[u8]) -> Result<ExecuteRequest> {
    let mut cursor = Cursor::new(body);
    let _flags = cursor.read_u32()?;
    let rop_buffer_size = cursor.read_u32()? as usize;
    let rop_buffer = cursor.read_bytes(rop_buffer_size)?.to_vec();
    let _max_rop_out = cursor.read_u32()?;
    let auxiliary_buffer_size = cursor.read_u32()? as usize;
    let _auxiliary_buffer = cursor.read_bytes(auxiliary_buffer_size)?;
    Ok(ExecuteRequest { rop_buffer })
}

fn execute_success_rop_buffer(body: &[u8]) -> Option<&[u8]> {
    let mut cursor = Cursor::new(body);
    cursor.read_u32().ok()?;
    cursor.read_u32().ok()?;
    cursor.read_u32().ok()?;
    let rop_buffer_size = cursor.read_u32().ok()? as usize;
    cursor.read_bytes(rop_buffer_size).ok()
}

const MAX_ROP_DEBUG_ENTRIES: usize = 32;

#[derive(Debug, Default)]
struct RopRequestDebugSummary {
    ids: Vec<u8>,
    ids_csv: String,
    handle_count: usize,
    handle_table_summary: String,
    extended: bool,
    parse_error: String,
}

#[derive(Debug, Default)]
struct RopResponseDebugSummary {
    ids_csv: String,
    results_csv: String,
    count: usize,
    handle_count: usize,
    handle_table_summary: String,
    extended: bool,
    parse_error: String,
}

#[derive(Debug, Default)]
struct LogonResponseDebugSummary {
    present: bool,
    output_handle_index: String,
    error_code: String,
    logon_flags: String,
    special_folder_ids: String,
    response_flags: String,
    mailbox_guid: String,
    replid: String,
    replica_guid: String,
    parse_error: String,
}

fn log_execute_rop_debug(
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    headers: &HeaderMap,
    session_id: &str,
    request_id: &str,
    request: &RopRequestDebugSummary,
    request_rop_buffer: &[u8],
    response_rop_buffer: &[u8],
    session: &MapiSession,
    post_hierarchy_observation: PostHierarchyExecuteObservation,
) {
    let response = summarize_response_rop_buffer(response_rop_buffer, &request.ids);
    let logon = summarize_logon_response_rop(response_rop_buffer, &request.ids);
    let endpoint = match endpoint {
        MapiEndpoint::Emsmdb => "emsmdb",
        MapiEndpoint::Nspi => "nspi",
    };
    let client_request_id = safe_header(headers, "client-request-id").unwrap_or_default();
    let client_info = safe_header(headers, "x-clientinfo").unwrap_or_default();
    let client_application = safe_header(headers, "x-clientapplication").unwrap_or_default();
    let post_hierarchy = post_hierarchy_action_summary(session, false);
    let message = "rca debug mapi execute rops";

    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = endpoint,
        tenant_id = %principal.tenant_id,
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = "Execute",
        session_context_id = %session_id,
        mapi_request_id = request_id,
        client_request_id = %client_request_id,
        client_info = %client_info,
        client_application = %client_application,
        request_rop_ids = %request.ids_csv,
        request_rop_count = request.ids.len(),
        request_handle_count = request.handle_count,
        input_handle_table_summary = %request.handle_table_summary,
        request_extended_rop_buffer = request.extended,
        request_rop_parse_error = %request.parse_error,
        response_rop_ids = %response.ids_csv,
        response_rop_results_best_effort = %response.results_csv,
        response_rop_count = response.count,
        response_handle_count = response.handle_count,
        output_handle_table_summary = %response.handle_table_summary,
        response_extended_rop_buffer = response.extended,
        response_rop_parse_error = %response.parse_error,
        last_completed_hierarchy_sync_root = %post_hierarchy.last_completed_hierarchy_sync_root,
        content_sync_started_after_hierarchy =
            post_hierarchy.content_sync_configure_observed,
        post_hierarchy_execute_count = post_hierarchy.execute_count,
        post_hierarchy_rop_ids_seen = %post_hierarchy.rop_ids_seen,
        logon_response_present = logon.present,
        logon_output_handle_index = %logon.output_handle_index,
        logon_error_code = %logon.error_code,
        logon_flags = %logon.logon_flags,
        logon_special_folder_ids = %logon.special_folder_ids,
        logon_response_flags = %logon.response_flags,
        logon_mailbox_guid = %logon.mailbox_guid,
        logon_replid = %logon.replid,
        logon_replica_guid = %logon.replica_guid,
        logon_parse_error = %logon.parse_error,
        request_rop_buffer_bytes = request_rop_buffer.len(),
        response_rop_buffer_bytes = response_rop_buffer.len(),
        message = message,
    );

    if endpoint == "emsmdb"
        && (post_hierarchy_observation.first_execute
            || post_hierarchy_observation.first_bootstrap_probe
            || post_hierarchy_observation.first_set_properties_probe)
    {
        let probe = summarize_first_post_hierarchy_probe(request_rop_buffer, response_rop_buffer);
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = endpoint,
            tenant_id = %principal.tenant_id,
            account_id = %principal.account_id,
            mailbox = %principal.email,
            request_type = "Execute",
            session_context_id = %session_id,
            mapi_request_id = request_id,
            client_request_id = %client_request_id,
            client_info = %client_info,
            client_application = %client_application,
            last_completed_hierarchy_sync_root =
                %post_hierarchy.last_completed_hierarchy_sync_root,
            first_post_hierarchy_execute = post_hierarchy_observation.first_execute,
            first_post_hierarchy_bootstrap_probe =
                post_hierarchy_observation.first_bootstrap_probe,
            first_post_hierarchy_set_properties_probe =
                post_hierarchy_observation.first_set_properties_probe,
            request_rop_ids = %request.ids_csv,
            response_rop_results_best_effort = %response.results_csv,
            open_folder_request_count = probe.open_folder_request_count,
            open_folder_requests = %probe.open_folder_requests,
            open_folder_response_shapes = %probe.open_folder_response_shapes,
            get_properties_specific_request_count = probe.get_properties_specific_request_count,
            get_properties_specific_requests = %probe.get_properties_specific_requests,
            get_properties_specific_response_shapes =
                %probe.get_properties_specific_response_shapes,
            set_properties_request_count = probe.set_properties_request_count,
            set_properties_requests = %probe.set_properties_requests,
            set_properties_response_shapes = %probe.set_properties_response_shapes,
            probe_parse_error = %probe.parse_error,
            "rca debug mapi post hierarchy execute probe"
        );
    }

    if endpoint == "emsmdb" && session.post_hierarchy_actions.set_properties_probe_observed {
        let probe = summarize_first_post_hierarchy_probe(request_rop_buffer, response_rop_buffer);
        let root_logon_requests =
            summarize_root_logon_default_folder_getprops_requests(request_rop_buffer, session);
        if !root_logon_requests.is_empty() {
            tracing::info!(
                rca_debug = true,
                adapter = "mapi",
                endpoint = endpoint,
                tenant_id = %principal.tenant_id,
                account_id = %principal.account_id,
                mailbox = %principal.email,
                request_type = "Execute",
                session_context_id = %session_id,
                mapi_request_id = request_id,
                client_request_id = %client_request_id,
                client_info = %client_info,
                client_application = %client_application,
                last_completed_hierarchy_sync_root =
                    %post_hierarchy.last_completed_hierarchy_sync_root,
                post_hierarchy_execute_count = post_hierarchy.execute_count,
                request_rop_ids = %request.ids_csv,
                response_rop_results_best_effort = %response.results_csv,
                get_properties_specific_request_count = probe.get_properties_specific_request_count,
                get_properties_specific_requests = %probe.get_properties_specific_requests,
                get_properties_specific_root_logon_requests = %root_logon_requests,
                get_properties_specific_response_shapes =
                    %probe.get_properties_specific_response_shapes,
                probe_parse_error = %probe.parse_error,
                "rca debug mapi post setprops default folder get props"
            );
        }
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct FirstPostHierarchyProbeDebugSummary {
    open_folder_request_count: usize,
    open_folder_requests: String,
    open_folder_response_shapes: String,
    get_properties_specific_request_count: usize,
    get_properties_specific_requests: String,
    get_properties_specific_response_shapes: String,
    set_properties_request_count: usize,
    set_properties_requests: String,
    set_properties_response_shapes: String,
    parse_error: String,
}

#[derive(Debug, PartialEq, Eq)]
struct OpenFolderProbeRequest {
    output_handle_index: u8,
    folder_id: u64,
}

#[derive(Debug, PartialEq, Eq)]
struct GetPropertiesSpecificProbeRequest {
    input_handle_index: u8,
    property_tags: Vec<u32>,
}

#[derive(Debug, PartialEq, Eq)]
struct SetPropertiesProbeRequest {
    input_handle_index: u8,
    property_tags: Vec<u32>,
    property_value_shapes: String,
    default_folder_entry_id_values: String,
    parse_error: String,
}

fn summarize_first_post_hierarchy_probe(
    request_rop_buffer: &[u8],
    response_rop_buffer: &[u8],
) -> FirstPostHierarchyProbeDebugSummary {
    let mut summary = FirstPostHierarchyProbeDebugSummary::default();
    let Some((requests, _request_handle_table)) = split_rop_buffer(request_rop_buffer) else {
        summary.parse_error = "invalid request ROP buffer".to_string();
        return summary;
    };
    let mut request_cursor = Cursor::new(requests);
    let mut request_rop_ids = Vec::new();
    let mut open_folder_requests = Vec::new();
    let mut get_properties_requests = Vec::new();
    let mut set_properties_requests = Vec::new();
    while request_cursor.remaining() > 0 {
        let request = match read_rop_request(&mut request_cursor) {
            Ok(request) => request,
            Err(error) => {
                summary.parse_error = error.to_string();
                break;
            }
        };
        let rop_id = request.typed().rop_id();
        request_rop_ids.push(rop_id);
        match rop_id {
            0x02 => open_folder_requests.push(OpenFolderProbeRequest {
                output_handle_index: request.output_handle_index.unwrap_or(0),
                folder_id: request.folder_id().unwrap_or(ROOT_FOLDER_ID),
            }),
            0x07 => get_properties_requests.push(GetPropertiesSpecificProbeRequest {
                input_handle_index: request.input_handle_index().unwrap_or(0),
                property_tags: request.property_tags(),
            }),
            0x0A | 0x79 => set_properties_requests.push(set_properties_probe_request(&request)),
            _ => {}
        }
    }

    summary.open_folder_request_count = open_folder_requests.len();
    summary.open_folder_requests = open_folder_requests
        .iter()
        .map(|request| {
            format!(
                "out={};folder=0x{:016x};name={}",
                request.output_handle_index,
                request.folder_id,
                post_hierarchy_probe_folder_name(request.folder_id)
            )
        })
        .collect::<Vec<_>>()
        .join("|");
    summary.get_properties_specific_request_count = get_properties_requests.len();
    summary.get_properties_specific_requests = get_properties_requests
        .iter()
        .map(|request| {
            format!(
                "in={};tags={}",
                request.input_handle_index,
                format_debug_property_tags(&request.property_tags)
            )
        })
        .collect::<Vec<_>>()
        .join("|");
    summary.set_properties_request_count = set_properties_requests.len();
    summary.set_properties_requests = set_properties_requests
        .iter()
        .map(|request| {
            format!(
                "in={};tags={};values={};default_folder_entry_ids={};parse_error={}",
                request.input_handle_index,
                format_debug_property_tags(&request.property_tags),
                request.property_value_shapes,
                request.default_folder_entry_id_values,
                request.parse_error
            )
        })
        .collect::<Vec<_>>()
        .join("|");

    let Some((responses, _response_handle_table)) = split_rop_buffer(response_rop_buffer) else {
        if summary.parse_error.is_empty() {
            summary.parse_error = "invalid response ROP buffer".to_string();
        }
        return summary;
    };
    let mut response_offset = 0usize;
    let mut open_folder_index = 0usize;
    let mut get_properties_index = 0usize;
    let mut set_properties_index = 0usize;
    let mut open_folder_responses = Vec::new();
    let mut get_properties_responses = Vec::new();
    let mut set_properties_responses = Vec::new();
    for rop_id in request_rop_ids {
        if rop_has_no_response(rop_id) {
            continue;
        }
        let Some(found) = responses
            .get(response_offset..)
            .and_then(|remaining| remaining.iter().position(|candidate| *candidate == rop_id))
        else {
            break;
        };
        response_offset += found;
        match rop_id {
            0x02 => {
                if let Some(request) = open_folder_requests.get(open_folder_index) {
                    open_folder_responses.push(summarize_open_folder_probe_response(
                        responses,
                        response_offset,
                        request,
                    ));
                }
                open_folder_index = open_folder_index.saturating_add(1);
            }
            0x07 => {
                if let Some(request) = get_properties_requests.get(get_properties_index) {
                    get_properties_responses.push(summarize_get_properties_probe_response(
                        responses,
                        response_offset,
                        request,
                    ));
                }
                get_properties_index = get_properties_index.saturating_add(1);
            }
            0x0A | 0x79 => {
                if let Some(request) = set_properties_requests.get(set_properties_index) {
                    set_properties_responses.push(summarize_set_properties_probe_response(
                        responses,
                        response_offset,
                        request,
                    ));
                }
                set_properties_index = set_properties_index.saturating_add(1);
            }
            _ => {}
        }
        response_offset = response_offset.saturating_add(6);
    }
    summary.open_folder_response_shapes = open_folder_responses.join("|");
    summary.get_properties_specific_response_shapes = get_properties_responses.join("|");
    summary.set_properties_response_shapes = set_properties_responses.join("|");
    summary
}

fn summarize_root_logon_default_folder_getprops_requests(
    rop_buffer: &[u8],
    session: &MapiSession,
) -> String {
    let Some((requests, handle_table)) = split_rop_buffer(rop_buffer) else {
        return String::new();
    };
    let Ok(input_handles) = read_handle_table(handle_table) else {
        return String::new();
    };
    let mut cursor = Cursor::new(requests);
    let mut opened_root_output_indexes = HashSet::new();
    let mut getprops = Vec::new();
    while cursor.remaining() > 0 {
        let Ok(request) = read_rop_request(&mut cursor) else {
            break;
        };
        match request.typed().rop_id() {
            0x02 if request.folder_id() == Some(ROOT_FOLDER_ID) => {
                if let Some(output_handle_index) = request.output_handle_index {
                    opened_root_output_indexes.insert(output_handle_index);
                }
            }
            0x07 if request
                .property_tags()
                .iter()
                .copied()
                .any(is_default_folder_identification_property_tag) =>
            {
                let input_handle_index = request.input_handle_index().unwrap_or(0);
                let target = if opened_root_output_indexes.contains(&input_handle_index) {
                    Some("root")
                } else {
                    match input_object(session, &input_handles, &request) {
                        Some(MapiObject::Logon) => Some("logon"),
                        Some(MapiObject::Folder { folder_id, .. })
                            if *folder_id == ROOT_FOLDER_ID =>
                        {
                            Some("root")
                        }
                        _ => None,
                    }
                };
                if let Some(target) = target {
                    getprops.push(format!(
                        "in={input_handle_index};target={target};tags={}",
                        format_debug_property_tags(&request.property_tags())
                    ));
                }
            }
            _ => {}
        }
    }
    getprops.join("|")
}

async fn folder_properties_for_open<S>(
    store: &S,
    principal: &AccountPrincipal,
    session: &MapiSession,
    folder_id: u64,
) -> HashMap<u32, MapiValue>
where
    S: ExchangeStore,
{
    let mut properties = HashMap::new();
    match store
        .fetch_mapi_folder_properties(principal.account_id, &[folder_id])
        .await
    {
        Ok(records) => {
            for record in records {
                if record.folder_id == folder_id {
                    let mut cursor = Cursor::new(&record.value_bytes);
                    match parse_mapi_property_value(&mut cursor, record.property_tag) {
                        Ok(value) => {
                            if folder_id == ROOT_FOLDER_ID
                                && is_default_folder_identification_property_tag(
                                    record.property_tag,
                                )
                            {
                                continue;
                            }
                            properties.insert(record.property_tag, value);
                        }
                        Err(error) => tracing::warn!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            account_id = %principal.account_id,
                            mailbox = %principal.email,
                            folder_id = %format!("{folder_id:#018x}"),
                            property_tag = %format!("{:#010x}", record.property_tag),
                            error = %error,
                            "rca debug mapi folder property hydrate parse failed"
                        ),
                    }
                }
            }
        }
        Err(error) => tracing::warn!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            account_id = %principal.account_id,
            mailbox = %principal.email,
            folder_id = %format!("{folder_id:#018x}"),
            error = %error,
            "rca debug mapi folder property hydrate failed"
        ),
    }
    if folder_id == ROOT_FOLDER_ID {
        properties.extend(session.root_default_folder_properties.clone());
    }
    properties
}

fn mapi_folder_property_values(
    folder_id: u64,
    values: &[(u32, MapiValue)],
) -> Vec<MapiFolderPropertyValue> {
    values
        .iter()
        .filter_map(|(tag, value)| {
            let property_tag = canonical_property_storage_tag(*tag);
            if folder_id == IPM_SUBTREE_FOLDER_ID && property_tag == PID_TAG_OST_OSTID {
                return None;
            }
            if folder_id == ROOT_FOLDER_ID
                && is_default_folder_identification_property_tag(property_tag)
            {
                return None;
            }
            let mut value_bytes = Vec::new();
            write_mapi_value(&mut value_bytes, property_tag, value);
            Some(MapiFolderPropertyValue {
                folder_id,
                property_tag,
                value_bytes,
            })
        })
        .collect()
}

fn mapi_folder_property_tags(property_tags: &[u32]) -> Vec<u32> {
    property_tags
        .iter()
        .flat_map(|tag| [*tag, canonical_property_storage_tag(*tag)])
        .collect()
}

fn remember_root_default_folder_properties(
    _session: &mut MapiSession,
    object: Option<&MapiObject>,
    _values: &[(u32, MapiValue)],
) {
    if !matches!(
        object,
        Some(MapiObject::Folder {
            folder_id: ROOT_FOLDER_ID,
            ..
        })
    ) {
        return;
    }
}

fn forget_root_default_folder_properties(
    session: &mut MapiSession,
    object: Option<&MapiObject>,
    property_tags: &[u32],
) {
    if !matches!(
        object,
        Some(MapiObject::Folder {
            folder_id: ROOT_FOLDER_ID,
            ..
        })
    ) {
        return;
    }
    for tag in property_tags {
        let storage_tag = canonical_property_storage_tag(*tag);
        if is_default_folder_identification_property_tag(storage_tag) {
            session.root_default_folder_properties.remove(&storage_tag);
        }
    }
}

fn set_properties_probe_request(request: &RopRequest) -> SetPropertiesProbeRequest {
    match request.property_values() {
        Ok(values) => SetPropertiesProbeRequest {
            input_handle_index: request.input_handle_index().unwrap_or(0),
            property_tags: values.iter().map(|(tag, _value)| *tag).collect(),
            property_value_shapes: values
                .iter()
                .map(|(tag, value)| format!("{tag:#010x}:{}", mapi_value_debug_shape(value)))
                .collect::<Vec<_>>()
                .join(","),
            default_folder_entry_id_values: default_folder_entry_id_values_for_debug(&values),
            parse_error: String::new(),
        },
        Err(error) => SetPropertiesProbeRequest {
            input_handle_index: request.input_handle_index().unwrap_or(0),
            property_tags: Vec::new(),
            property_value_shapes: String::new(),
            default_folder_entry_id_values: String::new(),
            parse_error: error.to_string(),
        },
    }
}

fn default_folder_entry_id_values_for_debug(values: &[(u32, MapiValue)]) -> String {
    values
        .iter()
        .filter_map(|(tag, value)| {
            let storage_tag = canonical_property_storage_tag(*tag);
            let expected_folder_id = default_folder_entry_id_expected_folder_id(storage_tag)?;
            let property_name = default_folder_entry_id_property_name(storage_tag);
            let MapiValue::Binary(bytes) = value else {
                return Some(format!(
                    "{storage_tag:#010x}:{property_name}:value_type={}",
                    mapi_value_debug_shape(value)
                ));
            };
            let decoded_folder_id =
                crate::mapi::identity::object_id_from_folder_identifier_bytes(bytes).unwrap_or(0);
            let decoded_name = if decoded_folder_id == 0 {
                "invalid"
            } else {
                post_hierarchy_probe_folder_name(decoded_folder_id)
            };
            Some(format!(
                "{storage_tag:#010x}:{property_name}:bytes={}:decoded_folder_id=0x{decoded_folder_id:016x}:decoded_name={decoded_name}:expected_folder_id=0x{expected_folder_id:016x}:matches_expected={}",
                bytes.len(),
                decoded_folder_id == expected_folder_id
            ))
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn default_folder_entry_id_expected_folder_id(tag: u32) -> Option<u64> {
    match canonical_property_storage_tag(tag) {
        PID_TAG_IPM_APPOINTMENT_ENTRY_ID => Some(CALENDAR_FOLDER_ID),
        PID_TAG_IPM_CONTACT_ENTRY_ID => Some(CONTACTS_FOLDER_ID),
        PID_TAG_IPM_JOURNAL_ENTRY_ID => Some(JOURNAL_FOLDER_ID),
        PID_TAG_IPM_NOTE_ENTRY_ID => Some(NOTES_FOLDER_ID),
        PID_TAG_IPM_TASK_ENTRY_ID => Some(TASKS_FOLDER_ID),
        PID_TAG_REM_ONLINE_ENTRY_ID => Some(REMINDERS_FOLDER_ID),
        _ => None,
    }
}

fn default_folder_entry_id_property_name(tag: u32) -> &'static str {
    match canonical_property_storage_tag(tag) {
        PID_TAG_IPM_APPOINTMENT_ENTRY_ID => "PidTagIpmAppointmentEntryId",
        PID_TAG_IPM_CONTACT_ENTRY_ID => "PidTagIpmContactEntryId",
        PID_TAG_IPM_JOURNAL_ENTRY_ID => "PidTagIpmJournalEntryId",
        PID_TAG_IPM_NOTE_ENTRY_ID => "PidTagIpmNoteEntryId",
        PID_TAG_IPM_TASK_ENTRY_ID => "PidTagIpmTaskEntryId",
        PID_TAG_REM_ONLINE_ENTRY_ID => "PidTagRemOnlineEntryId",
        _ => "unknown",
    }
}

fn summarize_open_folder_probe_response(
    responses: &[u8],
    offset: usize,
    request: &OpenFolderProbeRequest,
) -> String {
    let result = read_response_error_code(responses, offset)
        .map(|code| format!("{code:#010x}"))
        .unwrap_or_else(|| "truncated".to_string());
    let has_rules = responses
        .get(offset + 6)
        .map(|value| value.to_string())
        .unwrap_or_else(|| "truncated".to_string());
    let is_ghosted = responses
        .get(offset + 7)
        .map(|value| value.to_string())
        .unwrap_or_else(|| "truncated".to_string());
    format!(
        "out={};folder=0x{:016x};name={};result={result};has_rules={has_rules};is_ghosted={is_ghosted}",
        request.output_handle_index,
        request.folder_id,
        post_hierarchy_probe_folder_name(request.folder_id)
    )
}

fn summarize_get_properties_probe_response(
    responses: &[u8],
    offset: usize,
    request: &GetPropertiesSpecificProbeRequest,
) -> String {
    let result = read_response_error_code(responses, offset)
        .map(|code| format!("{code:#010x}"))
        .unwrap_or_else(|| "truncated".to_string());
    let row_shape = match responses.get(offset + 6).copied() {
        Some(0) => "standard",
        Some(1) => "flagged",
        Some(_) => "unknown",
        None => "truncated",
    };
    let values = summarize_get_properties_probe_response_values(responses, offset, request);
    format!(
        "in={};result={result};row={row_shape};tags={};values={values}",
        request.input_handle_index,
        format_debug_property_tags(&request.property_tags)
    )
}

fn summarize_get_properties_probe_response_values(
    responses: &[u8],
    offset: usize,
    request: &GetPropertiesSpecificProbeRequest,
) -> String {
    if responses.get(offset + 6).copied() != Some(0) {
        return "not-standard-row".to_string();
    }
    let mut cursor = Cursor::new(responses.get(offset + 7..).unwrap_or_default());
    let mut values = Vec::new();
    for tag in &request.property_tags {
        match parse_property_value_for_tag(&mut cursor, *tag) {
            Ok(value) => values.push(format!("{tag:#010x}:{}", mapi_value_debug_shape(&value))),
            Err(error) => {
                values.push(format!("{tag:#010x}:parse_error={error}"));
                break;
            }
        }
    }
    values.join(",")
}

fn summarize_set_properties_probe_response(
    responses: &[u8],
    offset: usize,
    request: &SetPropertiesProbeRequest,
) -> String {
    let result = read_response_error_code(responses, offset)
        .map(|code| format!("{code:#010x}"))
        .unwrap_or_else(|| "truncated".to_string());
    let property_problem_count = responses
        .get(offset + 6..offset + 8)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u16::from_le_bytes)
        .map(|count| count.to_string())
        .unwrap_or_else(|| "truncated".to_string());
    format!(
        "in={};result={result};property_problem_count={property_problem_count};tags={}",
        request.input_handle_index,
        format_debug_property_tags(&request.property_tags)
    )
}

fn mapi_value_debug_shape(value: &MapiValue) -> String {
    match value {
        MapiValue::Bool(_) => "bool".to_string(),
        MapiValue::I16(_) => "i16".to_string(),
        MapiValue::I32(_) => "i32".to_string(),
        MapiValue::I64(_) => "i64".to_string(),
        MapiValue::U32(_) => "u32".to_string(),
        MapiValue::U64(_) => "u64".to_string(),
        MapiValue::String(value) => format!("string:chars={}", value.chars().count()),
        MapiValue::Binary(value) => format!("binary:bytes={}", value.len()),
        MapiValue::Guid(_) => "guid".to_string(),
        MapiValue::Error(error) => format!("error:{error:#010x}"),
        MapiValue::MultiI16(value) => format!("multi_i16:count={}", value.len()),
        MapiValue::MultiI32(value) => format!("multi_i32:count={}", value.len()),
        MapiValue::MultiI64(value) => format!("multi_i64:count={}", value.len()),
        MapiValue::MultiString(value) => format!("multi_string:count={}", value.len()),
        MapiValue::MultiBinary(value) => format!("multi_binary:count={}", value.len()),
        MapiValue::MultiGuid(value) => format!("multi_guid:count={}", value.len()),
    }
}

fn post_hierarchy_probe_folder_name(folder_id: u64) -> &'static str {
    match folder_id {
        ROOT_FOLDER_ID => "root",
        IPM_SUBTREE_FOLDER_ID => "ipm_subtree",
        INBOX_FOLDER_ID => "inbox",
        DRAFTS_FOLDER_ID => "drafts",
        SENT_FOLDER_ID => "sent",
        TRASH_FOLDER_ID => "trash",
        OUTBOX_FOLDER_ID => "outbox",
        CALENDAR_FOLDER_ID => "calendar",
        CONTACTS_FOLDER_ID => "contacts",
        JOURNAL_FOLDER_ID => "journal",
        NOTES_FOLDER_ID => "notes",
        TASKS_FOLDER_ID => "tasks",
        REMINDERS_FOLDER_ID => "reminders",
        _ => "other",
    }
}

fn format_debug_property_tags(tags: &[u32]) -> String {
    tags.iter()
        .map(|tag| format!("{tag:#010x}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn summarize_request_rop_buffer(rop_buffer: &[u8]) -> RopRequestDebugSummary {
    let mut summary = RopRequestDebugSummary {
        extended: is_rpc_header_ext_rop_buffer(rop_buffer),
        ..RopRequestDebugSummary::default()
    };
    let Some((requests, handle_table)) = split_rop_buffer(rop_buffer) else {
        summary.parse_error = "invalid ROP buffer".to_string();
        return summary;
    };
    let handle_summary = summarize_handle_table(handle_table, &mut summary.parse_error);
    summary.handle_count = handle_summary.0;
    summary.handle_table_summary = handle_summary.1;

    let mut cursor = Cursor::new(requests);
    while cursor.remaining() > 0 && summary.ids.len() < MAX_ROP_DEBUG_ENTRIES {
        match read_rop_request(&mut cursor) {
            Ok(request) => summary.ids.push(request.typed().rop_id()),
            Err(error) => {
                summary.parse_error = error.to_string();
                break;
            }
        }
    }
    summary.ids_csv = rop_ids_csv(&summary.ids);
    summary
}

fn summarize_response_rop_buffer(
    rop_buffer: &[u8],
    request_rop_ids: &[u8],
) -> RopResponseDebugSummary {
    let mut summary = RopResponseDebugSummary {
        extended: is_rpc_header_ext_rop_buffer(rop_buffer),
        ..RopResponseDebugSummary::default()
    };
    let Some((responses, handle_table)) = split_rop_buffer(rop_buffer) else {
        summary.parse_error = "invalid ROP buffer".to_string();
        return summary;
    };
    let handle_summary = summarize_handle_table(handle_table, &mut summary.parse_error);
    summary.handle_count = handle_summary.0;
    summary.handle_table_summary = handle_summary.1;

    let mut offset = 0usize;
    let mut ids = Vec::new();
    let mut results = Vec::new();
    for expected_rop_id in request_rop_ids.iter().copied().take(MAX_ROP_DEBUG_ENTRIES) {
        if rop_has_no_response(expected_rop_id) {
            continue;
        }
        let Some(found) = responses.get(offset..).and_then(|remaining| {
            remaining
                .iter()
                .position(|rop_id| *rop_id == expected_rop_id)
        }) else {
            break;
        };
        offset += found;
        let rop_id = responses[offset];
        ids.push(rop_id);
        if let Some(error_code) = read_response_error_code(responses, offset) {
            results.push(format!("{}:{error_code:#010x}", rop_id_hex(rop_id)));
        } else {
            results.push(format!("{}:truncated", rop_id_hex(rop_id)));
        }
        offset = offset.saturating_add(6);
    }

    summary.count = ids.len();
    summary.ids_csv = rop_ids_csv(&ids);
    summary.results_csv = results.join(",");
    summary
}

fn rop_has_no_response(rop_id: u8) -> bool {
    matches!(rop_id, 0x01)
}

fn summarize_logon_response_rop(
    rop_buffer: &[u8],
    request_rop_ids: &[u8],
) -> LogonResponseDebugSummary {
    if !request_rop_ids.contains(&0xFE) {
        return LogonResponseDebugSummary::default();
    }
    let mut summary = LogonResponseDebugSummary {
        present: true,
        ..LogonResponseDebugSummary::default()
    };
    let Some((responses, _handle_table)) = split_rop_buffer(rop_buffer) else {
        summary.parse_error = "invalid ROP buffer".to_string();
        return summary;
    };
    let Some(offset) = responses.iter().position(|rop_id| *rop_id == 0xFE) else {
        summary.parse_error = "missing RopLogon response".to_string();
        return summary;
    };
    let result = (|| -> Result<()> {
        let mut cursor = Cursor::new(&responses[offset..]);
        let rop_id = cursor.read_u8()?;
        if rop_id != 0xFE {
            return Err(anyhow::anyhow!("unexpected ROP response"));
        }
        summary.output_handle_index = cursor.read_u8()?.to_string();
        let error_code = cursor.read_u32()?;
        summary.error_code = format!("{error_code:#010x}");
        if error_code != 0 {
            return Ok(());
        }
        summary.logon_flags = format!("{:#04x}", cursor.read_u8()?);
        let mut folder_ids = Vec::with_capacity(PRIVATE_LOGON_SPECIAL_FOLDER_IDS.len());
        for _ in PRIVATE_LOGON_SPECIAL_FOLDER_IDS {
            folder_ids.push(format!("{:#018x}", read_u64(&mut cursor)?));
        }
        summary.special_folder_ids = folder_ids.join(",");
        summary.response_flags = format!("{:#04x}", cursor.read_u8()?);
        summary.mailbox_guid = read_guid_le(&mut cursor)?;
        summary.replid = cursor.read_u16()?.to_string();
        summary.replica_guid = bytes_to_hex(cursor.read_bytes(16)?);
        cursor.read_bytes(8)?;
        read_u64(&mut cursor)?;
        cursor.read_u32()?;
        Ok(())
    })();
    if let Err(error) = result {
        summary.parse_error = error.to_string();
    }
    summary
}

fn read_u64(cursor: &mut Cursor<'_>) -> Result<u64> {
    let bytes = cursor.read_bytes(8)?;
    Ok(u64::from_le_bytes(bytes.try_into()?))
}

fn read_guid_le(cursor: &mut Cursor<'_>) -> Result<String> {
    let bytes = cursor.read_bytes(16)?;
    Ok(Uuid::from_bytes_le(bytes.try_into()?).to_string())
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join("")
}

fn summarize_handle_table(handle_table: &[u8], parse_error: &mut String) -> (usize, String) {
    match read_handle_table(handle_table) {
        Ok(handles) => {
            let handles_csv = handles
                .iter()
                .map(|handle| format!("0x{handle:08x}"))
                .collect::<Vec<_>>()
                .join(",");
            (
                handles.len(),
                format!("count={};handles={handles_csv}", handles.len()),
            )
        }
        Err(error) => {
            *parse_error = error.to_string();
            let count = handle_table.len() / 4;
            (
                count,
                format!(
                    "invalid;bytes={};best_effort_count={count}",
                    handle_table.len()
                ),
            )
        }
    }
}

fn read_response_error_code(responses: &[u8], offset: usize) -> Option<u32> {
    let bytes = responses.get(offset + 2..offset + 6)?;
    Some(u32::from_le_bytes(bytes.try_into().ok()?))
}

fn rop_ids_csv(rop_ids: &[u8]) -> String {
    rop_ids
        .iter()
        .map(|rop_id| rop_id_hex(*rop_id))
        .collect::<Vec<_>>()
        .join(",")
}

fn rop_id_hex(rop_id: u8) -> String {
    format!("0x{rop_id:02x}")
}

pub(in crate::mapi) async fn execute_rops<S, V>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    validator: &Validator<V>,
    rop_buffer: &[u8],
) -> Vec<u8>
where
    S: ExchangeStore,
    V: Detector,
{
    let Some((requests, handle_table)) = split_rop_buffer(rop_buffer) else {
        return rop_buffer_with_response(rop_parse_error_response(), &[]);
    };
    let extended = is_rpc_header_ext_rop_buffer(rop_buffer);
    let mut handle_slots = match read_handle_table(handle_table) {
        Ok(handle_slots) => handle_slots,
        Err(_) => {
            let response = if extended {
                rop_buffer_with_response_spec(rop_parse_error_response(), &[])
            } else {
                rop_buffer_with_response(rop_parse_error_response(), &[])
            };
            return if extended {
                rpc_header_ext_rop_buffer(response)
            } else {
                response
            };
        }
    };
    if let Some(max_input_handle) = handle_slots
        .iter()
        .copied()
        .filter(|handle| *handle != u32::MAX)
        .max()
    {
        session.next_handle = session.next_handle.max(max_input_handle.saturating_add(1));
    }

    let mut cursor = Cursor::new(requests);
    let mut responses = Vec::new();
    let mut output_handles = Vec::new();
    let mut echo_input_handle_table = false;
    while cursor.remaining() > 0 {
        let request = match read_rop_request(&mut cursor) {
            Ok(request) => request,
            Err(_) => {
                responses.extend_from_slice(&rop_parse_error_response());
                break;
            }
        };
        let typed_request = request.typed();
        let mut completed_hierarchy_sync_root = None;
        let mut content_sync_configure_observed = false;
        match RopId::from_u8(typed_request.rop_id()) {
            Some(RopId::Release) => {
                if matches!(
                    input_object(session, &handle_slots, &request),
                    Some(MapiObject::Logon)
                ) {
                    session.record_logoff_after_hierarchy_completion();
                }
                release_handle_slot(session, &mut handle_slots, &request);
            }
            Some(RopId::OpenFolder) => {
                let folder_id = request.folder_id().unwrap_or(ROOT_FOLDER_ID);
                if folder_row_for_id(folder_id, mailboxes).is_none()
                    && snapshot.collaboration_folder_for_id(folder_id).is_none()
                    && !is_advertised_special_folder(folder_id)
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x02,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                }
                let properties =
                    folder_properties_for_open(store, principal, session, folder_id).await;
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::Folder {
                        folder_id,
                        properties,
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_open_folder_response(&request));
                output_handles.push(handle);
            }
            Some(RopId::OpenMessage) => {
                let folder_id = request.folder_id().unwrap_or(INBOX_FOLDER_ID);
                let message_id = request.message_id().unwrap_or(0);
                if let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails) {
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::Message {
                            folder_id,
                            message_id,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(&rop_open_message_response(
                        &request,
                        &email.subject,
                        message_recipients(email).len(),
                    ));
                    output_handles.push(handle);
                } else if let Some(message) = snapshot.todo_search_message_for_id(message_id) {
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::Message {
                            folder_id: TODO_SEARCH_FOLDER_ID,
                            message_id,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(&rop_open_message_response(
                        &request,
                        &message.email.subject,
                        message_recipients(&message.email).len(),
                    ));
                    output_handles.push(handle);
                } else if let Some(message) =
                    snapshot.tracked_mail_processing_message_for_id(message_id)
                {
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::Message {
                            folder_id: TRACKED_MAIL_PROCESSING_FOLDER_ID,
                            message_id,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(&rop_open_message_response(
                        &request,
                        &message.email.subject,
                        message_recipients(&message.email).len(),
                    ));
                    output_handles.push(handle);
                } else if let Some(contact) = snapshot.contact_for_id(folder_id, message_id) {
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::Contact {
                            folder_id,
                            contact_id: message_id,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(&rop_open_message_response(
                        &request,
                        &contact.contact.name,
                        0,
                    ));
                    output_handles.push(handle);
                } else if let Some(event) = snapshot.event_for_id(folder_id, message_id) {
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::Event {
                            folder_id,
                            event_id: message_id,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(&rop_open_message_response(
                        &request,
                        &event.event.title,
                        0,
                    ));
                    output_handles.push(handle);
                } else if let Some(task) = snapshot.task_for_id(folder_id, message_id) {
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::Task {
                            folder_id,
                            task_id: message_id,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(&rop_open_message_response(
                        &request,
                        &task.task.title,
                        0,
                    ));
                    output_handles.push(handle);
                } else if let Some(note) = snapshot.note_for_id(folder_id, message_id) {
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::Note {
                            folder_id,
                            note_id: message_id,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(&rop_open_message_response(
                        &request,
                        &note.note.title,
                        0,
                    ));
                    output_handles.push(handle);
                } else if let Some(entry) = snapshot.journal_entry_for_id(folder_id, message_id) {
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::JournalEntry {
                            folder_id,
                            journal_entry_id: message_id,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(&rop_open_message_response(
                        &request,
                        &entry.entry.subject,
                        0,
                    ));
                    output_handles.push(handle);
                } else if folder_id == COMMON_VIEWS_FOLDER_ID {
                    if let Some(message) =
                        snapshot.search_folder_definition_message_for_id(message_id)
                    {
                        let handle = session.allocate_output_handle(
                            request.output_handle_index,
                            MapiObject::SearchFolderDefinition {
                                folder_id,
                                definition_id: message_id,
                            },
                        );
                        set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                        responses.extend_from_slice(&rop_open_message_response(
                            &request,
                            &message.definition.display_name,
                            0,
                        ));
                        output_handles.push(handle);
                    } else {
                        responses.extend_from_slice(&rop_error_response(
                            0x03,
                            request.output_handle_index.unwrap_or(0),
                            0x8004_010F,
                        ));
                    }
                } else {
                    responses.extend_from_slice(&rop_error_response(
                        0x03,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                }
            }
            Some(RopId::GetHierarchyTable) => {
                if input_handle(&handle_slots, &request).is_none() {
                    responses.extend_from_slice(&rop_handle_index_error_response(&request));
                    continue;
                }
                let folder_id = input_object(session, &handle_slots, &request)
                    .and_then(|object| object.folder_id())
                    .unwrap_or(ROOT_FOLDER_ID);
                let columns = default_hierarchy_columns();
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::HierarchyTable {
                        folder_id,
                        columns,
                        sort_orders: Vec::new(),
                        restriction: None,
                        bookmarks: HashMap::new(),
                        next_bookmark: 1,
                        position: 0,
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_get_hierarchy_table_response(
                    &request,
                    hierarchy_row_count(folder_id, mailboxes, snapshot),
                ));
                output_handles.push(handle);
            }
            Some(RopId::GetContentsTable) => {
                if input_handle(&handle_slots, &request).is_none() {
                    responses.extend_from_slice(&rop_handle_index_error_response(&request));
                    continue;
                }
                let folder_id = input_object(session, &handle_slots, &request)
                    .and_then(|object| object.folder_id())
                    .unwrap_or(INBOX_FOLDER_ID);
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
                    continue;
                }
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::ContentsTable {
                        folder_id,
                        associated: request
                            .payload
                            .first()
                            .is_some_and(|flags| flags & 0x02 != 0),
                        columns: Vec::new(),
                        sort_orders: Vec::new(),
                        restriction: None,
                        bookmarks: HashMap::new(),
                        next_bookmark: 1,
                        position: 0,
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_get_contents_table_response(
                    &request,
                    if request
                        .payload
                        .first()
                        .is_some_and(|flags| flags & 0x02 != 0)
                    {
                        associated_folder_message_count(folder_id, snapshot)
                    } else {
                        folder_message_count(folder_id, mailboxes, emails, snapshot)
                    },
                ));
                output_handles.push(handle);
            }
            Some(RopId::CreateMessage) => {
                let folder_id = request.folder_id().unwrap_or_else(|| {
                    input_object(session, &handle_slots, &request)
                        .and_then(MapiObject::folder_id)
                        .unwrap_or(INBOX_FOLDER_ID)
                });
                if !snapshot
                    .folder_access_for_principal(folder_id, principal.account_id)
                    .map(|access| access.may_write)
                    .unwrap_or(true)
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x06,
                        request.output_handle_index.unwrap_or(0),
                        0x8007_0005,
                    ));
                    continue;
                }
                if snapshot.collaboration_folder_for_id(folder_id).is_none()
                    && folder_row_for_id(folder_id, mailboxes).is_none()
                    && !matches!(
                        folder_id,
                        INBOX_FOLDER_ID
                            | DRAFTS_FOLDER_ID
                            | SENT_FOLDER_ID
                            | TRASH_FOLDER_ID
                            | OUTBOX_FOLDER_ID
                            | NOTES_FOLDER_ID
                            | JOURNAL_FOLDER_ID
                    )
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x06,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                }

                let pending_object = match snapshot
                    .collaboration_folder_for_id(folder_id)
                    .map(|folder| folder.kind)
                {
                    Some(MapiCollaborationFolderKind::Contacts) => MapiObject::PendingContact {
                        folder_id,
                        properties: HashMap::new(),
                    },
                    Some(MapiCollaborationFolderKind::Calendar) => MapiObject::PendingEvent {
                        folder_id,
                        properties: HashMap::new(),
                    },
                    Some(MapiCollaborationFolderKind::Task) => MapiObject::PendingTask {
                        folder_id,
                        properties: HashMap::new(),
                    },
                    _ if folder_id == NOTES_FOLDER_ID => MapiObject::PendingNote {
                        folder_id,
                        properties: HashMap::new(),
                    },
                    _ if folder_id == JOURNAL_FOLDER_ID => MapiObject::PendingJournalEntry {
                        folder_id,
                        properties: HashMap::new(),
                    },
                    _ => MapiObject::PendingMessage {
                        folder_id,
                        properties: HashMap::new(),
                        recipients: Vec::new(),
                    },
                };
                let handle =
                    session.allocate_output_handle(request.output_handle_index, pending_object);
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_create_message_response(&request));
                output_handles.push(handle);
            }
            Some(RopId::GetPropertiesSpecific) => {
                echo_input_handle_table = true;
                responses.extend_from_slice(&rop_get_properties_specific_response(
                    &request,
                    input_object(session, &handle_slots, &request),
                    principal,
                    mailboxes,
                    emails,
                    snapshot,
                ));
            }
            Some(RopId::GetPropertiesAll) => {
                responses.extend_from_slice(&rop_get_properties_all_response(
                    &request,
                    input_object(session, &handle_slots, &request),
                    principal,
                    mailboxes,
                    emails,
                    snapshot,
                ))
            }
            Some(RopId::GetPropertiesList) => {
                responses.extend_from_slice(&rop_get_properties_list_response(
                    &request,
                    input_object(session, &handle_slots, &request),
                ))
            }
            Some(RopId::SetProperties | RopId::SetPropertiesNoReplicate) => {
                echo_input_handle_table = true;
                let set_result = match request.property_values() {
                    Ok(values) => match input_object(session, &handle_slots, &request).cloned() {
                        Some(MapiObject::Message {
                            folder_id,
                            message_id,
                        }) => {
                            apply_canonical_message_property_values(
                                store, principal, folder_id, message_id, values, mailboxes, emails,
                            )
                            .await
                        }
                        Some(MapiObject::Contact {
                            folder_id,
                            contact_id,
                        }) => {
                            apply_canonical_contact_property_values(
                                store, principal, folder_id, contact_id, values, snapshot,
                            )
                            .await
                        }
                        Some(MapiObject::Event {
                            folder_id,
                            event_id,
                        }) => {
                            apply_canonical_event_property_values(
                                store, principal, folder_id, event_id, values, snapshot,
                            )
                            .await
                        }
                        Some(MapiObject::Task { folder_id, task_id }) => {
                            apply_canonical_task_property_values(
                                store, principal, folder_id, task_id, values, snapshot,
                            )
                            .await
                        }
                        Some(MapiObject::Note { folder_id, note_id }) => {
                            apply_canonical_note_property_values(
                                store, principal, folder_id, note_id, values, snapshot,
                            )
                            .await
                        }
                        Some(MapiObject::JournalEntry {
                            folder_id,
                            journal_entry_id,
                        }) => {
                            apply_canonical_journal_entry_property_values(
                                store,
                                principal,
                                folder_id,
                                journal_entry_id,
                                values,
                                snapshot,
                            )
                            .await
                        }
                        Some(MapiObject::Folder { folder_id, .. }) => {
                            let stored_values = mapi_folder_property_values(folder_id, &values);
                            if let Err(error) = store
                                .store_mapi_folder_properties(principal.account_id, &stored_values)
                                .await
                            {
                                tracing::warn!(
                                    rca_debug = true,
                                    adapter = "mapi",
                                    endpoint = "emsmdb",
                                    account_id = %principal.account_id,
                                    mailbox = %principal.email,
                                    folder_id = %format!("{folder_id:#018x}"),
                                    property_tags = %format_debug_property_tags(
                                        &stored_values
                                            .iter()
                                            .map(|value| value.property_tag)
                                            .collect::<Vec<_>>()
                                    ),
                                    error = %error,
                                    "rca debug mapi folder property persist failed"
                                );
                            }
                            let object = input_object(session, &handle_slots, &request).cloned();
                            remember_root_default_folder_properties(
                                session,
                                object.as_ref(),
                                &values,
                            );
                            apply_mapi_property_values(
                                input_object_mut(session, &handle_slots, &request),
                                values,
                            )
                        }
                        object => {
                            remember_root_default_folder_properties(
                                session,
                                object.as_ref(),
                                &values,
                            );
                            apply_mapi_property_values(
                                input_object_mut(session, &handle_slots, &request),
                                values,
                            )
                        }
                    },
                    Err(error) => Err(error),
                };
                match set_result {
                    Ok(()) => responses.extend_from_slice(&rop_set_properties_response(&request)),
                    Err(_) => responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            Some(RopId::DeleteProperties | RopId::DeletePropertiesNoReplicate) => {
                let property_tags = request.property_tags();
                let delete_result = match input_object(session, &handle_slots, &request).cloned() {
                    Some(MapiObject::Folder { folder_id, .. }) => {
                        let stored_tags = mapi_folder_property_tags(&property_tags);
                        if let Err(error) = store
                            .delete_mapi_folder_properties(
                                principal.account_id,
                                folder_id,
                                &stored_tags,
                            )
                            .await
                        {
                            tracing::warn!(
                                rca_debug = true,
                                adapter = "mapi",
                                endpoint = "emsmdb",
                                account_id = %principal.account_id,
                                mailbox = %principal.email,
                                folder_id = %format!("{folder_id:#018x}"),
                                property_tags = %format_debug_property_tags(&stored_tags),
                                error = %error,
                                "rca debug mapi folder property delete failed"
                            );
                        }
                        let object = input_object(session, &handle_slots, &request).cloned();
                        forget_root_default_folder_properties(
                            session,
                            object.as_ref(),
                            &property_tags,
                        );
                        delete_mapi_properties(
                            input_object_mut(session, &handle_slots, &request),
                            &property_tags,
                        )
                    }
                    _ => delete_mapi_properties(
                        input_object_mut(session, &handle_slots, &request),
                        &property_tags,
                    ),
                };
                match delete_result {
                    Ok(()) => {
                        responses.extend_from_slice(&rop_delete_properties_response(&request))
                    }
                    Err(_) => responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            Some(RopId::SaveChangesMessage) => {
                let Some(handle) = input_handle(&handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x0C,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                match session.handles.get(&handle).cloned() {
                    Some(MapiObject::PendingContact {
                        folder_id,
                        properties,
                    }) => {
                        let Some(folder) = snapshot.collaboration_folder_for_id(folder_id) else {
                            responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            ));
                            continue;
                        };
                        let input = contact_input_from_mapi(
                            principal.account_id,
                            None,
                            &default_contact_for_mapping(
                                principal.account_id,
                                &folder.collection.id,
                            ),
                            &properties,
                        );
                        match store
                            .create_accessible_contact(
                                principal.account_id,
                                Some(&folder.collection.id),
                                input,
                            )
                            .await
                        {
                            Ok(contact) => {
                                let contact_id = match remember_created_mapi_identity(
                                    store,
                                    principal,
                                    MapiIdentityObjectKind::Contact,
                                    contact.id,
                                    None,
                                )
                                .await
                                {
                                    Ok(contact_id) => contact_id,
                                    Err(_) => {
                                        responses.extend_from_slice(&rop_error_response(
                                            0x0C,
                                            request.response_handle_index(),
                                            0x8004_010F,
                                        ));
                                        continue;
                                    }
                                };
                                session.handles.insert(
                                    handle,
                                    MapiObject::Contact {
                                        folder_id,
                                        contact_id,
                                    },
                                );
                                session.record_notification(MapiNotificationEvent {
                                    folder_id,
                                    kind: MapiNotificationKind::Content,
                                });
                                responses.extend_from_slice(&rop_save_changes_message_response(
                                    &request, contact_id,
                                ));
                            }
                            Err(_) => responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            )),
                        }
                        continue;
                    }
                    Some(MapiObject::PendingEvent {
                        folder_id,
                        properties,
                    }) => {
                        let Some(folder) = snapshot.collaboration_folder_for_id(folder_id) else {
                            responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            ));
                            continue;
                        };
                        let input = match event_input_from_mapi(
                            principal.account_id,
                            None,
                            &default_event_for_mapping(principal.account_id, &folder.collection.id),
                            &properties,
                        ) {
                            Ok(input) => input,
                            Err(_) => {
                                responses.extend_from_slice(&rop_error_response(
                                    0x0C,
                                    request.response_handle_index(),
                                    0x8004_0102,
                                ));
                                continue;
                            }
                        };
                        match store
                            .create_accessible_event(
                                principal.account_id,
                                Some(&folder.collection.id),
                                input,
                            )
                            .await
                        {
                            Ok(event) => {
                                let event_id = match remember_created_mapi_identity(
                                    store,
                                    principal,
                                    MapiIdentityObjectKind::CalendarEvent,
                                    event.id,
                                    None,
                                )
                                .await
                                {
                                    Ok(event_id) => event_id,
                                    Err(_) => {
                                        responses.extend_from_slice(&rop_error_response(
                                            0x0C,
                                            request.response_handle_index(),
                                            0x8004_010F,
                                        ));
                                        continue;
                                    }
                                };
                                session.handles.insert(
                                    handle,
                                    MapiObject::Event {
                                        folder_id,
                                        event_id,
                                    },
                                );
                                session.record_notification(MapiNotificationEvent {
                                    folder_id,
                                    kind: MapiNotificationKind::Content,
                                });
                                responses.extend_from_slice(&rop_save_changes_message_response(
                                    &request, event_id,
                                ));
                            }
                            Err(_) => responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            )),
                        }
                        continue;
                    }
                    Some(MapiObject::PendingTask {
                        folder_id,
                        properties,
                    }) => {
                        let Some(folder) = snapshot.collaboration_folder_for_id(folder_id) else {
                            responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            ));
                            continue;
                        };
                        let input = task_input_from_mapi(
                            principal.account_id,
                            None,
                            &default_task_for_mapping(principal.account_id, &folder.collection.id),
                            Some(&folder.collection.id),
                            &properties,
                        );
                        match store
                            .create_accessible_task(principal.account_id, input)
                            .await
                        {
                            Ok(task) => {
                                let task_id = match remember_created_mapi_identity(
                                    store,
                                    principal,
                                    MapiIdentityObjectKind::Task,
                                    task.id,
                                    None,
                                )
                                .await
                                {
                                    Ok(task_id) => task_id,
                                    Err(_) => {
                                        responses.extend_from_slice(&rop_error_response(
                                            0x0C,
                                            request.response_handle_index(),
                                            0x8004_010F,
                                        ));
                                        continue;
                                    }
                                };
                                session
                                    .handles
                                    .insert(handle, MapiObject::Task { folder_id, task_id });
                                session.record_notification(MapiNotificationEvent {
                                    folder_id,
                                    kind: MapiNotificationKind::Content,
                                });
                                responses.extend_from_slice(&rop_save_changes_message_response(
                                    &request, task_id,
                                ));
                            }
                            Err(_) => responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            )),
                        }
                        continue;
                    }
                    Some(MapiObject::PendingNote {
                        folder_id,
                        properties,
                    }) => {
                        let input = note_input_from_mapi(
                            principal.account_id,
                            None,
                            &default_note_for_mapping(),
                            &properties,
                        );
                        match store.upsert_mapi_note(input).await {
                            Ok(note) => {
                                let note_id = match remember_created_mapi_identity(
                                    store,
                                    principal,
                                    MapiIdentityObjectKind::Note,
                                    note.id,
                                    None,
                                )
                                .await
                                {
                                    Ok(note_id) => note_id,
                                    Err(_) => {
                                        responses.extend_from_slice(&rop_error_response(
                                            0x0C,
                                            request.response_handle_index(),
                                            0x8004_010F,
                                        ));
                                        continue;
                                    }
                                };
                                session
                                    .handles
                                    .insert(handle, MapiObject::Note { folder_id, note_id });
                                session.record_notification(MapiNotificationEvent {
                                    folder_id,
                                    kind: MapiNotificationKind::Content,
                                });
                                responses.extend_from_slice(&rop_save_changes_message_response(
                                    &request, note_id,
                                ));
                            }
                            Err(_) => responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            )),
                        }
                        continue;
                    }
                    Some(MapiObject::PendingJournalEntry {
                        folder_id,
                        properties,
                    }) => {
                        let input = journal_entry_input_from_mapi(
                            principal.account_id,
                            None,
                            &default_journal_entry_for_mapping(),
                            &properties,
                        );
                        match store.upsert_mapi_journal_entry(input).await {
                            Ok(entry) => {
                                let journal_entry_id = match remember_created_mapi_identity(
                                    store,
                                    principal,
                                    MapiIdentityObjectKind::JournalEntry,
                                    entry.id,
                                    None,
                                )
                                .await
                                {
                                    Ok(journal_entry_id) => journal_entry_id,
                                    Err(_) => {
                                        responses.extend_from_slice(&rop_error_response(
                                            0x0C,
                                            request.response_handle_index(),
                                            0x8004_010F,
                                        ));
                                        continue;
                                    }
                                };
                                session.handles.insert(
                                    handle,
                                    MapiObject::JournalEntry {
                                        folder_id,
                                        journal_entry_id,
                                    },
                                );
                                session.record_notification(MapiNotificationEvent {
                                    folder_id,
                                    kind: MapiNotificationKind::Content,
                                });
                                responses.extend_from_slice(&rop_save_changes_message_response(
                                    &request,
                                    journal_entry_id,
                                ));
                            }
                            Err(_) => responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            )),
                        }
                        continue;
                    }
                    Some(MapiObject::Contact { contact_id, .. })
                    | Some(MapiObject::Event {
                        event_id: contact_id,
                        ..
                    })
                    | Some(MapiObject::Task {
                        task_id: contact_id,
                        ..
                    })
                    | Some(MapiObject::Note {
                        note_id: contact_id,
                        ..
                    })
                    | Some(MapiObject::JournalEntry {
                        journal_entry_id: contact_id,
                        ..
                    }) => {
                        responses.extend_from_slice(&rop_save_changes_message_response(
                            &request, contact_id,
                        ));
                        continue;
                    }
                    _ => {}
                }
                let Some(MapiObject::PendingMessage {
                    folder_id,
                    properties,
                    recipients,
                }) = session.handles.get(&handle).cloned()
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x0C,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                };
                let Some(mailbox) = folder_row_for_id(folder_id, mailboxes) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x0C,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let input =
                    jmap_import_from_pending_message(principal, mailbox, &properties, &recipients);
                match store
                    .import_jmap_email(
                        input,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "mapi-save-message".to_string(),
                            subject: format!("folder:{}", mailbox.id),
                        },
                    )
                    .await
                {
                    Ok(email) => {
                        let message_id = match remember_created_mapi_identity(
                            store,
                            principal,
                            MapiIdentityObjectKind::Message,
                            email.id,
                            None,
                        )
                        .await
                        {
                            Ok(message_id) => message_id,
                            Err(_) => {
                                responses.extend_from_slice(&rop_error_response(
                                    0x0C,
                                    request.response_handle_index(),
                                    0x8004_010F,
                                ));
                                continue;
                            }
                        };
                        session.handles.insert(
                            handle,
                            MapiObject::Message {
                                folder_id,
                                message_id,
                            },
                        );
                        session.record_notification(MapiNotificationEvent {
                            folder_id,
                            kind: MapiNotificationKind::Content,
                        });
                        responses.extend_from_slice(&rop_save_changes_message_response(
                            &request, message_id,
                        ));
                    }
                    Err(_) => responses.extend_from_slice(&rop_error_response(
                        0x0C,
                        request.response_handle_index(),
                        0x8004_010F,
                    )),
                }
            }
            Some(RopId::RemoveAllRecipients) => {
                match input_object_mut(session, &handle_slots, &request) {
                    Some(MapiObject::PendingMessage { recipients, .. }) => {
                        recipients.clear();
                        responses.extend_from_slice(&rop_simple_success_response(&request));
                    }
                    _ => responses.extend_from_slice(&rop_error_response(
                        0x0D,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            Some(RopId::ModifyRecipients) => {
                match input_object_mut(session, &handle_slots, &request) {
                    Some(MapiObject::PendingMessage { recipients, .. }) => {
                        match request.modify_recipients() {
                            Ok(changes) => {
                                apply_pending_recipient_changes(recipients, changes);
                                responses.extend_from_slice(&rop_simple_success_response(&request));
                            }
                            Err(_) => responses.extend_from_slice(&rop_error_response(
                                0x0E,
                                request.response_handle_index(),
                                0x8004_0102,
                            )),
                        }
                    }
                    _ => responses.extend_from_slice(&rop_error_response(
                        0x0E,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            Some(RopId::ReadRecipients) => {
                responses.extend_from_slice(&rop_read_recipients_response(
                    &request,
                    input_object(session, &handle_slots, &request),
                    mailboxes,
                    emails,
                    snapshot,
                ))
            }
            Some(RopId::ReloadCachedInformation) => {
                responses.extend_from_slice(&rop_reload_cached_information_response(
                    &request,
                    input_object(session, &handle_slots, &request),
                    mailboxes,
                    emails,
                    snapshot,
                ))
            }
            Some(RopId::SetMessageReadFlag) => {
                let Some(MapiObject::Message {
                    folder_id,
                    message_id,
                }) = input_object(session, &handle_slots, &request)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x11,
                        request.response_handle_index(),
                        0x0000_04B9,
                    ));
                    continue;
                };
                let Some(email) = message_for_id(*folder_id, *message_id, mailboxes, emails) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x11,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let unread = unread_from_read_flags(request.read_flags());
                let changed = unread.is_some_and(|unread| unread != email.unread);
                if let Some(unread) = unread {
                    if !snapshot
                        .folder_access_for_principal(*folder_id, principal.account_id)
                        .map(|access| access.may_write)
                        .unwrap_or(true)
                    {
                        responses.extend_from_slice(&rop_error_response(
                            0x11,
                            request.response_handle_index(),
                            0x8007_0005,
                        ));
                        continue;
                    }
                    if store
                        .update_jmap_email_flags(
                            principal.account_id,
                            email.id,
                            Some(unread),
                            None,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "mapi-set-message-read-flag".to_string(),
                                subject: format!("message:{}", email.id),
                            },
                        )
                        .await
                        .is_err()
                    {
                        responses.extend_from_slice(&rop_error_response(
                            0x11,
                            request.response_handle_index(),
                            0x8004_010F,
                        ));
                        continue;
                    }
                }
                if changed {
                    session.record_notification(MapiNotificationEvent {
                        folder_id: *folder_id,
                        kind: MapiNotificationKind::Content,
                    });
                }
                responses.extend_from_slice(&rop_set_message_read_flag_response(&request, changed));
            }
            Some(RopId::SetColumns) => match input_object_mut(session, &handle_slots, &request) {
                Some(MapiObject::HierarchyTable { columns, .. })
                | Some(MapiObject::ContentsTable { columns, .. })
                | Some(MapiObject::AttachmentTable { columns, .. })
                | Some(MapiObject::PermissionTable { columns, .. }) => {
                    *columns = request.property_tags();
                    responses.extend_from_slice(&rop_set_columns_response(&request));
                }
                _ => responses.extend_from_slice(&rop_error_response(
                    0x12,
                    request.response_handle_index(),
                    0x8004_0102,
                )),
            },
            Some(RopId::SortTable) => match input_object_mut(session, &handle_slots, &request) {
                Some(MapiObject::HierarchyTable {
                    sort_orders,
                    position,
                    bookmarks,
                    ..
                })
                | Some(MapiObject::ContentsTable {
                    sort_orders,
                    position,
                    bookmarks,
                    ..
                })
                | Some(MapiObject::AttachmentTable {
                    sort_orders,
                    position,
                    bookmarks,
                    ..
                }) => {
                    *sort_orders = request.sort_orders();
                    *position = 0;
                    bookmarks.clear();
                    responses.extend_from_slice(&rop_sort_table_response(&request));
                }
                _ => responses.extend_from_slice(&rop_error_response(
                    0x13,
                    request.response_handle_index(),
                    0x8004_0102,
                )),
            },
            Some(RopId::Restrict) => match input_object_mut(session, &handle_slots, &request) {
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
                })
                | Some(MapiObject::AttachmentTable {
                    restriction,
                    position,
                    bookmarks,
                    ..
                }) => match request.restriction() {
                    Ok(parsed) => {
                        *restriction = parsed;
                        *position = 0;
                        bookmarks.clear();
                        responses.extend_from_slice(&rop_restrict_response(&request));
                    }
                    Err(_) => responses.extend_from_slice(&rop_error_response(
                        0x14,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                },
                _ => responses.extend_from_slice(&rop_error_response(
                    0x14,
                    request.response_handle_index(),
                    0x8004_0102,
                )),
            },
            Some(RopId::QueryRows) => responses.extend_from_slice(&rop_query_rows_response(
                &request,
                input_object_mut(session, &handle_slots, &request),
                mailboxes,
                emails,
                snapshot,
            )),
            Some(RopId::Abort) => responses.extend_from_slice(&rop_get_status_response(
                &request,
                input_object(session, &handle_slots, &request),
            )),
            Some(RopId::GetStatus) => responses.extend_from_slice(&rop_query_position_response(
                &request,
                input_object(session, &handle_slots, &request),
                mailboxes,
                emails,
                snapshot,
            )),
            Some(RopId::QueryPosition) => responses.extend_from_slice(&rop_seek_row_response(
                &request,
                input_object_mut(session, &handle_slots, &request),
                mailboxes,
                emails,
                snapshot,
            )),
            Some(RopId::SeekRow) => responses.extend_from_slice(&rop_seek_row_bookmark_response(
                &request,
                input_object_mut(session, &handle_slots, &request),
                mailboxes,
                emails,
                snapshot,
            )),
            Some(RopId::SeekRowBookmark) => {
                responses.extend_from_slice(&rop_seek_row_fractional_response(
                    &request,
                    input_object_mut(session, &handle_slots, &request),
                    mailboxes,
                    emails,
                    snapshot,
                ))
            }
            Some(RopId::SeekRowFractional) => {
                responses.extend_from_slice(&rop_create_bookmark_response(
                    &request,
                    input_object_mut(session, &handle_slots, &request),
                    mailboxes,
                    emails,
                    snapshot,
                ))
            }
            Some(RopId::QueryColumnsAll) => {
                responses.extend_from_slice(&rop_query_columns_all_response(
                    &request,
                    input_object(session, &handle_slots, &request),
                    snapshot,
                ))
            }
            Some(RopId::CreateFolder) => {
                let parent_folder_id = match input_object(session, &handle_slots, &request)
                    .and_then(MapiObject::folder_id)
                {
                    Some(folder_id) => folder_id,
                    None => {
                        responses.extend_from_slice(&rop_error_response(
                            0x1C,
                            request.output_handle_index.unwrap_or(0),
                            0x0000_04B9,
                        ));
                        continue;
                    }
                };
                if !is_root_hierarchy_folder(parent_folder_id)
                    && folder_row_for_id(parent_folder_id, mailboxes).is_none()
                    && role_for_folder_id(parent_folder_id).is_none()
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x1C,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                }

                let display_name = request.create_folder_display_name();
                let display_name = display_name.trim();
                if display_name.is_empty() || request.create_folder_type() == 0 {
                    responses.extend_from_slice(&rop_error_response(
                        0x1C,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_0102,
                    ));
                    continue;
                }

                if request.create_folder_open_existing() {
                    if let Some(existing) = mailboxes
                        .iter()
                        .find(|mailbox| mailbox.name.eq_ignore_ascii_case(display_name))
                    {
                        let folder_id = mapi_folder_id(existing);
                        let properties =
                            folder_properties_for_open(store, principal, session, folder_id).await;
                        let handle = session.allocate_output_handle(
                            request.output_handle_index,
                            MapiObject::Folder {
                                folder_id,
                                properties,
                            },
                        );
                        set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                        responses.extend_from_slice(&rop_create_folder_response(
                            &request, folder_id, true,
                        ));
                        output_handles.push(handle);
                        continue;
                    }
                }

                match store
                    .create_jmap_mailbox(
                        JmapMailboxCreateInput {
                            account_id: principal.account_id,
                            name: display_name.to_string(),
                            parent_id: None,
                            sort_order: None,
                            is_subscribed: true,
                        },
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "mapi-create-folder".to_string(),
                            subject: display_name.to_string(),
                        },
                    )
                    .await
                {
                    Ok(mailbox) => {
                        let folder_id = match remember_created_mapi_identity(
                            store,
                            principal,
                            MapiIdentityObjectKind::Mailbox,
                            mailbox.id,
                            None,
                        )
                        .await
                        {
                            Ok(folder_id) => folder_id,
                            Err(_) => {
                                responses.extend_from_slice(&rop_error_response(
                                    0x1C,
                                    request.output_handle_index.unwrap_or(0),
                                    0x8004_0102,
                                ));
                                continue;
                            }
                        };
                        let handle = session.allocate_output_handle(
                            request.output_handle_index,
                            MapiObject::Folder {
                                folder_id,
                                properties: HashMap::new(),
                            },
                        );
                        set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                        responses.extend_from_slice(&rop_create_folder_response(
                            &request, folder_id, false,
                        ));
                        session.record_notification(MapiNotificationEvent {
                            folder_id: parent_folder_id,
                            kind: MapiNotificationKind::Hierarchy,
                        });
                        output_handles.push(handle);
                    }
                    Err(_) => responses.extend_from_slice(&rop_error_response(
                        0x1C,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_0102,
                    )),
                }
            }
            Some(RopId::DeleteFolder) => {
                let Some(_parent_folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x1D,
                        request.response_handle_index(),
                        0x0000_04B9,
                    ));
                    continue;
                };
                let Some(folder_id) = request.delete_folder_id() else {
                    responses.extend_from_slice(&rop_error_response(
                        0x1D,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                };
                let Some(mailbox) = folder_row_for_id(folder_id, mailboxes) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x1D,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                if mailbox.role != "custom" {
                    responses.extend_from_slice(&rop_error_response(
                        0x1D,
                        request.response_handle_index(),
                        0x8007_0005,
                    ));
                    continue;
                }

                let partial_completion = store
                    .destroy_jmap_mailbox(
                        principal.account_id,
                        mailbox.id,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "mapi-delete-folder".to_string(),
                            subject: format!("folder:{}", mailbox.id),
                        },
                    )
                    .await
                    .is_err();
                if !partial_completion {
                    session.record_notification(MapiNotificationEvent {
                        folder_id: _parent_folder_id,
                        kind: MapiNotificationKind::Hierarchy,
                    });
                }
                responses.extend_from_slice(&rop_partial_completion_response(
                    0x1D,
                    request.response_handle_index(),
                    partial_completion,
                ));
            }
            Some(
                RopId::DeleteMessages
                | RopId::HardDeleteMessages
                | RopId::HardDeleteMessagesExtended,
            ) => {
                let folder_id = match input_object(session, &handle_slots, &request) {
                    Some(MapiObject::Folder { folder_id, .. }) => *folder_id,
                    _ if request.rop_id == RopId::HardDeleteMessages.as_u8() => {
                        responses.extend_from_slice(&unsupported_rop_response(
                            request.rop_id,
                            request.response_handle_index(),
                        ));
                        continue;
                    }
                    _ => {
                        responses.extend_from_slice(&rop_error_response(
                            request.rop_id,
                            request.response_handle_index(),
                            0x0000_04B9,
                        ));
                        continue;
                    }
                };
                let mut partial_completion = false;
                if !snapshot
                    .folder_access_for_principal(folder_id, principal.account_id)
                    .map(|access| access.may_delete)
                    .unwrap_or(true)
                {
                    responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8007_0005,
                    ));
                    continue;
                }
                for message_id in request.message_ids() {
                    if let Some(contact) = snapshot.contact_for_id(folder_id, message_id) {
                        if store
                            .delete_accessible_contact(principal.account_id, contact.canonical_id)
                            .await
                            .is_err()
                        {
                            partial_completion = true;
                        }
                        continue;
                    }
                    if let Some(event) = snapshot.event_for_id(folder_id, message_id) {
                        if store
                            .delete_accessible_event(principal.account_id, event.canonical_id)
                            .await
                            .is_err()
                        {
                            partial_completion = true;
                        }
                        continue;
                    }
                    if let Some(task) = snapshot.task_for_id(folder_id, message_id) {
                        if store
                            .delete_accessible_task(principal.account_id, task.canonical_id)
                            .await
                            .is_err()
                        {
                            partial_completion = true;
                        }
                        continue;
                    }
                    if let Some(note) = snapshot.note_for_id(folder_id, message_id) {
                        if store
                            .delete_mapi_note(principal.account_id, note.canonical_id)
                            .await
                            .is_err()
                        {
                            partial_completion = true;
                        }
                        continue;
                    }
                    if let Some(entry) = snapshot.journal_entry_for_id(folder_id, message_id) {
                        if store
                            .delete_mapi_journal_entry(principal.account_id, entry.canonical_id)
                            .await
                            .is_err()
                        {
                            partial_completion = true;
                        }
                        continue;
                    }
                    let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails)
                    else {
                        partial_completion = true;
                        continue;
                    };
                    let result = if request.rop_id == 0x59
                        || request.rop_id == 0x91
                        || email.mailbox_role == "trash"
                    {
                        store
                            .delete_jmap_email_from_mailbox(
                                principal.account_id,
                                email.mailbox_id,
                                email.id,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-delete-message".to_string(),
                                    subject: format!("message:{}", email.id),
                                },
                            )
                            .await
                            .map(|_| ())
                    } else if let Some(trash_mailbox) =
                        mailboxes.iter().find(|mailbox| mailbox.role == "trash")
                    {
                        store
                            .move_jmap_email_from_mailbox(
                                principal.account_id,
                                email.mailbox_id,
                                email.id,
                                trash_mailbox.id,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-move-message-to-trash".to_string(),
                                    subject: format!("message:{}->{}", email.id, trash_mailbox.id),
                                },
                            )
                            .await
                            .map(|_| ())
                    } else {
                        store
                            .delete_jmap_email_from_mailbox(
                                principal.account_id,
                                email.mailbox_id,
                                email.id,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-delete-message-without-trash".to_string(),
                                    subject: format!("message:{}", email.id),
                                },
                            )
                            .await
                            .map(|_| ())
                    };
                    if result.is_err() {
                        partial_completion = true;
                    }
                }
                if !partial_completion {
                    session.record_notification(MapiNotificationEvent {
                        folder_id,
                        kind: MapiNotificationKind::Content,
                    });
                }
                responses.extend_from_slice(&rop_partial_completion_response(
                    request.rop_id,
                    request.response_handle_index(),
                    partial_completion,
                ));
            }
            Some(RopId::GetMessageStatus | RopId::SetMessageStatus) => {
                let folder_id = match input_object(session, &handle_slots, &request)
                    .and_then(MapiObject::folder_id)
                {
                    Some(folder_id) => folder_id,
                    None => {
                        responses.extend_from_slice(&rop_error_response(
                            0x20,
                            request.response_handle_index(),
                            0x0000_04B9,
                        ));
                        continue;
                    }
                };
                let message_id = request.status_message_id().unwrap_or(0);
                if message_for_id(folder_id, message_id, mailboxes, emails).is_none() {
                    responses.extend_from_slice(&rop_error_response(
                        0x20,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                }
                let key = (folder_id, message_id);
                let old_status = session.message_statuses.get(&key).copied().unwrap_or(0);
                if request.rop_id == 0x20 {
                    let mask = request.message_status_mask();
                    let new_status = (old_status & !mask) | (request.message_status_flags() & mask);
                    if new_status == 0 {
                        session.message_statuses.remove(&key);
                    } else {
                        session.message_statuses.insert(key, new_status);
                    }
                }
                responses.extend_from_slice(&rop_message_status_response(&request, old_status));
            }
            Some(RopId::FindRow) => responses.extend_from_slice(&rop_find_row_response(
                &request,
                input_object_mut(session, &handle_slots, &request),
                mailboxes,
                emails,
                snapshot,
            )),
            Some(RopId::GetValidAttachments) => {
                responses.extend_from_slice(&rop_get_valid_attachments_response(
                    &request,
                    input_object(session, &handle_slots, &request),
                    snapshot,
                ))
            }
            Some(RopId::GetAttachmentTable) => {
                let Some(MapiObject::Message {
                    folder_id,
                    message_id,
                }) = input_object(session, &handle_slots, &request)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x21,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                };
                let row_count = snapshot
                    .attachments_for_message(*folder_id, *message_id)
                    .unwrap_or_default()
                    .len() as u32;
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::AttachmentTable {
                        folder_id: *folder_id,
                        message_id: *message_id,
                        columns: Vec::new(),
                        sort_orders: Vec::new(),
                        restriction: None,
                        bookmarks: HashMap::new(),
                        next_bookmark: 1,
                        position: 0,
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses
                    .extend_from_slice(&rop_get_attachment_table_response(&request, row_count));
                output_handles.push(handle);
            }
            Some(RopId::OpenAttachment) => {
                let Some(MapiObject::Message {
                    folder_id,
                    message_id,
                }) = input_object(session, &handle_slots, &request)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x22,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                };
                let attach_num = request.attach_num().unwrap_or(u32::MAX);
                if snapshot
                    .attachment_for_message(*folder_id, *message_id, attach_num)
                    .is_some()
                {
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::Attachment {
                            folder_id: *folder_id,
                            message_id: *message_id,
                            attach_num,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(&rop_open_attachment_response(&request));
                    output_handles.push(handle);
                } else {
                    responses.extend_from_slice(&rop_error_response(
                        0x22,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                }
            }
            Some(RopId::CreateAttachment) => {
                let Some(MapiObject::Message {
                    folder_id,
                    message_id,
                }) = input_object(session, &handle_slots, &request)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x23,
                        request.output_handle_index.unwrap_or(0),
                        0x0000_04B9,
                    ));
                    continue;
                };
                if message_for_id(*folder_id, *message_id, mailboxes, emails).is_none() {
                    responses.extend_from_slice(&rop_error_response(
                        0x23,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                }

                let attach_num =
                    next_pending_attachment_num(session, *folder_id, *message_id, snapshot);
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::PendingAttachment {
                        folder_id: *folder_id,
                        message_id: *message_id,
                        attach_num,
                        properties: HashMap::new(),
                        data: Vec::new(),
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_create_attachment_response(&request, attach_num));
                output_handles.push(handle);
            }
            Some(RopId::DeleteAttachment) => {
                let Some(MapiObject::Message {
                    folder_id,
                    message_id,
                }) = input_object(session, &handle_slots, &request)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x24,
                        request.response_handle_index(),
                        0x0000_04B9,
                    ));
                    continue;
                };
                let attach_num = request.attach_num().unwrap_or(u32::MAX);
                let Some(attachment) =
                    snapshot.attachment_for_message(*folder_id, *message_id, attach_num)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x24,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                match store
                    .delete_message_attachment(
                        principal.account_id,
                        &attachment.file_reference,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "mapi-delete-attachment".to_string(),
                            subject: attachment.file_reference.clone(),
                        },
                    )
                    .await
                {
                    Ok(Some(_)) => {
                        responses.extend_from_slice(&rop_simple_success_response(&request))
                    }
                    _ => responses.extend_from_slice(&rop_error_response(
                        0x24,
                        request.response_handle_index(),
                        0x8004_010F,
                    )),
                }
            }
            Some(RopId::SaveChangesAttachment) => {
                let Some(handle) = input_handle(&handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x25,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let Some(MapiObject::PendingAttachment {
                    folder_id,
                    message_id,
                    attach_num,
                    properties,
                    data,
                }) = session.handles.get(&handle).cloned()
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x25,
                        request.response_handle_index(),
                        0x0000_04B9,
                    ));
                    continue;
                };
                let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x25,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let attachment = pending_attachment_upload(attach_num, &properties, data);
                let validation = validator.validate_bytes(
                    ValidationRequest {
                        ingress_context: IngressContext::ExchangeAttachment,
                        declared_mime: Some(attachment.media_type.clone()),
                        filename: Some(attachment.file_name.clone()),
                        expected_kind: mapi_expected_attachment_kind(
                            &attachment.media_type,
                            &attachment.file_name,
                        ),
                    },
                    &attachment.blob_bytes,
                );
                let Ok(outcome) = validation else {
                    responses.extend_from_slice(&rop_error_response(
                        0x25,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                };
                if outcome.policy_decision != PolicyDecision::Accept {
                    responses.extend_from_slice(&rop_error_response(
                        0x25,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                }
                let mut attachment = attachment;
                if attachment.media_type == "application/octet-stream"
                    && !outcome.detected_mime.trim().is_empty()
                {
                    attachment.media_type = outcome.detected_mime;
                }
                match store
                    .add_message_attachment(
                        principal.account_id,
                        email.id,
                        attachment,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "mapi-save-attachment".to_string(),
                            subject: format!("message:{}", email.id),
                        },
                    )
                    .await
                {
                    Ok(Some((_email, stored))) => {
                        session.handles.insert(
                            handle,
                            MapiObject::SavedAttachment {
                                folder_id,
                                message_id,
                                attach_num,
                                file_reference: stored.file_reference,
                                file_name: stored.file_name,
                                media_type: stored.media_type,
                                size_octets: stored.size_octets,
                            },
                        );
                        responses.extend_from_slice(&rop_simple_success_response(&request));
                    }
                    _ => responses.extend_from_slice(&rop_error_response(
                        0x25,
                        request.response_handle_index(),
                        0x8004_010F,
                    )),
                }
            }
            Some(RopId::OpenStream) => {
                let Some(input_handle) = input_handle(&handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x2B,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                };
                let Some((stream_data, writable_target)) = open_stream_data(
                    store,
                    principal,
                    session,
                    input_handle,
                    request.stream_property_tag().unwrap_or(0),
                    request.stream_open_mode().unwrap_or(0),
                    mailboxes,
                    emails,
                    snapshot,
                )
                .await
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x2B,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                };
                let stream_size = stream_data.len();
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::AttachmentStream {
                        data: stream_data,
                        position: 0,
                        writable_target,
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_open_stream_response(&request, stream_size));
                output_handles.push(handle);
            }
            Some(RopId::ReadStream) => {
                let Some(stream) = input_object_mut(session, &handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x2C,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                responses.extend_from_slice(&rop_read_stream_response(&request, stream));
            }
            Some(RopId::SeekStream) => {
                let Some(stream) = input_object_mut(session, &handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x2E,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                responses.extend_from_slice(&rop_seek_stream_response(&request, stream));
            }
            Some(RopId::SetStreamSize) => {
                let Some(stream_handle) = input_handle(&handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x2F,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                match set_attachment_stream_size(
                    session,
                    stream_handle,
                    request.stream_size().unwrap_or(u64::MAX),
                ) {
                    Some(()) => responses.extend_from_slice(&rop_simple_success_response(&request)),
                    None => responses.extend_from_slice(&rop_error_response(
                        0x2F,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            Some(RopId::WriteStream | RopId::WriteStreamExtended2 | RopId::WriteStreamExtended) => {
                let Some(stream_handle) = input_handle(&handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                match write_stream(session, stream_handle, request.stream_write_data()) {
                    Some(written) => {
                        responses.extend_from_slice(&rop_write_stream_response(&request, written))
                    }
                    None => {
                        let error_code = stream_write_error_code(
                            stream_write_error(session, stream_handle)
                                .unwrap_or(StreamWriteError::NotFound),
                        );
                        responses.extend_from_slice(&rop_error_response(
                            request.rop_id,
                            request.response_handle_index(),
                            error_code,
                        ))
                    }
                }
            }
            Some(RopId::CopyToStream) => {
                let Some(source_handle) = input_handle(&handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x3A,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let Some(destination_handle) = request.move_copy_target_handle(&handle_slots)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x3A,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                match copy_stream(
                    session,
                    source_handle,
                    destination_handle,
                    request.stream_size().unwrap_or(u64::MAX),
                ) {
                    Some((read, written)) => responses
                        .extend_from_slice(&rop_copy_to_stream_response(&request, read, written)),
                    None => responses.extend_from_slice(&rop_error_response(
                        0x3A,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            Some(RopId::GetStreamSize) => match input_object(session, &handle_slots, &request) {
                Some(MapiObject::AttachmentStream { data, .. }) => {
                    responses.extend_from_slice(&rop_get_stream_size_response(&request, data.len()))
                }
                _ => responses.extend_from_slice(&rop_error_response(
                    0x5E,
                    request.response_handle_index(),
                    0x8004_010F,
                )),
            },
            Some(RopId::CloneStream) => {
                match input_object(session, &handle_slots, &request).cloned() {
                    Some(MapiObject::AttachmentStream {
                        data,
                        position,
                        writable_target: None,
                    }) => {
                        let handle = session.allocate_output_handle(
                            request.output_handle_index,
                            MapiObject::AttachmentStream {
                                data,
                                position,
                                writable_target: None,
                            },
                        );
                        set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                        responses.extend_from_slice(&rop_simple_success_response(&request));
                        output_handles.push(handle);
                    }
                    Some(MapiObject::AttachmentStream { .. }) => responses.extend_from_slice(
                        &rop_error_response(0x3B, request.response_handle_index(), 0x8004_0102),
                    ),
                    _ => responses.extend_from_slice(&rop_error_response(
                        0x3B,
                        request.response_handle_index(),
                        0x8004_010F,
                    )),
                }
            }
            Some(RopId::SetLocalReplicaMidsetDeleted | RopId::SetLocalReplicaMidsetExpired) => {
                responses.extend_from_slice(&rop_error_response(
                    request.rop_id,
                    request.response_handle_index(),
                    0x8004_0102,
                ))
            }
            Some(RopId::GetDeletedMessages) => match input_object(session, &handle_slots, &request)
            {
                Some(MapiObject::AttachmentStream { .. }) => {
                    responses.extend_from_slice(&rop_simple_success_response(&request))
                }
                _ => responses.extend_from_slice(&rop_error_response(
                    0x5D,
                    request.response_handle_index(),
                    0x8004_010F,
                )),
            },
            Some(RopId::SubmitMessage | RopId::TransportSend) => {
                let Some(handle) = input_handle(&handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let Some(object) = session.handles.get(&handle).cloned() else {
                    responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x0000_04B9,
                    ));
                    continue;
                };
                let input = match object {
                    MapiObject::PendingMessage {
                        properties,
                        recipients,
                        ..
                    } => mapi_submit_from_pending_message(principal, &properties, &recipients),
                    MapiObject::Message {
                        folder_id,
                        message_id,
                    } => {
                        let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails)
                        else {
                            responses.extend_from_slice(&rop_error_response(
                                request.rop_id,
                                request.response_handle_index(),
                                0x8004_010F,
                            ));
                            continue;
                        };
                        if email.mailbox_role != "drafts" {
                            responses.extend_from_slice(&rop_error_response(
                                request.rop_id,
                                request.response_handle_index(),
                                0x8004_0102,
                            ));
                            continue;
                        }
                        let protected_emails = match store
                            .fetch_jmap_emails_with_protected_bcc(principal.account_id, &[email.id])
                            .await
                        {
                            Ok(emails) => emails,
                            Err(error) => {
                                warn!(
                                    error = %error,
                                    "failed to load protected Bcc recipients for MAPI draft submit"
                                );
                                responses.extend_from_slice(&rop_error_response(
                                    request.rop_id,
                                    request.response_handle_index(),
                                    0x8004_010F,
                                ));
                                continue;
                            }
                        };
                        let protected_email =
                            protected_emails.iter().find(|loaded| loaded.id == email.id);
                        let source_email = protected_email.unwrap_or(email);
                        let attachments = match mapi_submit_attachments_from_email(
                            store,
                            principal.account_id,
                            source_email,
                        )
                        .await
                        {
                            Ok(attachments) => attachments,
                            Err(error) => {
                                warn!(
                                    error = %error,
                                    "failed to load attachments for MAPI draft submit"
                                );
                                responses.extend_from_slice(&rop_error_response(
                                    request.rop_id,
                                    request.response_handle_index(),
                                    0x8004_010F,
                                ));
                                continue;
                            }
                        };
                        mapi_submit_from_email(principal, source_email, attachments)
                    }
                    _ => {
                        responses.extend_from_slice(&rop_error_response(
                            request.rop_id,
                            request.response_handle_index(),
                            0x0000_04B9,
                        ));
                        continue;
                    }
                };
                match store
                    .submit_message(
                        input,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "mapi-submit-message".to_string(),
                            subject: format!("handle:{handle}"),
                        },
                    )
                    .await
                {
                    Ok(submitted) => {
                        let message_id = match remember_created_mapi_identity(
                            store,
                            principal,
                            MapiIdentityObjectKind::Message,
                            submitted.message_id,
                            None,
                        )
                        .await
                        {
                            Ok(message_id) => message_id,
                            Err(_) => {
                                responses.extend_from_slice(&rop_error_response(
                                    request.rop_id,
                                    request.response_handle_index(),
                                    0x8004_010F,
                                ));
                                continue;
                            }
                        };
                        session.handles.insert(
                            handle,
                            MapiObject::Message {
                                folder_id: submitted_mapi_folder_id(&submitted, mailboxes),
                                message_id,
                            },
                        );
                        if request.rop_id == 0x4A {
                            responses
                                .extend_from_slice(&rop_transport_send_success_response(&request));
                        } else {
                            responses.extend_from_slice(&rop_simple_success_response(&request));
                        }
                    }
                    Err(_) => responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8004_010F,
                    )),
                }
            }
            Some(RopId::MoveCopyMessages) => {
                let source_folder_id = match input_object(session, &handle_slots, &request) {
                    Some(MapiObject::Folder { folder_id, .. }) => *folder_id,
                    _ => {
                        responses.extend_from_slice(&rop_error_response(
                            0x33,
                            request.response_handle_index(),
                            0x0000_04B9,
                        ));
                        continue;
                    }
                };
                let target_folder_id = match request
                    .move_copy_target_handle(&handle_slots)
                    .and_then(|handle| {
                        session
                            .handles
                            .get(&handle)
                            .and_then(|object| object.folder_id())
                    }) {
                    Some(folder_id) => folder_id,
                    None => {
                        responses.extend_from_slice(&rop_error_response(
                            0x33,
                            request.response_handle_index(),
                            0x8004_010F,
                        ));
                        continue;
                    }
                };
                if matches!(source_folder_id, NOTES_FOLDER_ID | JOURNAL_FOLDER_ID) {
                    let mut partial_completion = false;
                    for message_id in request.move_copy_message_ids() {
                        if source_folder_id == NOTES_FOLDER_ID {
                            let Some(note) = snapshot.note_for_id(source_folder_id, message_id)
                            else {
                                partial_completion = true;
                                continue;
                            };
                            if target_folder_id != NOTES_FOLDER_ID {
                                partial_completion = true;
                                continue;
                            }
                            if request.move_copy_want_copy() {
                                match store
                                    .upsert_mapi_note(UpsertClientNoteInput {
                                        id: None,
                                        account_id: principal.account_id,
                                        title: note.note.title.clone(),
                                        body_text: note.note.body_text.clone(),
                                        color: note.note.color.clone(),
                                        categories_json: note.note.categories_json.clone(),
                                    })
                                    .await
                                {
                                    Ok(copied) => {
                                        if remember_created_mapi_identity(
                                            store,
                                            principal,
                                            MapiIdentityObjectKind::Note,
                                            copied.id,
                                            None,
                                        )
                                        .await
                                        .is_err()
                                        {
                                            partial_completion = true;
                                        }
                                    }
                                    Err(_) => partial_completion = true,
                                }
                            }
                            continue;
                        }
                        let Some(entry) =
                            snapshot.journal_entry_for_id(source_folder_id, message_id)
                        else {
                            partial_completion = true;
                            continue;
                        };
                        if target_folder_id != JOURNAL_FOLDER_ID {
                            partial_completion = true;
                            continue;
                        }
                        if request.move_copy_want_copy() {
                            match store
                                .upsert_mapi_journal_entry(UpsertJournalEntryInput {
                                    id: None,
                                    account_id: principal.account_id,
                                    subject: entry.entry.subject.clone(),
                                    body_text: entry.entry.body_text.clone(),
                                    entry_type: entry.entry.entry_type.clone(),
                                    message_class: entry.entry.message_class.clone(),
                                    starts_at: entry.entry.starts_at.clone(),
                                    ends_at: entry.entry.ends_at.clone(),
                                    occurred_at: entry.entry.occurred_at.clone(),
                                    companies_json: entry.entry.companies_json.clone(),
                                    contacts_json: entry.entry.contacts_json.clone(),
                                })
                                .await
                            {
                                Ok(copied) => {
                                    if remember_created_mapi_identity(
                                        store,
                                        principal,
                                        MapiIdentityObjectKind::JournalEntry,
                                        copied.id,
                                        None,
                                    )
                                    .await
                                    .is_err()
                                    {
                                        partial_completion = true;
                                    }
                                }
                                Err(_) => partial_completion = true,
                            }
                        }
                    }
                    responses.extend_from_slice(&rop_partial_completion_response(
                        0x33,
                        request.response_handle_index(),
                        partial_completion,
                    ));
                    continue;
                }
                let Some(target_mailbox) = folder_row_for_id(target_folder_id, mailboxes) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x33,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let mut partial_completion = false;
                for message_id in request.move_copy_message_ids() {
                    let Some(email) =
                        message_for_id(source_folder_id, message_id, mailboxes, emails)
                    else {
                        partial_completion = true;
                        continue;
                    };
                    let result = if request.move_copy_want_copy() {
                        store
                            .copy_jmap_email(
                                principal.account_id,
                                email.id,
                                target_mailbox.id,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-copy-message".to_string(),
                                    subject: format!("message:{}->{}", email.id, target_mailbox.id),
                                },
                            )
                            .await
                            .map(|_| ())
                    } else {
                        store
                            .move_jmap_email(
                                principal.account_id,
                                email.id,
                                target_mailbox.id,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-move-message".to_string(),
                                    subject: format!("message:{}->{}", email.id, target_mailbox.id),
                                },
                            )
                            .await
                            .map(|_| ())
                    };
                    if result.is_err() {
                        partial_completion = true;
                    }
                }
                responses.extend_from_slice(&rop_partial_completion_response(
                    0x33,
                    request.response_handle_index(),
                    partial_completion,
                ));
            }
            Some(RopId::SetReceiveFolder) => responses.extend_from_slice(&rop_error_response(
                0x26,
                request.response_handle_index(),
                0x8004_0102,
            )),
            Some(RopId::GetReceiveFolder) => {
                echo_input_handle_table = true;
                let Some(message_class) = request.receive_folder_message_class() else {
                    responses.extend_from_slice(&rop_error_response(
                        0x27,
                        request.response_handle_index(),
                        0x8007_0057,
                    ));
                    continue;
                };
                if !valid_receive_folder_message_class(message_class) {
                    responses.extend_from_slice(&rop_error_response(
                        0x27,
                        request.response_handle_index(),
                        0x8007_0057,
                    ));
                    continue;
                }
                responses.extend_from_slice(&rop_get_receive_folder_response(
                    &request,
                    explicit_receive_folder_message_class(message_class),
                ));
            }
            Some(RopId::SetReadFlags) => {
                let folder_id = match input_object(session, &handle_slots, &request) {
                    Some(MapiObject::Folder { folder_id, .. }) => *folder_id,
                    _ => {
                        responses.extend_from_slice(&rop_error_response(
                            0x66,
                            request.response_handle_index(),
                            0x0000_04B9,
                        ));
                        continue;
                    }
                };
                let unread = unread_from_read_flags(request.read_flags());
                let mut partial_completion = false;
                for message_id in request.message_ids() {
                    let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails)
                    else {
                        partial_completion = true;
                        continue;
                    };
                    if let Some(unread) = unread {
                        if store
                            .update_jmap_email_flags(
                                principal.account_id,
                                email.id,
                                Some(unread),
                                None,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-set-read-flags".to_string(),
                                    subject: format!("message:{}", email.id),
                                },
                            )
                            .await
                            .is_err()
                        {
                            partial_completion = true;
                        }
                    }
                }
                responses
                    .extend_from_slice(&rop_set_read_flags_response(&request, partial_completion));
            }
            Some(RopId::SynchronizationConfigure) => {
                let Some(folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x70,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let sync_type = request.sync_type();
                let sync_flags = request.sync_flags();
                let sync_extra_flags = request.sync_extra_flags();
                let sync_property_tags = request.sync_property_tags();
                let sync_property_tags_hex = sync_property_tags
                    .iter()
                    .map(|tag| format!("0x{tag:08x}"))
                    .collect::<Vec<_>>()
                    .join(",");
                let checkpoint_kind = sync_checkpoint_kind(sync_type);
                let checkpoint_mailbox_id =
                    sync_checkpoint_mailbox_id(folder_id, sync_type, mailboxes);
                let checkpoint = match store
                    .fetch_mapi_sync_checkpoint(
                        principal.account_id,
                        checkpoint_mailbox_id,
                        checkpoint_kind,
                    )
                    .await
                {
                    Ok(checkpoint) => checkpoint,
                    Err(_) => {
                        responses.extend_from_slice(&rop_error_response(
                            0x70,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        continue;
                    }
                };
                let checkpoint_status = checkpoint
                    .as_ref()
                    .map(|checkpoint| {
                        hierarchy_checkpoint_status(checkpoint_kind, folder_id, checkpoint)
                    })
                    .unwrap_or("missing");
                let checkpoint_cursor_source = checkpoint
                    .as_ref()
                    .and_then(|checkpoint| checkpoint.cursor_json.get("source"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("")
                    .to_string();
                let checkpoint_cursor_sync_root_folder_id = checkpoint
                    .as_ref()
                    .and_then(|checkpoint| checkpoint.cursor_json.get("syncRootFolderId"))
                    .and_then(serde_json::Value::as_u64)
                    .map(|id| format!("0x{id:016x}"))
                    .unwrap_or_default();
                let checkpoint_cursor_hierarchy_sync_version = checkpoint
                    .as_ref()
                    .and_then(|checkpoint| checkpoint.cursor_json.get("hierarchySyncVersion"))
                    .and_then(serde_json::Value::as_u64)
                    .map(|version| version.to_string())
                    .unwrap_or_default();
                let checkpoint_cursor_change_sequence = checkpoint
                    .as_ref()
                    .map(|checkpoint| checkpoint.last_change_sequence)
                    .unwrap_or_default();
                let checkpoint_cursor_modseq = checkpoint
                    .as_ref()
                    .map(|checkpoint| checkpoint.last_modseq)
                    .unwrap_or_default();
                let checkpoint = checkpoint.filter(|_| checkpoint_status == "usable");
                let since = checkpoint
                    .as_ref()
                    .map(|checkpoint| checkpoint.last_change_sequence)
                    .unwrap_or(0);
                let changes = match store
                    .fetch_mapi_sync_changes(
                        principal.account_id,
                        checkpoint_mailbox_id,
                        checkpoint_kind,
                        since,
                    )
                    .await
                {
                    Ok(changes) => changes,
                    Err(_) => {
                        responses.extend_from_slice(&rop_error_response(
                            0x70,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        continue;
                    }
                };
                let all_sync_mailboxes = sync_mailboxes_for(folder_id, sync_type, mailboxes);
                let all_sync_emails = sync_emails_for(folder_id, sync_type, mailboxes, emails);
                let all_special_sync_objects =
                    special_sync_objects_for(folder_id, sync_type, snapshot);
                let available_sync_mailbox_count = all_sync_mailboxes.len();
                let available_sync_email_count = all_sync_emails.len();
                let available_special_sync_object_count = all_special_sync_objects.len();
                let (delta_sync_mailboxes, delta_sync_emails, delta_special_sync_objects) =
                    if checkpoint.is_some() {
                        let changed_special_ids: &[Uuid] = match folder_id {
                            NOTES_FOLDER_ID => &changes.changed_note_ids,
                            JOURNAL_FOLDER_ID => &changes.changed_journal_entry_ids,
                            _ => &[],
                        };
                        (
                            changed_sync_mailboxes(
                                all_sync_mailboxes.clone(),
                                &changes.changed_mailbox_ids,
                            ),
                            changed_sync_emails(
                                all_sync_emails.clone(),
                                &changes.changed_message_ids,
                            ),
                            changed_special_sync_objects(
                                all_special_sync_objects.clone(),
                                changed_special_ids,
                            ),
                        )
                    } else {
                        (
                            all_sync_mailboxes.clone(),
                            all_sync_emails.clone(),
                            all_special_sync_objects.clone(),
                        )
                    };
                let sync_attachment_facts =
                    sync_attachment_facts_for(folder_id, &all_sync_emails, snapshot);
                let delta_attachment_facts =
                    sync_attachment_facts_for(folder_id, &delta_sync_emails, snapshot);
                let aggregate_sync_emails = if sync_type == 0x02 {
                    emails.to_vec()
                } else {
                    all_sync_emails.clone()
                };
                let state_attachment_facts =
                    sync_attachment_facts_for(folder_id, &all_sync_emails, snapshot);
                let aggregate_attachment_facts =
                    sync_attachment_facts_for(folder_id, &aggregate_sync_emails, snapshot);
                let mut deleted_message_ids = if checkpoint.is_some() {
                    mapi_message_ids_for_deleted_changes(
                        store,
                        principal,
                        &changes.deleted_message_ids,
                    )
                    .await
                    .unwrap_or_default()
                } else {
                    Vec::new()
                };
                if checkpoint.is_some() && folder_id == NOTES_FOLDER_ID {
                    deleted_message_ids.extend(
                        mapi_object_ids_for_deleted_changes(
                            store,
                            principal,
                            MapiIdentityObjectKind::Note,
                            &changes.deleted_note_ids,
                        )
                        .await
                        .unwrap_or_default(),
                    );
                }
                if checkpoint.is_some() && folder_id == JOURNAL_FOLDER_ID {
                    deleted_message_ids.extend(
                        mapi_object_ids_for_deleted_changes(
                            store,
                            principal,
                            MapiIdentityObjectKind::JournalEntry,
                            &changes.deleted_journal_entry_ids,
                        )
                        .await
                        .unwrap_or_default(),
                    );
                }
                let state = mapi_mailstore::sync_state_token_with_special_objects(
                    sync_type,
                    folder_id,
                    &all_sync_mailboxes,
                    &all_sync_emails,
                    &state_attachment_facts,
                    &all_special_sync_objects,
                );
                let force_hierarchy_count_properties =
                    force_hierarchy_count_properties_experiment_enabled();
                let transfer_buffer =
                    mapi_mailstore::sync_manifest_buffer_with_special_objects_and_final_state(
                        sync_type,
                        sync_flags,
                        sync_extra_flags,
                        &sync_property_tags,
                        folder_id,
                        &all_sync_mailboxes,
                        &all_sync_emails,
                        &sync_attachment_facts,
                        &all_special_sync_objects,
                        &[],
                        mailboxes,
                        &all_sync_mailboxes,
                        &all_sync_emails,
                        &state_attachment_facts,
                        &all_special_sync_objects,
                        &aggregate_sync_emails,
                        &aggregate_attachment_facts,
                        changes.current_change_sequence,
                        force_hierarchy_count_properties,
                    );
                let hierarchy_content_candidate =
                    mapi_mailstore::hierarchy_content_count_omission_candidate(
                        sync_type,
                        sync_flags,
                        &sync_property_tags,
                        folder_id,
                        &all_sync_mailboxes,
                        &aggregate_sync_emails,
                    );
                mapi_mailstore::log_hierarchy_transfer_debug(
                    sync_type,
                    sync_flags,
                    folder_id,
                    &sync_property_tags,
                    &transfer_buffer,
                );
                let incremental_transfer_buffer = checkpoint.as_ref().map(|_| {
                    mapi_mailstore::sync_manifest_buffer_with_special_objects_and_final_state(
                        sync_type,
                        sync_flags,
                        sync_extra_flags,
                        &sync_property_tags,
                        folder_id,
                        &delta_sync_mailboxes,
                        &delta_sync_emails,
                        &delta_attachment_facts,
                        &delta_special_sync_objects,
                        &deleted_message_ids,
                        mailboxes,
                        &all_sync_mailboxes,
                        &all_sync_emails,
                        &state_attachment_facts,
                        &all_special_sync_objects,
                        &aggregate_sync_emails,
                        &aggregate_attachment_facts,
                        changes.current_change_sequence,
                        force_hierarchy_count_properties,
                    )
                });
                let incremental_hierarchy_content_candidate = checkpoint.as_ref().and_then(|_| {
                    mapi_mailstore::hierarchy_content_count_omission_candidate(
                        sync_type,
                        sync_flags,
                        &sync_property_tags,
                        folder_id,
                        &delta_sync_mailboxes,
                        &aggregate_sync_emails,
                    )
                });
                let checkpoint_delta_mailbox_count = delta_sync_mailboxes.len();
                let checkpoint_delta_email_count = delta_sync_emails.len();
                let checkpoint_deleted_message_count = deleted_message_ids.len();
                let incremental_transfer_buffer_bytes = incremental_transfer_buffer
                    .as_ref()
                    .map(|buffer| buffer.len())
                    .unwrap_or_default();
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    mailbox = %principal.email,
                    request_type = "Execute",
                    request_rop_id = "0x70",
                    folder_id = format_args!("0x{folder_id:016x}"),
                    sync_type = format_args!("0x{sync_type:02x}"),
                    sync_flags = format_args!("0x{sync_flags:04x}"),
                    sync_extra_flags = format_args!("0x{sync_extra_flags:08x}"),
                    sync_property_tag_count = sync_property_tags.len(),
                    sync_property_tags = %sync_property_tags_hex,
                    sync_property_filter_mode =
                        sync_property_filter_mode(sync_flags, &sync_property_tags),
                    force_hierarchy_count_properties_experiment =
                        force_hierarchy_count_properties,
                    checkpoint_loaded = checkpoint.is_some(),
                    checkpoint_status,
                    checkpoint_cursor_source,
                    checkpoint_cursor_sync_root_folder_id = %checkpoint_cursor_sync_root_folder_id,
                    checkpoint_cursor_hierarchy_sync_version =
                        %checkpoint_cursor_hierarchy_sync_version,
                    checkpoint_cursor_change_sequence,
                    checkpoint_cursor_modseq,
                    snapshot_mailbox_count = mailboxes.len(),
                    snapshot_email_count = emails.len(),
                    available_sync_mailbox_count,
                    available_sync_email_count,
                    available_special_sync_object_count,
                    sync_mailbox_count = all_sync_mailboxes.len(),
                    sync_email_count = all_sync_emails.len(),
                    sync_special_object_count = all_special_sync_objects.len(),
                    checkpoint_delta_mailbox_count,
                    checkpoint_delta_email_count,
                    checkpoint_delta_special_object_count = delta_special_sync_objects.len(),
                    checkpoint_deleted_message_count,
                    current_change_sequence = changes.current_change_sequence,
                    transfer_buffer_bytes = transfer_buffer.len(),
                    incremental_transfer_buffer_bytes,
                    "rca debug mapi sync configure"
                );
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::SynchronizationSource {
                        folder_id,
                        mailbox_id: checkpoint_mailbox_id,
                        checkpoint_kind,
                        checkpoint_change_sequence: changes.current_change_sequence,
                        checkpoint_modseq: changes.current_modseq,
                        sync_type,
                        state,
                        state_upload_buffer: Vec::new(),
                        client_state_uploaded_bytes: 0,
                        incremental_transfer_buffer,
                        hierarchy_content_candidate,
                        incremental_hierarchy_content_candidate,
                        transfer_buffer,
                        transfer_position: 0,
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_synchronization_configure_response(&request));
                output_handles.push(handle);
                content_sync_configure_observed = sync_type == 0x01;
            }
            Some(RopId::FastTransferSourceCopyMessages) => {
                let Some(folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x4B,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let requested_ids = request.fast_transfer_message_ids();
                let mut selected = emails_for_folder(folder_id, mailboxes, emails)
                    .into_iter()
                    .filter(|email| requested_ids.contains(&mapi_message_id(email)))
                    .cloned()
                    .collect::<Vec<_>>();
                selected.sort_by(|left, right| left.id.cmp(&right.id));
                let sync_attachment_facts =
                    sync_attachment_facts_for(folder_id, &selected, snapshot);
                let transfer_buffer =
                    mapi_mailstore::fast_transfer_manifest_buffer_with_attachments(
                        folder_id,
                        &[],
                        &selected,
                        &sync_attachment_facts,
                    );
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::SynchronizationSource {
                        folder_id,
                        mailbox_id: None,
                        checkpoint_kind: MapiCheckpointKind::Content,
                        checkpoint_change_sequence: 0,
                        checkpoint_modseq: 1,
                        sync_type: 0,
                        state: Vec::new(),
                        state_upload_buffer: Vec::new(),
                        client_state_uploaded_bytes: 0,
                        incremental_transfer_buffer: None,
                        hierarchy_content_candidate: None,
                        incremental_hierarchy_content_candidate: None,
                        transfer_buffer,
                        transfer_position: 0,
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_fast_transfer_source_copy_response(&request));
                output_handles.push(handle);
            }
            Some(
                RopId::FastTransferSourceCopyFolder
                | RopId::FastTransferSourceCopyTo
                | RopId::FastTransferSourceCopyProperties,
            ) => {
                let Some(object) = input_object(session, &handle_slots, &request).cloned() else {
                    responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let Some((folder_id, transfer_buffer)) =
                    fast_transfer_manifest_for_object(&object, mailboxes, emails, snapshot)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                };
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::SynchronizationSource {
                        folder_id,
                        mailbox_id: None,
                        checkpoint_kind: MapiCheckpointKind::Content,
                        checkpoint_change_sequence: 0,
                        checkpoint_modseq: 1,
                        sync_type: 0,
                        state: Vec::new(),
                        state_upload_buffer: Vec::new(),
                        client_state_uploaded_bytes: 0,
                        incremental_transfer_buffer: None,
                        hierarchy_content_candidate: None,
                        incremental_hierarchy_content_candidate: None,
                        transfer_buffer,
                        transfer_position: 0,
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_fast_transfer_source_copy_response(&request));
                output_handles.push(handle);
            }
            Some(RopId::FastTransferSourceGetBuffer) => {
                match input_object_mut(session, &handle_slots, &request) {
                    Some(MapiObject::SynchronizationSource {
                        folder_id,
                        mailbox_id,
                        checkpoint_kind,
                        checkpoint_change_sequence,
                        checkpoint_modseq,
                        sync_type,
                        state,
                        state_upload_buffer,
                        client_state_uploaded_bytes,
                        incremental_transfer_buffer,
                        transfer_buffer,
                        transfer_position,
                        ..
                    }) => {
                        let requested_buffer_bytes = request.fast_transfer_buffer_size();
                        let previous_transfer_position = *transfer_position;
                        let response = rop_fast_transfer_source_get_buffer_response(
                            &request,
                            transfer_buffer,
                            transfer_position,
                        );
                        let completed = *transfer_position >= transfer_buffer.len();
                        if completed && *sync_type == 0x02 {
                            completed_hierarchy_sync_root = Some(*folder_id);
                        }
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            mailbox = %principal.email,
                            request_type = "Execute",
                            request_rop_id = "0x4e",
                            folder_id = format_args!("0x{:016x}", *folder_id),
                            sync_type = format_args!("0x{:02x}", *sync_type),
                            checkpoint_kind = checkpoint_kind.as_str(),
                            checkpoint_mailbox_id = (*mailbox_id)
                                .map(|id| id.to_string())
                                .unwrap_or_default(),
                            checkpoint_change_sequence = *checkpoint_change_sequence,
                            checkpoint_modseq = *checkpoint_modseq,
                            sync_state_bytes = state.len(),
                            upload_state_buffer_bytes = state_upload_buffer.len(),
                            upload_state_client_bytes = *client_state_uploaded_bytes,
                            incremental_transfer_available = incremental_transfer_buffer.is_some(),
                            incremental_transfer_buffer_bytes = incremental_transfer_buffer
                                .as_ref()
                                .map(|buffer| buffer.len())
                                .unwrap_or_default(),
                            requested_buffer_bytes,
                            transfer_position_before = previous_transfer_position,
                            transfer_position_after = *transfer_position,
                            transfer_buffer_bytes = transfer_buffer.len(),
                            transfer_chunk_bytes =
                                (*transfer_position).saturating_sub(previous_transfer_position),
                            transfer_completed = completed,
                            transfer_status = if completed { "0x0003" } else { "0x0001" },
                            "rca debug mapi fast transfer get buffer"
                        );
                        let checkpoint = (
                            *mailbox_id,
                            *checkpoint_kind,
                            *checkpoint_change_sequence,
                            *checkpoint_modseq,
                            *sync_type,
                            *folder_id,
                        );
                        responses.extend_from_slice(&response);
                        if completed && matches!(checkpoint.4, 0x01 | 0x02) {
                            let mut cursor_json = serde_json::json!({
                                "syncType": checkpoint.4,
                                "syncRootFolderId": checkpoint.5,
                                "source": "emsmdb-ics-download"
                            });
                            if checkpoint.1 == MapiCheckpointKind::Hierarchy {
                                cursor_json["hierarchySyncVersion"] =
                                    serde_json::json!(HIERARCHY_SYNC_CURSOR_VERSION);
                            }
                            let checkpoint_result = store
                                .store_mapi_sync_checkpoint(
                                    principal.account_id,
                                    checkpoint.0,
                                    checkpoint.1,
                                    checkpoint.2,
                                    checkpoint.3,
                                    cursor_json,
                                )
                                .await;
                            match checkpoint_result {
                                Ok(stored_checkpoint) => tracing::info!(
                                    rca_debug = true,
                                    adapter = "mapi",
                                    endpoint = "emsmdb",
                                    mailbox = %principal.email,
                                    request_type = "Execute",
                                    request_rop_id = "0x4e",
                                    folder_id = format_args!("0x{:016x}", *folder_id),
                                    sync_type = format_args!("0x{:02x}", checkpoint.4),
                                    checkpoint_kind = checkpoint.1.as_str(),
                                    checkpoint_mailbox_id = checkpoint
                                        .0
                                        .map(|id| id.to_string())
                                        .unwrap_or_default(),
                                    checkpoint_change_sequence = checkpoint.2,
                                    checkpoint_modseq = checkpoint.3,
                                    stored_change_sequence = stored_checkpoint.last_change_sequence,
                                    stored_modseq = stored_checkpoint.last_modseq,
                                    sync_state_bytes = state.len(),
                                    upload_state_buffer_bytes = state_upload_buffer.len(),
                                    upload_state_client_bytes = *client_state_uploaded_bytes,
                                    incremental_transfer_available = incremental_transfer_buffer.is_some(),
                                    transfer_buffer_bytes = transfer_buffer.len(),
                                    transfer_position = *transfer_position,
                                    checkpoint_store_status = "ok",
                                    "rca debug mapi sync checkpoint store"
                                ),
                                Err(error) => tracing::warn!(
                                    rca_debug = true,
                                    adapter = "mapi",
                                    endpoint = "emsmdb",
                                    mailbox = %principal.email,
                                    request_type = "Execute",
                                    request_rop_id = "0x4e",
                                    folder_id = format_args!("0x{:016x}", *folder_id),
                                    sync_type = format_args!("0x{:02x}", checkpoint.4),
                                    checkpoint_kind = checkpoint.1.as_str(),
                                    checkpoint_mailbox_id = checkpoint
                                        .0
                                        .map(|id| id.to_string())
                                        .unwrap_or_default(),
                                    checkpoint_change_sequence = checkpoint.2,
                                    checkpoint_modseq = checkpoint.3,
                                    sync_state_bytes = state.len(),
                                    upload_state_buffer_bytes = state_upload_buffer.len(),
                                    upload_state_client_bytes = *client_state_uploaded_bytes,
                                    incremental_transfer_available = incremental_transfer_buffer.is_some(),
                                    transfer_buffer_bytes = transfer_buffer.len(),
                                    transfer_position = *transfer_position,
                                    checkpoint_store_status = "error",
                                    error = %error,
                                    "rca debug mapi sync checkpoint store"
                                ),
                            }
                        }
                    }
                    _ => responses.extend_from_slice(&rop_error_response(
                        0x4E,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            Some(RopId::SynchronizationImportReadStateChanges) => {
                match input_object(session, &handle_slots, &request) {
                    Some(MapiObject::SynchronizationSource { .. })
                    | Some(MapiObject::SynchronizationCollector { .. }) => {
                        responses.extend_from_slice(&rop_simple_success_response(&request));
                    }
                    _ => responses.extend_from_slice(&rop_error_response(
                        0x86,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            Some(RopId::SynchronizationUploadStateStreamBegin) => {
                match input_object_mut(session, &handle_slots, &request) {
                    Some(MapiObject::SynchronizationSource {
                        folder_id,
                        state_upload_buffer,
                        ..
                    })
                    | Some(MapiObject::SynchronizationCollector {
                        folder_id,
                        state_upload_buffer,
                        ..
                    }) => {
                        state_upload_buffer.clear();
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            mailbox = %principal.email,
                            request_type = "Execute",
                            request_rop_id = "0x75",
                            folder_id = format_args!("0x{:016x}", *folder_id),
                            upload_state_property_tag = format_args!(
                                "0x{:08x}",
                                request.upload_state_property_tag().unwrap_or_default()
                            ),
                            upload_state_declared_bytes =
                                request.upload_state_transfer_size().unwrap_or_default(),
                            "rca debug mapi sync upload state begin"
                        );
                        responses.extend_from_slice(&rop_simple_success_response(&request));
                    }
                    _ => responses.extend_from_slice(&rop_error_response(
                        0x75,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            Some(RopId::SynchronizationUploadStateStreamContinue) => {
                match input_object_mut(session, &handle_slots, &request) {
                    Some(MapiObject::SynchronizationSource {
                        folder_id,
                        state_upload_buffer,
                        ..
                    })
                    | Some(MapiObject::SynchronizationCollector {
                        folder_id,
                        state_upload_buffer,
                        ..
                    }) => {
                        let stream_data = request.stream_data();
                        state_upload_buffer.extend_from_slice(stream_data);
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            mailbox = %principal.email,
                            request_type = "Execute",
                            request_rop_id = "0x76",
                            folder_id = format_args!("0x{:016x}", *folder_id),
                            upload_state_chunk_bytes = stream_data.len(),
                            upload_state_buffer_bytes = state_upload_buffer.len(),
                            "rca debug mapi sync upload state continue"
                        );
                        responses.extend_from_slice(&rop_simple_success_response(&request));
                    }
                    _ => responses.extend_from_slice(&rop_error_response(
                        0x76,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            Some(RopId::SynchronizationUploadStateStreamEnd) => {
                match input_object_mut(session, &handle_slots, &request) {
                    Some(MapiObject::SynchronizationSource {
                        folder_id,
                        state,
                        state_upload_buffer,
                        client_state_uploaded_bytes,
                        incremental_transfer_buffer,
                        hierarchy_content_candidate,
                        incremental_hierarchy_content_candidate,
                        transfer_buffer,
                        transfer_position,
                        ..
                    }) => {
                        let uploaded_bytes = state_upload_buffer.len();
                        let may_use_incremental = *client_state_uploaded_bytes == 0;
                        state_upload_buffer.clear();
                        *client_state_uploaded_bytes =
                            (*client_state_uploaded_bytes).saturating_add(uploaded_bytes);
                        let mut selected_checkpoint_delta = false;
                        if may_use_incremental && uploaded_bytes > 0 {
                            if let Some(buffer) = incremental_transfer_buffer.take() {
                                *transfer_buffer = buffer;
                                *transfer_position = 0;
                                *hierarchy_content_candidate =
                                    incremental_hierarchy_content_candidate.take();
                                selected_checkpoint_delta = true;
                            }
                        }
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            mailbox = %principal.email,
                            request_type = "Execute",
                            request_rop_id = "0x77",
                            folder_id = format_args!("0x{:016x}", *folder_id),
                            upload_state_total_bytes = state.len(),
                            upload_state_stream_bytes = uploaded_bytes,
                            upload_state_client_bytes = *client_state_uploaded_bytes,
                            upload_state_selected_checkpoint_delta = selected_checkpoint_delta,
                            transfer_buffer_bytes = transfer_buffer.len(),
                            transfer_position = *transfer_position,
                            "rca debug mapi sync upload state end"
                        );
                        responses.extend_from_slice(&rop_simple_success_response(&request));
                    }
                    Some(MapiObject::SynchronizationCollector {
                        folder_id,
                        state,
                        state_upload_buffer,
                        ..
                    }) => {
                        commit_uploaded_sync_state(state, state_upload_buffer);
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            mailbox = %principal.email,
                            request_type = "Execute",
                            request_rop_id = "0x77",
                            folder_id = format_args!("0x{:016x}", *folder_id),
                            upload_state_total_bytes = state.len(),
                            "rca debug mapi sync upload state end"
                        );
                        responses.extend_from_slice(&rop_simple_success_response(&request));
                    }
                    _ => responses.extend_from_slice(&rop_error_response(
                        0x77,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            Some(RopId::SynchronizationOpenCollector) => {
                let Some(folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x7E,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::SynchronizationCollector {
                        folder_id,
                        mailbox_id: sync_checkpoint_mailbox_id(folder_id, 0x01, mailboxes),
                        checkpoint_kind: MapiCheckpointKind::Content,
                        state: Vec::new(),
                        state_upload_buffer: Vec::new(),
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_simple_success_response(&request));
                output_handles.push(handle);
            }
            Some(RopId::GetPerUserGuidLongTermIds) => {
                let Some((
                    folder_id,
                    mailbox_id,
                    checkpoint_kind,
                    checkpoint_change_sequence,
                    checkpoint_modseq,
                    sync_type,
                    state,
                )) = synchronization_context_state(input_object(session, &handle_slots, &request))
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x82,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                };
                let transfer_buffer = if state.is_empty() && matches!(sync_type, 0x01 | 0x02) {
                    let sync_mailboxes = sync_mailboxes_for(folder_id, sync_type, mailboxes);
                    let sync_emails = sync_emails_for(folder_id, sync_type, mailboxes, emails);
                    let sync_attachment_facts =
                        sync_attachment_facts_for(folder_id, &sync_emails, snapshot);
                    mapi_mailstore::sync_state_token_with_attachments(
                        sync_type,
                        folder_id,
                        &sync_mailboxes,
                        &sync_emails,
                        &sync_attachment_facts,
                    )
                } else {
                    state
                };
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::SynchronizationSource {
                        folder_id,
                        mailbox_id,
                        checkpoint_kind,
                        checkpoint_change_sequence,
                        checkpoint_modseq,
                        sync_type,
                        state: transfer_buffer.clone(),
                        state_upload_buffer: Vec::new(),
                        client_state_uploaded_bytes: 0,
                        incremental_transfer_buffer: None,
                        hierarchy_content_candidate: None,
                        incremental_hierarchy_content_candidate: None,
                        transfer_buffer,
                        transfer_position: 0,
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses
                    .extend_from_slice(&rop_synchronization_get_transfer_state_response(&request));
                output_handles.push(handle);
            }
            Some(RopId::SynchronizationImportMessageChange) => {
                let Some(folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x72,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let property_values = match request.import_property_values() {
                    Ok(values) => values,
                    Err(_) => {
                        responses.extend_from_slice(&rop_error_response(
                            0x72,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        continue;
                    }
                };
                let message_id = request.import_message_id().unwrap_or(0);
                if message_id != 0
                    && message_for_id(folder_id, message_id, mailboxes, emails).is_some()
                {
                    if apply_canonical_message_property_values(
                        store,
                        principal,
                        folder_id,
                        message_id,
                        property_values,
                        mailboxes,
                        emails,
                    )
                    .await
                    .is_err()
                    {
                        responses.extend_from_slice(&rop_error_response(
                            0x72,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        continue;
                    }
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::Message {
                            folder_id,
                            message_id,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(
                        &rop_synchronization_import_message_change_response(&request, message_id),
                    );
                    output_handles.push(handle);
                } else if message_id != 0 && snapshot.note_for_id(folder_id, message_id).is_some() {
                    if apply_canonical_note_property_values(
                        store,
                        principal,
                        folder_id,
                        message_id,
                        property_values,
                        snapshot,
                    )
                    .await
                    .is_err()
                    {
                        responses.extend_from_slice(&rop_error_response(
                            0x72,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        continue;
                    }
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::Note {
                            folder_id,
                            note_id: message_id,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(
                        &rop_synchronization_import_message_change_response(&request, message_id),
                    );
                    output_handles.push(handle);
                } else if message_id != 0
                    && snapshot
                        .journal_entry_for_id(folder_id, message_id)
                        .is_some()
                {
                    if apply_canonical_journal_entry_property_values(
                        store,
                        principal,
                        folder_id,
                        message_id,
                        property_values,
                        snapshot,
                    )
                    .await
                    .is_err()
                    {
                        responses.extend_from_slice(&rop_error_response(
                            0x72,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        continue;
                    }
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::JournalEntry {
                            folder_id,
                            journal_entry_id: message_id,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(
                        &rop_synchronization_import_message_change_response(&request, message_id),
                    );
                    output_handles.push(handle);
                } else {
                    let pending_object = match folder_id {
                        NOTES_FOLDER_ID => MapiObject::PendingNote {
                            folder_id,
                            properties: property_values.into_iter().collect(),
                        },
                        JOURNAL_FOLDER_ID => MapiObject::PendingJournalEntry {
                            folder_id,
                            properties: property_values.into_iter().collect(),
                        },
                        _ => MapiObject::PendingMessage {
                            folder_id,
                            properties: property_values.into_iter().collect(),
                            recipients: Vec::new(),
                        },
                    };
                    let handle =
                        session.allocate_output_handle(request.output_handle_index, pending_object);
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(
                        &rop_synchronization_import_message_change_response(&request, 0),
                    );
                    output_handles.push(handle);
                }
            }
            Some(RopId::SynchronizationImportHierarchyChange) => {
                let Some(_folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x73,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let (hierarchy_values, property_values) = match request.import_hierarchy_values() {
                    Ok(values) => values,
                    Err(_) => {
                        responses.extend_from_slice(&rop_error_response(
                            0x73,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        continue;
                    }
                };
                let display_name = hierarchy_display_name(&hierarchy_values, &property_values);
                let Some(display_name) = display_name else {
                    responses.extend_from_slice(&rop_error_response(
                        0x73,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                };
                if system_folder_display_name(&display_name) {
                    responses.extend_from_slice(&rop_error_response(
                        0x73,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                }
                if let Some(existing) =
                    imported_hierarchy_existing_mailbox(&hierarchy_values, &display_name, mailboxes)
                {
                    if existing.role == "custom"
                        && existing.name.eq_ignore_ascii_case(&display_name)
                    {
                        responses.extend_from_slice(
                            &rop_synchronization_import_hierarchy_change_response(
                                &request,
                                mapi_folder_id(existing),
                            ),
                        );
                    } else {
                        responses.extend_from_slice(&rop_error_response(
                            0x73,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                    }
                    continue;
                }

                match store
                    .create_jmap_mailbox(
                        JmapMailboxCreateInput {
                            account_id: principal.account_id,
                            name: display_name.clone(),
                            parent_id: None,
                            sort_order: None,
                            is_subscribed: true,
                        },
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "mapi-sync-import-hierarchy-change".to_string(),
                            subject: display_name.clone(),
                        },
                    )
                    .await
                {
                    Ok(mailbox) => {
                        let folder_id = match remember_created_mapi_identity(
                            store,
                            principal,
                            MapiIdentityObjectKind::Mailbox,
                            mailbox.id,
                            None,
                        )
                        .await
                        {
                            Ok(folder_id) => folder_id,
                            Err(_) => {
                                responses.extend_from_slice(&rop_error_response(
                                    0x73,
                                    request.response_handle_index(),
                                    0x8004_0102,
                                ));
                                continue;
                            }
                        };
                        responses.extend_from_slice(
                            &rop_synchronization_import_hierarchy_change_response(
                                &request, folder_id,
                            ),
                        );
                    }
                    Err(_) => responses.extend_from_slice(&rop_error_response(
                        0x73,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            Some(RopId::SynchronizationImportDeletes) => {
                let Some(folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x74,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let mut partial_completion = false;
                let hard_delete = request.import_delete_hard_delete();
                for message_id in request.import_delete_message_ids() {
                    if let Some(note) = snapshot.note_for_id(folder_id, message_id) {
                        if store
                            .delete_mapi_note(principal.account_id, note.canonical_id)
                            .await
                            .is_err()
                        {
                            partial_completion = true;
                        }
                        continue;
                    }
                    if let Some(entry) = snapshot.journal_entry_for_id(folder_id, message_id) {
                        if store
                            .delete_mapi_journal_entry(principal.account_id, entry.canonical_id)
                            .await
                            .is_err()
                        {
                            partial_completion = true;
                        }
                        continue;
                    }
                    let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails)
                    else {
                        partial_completion = true;
                        continue;
                    };
                    let result = if hard_delete || email.mailbox_role == "trash" {
                        store
                            .delete_jmap_email_from_mailbox(
                                principal.account_id,
                                email.mailbox_id,
                                email.id,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-sync-import-hard-delete".to_string(),
                                    subject: format!("message:{}", email.id),
                                },
                            )
                            .await
                            .map(|_| ())
                    } else if let Some(trash_mailbox) =
                        mailboxes.iter().find(|mailbox| mailbox.role == "trash")
                    {
                        store
                            .move_jmap_email_from_mailbox(
                                principal.account_id,
                                email.mailbox_id,
                                email.id,
                                trash_mailbox.id,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-sync-import-soft-delete".to_string(),
                                    subject: format!("message:{}->{}", email.id, trash_mailbox.id),
                                },
                            )
                            .await
                            .map(|_| ())
                    } else {
                        store
                            .delete_jmap_email_from_mailbox(
                                principal.account_id,
                                email.mailbox_id,
                                email.id,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-sync-import-delete-without-trash".to_string(),
                                    subject: format!("message:{}", email.id),
                                },
                            )
                            .await
                            .map(|_| ())
                    };
                    if result.is_err() {
                        partial_completion = true;
                    }
                }
                responses.extend_from_slice(&rop_partial_completion_response(
                    0x74,
                    request.response_handle_index(),
                    partial_completion,
                ));
            }
            Some(RopId::SynchronizationImportMessageMove) => {
                let Some((message_id, target_folder_id)) = request.import_move() else {
                    responses.extend_from_slice(&rop_error_response(
                        0x78,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                };
                let source_folder_id = input_object(session, &handle_slots, &request)
                    .and_then(MapiObject::folder_id)
                    .unwrap_or(INBOX_FOLDER_ID);
                if let Some(note) = snapshot.note_for_id(source_folder_id, message_id) {
                    if target_folder_id == NOTES_FOLDER_ID {
                        responses.extend_from_slice(
                            &rop_synchronization_import_message_move_response(&request, note.id),
                        );
                    } else {
                        responses.extend_from_slice(&rop_error_response(
                            0x78,
                            request.response_handle_index(),
                            0x8004_010F,
                        ));
                    }
                    continue;
                }
                if let Some(entry) = snapshot.journal_entry_for_id(source_folder_id, message_id) {
                    if target_folder_id == JOURNAL_FOLDER_ID {
                        responses.extend_from_slice(
                            &rop_synchronization_import_message_move_response(&request, entry.id),
                        );
                    } else {
                        responses.extend_from_slice(&rop_error_response(
                            0x78,
                            request.response_handle_index(),
                            0x8004_010F,
                        ));
                    }
                    continue;
                }
                let Some(email) = message_for_id(source_folder_id, message_id, mailboxes, emails)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x78,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let Some(target_mailbox) = folder_row_for_id(target_folder_id, mailboxes) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x78,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                match store
                    .move_jmap_email_from_mailbox(
                        principal.account_id,
                        email.mailbox_id,
                        email.id,
                        target_mailbox.id,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "mapi-sync-import-move".to_string(),
                            subject: format!("message:{}->{}", email.id, target_mailbox.id),
                        },
                    )
                    .await
                {
                    Ok(moved) => {
                        let moved_id = match remember_created_mapi_identity(
                            store,
                            principal,
                            MapiIdentityObjectKind::Message,
                            moved.id,
                            None,
                        )
                        .await
                        {
                            Ok(moved_id) => moved_id,
                            Err(_) => {
                                responses.extend_from_slice(&rop_error_response(
                                    0x78,
                                    request.response_handle_index(),
                                    0x8004_010F,
                                ));
                                continue;
                            }
                        };
                        responses.extend_from_slice(
                            &rop_synchronization_import_message_move_response(&request, moved_id),
                        );
                    }
                    Err(_) => responses.extend_from_slice(&rop_error_response(
                        0x78,
                        request.response_handle_index(),
                        0x8004_010F,
                    )),
                }
            }
            Some(RopId::SetLocalReplicaMidsetDeletedNoReplicate) => {
                let folder_id = input_object(session, &handle_slots, &request)
                    .and_then(MapiObject::folder_id)
                    .unwrap_or(INBOX_FOLDER_ID);
                let mut partial_completion = false;
                for (message_id, unread) in request.import_read_state_changes() {
                    let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails)
                    else {
                        partial_completion = true;
                        continue;
                    };
                    if store
                        .update_jmap_email_flags(
                            principal.account_id,
                            email.id,
                            Some(unread),
                            None,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "mapi-sync-import-read-state".to_string(),
                                subject: format!("message:{}", email.id),
                            },
                        )
                        .await
                        .is_err()
                    {
                        partial_completion = true;
                    }
                }
                responses.extend_from_slice(&rop_partial_completion_response(
                    0x80,
                    request.response_handle_index(),
                    partial_completion,
                ));
            }
            Some(RopId::SetLocalReplicaMidsetDeletedExtended) => {
                match input_object_mut(session, &handle_slots, &request) {
                    Some(MapiObject::SynchronizationSource { state, .. })
                    | Some(MapiObject::SynchronizationCollector { state, .. }) => {
                        state.extend_from_slice(request.local_replica_midset_deleted());
                        responses.extend_from_slice(&rop_simple_success_response(&request));
                    }
                    _ => responses.extend_from_slice(&rop_error_response(
                        0x93,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            Some(RopId::GetLocalReplicaIds) => {
                echo_input_handle_table = true;
                let (first_global_counter, _) = mapi_mailstore::local_replica_id_range(
                    principal.account_id,
                    request.local_replica_id_count(),
                    session.next_local_replica_sequence,
                );
                session.next_local_replica_sequence =
                    session.next_local_replica_sequence.saturating_add(1).max(1);
                responses.extend_from_slice(&rop_get_local_replica_ids_response(
                    &request,
                    first_global_counter,
                ));
            }
            Some(RopId::HardDeleteMessagesAndSubfolders) => responses.extend_from_slice(
                &rop_error_response(request.rop_id, request.response_handle_index(), 0x8004_0102),
            ),
            Some(RopId::GetTransportFolder) => {
                responses.extend_from_slice(&rop_get_transport_folder_response(&request))
            }
            Some(RopId::OptionsData) => {
                responses.extend_from_slice(&rop_options_data_response(&request))
            }
            Some(RopId::GetReceiveFolderTable) => {
                responses.extend_from_slice(&rop_get_receive_folder_table_response(&request))
            }
            Some(RopId::LongTermIdFromId) => {
                responses.extend_from_slice(&rop_long_term_id_from_id_response(&request))
            }
            Some(RopId::IdFromLongTermId) => {
                responses.extend_from_slice(&rop_id_from_long_term_id_response(&request))
            }
            Some(RopId::PublicFolderIsGhosted) => {
                responses.extend_from_slice(&rop_public_folder_is_ghosted_response(&request))
            }
            Some(RopId::GetAddressTypes) => {
                echo_input_handle_table = true;
                responses.extend_from_slice(&rop_get_address_types_response(&request));
            }
            Some(RopId::GetNamesFromPropertyIds) => responses
                .extend_from_slice(&rop_get_names_from_property_ids_response(&request, session)),
            Some(RopId::GetPropertyIdsFromNames) => {
                echo_input_handle_table = true;
                let properties = match request.named_property_names() {
                    Ok(properties) => properties,
                    Err(_) => {
                        responses.extend_from_slice(&rop_error_response(
                            0x56,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        continue;
                    }
                };
                if properties.is_empty()
                    && matches!(
                        input_object(session, &handle_slots, &request),
                        Some(MapiObject::Logon)
                    )
                {
                    let property_ids = session
                        .named_properties_for_query(None)
                        .into_iter()
                        .map(|(property_id, _property)| property_id)
                        .collect::<Vec<_>>();
                    responses.extend_from_slice(&rop_get_property_ids_from_names_response(
                        &request,
                        &property_ids,
                    ));
                    continue;
                }
                let mut property_ids = Vec::with_capacity(properties.len());
                let mut exhausted = false;
                for property in properties {
                    match session.property_id_for_name(property, request.named_property_create()) {
                        Some(property_id) => property_ids.push(property_id),
                        None if request.named_property_create() => {
                            exhausted = true;
                            break;
                        }
                        None => property_ids.push(0),
                    }
                }
                if exhausted {
                    responses.extend_from_slice(&rop_error_response(
                        0x56,
                        request.response_handle_index(),
                        0x8007_000E,
                    ));
                } else {
                    responses.extend_from_slice(&rop_get_property_ids_from_names_response(
                        &request,
                        &property_ids,
                    ));
                }
            }
            Some(RopId::QueryNamedProperties) => {
                responses.extend_from_slice(&rop_query_named_properties_response(&request, session))
            }
            Some(RopId::RegisterNotification) => {
                let notification_types = request.notification_types().unwrap_or(0);
                if !supported_notification_types(notification_types) {
                    responses.extend_from_slice(&unsupported_rop_response(
                        0x29,
                        request.response_handle_index(),
                    ));
                    continue;
                }
                let registration = notification_registration_from_request(&request);
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
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_register_notification_response(&request));
                output_handles.push(handle);
            }
            Some(RopId::GetPermissionsTable) => {
                let Some(folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_handle_index_error_response(&request));
                    continue;
                };
                if folder_row_for_id(folder_id, mailboxes).is_none()
                    && role_for_folder_id(folder_id).is_none()
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x3E,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                }
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::PermissionTable {
                        folder_id,
                        columns: default_permission_columns(),
                        position: 0,
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_get_permissions_table_response(&request));
                output_handles.push(handle);
            }
            Some(RopId::ModifyPermissions) => {
                responses.extend_from_slice(&rop_modify_permissions_response(&request))
            }
            Some(RopId::GetStoreState) => {
                responses.extend_from_slice(&rop_get_store_state_response(&request))
            }
            Some(RopId::ResetTable) => {
                if input_object_mut(session, &handle_slots, &request)
                    .is_some_and(reset_table_position)
                {
                    responses.extend_from_slice(&rop_reset_table_response(&request));
                } else {
                    responses.extend_from_slice(&rop_error_response(
                        0x81,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                }
            }
            Some(RopId::GetAttachmentTableExtended) => {
                responses.extend_from_slice(&rop_free_bookmark_response(
                    &request,
                    input_object_mut(session, &handle_slots, &request),
                ))
            }
            Some(RopId::Logon) => {
                if request.payload.first().copied().unwrap_or(0) & 0x01 == 0 {
                    responses.extend_from_slice(&unsupported_rop_response(
                        0xFE,
                        request.response_handle_index(),
                    ));
                    continue;
                }
                let handle =
                    session.allocate_output_handle(request.output_handle_index, MapiObject::Logon);
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_logon_response_body(principal, &request));
                output_handles.push(handle);
            }
            Some(rop_id) => responses.extend_from_slice(&unsupported_rop_response(
                rop_id.as_u8(),
                request.response_handle_index(),
            )),
            None => responses.extend_from_slice(&unsupported_rop_response(
                request.rop_id,
                request.response_handle_index(),
            )),
        }
        if let Some(sync_root_folder_id) = completed_hierarchy_sync_root {
            session.record_completed_hierarchy_sync(sync_root_folder_id);
        }
        if content_sync_configure_observed {
            session.record_content_sync_configure();
        }
        if typed_request.unsupported_is_terminal() {
            break;
        }
    }
    let response_handles =
        response_handle_table(&handle_slots, &output_handles, echo_input_handle_table);
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

async fn mapi_submit_attachments_from_email<S>(
    store: &S,
    account_id: Uuid,
    email: &JmapEmail,
) -> Result<Vec<AttachmentUploadInput>>
where
    S: ExchangeStore,
{
    if !email.has_attachments {
        return Ok(Vec::new());
    }

    let attachments = store
        .fetch_message_attachments(account_id, email.id)
        .await?;
    let mut uploads = Vec::with_capacity(attachments.len());
    for attachment in attachments {
        let Some(content) = store
            .fetch_attachment_content(account_id, &attachment.file_reference)
            .await?
        else {
            return Err(anyhow::anyhow!(
                "missing attachment content for {}",
                attachment.file_reference
            ));
        };
        uploads.push(AttachmentUploadInput {
            file_name: content.file_name,
            media_type: content.media_type,
            disposition: None,
            content_id: None,
            blob_bytes: content.blob_bytes,
        });
    }
    Ok(uploads)
}

async fn mapi_message_ids_for_deleted_changes<S>(
    store: &S,
    principal: &AccountPrincipal,
    message_ids: &[Uuid],
) -> Result<Vec<u64>>
where
    S: ExchangeStore,
{
    mapi_object_ids_for_deleted_changes(
        store,
        principal,
        MapiIdentityObjectKind::Message,
        message_ids,
    )
    .await
}

async fn mapi_object_ids_for_deleted_changes<S>(
    store: &S,
    principal: &AccountPrincipal,
    object_kind: MapiIdentityObjectKind,
    object_ids: &[Uuid],
) -> Result<Vec<u64>>
where
    S: ExchangeStore,
{
    let requests = object_ids
        .iter()
        .map(|object_id| MapiIdentityRequest {
            object_kind,
            canonical_id: *object_id,
            reserved_global_counter: None,
        })
        .collect::<Vec<_>>();
    let identities = store
        .fetch_or_allocate_mapi_identities(principal.account_id, &requests)
        .await?;
    for identity in &identities {
        crate::mapi::identity::remember_mapi_identity(identity.canonical_id, identity.object_id);
    }
    Ok(identities
        .into_iter()
        .map(|identity| identity.object_id)
        .collect())
}

async fn remember_created_mapi_identity<S>(
    store: &S,
    principal: &AccountPrincipal,
    object_kind: MapiIdentityObjectKind,
    canonical_id: Uuid,
    reserved_global_counter: Option<u64>,
) -> Result<u64>
where
    S: ExchangeStore,
{
    let requests = [MapiIdentityRequest {
        object_kind,
        canonical_id,
        reserved_global_counter,
    }];
    let records = store
        .fetch_or_allocate_mapi_identities(principal.account_id, &requests)
        .await?;
    let object_id = records
        .first()
        .map(|record| record.object_id)
        .ok_or_else(|| anyhow::anyhow!("MAPI identity allocator returned no record"))?;
    crate::mapi::identity::remember_mapi_identity(canonical_id, object_id);
    Ok(object_id)
}

fn hierarchy_checkpoint_status(
    checkpoint_kind: MapiCheckpointKind,
    folder_id: u64,
    checkpoint: &MapiSyncCheckpoint,
) -> &'static str {
    if checkpoint_kind != MapiCheckpointKind::Hierarchy {
        return "usable";
    }
    if checkpoint
        .cursor_json
        .get("source")
        .and_then(serde_json::Value::as_str)
        != Some("emsmdb-ics-download")
    {
        return "stale-source";
    }
    if checkpoint
        .cursor_json
        .get("hierarchySyncVersion")
        .and_then(serde_json::Value::as_u64)
        != Some(HIERARCHY_SYNC_CURSOR_VERSION)
    {
        return "stale-version";
    }
    if checkpoint
        .cursor_json
        .get("syncRootFolderId")
        .and_then(serde_json::Value::as_u64)
        != Some(folder_id)
    {
        return "stale-root";
    }
    "usable"
}

fn sync_property_filter_mode(sync_flags: u16, requested_property_tags: &[u32]) -> &'static str {
    if requested_property_tags.is_empty() {
        "none"
    } else if sync_flags & 0x0080 == 0 {
        "exclude"
    } else {
        "only-specified"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execute_rop_debug_summary_decodes_ids_and_return_codes() {
        let mut request_bytes = vec![0x02, 0, 0, 1];
        request_bytes.extend_from_slice(&ROOT_FOLDER_ID.to_le_bytes());
        request_bytes.push(0);
        let request_buffer = rop_buffer_with_response(request_bytes, &[0]);
        let request_summary = summarize_request_rop_buffer(&request_buffer);

        assert_eq!(request_summary.ids, vec![0x02]);
        assert_eq!(request_summary.ids_csv, "0x02");
        assert_eq!(request_summary.handle_count, 1);
        assert!(request_summary.parse_error.is_empty());

        let request = RopRequest {
            rop_id: 0x02,
            input_handle_index: Some(0),
            output_handle_index: Some(1),
            payload: Vec::new(),
        };
        let response_buffer = rop_buffer_with_response(rop_open_folder_response(&request), &[42]);
        let response_summary =
            summarize_response_rop_buffer(&response_buffer, &request_summary.ids);

        assert_eq!(response_summary.ids_csv, "0x02");
        assert_eq!(response_summary.results_csv, "0x02:0x00000000");
        assert_eq!(response_summary.count, 1);
        assert_eq!(response_summary.handle_count, 1);
        assert!(response_summary.parse_error.is_empty());
    }

    #[test]
    fn execute_rop_debug_summary_skips_release_rops_without_responses() {
        let request = RopRequest {
            rop_id: 0x7F,
            input_handle_index: Some(1),
            output_handle_index: None,
            payload: 2u32.to_le_bytes().to_vec(),
        };
        let response_buffer =
            rop_buffer_with_response(rop_get_local_replica_ids_response(&request, 42), &[42]);
        let response_summary = summarize_response_rop_buffer(&response_buffer, &[0x01, 0x7F]);

        assert_eq!(response_summary.ids_csv, "0x7f");
        assert_eq!(response_summary.results_csv, "0x7f:0x00000000");
        assert_eq!(response_summary.count, 1);
        assert_eq!(response_summary.handle_count, 1);
        assert!(response_summary.parse_error.is_empty());
    }

    #[test]
    fn logon_response_debug_summary_decodes_private_mailbox_fields() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::from_u128(0xaaaaaaaa_aaaa_aaaa_aaaa_aaaaaaaaaaaa),
            account_id: Uuid::from_u128(0xbbbbbbbb_bbbb_bbbb_bbbb_bbbbbbbbbbbb),
            email: "alice@example.test".to_string(),
            display_name: "Alice".to_string(),
        };
        let request = RopRequest {
            rop_id: 0xFE,
            input_handle_index: Some(0),
            output_handle_index: Some(1),
            payload: vec![0x01],
        };
        let response_buffer =
            rop_buffer_with_response(rop_logon_response_body(&principal, &request), &[42]);

        let summary = summarize_logon_response_rop(&response_buffer, &[0xFE]);

        assert!(summary.present);
        assert_eq!(summary.output_handle_index, "1");
        assert_eq!(summary.error_code, "0x00000000");
        assert_eq!(summary.logon_flags, "0x01");
        assert!(summary
            .special_folder_ids
            .starts_with(&format!("{ROOT_FOLDER_ID:#018x}")));
        assert_eq!(summary.response_flags, "0x07");
        assert_eq!(summary.mailbox_guid, principal.account_id.to_string());
        assert_eq!(summary.replid, "1");
        assert_eq!(summary.replica_guid.len(), 32);
        assert!(summary.parse_error.is_empty());
    }

    #[test]
    fn first_post_hierarchy_probe_summary_identifies_open_folder_and_getprops_shapes() {
        let mut request_bytes = vec![0x02, 0x00, 0x00, 0x01];
        request_bytes.extend_from_slice(&CALENDAR_FOLDER_ID.to_le_bytes());
        request_bytes.push(0);
        request_bytes.extend_from_slice(&[0x07, 0x00, 0x01]);
        request_bytes.extend_from_slice(&4096u16.to_le_bytes());
        request_bytes.extend_from_slice(&2u16.to_le_bytes());
        request_bytes.extend_from_slice(&PID_TAG_DISPLAY_NAME_W.to_le_bytes());
        request_bytes.extend_from_slice(&PID_TAG_CONTENT_COUNT.to_le_bytes());
        let request_buffer = rop_buffer_with_response(request_bytes, &[1, u32::MAX]);

        let open_folder_request = RopRequest {
            rop_id: 0x02,
            input_handle_index: Some(0),
            output_handle_index: Some(1),
            payload: Vec::new(),
        };
        let mut responses = rop_open_folder_response(&open_folder_request);
        responses.extend_from_slice(&[0x07, 0x01]);
        responses.extend_from_slice(&0u32.to_le_bytes());
        responses.push(0);
        responses.extend_from_slice(&utf16z_bytes("Calendar"));
        responses.extend_from_slice(&0u32.to_le_bytes());
        let response_buffer = rop_buffer_with_response(responses, &[1]);

        let summary = summarize_first_post_hierarchy_probe(&request_buffer, &response_buffer);

        assert_eq!(summary.open_folder_request_count, 1);
        assert!(summary
            .open_folder_requests
            .contains(&format!("folder=0x{CALENDAR_FOLDER_ID:016x};name=calendar")));
        assert!(summary
            .open_folder_response_shapes
            .contains("result=0x00000000;has_rules=0;is_ghosted=0"));
        assert_eq!(summary.get_properties_specific_request_count, 1);
        assert!(summary
            .get_properties_specific_requests
            .contains("tags=0x3001001f,0x36020003"));
        assert!(summary
            .get_properties_specific_response_shapes
            .contains("result=0x00000000;row=standard"));
        assert!(summary.parse_error.is_empty());
    }

    #[test]
    fn post_hierarchy_probe_summary_marks_default_folder_entry_id_getprops() {
        let mut request_bytes = vec![0x07, 0x00, 0x01];
        request_bytes.extend_from_slice(&4096u16.to_le_bytes());
        request_bytes.extend_from_slice(&1u16.to_le_bytes());
        request_bytes.extend_from_slice(&PID_TAG_IPM_APPOINTMENT_ENTRY_ID.to_le_bytes());
        let request_buffer = rop_buffer_with_response(request_bytes, &[1]);

        let mut responses = vec![0x07, 0x01];
        responses.extend_from_slice(&0u32.to_le_bytes());
        responses.push(0);
        responses.extend_from_slice(&24u16.to_le_bytes());
        responses.extend_from_slice(&[0xAA; 24]);
        let response_buffer = rop_buffer_with_response(responses, &[1]);

        let summary = summarize_first_post_hierarchy_probe(&request_buffer, &response_buffer);

        assert!(summary
            .get_properties_specific_response_shapes
            .contains("values=0x36d00102:binary:bytes=24"));
        assert!(summary.parse_error.is_empty());
    }

    #[test]
    fn root_default_folder_setprops_are_reused_by_next_root_getprops() {
        let mut session = test_emsmdb_session();
        let root = MapiObject::Folder {
            folder_id: ROOT_FOLDER_ID,
            properties: HashMap::new(),
        };
        let client_value = vec![0xAA; 24];
        let values = vec![
            (
                PID_TAG_IPM_APPOINTMENT_ENTRY_ID,
                MapiValue::Binary(client_value),
            ),
            (
                PID_TAG_DISPLAY_NAME_W,
                MapiValue::String("not-a-root-default-folder-prop".to_string()),
            ),
        ];

        remember_root_default_folder_properties(&mut session, Some(&root), &values);
        let reopened_root = MapiObject::Folder {
            folder_id: ROOT_FOLDER_ID,
            properties: session.root_default_folder_properties.clone(),
        };
        let request = get_properties_specific_request(&[PID_TAG_IPM_APPOINTMENT_ENTRY_ID]);
        let response = rop_get_properties_specific_response(
            &request,
            Some(&reopened_root),
            &test_principal(),
            &[],
            &[],
            &empty_snapshot(),
        );

        let mut cursor = Cursor::new(&response[7..]);
        let expected_value = crate::mapi::identity::long_term_id_from_object_id(CALENDAR_FOLDER_ID)
            .unwrap()
            .to_vec();
        assert_eq!(
            parse_property_value_for_tag(&mut cursor, PID_TAG_IPM_APPOINTMENT_ENTRY_ID).unwrap(),
            MapiValue::Binary(expected_value)
        );
        let MapiObject::Folder { properties, .. } = &reopened_root else {
            panic!("expected reopened root folder object");
        };
        assert!(!properties.contains_key(&PID_TAG_IPM_APPOINTMENT_ENTRY_ID));
        assert!(!properties.contains_key(&canonical_property_storage_tag(PID_TAG_DISPLAY_NAME_W)));
    }

    #[test]
    fn first_post_hierarchy_probe_summary_identifies_set_properties_shapes() {
        let mut property_value = Vec::new();
        property_value.extend_from_slice(&PID_TAG_IPM_APPOINTMENT_ENTRY_ID.to_le_bytes());
        property_value.extend_from_slice(&3u16.to_le_bytes());
        property_value.extend_from_slice(&[0xAA, 0xBB, 0xCC]);
        let property_value_size = property_value.len() + 2;
        let mut request_bytes = vec![0x0A, 0x00, 0x01];
        request_bytes.extend_from_slice(&(property_value_size as u16).to_le_bytes());
        request_bytes.extend_from_slice(&1u16.to_le_bytes());
        request_bytes.extend_from_slice(&property_value);
        let request_buffer = rop_buffer_with_response(request_bytes, &[1]);

        let request = RopRequest {
            rop_id: 0x0A,
            input_handle_index: Some(1),
            output_handle_index: None,
            payload: Vec::new(),
        };
        let response_buffer = rop_buffer_with_response(rop_set_properties_response(&request), &[1]);

        let summary = summarize_first_post_hierarchy_probe(&request_buffer, &response_buffer);

        assert_eq!(summary.set_properties_request_count, 1);
        assert!(summary
            .set_properties_requests
            .contains("tags=0x36d00102;values=0x36d00102:binary:bytes=3"));
        assert!(summary
            .set_properties_response_shapes
            .contains("result=0x00000000;property_problem_count=0"));
        assert!(summary.parse_error.is_empty());
    }

    fn utf16z_bytes(value: &str) -> Vec<u8> {
        value
            .encode_utf16()
            .chain(std::iter::once(0))
            .flat_map(u16::to_le_bytes)
            .collect()
    }

    fn test_emsmdb_session() -> MapiSession {
        MapiSession {
            endpoint: MapiEndpoint::Emsmdb,
            tenant_id: Uuid::from_u128(0xaaaaaaaa_aaaa_aaaa_aaaa_aaaaaaaaaaaa),
            account_id: Uuid::from_u128(0xbbbbbbbb_bbbb_bbbb_bbbb_bbbbbbbbbbbb),
            email: "alice@example.test".to_string(),
            last_seen_at: SystemTime::UNIX_EPOCH,
            next_handle: 1,
            handles: HashMap::new(),
            message_statuses: HashMap::new(),
            root_default_folder_properties: HashMap::new(),
            named_properties: HashMap::new(),
            named_property_ids: HashMap::new(),
            next_named_property_id: FIRST_NAMED_PROPERTY_ID,
            next_local_replica_sequence: 1,
            notification_cursor: None,
            pending_notifications: VecDeque::new(),
            completed_execute_requests: HashMap::new(),
            completed_execute_request_order: VecDeque::new(),
            post_hierarchy_actions: PostHierarchyActionState::default(),
        }
    }

    fn get_properties_specific_request(property_tags: &[u32]) -> RopRequest {
        let mut payload = Vec::new();
        payload.extend_from_slice(&4096u16.to_le_bytes());
        payload.extend_from_slice(&(property_tags.len() as u16).to_le_bytes());
        for tag in property_tags {
            payload.extend_from_slice(&tag.to_le_bytes());
        }
        RopRequest {
            rop_id: 0x07,
            input_handle_index: Some(0),
            output_handle_index: None,
            payload,
        }
    }

    fn test_principal() -> AccountPrincipal {
        AccountPrincipal {
            tenant_id: Uuid::from_u128(0xaaaaaaaa_aaaa_aaaa_aaaa_aaaaaaaaaaaa),
            account_id: Uuid::from_u128(0xbbbbbbbb_bbbb_bbbb_bbbb_bbbbbbbbbbbb),
            email: "alice@example.test".to_string(),
            display_name: "Alice".to_string(),
        }
    }

    fn empty_snapshot() -> MapiMailStoreSnapshot {
        MapiMailStoreSnapshot::new(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
    }
}
