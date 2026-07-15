use super::*;

pub(in crate::mapi) static MAPI_SESSIONS: OnceLock<Mutex<HashMap<String, MapiSession>>> =
    OnceLock::new();
pub(in crate::mapi) static MAPI_ACTIVE_SESSION_REQUESTS: OnceLock<Mutex<HashSet<String>>> =
    OnceLock::new();

pub(in crate::mapi) fn sessions() -> &'static Mutex<HashMap<String, MapiSession>> {
    MAPI_SESSIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(in crate::mapi) fn active_session_requests() -> &'static Mutex<HashSet<String>> {
    MAPI_ACTIVE_SESSION_REQUESTS.get_or_init(|| Mutex::new(HashSet::new()))
}

pub(in crate::mapi) struct ActiveSessionRequest {
    session_id: String,
}

impl Drop for ActiveSessionRequest {
    fn drop(&mut self) {
        let mut guard = active_session_requests()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.remove(&self.session_id);
    }
}

pub(in crate::mapi) fn begin_active_session_request(
    session_id: &str,
) -> Option<ActiveSessionRequest> {
    let mut guard = active_session_requests()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if guard.insert(session_id.to_string()) {
        Some(ActiveSessionRequest {
            session_id: session_id.to_string(),
        })
    } else {
        None
    }
}

#[cfg(test)]
pub(crate) fn begin_active_session_request_for_test(session_id: &str) -> impl Drop {
    begin_active_session_request(session_id).expect("test session should not already be active")
}

pub(in crate::mapi) fn session_request_is_active(session_id: &str) -> bool {
    let guard = active_session_requests()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    guard.contains(session_id)
}

pub(in crate::mapi) fn reconnect_session(
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    headers: &HeaderMap,
    request_type: &str,
    request_id: &str,
) -> std::result::Result<Option<String>, Response> {
    let Some(previous_session_id) = request_cookie(endpoint, headers) else {
        return Ok(None);
    };
    if session_request_is_active(&previous_session_id) {
        return Err(mapi_diagnostic_response(
            request_type,
            request_id,
            15,
            "MAPI session already has an active request",
        ));
    }
    let Some(mut session) = remove_session(&previous_session_id) else {
        return Ok(None);
    };
    if !session_matches(&session, endpoint, principal) {
        store_session(previous_session_id, session);
        return Ok(None);
    }

    session.record_transport_request(request_type, request_id);
    let session_id = Uuid::new_v4().to_string();
    if endpoint == MapiEndpoint::Emsmdb {
        store_session(previous_session_id, session.clone());
    }
    store_session(session_id.clone(), session);
    Ok(Some(session_id))
}

pub(in crate::mapi) fn create_session(
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    request_type: &str,
    request_id: &str,
) -> String {
    let session_id = Uuid::new_v4().to_string();
    let now = SystemTime::now();
    let mut session = MapiSession {
        endpoint,
        tenant_id: principal.tenant_id,
        account_id: principal.account_id,
        email: principal.email.clone(),
        created_at: now,
        last_seen_at: now,
        first_request_type: String::new(),
        first_request_id: String::new(),
        last_request_type: String::new(),
        last_request_id: String::new(),
        request_count: 0,
        execute_request_count: 0,
        next_handle: 1,
        handles: HashMap::new(),
        message_statuses: HashMap::new(),
        message_save_generations: HashMap::new(),
        message_handle_generations: HashMap::new(),
        pending_message_recipient_replacements: HashMap::new(),
        pending_message_attachments: HashMap::new(),
        pending_attachment_parent_messages: HashMap::new(),
        pending_event_attachment_transactions: HashMap::new(),
        pending_attachment_deletions: HashSet::new(),
        pending_embedded_message_ids: HashMap::new(),
        pending_embedded_message_attachments: HashMap::new(),
        saved_embedded_messages: HashMap::new(),
        saved_search_folder_definitions: HashMap::new(),
        special_folder_aliases: HashMap::new(),
        deleted_advertised_special_folders: HashSet::new(),
        deleted_search_folder_definitions: HashSet::new(),
        named_properties: HashMap::new(),
        named_property_ids: HashMap::new(),
        next_named_property_id: FIRST_NAMED_PROPERTY_ID,
        next_local_replica_sequence: 1,
        notification_cursor: None,
        pending_notifications: VecDeque::new(),
        table_notification_eligible_handles: HashSet::new(),
        table_notification_active_handles: HashSet::new(),
        completed_execute_requests: HashMap::new(),
        completed_execute_request_order: VecDeque::new(),
        post_hierarchy_actions: PostHierarchyActionState::default(),
        default_view_advertisements: HashMap::new(),
        inbox_associated_config_stream_handles: HashSet::new(),
        inbox_rule_organizer_stream_handles: HashSet::new(),
        logon_identity: None,
        outlook_smart_input_variant: configured_smart_input_variant(),
        outlook_smart_input_variant_applied: false,
    };
    session.record_transport_request(request_type, request_id);
    let mut guard = sessions()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    prune_expired_sessions_locked(&mut guard, now);
    guard.insert(session_id.clone(), session);
    session_id
}

pub(crate) fn create_rpc_emsmdb_context(principal: &AccountPrincipal) -> [u8; 20] {
    let session_id = create_session(MapiEndpoint::Emsmdb, principal, "RpcConnect", "");
    let session_uuid = Uuid::parse_str(&session_id).unwrap_or_else(|_| Uuid::new_v4());
    let mut context = [0u8; 20];
    context[4..20].copy_from_slice(session_uuid.as_bytes());
    context
}

pub(crate) async fn execute_rpc_emsmdb_rops<S, V>(
    store: &S,
    validator: &Validator<V>,
    principal: &AccountPrincipal,
    context_handle: &[u8],
    rop_buffer: &[u8],
) -> Result<Vec<u8>>
where
    S: ExchangeStore,
    V: Detector,
{
    let session_id = rpc_context_session_id(context_handle)
        .ok_or_else(|| anyhow!("invalid RPC/HTTP EMSMDB context handle"))?;
    let Some(session) = get_session(&session_id) else {
        return Err(anyhow!("RPC/HTTP EMSMDB session context not found"));
    };
    if !session_matches(&session, MapiEndpoint::Emsmdb, principal) {
        return Err(anyhow!("RPC/HTTP EMSMDB authentication context changed"));
    }
    if rop_buffer.is_empty() {
        return Err(anyhow!("RPC/HTTP EMSMDB ROP payload is empty"));
    }

    let access_plan = plan_mapi_store_access(&session, rop_buffer);
    let mut snapshot =
        load_mapi_store_for_access_plan(store, principal.account_id, &access_plan, 500).await?;
    let mailboxes = snapshot.mailboxes();
    let emails = snapshot.emails();
    let Some(mut session) = remove_session(&session_id) else {
        return Err(anyhow!("RPC/HTTP EMSMDB session context not found"));
    };
    if !session_matches(&session, MapiEndpoint::Emsmdb, principal) {
        return Err(anyhow!("RPC/HTTP EMSMDB authentication context changed"));
    }
    let rop_buffer = execute_rops(
        store,
        principal,
        "rpc-http",
        &mut session,
        &mailboxes,
        &emails,
        &mut snapshot,
        validator,
        rop_buffer,
        false,
        0,
        "rpc_http_request_not_summarized",
        "",
        "",
        "rpc_http_request_not_summarized",
    )
    .await;
    store_session(session_id, session);
    Ok(rop_buffer)
}

pub(in crate::mapi) fn rpc_context_session_id(context_handle: &[u8]) -> Option<String> {
    if context_handle.len() < 20 {
        return None;
    }
    let uuid = Uuid::from_slice(&context_handle[4..20]).ok()?;
    Some(uuid.to_string())
}

pub(in crate::mapi) fn remove_session(session_id: &str) -> Option<MapiSession> {
    let now = SystemTime::now();
    let mut guard = sessions()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    prune_expired_sessions_locked(&mut guard, now);
    guard.remove(session_id)
}

pub(in crate::mapi) fn store_session(session_id: String, mut session: MapiSession) {
    let now = SystemTime::now();
    session.last_seen_at = now;
    let mut guard = sessions()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    prune_expired_sessions_locked(&mut guard, now);
    guard.insert(session_id, session);
}

pub(in crate::mapi) fn get_session(session_id: &str) -> Option<MapiSession> {
    let now = SystemTime::now();
    let mut guard = sessions()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    prune_expired_sessions_locked(&mut guard, now);
    guard.get(session_id).cloned()
}

pub(in crate::mapi) fn prune_expired_sessions_locked(
    sessions: &mut HashMap<String, MapiSession>,
    now: SystemTime,
) {
    sessions.retain(|_, session| !session_is_expired(session, now));
}

pub(in crate::mapi) fn session_is_expired(session: &MapiSession, now: SystemTime) -> bool {
    let max_age = Duration::from_secs(u64::from(MAPI_SESSION_MAX_AGE_SECONDS));
    now.duration_since(session.last_seen_at)
        .map(|idle| idle > max_age)
        .unwrap_or(false)
}

pub(in crate::mapi) fn session_matches(
    session: &MapiSession,
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
) -> bool {
    session.endpoint == endpoint
        && session.tenant_id == principal.tenant_id
        && session.account_id == principal.account_id
        && session.email == principal.email
}

pub(in crate::mapi) fn established_session_request(
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    headers: &HeaderMap,
    request_type: &str,
    request_id: &str,
) -> std::result::Result<ActiveSessionRequest, Response> {
    let Some(session_id) = request_cookie(endpoint, headers) else {
        return Err(mapi_diagnostic_response(
            request_type,
            request_id,
            13,
            "missing MAPI session cookie",
        ));
    };
    if !request_sequence_cookie_matches(endpoint, headers, &session_id) {
        return Err(mapi_diagnostic_response(
            request_type,
            request_id,
            6,
            "invalid MAPI request sequence cookie",
        ));
    }
    let Some(active_request) = begin_active_session_request(&session_id) else {
        return Err(mapi_diagnostic_response(
            request_type,
            request_id,
            15,
            "MAPI session already has an active request",
        ));
    };
    let Some(session) = get_session(&session_id) else {
        return Err(mapi_diagnostic_response(
            request_type,
            request_id,
            10,
            "MAPI session context not found",
        ));
    };
    if !session_matches(&session, endpoint, principal) {
        return Err(mapi_diagnostic_response(
            request_type,
            request_id,
            10,
            "MAPI authentication context changed",
        ));
    }
    let mut session = session;
    session.record_transport_request(request_type, request_id);
    store_session(session_id, session);
    Ok(active_request)
}

pub(in crate::mapi) fn cache_execute_response(
    session: &mut MapiSession,
    request_id: &str,
    rop_fingerprint: u64,
    response_body: &[u8],
    request_rop_ids: String,
    response_rop_ids: String,
    response_rop_results: String,
    response_rop_buffer_bytes: usize,
) {
    if !session.completed_execute_requests.contains_key(request_id) {
        while session.completed_execute_requests.len() >= MAX_CACHED_EXECUTE_REQUESTS {
            if let Some(oldest_key) = session.completed_execute_request_order.pop_front() {
                session.completed_execute_requests.remove(&oldest_key);
            } else {
                break;
            }
        }
        session
            .completed_execute_request_order
            .push_back(request_id.to_string());
    }
    session.completed_execute_requests.insert(
        request_id.to_string(),
        CachedExecuteResponse {
            rop_fingerprint,
            response_body: response_body.to_vec(),
            request_rop_ids,
            response_rop_ids,
            response_rop_results,
            response_rop_buffer_bytes,
        },
    );
}

pub(in crate::mapi) fn mapi_payload_fingerprint(bytes: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

    bytes.iter().fold(FNV_OFFSET, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
    })
}
