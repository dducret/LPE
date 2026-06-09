use super::dispatch::*;
use super::notifications::*;
use super::properties::*;
use super::rop::*;
use super::store_adapter::*;
use super::sync::*;
use super::transport::*;
use super::*;
use crate::mapi_store::MapiAssociatedConfigMessage;
use lpe_storage::{JmapEmail, SearchFolderDefinition};

const MAX_POST_HIERARCHY_ROP_IDS: usize = 64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) struct MapiSession {
    pub(in crate::mapi) endpoint: MapiEndpoint,
    pub(in crate::mapi) tenant_id: Uuid,
    pub(in crate::mapi) account_id: Uuid,
    pub(in crate::mapi) email: String,
    pub(in crate::mapi) created_at: SystemTime,
    pub(in crate::mapi) last_seen_at: SystemTime,
    pub(in crate::mapi) first_request_type: String,
    pub(in crate::mapi) first_request_id: String,
    pub(in crate::mapi) last_request_type: String,
    pub(in crate::mapi) last_request_id: String,
    pub(in crate::mapi) request_count: usize,
    pub(in crate::mapi) execute_request_count: usize,
    pub(in crate::mapi) next_handle: u32,
    pub(in crate::mapi) handles: HashMap<u32, MapiObject>,
    pub(in crate::mapi) message_statuses: HashMap<(u64, u64), u32>,
    pub(in crate::mapi) saved_search_folder_definitions:
        HashMap<u64, MapiSavedSearchFolderDefinition>,
    pub(in crate::mapi) special_folder_aliases: HashMap<u64, u64>,
    pub(in crate::mapi) deleted_advertised_special_folders: HashSet<u64>,
    pub(in crate::mapi) named_properties: HashMap<MapiNamedProperty, u16>,
    pub(in crate::mapi) named_property_ids: HashMap<u16, MapiNamedProperty>,
    pub(in crate::mapi) next_named_property_id: u16,
    pub(in crate::mapi) next_local_replica_sequence: u64,
    pub(in crate::mapi) notification_cursor: Option<i64>,
    pub(in crate::mapi) pending_notifications: VecDeque<MapiNotificationEvent>,
    pub(in crate::mapi) completed_execute_requests: HashMap<String, CachedExecuteResponse>,
    pub(in crate::mapi) completed_execute_request_order: VecDeque<String>,
    pub(in crate::mapi) post_hierarchy_actions: PostHierarchyActionState,
    pub(in crate::mapi) inbox_associated_config_stream_handles: HashSet<u32>,
    pub(in crate::mapi) logon_identity: Option<MapiLogonIdentityDebug>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(in crate::mapi) struct MapiLogonIdentityDebug {
    pub(in crate::mapi) mailbox_guid: String,
    pub(in crate::mapi) replid: String,
    pub(in crate::mapi) replica_guid: String,
    pub(in crate::mapi) response_flags: String,
    pub(in crate::mapi) special_folder_ids: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) struct CachedExecuteResponse {
    pub(in crate::mapi) rop_fingerprint: u64,
    pub(in crate::mapi) response_body: Vec<u8>,
    pub(in crate::mapi) request_rop_ids: String,
    pub(in crate::mapi) response_rop_ids: String,
    pub(in crate::mapi) response_rop_results: String,
    pub(in crate::mapi) response_rop_buffer_bytes: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(in crate::mapi) struct PostHierarchyActionState {
    pub(in crate::mapi) last_completed_hierarchy_sync_root: Option<u64>,
    pub(in crate::mapi) last_successful_hierarchy_get_buffer_summary: String,
    pub(in crate::mapi) execute_count: usize,
    pub(in crate::mapi) rop_ids_seen: Vec<u8>,
    pub(in crate::mapi) opened_folder_ids: Vec<u64>,
    pub(in crate::mapi) bootstrap_probe_observed: bool,
    pub(in crate::mapi) set_properties_probe_observed: bool,
    pub(in crate::mapi) content_sync_configure_observed: bool,
    pub(in crate::mapi) release_client_initiated: bool,
    pub(in crate::mapi) logoff_client_initiated: bool,
    pub(in crate::mapi) completed_sync_checkpoint_folder_ids: Vec<u64>,
    pub(in crate::mapi) completed_sync_checkpoint_summaries: Vec<String>,
    pub(in crate::mapi) inbox_open_folder_probe_count: usize,
    pub(in crate::mapi) inbox_folder_type_getprops_probe_count: usize,
    pub(in crate::mapi) inbox_normal_contents_table_observed: bool,
    pub(in crate::mapi) inbox_associated_contents_table_observed: bool,
    pub(in crate::mapi) inbox_associated_config_open_observed: bool,
    pub(in crate::mapi) inbox_associated_config_stream_open_observed: bool,
    pub(in crate::mapi) inbox_associated_config_stream_read_observed: bool,
    pub(in crate::mapi) last_inbox_open_folder_context: String,
    pub(in crate::mapi) last_inbox_contents_table_context: String,
    pub(in crate::mapi) last_inbox_associated_query_context: String,
    pub(in crate::mapi) last_inbox_associated_find_context: String,
    pub(in crate::mapi) last_common_views_inbox_shortcut_context: String,
    pub(in crate::mapi) last_inbox_hierarchy_table_context: String,
    pub(in crate::mapi) last_inbox_hierarchy_query_context: String,
    pub(in crate::mapi) last_inbox_related_release_context: String,
    pub(in crate::mapi) last_inbox_folder_type_getprops_context: String,
    pub(in crate::mapi) first_inbox_loop_transition_context: String,
    pub(in crate::mapi) inbox_loop_transition_logged: bool,
    pub(in crate::mapi) post_inbox_fai_handoff_logged: bool,
    pub(in crate::mapi) recent_probe_actions: Vec<String>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(in crate::mapi) struct PostHierarchyExecuteObservation {
    pub(in crate::mapi) first_execute: bool,
    pub(in crate::mapi) first_bootstrap_probe: bool,
    pub(in crate::mapi) first_set_properties_probe: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) struct PendingRecipient {
    pub(in crate::mapi) row_id: u32,
    pub(in crate::mapi) recipient_type: u8,
    pub(in crate::mapi) address: String,
    pub(in crate::mapi) display_name: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) enum PendingRecipientChange {
    Upsert(PendingRecipient),
    Delete(u32),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::mapi) enum StreamWriteTarget {
    PendingAttachment(u32),
    PendingMessageProperty { handle: u32, property_tag: u32 },
    PendingAssociatedMessageProperty { handle: u32, property_tag: u32 },
    PublicFolderItemProperty { handle: u32, property_tag: u32 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::mapi) enum StreamWriteError {
    NotFound,
    AccessDenied,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) struct TableBookmark {
    pub(in crate::mapi) position: usize,
    pub(in crate::mapi) row_key: Option<u64>,
}

#[derive(Clone, Debug)]
pub(in crate::mapi) struct MapiSavedEmail {
    pub(in crate::mapi) email: JmapEmail,
}

impl PartialEq for MapiSavedEmail {
    fn eq(&self, other: &Self) -> bool {
        self.email.id == other.email.id
    }
}

impl Eq for MapiSavedEmail {}

#[derive(Clone, Debug)]
pub(in crate::mapi) struct MapiSavedSearchFolderDefinition {
    pub(in crate::mapi) definition: SearchFolderDefinition,
}

impl PartialEq for MapiSavedSearchFolderDefinition {
    fn eq(&self, other: &Self) -> bool {
        self.definition.id == other.definition.id
    }
}

impl Eq for MapiSavedSearchFolderDefinition {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) enum MapiObject {
    Logon,
    PublicFolderLogon,
    Folder {
        folder_id: u64,
        properties: HashMap<u32, MapiValue>,
    },
    Message {
        folder_id: u64,
        message_id: u64,
        saved_email: Option<MapiSavedEmail>,
    },
    Contact {
        folder_id: u64,
        contact_id: u64,
    },
    Event {
        folder_id: u64,
        event_id: u64,
    },
    Task {
        folder_id: u64,
        task_id: u64,
    },
    Note {
        folder_id: u64,
        note_id: u64,
    },
    JournalEntry {
        folder_id: u64,
        journal_entry_id: u64,
    },
    ConversationAction {
        folder_id: u64,
        conversation_action_id: u64,
    },
    NavigationShortcut {
        folder_id: u64,
        shortcut_id: u64,
    },
    CommonViewNamedView {
        folder_id: u64,
        view_id: u64,
    },
    AssociatedConfig {
        folder_id: u64,
        config_id: u64,
        saved_message: Option<MapiAssociatedConfigMessage>,
    },
    DelegateFreeBusyMessage {
        folder_id: u64,
        message_id: u64,
    },
    RecoverableItem {
        folder_id: u64,
        item_id: u64,
    },
    PublicFolderItem {
        folder_id: u64,
        item_id: u64,
        properties: HashMap<u32, MapiValue>,
    },
    PendingMessage {
        folder_id: u64,
        properties: HashMap<u32, MapiValue>,
        recipients: Vec<PendingRecipient>,
    },
    PendingAssociatedMessage {
        folder_id: u64,
        properties: HashMap<u32, MapiValue>,
    },
    PendingContact {
        folder_id: u64,
        properties: HashMap<u32, MapiValue>,
    },
    PendingEvent {
        folder_id: u64,
        properties: HashMap<u32, MapiValue>,
    },
    PendingTask {
        folder_id: u64,
        properties: HashMap<u32, MapiValue>,
    },
    PendingNote {
        folder_id: u64,
        properties: HashMap<u32, MapiValue>,
    },
    PendingJournalEntry {
        folder_id: u64,
        properties: HashMap<u32, MapiValue>,
    },
    PendingConversationAction {
        folder_id: u64,
        properties: HashMap<u32, MapiValue>,
    },
    PendingNavigationShortcut {
        folder_id: u64,
        properties: HashMap<u32, MapiValue>,
    },
    HierarchyTable {
        folder_id: u64,
        columns: Vec<u32>,
        sort_orders: Vec<MapiSortOrder>,
        category_count: u16,
        expanded_count: u16,
        collapsed_categories: HashSet<u64>,
        deleted_advertised_special_folders: HashSet<u64>,
        restriction: Option<MapiRestriction>,
        bookmarks: HashMap<Vec<u8>, TableBookmark>,
        next_bookmark: u32,
        position: usize,
    },
    ContentsTable {
        folder_id: u64,
        associated: bool,
        columns: Vec<u32>,
        sort_orders: Vec<MapiSortOrder>,
        category_count: u16,
        expanded_count: u16,
        collapsed_categories: HashSet<u64>,
        restriction: Option<MapiRestriction>,
        bookmarks: HashMap<Vec<u8>, TableBookmark>,
        next_bookmark: u32,
        position: usize,
    },
    AttachmentTable {
        folder_id: u64,
        message_id: u64,
        columns: Vec<u32>,
        sort_orders: Vec<MapiSortOrder>,
        restriction: Option<MapiRestriction>,
        bookmarks: HashMap<Vec<u8>, TableBookmark>,
        next_bookmark: u32,
        position: usize,
    },
    PermissionTable {
        folder_id: u64,
        columns: Vec<u32>,
        position: usize,
    },
    RuleTable {
        folder_id: u64,
        columns: Vec<u32>,
        position: usize,
    },
    Attachment {
        folder_id: u64,
        message_id: u64,
        attach_num: u32,
    },
    PendingAttachment {
        folder_id: u64,
        message_id: u64,
        attach_num: u32,
        properties: HashMap<u32, MapiValue>,
        data: Vec<u8>,
    },
    SavedAttachment {
        folder_id: u64,
        message_id: u64,
        attach_num: u32,
        file_reference: String,
        file_name: String,
        media_type: String,
        size_octets: u64,
    },
    AttachmentStream {
        data: Vec<u8>,
        position: usize,
        writable_target: Option<StreamWriteTarget>,
    },
    NotificationSubscription {
        registration: MapiNotificationRegistration,
    },
    SynchronizationSource {
        folder_id: u64,
        mailbox_id: Option<Uuid>,
        checkpoint_kind: MapiCheckpointKind,
        checkpoint_change_sequence: u64,
        checkpoint_modseq: u64,
        checkpoint_store_allowed: bool,
        checkpoint_skip_reason: &'static str,
        sync_type: u8,
        initial_state: Vec<u8>,
        state: Vec<u8>,
        state_upload_property_tag: Option<u32>,
        state_upload_buffer: Vec<u8>,
        client_state_uploaded_bytes: usize,
        client_state_uploaded_marker_mask: u8,
        incremental_transfer_buffer: Option<Vec<u8>>,
        transfer_buffer: Vec<u8>,
        transfer_position: usize,
    },
    SynchronizationCollector {
        folder_id: u64,
        mailbox_id: Option<Uuid>,
        checkpoint_kind: MapiCheckpointKind,
        sync_type: u8,
        state: Vec<u8>,
        state_upload_property_tag: Option<u32>,
        state_upload_buffer: Vec<u8>,
        client_state_uploaded_bytes: usize,
        client_state_uploaded_marker_mask: u8,
        uploaded_object_ids: Vec<u64>,
        uploaded_normal_change_numbers: Vec<u64>,
        uploaded_fai_change_numbers: Vec<u64>,
        uploaded_read_change_numbers: Vec<u64>,
    },
    FastTransferDestination {
        folder_id: u64,
        target_handle: u32,
        buffer: Vec<u8>,
    },
}

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
        saved_search_folder_definitions: HashMap::new(),
        special_folder_aliases: HashMap::new(),
        deleted_advertised_special_folders: HashSet::new(),
        named_properties: HashMap::new(),
        named_property_ids: HashMap::new(),
        next_named_property_id: FIRST_NAMED_PROPERTY_ID,
        next_local_replica_sequence: 1,
        notification_cursor: None,
        pending_notifications: VecDeque::new(),
        completed_execute_requests: HashMap::new(),
        completed_execute_request_order: VecDeque::new(),
        post_hierarchy_actions: PostHierarchyActionState::default(),
        inbox_associated_config_stream_handles: HashSet::new(),
        logon_identity: None,
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
    let snapshot =
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
        &snapshot,
        validator,
        rop_buffer,
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

impl MapiSession {
    pub(in crate::mapi) fn record_logon_identity(&mut self, identity: MapiLogonIdentityDebug) {
        self.logon_identity = Some(identity);
    }

    pub(in crate::mapi) fn record_transport_request(
        &mut self,
        request_type: &str,
        request_id: &str,
    ) {
        if self.request_count == 0 {
            self.first_request_type = request_type.to_string();
            self.first_request_id = request_id.to_string();
        }
        self.last_request_type = request_type.to_string();
        self.last_request_id = request_id.to_string();
        self.request_count = self.request_count.saturating_add(1);
        if request_type == "Execute" {
            self.execute_request_count = self.execute_request_count.saturating_add(1);
        }
    }

    pub(in crate::mapi) fn record_completed_sync_checkpoint(
        &mut self,
        folder_id: u64,
        folder_role: &str,
        folder_container_class: &str,
        checkpoint_kind: &str,
        sync_type: u8,
        status: &str,
    ) {
        if self
            .post_hierarchy_actions
            .completed_sync_checkpoint_summaries
            .len()
            >= 64
        {
            return;
        }
        let summary = format!(
            "folder=0x{folder_id:016x};role={folder_role};container={folder_container_class};kind={checkpoint_kind};sync=0x{sync_type:02x};status={status}"
        );
        if !self
            .post_hierarchy_actions
            .completed_sync_checkpoint_summaries
            .contains(&summary)
        {
            if !self
                .post_hierarchy_actions
                .completed_sync_checkpoint_folder_ids
                .contains(&folder_id)
            {
                self.post_hierarchy_actions
                    .completed_sync_checkpoint_folder_ids
                    .push(folder_id);
            }
            self.post_hierarchy_actions
                .completed_sync_checkpoint_summaries
                .push(summary);
        }
    }

    pub(in crate::mapi) fn record_opened_folder(&mut self, folder_id: u64) {
        if self.post_hierarchy_actions.opened_folder_ids.len() >= 64
            || self
                .post_hierarchy_actions
                .opened_folder_ids
                .contains(&folder_id)
        {
            return;
        }
        self.post_hierarchy_actions
            .opened_folder_ids
            .push(folder_id);
    }

    pub(in crate::mapi) fn record_inbox_open_folder_probe(&mut self) {
        self.post_hierarchy_actions.inbox_open_folder_probe_count = self
            .post_hierarchy_actions
            .inbox_open_folder_probe_count
            .saturating_add(1);
    }

    pub(in crate::mapi) fn record_inbox_folder_type_getprops_probe(&mut self) {
        self.post_hierarchy_actions
            .inbox_folder_type_getprops_probe_count = self
            .post_hierarchy_actions
            .inbox_folder_type_getprops_probe_count
            .saturating_add(1);
    }

    pub(in crate::mapi) fn record_inbox_normal_contents_table(&mut self) {
        self.post_hierarchy_actions
            .inbox_normal_contents_table_observed = true;
    }

    pub(in crate::mapi) fn record_inbox_associated_contents_table(&mut self) {
        self.post_hierarchy_actions
            .inbox_associated_contents_table_observed = true;
    }

    pub(in crate::mapi) fn record_inbox_associated_config_open(&mut self) {
        self.post_hierarchy_actions
            .inbox_associated_config_open_observed = true;
    }

    pub(in crate::mapi) fn record_inbox_associated_config_stream_open(&mut self) {
        self.post_hierarchy_actions
            .inbox_associated_config_stream_open_observed = true;
    }

    pub(in crate::mapi) fn record_inbox_associated_config_stream_handle(&mut self, handle: u32) {
        self.inbox_associated_config_stream_handles.insert(handle);
    }

    pub(in crate::mapi) fn is_inbox_associated_config_stream_handle(&self, handle: u32) -> bool {
        self.inbox_associated_config_stream_handles
            .contains(&handle)
    }

    pub(in crate::mapi) fn record_inbox_associated_config_stream_read(&mut self) {
        self.post_hierarchy_actions
            .inbox_associated_config_stream_read_observed = true;
    }

    pub(in crate::mapi) fn record_last_inbox_open_folder_context(&mut self, context: String) {
        self.post_hierarchy_actions.last_inbox_open_folder_context = context;
    }

    pub(in crate::mapi) fn record_last_inbox_contents_table_context(&mut self, context: String) {
        self.post_hierarchy_actions
            .last_inbox_contents_table_context = context;
    }

    pub(in crate::mapi) fn record_last_inbox_associated_query_context(&mut self, context: String) {
        self.post_hierarchy_actions
            .last_inbox_associated_query_context = context;
    }

    pub(in crate::mapi) fn record_last_inbox_associated_find_context(&mut self, context: String) {
        self.post_hierarchy_actions
            .last_inbox_associated_find_context = context;
    }

    pub(in crate::mapi) fn record_last_common_views_inbox_shortcut_context(
        &mut self,
        context: String,
    ) {
        self.post_hierarchy_actions
            .last_common_views_inbox_shortcut_context = context;
    }

    pub(in crate::mapi) fn record_last_inbox_hierarchy_table_context(&mut self, context: String) {
        self.post_hierarchy_actions
            .last_inbox_hierarchy_table_context = context;
    }

    pub(in crate::mapi) fn record_last_inbox_hierarchy_query_context(&mut self, context: String) {
        self.post_hierarchy_actions
            .last_inbox_hierarchy_query_context = context;
    }

    pub(in crate::mapi) fn record_last_inbox_related_release_context(&mut self, context: String) {
        self.post_hierarchy_actions
            .last_inbox_related_release_context = context;
    }

    pub(in crate::mapi) fn record_last_inbox_folder_type_getprops_context(
        &mut self,
        context: String,
    ) {
        self.post_hierarchy_actions
            .last_inbox_folder_type_getprops_context = context;
    }

    pub(in crate::mapi) fn record_first_inbox_loop_transition_context(&mut self, context: String) {
        if self
            .post_hierarchy_actions
            .first_inbox_loop_transition_context
            .is_empty()
        {
            self.post_hierarchy_actions
                .first_inbox_loop_transition_context = context;
        }
    }

    pub(in crate::mapi) fn mark_inbox_loop_transition_logged(&mut self) {
        self.post_hierarchy_actions.inbox_loop_transition_logged = true;
    }

    pub(in crate::mapi) fn mark_post_inbox_fai_handoff_logged(&mut self) {
        self.post_hierarchy_actions.post_inbox_fai_handoff_logged = true;
    }

    pub(in crate::mapi) fn record_recent_probe_action(&mut self, action: String) {
        if self.post_hierarchy_actions.recent_probe_actions.len() >= 8 {
            self.post_hierarchy_actions.recent_probe_actions.remove(0);
        }
        self.post_hierarchy_actions
            .recent_probe_actions
            .push(action);
    }

    pub(in crate::mapi) fn record_special_folder_alias(&mut self, alias_id: u64, folder_id: u64) {
        if alias_id == folder_id || self.special_folder_aliases.len() >= 32 {
            return;
        }
        self.special_folder_aliases.insert(alias_id, folder_id);
    }

    pub(in crate::mapi) fn remember_search_folder_definition(
        &mut self,
        folder_id: u64,
        definition: SearchFolderDefinition,
    ) {
        self.saved_search_folder_definitions
            .insert(folder_id, MapiSavedSearchFolderDefinition { definition });
    }

    pub(in crate::mapi) fn search_folder_definition(
        &self,
        folder_id: u64,
    ) -> Option<&SearchFolderDefinition> {
        self.saved_search_folder_definitions
            .get(&folder_id)
            .map(|saved| &saved.definition)
    }

    pub(in crate::mapi) fn resolve_special_folder_alias(&self, folder_id: u64) -> u64 {
        self.special_folder_aliases
            .get(&folder_id)
            .copied()
            .unwrap_or(folder_id)
    }

    pub(in crate::mapi) fn record_deleted_advertised_special_folder(&mut self, folder_id: u64) {
        self.deleted_advertised_special_folders.insert(folder_id);
    }

    pub(in crate::mapi) fn advertised_special_folder_was_deleted(&self, folder_id: u64) -> bool {
        self.deleted_advertised_special_folders.contains(&folder_id)
    }

    pub(in crate::mapi) fn allocate_output_handle(
        &mut self,
        output_handle_index: Option<u8>,
        object: MapiObject,
    ) -> u32 {
        self.allocate_output_handle_avoiding(output_handle_index, object, &HashSet::new())
    }

    pub(in crate::mapi) fn allocate_output_handle_avoiding(
        &mut self,
        output_handle_index: Option<u8>,
        object: MapiObject,
        reserved_handles: &HashSet<u32>,
    ) -> u32 {
        while self.handles.contains_key(&self.next_handle)
            || reserved_handles.contains(&self.next_handle)
        {
            self.next_handle = self.next_handle.saturating_add(1).max(1);
        }
        let preferred = output_handle_index.map(|index| index as u32 + 1);
        let handle = preferred
            .filter(|handle| {
                *handle >= self.next_handle
                    && !self.handles.contains_key(handle)
                    && !reserved_handles.contains(handle)
            })
            .unwrap_or(self.next_handle);
        self.next_handle = self.next_handle.saturating_add(1).max(1);
        if handle >= self.next_handle {
            self.next_handle = handle.saturating_add(1).max(1);
        }
        self.handles.insert(handle, object);
        handle
    }

    pub(in crate::mapi) fn record_notification(&mut self, event: MapiNotificationEvent) {
        if self.handles.values().any(|object| {
            matches!(
                object,
                MapiObject::NotificationSubscription { registration }
                    if registration_matches_event(registration, &event)
            )
        }) {
            self.pending_notifications.push_back(event);
        }
    }

    pub(in crate::mapi) fn take_pending_notifications(&mut self) -> Vec<MapiNotificationEvent> {
        self.pending_notifications.drain(..).collect()
    }

    pub(in crate::mapi) fn matching_notifications(
        &self,
        events: Vec<MapiNotificationEvent>,
    ) -> Vec<MapiNotificationEvent> {
        events
            .into_iter()
            .filter(|event| {
                self.handles.values().any(|object| {
                    matches!(
                        object,
                        MapiObject::NotificationSubscription { registration }
                            if registration_matches_event(registration, event)
                    )
                })
            })
            .collect()
    }

    pub(in crate::mapi) fn hierarchy_sync_completed(&self) -> bool {
        self.post_hierarchy_actions
            .last_completed_hierarchy_sync_root
            .is_some()
    }

    pub(in crate::mapi) fn record_completed_hierarchy_sync(
        &mut self,
        sync_root_folder_id: u64,
        get_buffer_summary: String,
    ) {
        self.post_hierarchy_actions
            .last_completed_hierarchy_sync_root = Some(sync_root_folder_id);
        self.post_hierarchy_actions
            .last_successful_hierarchy_get_buffer_summary = get_buffer_summary;
    }

    pub(in crate::mapi) fn record_content_sync_configure(&mut self) {
        self.post_hierarchy_actions.content_sync_configure_observed = true;
    }

    pub(in crate::mapi) fn record_logoff_after_hierarchy_completion(&mut self) {
        if self.hierarchy_sync_completed() {
            self.post_hierarchy_actions.logoff_client_initiated = true;
        }
    }

    pub(in crate::mapi) fn record_execute_after_hierarchy_completion(
        &mut self,
        rop_ids: &[u8],
    ) -> PostHierarchyExecuteObservation {
        if !self.hierarchy_sync_completed() {
            return PostHierarchyExecuteObservation::default();
        }
        let first_execute = self.post_hierarchy_actions.execute_count == 0;
        let contains_bootstrap_probe = rop_ids.iter().any(|rop_id| matches!(rop_id, 0x02 | 0x07));
        let first_bootstrap_probe =
            contains_bootstrap_probe && !self.post_hierarchy_actions.bootstrap_probe_observed;
        if contains_bootstrap_probe {
            self.post_hierarchy_actions.bootstrap_probe_observed = true;
        }
        let contains_set_properties_probe =
            rop_ids.iter().any(|rop_id| matches!(rop_id, 0x0A | 0x79));
        let first_set_properties_probe = contains_set_properties_probe
            && !self.post_hierarchy_actions.set_properties_probe_observed;
        if contains_set_properties_probe {
            self.post_hierarchy_actions.set_properties_probe_observed = true;
        }
        self.post_hierarchy_actions.execute_count =
            self.post_hierarchy_actions.execute_count.saturating_add(1);
        for rop_id in rop_ids.iter().copied() {
            if self.post_hierarchy_actions.rop_ids_seen.len() < MAX_POST_HIERARCHY_ROP_IDS
                && !self.post_hierarchy_actions.rop_ids_seen.contains(&rop_id)
            {
                self.post_hierarchy_actions.rop_ids_seen.push(rop_id);
            }
        }
        if rop_ids.contains(&0x01) {
            self.post_hierarchy_actions.release_client_initiated = true;
        }
        PostHierarchyExecuteObservation {
            first_execute,
            first_bootstrap_probe,
            first_set_properties_probe,
        }
    }

    pub(in crate::mapi) fn property_id_for_name(
        &mut self,
        property: MapiNamedProperty,
        create: bool,
    ) -> Option<u16> {
        let property = normalize_named_property(property);
        if property.guid == PS_MAPI_GUID {
            if let MapiNamedPropertyKind::Lid(lid) = &property.kind {
                return u16::try_from(*lid).ok();
            }
        }
        if let Some(property_id) = self.named_properties.get(&property).copied() {
            return Some(property_id);
        }
        if let Some(property_id) = well_known_named_property_id(&property) {
            self.named_properties.insert(property.clone(), property_id);
            self.named_property_ids.insert(property_id, property);
            return Some(property_id);
        }
        if !create || self.next_named_property_id > MAX_NAMED_PROPERTY_ID {
            return None;
        }

        while self.next_named_property_id <= MAX_NAMED_PROPERTY_ID
            && (self
                .named_property_ids
                .contains_key(&self.next_named_property_id)
                || is_reserved_named_property_id(self.next_named_property_id))
        {
            self.next_named_property_id = self.next_named_property_id.saturating_add(1);
        }
        if self.next_named_property_id > MAX_NAMED_PROPERTY_ID {
            return None;
        }
        let property_id = self.next_named_property_id;
        self.next_named_property_id = self.next_named_property_id.saturating_add(1);
        self.named_properties.insert(property.clone(), property_id);
        self.named_property_ids.insert(property_id, property);
        Some(property_id)
    }

    pub(in crate::mapi) fn cache_named_property(
        &mut self,
        property_id: u16,
        property: MapiNamedProperty,
    ) {
        let property = normalize_named_property(property);
        let canonical_property_id = well_known_named_property_id(&property).unwrap_or(property_id);
        self.named_properties
            .insert(property.clone(), canonical_property_id);
        self.named_property_ids
            .insert(canonical_property_id, property.clone());
        if property_id != canonical_property_id {
            self.named_property_ids.insert(property_id, property);
        }
        if property_id >= self.next_named_property_id {
            self.next_named_property_id = property_id.saturating_add(1);
        }
    }

    pub(in crate::mapi) fn property_name_for_id(&self, property_id: u16) -> MapiNamedProperty {
        self.named_property_ids
            .get(&property_id)
            .cloned()
            .or_else(|| well_known_named_property_for_id(property_id))
            .unwrap_or(MapiNamedProperty {
                guid: PS_MAPI_GUID,
                kind: MapiNamedPropertyKind::Lid(u32::from(property_id)),
            })
    }

    pub(in crate::mapi) fn named_properties_for_query(
        &self,
        guid: Option<[u8; 16]>,
    ) -> Vec<(u16, MapiNamedProperty)> {
        let mut properties = self
            .named_property_ids
            .iter()
            .filter(|(_property_id, property)| match guid {
                Some(guid) => property.guid == guid,
                None => true,
            })
            .map(|(property_id, property)| (*property_id, property.clone()))
            .collect::<Vec<_>>();
        properties.sort_by_key(|(property_id, _property)| *property_id);
        properties
    }
}

pub(in crate::mapi) fn normalize_named_property(
    mut property: MapiNamedProperty,
) -> MapiNamedProperty {
    if property.guid == PS_INTERNET_HEADERS_GUID {
        if let MapiNamedPropertyKind::Name(name) = property.kind {
            property.kind = MapiNamedPropertyKind::Name(name.to_ascii_lowercase());
        }
    }
    property
}

impl MapiObject {
    pub(in crate::mapi) fn folder_id(&self) -> Option<u64> {
        match self {
            MapiObject::AttachmentStream { .. } | MapiObject::NotificationSubscription { .. } => {
                None
            }
            MapiObject::Logon => Some(ROOT_FOLDER_ID),
            MapiObject::PublicFolderLogon => Some(PUBLIC_FOLDERS_ROOT_FOLDER_ID),
            MapiObject::Folder { folder_id, .. }
            | MapiObject::Message { folder_id, .. }
            | MapiObject::Contact { folder_id, .. }
            | MapiObject::Event { folder_id, .. }
            | MapiObject::Task { folder_id, .. }
            | MapiObject::Note { folder_id, .. }
            | MapiObject::JournalEntry { folder_id, .. }
            | MapiObject::ConversationAction { folder_id, .. }
            | MapiObject::NavigationShortcut { folder_id, .. }
            | MapiObject::CommonViewNamedView { folder_id, .. }
            | MapiObject::AssociatedConfig { folder_id, .. }
            | MapiObject::DelegateFreeBusyMessage { folder_id, .. }
            | MapiObject::RecoverableItem { folder_id, .. }
            | MapiObject::PublicFolderItem { folder_id, .. }
            | MapiObject::PendingMessage { folder_id, .. }
            | MapiObject::PendingAssociatedMessage { folder_id, .. }
            | MapiObject::PendingContact { folder_id, .. }
            | MapiObject::PendingEvent { folder_id, .. }
            | MapiObject::PendingTask { folder_id, .. }
            | MapiObject::PendingNote { folder_id, .. }
            | MapiObject::PendingJournalEntry { folder_id, .. }
            | MapiObject::PendingConversationAction { folder_id, .. }
            | MapiObject::PendingNavigationShortcut { folder_id, .. }
            | MapiObject::HierarchyTable { folder_id, .. }
            | MapiObject::ContentsTable { folder_id, .. }
            | MapiObject::AttachmentTable { folder_id, .. }
            | MapiObject::PermissionTable { folder_id, .. }
            | MapiObject::RuleTable { folder_id, .. }
            | MapiObject::Attachment { folder_id, .. }
            | MapiObject::PendingAttachment { folder_id, .. }
            | MapiObject::SavedAttachment { folder_id, .. }
            | MapiObject::SynchronizationSource { folder_id, .. }
            | MapiObject::SynchronizationCollector { folder_id, .. }
            | MapiObject::FastTransferDestination { folder_id, .. } => Some(*folder_id),
        }
    }
}

pub(in crate::mapi) fn input_object<'a>(
    session: &'a MapiSession,
    input_handles: &[u32],
    request: &RopRequest,
) -> Option<&'a MapiObject> {
    let handle = input_handle(input_handles, request)?;
    session.handles.get(&handle)
}

pub(in crate::mapi) fn input_object_mut<'a>(
    session: &'a mut MapiSession,
    input_handles: &[u32],
    request: &RopRequest,
) -> Option<&'a mut MapiObject> {
    let handle = input_handle(input_handles, request)?;
    session.handles.get_mut(&handle)
}

pub(in crate::mapi) fn synchronization_context_state(
    object: Option<&MapiObject>,
) -> Option<(
    u64,
    Option<Uuid>,
    MapiCheckpointKind,
    u64,
    u64,
    bool,
    &'static str,
    u8,
    Vec<u8>,
)> {
    match object {
        Some(MapiObject::SynchronizationSource {
            folder_id,
            mailbox_id,
            checkpoint_kind,
            checkpoint_change_sequence,
            checkpoint_modseq,
            checkpoint_store_allowed,
            checkpoint_skip_reason,
            sync_type,
            initial_state,
            state,
            transfer_buffer,
            transfer_position,
            ..
        }) => {
            let transfer_state = if *transfer_position >= transfer_buffer.len() {
                state.clone()
            } else {
                initial_state.clone()
            };
            Some((
                *folder_id,
                *mailbox_id,
                *checkpoint_kind,
                *checkpoint_change_sequence,
                *checkpoint_modseq,
                *checkpoint_store_allowed,
                *checkpoint_skip_reason,
                *sync_type,
                transfer_state,
            ))
        }
        Some(MapiObject::SynchronizationCollector {
            folder_id,
            mailbox_id,
            checkpoint_kind,
            sync_type,
            state,
            ..
        }) => Some((
            *folder_id,
            *mailbox_id,
            *checkpoint_kind,
            0,
            1,
            true,
            "",
            *sync_type,
            state.clone(),
        )),
        _ => None,
    }
}

pub(in crate::mapi) fn input_handle(input_handles: &[u32], request: &RopRequest) -> Option<u32> {
    input_handles
        .get(request.input_handle_index()? as usize)
        .copied()
        .filter(|handle| *handle != u32::MAX)
}

pub(in crate::mapi) fn set_handle_slot(
    handle_slots: &mut Vec<u32>,
    output_handle_index: Option<u8>,
    handle: u32,
) {
    let Some(index) = output_handle_index.map(usize::from) else {
        return;
    };
    if handle_slots.len() <= index {
        handle_slots.resize(index + 1, u32::MAX);
    }
    handle_slots[index] = handle;
}

pub(in crate::mapi) fn release_handle_slot(
    session: &mut MapiSession,
    handle_slots: &mut [u32],
    request: &RopRequest,
) {
    let Some(index) = request.input_handle_index().map(usize::from) else {
        return;
    };
    let Some(handle) = handle_slots.get_mut(index) else {
        return;
    };
    if *handle != u32::MAX {
        session.handles.remove(handle);
    }
    *handle = u32::MAX;
}

pub(in crate::mapi) fn response_handle_table(
    handle_slots: &[u32],
    output_handles: &[u32],
    echo_input_handles: bool,
) -> Vec<u32> {
    if echo_input_handles {
        return handle_slots.to_vec();
    }
    let mut handles = handle_slots.to_vec();
    while handles.last().is_some_and(|handle| *handle == u32::MAX) {
        handles.pop();
    }
    if handles.is_empty() {
        output_handles.to_vec()
    } else {
        handles
    }
}

pub(in crate::mapi) fn reset_table_position(object: &mut MapiObject) -> bool {
    match object {
        MapiObject::HierarchyTable {
            position,
            bookmarks,
            ..
        }
        | MapiObject::ContentsTable {
            position,
            bookmarks,
            ..
        }
        | MapiObject::AttachmentTable {
            position,
            bookmarks,
            ..
        } => {
            *position = 0;
            bookmarks.clear();
            true
        }
        MapiObject::PermissionTable { position, .. } => {
            *position = 0;
            true
        }
        MapiObject::RuleTable { position, .. } => {
            *position = 0;
            true
        }
        _ => false,
    }
}

pub(in crate::mapi) fn read_handle_table(handle_table: &[u8]) -> Result<Vec<u32>> {
    if handle_table.len() % 4 != 0 {
        return Err(anyhow!("ROP handle table length is not a multiple of 4"));
    }
    Ok(handle_table
        .chunks_exact(4)
        .map(|bytes| u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn principal() -> AccountPrincipal {
        AccountPrincipal {
            tenant_id: Uuid::from_u128(0xaaaaaaaa_aaaa_aaaa_aaaa_aaaaaaaaaaaa),
            account_id: Uuid::from_u128(0xbbbbbbbb_bbbb_bbbb_bbbb_bbbbbbbbbbbb),
            email: "user@example.test".to_string(),
            display_name: "User".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        }
    }

    #[test]
    fn reconnect_session_rejects_active_context() {
        let principal = principal();
        let session_id = create_session(MapiEndpoint::Emsmdb, &principal, "Connect", "test:1");
        let _active = begin_active_session_request(&session_id).unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            "cookie",
            HeaderValue::from_str(&format!("MapiContext={session_id}")).unwrap(),
        );

        let Err(response) = reconnect_session(
            MapiEndpoint::Emsmdb,
            &principal,
            &headers,
            "Connect",
            "{11111111-2222-3333-4444-555555555555}:1",
        ) else {
            panic!("active session reconnect should be rejected");
        };

        assert_eq!(
            response_header(&response, "x-requesttype").unwrap(),
            "Connect"
        );
        assert_eq!(
            response_header(&response, "x-requestid").unwrap(),
            "{11111111-2222-3333-4444-555555555555}:1"
        );
        assert_eq!(response_header(&response, "x-responsecode").unwrap(), "15");
        remove_session(&session_id);
    }

    #[test]
    fn execute_replay_cache_evicts_oldest_inserted_request_id() {
        let principal = principal();
        let session_id = create_session(MapiEndpoint::Emsmdb, &principal, "Connect", "test:1");
        let mut session = remove_session(&session_id).unwrap();

        for index in 0..=MAX_CACHED_EXECUTE_REQUESTS {
            cache_execute_response(
                &mut session,
                &format!("{{11111111-2222-3333-4444-555555555555}}:{index}"),
                index as u64,
                &[index as u8],
                format!("request-{index}"),
                format!("response-{index}"),
                format!("result-{index}"),
                index,
            );
        }

        assert!(!session
            .completed_execute_requests
            .contains_key("{11111111-2222-3333-4444-555555555555}:0"));
        assert!(session
            .completed_execute_requests
            .contains_key("{11111111-2222-3333-4444-555555555555}:1"));
        assert!(session.completed_execute_requests.contains_key(&format!(
            "{{11111111-2222-3333-4444-555555555555}}:{MAX_CACHED_EXECUTE_REQUESTS}"
        )));
        assert_eq!(
            session.completed_execute_requests.len(),
            MAX_CACHED_EXECUTE_REQUESTS
        );
    }

    #[test]
    fn session_records_transport_request_lifetime() {
        let principal = principal();
        let session_id = create_session(MapiEndpoint::Emsmdb, &principal, "Connect", "test:1");
        let mut session = remove_session(&session_id).unwrap();

        session.record_transport_request("Execute", "test:2");
        session.record_transport_request("Disconnect", "test:3");

        assert_eq!(session.first_request_type, "Connect");
        assert_eq!(session.first_request_id, "test:1");
        assert_eq!(session.last_request_type, "Disconnect");
        assert_eq!(session.last_request_id, "test:3");
        assert_eq!(session.request_count, 3);
        assert_eq!(session.execute_request_count, 1);
    }

    #[test]
    fn session_remembers_deleted_advertised_special_folder() {
        let principal = principal();
        let session_id = create_session(MapiEndpoint::Emsmdb, &principal, "Connect", "test:1");
        let mut session = remove_session(&session_id).unwrap();

        assert!(!session.advertised_special_folder_was_deleted(QUICK_STEP_SETTINGS_FOLDER_ID));

        session.record_deleted_advertised_special_folder(QUICK_STEP_SETTINGS_FOLDER_ID);

        assert!(session.advertised_special_folder_was_deleted(QUICK_STEP_SETTINGS_FOLDER_ID));
    }

    #[test]
    fn session_remembers_saved_search_folder_definition() {
        let principal = principal();
        let session_id = create_session(MapiEndpoint::Emsmdb, &principal, "Connect", "test:1");
        let mut session = remove_session(&session_id).unwrap();
        let folder_id = 0x0000_0000_0157_0001;
        let definition = SearchFolderDefinition {
            id: Uuid::from_u128(0x157),
            account_id: principal.account_id,
            role: "custom".to_string(),
            display_name: "Unread Mail".to_string(),
            definition_kind: "user_saved".to_string(),
            result_object_kind: "email".to_string(),
            scope_json: serde_json::json!({"kind": "mapi_bounded"}),
            restriction_json: serde_json::json!({"kind": "mapi_bounded", "all": []}),
            excluded_folder_roles: Vec::new(),
            is_builtin: false,
        };

        session.remember_search_folder_definition(folder_id, definition);

        assert_eq!(
            session
                .search_folder_definition(folder_id)
                .map(|definition| definition.display_name.as_str()),
            Some("Unread Mail")
        );
    }

    #[test]
    fn session_records_completed_sync_checkpoint_once() {
        let principal = principal();
        let session_id = create_session(MapiEndpoint::Emsmdb, &principal, "Connect", "test:1");
        let mut session = remove_session(&session_id).unwrap();

        session.record_completed_sync_checkpoint(
            0x0000_0000_0010_0001,
            "calendar",
            "IPF.Appointment",
            "content",
            0x01,
            "ok",
        );
        session.record_completed_sync_checkpoint(
            0x0000_0000_0010_0001,
            "calendar",
            "IPF.Appointment",
            "content",
            0x01,
            "ok",
        );

        assert_eq!(
            session
                .post_hierarchy_actions
                .completed_sync_checkpoint_summaries,
            vec![
                "folder=0x0000000000100001;role=calendar;container=IPF.Appointment;kind=content;sync=0x01;status=ok"
                    .to_string()
            ]
        );
    }

    #[test]
    fn response_handle_table_preserves_sparse_output_handle_indexes() {
        let handles = response_handle_table(&[10, 20, 30], &[20, 30], false);

        assert_eq!(handles, vec![10, 20, 30]);
    }

    #[test]
    fn response_handle_table_can_echo_released_input_slots() {
        let handles = response_handle_table(&[u32::MAX], &[], true);

        assert_eq!(handles, vec![u32::MAX]);
    }

    #[test]
    fn allocate_output_handle_prefers_free_low_output_slot_handle() {
        let principal = principal();
        let session_id = create_session(MapiEndpoint::Emsmdb, &principal, "Connect", "test:1");
        let mut session = remove_session(&session_id).unwrap();

        let logon_handle = session.allocate_output_handle(Some(0), MapiObject::Logon);
        let source_handle = session.allocate_output_handle(
            Some(1),
            MapiObject::Folder {
                folder_id: 0x0000_0000_0004_0001,
                properties: HashMap::new(),
            },
        );

        assert_eq!(logon_handle, 1);
        assert_eq!(source_handle, 2);
    }

    #[test]
    fn allocate_output_handle_skips_reserved_same_execute_handle() {
        let principal = principal();
        let session_id = create_session(MapiEndpoint::Emsmdb, &principal, "Connect", "test:1");
        let mut session = remove_session(&session_id).unwrap();
        session.allocate_output_handle(Some(0), MapiObject::Logon);
        session.allocate_output_handle(
            Some(1),
            MapiObject::Folder {
                folder_id: ROOT_FOLDER_ID,
                properties: HashMap::new(),
            },
        );
        let released_handle = session.allocate_output_handle(
            Some(2),
            MapiObject::Folder {
                folder_id: INBOX_FOLDER_ID,
                properties: HashMap::new(),
            },
        );
        session.handles.remove(&released_handle);

        let handle = session.allocate_output_handle_avoiding(
            Some(2),
            MapiObject::Folder {
                folder_id: INBOX_FOLDER_ID,
                properties: HashMap::new(),
            },
            &HashSet::from([released_handle]),
        );

        assert_eq!(released_handle, 3);
        assert_ne!(handle, released_handle);
        assert!(handle > released_handle);
    }

    #[test]
    fn allocate_output_handle_does_not_reuse_old_low_slot_handle() {
        let principal = principal();
        let session_id = create_session(MapiEndpoint::Emsmdb, &principal, "Connect", "test:1");
        let mut session = remove_session(&session_id).unwrap();
        session.allocate_output_handle(Some(0), MapiObject::Logon);
        session.allocate_output_handle(
            Some(1),
            MapiObject::Folder {
                folder_id: ROOT_FOLDER_ID,
                properties: HashMap::new(),
            },
        );
        let released_handle = session.allocate_output_handle(
            Some(2),
            MapiObject::Folder {
                folder_id: INBOX_FOLDER_ID,
                properties: HashMap::new(),
            },
        );
        session.handles.remove(&released_handle);

        let handle = session.allocate_output_handle(
            Some(2),
            MapiObject::Folder {
                folder_id: INBOX_FOLDER_ID,
                properties: HashMap::new(),
            },
        );

        assert_eq!(released_handle, 3);
        assert_eq!(handle, 4);
    }

    #[test]
    fn cached_named_property_updates_bidirectional_registry() {
        let principal = principal();
        let session_id = create_session(MapiEndpoint::Emsmdb, &principal, "Connect", "test:1");
        let mut session = remove_session(&session_id).unwrap();
        let property = MapiNamedProperty {
            guid: PSETID_COMMON_GUID,
            kind: MapiNamedPropertyKind::Name("custom-field".to_string()),
        };

        session.cache_named_property(0x8001, property.clone());

        assert_eq!(
            session.property_id_for_name(property.clone(), false),
            Some(0x8001)
        );
        assert_eq!(session.property_name_for_id(0x8001), property);
        assert_eq!(session.next_named_property_id, 0x8002);
    }

    #[test]
    fn cached_well_known_named_property_keeps_dynamic_id_as_alias() {
        let principal = principal();
        let session_id = create_session(MapiEndpoint::Emsmdb, &principal, "Connect", "test:1");
        let mut session = remove_session(&session_id).unwrap();
        let property = MapiNamedProperty {
            guid: PSETID_SHARING_GUID,
            kind: MapiNamedPropertyKind::Name(
                "SharingCalendarGroupEntryAssociatedLocalFolderId".to_string(),
            ),
        };

        session.cache_named_property(0x8fff, property.clone());

        assert_eq!(
            session.property_id_for_name(property.clone(), false),
            Some(0x8010)
        );
        assert_eq!(session.property_name_for_id(0x8010), property);
        assert_eq!(session.property_name_for_id(0x8fff), property);
        assert_eq!(session.next_named_property_id, 0x9000);
    }

    #[test]
    fn ps_mapi_lid_maps_directly_even_in_named_property_range() {
        let principal = principal();
        let session_id = create_session(MapiEndpoint::Emsmdb, &principal, "Connect", "test:1");
        let mut session = remove_session(&session_id).unwrap();
        let property = MapiNamedProperty {
            guid: PS_MAPI_GUID,
            kind: MapiNamedPropertyKind::Lid(0x8503),
        };

        assert_eq!(session.property_id_for_name(property, false), Some(0x8503));
        assert_eq!(session.next_named_property_id, FIRST_NAMED_PROPERTY_ID);
    }
}
