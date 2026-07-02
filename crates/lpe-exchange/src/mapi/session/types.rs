use super::*;

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
    pub(in crate::mapi) message_save_generations: HashMap<(u64, u64), u64>,
    pub(in crate::mapi) message_handle_generations: HashMap<u32, u64>,
    pub(in crate::mapi) pending_message_recipient_replacements: HashMap<u32, Vec<PendingRecipient>>,
    pub(in crate::mapi) pending_message_attachments:
        HashMap<u32, Vec<(u32, AttachmentUploadInput)>>,
    pub(in crate::mapi) pending_attachment_parent_messages: HashMap<u32, u32>,
    pub(in crate::mapi) pending_attachment_deletions: HashSet<(u64, u64, u32)>,
    pub(in crate::mapi) pending_embedded_message_ids: HashMap<u32, u64>,
    pub(in crate::mapi) pending_embedded_message_attachments: HashMap<u32, (u64, u64, u32)>,
    pub(in crate::mapi) saved_embedded_messages: HashMap<(u64, u64, u32), HashMap<u32, MapiValue>>,
    pub(in crate::mapi) saved_search_folder_definitions:
        HashMap<u64, MapiSavedSearchFolderDefinition>,
    pub(in crate::mapi) special_folder_aliases: HashMap<u64, u64>,
    pub(in crate::mapi) deleted_advertised_special_folders: HashSet<u64>,
    pub(in crate::mapi) deleted_search_folder_definitions: HashSet<u64>,
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
    pub(in crate::mapi) inbox_rule_organizer_stream_handles: HashSet<u32>,
    pub(in crate::mapi) logon_identity: Option<MapiLogonIdentityDebug>,
    pub(in crate::mapi) outlook_smart_input_variant: String,
    pub(in crate::mapi) outlook_smart_input_variant_applied: bool,
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
    pub(in crate::mapi) last_default_folder_hierarchy_membership_summary: String,
    pub(in crate::mapi) execute_count: usize,
    pub(in crate::mapi) rop_ids_seen: Vec<u8>,
    pub(in crate::mapi) opened_folder_ids: Vec<u64>,
    pub(in crate::mapi) bootstrap_probe_observed: bool,
    pub(in crate::mapi) set_properties_probe_observed: bool,
    pub(in crate::mapi) last_getprops_request_contract: String,
    pub(in crate::mapi) last_setprops_request_contract: String,
    pub(in crate::mapi) request_contract_sequence: Vec<String>,
    pub(in crate::mapi) content_sync_configure_observed: bool,
    pub(in crate::mapi) release_client_initiated: bool,
    pub(in crate::mapi) logoff_client_initiated: bool,
    pub(in crate::mapi) completed_sync_checkpoint_folder_ids: Vec<u64>,
    pub(in crate::mapi) completed_sync_checkpoint_summaries: Vec<String>,
    pub(in crate::mapi) inbox_open_folder_probe_count: usize,
    pub(in crate::mapi) inbox_folder_type_getprops_probe_count: usize,
    pub(in crate::mapi) inbox_normal_contents_table_observed: bool,
    pub(in crate::mapi) inbox_normal_contents_table_setcolumns_observed: bool,
    pub(in crate::mapi) inbox_normal_contents_table_query_rows_observed: bool,
    pub(in crate::mapi) last_inbox_normal_contents_table_setcolumns_handle: Option<u32>,
    pub(in crate::mapi) last_inbox_normal_contents_table_query_rows_handle: Option<u32>,
    pub(in crate::mapi) inbox_associated_contents_table_observed: bool,
    pub(in crate::mapi) inbox_associated_broad_ipm_configuration_findrow_matched: bool,
    pub(in crate::mapi) inbox_associated_query_rows_returned_non_empty: bool,
    pub(in crate::mapi) inbox_associated_query_rows_reached_end: bool,
    pub(in crate::mapi) receive_folder_verification_passed: bool,
    pub(in crate::mapi) inbox_associated_config_open_observed: bool,
    pub(in crate::mapi) inbox_associated_config_stream_open_observed: bool,
    pub(in crate::mapi) inbox_associated_config_stream_read_observed: bool,
    pub(in crate::mapi) inbox_rule_organizer_stream_read_observed: bool,
    pub(in crate::mapi) last_inbox_open_folder_context: String,
    pub(in crate::mapi) last_inbox_contents_table_context: String,
    pub(in crate::mapi) last_inbox_normal_contents_table_setcolumns_context: String,
    pub(in crate::mapi) last_inbox_normal_contents_table_query_rows_context: String,
    pub(in crate::mapi) last_inbox_associated_query_context: String,
    pub(in crate::mapi) last_inbox_associated_non_empty_query_context: String,
    pub(in crate::mapi) last_inbox_associated_end_query_context: String,
    pub(in crate::mapi) last_inbox_associated_find_context: String,
    pub(in crate::mapi) last_inbox_rule_organizer_stream_context: String,
    pub(in crate::mapi) last_common_views_inbox_shortcut_context: String,
    pub(in crate::mapi) last_inbox_hierarchy_table_context: String,
    pub(in crate::mapi) last_inbox_hierarchy_query_context: String,
    pub(in crate::mapi) last_inbox_related_release_context: String,
    pub(in crate::mapi) last_inbox_folder_type_getprops_context: String,
    pub(in crate::mapi) last_successful_execute_context: String,
    pub(in crate::mapi) last_successful_non_release_execute_context: String,
    pub(in crate::mapi) last_table_context: String,
    pub(in crate::mapi) last_table_query_rows_context: String,
    pub(in crate::mapi) last_table_release_context: String,
    pub(in crate::mapi) first_inbox_loop_transition_context: String,
    pub(in crate::mapi) inbox_loop_transition_logged: bool,
    pub(in crate::mapi) post_inbox_fai_handoff_logged: bool,
    pub(in crate::mapi) post_common_views_handoff_logged: bool,
    pub(in crate::mapi) post_inbox_fai_reopen_logged: bool,
    pub(in crate::mapi) post_inbox_fai_folder_type_probe_loop_logged: bool,
    pub(in crate::mapi) post_rule_organizer_stream_reopen_logged: bool,
    pub(in crate::mapi) recent_probe_actions: Vec<String>,
    pub(in crate::mapi) outlook_view_failure_trace_events: Vec<String>,
    pub(in crate::mapi) outlook_stream_batch_observed: bool,
    pub(in crate::mapi) outlook_stream_batch_summaries: Vec<String>,
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
    AssociatedConfigProperty { handle: u32, property_tag: u32 },
    PublicFolderItemProperty { handle: u32, property_tag: u32 },
    VolatileProperty,
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
        pending_properties: HashMap<u32, MapiValue>,
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
    SearchFolderDefinitionMessage {
        folder_id: u64,
        message_id: u64,
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
        columns_set: bool,
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
        columns_set: bool,
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
        columns_set: bool,
        sort_orders: Vec<MapiSortOrder>,
        restriction: Option<MapiRestriction>,
        bookmarks: HashMap<Vec<u8>, TableBookmark>,
        next_bookmark: u32,
        position: usize,
    },
    PermissionTable {
        folder_id: u64,
        columns: Vec<u32>,
        columns_set: bool,
        position: usize,
    },
    RuleTable {
        folder_id: u64,
        columns: Vec<u32>,
        columns_set: bool,
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
        disposition: Option<String>,
        content_id: Option<String>,
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
        checkpoint_zero_delta: bool,
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
