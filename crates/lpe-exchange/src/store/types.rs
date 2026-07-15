use lpe_storage::CollaborationRights;
use uuid::Uuid;

use crate::mapi::notifications::MapiNotificationEvent;
use crate::mapi::properties::MapiNamedProperty;

#[derive(Debug, Clone)]
pub(crate) enum MapiEventCreateOutcome {
    Created(lpe_storage::MapiEventCreateResult),
    NotFound,
    AccessDenied,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MapiIdentityObjectKind {
    Account,
    Mailbox,
    Message,
    Contact,
    CalendarEvent,
    Task,
    Note,
    JournalEntry,
    SearchFolderDefinition,
    ConversationAction,
    NavigationShortcut,
    AssociatedConfig,
    DelegateFreeBusyMessage,
    PublicFolder,
    PublicFolderItem,
    Rule,
}

impl MapiIdentityObjectKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Account => "account",
            Self::Mailbox => "mailbox",
            Self::Message => "message",
            Self::Contact => "contact",
            Self::CalendarEvent => "calendar_event",
            Self::Task => "task",
            Self::Note => "note",
            Self::JournalEntry => "journal_entry",
            Self::SearchFolderDefinition => "search_folder_definition",
            Self::ConversationAction => "conversation_action",
            Self::NavigationShortcut => "navigation_shortcut",
            Self::AssociatedConfig => "associated_config",
            Self::DelegateFreeBusyMessage => "delegate_freebusy_message",
            Self::PublicFolder => "public_folder",
            Self::PublicFolderItem => "public_folder_item",
            Self::Rule => "sieve_script",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MapiNavigationShortcutRecord {
    pub(crate) id: Uuid,
    pub(crate) account_id: Uuid,
    pub(crate) subject: String,
    pub(crate) target_folder_id: Option<u64>,
    pub(crate) shortcut_type: u32,
    pub(crate) flags: u32,
    pub(crate) save_stamp: u32,
    pub(crate) section: u32,
    pub(crate) ordinal: u32,
    pub(crate) group_header_id: Option<Uuid>,
    pub(crate) group_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UpsertMapiNavigationShortcutInput {
    pub(crate) id: Option<Uuid>,
    pub(crate) account_id: Uuid,
    pub(crate) subject: String,
    pub(crate) target_folder_id: Option<u64>,
    pub(crate) shortcut_type: u32,
    pub(crate) flags: u32,
    pub(crate) save_stamp: u32,
    pub(crate) section: u32,
    pub(crate) ordinal: u32,
    pub(crate) group_header_id: Option<Uuid>,
    pub(crate) group_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MapiAssociatedConfigRecord {
    pub(crate) id: Uuid,
    pub(crate) account_id: Uuid,
    pub(crate) folder_id: u64,
    pub(crate) message_class: String,
    pub(crate) subject: String,
    pub(crate) properties_json: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UpsertMapiAssociatedConfigInput {
    pub(crate) id: Option<Uuid>,
    pub(crate) account_id: Uuid,
    pub(crate) folder_id: u64,
    pub(crate) message_class: String,
    pub(crate) subject: String,
    pub(crate) properties_json: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MapiIdentityRequest {
    pub(crate) object_kind: MapiIdentityObjectKind,
    pub(crate) canonical_id: Uuid,
    pub(crate) reserved_global_counter: Option<u64>,
    pub(crate) source_key: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MapiIdentityRecord {
    pub(crate) object_kind: MapiIdentityObjectKind,
    pub(crate) canonical_id: Uuid,
    pub(crate) object_id: u64,
    pub(crate) source_key: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MapiIdentityLookupRecord {
    pub(crate) object_kind: MapiIdentityObjectKind,
    pub(crate) canonical_id: Uuid,
    pub(crate) object_id: u64,
    pub(crate) source_key: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MapiNamedPropertyMapping {
    pub(crate) property_id: u16,
    pub(crate) property: MapiNamedProperty,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum MapiCustomPropertyObjectKind {
    Message,
    Contact,
    CalendarEvent,
    Task,
    Note,
    JournalEntry,
    Attachment,
    PublicFolderItem,
}

impl MapiCustomPropertyObjectKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Message => "message",
            Self::Contact => "contact",
            Self::CalendarEvent => "calendar_event",
            Self::Task => "task",
            Self::Note => "note",
            Self::JournalEntry => "journal_entry",
            Self::Attachment => "attachment",
            Self::PublicFolderItem => "public_folder_item",
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MapiCustomPropertyValue {
    pub(crate) property_tag: u32,
    pub(crate) property_type: u16,
    pub(crate) property_value: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MapiFolderProfilePropertyValue {
    pub(crate) folder_id: u64,
    pub(crate) property_tag: u32,
    pub(crate) property_type: u16,
    pub(crate) property_value: Vec<u8>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum MapiCheckpointKind {
    Hierarchy,
    Content,
    ReadState,
}

impl MapiCheckpointKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Hierarchy => "hierarchy",
            Self::Content => "content",
            Self::ReadState => "read_state",
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MapiSyncCheckpoint {
    pub(crate) mailbox_id: Option<Uuid>,
    pub(crate) checkpoint_kind: MapiCheckpointKind,
    pub(crate) last_change_sequence: u64,
    pub(crate) last_modseq: u64,
    pub(crate) cursor_json: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MapiNotificationPoll {
    pub(crate) event_pending: bool,
    pub(crate) cursor: Option<i64>,
    pub(crate) events: Vec<MapiNotificationEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EwsUserConfiguration {
    pub(crate) id: Uuid,
    pub(crate) scope_kind: String,
    pub(crate) mailbox_id: Option<Uuid>,
    pub(crate) public_folder_id: Option<Uuid>,
    pub(crate) config_name: String,
    pub(crate) config_class: String,
    pub(crate) dictionary_json: serde_json::Value,
    pub(crate) xml_payload: Option<String>,
    pub(crate) binary_payload: Option<Vec<u8>>,
    pub(crate) modseq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EwsUserConfigurationKey {
    pub(crate) scope_kind: String,
    pub(crate) mailbox_id: Option<Uuid>,
    pub(crate) public_folder_id: Option<Uuid>,
    pub(crate) config_name: String,
    pub(crate) config_class: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UpsertEwsUserConfigurationInput {
    pub(crate) account_id: Uuid,
    pub(crate) key: EwsUserConfigurationKey,
    pub(crate) dictionary_json: serde_json::Value,
    pub(crate) xml_payload: Option<String>,
    pub(crate) binary_payload: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EwsRetentionPolicyTag {
    pub(crate) id: Uuid,
    pub(crate) display_name: String,
    pub(crate) tag_type: String,
    pub(crate) action: String,
    pub(crate) retention_days: Option<i32>,
    pub(crate) is_visible: bool,
    pub(crate) description: String,
    pub(crate) opted_into: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EwsSearchableMailbox {
    pub(crate) account_id: Uuid,
    pub(crate) email: String,
    pub(crate) display_name: String,
    pub(crate) litigation_hold_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EwsDiscoverySearchConfig {
    pub(crate) id: Uuid,
    pub(crate) display_name: String,
    pub(crate) query_text: String,
    pub(crate) updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EwsDiscoverySearchItem {
    pub(crate) id: Uuid,
    pub(crate) account_id: Uuid,
    pub(crate) mailbox_message_id: Uuid,
    pub(crate) message_id: Uuid,
    pub(crate) subject: String,
    pub(crate) preview: String,
    pub(crate) rank: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EwsDiscoverySearchResult {
    pub(crate) search_id: Uuid,
    pub(crate) job_id: Uuid,
    pub(crate) query_text: String,
    pub(crate) result_count: usize,
    pub(crate) items: Vec<EwsDiscoverySearchItem>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EwsMessageTrackingReport {
    pub(crate) report_id: String,
    pub(crate) account_id: Uuid,
    pub(crate) sender: String,
    pub(crate) recipients: Vec<String>,
    pub(crate) subject: String,
    pub(crate) submitted_at: String,
    pub(crate) status: String,
    pub(crate) trace_id: Option<String>,
    pub(crate) remote_message_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EwsMessageTrackingEvent {
    pub(crate) event_source: String,
    pub(crate) event_kind: String,
    pub(crate) recipient_address: Option<String>,
    pub(crate) timestamp: String,
    pub(crate) dsn_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EwsMessageTrackingReportDetail {
    pub(crate) report: EwsMessageTrackingReport,
    pub(crate) events: Vec<EwsMessageTrackingEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EwsHoldMailbox {
    pub(crate) account_id: Uuid,
    pub(crate) email: String,
    pub(crate) display_name: String,
    pub(crate) hold_id: Option<Uuid>,
    pub(crate) hold_name: Option<String>,
    pub(crate) query_text: Option<String>,
    pub(crate) active: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EwsNonIndexableReport {
    pub(crate) id: Uuid,
    pub(crate) account_id: Uuid,
    pub(crate) email: String,
    pub(crate) report_kind: String,
    pub(crate) reason: String,
    pub(crate) message_id: Option<Uuid>,
    pub(crate) attachment_id: Option<Uuid>,
    pub(crate) detected_at: String,
    pub(crate) resolved: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EwsTransferEntry {
    pub(crate) id: Uuid,
    pub(crate) ordinal: i32,
    pub(crate) item_kind: String,
    pub(crate) canonical_id: Option<Uuid>,
    pub(crate) source_item_id: Option<String>,
    pub(crate) status: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EwsTransferJob {
    pub(crate) id: Uuid,
    pub(crate) direction: String,
    pub(crate) status: String,
    pub(crate) total_items: usize,
    pub(crate) entries: Vec<EwsTransferEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EwsMailAppManifest {
    pub(crate) catalog_id: Uuid,
    pub(crate) app_id: String,
    pub(crate) display_name: String,
    pub(crate) manifest_xml: String,
    pub(crate) provider_name: String,
    pub(crate) version: String,
    pub(crate) installation_status: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EwsMailAppInstall {
    pub(crate) catalog_id: Uuid,
    pub(crate) app_id: String,
    pub(crate) status: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EwsMailAppTokenEvent {
    pub(crate) id: Uuid,
    pub(crate) catalog_id: Uuid,
    pub(crate) app_id: String,
    pub(crate) issued_at: String,
    pub(crate) expires_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EwsAppMarketplacePolicy {
    pub(crate) enabled: bool,
    pub(crate) url: Option<String>,
}

impl Default for EwsAppMarketplacePolicy {
    fn default() -> Self {
        Self {
            enabled: false,
            url: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EwsUnifiedMessagingCall {
    pub(crate) id: Uuid,
    pub(crate) call_id: String,
    pub(crate) call_kind: String,
    pub(crate) status: String,
    pub(crate) phone_number: Option<String>,
    pub(crate) message_id: Option<Uuid>,
    pub(crate) requested_at: String,
    pub(crate) updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EwsDelegatePreferences {
    pub(crate) meeting_request_delivery: String,
    pub(crate) receives_meeting_request_copy: bool,
    pub(crate) may_view_private_items: bool,
}

impl Default for EwsDelegatePreferences {
    fn default() -> Self {
        Self {
            meeting_request_delivery: "delegate_and_owner".to_string(),
            receives_meeting_request_copy: true,
            may_view_private_items: false,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct EwsDelegate {
    pub(crate) owner_account_id: Uuid,
    pub(crate) grantee_account_id: Uuid,
    pub(crate) grantee_email: String,
    pub(crate) grantee_display_name: String,
    pub(crate) inbox_rights: CollaborationRights,
    pub(crate) calendar_rights: CollaborationRights,
    pub(crate) may_send_on_behalf: bool,
    pub(crate) may_send_as: bool,
    pub(crate) preferences: EwsDelegatePreferences,
}

#[derive(Debug, Clone)]
pub(crate) struct UpsertEwsDelegateInput {
    pub(crate) owner_account_id: Uuid,
    pub(crate) grantee_email: String,
    pub(crate) inbox_rights: CollaborationRights,
    pub(crate) calendar_rights: CollaborationRights,
    pub(crate) may_send_on_behalf: bool,
    pub(crate) preferences: EwsDelegatePreferences,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MapiAssociatedConfigChange {
    pub(crate) folder_id: u64,
    pub(crate) config_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MapiSyncChangeSet {
    pub(crate) current_change_sequence: u64,
    pub(crate) current_modseq: u64,
    pub(crate) changed_mailbox_ids: Vec<Uuid>,
    pub(crate) deleted_mailbox_object_ids: Vec<u64>,
    pub(crate) deleted_search_folder_object_ids: Vec<u64>,
    pub(crate) changed_message_ids: Vec<Uuid>,
    pub(crate) changed_contact_ids: Vec<Uuid>,
    pub(crate) changed_calendar_event_ids: Vec<Uuid>,
    pub(crate) changed_task_ids: Vec<Uuid>,
    pub(crate) changed_note_ids: Vec<Uuid>,
    pub(crate) changed_journal_entry_ids: Vec<Uuid>,
    pub(crate) changed_conversation_action_ids: Vec<Uuid>,
    pub(crate) changed_navigation_shortcut_ids: Vec<Uuid>,
    pub(crate) changed_associated_config_ids: Vec<MapiAssociatedConfigChange>,
    pub(crate) deleted_message_ids: Vec<Uuid>,
    pub(crate) deleted_contact_ids: Vec<Uuid>,
    pub(crate) deleted_calendar_event_ids: Vec<Uuid>,
    pub(crate) deleted_task_ids: Vec<Uuid>,
    pub(crate) deleted_note_ids: Vec<Uuid>,
    pub(crate) deleted_journal_entry_ids: Vec<Uuid>,
    pub(crate) deleted_conversation_action_ids: Vec<Uuid>,
    pub(crate) deleted_navigation_shortcut_ids: Vec<Uuid>,
    pub(crate) deleted_associated_config_ids: Vec<MapiAssociatedConfigChange>,
}

impl Default for MapiSyncChangeSet {
    fn default() -> Self {
        Self {
            current_change_sequence: 0,
            current_modseq: 1,
            changed_mailbox_ids: Vec::new(),
            deleted_mailbox_object_ids: Vec::new(),
            deleted_search_folder_object_ids: Vec::new(),
            changed_message_ids: Vec::new(),
            changed_contact_ids: Vec::new(),
            changed_calendar_event_ids: Vec::new(),
            changed_task_ids: Vec::new(),
            changed_note_ids: Vec::new(),
            changed_journal_entry_ids: Vec::new(),
            changed_conversation_action_ids: Vec::new(),
            changed_navigation_shortcut_ids: Vec::new(),
            changed_associated_config_ids: Vec::new(),
            deleted_message_ids: Vec::new(),
            deleted_contact_ids: Vec::new(),
            deleted_calendar_event_ids: Vec::new(),
            deleted_task_ids: Vec::new(),
            deleted_note_ids: Vec::new(),
            deleted_journal_entry_ids: Vec::new(),
            deleted_conversation_action_ids: Vec::new(),
            deleted_navigation_shortcut_ids: Vec::new(),
            deleted_associated_config_ids: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExchangeAddressBookEntry {
    pub(crate) id: Uuid,
    pub(crate) display_name: String,
    pub(crate) email: String,
    pub(crate) entry_kind: ExchangeAddressBookEntryKind,
    pub(crate) directory_kind: ExchangeAddressBookDirectoryKind,
    pub(crate) member_emails: Vec<String>,
    pub(crate) details: ExchangeAddressBookEntryDetails,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ExchangeAddressBookEntryDetails {
    pub(crate) given_name: String,
    pub(crate) surname: String,
    pub(crate) nickname: String,
    pub(crate) primary_phone: String,
    pub(crate) mobile_phone: String,
    pub(crate) home_phone: String,
    pub(crate) business2_phones: Vec<String>,
    pub(crate) company_name: String,
    pub(crate) title: String,
    pub(crate) department_name: String,
    pub(crate) postal_address: String,
    pub(crate) street_address: String,
    pub(crate) locality: String,
    pub(crate) state_or_province: String,
    pub(crate) country: String,
    pub(crate) postal_code: String,
    pub(crate) phonetic_given_name: String,
    pub(crate) phonetic_surname: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EwsImGroup {
    pub(crate) id: Uuid,
    pub(crate) display_name: String,
    pub(crate) modseq: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EwsImGroupMember {
    pub(crate) id: Uuid,
    pub(crate) group_id: Uuid,
    pub(crate) member_kind: String,
    pub(crate) contact_id: Option<Uuid>,
    pub(crate) account_id: Option<Uuid>,
    pub(crate) external_address: Option<String>,
    pub(crate) display_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EwsImList {
    pub(crate) groups: Vec<EwsImGroup>,
    pub(crate) members: Vec<EwsImGroupMember>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EwsImMemberInput {
    pub(crate) member_kind: String,
    pub(crate) contact_id: Option<Uuid>,
    pub(crate) account_id: Option<Uuid>,
    pub(crate) external_address: Option<String>,
    pub(crate) display_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MapiContentTableQuery {
    pub(crate) mailbox_id: Uuid,
    pub(crate) position: u64,
    pub(crate) limit: u64,
    pub(crate) sort_orders: Vec<MapiContentTableSort>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MapiContentTableQueryResult {
    pub(crate) ids: Vec<Uuid>,
    pub(crate) total: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MapiContentTableSort {
    pub(crate) field: MapiContentTableSortField,
    pub(crate) descending: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MapiContentTableSortField {
    ReceivedAt,
    ClientSubmitTime,
    Subject,
    SenderName,
    SenderEmail,
    DisplayTo,
    MessageSize,
    HasAttachments,
    MessageFlags,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExchangeAddressBookEntryKind {
    Account,
    Contact,
    DistributionList,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExchangeAddressBookDirectoryKind {
    Person,
    Room,
    Equipment,
}
