use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use lpe_mail_auth::StoreFuture;
use lpe_storage::{
    AccessibleContact, AccessibleEvent, ActiveSyncAttachment, CalendarEventAttachment, ClientNote,
    ClientReminder, ClientTask, CollaborationCollection, ConversationAction,
    DelegateFreeBusyMessageObject, JmapEmail, JmapMailbox, JournalEntry, MailboxRule, PublicFolder,
    PublicFolderItem, PublicFolderPermission, PublicFolderReplica, RecoverableItem, ReminderQuery,
    SearchFolderDefinition,
};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use uuid::Uuid;

use crate::mapi::permissions::{
    access_from_rights, reserved_permission_rows, rights_from_grant, MapiFolderAccess,
    MapiFolderPermission,
};
use crate::store::ExchangeStore;
use crate::store::MapiAssociatedConfigRecord;
use crate::store::{MapiIdentityObjectKind, MapiIdentityRequest, MapiNavigationShortcutRecord};

#[derive(Debug, Clone)]
pub(crate) struct MapiMailStoreSnapshot {
    folders: Vec<MapiFolder>,
    public_folders: Vec<MapiPublicFolder>,
    public_folder_items: Vec<MapiPublicFolderItem>,
    public_folder_replicas: Vec<MapiPublicFolderReplica>,
    collaboration_folders: Vec<MapiCollaborationFolder>,
    messages: Vec<MapiMessage>,
    contacts: Vec<MapiContact>,
    events: Vec<MapiEvent>,
    tasks: Vec<MapiTask>,
    notes: Vec<MapiNote>,
    journal_entries: Vec<MapiJournalEntry>,
    search_folder_definitions: Vec<SearchFolderDefinition>,
    rules: Vec<MapiRule>,
    navigation_shortcuts: Vec<MapiNavigationShortcutMessage>,
    associated_configs: Vec<MapiAssociatedConfigMessage>,
    associated_config_identity_ids: Vec<MapiAssociatedConfigIdentity>,
    conversation_actions: Vec<MapiConversationActionMessage>,
    delegate_freebusy_messages: Vec<MapiDelegateFreeBusyMessage>,
    recoverable_items: Vec<MapiRecoverableItemMessage>,
    reminders: Vec<ClientReminder>,
    folder_permissions: Vec<MapiFolderPermission>,
    public_folder_permissions: Vec<MapiFolderPermission>,
    content_windows: Vec<MapiContentTableWindow>,
}

#[derive(Debug, Clone)]
pub(crate) struct MapiFolder {
    pub(crate) id: u64,
    pub(crate) canonical_id: Uuid,
    pub(crate) mailbox: JmapMailbox,
}

#[derive(Debug, Clone)]
pub(crate) struct MapiPublicFolder {
    pub(crate) id: u64,
    pub(crate) folder: PublicFolder,
    pub(crate) item_count: u32,
    pub(crate) child_count: u32,
}

#[derive(Debug, Clone)]
pub(crate) struct MapiPublicFolderItem {
    pub(crate) id: u64,
    pub(crate) folder_id: u64,
    pub(crate) item: PublicFolderItem,
}

#[derive(Debug, Clone)]
pub(crate) struct MapiPublicFolderReplica {
    pub(crate) folder_id: u64,
    pub(crate) replica: PublicFolderReplica,
}

#[derive(Debug, Clone)]
pub(crate) struct MapiCollaborationFolder {
    pub(crate) id: u64,
    pub(crate) kind: MapiCollaborationFolderKind,
    pub(crate) collection: CollaborationCollection,
    pub(crate) item_count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MapiCollaborationFolderKind {
    Contacts,
    Calendar,
    Task,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct MapiMessage {
    pub(crate) id: u64,
    pub(crate) folder_id: u64,
    pub(crate) canonical_id: Uuid,
    pub(crate) email: JmapEmail,
    pub(crate) attachments: Vec<MapiAttachment>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct MapiContact {
    pub(crate) id: u64,
    pub(crate) folder_id: u64,
    pub(crate) canonical_id: Uuid,
    pub(crate) contact: AccessibleContact,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct MapiEvent {
    pub(crate) id: u64,
    pub(crate) folder_id: u64,
    pub(crate) canonical_id: Uuid,
    pub(crate) event: AccessibleEvent,
    pub(crate) attachments: Vec<MapiAttachment>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct MapiTask {
    pub(crate) id: u64,
    pub(crate) folder_id: u64,
    pub(crate) canonical_id: Uuid,
    pub(crate) task: ClientTask,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct MapiNote {
    pub(crate) id: u64,
    pub(crate) folder_id: u64,
    pub(crate) canonical_id: Uuid,
    pub(crate) note: ClientNote,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct MapiJournalEntry {
    pub(crate) id: u64,
    pub(crate) folder_id: u64,
    pub(crate) canonical_id: Uuid,
    pub(crate) entry: JournalEntry,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct MapiRule {
    pub(crate) id: u64,
    pub(crate) canonical_id: Uuid,
    pub(crate) name: String,
    pub(crate) is_active: bool,
    pub(crate) condition_summary: String,
    pub(crate) action_summary: String,
    pub(crate) updated_at: String,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct MapiNavigationShortcutMessage {
    pub(crate) id: u64,
    pub(crate) folder_id: u64,
    pub(crate) canonical_id: Uuid,
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

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct MapiCommonViewNamedViewMessage {
    pub(crate) id: u64,
    pub(crate) folder_id: u64,
    pub(crate) canonical_id: Uuid,
    pub(crate) name: String,
    pub(crate) view_flags: u32,
    pub(crate) view_type: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct MapiAssociatedConfigMessage {
    pub(crate) id: u64,
    pub(crate) folder_id: u64,
    pub(crate) canonical_id: Uuid,
    pub(crate) message_class: String,
    pub(crate) subject: String,
    pub(crate) properties_json: serde_json::Value,
}

#[derive(Debug, Clone)]
pub(crate) struct MapiAssociatedConfigIdentity {
    pub(crate) canonical_id: Uuid,
    pub(crate) object_id: u64,
}

fn deduplicate_associated_config_messages(
    messages: Vec<MapiAssociatedConfigMessage>,
) -> Vec<MapiAssociatedConfigMessage> {
    let mut seen = HashSet::new();
    messages
        .into_iter()
        .filter(|message| seen.insert((message.folder_id, message.message_class.clone())))
        .collect()
}

mod associated_config;
mod snapshot;
#[cfg(test)]
pub(crate) use associated_config::OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_ID;
use associated_config::*;
pub(crate) use associated_config::{
    is_outlook_common_views_default_named_view_id,
    is_outlook_common_views_default_navigation_shortcut_id, is_outlook_configuration_message_class,
    is_outlook_configuration_message_class_name, is_outlook_contact_default_associated_config_id,
    is_outlook_default_conversation_action_id, is_outlook_default_folder_named_view_id,
    is_outlook_inbox_default_associated_config_id,
    is_outlook_inbox_virtual_only_associated_config_id, is_outlook_local_freebusy_message_id,
    is_outlook_quick_step_default_associated_config_id,
    is_outlook_umolk_user_options_message_class,
    modeled_virtual_associated_config_message_for_canonical_id,
    outlook_default_folder_named_view_id, outlook_default_folder_named_view_name,
    outlook_inbox_broad_startup_associated_config_defaults,
    outlook_inbox_exact_virtual_associated_config_for_message_class,
    outlook_inbox_message_list_settings_default, OUTLOOK_COMMON_VIEWS_COMPACT_NAMED_VIEW_ID,
    OUTLOOK_COMMON_VIEWS_SENT_TO_NAMED_VIEW_ID, OUTLOOK_DEFAULT_FOLDER_NAMED_VIEW_ID,
    OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_CLASS, OUTLOOK_INBOX_RULE_ORGANIZER_CONFIG_CLASS,
    OUTLOOK_QUICK_STEP_CUSTOM_ACTION_CLASS,
};

pub(crate) enum MapiCommonViewsMessage {
    NavigationShortcut(MapiNavigationShortcutMessage),
    #[allow(dead_code)]
    NamedView(MapiCommonViewNamedViewMessage),
    SearchFolderDefinition(SearchFolderDefinition),
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct MapiConversationActionMessage {
    pub(crate) id: u64,
    pub(crate) folder_id: u64,
    pub(crate) canonical_id: Uuid,
    pub(crate) action: ConversationAction,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct MapiDelegateFreeBusyMessage {
    pub(crate) id: u64,
    pub(crate) folder_id: u64,
    pub(crate) canonical_id: Uuid,
    pub(crate) message: DelegateFreeBusyMessageObject,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct MapiRecoverableItemMessage {
    pub(crate) id: u64,
    pub(crate) folder_id: u64,
    pub(crate) canonical_id: Uuid,
    pub(crate) item: RecoverableItem,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct MapiAttachment {
    pub(crate) attach_num: u32,
    pub(crate) canonical_id: Uuid,
    pub(crate) file_reference: String,
    pub(crate) file_name: String,
    pub(crate) media_type: String,
    pub(crate) disposition: Option<String>,
    pub(crate) content_id: Option<String>,
    pub(crate) size_octets: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct MapiContentTableWindow {
    pub(crate) folder_id: u64,
    pub(crate) view_signature: u64,
    pub(crate) offset: usize,
    pub(crate) total: usize,
    pub(crate) message_ids: Vec<Uuid>,
}

fn fixed_search_folder_role(folder_id: u64) -> Option<&'static str> {
    match folder_id {
        crate::mapi::identity::CONTACTS_SEARCH_FOLDER_ID => Some("contacts_search"),
        crate::mapi::identity::TODO_SEARCH_FOLDER_ID => Some("todo_search"),
        crate::mapi::identity::REMINDERS_FOLDER_ID => Some("reminders"),
        crate::mapi::identity::TRACKED_MAIL_PROCESSING_FOLDER_ID => Some("tracked_mail_processing"),
        _ => None,
    }
}

fn associated_config_source_key(properties_json: &serde_json::Value) -> Option<Vec<u8>> {
    let value = properties_json
        .get("0x65e00102")
        .filter(|value| value.get("type").and_then(serde_json::Value::as_str) == Some("binary"))?
        .get("value")?
        .as_str()?;
    hex_to_bytes(value)
}

fn hex_to_bytes(value: &str) -> Option<Vec<u8>> {
    if value.len() % 2 != 0 {
        return None;
    }
    let mut bytes = Vec::with_capacity(value.len() / 2);
    for chunk in value.as_bytes().chunks(2) {
        let high = hex_digit(chunk[0])?;
        let low = hex_digit(chunk[1])?;
        bytes.push((high << 4) | low);
    }
    Some(bytes)
}

fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn mapi_search_folder_definition_to_folder(
    definition: &SearchFolderDefinition,
) -> Option<MapiFolder> {
    if definition.is_builtin || definition.definition_kind != "user_saved" {
        return None;
    }
    let id = crate::mapi::identity::mapped_mapi_object_id(&definition.id)?;
    Some(MapiFolder {
        id,
        canonical_id: definition.id,
        mailbox: JmapMailbox {
            id: definition.id,
            parent_id: None,
            role: mapi_search_folder_role(&definition.result_object_kind).to_string(),
            name: definition.display_name.clone(),
            sort_order: i32::MAX,
            modseq: crate::mapi::identity::global_counter_from_store_id(id).unwrap_or(1),
            total_emails: 0,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        },
    })
}

fn user_saved_search_folder_projection_key(definition: &SearchFolderDefinition) -> String {
    format!(
        "{}\x1f{}",
        definition.display_name.trim().to_ascii_lowercase(),
        definition.result_object_kind
    )
}

fn user_saved_search_folder_is_projectable(definition: &SearchFolderDefinition) -> bool {
    if definition.is_builtin || definition.definition_kind != "user_saved" {
        return true;
    }
    if definition
        .restriction_json
        .get("kind")
        .and_then(serde_json::Value::as_str)
        != Some("mapi_bounded")
    {
        return true;
    }
    definition
        .restriction_json
        .get("all")
        .and_then(serde_json::Value::as_array)
        .map(|clauses| !clauses.is_empty())
        .unwrap_or(true)
}

fn mapi_search_folder_role(result_object_kind: &str) -> &'static str {
    match result_object_kind {
        "contact" => "__mapi_search_folder_contact",
        "task" => "__mapi_search_folder_task",
        "mixed" => "__mapi_search_folder_mixed",
        _ => "__mapi_search_folder_message",
    }
}

pub(crate) trait MapiStore: ExchangeStore {
    fn load_mapi_mail_store<'a>(
        &'a self,
        account_id: Uuid,
        message_limit: u64,
    ) -> StoreFuture<'a, MapiMailStoreSnapshot>;
}

impl<T: ExchangeStore> MapiStore for T {
    fn load_mapi_mail_store<'a>(
        &'a self,
        account_id: Uuid,
        message_limit: u64,
    ) -> StoreFuture<'a, MapiMailStoreSnapshot> {
        Box::pin(async move {
            let mailboxes = self.ensure_jmap_system_mailboxes(account_id).await?;
            let mut message_ids = self.fetch_all_jmap_email_ids(account_id).await?;
            message_ids.truncate(message_limit.min(usize::MAX as u64) as usize);
            let emails = self.fetch_jmap_emails(account_id, &message_ids).await?;
            let mut attachments = Vec::with_capacity(emails.len());
            for email in &emails {
                let message_attachments =
                    self.fetch_message_attachments(account_id, email.id).await?;
                attachments.push((email.id, message_attachments));
            }
            let contact_collections = self
                .fetch_accessible_contact_collections(account_id)
                .await?;
            let calendar_collections = self
                .fetch_accessible_calendar_collections(account_id)
                .await?;
            let task_collections = self.fetch_accessible_task_collections(account_id).await?;
            let mut contacts = Vec::new();
            for collection in &contact_collections {
                contacts.extend(
                    self.fetch_accessible_contacts_in_collection(account_id, &collection.id)
                        .await?,
                );
            }
            let mut events = Vec::new();
            if calendar_collections.is_empty() {
                events.extend(
                    self.fetch_accessible_events_in_collection(account_id, "default")
                        .await
                        .unwrap_or_default(),
                );
            } else {
                for collection in &calendar_collections {
                    events.extend(
                        self.fetch_accessible_events_in_collection(account_id, &collection.id)
                            .await?,
                    );
                }
            }
            let mut tasks = Vec::new();
            for collection in &task_collections {
                tasks.extend(
                    self.fetch_accessible_tasks_in_collection(account_id, &collection.id)
                        .await?,
                );
            }
            let notes = self.fetch_mapi_notes(account_id).await?;
            let journal_entries = self.fetch_mapi_journal_entries(account_id).await?;
            let search_folder_definitions = self.fetch_search_folders(account_id).await?;
            let rules = self.list_mailbox_rules(account_id).await?;
            let navigation_shortcuts = self.fetch_mapi_navigation_shortcuts(account_id).await?;
            let mut associated_configs = self.fetch_mapi_associated_configs(account_id).await?;
            let dropped_empty_synthetic_inbox_configs = associated_configs
                .iter()
                .filter(|config| is_empty_synthetic_inbox_associated_config(config))
                .count();
            if dropped_empty_synthetic_inbox_configs > 0 {
                associated_configs
                    .retain(|config| !is_empty_synthetic_inbox_associated_config(config));
                tracing::debug!(
                rca_debug = true,
                adapter = "mapi",
                        account_id = %account_id,
                        folder_id = crate::mapi::identity::INBOX_FOLDER_ID,
                        dropped_empty_synthetic_inbox_configs,
                        "rca debug mapi dropped empty synthetic inbox associated configs"
                    );
            }
            let dropped_empty_named_view_configs = associated_configs
                .iter()
                .filter(|config| is_empty_outlook_inbox_named_view_placeholder(config))
                .count();
            if dropped_empty_named_view_configs > 0 {
                associated_configs
                    .retain(|config| !is_empty_outlook_inbox_named_view_placeholder(config));
                tracing::debug!(
                    rca_debug = true,
                    adapter = "mapi",
                    account_id = %account_id,
                    folder_id = crate::mapi::identity::INBOX_FOLDER_ID,
                    dropped_empty_named_view_configs,
                    message_class = OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_CLASS,
                    "rca debug mapi dropped empty inbox named view associated config"
                );
            }
            let dropped_empty_rule_organizer_configs = associated_configs
                .iter()
                .filter(|config| is_empty_outlook_rule_organizer_placeholder(config))
                .count();
            if dropped_empty_rule_organizer_configs > 0 {
                associated_configs
                    .retain(|config| !is_empty_outlook_rule_organizer_placeholder(config));
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    account_id = %account_id,
                    folder_id = crate::mapi::identity::INBOX_FOLDER_ID,
                    dropped_empty_rule_organizer_configs,
                    message_class = OUTLOOK_INBOX_RULE_ORGANIZER_CONFIG_CLASS,
                    stream_property = OUTLOOK_RULE_ORGANIZER_BINARY_6802_JSON_KEY,
                    "rca debug mapi dropped empty inbox rule organizer associated config"
                );
            }
            let dropped_stale_umolk_user_options_configs = associated_configs
                .iter()
                .filter(|config| is_stale_outlook_umolk_user_options_placeholder(config))
                .count();
            if dropped_stale_umolk_user_options_configs > 0 {
                associated_configs
                    .retain(|config| !is_stale_outlook_umolk_user_options_placeholder(config));
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    account_id = %account_id,
                    folder_id = crate::mapi::identity::INBOX_FOLDER_ID,
                    dropped_stale_umolk_user_options_configs,
                    message_class = OUTLOOK_INBOX_UMOLK_USER_OPTIONS_CONFIG_CLASS,
                    stream_property = "0x7c070102",
                    "rca debug mapi dropped stale inbox umolk user options associated config"
                );
            }
            log_outlook_inbox_associated_config_bootstrap(
                account_id,
                &associated_configs,
                &[],
                &[],
            );
            let conversation_actions = self.fetch_conversation_actions(account_id).await?;
            let delegate_freebusy_messages =
                self.fetch_delegate_freebusy_messages(account_id).await?;
            let public_trees = self.fetch_public_folder_trees(account_id).await?;
            let mut public_folders = Vec::new();
            let mut pending_public_folder_ids = public_trees
                .iter()
                .filter_map(|tree| tree.root_folder_id)
                .collect::<Vec<_>>();
            while let Some(folder_id) = pending_public_folder_ids.pop() {
                let folder = self.fetch_public_folder(account_id, folder_id).await?;
                pending_public_folder_ids.extend(
                    self.fetch_public_folder_children(account_id, folder_id)
                        .await?
                        .into_iter()
                        .map(|child| child.id),
                );
                public_folders.push(folder);
            }
            let mut public_folder_items = Vec::new();
            let mut public_folder_permissions = Vec::new();
            let mut public_folder_replicas = Vec::new();
            for folder in &public_folders {
                public_folder_items.extend(
                    self.fetch_public_folder_items(account_id, folder.id)
                        .await?,
                );
                public_folder_permissions.extend(
                    self.fetch_public_folder_permissions(account_id, folder.id)
                        .await?,
                );
                public_folder_replicas.extend(
                    self.fetch_public_folder_replicas(account_id, folder.id)
                        .await?,
                );
            }
            let mut recoverable_items = Vec::new();
            for folder in ["deletions", "versions", "purges"] {
                recoverable_items.extend(
                    self.list_recoverable_items(account_id, Some(folder))
                        .await?,
                );
            }
            let reminders = self
                .query_client_reminders(
                    account_id,
                    ReminderQuery {
                        include_inactive: false,
                    },
                )
                .await?;
            let identity_requests = mapi_identity_requests(
                &mailboxes,
                &emails,
                &contact_collections,
                &calendar_collections,
                &task_collections,
                &contacts,
                &events,
                &tasks,
                &notes,
                &journal_entries,
                &search_folder_definitions,
                &rules,
                &navigation_shortcuts,
                &associated_configs,
                &conversation_actions,
                &delegate_freebusy_messages,
                &public_folders,
                &public_folder_items,
            );
            for identity in self
                .fetch_or_allocate_mapi_identities(account_id, &identity_requests)
                .await?
            {
                crate::mapi::identity::remember_mapi_identity_with_source_key(
                    identity.canonical_id,
                    identity.object_id,
                    Some(identity.source_key),
                );
            }
            let mailbox_ids = mailboxes
                .iter()
                .map(|mailbox| mailbox.id)
                .collect::<Vec<_>>();
            let folder_permissions = self
                .fetch_mapi_folder_permissions(account_id, &mailbox_ids)
                .await?;
            Ok(MapiMailStoreSnapshot::new(
                mailboxes,
                emails,
                attachments,
                contact_collections,
                calendar_collections,
                task_collections,
                contacts,
                events,
                tasks,
                folder_permissions,
            ))
            .map(|snapshot| snapshot.with_notes_and_journal(notes, journal_entries))
            .map(|snapshot| snapshot.with_search_folder_definitions(search_folder_definitions))
            .map(|snapshot| snapshot.with_rules(rules))
            .map(|snapshot| snapshot.with_navigation_shortcuts(navigation_shortcuts))
            .map(|snapshot| snapshot.with_associated_configs(associated_configs))
            .map(|snapshot| snapshot.with_conversation_actions(conversation_actions))
            .map(|snapshot| snapshot.with_delegate_freebusy_messages(delegate_freebusy_messages))
            .map(|snapshot| snapshot.with_recoverable_items(recoverable_items))
            .map(|snapshot| snapshot.with_reminders(reminders))
            .map(|snapshot| {
                snapshot.with_public_folders(
                    public_folders,
                    public_folder_items,
                    public_folder_permissions,
                )
            })
            .map(|snapshot| snapshot.with_public_folder_replicas(public_folder_replicas))
        })
    }
}

fn mapi_public_folder_permission(permission: PublicFolderPermission) -> MapiFolderPermission {
    MapiFolderPermission {
        mailbox_id: permission.public_folder_id,
        member_account_id: Some(permission.principal_account_id),
        member_name: permission.principal_display_name,
        rights: rights_from_grant(
            permission.rights.may_read,
            permission.rights.may_write,
            permission.rights.may_delete,
            permission.rights.may_share,
        ),
    }
}

fn mapi_identity_requests(
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    contact_collections: &[CollaborationCollection],
    calendar_collections: &[CollaborationCollection],
    task_collections: &[CollaborationCollection],
    contacts: &[AccessibleContact],
    events: &[AccessibleEvent],
    tasks: &[ClientTask],
    notes: &[ClientNote],
    journal_entries: &[JournalEntry],
    search_folder_definitions: &[SearchFolderDefinition],
    rules: &[MailboxRule],
    navigation_shortcuts: &[MapiNavigationShortcutRecord],
    associated_configs: &[MapiAssociatedConfigRecord],
    conversation_actions: &[ConversationAction],
    delegate_freebusy_messages: &[DelegateFreeBusyMessageObject],
    public_folders: &[PublicFolder],
    public_folder_items: &[PublicFolderItem],
) -> Vec<MapiIdentityRequest> {
    let mut requests = Vec::new();
    requests.extend(
        mailboxes
            .iter()
            .filter(|mailbox| !is_virtual_special_mailbox(mailbox))
            .map(|mailbox| MapiIdentityRequest {
                object_kind: MapiIdentityObjectKind::Mailbox,
                canonical_id: mailbox.id,
                reserved_global_counter: reserved_folder_counter_for_role(&mailbox.role),
                source_key: None,
            }),
    );
    requests.extend(collaboration_folder_identity_requests(
        contact_collections,
        calendar_collections,
        task_collections,
    ));
    requests.extend(emails.iter().map(|email| MapiIdentityRequest {
        object_kind: MapiIdentityObjectKind::Message,
        canonical_id: email.id,
        reserved_global_counter: None,
        source_key: None,
    }));
    requests.extend(contacts.iter().map(|contact| MapiIdentityRequest {
        object_kind: MapiIdentityObjectKind::Contact,
        canonical_id: contact.id,
        reserved_global_counter: None,
        source_key: None,
    }));
    requests.extend(events.iter().map(|event| MapiIdentityRequest {
        object_kind: MapiIdentityObjectKind::CalendarEvent,
        canonical_id: event.id,
        reserved_global_counter: None,
        source_key: None,
    }));
    requests.extend(tasks.iter().map(|task| MapiIdentityRequest {
        object_kind: MapiIdentityObjectKind::Task,
        canonical_id: task.id,
        reserved_global_counter: None,
        source_key: None,
    }));
    requests.extend(notes.iter().map(|note| MapiIdentityRequest {
        object_kind: MapiIdentityObjectKind::Note,
        canonical_id: note.id,
        reserved_global_counter: None,
        source_key: None,
    }));
    requests.extend(journal_entries.iter().map(|entry| MapiIdentityRequest {
        object_kind: MapiIdentityObjectKind::JournalEntry,
        canonical_id: entry.id,
        reserved_global_counter: None,
        source_key: None,
    }));
    requests.extend(
        search_folder_definitions
            .iter()
            .map(|definition| MapiIdentityRequest {
                object_kind: MapiIdentityObjectKind::SearchFolderDefinition,
                canonical_id: definition.id,
                reserved_global_counter: None,
                source_key: None,
            }),
    );
    requests.extend(rules.iter().map(|rule| MapiIdentityRequest {
        object_kind: MapiIdentityObjectKind::Rule,
        canonical_id: rule.id,
        reserved_global_counter: None,
        source_key: None,
    }));
    requests.extend(
        conversation_actions
            .iter()
            .map(|action| MapiIdentityRequest {
                object_kind: MapiIdentityObjectKind::ConversationAction,
                canonical_id: action.id,
                reserved_global_counter: None,
                source_key: None,
            }),
    );
    requests.extend(
        navigation_shortcuts
            .iter()
            .map(|shortcut| MapiIdentityRequest {
                object_kind: MapiIdentityObjectKind::NavigationShortcut,
                canonical_id: shortcut.id,
                reserved_global_counter: None,
                source_key: None,
            }),
    );
    requests.extend(associated_configs.iter().map(|config| MapiIdentityRequest {
        object_kind: MapiIdentityObjectKind::AssociatedConfig,
        canonical_id: config.id,
        reserved_global_counter: None,
        source_key: None,
    }));
    requests.extend(
        delegate_freebusy_messages
            .iter()
            .map(|message| MapiIdentityRequest {
                object_kind: MapiIdentityObjectKind::DelegateFreeBusyMessage,
                canonical_id: message.id,
                reserved_global_counter: None,
                source_key: None,
            }),
    );
    requests.extend(public_folders.iter().map(|folder| MapiIdentityRequest {
        object_kind: MapiIdentityObjectKind::PublicFolder,
        canonical_id: folder.id,
        reserved_global_counter: None,
        source_key: None,
    }));
    requests.extend(public_folder_items.iter().map(|item| MapiIdentityRequest {
        object_kind: MapiIdentityObjectKind::PublicFolderItem,
        canonical_id: item.id,
        reserved_global_counter: None,
        source_key: None,
    }));
    requests
}

pub(crate) fn collaboration_folder_identity_requests(
    contact_collections: &[CollaborationCollection],
    calendar_collections: &[CollaborationCollection],
    task_collections: &[CollaborationCollection],
) -> Vec<MapiIdentityRequest> {
    let mut requests = contact_collections
        .iter()
        .filter_map(|collection| {
            collaboration_folder_identity_canonical_id(
                MapiCollaborationFolderKind::Contacts,
                collection,
            )
        })
        .chain(calendar_collections.iter().filter_map(|collection| {
            collaboration_folder_identity_canonical_id(
                MapiCollaborationFolderKind::Calendar,
                collection,
            )
        }))
        .chain(task_collections.iter().filter_map(|collection| {
            collaboration_folder_identity_canonical_id(
                MapiCollaborationFolderKind::Task,
                collection,
            )
        }))
        .map(|canonical_id| MapiIdentityRequest {
            object_kind: MapiIdentityObjectKind::Mailbox,
            canonical_id,
            reserved_global_counter: None,
            source_key: None,
        })
        .collect::<Vec<_>>();

    if calendar_collections
        .iter()
        .any(|collection| matches!(collection.id.as_str(), "default" | "calendar"))
    {
        let mailbox = crate::mapi_mailstore::virtual_special_mailbox(
            crate::mapi::identity::CALENDAR_FOLDER_ID,
        )
        .expect("default Calendar virtual mailbox");
        requests.push(MapiIdentityRequest {
            object_kind: MapiIdentityObjectKind::Mailbox,
            canonical_id: mailbox.id,
            reserved_global_counter: Some(crate::mapi::identity::CALENDAR_FOLDER_COUNTER),
            source_key: None,
        });
    }

    requests
}

pub(crate) fn is_virtual_special_mailbox(mailbox: &JmapMailbox) -> bool {
    reserved_folder_counter_for_role(&mailbox.role).is_some_and(|counter| {
        crate::mapi_mailstore::virtual_special_mailbox(crate::mapi::identity::mapi_store_id(
            counter,
        ))
        .is_some_and(|virtual_mailbox| virtual_mailbox.id == mailbox.id)
    })
}

fn mapi_message_folder_id(email: &JmapEmail, folders: &[MapiFolder]) -> u64 {
    folders
        .iter()
        .find(|folder| folder.canonical_id == email.mailbox_id)
        .map(|folder| folder.id)
        .unwrap_or_else(|| mapi_folder_id_for_role(&email.mailbox_role))
}

fn mapi_folder_id(mailbox: &JmapMailbox) -> u64 {
    reserved_folder_id_for_role(&mailbox.role)
        .or_else(|| crate::mapi::identity::mapped_mapi_object_id(&mailbox.id))
        .expect("MAPI folder identity mapping missing")
}

fn mapi_folder_id_for_role(role: &str) -> u64 {
    reserved_folder_id_for_role(role).unwrap_or(crate::mapi::identity::ROOT_FOLDER_ID)
}

fn mapi_message_id(email: &JmapEmail) -> u64 {
    mapi_item_id(&email.id)
}

fn mapi_item_id(id: &Uuid) -> u64 {
    crate::mapi::identity::mapped_mapi_object_id(id).expect("MAPI item identity mapping missing")
}

fn mapi_event_id_matches(event: &MapiEvent, object_id: u64) -> bool {
    event.id == object_id
        || crate::mapi::identity::object_id_matches(&event.canonical_id, object_id)
}

fn mapi_public_folder_id(folder: &PublicFolder) -> u64 {
    crate::mapi::identity::mapped_mapi_object_id(&folder.id)
        .expect("MAPI public folder identity mapping missing")
}

pub(crate) fn mapi_recoverable_item_id(id: &Uuid) -> u64 {
    crate::mapi::identity::legacy_migration_object_id(id)
}

pub(crate) fn recoverable_mapi_folder_id(folder: &str) -> Option<u64> {
    match folder {
        "deletions" => Some(crate::mapi::identity::RECOVERABLE_ITEMS_DELETIONS_FOLDER_ID),
        "versions" => Some(crate::mapi::identity::RECOVERABLE_ITEMS_VERSIONS_FOLDER_ID),
        "purges" => Some(crate::mapi::identity::RECOVERABLE_ITEMS_PURGES_FOLDER_ID),
        _ => None,
    }
}

pub(crate) fn recoverable_storage_folder(folder_id: u64) -> Option<&'static str> {
    match folder_id {
        crate::mapi::identity::RECOVERABLE_ITEMS_DELETIONS_FOLDER_ID => Some("deletions"),
        crate::mapi::identity::RECOVERABLE_ITEMS_VERSIONS_FOLDER_ID => Some("versions"),
        crate::mapi::identity::RECOVERABLE_ITEMS_PURGES_FOLDER_ID => Some("purges"),
        _ => None,
    }
}

fn mapi_collaboration_folder_id(
    kind: MapiCollaborationFolderKind,
    collection: &CollaborationCollection,
) -> u64 {
    match (kind, collection.id.as_str()) {
        (MapiCollaborationFolderKind::Contacts, "default" | "contacts") => {
            crate::mapi::identity::CONTACTS_FOLDER_ID
        }
        (MapiCollaborationFolderKind::Contacts, "suggested_contacts") => {
            crate::mapi::identity::SUGGESTED_CONTACTS_FOLDER_ID
        }
        (MapiCollaborationFolderKind::Contacts, "quick_contacts") => {
            crate::mapi::identity::QUICK_CONTACTS_FOLDER_ID
        }
        (MapiCollaborationFolderKind::Contacts, "im_contact_list") => {
            crate::mapi::identity::IM_CONTACT_LIST_FOLDER_ID
        }
        (MapiCollaborationFolderKind::Calendar, "default" | "calendar") => {
            crate::mapi::identity::CALENDAR_FOLDER_ID
        }
        (MapiCollaborationFolderKind::Task, "default" | "tasks") => {
            crate::mapi::identity::TASKS_FOLDER_ID
        }
        _ => collaboration_folder_identity_canonical_id(kind, collection)
            .map(|id| {
                crate::mapi::identity::mapped_mapi_object_id(&id)
                    .unwrap_or_else(|| crate::mapi::identity::legacy_migration_object_id(&id))
            })
            .expect("MAPI collaboration folder identity mapping missing"),
    }
}

pub(crate) fn collaboration_folder_identity_canonical_id(
    kind: MapiCollaborationFolderKind,
    collection: &CollaborationCollection,
) -> Option<Uuid> {
    collaboration_collection_identity_key(kind, &collection.id)
}

fn collaboration_collection_identity_key(
    kind: MapiCollaborationFolderKind,
    collection_id: &str,
) -> Option<Uuid> {
    match (kind, collection_id) {
        (MapiCollaborationFolderKind::Contacts, "default" | "contacts")
        | (MapiCollaborationFolderKind::Contacts, "suggested_contacts")
        | (MapiCollaborationFolderKind::Contacts, "quick_contacts")
        | (MapiCollaborationFolderKind::Contacts, "im_contact_list")
        | (MapiCollaborationFolderKind::Calendar, "default" | "calendar")
        | (MapiCollaborationFolderKind::Task, "default" | "tasks") => None,
        _ => Some(deterministic_collaboration_folder_uuid(kind, collection_id)),
    }
}

fn deterministic_collaboration_folder_uuid(
    kind: MapiCollaborationFolderKind,
    collection_id: &str,
) -> Uuid {
    let mut hash = Sha256::new();
    hash.update(b"lpe:mapi:collaboration-folder:v1");
    hash.update([0]);
    hash.update(match kind {
        MapiCollaborationFolderKind::Contacts => b"contacts".as_slice(),
        MapiCollaborationFolderKind::Calendar => b"calendar".as_slice(),
        MapiCollaborationFolderKind::Task => b"task".as_slice(),
    });
    hash.update([0]);
    hash.update(collection_id.as_bytes());
    let digest = hash.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn task_collection_matches(task: &ClientTask, collection_id: &str) -> bool {
    matches!(collection_id, "tasks" | "default") || task.task_list_id.to_string() == collection_id
}

fn mailbox_contact_sync_default_supported(mailbox: &JmapMailbox) -> bool {
    if matches!(
        mailbox.role.as_str(),
        "contacts" | "suggested_contacts" | "quick_contacts" | "im_contact_list"
    ) {
        return true;
    }
    if mailbox.parent_id.is_some() {
        return false;
    }
    matches!(
        mailbox.name.trim().to_ascii_lowercase().as_str(),
        "contacts" | "suggested contacts" | "quick contacts" | "im contact list"
    )
}

pub(crate) fn reserved_folder_counter_for_role(role: &str) -> Option<u64> {
    match role {
        "__mapi_deferred_action" => Some(crate::mapi::identity::DEFERRED_ACTION_FOLDER_COUNTER),
        "__mapi_spooler_queue" => Some(crate::mapi::identity::SPOOLER_QUEUE_FOLDER_COUNTER),
        "__mapi_ipm_subtree" => Some(crate::mapi::identity::IPM_SUBTREE_FOLDER_COUNTER),
        "inbox" => Some(crate::mapi::identity::INBOX_FOLDER_COUNTER),
        "outbox" => Some(crate::mapi::identity::OUTBOX_FOLDER_COUNTER),
        "drafts" => Some(crate::mapi::identity::DRAFTS_FOLDER_COUNTER),
        "sent" => Some(crate::mapi::identity::SENT_FOLDER_COUNTER),
        "trash" => Some(crate::mapi::identity::TRASH_FOLDER_COUNTER),
        "__mapi_common_views" => Some(crate::mapi::identity::COMMON_VIEWS_FOLDER_COUNTER),
        "__mapi_schedule" => Some(crate::mapi::identity::SCHEDULE_FOLDER_COUNTER),
        "__mapi_search" => Some(crate::mapi::identity::SEARCH_FOLDER_COUNTER),
        "__mapi_views" => Some(crate::mapi::identity::VIEWS_FOLDER_COUNTER),
        "__mapi_shortcuts" => Some(crate::mapi::identity::SHORTCUTS_FOLDER_COUNTER),
        "contacts" => Some(crate::mapi::identity::CONTACTS_FOLDER_COUNTER),
        "suggested_contacts" => Some(crate::mapi::identity::SUGGESTED_CONTACTS_FOLDER_COUNTER),
        "quick_contacts" => Some(crate::mapi::identity::QUICK_CONTACTS_FOLDER_COUNTER),
        "im_contact_list" => Some(crate::mapi::identity::IM_CONTACT_LIST_FOLDER_COUNTER),
        "contacts_search" => Some(crate::mapi::identity::CONTACTS_SEARCH_FOLDER_COUNTER),
        "document_libraries" => Some(crate::mapi::identity::DOCUMENT_LIBRARIES_FOLDER_COUNTER),
        "calendar" => Some(crate::mapi::identity::CALENDAR_FOLDER_COUNTER),
        "journal" => Some(crate::mapi::identity::JOURNAL_FOLDER_COUNTER),
        "notes" => Some(crate::mapi::identity::NOTES_FOLDER_COUNTER),
        "tasks" => Some(crate::mapi::identity::TASKS_FOLDER_COUNTER),
        "reminders" => Some(crate::mapi::identity::REMINDERS_FOLDER_COUNTER),
        "sync_issues" => Some(crate::mapi::identity::SYNC_ISSUES_FOLDER_COUNTER),
        "conflicts" => Some(crate::mapi::identity::CONFLICTS_FOLDER_COUNTER),
        "local_failures" => Some(crate::mapi::identity::LOCAL_FAILURES_FOLDER_COUNTER),
        "server_failures" => Some(crate::mapi::identity::SERVER_FAILURES_FOLDER_COUNTER),
        "junk" => Some(crate::mapi::identity::JUNK_FOLDER_COUNTER),
        "rss_feeds" => Some(crate::mapi::identity::RSS_FEEDS_FOLDER_COUNTER),
        "tracked_mail_processing" => {
            Some(crate::mapi::identity::TRACKED_MAIL_PROCESSING_FOLDER_COUNTER)
        }
        "todo_search" => Some(crate::mapi::identity::TODO_SEARCH_FOLDER_COUNTER),
        "conversation_action_settings" => {
            Some(crate::mapi::identity::CONVERSATION_ACTION_SETTINGS_FOLDER_COUNTER)
        }
        "conversation_actions" => {
            Some(crate::mapi::identity::CONVERSATION_ACTION_SETTINGS_FOLDER_COUNTER)
        }
        "archive" => Some(crate::mapi::identity::ARCHIVE_FOLDER_COUNTER),
        "__mapi_freebusy_data" => Some(crate::mapi::identity::FREEBUSY_DATA_FOLDER_COUNTER),
        "freebusy_data" => Some(crate::mapi::identity::FREEBUSY_DATA_FOLDER_COUNTER),
        "conversation_history" => Some(crate::mapi::identity::CONVERSATION_HISTORY_FOLDER_COUNTER),
        _ => None,
    }
}

fn reserved_folder_id_for_role(role: &str) -> Option<u64> {
    reserved_folder_counter_for_role(role).map(crate::mapi::identity::mapi_store_id)
}

fn deduplicate_navigation_shortcuts(
    shortcuts: Vec<MapiNavigationShortcutMessage>,
) -> Vec<MapiNavigationShortcutMessage> {
    let mut seen_links = HashSet::new();
    let mut seen_headers = HashSet::new();
    shortcuts
        .into_iter()
        .filter(|shortcut| {
            if let Some(target_folder_id) = shortcut.target_folder_id {
                seen_links.insert((target_folder_id, shortcut.shortcut_type, shortcut.section))
            } else {
                seen_headers.insert((
                    shortcut.subject.clone(),
                    shortcut.shortcut_type,
                    shortcut.section,
                    shortcut.group_header_id,
                    shortcut.group_name.clone(),
                ))
            }
        })
        .collect()
}

fn append_missing_default_common_views_shortcuts(
    shortcuts: &mut Vec<MapiNavigationShortcutMessage>,
) {
    for default_shortcut in outlook_common_views_default_navigation_shortcuts()
        .into_iter()
        .filter(|shortcut| shortcut.section == 1)
    {
        let exists = shortcuts.iter().any(|shortcut| {
            if default_shortcut.shortcut_type == 4 {
                shortcut.shortcut_type == 4
                    && shortcut.section == default_shortcut.section
                    && shortcut.group_header_id == default_shortcut.group_header_id
                    && shortcut.group_name == default_shortcut.group_name
            } else {
                shortcut.shortcut_type == default_shortcut.shortcut_type
                    && shortcut.target_folder_id == default_shortcut.target_folder_id
            }
        });
        if !exists {
            shortcuts.push(default_shortcut);
        }
    }
}

fn replace_persisted_default_mail_favorite_shortcuts(
    shortcuts: &mut Vec<MapiNavigationShortcutMessage>,
) {
    let defaults = outlook_common_views_default_navigation_shortcuts();
    for default_shortcut in defaults.into_iter().filter(|shortcut| {
        shortcut.shortcut_type == 0
            && shortcut.section == 1
            && shortcut.group_name == OUTLOOK_MAIL_FAVORITES_GROUP_NAME
            && matches!(
                shortcut.target_folder_id,
                Some(crate::mapi::identity::INBOX_FOLDER_ID)
                    | Some(crate::mapi::identity::SENT_FOLDER_ID)
                    | Some(crate::mapi::identity::TRASH_FOLDER_ID)
            )
    }) {
        if let Some(existing) = shortcuts.iter_mut().find(|shortcut| {
            shortcut.shortcut_type == default_shortcut.shortcut_type
                && shortcut.target_folder_id == default_shortcut.target_folder_id
                && shortcut.section == default_shortcut.section
                && shortcut
                    .subject
                    .eq_ignore_ascii_case(&default_shortcut.subject)
        }) {
            *existing = default_shortcut;
        }
    }
}

fn normalize_navigation_shortcut_group_name(
    section: u32,
    group_header_id: Option<Uuid>,
    group_name: &str,
) -> String {
    if section == 1
        && group_header_id == Some(crate::mapi::properties::default_wlink_group_uuid())
        && (group_name.trim().is_empty()
            || group_name.eq_ignore_ascii_case("Mail")
            || group_name.eq_ignore_ascii_case(OUTLOOK_MAIL_FAVORITES_GROUP_NAME))
    {
        OUTLOOK_MAIL_FAVORITES_GROUP_NAME.to_string()
    } else {
        group_name.to_string()
    }
}

fn materialize_default_mail_group_header(shortcuts: &mut Vec<MapiNavigationShortcutMessage>) {
    let default_group_id = crate::mapi::properties::default_wlink_group_uuid();
    let has_default_mail_link = shortcuts.iter().any(|shortcut| {
        shortcut.shortcut_type != 4
            && shortcut.section == 1
            && shortcut.group_header_id == Some(default_group_id)
            && shortcut.group_name == OUTLOOK_MAIL_FAVORITES_GROUP_NAME
    });
    let has_default_mail_header = shortcuts.iter().any(|shortcut| {
        shortcut.shortcut_type == 4
            && shortcut.section == 1
            && shortcut.group_header_id == Some(default_group_id)
            && shortcut.group_name == OUTLOOK_MAIL_FAVORITES_GROUP_NAME
    });

    if has_default_mail_link && !has_default_mail_header {
        if let Some(header) = outlook_common_views_default_navigation_shortcuts()
            .into_iter()
            .find(|shortcut| {
                shortcut.shortcut_type == 4
                    && shortcut.group_name == OUTLOOK_MAIL_FAVORITES_GROUP_NAME
            })
        {
            shortcuts.push(header);
        }
    }
}

fn format_navigation_shortcut_debug_summary(shortcuts: &[MapiNavigationShortcutMessage]) -> String {
    shortcuts
        .iter()
        .take(8)
        .map(format_navigation_shortcut_debug_entry)
        .collect::<Vec<_>>()
        .join("|")
}

fn format_common_views_table_shortcut_debug_summary(messages: &[MapiCommonViewsMessage]) -> String {
    messages
        .iter()
        .filter_map(|message| match message {
            MapiCommonViewsMessage::NavigationShortcut(shortcut) => Some(shortcut),
            MapiCommonViewsMessage::NamedView(_)
            | MapiCommonViewsMessage::SearchFolderDefinition(_) => None,
        })
        .take(8)
        .map(format_navigation_shortcut_debug_entry)
        .collect::<Vec<_>>()
        .join("|")
}

fn common_views_search_folder_definition_is_projectable(
    definition: &SearchFolderDefinition,
) -> bool {
    definition
        .restriction_json
        .get("pidTagSearchFolderDefinition")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| BASE64_STANDARD.decode(value).ok())
        .is_some_and(|blob| search_folder_definition_blob_has_required_blocks(&blob))
}

fn search_folder_definition_blob_has_required_blocks(blob: &[u8]) -> bool {
    if blob.len() < 30 || blob.get(0..4) != Some(&0x0000_1004u32.to_le_bytes()) {
        return false;
    }
    let storage_type = u32::from_le_bytes(blob[4..8].try_into().unwrap());
    let mut offset = 12usize;
    let Some(text_len) = blob.get(offset).copied() else {
        return false;
    };
    offset += 1;
    let text_len = if text_len == u8::MAX {
        let Some(bytes) = blob.get(offset..offset + 2) else {
            return false;
        };
        offset += 2;
        u16::from_le_bytes(bytes.try_into().unwrap()) as usize
    } else {
        text_len as usize
    };
    offset = match offset.checked_add(text_len) {
        Some(offset) if offset <= blob.len() => offset,
        _ => return false,
    };
    if blob.get(offset..offset + 9).is_none() {
        return false;
    }
    offset += 8;
    let folder_list_1_len = blob[offset] as usize;
    offset += 1 + folder_list_1_len;
    let Some(bytes) = blob.get(offset..offset + 4) else {
        return false;
    };
    let folder_list_2_len = u32::from_le_bytes(bytes.try_into().unwrap()) as usize;
    offset += 4;
    if storage_type & 0x40 != 0 && folder_list_2_len == 0 {
        return false;
    }
    offset = match offset.checked_add(folder_list_2_len) {
        Some(offset) if offset <= blob.len() => offset,
        _ => return false,
    };
    if blob.get(offset..offset + 4).is_none() {
        return false;
    }
    offset += 4;
    if storage_type & 0x08 != 0 && blob.len().saturating_sub(offset) <= 4 {
        return false;
    }
    blob.get(blob.len().saturating_sub(4)..) == Some(&0u32.to_le_bytes())
}

fn format_navigation_shortcut_debug_entry(shortcut: &MapiNavigationShortcutMessage) -> String {
    format!(
        "id=0x{:016x};canonical_id={};subject={};target={};type={};flags=0x{:08x};section={};ordinal={};group_header={};group_name={}",
        shortcut.id,
        shortcut.canonical_id,
        shortcut.subject,
        shortcut
            .target_folder_id
            .map(|target| format!("0x{target:016x}"))
            .unwrap_or_else(|| "none".to_string()),
        shortcut.shortcut_type,
        shortcut.flags,
        shortcut.section,
        shortcut.ordinal,
        shortcut
            .group_header_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "none".to_string()),
        shortcut.group_name,
    )
}

#[cfg(test)]
mod tests;
