use lpe_mail_auth::StoreFuture;
use lpe_storage::{
    AccessibleContact, AccessibleEvent, ActiveSyncAttachment, CalendarEventAttachment, ClientNote,
    ClientReminder, ClientTask, CollaborationCollection, ConversationAction,
    DelegateFreeBusyMessageObject, JmapEmail, JmapMailbox, JournalEntry, MailboxRule, PublicFolder,
    PublicFolderItem, PublicFolderPermission, PublicFolderReplica, RecoverableItem, ReminderQuery,
    SearchFolderDefinition,
};
use sha2::{Digest, Sha256};
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
    pub(crate) section: u32,
    pub(crate) ordinal: u32,
    pub(crate) group_header_id: Option<Uuid>,
    pub(crate) group_name: String,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct MapiAssociatedConfigMessage {
    pub(crate) id: u64,
    pub(crate) folder_id: u64,
    pub(crate) canonical_id: Uuid,
    pub(crate) message_class: String,
    pub(crate) subject: String,
    pub(crate) properties_json: serde_json::Value,
}

pub(crate) enum MapiCommonViewsMessage {
    NavigationShortcut(MapiNavigationShortcutMessage),
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

impl MapiMailStoreSnapshot {
    pub(crate) fn empty() -> Self {
        Self::new(
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

    pub(crate) fn new(
        mailboxes: Vec<JmapMailbox>,
        emails: Vec<JmapEmail>,
        attachments: Vec<(Uuid, Vec<ActiveSyncAttachment>)>,
        contact_collections: Vec<CollaborationCollection>,
        calendar_collections: Vec<CollaborationCollection>,
        task_collections: Vec<CollaborationCollection>,
        contacts: Vec<AccessibleContact>,
        events: Vec<AccessibleEvent>,
        tasks: Vec<ClientTask>,
        folder_permissions: Vec<MapiFolderPermission>,
    ) -> Self {
        let folders = mailboxes
            .into_iter()
            .map(|mailbox| MapiFolder {
                id: mapi_folder_id(&mailbox),
                canonical_id: mailbox.id,
                mailbox,
            })
            .collect::<Vec<_>>();
        let messages = emails
            .into_iter()
            .map(|email| {
                let folder_id = mapi_message_folder_id(&email, &folders);
                let message_attachments = attachments
                    .iter()
                    .find(|(message_id, _)| *message_id == email.id)
                    .map(|(_, attachments)| attachments.as_slice())
                    .unwrap_or_default()
                    .iter()
                    .enumerate()
                    .map(|(index, attachment)| MapiAttachment {
                        attach_num: index as u32,
                        canonical_id: attachment.id,
                        file_reference: attachment.file_reference.clone(),
                        file_name: attachment.file_name.clone(),
                        media_type: attachment.media_type.clone(),
                        size_octets: attachment.size_octets,
                    })
                    .collect::<Vec<_>>();
                MapiMessage {
                    id: mapi_message_id(&email),
                    folder_id,
                    canonical_id: email.id,
                    email,
                    attachments: message_attachments,
                }
            })
            .collect();
        let mut collaboration_folders = Vec::new();
        collaboration_folders.extend(contact_collections.into_iter().map(|collection| {
            let id =
                mapi_collaboration_folder_id(MapiCollaborationFolderKind::Contacts, &collection);
            let item_count = contacts
                .iter()
                .filter(|contact| contact.collection_id == collection.id)
                .count()
                .min(u32::MAX as usize) as u32;
            MapiCollaborationFolder {
                id,
                kind: MapiCollaborationFolderKind::Contacts,
                collection,
                item_count,
            }
        }));
        collaboration_folders.extend(calendar_collections.into_iter().map(|collection| {
            let id =
                mapi_collaboration_folder_id(MapiCollaborationFolderKind::Calendar, &collection);
            let item_count = events
                .iter()
                .filter(|event| event.collection_id == collection.id)
                .count()
                .min(u32::MAX as usize) as u32;
            MapiCollaborationFolder {
                id,
                kind: MapiCollaborationFolderKind::Calendar,
                collection,
                item_count,
            }
        }));
        collaboration_folders.extend(task_collections.into_iter().map(|collection| {
            let id = mapi_collaboration_folder_id(MapiCollaborationFolderKind::Task, &collection);
            let item_count = tasks
                .iter()
                .filter(|task| task_collection_matches(task, &collection.id))
                .count()
                .min(u32::MAX as usize) as u32;
            MapiCollaborationFolder {
                id,
                kind: MapiCollaborationFolderKind::Task,
                collection,
                item_count,
            }
        }));
        let contacts = contacts
            .into_iter()
            .filter_map(|contact| {
                let folder_id = collaboration_folders
                    .iter()
                    .find(|folder| {
                        folder.kind == MapiCollaborationFolderKind::Contacts
                            && folder.collection.id == contact.collection_id
                    })
                    .map(|folder| folder.id)?;
                Some(MapiContact {
                    id: mapi_item_id(&contact.id),
                    folder_id,
                    canonical_id: contact.id,
                    contact,
                })
            })
            .collect();
        let events = events
            .into_iter()
            .filter_map(|event| {
                let folder_id = collaboration_folders
                    .iter()
                    .find(|folder| {
                        folder.kind == MapiCollaborationFolderKind::Calendar
                            && folder.collection.id == event.collection_id
                    })
                    .map(|folder| folder.id)
                    .or_else(|| {
                        matches!(event.collection_id.as_str(), "default" | "calendar")
                            .then_some(crate::mapi::identity::CALENDAR_FOLDER_ID)
                    })?;
                Some(MapiEvent {
                    id: mapi_item_id(&event.id),
                    folder_id,
                    canonical_id: event.id,
                    event,
                    attachments: Vec::new(),
                })
            })
            .collect();
        let tasks = tasks
            .into_iter()
            .filter_map(|task| {
                let folder_id = collaboration_folders
                    .iter()
                    .find(|folder| {
                        folder.kind == MapiCollaborationFolderKind::Task
                            && task_collection_matches(&task, &folder.collection.id)
                    })
                    .map(|folder| folder.id)?;
                Some(MapiTask {
                    id: mapi_item_id(&task.id),
                    folder_id,
                    canonical_id: task.id,
                    task,
                })
            })
            .collect();
        Self {
            folders,
            public_folders: Vec::new(),
            public_folder_items: Vec::new(),
            public_folder_replicas: Vec::new(),
            collaboration_folders,
            messages,
            contacts,
            events,
            tasks,
            notes: Vec::new(),
            journal_entries: Vec::new(),
            search_folder_definitions: Vec::new(),
            rules: Vec::new(),
            navigation_shortcuts: Vec::new(),
            associated_configs: Vec::new(),
            conversation_actions: Vec::new(),
            delegate_freebusy_messages: Vec::new(),
            recoverable_items: Vec::new(),
            reminders: Vec::new(),
            folder_permissions,
            public_folder_permissions: Vec::new(),
            content_windows: Vec::new(),
        }
    }

    pub(crate) fn with_search_folder_definitions(
        mut self,
        search_folder_definitions: Vec<SearchFolderDefinition>,
    ) -> Self {
        self.folders
            .retain(|folder| !folder.mailbox.role.starts_with("__mapi_search_folder_"));
        self.folders.extend(
            search_folder_definitions
                .iter()
                .filter_map(mapi_search_folder_definition_to_folder),
        );
        self.search_folder_definitions = search_folder_definitions;
        self
    }

    pub(crate) fn with_rules(mut self, rules: Vec<MailboxRule>) -> Self {
        self.rules = rules
            .into_iter()
            .map(|rule| {
                let id = mapi_item_id(&rule.id);
                MapiRule {
                    id,
                    canonical_id: rule.id,
                    name: rule.name,
                    is_active: rule.is_active,
                    condition_summary: rule.condition_summary,
                    action_summary: rule.action_summary,
                    updated_at: rule.updated_at,
                }
            })
            .collect();
        self
    }

    pub(crate) fn with_conversation_actions(
        mut self,
        conversation_actions: Vec<ConversationAction>,
    ) -> Self {
        self.conversation_actions = conversation_actions
            .into_iter()
            .map(|action| MapiConversationActionMessage {
                id: mapi_item_id(&action.id),
                folder_id: crate::mapi::identity::CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
                canonical_id: action.id,
                action,
            })
            .collect();
        self
    }

    pub(crate) fn with_delegate_freebusy_messages(
        mut self,
        messages: Vec<DelegateFreeBusyMessageObject>,
    ) -> Self {
        self.delegate_freebusy_messages = messages
            .into_iter()
            .map(|message| MapiDelegateFreeBusyMessage {
                id: mapi_item_id(&message.id),
                folder_id: crate::mapi::identity::FREEBUSY_DATA_FOLDER_ID,
                canonical_id: message.id,
                message,
            })
            .collect();
        self
    }

    pub(crate) fn with_recoverable_items(
        mut self,
        recoverable_items: Vec<RecoverableItem>,
    ) -> Self {
        self.recoverable_items = recoverable_items
            .into_iter()
            .filter_map(|item| {
                let folder_id = recoverable_mapi_folder_id(&item.recoverable_folder)?;
                Some(MapiRecoverableItemMessage {
                    id: mapi_recoverable_item_id(&item.id),
                    folder_id,
                    canonical_id: item.id,
                    item,
                })
            })
            .collect();
        self
    }

    pub(crate) fn with_navigation_shortcuts(
        mut self,
        navigation_shortcuts: Vec<MapiNavigationShortcutRecord>,
    ) -> Self {
        self.navigation_shortcuts = navigation_shortcuts
            .into_iter()
            .map(|shortcut| MapiNavigationShortcutMessage {
                id: mapi_item_id(&shortcut.id),
                folder_id: crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
                canonical_id: shortcut.id,
                subject: shortcut.subject,
                target_folder_id: shortcut.target_folder_id,
                shortcut_type: shortcut.shortcut_type,
                flags: shortcut.flags,
                section: shortcut.section,
                ordinal: shortcut.ordinal,
                group_header_id: shortcut.group_header_id,
                group_name: shortcut.group_name,
            })
            .collect();
        self
    }

    pub(crate) fn with_associated_configs(
        mut self,
        configs: Vec<MapiAssociatedConfigRecord>,
    ) -> Self {
        self.associated_configs = configs
            .into_iter()
            .map(|config| MapiAssociatedConfigMessage {
                id: mapi_item_id(&config.id),
                folder_id: config.folder_id,
                canonical_id: config.id,
                message_class: config.message_class,
                subject: config.subject,
                properties_json: config.properties_json,
            })
            .collect();
        self
    }

    pub(crate) fn with_reminders(mut self, reminders: Vec<ClientReminder>) -> Self {
        self.reminders = reminders;
        self
    }

    pub(crate) fn with_public_folders(
        mut self,
        folders: Vec<PublicFolder>,
        items: Vec<PublicFolderItem>,
        permissions: Vec<PublicFolderPermission>,
    ) -> Self {
        let all_folders = folders.clone();
        self.public_folders = folders
            .into_iter()
            .map(|folder| {
                let item_count = items
                    .iter()
                    .filter(|item| item.public_folder_id == folder.id)
                    .count()
                    .min(u32::MAX as usize) as u32;
                let child_count = all_folders
                    .iter()
                    .filter(|candidate| candidate.parent_folder_id == Some(folder.id))
                    .count()
                    .min(u32::MAX as usize) as u32;
                MapiPublicFolder {
                    id: mapi_public_folder_id(&folder),
                    folder,
                    item_count,
                    child_count,
                }
            })
            .collect();
        self.public_folder_items = items
            .into_iter()
            .filter_map(|item| {
                let folder_id =
                    crate::mapi::identity::mapped_mapi_object_id(&item.public_folder_id)?;
                Some(MapiPublicFolderItem {
                    id: mapi_item_id(&item.id),
                    folder_id,
                    item,
                })
            })
            .collect();
        self.public_folder_permissions = permissions
            .into_iter()
            .map(mapi_public_folder_permission)
            .collect();
        self
    }

    pub(crate) fn with_public_folder_replicas(
        mut self,
        replicas: Vec<PublicFolderReplica>,
    ) -> Self {
        let mut replicas = replicas;
        replicas.sort_by(|left, right| {
            left.sort_order
                .cmp(&right.sort_order)
                .then_with(|| left.server_name.cmp(&right.server_name))
        });
        self.public_folder_replicas = replicas
            .into_iter()
            .filter_map(|replica| {
                let folder_id =
                    crate::mapi::identity::mapped_mapi_object_id(&replica.public_folder_id)?;
                Some(MapiPublicFolderReplica { folder_id, replica })
            })
            .collect();
        self
    }

    pub(crate) fn with_notes_and_journal(
        mut self,
        notes: Vec<ClientNote>,
        journal_entries: Vec<JournalEntry>,
    ) -> Self {
        self.notes = notes
            .into_iter()
            .map(|note| MapiNote {
                id: mapi_item_id(&note.id),
                folder_id: crate::mapi::identity::NOTES_FOLDER_ID,
                canonical_id: note.id,
                note,
            })
            .collect();
        self.journal_entries = journal_entries
            .into_iter()
            .map(|entry| MapiJournalEntry {
                id: mapi_item_id(&entry.id),
                folder_id: crate::mapi::identity::JOURNAL_FOLDER_ID,
                canonical_id: entry.id,
                entry,
            })
            .collect();
        self
    }

    pub(crate) fn with_content_windows(
        mut self,
        content_windows: Vec<MapiContentTableWindow>,
    ) -> Self {
        self.content_windows = content_windows;
        self
    }

    pub(crate) fn with_calendar_attachments(
        mut self,
        calendar_attachments: Vec<(Uuid, Vec<CalendarEventAttachment>)>,
    ) -> Self {
        for event in &mut self.events {
            event.attachments = calendar_attachments
                .iter()
                .find(|(event_id, _)| *event_id == event.canonical_id)
                .map(|(_, attachments)| attachments.as_slice())
                .unwrap_or_default()
                .iter()
                .enumerate()
                .map(|(index, attachment)| MapiAttachment {
                    attach_num: index as u32,
                    canonical_id: attachment.id,
                    file_reference: attachment.file_reference.clone(),
                    file_name: attachment.file_name.clone(),
                    media_type: attachment.media_type.clone(),
                    size_octets: attachment.size_octets,
                })
                .collect();
        }
        self
    }

    pub(crate) fn mailboxes(&self) -> Vec<JmapMailbox> {
        self.folders
            .iter()
            .map(|folder| folder.mailbox.clone())
            .collect()
    }

    pub(crate) fn emails(&self) -> Vec<JmapEmail> {
        self.messages
            .iter()
            .map(|message| message.email.clone())
            .collect()
    }

    pub(crate) fn content_table_window_emails(
        &self,
        folder_id: u64,
        view_signature: u64,
        offset: usize,
        limit: usize,
    ) -> Option<(usize, Vec<&JmapEmail>)> {
        let window = self.content_windows.iter().find(|window| {
            window.folder_id == folder_id
                && window.view_signature == view_signature
                && window.offset == offset
                && (window.message_ids.len() >= limit
                    || offset + window.message_ids.len() >= window.total)
        })?;
        let emails = window
            .message_ids
            .iter()
            .filter_map(|id| {
                self.messages
                    .iter()
                    .find(|message| message.canonical_id == *id)
                    .map(|message| &message.email)
            })
            .collect::<Vec<_>>();
        Some((window.total, emails))
    }

    pub(crate) fn content_table_total(&self, folder_id: u64, view_signature: u64) -> Option<usize> {
        self.content_windows
            .iter()
            .find(|window| window.folder_id == folder_id && window.view_signature == view_signature)
            .map(|window| window.total)
    }

    pub(crate) fn attachments_for_message(
        &self,
        folder_id: u64,
        message_id: u64,
    ) -> Option<&[MapiAttachment]> {
        if let Some(message) = self
            .messages
            .iter()
            .find(|message| message.folder_id == folder_id && message.id == message_id)
        {
            return Some(message.attachments.as_slice());
        }
        self.events
            .iter()
            .find(|event| event.folder_id == folder_id && event.id == message_id)
            .map(|event| event.attachments.as_slice())
    }

    pub(crate) fn attachment_for_message(
        &self,
        folder_id: u64,
        message_id: u64,
        attach_num: u32,
    ) -> Option<&MapiAttachment> {
        self.attachments_for_message(folder_id, message_id)?
            .iter()
            .find(|attachment| attachment.attach_num == attach_num)
    }

    pub(crate) fn collaboration_folders(&self) -> &[MapiCollaborationFolder] {
        &self.collaboration_folders
    }

    pub(crate) fn public_folders(&self) -> &[MapiPublicFolder] {
        &self.public_folders
    }

    pub(crate) fn public_folder_for_id(&self, folder_id: u64) -> Option<&MapiPublicFolder> {
        self.public_folders
            .iter()
            .find(|folder| folder.id == folder_id)
    }

    pub(crate) fn public_folder_replica_server_names(&self, folder_id: u64) -> Vec<String> {
        self.public_folder_replicas
            .iter()
            .filter(|replica| {
                replica.folder_id == folder_id && replica.replica.lifecycle_state == "active"
            })
            .map(|replica| replica.replica.server_name.clone())
            .collect()
    }

    pub(crate) fn public_folder_items_for_folder(
        &self,
        folder_id: u64,
    ) -> Vec<&MapiPublicFolderItem> {
        self.public_folder_items
            .iter()
            .filter(|item| item.folder_id == folder_id)
            .collect()
    }

    pub(crate) fn public_folder_item_for_id(
        &self,
        folder_id: u64,
        item_id: u64,
    ) -> Option<&MapiPublicFolderItem> {
        self.public_folder_items
            .iter()
            .find(|item| item.folder_id == folder_id && item.id == item_id)
    }

    pub(crate) fn collaboration_folder_for_id(
        &self,
        folder_id: u64,
    ) -> Option<&MapiCollaborationFolder> {
        self.collaboration_folders
            .iter()
            .find(|folder| folder.id == folder_id)
    }

    pub(crate) fn contacts_for_folder(&self, folder_id: u64) -> Vec<&MapiContact> {
        self.contacts
            .iter()
            .filter(|contact| contact.folder_id == folder_id)
            .collect()
    }

    pub(crate) fn contacts_search_results(&self) -> Vec<&MapiContact> {
        if self
            .search_folder_definition_for_role("contacts_search")
            .is_none()
        {
            return Vec::new();
        }
        self.contacts.iter().collect()
    }

    pub(crate) fn contact_for_id(&self, folder_id: u64, item_id: u64) -> Option<&MapiContact> {
        if folder_id == crate::mapi::identity::CONTACTS_SEARCH_FOLDER_ID {
            return self
                .contacts_search_results()
                .into_iter()
                .find(|contact| contact.id == item_id);
        }
        self.contacts
            .iter()
            .find(|contact| contact.folder_id == folder_id && contact.id == item_id)
    }

    pub(crate) fn events_for_folder(&self, folder_id: u64) -> Vec<&MapiEvent> {
        self.events
            .iter()
            .filter(|event| event.folder_id == folder_id)
            .collect()
    }

    pub(crate) fn event_for_id(&self, folder_id: u64, item_id: u64) -> Option<&MapiEvent> {
        if folder_id == crate::mapi::identity::REMINDERS_FOLDER_ID {
            return self
                .reminder_events()
                .into_iter()
                .find(|event| mapi_event_id_matches(event, item_id));
        }
        self.events
            .iter()
            .find(|event| event.folder_id == folder_id && mapi_event_id_matches(event, item_id))
    }

    pub(crate) fn tasks_for_folder(&self, folder_id: u64) -> Vec<&MapiTask> {
        self.tasks
            .iter()
            .filter(|task| task.folder_id == folder_id)
            .collect()
    }

    pub(crate) fn todo_search_results(&self) -> Vec<&MapiTask> {
        if self
            .search_folder_definition_for_role("todo_search")
            .is_none()
        {
            return Vec::new();
        }
        self.tasks.iter().collect()
    }

    pub(crate) fn todo_search_messages(&self) -> Vec<&MapiMessage> {
        let Some(definition) = self.search_folder_definition_for_role("todo_search") else {
            return Vec::new();
        };
        self.messages
            .iter()
            .filter(|message| {
                !definition
                    .excluded_folder_roles
                    .iter()
                    .any(|role| role == &message.email.mailbox_role)
            })
            .filter(|message| {
                message.email.followup_icon > 0
                    || message.email.todo_item_flags != 0
                    || matches!(
                        message.email.followup_flag_status.as_str(),
                        "flagged" | "complete"
                    )
            })
            .collect()
    }

    pub(crate) fn todo_search_message_for_id(&self, message_id: u64) -> Option<&MapiMessage> {
        self.todo_search_messages()
            .into_iter()
            .find(|message| message.id == message_id)
    }

    pub(crate) fn tracked_mail_processing_messages(&self) -> Vec<&MapiMessage> {
        let Some(definition) = self.search_folder_definition_for_role("tracked_mail_processing")
        else {
            return Vec::new();
        };
        self.messages
            .iter()
            .filter(|message| {
                !definition
                    .excluded_folder_roles
                    .iter()
                    .any(|role| role == &message.email.mailbox_role)
            })
            .filter(|message| message.email.swapped_todo_store_id.is_some())
            .collect()
    }

    pub(crate) fn tracked_mail_processing_message_for_id(
        &self,
        message_id: u64,
    ) -> Option<&MapiMessage> {
        self.tracked_mail_processing_messages()
            .into_iter()
            .find(|message| message.id == message_id)
    }

    pub(crate) fn reminder_events(&self) -> Vec<&MapiEvent> {
        if self
            .search_folder_definition_for_role("reminders")
            .is_none()
        {
            return Vec::new();
        }
        self.reminders
            .iter()
            .filter(|reminder| reminder.source_type == "calendar")
            .filter_map(|reminder| {
                self.events
                    .iter()
                    .find(|event| event.canonical_id == reminder.source_id)
            })
            .collect()
    }

    pub(crate) fn reminder_tasks(&self) -> Vec<&MapiTask> {
        if self
            .search_folder_definition_for_role("reminders")
            .is_none()
        {
            return Vec::new();
        }
        self.reminders
            .iter()
            .filter(|reminder| reminder.source_type == "task")
            .filter_map(|reminder| {
                self.tasks
                    .iter()
                    .find(|task| task.canonical_id == reminder.source_id)
            })
            .collect()
    }

    pub(crate) fn reminder_messages(&self) -> Vec<&MapiMessage> {
        let Some(definition) = self.search_folder_definition_for_role("reminders") else {
            return Vec::new();
        };
        self.reminders
            .iter()
            .filter(|reminder| reminder.source_type == "mail")
            .filter_map(|reminder| {
                self.messages
                    .iter()
                    .find(|message| message.canonical_id == reminder.source_id)
            })
            .filter(|message| {
                !definition
                    .excluded_folder_roles
                    .iter()
                    .any(|role| role == &message.email.mailbox_role)
            })
            .collect()
    }

    pub(crate) fn reminder_message_for_id(&self, message_id: u64) -> Option<&MapiMessage> {
        self.reminder_messages()
            .into_iter()
            .find(|message| message.id == message_id)
    }

    pub(crate) fn reminder_for_source(
        &self,
        source_type: &str,
        source_id: Uuid,
    ) -> Option<&ClientReminder> {
        self.reminders
            .iter()
            .find(|reminder| reminder.source_type == source_type && reminder.source_id == source_id)
    }

    pub(crate) fn task_for_id(&self, folder_id: u64, item_id: u64) -> Option<&MapiTask> {
        if folder_id == crate::mapi::identity::REMINDERS_FOLDER_ID {
            return self
                .reminder_tasks()
                .into_iter()
                .find(|task| task.id == item_id);
        }
        if folder_id == crate::mapi::identity::TODO_SEARCH_FOLDER_ID {
            return self
                .todo_search_results()
                .into_iter()
                .find(|task| task.id == item_id);
        }
        self.tasks
            .iter()
            .find(|task| task.folder_id == folder_id && task.id == item_id)
    }

    pub(crate) fn notes_for_folder(&self, folder_id: u64) -> Vec<&MapiNote> {
        self.notes
            .iter()
            .filter(|note| note.folder_id == folder_id)
            .collect()
    }

    pub(crate) fn note_for_id(&self, folder_id: u64, item_id: u64) -> Option<&MapiNote> {
        self.notes
            .iter()
            .find(|note| note.folder_id == folder_id && note.id == item_id)
    }

    pub(crate) fn journal_entries_for_folder(&self, folder_id: u64) -> Vec<&MapiJournalEntry> {
        self.journal_entries
            .iter()
            .filter(|entry| entry.folder_id == folder_id)
            .collect()
    }

    pub(crate) fn journal_entry_for_id(
        &self,
        folder_id: u64,
        item_id: u64,
    ) -> Option<&MapiJournalEntry> {
        self.journal_entries
            .iter()
            .find(|entry| entry.folder_id == folder_id && entry.id == item_id)
    }

    pub(crate) fn search_folder_definition_for_role(
        &self,
        role: &str,
    ) -> Option<&SearchFolderDefinition> {
        self.search_folder_definitions
            .iter()
            .find(|definition| definition.role == role)
    }

    pub(crate) fn search_folder_definition_for_folder_id(
        &self,
        folder_id: u64,
    ) -> Option<&SearchFolderDefinition> {
        self.search_folder_definitions.iter().find(|definition| {
            crate::mapi::identity::mapped_mapi_object_id(&definition.id) == Some(folder_id)
        })
    }

    pub(crate) fn rules(&self) -> &[MapiRule] {
        &self.rules
    }

    pub(crate) fn navigation_shortcut_messages(&self) -> Vec<MapiNavigationShortcutMessage> {
        self.navigation_shortcuts.clone()
    }

    pub(crate) fn common_views_messages(&self) -> impl Iterator<Item = MapiCommonViewsMessage> {
        let messages = self
            .navigation_shortcut_messages()
            .into_iter()
            .map(MapiCommonViewsMessage::NavigationShortcut)
            .collect::<Vec<_>>();
        messages.into_iter()
    }

    pub(crate) fn navigation_shortcut_message_for_id(
        &self,
        item_id: u64,
    ) -> Option<MapiNavigationShortcutMessage> {
        self.navigation_shortcut_messages()
            .into_iter()
            .find(|message| message.id == item_id)
    }

    pub(crate) fn associated_config_messages_for_folder(
        &self,
        folder_id: u64,
    ) -> Vec<MapiAssociatedConfigMessage> {
        self.associated_configs
            .iter()
            .filter(|message| message.folder_id == folder_id)
            .cloned()
            .collect()
    }

    pub(crate) fn associated_config_message_for_id(
        &self,
        item_id: u64,
    ) -> Option<MapiAssociatedConfigMessage> {
        self.associated_configs
            .iter()
            .find(|message| message.id == item_id)
            .cloned()
    }

    pub(crate) fn associated_config_message_for_folder_and_source_key_id(
        &self,
        folder_id: u64,
        item_id: u64,
    ) -> Option<MapiAssociatedConfigMessage> {
        crate::mapi::identity::global_counter_from_store_id(item_id)?;
        let source_key = crate::mapi::identity::source_key_for_object_id(item_id);
        self.associated_configs
            .iter()
            .find(|message| {
                message.folder_id == folder_id
                    && associated_config_source_key(&message.properties_json)
                        .is_some_and(|message_source_key| message_source_key == source_key)
            })
            .cloned()
    }

    pub(crate) fn conversation_action_messages(&self) -> &[MapiConversationActionMessage] {
        &self.conversation_actions
    }

    pub(crate) fn conversation_action_message_for_id(
        &self,
        item_id: u64,
    ) -> Option<&MapiConversationActionMessage> {
        self.conversation_actions
            .iter()
            .find(|message| message.id == item_id)
    }

    pub(crate) fn delegate_freebusy_messages(&self) -> &[MapiDelegateFreeBusyMessage] {
        &self.delegate_freebusy_messages
    }

    pub(crate) fn delegate_freebusy_message_for_id(
        &self,
        item_id: u64,
    ) -> Option<&MapiDelegateFreeBusyMessage> {
        self.delegate_freebusy_messages
            .iter()
            .find(|message| message.id == item_id)
    }

    pub(crate) fn recoverable_items_for_folder(
        &self,
        folder_id: u64,
    ) -> Vec<&MapiRecoverableItemMessage> {
        self.recoverable_items
            .iter()
            .filter(|item| item.folder_id == folder_id)
            .collect()
    }

    pub(crate) fn recoverable_item_for_id(
        &self,
        folder_id: u64,
        item_id: u64,
    ) -> Option<&MapiRecoverableItemMessage> {
        self.recoverable_items
            .iter()
            .find(|item| item.folder_id == folder_id && item.id == item_id)
    }

    pub(crate) fn permissions_for_folder(&self, folder_id: u64) -> Vec<MapiFolderPermission> {
        if let Some(folder) = self.folders.iter().find(|folder| folder.id == folder_id) {
            let mut permissions = reserved_permission_rows(folder.canonical_id);
            permissions.extend(
                self.folder_permissions
                    .iter()
                    .filter(|permission| permission.mailbox_id == folder.canonical_id)
                    .cloned(),
            );
            return permissions;
        }
        if let Some(folder) = self
            .public_folders
            .iter()
            .find(|folder| folder.id == folder_id)
        {
            let mut permissions = reserved_permission_rows(folder.folder.id);
            permissions.extend(
                self.public_folder_permissions
                    .iter()
                    .filter(|permission| permission.mailbox_id == folder.folder.id)
                    .cloned(),
            );
            return permissions;
        }
        Vec::new()
    }

    pub(crate) fn folder_access_for_principal(
        &self,
        folder_id: u64,
        principal_account_id: Uuid,
    ) -> Option<MapiFolderAccess> {
        if let Some(folder) = self.folders.iter().find(|folder| folder.id == folder_id) {
            let permission = self.folder_permissions.iter().find(|permission| {
                permission.mailbox_id == folder.canonical_id
                    && permission.member_account_id == Some(principal_account_id)
            })?;
            return Some(access_from_rights(permission.rights));
        }
        let folder = self
            .collaboration_folders
            .iter()
            .find(|folder| folder.id == folder_id)?;
        if folder.collection.owner_account_id == principal_account_id || folder.collection.is_owned
        {
            return Some(access_from_rights(rights_from_grant(
                true, true, true, true,
            )));
        }
        Some(access_from_rights(rights_from_grant(
            folder.collection.rights.may_read,
            folder.collection.rights.may_write,
            folder.collection.rights.may_delete,
            folder.collection.rights.may_share,
        )))
    }

    #[cfg(test)]
    pub(crate) fn folders(&self) -> &[MapiFolder] {
        &self.folders
    }

    #[cfg(test)]
    pub(crate) fn messages(&self) -> &[MapiMessage] {
        &self.messages
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
            is_subscribed: true,
        },
    })
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
            let associated_configs = self.fetch_mapi_associated_configs(account_id).await?;
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
    requests.extend(mailboxes.iter().map(|mailbox| MapiIdentityRequest {
        object_kind: MapiIdentityObjectKind::Mailbox,
        canonical_id: mailbox.id,
        reserved_global_counter: reserved_folder_counter_for_role(&mailbox.role),
        source_key: None,
    }));
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
    if let Some(request) = default_calendar_folder_identity_request(calendar_collections) {
        requests.push(request);
    }
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
    contact_collections
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
        .collect()
}

pub(crate) fn default_calendar_folder_identity_request(
    calendar_collections: &[CollaborationCollection],
) -> Option<MapiIdentityRequest> {
    calendar_collections
        .iter()
        .any(|collection| matches!(collection.id.as_str(), "default" | "calendar"))
        .then(|| {
            crate::mapi_mailstore::virtual_special_mailbox(
                crate::mapi::identity::CALENDAR_FOLDER_ID,
            )
        })
        .flatten()
        .map(|mailbox| MapiIdentityRequest {
            object_kind: MapiIdentityObjectKind::Mailbox,
            canonical_id: mailbox.id,
            reserved_global_counter: Some(crate::mapi::identity::CALENDAR_FOLDER_COUNTER),
            source_key: None,
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
            .and_then(|id| crate::mapi::identity::mapped_mapi_object_id(&id))
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

#[cfg(test)]
mod tests {
    use super::*;
    use lpe_storage::{
        AccessibleContact, CollaborationCollection, CollaborationRights, JmapEmailAddress,
        JmapEmailMailboxState,
    };

    fn exchange_builtin_excluded_folder_roles() -> Vec<String> {
        [
            "trash",
            "junk",
            "drafts",
            "outbox",
            "conflicts",
            "local_failures",
            "server_failures",
            "sync_issues",
        ]
        .into_iter()
        .map(str::to_string)
        .collect()
    }

    #[test]
    fn advertised_special_mailbox_roles_have_reserved_mapi_counters() {
        let cases = [
            (
                "sync_issues",
                crate::mapi::identity::SYNC_ISSUES_FOLDER_COUNTER,
            ),
            ("conflicts", crate::mapi::identity::CONFLICTS_FOLDER_COUNTER),
            (
                "local_failures",
                crate::mapi::identity::LOCAL_FAILURES_FOLDER_COUNTER,
            ),
            (
                "server_failures",
                crate::mapi::identity::SERVER_FAILURES_FOLDER_COUNTER,
            ),
            ("junk", crate::mapi::identity::JUNK_FOLDER_COUNTER),
            ("rss_feeds", crate::mapi::identity::RSS_FEEDS_FOLDER_COUNTER),
            ("archive", crate::mapi::identity::ARCHIVE_FOLDER_COUNTER),
            (
                "conversation_history",
                crate::mapi::identity::CONVERSATION_HISTORY_FOLDER_COUNTER,
            ),
        ];

        for (role, counter) in cases {
            assert_eq!(reserved_folder_counter_for_role(role), Some(counter));
        }
    }

    #[test]
    fn snapshot_projects_canonical_mailbox_message_and_attachment_ids() {
        let mailbox_id = Uuid::parse_str("44444444-4444-4444-4444-444444444444").unwrap();
        let message_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap();
        let attachment_id = Uuid::parse_str("abababab-abab-abab-abab-abababababab").unwrap();
        crate::mapi::identity::remember_mapi_identity(
            mailbox_id,
            crate::mapi::identity::mapi_store_id(17),
        );
        crate::mapi::identity::remember_mapi_identity(
            message_id,
            crate::mapi::identity::mapi_store_id(18),
        );
        let mailbox = JmapMailbox {
            id: mailbox_id,
            parent_id: None,
            role: "custom".to_string(),
            name: "RCA Sync".to_string(),
            sort_order: 10,
            modseq: 40,
            total_emails: 1,
            unread_emails: 0,
            is_subscribed: true,
        };
        let email = JmapEmail {
            id: message_id,
            thread_id: Uuid::parse_str("12121212-1212-1212-1212-121212121212").unwrap(),
            mailbox_id,
            mailbox_role: "custom".to_string(),
            mailbox_name: "RCA Sync".to_string(),
            modseq: 41,
            mailbox_ids: vec![mailbox_id],
            mailbox_states: vec![JmapEmailMailboxState {
                mailbox_id,
                role: "custom".to_string(),
                name: "RCA Sync".to_string(),
                modseq: 41,
                unread: false,
                flagged: false,
                followup_flag_status: "none".to_string(),
                followup_icon: 0,
                todo_item_flags: 0,
                followup_request: String::new(),
                followup_start_at: None,
                followup_due_at: None,
                followup_completed_at: None,
                reminder_set: false,
                reminder_at: None,
                reminder_dismissed_at: None,
                swapped_todo_store_id: None,
                swapped_todo_data: None,
                categories: Vec::new(),
                draft: false,
            }],
            received_at: "2026-05-03T12:00:00Z".to_string(),
            sent_at: None,
            from_address: "alice@example.test".to_string(),
            from_display: Some("Alice".to_string()),
            sender_address: None,
            sender_display: None,
            sender_authorization_kind: "self".to_string(),
            submitted_by_account_id: Uuid::nil(),
            to: vec![JmapEmailAddress {
                address: "bob@example.test".to_string(),
                display_name: Some("Bob".to_string()),
            }],
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: "Hello".to_string(),
            preview: "Hello".to_string(),
            body_text: "Hello".to_string(),
            body_html_sanitized: None,
            unread: false,
            flagged: false,
            followup_flag_status: "none".to_string(),
            followup_icon: 0,
            todo_item_flags: 0,
            followup_request: String::new(),
            followup_start_at: None,
            followup_due_at: None,
            followup_completed_at: None,
            reminder_set: false,
            reminder_at: None,
            reminder_dismissed_at: None,
            swapped_todo_store_id: None,
            swapped_todo_data: None,
            categories: Vec::new(),
            has_attachments: true,
            size_octets: 42,
            internet_message_id: None,
            mime_blob_ref: None,
            delivery_status: "stored".to_string(),
        };
        let attachment = ActiveSyncAttachment {
            id: attachment_id,
            message_id,
            file_name: "brief.pdf".to_string(),
            media_type: "application/pdf".to_string(),
            size_octets: 5,
            file_reference: "attachment-ref".to_string(),
        };

        let snapshot = MapiMailStoreSnapshot::new(
            vec![mailbox],
            vec![email],
            vec![(message_id, vec![attachment])],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );

        assert_eq!(snapshot.folders().len(), 1);
        assert_eq!(snapshot.messages().len(), 1);
        assert_eq!(snapshot.messages()[0].canonical_id, message_id);
        assert_eq!(snapshot.messages()[0].folder_id, snapshot.folders()[0].id);
        assert_eq!(
            snapshot.messages()[0].attachments[0].canonical_id,
            attachment_id
        );
        assert_eq!(snapshot.messages()[0].attachments[0].attach_num, 0);
    }

    #[test]
    fn snapshot_projects_outlook_contact_books_into_fixed_mapi_folders() {
        let account_id = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap();
        let rights = CollaborationRights {
            may_read: true,
            may_write: true,
            may_delete: true,
            may_share: false,
        };
        let cases = [
            (
                "suggested_contacts",
                "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                "Suggested Contacts",
                crate::mapi::identity::SUGGESTED_CONTACTS_FOLDER_ID,
            ),
            (
                "quick_contacts",
                "cccccccc-cccc-cccc-cccc-cccccccccccc",
                "Quick Contacts",
                crate::mapi::identity::QUICK_CONTACTS_FOLDER_ID,
            ),
            (
                "im_contact_list",
                "dddddddd-dddd-dddd-dddd-dddddddddddd",
                "IM Contact List",
                crate::mapi::identity::IM_CONTACT_LIST_FOLDER_ID,
            ),
        ];
        let collections = cases
            .iter()
            .map(
                |(collection_id, _, display_name, _)| CollaborationCollection {
                    id: (*collection_id).to_string(),
                    kind: "contacts".to_string(),
                    owner_account_id: account_id,
                    owner_email: "alice@example.test".to_string(),
                    owner_display_name: "Alice".to_string(),
                    display_name: (*display_name).to_string(),
                    is_owned: true,
                    rights: rights.clone(),
                },
            )
            .collect::<Vec<_>>();
        let contacts = cases
            .iter()
            .enumerate()
            .map(|(index, (collection_id, contact_id, _, _))| {
                let contact_id = Uuid::parse_str(contact_id).unwrap();
                crate::mapi::identity::remember_mapi_identity(
                    contact_id,
                    crate::mapi::identity::mapi_store_id(92 + index as u64),
                );
                AccessibleContact {
                    id: contact_id,
                    collection_id: (*collection_id).to_string(),
                    owner_account_id: account_id,
                    owner_email: "alice@example.test".to_string(),
                    owner_display_name: "Alice".to_string(),
                    rights: rights.clone(),
                    name: "Outlook Contact".to_string(),
                    role: String::new(),
                    email: "contact@example.test".to_string(),
                    phone: String::new(),
                    team: String::new(),
                    notes: String::new(),
                }
            })
            .collect::<Vec<_>>();

        let snapshot = MapiMailStoreSnapshot::new(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            collections,
            Vec::new(),
            Vec::new(),
            contacts,
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );

        for (_, contact_id, _, folder_id) in cases {
            assert!(snapshot
                .collaboration_folders()
                .iter()
                .any(|folder| folder.id == folder_id));
            assert_eq!(
                snapshot.contacts_for_folder(folder_id)[0].canonical_id,
                Uuid::parse_str(contact_id).unwrap()
            );
        }

        let definition_id = Uuid::parse_str("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee").unwrap();
        crate::mapi::identity::remember_mapi_identity(
            definition_id,
            crate::mapi::identity::mapi_store_id(95),
        );
        let snapshot = snapshot.with_search_folder_definitions(vec![SearchFolderDefinition {
            id: definition_id,
            account_id,
            role: "contacts_search".to_string(),
            display_name: "Contacts Search".to_string(),
            definition_kind: "exchange_builtin".to_string(),
            result_object_kind: "contact".to_string(),
            scope_json: serde_json::json!({"scope": "contacts_folders"}),
            restriction_json: serde_json::json!({"kind": "exchange_contacts_search"}),
            excluded_folder_roles: Vec::new(),
            is_builtin: true,
        }]);
        assert_eq!(snapshot.contacts_search_results().len(), 3);
        assert!(snapshot
            .contact_for_id(
                crate::mapi::identity::CONTACTS_SEARCH_FOLDER_ID,
                crate::mapi::identity::mapi_store_id(92)
            )
            .is_some());
    }

    #[test]
    fn collaboration_folder_identity_requests_cover_custom_and_shared_collections() {
        let owner_id = Uuid::parse_str("99999999-9999-4999-8999-999999999999").unwrap();
        let custom_calendar_id = Uuid::parse_str("aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa").unwrap();
        let rights = CollaborationRights {
            may_read: true,
            may_write: true,
            may_delete: true,
            may_share: true,
        };
        let contact_collections = vec![CollaborationCollection {
            id: format!("shared-contacts-{owner_id}"),
            kind: "contacts".to_string(),
            owner_account_id: owner_id,
            owner_email: "owner@example.test".to_string(),
            owner_display_name: "Owner".to_string(),
            display_name: "Owner Contacts".to_string(),
            is_owned: false,
            rights: rights.clone(),
        }];
        let calendar_collections = vec![
            CollaborationCollection {
                id: custom_calendar_id.to_string(),
                kind: "calendar".to_string(),
                owner_account_id: owner_id,
                owner_email: "owner@example.test".to_string(),
                owner_display_name: "Owner".to_string(),
                display_name: "Custom".to_string(),
                is_owned: true,
                rights: rights.clone(),
            },
            CollaborationCollection {
                id: format!("shared-calendar-{owner_id}"),
                kind: "calendar".to_string(),
                owner_account_id: owner_id,
                owner_email: "owner@example.test".to_string(),
                owner_display_name: "Owner".to_string(),
                display_name: "Owner Calendar".to_string(),
                is_owned: false,
                rights: rights.clone(),
            },
        ];
        let task_collections = vec![CollaborationCollection {
            id: format!("shared-tasks-{owner_id}"),
            kind: "tasks".to_string(),
            owner_account_id: owner_id,
            owner_email: "owner@example.test".to_string(),
            owner_display_name: "Owner".to_string(),
            display_name: "Owner Tasks".to_string(),
            is_owned: false,
            rights,
        }];

        let requests = collaboration_folder_identity_requests(
            &contact_collections,
            &calendar_collections,
            &task_collections,
        );
        let canonical_ids = requests
            .iter()
            .map(|request| request.canonical_id)
            .collect::<Vec<_>>();

        assert_eq!(requests.len(), 4);
        assert!(requests
            .iter()
            .all(|request| request.object_kind == MapiIdentityObjectKind::Mailbox));
        assert_eq!(
            canonical_ids
                .iter()
                .copied()
                .collect::<std::collections::HashSet<_>>()
                .len(),
            4
        );
        assert!(!canonical_ids.contains(&owner_id));
        assert!(!canonical_ids.contains(&custom_calendar_id));
    }

    #[test]
    fn snapshot_uses_allocated_identities_for_custom_and_shared_collaboration_folders() {
        let owner_id = Uuid::parse_str("99999999-9999-4999-8999-999999999999").unwrap();
        let rights = CollaborationRights {
            may_read: true,
            may_write: true,
            may_delete: true,
            may_share: true,
        };
        let contact_collection = CollaborationCollection {
            id: format!("shared-contacts-{owner_id}"),
            kind: "contacts".to_string(),
            owner_account_id: owner_id,
            owner_email: "owner@example.test".to_string(),
            owner_display_name: "Owner".to_string(),
            display_name: "Owner Contacts".to_string(),
            is_owned: false,
            rights: rights.clone(),
        };
        let calendar_collection = CollaborationCollection {
            id: format!("shared-calendar-{owner_id}"),
            kind: "calendar".to_string(),
            owner_account_id: owner_id,
            owner_email: "owner@example.test".to_string(),
            owner_display_name: "Owner".to_string(),
            display_name: "Owner Calendar".to_string(),
            is_owned: false,
            rights: rights.clone(),
        };
        let task_collection = CollaborationCollection {
            id: format!("shared-tasks-{owner_id}"),
            kind: "tasks".to_string(),
            owner_account_id: owner_id,
            owner_email: "owner@example.test".to_string(),
            owner_display_name: "Owner".to_string(),
            display_name: "Owner Tasks".to_string(),
            is_owned: false,
            rights,
        };
        let cases = [
            (
                MapiCollaborationFolderKind::Contacts,
                &contact_collection,
                crate::mapi::identity::mapi_store_id(201),
            ),
            (
                MapiCollaborationFolderKind::Calendar,
                &calendar_collection,
                crate::mapi::identity::mapi_store_id(202),
            ),
            (
                MapiCollaborationFolderKind::Task,
                &task_collection,
                crate::mapi::identity::mapi_store_id(203),
            ),
        ];
        for (kind, collection, object_id) in cases {
            let canonical_id =
                collaboration_folder_identity_canonical_id(kind, collection).unwrap();
            crate::mapi::identity::remember_mapi_identity(canonical_id, object_id);
        }

        let snapshot = MapiMailStoreSnapshot::new(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec![contact_collection],
            vec![calendar_collection],
            vec![task_collection],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );
        let folder_ids = snapshot
            .collaboration_folders()
            .iter()
            .map(|folder| folder.id)
            .collect::<Vec<_>>();

        assert!(folder_ids.contains(&crate::mapi::identity::mapi_store_id(201)));
        assert!(folder_ids.contains(&crate::mapi::identity::mapi_store_id(202)));
        assert!(folder_ids.contains(&crate::mapi::identity::mapi_store_id(203)));
        assert_eq!(
            folder_ids
                .iter()
                .copied()
                .collect::<std::collections::HashSet<_>>()
                .len(),
            3
        );
    }

    #[test]
    fn snapshot_projects_canonical_notes_and_journal_into_default_mapi_folders() {
        let note_id = Uuid::parse_str("51515151-5151-5151-5151-515151515151").unwrap();
        let journal_id = Uuid::parse_str("61616161-6161-6161-6161-616161616161").unwrap();
        crate::mapi::identity::remember_mapi_identity(
            note_id,
            crate::mapi::identity::mapi_store_id(90),
        );
        crate::mapi::identity::remember_mapi_identity(
            journal_id,
            crate::mapi::identity::mapi_store_id(91),
        );

        let snapshot = MapiMailStoreSnapshot::new(
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
        .with_notes_and_journal(
            vec![ClientNote {
                id: note_id,
                title: "Sticky note".to_string(),
                body_text: "Remember Outlook content tables".to_string(),
                color: "yellow".to_string(),
                categories_json: "[]".to_string(),
                created_at: "2026-05-19T12:00:00Z".to_string(),
                updated_at: "2026-05-19T12:30:00Z".to_string(),
            }],
            vec![JournalEntry {
                id: journal_id,
                subject: "Support call".to_string(),
                body_text: "Call notes".to_string(),
                entry_type: "phone-call".to_string(),
                message_class: "IPM.Activity".to_string(),
                starts_at: Some("2026-05-19T13:00:00Z".to_string()),
                ends_at: Some("2026-05-19T13:15:00Z".to_string()),
                occurred_at: None,
                companies_json: "[]".to_string(),
                contacts_json: "[]".to_string(),
                created_at: "2026-05-19T12:55:00Z".to_string(),
                updated_at: "2026-05-19T13:15:00Z".to_string(),
            }],
        );

        let notes = snapshot.notes_for_folder(crate::mapi::identity::NOTES_FOLDER_ID);
        let journal = snapshot.journal_entries_for_folder(crate::mapi::identity::JOURNAL_FOLDER_ID);
        assert_eq!(notes.len(), 1);
        assert_eq!(notes[0].id, crate::mapi::identity::mapi_store_id(90));
        assert_eq!(notes[0].folder_id, crate::mapi::identity::NOTES_FOLDER_ID);
        assert_eq!(journal.len(), 1);
        assert_eq!(journal[0].id, crate::mapi::identity::mapi_store_id(91));
        assert_eq!(
            journal[0].folder_id,
            crate::mapi::identity::JOURNAL_FOLDER_ID
        );
    }

    #[test]
    fn snapshot_carries_persisted_search_folder_definitions() {
        let definition_id = Uuid::parse_str("aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa").unwrap();
        crate::mapi::identity::remember_mapi_identity(
            definition_id,
            crate::mapi::identity::mapi_store_id(96),
        );
        let definition = SearchFolderDefinition {
            id: definition_id,
            account_id: Uuid::parse_str("bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb").unwrap(),
            role: "reminders".to_string(),
            display_name: "Reminders".to_string(),
            definition_kind: "exchange_builtin".to_string(),
            result_object_kind: "mixed".to_string(),
            scope_json: serde_json::json!({"scope": "top_of_personal_folders"}),
            restriction_json: serde_json::json!({"kind": "exchange_reminders"}),
            excluded_folder_roles: exchange_builtin_excluded_folder_roles(),
            is_builtin: true,
        };
        let snapshot = MapiMailStoreSnapshot::new(
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
        .with_search_folder_definitions(vec![definition]);

        let reminders = snapshot
            .search_folder_definition_for_role("reminders")
            .expect("persisted reminders definition");
        assert_eq!(reminders.definition_kind, "exchange_builtin");
        assert_eq!(reminders.result_object_kind, "mixed");
        assert_eq!(
            reminders.excluded_folder_roles,
            exchange_builtin_excluded_folder_roles()
        );
        assert!(snapshot
            .search_folder_definition_for_role("todo_search")
            .is_none());
    }

    #[test]
    fn snapshot_projects_user_saved_search_folder_as_mapi_folder() {
        let definition_id = Uuid::parse_str("aaaaaaaa-2222-4111-8111-aaaaaaaaaaaa").unwrap();
        let folder_id = crate::mapi::identity::mapi_store_id(122);
        crate::mapi::identity::remember_mapi_identity(definition_id, folder_id);
        let snapshot = MapiMailStoreSnapshot::new(
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
        .with_search_folder_definitions(vec![SearchFolderDefinition {
            id: definition_id,
            account_id: Uuid::parse_str("bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb").unwrap(),
            role: "custom".to_string(),
            display_name: "Unread from Alice".to_string(),
            definition_kind: "user_saved".to_string(),
            result_object_kind: "message".to_string(),
            scope_json: serde_json::json!({"scope": "top_of_personal_folders"}),
            restriction_json: serde_json::json!({"kind": "text", "query": "alice"}),
            excluded_folder_roles: vec!["trash".to_string()],
            is_builtin: false,
        }]);

        let folder = snapshot
            .folders()
            .iter()
            .find(|folder| folder.canonical_id == definition_id)
            .expect("user search folder projected");
        assert_eq!(folder.id, folder_id);
        assert_eq!(folder.mailbox.name, "Unread from Alice");
        assert_eq!(folder.mailbox.role, "__mapi_search_folder_message");
    }

    #[test]
    fn snapshot_projects_canonical_tasks_into_todo_search_results() {
        let account_id = Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").unwrap();
        let task_id = Uuid::parse_str("11111111-2222-4333-8444-555555555555").unwrap();
        let definition_id = Uuid::parse_str("99999999-9999-4999-8999-999999999999").unwrap();
        crate::mapi::identity::remember_mapi_identity(
            task_id,
            crate::mapi::identity::mapi_store_id(97),
        );
        crate::mapi::identity::remember_mapi_identity(
            definition_id,
            crate::mapi::identity::mapi_store_id(98),
        );
        let rights = CollaborationRights {
            may_read: true,
            may_write: true,
            may_delete: true,
            may_share: false,
        };
        let task_list_id = Uuid::parse_str("12121212-3434-4565-8787-909090909090").unwrap();
        let task = ClientTask {
            id: task_id,
            owner_account_id: account_id,
            owner_email: "alice@example.test".to_string(),
            owner_display_name: "Alice".to_string(),
            is_owned: true,
            rights: rights.clone(),
            task_list_id,
            task_list_sort_order: 0,
            title: "Follow up".to_string(),
            description: String::new(),
            status: "needs-action".to_string(),
            due_at: Some("2026-05-21T09:00:00Z".to_string()),
            completed_at: None,
            recurrence_rule: String::new(),
            sort_order: 0,
            updated_at: "2026-05-20T09:00:00Z".to_string(),
        };
        let snapshot = MapiMailStoreSnapshot::new(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec![CollaborationCollection {
                id: "default".to_string(),
                kind: "tasks".to_string(),
                owner_account_id: account_id,
                owner_email: "alice@example.test".to_string(),
                owner_display_name: "Alice".to_string(),
                display_name: "Tasks".to_string(),
                is_owned: true,
                rights,
            }],
            Vec::new(),
            Vec::new(),
            vec![task],
            Vec::new(),
        )
        .with_search_folder_definitions(vec![SearchFolderDefinition {
            id: definition_id,
            account_id,
            role: "todo_search".to_string(),
            display_name: "To-Do".to_string(),
            definition_kind: "exchange_builtin".to_string(),
            result_object_kind: "mixed".to_string(),
            scope_json: serde_json::json!({"scope": "top_of_personal_folders"}),
            restriction_json: serde_json::json!({"kind": "exchange_todo"}),
            excluded_folder_roles: exchange_builtin_excluded_folder_roles(),
            is_builtin: true,
        }]);

        assert_eq!(snapshot.todo_search_results().len(), 1);
        assert!(snapshot
            .task_for_id(
                crate::mapi::identity::TODO_SEARCH_FOLDER_ID,
                crate::mapi::identity::mapi_store_id(97)
            )
            .is_some());
    }

    #[test]
    fn snapshot_projects_followup_mail_into_todo_search_results() {
        let account_id = Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").unwrap();
        let mailbox_id = Uuid::parse_str("44444444-4444-4444-8444-444444444444").unwrap();
        let message_id = Uuid::parse_str("55555555-5555-4555-8555-555555555555").unwrap();
        let definition_id = Uuid::parse_str("99999999-9999-4999-8999-999999999999").unwrap();
        crate::mapi::identity::remember_mapi_identity(
            mailbox_id,
            crate::mapi::identity::mapi_store_id(18),
        );
        crate::mapi::identity::remember_mapi_identity(
            message_id,
            crate::mapi::identity::mapi_store_id(19),
        );
        crate::mapi::identity::remember_mapi_identity(
            definition_id,
            crate::mapi::identity::mapi_store_id(20),
        );
        let mailbox = JmapMailbox {
            id: mailbox_id,
            parent_id: None,
            role: "inbox".to_string(),
            name: "Inbox".to_string(),
            sort_order: 0,
            modseq: 1,
            total_emails: 1,
            unread_emails: 0,
            is_subscribed: true,
        };
        let email = JmapEmail {
            id: message_id,
            thread_id: Uuid::parse_str("12121212-1212-4212-8212-121212121212").unwrap(),
            mailbox_id,
            mailbox_role: "inbox".to_string(),
            mailbox_name: "Inbox".to_string(),
            modseq: 2,
            mailbox_ids: vec![mailbox_id],
            mailbox_states: vec![JmapEmailMailboxState {
                mailbox_id,
                role: "inbox".to_string(),
                name: "Inbox".to_string(),
                modseq: 2,
                unread: false,
                flagged: true,
                followup_flag_status: "flagged".to_string(),
                followup_icon: 6,
                todo_item_flags: 8,
                followup_request: "Follow up".to_string(),
                followup_start_at: None,
                followup_due_at: None,
                followup_completed_at: None,
                reminder_set: false,
                reminder_at: None,
                reminder_dismissed_at: None,
                swapped_todo_store_id: None,
                swapped_todo_data: None,
                categories: Vec::new(),
                draft: false,
            }],
            received_at: "2026-05-20T12:00:00Z".to_string(),
            sent_at: None,
            from_address: "alice@example.test".to_string(),
            from_display: Some("Alice".to_string()),
            sender_address: None,
            sender_display: None,
            sender_authorization_kind: "self".to_string(),
            submitted_by_account_id: account_id,
            to: Vec::new(),
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: "Flagged mail".to_string(),
            preview: "Flagged mail".to_string(),
            body_text: "Flagged mail".to_string(),
            body_html_sanitized: None,
            unread: false,
            flagged: true,
            followup_flag_status: "flagged".to_string(),
            followup_icon: 6,
            todo_item_flags: 8,
            followup_request: "Follow up".to_string(),
            followup_start_at: None,
            followup_due_at: None,
            followup_completed_at: None,
            reminder_set: false,
            reminder_at: None,
            reminder_dismissed_at: None,
            swapped_todo_store_id: None,
            swapped_todo_data: None,
            categories: Vec::new(),
            has_attachments: false,
            size_octets: 42,
            internet_message_id: None,
            mime_blob_ref: None,
            delivery_status: "stored".to_string(),
        };
        let snapshot = MapiMailStoreSnapshot::new(
            vec![mailbox],
            vec![email],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .with_search_folder_definitions(vec![SearchFolderDefinition {
            id: definition_id,
            account_id,
            role: "todo_search".to_string(),
            display_name: "To-Do".to_string(),
            definition_kind: "exchange_builtin".to_string(),
            result_object_kind: "mixed".to_string(),
            scope_json: serde_json::json!({"scope": "top_of_personal_folders"}),
            restriction_json: serde_json::json!({"kind": "exchange_todo"}),
            excluded_folder_roles: exchange_builtin_excluded_folder_roles(),
            is_builtin: true,
        }]);

        assert_eq!(snapshot.todo_search_messages().len(), 1);
        let message_id = snapshot.todo_search_messages()[0].id;
        assert!(snapshot.todo_search_message_for_id(message_id).is_some());
    }

    #[test]
    fn snapshot_projects_swapped_todo_mail_into_tracked_mail_processing_results() {
        let account_id = Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").unwrap();
        let mailbox_id = Uuid::parse_str("44444444-4444-4444-8444-444444444444").unwrap();
        let message_id = Uuid::parse_str("66666666-6666-4666-8666-666666666666").unwrap();
        let store_id = Uuid::parse_str("77777777-7777-4777-8777-777777777777").unwrap();
        let definition_id = Uuid::parse_str("88888888-8888-4888-8888-888888888888").unwrap();
        crate::mapi::identity::remember_mapi_identity(
            mailbox_id,
            crate::mapi::identity::mapi_store_id(20),
        );
        crate::mapi::identity::remember_mapi_identity(
            message_id,
            crate::mapi::identity::mapi_store_id(21),
        );
        crate::mapi::identity::remember_mapi_identity(
            definition_id,
            crate::mapi::identity::mapi_store_id(22),
        );
        let mailbox = JmapMailbox {
            id: mailbox_id,
            parent_id: None,
            role: "sent".to_string(),
            name: "Sent".to_string(),
            sort_order: 0,
            modseq: 1,
            total_emails: 1,
            unread_emails: 0,
            is_subscribed: true,
        };
        let email = JmapEmail {
            id: message_id,
            thread_id: Uuid::parse_str("12121212-1212-4212-8212-121212121212").unwrap(),
            mailbox_id,
            mailbox_role: "sent".to_string(),
            mailbox_name: "Sent".to_string(),
            modseq: 2,
            mailbox_ids: vec![mailbox_id],
            mailbox_states: vec![JmapEmailMailboxState {
                mailbox_id,
                role: "sent".to_string(),
                name: "Sent".to_string(),
                modseq: 2,
                unread: false,
                flagged: false,
                followup_flag_status: "none".to_string(),
                followup_icon: 0,
                todo_item_flags: 0,
                followup_request: String::new(),
                followup_start_at: None,
                followup_due_at: None,
                followup_completed_at: None,
                reminder_set: false,
                reminder_at: None,
                reminder_dismissed_at: None,
                swapped_todo_store_id: Some(store_id),
                swapped_todo_data: Some(vec![9, 8, 7]),
                categories: Vec::new(),
                draft: false,
            }],
            received_at: "2026-05-20T12:00:00Z".to_string(),
            sent_at: Some("2026-05-20T12:00:00Z".to_string()),
            from_address: "alice@example.test".to_string(),
            from_display: Some("Alice".to_string()),
            sender_address: None,
            sender_display: None,
            sender_authorization_kind: "self".to_string(),
            submitted_by_account_id: account_id,
            to: Vec::new(),
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: "Tracked mail".to_string(),
            preview: "Tracked mail".to_string(),
            body_text: "Tracked mail".to_string(),
            body_html_sanitized: None,
            unread: false,
            flagged: false,
            followup_flag_status: "none".to_string(),
            followup_icon: 0,
            todo_item_flags: 0,
            followup_request: String::new(),
            followup_start_at: None,
            followup_due_at: None,
            followup_completed_at: None,
            reminder_set: false,
            reminder_at: None,
            reminder_dismissed_at: None,
            swapped_todo_store_id: Some(store_id),
            swapped_todo_data: Some(vec![9, 8, 7]),
            categories: Vec::new(),
            has_attachments: false,
            size_octets: 42,
            internet_message_id: None,
            mime_blob_ref: None,
            delivery_status: "stored".to_string(),
        };
        let snapshot = MapiMailStoreSnapshot::new(
            vec![mailbox],
            vec![email],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .with_search_folder_definitions(vec![SearchFolderDefinition {
            id: definition_id,
            account_id,
            role: "tracked_mail_processing".to_string(),
            display_name: "Tracked Mail Processing".to_string(),
            definition_kind: "exchange_builtin".to_string(),
            result_object_kind: "message".to_string(),
            scope_json: serde_json::json!({"scope": "top_of_personal_folders"}),
            restriction_json: serde_json::json!({"kind": "exchange_tracked_mail_processing"}),
            excluded_folder_roles: exchange_builtin_excluded_folder_roles(),
            is_builtin: true,
        }]);

        assert_eq!(snapshot.tracked_mail_processing_messages().len(), 1);
        assert!(snapshot
            .tracked_mail_processing_message_for_id(crate::mapi::identity::mapi_store_id(21))
            .is_some());
    }

    #[test]
    fn snapshot_projects_reminders_as_underlying_calendar_and_task_links() {
        let account_id = Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").unwrap();
        let mailbox_id = Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap();
        let message_id = Uuid::parse_str("11112222-3333-4444-8555-666677778888").unwrap();
        let excluded_message_id = Uuid::parse_str("11112222-3333-4444-8555-666677778889").unwrap();
        let event_id = Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap();
        let task_id = Uuid::parse_str("33333333-3333-4333-8333-333333333333").unwrap();
        let search_definition_id = Uuid::parse_str("44444444-4444-4444-8444-444444444444").unwrap();
        crate::mapi::identity::remember_mapi_identity(
            message_id,
            crate::mapi::identity::mapi_store_id(97),
        );
        crate::mapi::identity::remember_mapi_identity(
            excluded_message_id,
            crate::mapi::identity::mapi_store_id(101),
        );
        crate::mapi::identity::remember_mapi_identity(
            event_id,
            crate::mapi::identity::mapi_store_id(98),
        );
        crate::mapi::identity::remember_mapi_identity(
            task_id,
            crate::mapi::identity::mapi_store_id(99),
        );
        crate::mapi::identity::remember_mapi_identity(
            search_definition_id,
            crate::mapi::identity::mapi_store_id(100),
        );
        let rights = CollaborationRights {
            may_read: true,
            may_write: true,
            may_delete: true,
            may_share: false,
        };
        let event = AccessibleEvent {
            id: event_id,
            uid: "event-uid".to_string(),
            collection_id: "default".to_string(),
            owner_account_id: account_id,
            owner_email: "alice@example.test".to_string(),
            owner_display_name: "Alice".to_string(),
            rights: rights.clone(),
            date: "2026-05-21".to_string(),
            time: "09:00".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 30,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Standup".to_string(),
            location: "Room 1".to_string(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: "[]".to_string(),
            notes: String::new(),
            body_html: String::new(),
        };
        let task_list_id = Uuid::parse_str("12121212-3434-4565-8787-909090909090").unwrap();
        let task = ClientTask {
            id: task_id,
            owner_account_id: account_id,
            owner_email: "alice@example.test".to_string(),
            owner_display_name: "Alice".to_string(),
            is_owned: true,
            rights: rights.clone(),
            task_list_id,
            task_list_sort_order: 0,
            title: "Follow up".to_string(),
            description: String::new(),
            status: "needs-action".to_string(),
            due_at: Some("2026-05-21T12:00:00Z".to_string()),
            completed_at: None,
            recurrence_rule: String::new(),
            sort_order: 0,
            updated_at: "2026-05-20T09:00:00Z".to_string(),
        };
        let mailbox = JmapMailbox {
            id: mailbox_id,
            parent_id: None,
            role: "inbox".to_string(),
            name: "Inbox".to_string(),
            sort_order: 10,
            modseq: 1,
            total_emails: 1,
            unread_emails: 0,
            is_subscribed: true,
        };
        let email = JmapEmail {
            id: message_id,
            thread_id: Uuid::parse_str("99999999-9999-4999-8999-999999999999").unwrap(),
            mailbox_id,
            mailbox_role: "inbox".to_string(),
            mailbox_name: "Inbox".to_string(),
            modseq: 2,
            mailbox_ids: vec![mailbox_id],
            mailbox_states: vec![JmapEmailMailboxState {
                mailbox_id,
                role: "inbox".to_string(),
                name: "Inbox".to_string(),
                modseq: 2,
                unread: false,
                flagged: true,
                followup_flag_status: "flagged".to_string(),
                followup_icon: 6,
                todo_item_flags: 8,
                followup_request: "Follow up".to_string(),
                followup_start_at: None,
                followup_due_at: Some("2026-05-21T17:00:00Z".to_string()),
                followup_completed_at: None,
                reminder_set: true,
                reminder_at: Some("2026-05-21T16:45:00Z".to_string()),
                reminder_dismissed_at: None,
                swapped_todo_store_id: None,
                swapped_todo_data: None,
                categories: Vec::new(),
                draft: false,
            }],
            received_at: "2026-05-20T12:00:00Z".to_string(),
            sent_at: None,
            from_address: "alice@example.test".to_string(),
            from_display: Some("Alice".to_string()),
            sender_address: None,
            sender_display: None,
            sender_authorization_kind: "self".to_string(),
            submitted_by_account_id: account_id,
            to: Vec::new(),
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: "Mail reminder".to_string(),
            preview: "Mail reminder".to_string(),
            body_text: "Mail reminder".to_string(),
            body_html_sanitized: None,
            unread: false,
            flagged: true,
            followup_flag_status: "flagged".to_string(),
            followup_icon: 6,
            todo_item_flags: 8,
            followup_request: "Follow up".to_string(),
            followup_start_at: None,
            followup_due_at: Some("2026-05-21T17:00:00Z".to_string()),
            followup_completed_at: None,
            reminder_set: true,
            reminder_at: Some("2026-05-21T16:45:00Z".to_string()),
            reminder_dismissed_at: None,
            swapped_todo_store_id: None,
            swapped_todo_data: None,
            categories: Vec::new(),
            has_attachments: false,
            size_octets: 42,
            internet_message_id: None,
            mime_blob_ref: None,
            delivery_status: "stored".to_string(),
        };
        let mut excluded_email = email.clone();
        excluded_email.id = excluded_message_id;
        excluded_email.mailbox_role = "drafts".to_string();
        excluded_email.mailbox_name = "Drafts".to_string();
        excluded_email.mailbox_states[0].role = "drafts".to_string();
        excluded_email.mailbox_states[0].name = "Drafts".to_string();
        let snapshot = MapiMailStoreSnapshot::new(
            vec![mailbox],
            vec![email, excluded_email],
            Vec::new(),
            Vec::new(),
            vec![CollaborationCollection {
                id: "default".to_string(),
                kind: "calendar".to_string(),
                owner_account_id: account_id,
                owner_email: "alice@example.test".to_string(),
                owner_display_name: "Alice".to_string(),
                display_name: "Calendar".to_string(),
                is_owned: true,
                rights: rights.clone(),
            }],
            vec![CollaborationCollection {
                id: "default".to_string(),
                kind: "tasks".to_string(),
                owner_account_id: account_id,
                owner_email: "alice@example.test".to_string(),
                owner_display_name: "Alice".to_string(),
                display_name: "Tasks".to_string(),
                is_owned: true,
                rights,
            }],
            Vec::new(),
            vec![event],
            vec![task],
            Vec::new(),
        )
        .with_search_folder_definitions(vec![SearchFolderDefinition {
            id: search_definition_id,
            account_id,
            role: "reminders".to_string(),
            display_name: "Reminders".to_string(),
            definition_kind: "exchange_builtin".to_string(),
            result_object_kind: "mixed".to_string(),
            scope_json: serde_json::json!({"scope": "top_of_personal_folders"}),
            restriction_json: serde_json::json!({"kind": "exchange_reminders"}),
            excluded_folder_roles: exchange_builtin_excluded_folder_roles(),
            is_builtin: true,
        }])
        .with_reminders(vec![
            ClientReminder {
                source_type: "mail".to_string(),
                source_id: excluded_message_id,
                occurrence_start_at: None,
                title: "Draft reminder".to_string(),
                due_at: Some("2026-05-21T17:00:00Z".to_string()),
                reminder_at: "2026-05-21T16:45:00Z".to_string(),
                dismissed_at: None,
                completed_at: None,
                status: "pending".to_string(),
            },
            ClientReminder {
                source_type: "mail".to_string(),
                source_id: message_id,
                occurrence_start_at: None,
                title: "Mail reminder".to_string(),
                due_at: Some("2026-05-21T17:00:00Z".to_string()),
                reminder_at: "2026-05-21T16:45:00Z".to_string(),
                dismissed_at: None,
                completed_at: None,
                status: "pending".to_string(),
            },
            ClientReminder {
                source_type: "calendar".to_string(),
                source_id: event_id,
                occurrence_start_at: None,
                title: "Standup".to_string(),
                due_at: Some("2026-05-21T09:30:00Z".to_string()),
                reminder_at: "2026-05-21T09:00:00Z".to_string(),
                dismissed_at: None,
                completed_at: None,
                status: "pending".to_string(),
            },
            ClientReminder {
                source_type: "task".to_string(),
                source_id: task_id,
                occurrence_start_at: None,
                title: "Follow up".to_string(),
                due_at: Some("2026-05-21T12:00:00Z".to_string()),
                reminder_at: "2026-05-21T11:45:00Z".to_string(),
                dismissed_at: None,
                completed_at: None,
                status: "pending".to_string(),
            },
        ]);

        assert_eq!(snapshot.reminder_events().len(), 1);
        assert_eq!(snapshot.reminder_tasks().len(), 1);
        assert_eq!(snapshot.reminder_messages().len(), 1);
        assert!(snapshot
            .reminder_message_for_id(crate::mapi::identity::mapi_store_id(101))
            .is_none());
        assert!(snapshot
            .event_for_id(
                crate::mapi::identity::REMINDERS_FOLDER_ID,
                crate::mapi::identity::mapi_store_id(98)
            )
            .is_some());
        assert!(snapshot
            .task_for_id(
                crate::mapi::identity::REMINDERS_FOLDER_ID,
                crate::mapi::identity::mapi_store_id(99)
            )
            .is_some());
    }

    #[test]
    fn snapshot_projects_computed_delegate_freebusy_messages() {
        let message_id = Uuid::parse_str("56565656-5656-4656-8656-565656565656").unwrap();
        crate::mapi::identity::remember_mapi_identity(
            message_id,
            crate::mapi::identity::mapi_store_id(610),
        );
        let snapshot = MapiMailStoreSnapshot::empty().with_delegate_freebusy_messages(vec![
            DelegateFreeBusyMessageObject {
                id: message_id,
                account_id: Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").unwrap(),
                owner_account_id: Uuid::parse_str("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb").unwrap(),
                owner_email: "owner@example.test".to_string(),
                message_kind: "freebusy".to_string(),
                subject: "owner@example.test: busy".to_string(),
                body_text: "busy from 2026-05-26T08:00:00Z to 2026-05-26T09:00:00Z".to_string(),
                starts_at: Some("2026-05-26T08:00:00Z".to_string()),
                ends_at: Some("2026-05-26T09:00:00Z".to_string()),
                busy_status: Some("busy".to_string()),
                payload_json: "{}".to_string(),
                updated_at: "2026-05-26T08:00:00Z".to_string(),
            },
        ]);

        assert_eq!(snapshot.delegate_freebusy_messages().len(), 1);
        let projected_id = snapshot.delegate_freebusy_messages()[0].id;
        assert!(snapshot
            .delegate_freebusy_message_for_id(projected_id)
            .is_some());
    }
}
