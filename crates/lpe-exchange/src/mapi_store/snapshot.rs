use super::*;
use crate::mapi_mailstore;
use anyhow::{anyhow, Result};

mod calendar_identity;

fn calendar_mapi_attachments(attachments: &[CalendarEventAttachment]) -> Vec<MapiAttachment> {
    attachments
        .iter()
        .enumerate()
        .map(|(index, attachment)| MapiAttachment {
            attach_num: index as u32,
            canonical_id: attachment.id,
            file_reference: attachment.file_reference.clone(),
            file_name: attachment.file_name.clone(),
            media_type: attachment.media_type.clone(),
            disposition: None,
            content_id: None,
            size_octets: attachment.size_octets,
        })
        .collect()
}

impl MapiMailStoreSnapshot {
    pub(crate) fn with_search_folder_definitions(
        mut self,
        search_folder_definitions: Vec<SearchFolderDefinition>,
    ) -> Self {
        self.folders
            .retain(|folder| !folder.mailbox.role.starts_with("__mapi_search_folder_"));
        let mut projected_user_saved_keys = HashSet::new();
        self.folders
            .extend(search_folder_definitions.iter().filter_map(|definition| {
                if !user_saved_search_folder_is_projectable(definition) {
                    return None;
                }
                if definition.is_builtin || definition.definition_kind != "user_saved" {
                    return mapi_search_folder_definition_to_folder(definition);
                }
                let key = user_saved_search_folder_projection_key(definition);
                if !projected_user_saved_keys.insert(key) {
                    return None;
                }
                mapi_search_folder_definition_to_folder(definition)
            }));
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
        if self.delegate_freebusy_messages.is_empty() {
            self.delegate_freebusy_messages
                .push(virtual_local_freebusy_message());
        }
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
        let account_id = navigation_shortcuts
            .first()
            .map(|shortcut| shortcut.account_id);
        let persisted_shortcut_count = navigation_shortcuts.len();
        let persisted_navigation_shortcuts = navigation_shortcuts
            .into_iter()
            .map(|shortcut| {
                let group_header_id = Some(
                    shortcut
                        .group_header_id
                        .unwrap_or_else(crate::mapi::properties::default_wlink_group_uuid),
                );
                MapiNavigationShortcutMessage {
                    id: mapi_item_id(&shortcut.id),
                    folder_id: crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
                    canonical_id: shortcut.id,
                    subject: shortcut.subject,
                    target_folder_id: shortcut.target_folder_id,
                    shortcut_type: shortcut.shortcut_type,
                    flags: shortcut.flags,
                    save_stamp: shortcut.save_stamp,
                    section: shortcut.section,
                    ordinal: shortcut.ordinal,
                    group_header_id,
                    group_name: normalize_navigation_shortcut_group_name(
                        shortcut.section,
                        group_header_id,
                        &shortcut.group_name,
                    ),
                }
            })
            .collect::<Vec<_>>();
        let persisted_navigation_shortcut_summary =
            format_navigation_shortcut_debug_summary(&persisted_navigation_shortcuts);
        self.navigation_shortcuts = persisted_navigation_shortcuts;
        let deduped_shortcuts = self.navigation_shortcut_messages();
        let table_messages = self.common_views_table_messages().collect::<Vec<_>>();
        let table_shortcut_count = table_messages
            .iter()
            .filter(|message| matches!(message, MapiCommonViewsMessage::NavigationShortcut(_)))
            .count();
        let default_table_shortcut_count = table_messages
            .iter()
            .filter(|message| {
                matches!(
                    message,
                    MapiCommonViewsMessage::NavigationShortcut(shortcut)
                        if is_outlook_common_views_default_navigation_shortcut_id(shortcut.id)
                )
            })
            .count();
        tracing::debug!(
            rca_debug = true,
            adapter = "mapi",
            account_id = %account_id
                .map(|id| id.to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            persisted_navigation_shortcut_count = persisted_shortcut_count,
            materialized_navigation_shortcut_count = table_shortcut_count,
            deduped_navigation_shortcut_count = deduped_shortcuts.len(),
            common_views_table_shortcut_count = table_shortcut_count,
            common_views_default_table_shortcut_count = default_table_shortcut_count,
            common_views_named_view_count = table_messages.len().saturating_sub(table_shortcut_count),
            persisted_navigation_shortcuts =
                %persisted_navigation_shortcut_summary,
            materialized_navigation_shortcuts =
                %format_common_views_table_shortcut_debug_summary(&table_messages),
            deduped_navigation_shortcuts =
                %format_navigation_shortcut_debug_summary(&deduped_shortcuts),
            common_views_table_shortcuts =
                %format_common_views_table_shortcut_debug_summary(&table_messages),
            "rca debug mapi navigation shortcut snapshot contract"
        );
        self
    }

    pub(crate) fn with_associated_configs(
        mut self,
        configs: Vec<MapiAssociatedConfigRecord>,
    ) -> Self {
        self.associated_configs = deduplicate_associated_config_messages(
            configs
                .into_iter()
                .filter(|config| {
                    !is_empty_synthetic_inbox_associated_config(config)
                        && !is_empty_outlook_inbox_named_view_placeholder(config)
                        && !is_stale_outlook_umolk_user_options_placeholder(config)
                })
                .map(|config| MapiAssociatedConfigMessage {
                    id: mapi_item_id(&config.id),
                    folder_id: config.folder_id,
                    canonical_id: config.id,
                    message_class: config.message_class,
                    subject: config.subject,
                    properties_json: config.properties_json,
                })
                .collect(),
        );
        self
    }

    pub(crate) fn with_associated_config_identity_ids(
        mut self,
        ids: Vec<MapiAssociatedConfigIdentity>,
    ) -> Self {
        self.associated_config_identity_ids = ids;
        self
    }

    pub(crate) fn with_reminders(mut self, reminders: Vec<ClientReminder>) -> Self {
        self.reminders = reminders;
        self
    }

    pub(crate) fn with_event_versions(mut self, versions: Vec<MapiEventVersion>) -> Result<Self> {
        let versioned_event_ids = versions
            .iter()
            .map(|version| version.event_id)
            .collect::<HashSet<_>>();
        for version in versions {
            self.remember_event_version(version);
        }
        if let Some(event) = self
            .events
            .iter()
            .find(|event| !versioned_event_ids.contains(&event.canonical_id))
        {
            return Err(anyhow!(
                "durable MAPI Event version is missing for canonical Event {}",
                event.canonical_id
            ));
        }
        Ok(self)
    }

    pub(crate) fn remember_event_version(&mut self, version: MapiEventVersion) {
        if let Some(event) = self
            .events
            .iter_mut()
            .find(|event| event.canonical_id == version.event_id)
        {
            event.version = version;
        }
    }

    pub(crate) fn remember_event_reminder_state(
        &mut self,
        event_id: Uuid,
        state: MapiEventReminderState,
    ) {
        let reminder = state.reminder_set.then(|| {
            let reminder_at = state.reminder_at?;
            let event = self
                .events
                .iter()
                .find(|event| event.canonical_id == event_id)?;
            let dismissed = state.reminder_dismissed_at.is_some();
            Some(ClientReminder {
                source_type: "calendar".to_string(),
                source_id: event_id,
                occurrence_start_at: None,
                title: event.event.title.clone(),
                due_at: None,
                reminder_at,
                dismissed_at: state.reminder_dismissed_at,
                completed_at: None,
                status: if dismissed { "dismissed" } else { "pending" }.to_string(),
            })
        });
        self.event_reminder_overrides
            .insert(event_id, reminder.flatten());
    }

    pub(crate) fn remember_created_event(
        &mut self,
        folder_id: u64,
        event_id: u64,
        event: AccessibleEvent,
        attachments: Vec<CalendarEventAttachment>,
    ) {
        let canonical_id = event.id;
        let version = fallback_event_version(&event, event_id);
        self.events.push(MapiEvent {
            id: event_id,
            source_key: mapi_mailstore::source_key_for_store_id(event_id),
            folder_id,
            canonical_id,
            event,
            version,
            attachments: calendar_mapi_attachments(&attachments),
        });
        if let Some(folder) = self
            .collaboration_folders
            .iter_mut()
            .find(|folder| folder.id == folder_id)
        {
            folder.item_count = folder.item_count.saturating_add(1);
        }
    }

    pub(crate) fn remember_updated_event(
        &mut self,
        folder_id: u64,
        event_id: u64,
        event: AccessibleEvent,
        version: MapiEventVersion,
        attachments: Vec<CalendarEventAttachment>,
    ) {
        if let Some(current) = self
            .events
            .iter_mut()
            .find(|current| current.folder_id == folder_id && current.id == event_id)
        {
            current.canonical_id = event.id;
            current.event = event;
            current.version = version;
            current.attachments = calendar_mapi_attachments(&attachments);
        }
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

    #[cfg(test)]
    pub(crate) fn with_collaboration_folder_item_count(
        mut self,
        folder_id: u64,
        item_count: u32,
    ) -> Self {
        if let Some(folder) = self
            .collaboration_folders
            .iter_mut()
            .find(|folder| folder.id == folder_id)
        {
            folder.item_count = item_count;
        }
        self
    }

    pub(crate) fn with_calendar_attachments(
        mut self,
        calendar_attachments: Vec<(Uuid, Vec<CalendarEventAttachment>)>,
    ) -> Self {
        for event in &mut self.events {
            let attachments = calendar_attachments
                .iter()
                .find(|(event_id, _)| *event_id == event.canonical_id)
                .map(|(_, attachments)| attachments.as_slice())
                .unwrap_or_default();
            event.attachments = calendar_mapi_attachments(attachments);
        }
        self
    }

    pub(crate) fn mailboxes(&self) -> Vec<JmapMailbox> {
        self.folders
            .iter()
            .map(|folder| folder.mailbox.clone())
            .collect()
    }

    pub(crate) fn folder_change_number(&self, folder_id: u64) -> Option<u64> {
        self.folder_versions.change_number(folder_id)
    }

    pub(crate) fn folder_version(&self, folder_id: u64) -> Option<&MapiFolderVersion> {
        self.folder_versions.version(folder_id)
    }

    pub(crate) fn folder_versions(&self) -> Vec<MapiFolderVersion> {
        self.folder_versions.all()
    }

    pub(crate) fn upsert_folder_version(&mut self, version: MapiFolderVersion) {
        self.folder_versions.upsert(version);
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
        self.content_windows
            .iter()
            .filter(|window| {
                window.folder_id == folder_id
                    && window.view_signature == view_signature
                    && offset >= window.offset
                    && offset <= window.offset.saturating_add(window.message_ids.len())
            })
            .find_map(|window| {
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
                if emails.len() != window.message_ids.len() {
                    return None;
                }
                let local_offset = offset.saturating_sub(window.offset);
                let available = emails.len().saturating_sub(local_offset);
                let window_reaches_end = window.offset.saturating_add(emails.len()) >= window.total;
                if available < limit && !window_reaches_end {
                    return None;
                }
                let row_count = available.min(limit);
                Some((
                    window.total,
                    emails[local_offset..local_offset + row_count].to_vec(),
                ))
            })
    }

    pub(crate) fn content_table_window_emails_containing(
        &self,
        folder_id: u64,
        view_signature: u64,
        position: usize,
    ) -> Option<(usize, usize, Vec<&JmapEmail>)> {
        self.content_windows
            .iter()
            .filter(|window| {
                window.folder_id == folder_id
                    && window.view_signature == view_signature
                    && position >= window.offset
                    && position <= window.offset.saturating_add(window.message_ids.len())
            })
            .filter_map(|window| {
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
                if emails.len() != window.message_ids.len() {
                    return None;
                }
                let local_offset = position.saturating_sub(window.offset);
                let tail_len = emails.len().saturating_sub(local_offset);
                Some((window.offset, window.total, emails, tail_len))
            })
            .max_by_key(|(offset, _, _, tail_len)| (*tail_len, *offset))
            .map(|(offset, total, emails, _)| (offset, total, emails))
    }

    pub(crate) fn content_table_total(&self, folder_id: u64, view_signature: u64) -> Option<usize> {
        self.content_windows
            .iter()
            .find(|window| {
                window.folder_id == folder_id
                    && window.view_signature == view_signature
                    && (window.message_ids.is_empty()
                        || window.message_ids.iter().all(|id| {
                            self.messages
                                .iter()
                                .any(|message| message.canonical_id == *id)
                        }))
            })
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
        if source_type == "calendar" {
            if let Some(reminder) = self.event_reminder_overrides.get(&source_id) {
                return reminder.as_ref();
            }
        }
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
        self.search_folder_definitions
            .iter()
            .find(|definition| {
                crate::mapi::identity::mapped_mapi_object_id(&definition.id) == Some(folder_id)
            })
            .or_else(|| {
                fixed_search_folder_role(folder_id)
                    .and_then(|role| self.search_folder_definition_for_role(role))
            })
    }

    pub(crate) fn user_saved_search_folder_definition_by_display_name(
        &self,
        display_name: &str,
        result_object_kind: &str,
    ) -> Option<&SearchFolderDefinition> {
        let display_name = display_name.trim();
        self.search_folder_definitions.iter().find(|definition| {
            !definition.is_builtin
                && definition.definition_kind == "user_saved"
                && user_saved_search_folder_is_projectable(definition)
                && definition.result_object_kind == result_object_kind
                && definition
                    .display_name
                    .trim()
                    .eq_ignore_ascii_case(display_name)
        })
    }

    pub(crate) fn rules(&self) -> &[MapiRule] {
        &self.rules
    }

    pub(crate) fn navigation_shortcut_messages(&self) -> Vec<MapiNavigationShortcutMessage> {
        deduplicate_navigation_shortcuts(self.navigation_shortcuts.clone())
    }

    pub(crate) fn common_views_table_messages(
        &self,
    ) -> impl Iterator<Item = MapiCommonViewsMessage> {
        let shortcuts = self.navigation_shortcut_messages();
        // [MS-OXOCFG] sections 3.1.4.9 and 3.1.4.10.2: navigation
        // shortcuts are client-owned Common Views FAI messages. Preserve
        // their canonical stored identity in the associated table.
        let mut table_shortcuts = shortcuts;
        append_missing_default_common_views_shortcuts(&mut table_shortcuts);
        replace_persisted_default_mail_favorite_shortcuts(&mut table_shortcuts);
        materialize_default_mail_group_header(&mut table_shortcuts);
        let messages = table_shortcuts
            .into_iter()
            .map(MapiCommonViewsMessage::NavigationShortcut)
            .chain(
                outlook_common_views_default_named_views()
                    .into_iter()
                    .map(MapiCommonViewsMessage::NamedView),
            )
            .chain(
                self.search_folder_definitions
                    .clone()
                    .into_iter()
                    .filter(common_views_search_folder_definition_is_projectable)
                    .map(MapiCommonViewsMessage::SearchFolderDefinition),
            )
            .collect::<Vec<_>>();
        messages.into_iter()
    }

    pub(crate) fn common_views_messages(&self) -> impl Iterator<Item = MapiCommonViewsMessage> {
        let messages = self
            .navigation_shortcut_messages()
            .into_iter()
            .map(MapiCommonViewsMessage::NavigationShortcut)
            .collect::<Vec<_>>();
        messages.into_iter()
    }

    pub(crate) fn associated_config_sync_messages_for_folder(
        &self,
        folder_id: u64,
    ) -> Vec<MapiAssociatedConfigMessage> {
        self.associated_config_messages_for_folder(folder_id)
    }

    pub(crate) fn navigation_shortcut_message_for_id(
        &self,
        item_id: u64,
    ) -> Option<MapiNavigationShortcutMessage> {
        self.navigation_shortcut_messages()
            .into_iter()
            .find(|message| message.id == item_id)
    }

    pub(crate) fn navigation_shortcut_table_message_for_id(
        &self,
        item_id: u64,
    ) -> Option<MapiNavigationShortcutMessage> {
        self.navigation_shortcut_message_for_id(item_id)
            .or_else(|| {
                outlook_common_views_default_navigation_shortcuts()
                    .into_iter()
                    .find(|shortcut| shortcut.section == 1 && shortcut.id == item_id)
            })
    }

    pub(crate) fn common_view_named_view_message_for_id(
        &self,
        item_id: u64,
    ) -> Option<MapiCommonViewNamedViewMessage> {
        outlook_common_views_default_named_views()
            .into_iter()
            .find(|message| message.id == item_id)
    }

    pub(crate) fn named_view_message_for_folder_and_id(
        &self,
        folder_id: u64,
        item_id: u64,
    ) -> Option<MapiCommonViewNamedViewMessage> {
        self.common_view_named_view_message_for_id(item_id)
            .or_else(|| self.default_folder_named_view_message(folder_id, item_id))
            .filter(|message| message.folder_id == folder_id)
    }

    pub(crate) fn default_folder_named_view_message(
        &self,
        folder_id: u64,
        item_id: u64,
    ) -> Option<MapiCommonViewNamedViewMessage> {
        if folder_id == crate::mapi::identity::SENT_FOLDER_ID {
            return None;
        }
        let view_id = outlook_default_folder_named_view_id(folder_id);
        (item_id == view_id || item_id == OUTLOOK_DEFAULT_FOLDER_NAMED_VIEW_ID).then(|| {
            MapiCommonViewNamedViewMessage {
                id: view_id,
                folder_id,
                canonical_id: outlook_default_folder_named_view_canonical_id(folder_id),
                name: outlook_default_folder_named_view_name(folder_id).to_string(),
                view_flags: 14_745_605,
                view_type: 8,
            }
        })
    }

    pub(crate) fn associated_config_messages_for_folder(
        &self,
        folder_id: u64,
    ) -> Vec<MapiAssociatedConfigMessage> {
        let mut messages = self
            .associated_configs
            .iter()
            .filter(|message| message.folder_id == folder_id)
            .cloned()
            .collect::<Vec<_>>();
        if folder_id == crate::mapi::identity::INBOX_FOLDER_ID {
            for default_message in outlook_inbox_associated_config_sync_defaults(folder_id) {
                if !messages
                    .iter()
                    .any(|message| message.message_class == default_message.message_class)
                {
                    messages.push(default_message);
                }
            }
        } else if self.contact_sync_default_supported_folder(folder_id) {
            for default_message in outlook_contact_associated_config_defaults(folder_id) {
                if !messages
                    .iter()
                    .any(|message| message.message_class == default_message.message_class)
                {
                    messages.push(default_message);
                }
            }
        }
        deduplicate_associated_config_messages(messages)
    }

    pub(crate) fn associated_config_message_for_id(
        &self,
        item_id: u64,
    ) -> Option<MapiAssociatedConfigMessage> {
        self.associated_configs
            .iter()
            .find(|message| message.id == item_id)
            .cloned()
            .or_else(|| {
                outlook_inbox_associated_config_sync_defaults(
                    crate::mapi::identity::INBOX_FOLDER_ID,
                )
                .into_iter()
                .find(|message| message.id == item_id)
            })
            .or_else(|| {
                outlook_inbox_broad_startup_associated_config_defaults()
                    .into_iter()
                    .find(|message| message.id == item_id)
            })
            .or_else(|| outlook_inbox_exact_virtual_associated_config_for_id(item_id))
            .or_else(|| {
                [
                    crate::mapi::identity::CONTACTS_FOLDER_ID,
                    crate::mapi::identity::SUGGESTED_CONTACTS_FOLDER_ID,
                    crate::mapi::identity::QUICK_CONTACTS_FOLDER_ID,
                    crate::mapi::identity::IM_CONTACT_LIST_FOLDER_ID,
                ]
                .into_iter()
                .flat_map(outlook_contact_associated_config_defaults)
                .find(|message| message.id == item_id)
            })
            .or_else(|| {
                self.collaboration_folders
                    .iter()
                    .filter(|folder| folder.kind == MapiCollaborationFolderKind::Contacts)
                    .flat_map(|folder| outlook_contact_associated_config_defaults(folder.id))
                    .find(|message| message.id == item_id)
            })
            .or_else(|| {
                self.folders
                    .iter()
                    .filter(|folder| mailbox_contact_sync_default_supported(&folder.mailbox))
                    .flat_map(|folder| outlook_contact_associated_config_defaults(folder.id))
                    .find(|message| message.id == item_id)
            })
    }

    pub(crate) fn associated_config_message_for_identity_id(
        &self,
        item_id: u64,
    ) -> Option<MapiAssociatedConfigMessage> {
        let canonical_id = self
            .associated_config_identity_ids
            .iter()
            .find(|identity| identity.object_id == item_id)
            .map(|identity| identity.canonical_id)?;
        self.associated_config_messages_for_folder(crate::mapi::identity::INBOX_FOLDER_ID)
            .into_iter()
            .chain(
                [
                    crate::mapi::identity::CONTACTS_FOLDER_ID,
                    crate::mapi::identity::SUGGESTED_CONTACTS_FOLDER_ID,
                    crate::mapi::identity::QUICK_CONTACTS_FOLDER_ID,
                    crate::mapi::identity::IM_CONTACT_LIST_FOLDER_ID,
                ]
                .into_iter()
                .flat_map(outlook_contact_associated_config_defaults),
            )
            .find(|message| message.canonical_id == canonical_id)
    }

    pub(crate) fn associated_config_message_for_folder_and_source_key_id(
        &self,
        folder_id: u64,
        item_id: u64,
    ) -> Option<MapiAssociatedConfigMessage> {
        crate::mapi::identity::global_counter_from_store_id(item_id)?;
        let source_key = crate::mapi::identity::source_key_for_object_id(item_id);
        self.associated_config_messages_for_folder(folder_id)
            .into_iter()
            .find(|message| message.folder_id == folder_id && message.id == item_id)
            .or_else(|| {
                self.associated_config_message_for_folder_and_source_key(folder_id, &source_key)
            })
    }

    pub(crate) fn associated_config_message_for_folder_and_source_key(
        &self,
        folder_id: u64,
        source_key: &[u8],
    ) -> Option<MapiAssociatedConfigMessage> {
        self.associated_config_messages_for_folder(folder_id)
            .into_iter()
            .find(|message| {
                message.folder_id == folder_id
                    && associated_config_source_key(&message.properties_json)
                        .is_some_and(|message_source_key| message_source_key == source_key)
            })
    }

    pub(crate) fn associated_config_identity_matches_folder(
        &self,
        folder_id: u64,
        item_id: u64,
    ) -> bool {
        self.associated_config_message_for_id(item_id)
            .or_else(|| self.associated_config_message_for_identity_id(item_id))
            .is_some_and(|message| message.folder_id == folder_id)
    }

    fn contact_sync_default_supported_folder(&self, folder_id: u64) -> bool {
        matches!(
            folder_id,
            crate::mapi::identity::CONTACTS_FOLDER_ID
                | crate::mapi::identity::SUGGESTED_CONTACTS_FOLDER_ID
                | crate::mapi::identity::QUICK_CONTACTS_FOLDER_ID
                | crate::mapi::identity::IM_CONTACT_LIST_FOLDER_ID
        ) || self.collaboration_folders.iter().any(|folder| {
            folder.kind == MapiCollaborationFolderKind::Contacts && folder.id == folder_id
        }) || self.folders.iter().any(|folder| {
            folder.id == folder_id && mailbox_contact_sync_default_supported(&folder.mailbox)
        })
    }

    pub(crate) fn conversation_action_messages(&self) -> &[MapiConversationActionMessage] {
        &self.conversation_actions
    }

    pub(crate) fn conversation_action_table_messages(&self) -> Vec<MapiConversationActionMessage> {
        self.conversation_actions.clone()
    }

    pub(crate) fn conversation_action_message_for_id(
        &self,
        item_id: u64,
    ) -> Option<&MapiConversationActionMessage> {
        self.conversation_actions
            .iter()
            .find(|message| message.id == item_id)
    }

    pub(crate) fn conversation_action_table_message_for_id(
        &self,
        item_id: u64,
    ) -> Option<MapiConversationActionMessage> {
        self.conversation_action_message_for_id(item_id).cloned()
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

fn fallback_event_version(event: &AccessibleEvent, event_id: u64) -> MapiEventVersion {
    let change_number = mapi_mailstore::change_number_for_store_id(event_id);
    MapiEventVersion {
        event_id: event.id,
        canonical_modseq: 1,
        change_number,
        change_key: mapi_mailstore::change_key_for_change_number(change_number),
        predecessor_change_list: mapi_mailstore::predecessor_change_list(change_number),
        updated_at: format!("{}T{}:00Z", event.date, event.time),
    }
}
