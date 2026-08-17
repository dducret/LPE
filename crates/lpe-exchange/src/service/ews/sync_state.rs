use serde::{Deserialize, Serialize};

use super::super::*;
use crate::store::EwsSyncCursor;

const LEGACY_COLLABORATION_SYNC_STATE_VERSION: &str = "v2";
const LEGACY_PUBLIC_FOLDER_SYNC_STATE_VERSION: &str = "v3";
const EWS_SYNC_CURSOR_TOKEN_VERSION: &str = "lpe-ews-sync.v1";
const EWS_SYNC_CURSOR_MAX_ITEMS: usize = 1_024;
const EWS_SYNC_CURSOR_MAX_ITEM_BYTES: usize = 512;

impl<S, V> ExchangeService<S, V>
where
    S: ExchangeStore + Clone + Send + Sync + 'static,
    V: Detector + Clone + Send + Sync + 'static,
{
    pub(in crate::service) async fn sync_folder_items(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let mut changes = String::new();
        let mut includes_last = true;
        let max_changes = requested_max_changes(request)?;
        if element_contents(request, "SyncState").len() > 1 {
            bail!("SyncFolderItems accepts at most one SyncState");
        }
        let requested_state = requested_sync_state(request);
        let cursor = match requested_state
            .as_deref()
            .filter(|state| state.starts_with(EWS_SYNC_CURSOR_TOKEN_VERSION))
        {
            Some(state) => {
                let (kind, cursor_id) = parse_ews_sync_cursor_token(state)?;
                let cursor = self
                    .store
                    .fetch_ews_sync_cursor(principal.account_id, cursor_id)
                    .await?
                    .ok_or_else(|| anyhow!("SyncState is no longer available"))?;
                Some((kind, cursor))
            }
            None => None,
        };
        let sync_state = match match cursor.as_ref().map(|(kind, _)| kind.as_str()) {
            Some("mailbox") => FolderKind::Mailbox,
            Some("contacts") => FolderKind::Contacts,
            Some("calendar") => FolderKind::Calendar,
            Some("tasks") => FolderKind::Tasks,
            Some("public-folder") => FolderKind::PublicFolders,
            Some(_) => bail!("SyncState kind is invalid"),
            None => requested_folder_kind(request).unwrap_or(FolderKind::Contacts),
        } {
            FolderKind::Root => "root:0".to_string(),
            FolderKind::Contacts => {
                let collection_id = requested_cursor_collection_id(
                    request,
                    cursor.as_ref().map(|(_, cursor)| cursor),
                    "contacts",
                    CONTACTS_FOLDER_ID,
                )?;
                let contacts = self
                    .store
                    .fetch_accessible_contacts_in_collection(principal.account_id, &collection_id)
                    .await?;
                let change_keys =
                    contact_change_keys(&self.store, principal.account_id, &contacts).await?;
                let current_items = contacts
                    .iter()
                    .map(|contact| {
                        Ok((
                            contact.id,
                            change_key_for(&change_keys, contact.id, "contact")?.to_string(),
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let current_set = current_items
                    .iter()
                    .map(|(id, _)| *id)
                    .collect::<HashSet<_>>();
                let scope = format!("contacts:{collection_id}");
                let previous_state = match cursor.as_ref() {
                    Some((_, cursor)) => cursor_snapshot(cursor, &scope)?,
                    None => requested_state
                        .as_deref()
                        .map(|state| {
                            collaboration_sync_state_items(
                                state,
                                principal.account_id,
                                "contacts",
                                &collection_id,
                            )
                        })
                        .transpose()?
                        .unwrap_or_default(),
                };
                let mut next_by_id = sync_state_items_by_id(&previous_state.items);
                let previous_by_id = next_by_id.clone();
                let mut pending_changes = Vec::new();
                for contact in &contacts {
                    let current_change_key = change_key_for(&change_keys, contact.id, "contact")?;
                    match previous_by_id.get(&contact.id) {
                        None => {
                            pending_changes.push((
                                contact.id,
                                Some(current_change_key.to_string()),
                                format!(
                                    "<t:Create>{}</t:Create>",
                                    contact_item_xml_with_change_key(contact, &current_change_key),
                                ),
                            ));
                        }
                        Some(None) => {
                            pending_changes.push((
                                contact.id,
                                Some(current_change_key.to_string()),
                                format!(
                                    "<t:Update>{}</t:Update>",
                                    contact_item_xml_with_change_key(contact, &current_change_key),
                                ),
                            ));
                        }
                        Some(Some(previous_change_key))
                            if !previous_state.is_current_version
                                || previous_change_key != &current_change_key =>
                        {
                            pending_changes.push((
                                contact.id,
                                Some(current_change_key.to_string()),
                                format!(
                                    "<t:Update>{}</t:Update>",
                                    contact_item_xml_with_change_key(contact, &current_change_key),
                                ),
                            ));
                        }
                        _ => {}
                    }
                }
                for item in previous_state.items {
                    let contact_id = item.id;
                    if !current_set.contains(&contact_id) {
                        let change_key = item.change_key.as_deref().unwrap_or("deleted");
                        pending_changes.push((
                            contact_id,
                            None,
                            format!(
                                "<t:Delete><t:ItemId Id=\"contact:{contact_id}\" ChangeKey=\"{}\"/></t:Delete>",
                                escape_xml(change_key),
                            ),
                        ));
                    }
                }
                includes_last = append_sync_change_page(
                    &mut changes,
                    pending_changes,
                    max_changes,
                    &mut next_by_id,
                );
                let snapshot = CollaborationSyncState {
                    is_current_version: true,
                    items: next_by_id
                        .into_iter()
                        .filter_map(|(id, change_key)| {
                            change_key.map(|change_key| SyncStateItem {
                                id,
                                change_key: Some(change_key),
                            })
                        })
                        .collect(),
                };
                validate_collaboration_sync_snapshot(&snapshot)?;
                let cursor_id = self
                    .store
                    .store_ews_sync_cursor(
                        principal.account_id,
                        &scope,
                        serde_json::to_value(snapshot)?,
                    )
                    .await?;
                ews_sync_cursor_token("contacts", cursor_id)
            }
            FolderKind::Calendar => {
                let collection_id = requested_cursor_collection_id(
                    request,
                    cursor.as_ref().map(|(_, cursor)| cursor),
                    "calendar",
                    CALENDAR_FOLDER_ID,
                )?;
                let events = self
                    .store
                    .fetch_accessible_events_in_collection(principal.account_id, &collection_id)
                    .await?;
                let change_keys =
                    event_change_keys(&self.store, principal.account_id, &events).await?;
                let current_items = events
                    .iter()
                    .map(|event| {
                        Ok((
                            event.id,
                            change_key_for(&change_keys, event.id, "calendar")?.to_string(),
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let current_set = current_items
                    .iter()
                    .map(|(id, _)| *id)
                    .collect::<HashSet<_>>();
                let scope = format!("calendar:{collection_id}");
                let previous_state = match cursor.as_ref() {
                    Some((_, cursor)) => cursor_snapshot(cursor, &scope)?,
                    None => requested_state
                        .as_deref()
                        .map(|state| {
                            collaboration_sync_state_items(
                                state,
                                principal.account_id,
                                "calendar",
                                &collection_id,
                            )
                        })
                        .transpose()?
                        .unwrap_or_default(),
                };
                let mut next_by_id = sync_state_items_by_id(&previous_state.items);
                let previous_by_id = next_by_id.clone();
                let mut pending_changes = Vec::new();
                for event in &events {
                    let current_change_key = change_key_for(&change_keys, event.id, "calendar")?;
                    match previous_by_id.get(&event.id) {
                        None => {
                            pending_changes.push((
                                event.id,
                                Some(current_change_key.to_string()),
                                format!(
                                    "<t:Create>{}</t:Create>",
                                    calendar_item_xml_with_change_key(event, &current_change_key),
                                ),
                            ));
                        }
                        Some(None) => {
                            pending_changes.push((
                                event.id,
                                Some(current_change_key.to_string()),
                                format!(
                                    "<t:Update>{}</t:Update>",
                                    calendar_item_xml_with_change_key(event, &current_change_key),
                                ),
                            ));
                        }
                        Some(Some(previous_change_key))
                            if !previous_state.is_current_version
                                || previous_change_key != &current_change_key =>
                        {
                            pending_changes.push((
                                event.id,
                                Some(current_change_key.to_string()),
                                format!(
                                    "<t:Update>{}</t:Update>",
                                    calendar_item_xml_with_change_key(event, &current_change_key),
                                ),
                            ));
                        }
                        _ => {}
                    }
                }
                for item in previous_state.items {
                    let event_id = item.id;
                    if !current_set.contains(&event_id) {
                        let change_key = item.change_key.as_deref().unwrap_or("deleted");
                        pending_changes.push((
                            event_id,
                            None,
                            format!(
                                "<t:Delete><t:ItemId Id=\"event:{event_id}\" ChangeKey=\"{}\"/></t:Delete>",
                                escape_xml(change_key),
                            ),
                        ));
                    }
                }
                includes_last = append_sync_change_page(
                    &mut changes,
                    pending_changes,
                    max_changes,
                    &mut next_by_id,
                );
                let snapshot = CollaborationSyncState {
                    is_current_version: true,
                    items: next_by_id
                        .into_iter()
                        .filter_map(|(id, change_key)| {
                            change_key.map(|change_key| SyncStateItem {
                                id,
                                change_key: Some(change_key),
                            })
                        })
                        .collect(),
                };
                validate_collaboration_sync_snapshot(&snapshot)?;
                let cursor_id = self
                    .store
                    .store_ews_sync_cursor(
                        principal.account_id,
                        &scope,
                        serde_json::to_value(snapshot)?,
                    )
                    .await?;
                ews_sync_cursor_token("calendar", cursor_id)
            }
            FolderKind::Tasks => {
                let collection_id = requested_cursor_collection_id(
                    request,
                    cursor.as_ref().map(|(_, cursor)| cursor),
                    "tasks",
                    TASKS_FOLDER_ID,
                )?;
                let tasks = self
                    .store
                    .fetch_accessible_tasks_in_collection(principal.account_id, &collection_id)
                    .await?;
                let change_keys =
                    task_change_keys(&self.store, principal.account_id, &tasks).await?;
                let current_items = tasks
                    .iter()
                    .map(|task| {
                        Ok((
                            task.id,
                            change_key_for(&change_keys, task.id, "task")?.to_string(),
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let current_set = current_items
                    .iter()
                    .map(|(id, _)| *id)
                    .collect::<HashSet<_>>();
                let scope = format!("tasks:{collection_id}");
                let previous_state = match cursor.as_ref() {
                    Some((_, cursor)) => cursor_snapshot(cursor, &scope)?,
                    None => requested_state
                        .as_deref()
                        .map(|state| {
                            collaboration_sync_state_items(
                                state,
                                principal.account_id,
                                "tasks",
                                &collection_id,
                            )
                        })
                        .transpose()?
                        .unwrap_or_default(),
                };
                let mut next_by_id = sync_state_items_by_id(&previous_state.items);
                let previous_by_id = next_by_id.clone();
                let mut pending_changes = Vec::new();
                for task in &tasks {
                    let current_change_key = change_key_for(&change_keys, task.id, "task")?;
                    match previous_by_id.get(&task.id) {
                        None => {
                            pending_changes.push((
                                task.id,
                                Some(current_change_key.to_string()),
                                format!(
                                    "<t:Create>{}</t:Create>",
                                    task_item_xml_with_change_key(task, &current_change_key),
                                ),
                            ));
                        }
                        Some(None) => {
                            pending_changes.push((
                                task.id,
                                Some(current_change_key.to_string()),
                                format!(
                                    "<t:Update>{}</t:Update>",
                                    task_item_xml_with_change_key(task, &current_change_key),
                                ),
                            ));
                        }
                        Some(Some(previous_change_key))
                            if !previous_state.is_current_version
                                || previous_change_key != &current_change_key =>
                        {
                            pending_changes.push((
                                task.id,
                                Some(current_change_key.to_string()),
                                format!(
                                    "<t:Update>{}</t:Update>",
                                    task_item_xml_with_change_key(task, &current_change_key),
                                ),
                            ));
                        }
                        _ => {}
                    }
                }
                for item in previous_state.items {
                    let task_id = item.id;
                    if !current_set.contains(&task_id) {
                        let change_key = item.change_key.as_deref().unwrap_or("deleted");
                        pending_changes.push((
                            task_id,
                            None,
                            format!(
                                "<t:Delete><t:ItemId Id=\"task:{task_id}\" ChangeKey=\"{}\"/></t:Delete>",
                                escape_xml(change_key),
                            ),
                        ));
                    }
                }
                includes_last = append_sync_change_page(
                    &mut changes,
                    pending_changes,
                    max_changes,
                    &mut next_by_id,
                );
                let snapshot = CollaborationSyncState {
                    is_current_version: true,
                    items: next_by_id
                        .into_iter()
                        .filter_map(|(id, change_key)| {
                            change_key.map(|change_key| SyncStateItem {
                                id,
                                change_key: Some(change_key),
                            })
                        })
                        .collect(),
                };
                validate_collaboration_sync_snapshot(&snapshot)?;
                let cursor_id = self
                    .store
                    .store_ews_sync_cursor(
                        principal.account_id,
                        &scope,
                        serde_json::to_value(snapshot)?,
                    )
                    .await?;
                ews_sync_cursor_token("tasks", cursor_id)
            }
            FolderKind::Mailbox => {
                let state = match cursor.as_ref() {
                    Some((_, cursor)) => {
                        serde_json::from_value::<MailboxSyncState>(cursor.snapshot_json.clone())
                            .map(Some)
                            .map_err(|_| anyhow!("SyncState snapshot is invalid"))?
                    }
                    None if requested_state.is_some() => {
                        bail!("SyncState is not a supported LPE synchronization cursor")
                    }
                    None => None,
                };
                let mailbox_ids = self
                    .requested_mailbox_folder_ids(principal, request)
                    .await?;
                if mailbox_ids.len() > 1 {
                    bail!("SyncFolderItems requires exactly one mailbox folder");
                }
                let mailbox_id = match (mailbox_ids.first().copied(), state.as_ref()) {
                    (Some(mailbox_id), Some(state)) if mailbox_id != state.mailbox_id => {
                        bail!("SyncState does not belong to SyncFolderId")
                    }
                    (Some(mailbox_id), _) => mailbox_id,
                    (None, Some(state)) => state.mailbox_id,
                    (None, None) => bail!("SyncFolderItems requires a mailbox SyncFolderId"),
                };
                if let Some((_, cursor)) = cursor.as_ref() {
                    if cursor.scope != format!("mailbox:{mailbox_id}") {
                        bail!("SyncState does not belong to SyncFolderId");
                    }
                }
                if !self
                    .store
                    .fetch_jmap_mailboxes(principal.account_id)
                    .await?
                    .iter()
                    .any(|mailbox| mailbox.id == mailbox_id)
                {
                    bail!("SyncFolderItems mailbox folder is not visible to the authenticated account");
                }
                match state {
                    Some(state) if state.snapshot_after.is_some() => {
                        let (emails, fence) = mailbox_sync_snapshot(
                            &self.store,
                            principal.account_id,
                            mailbox_id,
                            state.snapshot_after,
                        )
                        .await?;
                        includes_last = emails.len() <= max_changes;
                        let page = emails.into_iter().take(max_changes).collect::<Vec<_>>();
                        for email in &page {
                            changes.push_str("<t:Create>");
                            changes.push_str(&message_summary_xml_for_mailbox(email, mailbox_id));
                            changes.push_str("</t:Create>");
                        }
                        let next = if includes_last {
                            MailboxSyncState::replay(mailbox_id, fence)
                        } else {
                            MailboxSyncState::snapshot(
                                mailbox_id,
                                fence,
                                page.last().map(|email| email.id),
                            )
                        };
                        let cursor_id = self
                            .store
                            .store_ews_sync_cursor(
                                principal.account_id,
                                &format!("mailbox:{mailbox_id}"),
                                serde_json::to_value(next)?,
                            )
                            .await?;
                        ews_sync_cursor_token("mailbox", cursor_id)
                    }
                    Some(state) => {
                        let replay = self
                            .store
                            .replay_ews_mailbox_item_sync(
                                principal.account_id,
                                mailbox_id,
                                state.cursor.min(i64::MAX as u64) as i64,
                                max_changes,
                            )
                            .await?;
                        if replay.expired {
                            bail!("SyncState is no longer available in canonical change-log retention");
                        }
                        includes_last = !replay.more_events;
                        let ids = replay
                            .events
                            .iter()
                            .filter(|event| {
                                !matches!(event.change_kind.as_str(), "destroyed" | "expunged")
                            })
                            .map(|event| event.message_id)
                            .collect::<Vec<_>>();
                        let emails = self
                            .store
                            .fetch_jmap_emails(principal.account_id, &ids)
                            .await?
                            .into_iter()
                            .filter(|email| {
                                email
                                    .mailbox_states
                                    .iter()
                                    .any(|item| item.mailbox_id == mailbox_id)
                            })
                            .map(|email| (email.id, email))
                            .collect::<HashMap<_, _>>();
                        for event in replay.events {
                            match event.change_kind.as_str() {
                                "created" | "moved" | "updated" => {
                                    let Some(email) = emails.get(&event.message_id) else { continue };
                                    changes.push_str(if matches!(event.change_kind.as_str(), "created" | "moved") { "<t:Create>" } else { "<t:Update>" });
                                    changes.push_str(&message_summary_xml_for_mailbox(email, mailbox_id));
                                    changes.push_str(if matches!(event.change_kind.as_str(), "created" | "moved") { "</t:Create>" } else { "</t:Update>" });
                                }
                                "destroyed" | "expunged" => changes.push_str(&format!(
                                    "<t:Delete><t:ItemId Id=\"message:{}\" ChangeKey=\"{}\"/></t:Delete>",
                                    event.message_id,
                                    escape_xml(&versioned_change_key("message", &event.message_id.to_string(), &event.modseq.to_string())),
                                )),
                                _ => {}
                            }
                        }
                        let cursor_id = self
                            .store
                            .store_ews_sync_cursor(
                                principal.account_id,
                                &format!("mailbox:{mailbox_id}"),
                                serde_json::to_value(MailboxSyncState::replay(
                                    mailbox_id,
                                    replay.next_cursor.max(0) as u64,
                                ))?,
                            )
                            .await?;
                        ews_sync_cursor_token("mailbox", cursor_id)
                    }
                    None => {
                        let (emails, fence) = mailbox_sync_snapshot(
                            &self.store,
                            principal.account_id,
                            mailbox_id,
                            None,
                        )
                        .await?;
                        includes_last = emails.len() <= max_changes;
                        let page = emails.into_iter().take(max_changes).collect::<Vec<_>>();
                        for email in &page {
                            changes.push_str("<t:Create>");
                            changes.push_str(&message_summary_xml_for_mailbox(email, mailbox_id));
                            changes.push_str("</t:Create>");
                        }
                        let next = if includes_last {
                            MailboxSyncState::replay(mailbox_id, fence)
                        } else {
                            MailboxSyncState::snapshot(
                                mailbox_id,
                                fence,
                                page.last().map(|email| email.id),
                            )
                        };
                        let cursor_id = self
                            .store
                            .store_ews_sync_cursor(
                                principal.account_id,
                                &format!("mailbox:{mailbox_id}"),
                                serde_json::to_value(next)?,
                            )
                            .await?;
                        ews_sync_cursor_token("mailbox", cursor_id)
                    }
                }
            }
            FolderKind::PublicFolders => {
                let folder_id = match requested_public_folder_ids(request).into_iter().next() {
                    Some(folder_id) => folder_id,
                    None => match cursor.as_ref().map(|(_, cursor)| cursor.scope.as_str()) {
                        Some(scope) => Uuid::parse_str(
                            scope
                                .strip_prefix("public-folder:")
                                .ok_or_else(|| anyhow!("SyncState kind is invalid"))?,
                        )
                        .map_err(|_| anyhow!("SyncState snapshot is invalid"))?,
                        None => {
                            return Ok(sync_folder_items_response(
                                "public-folder:0",
                                String::new(),
                                true,
                            ))
                        }
                    },
                };
                let items = self
                    .store
                    .fetch_public_folder_items(principal.account_id, folder_id)
                    .await?;
                let current_items = items
                    .iter()
                    .map(|item| (item.id, public_folder_item_change_key(item), item.is_read))
                    .collect::<Vec<_>>();
                let current_set = current_items
                    .iter()
                    .map(|(id, _, _)| *id)
                    .collect::<HashSet<_>>();
                let collection_id = folder_id.to_string();
                let scope = format!("public-folder:{collection_id}");
                let previous_state = match cursor.as_ref() {
                    Some((_, cursor)) => cursor_snapshot(cursor, &scope)?,
                    None => requested_state
                        .as_deref()
                        .map(|state| {
                            public_folder_sync_state_items(
                                state,
                                principal.account_id,
                                &collection_id,
                            )
                        })
                        .transpose()?
                        .unwrap_or_default(),
                };
                let previous_by_id = previous_state
                    .items
                    .iter()
                    .map(|item| (item.id, item))
                    .collect::<HashMap<_, _>>();
                for item in &items {
                    let current_change_key = public_folder_item_change_key(item);
                    match previous_by_id.get(&item.id) {
                        None => {
                            changes.push_str("<t:Create>");
                            changes.push_str(&public_folder_item_summary_xml(item));
                            changes.push_str("</t:Create>");
                        }
                        Some(previous)
                            if !previous_state.is_current_version
                                || previous.change_key.as_deref()
                                    != Some(current_change_key.as_str()) =>
                        {
                            changes.push_str("<t:Update>");
                            changes.push_str(&public_folder_item_summary_xml(item));
                            changes.push_str("</t:Update>");
                        }
                        Some(previous) if previous.is_read != Some(item.is_read) => {
                            changes.push_str("<t:ReadFlagChange>");
                            changes.push_str(&format!(
                                "<t:ItemId Id=\"public-folder-item:{}\" ChangeKey=\"{}\"/><t:IsRead>{}</t:IsRead>",
                                item.id,
                                escape_xml(&current_change_key),
                                item.is_read,
                            ));
                            changes.push_str("</t:ReadFlagChange>");
                        }
                        _ => {}
                    }
                }
                for item in previous_state.items {
                    let item_id = item.id;
                    if !current_set.contains(&item_id) {
                        let change_key = item.change_key.as_deref().unwrap_or("deleted");
                        changes.push_str("<t:Delete>");
                        changes.push_str(&format!(
                            "<t:ItemId Id=\"public-folder-item:{item_id}\" ChangeKey=\"{}\"/>",
                            escape_xml(change_key),
                        ));
                        changes.push_str("</t:Delete>");
                    }
                }
                let snapshot = PublicFolderSyncState {
                    is_current_version: true,
                    items: current_items
                        .into_iter()
                        .map(|(id, change_key, is_read)| PublicFolderSyncStateItem {
                            id,
                            change_key: Some(change_key),
                            is_read: Some(is_read),
                        })
                        .collect(),
                };
                validate_public_folder_sync_snapshot(&snapshot)?;
                let cursor_id = self
                    .store
                    .store_ews_sync_cursor(
                        principal.account_id,
                        &scope,
                        serde_json::to_value(snapshot)?,
                    )
                    .await?;
                ews_sync_cursor_token("public-folder", cursor_id)
            }
        };

        Ok(sync_folder_items_response(
            &sync_state,
            changes,
            includes_last,
        ))
    }
}

pub(in crate::service) fn ews_sync_cursor_token(kind: &str, cursor_id: Uuid) -> String {
    format!("{EWS_SYNC_CURSOR_TOKEN_VERSION}.{kind}.{cursor_id}")
}

pub(in crate::service) fn parse_ews_sync_cursor_token(value: &str) -> Result<(String, Uuid)> {
    let parts = value.split('.').collect::<Vec<_>>();
    let ["lpe-ews-sync", "v1", kind, cursor_id] = parts.as_slice() else {
        bail!("SyncState is not a supported LPE synchronization cursor");
    };
    if !matches!(
        *kind,
        "hierarchy" | "mailbox" | "contacts" | "calendar" | "tasks" | "public-folder"
    ) {
        bail!("SyncState kind is invalid");
    }
    Ok((
        (*kind).to_string(),
        Uuid::parse_str(cursor_id).map_err(|_| anyhow!("SyncState cursor is invalid"))?,
    ))
}

fn cursor_snapshot<T>(cursor: &EwsSyncCursor, scope: &str) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    if cursor.scope != scope {
        bail!("SyncState does not belong to the requested folder");
    }
    serde_json::from_value(cursor.snapshot_json.clone())
        .map_err(|_| anyhow!("SyncState snapshot is invalid"))
}

fn requested_cursor_collection_id(
    request: &str,
    cursor: Option<&EwsSyncCursor>,
    kind: &str,
    default_id: &str,
) -> Result<String> {
    if let Some(collection_id) = requested_collection_id_in(request, "SyncFolderId") {
        return Ok(collection_id.to_string());
    }
    if let Some(cursor) = cursor {
        return cursor
            .scope
            .strip_prefix(&format!("{kind}:"))
            .filter(|collection_id| !collection_id.is_empty())
            .map(str::to_string)
            .ok_or_else(|| anyhow!("SyncState does not belong to the requested folder"));
    }
    Ok(requested_sync_collection_id(request, kind, default_id))
}

pub(in crate::service) async fn contact_change_keys<S>(
    store: &S,
    principal_account_id: Uuid,
    contacts: &[AccessibleContact],
) -> Result<HashMap<Uuid, String>>
where
    S: ExchangeStore + ?Sized,
{
    let mut versions = HashMap::new();
    for collection_id in contacts
        .iter()
        .map(|contact| contact.collection_id.as_str())
        .collect::<HashSet<_>>()
    {
        versions.extend(
            store
                .fetch_contact_sync_versions(principal_account_id, collection_id)
                .await?,
        );
    }
    contacts
        .iter()
        .map(|contact| {
            Ok((
                contact.id,
                contact_change_key(
                    contact,
                    change_key_version(&versions, contact.id, "contact")?,
                ),
            ))
        })
        .collect()
}

pub(in crate::service) async fn event_change_keys<S>(
    store: &S,
    principal_account_id: Uuid,
    events: &[AccessibleEvent],
) -> Result<HashMap<Uuid, String>>
where
    S: ExchangeStore + ?Sized,
{
    let mut versions = HashMap::new();
    for collection_id in events
        .iter()
        .map(|event| event.collection_id.as_str())
        .collect::<HashSet<_>>()
    {
        versions.extend(
            store
                .fetch_event_sync_versions(principal_account_id, collection_id)
                .await?,
        );
    }
    events
        .iter()
        .map(|event| {
            Ok((
                event.id,
                calendar_change_key(event, change_key_version(&versions, event.id, "calendar")?),
            ))
        })
        .collect()
}

pub(in crate::service) async fn task_change_keys<S>(
    store: &S,
    principal_account_id: Uuid,
    tasks: &[ClientTask],
) -> Result<HashMap<Uuid, String>>
where
    S: ExchangeStore + ?Sized,
{
    let mut versions = HashMap::new();
    for collection_id in tasks
        .iter()
        .map(|task| task.task_list_id.to_string())
        .collect::<HashSet<_>>()
    {
        versions.extend(
            store
                .fetch_task_sync_versions(principal_account_id, &collection_id)
                .await?,
        );
    }
    tasks
        .iter()
        .map(|task| {
            Ok((
                task.id,
                task_change_key(task, change_key_version(&versions, task.id, "task")?),
            ))
        })
        .collect()
}

pub(in crate::service) fn change_key_for<'a>(
    change_keys: &'a HashMap<Uuid, String>,
    item_id: Uuid,
    item_kind: &str,
) -> Result<&'a str> {
    change_keys
        .get(&item_id)
        .map(String::as_str)
        .ok_or_else(|| anyhow!("missing durable {item_kind} version for EWS ChangeKey"))
}

fn change_key_version<'a>(
    versions: &'a HashMap<Uuid, String>,
    item_id: Uuid,
    item_kind: &str,
) -> Result<&'a str> {
    versions
        .get(&item_id)
        .map(String::as_str)
        .ok_or_else(|| anyhow!("missing durable {item_kind} version for EWS ChangeKey"))
}

async fn mailbox_sync_snapshot<S>(
    store: &S,
    account_id: Uuid,
    mailbox_id: Uuid,
    after: Option<Uuid>,
) -> Result<(Vec<JmapEmail>, u64)>
where
    S: ExchangeStore + ?Sized,
{
    let mut ids = Vec::new();
    let mut position = 0;
    loop {
        let query = store
            .query_jmap_email_ids(
                account_id,
                Some(mailbox_id),
                None,
                position,
                MAILBOX_QUERY_LIMIT,
            )
            .await?;
        let returned = query.ids.len() as u64;
        ids.extend(query.ids);
        if returned == 0 || position.saturating_add(returned) >= query.total {
            break;
        }
        position += returned;
    }
    let replay = store
        .replay_ews_mailbox_item_sync(account_id, mailbox_id, 0, 1)
        .await?;
    let mut emails = store
        .fetch_jmap_emails(account_id, &ids)
        .await?
        .into_iter()
        .filter(|email| {
            email
                .mailbox_states
                .iter()
                .any(|item| item.mailbox_id == mailbox_id)
        })
        .filter(|email| after.is_none_or(|after| email.id > after))
        .collect::<Vec<_>>();
    emails.sort_unstable_by_key(|email| email.id);
    Ok((emails, replay.current_cursor.unwrap_or(0).max(0) as u64))
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct MailboxSyncState {
    mailbox_id: Uuid,
    cursor: u64,
    snapshot_after: Option<Uuid>,
}

impl MailboxSyncState {
    fn replay(mailbox_id: Uuid, cursor: u64) -> Self {
        Self {
            mailbox_id,
            cursor,
            snapshot_after: None,
        }
    }

    fn snapshot(mailbox_id: Uuid, cursor: u64, snapshot_after: Option<Uuid>) -> Self {
        Self {
            mailbox_id,
            cursor,
            snapshot_after,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PublicFolderSyncStateItem {
    id: Uuid,
    change_key: Option<String>,
    is_read: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PublicFolderSyncState {
    is_current_version: bool,
    items: Vec<PublicFolderSyncStateItem>,
}

impl Default for PublicFolderSyncState {
    fn default() -> Self {
        Self {
            is_current_version: true,
            items: Vec::new(),
        }
    }
}

fn validate_public_folder_sync_snapshot(state: &PublicFolderSyncState) -> Result<()> {
    if state.items.len() > EWS_SYNC_CURSOR_MAX_ITEMS
        || state.items.iter().any(|item| {
            item.id.to_string().len() + item.change_key.as_deref().unwrap_or_default().len() + 3
                > EWS_SYNC_CURSOR_MAX_ITEM_BYTES
        })
    {
        bail!("SyncState contains too many or oversized item entries");
    }
    Ok(())
}

fn public_folder_sync_state_items(
    sync_state: &str,
    _account_id: Uuid,
    collection_id: &str,
) -> Result<PublicFolderSyncState> {
    if sync_state == format!("public-folder:{collection_id}:0")
        || sync_state
            == format!("public-folder:{collection_id}:{LEGACY_PUBLIC_FOLDER_SYNC_STATE_VERSION}:0")
    {
        return Ok(PublicFolderSyncState::default());
    }
    bail!("nonempty legacy public-folder SyncState is not supported")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(in crate::service) struct SyncStateItem {
    pub(in crate::service) id: Uuid,
    pub(in crate::service) change_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(in crate::service) struct CollaborationSyncState {
    pub(in crate::service) is_current_version: bool,
    pub(in crate::service) items: Vec<SyncStateItem>,
}

impl Default for CollaborationSyncState {
    fn default() -> Self {
        Self {
            is_current_version: true,
            items: Vec::new(),
        }
    }
}

fn validate_collaboration_sync_snapshot(state: &CollaborationSyncState) -> Result<()> {
    if state.items.len() > EWS_SYNC_CURSOR_MAX_ITEMS
        || state.items.iter().any(|item| {
            item.id.to_string().len() + item.change_key.as_deref().unwrap_or_default().len() + 1
                > EWS_SYNC_CURSOR_MAX_ITEM_BYTES
        })
    {
        bail!("SyncState contains too many or oversized item entries");
    }
    Ok(())
}

pub(in crate::service) fn collaboration_sync_state_items(
    sync_state: &str,
    _account_id: Uuid,
    kind: &str,
    collection_id: &str,
) -> Result<CollaborationSyncState> {
    if sync_state == format!("{kind}:{collection_id}:0")
        || sync_state
            == format!("{kind}:{collection_id}:{LEGACY_COLLABORATION_SYNC_STATE_VERSION}:0")
    {
        return Ok(CollaborationSyncState::default());
    }
    bail!("nonempty legacy collaboration SyncState is not supported")
}

pub(in crate::service) fn collaboration_sync_state_collection_id<'a>(
    sync_state: &'a str,
    kind: &str,
) -> Option<&'a str> {
    sync_state
        .strip_prefix(&format!("{kind}:"))?
        .split(':')
        .next()
}

pub(in crate::service) fn requested_sync_collection_id(
    request: &str,
    kind: &str,
    default_id: &str,
) -> String {
    if let Some(collection_id) = requested_collection_id_in(request, "SyncFolderId") {
        return collection_id.to_string();
    }
    if let Some(sync_state) = requested_sync_state(request) {
        if let Some(collection_id) = collaboration_sync_state_collection_id(&sync_state, kind) {
            return collection_id.to_string();
        }
    }
    default_id.to_string()
}

pub(in crate::service) fn requested_sync_state(request: &str) -> Option<String> {
    element_text(request, "SyncState").filter(|value| !value.trim().is_empty())
}

fn append_sync_change_page(
    changes: &mut String,
    pending_changes: Vec<(Uuid, Option<String>, String)>,
    max_changes: usize,
    next_by_id: &mut HashMap<Uuid, Option<String>>,
) -> bool {
    let includes_last = pending_changes.len() <= max_changes;
    for (id, change_key, change) in pending_changes.into_iter().take(max_changes) {
        changes.push_str(&change);
        if let Some(change_key) = change_key {
            next_by_id.insert(id, Some(change_key));
        } else {
            next_by_id.remove(&id);
        }
    }
    includes_last
}

fn requested_max_changes(request: &str) -> Result<usize> {
    match element_text(request, "MaxChangesReturned") {
        None => Ok(MAILBOX_QUERY_LIMIT as usize),
        Some(value) => {
            let value = value
                .parse::<usize>()
                .map_err(|_| anyhow!("MaxChangesReturned must be a positive integer"))?;
            if value == 0 {
                bail!("MaxChangesReturned must be a positive integer");
            }
            Ok(value.min(MAILBOX_QUERY_LIMIT as usize))
        }
    }
}

pub(in crate::service) fn mailbox_sync_state_folder_id(sync_state: &str) -> Option<Uuid> {
    let rest = sync_state.strip_prefix("mailbox:")?;
    let folder_id = rest.split_once(':')?.0;
    Uuid::parse_str(folder_id).ok()
}

pub(in crate::service) fn sync_state_items_by_id(
    items: &[SyncStateItem],
) -> HashMap<Uuid, Option<String>> {
    items
        .iter()
        .map(|item| (item.id, item.change_key.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        collaboration_sync_state_items, ews_sync_cursor_token, parse_ews_sync_cursor_token,
        public_folder_sync_state_items,
    };
    use uuid::Uuid;

    fn uuid(value: &str) -> Uuid {
        Uuid::parse_str(value).unwrap()
    }

    #[test]
    fn opaque_sync_cursor_token_is_bounded_to_a_known_kind() {
        let cursor_id = uuid("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
        let state = ews_sync_cursor_token("contacts", cursor_id);
        assert_eq!(
            parse_ews_sync_cursor_token(&state).unwrap(),
            ("contacts".to_string(), cursor_id)
        );
        assert!(parse_ews_sync_cursor_token(
            "lpe-ews-sync.v1.unknown.bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
        )
        .is_err());
    }

    #[test]
    fn legacy_empty_sync_states_reset_but_nonempty_states_are_rejected() {
        let account_id = uuid("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
        assert!(collaboration_sync_state_items(
            "contacts:default:v2:0",
            account_id,
            "contacts",
            "default"
        )
        .is_ok());
        assert!(collaboration_sync_state_items(
            "contacts:default:v2:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb=ck-contact",
            account_id,
            "contacts",
            "default",
        )
        .is_err());
        assert!(public_folder_sync_state_items(
            "public-folder:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:v3:cccccccc-cccc-cccc-cccc-cccccccccccc=ck-public|1",
            account_id,
            "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        )
        .is_err());
    }
}
