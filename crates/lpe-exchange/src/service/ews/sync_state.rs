use super::super::*;

const COLLABORATION_SYNC_STATE_VERSION: &str = "v2";

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
        let sync_state = match requested_folder_kind(request).unwrap_or(FolderKind::Contacts) {
            FolderKind::Root => "root:0".to_string(),
            FolderKind::Contacts => {
                let collection_id =
                    requested_sync_collection_id(request, "contacts", CONTACTS_FOLDER_ID);
                let contacts = self
                    .store
                    .fetch_accessible_contacts_in_collection(principal.account_id, &collection_id)
                    .await?;
                let sync_versions = sync_version_by_id(
                    self.store
                        .fetch_contact_sync_versions(principal.account_id, &collection_id)
                        .await?,
                );
                let current_items = contacts
                    .iter()
                    .map(|contact| {
                        (
                            contact.id,
                            contact_change_key(
                                contact,
                                sync_versions.get(&contact.id).map(String::as_str),
                            ),
                        )
                    })
                    .collect::<Vec<_>>();
                let current_set = current_items
                    .iter()
                    .map(|(id, _)| *id)
                    .collect::<HashSet<_>>();
                let previous_state = requested_sync_state(request)
                    .map(|state| collaboration_sync_state_items(&state, "contacts", &collection_id))
                    .unwrap_or_default();
                let previous_by_id = sync_state_items_by_id(&previous_state.items);
                for contact in &contacts {
                    let current_change_key = contact_change_key(
                        contact,
                        sync_versions.get(&contact.id).map(String::as_str),
                    );
                    match previous_by_id.get(&contact.id) {
                        None => {
                            changes.push_str("<t:Create>");
                            changes.push_str(&contact_item_xml_with_change_key(
                                contact,
                                &current_change_key,
                            ));
                            changes.push_str("</t:Create>");
                        }
                        Some(None) => {
                            changes.push_str("<t:Update>");
                            changes.push_str(&contact_item_xml_with_change_key(
                                contact,
                                &current_change_key,
                            ));
                            changes.push_str("</t:Update>");
                        }
                        Some(Some(previous_change_key))
                            if !previous_state.is_current_version
                                || previous_change_key != &current_change_key =>
                        {
                            changes.push_str("<t:Update>");
                            changes.push_str(&contact_item_xml_with_change_key(
                                contact,
                                &current_change_key,
                            ));
                            changes.push_str("</t:Update>");
                        }
                        _ => {}
                    }
                }
                for item in previous_state.items {
                    let contact_id = item.id;
                    if !current_set.contains(&contact_id) {
                        changes.push_str("<t:Delete>");
                        changes.push_str(&format!(
                            "<t:ItemId Id=\"contact:{contact_id}\" ChangeKey=\"deleted\"/>"
                        ));
                        changes.push_str("</t:Delete>");
                    }
                }
                collaboration_sync_state("contacts", &collection_id, &current_items)
            }
            FolderKind::Calendar => {
                let collection_id =
                    requested_sync_collection_id(request, "calendar", CALENDAR_FOLDER_ID);
                let events = self
                    .store
                    .fetch_accessible_events_in_collection(principal.account_id, &collection_id)
                    .await?;
                let sync_versions = sync_version_by_id(
                    self.store
                        .fetch_event_sync_versions(principal.account_id, &collection_id)
                        .await?,
                );
                let current_items = events
                    .iter()
                    .map(|event| {
                        (
                            event.id,
                            calendar_change_key(
                                event,
                                sync_versions.get(&event.id).map(String::as_str),
                            ),
                        )
                    })
                    .collect::<Vec<_>>();
                let current_set = current_items
                    .iter()
                    .map(|(id, _)| *id)
                    .collect::<HashSet<_>>();
                let previous_state = requested_sync_state(request)
                    .map(|state| collaboration_sync_state_items(&state, "calendar", &collection_id))
                    .unwrap_or_default();
                let previous_by_id = sync_state_items_by_id(&previous_state.items);
                for event in &events {
                    let current_change_key = calendar_change_key(
                        event,
                        sync_versions.get(&event.id).map(String::as_str),
                    );
                    match previous_by_id.get(&event.id) {
                        None => {
                            changes.push_str("<t:Create>");
                            changes.push_str(&calendar_item_xml_with_change_key(
                                event,
                                &current_change_key,
                            ));
                            changes.push_str("</t:Create>");
                        }
                        Some(None) => {
                            changes.push_str("<t:Update>");
                            changes.push_str(&calendar_item_xml_with_change_key(
                                event,
                                &current_change_key,
                            ));
                            changes.push_str("</t:Update>");
                        }
                        Some(Some(previous_change_key))
                            if !previous_state.is_current_version
                                || previous_change_key != &current_change_key =>
                        {
                            changes.push_str("<t:Update>");
                            changes.push_str(&calendar_item_xml_with_change_key(
                                event,
                                &current_change_key,
                            ));
                            changes.push_str("</t:Update>");
                        }
                        _ => {}
                    }
                }
                for item in previous_state.items {
                    let event_id = item.id;
                    if !current_set.contains(&event_id) {
                        changes.push_str("<t:Delete>");
                        changes.push_str(&format!(
                            "<t:ItemId Id=\"event:{event_id}\" ChangeKey=\"deleted\"/>"
                        ));
                        changes.push_str("</t:Delete>");
                    }
                }
                collaboration_sync_state("calendar", &collection_id, &current_items)
            }
            FolderKind::Tasks => {
                let collection_id = requested_sync_collection_id(request, "tasks", TASKS_FOLDER_ID);
                let tasks = self
                    .store
                    .fetch_accessible_tasks_in_collection(principal.account_id, &collection_id)
                    .await?;
                let sync_versions = sync_version_by_id(
                    self.store
                        .fetch_task_sync_versions(principal.account_id, &collection_id)
                        .await?,
                );
                let current_items = tasks
                    .iter()
                    .map(|task| {
                        (
                            task.id,
                            task_change_key(task, sync_versions.get(&task.id).map(String::as_str)),
                        )
                    })
                    .collect::<Vec<_>>();
                let current_set = current_items
                    .iter()
                    .map(|(id, _)| *id)
                    .collect::<HashSet<_>>();
                let previous_state = requested_sync_state(request)
                    .map(|state| collaboration_sync_state_items(&state, "tasks", &collection_id))
                    .unwrap_or_default();
                let previous_by_id = sync_state_items_by_id(&previous_state.items);
                for task in &tasks {
                    let current_change_key =
                        task_change_key(task, sync_versions.get(&task.id).map(String::as_str));
                    match previous_by_id.get(&task.id) {
                        None => {
                            changes.push_str("<t:Create>");
                            changes.push_str(&task_item_xml_with_change_key(
                                task,
                                &current_change_key,
                            ));
                            changes.push_str("</t:Create>");
                        }
                        Some(None) => {
                            changes.push_str("<t:Update>");
                            changes.push_str(&task_item_xml_with_change_key(
                                task,
                                &current_change_key,
                            ));
                            changes.push_str("</t:Update>");
                        }
                        Some(Some(previous_change_key))
                            if !previous_state.is_current_version
                                || previous_change_key != &current_change_key =>
                        {
                            changes.push_str("<t:Update>");
                            changes.push_str(&task_item_xml_with_change_key(
                                task,
                                &current_change_key,
                            ));
                            changes.push_str("</t:Update>");
                        }
                        _ => {}
                    }
                }
                for item in previous_state.items {
                    let task_id = item.id;
                    if !current_set.contains(&task_id) {
                        changes.push_str("<t:Delete>");
                        changes.push_str(&format!(
                            "<t:ItemId Id=\"task:{task_id}\" ChangeKey=\"deleted\"/>"
                        ));
                        changes.push_str("</t:Delete>");
                    }
                }
                collaboration_sync_state("tasks", &collection_id, &current_items)
            }
            FolderKind::Mailbox => {
                let Some(mailbox_id) = self
                    .requested_mailbox_folder_ids(principal, request)
                    .await?
                    .into_iter()
                    .next()
                else {
                    return Ok(sync_folder_items_response("mailbox:0", String::new()));
                };
                let query = self
                    .store
                    .query_jmap_email_ids(
                        principal.account_id,
                        Some(mailbox_id),
                        None,
                        0,
                        MAILBOX_QUERY_LIMIT,
                    )
                    .await?;
                let emails = self
                    .store
                    .fetch_jmap_emails(principal.account_id, &query.ids)
                    .await?
                    .into_iter()
                    .filter(|email| email.mailbox_id == mailbox_id)
                    .collect::<Vec<_>>();
                let current_ids = emails.iter().map(|email| email.id).collect::<Vec<_>>();
                let current_set = current_ids.iter().copied().collect::<HashSet<_>>();
                let previous_ids = requested_sync_state(request)
                    .map(|state| mailbox_sync_state_ids(&state, mailbox_id))
                    .unwrap_or_default();
                let previous_set = previous_ids.iter().copied().collect::<HashSet<_>>();

                for email in &emails {
                    if !previous_set.contains(&email.id) {
                        changes.push_str("<t:Create>");
                        changes.push_str(&message_summary_xml(email));
                        changes.push_str("</t:Create>");
                    }
                }
                for message_id in previous_ids {
                    if !current_set.contains(&message_id) {
                        changes.push_str("<t:Delete>");
                        changes.push_str(&format!(
                            "<t:ItemId Id=\"message:{message_id}\" ChangeKey=\"deleted\"/>"
                        ));
                        changes.push_str("</t:Delete>");
                    }
                }
                mailbox_sync_state(mailbox_id, &current_ids)
            }
            FolderKind::PublicFolders => {
                let Some(folder_id) = requested_public_folder_ids(request).into_iter().next()
                else {
                    return Ok(sync_folder_items_response("public-folder:0", String::new()));
                };
                let items = self
                    .store
                    .fetch_public_folder_items(principal.account_id, folder_id)
                    .await?;
                let current_items = items
                    .iter()
                    .map(|item| (item.id, public_folder_item_change_key(item)))
                    .collect::<Vec<_>>();
                for item in &items {
                    changes.push_str("<t:Create>");
                    changes.push_str(&public_folder_item_summary_xml(item));
                    changes.push_str("</t:Create>");
                }
                collaboration_sync_state("public-folder", &folder_id.to_string(), &current_items)
            }
        };

        Ok(sync_folder_items_response(&sync_state, changes))
    }
}

pub(in crate::service) fn collaboration_sync_state(
    kind: &str,
    collection_id: &str,
    items: &[(Uuid, String)],
) -> String {
    let item_list = items
        .iter()
        .map(|(id, change_key)| format!("{id}={change_key}"))
        .collect::<Vec<_>>()
        .join(",");
    if item_list.is_empty() {
        format!("{kind}:{collection_id}:{COLLABORATION_SYNC_STATE_VERSION}:0")
    } else {
        format!("{kind}:{collection_id}:{COLLABORATION_SYNC_STATE_VERSION}:{item_list}")
    }
}

#[derive(Debug, Clone)]
pub(in crate::service) struct SyncStateItem {
    pub(in crate::service) id: Uuid,
    pub(in crate::service) change_key: Option<String>,
}

#[derive(Debug, Clone)]
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

pub(in crate::service) fn collaboration_sync_state_items(
    sync_state: &str,
    kind: &str,
    collection_id: &str,
) -> CollaborationSyncState {
    let prefix = format!("{kind}:{collection_id}:");
    let Some(values) = sync_state.strip_prefix(&prefix) else {
        return CollaborationSyncState::default();
    };
    let (is_current_version, values) = if let Some(values) =
        values.strip_prefix(&format!("{COLLABORATION_SYNC_STATE_VERSION}:"))
    {
        (true, values)
    } else {
        (false, values)
    };
    let items = values
        .split(',')
        .filter(|value| !value.is_empty() && *value != "0")
        .filter_map(|value| {
            if let Some((id, change_key)) = value.split_once('=') {
                return Uuid::parse_str(id).ok().map(|id| SyncStateItem {
                    id,
                    change_key: Some(change_key.to_string()),
                });
            }
            Uuid::parse_str(value).ok().map(|id| SyncStateItem {
                id,
                change_key: None,
            })
        })
        .collect();
    CollaborationSyncState {
        is_current_version,
        items,
    }
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

pub(in crate::service) fn mailbox_sync_state(mailbox_id: Uuid, message_ids: &[Uuid]) -> String {
    format!(
        "mailbox:{mailbox_id}:{}",
        message_ids
            .iter()
            .map(Uuid::to_string)
            .collect::<Vec<_>>()
            .join(",")
    )
}

pub(in crate::service) fn mailbox_sync_state_ids(sync_state: &str, mailbox_id: Uuid) -> Vec<Uuid> {
    let prefix = format!("mailbox:{mailbox_id}:");
    sync_state
        .strip_prefix(&prefix)
        .unwrap_or_default()
        .split(',')
        .filter(|value| !value.is_empty() && *value != "0")
        .filter_map(|value| Uuid::parse_str(value).ok())
        .collect()
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

pub(in crate::service) fn sync_version_by_id(items: Vec<(Uuid, String)>) -> HashMap<Uuid, String> {
    items.into_iter().collect()
}
