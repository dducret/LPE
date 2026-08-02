use super::super::*;

const COLLABORATION_SYNC_STATE_VERSION: &str = "v2";
const PUBLIC_FOLDER_SYNC_STATE_VERSION: &str = "v3";

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
                let previous_state = requested_sync_state(request)
                    .map(|state| collaboration_sync_state_items(&state, "contacts", &collection_id))
                    .unwrap_or_default();
                let previous_by_id = sync_state_items_by_id(&previous_state.items);
                for contact in &contacts {
                    let current_change_key = change_key_for(&change_keys, contact.id, "contact")?;
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
                        let change_key = item.change_key.as_deref().unwrap_or("deleted");
                        changes.push_str("<t:Delete>");
                        changes.push_str(&format!(
                            "<t:ItemId Id=\"contact:{contact_id}\" ChangeKey=\"{}\"/>",
                            escape_xml(change_key),
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
                let previous_state = requested_sync_state(request)
                    .map(|state| collaboration_sync_state_items(&state, "calendar", &collection_id))
                    .unwrap_or_default();
                let previous_by_id = sync_state_items_by_id(&previous_state.items);
                for event in &events {
                    let current_change_key = change_key_for(&change_keys, event.id, "calendar")?;
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
                        let change_key = item.change_key.as_deref().unwrap_or("deleted");
                        changes.push_str("<t:Delete>");
                        changes.push_str(&format!(
                            "<t:ItemId Id=\"event:{event_id}\" ChangeKey=\"{}\"/>",
                            escape_xml(change_key),
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
                let previous_state = requested_sync_state(request)
                    .map(|state| collaboration_sync_state_items(&state, "tasks", &collection_id))
                    .unwrap_or_default();
                let previous_by_id = sync_state_items_by_id(&previous_state.items);
                for task in &tasks {
                    let current_change_key = change_key_for(&change_keys, task.id, "task")?;
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
                        let change_key = item.change_key.as_deref().unwrap_or("deleted");
                        changes.push_str("<t:Delete>");
                        changes.push_str(&format!(
                            "<t:ItemId Id=\"task:{task_id}\" ChangeKey=\"{}\"/>",
                            escape_xml(change_key),
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
                    .filter(|email| {
                        email
                            .mailbox_states
                            .iter()
                            .any(|state| state.mailbox_id == mailbox_id)
                    })
                    .collect::<Vec<_>>();
                let collection_id = mailbox_id.to_string();
                let current_items = emails
                    .iter()
                    .map(|email| (email.id, message_change_key(email)))
                    .collect::<Vec<_>>();
                let current_set = current_items
                    .iter()
                    .map(|(id, _)| *id)
                    .collect::<HashSet<_>>();
                let previous_state = requested_sync_state(request)
                    .map(|state| collaboration_sync_state_items(&state, "mailbox", &collection_id))
                    .unwrap_or_default();
                let previous_by_id = sync_state_items_by_id(&previous_state.items);
                for email in &emails {
                    let current_change_key = message_change_key(email);
                    match previous_by_id.get(&email.id) {
                        None => {
                            changes.push_str("<t:Create>");
                            changes.push_str(&message_summary_xml_for_mailbox(email, mailbox_id));
                            changes.push_str("</t:Create>");
                        }
                        Some(None) => {
                            changes.push_str("<t:Update>");
                            changes.push_str(&message_summary_xml_for_mailbox(email, mailbox_id));
                            changes.push_str("</t:Update>");
                        }
                        Some(Some(previous_change_key))
                            if !previous_state.is_current_version
                                || previous_change_key != &current_change_key =>
                        {
                            changes.push_str("<t:Update>");
                            changes.push_str(&message_summary_xml_for_mailbox(email, mailbox_id));
                            changes.push_str("</t:Update>");
                        }
                        _ => {}
                    }
                }
                for item in previous_state.items {
                    let message_id = item.id;
                    if !current_set.contains(&message_id) {
                        let change_key = item.change_key.as_deref().unwrap_or("deleted");
                        changes.push_str("<t:Delete>");
                        changes.push_str(&format!(
                            "<t:ItemId Id=\"message:{message_id}\" ChangeKey=\"{}\"/>",
                            escape_xml(change_key),
                        ));
                        changes.push_str("</t:Delete>");
                    }
                }
                collaboration_sync_state("mailbox", &collection_id, &current_items)
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
                    .map(|item| (item.id, public_folder_item_change_key(item), item.is_read))
                    .collect::<Vec<_>>();
                let current_set = current_items
                    .iter()
                    .map(|(id, _, _)| *id)
                    .collect::<HashSet<_>>();
                let collection_id = folder_id.to_string();
                let previous_state = requested_sync_state(request)
                    .map(|state| public_folder_sync_state_items(&state, &collection_id))
                    .unwrap_or_default();
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
                public_folder_sync_state(&collection_id, &current_items)
            }
        };

        Ok(sync_folder_items_response(&sync_state, changes))
    }
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
struct PublicFolderSyncStateItem {
    id: Uuid,
    change_key: Option<String>,
    is_read: Option<bool>,
}

#[derive(Debug, Clone)]
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

fn public_folder_sync_state(collection_id: &str, items: &[(Uuid, String, bool)]) -> String {
    let item_list = items
        .iter()
        .map(|(id, change_key, is_read)| format!("{id}={change_key}|{}", u8::from(*is_read)))
        .collect::<Vec<_>>()
        .join(",");
    if item_list.is_empty() {
        format!("public-folder:{collection_id}:{PUBLIC_FOLDER_SYNC_STATE_VERSION}:0")
    } else {
        format!("public-folder:{collection_id}:{PUBLIC_FOLDER_SYNC_STATE_VERSION}:{item_list}")
    }
}

fn public_folder_sync_state_items(sync_state: &str, collection_id: &str) -> PublicFolderSyncState {
    let prefix = format!("public-folder:{collection_id}:");
    let Some(values) = sync_state.strip_prefix(&prefix) else {
        return PublicFolderSyncState::default();
    };
    let Some(values) = values.strip_prefix(&format!("{PUBLIC_FOLDER_SYNC_STATE_VERSION}:")) else {
        let legacy = collaboration_sync_state_items(sync_state, "public-folder", collection_id);
        return PublicFolderSyncState {
            is_current_version: false,
            items: legacy
                .items
                .into_iter()
                .map(|item| PublicFolderSyncStateItem {
                    id: item.id,
                    change_key: item.change_key,
                    is_read: None,
                })
                .collect(),
        };
    };
    let items = values
        .split(',')
        .filter(|value| !value.is_empty() && *value != "0")
        .filter_map(|value| {
            let (id, value) = value.split_once('=')?;
            let (change_key, is_read) = value.rsplit_once('|')?;
            let is_read = match is_read {
                "0" => false,
                "1" => true,
                _ => return None,
            };
            Uuid::parse_str(id)
                .ok()
                .map(|id| PublicFolderSyncStateItem {
                    id,
                    change_key: Some(change_key.to_string()),
                    is_read: Some(is_read),
                })
        })
        .collect();
    PublicFolderSyncState {
        is_current_version: true,
        items,
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
