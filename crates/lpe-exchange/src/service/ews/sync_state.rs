use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use sha2::{Digest, Sha256};

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
        let mut includes_last = true;
        let max_changes = requested_max_changes(request)?;
        let sync_state = match if requested_sync_state(request)
            .as_deref()
            .is_some_and(|state| state.starts_with("lpe-sync."))
        {
            FolderKind::Mailbox
        } else {
            requested_folder_kind(request).unwrap_or(FolderKind::Contacts)
        } {
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
                collaboration_sync_state(
                    "contacts",
                    &collection_id,
                    &next_by_id
                        .into_iter()
                        .filter_map(|(id, change_key)| {
                            change_key.map(|change_key| (id, change_key))
                        })
                        .collect::<Vec<_>>(),
                )
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
                collaboration_sync_state(
                    "calendar",
                    &collection_id,
                    &next_by_id
                        .into_iter()
                        .filter_map(|(id, change_key)| {
                            change_key.map(|change_key| (id, change_key))
                        })
                        .collect::<Vec<_>>(),
                )
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
                collaboration_sync_state(
                    "tasks",
                    &collection_id,
                    &next_by_id
                        .into_iter()
                        .filter_map(|(id, change_key)| {
                            change_key.map(|change_key| (id, change_key))
                        })
                        .collect::<Vec<_>>(),
                )
            }
            FolderKind::Mailbox => {
                if element_contents(request, "SyncState").len() > 1 {
                    bail!("SyncFolderItems accepts at most one SyncState");
                }
                let state = requested_sync_state(request)
                    .map(|state| parse_mailbox_sync_state(&state, principal.account_id))
                    .transpose()?;
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
                        mailbox_sync_state(principal.account_id, &next)
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
                                "created" | "updated" => {
                                    let Some(email) = emails.get(&event.message_id) else { continue };
                                    changes.push_str(if event.change_kind == "created" { "<t:Create>" } else { "<t:Update>" });
                                    changes.push_str(&message_summary_xml_for_mailbox(email, mailbox_id));
                                    changes.push_str(if event.change_kind == "created" { "</t:Create>" } else { "</t:Update>" });
                                }
                                "destroyed" | "expunged" => changes.push_str(&format!(
                                    "<t:Delete><t:ItemId Id=\"message:{}\" ChangeKey=\"{}\"/></t:Delete>",
                                    event.message_id,
                                    escape_xml(&versioned_change_key("message", &event.message_id.to_string(), &event.modseq.to_string())),
                                )),
                                _ => {}
                            }
                        }
                        mailbox_sync_state(
                            principal.account_id,
                            &MailboxSyncState::replay(mailbox_id, replay.next_cursor.max(0) as u64),
                        )
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
                        mailbox_sync_state(principal.account_id, &next)
                    }
                }
            }
            FolderKind::PublicFolders => {
                let Some(folder_id) = requested_public_folder_ids(request).into_iter().next()
                else {
                    return Ok(sync_folder_items_response(
                        "public-folder:0",
                        String::new(),
                        true,
                    ));
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

        Ok(sync_folder_items_response(
            &sync_state,
            changes,
            includes_last,
        ))
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

#[derive(Debug, Default)]
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

fn mailbox_sync_state(account_id: Uuid, state: &MailboxSyncState) -> String {
    let (mode, after) = match state.snapshot_after {
        Some(after) => ("snapshot", after.to_string()),
        None => ("replay", "-".to_string()),
    };
    let payload = URL_SAFE_NO_PAD.encode(format!(
        "{account_id}|{}|{mode}|{}|{after}",
        state.mailbox_id, state.cursor,
    ));
    format!(
        "lpe-sync.v2.{payload}.{}",
        mailbox_sync_state_digest(&payload)
    )
}

fn parse_mailbox_sync_state(state: &str, account_id: Uuid) -> Result<MailboxSyncState> {
    let parts = state.split('.').collect::<Vec<_>>();
    let ["lpe-sync", "v2", payload, signature] = parts.as_slice() else {
        bail!("SyncState is not a supported mailbox synchronization token");
    };
    if mailbox_sync_state_digest(payload) != *signature {
        bail!("SyncState integrity validation failed");
    }
    let decoded = URL_SAFE_NO_PAD
        .decode(payload.as_bytes())
        .map_err(|_| anyhow!("SyncState payload is invalid"))?;
    let decoded =
        String::from_utf8(decoded).map_err(|_| anyhow!("SyncState payload is invalid"))?;
    let mut values = decoded.split('|');
    let token_account_id = values
        .next()
        .ok_or_else(|| anyhow!("SyncState account binding is missing"))
        .and_then(|value| {
            Uuid::parse_str(value).map_err(|_| anyhow!("SyncState account binding is invalid"))
        })?;
    if token_account_id != account_id {
        bail!("SyncState belongs to another authenticated account");
    }
    let mailbox_id = values
        .next()
        .ok_or_else(|| anyhow!("SyncState mailbox binding is missing"))
        .and_then(|value| {
            Uuid::parse_str(value).map_err(|_| anyhow!("SyncState mailbox binding is invalid"))
        })?;
    let mode = values
        .next()
        .ok_or_else(|| anyhow!("SyncState mode is missing"))?;
    let cursor = values
        .next()
        .ok_or_else(|| anyhow!("SyncState cursor is missing"))?
        .parse::<u64>()
        .map_err(|_| anyhow!("SyncState cursor is invalid"))?;
    let snapshot_after = match (
        mode,
        values
            .next()
            .ok_or_else(|| anyhow!("SyncState snapshot cursor is missing"))?,
    ) {
        ("replay", "-") => None,
        ("snapshot", value) => Some(
            Uuid::parse_str(value).map_err(|_| anyhow!("SyncState snapshot cursor is invalid"))?,
        ),
        _ => bail!("SyncState mode is invalid"),
    };
    if values.next().is_some() {
        bail!("SyncState payload is malformed");
    }
    Ok(MailboxSyncState {
        mailbox_id,
        cursor,
        snapshot_after,
    })
}

fn mailbox_sync_state_digest(payload: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"lpe-ews-mailbox-sync-v1\0");
    digest.update(payload.as_bytes());
    URL_SAFE_NO_PAD.encode(digest.finalize())
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
