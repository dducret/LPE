use super::properties::*;
use super::rop::*;
use super::session::*;
use super::sync::{CALENDAR_FOLDER_ID, INBOX_FOLDER_ID, ROOT_FOLDER_ID};
use super::tables::*;
use super::*;
use crate::mapi_store;
use crate::store::{
    MapiContentTableQuery, MapiContentTableSort, MapiContentTableSortField,
    MapiIdentityLookupRecord,
};
use anyhow::Context;
use lpe_storage::ReminderQuery;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::mapi) struct MapiAccessPlan {
    pub(in crate::mapi) requires_full_snapshot: bool,
    pub(in crate::mapi) object_ids: Vec<u64>,
    pub(in crate::mapi) content_queries: Vec<MapiContentAccessQuery>,
}

impl MapiAccessPlan {
    fn full() -> Self {
        Self {
            requires_full_snapshot: true,
            object_ids: Vec::new(),
            content_queries: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::mapi) struct MapiContentAccessQuery {
    pub(in crate::mapi) folder_id: u64,
    pub(in crate::mapi) view_signature: u64,
    pub(in crate::mapi) offset: usize,
    pub(in crate::mapi) limit: usize,
    pub(in crate::mapi) sort_orders: Vec<MapiContentTableSort>,
}

pub(in crate::mapi) fn plan_mapi_store_access(
    session: &MapiSession,
    rop_buffer: &[u8],
) -> MapiAccessPlan {
    let Some((requests, handle_table)) = split_rop_buffer(rop_buffer) else {
        return MapiAccessPlan::full();
    };
    let Ok(mut handle_slots) = read_handle_table(handle_table) else {
        return MapiAccessPlan::full();
    };
    let mut plan = MapiAccessPlan {
        requires_full_snapshot: false,
        object_ids: Vec::new(),
        content_queries: Vec::new(),
    };
    for object in session.handles.values() {
        add_object_ids_for_handle(&mut plan, object);
    }
    let mut simulated_handles = session.handles.clone();
    let mut simulated_next_handle = session.next_handle;

    let mut cursor = Cursor::new(requests);
    while cursor.remaining() > 0 {
        let Ok(request) = read_rop_request(&mut cursor) else {
            return MapiAccessPlan::full();
        };
        if rop_requires_full_snapshot(request.rop_id) {
            return MapiAccessPlan::full();
        }
        if let Some(folder_id) = request.folder_id() {
            push_unique(&mut plan.object_ids, folder_id);
        }
        if let Some(message_id) = request.message_id() {
            push_unique(&mut plan.object_ids, message_id);
        }
        if let Some(message_id) = request.status_message_id() {
            push_unique(&mut plan.object_ids, message_id);
        }
        if let Some(object_id) = request.long_term_source_object_id() {
            push_unique(&mut plan.object_ids, object_id);
        }
        for message_id in request
            .message_ids()
            .into_iter()
            .chain(request.move_copy_message_ids())
            .chain(request.fast_transfer_message_ids())
            .chain(request.import_delete_message_ids())
            .chain(
                request
                    .import_read_state_changes()
                    .into_iter()
                    .map(|(message_id, _)| message_id),
            )
        {
            push_unique(&mut plan.object_ids, message_id);
        }
        if let Some(message_id) = request.import_message_id() {
            push_unique(&mut plan.object_ids, message_id);
        }
        if let Some((message_id, target_folder_id)) = request.import_move() {
            push_unique(&mut plan.object_ids, message_id);
            push_unique(&mut plan.object_ids, target_folder_id);
        }
        if let Some(folder_id) = request.delete_folder_id() {
            push_unique(&mut plan.object_ids, folder_id);
        }
        if let Some(target_handle) = request.move_copy_target_handle(&handle_slots) {
            if let Some(object) = session.handles.get(&target_handle) {
                add_object_ids_for_handle(&mut plan, object);
            }
        }
        if let Some(handle) = input_handle(&handle_slots, &request) {
            if let Some(object) = simulated_handles.get(&handle) {
                add_object_ids_for_handle(&mut plan, object);
            }
        }
        simulate_table_access(
            &mut plan,
            &mut simulated_handles,
            &mut simulated_next_handle,
            &mut handle_slots,
            &request,
        );
        if plan.requires_full_snapshot {
            return plan;
        }
    }
    plan
}

pub(in crate::mapi) fn hierarchy_sync_selective_fallback_plan(
    rop_buffer: &[u8],
) -> Option<MapiAccessPlan> {
    let (requests, _) = split_rop_buffer(rop_buffer)?;
    let mut saw_hierarchy_configure = false;
    let mut cursor = Cursor::new(requests);
    while cursor.remaining() > 0 {
        let request = read_rop_request(&mut cursor).ok()?;
        match request.rop_id {
            0x70 if request.sync_type() == 0x02 => saw_hierarchy_configure = true,
            0x4E if saw_hierarchy_configure => {}
            rop_id if rop_requires_full_snapshot(rop_id) => return None,
            _ => {}
        }
    }

    saw_hierarchy_configure.then_some(MapiAccessPlan {
        requires_full_snapshot: false,
        object_ids: Vec::new(),
        content_queries: Vec::new(),
    })
}

pub(in crate::mapi) async fn load_mapi_store_for_access_plan<S>(
    store: &S,
    account_id: Uuid,
    plan: &MapiAccessPlan,
    full_message_limit: u64,
) -> Result<MapiMailStoreSnapshot>
where
    S: ExchangeStore,
{
    if plan.requires_full_snapshot {
        log_mapi_store_full_snapshot(account_id, plan);
        return store
            .load_mapi_mail_store(account_id, full_message_limit)
            .await
            .context("load full MAPI mail store snapshot");
    }

    log_mapi_store_load_step(account_id, plan, "ensure system mailboxes", 0);
    let mut mailboxes = store
        .ensure_jmap_system_mailboxes(account_id)
        .await
        .context("ensure MAPI system mailboxes")?;
    let mailbox_requests = mapi_identity_requests_for_mailboxes(&mailboxes);
    log_mapi_store_load_step(
        account_id,
        plan,
        "allocate mailbox identities",
        mailbox_requests.len(),
    );
    for identity in store
        .fetch_or_allocate_mapi_identities(account_id, &mailbox_requests)
        .await
        .context("allocate MAPI mailbox identities")?
    {
        crate::mapi::identity::remember_mapi_identity(identity.canonical_id, identity.object_id);
    }

    log_mapi_store_load_step(
        account_id,
        plan,
        "fetch requested identities",
        plan.object_ids.len(),
    );
    let identities = store
        .fetch_mapi_identities_by_object_ids(account_id, &plan.object_ids)
        .await
        .context("fetch MAPI requested object identities")?;
    log_mapi_requested_identity_resolution(account_id, plan, &identities);
    for identity in &identities {
        crate::mapi::identity::remember_mapi_identity(identity.canonical_id, identity.object_id);
    }

    let requested_mailbox_ids = identities
        .iter()
        .filter(|identity| identity.object_kind == MapiIdentityObjectKind::Mailbox)
        .map(|identity| identity.canonical_id)
        .collect::<Vec<_>>();
    if requested_mailbox_ids
        .iter()
        .any(|mailbox_id| !mailboxes.iter().any(|mailbox| mailbox.id == *mailbox_id))
    {
        log_mapi_store_load_step(
            account_id,
            plan,
            "fetch requested mailboxes",
            requested_mailbox_ids.len(),
        );
        let all_mailboxes = store
            .fetch_jmap_mailboxes(account_id)
            .await
            .context("fetch requested MAPI mailbox folders")?;
        merge_requested_mailboxes(&mut mailboxes, &all_mailboxes, &requested_mailbox_ids);
    }

    let mut content_windows = Vec::new();
    let mut content_message_ids = Vec::new();
    for (query_index, query) in plan.content_queries.iter().enumerate() {
        let Some(mailbox_id) = mailbox_id_for_mapi_folder_id(&mailboxes, query.folder_id) else {
            continue;
        };
        log_mapi_store_load_step(account_id, plan, "query content table ids", query_index);
        let result = store
            .query_mapi_content_table_ids(
                account_id,
                MapiContentTableQuery {
                    mailbox_id,
                    position: query.offset as u64,
                    limit: query.limit as u64,
                    sort_orders: query.sort_orders.clone(),
                },
            )
            .await
            .with_context(|| {
                format!(
                    "query MAPI content table ids for folder {:#018x} offset {} limit {}",
                    query.folder_id, query.offset, query.limit
                )
            })?;
        content_message_ids.extend(result.ids.iter().copied());
        content_windows.push(mapi_store::MapiContentTableWindow {
            folder_id: query.folder_id,
            view_signature: query.view_signature,
            offset: query.offset,
            total: result.total.min(usize::MAX as u64) as usize,
            message_ids: result.ids,
        });
    }

    let mut message_ids = identities
        .iter()
        .filter(|identity| identity.object_kind == MapiIdentityObjectKind::Message)
        .map(|identity| identity.canonical_id)
        .collect::<Vec<_>>();
    for message_id in content_message_ids {
        if !message_ids.contains(&message_id) {
            message_ids.push(message_id);
        }
    }
    let message_identity_requests = message_ids
        .iter()
        .map(|message_id| MapiIdentityRequest {
            object_kind: MapiIdentityObjectKind::Message,
            canonical_id: *message_id,
            reserved_global_counter: None,
        })
        .collect::<Vec<_>>();
    log_mapi_store_load_step(
        account_id,
        plan,
        "allocate message identities",
        message_identity_requests.len(),
    );
    for identity in store
        .fetch_or_allocate_mapi_identities(account_id, &message_identity_requests)
        .await
        .context("allocate MAPI message identities")?
    {
        crate::mapi::identity::remember_mapi_identity(identity.canonical_id, identity.object_id);
    }
    log_mapi_store_load_step(
        account_id,
        plan,
        "fetch content messages",
        message_ids.len(),
    );
    let emails = store
        .fetch_jmap_emails(account_id, &message_ids)
        .await
        .with_context(|| format!("fetch {} MAPI content messages", message_ids.len()))?;
    let mut attachments = Vec::with_capacity(emails.len());
    for (email_index, email) in emails.iter().enumerate() {
        log_mapi_store_load_step(account_id, plan, "fetch message attachments", email_index);
        attachments.push((
            email.id,
            store
                .fetch_message_attachments(account_id, email.id)
                .await
                .with_context(|| format!("fetch MAPI attachments for message {}", email.id))?,
        ));
    }

    log_mapi_store_load_step(account_id, plan, "fetch contact collections", 0);
    let contact_collections = store
        .fetch_accessible_contact_collections(account_id)
        .await
        .context("fetch MAPI contact collections")?;
    log_mapi_store_load_step(account_id, plan, "fetch calendar collections", 0);
    let calendar_collections = store
        .fetch_accessible_calendar_collections(account_id)
        .await
        .context("fetch MAPI calendar collections")?;
    log_mapi_store_load_step(account_id, plan, "fetch task collections", 0);
    let task_collections = store
        .fetch_accessible_task_collections(account_id)
        .await
        .context("fetch MAPI task collections")?;
    log_mapi_store_load_step(account_id, plan, "fetch search folders", 0);
    let search_folder_definitions = store
        .fetch_search_folders(account_id)
        .await
        .context("fetch MAPI search folders")?;
    log_mapi_store_load_step(account_id, plan, "fetch conversation actions", 0);
    let conversation_actions = store
        .fetch_conversation_actions(account_id)
        .await
        .context("fetch MAPI conversation actions")?;
    log_mapi_store_load_step(account_id, plan, "fetch delegate freebusy messages", 0);
    let delegate_freebusy_messages = store
        .fetch_delegate_freebusy_messages(account_id)
        .await
        .context("fetch MAPI delegate freebusy messages")?;
    log_mapi_store_load_step(account_id, plan, "fetch reminders", 0);
    let reminders = store
        .query_client_reminders(
            account_id,
            ReminderQuery {
                include_inactive: false,
            },
        )
        .await
        .context("fetch MAPI reminders")?;
    let snapshot_backed_contents = plan
        .content_queries
        .iter()
        .any(|query| mailbox_id_for_mapi_folder_id(&mailboxes, query.folder_id).is_none())
        || plan.object_ids.contains(&CALENDAR_FOLDER_ID);
    let contact_ids = identities
        .iter()
        .filter(|identity| identity.object_kind == MapiIdentityObjectKind::Contact)
        .map(|identity| identity.canonical_id)
        .collect::<Vec<_>>();
    let event_ids = identities
        .iter()
        .filter(|identity| identity.object_kind == MapiIdentityObjectKind::CalendarEvent)
        .map(|identity| identity.canonical_id)
        .collect::<Vec<_>>();
    let task_ids = identities
        .iter()
        .filter(|identity| identity.object_kind == MapiIdentityObjectKind::Task)
        .map(|identity| identity.canonical_id)
        .collect::<Vec<_>>();
    let note_ids = identities
        .iter()
        .filter(|identity| identity.object_kind == MapiIdentityObjectKind::Note)
        .map(|identity| identity.canonical_id)
        .collect::<Vec<_>>();
    let journal_entry_ids = identities
        .iter()
        .filter(|identity| identity.object_kind == MapiIdentityObjectKind::JournalEntry)
        .map(|identity| identity.canonical_id)
        .collect::<Vec<_>>();
    let contacts = if snapshot_backed_contents {
        log_mapi_store_load_step(
            account_id,
            plan,
            "fetch snapshot contacts",
            contact_collections.len(),
        );
        let mut contacts = Vec::new();
        for collection in &contact_collections {
            contacts.extend(
                store
                    .fetch_accessible_contacts_in_collection(account_id, &collection.id)
                    .await
                    .with_context(|| {
                        format!("fetch MAPI contacts in collection {}", collection.id)
                    })?,
            );
        }
        contacts
    } else {
        log_mapi_store_load_step(account_id, plan, "fetch contacts by id", contact_ids.len());
        store
            .fetch_accessible_contacts_by_ids(account_id, &contact_ids)
            .await
            .with_context(|| format!("fetch {} MAPI contacts by id", contact_ids.len()))?
    };
    let events = if snapshot_backed_contents {
        log_mapi_store_load_step(
            account_id,
            plan,
            "fetch snapshot events",
            calendar_collections.len(),
        );
        let mut events = Vec::new();
        for collection in &calendar_collections {
            events.extend(
                store
                    .fetch_accessible_events_in_collection(account_id, &collection.id)
                    .await
                    .with_context(|| {
                        format!("fetch MAPI events in collection {}", collection.id)
                    })?,
            );
        }
        events
    } else {
        log_mapi_store_load_step(account_id, plan, "fetch events by id", event_ids.len());
        store
            .fetch_accessible_events_by_ids(account_id, &event_ids)
            .await
            .with_context(|| format!("fetch {} MAPI events by id", event_ids.len()))?
    };
    let calendar_attachments = store
        .fetch_calendar_attachments_for_events(
            account_id,
            &events.iter().map(|event| event.id).collect::<Vec<_>>(),
        )
        .await
        .context("fetch MAPI calendar attachments")?;
    let tasks = if snapshot_backed_contents {
        log_mapi_store_load_step(
            account_id,
            plan,
            "fetch snapshot tasks",
            task_collections.len(),
        );
        let mut tasks = Vec::new();
        for collection in &task_collections {
            tasks.extend(
                store
                    .fetch_accessible_tasks_in_collection(account_id, &collection.id)
                    .await
                    .with_context(|| format!("fetch MAPI tasks in collection {}", collection.id))?,
            );
        }
        tasks
    } else {
        log_mapi_store_load_step(account_id, plan, "fetch tasks by id", task_ids.len());
        store
            .fetch_accessible_tasks_by_ids(account_id, &task_ids)
            .await
            .with_context(|| format!("fetch {} MAPI tasks by id", task_ids.len()))?
    };
    let notes = if snapshot_backed_contents {
        log_mapi_store_load_step(account_id, plan, "fetch snapshot notes", 0);
        store
            .fetch_mapi_notes(account_id)
            .await
            .context("fetch MAPI notes")?
    } else {
        log_mapi_store_load_step(account_id, plan, "fetch notes by id", note_ids.len());
        store
            .fetch_mapi_notes_by_ids(account_id, &note_ids)
            .await
            .with_context(|| format!("fetch {} MAPI notes by id", note_ids.len()))?
    };
    let journal_entries = if snapshot_backed_contents {
        log_mapi_store_load_step(account_id, plan, "fetch snapshot journal entries", 0);
        store
            .fetch_mapi_journal_entries(account_id)
            .await
            .context("fetch MAPI journal entries")?
    } else {
        log_mapi_store_load_step(
            account_id,
            plan,
            "fetch journal entries by id",
            journal_entry_ids.len(),
        );
        store
            .fetch_mapi_journal_entries_by_ids(account_id, &journal_entry_ids)
            .await
            .with_context(|| {
                format!(
                    "fetch {} MAPI journal entries by id",
                    journal_entry_ids.len()
                )
            })?
    };
    log_mapi_requested_collaboration_resolution(
        account_id,
        plan,
        snapshot_backed_contents,
        &identities,
        &contacts
            .iter()
            .map(|contact| contact.id)
            .collect::<Vec<_>>(),
        &events.iter().map(|event| event.id).collect::<Vec<_>>(),
        &tasks.iter().map(|task| task.id).collect::<Vec<_>>(),
        &notes.iter().map(|note| note.id).collect::<Vec<_>>(),
        &journal_entries
            .iter()
            .map(|entry| entry.id)
            .collect::<Vec<_>>(),
        contact_ids.len(),
        contacts.len(),
        event_ids.len(),
        events.len(),
        task_ids.len(),
        tasks.len(),
        note_ids.len(),
        notes.len(),
        journal_entry_ids.len(),
        journal_entries.len(),
    );
    let identity_requests = contacts
        .iter()
        .map(|contact| MapiIdentityRequest {
            object_kind: MapiIdentityObjectKind::Contact,
            canonical_id: contact.id,
            reserved_global_counter: None,
        })
        .chain(events.iter().map(|event| MapiIdentityRequest {
            object_kind: MapiIdentityObjectKind::CalendarEvent,
            canonical_id: event.id,
            reserved_global_counter: None,
        }))
        .chain(tasks.iter().map(|task| MapiIdentityRequest {
            object_kind: MapiIdentityObjectKind::Task,
            canonical_id: task.id,
            reserved_global_counter: None,
        }))
        .chain(notes.iter().map(|note| MapiIdentityRequest {
            object_kind: MapiIdentityObjectKind::Note,
            canonical_id: note.id,
            reserved_global_counter: None,
        }))
        .chain(journal_entries.iter().map(|entry| MapiIdentityRequest {
            object_kind: MapiIdentityObjectKind::JournalEntry,
            canonical_id: entry.id,
            reserved_global_counter: None,
        }))
        .chain(
            search_folder_definitions
                .iter()
                .map(|definition| MapiIdentityRequest {
                    object_kind: MapiIdentityObjectKind::SearchFolderDefinition,
                    canonical_id: definition.id,
                    reserved_global_counter: None,
                }),
        )
        .chain(
            conversation_actions
                .iter()
                .map(|action| MapiIdentityRequest {
                    object_kind: MapiIdentityObjectKind::ConversationAction,
                    canonical_id: action.id,
                    reserved_global_counter: None,
                }),
        )
        .chain(
            delegate_freebusy_messages
                .iter()
                .map(|message| MapiIdentityRequest {
                    object_kind: MapiIdentityObjectKind::DelegateFreeBusyMessage,
                    canonical_id: message.id,
                    reserved_global_counter: None,
                }),
        )
        .collect::<Vec<_>>();
    log_mapi_store_load_step(
        account_id,
        plan,
        "allocate non-message identities",
        identity_requests.len(),
    );
    for identity in store
        .fetch_or_allocate_mapi_identities(account_id, &identity_requests)
        .await
        .context("allocate MAPI non-message identities")?
    {
        crate::mapi::identity::remember_mapi_identity(identity.canonical_id, identity.object_id);
    }
    let mailbox_ids = mailboxes
        .iter()
        .map(|mailbox| mailbox.id)
        .collect::<Vec<_>>();
    log_mapi_store_load_step(
        account_id,
        plan,
        "fetch folder permissions",
        mailbox_ids.len(),
    );
    let folder_permissions = store
        .fetch_mapi_folder_permissions(account_id, &mailbox_ids)
        .await
        .context("fetch MAPI folder permissions")?;

    log_mapi_store_load_summary(
        account_id,
        plan,
        snapshot_backed_contents,
        mailboxes.len(),
        emails.len(),
        attachments.len(),
        contact_collections.len(),
        calendar_collections.len(),
        task_collections.len(),
        contacts.len(),
        events.len(),
        tasks.len(),
        notes.len(),
        journal_entries.len(),
        search_folder_definitions.len(),
        conversation_actions.len(),
        reminders.len(),
        folder_permissions.len(),
        content_windows.len(),
        event_ids.len(),
        calendar_collections
            .iter()
            .any(|collection| matches!(collection.id.as_str(), "default" | "calendar")),
        events
            .iter()
            .filter(|event| matches!(event.collection_id.as_str(), "default" | "calendar"))
            .count(),
    );
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
    )
    .with_notes_and_journal(notes, journal_entries)
    .with_search_folder_definitions(search_folder_definitions)
    .with_conversation_actions(conversation_actions)
    .with_delegate_freebusy_messages(delegate_freebusy_messages)
    .with_reminders(reminders)
    .with_content_windows(content_windows)
    .with_calendar_attachments(calendar_attachments))
}

fn log_mapi_store_load_step(
    account_id: Uuid,
    plan: &MapiAccessPlan,
    step: &'static str,
    item_count: usize,
) {
    tracing::debug!(
        rca_debug = true,
        adapter = "mapi",
        request_type = "Execute",
        account_id = %account_id,
        full_snapshot = plan.requires_full_snapshot,
        object_id_count = plan.object_ids.len(),
        content_query_count = plan.content_queries.len(),
        step = step,
        item_count = item_count,
        message = "rca debug mapi execute store load step",
    );
}

fn log_mapi_store_full_snapshot(account_id: Uuid, plan: &MapiAccessPlan) {
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        request_type = "Execute",
        account_id = %account_id,
        full_snapshot = true,
        object_id_count = plan.object_ids.len(),
        content_query_count = plan.content_queries.len(),
        message = "rca debug mapi execute store load summary",
    );
}

fn log_mapi_requested_identity_resolution(
    account_id: Uuid,
    plan: &MapiAccessPlan,
    identities: &[MapiIdentityLookupRecord],
) {
    if plan.object_ids.is_empty() {
        return;
    }

    let resolved_object_ids = identities
        .iter()
        .map(|identity| identity.object_id)
        .collect::<Vec<_>>();
    let missing_object_ids = plan
        .object_ids
        .iter()
        .copied()
        .filter(|object_id| !resolved_object_ids.contains(object_id))
        .collect::<Vec<_>>();
    let expected_unbacked_object_ids = missing_object_ids
        .iter()
        .copied()
        .filter(|object_id| is_expected_unbacked_mapi_object(*object_id))
        .collect::<Vec<_>>();
    let unresolved_object_ids = missing_object_ids
        .iter()
        .copied()
        .filter(|object_id| !is_expected_unbacked_mapi_object(*object_id))
        .collect::<Vec<_>>();
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        request_type = "Execute",
        account_id = %account_id,
        full_snapshot = false,
        object_id_count = plan.object_ids.len(),
        content_query_count = plan.content_queries.len(),
        requested_object_ids = %format_mapi_object_ids(&plan.object_ids),
        resolved_identity_count = identities.len(),
        resolved_identity_object_ids = %format_mapi_object_ids(&resolved_object_ids),
        resolved_identity_kinds = %format_mapi_identity_kinds(identities),
        expected_unbacked_object_id_count = expected_unbacked_object_ids.len(),
        expected_unbacked_object_ids = %format_mapi_object_ids(&expected_unbacked_object_ids),
        unresolved_object_id_count = unresolved_object_ids.len(),
        unresolved_object_ids = %format_mapi_object_ids(&unresolved_object_ids),
        unresolved_object_scopes = %format_unresolved_mapi_object_scopes(&unresolved_object_ids),
        message = "rca debug mapi requested identity resolution",
    );
}

#[allow(clippy::too_many_arguments)]
fn log_mapi_requested_collaboration_resolution(
    account_id: Uuid,
    plan: &MapiAccessPlan,
    snapshot_backed_contents: bool,
    identities: &[MapiIdentityLookupRecord],
    loaded_contact_ids: &[Uuid],
    loaded_event_ids: &[Uuid],
    loaded_task_ids: &[Uuid],
    loaded_note_ids: &[Uuid],
    loaded_journal_entry_ids: &[Uuid],
    requested_contact_identity_count: usize,
    loaded_contact_count: usize,
    requested_calendar_event_identity_count: usize,
    loaded_calendar_event_count: usize,
    requested_task_identity_count: usize,
    loaded_task_count: usize,
    requested_note_identity_count: usize,
    loaded_note_count: usize,
    requested_journal_entry_identity_count: usize,
    loaded_journal_entry_count: usize,
) {
    if plan.object_ids.is_empty()
        || requested_contact_identity_count
            + requested_calendar_event_identity_count
            + requested_task_identity_count
            + requested_note_identity_count
            + requested_journal_entry_identity_count
            == 0
    {
        return;
    }

    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        request_type = "Execute",
        account_id = %account_id,
        full_snapshot = false,
        object_id_count = plan.object_ids.len(),
        content_query_count = plan.content_queries.len(),
        snapshot_backed_contents,
        requested_contact_identity_count,
        loaded_contact_count,
        missing_contact_count = requested_contact_identity_count.saturating_sub(loaded_contact_count),
        requested_calendar_event_identity_count,
        loaded_calendar_event_count,
        missing_calendar_event_count = requested_calendar_event_identity_count.saturating_sub(loaded_calendar_event_count),
        requested_task_identity_count,
        loaded_task_count,
        missing_task_count = requested_task_identity_count.saturating_sub(loaded_task_count),
        requested_note_identity_count,
        loaded_note_count,
        missing_note_count = requested_note_identity_count.saturating_sub(loaded_note_count),
        requested_journal_entry_identity_count,
        loaded_journal_entry_count,
        missing_journal_entry_count = requested_journal_entry_identity_count.saturating_sub(loaded_journal_entry_count),
        missing_contact_identities = %format_missing_mapi_identities(identities, MapiIdentityObjectKind::Contact, loaded_contact_ids),
        missing_calendar_event_identities = %format_missing_mapi_identities(identities, MapiIdentityObjectKind::CalendarEvent, loaded_event_ids),
        missing_task_identities = %format_missing_mapi_identities(identities, MapiIdentityObjectKind::Task, loaded_task_ids),
        missing_note_identities = %format_missing_mapi_identities(identities, MapiIdentityObjectKind::Note, loaded_note_ids),
        missing_journal_entry_identities = %format_missing_mapi_identities(identities, MapiIdentityObjectKind::JournalEntry, loaded_journal_entry_ids),
        message = "rca debug mapi requested collaboration resolution",
    );
}

fn format_missing_mapi_identities(
    identities: &[MapiIdentityLookupRecord],
    object_kind: MapiIdentityObjectKind,
    loaded_canonical_ids: &[Uuid],
) -> String {
    identities
        .iter()
        .filter(|identity| {
            identity.object_kind == object_kind
                && !loaded_canonical_ids.contains(&identity.canonical_id)
        })
        .map(|identity| {
            format!(
                "object_id={:#018x};canonical_id={};kind={}",
                identity.object_id,
                identity.canonical_id,
                mapi_identity_kind_name(identity.object_kind)
            )
        })
        .collect::<Vec<_>>()
        .join("|")
}

fn format_mapi_object_ids(object_ids: &[u64]) -> String {
    object_ids
        .iter()
        .map(|object_id| format!("{object_id:#018x}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn format_unresolved_mapi_object_scopes(object_ids: &[u64]) -> String {
    object_ids
        .iter()
        .map(|object_id| {
            format!(
                "{object_id:#018x}:{}",
                unresolved_mapi_object_scope(*object_id)
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn unresolved_mapi_object_scope(object_id: u64) -> &'static str {
    if is_expected_unbacked_mapi_object(object_id) {
        return "advertised_special_folder";
    }
    if crate::mapi::identity::global_counter_from_store_id(object_id).is_some() {
        "unallocated_store_object"
    } else {
        "foreign_or_invalid_replid"
    }
}

fn is_expected_unbacked_mapi_object(object_id: u64) -> bool {
    is_advertised_special_folder(object_id)
}

fn format_mapi_identity_kinds(identities: &[MapiIdentityLookupRecord]) -> String {
    identities
        .iter()
        .map(|identity| mapi_identity_kind_name(identity.object_kind))
        .collect::<Vec<_>>()
        .join(",")
}

fn mapi_identity_kind_name(object_kind: MapiIdentityObjectKind) -> &'static str {
    match object_kind {
        MapiIdentityObjectKind::Account => "account",
        MapiIdentityObjectKind::Mailbox => "mailbox",
        MapiIdentityObjectKind::Message => "message",
        MapiIdentityObjectKind::Contact => "contact",
        MapiIdentityObjectKind::CalendarEvent => "calendar_event",
        MapiIdentityObjectKind::Task => "task",
        MapiIdentityObjectKind::SearchFolderDefinition => "search_folder_definition",
        MapiIdentityObjectKind::ConversationAction => "conversation_action",
        MapiIdentityObjectKind::NavigationShortcut => "navigation_shortcut",
        MapiIdentityObjectKind::Note => "note",
        MapiIdentityObjectKind::JournalEntry => "journal_entry",
        MapiIdentityObjectKind::DelegateFreeBusyMessage => "delegate_freebusy_message",
    }
}

#[allow(clippy::too_many_arguments)]
fn log_mapi_store_load_summary(
    account_id: Uuid,
    plan: &MapiAccessPlan,
    snapshot_backed_contents: bool,
    mailbox_count: usize,
    email_count: usize,
    attachment_set_count: usize,
    contact_collection_count: usize,
    calendar_collection_count: usize,
    task_collection_count: usize,
    contact_count: usize,
    calendar_event_count: usize,
    task_count: usize,
    note_count: usize,
    journal_entry_count: usize,
    search_folder_count: usize,
    conversation_action_count: usize,
    reminder_count: usize,
    folder_permission_count: usize,
    content_window_count: usize,
    requested_calendar_event_identity_count: usize,
    default_calendar_collection_loaded: bool,
    loaded_default_calendar_event_count: usize,
) {
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        request_type = "Execute",
        account_id = %account_id,
        full_snapshot = false,
        object_id_count = plan.object_ids.len(),
        content_query_count = plan.content_queries.len(),
        snapshot_backed_contents,
        mailbox_count,
        email_count,
        attachment_set_count,
        contact_collection_count,
        calendar_collection_count,
        default_calendar_collection_loaded,
        requested_calendar_event_identity_count,
        calendar_event_count,
        loaded_default_calendar_event_count,
        task_collection_count,
        contact_count,
        task_count,
        note_count,
        journal_entry_count,
        search_folder_count,
        conversation_action_count,
        reminder_count,
        folder_permission_count,
        content_window_count,
        message = "rca debug mapi execute store load summary",
    );
}

fn rop_requires_full_snapshot(rop_id: u8) -> bool {
    matches!(
        rop_id,
        0x18 | 0x19
            | 0x1A
            | 0x1B
            | 0x4B
            | 0x4C
            | 0x4D
            | 0x4E
            | 0x4F
            | 0x58
            | 0x70
            | 0x72
            | 0x73
            | 0x74
            | 0x78
            | 0x92
    )
}

fn simulate_table_access(
    plan: &mut MapiAccessPlan,
    handles: &mut HashMap<u32, MapiObject>,
    next_handle: &mut u32,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
) {
    match request.rop_id {
        0x02 => {
            let folder_id = request.folder_id().unwrap_or(ROOT_FOLDER_ID);
            let handle = simulate_allocate_handle(
                handles,
                next_handle,
                request.output_handle_index,
                MapiObject::Folder {
                    folder_id,
                    properties: HashMap::new(),
                },
            );
            set_handle_slot(handle_slots, request.output_handle_index, handle);
        }
        0x04 => {
            let folder_id = input_handle(handle_slots, request)
                .and_then(|handle| handles.get(&handle))
                .and_then(MapiObject::folder_id)
                .unwrap_or(ROOT_FOLDER_ID);
            let handle = simulate_allocate_handle(
                handles,
                next_handle,
                request.output_handle_index,
                MapiObject::HierarchyTable {
                    folder_id,
                    columns: default_hierarchy_columns(),
                    sort_orders: Vec::new(),
                    restriction: None,
                    bookmarks: HashMap::new(),
                    next_bookmark: 1,
                    position: 0,
                },
            );
            set_handle_slot(handle_slots, request.output_handle_index, handle);
        }
        0x05 => {
            let folder_id = input_handle(handle_slots, request)
                .and_then(|handle| handles.get(&handle))
                .and_then(MapiObject::folder_id)
                .unwrap_or(INBOX_FOLDER_ID);
            let handle = simulate_allocate_handle(
                handles,
                next_handle,
                request.output_handle_index,
                MapiObject::ContentsTable {
                    folder_id,
                    associated: request
                        .payload
                        .first()
                        .is_some_and(|flags| flags & 0x02 != 0),
                    columns: Vec::new(),
                    sort_orders: Vec::new(),
                    restriction: None,
                    bookmarks: HashMap::new(),
                    next_bookmark: 1,
                    position: 0,
                },
            );
            set_handle_slot(handle_slots, request.output_handle_index, handle);
        }
        0x06 => {
            let folder_id = request.folder_id().unwrap_or_else(|| {
                input_handle(handle_slots, request)
                    .and_then(|handle| handles.get(&handle))
                    .and_then(MapiObject::folder_id)
                    .unwrap_or(INBOX_FOLDER_ID)
            });
            let object =
                if folder_id == crate::mapi::identity::CONVERSATION_ACTION_SETTINGS_FOLDER_ID {
                    MapiObject::PendingConversationAction {
                        folder_id,
                        properties: HashMap::new(),
                    }
                } else {
                    MapiObject::PendingMessage {
                        folder_id,
                        properties: HashMap::new(),
                        recipients: Vec::new(),
                    }
                };
            let handle =
                simulate_allocate_handle(handles, next_handle, request.output_handle_index, object);
            set_handle_slot(handle_slots, request.output_handle_index, handle);
        }
        0x0C => {
            if input_handle(handle_slots, request)
                .and_then(|handle| handles.get(&handle))
                .is_some_and(|object| {
                    matches!(object, MapiObject::PendingConversationAction { .. })
                })
            {
                plan.requires_full_snapshot = true;
                return;
            }
        }
        0x12 => {
            if let Some(MapiObject::ContentsTable { columns, .. }) =
                input_handle(handle_slots, request).and_then(|handle| handles.get_mut(&handle))
            {
                *columns = request.property_tags();
            }
        }
        0x13 => {
            if let Some(MapiObject::ContentsTable {
                sort_orders,
                position,
                bookmarks,
                ..
            }) = input_handle(handle_slots, request).and_then(|handle| handles.get_mut(&handle))
            {
                let parsed = request.sort_orders();
                if mapi_content_table_sort_orders(&parsed).is_none() {
                    plan.requires_full_snapshot = true;
                    return;
                }
                *sort_orders = parsed;
                *position = 0;
                bookmarks.clear();
            }
        }
        0x14 => {
            if let Some(MapiObject::ContentsTable {
                restriction,
                position,
                bookmarks,
                ..
            }) = input_handle(handle_slots, request).and_then(|handle| handles.get_mut(&handle))
            {
                match request.restriction() {
                    Ok(None) => *restriction = None,
                    Ok(Some(_)) | Err(_) => {
                        plan.requires_full_snapshot = true;
                        return;
                    }
                }
                *position = 0;
                bookmarks.clear();
            }
        }
        0x15 => {
            let Some(MapiObject::ContentsTable {
                folder_id,
                sort_orders,
                restriction,
                position,
                ..
            }) = input_handle(handle_slots, request).and_then(|handle| handles.get_mut(&handle))
            else {
                return;
            };
            if restriction.is_some() {
                plan.requires_full_snapshot = true;
                return;
            }
            let Some(sql_sort_orders) = mapi_content_table_sort_orders(sort_orders) else {
                plan.requires_full_snapshot = true;
                return;
            };
            let row_count = request.query_row_count().unwrap_or(0);
            let offset = if request.query_forward_read() {
                *position
            } else {
                (*position).saturating_sub(row_count)
            };
            add_content_query(
                plan,
                *folder_id,
                table_view_signature(sort_orders, restriction.as_ref()),
                offset,
                row_count,
                sql_sort_orders,
            );
            if !request.query_no_advance() {
                if request.query_forward_read() {
                    *position = (*position).saturating_add(row_count);
                } else {
                    *position = offset;
                }
            }
        }
        0x17 => {
            let Some(MapiObject::ContentsTable {
                folder_id,
                sort_orders,
                restriction,
                position,
                ..
            }) = input_handle(handle_slots, request).and_then(|handle| handles.get(&handle))
            else {
                return;
            };
            if restriction.is_some() {
                plan.requires_full_snapshot = true;
                return;
            }
            let Some(sql_sort_orders) = mapi_content_table_sort_orders(sort_orders) else {
                plan.requires_full_snapshot = true;
                return;
            };
            add_content_query(
                plan,
                *folder_id,
                table_view_signature(sort_orders, restriction.as_ref()),
                *position,
                0,
                sql_sort_orders,
            );
        }
        _ => {}
    }
}

fn simulate_allocate_handle(
    handles: &mut HashMap<u32, MapiObject>,
    next_handle: &mut u32,
    output_handle_index: Option<u8>,
    object: MapiObject,
) -> u32 {
    let preferred = output_handle_index.map(|index| index as u32 + 1);
    let handle = preferred
        .filter(|handle| !handles.contains_key(handle))
        .unwrap_or(*next_handle);
    *next_handle = next_handle.saturating_add(1).max(1);
    if handle >= *next_handle {
        *next_handle = handle.saturating_add(1).max(1);
    }
    handles.insert(handle, object);
    handle
}

fn add_content_query(
    plan: &mut MapiAccessPlan,
    folder_id: u64,
    view_signature: u64,
    offset: usize,
    limit: usize,
    sort_orders: Vec<MapiContentTableSort>,
) {
    if plan.content_queries.iter().any(|query| {
        query.folder_id == folder_id
            && query.view_signature == view_signature
            && query.offset == offset
            && query.limit == limit
    }) {
        return;
    }
    plan.content_queries.push(MapiContentAccessQuery {
        folder_id,
        view_signature,
        offset,
        limit,
        sort_orders,
    });
}

fn mapi_content_table_sort_orders(
    sort_orders: &[MapiSortOrder],
) -> Option<Vec<MapiContentTableSort>> {
    sort_orders
        .iter()
        .map(|sort| {
            let field = match sort.property_tag {
                PID_TAG_MESSAGE_DELIVERY_TIME | PID_TAG_LAST_MODIFICATION_TIME => {
                    MapiContentTableSortField::ReceivedAt
                }
                PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
                    MapiContentTableSortField::Subject
                }
                PID_TAG_SENDER_NAME_W => MapiContentTableSortField::SenderName,
                PID_TAG_SENDER_EMAIL_ADDRESS_W => MapiContentTableSortField::SenderEmail,
                PID_TAG_DISPLAY_TO_W => MapiContentTableSortField::DisplayTo,
                PID_TAG_MESSAGE_SIZE => MapiContentTableSortField::MessageSize,
                PID_TAG_HAS_ATTACHMENTS => MapiContentTableSortField::HasAttachments,
                PID_TAG_MESSAGE_FLAGS => MapiContentTableSortField::MessageFlags,
                _ => return None,
            };
            Some(MapiContentTableSort {
                field,
                descending: sort.order == 0x01,
            })
        })
        .collect()
}

fn add_object_ids_for_handle(plan: &mut MapiAccessPlan, object: &MapiObject) {
    match object {
        MapiObject::Folder { folder_id, .. }
        | MapiObject::HierarchyTable { folder_id, .. }
        | MapiObject::ContentsTable { folder_id, .. }
        | MapiObject::PendingMessage { folder_id, .. }
        | MapiObject::PendingContact { folder_id, .. }
        | MapiObject::PendingEvent { folder_id, .. }
        | MapiObject::PendingTask { folder_id, .. }
        | MapiObject::PendingNote { folder_id, .. }
        | MapiObject::PendingJournalEntry { folder_id, .. }
        | MapiObject::PendingConversationAction { folder_id, .. }
        | MapiObject::PendingNavigationShortcut { folder_id, .. }
        | MapiObject::SynchronizationSource { folder_id, .. }
        | MapiObject::SynchronizationCollector { folder_id, .. }
        | MapiObject::PermissionTable { folder_id, .. } => {
            push_unique(&mut plan.object_ids, *folder_id);
        }
        MapiObject::Message {
            folder_id,
            message_id,
        }
        | MapiObject::AttachmentTable {
            folder_id,
            message_id,
            ..
        }
        | MapiObject::Attachment {
            folder_id,
            message_id,
            ..
        }
        | MapiObject::PendingAttachment {
            folder_id,
            message_id,
            ..
        }
        | MapiObject::SavedAttachment {
            folder_id,
            message_id,
            ..
        } => {
            push_unique(&mut plan.object_ids, *folder_id);
            push_unique(&mut plan.object_ids, *message_id);
        }
        MapiObject::Contact {
            folder_id,
            contact_id,
        } => {
            push_unique(&mut plan.object_ids, *folder_id);
            push_unique(&mut plan.object_ids, *contact_id);
        }
        MapiObject::Event {
            folder_id,
            event_id,
        } => {
            push_unique(&mut plan.object_ids, *folder_id);
            push_unique(&mut plan.object_ids, *event_id);
        }
        MapiObject::Task { folder_id, task_id } => {
            push_unique(&mut plan.object_ids, *folder_id);
            push_unique(&mut plan.object_ids, *task_id);
        }
        MapiObject::Note { folder_id, note_id } => {
            push_unique(&mut plan.object_ids, *folder_id);
            push_unique(&mut plan.object_ids, *note_id);
        }
        MapiObject::JournalEntry {
            folder_id,
            journal_entry_id,
        } => {
            push_unique(&mut plan.object_ids, *folder_id);
            push_unique(&mut plan.object_ids, *journal_entry_id);
        }
        MapiObject::ConversationAction {
            folder_id,
            conversation_action_id,
        } => {
            push_unique(&mut plan.object_ids, *folder_id);
            push_unique(&mut plan.object_ids, *conversation_action_id);
        }
        MapiObject::NavigationShortcut {
            folder_id,
            shortcut_id,
        } => {
            push_unique(&mut plan.object_ids, *folder_id);
            push_unique(&mut plan.object_ids, *shortcut_id);
        }
        MapiObject::DelegateFreeBusyMessage {
            folder_id,
            message_id,
        } => {
            push_unique(&mut plan.object_ids, *folder_id);
            push_unique(&mut plan.object_ids, *message_id);
        }
        MapiObject::AttachmentStream { .. }
        | MapiObject::NotificationSubscription { .. }
        | MapiObject::Logon => {}
    }
}

fn push_unique(values: &mut Vec<u64>, value: u64) {
    if value != 0 && !values.contains(&value) {
        values.push(value);
    }
}

fn mapi_identity_requests_for_mailboxes(mailboxes: &[JmapMailbox]) -> Vec<MapiIdentityRequest> {
    mailboxes
        .iter()
        .map(|mailbox| MapiIdentityRequest {
            object_kind: MapiIdentityObjectKind::Mailbox,
            canonical_id: mailbox.id,
            reserved_global_counter: mapi_store::reserved_folder_counter_for_role(&mailbox.role),
        })
        .collect()
}

fn merge_requested_mailboxes(
    mailboxes: &mut Vec<JmapMailbox>,
    all_mailboxes: &[JmapMailbox],
    requested_mailbox_ids: &[Uuid],
) {
    for requested_id in requested_mailbox_ids {
        if mailboxes.iter().any(|mailbox| mailbox.id == *requested_id) {
            continue;
        }
        if let Some(mailbox) = all_mailboxes
            .iter()
            .find(|mailbox| mailbox.id == *requested_id)
        {
            mailboxes.push(mailbox.clone());
        }
    }
}

fn mailbox_id_for_mapi_folder_id(mailboxes: &[JmapMailbox], folder_id: u64) -> Option<Uuid> {
    mailboxes
        .iter()
        .find(|mailbox| mapi_folder_id(mailbox) == folder_id)
        .map(|mailbox| mailbox.id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, VecDeque};
    use std::time::SystemTime;

    fn empty_session() -> MapiSession {
        MapiSession {
            endpoint: MapiEndpoint::Emsmdb,
            tenant_id: Uuid::nil(),
            account_id: Uuid::nil(),
            email: "test@example.test".to_string(),
            created_at: SystemTime::UNIX_EPOCH,
            last_seen_at: SystemTime::UNIX_EPOCH,
            first_request_type: "Connect".to_string(),
            first_request_id: "test:1".to_string(),
            last_request_type: "Connect".to_string(),
            last_request_id: "test:1".to_string(),
            request_count: 1,
            execute_request_count: 0,
            next_handle: 1,
            handles: HashMap::new(),
            message_statuses: HashMap::new(),
            named_properties: HashMap::new(),
            named_property_ids: HashMap::new(),
            next_named_property_id: FIRST_NAMED_PROPERTY_ID,
            next_local_replica_sequence: 1,
            notification_cursor: None,
            pending_notifications: VecDeque::new(),
            completed_execute_requests: HashMap::new(),
            completed_execute_request_order: VecDeque::new(),
            post_hierarchy_actions: PostHierarchyActionState::default(),
        }
    }

    fn single_rop_buffer(rop: &[u8]) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&(rop.len() as u16).to_le_bytes());
        buffer.extend_from_slice(rop);
        buffer.extend_from_slice(&1u32.to_le_bytes());
        buffer
    }

    fn mailbox(id: &str, role: &str, name: &str) -> JmapMailbox {
        JmapMailbox {
            id: Uuid::parse_str(id).unwrap(),
            parent_id: None,
            role: role.to_string(),
            name: name.to_string(),
            sort_order: 40,
            modseq: 40,
            total_emails: 0,
            unread_emails: 0,
            is_subscribed: true,
        }
    }

    #[test]
    fn merge_requested_mailboxes_adds_custom_identity_rows() {
        let inbox = mailbox("11111111-1111-1111-1111-111111111111", "inbox", "Inbox");
        let custom = mailbox("22222222-2222-2222-2222-222222222222", "custom", "RCA Sync");
        let missing = Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap();
        let mut loaded = vec![inbox.clone()];
        let all_mailboxes = vec![inbox, custom.clone()];

        merge_requested_mailboxes(
            &mut loaded,
            &all_mailboxes,
            &[custom.id, custom.id, missing],
        );

        assert_eq!(loaded.len(), 2);
        assert!(loaded.iter().any(|mailbox| mailbox.id == custom.id));
    }

    #[test]
    fn access_plan_includes_long_term_id_source_in_trailing_replid_form() {
        let object_id = crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 9,
        );
        let global_counter = crate::mapi::identity::global_counter_from_store_id(object_id)
            .expect("dynamic object id has a global counter");
        let mut rop = vec![0x43, 0x00, 0x00];
        rop.extend_from_slice(&crate::mapi::identity::globcnt_bytes(global_counter));
        rop.extend_from_slice(&1u16.to_le_bytes());

        let plan = plan_mapi_store_access(&empty_session(), &single_rop_buffer(&rop));

        assert!(
            plan.object_ids.contains(&object_id),
            "object_id={object_id:#018x} plan={:?}",
            plan.object_ids
        );
    }

    #[test]
    fn access_plan_does_not_decode_get_properties_payload_as_object_id() {
        let mut rop = vec![0x07, 0x00, 0x00];
        rop.extend_from_slice(&[0x01, 0x00]);
        rop.extend_from_slice(&1u16.to_le_bytes());
        rop.extend_from_slice(&[0x00, 0x00, 0x2f, 0x00]);

        let plan = plan_mapi_store_access(&empty_session(), &single_rop_buffer(&rop));

        assert!(plan.object_ids.is_empty(), "plan={:?}", plan.object_ids);
    }

    #[test]
    fn access_plan_does_not_decode_set_properties_payload_as_import_source_key() {
        let mut rop = vec![0x0A, 0x00, 0x00];
        rop.extend_from_slice(&[0x01, 0x00]);
        rop.extend_from_slice(&PID_TAG_SOURCE_KEY.to_le_bytes());
        rop.extend_from_slice(&22u16.to_le_bytes());
        rop.extend_from_slice(&crate::mapi::identity::source_key_for_object_id(
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 12,
            ),
        ));

        let plan = plan_mapi_store_access(&empty_session(), &single_rop_buffer(&rop));

        assert!(plan.object_ids.is_empty(), "plan={:?}", plan.object_ids);
    }

    #[test]
    fn access_plan_does_not_decode_set_properties_payload_as_read_state_change() {
        let mut rop = vec![0x0A, 0x00, 0x00];
        rop.extend_from_slice(&[0x01, 0x00]);
        rop.extend_from_slice(&PID_TAG_OST_OSTID.to_le_bytes());
        rop.extend_from_slice(&20u16.to_le_bytes());
        rop.extend_from_slice(&[
            0xea, 0x33, 0x94, 0x46, 0x27, 0xb9, 0x4a, 0x9c, 0xb0, 0xde, 0x87, 0x3f, 0x03, 0xa3,
            0x53, 0x76, 0x00, 0x00, 0x00, 0x00,
        ]);

        let plan = plan_mapi_store_access(&empty_session(), &single_rop_buffer(&rop));

        assert!(plan.object_ids.is_empty(), "plan={:?}", plan.object_ids);
    }

    #[test]
    fn access_plan_decodes_synchronization_import_read_state_changes() {
        let message_id = crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 42,
        );
        let message_id_bytes = crate::mapi::identity::wire_id_bytes_from_object_id(message_id)
            .expect("MAPI store id is encodable");
        let mut rop = vec![0x80, 0x00, 0x00];
        rop.extend_from_slice(&11u16.to_le_bytes());
        rop.extend_from_slice(&8u16.to_le_bytes());
        rop.extend_from_slice(&message_id_bytes);
        rop.push(1);

        let buffer = single_rop_buffer(&rop);
        let (requests, _) = split_rop_buffer(&buffer).expect("ROP buffer should split");
        let mut cursor = Cursor::new(requests);
        let request = read_rop_request(&mut cursor).expect("ROP request should parse");
        assert_eq!(
            request.import_read_state_changes(),
            vec![(message_id, false)]
        );
        assert_eq!(cursor.remaining(), 0);
        assert!(!rop_requires_full_snapshot(0x80));

        let plan = plan_mapi_store_access(&empty_session(), &buffer);

        assert_eq!(
            plan.object_ids,
            vec![message_id],
            "requires_full_snapshot={}",
            plan.requires_full_snapshot
        );
    }

    #[test]
    fn access_plan_preloads_long_term_id_from_id_source() {
        let object_id = crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 59,
        );
        let mut rop = vec![0x43, 0x00, 0x00];
        rop.extend_from_slice(
            &crate::mapi::identity::wire_id_bytes_from_object_id(object_id)
                .expect("MAPI store id is encodable"),
        );

        let plan = plan_mapi_store_access(&empty_session(), &single_rop_buffer(&rop));

        assert_eq!(plan.object_ids, vec![object_id]);
    }

    #[test]
    fn missing_mapi_identity_summary_names_object_and_canonical_ids() {
        let missing_id = Uuid::parse_str("fb129372-d6b6-4d69-99f7-977ab2a8093f").unwrap();
        let loaded_id = Uuid::parse_str("17b18079-e962-4d53-9d2f-d68cfb37dcad").unwrap();
        let identities = vec![
            MapiIdentityLookupRecord {
                object_kind: MapiIdentityObjectKind::Contact,
                canonical_id: missing_id,
                object_id: 0x0000_0000_003b_0001,
                source_key: Vec::new(),
            },
            MapiIdentityLookupRecord {
                object_kind: MapiIdentityObjectKind::Contact,
                canonical_id: loaded_id,
                object_id: 0x0000_0000_0037_0001,
                source_key: Vec::new(),
            },
        ];

        assert_eq!(
            format_missing_mapi_identities(
                &identities,
                MapiIdentityObjectKind::Contact,
                &[loaded_id],
            ),
            "object_id=0x00000000003b0001;canonical_id=fb129372-d6b6-4d69-99f7-977ab2a8093f;kind=contact"
        );
    }

    #[test]
    fn unresolved_mapi_identity_summary_classifies_expected_special_and_invalid_ids() {
        let invalid_replid_id = 0x0201_047c_2800_0002;
        let dynamic_id = crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 10,
        );

        assert_eq!(
            format_unresolved_mapi_object_scopes(&[
                ROOT_FOLDER_ID,
                dynamic_id,
                invalid_replid_id
            ]),
            format!(
                "{ROOT_FOLDER_ID:#018x}:advertised_special_folder,{dynamic_id:#018x}:unallocated_store_object,{invalid_replid_id:#018x}:foreign_or_invalid_replid"
            )
        );
    }

    #[test]
    fn expected_unbacked_mapi_objects_are_only_advertised_special_folders() {
        let dynamic_id = crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 10,
        );

        assert!(is_expected_unbacked_mapi_object(ROOT_FOLDER_ID));
        assert!(!is_expected_unbacked_mapi_object(dynamic_id));
    }
}
