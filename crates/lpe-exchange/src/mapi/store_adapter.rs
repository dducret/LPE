use super::properties::*;
use super::rop::*;
use super::session::*;
use super::sync::{
    CALENDAR_FOLDER_ID, COMMON_VIEWS_FOLDER_ID, CONTACTS_FOLDER_ID, CONTACTS_SEARCH_FOLDER_ID,
    IM_CONTACT_LIST_FOLDER_ID, INBOX_FOLDER_ID, JOURNAL_FOLDER_ID, NOTES_FOLDER_ID,
    QUICK_CONTACTS_FOLDER_ID, REMINDERS_FOLDER_ID, ROOT_FOLDER_ID, SENT_FOLDER_ID,
    SUGGESTED_CONTACTS_FOLDER_ID, TASKS_FOLDER_ID, TODO_SEARCH_FOLDER_ID,
    TRACKED_MAIL_PROCESSING_FOLDER_ID, TRASH_FOLDER_ID,
};
use super::tables::*;
use super::*;
use crate::mapi_store;
use crate::store::{
    MapiContentTableQuery, MapiContentTableSort, MapiContentTableSortField,
    MapiIdentityLookupRecord, MapiIdentityRequest,
};
use anyhow::Context;
use lpe_storage::ReminderQuery;

mod access_plan;

pub(in crate::mapi) use access_plan::*;

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
    let allocated_mailbox_identities = store
        .fetch_or_allocate_mapi_identities(account_id, &mailbox_requests)
        .await
        .context("allocate MAPI mailbox identities")?;
    for identity in &allocated_mailbox_identities {
        crate::mapi::identity::remember_mapi_identity_with_source_key(
            identity.canonical_id,
            identity.object_id,
            Some(identity.source_key.clone()),
        );
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
    log_mapi_store_load_step(account_id, plan, "fetch search folders", 0);
    let search_folder_definitions = store
        .fetch_search_folders(account_id)
        .await
        .context("fetch MAPI search folders")?;
    let search_folder_definition_ids = search_folder_definitions
        .iter()
        .map(|definition| definition.id)
        .collect::<HashSet<_>>();
    log_mapi_store_load_step(account_id, plan, "fetch associated config messages", 0);
    let associated_configs = store
        .fetch_mapi_associated_configs(account_id)
        .await
        .context("fetch MAPI associated config messages")?;
    let named_property_mappings = store
        .fetch_mapi_named_properties(account_id, None)
        .await
        .context("fetch MAPI named property mappings")?;
    let associated_config_ids = associated_configs
        .iter()
        .map(|config| config.id)
        .collect::<HashSet<_>>();

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
    let mailbox_ids = mailboxes
        .iter()
        .map(|mailbox| mailbox.id)
        .collect::<HashSet<_>>();
    let identities = identities
        .into_iter()
        .filter(|identity| {
            let has_backing_row = requested_identity_has_backing_row(
                identity,
                &mailbox_ids,
                &search_folder_definition_ids,
                &associated_config_ids,
            );
            if !has_backing_row {
                crate::mapi::identity::forget_mapi_identity(&identity.canonical_id);
            }
            has_backing_row
        })
        .collect::<Vec<_>>();
    for identity in &identities {
        crate::mapi::identity::remember_mapi_identity_with_source_key(
            identity.canonical_id,
            identity.object_id,
            Some(identity.source_key.clone()),
        );
    }

    let mut content_windows = Vec::new();
    let mut content_message_ids = Vec::new();
    for (query_index, query) in plan.content_queries.iter().enumerate() {
        let Some(mailbox_id) = mailbox_id_for_mapi_folder_id(&mailboxes, query.folder_id) else {
            continue;
        };
        log_mapi_store_load_step(account_id, plan, "query content table ids", query_index);
        // Deleted Items is a heterogeneous MAPI table: normal canonical mail
        // rows and deleted Calendar rows share one sort/cursor space. Its
        // global first K rows cannot contain a mail row after the Kth mail row,
        // so offset+limit mail candidates are sufficient before merging and
        // slicing with all Calendar candidates.
        let mixed_deleted_items = query.folder_id == TRASH_FOLDER_ID;
        let mixed_deleted_items_mail_limit = query
            .offset
            .saturating_add(query.limit)
            .min(i64::MAX as usize) as u64;
        let result = store
            .query_mapi_content_table_ids(
                account_id,
                MapiContentTableQuery {
                    mailbox_id,
                    position: if mixed_deleted_items {
                        0
                    } else {
                        query.offset as u64
                    },
                    limit: if mixed_deleted_items {
                        mixed_deleted_items_mail_limit
                    } else {
                        query.limit as u64
                    },
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
            offset: if mixed_deleted_items { 0 } else { query.offset },
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
            source_key: None,
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
        crate::mapi::identity::remember_mapi_identity_with_source_key(
            identity.canonical_id,
            identity.object_id,
            Some(identity.source_key),
        );
    }
    log_mapi_store_load_step(
        account_id,
        plan,
        "fetch content messages",
        message_ids.len(),
    );
    let emails = if message_ids.is_empty() {
        Vec::new()
    } else {
        store
            .fetch_jmap_emails(account_id, &message_ids)
            .await
            .with_context(|| format!("fetch {} MAPI content messages", message_ids.len()))?
    };
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
    let collaboration_identity_requests = mapi_store::collaboration_folder_identity_requests(
        &contact_collections,
        &calendar_collections,
        &task_collections,
    );
    log_mapi_store_load_step(
        account_id,
        plan,
        "allocate collaboration collection identities",
        collaboration_identity_requests.len(),
    );
    for identity in store
        .fetch_or_allocate_mapi_identities(account_id, &collaboration_identity_requests)
        .await
        .context("allocate MAPI collaboration collection identities")?
    {
        crate::mapi::identity::remember_mapi_identity_with_source_key(
            identity.canonical_id,
            identity.object_id,
            Some(identity.source_key),
        );
    }
    log_mapi_store_load_step(account_id, plan, "fetch conversation actions", 0);
    let conversation_actions = store
        .fetch_conversation_actions(account_id)
        .await
        .context("fetch MAPI conversation actions")?;
    let snapshot_backed_contents = requires_snapshot_backed_contents(plan, &mailboxes);
    let navigation_shortcut_ids = identities
        .iter()
        .filter(|identity| identity.object_kind == MapiIdentityObjectKind::NavigationShortcut)
        .map(|identity| identity.canonical_id)
        .collect::<Vec<_>>();
    let navigation_shortcuts = if snapshot_backed_contents || !navigation_shortcut_ids.is_empty() {
        log_mapi_store_load_step(
            account_id,
            plan,
            "fetch navigation shortcuts",
            navigation_shortcut_ids.len(),
        );
        let mut shortcuts = store
            .fetch_mapi_navigation_shortcuts(account_id)
            .await
            .context("fetch MAPI navigation shortcuts")?;
        if !snapshot_backed_contents {
            shortcuts.retain(|shortcut| navigation_shortcut_ids.contains(&shortcut.id));
        }
        shortcuts
    } else {
        Vec::new()
    };
    log_mapi_store_load_step(account_id, plan, "fetch delegate freebusy messages", 0);
    let delegate_freebusy_messages = store
        .fetch_delegate_freebusy_messages(account_id)
        .await
        .context("fetch MAPI delegate freebusy messages")?;
    log_mapi_store_load_step(account_id, plan, "fetch recoverable items", 0);
    let mut recoverable_items = Vec::new();
    for folder in ["deletions", "versions", "purges"] {
        recoverable_items.extend(
            store
                .list_recoverable_items(account_id, Some(folder))
                .await
                .with_context(|| format!("fetch MAPI recoverable items in {folder}"))?,
        );
    }
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
        if contact_ids.is_empty() {
            Vec::new()
        } else {
            store
                .fetch_accessible_contacts_by_ids(account_id, &contact_ids)
                .await
                .with_context(|| format!("fetch {} MAPI contacts by id", contact_ids.len()))?
        }
    };
    let events = if snapshot_backed_contents {
        log_mapi_store_load_step(
            account_id,
            plan,
            "fetch snapshot events",
            calendar_collections.len(),
        );
        let mut events = Vec::new();
        if calendar_collections.is_empty() {
            events.extend(
                store
                    .fetch_accessible_events_in_collection(account_id, "default")
                    .await
                    .context("fetch MAPI events in default calendar collection")?,
            );
        } else {
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
        }
        events
    } else {
        log_mapi_store_load_step(account_id, plan, "fetch events by id", event_ids.len());
        if event_ids.is_empty() {
            Vec::new()
        } else {
            store
                .fetch_accessible_events_by_ids(account_id, &event_ids)
                .await
                .with_context(|| format!("fetch {} MAPI events by id", event_ids.len()))?
        }
    };
    let needs_deleted_events = plan.object_ids.contains(&TRASH_FOLDER_ID)
        || plan
            .content_queries
            .iter()
            .any(|query| query.folder_id == TRASH_FOLDER_ID)
        || identities
            .iter()
            .any(|identity| identity.object_kind == MapiIdentityObjectKind::DeletedCalendarEvent);
    let deleted_events = if needs_deleted_events {
        store
            .fetch_accessible_deleted_events(account_id)
            .await
            .context("fetch deleted Calendar events")?
    } else {
        Vec::new()
    };
    let calendar_event_ids = events
        .iter()
        .chain(deleted_events.iter())
        .map(|event| event.id)
        .collect::<Vec<_>>();
    let calendar_attachments = if calendar_event_ids.is_empty() {
        Vec::new()
    } else {
        store
            .fetch_calendar_attachments_for_events(account_id, &calendar_event_ids)
            .await
            .context("fetch MAPI calendar attachments")?
    };
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
        if task_ids.is_empty() {
            Vec::new()
        } else {
            store
                .fetch_accessible_tasks_by_ids(account_id, &task_ids)
                .await
                .with_context(|| format!("fetch {} MAPI tasks by id", task_ids.len()))?
        }
    };
    let notes = if snapshot_backed_contents {
        log_mapi_store_load_step(account_id, plan, "fetch snapshot notes", 0);
        store
            .fetch_mapi_notes(account_id)
            .await
            .context("fetch MAPI notes")?
    } else {
        log_mapi_store_load_step(account_id, plan, "fetch notes by id", note_ids.len());
        if note_ids.is_empty() {
            Vec::new()
        } else {
            store
                .fetch_mapi_notes_by_ids(account_id, &note_ids)
                .await
                .with_context(|| format!("fetch {} MAPI notes by id", note_ids.len()))?
        }
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
        if journal_entry_ids.is_empty() {
            Vec::new()
        } else {
            store
                .fetch_mapi_journal_entries_by_ids(account_id, &journal_entry_ids)
                .await
                .with_context(|| {
                    format!(
                        "fetch {} MAPI journal entries by id",
                        journal_entry_ids.len()
                    )
                })?
        }
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
    let raw_identity_requests = contacts
        .iter()
        .map(|contact| MapiIdentityRequest {
            object_kind: MapiIdentityObjectKind::Contact,
            canonical_id: contact.id,
            reserved_global_counter: None,
            source_key: None,
        })
        .chain(crate::mapi_store::collaboration_folder_identity_requests(
            &contact_collections,
            &calendar_collections,
            &task_collections,
        ))
        .chain(events.iter().map(|event| MapiIdentityRequest {
            object_kind: MapiIdentityObjectKind::CalendarEvent,
            canonical_id: event.id,
            reserved_global_counter: None,
            source_key: None,
        }))
        .chain(deleted_events.iter().map(|event| MapiIdentityRequest {
            object_kind: MapiIdentityObjectKind::DeletedCalendarEvent,
            canonical_id: event.id,
            reserved_global_counter: None,
            source_key: None,
        }))
        .chain(tasks.iter().map(|task| MapiIdentityRequest {
            object_kind: MapiIdentityObjectKind::Task,
            canonical_id: task.id,
            reserved_global_counter: None,
            source_key: None,
        }))
        .chain(notes.iter().map(|note| MapiIdentityRequest {
            object_kind: MapiIdentityObjectKind::Note,
            canonical_id: note.id,
            reserved_global_counter: None,
            source_key: None,
        }))
        .chain(journal_entries.iter().map(|entry| MapiIdentityRequest {
            object_kind: MapiIdentityObjectKind::JournalEntry,
            canonical_id: entry.id,
            reserved_global_counter: None,
            source_key: None,
        }))
        .chain(
            search_folder_definitions
                .iter()
                .map(|definition| MapiIdentityRequest {
                    object_kind: MapiIdentityObjectKind::SearchFolderDefinition,
                    canonical_id: definition.id,
                    reserved_global_counter: None,
                    source_key: None,
                }),
        )
        .chain(
            conversation_actions
                .iter()
                .map(|action| MapiIdentityRequest {
                    object_kind: MapiIdentityObjectKind::ConversationAction,
                    canonical_id: action.id,
                    reserved_global_counter: None,
                    source_key: None,
                }),
        )
        .chain(
            navigation_shortcuts
                .iter()
                .map(|shortcut| MapiIdentityRequest {
                    object_kind: MapiIdentityObjectKind::NavigationShortcut,
                    canonical_id: shortcut.id,
                    reserved_global_counter: None,
                    source_key: None,
                }),
        )
        .chain(
            delegate_freebusy_messages
                .iter()
                .map(|message| MapiIdentityRequest {
                    object_kind: MapiIdentityObjectKind::DelegateFreeBusyMessage,
                    canonical_id: message.id,
                    reserved_global_counter: None,
                    source_key: None,
                }),
        )
        .chain(associated_configs.iter().map(|config| MapiIdentityRequest {
            object_kind: MapiIdentityObjectKind::AssociatedConfig,
            canonical_id: config.id,
            reserved_global_counter: None,
            source_key: None,
        }))
        .collect::<Vec<_>>();
    let raw_identity_request_count = raw_identity_requests.len();
    let identity_requests = deduplicate_mapi_identity_requests(raw_identity_requests);
    log_mapi_store_load_step(
        account_id,
        plan,
        "allocate non-message identities",
        identity_requests.len(),
    );
    log_mapi_identity_request_summary(
        account_id,
        plan,
        "non-message",
        raw_identity_request_count,
        &identity_requests,
    );
    let allocated_non_message_identities = store
        .fetch_or_allocate_mapi_identities(account_id, &identity_requests)
        .await
        .context("allocate MAPI non-message identities")?;
    log_mapi_store_load_step(
        account_id,
        plan,
        "allocated non-message identities",
        allocated_non_message_identities.len(),
    );
    for identity in &allocated_non_message_identities {
        crate::mapi::identity::remember_mapi_identity_with_source_key(
            identity.canonical_id,
            identity.object_id,
            Some(identity.source_key.clone()),
        );
    }
    let snapshot_identities = allocated_mailbox_identities
        .iter()
        .chain(allocated_non_message_identities.iter())
        .cloned()
        .collect::<Vec<_>>();
    let associated_config_identity_ids = snapshot_identities
        .iter()
        .filter(|identity| {
            identity.object_kind == MapiIdentityObjectKind::AssociatedConfig
                && (associated_config_ids.contains(&identity.canonical_id)
                    || mapi_store::modeled_virtual_associated_config_message_for_canonical_id(
                        identity.canonical_id,
                    )
                    .is_some())
        })
        .cloned()
        .map(|record| mapi_store::MapiAssociatedConfigIdentity { record })
        .collect::<Vec<_>>();
    let loaded_event_ids = events
        .iter()
        .chain(deleted_events.iter())
        .map(|event| event.id)
        .collect::<Vec<_>>();
    log_mapi_store_load_step(
        account_id,
        plan,
        "fetch durable MAPI event versions",
        loaded_event_ids.len(),
    );
    let event_versions = store
        .fetch_mapi_event_versions(account_id, &loaded_event_ids)
        .await
        .context("fetch durable MAPI Event versions")?;
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
        &search_folder_definitions,
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
    let snapshot = MapiMailStoreSnapshot::new_with_scoped_calendar_identities(
        mailboxes,
        emails,
        attachments,
        contact_collections,
        calendar_collections,
        task_collections,
        contacts,
        events,
        deleted_events,
        tasks,
        folder_permissions,
        &snapshot_identities,
    )?
    .with_event_versions(event_versions)
    .context("apply durable MAPI Event versions to selective snapshot")?
    .with_notes_and_journal(notes, journal_entries)
    .with_search_folder_definitions(search_folder_definitions)
    .with_conversation_actions(conversation_actions)
    .with_navigation_shortcuts(navigation_shortcuts)
    .with_navigation_shortcut_identities(&snapshot_identities)?
    .with_named_property_mappings(named_property_mappings)
    .with_associated_configs(associated_configs)
    .with_associated_config_identity_ids(associated_config_identity_ids)
    .with_delegate_freebusy_messages(delegate_freebusy_messages)
    .with_recoverable_items(recoverable_items)
    .with_reminders(reminders)
    .with_content_windows(content_windows)
    .with_calendar_attachments(calendar_attachments);
    Ok(snapshot)
}

fn requested_identity_has_backing_row(
    identity: &MapiIdentityLookupRecord,
    mailbox_ids: &HashSet<Uuid>,
    search_folder_definition_ids: &HashSet<Uuid>,
    associated_config_ids: &HashSet<Uuid>,
) -> bool {
    match identity.object_kind {
        MapiIdentityObjectKind::Mailbox => mailbox_ids.contains(&identity.canonical_id),
        MapiIdentityObjectKind::SearchFolderDefinition => {
            search_folder_definition_ids.contains(&identity.canonical_id)
        }
        MapiIdentityObjectKind::AssociatedConfig => {
            associated_config_ids.contains(&identity.canonical_id)
                || mapi_store::modeled_virtual_associated_config_message_for_canonical_id(
                    identity.canonical_id,
                )
                .is_some()
        }
        _ => true,
    }
}

fn log_mapi_store_load_step(
    account_id: Uuid,
    plan: &MapiAccessPlan,
    step: &'static str,
    item_count: usize,
) {
    if item_count > 0 {
        return;
    }
    tracing::debug!(
        rca_debug = true,
        adapter = "mapi",
        request_type = "Execute",
        account_id = %account_id,
        full_snapshot = plan.requires_full_snapshot,
        object_id_count = plan.object_ids.len(),
        object_ids = %format_mapi_object_ids(&plan.object_ids),
        content_query_count = plan.content_queries.len(),
        step = step,
        item_count = item_count,
        message = "rca debug mapi execute store load step",
    );
}

fn log_mapi_store_full_snapshot(account_id: Uuid, plan: &MapiAccessPlan) {
    tracing::debug!(
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
    if unresolved_object_ids.is_empty() {
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

    tracing::debug!(
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

fn deduplicate_mapi_identity_requests(
    requests: Vec<MapiIdentityRequest>,
) -> Vec<MapiIdentityRequest> {
    let mut deduplicated = Vec::with_capacity(requests.len());
    for request in requests {
        if !deduplicated.iter().any(|existing: &MapiIdentityRequest| {
            existing.object_kind == request.object_kind
                && existing.canonical_id == request.canonical_id
        }) {
            deduplicated.push(request);
        }
    }
    deduplicated
}

fn log_mapi_identity_request_summary(
    account_id: Uuid,
    plan: &MapiAccessPlan,
    request_set: &'static str,
    raw_count: usize,
    requests: &[MapiIdentityRequest],
) {
    let mut kind_counts = Vec::<(&'static str, usize)>::new();
    let mut reserved_counter_count = 0usize;
    let mut source_key_count = 0usize;
    for request in requests {
        let kind = mapi_identity_kind_name(request.object_kind);
        if let Some((_, count)) = kind_counts
            .iter_mut()
            .find(|(candidate_kind, _)| *candidate_kind == kind)
        {
            *count += 1;
        } else {
            kind_counts.push((kind, 1));
        }
        if request.reserved_global_counter.is_some() {
            reserved_counter_count += 1;
        }
        if request.source_key.is_some() {
            source_key_count += 1;
        }
    }
    let kind_counts = kind_counts
        .into_iter()
        .map(|(kind, count)| format!("{kind}={count}"))
        .collect::<Vec<_>>()
        .join(",");
    let sample = requests
        .iter()
        .take(12)
        .map(|request| {
            format!(
                "{}:{}",
                mapi_identity_kind_name(request.object_kind),
                request.canonical_id
            )
        })
        .collect::<Vec<_>>()
        .join("|");
    tracing::debug!(
        rca_debug = true,
        adapter = "mapi",
        request_type = "Execute",
        account_id = %account_id,
        full_snapshot = false,
        object_id_count = plan.object_ids.len(),
        content_query_count = plan.content_queries.len(),
        request_set,
        raw_request_count = raw_count,
        deduplicated_request_count = requests.len(),
        duplicate_request_count = raw_count.saturating_sub(requests.len()),
        reserved_counter_count,
        source_key_count,
        kind_counts = %kind_counts,
        request_sample = %sample,
        message = "rca debug mapi identity allocation request summary",
    );
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
    if is_advertised_special_folder(object_id) {
        return "advertised_special_folder";
    }
    if mapi_store::is_outlook_inbox_default_associated_config_id(object_id) {
        return "virtual_inbox_associated_config";
    }
    if mapi_store::is_outlook_common_views_default_navigation_shortcut_id(object_id) {
        return "virtual_common_view_navigation_shortcut";
    }
    if mapi_store::is_outlook_default_conversation_action_id(object_id) {
        return "virtual_conversation_action";
    }
    if mapi_store::is_outlook_local_freebusy_message_id(object_id) {
        return "virtual_local_freebusy_message";
    }
    if crate::mapi::identity::global_counter_from_store_id(object_id).is_some() {
        "unallocated_store_object"
    } else {
        "foreign_or_invalid_replid"
    }
}

fn is_expected_unbacked_mapi_object(object_id: u64) -> bool {
    is_advertised_special_folder(object_id)
        || mapi_store::is_outlook_inbox_default_associated_config_id(object_id)
        || mapi_store::is_outlook_common_views_default_navigation_shortcut_id(object_id)
        || mapi_store::is_outlook_default_conversation_action_id(object_id)
        || mapi_store::is_outlook_local_freebusy_message_id(object_id)
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
        MapiIdentityObjectKind::DeletedCalendarEvent => "deleted_calendar_event",
        MapiIdentityObjectKind::Task => "task",
        MapiIdentityObjectKind::Rule => "sieve_script",
        MapiIdentityObjectKind::SearchFolderDefinition => "search_folder_definition",
        MapiIdentityObjectKind::ConversationAction => "conversation_action",
        MapiIdentityObjectKind::NavigationShortcut => "navigation_shortcut",
        MapiIdentityObjectKind::AssociatedConfig => "associated_config",
        MapiIdentityObjectKind::Note => "note",
        MapiIdentityObjectKind::JournalEntry => "journal_entry",
        MapiIdentityObjectKind::DelegateFreeBusyMessage => "delegate_freebusy_message",
        MapiIdentityObjectKind::PublicFolder => "public_folder",
        MapiIdentityObjectKind::PublicFolderItem => "public_folder_item",
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
    search_folder_definitions: &[lpe_storage::SearchFolderDefinition],
    conversation_action_count: usize,
    reminder_count: usize,
    folder_permission_count: usize,
    content_window_count: usize,
    requested_calendar_event_identity_count: usize,
    default_calendar_collection_loaded: bool,
    loaded_default_calendar_event_count: usize,
) {
    tracing::debug!(
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
        search_folder_roles = %format_search_folder_roles(search_folder_definitions),
        conversation_action_count,
        reminder_count,
        folder_permission_count,
        content_window_count,
        message = "rca debug mapi execute store load summary",
    );
}

fn format_search_folder_roles(definitions: &[lpe_storage::SearchFolderDefinition]) -> String {
    definitions
        .iter()
        .map(|definition| {
            format!(
                "{}:{}:{}:{}",
                definition.role,
                definition.definition_kind,
                definition.result_object_kind,
                if definition.is_builtin {
                    "builtin"
                } else {
                    "user"
                }
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(test)]
mod tests;
