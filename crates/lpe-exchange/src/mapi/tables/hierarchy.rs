use super::*;
use crate::mapi_store::MapiPublicFolder;

#[derive(Clone, Copy)]
pub(super) enum HierarchyRow<'a> {
    Mailbox(&'a JmapMailbox),
    PublicFolder(&'a MapiPublicFolder),
    Collaboration(&'a MapiCollaborationFolder),
    Special(u64),
}

#[cfg(test)]
pub(super) fn hierarchy_rows<'a>(
    folder_id: u64,
    mailboxes: &'a [JmapMailbox],
    snapshot: &'a MapiMailStoreSnapshot,
    restriction: Option<&MapiRestriction>,
    sort_orders: &[MapiSortOrder],
    mailbox_guid: Uuid,
) -> Vec<HierarchyRow<'a>> {
    hierarchy_table_rows_excluding_deleted(
        folder_id,
        mailboxes,
        snapshot,
        restriction,
        sort_orders,
        mailbox_guid,
        &HashSet::new(),
    )
}

pub(super) fn hierarchy_rows_excluding_deleted<'a>(
    folder_id: u64,
    mailboxes: &'a [JmapMailbox],
    snapshot: &'a MapiMailStoreSnapshot,
    restriction: Option<&MapiRestriction>,
    sort_orders: &[MapiSortOrder],
    mailbox_guid: Uuid,
    deleted_advertised_special_folders: &HashSet<u64>,
) -> Vec<HierarchyRow<'a>> {
    if folder_id == PUBLIC_FOLDERS_ROOT_FOLDER_ID {
        let mut rows = snapshot
            .public_folders()
            .iter()
            .filter(|folder| folder.folder.parent_folder_id.is_none())
            .filter(|folder| restriction_matches_public_folder(restriction, folder))
            .map(HierarchyRow::PublicFolder)
            .collect::<Vec<_>>();
        sort_hierarchy_rows(&mut rows, sort_orders);
        return rows;
    }
    let mut rows = if folder_id == SYNC_ISSUES_FOLDER_ID {
        Vec::new()
    } else {
        mailboxes
            .iter()
            .filter(|mailbox| {
                !mailbox_shadowed_by_active_outlook_special_folder(
                    mailbox,
                    deleted_advertised_special_folders,
                )
            })
            .filter(|mailbox| mapi_folder_id(mailbox) != REMINDERS_FOLDER_ID)
            .filter(|mailbox| mapi_parent_folder_id(mailbox) == folder_id)
            .filter(|mailbox| {
                restriction_matches_mailbox_with_context_for_account(
                    restriction,
                    mailbox,
                    mailboxes,
                    mailbox_guid,
                )
            })
            .map(HierarchyRow::Mailbox)
            .chain(
                snapshot
                    .collaboration_folders()
                    .iter()
                    .filter(|folder| !collaboration_folder_shadows_outlook_special_folder(folder))
                    .filter(|folder| restriction_matches_collaboration_folder(restriction, folder))
                    .map(HierarchyRow::Collaboration),
            )
            .collect::<Vec<_>>()
    };
    let mut folder_ids = rows.iter().map(hierarchy_row_id).collect::<HashSet<_>>();
    if folder_id == ROOT_FOLDER_ID {
        for special_folder_id in ROOT_HIERARCHY_FOLDER_IDS {
            if !deleted_advertised_special_folders.contains(special_folder_id)
                && folder_ids.insert(*special_folder_id)
                && special_hierarchy_row_matches(*special_folder_id, restriction, mailbox_guid)
            {
                rows.push(HierarchyRow::Special(*special_folder_id));
            }
        }
    } else if folder_id == IPM_SUBTREE_FOLDER_ID {
        for special_folder_id in IPM_SUBTREE_HIERARCHY_FOLDER_IDS {
            if !deleted_advertised_special_folders.contains(special_folder_id)
                && folder_ids.insert(*special_folder_id)
                && special_hierarchy_row_matches(*special_folder_id, restriction, mailbox_guid)
            {
                rows.push(HierarchyRow::Special(*special_folder_id));
            }
        }
    } else if folder_id == SEARCH_FOLDER_ID {
        for special_folder_id in SEARCH_HIERARCHY_FOLDER_IDS {
            if !deleted_advertised_special_folders.contains(special_folder_id)
                && folder_ids.insert(*special_folder_id)
                && special_hierarchy_row_matches(*special_folder_id, restriction, mailbox_guid)
            {
                rows.push(HierarchyRow::Special(*special_folder_id));
            }
        }
    } else if snapshot.public_folder_for_id(folder_id).is_some() {
        rows =
            snapshot
                .public_folders()
                .iter()
                .filter(|folder| {
                    folder.folder.parent_folder_id.and_then(|parent_id| {
                        crate::mapi::identity::mapped_mapi_object_id(&parent_id)
                    }) == Some(folder_id)
                })
                .filter(|folder| restriction_matches_public_folder(restriction, folder))
                .map(HierarchyRow::PublicFolder)
                .collect::<Vec<_>>();
    }
    sort_hierarchy_rows(&mut rows, sort_orders);
    rows
}

pub(super) fn hierarchy_table_rows_excluding_deleted<'a>(
    folder_id: u64,
    mailboxes: &'a [JmapMailbox],
    snapshot: &'a MapiMailStoreSnapshot,
    restriction: Option<&MapiRestriction>,
    sort_orders: &[MapiSortOrder],
    mailbox_guid: Uuid,
    deleted_advertised_special_folders: &HashSet<u64>,
) -> Vec<HierarchyRow<'a>> {
    let mut rows = hierarchy_rows_excluding_deleted(
        folder_id,
        mailboxes,
        snapshot,
        restriction,
        sort_orders,
        mailbox_guid,
        deleted_advertised_special_folders,
    );
    if folder_id != IPM_SUBTREE_FOLDER_ID {
        rows.retain(|row| !matches!(row, HierarchyRow::Collaboration(_)));
    }
    rows
}

const ROOT_HIERARCHY_FOLDER_IDS: &[u64] = &[
    DEFERRED_ACTION_FOLDER_ID,
    SEARCH_FOLDER_ID,
    REMINDERS_FOLDER_ID,
    TRACKED_MAIL_PROCESSING_FOLDER_ID,
    TODO_SEARCH_FOLDER_ID,
    COMMON_VIEWS_FOLDER_ID,
    SCHEDULE_FOLDER_ID,
    VIEWS_FOLDER_ID,
    SHORTCUTS_FOLDER_ID,
    IPM_SUBTREE_FOLDER_ID,
    SPOOLER_QUEUE_FOLDER_ID,
    FREEBUSY_DATA_FOLDER_ID,
    DOCUMENT_LIBRARIES_FOLDER_ID,
];

const IPM_SUBTREE_HIERARCHY_FOLDER_IDS: &[u64] = &[
    INBOX_FOLDER_ID,
    DRAFTS_FOLDER_ID,
    OUTBOX_FOLDER_ID,
    SENT_FOLDER_ID,
    TRASH_FOLDER_ID,
    CONTACTS_FOLDER_ID,
    SUGGESTED_CONTACTS_FOLDER_ID,
    CALENDAR_FOLDER_ID,
    JOURNAL_FOLDER_ID,
    NOTES_FOLDER_ID,
    TASKS_FOLDER_ID,
    SYNC_ISSUES_FOLDER_ID,
    JUNK_FOLDER_ID,
    RSS_FEEDS_FOLDER_ID,
    ARCHIVE_FOLDER_ID,
];

const SEARCH_HIERARCHY_FOLDER_IDS: &[u64] = &[CONTACTS_SEARCH_FOLDER_ID];

fn sort_hierarchy_rows(rows: &mut [HierarchyRow<'_>], sort_orders: &[MapiSortOrder]) {
    if sort_orders.is_empty() {
        return;
    }
    rows.sort_by(|left, right| {
        for sort_order in sort_orders {
            let ordering = match sort_order.property_tag {
                PID_TAG_DISPLAY_NAME_W => compare_case_insensitive(
                    hierarchy_row_display_name(left),
                    hierarchy_row_display_name(right),
                ),
                PID_TAG_CONTENT_COUNT => {
                    hierarchy_row_content_count(left).cmp(&hierarchy_row_content_count(right))
                }
                PID_TAG_CONTENT_UNREAD_COUNT => {
                    hierarchy_row_unread_count(left).cmp(&hierarchy_row_unread_count(right))
                }
                PID_TAG_FOLDER_ID => hierarchy_row_id(left).cmp(&hierarchy_row_id(right)),
                _ => Ordering::Equal,
            };
            let ordering = apply_sort_direction(ordering, sort_order.order);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        hierarchy_row_id(left).cmp(&hierarchy_row_id(right))
    });
}

pub(super) fn hierarchy_row_display_name<'a>(row: &'a HierarchyRow<'a>) -> &'a str {
    match row {
        HierarchyRow::Mailbox(mailbox) if mailbox.role == "conversation_history" => {
            "Conversation History"
        }
        HierarchyRow::Mailbox(mailbox) => &mailbox.name,
        HierarchyRow::PublicFolder(folder) => &folder.folder.display_name,
        HierarchyRow::Collaboration(folder) => &folder.collection.display_name,
        HierarchyRow::Special(folder_id) => special_folder_metadata(*folder_id).0,
    }
}

pub(in crate::mapi) fn mailbox_shadowed_by_active_outlook_special_folder(
    mailbox: &JmapMailbox,
    deleted_advertised_special_folders: &HashSet<u64>,
) -> bool {
    if mapi_parent_folder_id(mailbox) != IPM_SUBTREE_FOLDER_ID {
        return false;
    }

    let shadows = matches!(
        mailbox.name.trim().to_ascii_lowercase().as_str(),
        "archive"
            | "calendar"
            | "conflicts"
            | "contacts"
            | "contacts search"
            | "conversation history"
            | "conversation action settings"
            | "drafts"
            | "im contact list"
            | "journal"
            | "junk e-mail"
            | "local failures"
            | "notes"
            | "quick contacts"
            | "quick step settings"
            | "rss feeds"
            | "server failures"
            | "suggested contacts"
            | "sync issues"
            | "tasks"
    );
    if !shadows {
        return false;
    }
    advertised_special_folder_id_for_create(IPM_SUBTREE_FOLDER_ID, mailbox.name.trim())
        .map(|folder_id| !deleted_advertised_special_folders.contains(&folder_id))
        .unwrap_or(true)
}

fn collaboration_folder_shadows_outlook_special_folder(folder: &MapiCollaborationFolder) -> bool {
    let display_name = folder.collection.display_name.trim().to_ascii_lowercase();
    match folder.kind {
        MapiCollaborationFolderKind::Contacts => matches!(
            display_name.as_str(),
            "contacts"
                | "suggested contacts"
                | "quick contacts"
                | "im contact list"
                | "contacts search"
        ),
        MapiCollaborationFolderKind::Calendar => display_name == "calendar",
        MapiCollaborationFolderKind::Task => display_name == "tasks",
    }
}

fn hierarchy_row_content_count(row: &HierarchyRow<'_>) -> u32 {
    match row {
        HierarchyRow::Mailbox(mailbox) => mailbox.total_emails,
        HierarchyRow::PublicFolder(folder) => folder.item_count,
        HierarchyRow::Collaboration(folder) => folder.item_count,
        HierarchyRow::Special(_) => 0,
    }
}

fn hierarchy_row_unread_count(row: &HierarchyRow<'_>) -> u32 {
    match row {
        HierarchyRow::Mailbox(mailbox) => mailbox.unread_emails,
        HierarchyRow::PublicFolder(_)
        | HierarchyRow::Collaboration(_)
        | HierarchyRow::Special(_) => 0,
    }
}

pub(super) fn hierarchy_row_id(row: &HierarchyRow<'_>) -> u64 {
    match row {
        HierarchyRow::Mailbox(mailbox) => mapi_folder_id(mailbox),
        HierarchyRow::PublicFolder(folder) => folder.id,
        HierarchyRow::Collaboration(folder) => folder.id,
        HierarchyRow::Special(folder_id) => *folder_id,
    }
}

pub(super) fn hierarchy_row_parent_id(row: &HierarchyRow<'_>, _mailboxes: &[JmapMailbox]) -> u64 {
    match row {
        HierarchyRow::Mailbox(mailbox) => mapi_parent_folder_id(mailbox),
        HierarchyRow::PublicFolder(folder) => folder
            .folder
            .parent_folder_id
            .and_then(|parent_id| crate::mapi::identity::mapped_mapi_object_id(&parent_id))
            .unwrap_or(PUBLIC_FOLDERS_ROOT_FOLDER_ID),
        HierarchyRow::Collaboration(_) => IPM_SUBTREE_FOLDER_ID,
        HierarchyRow::Special(folder_id) => special_folder_metadata(*folder_id).1,
    }
}

pub(super) fn hierarchy_row_property_value(
    row: &HierarchyRow<'_>,
    mailboxes: &[JmapMailbox],
    property_tag: u32,
    mailbox_guid: Uuid,
) -> Option<MapiValue> {
    match row {
        HierarchyRow::Mailbox(mailbox) => mailbox_property_value_with_context_for_account(
            mailbox,
            mailboxes,
            property_tag,
            mailbox_guid,
        ),
        HierarchyRow::PublicFolder(folder) => public_folder_property_value(folder, property_tag),
        HierarchyRow::Collaboration(folder) => {
            collaboration_folder_property_value(folder, property_tag)
        }
        HierarchyRow::Special(folder_id) => {
            special_folder_property_value(*folder_id, property_tag, mailbox_guid)
        }
    }
}

pub(super) fn hierarchy_row_expected_container_class<'a>(
    row: &'a HierarchyRow<'a>,
) -> Option<&'a str> {
    match row {
        HierarchyRow::Collaboration(folder) => {
            Some(collaboration_folder_message_class(folder.kind))
        }
        HierarchyRow::Special(folder_id) => debug_expected_container_class(*folder_id),
        HierarchyRow::Mailbox(mailbox) => Some(folder_message_class(mailbox)),
        HierarchyRow::PublicFolder(folder) => Some(folder.folder.folder_class.as_str()),
    }
}

pub(super) fn hierarchy_row_matches(
    row: &HierarchyRow<'_>,
    mailboxes: &[JmapMailbox],
    restriction: Option<&MapiRestriction>,
    mailbox_guid: Uuid,
) -> bool {
    match row {
        HierarchyRow::Mailbox(mailbox) => restriction_matches_mailbox_with_context_for_account(
            restriction,
            mailbox,
            mailboxes,
            mailbox_guid,
        ),
        HierarchyRow::Collaboration(folder) => {
            restriction_matches_collaboration_folder(restriction, folder)
        }
        HierarchyRow::PublicFolder(folder) => {
            restriction_matches_public_folder(restriction, folder)
        }
        HierarchyRow::Special(folder_id) => {
            special_hierarchy_row_matches(*folder_id, restriction, mailbox_guid)
        }
    }
}

pub(super) fn special_hierarchy_row_matches(
    folder_id: u64,
    restriction: Option<&MapiRestriction>,
    mailbox_guid: Uuid,
) -> bool {
    restriction_matches(restriction, |property_tag| {
        special_folder_property_value(folder_id, property_tag, mailbox_guid)
    })
}

pub(super) fn log_sync_issues_hierarchy_query_rows(
    request: &RopRequest,
    folder_id: u64,
    columns: &[u32],
    restriction: Option<&MapiRestriction>,
    sort_orders: &[MapiSortOrder],
    position: usize,
    rows: &[HierarchyRow<'_>],
    _mailbox_guid: Uuid,
) {
    if folder_id != SYNC_ISSUES_FOLDER_ID {
        return;
    }
    let requested_row_count = request.query_row_count().unwrap_or(rows.len());
    let selected_indexes = selected_row_indexes(
        rows.len(),
        position,
        request.query_forward_read(),
        requested_row_count,
    );
    let selected_row_summary = selected_indexes
        .iter()
        .map(|index| {
            let row = &rows[*index];
            let row_id = hierarchy_row_id(row);
            format!(
                "index={index}:folder_id=0x{row_id:016x}:display_name={}:parent=0x{:016x}",
                hierarchy_row_display_name(row),
                hierarchy_row_parent_id(row, &[])
            )
        })
        .collect::<Vec<_>>()
        .join("|");
    let child_candidate_summary = "suppressed_until_backed";

    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        request_type = "Execute",
        request_rop_id = "0x15",
        folder_id = %format!("0x{folder_id:016x}"),
        folder_role = "sync_issues",
        current_position = position,
        requested_forward_read = request.query_forward_read(),
        requested_row_count,
        requested_no_advance = request.query_no_advance(),
        table_total_row_count = rows.len(),
        selected_row_count = selected_indexes.len(),
        selected_row_summary = %selected_row_summary,
        child_candidate_summary = %child_candidate_summary,
        table_has_restriction = restriction.is_some(),
        table_sort_order_count = sort_orders.len(),
        selected_property_tag_count = columns.len(),
        selected_property_tags = %columns
            .iter()
            .map(|tag| format!("0x{tag:08x}"))
            .collect::<Vec<_>>()
            .join(","),
        "rca debug mapi sync issues hierarchy query rows"
    );
}

pub(in crate::mapi) fn special_folder_property_value(
    folder_id: u64,
    property_tag: u32,
    mailbox_guid: Uuid,
) -> Option<MapiValue> {
    let (display_name, parent_folder_id, message_class, has_subfolders) =
        special_folder_metadata(folder_id);
    let change_number = mapi_mailstore::change_number_for_store_id(folder_id);
    match canonical_property_storage_tag(property_tag) {
        PID_TAG_DISPLAY_NAME_W => Some(MapiValue::String(display_name.to_string())),
        PID_TAG_ENTRY_ID => {
            crate::mapi::identity::folder_entry_id_from_object_id(mailbox_guid, folder_id)
                .map(MapiValue::Binary)
        }
        PID_TAG_RECORD_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
            folder_id,
        ))),
        PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
            crate::mapi::identity::instance_key_for_object_id(folder_id),
        )),
        PID_TAG_FOLDER_ID => Some(MapiValue::U64(folder_id)),
        PID_TAG_PARENT_FOLDER_ID => Some(MapiValue::U64(parent_folder_id)),
        PID_TAG_FOLDER_TYPE => Some(MapiValue::U32(special_folder_type(folder_id))),
        PID_TAG_CONTENT_COUNT | PID_TAG_CONTENT_UNREAD_COUNT | PID_TAG_DELETED_COUNT_TOTAL => {
            Some(MapiValue::U32(0))
        }
        PID_TAG_ACCESS | PID_TAG_RIGHTS => Some(MapiValue::U32(MAPI_FOLDER_ACCESS)),
        PID_TAG_EXTENDED_FOLDER_FLAGS => Some(MapiValue::Binary(extended_folder_flags_for_folder(
            folder_id,
        ))),
        PID_TAG_RETENTION_PERIOD | PID_TAG_RETENTION_FLAGS | PID_TAG_ARCHIVE_PERIOD => {
            Some(MapiValue::U32(0))
        }
        PID_TAG_FOLDER_FORM_FLAGS | PID_TAG_FOLDER_VIEWS_ONLY | PID_TAG_FOLDER_VIEWLIST_FLAGS => {
            Some(MapiValue::U32(0))
        }
        PID_TAG_DEFAULT_FORM_NAME_W => Some(MapiValue::String(String::new())),
        PID_TAG_DEFAULT_VIEW_ENTRY_ID
            if default_view_supported_folder(folder_id, message_class) =>
        {
            default_folder_view_entry_id(mailbox_guid, folder_id, message_class)
        }
        PID_TAG_FOLDER_FORM_STORAGE => Some(MapiValue::Binary(Vec::new())),
        PID_TAG_SUBFOLDERS => Some(MapiValue::Bool(
            has_subfolders && folder_id != SYNC_ISSUES_FOLDER_ID,
        )),
        PID_TAG_ATTRIBUTE_HIDDEN => Some(MapiValue::Bool(matches!(
            folder_id,
            CONVERSATION_ACTION_SETTINGS_FOLDER_ID | QUICK_STEP_SETTINGS_FOLDER_ID
        ))),
        PID_TAG_CONTAINER_CLASS_W | PID_TAG_MESSAGE_CLASS_W if message_class.is_empty() => None,
        PID_TAG_CONTAINER_CLASS_W | PID_TAG_MESSAGE_CLASS_W => {
            Some(MapiValue::String(message_class.to_string()))
        }
        PID_TAG_DEFAULT_POST_MESSAGE_CLASS_STRING8 | PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W => {
            default_post_message_class_for_container_class(message_class)
                .map(|default_class| MapiValue::String(default_class.to_string()))
        }
        PID_TAG_LAST_MODIFICATION_TIME
        | PID_TAG_LOCAL_COMMIT_TIME
        | PID_TAG_LOCAL_COMMIT_TIME_MAX
        | PID_TAG_HIER_REV => Some(MapiValue::I64(mapi_mailstore::filetime_from_change_number(
            change_number,
        ) as i64)),
        PID_TAG_HIERARCHY_CHANGE_NUMBER => {
            Some(MapiValue::U32(change_number.min(u64::from(u32::MAX)) as u32))
        }
        PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
            folder_id,
        ))),
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(parent_folder_id),
        )),
        PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::change_key_for_change_number(change_number),
        )),
        PID_TAG_PREDECESSOR_CHANGE_LIST => Some(MapiValue::Binary(
            mapi_mailstore::predecessor_change_list(change_number),
        )),
        PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(change_number)),
        _ if folder_id == INBOX_FOLDER_ID => {
            special_folder_identification_property_value(mailbox_guid, property_tag)
        }
        _ => None,
    }
}

pub(super) fn serialize_hierarchy_row(
    row: HierarchyRow<'_>,
    mailboxes: &[JmapMailbox],
    snapshot: &MapiMailStoreSnapshot,
    columns: &[u32],
    mailbox_guid: Uuid,
) -> Vec<u8> {
    match row {
        HierarchyRow::Mailbox(mailbox) => {
            serialize_folder_row_with_context(mailbox, mailboxes, columns, mailbox_guid)
        }
        HierarchyRow::Collaboration(folder) => serialize_collaboration_folder_row_with_context(
            folder,
            columns,
            associated_folder_message_count(folder.id, snapshot),
        ),
        HierarchyRow::PublicFolder(folder) => serialize_public_folder_row(folder, columns),
        HierarchyRow::Special(folder_id)
            if matches!(folder_id, ROOT_FOLDER_ID | IPM_SUBTREE_FOLDER_ID) =>
        {
            serialize_advertised_special_folder_row_with_mailbox_guid(
                folder_id,
                columns,
                mailbox_guid,
            )
        }
        HierarchyRow::Special(folder_id) => {
            let emails = snapshot.emails();
            let content_count = folder_message_count(folder_id, mailboxes, &emails, snapshot);
            serialize_advertised_special_folder_row_with_counts(
                folder_id,
                columns,
                mailbox_guid,
                content_count,
                0,
                0,
            )
        }
    }
}
