use super::*;
use crate::mapi::notifications::{MapiNotificationEvent, MapiNotificationKind};
use crate::mapi::wire::MapiNotificationEventMask;
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
        false,
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
    let mut rows = if matches!(folder_id, ROOT_FOLDER_ID | SYNC_ISSUES_FOLDER_ID) {
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
            .collect::<Vec<_>>()
    };
    if folder_id == IPM_SUBTREE_FOLDER_ID {
        rows.extend(
            snapshot
                .collaboration_folders()
                .iter()
                .filter(|folder| !collaboration_folder_shadows_outlook_special_folder(folder))
                .filter(|folder| restriction_matches_collaboration_folder(restriction, folder))
                .map(HierarchyRow::Collaboration),
        );
    }
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
    depth: bool,
) -> Vec<HierarchyRow<'a>> {
    if depth {
        // [MS-OXCFOLD] section 2.2.1.13.1: Depth lists every level below the
        // input folder, while the normal hierarchy table remains direct-only.
        let mut seen_folder_ids = HashSet::from([folder_id]);
        let mut parent_folder_ids = vec![folder_id];
        let mut rows = Vec::new();
        while let Some(parent_folder_id) = parent_folder_ids.pop() {
            for row in hierarchy_rows_excluding_deleted(
                parent_folder_id,
                mailboxes,
                snapshot,
                None,
                &[],
                mailbox_guid,
                deleted_advertised_special_folders,
            ) {
                let row_id = hierarchy_row_id(&row);
                if seen_folder_ids.insert(row_id) {
                    parent_folder_ids.push(row_id);
                    rows.push(row);
                }
            }
        }
        rows.retain(|row| hierarchy_row_matches(row, mailboxes, restriction, mailbox_guid));
        sort_hierarchy_rows(&mut rows, sort_orders);
        rows
    } else {
        hierarchy_rows_excluding_deleted(
            folder_id,
            mailboxes,
            snapshot,
            restriction,
            sort_orders,
            mailbox_guid,
            deleted_advertised_special_folders,
        )
    }
}

/// The informative hierarchy-table form is safe only when the current table
/// row remains in place. Restricted views and content-count sorts can instead
/// require an add/delete notification, so callers retain the basic fallback.
pub(in crate::mapi) struct HierarchyTableRowModified {
    pub(in crate::mapi) folder_id: u64,
    pub(in crate::mapi) insert_after_folder_id: u64,
    pub(in crate::mapi) row_data: Vec<u8>,
}

/// [MS-OXCNOTIF] sections 2.2.1.4.1.2 and 3.1.4.3: an informative hierarchy
/// table notification carries the changed folder row using that table's last
/// RopSetColumns projection.
pub(in crate::mapi) fn hierarchy_table_row_modified(
    table: &MapiObject,
    event: &MapiNotificationEvent,
    mailboxes: &[JmapMailbox],
    snapshot: &MapiMailStoreSnapshot,
    mailbox_guid: Uuid,
) -> Option<HierarchyTableRowModified> {
    // table_notifications retargets a child-content aggregate event to its
    // parent hierarchy table while retaining that parent in parent_folder_id.
    if event.kind != MapiNotificationKind::Hierarchy
        || event.parent_folder_id != Some(event.folder_id)
        || event.event_mask & 0x0FFF != MapiNotificationEventMask::TableModified.as_u16()
    {
        return None;
    }
    let changed_folder_id = event.message_id?;
    let MapiObject::HierarchyTable {
        folder_id,
        depth,
        columns,
        columns_set,
        sort_orders,
        restriction,
        deleted_advertised_special_folders,
        ..
    } = table
    else {
        return None;
    };
    if !*columns_set
        || restriction.is_some()
        || sort_orders.iter().any(|sort_order| {
            matches!(
                sort_order.property_tag,
                PID_TAG_CONTENT_COUNT | PID_TAG_CONTENT_UNREAD_COUNT
            )
        })
    {
        return None;
    }

    let rows = hierarchy_table_rows_excluding_deleted(
        *folder_id,
        mailboxes,
        snapshot,
        None,
        sort_orders,
        mailbox_guid,
        deleted_advertised_special_folders,
        *depth,
    );
    let changed_row_index = rows
        .iter()
        .position(|row| hierarchy_row_id(row) == changed_folder_id)?;
    let changed_row = *rows.get(changed_row_index)?;
    let insert_after_folder_id = changed_row_index
        .checked_sub(1)
        .and_then(|index| rows.get(index))
        .map(hierarchy_row_id)
        .unwrap_or_default();

    Some(HierarchyTableRowModified {
        folder_id: changed_folder_id,
        insert_after_folder_id,
        row_data: serialize_hierarchy_property_row(
            changed_row,
            mailboxes,
            snapshot,
            columns,
            mailbox_guid,
        ),
    })
}

pub(in crate::mapi) fn hierarchy_depth_folder_ids_excluding_deleted(
    folder_id: u64,
    mailboxes: &[JmapMailbox],
    snapshot: &MapiMailStoreSnapshot,
    deleted_advertised_special_folders: &HashSet<u64>,
) -> HashSet<u64> {
    hierarchy_table_rows_excluding_deleted(
        folder_id,
        mailboxes,
        snapshot,
        None,
        &[],
        Uuid::nil(),
        deleted_advertised_special_folders,
        true,
    )
    .into_iter()
    .map(|row| hierarchy_row_id(&row))
    .collect()
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
    if canonical_property_storage_tag(property_tag) == PID_TAG_FOLDER_FLAGS {
        return Some(MapiValue::U32(hierarchy_row_folder_flags(row, mailboxes)));
    }
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

/// [MS-OXCFOLD] section 2.2.2.2.1.5 and [MS-OXPROPS] section 2.701:
/// PidTagFolderFlags is the bitwise combination of IPM, search, and normal
/// folder state. Exchange projects the Finder container itself as normal.
pub(super) fn hierarchy_row_folder_flags(row: &HierarchyRow<'_>, mailboxes: &[JmapMailbox]) -> u32 {
    const FOLDER_FLAGS_IPM: u32 = 0x0000_0001;
    const FOLDER_FLAGS_SEARCH: u32 = 0x0000_0002;
    const FOLDER_FLAGS_NORMAL: u32 = 0x0000_0004;

    let folder_id = hierarchy_row_id(row);
    let is_ipm = match row {
        HierarchyRow::Collaboration(_) => true,
        HierarchyRow::PublicFolder(_) => false,
        HierarchyRow::Mailbox(_) | HierarchyRow::Special(_) => {
            hierarchy_folder_is_in_ipm_subtree(folder_id, mailboxes)
        }
    };
    let is_search = match row {
        HierarchyRow::Mailbox(mailbox) => folder_type(mailbox) == FOLDER_SEARCH,
        HierarchyRow::Special(folder_id) => matches!(
            *folder_id,
            SPOOLER_QUEUE_FOLDER_ID
                | REMINDERS_FOLDER_ID
                | TRACKED_MAIL_PROCESSING_FOLDER_ID
                | TODO_SEARCH_FOLDER_ID
                | CONTACTS_SEARCH_FOLDER_ID
        ),
        HierarchyRow::PublicFolder(_) | HierarchyRow::Collaboration(_) => false,
    };

    u32::from(is_ipm) * FOLDER_FLAGS_IPM
        | if is_search {
            FOLDER_FLAGS_SEARCH
        } else {
            FOLDER_FLAGS_NORMAL
        }
}

fn hierarchy_folder_is_in_ipm_subtree(folder_id: u64, mailboxes: &[JmapMailbox]) -> bool {
    let mut current_folder_id = folder_id;
    let mut visited = HashSet::new();
    loop {
        if current_folder_id == IPM_SUBTREE_FOLDER_ID {
            return true;
        }
        if matches!(
            current_folder_id,
            0 | ROOT_FOLDER_ID | PUBLIC_FOLDERS_ROOT_FOLDER_ID
        ) || !visited.insert(current_folder_id)
        {
            return false;
        }
        if let Some(mailbox) = mailboxes
            .iter()
            .find(|mailbox| try_mapi_folder_id(mailbox) == Some(current_folder_id))
        {
            current_folder_id = mapi_parent_folder_id(mailbox);
        } else if is_advertised_special_folder(current_folder_id) {
            current_folder_id = special_folder_metadata(current_folder_id).1;
        } else {
            return false;
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
    special_folder_property_value_with_change_number(
        folder_id,
        property_tag,
        mailbox_guid,
        mapi_mailstore::change_number_for_store_id(folder_id),
    )
}

pub(in crate::mapi) fn special_folder_property_value_with_change_number(
    folder_id: u64,
    property_tag: u32,
    mailbox_guid: Uuid,
    change_number: u64,
) -> Option<MapiValue> {
    let (display_name, parent_folder_id, message_class, has_subfolders) =
        special_folder_metadata(folder_id);
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
        PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_FOLDER_ACCESS)),
        PID_TAG_RIGHTS if folder_id != PUBLIC_FOLDERS_ROOT_FOLDER_ID => {
            Some(MapiValue::U32(crate::mapi::permissions::owner_rights()))
        }
        PID_TAG_EXTENDED_FOLDER_FLAGS => Some(MapiValue::Binary(extended_folder_flags_for_folder(
            folder_id,
        ))),
        PID_TAG_RETENTION_PERIOD | PID_TAG_RETENTION_FLAGS => Some(MapiValue::U32(0)),
        PID_TAG_ARCHIVE_PERIOD => None,
        PID_TAG_DEFAULT_VIEW_ENTRY_ID
            if default_view_supported_folder(folder_id, message_class) =>
        {
            default_folder_view_entry_id(mailbox_guid, folder_id, message_class)
        }
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
        PID_TAG_SERIALIZED_REPLID_GUID_MAP => Some(MapiValue::Binary(serialized_replid_guid_map())),
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
    let local_commit_time_max =
        snapshot.folder_local_commit_time_max(hierarchy_row_id(&row), mailboxes);
    if columns.iter().any(|column| {
        let column = canonical_property_storage_tag(*column);
        column == PID_TAG_FOLDER_FLAGS
            || (column == PID_TAG_LOCAL_COMMIT_TIME_MAX && local_commit_time_max.is_some())
    }) {
        let mut serialized = Vec::new();
        for column in columns {
            match canonical_property_storage_tag(*column) {
                PID_TAG_FOLDER_FLAGS => write_mapi_value(
                    &mut serialized,
                    *column,
                    &MapiValue::U32(hierarchy_row_folder_flags(&row, mailboxes)),
                ),
                PID_TAG_LOCAL_COMMIT_TIME_MAX => match local_commit_time_max {
                    Some(local_commit_time_max) => write_mapi_value(
                        &mut serialized,
                        *column,
                        &MapiValue::U64(local_commit_time_max),
                    ),
                    None => {
                        serialized.extend_from_slice(&serialize_hierarchy_row_from_backing_object(
                            row,
                            mailboxes,
                            snapshot,
                            std::slice::from_ref(column),
                            mailbox_guid,
                        ))
                    }
                },
                _ => {
                    serialized.extend_from_slice(&serialize_hierarchy_row_from_backing_object(
                        row,
                        mailboxes,
                        snapshot,
                        std::slice::from_ref(column),
                        mailbox_guid,
                    ));
                }
            }
        }
        return serialized;
    }

    serialize_hierarchy_row_from_backing_object(row, mailboxes, snapshot, columns, mailbox_guid)
}

/// [MS-OXCDATA] sections 2.8.1.2 and 2.11.5: selected hierarchy properties
/// that are absent use a FlaggedPropertyRow with MAPI_E_NOT_FOUND rather than
/// a fabricated type default. Exchange does this for PidTagFolderXViewInfoE.
pub(super) fn serialize_hierarchy_property_row(
    row: HierarchyRow<'_>,
    mailboxes: &[JmapMailbox],
    snapshot: &MapiMailStoreSnapshot,
    columns: &[u32],
    mailbox_guid: Uuid,
) -> Vec<u8> {
    let present = columns
        .iter()
        .map(|column| {
            hierarchy_row_property_is_present(&row, mailboxes, snapshot, *column, mailbox_guid)
        })
        .collect::<Vec<_>>();
    if present.iter().all(|present| *present) {
        let values = serialize_hierarchy_row(row, mailboxes, snapshot, columns, mailbox_guid);
        let mut property_row = Vec::new();
        write_query_rows_property_row(&mut property_row, columns, &values);
        return property_row;
    }

    let mut property_row = vec![1];
    for (column, present) in columns.iter().zip(present) {
        if present {
            property_row.push(0);
            let value = serialize_hierarchy_row(
                row,
                mailboxes,
                snapshot,
                std::slice::from_ref(column),
                mailbox_guid,
            );
            let mut standard = Vec::new();
            write_query_rows_property_row(&mut standard, std::slice::from_ref(column), &value);
            property_row.extend_from_slice(&standard[1..]);
        } else {
            property_row.push(0x0A);
            write_u32(&mut property_row, ROP_ERROR_NOT_FOUND);
        }
    }
    property_row
}

fn hierarchy_row_property_is_present(
    row: &HierarchyRow<'_>,
    mailboxes: &[JmapMailbox],
    snapshot: &MapiMailStoreSnapshot,
    property_tag: u32,
    mailbox_guid: Uuid,
) -> bool {
    let property_tag = canonical_property_storage_tag(property_tag);
    if matches!(
        property_tag,
        PID_TAG_DISPLAY_NAME_W
            | PID_TAG_FOLDER_ID
            | PID_TAG_PARENT_FOLDER_ID
            | PID_TAG_CONTENT_COUNT
            | PID_TAG_CONTENT_UNREAD_COUNT
            | PID_TAG_SUBFOLDERS
            | PID_TAG_FOLDER_TYPE
            | PID_TAG_FOLDER_FLAGS
            | PID_TAG_ACCESS
    ) {
        return true;
    }
    snapshot
        .folder_version(hierarchy_row_id(row))
        .and_then(|version| folder_version_property_value(version, property_tag))
        .is_some()
        || hierarchy_row_property_value(row, mailboxes, property_tag, mailbox_guid).is_some()
}

fn serialize_hierarchy_row_from_backing_object(
    row: HierarchyRow<'_>,
    mailboxes: &[JmapMailbox],
    snapshot: &MapiMailStoreSnapshot,
    columns: &[u32],
    mailbox_guid: Uuid,
) -> Vec<u8> {
    match row {
        HierarchyRow::Mailbox(mailbox) => {
            let folder_id = mapi_folder_id(mailbox);
            serialize_folder_row_with_context_and_version(
                mailbox,
                mailboxes,
                columns,
                mailbox_guid,
                snapshot.folder_version(folder_id),
            )
        }
        HierarchyRow::Collaboration(folder) => {
            serialize_collaboration_folder_row_with_context_and_version(
                folder,
                columns,
                associated_folder_message_count(folder.id, snapshot),
                snapshot.folder_version(folder.id),
            )
        }
        HierarchyRow::PublicFolder(folder) => serialize_public_folder_row(folder, columns),
        HierarchyRow::Special(folder_id)
            if matches!(folder_id, ROOT_FOLDER_ID | IPM_SUBTREE_FOLDER_ID) =>
        {
            serialize_advertised_special_folder_row_with_counts_and_version(
                folder_id,
                columns,
                mailbox_guid,
                0,
                0,
                0,
                snapshot.folder_version(folder_id),
            )
        }
        HierarchyRow::Special(folder_id) => {
            let emails = snapshot.emails();
            let content_count = folder_message_count(folder_id, mailboxes, &emails, snapshot);
            serialize_advertised_special_folder_row_with_counts_and_version(
                folder_id,
                columns,
                mailbox_guid,
                content_count,
                0,
                0,
                snapshot.folder_version(folder_id),
            )
        }
    }
}
