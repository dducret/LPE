use super::permissions::*;
use super::properties::*;
use super::rop::*;
use super::session::*;
use super::sync::*;
use super::wire::MapiPropertyType;
use super::*;
use crate::mapi::identity::{
    CONVERSATION_MEMBERS_CONTENTS_TABLE_ID, QUICK_STEP_SETTINGS_FOLDER_ID,
    RECOVERABLE_ITEMS_DELETIONS_FOLDER_ID, RECOVERABLE_ITEMS_PURGES_FOLDER_ID,
    RECOVERABLE_ITEMS_ROOT_FOLDER_ID, RECOVERABLE_ITEMS_VERSIONS_FOLDER_ID,
};
use crate::mapi_store::{
    MapiAssociatedConfigMessage, MapiCommonViewNamedViewMessage, MapiCommonViewsMessage,
    MapiConversationActionMessage, MapiDelegateFreeBusyMessage, MapiEvent, MapiMessage,
    MapiNavigationShortcutMessage, MapiPublicFolder, MapiPublicFolderItem, MapiTask,
};
use lpe_storage::SearchFolderDefinition;

mod attachments;
mod collaboration_items;
mod collapse;
mod columns;
mod contents;
mod diagnostics;
mod flags;
mod folders;
mod pending;
mod public_folders;
mod recipients;
mod recoverable_items;
mod row_codecs;
mod rules;
mod search_folders;
mod sorting;
mod state;
mod time;

pub(super) use attachments::*;
pub(in crate::mapi) use collaboration_items::*;
pub(in crate::mapi) use collapse::*;
pub(super) use columns::*;
use contents::*;
use diagnostics::*;
pub(in crate::mapi) use flags::*;
pub(in crate::mapi) use folders::*;
pub(in crate::mapi) use pending::*;
pub(in crate::mapi) use public_folders::*;
pub(in crate::mapi) use recipients::*;
pub(in crate::mapi) use recoverable_items::*;
pub(super) use row_codecs::*;
pub(super) use rules::*;
use search_folders::*;
pub(in crate::mapi) use sorting::*;
pub(in crate::mapi) use state::*;
pub(in crate::mapi) use time::*;

pub(in crate::mapi) fn hierarchy_row_count_excluding_deleted(
    folder_id: u64,
    mailboxes: &[JmapMailbox],
    snapshot: &MapiMailStoreSnapshot,
    deleted_advertised_special_folders: &HashSet<u64>,
) -> u32 {
    if is_queryable_hierarchy_folder(folder_id)
        || snapshot.public_folder_for_id(folder_id).is_some()
    {
        hierarchy_rows_excluding_deleted(
            folder_id,
            mailboxes,
            snapshot,
            None,
            &[],
            Uuid::nil(),
            deleted_advertised_special_folders,
        )
        .len()
        .min(u32::MAX as usize) as u32
    } else {
        0
    }
}

pub(in crate::mapi) fn folder_message_count(
    folder_id: u64,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> u32 {
    if folder_id == CONVERSATION_MEMBERS_CONTENTS_TABLE_ID {
        return emails.len().min(u32::MAX as usize) as u32;
    }
    if let Some(folder) = snapshot.collaboration_folder_for_id(folder_id) {
        return match folder.kind {
            MapiCollaborationFolderKind::Contacts => snapshot
                .contacts_for_folder(folder_id)
                .len()
                .min(u32::MAX as usize) as u32,
            MapiCollaborationFolderKind::Calendar => snapshot
                .events_for_folder(folder_id)
                .len()
                .min(u32::MAX as usize) as u32,
            MapiCollaborationFolderKind::Task => snapshot
                .tasks_for_folder(folder_id)
                .len()
                .min(u32::MAX as usize) as u32,
        };
    }
    if folder_id == CALENDAR_FOLDER_ID {
        return snapshot
            .events_for_folder(folder_id)
            .len()
            .min(u32::MAX as usize) as u32;
    }
    if folder_id == CONTACTS_SEARCH_FOLDER_ID {
        return snapshot
            .contacts_search_results()
            .len()
            .min(u32::MAX as usize) as u32;
    }
    if folder_id == FREEBUSY_DATA_FOLDER_ID {
        return snapshot
            .delegate_freebusy_messages()
            .len()
            .min(u32::MAX as usize) as u32;
    }
    if is_contact_contents_folder(folder_id) {
        return snapshot
            .contacts_for_folder(folder_id)
            .len()
            .min(u32::MAX as usize) as u32;
    }
    if let Some(folder) = snapshot.public_folder_for_id(folder_id) {
        return folder.item_count;
    }
    if folder_id == REMINDERS_FOLDER_ID {
        let count = snapshot.reminder_tasks().len() + snapshot.reminder_messages().len();
        return count.min(u32::MAX as usize) as u32;
    }
    if crate::mapi_store::recoverable_storage_folder(folder_id).is_some() {
        return snapshot
            .recoverable_items_for_folder(folder_id)
            .len()
            .min(u32::MAX as usize) as u32;
    }
    folder_row_for_id(folder_id, mailboxes)
        .map(|mailbox| mailbox.total_emails)
        .unwrap_or_else(|| emails_for_folder(folder_id, mailboxes, emails).len() as u32)
}

fn is_contact_contents_folder(folder_id: u64) -> bool {
    matches!(
        folder_id,
        CONTACTS_FOLDER_ID | SUGGESTED_CONTACTS_FOLDER_ID | QUICK_CONTACTS_FOLDER_ID
    )
}

pub(in crate::mapi) fn associated_folder_message_count(
    folder_id: u64,
    snapshot: &MapiMailStoreSnapshot,
) -> u32 {
    if folder_id == COMMON_VIEWS_FOLDER_ID {
        snapshot
            .common_views_table_messages()
            .count()
            .min(u32::MAX as usize) as u32
    } else if folder_id == CONVERSATION_ACTION_SETTINGS_FOLDER_ID {
        snapshot
            .conversation_action_table_messages()
            .len()
            .min(u32::MAX as usize) as u32
    } else if folder_id == FREEBUSY_DATA_FOLDER_ID {
        snapshot
            .delegate_freebusy_messages()
            .len()
            .min(u32::MAX as usize) as u32
    } else if has_associated_table_rows(folder_id, snapshot) {
        associated_table_rows(folder_id, snapshot, None, Uuid::nil())
            .len()
            .min(u32::MAX as usize) as u32
    } else if snapshot
        .collaboration_folder_for_id(folder_id)
        .is_some_and(|folder| folder.kind == MapiCollaborationFolderKind::Calendar)
    {
        0
    } else {
        0
    }
}

pub(in crate::mapi) fn restricted_associated_folder_message_count(
    folder_id: u64,
    snapshot: &MapiMailStoreSnapshot,
    restriction: Option<&MapiRestriction>,
    mailbox_guid: Uuid,
) -> usize {
    if folder_id == COMMON_VIEWS_FOLDER_ID {
        snapshot
            .common_views_table_messages()
            .filter(|message| {
                restriction_matches_common_views_message(restriction, message, mailbox_guid)
            })
            .count()
    } else if folder_id == CONVERSATION_ACTION_SETTINGS_FOLDER_ID {
        snapshot
            .conversation_action_table_messages()
            .iter()
            .filter(|message| {
                restriction_matches(restriction, |property_tag| {
                    conversation_action_property_value(message, property_tag)
                })
            })
            .count()
    } else if folder_id == FREEBUSY_DATA_FOLDER_ID {
        snapshot
            .delegate_freebusy_messages()
            .iter()
            .filter(|message| {
                restriction_matches(restriction, |property_tag| {
                    delegate_freebusy_property_value(message, property_tag)
                })
            })
            .count()
    } else {
        associated_table_rows(folder_id, snapshot, restriction, mailbox_guid).len()
    }
}

#[derive(Clone)]
enum AssociatedTableRow {
    Config(MapiAssociatedConfigMessage),
    NamedView(MapiCommonViewNamedViewMessage),
}

fn has_associated_table_rows(folder_id: u64, snapshot: &MapiMailStoreSnapshot) -> bool {
    !snapshot
        .associated_config_messages_for_folder(folder_id)
        .is_empty()
        || default_folder_associated_named_view(snapshot, folder_id).is_some()
}

fn should_use_associated_config_table(
    folder_id: u64,
    snapshot: &MapiMailStoreSnapshot,
    restriction: Option<&MapiRestriction>,
) -> bool {
    if has_associated_table_rows(folder_id, snapshot) {
        return true;
    }
    if folder_id == INBOX_FOLDER_ID
        && (restriction.is_none() || is_broad_outlook_configuration_restriction(restriction))
    {
        return true;
    }
    folder_id == INBOX_FOLDER_ID
        && exact_message_class_restriction_value(restriction)
            .and_then(
                crate::mapi_store::outlook_inbox_exact_virtual_associated_config_for_message_class,
            )
            .is_some()
}

fn associated_table_rows(
    folder_id: u64,
    snapshot: &MapiMailStoreSnapshot,
    restriction: Option<&MapiRestriction>,
    _mailbox_guid: Uuid,
) -> Vec<AssociatedTableRow> {
    let mut config_messages = snapshot.associated_config_messages_for_folder(folder_id);
    append_exact_virtual_inbox_associated_config(folder_id, restriction, &mut config_messages);
    let mut rows = config_messages
        .into_iter()
        .filter(|message| {
            restriction_matches_associated_config(restriction, message)
                && associated_config_visible_in_table(folder_id, restriction, message)
        })
        .map(AssociatedTableRow::Config)
        .collect::<Vec<_>>();
    if let Some(message) = default_folder_associated_named_view(snapshot, folder_id) {
        if restriction_matches_common_view_named_view(restriction, &message, _mailbox_guid) {
            rows.push(AssociatedTableRow::NamedView(message));
        }
    }
    rows
}

fn default_folder_associated_named_view(
    snapshot: &MapiMailStoreSnapshot,
    folder_id: u64,
) -> Option<MapiCommonViewNamedViewMessage> {
    let container_class = snapshot
        .collaboration_folder_for_id(folder_id)
        .map(|folder| collaboration_folder_message_class(folder.kind))
        .or_else(|| {
            let (_, _, container_class, _) = special_folder_metadata(folder_id);
            (!container_class.is_empty()).then_some(container_class)
        })?;
    default_view_supported_folder(folder_id, container_class)
        .then(|| {
            snapshot.default_folder_named_view_message(
                folder_id,
                crate::mapi_store::OUTLOOK_DEFAULT_FOLDER_NAMED_VIEW_ID,
            )
        })
        .flatten()
}

fn append_exact_virtual_inbox_associated_config(
    folder_id: u64,
    restriction: Option<&MapiRestriction>,
    messages: &mut Vec<MapiAssociatedConfigMessage>,
) {
    if folder_id != INBOX_FOLDER_ID {
        return;
    }
    if restriction.is_none() || is_broad_outlook_configuration_restriction(restriction) {
        append_modeled_inbox_broad_startup_configs(messages);
    }
    let Some(message_class) = exact_message_class_restriction_value(restriction) else {
        return;
    };
    let Some(message) =
        crate::mapi_store::outlook_inbox_exact_virtual_associated_config_for_message_class(
            message_class,
        )
    else {
        return;
    };
    if !messages.iter().any(|existing| {
        existing
            .message_class
            .eq_ignore_ascii_case(&message.message_class)
    }) {
        messages.push(message);
    }
}

fn append_modeled_inbox_broad_startup_configs(messages: &mut Vec<MapiAssociatedConfigMessage>) {
    for message in crate::mapi_store::outlook_inbox_broad_startup_associated_config_defaults() {
        if !messages.iter().any(|existing| {
            existing.id == message.id
                && existing
                    .message_class
                    .eq_ignore_ascii_case(&message.message_class)
        }) {
            messages.push(message);
        }
    }
}

fn exact_message_class_restriction_value(restriction: Option<&MapiRestriction>) -> Option<&str> {
    match restriction? {
        MapiRestriction::Property {
            relop: 0x04,
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            value: MapiValue::String(value),
        }
        | MapiRestriction::Content {
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            value,
            ..
        } => Some(value.as_str()),
        _ => None,
    }
}

fn is_broad_outlook_configuration_restriction(restriction: Option<&MapiRestriction>) -> bool {
    restriction.is_some_and(is_broad_outlook_configuration_find_row)
}

pub(in crate::mapi) fn restriction_matches_common_views_message(
    restriction: Option<&MapiRestriction>,
    message: &MapiCommonViewsMessage,
    mailbox_guid: Uuid,
) -> bool {
    match message {
        MapiCommonViewsMessage::NavigationShortcut(shortcut) => {
            restriction_matches_navigation_shortcut(restriction, shortcut, mailbox_guid)
        }
        MapiCommonViewsMessage::NamedView(view) => {
            restriction_matches_common_view_named_view(restriction, view, mailbox_guid)
        }
        MapiCommonViewsMessage::SearchFolderDefinition(definition) => {
            restriction_matches(restriction, |property_tag| {
                search_folder_definition_message_property_value(
                    definition,
                    mailbox_guid,
                    property_tag,
                )
            })
        }
    }
}

pub(in crate::mapi) fn calendar_content_rows<'a>(
    snapshot: &'a MapiMailStoreSnapshot,
    folder_id: u64,
    restriction: Option<&MapiRestriction>,
) -> Vec<&'a MapiEvent> {
    let mut rows = snapshot.events_for_folder(folder_id);
    rows.retain(|event| restriction_matches_event(restriction, event));
    rows
}

fn restriction_matches_event(restriction: Option<&MapiRestriction>, event: &MapiEvent) -> bool {
    restriction_matches(restriction, |property_tag| {
        event_property_value(&event.event, event.id, event.folder_id, property_tag)
    })
}

fn restriction_matches_email_in_snapshot(
    restriction: Option<&MapiRestriction>,
    email: &JmapEmail,
    folder_id: u64,
    snapshot: &MapiMailStoreSnapshot,
) -> bool {
    restriction_matches_email_with_attachments(
        restriction,
        email,
        snapshot
            .attachments_for_message(folder_id, mapi_message_id(email))
            .unwrap_or_default(),
    )
}

fn restriction_matches_conversation_member_in_snapshot(
    restriction: Option<&MapiRestriction>,
    email: &JmapEmail,
    snapshot: &MapiMailStoreSnapshot,
) -> bool {
    restriction_matches_email_in_snapshot(
        restriction,
        email,
        mapi_folder_id_for_email(email),
        snapshot,
    )
}

fn restriction_matches_public_folder_item(
    restriction: Option<&MapiRestriction>,
    item: &MapiPublicFolderItem,
) -> bool {
    restriction_matches(restriction, |property_tag| {
        public_folder_item_property_value(item, property_tag)
    })
}

fn is_top_level_count_restriction(restriction: Option<&MapiRestriction>) -> bool {
    matches!(restriction, Some(MapiRestriction::Count { .. }))
}

fn retain_rows_by_restriction<T>(
    rows: &mut Vec<T>,
    restriction: Option<&MapiRestriction>,
    mut matches_restriction: impl FnMut(&T, Option<&MapiRestriction>) -> bool,
) {
    if let Some(MapiRestriction::Count { count, child }) = restriction {
        let mut remaining = *count as usize;
        rows.retain(|row| {
            if remaining == 0 || !matches_restriction(row, Some(child)) {
                return false;
            }
            remaining -= 1;
            true
        });
    } else {
        rows.retain(|row| matches_restriction(row, restriction));
    }
}

const TABLE_LEAF_ROW: u32 = 0x0000_0001;

#[derive(Clone, Copy)]
enum HierarchyRow<'a> {
    Mailbox(&'a JmapMailbox),
    PublicFolder(&'a MapiPublicFolder),
    Collaboration(&'a MapiCollaborationFolder),
    Special(u64),
}

#[cfg(test)]
fn hierarchy_rows<'a>(
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

fn hierarchy_rows_excluding_deleted<'a>(
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

fn hierarchy_table_rows_excluding_deleted<'a>(
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

fn hierarchy_row_display_name<'a>(row: &'a HierarchyRow<'a>) -> &'a str {
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

fn hierarchy_row_id(row: &HierarchyRow<'_>) -> u64 {
    match row {
        HierarchyRow::Mailbox(mailbox) => mapi_folder_id(mailbox),
        HierarchyRow::PublicFolder(folder) => folder.id,
        HierarchyRow::Collaboration(folder) => folder.id,
        HierarchyRow::Special(folder_id) => *folder_id,
    }
}

fn hierarchy_row_parent_id(row: &HierarchyRow<'_>, _mailboxes: &[JmapMailbox]) -> u64 {
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

fn hierarchy_row_property_value(
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

fn hierarchy_row_expected_container_class<'a>(row: &'a HierarchyRow<'a>) -> Option<&'a str> {
    match row {
        HierarchyRow::Collaboration(folder) => {
            Some(collaboration_folder_message_class(folder.kind))
        }
        HierarchyRow::Special(folder_id) => debug_expected_container_class(*folder_id),
        HierarchyRow::Mailbox(mailbox) => Some(folder_message_class(mailbox)),
        HierarchyRow::PublicFolder(folder) => Some(folder.folder.folder_class.as_str()),
    }
}

fn hierarchy_row_matches(
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

fn special_hierarchy_row_matches(
    folder_id: u64,
    restriction: Option<&MapiRestriction>,
    mailbox_guid: Uuid,
) -> bool {
    restriction_matches(restriction, |property_tag| {
        special_folder_property_value(folder_id, property_tag, mailbox_guid)
    })
}

fn log_sync_issues_hierarchy_query_rows(
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
        PID_TAG_ARCHIVE_TAG | PID_TAG_POLICY_TAG => Some(MapiValue::Binary(Vec::new())),
        PID_TAG_RETENTION_PERIOD | PID_TAG_RETENTION_FLAGS | PID_TAG_ARCHIVE_PERIOD => {
            Some(MapiValue::U32(0))
        }
        PID_TAG_FOLDER_WEBVIEWINFO | PID_TAG_FOLDER_XVIEWINFO_E => {
            Some(MapiValue::Binary(Vec::new()))
        }
        OUTLOOK_UNDOCUMENTED_FOLDER_BINARY_120C => Some(MapiValue::Binary(Vec::new())),
        PID_TAG_FOLDER_FORM_FLAGS | PID_TAG_FOLDER_VIEWS_ONLY | PID_TAG_FOLDER_VIEWLIST_FLAGS => {
            Some(MapiValue::U32(0))
        }
        PID_TAG_DEFAULT_FORM_NAME_W => Some(MapiValue::String(String::new())),
        PID_TAG_DEFAULT_VIEW_ENTRY_ID
            if default_view_supported_folder(folder_id, message_class) =>
        {
            default_folder_view_entry_id(mailbox_guid, folder_id, message_class)
        }
        tag if is_acl_member_name_property_tag(tag) => Some(MapiValue::String(String::new())),
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

fn serialize_hierarchy_row(
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
            serialize_advertised_special_folder_row_with_mailbox_guid(
                folder_id,
                columns,
                mailbox_guid,
            )
        }
    }
}

pub(in crate::mapi) fn rop_query_rows_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    mailbox_guid: Uuid,
) -> Vec<u8> {
    if !object.as_deref().is_some_and(is_table_object) {
        return rop_error_response(0x15, request.response_handle_index(), 0x8004_0102);
    }
    if !query_rows_request_is_valid(request) {
        return rop_error_response(0x15, request.response_handle_index(), 0x8007_0057);
    }
    if !object.as_deref().is_some_and(table_columns_are_available) {
        return rop_error_response(0x15, request.response_handle_index(), 0x0000_04B9);
    }

    let response_columns = query_rows_response_columns(object.as_deref(), snapshot);
    let total_row_count = object
        .as_deref()
        .map(|object| {
            table_position_and_count(Some(object), mailboxes, emails, snapshot, mailbox_guid).1
        })
        .unwrap_or(0);
    let mut response = vec![0x15, request.response_handle_index()];
    write_u32(&mut response, 0);
    let mut start_position = 0usize;
    let mut position_base = 0usize;
    let rows = match object {
        Some(MapiObject::HierarchyTable {
            folder_id,
            columns,
            sort_orders,
            restriction,
            deleted_advertised_special_folders,
            position: table_position,
            ..
        }) if is_queryable_hierarchy_folder(*folder_id)
            || snapshot.public_folder_for_id(*folder_id).is_some() =>
        {
            start_position = *table_position;
            let columns = if columns.is_empty() {
                default_hierarchy_columns()
            } else {
                columns.clone()
            };
            let rows = hierarchy_table_rows_excluding_deleted(
                *folder_id,
                mailboxes,
                snapshot,
                restriction.as_ref(),
                sort_orders,
                mailbox_guid,
                deleted_advertised_special_folders,
            );
            log_sync_issues_hierarchy_query_rows(
                request,
                *folder_id,
                &columns,
                restriction.as_ref(),
                sort_orders,
                *table_position,
                &rows,
                mailbox_guid,
            );
            rows.into_iter()
                .map(|row| {
                    serialize_hierarchy_row(row, mailboxes, snapshot, &columns, mailbox_guid)
                })
                .collect::<Vec<_>>()
        }
        Some(MapiObject::ContentsTable {
            folder_id,
            associated,
            columns,
            sort_orders,
            category_count,
            expanded_count,
            collapsed_categories,
            restriction,
            position: table_position,
            ..
        }) => {
            start_position = *table_position;
            let columns = if columns.is_empty() {
                if !*associated
                    && (is_contact_contents_folder(*folder_id)
                        || *folder_id == CONTACTS_SEARCH_FOLDER_ID
                        || snapshot
                            .collaboration_folder_for_id(*folder_id)
                            .is_some_and(|folder| {
                                folder.kind == MapiCollaborationFolderKind::Contacts
                            }))
                {
                    default_contact_property_tags()
                } else if *associated && *folder_id == COMMON_VIEWS_FOLDER_ID {
                    default_navigation_shortcut_property_tags()
                } else if *associated && *folder_id == CONVERSATION_ACTION_SETTINGS_FOLDER_ID {
                    default_conversation_action_property_tags()
                } else if *folder_id == FREEBUSY_DATA_FOLDER_ID {
                    default_message_property_tags()
                } else if *associated
                    && (*folder_id == CALENDAR_FOLDER_ID
                        || snapshot
                            .collaboration_folder_for_id(*folder_id)
                            .is_some_and(|folder| {
                                folder.kind == MapiCollaborationFolderKind::Calendar
                            }))
                {
                    default_calendar_configuration_property_tags()
                } else if *associated
                    && should_use_associated_config_table(
                        *folder_id,
                        snapshot,
                        restriction.as_ref(),
                    )
                {
                    default_associated_config_columns()
                } else {
                    default_contents_columns()
                }
            } else {
                columns.clone()
            };
            if *folder_id == FREEBUSY_DATA_FOLDER_ID {
                snapshot
                    .delegate_freebusy_messages()
                    .iter()
                    .filter(|message| {
                        restriction_matches(restriction.as_ref(), |property_tag| {
                            delegate_freebusy_property_value(message, property_tag)
                        })
                    })
                    .map(|message| serialize_delegate_freebusy_row(message, &columns))
                    .collect::<Vec<_>>()
            } else if *associated {
                if *folder_id == COMMON_VIEWS_FOLDER_ID {
                    let mut rows = snapshot.common_views_table_messages().collect::<Vec<_>>();
                    let total_common_views_rows = rows.len();
                    let navigation_shortcut_count = rows
                        .iter()
                        .filter(|message| {
                            matches!(message, MapiCommonViewsMessage::NavigationShortcut(_))
                        })
                        .count();
                    let virtual_navigation_shortcut_count = rows
                        .iter()
                        .filter(|message| {
                            matches!(
                                message,
                                MapiCommonViewsMessage::NavigationShortcut(shortcut)
                                    if crate::mapi_store::is_outlook_common_views_default_navigation_shortcut_id(shortcut.id)
                            )
                        })
                        .count();
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        request_type = "Execute",
                        request_rop_id = "0x15",
                        folder_id = %format!("0x{folder_id:016x}"),
                        folder_role = role_for_folder_id(*folder_id).unwrap_or(""),
                        associated = true,
                        common_views_row_count = total_common_views_rows,
                        common_views_navigation_shortcut_count = navigation_shortcut_count,
                        common_views_persisted_navigation_shortcut_count =
                            navigation_shortcut_count.saturating_sub(virtual_navigation_shortcut_count),
                        common_views_virtual_navigation_shortcut_count =
                            virtual_navigation_shortcut_count,
                        common_views_named_view_count =
                            total_common_views_rows.saturating_sub(navigation_shortcut_count),
                        common_views_navigation_projection = false,
                        table_has_restriction = restriction.is_some(),
                        current_position = *table_position,
                        selected_property_tag_count = columns.len(),
                        selected_property_tags = %format_table_property_tags(&columns),
                        "rca debug outlook common views query rows"
                    );
                    rows.retain(|message| {
                        restriction_matches_common_views_message(
                            restriction.as_ref(),
                            message,
                            mailbox_guid,
                        )
                    });
                    sort_common_views_messages(&mut rows, sort_orders);
                    rows.iter()
                        .map(|message| {
                            serialize_common_views_row_with_mailbox_guid(
                                message,
                                mailbox_guid,
                                &columns,
                            )
                        })
                        .collect::<Vec<_>>()
                } else if *folder_id == CONVERSATION_ACTION_SETTINGS_FOLDER_ID {
                    snapshot
                        .conversation_action_table_messages()
                        .iter()
                        .filter(|message| {
                            restriction_matches(restriction.as_ref(), |property_tag| {
                                conversation_action_property_value(message, property_tag)
                            })
                        })
                        .map(|message| serialize_conversation_action_row(message, &columns))
                        .collect::<Vec<_>>()
                } else if should_use_associated_config_table(
                    *folder_id,
                    snapshot,
                    restriction.as_ref(),
                ) {
                    let mut rows = associated_table_rows(
                        *folder_id,
                        snapshot,
                        restriction.as_ref(),
                        mailbox_guid,
                    );
                    sort_associated_table_rows(&mut rows, sort_orders, mailbox_guid);
                    rows.iter()
                        .map(|message| {
                            serialize_associated_table_row(message, mailbox_guid, &columns)
                        })
                        .collect::<Vec<_>>()
                } else if *folder_id == CALENDAR_FOLDER_ID
                    || snapshot
                        .collaboration_folder_for_id(*folder_id)
                        .is_some_and(|folder| folder.kind == MapiCollaborationFolderKind::Calendar)
                {
                    Vec::new()
                } else {
                    Vec::new()
                }
            } else if snapshot.public_folder_for_id(*folder_id).is_some() {
                let mut rows = snapshot.public_folder_items_for_folder(*folder_id);
                retain_rows_by_restriction(&mut rows, restriction.as_ref(), |item, restriction| {
                    restriction_matches_public_folder_item(restriction, item)
                });
                rows.into_iter()
                    .map(|item| serialize_public_folder_item_row(item, &columns))
                    .collect::<Vec<_>>()
            } else if *folder_id == CONVERSATION_MEMBERS_CONTENTS_TABLE_ID {
                let mut rows = emails.iter().collect::<Vec<_>>();
                retain_rows_by_restriction(
                    &mut rows,
                    restriction.as_ref(),
                    |email, restriction| {
                        restriction_matches_conversation_member_in_snapshot(
                            restriction,
                            email,
                            snapshot,
                        )
                    },
                );
                sort_emails(&mut rows, sort_orders);
                rows.into_iter()
                    .map(|email| serialize_message_row(email, &columns))
                    .collect::<Vec<_>>()
            } else if *folder_id == CALENDAR_FOLDER_ID {
                let mut rows = calendar_content_rows(snapshot, *folder_id, restriction.as_ref());
                sort_events(&mut rows, sort_orders);
                rows.into_iter()
                    .map(|event| {
                        serialize_event_row(&event.event, event.id, event.folder_id, &columns)
                    })
                    .collect::<Vec<_>>()
            } else if let Some(folder) = snapshot.collaboration_folder_for_id(*folder_id) {
                match folder.kind {
                    MapiCollaborationFolderKind::Contacts => {
                        let mut rows = snapshot.contacts_for_folder(*folder_id);
                        rows.retain(|contact| {
                            restriction_matches_contact_in_folder(
                                restriction.as_ref(),
                                &contact.contact,
                                *folder_id,
                            )
                        });
                        sort_contacts(&mut rows, sort_orders);
                        rows.into_iter()
                            .map(|contact| {
                                serialize_contact_row(
                                    &contact.contact,
                                    contact.id,
                                    contact.folder_id,
                                    &columns,
                                )
                            })
                            .collect::<Vec<_>>()
                    }
                    MapiCollaborationFolderKind::Calendar => {
                        let mut rows =
                            calendar_content_rows(snapshot, *folder_id, restriction.as_ref());
                        sort_events(&mut rows, sort_orders);
                        rows.into_iter()
                            .map(|event| {
                                serialize_event_row(
                                    &event.event,
                                    event.id,
                                    event.folder_id,
                                    &columns,
                                )
                            })
                            .collect::<Vec<_>>()
                    }
                    MapiCollaborationFolderKind::Task => {
                        let mut rows = snapshot.tasks_for_folder(*folder_id);
                        rows.retain(|task| {
                            restriction_matches_task(restriction.as_ref(), &task.task)
                        });
                        sort_tasks(&mut rows, sort_orders);
                        rows.into_iter()
                            .map(|task| {
                                serialize_task_row(&task.task, task.id, task.folder_id, &columns)
                            })
                            .collect::<Vec<_>>()
                    }
                }
            } else if is_contact_contents_folder(*folder_id) {
                let mut rows = snapshot.contacts_for_folder(*folder_id);
                rows.retain(|contact| {
                    restriction_matches_contact_in_folder(
                        restriction.as_ref(),
                        &contact.contact,
                        *folder_id,
                    )
                });
                sort_contacts(&mut rows, sort_orders);
                rows.into_iter()
                    .map(|contact| {
                        serialize_contact_row(
                            &contact.contact,
                            contact.id,
                            contact.folder_id,
                            &columns,
                        )
                    })
                    .collect::<Vec<_>>()
            } else if *folder_id == CONTACTS_SEARCH_FOLDER_ID {
                let mut rows = snapshot.contacts_search_results();
                rows.retain(|contact| {
                    restriction_matches_contact_in_folder(
                        restriction.as_ref(),
                        &contact.contact,
                        CONTACTS_SEARCH_FOLDER_ID,
                    )
                });
                sort_contacts(&mut rows, sort_orders);
                rows.into_iter()
                    .map(|contact| {
                        serialize_contact_row(
                            &contact.contact,
                            contact.id,
                            CONTACTS_SEARCH_FOLDER_ID,
                            &columns,
                        )
                    })
                    .collect::<Vec<_>>()
            } else if *folder_id == TODO_SEARCH_FOLDER_ID {
                let mut rows = todo_search_content_rows(snapshot, restriction.as_ref());
                sort_search_content_rows(&mut rows, sort_orders);
                rows.into_iter()
                    .map(|row| serialize_search_content_row(row, snapshot, &columns, false))
                    .collect::<Vec<_>>()
            } else if *folder_id == TRACKED_MAIL_PROCESSING_FOLDER_ID {
                let mut rows = snapshot.tracked_mail_processing_messages();
                retain_rows_by_restriction(
                    &mut rows,
                    restriction.as_ref(),
                    |message, restriction| {
                        restriction_matches_email_in_snapshot(
                            restriction,
                            &message.email,
                            *folder_id,
                            snapshot,
                        )
                    },
                );
                sort_mapi_messages(&mut rows, sort_orders);
                rows.into_iter()
                    .map(|message| serialize_message_row(&message.email, &columns))
                    .collect::<Vec<_>>()
            } else if *folder_id == REMINDERS_FOLDER_ID {
                let mut rows = reminder_search_content_rows(snapshot, restriction.as_ref());
                sort_search_content_rows(&mut rows, sort_orders);
                rows.into_iter()
                    .map(|row| serialize_search_content_row(row, snapshot, &columns, true))
                    .collect::<Vec<_>>()
            } else if *folder_id == NOTES_FOLDER_ID {
                let mut rows = snapshot.notes_for_folder(*folder_id);
                rows.retain(|note| restriction_matches_note(restriction.as_ref(), &note.note));
                sort_notes(&mut rows, sort_orders);
                rows.into_iter()
                    .map(|note| serialize_note_row(&note.note, note.id, note.folder_id, &columns))
                    .collect::<Vec<_>>()
            } else if *folder_id == JOURNAL_FOLDER_ID {
                let mut rows = snapshot.journal_entries_for_folder(*folder_id);
                rows.retain(|entry| {
                    restriction_matches_journal_entry(restriction.as_ref(), &entry.entry)
                });
                sort_journal_entries(&mut rows, sort_orders);
                rows.into_iter()
                    .map(|entry| {
                        serialize_journal_entry_row(
                            &entry.entry,
                            entry.id,
                            entry.folder_id,
                            &columns,
                        )
                    })
                    .collect::<Vec<_>>()
            } else if crate::mapi_store::recoverable_storage_folder(*folder_id).is_some() {
                let mut rows = snapshot.recoverable_items_for_folder(*folder_id);
                sort_recoverable_items(&mut rows, sort_orders);
                rows.into_iter()
                    .map(|item| serialize_recoverable_item_row(item, &columns))
                    .collect::<Vec<_>>()
            } else {
                let window_offset = if request.query_forward_read() {
                    start_position
                } else {
                    start_position.saturating_sub(request.query_row_count().unwrap_or(0))
                };
                let window_emails = if is_top_level_count_restriction(restriction.as_ref()) {
                    None
                } else {
                    snapshot.content_table_window_emails(
                        *folder_id,
                        table_view_signature(sort_orders, restriction.as_ref()),
                        window_offset,
                        request.query_row_count().unwrap_or(0),
                    )
                };
                if let Some((_, window_emails)) = window_emails {
                    position_base = window_offset;
                    start_position = if request.query_forward_read() {
                        0
                    } else {
                        window_emails.len()
                    };
                    if *category_count > 0 {
                        categorized_email_rows(
                            *folder_id,
                            window_emails,
                            &columns,
                            sort_orders,
                            *expanded_count,
                            collapsed_categories,
                        )
                        .into_iter()
                        .map(|row| row.row)
                        .collect::<Vec<_>>()
                    } else {
                        window_emails
                            .into_iter()
                            .map(|email| serialize_message_row(email, &columns))
                            .collect::<Vec<_>>()
                    }
                } else {
                    let mut rows = emails_for_folder(*folder_id, mailboxes, emails);
                    retain_rows_by_restriction(
                        &mut rows,
                        restriction.as_ref(),
                        |email, restriction| {
                            restriction_matches_email_in_snapshot(
                                restriction,
                                email,
                                *folder_id,
                                snapshot,
                            )
                        },
                    );
                    sort_emails(&mut rows, sort_orders);
                    if *category_count > 0 {
                        categorized_email_rows(
                            *folder_id,
                            rows,
                            &columns,
                            sort_orders,
                            *expanded_count,
                            collapsed_categories,
                        )
                        .into_iter()
                        .map(|row| row.row)
                        .collect::<Vec<_>>()
                    } else {
                        rows.into_iter()
                            .map(|email| serialize_message_row(email, &columns))
                            .collect::<Vec<_>>()
                    }
                }
            }
        }
        Some(MapiObject::AttachmentTable {
            folder_id,
            message_id,
            columns,
            sort_orders,
            restriction,
            position: table_position,
            ..
        }) => {
            start_position = *table_position;
            let columns = if columns.is_empty() {
                default_attachment_columns()
            } else {
                columns.clone()
            };
            let mut rows = snapshot
                .attachments_for_message(*folder_id, *message_id)
                .unwrap_or_default()
                .iter()
                .collect::<Vec<_>>();
            retain_rows_by_restriction(
                &mut rows,
                restriction.as_ref(),
                |attachment, restriction| restriction_matches_attachment(restriction, attachment),
            );
            sort_attachments(&mut rows, sort_orders);
            rows.into_iter()
                .map(|attachment| serialize_attachment_row(attachment, &columns))
                .collect::<Vec<_>>()
        }
        Some(MapiObject::PermissionTable {
            folder_id,
            columns,
            columns_set: _,
            position: table_position,
        }) => {
            start_position = *table_position;
            let columns = if columns.is_empty() {
                default_permission_columns()
            } else {
                columns.clone()
            };
            snapshot
                .permissions_for_folder(*folder_id)
                .into_iter()
                .map(|permission| serialize_permission_row(&permission, &columns))
                .collect::<Vec<_>>()
        }
        Some(MapiObject::RuleTable {
            folder_id,
            columns,
            position: table_position,
            ..
        }) => {
            start_position = *table_position;
            let columns = if columns.is_empty() {
                default_rule_columns()
            } else {
                columns.clone()
            };
            if snapshot.public_folder_for_id(*folder_id).is_some() {
                Vec::new()
            } else {
                snapshot
                    .rules()
                    .iter()
                    .map(|rule| serialize_rule_row(rule, &columns))
                    .collect::<Vec<_>>()
            }
        }
        _ => Vec::new(),
    };
    start_position = start_position.min(total_row_count.saturating_sub(position_base));
    let row_count = request.query_row_count().unwrap_or(rows.len());
    let forward_read = request.query_forward_read();
    let (selected, next_position) = if forward_read {
        let selected = rows
            .into_iter()
            .skip(start_position)
            .take(row_count)
            .collect::<Vec<_>>();
        let next_position =
            position_base.saturating_add(start_position.saturating_add(selected.len()));
        (selected, next_position)
    } else {
        let end_position = start_position.min(rows.len());
        let selected_start = end_position.saturating_sub(row_count);
        let selected = rows[selected_start..end_position]
            .iter()
            .rev()
            .cloned()
            .collect::<Vec<_>>();
        (selected, position_base.saturating_add(selected_start))
    };
    if !request.query_no_advance() {
        if let Some(
            MapiObject::HierarchyTable { position, .. }
            | MapiObject::ContentsTable { position, .. }
            | MapiObject::AttachmentTable { position, .. }
            | MapiObject::PermissionTable { position, .. }
            | MapiObject::RuleTable { position, .. },
        ) = object
        {
            *position = next_position;
        }
    }
    let response_origin = if forward_read {
        if next_position >= total_row_count {
            0x02
        } else {
            0x01
        }
    } else if next_position == 0 {
        0x00
    } else {
        0x01
    };
    response.push(response_origin);
    response.extend_from_slice(&(selected.len() as u16).to_le_bytes());
    for row in selected {
        write_query_rows_property_row(&mut response, &response_columns, &row);
    }
    response
}

fn query_rows_request_is_valid(request: &RopRequest) -> bool {
    let Some(flags) = request.payload.first().copied() else {
        return false;
    };
    if flags & !0x03 != 0 {
        return false;
    }
    matches!(request.payload.get(1).copied(), Some(0x00 | 0x01))
        && request.payload.get(2..4).is_some()
}

fn query_rows_response_columns(
    object: Option<&MapiObject>,
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u32> {
    match object {
        Some(MapiObject::HierarchyTable {
            folder_id, columns, ..
        }) if is_queryable_hierarchy_folder(*folder_id)
            || snapshot.public_folder_for_id(*folder_id).is_some() =>
        {
            if columns.is_empty() {
                default_hierarchy_columns()
            } else {
                columns.clone()
            }
        }
        Some(MapiObject::ContentsTable {
            folder_id,
            associated,
            columns,
            ..
        }) => {
            if !columns.is_empty() {
                return columns.clone();
            }
            if !*associated
                && (is_contact_contents_folder(*folder_id)
                    || *folder_id == CONTACTS_SEARCH_FOLDER_ID
                    || snapshot
                        .collaboration_folder_for_id(*folder_id)
                        .is_some_and(|folder| folder.kind == MapiCollaborationFolderKind::Contacts))
            {
                default_contact_property_tags()
            } else if *associated && *folder_id == COMMON_VIEWS_FOLDER_ID {
                default_navigation_shortcut_property_tags()
            } else if *associated && *folder_id == CONVERSATION_ACTION_SETTINGS_FOLDER_ID {
                default_conversation_action_property_tags()
            } else if *associated && *folder_id == FREEBUSY_DATA_FOLDER_ID {
                default_message_property_tags()
            } else if *associated
                && (*folder_id == CALENDAR_FOLDER_ID
                    || snapshot
                        .collaboration_folder_for_id(*folder_id)
                        .is_some_and(|folder| folder.kind == MapiCollaborationFolderKind::Calendar))
            {
                default_calendar_configuration_property_tags()
            } else if *associated && should_use_associated_config_table(*folder_id, snapshot, None)
            {
                default_associated_config_columns()
            } else {
                default_contents_columns()
            }
        }
        Some(MapiObject::AttachmentTable { columns, .. }) => {
            if columns.is_empty() {
                default_attachment_columns()
            } else {
                columns.clone()
            }
        }
        Some(MapiObject::PermissionTable { columns, .. }) => {
            if columns.is_empty() {
                default_permission_columns()
            } else {
                columns.clone()
            }
        }
        Some(MapiObject::RuleTable { columns, .. }) => {
            if columns.is_empty() {
                default_rule_columns()
            } else {
                columns.clone()
            }
        }
        _ => Vec::new(),
    }
}

pub(in crate::mapi) fn outlook_bootstrap_row_invariant_summaries(
    object: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    mailbox_guid: Uuid,
    forward_read: bool,
    requested_row_count: usize,
) -> Vec<String> {
    match object {
        Some(MapiObject::HierarchyTable {
            folder_id,
            sort_orders,
            restriction,
            position,
            deleted_advertised_special_folders,
            ..
        }) if matches!(
            *folder_id,
            ROOT_FOLDER_ID | IPM_SUBTREE_FOLDER_ID | SYNC_ISSUES_FOLDER_ID
        ) =>
        {
            let rows = hierarchy_table_rows_excluding_deleted(
                *folder_id,
                mailboxes,
                snapshot,
                restriction.as_ref(),
                sort_orders,
                mailbox_guid,
                deleted_advertised_special_folders,
            );
            selected_row_indexes(rows.len(), *position, forward_read, requested_row_count)
                .into_iter()
                .map(|index| {
                    let row = &rows[index];
                    let object_id = hierarchy_row_id(row);
                    let parent_id = hierarchy_row_parent_id(row, mailboxes);
                    classify_outlook_bootstrap_row_invariants(
                        index,
                        "hierarchy_folder",
                        object_id,
                        Some(object_id),
                        Some(parent_id),
                        hierarchy_row_expected_container_class(row),
                        |tag| {
                            debug_folder_row_property_value(
                                || hierarchy_row_property_value(row, mailboxes, tag, mailbox_guid),
                                object_id,
                                parent_id,
                                tag,
                                mailbox_guid,
                                associated_folder_message_count(object_id, snapshot),
                            )
                        },
                    )
                })
                .collect()
        }
        Some(MapiObject::ContentsTable {
            folder_id,
            associated,
            columns,
            sort_orders,
            restriction,
            position,
            ..
        }) if *associated && *folder_id == COMMON_VIEWS_FOLDER_ID => {
            let mut rows = snapshot.common_views_table_messages().collect::<Vec<_>>();
            rows.retain(|message| {
                restriction_matches_common_views_message(
                    restriction.as_ref(),
                    message,
                    mailbox_guid,
                )
            });
            sort_common_views_messages(&mut rows, sort_orders);
            selected_row_indexes(rows.len(), *position, forward_read, requested_row_count)
                .into_iter()
                .map(|index| {
                    let message = &rows[index];
                    classify_outlook_bootstrap_row_invariants(
                        index,
                        "common_views_associated",
                        common_views_message_id(message),
                        None,
                        None,
                        None,
                        |tag| common_views_message_property_value(message, mailbox_guid, tag),
                    )
                })
                .collect()
        }
        Some(MapiObject::ContentsTable {
            folder_id,
            associated,
            sort_orders,
            restriction,
            position,
            ..
        }) if *associated && *folder_id == INBOX_FOLDER_ID => {
            let mut rows =
                associated_table_rows(*folder_id, snapshot, restriction.as_ref(), mailbox_guid);
            rows.retain(|row| associated_table_row_config(row).is_some());
            sort_associated_table_rows(&mut rows, sort_orders, mailbox_guid);
            selected_row_indexes(rows.len(), *position, forward_read, requested_row_count)
                .into_iter()
                .map(|index| {
                    let message = &rows[index];
                    classify_outlook_bootstrap_row_invariants(
                        index,
                        "inbox_associated",
                        associated_table_row_id(message),
                        None,
                        None,
                        None,
                        |tag| associated_table_row_property_value(message, mailbox_guid, tag),
                    )
                })
                .collect()
        }
        Some(MapiObject::ContentsTable {
            folder_id,
            associated,
            sort_orders,
            restriction,
            position,
            ..
        }) if !*associated && *folder_id == INBOX_FOLDER_ID => {
            let mut rows = emails_for_folder(*folder_id, mailboxes, emails);
            rows.retain(|email| {
                restriction_matches_email_in_snapshot(
                    restriction.as_ref(),
                    email,
                    *folder_id,
                    snapshot,
                )
            });
            sort_emails(&mut rows, sort_orders);
            selected_row_indexes(rows.len(), *position, forward_read, requested_row_count)
                .into_iter()
                .map(|index| {
                    let email = rows[index];
                    classify_outlook_bootstrap_row_invariants(
                        index,
                        "inbox_contents",
                        mapi_message_id(email),
                        None,
                        Some(INBOX_FOLDER_ID),
                        None,
                        |tag| email_property_value(email, tag),
                    )
                })
                .collect()
        }
        _ => Vec::new(),
    }
}

fn selected_row_indexes(
    row_len: usize,
    start_position: usize,
    forward_read: bool,
    requested_row_count: usize,
) -> Vec<usize> {
    let row_count = requested_row_count.min(row_len);
    if forward_read {
        return (start_position.min(row_len)..row_len)
            .take(row_count)
            .collect();
    }
    let end_position = start_position.min(row_len);
    let selected_start = end_position.saturating_sub(row_count);
    (selected_start..end_position).rev().collect()
}

pub(in crate::mapi) fn rop_query_columns_all_response(
    request: &RopRequest,
    object: Option<&MapiObject>,
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u8> {
    let columns = match object {
        Some(MapiObject::HierarchyTable { .. }) => default_folder_property_tags(),
        Some(MapiObject::ContentsTable {
            folder_id,
            associated,
            ..
        }) => {
            if *associated && *folder_id == COMMON_VIEWS_FOLDER_ID {
                default_navigation_shortcut_property_tags()
            } else if *associated && *folder_id == FREEBUSY_DATA_FOLDER_ID {
                default_message_property_tags()
            } else if *associated
                && snapshot
                    .collaboration_folder_for_id(*folder_id)
                    .is_some_and(|folder| folder.kind == MapiCollaborationFolderKind::Calendar)
            {
                default_calendar_configuration_property_tags()
            } else if *associated && should_use_associated_config_table(*folder_id, snapshot, None)
            {
                default_associated_config_columns()
            } else {
                match snapshot
                    .collaboration_folder_for_id(*folder_id)
                    .map(|folder| folder.kind)
                {
                    Some(MapiCollaborationFolderKind::Contacts) => default_contact_property_tags(),
                    Some(MapiCollaborationFolderKind::Calendar) => default_event_property_tags(),
                    Some(MapiCollaborationFolderKind::Task) => default_task_property_tags(),
                    None if *folder_id == CONTACTS_SEARCH_FOLDER_ID => {
                        default_contact_property_tags()
                    }
                    None if *folder_id == TODO_SEARCH_FOLDER_ID => default_task_property_tags(),
                    None if *folder_id == TRACKED_MAIL_PROCESSING_FOLDER_ID => {
                        default_message_property_tags()
                    }
                    None if *folder_id == REMINDERS_FOLDER_ID => default_reminder_property_tags(),
                    None if *folder_id == NOTES_FOLDER_ID => default_note_property_tags(),
                    None if *folder_id == JOURNAL_FOLDER_ID => {
                        default_journal_entry_property_tags()
                    }
                    None => default_message_property_tags(),
                }
            }
        }
        Some(MapiObject::AttachmentTable { .. }) => default_attachment_columns(),
        Some(MapiObject::PermissionTable { .. }) => default_permission_columns(),
        Some(MapiObject::RuleTable { .. }) => default_rule_columns(),
        _ => return rop_error_response(0x37, request.response_handle_index(), 0x8004_0102),
    };

    let mut response = vec![0x37, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.extend_from_slice(&(columns.len() as u16).to_le_bytes());
    for column in columns {
        response.extend_from_slice(&column.to_le_bytes());
    }
    response
}

fn common_views_message_property_value(
    message: &MapiCommonViewsMessage,
    mailbox_guid: Uuid,
    property_tag: u32,
) -> Option<MapiValue> {
    match message {
        MapiCommonViewsMessage::NavigationShortcut(message) => {
            navigation_shortcut_property_value(message, mailbox_guid, property_tag)
        }
        MapiCommonViewsMessage::NamedView(message) => {
            common_view_named_view_property_value(message, mailbox_guid, property_tag)
        }
        MapiCommonViewsMessage::SearchFolderDefinition(message) => {
            search_folder_definition_message_property_value(message, mailbox_guid, property_tag)
        }
    }
}

pub(in crate::mapi) fn rop_get_status_response(
    request: &RopRequest,
    object: Option<&MapiObject>,
) -> Vec<u8> {
    if !object.is_some_and(is_table_object) {
        return rop_error_response(0x16, request.response_handle_index(), 0x8004_0102);
    }

    let mut response = vec![0x16, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(0);
    response
}

pub(in crate::mapi) fn rop_query_position_response(
    request: &RopRequest,
    object: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    mailbox_guid: Uuid,
) -> Vec<u8> {
    if !object.is_some_and(is_table_object) {
        return rop_error_response(0x17, request.response_handle_index(), 0x8004_0102);
    }

    let (position, row_count) =
        table_position_and_count(object, mailboxes, emails, snapshot, mailbox_guid);
    let mut response = vec![0x17, request.response_handle_index()];
    write_u32(&mut response, 0);
    write_u32(&mut response, position as u32);
    write_u32(&mut response, row_count as u32);
    response
}

pub(in crate::mapi) fn rop_seek_row_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    mailbox_guid: Uuid,
) -> Vec<u8> {
    if !seek_row_request_is_valid(request) {
        return rop_error_response(0x18, request.response_handle_index(), 0x8007_0057);
    }
    let Some(object) = object else {
        return rop_error_response(0x18, request.response_handle_index(), 0x8004_0102);
    };
    let (current_position, total_rows) =
        table_position_and_count(Some(object), mailboxes, emails, snapshot, mailbox_guid);
    let replay_broad_startup_row = should_replay_inbox_broad_configuration_startup_seek(
        request,
        object,
        current_position,
        total_rows,
    );
    let Some(position) = table_position_mut(object) else {
        return rop_error_response(0x18, request.response_handle_index(), 0x8004_0102);
    };

    let requested_rows = request.seek_row_count().unwrap_or(0);
    let base_position = match request.seek_origin().unwrap_or(1) {
        0 => 0isize,
        2 => total_rows as isize,
        _ => current_position as isize,
    };
    let requested_position = base_position.saturating_add(requested_rows as isize);
    let new_position = if replay_broad_startup_row {
        0
    } else {
        requested_position.clamp(0, total_rows as isize)
    };
    let rows_sought = (new_position - base_position) as i32;
    *position = new_position as usize;

    let mut response = vec![0x18, request.response_handle_index()];
    write_u32(&mut response, 0);
    let want_row_moved_count = request.want_row_moved_count();
    response.push((want_row_moved_count && rows_sought != requested_rows) as u8);
    response.extend_from_slice(&if want_row_moved_count { rows_sought } else { 0 }.to_le_bytes());
    response
}

fn should_replay_inbox_broad_configuration_startup_seek(
    request: &RopRequest,
    object: &MapiObject,
    current_position: usize,
    total_rows: usize,
) -> bool {
    matches!(
        object,
        MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: true,
            restriction,
            ..
        } if is_broad_outlook_configuration_restriction(restriction.as_ref())
    ) && current_position == 0
        && total_rows == 1
        && request.seek_origin() == Some(1)
        && request.seek_row_count() == Some(1)
}

fn seek_row_request_is_valid(request: &RopRequest) -> bool {
    matches!(request.payload.first().copied(), Some(0x00..=0x02))
        && matches!(request.payload.get(5).copied(), Some(0x00 | 0x01))
        && request.payload.get(1..5).is_some()
}

pub(in crate::mapi) fn rop_create_bookmark_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    mailbox_guid: Uuid,
) -> Vec<u8> {
    let Some(object) = object else {
        return rop_error_response(0x1B, request.response_handle_index(), 0x8004_0102);
    };
    let row_key = table_row_keys(object, mailboxes, emails, snapshot, mailbox_guid)
        .get(table_position(object).unwrap_or(0))
        .copied()
        .filter(|row_key| *row_key != 0);
    let Some((position, bookmarks, next_bookmark)) = table_bookmark_state_mut(object) else {
        return rop_error_response(0x1B, request.response_handle_index(), 0x8004_0102);
    };
    let bookmark = next_bookmark.to_le_bytes().to_vec();
    bookmarks.insert(
        bookmark.clone(),
        TableBookmark {
            position: *position,
            row_key,
        },
    );
    *next_bookmark = next_bookmark.saturating_add(1).max(1);

    let mut response = vec![0x1B, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.extend_from_slice(&(bookmark.len() as u16).to_le_bytes());
    response.extend_from_slice(&bookmark);
    response
}

pub(in crate::mapi) fn rop_seek_row_bookmark_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    mailbox_guid: Uuid,
) -> Vec<u8> {
    if !seek_row_bookmark_request_is_valid(request) {
        return rop_error_response(0x19, request.response_handle_index(), 0x8007_0057);
    }
    let Some(object) = object else {
        return rop_error_response(0x19, request.response_handle_index(), 0x8004_0102);
    };
    if !table_columns_are_available(object) {
        return rop_error_response(0x19, request.response_handle_index(), 0x0000_04B9);
    }
    let row_keys = table_row_keys(object, mailboxes, emails, snapshot, mailbox_guid);
    let total_rows = row_keys.len();
    let Some((position, bookmarks, _next_bookmark)) = table_bookmark_state_mut(object) else {
        return rop_error_response(0x19, request.response_handle_index(), 0x8004_0102);
    };
    let Some(bookmark) = bookmarks.get(request.bookmark()).cloned() else {
        return rop_error_response(0x19, request.response_handle_index(), 0x8004_0405);
    };
    let mut row_no_longer_visible = false;
    let base_position = match bookmark.row_key {
        Some(row_key) => row_keys
            .iter()
            .position(|key| *key == row_key)
            .unwrap_or_else(|| {
                if !row_keys.contains(&0) {
                    row_no_longer_visible = true;
                }
                bookmark.position.min(total_rows)
            }),
        None => bookmark.position.min(total_rows),
    };

    let requested_rows = request.bookmark_row_count().unwrap_or(0);
    let requested_position = (base_position as isize).saturating_add(requested_rows as isize);
    let new_position = requested_position.clamp(0, total_rows as isize);
    let rows_sought = (new_position - base_position as isize) as i32;
    *position = new_position as usize;

    let mut response = vec![0x19, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(row_no_longer_visible as u8);
    response.push((request.bookmark_want_row_moved_count() && rows_sought != requested_rows) as u8);
    response.extend_from_slice(
        &if request.bookmark_want_row_moved_count() {
            rows_sought
        } else {
            0
        }
        .to_le_bytes(),
    );
    response
}

fn seek_row_bookmark_request_is_valid(request: &RopRequest) -> bool {
    let Some(size) = request
        .payload
        .get(..2)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u16::from_le_bytes)
        .map(usize::from)
    else {
        return false;
    };
    request.payload.get(2..2 + size).is_some()
        && request.payload.get(2 + size..6 + size).is_some()
        && matches!(request.payload.get(6 + size).copied(), Some(0x00 | 0x01))
}

pub(in crate::mapi) fn rop_seek_row_fractional_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    mailbox_guid: Uuid,
) -> Vec<u8> {
    if !seek_row_fractional_request_is_valid(request) {
        return rop_error_response(0x1A, request.response_handle_index(), 0x8007_0057);
    }
    let Some(object) = object else {
        return rop_error_response(0x1A, request.response_handle_index(), 0x8004_0102);
    };
    let total_rows =
        table_position_and_count(Some(object), mailboxes, emails, snapshot, mailbox_guid).1;
    let Some(position) = table_position_mut(object) else {
        return rop_error_response(0x1A, request.response_handle_index(), 0x8004_0102);
    };
    let (numerator, denominator) = request.fractional_position().unwrap_or((0, 1));
    let new_position = (total_rows as u128)
        .saturating_mul(numerator as u128)
        .checked_div(denominator as u128)
        .unwrap_or(0)
        .min(total_rows as u128) as usize;
    *position = new_position;

    let mut response = vec![0x1A, request.response_handle_index()];
    write_u32(&mut response, 0);
    response
}

fn seek_row_fractional_request_is_valid(request: &RopRequest) -> bool {
    request
        .fractional_position()
        .is_some_and(|(_numerator, denominator)| denominator != 0)
}

pub(in crate::mapi) fn rop_free_bookmark_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
) -> Vec<u8> {
    let Some(object) = object else {
        return rop_error_response(0x89, request.response_handle_index(), 0x8004_0102);
    };
    if !table_columns_are_available(object) {
        return rop_error_response(0x89, request.response_handle_index(), 0x0000_04B9);
    }
    let Some((_position, bookmarks, _next_bookmark)) = table_bookmark_state_mut(object) else {
        return rop_error_response(0x89, request.response_handle_index(), 0x8004_0102);
    };
    bookmarks.remove(request.bookmark());

    let mut response = vec![0x89, request.response_handle_index()];
    write_u32(&mut response, 0);
    response
}

fn rop_find_row_no_match_response(request: &RopRequest) -> Vec<u8> {
    rop_error_response(0x4F, request.response_handle_index(), 0x8004_010F)
}

fn is_broad_outlook_configuration_find_row(restriction: &MapiRestriction) -> bool {
    matches!(
        restriction,
        MapiRestriction::Property {
            relop: 0x02,
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            value: MapiValue::String(value),
        } | MapiRestriction::Content {
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            value,
            ..
        } if value.eq_ignore_ascii_case("IPM.Configuration.")
    )
}

pub(in crate::mapi) fn associated_config_visible_in_table(
    folder_id: u64,
    restriction: Option<&MapiRestriction>,
    message: &MapiAssociatedConfigMessage,
) -> bool {
    if folder_id != INBOX_FOLDER_ID {
        return true;
    }
    if message.message_class == "IPM.ExtendedRule.Message" {
        return false;
    }
    if crate::mapi_store::is_outlook_inbox_virtual_only_associated_config_id(message.id) {
        return matches!(
            message.message_class.as_str(),
            "IPM.Configuration.ELC"
                | "IPM.Configuration.MRM"
                | "IPM.Configuration.UMOLK.UserOptions"
                | "IPM.Sharing.Configuration"
                | "IPM.Sharing.Index"
                | "IPM.Aggregation"
        ) && restriction.is_some_and(|restriction| {
            message_class_restriction_matches_exact(restriction, &message.message_class)
        });
    }
    if message.message_class.starts_with("IPM.Configuration.") {
        if is_inbox_broad_startup_config_visible(restriction, message) {
            return true;
        }
        return restriction.is_some_and(|restriction| {
            message_class_restriction_matches_exact(restriction, &message.message_class)
        }) && !is_empty_inbox_configuration_placeholder(message);
    }
    !is_empty_inbox_configuration_placeholder(message)
}

fn is_inbox_broad_startup_config_visible(
    restriction: Option<&MapiRestriction>,
    message: &MapiAssociatedConfigMessage,
) -> bool {
    if !message.message_class.starts_with("IPM.Configuration.") {
        return false;
    }
    let exact = restriction.is_some_and(|restriction| {
        message_class_restriction_matches_exact(restriction, &message.message_class)
    });
    if exact {
        return !is_empty_inbox_configuration_placeholder(message);
    }
    if is_broad_outlook_configuration_restriction(restriction) {
        return is_modeled_inbox_broad_startup_config(message);
    }
    if restriction.is_none() {
        return is_modeled_inbox_broad_startup_config(message)
            || !is_empty_inbox_configuration_placeholder(message);
    }
    false
}

fn is_modeled_inbox_broad_startup_config(message: &MapiAssociatedConfigMessage) -> bool {
    crate::mapi_store::outlook_inbox_broad_startup_associated_config_defaults()
        .into_iter()
        .any(|modeled| {
            message.id == modeled.id
                && message
                    .message_class
                    .eq_ignore_ascii_case(&modeled.message_class)
        })
}

fn message_class_restriction_matches_exact(
    restriction: &MapiRestriction,
    message_class: &str,
) -> bool {
    matches!(
        restriction,
        MapiRestriction::Property {
            relop: 0x04,
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            value: MapiValue::String(value),
        } | MapiRestriction::Content {
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            value,
            ..
        } if value.eq_ignore_ascii_case(message_class)
    )
}

fn is_empty_inbox_configuration_placeholder(message: &MapiAssociatedConfigMessage) -> bool {
    if message.message_class == crate::mapi_store::OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_CLASS
        && message.subject == "Compact"
    {
        return message
            .properties_json
            .as_object()
            .is_some_and(|object| object.is_empty());
    }
    if !message.message_class.starts_with("IPM.Configuration.")
        || message.message_class == "IPM.Configuration.UMOLK.UserOptions"
    {
        return false;
    }
    let properties = mapi_properties_from_json(&message.properties_json);
    !properties.contains_key(&PID_TAG_ROAMING_DICTIONARY)
        && !properties.contains_key(&PID_TAG_ROAMING_XML_STREAM)
        && !properties.contains_key(&OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B)
        && !properties.contains_key(&0x7C09_0102)
}

fn outlook_configuration_prefix_restriction() -> MapiRestriction {
    MapiRestriction::Content {
        property_tag: PID_TAG_MESSAGE_CLASS_W,
        value: "IPM.Configuration.".to_string(),
        fuzzy_level_low: 0x0002,
        fuzzy_level_high: 0x0001,
    }
}

pub(in crate::mapi) fn rop_find_row_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    mailbox_guid: Uuid,
) -> Vec<u8> {
    if !find_row_request_is_valid(request) {
        return rop_error_response(0x4F, request.response_handle_index(), 0x8007_0057);
    }
    let Ok(restriction) = request.restriction() else {
        return rop_error_response(0x4F, request.response_handle_index(), 0x8004_0102);
    };
    let Some(restriction) = restriction else {
        return rop_error_response(0x4F, request.response_handle_index(), 0x8004_0102);
    };

    let Some(object) = object else {
        return rop_error_response(0x4F, request.response_handle_index(), 0x8004_0102);
    };
    if !table_columns_are_available(object) {
        return rop_error_response(0x4F, request.response_handle_index(), 0x0000_04B9);
    }
    let mut response = vec![0x4F, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(0);

    match object {
        MapiObject::HierarchyTable {
            folder_id,
            columns,
            sort_orders,
            restriction: table_restriction,
            deleted_advertised_special_folders,
            position,
            ..
        } if is_queryable_hierarchy_folder(*folder_id) => {
            let columns = if columns.is_empty() {
                default_hierarchy_columns()
            } else {
                columns.clone()
            };
            let rows = hierarchy_table_rows_excluding_deleted(
                *folder_id,
                mailboxes,
                snapshot,
                table_restriction.as_ref(),
                sort_orders,
                mailbox_guid,
                deleted_advertised_special_folders,
            );
            if let Some((index, row)) = find_hierarchy_row(
                rows.as_slice(),
                mailboxes,
                *position,
                request,
                Some(&restriction),
                mailbox_guid,
            ) {
                *position = index;
                response.push(1);
                write_standard_property_row(
                    &mut response,
                    &serialize_hierarchy_row(row, mailboxes, snapshot, &columns, mailbox_guid),
                );
            } else {
                return rop_find_row_no_match_response(request);
            }
        }
        MapiObject::ContentsTable {
            folder_id,
            associated,
            columns,
            sort_orders,
            restriction: table_restriction,
            position,
            ..
        } => {
            let columns = if columns.is_empty() {
                if *associated && *folder_id == COMMON_VIEWS_FOLDER_ID {
                    default_navigation_shortcut_property_tags()
                } else if *associated && *folder_id == CONVERSATION_ACTION_SETTINGS_FOLDER_ID {
                    default_conversation_action_property_tags()
                } else if *associated
                    && should_use_associated_config_table(
                        *folder_id,
                        snapshot,
                        table_restriction.as_ref(),
                    )
                {
                    default_associated_config_columns()
                } else {
                    default_contents_columns()
                }
            } else {
                columns.clone()
            };
            if *associated && *folder_id == COMMON_VIEWS_FOLDER_ID {
                let mut rows = snapshot.common_views_table_messages().collect::<Vec<_>>();
                sort_common_views_messages(&mut rows, sort_orders);
                let navigation_shortcut_count = rows
                    .iter()
                    .filter(|message| {
                        matches!(message, MapiCommonViewsMessage::NavigationShortcut(_))
                    })
                    .count();
                let virtual_navigation_shortcut_count = rows
                    .iter()
                    .filter(|message| {
                        matches!(
                            message,
                            MapiCommonViewsMessage::NavigationShortcut(shortcut)
                                if crate::mapi_store::is_outlook_common_views_default_navigation_shortcut_id(shortcut.id)
                        )
                    })
                    .count();
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    request_type = "Execute",
                    request_rop_id = "0x4f",
                    folder_id = %format!("0x{folder_id:016x}"),
                    folder_role = role_for_folder_id(*folder_id).unwrap_or(""),
                    associated = true,
                    common_views_row_count = rows.len(),
                    common_views_navigation_shortcut_count = navigation_shortcut_count,
                    common_views_persisted_navigation_shortcut_count =
                        navigation_shortcut_count.saturating_sub(virtual_navigation_shortcut_count),
                    common_views_virtual_navigation_shortcut_count =
                        virtual_navigation_shortcut_count,
                    common_views_named_view_count = rows.len().saturating_sub(navigation_shortcut_count),
                    current_position = *position,
                    selected_property_tag_count = columns.len(),
                    selected_property_tags = %format_table_property_tags(&columns),
                    "rca debug outlook common views find row"
                );
                let rows = rows.iter().collect::<Vec<_>>();
                if let Some((index, message)) = find_row(
                    rows.as_slice(),
                    *position,
                    request,
                    |message| match message {
                        MapiCommonViewsMessage::NavigationShortcut(shortcut) => {
                            restriction_matches_navigation_shortcut(
                                Some(&restriction),
                                shortcut,
                                mailbox_guid,
                            )
                        }
                        MapiCommonViewsMessage::NamedView(view) => {
                            restriction_matches_common_view_named_view(
                                Some(&restriction),
                                view,
                                mailbox_guid,
                            )
                        }
                        MapiCommonViewsMessage::SearchFolderDefinition(definition) => {
                            restriction_matches(Some(&restriction), |property_tag| {
                                search_folder_definition_message_property_value(
                                    definition,
                                    mailbox_guid,
                                    property_tag,
                                )
                            })
                        }
                    },
                ) {
                    *position = index;
                    response.push(1);
                    write_standard_property_row(
                        &mut response,
                        &serialize_common_views_row_with_mailbox_guid(
                            message,
                            mailbox_guid,
                            &columns,
                        ),
                    );
                } else {
                    return rop_find_row_no_match_response(request);
                }
            } else if *associated && *folder_id == CONVERSATION_ACTION_SETTINGS_FOLDER_ID {
                let rows = snapshot.conversation_action_table_messages();
                let rows = rows.iter().collect::<Vec<_>>();
                if let Some((index, message)) =
                    find_row(rows.as_slice(), *position, request, |message| {
                        restriction_matches(Some(&restriction), |property_tag| {
                            conversation_action_property_value(message, property_tag)
                        })
                    })
                {
                    *position = index;
                    response.push(1);
                    write_standard_property_row(
                        &mut response,
                        &serialize_conversation_action_row(message, &columns),
                    );
                } else {
                    return rop_find_row_no_match_response(request);
                }
            } else if *folder_id == FREEBUSY_DATA_FOLDER_ID {
                let rows = snapshot
                    .delegate_freebusy_messages()
                    .iter()
                    .collect::<Vec<_>>();
                if let Some((index, message)) =
                    find_row(rows.as_slice(), *position, request, |message| {
                        restriction_matches(Some(&restriction), |property_tag| {
                            delegate_freebusy_property_value(message, property_tag)
                        })
                    })
                {
                    *position = index;
                    response.push(1);
                    write_standard_property_row(
                        &mut response,
                        &serialize_delegate_freebusy_row(message, &columns),
                    );
                } else {
                    return rop_find_row_no_match_response(request);
                }
            } else if *associated
                && should_use_associated_config_table(*folder_id, snapshot, Some(&restriction))
            {
                let mut rows = associated_table_rows(
                    *folder_id,
                    snapshot,
                    table_restriction.as_ref(),
                    mailbox_guid,
                );
                if *folder_id == INBOX_FOLDER_ID {
                    for row in associated_table_rows(
                        *folder_id,
                        snapshot,
                        Some(&restriction),
                        mailbox_guid,
                    ) {
                        if !rows.iter().any(|existing| {
                            associated_table_row_id(existing) == associated_table_row_id(&row)
                        }) {
                            rows.push(row);
                        }
                    }
                }
                sort_associated_table_rows(&mut rows, sort_orders, mailbox_guid);
                let broad_outlook_configuration_probe = *folder_id == INBOX_FOLDER_ID
                    && is_broad_outlook_configuration_find_row(&restriction);
                let suppressed_virtual_default_count = if broad_outlook_configuration_probe {
                    rows.iter()
                        .filter_map(associated_table_row_config)
                        .filter(|message| {
                            crate::mapi_store::is_outlook_inbox_virtual_only_associated_config_id(
                                message.id,
                            )
                        })
                        .count()
                } else {
                    0
                };
                if suppressed_virtual_default_count > 0 {
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        request_type = "Execute",
                        request_rop_id = "0x4f",
                        folder_id = %format!("0x{folder_id:016x}"),
                        folder_role = role_for_folder_id(*folder_id).unwrap_or(""),
                        associated = true,
                        broad_outlook_configuration_probe = true,
                        suppressed_virtual_default_count,
                        total_candidate_count = rows.len(),
                        "rca debug outlook associated config broad find row virtual defaults suppressed"
                    );
                }
                let row_refs = rows.iter().collect::<Vec<_>>();
                if let Some((index, message)) =
                    find_row(row_refs.as_slice(), *position, request, |message| {
                        if broad_outlook_configuration_probe {
                            let Some(config) = associated_table_row_config(message) else {
                                return false;
                            };
                            if !associated_config_visible_in_table(
                                *folder_id,
                                Some(&restriction),
                                config,
                            ) {
                                return false;
                            }
                        }
                        associated_table_row_matches(message, Some(&restriction), mailbox_guid)
                    })
                {
                    let exact_virtual_row_probe = *folder_id == INBOX_FOLDER_ID
                        && associated_table_row_config(message).is_some_and(|config| {
                            crate::mapi_store::is_outlook_inbox_virtual_only_associated_config_id(
                                config.id,
                            ) && !associated_config_visible_in_table(
                                *folder_id,
                                table_restriction.as_ref(),
                                config,
                            )
                        });
                    if exact_virtual_row_probe {
                        *table_restriction = Some(restriction.clone());
                        *position = 0;
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            request_type = "Execute",
                            request_rop_id = "0x4f",
                            folder_id = %format!("0x{folder_id:016x}"),
                            folder_role = role_for_folder_id(*folder_id).unwrap_or(""),
                            associated = true,
                            matched_row_index = index,
                            matched_message_class = %associated_table_row_message_class(message),
                            "rca debug outlook associated config exact virtual find row followup query restricted"
                        );
                    } else {
                        *position = index;
                    }
                    if broad_outlook_configuration_probe {
                        *table_restriction = Some(outlook_configuration_prefix_restriction());
                        *position = 0;
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            request_type = "Execute",
                            request_rop_id = "0x4f",
                            folder_id = %format!("0x{folder_id:016x}"),
                            folder_role = role_for_folder_id(*folder_id).unwrap_or(""),
                            associated = true,
                            matched_row_index = index,
                            matched_message_class = %associated_table_row_message_class(message),
                            "rca debug outlook associated config broad find row followup query restricted"
                        );
                    }
                    response.push(1);
                    write_standard_property_row(
                        &mut response,
                        &serialize_associated_table_row(message, mailbox_guid, &columns),
                    );
                } else {
                    if broad_outlook_configuration_probe {
                        *table_restriction = Some(outlook_configuration_prefix_restriction());
                        *position = 0;
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            request_type = "Execute",
                            request_rop_id = "0x4f",
                            folder_id = %format!("0x{folder_id:016x}"),
                            folder_role = role_for_folder_id(*folder_id).unwrap_or(""),
                            associated = true,
                            "rca debug outlook associated config broad find row no match followup query restricted"
                        );
                    }
                    return rop_find_row_no_match_response(request);
                }
            } else if *folder_id == CALENDAR_FOLDER_ID {
                let mut rows =
                    calendar_content_rows(snapshot, *folder_id, table_restriction.as_ref());
                sort_events(&mut rows, sort_orders);
                if let Some((index, event)) =
                    find_row(rows.as_slice(), *position, request, |event| {
                        restriction_matches_event(Some(&restriction), event)
                    })
                {
                    *position = index;
                    response.push(1);
                    write_standard_property_row(
                        &mut response,
                        &serialize_event_row(&event.event, event.id, event.folder_id, &columns),
                    );
                } else {
                    return rop_find_row_no_match_response(request);
                }
            } else if let Some(folder) = snapshot.collaboration_folder_for_id(*folder_id) {
                match folder.kind {
                    MapiCollaborationFolderKind::Contacts => {
                        let mut rows = snapshot.contacts_for_folder(*folder_id);
                        rows.retain(|contact| {
                            restriction_matches_contact_in_folder(
                                table_restriction.as_ref(),
                                &contact.contact,
                                *folder_id,
                            )
                        });
                        sort_contacts(&mut rows, sort_orders);
                        if let Some((index, contact)) =
                            find_row(rows.as_slice(), *position, request, |contact| {
                                restriction_matches_contact_in_folder(
                                    Some(&restriction),
                                    &contact.contact,
                                    *folder_id,
                                )
                            })
                        {
                            *position = index;
                            response.push(1);
                            write_standard_property_row(
                                &mut response,
                                &serialize_contact_row(
                                    &contact.contact,
                                    contact.id,
                                    contact.folder_id,
                                    &columns,
                                ),
                            );
                        } else {
                            return rop_find_row_no_match_response(request);
                        }
                    }
                    MapiCollaborationFolderKind::Calendar => {
                        let mut rows =
                            calendar_content_rows(snapshot, *folder_id, table_restriction.as_ref());
                        sort_events(&mut rows, sort_orders);
                        if let Some((index, event)) =
                            find_row(rows.as_slice(), *position, request, |event| {
                                restriction_matches_event(Some(&restriction), event)
                            })
                        {
                            *position = index;
                            response.push(1);
                            write_standard_property_row(
                                &mut response,
                                &serialize_event_row(
                                    &event.event,
                                    event.id,
                                    event.folder_id,
                                    &columns,
                                ),
                            );
                        } else {
                            return rop_find_row_no_match_response(request);
                        }
                    }
                    MapiCollaborationFolderKind::Task => {
                        let mut rows = snapshot.tasks_for_folder(*folder_id);
                        rows.retain(|task| {
                            restriction_matches_task(table_restriction.as_ref(), &task.task)
                        });
                        sort_tasks(&mut rows, sort_orders);
                        if let Some((index, task)) =
                            find_row(rows.as_slice(), *position, request, |task| {
                                restriction_matches_task(Some(&restriction), &task.task)
                            })
                        {
                            *position = index;
                            response.push(1);
                            write_standard_property_row(
                                &mut response,
                                &serialize_task_row(&task.task, task.id, task.folder_id, &columns),
                            );
                        } else {
                            return rop_find_row_no_match_response(request);
                        }
                    }
                }
            } else if *folder_id == CONTACTS_SEARCH_FOLDER_ID {
                let mut rows = snapshot.contacts_search_results();
                rows.retain(|contact| {
                    restriction_matches_contact_in_folder(
                        table_restriction.as_ref(),
                        &contact.contact,
                        CONTACTS_SEARCH_FOLDER_ID,
                    )
                });
                sort_contacts(&mut rows, sort_orders);
                if let Some((index, contact)) =
                    find_row(rows.as_slice(), *position, request, |contact| {
                        restriction_matches_contact_in_folder(
                            Some(&restriction),
                            &contact.contact,
                            CONTACTS_SEARCH_FOLDER_ID,
                        )
                    })
                {
                    *position = index;
                    response.push(1);
                    write_standard_property_row(
                        &mut response,
                        &serialize_contact_row(
                            &contact.contact,
                            contact.id,
                            CONTACTS_SEARCH_FOLDER_ID,
                            &columns,
                        ),
                    );
                } else {
                    return rop_find_row_no_match_response(request);
                }
            } else if *folder_id == TODO_SEARCH_FOLDER_ID {
                let mut rows = todo_search_content_rows(snapshot, table_restriction.as_ref());
                sort_search_content_rows(&mut rows, sort_orders);
                let row_refs = rows.iter().collect::<Vec<_>>();
                if let Some((index, row)) =
                    find_row(row_refs.as_slice(), *position, request, |row| {
                        search_content_row_matches(row, Some(&restriction))
                    })
                {
                    *position = index;
                    response.push(1);
                    write_standard_property_row(
                        &mut response,
                        &serialize_search_content_row(*row, snapshot, &columns, false),
                    );
                } else {
                    return rop_find_row_no_match_response(request);
                }
            } else if *folder_id == TRACKED_MAIL_PROCESSING_FOLDER_ID {
                let mut rows = snapshot.tracked_mail_processing_messages();
                retain_rows_by_restriction(
                    &mut rows,
                    table_restriction.as_ref(),
                    |message, restriction| {
                        restriction_matches_email_in_snapshot(
                            restriction,
                            &message.email,
                            *folder_id,
                            snapshot,
                        )
                    },
                );
                sort_mapi_messages(&mut rows, sort_orders);
                if let Some((index, message)) =
                    find_row(rows.as_slice(), *position, request, |message| {
                        restriction_matches_email_in_snapshot(
                            Some(&restriction),
                            &message.email,
                            *folder_id,
                            snapshot,
                        )
                    })
                {
                    *position = index;
                    response.push(1);
                    write_standard_property_row(
                        &mut response,
                        &serialize_message_row(&message.email, &columns),
                    );
                } else {
                    return rop_find_row_no_match_response(request);
                }
            } else if *folder_id == REMINDERS_FOLDER_ID {
                let mut rows = reminder_search_content_rows(snapshot, table_restriction.as_ref());
                sort_search_content_rows(&mut rows, sort_orders);
                let row_refs = rows.iter().collect::<Vec<_>>();
                if let Some((index, row)) =
                    find_row(row_refs.as_slice(), *position, request, |row| {
                        search_content_row_matches(row, Some(&restriction))
                    })
                {
                    *position = index;
                    response.push(1);
                    write_standard_property_row(
                        &mut response,
                        &serialize_search_content_row(*row, snapshot, &columns, true),
                    );
                } else {
                    return rop_find_row_no_match_response(request);
                }
            } else if *folder_id == NOTES_FOLDER_ID {
                let mut rows = snapshot.notes_for_folder(*folder_id);
                rows.retain(|note| {
                    restriction_matches_note(table_restriction.as_ref(), &note.note)
                });
                sort_notes(&mut rows, sort_orders);
                if let Some((index, note)) = find_row(rows.as_slice(), *position, request, |note| {
                    restriction_matches_note(Some(&restriction), &note.note)
                }) {
                    *position = index;
                    response.push(1);
                    write_standard_property_row(
                        &mut response,
                        &serialize_note_row(&note.note, note.id, note.folder_id, &columns),
                    );
                } else {
                    return rop_find_row_no_match_response(request);
                }
            } else if *folder_id == JOURNAL_FOLDER_ID {
                let mut rows = snapshot.journal_entries_for_folder(*folder_id);
                rows.retain(|entry| {
                    restriction_matches_journal_entry(table_restriction.as_ref(), &entry.entry)
                });
                sort_journal_entries(&mut rows, sort_orders);
                if let Some((index, entry)) =
                    find_row(rows.as_slice(), *position, request, |entry| {
                        restriction_matches_journal_entry(Some(&restriction), &entry.entry)
                    })
                {
                    *position = index;
                    response.push(1);
                    write_standard_property_row(
                        &mut response,
                        &serialize_journal_entry_row(
                            &entry.entry,
                            entry.id,
                            entry.folder_id,
                            &columns,
                        ),
                    );
                } else {
                    return rop_find_row_no_match_response(request);
                }
            } else if snapshot.public_folder_for_id(*folder_id).is_some() {
                let mut rows = snapshot.public_folder_items_for_folder(*folder_id);
                retain_rows_by_restriction(
                    &mut rows,
                    table_restriction.as_ref(),
                    |item, restriction| restriction_matches_public_folder_item(restriction, item),
                );
                if let Some((index, item)) = find_row(rows.as_slice(), *position, request, |item| {
                    restriction_matches_public_folder_item(Some(&restriction), item)
                }) {
                    *position = index;
                    response.push(1);
                    write_standard_property_row(
                        &mut response,
                        &serialize_public_folder_item_row(item, &columns),
                    );
                } else {
                    return rop_find_row_no_match_response(request);
                }
            } else if crate::mapi_store::recoverable_storage_folder(*folder_id).is_some() {
                let mut rows = snapshot.recoverable_items_for_folder(*folder_id);
                sort_recoverable_items(&mut rows, sort_orders);
                if let Some((index, item)) = find_row(rows.as_slice(), *position, request, |item| {
                    restriction_matches(Some(&restriction), |property_tag| {
                        recoverable_item_property_value(item, property_tag)
                    })
                }) {
                    *position = index;
                    response.push(1);
                    write_standard_property_row(
                        &mut response,
                        &serialize_recoverable_item_row(item, &columns),
                    );
                } else {
                    return rop_find_row_no_match_response(request);
                }
            } else {
                let view_signature = table_view_signature(sort_orders, table_restriction.as_ref());
                let window_emails = if is_top_level_count_restriction(table_restriction.as_ref()) {
                    None
                } else {
                    snapshot.content_table_window_emails_containing(
                        *folder_id,
                        view_signature,
                        *position,
                    )
                };
                if let Some((offset, _total, window_emails)) = window_emails {
                    let local_position = position.saturating_sub(offset);
                    if let Some((index, email)) =
                        find_row(window_emails.as_slice(), local_position, request, |email| {
                            restriction_matches_email_in_snapshot(
                                Some(&restriction),
                                email,
                                *folder_id,
                                snapshot,
                            )
                        })
                    {
                        *position = offset.saturating_add(index);
                        response.push(1);
                        write_standard_property_row(
                            &mut response,
                            &serialize_message_row(email, &columns),
                        );
                    } else {
                        let mut rows = emails_for_folder(*folder_id, mailboxes, emails);
                        retain_rows_by_restriction(
                            &mut rows,
                            table_restriction.as_ref(),
                            |email, restriction| {
                                restriction_matches_email_in_snapshot(
                                    restriction,
                                    email,
                                    *folder_id,
                                    snapshot,
                                )
                            },
                        );
                        sort_emails(&mut rows, sort_orders);
                        let complete_rows_available =
                            !is_top_level_count_restriction(table_restriction.as_ref())
                                && snapshot
                                    .content_table_total(*folder_id, view_signature)
                                    .is_some_and(|total| total == rows.len());
                        let found = complete_rows_available.then(|| {
                            find_row(rows.as_slice(), *position, request, |email| {
                                restriction_matches_email_in_snapshot(
                                    Some(&restriction),
                                    email,
                                    *folder_id,
                                    snapshot,
                                )
                            })
                        });
                        if let Some(Some((index, email))) = found {
                            *position = index;
                            response.push(1);
                            write_standard_property_row(
                                &mut response,
                                &serialize_message_row(email, &columns),
                            );
                        } else {
                            return rop_find_row_no_match_response(request);
                        }
                    }
                } else {
                    let mut rows = emails_for_folder(*folder_id, mailboxes, emails);
                    retain_rows_by_restriction(
                        &mut rows,
                        table_restriction.as_ref(),
                        |email, restriction| {
                            restriction_matches_email_in_snapshot(
                                restriction,
                                email,
                                *folder_id,
                                snapshot,
                            )
                        },
                    );
                    sort_emails(&mut rows, sort_orders);
                    if let Some((index, email)) =
                        find_row(rows.as_slice(), *position, request, |email| {
                            restriction_matches_email_in_snapshot(
                                Some(&restriction),
                                email,
                                *folder_id,
                                snapshot,
                            )
                        })
                    {
                        *position = index;
                        response.push(1);
                        write_standard_property_row(
                            &mut response,
                            &serialize_message_row(email, &columns),
                        );
                    } else {
                        return rop_find_row_no_match_response(request);
                    }
                }
            }
        }
        MapiObject::AttachmentTable {
            folder_id,
            message_id,
            columns,
            sort_orders,
            restriction: table_restriction,
            position,
            ..
        } => {
            let columns = if columns.is_empty() {
                default_attachment_columns()
            } else {
                columns.clone()
            };
            let mut rows = snapshot
                .attachments_for_message(*folder_id, *message_id)
                .unwrap_or_default()
                .iter()
                .collect::<Vec<_>>();
            rows.retain(|attachment| {
                restriction_matches_attachment(table_restriction.as_ref(), attachment)
            });
            sort_attachments(&mut rows, sort_orders);
            if let Some((index, attachment)) =
                find_row(rows.as_slice(), *position, request, |attachment| {
                    restriction_matches_attachment(Some(&restriction), attachment)
                })
            {
                *position = index;
                response.push(1);
                write_standard_property_row(
                    &mut response,
                    &serialize_attachment_row(attachment, &columns),
                );
            } else {
                return rop_find_row_no_match_response(request);
            }
        }
        _ => return rop_error_response(0x4F, request.response_handle_index(), 0x8004_0102),
    }

    response
}

fn find_row_request_is_valid(request: &RopRequest) -> bool {
    request
        .payload
        .first()
        .is_some_and(|flags| flags & !0x01 == 0)
}

pub(in crate::mapi) fn find_row<'a, T>(
    rows: &'a [&'a T],
    current_position: usize,
    request: &RopRequest,
    matches: impl Fn(&T) -> bool,
) -> Option<(usize, &'a T)> {
    if rows.is_empty() {
        return None;
    }
    let start = match request.find_origin().unwrap_or(1) {
        0 => 0,
        2 => rows.len().saturating_sub(1),
        _ => current_position.min(rows.len()),
    };
    if request.find_backward() {
        let end = start.min(rows.len().saturating_sub(1));
        (0..=end)
            .rev()
            .find_map(|index| matches(rows[index]).then_some((index, rows[index])))
    } else {
        rows.iter()
            .enumerate()
            .skip(start)
            .find_map(|(index, row)| matches(row).then_some((index, *row)))
    }
}

fn find_hierarchy_row<'a>(
    rows: &'a [HierarchyRow<'a>],
    mailboxes: &[JmapMailbox],
    current_position: usize,
    request: &RopRequest,
    restriction: Option<&MapiRestriction>,
    mailbox_guid: Uuid,
) -> Option<(usize, HierarchyRow<'a>)> {
    if rows.is_empty() {
        return None;
    }
    let start = match request.find_origin().unwrap_or(1) {
        0 => 0,
        2 => rows.len().saturating_sub(1),
        _ => current_position.min(rows.len()),
    };
    if request.find_backward() {
        let end = start.min(rows.len().saturating_sub(1));
        (0..=end).rev().find_map(|index| {
            hierarchy_row_matches(&rows[index], mailboxes, restriction, mailbox_guid)
                .then_some((index, rows[index]))
        })
    } else {
        rows.iter()
            .enumerate()
            .skip(start)
            .find_map(|(index, row)| {
                hierarchy_row_matches(row, mailboxes, restriction, mailbox_guid)
                    .then_some((index, *row))
            })
    }
}

pub(in crate::mapi) fn table_position_and_count(
    object: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    mailbox_guid: Uuid,
) -> (usize, usize) {
    let (position, total) = match object {
        Some(MapiObject::HierarchyTable {
            folder_id,
            position,
            restriction,
            sort_orders,
            deleted_advertised_special_folders,
            ..
        }) if is_queryable_hierarchy_folder(*folder_id) => {
            let total = hierarchy_table_rows_excluding_deleted(
                *folder_id,
                mailboxes,
                snapshot,
                restriction.as_ref(),
                sort_orders,
                mailbox_guid,
                deleted_advertised_special_folders,
            )
            .len();
            (*position, total)
        }
        Some(MapiObject::ContentsTable {
            folder_id,
            associated,
            position,
            restriction,
            sort_orders,
            category_count,
            expanded_count,
            collapsed_categories,
            ..
        }) => {
            let total = if *folder_id == FREEBUSY_DATA_FOLDER_ID {
                restricted_associated_folder_message_count(
                    *folder_id,
                    snapshot,
                    restriction.as_ref(),
                    mailbox_guid,
                )
            } else if *associated {
                restricted_associated_folder_message_count(
                    *folder_id,
                    snapshot,
                    restriction.as_ref(),
                    mailbox_guid,
                )
            } else if *folder_id == CALENDAR_FOLDER_ID {
                calendar_content_rows(snapshot, *folder_id, restriction.as_ref()).len()
            } else if let Some(folder) = snapshot.collaboration_folder_for_id(*folder_id) {
                match folder.kind {
                    MapiCollaborationFolderKind::Contacts => snapshot
                        .contacts_for_folder(*folder_id)
                        .into_iter()
                        .filter(|contact| {
                            restriction_matches_contact_in_folder(
                                restriction.as_ref(),
                                &contact.contact,
                                *folder_id,
                            )
                        })
                        .count(),
                    MapiCollaborationFolderKind::Calendar => {
                        calendar_content_rows(snapshot, *folder_id, restriction.as_ref()).len()
                    }
                    MapiCollaborationFolderKind::Task => snapshot
                        .tasks_for_folder(*folder_id)
                        .into_iter()
                        .filter(|task| restriction_matches_task(restriction.as_ref(), &task.task))
                        .count(),
                }
            } else if is_contact_contents_folder(*folder_id) {
                snapshot
                    .contacts_for_folder(*folder_id)
                    .into_iter()
                    .filter(|contact| {
                        restriction_matches_contact_in_folder(
                            restriction.as_ref(),
                            &contact.contact,
                            *folder_id,
                        )
                    })
                    .count()
            } else if *folder_id == NOTES_FOLDER_ID {
                snapshot
                    .notes_for_folder(*folder_id)
                    .into_iter()
                    .filter(|note| restriction_matches_note(restriction.as_ref(), &note.note))
                    .count()
            } else if *folder_id == CONTACTS_SEARCH_FOLDER_ID {
                snapshot
                    .contacts_search_results()
                    .into_iter()
                    .filter(|contact| {
                        restriction_matches_contact_in_folder(
                            restriction.as_ref(),
                            &contact.contact,
                            CONTACTS_SEARCH_FOLDER_ID,
                        )
                    })
                    .count()
            } else if *folder_id == TODO_SEARCH_FOLDER_ID {
                let mut message_rows = snapshot
                    .todo_search_messages()
                    .into_iter()
                    .collect::<Vec<_>>();
                retain_rows_by_restriction(
                    &mut message_rows,
                    restriction.as_ref(),
                    |message, restriction| restriction_matches_email(restriction, &message.email),
                );
                message_rows.len()
                    + snapshot
                        .todo_search_results()
                        .into_iter()
                        .filter(|task| restriction_matches_task(restriction.as_ref(), &task.task))
                        .count()
            } else if *folder_id == TRACKED_MAIL_PROCESSING_FOLDER_ID {
                let mut rows = snapshot
                    .tracked_mail_processing_messages()
                    .into_iter()
                    .collect::<Vec<_>>();
                retain_rows_by_restriction(
                    &mut rows,
                    restriction.as_ref(),
                    |message, restriction| {
                        restriction_matches_email_in_snapshot(
                            restriction,
                            &message.email,
                            *folder_id,
                            snapshot,
                        )
                    },
                );
                rows.len()
            } else if *folder_id == REMINDERS_FOLDER_ID {
                snapshot
                    .reminder_tasks()
                    .into_iter()
                    .filter(|task| restriction_matches_task(restriction.as_ref(), &task.task))
                    .count()
                    + {
                        let mut message_rows =
                            snapshot.reminder_messages().into_iter().collect::<Vec<_>>();
                        retain_rows_by_restriction(
                            &mut message_rows,
                            restriction.as_ref(),
                            |message, restriction| {
                                restriction_matches_email(restriction, &message.email)
                            },
                        );
                        message_rows.len()
                    }
            } else if *folder_id == JOURNAL_FOLDER_ID {
                snapshot
                    .journal_entries_for_folder(*folder_id)
                    .into_iter()
                    .filter(|entry| {
                        restriction_matches_journal_entry(restriction.as_ref(), &entry.entry)
                    })
                    .count()
            } else if snapshot.public_folder_for_id(*folder_id).is_some() {
                let mut rows = snapshot.public_folder_items_for_folder(*folder_id);
                retain_rows_by_restriction(&mut rows, restriction.as_ref(), |item, restriction| {
                    restriction_matches_public_folder_item(restriction, item)
                });
                rows.len()
            } else if crate::mapi_store::recoverable_storage_folder(*folder_id).is_some() {
                snapshot.recoverable_items_for_folder(*folder_id).len()
            } else if *folder_id == CONVERSATION_MEMBERS_CONTENTS_TABLE_ID {
                let mut rows = emails.iter().collect::<Vec<_>>();
                retain_rows_by_restriction(
                    &mut rows,
                    restriction.as_ref(),
                    |email, restriction| {
                        restriction_matches_conversation_member_in_snapshot(
                            restriction,
                            email,
                            snapshot,
                        )
                    },
                );
                rows.len()
            } else if *category_count > 0 {
                let mut rows = emails_for_folder(*folder_id, mailboxes, emails);
                retain_rows_by_restriction(
                    &mut rows,
                    restriction.as_ref(),
                    |email, restriction| {
                        restriction_matches_email_in_snapshot(
                            restriction,
                            email,
                            *folder_id,
                            snapshot,
                        )
                    },
                );
                sort_emails(&mut rows, sort_orders);
                categorized_email_rows(
                    *folder_id,
                    rows,
                    &default_contents_columns(),
                    sort_orders,
                    *expanded_count,
                    collapsed_categories,
                )
                .len()
            } else {
                if is_top_level_count_restriction(restriction.as_ref()) {
                    None
                } else {
                    snapshot.content_table_total(
                        *folder_id,
                        table_view_signature(sort_orders, restriction.as_ref()),
                    )
                }
                .unwrap_or_else(|| {
                    let mut rows = emails_for_folder(*folder_id, mailboxes, emails);
                    retain_rows_by_restriction(
                        &mut rows,
                        restriction.as_ref(),
                        |email, restriction| {
                            restriction_matches_email_in_snapshot(
                                restriction,
                                email,
                                *folder_id,
                                snapshot,
                            )
                        },
                    );
                    rows.len()
                })
            };
            (*position, total)
        }
        Some(MapiObject::AttachmentTable {
            folder_id,
            message_id,
            position,
            restriction,
            ..
        }) => {
            let mut rows = snapshot
                .attachments_for_message(*folder_id, *message_id)
                .unwrap_or_default()
                .iter()
                .collect::<Vec<_>>();
            retain_rows_by_restriction(
                &mut rows,
                restriction.as_ref(),
                |attachment, restriction| restriction_matches_attachment(restriction, attachment),
            );
            (*position, rows.len())
        }
        Some(MapiObject::PermissionTable {
            folder_id,
            position,
            ..
        }) => (*position, snapshot.permissions_for_folder(*folder_id).len()),
        Some(MapiObject::RuleTable {
            folder_id,
            position,
            ..
        }) => (
            *position,
            if snapshot.public_folder_for_id(*folder_id).is_some() {
                0
            } else {
                snapshot.rules().len()
            },
        ),
        _ => (0, 0),
    };
    (position.min(total), total)
}

pub(in crate::mapi) fn table_position_mut(object: &mut MapiObject) -> Option<&mut usize> {
    match object {
        MapiObject::HierarchyTable { position, .. }
        | MapiObject::ContentsTable { position, .. }
        | MapiObject::AttachmentTable { position, .. }
        | MapiObject::PermissionTable { position, .. }
        | MapiObject::RuleTable { position, .. } => Some(position),
        _ => None,
    }
}

pub(in crate::mapi) fn table_position(object: &MapiObject) -> Option<usize> {
    match object {
        MapiObject::HierarchyTable { position, .. }
        | MapiObject::ContentsTable { position, .. }
        | MapiObject::AttachmentTable { position, .. }
        | MapiObject::PermissionTable { position, .. }
        | MapiObject::RuleTable { position, .. } => Some(*position),
        _ => None,
    }
}

pub(in crate::mapi) fn table_bookmark_state_mut(
    object: &mut MapiObject,
) -> Option<(&mut usize, &mut HashMap<Vec<u8>, TableBookmark>, &mut u32)> {
    match object {
        MapiObject::HierarchyTable {
            position,
            bookmarks,
            next_bookmark,
            ..
        }
        | MapiObject::ContentsTable {
            position,
            bookmarks,
            next_bookmark,
            ..
        } => Some((position, bookmarks, next_bookmark)),
        _ => None,
    }
}

pub(in crate::mapi) fn table_row_keys(
    object: &MapiObject,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    mailbox_guid: Uuid,
) -> Vec<u64> {
    match object {
        MapiObject::HierarchyTable {
            folder_id,
            sort_orders,
            restriction,
            deleted_advertised_special_folders,
            ..
        } if is_queryable_hierarchy_folder(*folder_id) => hierarchy_table_rows_excluding_deleted(
            *folder_id,
            mailboxes,
            snapshot,
            restriction.as_ref(),
            sort_orders,
            mailbox_guid,
            deleted_advertised_special_folders,
        )
        .into_iter()
        .map(|row| hierarchy_row_id(&row))
        .collect(),
        MapiObject::ContentsTable {
            folder_id,
            sort_orders,
            restriction,
            position,
            ..
        } => {
            if *folder_id == CALENDAR_FOLDER_ID {
                let mut rows = calendar_content_rows(snapshot, *folder_id, restriction.as_ref());
                sort_events(&mut rows, sort_orders);
                return rows.into_iter().map(|event| event.id).collect();
            }
            if let Some(folder) = snapshot.collaboration_folder_for_id(*folder_id) {
                return match folder.kind {
                    MapiCollaborationFolderKind::Contacts => {
                        let mut rows = snapshot.contacts_for_folder(*folder_id);
                        rows.retain(|contact| {
                            restriction_matches_contact_in_folder(
                                restriction.as_ref(),
                                &contact.contact,
                                *folder_id,
                            )
                        });
                        sort_contacts(&mut rows, sort_orders);
                        rows.into_iter().map(|contact| contact.id).collect()
                    }
                    MapiCollaborationFolderKind::Calendar => {
                        let mut rows =
                            calendar_content_rows(snapshot, *folder_id, restriction.as_ref());
                        sort_events(&mut rows, sort_orders);
                        rows.into_iter().map(|event| event.id).collect()
                    }
                    MapiCollaborationFolderKind::Task => {
                        let mut rows = snapshot.tasks_for_folder(*folder_id);
                        rows.retain(|task| {
                            restriction_matches_task(restriction.as_ref(), &task.task)
                        });
                        sort_tasks(&mut rows, sort_orders);
                        rows.into_iter().map(|task| task.id).collect()
                    }
                };
            }
            if is_contact_contents_folder(*folder_id) {
                let mut rows = snapshot.contacts_for_folder(*folder_id);
                rows.retain(|contact| {
                    restriction_matches_contact_in_folder(
                        restriction.as_ref(),
                        &contact.contact,
                        *folder_id,
                    )
                });
                sort_contacts(&mut rows, sort_orders);
                return rows.into_iter().map(|contact| contact.id).collect();
            }
            if *folder_id == NOTES_FOLDER_ID {
                let mut rows = snapshot.notes_for_folder(*folder_id);
                rows.retain(|note| restriction_matches_note(restriction.as_ref(), &note.note));
                sort_notes(&mut rows, sort_orders);
                return rows.into_iter().map(|note| note.id).collect();
            }
            if *folder_id == CONTACTS_SEARCH_FOLDER_ID {
                let mut rows = snapshot.contacts_search_results();
                rows.retain(|contact| {
                    restriction_matches_contact_in_folder(
                        restriction.as_ref(),
                        &contact.contact,
                        CONTACTS_SEARCH_FOLDER_ID,
                    )
                });
                sort_contacts(&mut rows, sort_orders);
                return rows.into_iter().map(|contact| contact.id).collect();
            }
            if *folder_id == FREEBUSY_DATA_FOLDER_ID {
                let mut rows = snapshot
                    .delegate_freebusy_messages()
                    .iter()
                    .collect::<Vec<_>>();
                rows.retain(|message| {
                    restriction_matches(restriction.as_ref(), |property_tag| {
                        delegate_freebusy_property_value(message, property_tag)
                    })
                });
                return rows.into_iter().map(|message| message.id).collect();
            }
            if *folder_id == TODO_SEARCH_FOLDER_ID {
                let mut rows = todo_search_content_rows(snapshot, restriction.as_ref());
                sort_search_content_rows(&mut rows, sort_orders);
                return rows
                    .into_iter()
                    .map(|row| search_content_row_id(&row))
                    .collect();
            }
            if *folder_id == TRACKED_MAIL_PROCESSING_FOLDER_ID {
                let mut rows = snapshot.tracked_mail_processing_messages();
                retain_rows_by_restriction(
                    &mut rows,
                    restriction.as_ref(),
                    |message, restriction| {
                        restriction_matches_email_in_snapshot(
                            restriction,
                            &message.email,
                            *folder_id,
                            snapshot,
                        )
                    },
                );
                sort_mapi_messages(&mut rows, sort_orders);
                return rows.into_iter().map(|message| message.id).collect();
            }
            if *folder_id == REMINDERS_FOLDER_ID {
                let mut rows = reminder_search_content_rows(snapshot, restriction.as_ref());
                sort_search_content_rows(&mut rows, sort_orders);
                return rows
                    .into_iter()
                    .map(|row| search_content_row_id(&row))
                    .collect();
            }
            if *folder_id == JOURNAL_FOLDER_ID {
                let mut rows = snapshot.journal_entries_for_folder(*folder_id);
                rows.retain(|entry| {
                    restriction_matches_journal_entry(restriction.as_ref(), &entry.entry)
                });
                sort_journal_entries(&mut rows, sort_orders);
                return rows.into_iter().map(|entry| entry.id).collect();
            }
            if crate::mapi_store::recoverable_storage_folder(*folder_id).is_some() {
                let mut rows = snapshot.recoverable_items_for_folder(*folder_id);
                sort_recoverable_items(&mut rows, sort_orders);
                return rows.into_iter().map(|item| item.id).collect();
            }
            let window_emails = if is_top_level_count_restriction(restriction.as_ref()) {
                None
            } else {
                snapshot.content_table_window_emails_containing(
                    *folder_id,
                    table_view_signature(sort_orders, restriction.as_ref()),
                    *position,
                )
            };
            if let Some((offset, total, window_emails)) = window_emails {
                let mut row_keys = vec![0; total];
                for (index, email) in window_emails.into_iter().enumerate() {
                    let row_index = offset.saturating_add(index);
                    if row_index < row_keys.len() {
                        row_keys[row_index] = mapi_message_id(email);
                    }
                }
                return row_keys;
            }
            let total = if is_top_level_count_restriction(restriction.as_ref()) {
                None
            } else {
                snapshot.content_table_total(
                    *folder_id,
                    table_view_signature(sort_orders, restriction.as_ref()),
                )
            };
            if let Some(total) = total {
                return vec![0; total];
            }
            let mut rows = emails_for_folder(*folder_id, mailboxes, emails);
            retain_rows_by_restriction(&mut rows, restriction.as_ref(), |email, restriction| {
                restriction_matches_email_in_snapshot(restriction, email, *folder_id, snapshot)
            });
            sort_emails(&mut rows, sort_orders);
            rows.into_iter().map(mapi_message_id).collect()
        }
        MapiObject::AttachmentTable {
            folder_id,
            message_id,
            sort_orders,
            restriction,
            ..
        } => {
            let mut rows = snapshot
                .attachments_for_message(*folder_id, *message_id)
                .unwrap_or_default()
                .iter()
                .collect::<Vec<_>>();
            retain_rows_by_restriction(
                &mut rows,
                restriction.as_ref(),
                |attachment, restriction| restriction_matches_attachment(restriction, attachment),
            );
            sort_attachments(&mut rows, sort_orders);
            rows.into_iter()
                .map(|attachment| u64::from(attachment.attach_num))
                .collect()
        }
        MapiObject::PermissionTable { folder_id, .. } => snapshot
            .permissions_for_folder(*folder_id)
            .into_iter()
            .map(|permission| {
                permission
                    .member_account_id
                    .and_then(|id| crate::mapi::identity::mapped_mapi_object_id(&id))
                    .unwrap_or(0)
            })
            .collect(),
        MapiObject::RuleTable { folder_id, .. } => {
            if snapshot.public_folder_for_id(*folder_id).is_some() {
                Vec::new()
            } else {
                snapshot.rules().iter().map(|rule| rule.id).collect()
            }
        }
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests;

pub(in crate::mapi) fn serialize_folder_row_with_context(
    mailbox: &JmapMailbox,
    mailboxes: &[JmapMailbox],
    columns: &[u32],
    mailbox_guid: Uuid,
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match *column {
            PID_TAG_DISPLAY_NAME_W => write_utf16z(&mut row, &mapi_mailbox_display_name(mailbox)),
            PID_TAG_FOLDER_ID => write_object_id(&mut row, mapi_folder_id(mailbox)),
            PID_TAG_PARENT_FOLDER_ID => write_object_id(&mut row, mapi_parent_folder_id(mailbox)),
            PID_TAG_CONTENT_COUNT => write_u32(&mut row, mailbox.total_emails),
            PID_TAG_CONTENT_UNREAD_COUNT => write_u32(&mut row, mailbox.unread_emails),
            PID_TAG_MESSAGE_SIZE => write_u32(
                &mut row,
                mailbox.size_octets.min(u64::from(u32::MAX)) as u32,
            ),
            PID_TAG_MESSAGE_SIZE_EXTENDED => write_u64(&mut row, mailbox.size_octets),
            PID_TAG_SUBFOLDERS => row.push(mailbox_has_subfolders(mailbox, mailboxes) as u8),
            PID_TAG_FOLDER_TYPE => write_u32(&mut row, folder_type(mailbox)),
            PID_TAG_ACCESS => write_u32(&mut row, MAPI_FOLDER_ACCESS),
            PID_TAG_CONTAINER_CLASS_W => write_utf16z(&mut row, folder_message_class(mailbox)),
            PID_TAG_MESSAGE_CLASS_W => write_utf16z(&mut row, folder_message_class(mailbox)),
            _ => match mailbox_property_value_with_context_for_account(
                mailbox,
                mailboxes,
                *column,
                mailbox_guid,
            ) {
                Some(value) => write_mapi_value(&mut row, *column, &value),
                None => write_property_default(&mut row, *column),
            },
        }
    }
    row
}

pub(in crate::mapi) fn serialize_collaboration_folder_row_with_context(
    folder: &MapiCollaborationFolder,
    columns: &[u32],
    associated_count: u32,
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match *column {
            PID_TAG_DISPLAY_NAME_W => write_utf16z(&mut row, &folder.collection.display_name),
            PID_TAG_FOLDER_ID => write_object_id(&mut row, folder.id),
            PID_TAG_PARENT_FOLDER_ID => write_object_id(&mut row, IPM_SUBTREE_FOLDER_ID),
            PID_TAG_CONTENT_COUNT => write_u32(&mut row, folder.item_count),
            PID_TAG_CONTENT_UNREAD_COUNT => write_u32(&mut row, 0),
            PID_TAG_ASSOCIATED_CONTENT_COUNT => write_u32(&mut row, associated_count),
            PID_TAG_SUBFOLDERS => row.push(0),
            PID_TAG_FOLDER_TYPE => write_u32(&mut row, FOLDER_GENERIC),
            PID_TAG_ACCESS => write_u32(&mut row, MAPI_FOLDER_ACCESS),
            PID_TAG_CONTAINER_CLASS_W => {
                write_utf16z(&mut row, collaboration_folder_message_class(folder.kind))
            }
            PID_TAG_MESSAGE_CLASS_W => {
                write_utf16z(&mut row, collaboration_folder_message_class(folder.kind))
            }
            _ => match collaboration_folder_property_value(folder, *column) {
                Some(value) => write_mapi_value(&mut row, *column, &value),
                None => write_property_default(&mut row, *column),
            },
        }
    }
    row
}

pub(in crate::mapi) fn serialize_public_folder_row(
    folder: &MapiPublicFolder,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    let parent_folder_id = folder
        .folder
        .parent_folder_id
        .and_then(|parent_id| crate::mapi::identity::mapped_mapi_object_id(&parent_id))
        .unwrap_or(PUBLIC_FOLDERS_ROOT_FOLDER_ID);
    for column in columns {
        match *column {
            PID_TAG_DISPLAY_NAME_W => write_utf16z(&mut row, &folder.folder.display_name),
            PID_TAG_FOLDER_ID => write_object_id(&mut row, folder.id),
            PID_TAG_PARENT_FOLDER_ID => write_object_id(&mut row, parent_folder_id),
            PID_TAG_CONTENT_COUNT => write_u32(&mut row, folder.item_count),
            PID_TAG_CONTENT_UNREAD_COUNT => write_u32(&mut row, 0),
            PID_TAG_SUBFOLDERS => row.push((folder.child_count > 0) as u8),
            PID_TAG_FOLDER_TYPE => write_u32(&mut row, FOLDER_GENERIC),
            PID_TAG_ACCESS => write_u32(&mut row, MAPI_FOLDER_ACCESS),
            PID_TAG_CONTAINER_CLASS_W | PID_TAG_MESSAGE_CLASS_W => {
                write_utf16z(&mut row, &folder.folder.folder_class)
            }
            _ => match public_folder_property_value(folder, *column) {
                Some(value) => write_mapi_value(&mut row, *column, &value),
                None => write_property_default(&mut row, *column),
            },
        }
    }
    row
}

pub(in crate::mapi) fn serialize_message_row(email: &JmapEmail, columns: &[u32]) -> Vec<u8> {
    serialize_message_row_with_table_instance(email, columns, 0, 0, None)
}

fn serialize_categorized_message_row(
    email: &JmapEmail,
    columns: &[u32],
    category_property_tag: u32,
    category_value: &str,
    instance_num: u32,
) -> Vec<u8> {
    serialize_message_row_with_table_instance(
        email,
        columns,
        instance_num,
        1,
        Some((category_property_tag, category_value)),
    )
}

fn serialize_message_row_with_table_instance(
    email: &JmapEmail,
    columns: &[u32],
    instance_num: u32,
    depth: u32,
    category_value: Option<(u32, &str)>,
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        let storage_tag = canonical_property_storage_tag(*column);
        if let Some((category_property_tag, value)) = category_value {
            if storage_tag == canonical_property_storage_tag(category_property_tag) {
                write_category_instance_value(&mut row, *column, value);
                continue;
            }
        }
        match *column {
            PID_TAG_FOLDER_ID | PID_TAG_PARENT_FOLDER_ID => {
                write_object_id(&mut row, mapi_folder_id_for_email(email))
            }
            PID_TAG_MID => write_object_id(&mut row, mapi_message_id(email)),
            PID_TAG_INST_ID => write_u64(&mut row, mapi_message_id(email)),
            PID_TAG_INSTANCE_NUM => write_u32(&mut row, instance_num),
            PID_TAG_ROW_TYPE => write_u32(&mut row, TABLE_LEAF_ROW),
            PID_TAG_DEPTH => write_u32(&mut row, depth),
            PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
                write_utf16z(&mut row, &email.subject)
            }
            PID_TAG_MESSAGE_CLASS_W | PID_TAG_ORIGINAL_MESSAGE_CLASS_W => {
                write_utf16z(&mut row, message_class_for_email(email))
            }
            PID_TAG_CREATION_TIME
            | PID_TAG_MESSAGE_DELIVERY_TIME
            | PID_TAG_LAST_MODIFICATION_TIME
            | PID_TAG_LOCAL_COMMIT_TIME => write_u64(
                &mut row,
                mapi_mailstore::filetime_from_rfc3339_utc(&email.received_at),
            ),
            PID_TAG_CLIENT_SUBMIT_TIME => write_u64(
                &mut row,
                email
                    .sent_at
                    .as_deref()
                    .map(mapi_mailstore::filetime_from_rfc3339_utc)
                    .unwrap_or_default(),
            ),
            PID_TAG_ACCESS => write_u32(&mut row, MAPI_MESSAGE_ACCESS),
            PID_TAG_ACCESS_LEVEL => write_u32(&mut row, 1),
            PID_TAG_IMPORTANCE => write_u32(&mut row, 1),
            PID_TAG_PRIORITY | PID_TAG_SENSITIVITY => write_u32(&mut row, 0),
            PID_TAG_SUBJECT_PREFIX_W => write_utf16z(&mut row, ""),
            PID_TAG_MESSAGE_FLAGS => write_u32(&mut row, message_flags(email)),
            PID_TAG_READ => row.push((!email.unread) as u8),
            PID_TAG_MESSAGE_SIZE => {
                write_u32(&mut row, email.size_octets.clamp(0, u32::MAX as i64) as u32)
            }
            PID_TAG_MESSAGE_SIZE_EXTENDED => write_u64(&mut row, email.size_octets.max(0) as u64),
            PID_TAG_SENDER_NAME_W => write_utf16z(&mut row, email_sender_name(email)),
            PID_TAG_SENDER_ADDRESS_TYPE_W => write_utf16z(&mut row, "SMTP"),
            PID_TAG_SENDER_EMAIL_ADDRESS_W | PID_TAG_SENDER_SMTP_ADDRESS_W => {
                write_utf16z(&mut row, email_sender_address(email))
            }
            PID_TAG_SENT_REPRESENTING_NAME_W => {
                write_utf16z(&mut row, email_sent_representing_name(email))
            }
            PID_TAG_SENT_REPRESENTING_ENTRY_ID => {
                write_u16_prefixed_bytes(&mut row, &sent_representing_entry_id(email))
            }
            PID_TAG_SENT_REPRESENTING_ADDRESS_TYPE_W => write_utf16z(&mut row, "SMTP"),
            PID_TAG_SENT_REPRESENTING_EMAIL_ADDRESS_W
            | PID_TAG_SENT_REPRESENTING_SMTP_ADDRESS_W => {
                write_utf16z(&mut row, email_sent_representing_address(email))
            }
            PID_TAG_DISPLAY_TO_W => write_utf16z(&mut row, &display_to(email)),
            PID_TAG_DISPLAY_CC_W => write_utf16z(&mut row, &display_cc(email)),
            PID_TAG_DISPLAY_BCC_W => write_utf16z(&mut row, &display_bcc(email)),
            PID_TAG_HAS_ATTACHMENTS => row.push(email.has_attachments as u8),
            PID_TAG_RTF_IN_SYNC => row.push(0),
            PID_TAG_BODY_W => write_utf16z(&mut row, &email.body_text),
            PID_TAG_RTF_COMPRESSED => {
                write_u16_prefixed_bytes(&mut row, &uncompressed_rtf_body(&email.body_text))
            }
            PID_TAG_NATIVE_BODY => write_u32(&mut row, native_body_format(email)),
            PID_TAG_INTERNET_CODEPAGE => write_u32(&mut row, 65001),
            PID_TAG_MESSAGE_LOCALE_ID => write_u32(&mut row, 0x0409),
            PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &crate::mapi::identity::instance_key_for_object_id(mapi_message_id(email)),
            ),
            PID_TAG_INTERNET_MESSAGE_ID_W => {
                write_utf16z(&mut row, email.internet_message_id.as_deref().unwrap_or(""))
            }
            PID_NAME_CONTENT_CLASS_W_TAG => write_utf16z(&mut row, "urn:content-classes:message"),
            _ => match email_property_value(email, *column) {
                Some(value) => write_mapi_value(&mut row, *column, &value),
                None => write_property_default(&mut row, *column),
            },
        }
    }
    row
}

pub(in crate::mapi) fn serialize_navigation_shortcut_row(
    message: &MapiNavigationShortcutMessage,
    principal: Option<&AccountPrincipal>,
    columns: &[u32],
) -> Vec<u8> {
    let account_id = principal
        .map(|principal| principal.account_id)
        .unwrap_or_default();
    let mut row = Vec::new();
    for column in columns {
        match navigation_shortcut_property_value(message, account_id, *column) {
            Some(value) => write_mapi_value(&mut row, *column, &value),
            None => write_property_default(&mut row, *column),
        }
    }
    row
}

fn serialize_common_views_row_with_mailbox_guid(
    message: &MapiCommonViewsMessage,
    mailbox_guid: Uuid,
    columns: &[u32],
) -> Vec<u8> {
    match message {
        MapiCommonViewsMessage::NavigationShortcut(message) => {
            serialize_navigation_shortcut_row_with_mailbox_guid(message, mailbox_guid, columns)
        }
        MapiCommonViewsMessage::NamedView(message) => {
            serialize_common_view_named_view_row_with_mailbox_guid(message, mailbox_guid, columns)
        }
        MapiCommonViewsMessage::SearchFolderDefinition(message) => {
            serialize_search_folder_definition_row_with_mailbox_guid(message, mailbox_guid, columns)
        }
    }
}

pub(in crate::mapi) fn serialize_search_folder_definition_row_with_mailbox_guid(
    message: &SearchFolderDefinition,
    mailbox_guid: Uuid,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match search_folder_definition_message_property_value(message, mailbox_guid, *column) {
            Some(value) => write_mapi_value(&mut row, *column, &value),
            None => write_property_default(&mut row, *column),
        }
    }
    row
}

fn serialize_navigation_shortcut_row_with_mailbox_guid(
    message: &MapiNavigationShortcutMessage,
    mailbox_guid: Uuid,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match navigation_shortcut_property_value(message, mailbox_guid, *column) {
            Some(value) => write_mapi_value(&mut row, *column, &value),
            None => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(in crate::mapi) fn serialize_common_view_named_view_row_with_mailbox_guid(
    message: &MapiCommonViewNamedViewMessage,
    mailbox_guid: Uuid,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match common_view_named_view_property_value(message, mailbox_guid, *column) {
            Some(value) => write_mapi_value(&mut row, *column, &value),
            None => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(in crate::mapi) fn serialize_conversation_action_row(
    message: &MapiConversationActionMessage,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match conversation_action_property_value(message, *column) {
            Some(value) => write_mapi_value(&mut row, *column, &value),
            None => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(in crate::mapi) fn serialize_delegate_freebusy_row(
    message: &MapiDelegateFreeBusyMessage,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match delegate_freebusy_property_value(message, *column) {
            Some(value) => write_mapi_value(&mut row, *column, &value),
            None => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(in crate::mapi) fn serialize_associated_config_row_with_mailbox_guid(
    message: &MapiAssociatedConfigMessage,
    mailbox_guid: Uuid,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match associated_config_property_value_with_mailbox_guid(message, mailbox_guid, *column) {
            Some(value) => write_mapi_value(&mut row, *column, &value),
            None => write_property_default(&mut row, *column),
        }
    }
    row
}

fn serialize_associated_table_row(
    message: &AssociatedTableRow,
    mailbox_guid: Uuid,
    columns: &[u32],
) -> Vec<u8> {
    match message {
        AssociatedTableRow::Config(message) => {
            serialize_associated_config_row_with_mailbox_guid(message, mailbox_guid, columns)
        }
        AssociatedTableRow::NamedView(message) => {
            serialize_common_view_named_view_row_with_mailbox_guid(message, mailbox_guid, columns)
        }
    }
}

fn associated_table_row_property_value(
    message: &AssociatedTableRow,
    mailbox_guid: Uuid,
    property_tag: u32,
) -> Option<MapiValue> {
    match message {
        AssociatedTableRow::Config(message) => {
            associated_config_property_value_with_mailbox_guid(message, mailbox_guid, property_tag)
        }
        AssociatedTableRow::NamedView(message) => {
            common_view_named_view_property_value(message, mailbox_guid, property_tag)
        }
    }
}

fn associated_table_row_matches(
    message: &AssociatedTableRow,
    restriction: Option<&MapiRestriction>,
    _mailbox_guid: Uuid,
) -> bool {
    match message {
        AssociatedTableRow::Config(message) => {
            restriction_matches_associated_config(restriction, message)
        }
        AssociatedTableRow::NamedView(message) => {
            restriction_matches_common_view_named_view(restriction, message, _mailbox_guid)
        }
    }
}

fn associated_table_row_config(
    message: &AssociatedTableRow,
) -> Option<&MapiAssociatedConfigMessage> {
    match message {
        AssociatedTableRow::Config(message) => Some(message),
        AssociatedTableRow::NamedView(_) => None,
    }
}

fn associated_table_row_id(message: &AssociatedTableRow) -> u64 {
    match message {
        AssociatedTableRow::Config(message) => message.id,
        AssociatedTableRow::NamedView(message) => message.id,
    }
}

fn associated_table_row_message_class(message: &AssociatedTableRow) -> &str {
    match message {
        AssociatedTableRow::Config(message) => &message.message_class,
        AssociatedTableRow::NamedView(_) => "IPM.Microsoft.FolderDesign.NamedView",
    }
}

fn associated_config_message_size(message: &MapiAssociatedConfigMessage) -> i64 {
    message
        .subject
        .len()
        .saturating_add(message.message_class.len())
        .saturating_add(message.properties_json.to_string().len())
        .min(i64::MAX as usize) as i64
}

pub(in crate::mapi) fn associated_config_property_value(
    message: &MapiAssociatedConfigMessage,
    property_tag: u32,
) -> Option<MapiValue> {
    associated_config_property_value_with_mailbox_guid(message, Uuid::nil(), property_tag)
}

pub(in crate::mapi) fn associated_config_property_value_with_mailbox_guid(
    message: &MapiAssociatedConfigMessage,
    mailbox_guid: Uuid,
    property_tag: u32,
) -> Option<MapiValue> {
    let lookup_tag = canonical_property_storage_tag(property_tag);
    let properties = mapi_properties_from_json(&message.properties_json);
    properties.get(&lookup_tag).cloned().or_else(|| {
        let change_number = mapi_mailstore::change_number_for_store_id(message.id);
        match lookup_tag {
            PID_TAG_MID => Some(MapiValue::U64(message.id)),
            PID_TAG_INST_ID => Some(MapiValue::U64(message.id)),
            PID_TAG_INSTANCE_NUM => Some(MapiValue::U32(0)),
            PID_TAG_ENTRY_ID => crate::mapi::identity::message_entry_id_from_object_ids(
                mailbox_guid,
                message.folder_id,
                message.id,
            )
            .map(MapiValue::Binary),
            PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
                crate::mapi::identity::instance_key_for_object_id(message.id),
            )),
            PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W | PID_TAG_CONVERSATION_TOPIC_W => {
                Some(MapiValue::String(message.subject.clone()))
            }
            PID_TAG_MESSAGE_CLASS_W | PID_TAG_ORIGINAL_MESSAGE_CLASS_W => {
                Some(MapiValue::String(message.message_class.clone()))
            }
            PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(0x0000_0040)),
            PID_TAG_MESSAGE_STATUS => Some(MapiValue::U32(0)),
            PID_TAG_ACCESS_LEVEL => Some(MapiValue::U32(1)),
            PID_TAG_SENT_MAIL_SVR_EID => Some(MapiValue::Binary(Vec::new())),
            PID_TAG_ASSOCIATED => Some(MapiValue::Bool(true)),
            PID_TAG_MESSAGE_SIZE => Some(mapi_message_size_value(associated_config_message_size(
                message,
            ))),
            PID_TAG_MESSAGE_SIZE_EXTENDED => Some(mapi_message_size_extended_value(
                associated_config_message_size(message),
            )),
            PID_TAG_FOLDER_ID => Some(MapiValue::U64(message.folder_id)),
            PID_TAG_PARENT_FOLDER_ID => Some(MapiValue::U64(message.folder_id)),
            PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
                message.id,
            ))),
            PID_TAG_RECORD_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
                message.id,
            ))),
            PID_TAG_SEARCH_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
                message.id,
            ))),
            PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
                mapi_mailstore::source_key_for_store_id(message.folder_id),
            )),
            PID_TAG_PARENT_ENTRY_ID => crate::mapi::identity::folder_entry_id_from_object_id(
                mailbox_guid,
                message.folder_id,
            )
            .map(MapiValue::Binary),
            PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
                mapi_mailstore::change_key_for_change_number(change_number),
            )),
            PID_TAG_PREDECESSOR_CHANGE_LIST => Some(MapiValue::Binary(
                mapi_mailstore::predecessor_change_list(change_number),
            )),
            PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(change_number)),
            PID_TAG_LAST_MODIFICATION_TIME
            | PID_TAG_LOCAL_COMMIT_TIME
            | PID_TAG_MESSAGE_DELIVERY_TIME => Some(MapiValue::I64(
                associated_config_last_modified_filetime(message)
                    .unwrap_or_else(|| mapi_mailstore::filetime_from_change_number(change_number))
                    as i64,
            )),
            PID_TAG_ROAMING_DATATYPES
                if message.message_class.starts_with("IPM.Configuration.") =>
            {
                Some(MapiValue::U32(configuration_roaming_datatypes(
                    &message.message_class,
                    &properties,
                )))
            }
            PID_TAG_ROAMING_DICTIONARY
                if message.message_class.starts_with("IPM.Configuration.") =>
            {
                (!matches!(
                    message.message_class.as_str(),
                    "IPM.Configuration.CategoryList" | "IPM.Configuration.WorkHours"
                ) && !properties.contains_key(&PID_TAG_ROAMING_DATATYPES))
                .then(|| MapiValue::Binary(minimal_roaming_dictionary_stream()))
            }
            PID_TAG_ROAMING_XML_STREAM
                if message.message_class == "IPM.Configuration.WorkHours" =>
            {
                (!properties.contains_key(&PID_TAG_ROAMING_DATATYPES))
                    .then(|| MapiValue::Binary(minimal_working_hours_roaming_xml_stream()))
            }
            PID_TAG_ROAMING_XML_STREAM
                if message.message_class == "IPM.Configuration.CategoryList" =>
            {
                (!properties.contains_key(&PID_TAG_ROAMING_DATATYPES))
                    .then(|| MapiValue::Binary(minimal_category_list_roaming_xml_stream()))
            }
            PID_TAG_ROAMING_XML_STREAM if message.message_class == "IPM.Configuration.MRM" => {
                (!properties.contains_key(&PID_TAG_ROAMING_DATATYPES))
                    .then(|| MapiValue::Binary(minimal_mrm_roaming_xml_stream()))
            }
            PID_TAG_ROAMING_DATATYPES
                if message.message_class
                    == crate::mapi_store::OUTLOOK_QUICK_STEP_CUSTOM_ACTION_CLASS =>
            {
                Some(MapiValue::U32(0x0000_0002))
            }
            PID_TAG_ROAMING_XML_STREAM
                if message.message_class
                    == crate::mapi_store::OUTLOOK_QUICK_STEP_CUSTOM_ACTION_CLASS =>
            {
                Some(MapiValue::Binary(minimal_custom_action_roaming_xml_stream()))
            }
            OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B
                if message.message_class
                    == crate::mapi_store::OUTLOOK_QUICK_STEP_CUSTOM_ACTION_CLASS =>
            {
                Some(MapiValue::Binary(Vec::new()))
            }
            OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B
                if message.message_class.starts_with("IPM.Configuration.") =>
            {
                Some(MapiValue::Binary(Vec::new()))
            }
            PID_NAME_CONTENT_CLASS_W_TAG
                if message.message_class.starts_with("IPM.Configuration.") =>
            {
                Some(MapiValue::String("urn:content-classes:message".to_string()))
            }
            PID_NAME_CONTENT_TYPE_W_TAG
                if message.message_class.starts_with("IPM.Configuration.") =>
            {
                Some(MapiValue::String("text/xml".to_string()))
            }
            PID_TAG_ROAMING_DATATYPES if is_outlook_contacts_helper_config(message) => {
                Some(MapiValue::U32(0))
            }
            0x685D_0003 if is_outlook_contacts_helper_config(message) => {
                Some(MapiValue::U32(outlook_configuration_stamp(message)))
            }
            OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B if is_outlook_contacts_helper_config(message) => {
                Some(MapiValue::Binary(Vec::new()))
            }
            tag if is_outlook_contacts_helper_config(message) => {
                outlook_contact_link_empty_property_value(tag)
            }
            PID_TAG_VIEW_DESCRIPTOR_FLAGS
                if message.message_class
                    == crate::mapi_store::OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_CLASS =>
            {
                Some(MapiValue::U32(14_745_605))
            }
            PID_TAG_VIEW_DESCRIPTOR_VERSION | PID_TAG_VIEW_DESCRIPTOR_VERSION_CANONICAL
                if message.message_class
                    == crate::mapi_store::OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_CLASS =>
            {
                Some(MapiValue::U32(8))
            }
            PID_TAG_VIEW_DESCRIPTOR_NAME_W
                if message.message_class
                    == crate::mapi_store::OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_CLASS =>
            {
                Some(MapiValue::String(message.subject.clone()))
            }
            PID_TAG_VIEW_DESCRIPTOR_STRINGS_W
                if message.message_class
                    == crate::mapi_store::OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_CLASS =>
            {
                let definition = outlook_mail_view_definition(&message.subject);
                log_view_definition_diagnostics(
                    message.folder_id,
                    message.id,
                    &message.subject,
                    &definition,
                );
                Some(MapiValue::String(view_descriptor_strings(&definition)))
            }
            PID_TAG_VIEW_DESCRIPTOR_VIEW_MODE
                if message.message_class
                    == crate::mapi_store::OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_CLASS =>
            {
                Some(MapiValue::U32(0))
            }
            PID_TAG_VIEW_DESCRIPTOR_BINARY
            | OUTLOOK_COMMON_VIEW_DESCRIPTOR_BINARY_6835
            | OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B
                if message.message_class
                    == crate::mapi_store::OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_CLASS =>
            {
                let definition = outlook_mail_view_definition(&message.subject);
                log_view_definition_diagnostics(
                    message.folder_id,
                    message.id,
                    &message.subject,
                    &definition,
                );
                Some(MapiValue::Binary(view_descriptor_binary(&definition)))
            }
            OUTLOOK_COMMON_VIEW_DESCRIPTOR_STRINGS_683C
                if message.message_class
                    == crate::mapi_store::OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_CLASS =>
            {
                let definition = outlook_mail_view_definition(&message.subject);
                log_view_definition_diagnostics(
                    message.folder_id,
                    message.id,
                    &message.subject,
                    &definition,
                );
                Some(MapiValue::Binary(view_descriptor_strings_binary(
                    &definition,
                )))
            }
            tag if message.message_class
                == crate::mapi_store::OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_CLASS
                && property_tag_id_matches(tag, PID_TAG_VIEW_DESCRIPTOR_CLSID) =>
            {
                Some(guid_property_value(
                    property_tag,
                    *message.canonical_id.as_bytes(),
                ))
            }
            tag if message.message_class
                == crate::mapi_store::OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_CLASS
                && property_tag_id_matches(tag, PID_TAG_VIEW_DESCRIPTOR_FOLDER_TYPE) =>
            {
                Some(guid_property_value(
                    property_tag,
                    common_view_named_view_folder_type_guid(),
                ))
            }
            tag if message.message_class
                == crate::mapi_store::OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_CLASS
                && property_tag_id_matches(tag, PID_TAG_WLINK_GROUP_HEADER_ID) =>
            {
                Some(guid_property_value(
                    property_tag,
                    default_wlink_group_guid(),
                ))
            }
            PID_LID_OUTLOOK_SHARING_PROVIDER_GUID_TAG
                if is_outlook_virtual_sharing_state_config(message) =>
            {
                Some(MapiValue::Guid(Uuid::nil().into_bytes()))
            }
            PID_LID_OUTLOOK_SHARING_REMOTE_NAME_TAG
                if is_outlook_virtual_sharing_state_config(message) =>
            {
                Some(MapiValue::String(String::new()))
            }
            PID_LID_OUTLOOK_SHARING_REMOTE_UID_TAG
                if is_outlook_virtual_sharing_state_config(message) =>
            {
                Some(MapiValue::String(String::new()))
            }
            PID_LID_OUTLOOK_SHARING_LOCAL_TYPE_TAG
                if is_outlook_virtual_sharing_state_config(message) =>
            {
                Some(MapiValue::Guid(Uuid::nil().into_bytes()))
            }
            PID_NAME_SHARING_SEND_AS_STATE_TAG | PID_LID_OUTLOOK_SHARING_8AA6_TAG
                if is_outlook_virtual_sharing_state_config(message) =>
            {
                Some(MapiValue::U32(0))
            }
            PID_LID_OUTLOOK_SHARING_CAPABILITIES_TAG
                if is_outlook_virtual_sharing_state_config(message) =>
            {
                Some(MapiValue::U32(0))
            }
            0x685D_0003 if message.message_class.starts_with("IPM.Configuration.") => {
                Some(MapiValue::U32(outlook_configuration_stamp(message)))
            }
            PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_MESSAGE_ACCESS)),
            _ => None,
        }
    })
}

fn associated_config_last_modified_filetime(message: &MapiAssociatedConfigMessage) -> Option<u64> {
    message
        .properties_json
        .get("__lpe_updated_at")
        .and_then(serde_json::Value::as_str)
        .map(mapi_mailstore::filetime_from_rfc3339_utc)
        .filter(|filetime| *filetime != 0)
}

fn is_outlook_virtual_sharing_state_config(message: &MapiAssociatedConfigMessage) -> bool {
    matches!(
        message.message_class.as_str(),
        "IPM.Aggregation" | "IPM.Sharing.Configuration" | "IPM.Sharing.Index"
    )
}

fn is_outlook_contacts_helper_config(message: &MapiAssociatedConfigMessage) -> bool {
    matches!(
        message.message_class.as_str(),
        "IPM.Microsoft.ContactLink.TimeStamp" | "IPM.Microsoft.OSC.ContactSync"
    )
}

fn outlook_contact_link_empty_property_value(property_tag: u32) -> Option<MapiValue> {
    let tag = MapiPropertyTag::new(property_tag);
    let property_id = u32::from(tag.property_id());
    if !matches!(property_id, 0x8450 | 0x80E1 | 0x80EA | 0x80EC | 0x80ED) {
        return None;
    }
    match tag.property_type_code() {
        0x0003 => Some(MapiValue::U32(0)),
        0x000B => Some(MapiValue::Bool(false)),
        0x0040 => Some(MapiValue::I64(0)),
        0x001E | 0x001F => Some(MapiValue::String(String::new())),
        0x0102 => Some(MapiValue::Binary(Vec::new())),
        0x1003 => Some(MapiValue::MultiI32(Vec::new())),
        0x101E | 0x101F => Some(MapiValue::MultiString(Vec::new())),
        0x1102 => Some(MapiValue::MultiBinary(Vec::new())),
        _ => None,
    }
}

fn configuration_roaming_datatypes(
    message_class: &str,
    properties: &HashMap<u32, MapiValue>,
) -> u32 {
    let mut datatypes = 0;
    if properties.contains_key(&0x7C09_0102) {
        datatypes |= 0x0000_0001;
    }
    if properties.contains_key(&PID_TAG_ROAMING_XML_STREAM) {
        datatypes |= 0x0000_0002;
    }
    if properties.contains_key(&PID_TAG_ROAMING_DICTIONARY) {
        datatypes |= 0x0000_0004;
    }
    if datatypes == 0 {
        match message_class {
            "IPM.Configuration.CategoryList"
            | "IPM.Configuration.MRM"
            | "IPM.Configuration.WorkHours" => 0x0000_0002,
            _ => 0x0000_0004,
        }
    } else {
        datatypes
    }
}

pub(in crate::mapi) fn minimal_roaming_dictionary_stream() -> Vec<u8> {
    br#"<?xml version="1.0" encoding="utf-8"?><UserConfiguration xmlns="dictionary.xsd"><Info version="LPE.1"/><Data><e k="18-OLPrefsVersion" v="9-1"/></Data></UserConfiguration>"#.to_vec()
}

fn minimal_custom_action_roaming_xml_stream() -> Vec<u8> {
    br#"<?xml version="1.0" encoding="utf-8"?><customActions xmlns="http://schemas.microsoft.com/office/outlook/quicksteps/2010" version="1"/>"#.to_vec()
}

fn minimal_working_hours_roaming_xml_stream() -> Vec<u8> {
    br#"<?xml version="1.0"?><Root xmlns="WorkingHours.xsd"><WorkHoursVersion1><TimeZone><Bias>0</Bias><Standard><Bias>0</Bias><ChangeDate><Time>02:00:00</Time><Date>0000/11/01</Date><DayOfWeek>0</DayOfWeek></ChangeDate></Standard><DaylightSavings><Bias>0</Bias><ChangeDate><Time>02:00:00</Time><Date>0000/03/02</Date><DayOfWeek>0</DayOfWeek></ChangeDate></DaylightSavings><Name>UTC</Name></TimeZone><TimeSlot><Start>09:00:00</Start><End>17:00:00</End></TimeSlot><WorkDays>Monday Tuesday Wednesday Thursday Friday</WorkDays></WorkHoursVersion1></Root>"#.to_vec()
}

fn minimal_category_list_roaming_xml_stream() -> Vec<u8> {
    br#"<?xml version="1.0"?><categories default="Red Category" lastSavedSession="0" lastSavedTime="1601-01-01T00:00:00.000" xmlns="CategoryList.xsd"><category name="Red Category" color="0" keyboardShortcut="0" usageCount="0" lastTimeUsedNotes="1601-01-01T00:00:00.000" lastTimeUsedJournal="1601-01-01T00:00:00.000" lastTimeUsedContacts="1601-01-01T00:00:00.000" lastTimeUsedTasks="1601-01-01T00:00:00.000" lastTimeUsedCalendar="1601-01-01T00:00:00.000" lastTimeUsedMail="1601-01-01T00:00:00.000" lastTimeUsed="1601-01-01T00:00:00.000" lastSessionUsed="0" guid="{2B7FC69C-7046-44A2-8FF3-007D7467DC82}"/></categories>"#.to_vec()
}

fn minimal_mrm_roaming_xml_stream() -> Vec<u8> {
    br#"<?xml version="1.0"?><UserConfiguration><Info version="LPE.1"><Data><RetentionHold Enabled="False" RetentionComment="" RetentionUrl=""/></Data></Info></UserConfiguration>"#.to_vec()
}

pub(super) fn property_tag_id_matches(left: u32, right: u32) -> bool {
    (left & 0xFFFF_0000) == (right & 0xFFFF_0000)
}

fn guid_property_value(property_tag: u32, guid: [u8; 16]) -> MapiValue {
    if (property_tag & 0x0000_FFFF) == 0x0102 {
        MapiValue::Binary(guid.to_vec())
    } else {
        MapiValue::Guid(guid)
    }
}

fn outlook_configuration_stamp(message: &MapiAssociatedConfigMessage) -> u32 {
    let mut hash = 0x811c_9dc5u32;
    for byte in message
        .message_class
        .as_bytes()
        .iter()
        .chain(message.subject.as_bytes())
    {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash.max(1)
}

fn delegate_freebusy_message_size(message: &MapiDelegateFreeBusyMessage) -> i64 {
    message
        .message
        .subject
        .len()
        .saturating_add(message.message.body_text.len())
        .saturating_add(message.message.payload_json.len())
        .min(i64::MAX as usize) as i64
}

pub(in crate::mapi) fn delegate_freebusy_property_value(
    message: &MapiDelegateFreeBusyMessage,
    property_tag: u32,
) -> Option<MapiValue> {
    let change_number = mapi_mailstore::change_number_for_store_id(message.id);
    match canonical_property_storage_tag(property_tag) {
        PID_TAG_MID => Some(MapiValue::U64(message.id)),
        PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
            crate::mapi::identity::instance_key_for_object_id(message.id),
        )),
        PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
            Some(MapiValue::String(message.message.subject.clone()))
        }
        PID_TAG_BODY_W => Some(MapiValue::String(message.message.body_text.clone())),
        PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String(
            if message.message.message_kind == "delegate" {
                "IPM.Microsoft.Delegate".to_string()
            } else {
                "IPM.Microsoft.ScheduleData.FreeBusy".to_string()
            },
        )),
        PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(0x0000_0040)),
        PID_TAG_ASSOCIATED => Some(MapiValue::Bool(true)),
        PID_TAG_MESSAGE_SIZE => Some(mapi_message_size_value(delegate_freebusy_message_size(
            message,
        ))),
        PID_TAG_MESSAGE_SIZE_EXTENDED => Some(mapi_message_size_extended_value(
            delegate_freebusy_message_size(message),
        )),
        PID_TAG_PARENT_FOLDER_ID => Some(MapiValue::U64(message.folder_id)),
        PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
            message.id,
        ))),
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(message.folder_id),
        )),
        PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::change_key_for_change_number(change_number),
        )),
        PID_TAG_PREDECESSOR_CHANGE_LIST => Some(MapiValue::Binary(
            mapi_mailstore::predecessor_change_list(change_number),
        )),
        PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(change_number)),
        PID_TAG_LAST_MODIFICATION_TIME
        | PID_TAG_LOCAL_COMMIT_TIME
        | PID_TAG_MESSAGE_DELIVERY_TIME => Some(MapiValue::I64(
            mapi_mailstore::filetime_from_rfc3339_utc(&message.message.updated_at) as i64,
        )),
        PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_MESSAGE_ACCESS)),
        PID_TAG_VIEW_DESCRIPTOR_VIEW_MODE => Some(MapiValue::U32(0)),
        OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B => Some(MapiValue::Binary(Vec::new())),
        0x6842_000B | 0x6843_000B | 0x684B_000B | 0x686D_000B | 0x686E_000B | 0x686F_000B => {
            Some(MapiValue::Bool(false))
        }
        0x6844_101F | 0x684A_101F => Some(MapiValue::MultiString(Vec::new())),
        0x6845_1102 | 0x6870_1102 => Some(MapiValue::MultiBinary(Vec::new())),
        0x686B_1003 | 0x6871_1003 => Some(MapiValue::MultiI32(Vec::new())),
        0x6872_001F => Some(MapiValue::String(String::new())),
        _ => None,
    }
}
