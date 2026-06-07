use super::permissions::*;
use super::properties::*;
use super::rop::*;
use super::session::*;
use super::sync::*;
use super::wire::MapiPropertyType;
use super::*;
use crate::mapi::identity::{
    QUICK_STEP_SETTINGS_FOLDER_ID, RECOVERABLE_ITEMS_DELETIONS_FOLDER_ID,
    RECOVERABLE_ITEMS_PURGES_FOLDER_ID, RECOVERABLE_ITEMS_ROOT_FOLDER_ID,
    RECOVERABLE_ITEMS_VERSIONS_FOLDER_ID,
};
use crate::mapi_store::{
    MapiAssociatedConfigMessage, MapiCommonViewsMessage, MapiConversationActionMessage,
    MapiDelegateFreeBusyMessage, MapiEvent, MapiMessage, MapiNavigationShortcutMessage,
    MapiPublicFolder, MapiPublicFolderItem, MapiRule, MapiTask,
};

pub(in crate::mapi) fn hierarchy_row_count(
    folder_id: u64,
    mailboxes: &[JmapMailbox],
    snapshot: &MapiMailStoreSnapshot,
) -> u32 {
    if is_queryable_hierarchy_folder(folder_id)
        || snapshot.public_folder_for_id(folder_id).is_some()
    {
        hierarchy_rows(folder_id, mailboxes, snapshot, None, &[], Uuid::nil())
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
    if let Some(folder) = snapshot.collaboration_folder_for_id(folder_id) {
        return folder.item_count;
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
    if let Some(folder) = snapshot.public_folder_for_id(folder_id) {
        return folder.item_count;
    }
    if folder_id == REMINDERS_FOLDER_ID {
        let count = snapshot.reminder_events().len()
            + snapshot.reminder_tasks().len()
            + snapshot.reminder_messages().len();
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
            .conversation_action_messages()
            .len()
            .min(u32::MAX as usize) as u32
    } else if folder_id == FREEBUSY_DATA_FOLDER_ID {
        snapshot
            .delegate_freebusy_messages()
            .len()
            .min(u32::MAX as usize) as u32
    } else if !snapshot
        .associated_config_messages_for_folder(folder_id)
        .is_empty()
    {
        snapshot
            .associated_config_messages_for_folder(folder_id)
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

pub(in crate::mapi) fn default_hierarchy_columns() -> Vec<u32> {
    vec![
        PID_TAG_DISPLAY_NAME_W,
        PID_TAG_ENTRY_ID,
        PID_TAG_INSTANCE_KEY,
        PID_TAG_FOLDER_ID,
        PID_TAG_PARENT_FOLDER_ID,
        PID_TAG_FOLDER_TYPE,
        PID_TAG_ACCESS,
        PID_TAG_SOURCE_KEY,
        PID_TAG_PARENT_SOURCE_KEY,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_CHANGE_NUMBER,
        PID_TAG_CONTENT_COUNT,
        PID_TAG_CONTENT_UNREAD_COUNT,
        PID_TAG_CONTAINER_CLASS_W,
        PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W,
        PID_TAG_SERIALIZED_REPLID_GUID_MAP,
        PID_TAG_SUBFOLDERS,
    ]
}

pub(in crate::mapi) fn default_contents_columns() -> Vec<u32> {
    vec![
        PID_TAG_MID,
        PID_TAG_INST_ID,
        PID_TAG_INSTANCE_NUM,
        PID_TAG_SUBJECT_W,
        PID_TAG_NORMALIZED_SUBJECT_W,
        PID_TAG_MESSAGE_CLASS_W,
        PID_TAG_MESSAGE_DELIVERY_TIME,
        PID_TAG_CLIENT_SUBMIT_TIME,
        PID_TAG_MESSAGE_FLAGS,
        PID_TAG_READ,
        PID_TAG_MESSAGE_SIZE,
        PID_TAG_SENDER_NAME_W,
        PID_TAG_SENDER_EMAIL_ADDRESS_W,
        PID_TAG_DISPLAY_TO_W,
        PID_TAG_DISPLAY_CC_W,
        PID_TAG_HAS_ATTACHMENTS,
        PID_TAG_ENTRY_ID,
        PID_TAG_INSTANCE_KEY,
        PID_TAG_SOURCE_KEY,
        PID_TAG_PARENT_SOURCE_KEY,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_CHANGE_NUMBER,
    ]
}

const COLLAPSE_STATE_MAGIC: &[u8; 6] = b"LPECS1";

#[derive(Clone)]
struct CategorizedTableRow {
    category_id: u64,
    leaf_count: usize,
    row: Vec<u8>,
    leaf: bool,
}

fn category_id_for_value(folder_id: u64, property_tag: u32, value: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in folder_id
        .to_le_bytes()
        .into_iter()
        .chain(property_tag.to_le_bytes())
        .chain(value.as_bytes().iter().copied())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01B3);
    }
    hash | 0x8000_0000_0000_0000
}

fn category_value_for_email(email: &JmapEmail, property_tag: u32) -> String {
    match canonical_property_storage_tag(property_tag) {
        PID_NAME_KEYWORDS_TAG => email
            .categories
            .iter()
            .find(|category| !category.trim().is_empty())
            .map(|category| category.trim().to_string())
            .unwrap_or_default(),
        tag => email_property_value(email, tag)
            .map(|value| category_value_to_string(&value))
            .unwrap_or_default(),
    }
}

fn category_value_to_string(value: &MapiValue) -> String {
    match value {
        MapiValue::Bool(value) => value.to_string(),
        MapiValue::I16(value) => value.to_string(),
        MapiValue::I32(value) => value.to_string(),
        MapiValue::I64(value) => value.to_string(),
        MapiValue::U32(value) => value.to_string(),
        MapiValue::U64(value) => value.to_string(),
        MapiValue::String(value) => value.clone(),
        MapiValue::MultiString(values) => values.first().cloned().unwrap_or_default(),
        MapiValue::Binary(value) => format_bytes_hex(value),
        MapiValue::Guid(value) => format_bytes_hex(value),
        MapiValue::Error(value) => format!("{value:#010x}"),
        MapiValue::MultiI16(values) => values.first().map(i16::to_string).unwrap_or_default(),
        MapiValue::MultiI32(values) => values.first().map(i32::to_string).unwrap_or_default(),
        MapiValue::MultiI64(values) => values.first().map(i64::to_string).unwrap_or_default(),
        MapiValue::MultiBinary(values) => values
            .first()
            .map(|value| format_bytes_hex(value))
            .unwrap_or_default(),
        MapiValue::MultiGuid(values) => values
            .first()
            .map(|value| format_bytes_hex(value))
            .unwrap_or_default(),
    }
}

fn format_bytes_hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join("")
}

fn categorized_email_rows(
    folder_id: u64,
    emails: Vec<&JmapEmail>,
    columns: &[u32],
    sort_orders: &[MapiSortOrder],
    expanded_count: u16,
    collapsed_categories: &HashSet<u64>,
) -> Vec<CategorizedTableRow> {
    let Some(category_sort) = sort_orders.first() else {
        return emails
            .into_iter()
            .map(|email| CategorizedTableRow {
                category_id: 0,
                leaf_count: 1,
                row: serialize_message_row(email, columns),
                leaf: true,
            })
            .collect();
    };
    let mut groups: Vec<(u64, String, Vec<&JmapEmail>)> = Vec::new();
    for email in emails {
        let value = category_value_for_email(email, category_sort.property_tag);
        let category_id = category_id_for_value(folder_id, category_sort.property_tag, &value);
        if let Some((_, _, rows)) = groups.iter_mut().find(|(id, _, _)| *id == category_id) {
            rows.push(email);
        } else {
            groups.push((category_id, value, vec![email]));
        }
    }
    groups.sort_by(|left, right| {
        apply_sort_direction(
            compare_case_insensitive(&left.1, &right.1),
            category_sort.order,
        )
    });

    let mut rows = Vec::new();
    for (category_id, value, leaves) in groups {
        rows.push(CategorizedTableRow {
            category_id,
            leaf_count: leaves.len(),
            row: serialize_category_header_row(
                category_id,
                &value,
                leaves.len(),
                category_sort.property_tag,
                columns,
            ),
            leaf: false,
        });
        if expanded_count > 0 && !collapsed_categories.contains(&category_id) {
            rows.extend(leaves.into_iter().map(|email| CategorizedTableRow {
                category_id,
                leaf_count: 1,
                row: serialize_message_row(email, columns),
                leaf: true,
            }));
        }
    }
    rows
}

fn serialize_category_header_row(
    category_id: u64,
    value: &str,
    leaf_count: usize,
    category_property_tag: u32,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match canonical_property_storage_tag(*column) {
            PID_TAG_INST_ID => write_u64(&mut row, category_id),
            PID_TAG_INSTANCE_NUM => write_u32(&mut row, 0),
            PID_TAG_ROW_TYPE => write_u32(&mut row, 1),
            PID_TAG_CONTENT_COUNT => write_u32(&mut row, leaf_count.min(u32::MAX as usize) as u32),
            PID_TAG_CONTENT_UNREAD_COUNT => write_u32(&mut row, 0),
            tag if tag == canonical_property_storage_tag(category_property_tag) => {
                write_mapi_value(&mut row, *column, &MapiValue::String(value.to_string()))
            }
            _ => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(in crate::mapi) fn default_navigation_shortcut_property_tags() -> Vec<u32> {
    vec![
        PID_TAG_MID,
        PID_TAG_ENTRY_ID,
        PID_TAG_INSTANCE_KEY,
        PID_TAG_SUBJECT_W,
        PID_TAG_NORMALIZED_SUBJECT_W,
        PID_TAG_MESSAGE_CLASS_W,
        PID_TAG_MESSAGE_FLAGS,
        PID_TAG_MESSAGE_SIZE,
        PID_TAG_ACCESS,
        PID_TAG_ASSOCIATED,
        PID_TAG_PARENT_FOLDER_ID,
        PID_TAG_SOURCE_KEY,
        PID_TAG_PARENT_SOURCE_KEY,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_CHANGE_NUMBER,
        PID_TAG_WLINK_SAVE_STAMP,
        PID_TAG_WLINK_TYPE,
        PID_TAG_WLINK_FLAGS,
        PID_TAG_WLINK_ORDINAL,
        PID_TAG_WLINK_ENTRY_ID,
        PID_TAG_WLINK_RECORD_KEY,
        PID_TAG_WLINK_STORE_ENTRY_ID,
        PID_TAG_WLINK_FOLDER_TYPE,
        PID_TAG_WLINK_GROUP_HEADER_ID,
        PID_TAG_WLINK_GROUP_CLSID,
        PID_TAG_WLINK_GROUP_NAME_W,
        PID_TAG_WLINK_SECTION,
    ]
}

pub(in crate::mapi) fn default_conversation_action_property_tags() -> Vec<u32> {
    vec![
        PID_TAG_MID,
        PID_TAG_ENTRY_ID,
        PID_TAG_INSTANCE_KEY,
        PID_TAG_SUBJECT_W,
        PID_TAG_NORMALIZED_SUBJECT_W,
        PID_TAG_MESSAGE_CLASS_W,
        PID_TAG_MESSAGE_FLAGS,
        PID_TAG_MESSAGE_SIZE,
        PID_TAG_ACCESS,
        PID_TAG_ASSOCIATED,
        PID_TAG_PARENT_FOLDER_ID,
        PID_TAG_SOURCE_KEY,
        PID_TAG_PARENT_SOURCE_KEY,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_CHANGE_NUMBER,
        PID_TAG_CONVERSATION_INDEX,
        PID_LID_CONVERSATION_ACTION_LAST_APPLIED_TIME_TAG,
        PID_LID_CONVERSATION_ACTION_MAX_DELIVERY_TIME_TAG,
        PID_LID_CONVERSATION_ACTION_MOVE_FOLDER_EID_TAG,
        PID_LID_CONVERSATION_ACTION_MOVE_STORE_EID_TAG,
        PID_LID_CONVERSATION_ACTION_VERSION_TAG,
        PID_LID_CONVERSATION_PROCESSED_TAG,
        PID_NAME_KEYWORDS_TAG,
    ]
}

pub(in crate::mapi) fn default_attachment_columns() -> Vec<u32> {
    vec![
        PID_TAG_ATTACH_NUM,
        PID_TAG_ATTACH_LONG_FILENAME_W,
        PID_TAG_ATTACH_FILENAME_W,
        PID_TAG_ATTACH_MIME_TAG_W,
        PID_TAG_ATTACH_SIZE,
        PID_TAG_ATTACH_METHOD,
        PID_TAG_RENDERING_POSITION,
        PID_TAG_ENTRY_ID,
        PID_TAG_INSTANCE_KEY,
    ]
}

pub(in crate::mapi) fn default_folder_property_tags() -> Vec<u32> {
    vec![
        PID_TAG_DISPLAY_NAME_W,
        PID_TAG_ENTRY_ID,
        PID_TAG_INSTANCE_KEY,
        PID_TAG_FOLDER_ID,
        PID_TAG_PARENT_FOLDER_ID,
        PID_TAG_FOLDER_TYPE,
        PID_TAG_ACCESS,
        PID_TAG_CONTENT_COUNT,
        PID_TAG_CONTENT_UNREAD_COUNT,
        PID_TAG_SUBFOLDERS,
        PID_TAG_CONTAINER_CLASS_W,
        PID_TAG_MESSAGE_CLASS_W,
        PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W,
        PID_TAG_LAST_MODIFICATION_TIME,
        PID_TAG_LOCAL_COMMIT_TIME,
        PID_TAG_LOCAL_COMMIT_TIME_MAX,
        PID_TAG_HIERARCHY_CHANGE_NUMBER,
        PID_TAG_HIER_REV,
        PID_TAG_SERIALIZED_REPLID_GUID_MAP,
        PID_TAG_SOURCE_KEY,
        PID_TAG_PARENT_SOURCE_KEY,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_CHANGE_NUMBER,
    ]
}

pub(in crate::mapi) fn default_store_property_tags() -> Vec<u32> {
    vec![
        PID_TAG_VALID_FOLDER_MASK,
        PID_TAG_IPM_SUBTREE_ENTRY_ID,
        PID_TAG_IPM_OUTBOX_ENTRY_ID,
        PID_TAG_IPM_WASTEBASKET_ENTRY_ID,
        PID_TAG_IPM_SENTMAIL_ENTRY_ID,
        PID_TAG_VIEWS_ENTRY_ID,
        PID_TAG_COMMON_VIEWS_ENTRY_ID,
        PID_TAG_FINDER_ENTRY_ID,
        PID_TAG_IPM_ARCHIVE_ENTRY_ID,
        PID_TAG_IPM_APPOINTMENT_ENTRY_ID,
        PID_TAG_IPM_CONTACT_ENTRY_ID,
        PID_TAG_IPM_JOURNAL_ENTRY_ID,
        PID_TAG_IPM_NOTE_ENTRY_ID,
        PID_TAG_IPM_TASK_ENTRY_ID,
        PID_TAG_REM_ONLINE_ENTRY_ID,
        PID_TAG_IPM_DRAFTS_ENTRY_ID,
        PID_TAG_ADDITIONAL_REN_ENTRY_IDS,
        PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX,
        PID_TAG_FREE_BUSY_ENTRY_IDS,
        PID_TAG_SERIALIZED_REPLID_GUID_MAP,
        PID_TAG_MAILBOX_OWNER_ENTRY_ID,
        PID_TAG_MAILBOX_OWNER_NAME_W,
        PID_TAG_SERVER_TYPE_DISPLAY_NAME_W,
        PID_TAG_PRIVATE,
        PID_TAG_USER_GUID,
        PID_TAG_MAX_SUBMIT_MESSAGE_SIZE,
    ]
}

pub(in crate::mapi) fn default_folder_identity_property_tags() -> Vec<u32> {
    vec![
        PID_TAG_VALID_FOLDER_MASK,
        PID_TAG_IPM_SUBTREE_ENTRY_ID,
        PID_TAG_IPM_OUTBOX_ENTRY_ID,
        PID_TAG_IPM_WASTEBASKET_ENTRY_ID,
        PID_TAG_IPM_SENTMAIL_ENTRY_ID,
        PID_TAG_VIEWS_ENTRY_ID,
        PID_TAG_COMMON_VIEWS_ENTRY_ID,
        PID_TAG_FINDER_ENTRY_ID,
        PID_TAG_IPM_ARCHIVE_ENTRY_ID,
        PID_TAG_IPM_APPOINTMENT_ENTRY_ID,
        PID_TAG_IPM_CONTACT_ENTRY_ID,
        PID_TAG_IPM_JOURNAL_ENTRY_ID,
        PID_TAG_IPM_NOTE_ENTRY_ID,
        PID_TAG_IPM_TASK_ENTRY_ID,
        PID_TAG_REM_ONLINE_ENTRY_ID,
        PID_TAG_IPM_DRAFTS_ENTRY_ID,
        PID_TAG_ADDITIONAL_REN_ENTRY_IDS,
        PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX,
        PID_TAG_FREE_BUSY_ENTRY_IDS,
    ]
}

const PID_TAG_RULE_ID: u32 = 0x6674_0014;
const PID_TAG_RULE_SEQUENCE: u32 = 0x6676_0003;
const PID_TAG_RULE_STATE: u32 = 0x6677_0003;
const PID_TAG_RULE_USER_FLAGS: u32 = 0x6678_0003;
const PID_TAG_RULE_CONDITION: u32 = 0x6679_00FD;
const PID_TAG_RULE_ACTIONS: u32 = 0x6680_00FE;
const PID_TAG_RULE_PROVIDER: u32 = 0x6681_001F;
const PID_TAG_RULE_NAME: u32 = 0x6682_001F;
const PID_TAG_RULE_LEVEL: u32 = 0x6683_0003;
const PID_TAG_RULE_PROVIDER_DATA: u32 = 0x6684_0102;
const ST_ENABLED: u32 = 0x0000_0001;

pub(in crate::mapi) fn default_rule_columns() -> Vec<u32> {
    vec![
        PID_TAG_RULE_ID,
        PID_TAG_RULE_SEQUENCE,
        PID_TAG_RULE_STATE,
        PID_TAG_RULE_USER_FLAGS,
        PID_TAG_RULE_PROVIDER,
        PID_TAG_RULE_NAME,
        PID_TAG_RULE_LEVEL,
        PID_TAG_RULE_PROVIDER_DATA,
        PID_TAG_RULE_CONDITION,
        PID_TAG_RULE_ACTIONS,
    ]
}

pub(in crate::mapi) fn rop_get_rules_table_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x3F, request.response_handle_index()];
    write_u32(&mut response, 0);
    response
}

#[derive(Clone, Copy)]
enum HierarchyRow<'a> {
    Mailbox(&'a JmapMailbox),
    PublicFolder(&'a MapiPublicFolder),
    Collaboration(&'a MapiCollaborationFolder),
    Special(u64),
}

enum SearchContentRow<'a> {
    Event(&'a MapiEvent),
    Message(&'a MapiMessage),
    Task(&'a MapiTask),
}

fn hierarchy_rows<'a>(
    folder_id: u64,
    mailboxes: &'a [JmapMailbox],
    snapshot: &'a MapiMailStoreSnapshot,
    restriction: Option<&MapiRestriction>,
    sort_orders: &[MapiSortOrder],
    mailbox_guid: Uuid,
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
            .filter(|mailbox| !mailbox_shadows_outlook_special_folder(mailbox))
            .filter(|mailbox| mapi_folder_id(mailbox) != REMINDERS_FOLDER_ID)
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
            if folder_ids.insert(*special_folder_id)
                && special_hierarchy_row_matches(*special_folder_id, restriction, mailbox_guid)
            {
                rows.push(HierarchyRow::Special(*special_folder_id));
            }
        }
    } else if folder_id == IPM_SUBTREE_FOLDER_ID {
        for special_folder_id in IPM_SUBTREE_HIERARCHY_FOLDER_IDS {
            if folder_ids.insert(*special_folder_id)
                && special_hierarchy_row_matches(*special_folder_id, restriction, mailbox_guid)
            {
                rows.push(HierarchyRow::Special(*special_folder_id));
            }
        }
    } else if folder_id == SYNC_ISSUES_FOLDER_ID {
        for special_folder_id in SYNC_ISSUES_HIERARCHY_FOLDER_IDS {
            if folder_ids.insert(*special_folder_id)
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
    QUICK_CONTACTS_FOLDER_ID,
    IM_CONTACT_LIST_FOLDER_ID,
    CONTACTS_SEARCH_FOLDER_ID,
    CALENDAR_FOLDER_ID,
    JOURNAL_FOLDER_ID,
    NOTES_FOLDER_ID,
    TASKS_FOLDER_ID,
    SYNC_ISSUES_FOLDER_ID,
    CONFLICTS_FOLDER_ID,
    LOCAL_FAILURES_FOLDER_ID,
    SERVER_FAILURES_FOLDER_ID,
    JUNK_FOLDER_ID,
    RSS_FEEDS_FOLDER_ID,
    ARCHIVE_FOLDER_ID,
    CONVERSATION_HISTORY_FOLDER_ID,
];

const SYNC_ISSUES_HIERARCHY_FOLDER_IDS: &[u64] = &[
    CONFLICTS_FOLDER_ID,
    LOCAL_FAILURES_FOLDER_ID,
    SERVER_FAILURES_FOLDER_ID,
];

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
        HierarchyRow::Mailbox(mailbox) => &mailbox.name,
        HierarchyRow::PublicFolder(folder) => &folder.folder.display_name,
        HierarchyRow::Collaboration(folder) => &folder.collection.display_name,
        HierarchyRow::Special(folder_id) => special_folder_metadata(*folder_id).0,
    }
}

pub(in crate::mapi) fn mailbox_shadows_outlook_special_folder(mailbox: &JmapMailbox) -> bool {
    if mapi_parent_folder_id(mailbox) != IPM_SUBTREE_FOLDER_ID {
        return false;
    }

    matches!(
        mailbox.name.trim().to_ascii_lowercase().as_str(),
        "archive"
            | "calendar"
            | "conflicts"
            | "contacts"
            | "contacts search"
            | "conversation action settings"
            | "conversation history"
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
    )
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
    mailbox_guid: Uuid,
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
    let child_candidate_summary = SYNC_ISSUES_HIERARCHY_FOLDER_IDS
        .iter()
        .map(|child_id| {
            format!(
                "folder_id=0x{child_id:016x}:display_name={}:restriction_match={}",
                special_folder_metadata(*child_id).0,
                special_hierarchy_row_matches(*child_id, restriction, mailbox_guid)
            )
        })
        .collect::<Vec<_>>()
        .join("|");

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
        PID_TAG_SUBFOLDERS => Some(MapiValue::Bool(has_subfolders)),
        PID_TAG_ATTRIBUTE_HIDDEN => Some(MapiValue::Bool(
            folder_id == CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
        )),
        PID_TAG_CONTAINER_CLASS_W | PID_TAG_MESSAGE_CLASS_W if message_class.is_empty() => None,
        PID_TAG_CONTAINER_CLASS_W | PID_TAG_MESSAGE_CLASS_W => {
            Some(MapiValue::String(message_class.to_string()))
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
    columns: &[u32],
    mailbox_guid: Uuid,
) -> Vec<u8> {
    match row {
        HierarchyRow::Mailbox(mailbox) => {
            serialize_folder_row_with_context(mailbox, mailboxes, columns, mailbox_guid)
        }
        HierarchyRow::Collaboration(folder) => serialize_collaboration_folder_row(folder, columns),
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

pub(in crate::mapi) fn default_message_property_tags() -> Vec<u32> {
    vec![
        PID_TAG_MID,
        PID_TAG_ENTRY_ID,
        PID_TAG_INSTANCE_KEY,
        PID_TAG_SUBJECT_W,
        PID_TAG_NORMALIZED_SUBJECT_W,
        PID_TAG_MESSAGE_CLASS_W,
        PID_TAG_MESSAGE_DELIVERY_TIME,
        PID_TAG_ACCESS,
        PID_TAG_MESSAGE_FLAGS,
        PID_TAG_FLAG_STATUS,
        PID_TAG_MESSAGE_SIZE,
        PID_TAG_LOCAL_COMMIT_TIME,
        PID_TAG_SENDER_NAME_W,
        PID_TAG_SENDER_EMAIL_ADDRESS_W,
        PID_TAG_DISPLAY_TO_W,
        PID_TAG_HAS_ATTACHMENTS,
        PID_TAG_BODY_W,
        PID_TAG_INTERNET_MESSAGE_ID_W,
        PID_TAG_SOURCE_KEY,
        PID_TAG_PARENT_SOURCE_KEY,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_CHANGE_NUMBER,
        PID_LID_COMMON_START_TAG,
        PID_LID_COMMON_END_TAG,
        PID_LID_COMPANIES_TAG,
        PID_LID_CONTACTS_TAG,
        PID_LID_LOG_TYPE_W_TAG,
        PID_LID_LOG_START_TAG,
        PID_LID_LOG_END_TAG,
        PID_LID_LOG_DURATION_TAG,
        PID_LID_LOG_FLAGS_TAG,
        PID_LID_LOG_TYPE_DESC_W_TAG,
    ]
}

fn default_calendar_configuration_property_tags() -> Vec<u32> {
    let mut tags = default_message_property_tags();
    tags.extend([
        PID_TAG_ASSOCIATED,
        PID_TAG_ROAMING_DATATYPES,
        PID_TAG_ROAMING_DICTIONARY,
        PID_TAG_ROAMING_XML_STREAM,
    ]);
    tags
}

pub(in crate::mapi) fn default_contact_property_tags() -> Vec<u32> {
    vec![
        PID_TAG_MID,
        PID_TAG_ENTRY_ID,
        PID_TAG_INSTANCE_KEY,
        PID_TAG_DISPLAY_NAME_W,
        PID_TAG_GIVEN_NAME_W,
        PID_TAG_SURNAME_W,
        PID_TAG_EMAIL_ADDRESS_W,
        PID_TAG_SMTP_ADDRESS_W,
        PID_TAG_MOBILE_TELEPHONE_NUMBER_W,
        PID_TAG_BUSINESS_TELEPHONE_NUMBER_W,
        PID_TAG_COMPANY_NAME_W,
        PID_TAG_TITLE_W,
        PID_TAG_BODY_W,
        PID_TAG_MESSAGE_CLASS_W,
        PID_TAG_ACCESS,
        PID_TAG_MESSAGE_FLAGS,
        PID_TAG_MESSAGE_SIZE,
        PID_TAG_SOURCE_KEY,
        PID_TAG_PARENT_SOURCE_KEY,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_CHANGE_NUMBER,
    ]
}

pub(in crate::mapi) fn default_event_property_tags() -> Vec<u32> {
    vec![
        PID_TAG_MID,
        PID_TAG_ENTRY_ID,
        PID_TAG_INSTANCE_KEY,
        PID_TAG_SUBJECT_W,
        PID_TAG_NORMALIZED_SUBJECT_W,
        PID_TAG_BODY_W,
        PID_TAG_START_DATE,
        PID_TAG_END_DATE,
        PID_TAG_LOCATION_W,
        PID_TAG_MESSAGE_CLASS_W,
        PID_LID_REMINDER_SET_TAG,
        PID_LID_REMINDER_DELTA_TAG,
        PID_LID_REMINDER_TIME_TAG,
        PID_LID_REMINDER_SIGNAL_TIME_TAG,
        PID_LID_REMINDER_OVERRIDE_TAG,
        PID_LID_REMINDER_PLAY_SOUND_TAG,
        PID_LID_REMINDER_FILE_PARAMETER_W_TAG,
        PID_TAG_ACCESS,
        PID_TAG_MESSAGE_FLAGS,
        PID_TAG_MESSAGE_SIZE,
        PID_TAG_SOURCE_KEY,
        PID_TAG_PARENT_SOURCE_KEY,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_CHANGE_NUMBER,
    ]
}

pub(in crate::mapi) fn default_task_property_tags() -> Vec<u32> {
    vec![
        PID_TAG_MID,
        PID_TAG_ENTRY_ID,
        PID_TAG_INSTANCE_KEY,
        PID_TAG_SUBJECT_W,
        PID_TAG_NORMALIZED_SUBJECT_W,
        PID_TAG_BODY_W,
        PID_TAG_MESSAGE_CLASS_W,
        PID_TAG_ACCESS,
        PID_TAG_MESSAGE_FLAGS,
        PID_TAG_FLAG_STATUS,
        PID_LID_REMINDER_SET_TAG,
        PID_LID_REMINDER_DELTA_TAG,
        PID_LID_REMINDER_TIME_TAG,
        PID_LID_REMINDER_SIGNAL_TIME_TAG,
        PID_LID_REMINDER_OVERRIDE_TAG,
        PID_LID_REMINDER_PLAY_SOUND_TAG,
        PID_LID_REMINDER_FILE_PARAMETER_W_TAG,
        PID_TAG_MESSAGE_SIZE,
        PID_TAG_SOURCE_KEY,
        PID_TAG_PARENT_SOURCE_KEY,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_CHANGE_NUMBER,
    ]
}

pub(in crate::mapi) fn default_reminder_property_tags() -> Vec<u32> {
    vec![
        PID_TAG_MID,
        PID_TAG_ENTRY_ID,
        PID_TAG_INSTANCE_KEY,
        PID_TAG_SUBJECT_W,
        PID_TAG_NORMALIZED_SUBJECT_W,
        PID_TAG_MESSAGE_CLASS_W,
        PID_TAG_ACCESS,
        PID_TAG_MESSAGE_FLAGS,
        PID_TAG_MESSAGE_SIZE,
        PID_TAG_SOURCE_KEY,
        PID_TAG_PARENT_SOURCE_KEY,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_CHANGE_NUMBER,
        PID_LID_REMINDER_SET_TAG,
        PID_LID_REMINDER_DELTA_TAG,
        PID_LID_REMINDER_TIME_TAG,
        PID_LID_REMINDER_SIGNAL_TIME_TAG,
        PID_LID_REMINDER_OVERRIDE_TAG,
        PID_LID_REMINDER_PLAY_SOUND_TAG,
        PID_LID_REMINDER_FILE_PARAMETER_W_TAG,
    ]
}

pub(in crate::mapi) fn default_note_property_tags() -> Vec<u32> {
    vec![
        PID_TAG_MID,
        PID_TAG_ENTRY_ID,
        PID_TAG_INSTANCE_KEY,
        PID_TAG_SUBJECT_W,
        PID_TAG_NORMALIZED_SUBJECT_W,
        PID_TAG_BODY_W,
        PID_TAG_MESSAGE_CLASS_W,
        PID_TAG_ACCESS,
        PID_TAG_MESSAGE_FLAGS,
        PID_TAG_MESSAGE_SIZE,
        PID_TAG_SOURCE_KEY,
        PID_TAG_PARENT_SOURCE_KEY,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_CHANGE_NUMBER,
    ]
}

pub(in crate::mapi) fn default_journal_entry_property_tags() -> Vec<u32> {
    vec![
        PID_TAG_MID,
        PID_TAG_ENTRY_ID,
        PID_TAG_INSTANCE_KEY,
        PID_TAG_SUBJECT_W,
        PID_TAG_NORMALIZED_SUBJECT_W,
        PID_TAG_BODY_W,
        PID_TAG_START_DATE,
        PID_TAG_END_DATE,
        PID_TAG_MESSAGE_CLASS_W,
        PID_TAG_ACCESS,
        PID_TAG_MESSAGE_FLAGS,
        PID_TAG_MESSAGE_SIZE,
        PID_TAG_SOURCE_KEY,
        PID_TAG_PARENT_SOURCE_KEY,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_CHANGE_NUMBER,
    ]
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
            let rows = hierarchy_rows(
                *folder_id,
                mailboxes,
                snapshot,
                restriction.as_ref(),
                sort_orders,
                mailbox_guid,
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
                .map(|row| serialize_hierarchy_row(row, mailboxes, &columns, mailbox_guid))
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
                if *associated && *folder_id == COMMON_VIEWS_FOLDER_ID {
                    default_navigation_shortcut_property_tags()
                } else if *associated && *folder_id == CONVERSATION_ACTION_SETTINGS_FOLDER_ID {
                    default_conversation_action_property_tags()
                } else if *associated && *folder_id == FREEBUSY_DATA_FOLDER_ID {
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
                } else {
                    default_contents_columns()
                }
            } else {
                columns.clone()
            };
            if *associated {
                if *folder_id == COMMON_VIEWS_FOLDER_ID {
                    let mut rows = snapshot.common_views_table_messages().collect::<Vec<_>>();
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
                        .conversation_action_messages()
                        .iter()
                        .map(|message| serialize_conversation_action_row(message, &columns))
                        .collect::<Vec<_>>()
                } else if *folder_id == FREEBUSY_DATA_FOLDER_ID {
                    snapshot
                        .delegate_freebusy_messages()
                        .iter()
                        .map(|message| serialize_delegate_freebusy_row(message, &columns))
                        .collect::<Vec<_>>()
                } else if !snapshot
                    .associated_config_messages_for_folder(*folder_id)
                    .is_empty()
                {
                    let mut rows = snapshot.associated_config_messages_for_folder(*folder_id);
                    sort_associated_config_messages(&mut rows, sort_orders);
                    rows.iter()
                        .map(|message| {
                            serialize_associated_config_row_with_mailbox_guid(
                                message,
                                mailbox_guid,
                                &columns,
                            )
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
                snapshot
                    .public_folder_items_for_folder(*folder_id)
                    .into_iter()
                    .map(|item| serialize_public_folder_item_row(item, &columns))
                    .collect::<Vec<_>>()
            } else if *folder_id == CALENDAR_FOLDER_ID {
                let mut rows = snapshot.events_for_folder(*folder_id);
                rows.retain(|event| restriction_matches_event(restriction.as_ref(), &event.event));
                sort_events(&mut rows, sort_orders);
                rows.into_iter()
                    .map(|event| {
                        serialize_event_row_with_attachments(
                            &event.event,
                            event.id,
                            event.folder_id,
                            !event.attachments.is_empty(),
                            &columns,
                        )
                    })
                    .collect::<Vec<_>>()
            } else if let Some(folder) = snapshot.collaboration_folder_for_id(*folder_id) {
                match folder.kind {
                    MapiCollaborationFolderKind::Contacts => {
                        let mut rows = snapshot.contacts_for_folder(*folder_id);
                        rows.retain(|contact| {
                            restriction_matches_contact(restriction.as_ref(), &contact.contact)
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
                        let mut rows = snapshot.events_for_folder(*folder_id);
                        rows.retain(|event| {
                            restriction_matches_event(restriction.as_ref(), &event.event)
                        });
                        sort_events(&mut rows, sort_orders);
                        rows.into_iter()
                            .map(|event| {
                                serialize_event_row_with_attachments(
                                    &event.event,
                                    event.id,
                                    event.folder_id,
                                    !event.attachments.is_empty(),
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
            } else if *folder_id == CONTACTS_SEARCH_FOLDER_ID {
                let mut rows = snapshot.contacts_search_results();
                rows.retain(|contact| {
                    restriction_matches_contact(restriction.as_ref(), &contact.contact)
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
                rows.retain(|message| {
                    restriction_matches_email(restriction.as_ref(), &message.email)
                });
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
                if let Some((_, window_emails)) = snapshot.content_table_window_emails(
                    *folder_id,
                    table_view_signature(sort_orders, restriction.as_ref()),
                    window_offset,
                    request.query_row_count().unwrap_or(0),
                ) {
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
                    rows.retain(|email| restriction_matches_email(restriction.as_ref(), email));
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
            rows.retain(|attachment| {
                restriction_matches_attachment(restriction.as_ref(), attachment)
            });
            sort_attachments(&mut rows, sort_orders);
            rows.into_iter()
                .map(|attachment| serialize_attachment_row(attachment, &columns))
                .collect::<Vec<_>>()
        }
        Some(MapiObject::PermissionTable {
            folder_id,
            columns,
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
            if *associated && *folder_id == COMMON_VIEWS_FOLDER_ID {
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
            ..
        }) if matches!(
            *folder_id,
            ROOT_FOLDER_ID | IPM_SUBTREE_FOLDER_ID | SYNC_ISSUES_FOLDER_ID
        ) =>
        {
            let rows = hierarchy_rows(
                *folder_id,
                mailboxes,
                snapshot,
                restriction.as_ref(),
                sort_orders,
                mailbox_guid,
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
                        debug_expected_container_class(object_id),
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
            sort_orders,
            restriction,
            position,
            ..
        }) if *associated && *folder_id == COMMON_VIEWS_FOLDER_ID => {
            let mut rows = snapshot.common_views_table_messages().collect::<Vec<_>>();
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
            position,
            ..
        }) if *associated && *folder_id == INBOX_FOLDER_ID => {
            let mut rows = snapshot.associated_config_messages_for_folder(*folder_id);
            sort_associated_config_messages(&mut rows, sort_orders);
            selected_row_indexes(rows.len(), *position, forward_read, requested_row_count)
                .into_iter()
                .map(|index| {
                    let message = &rows[index];
                    classify_outlook_bootstrap_row_invariants(
                        index,
                        "inbox_associated",
                        message.id,
                        None,
                        None,
                        None,
                        |tag| associated_config_property_value(message, tag),
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
            rows.retain(|email| restriction_matches_email(restriction.as_ref(), email));
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

fn debug_folder_row_property_value<F>(
    value: F,
    folder_id: u64,
    parent_folder_id: u64,
    property_tag: u32,
    mailbox_guid: Uuid,
    associated_count: u32,
) -> Option<MapiValue>
where
    F: FnOnce() -> Option<MapiValue>,
{
    value().or_else(|| match canonical_property_storage_tag(property_tag) {
        PID_TAG_PARENT_ENTRY_ID => {
            crate::mapi::identity::folder_entry_id_from_object_id(mailbox_guid, parent_folder_id)
                .map(MapiValue::Binary)
        }
        PID_TAG_ASSOCIATED_CONTENT_COUNT => Some(MapiValue::U32(associated_count)),
        PID_TAG_RECORD_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
            folder_id,
        ))),
        _ => None,
    })
}

fn classify_outlook_bootstrap_row_invariants<F>(
    row_index: usize,
    row_kind: &str,
    object_id: u64,
    expected_folder_id: Option<u64>,
    expected_parent_id: Option<u64>,
    expected_container_class: Option<&str>,
    mut value: F,
) -> String
where
    F: FnMut(u32) -> Option<MapiValue>,
{
    let entry_id = binary_property(value(PID_TAG_ENTRY_ID));
    let record_key = binary_property(value(PID_TAG_RECORD_KEY));
    let source_key = binary_property(value(PID_TAG_SOURCE_KEY));
    let parent_source_key = binary_property(value(PID_TAG_PARENT_SOURCE_KEY));
    let parent_entry_id = binary_property(value(PID_TAG_PARENT_ENTRY_ID));
    let folder_id = u64_property(value(PID_TAG_FOLDER_ID));
    let instance_key = binary_property(value(PID_TAG_INSTANCE_KEY));
    let display_name = string_property(value(PID_TAG_DISPLAY_NAME_W));
    let container_class = string_property(value(PID_TAG_CONTAINER_CLASS_W));
    let folder_type = u32_property(value(PID_TAG_FOLDER_TYPE));
    let content_count = count_property(value(PID_TAG_CONTENT_COUNT));
    let associated_content_count = count_property(value(PID_TAG_ASSOCIATED_CONTENT_COUNT));

    let entry_id_decoded = entry_id
        .as_deref()
        .and_then(crate::mapi::identity::object_id_from_folder_entry_id);
    let source_key_decoded = source_key
        .as_deref()
        .and_then(crate::mapi::identity::object_id_from_source_key);
    let parent_source_key_decoded = parent_source_key
        .as_deref()
        .and_then(crate::mapi::identity::object_id_from_source_key);
    let parent_entry_id_decoded = parent_entry_id
        .as_deref()
        .and_then(crate::mapi::identity::object_id_from_folder_entry_id);

    let folder_id_consistent = expected_folder_id
        .is_none_or(|expected| folder_id == Some(expected) && entry_id_decoded == Some(expected));
    let parent_id_consistent = expected_parent_id.is_none_or(|expected| {
        parent_source_key_decoded == Some(expected) && parent_entry_id_decoded == Some(expected)
    });
    let source_key_stable_non_empty = source_key
        .as_ref()
        .is_some_and(|bytes| !bytes.is_empty() && source_key_decoded == Some(object_id));
    let record_key_stable_non_empty = record_key
        .as_ref()
        .is_some_and(|bytes| !bytes.is_empty() && bytes == source_key.as_ref().unwrap_or(bytes));
    let instance_key_stable_non_empty =
        instance_key.as_ref().is_some_and(|bytes| !bytes.is_empty());
    let folder_property_row = !row_kind.ends_with("_associated") && row_kind != "inbox_contents";
    let folder_type_valid = !folder_property_row
        || folder_type
            .is_some_and(|value| matches!(value, FOLDER_ROOT | FOLDER_GENERIC | FOLDER_SEARCH));
    let content_count_present_non_negative = !folder_property_row || content_count.is_some();
    let associated_count_present_non_negative =
        !folder_property_row || associated_content_count.is_some();
    let container_class_status = match (expected_container_class, container_class.as_deref()) {
        (Some(expected), Some(actual)) if actual == expected => "match",
        (Some(_), Some(_)) => "mismatch",
        (Some(_), None) => "missing",
        (None, _) => "not_applicable",
    };

    let issues = [
        (!folder_id_consistent).then_some("folder_id"),
        (!parent_id_consistent).then_some("parent_id"),
        (!source_key_stable_non_empty).then_some("source_key"),
        (!record_key_stable_non_empty).then_some("record_key"),
        (!instance_key_stable_non_empty).then_some("instance_key"),
        (!folder_type_valid).then_some("folder_type"),
        (!content_count_present_non_negative).then_some("content_count"),
        (!associated_count_present_non_negative).then_some("associated_content_count"),
        (container_class_status == "mismatch" || container_class_status == "missing")
            .then_some("container_class"),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join("|");

    format!(
        "row={row_index} kind={row_kind} object_id={} display_name={} entry_id={} record_key={} source_key={} parent_source_key={} parent_entry_id={} folder_id={} instance_key={} container_class={} expected_container_class={} folder_type={} content_count={} associated_content_count={} folder_id_consistent={} parent_id_consistent={} source_key_stable_non_empty={} record_key_stable_non_empty={} instance_key_stable_non_empty={} folder_type_valid={} counts_present_non_negative={} container_class_status={} issues={}",
        format_debug_u64(Some(object_id)),
        display_name.unwrap_or_default(),
        format_debug_binary(entry_id.as_deref()),
        format_debug_binary(record_key.as_deref()),
        format_debug_binary(source_key.as_deref()),
        format_debug_binary(parent_source_key.as_deref()),
        format_debug_binary(parent_entry_id.as_deref()),
        format_debug_u64(folder_id),
        format_debug_binary(instance_key.as_deref()),
        container_class.unwrap_or_default(),
        expected_container_class.unwrap_or(""),
        format_debug_u32(folder_type),
        format_debug_u32(content_count),
        format_debug_u32(associated_content_count),
        folder_id_consistent,
        parent_id_consistent,
        source_key_stable_non_empty,
        record_key_stable_non_empty,
        instance_key_stable_non_empty,
        folder_type_valid,
        content_count_present_non_negative && associated_count_present_non_negative,
        container_class_status,
        if issues.is_empty() { "none" } else { &issues },
    )
}

fn binary_property(value: Option<MapiValue>) -> Option<Vec<u8>> {
    match value {
        Some(MapiValue::Binary(bytes)) => Some(bytes),
        _ => None,
    }
}

fn string_property(value: Option<MapiValue>) -> Option<String> {
    match value {
        Some(MapiValue::String(value)) => Some(value),
        _ => None,
    }
}

fn u64_property(value: Option<MapiValue>) -> Option<u64> {
    match value {
        Some(MapiValue::U64(value)) => Some(value),
        _ => None,
    }
}

fn u32_property(value: Option<MapiValue>) -> Option<u32> {
    match value {
        Some(MapiValue::U32(value)) => Some(value),
        Some(MapiValue::I32(value)) => u32::try_from(value).ok(),
        _ => None,
    }
}

fn count_property(value: Option<MapiValue>) -> Option<u32> {
    u32_property(value)
}

fn debug_expected_container_class(folder_id: u64) -> Option<&'static str> {
    match folder_id {
        ROOT_FOLDER_ID
        | IPM_SUBTREE_FOLDER_ID
        | COMMON_VIEWS_FOLDER_ID
        | VIEWS_FOLDER_ID
        | SCHEDULE_FOLDER_ID
        | SEARCH_FOLDER_ID
        | FREEBUSY_DATA_FOLDER_ID
        | SPOOLER_QUEUE_FOLDER_ID => None,
        CALENDAR_FOLDER_ID => Some("IPF.Appointment"),
        CONTACTS_FOLDER_ID | SUGGESTED_CONTACTS_FOLDER_ID | CONTACTS_SEARCH_FOLDER_ID => {
            Some("IPF.Contact")
        }
        QUICK_CONTACTS_FOLDER_ID => Some("IPF.Contact.MOC.QuickContacts"),
        IM_CONTACT_LIST_FOLDER_ID => Some("IPF.Contact.MOC.ImContactList"),
        TASKS_FOLDER_ID | TODO_SEARCH_FOLDER_ID => Some("IPF.Task"),
        NOTES_FOLDER_ID => Some("IPF.StickyNote"),
        JOURNAL_FOLDER_ID => Some("IPF.Journal"),
        SHORTCUTS_FOLDER_ID | DOCUMENT_LIBRARIES_FOLDER_ID => Some("IPF.ShortcutFolder"),
        RSS_FEEDS_FOLDER_ID => Some("IPF.Note.OutlookHomepage"),
        CONVERSATION_ACTION_SETTINGS_FOLDER_ID | QUICK_STEP_SETTINGS_FOLDER_ID => {
            Some("IPF.Configuration")
        }
        REMINDERS_FOLDER_ID => Some("Outlook.Reminder"),
        _ => Some("IPF.Note"),
    }
}

fn format_debug_binary(value: Option<&[u8]>) -> String {
    match value {
        Some(bytes) => format!(
            "present:{}:{}",
            bytes.len(),
            bytes
                .iter()
                .take(12)
                .map(|byte| format!("{byte:02x}"))
                .collect::<Vec<_>>()
                .join("")
        ),
        None => "missing".to_string(),
    }
}

fn format_debug_u64(value: Option<u64>) -> String {
    value
        .map(|value| format!("0x{value:016x}"))
        .unwrap_or_else(|| "missing".to_string())
}

fn format_debug_u32(value: Option<u32>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "missing".to_string())
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

pub(in crate::mapi) fn rop_expand_row_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    _snapshot: &MapiMailStoreSnapshot,
) -> Vec<u8> {
    let Some(category_id) = request.category_id() else {
        return rop_error_response(0x59, request.response_handle_index(), 0x8004_0102);
    };
    let Some(MapiObject::ContentsTable {
        folder_id,
        associated,
        columns,
        sort_orders,
        category_count,
        expanded_count,
        collapsed_categories,
        restriction,
        ..
    }) = object
    else {
        return rop_error_response(0x59, request.response_handle_index(), 0x8004_0102);
    };
    if *associated || *category_count == 0 || sort_orders.is_empty() {
        return rop_error_response(0x59, request.response_handle_index(), 0x8004_0102);
    }

    let columns = if columns.is_empty() {
        default_contents_columns()
    } else {
        columns.clone()
    };
    let mut source_rows = emails_for_folder(*folder_id, mailboxes, emails);
    source_rows.retain(|email| restriction_matches_email(restriction.as_ref(), email));
    sort_emails(&mut source_rows, sort_orders);
    let rows = categorized_email_rows(
        *folder_id,
        source_rows,
        &columns,
        sort_orders,
        1,
        &HashSet::new(),
    );
    let leaf_rows = rows
        .into_iter()
        .filter(|row| row.leaf && row.category_id == category_id)
        .map(|row| row.row)
        .collect::<Vec<_>>();
    if leaf_rows.is_empty() {
        return rop_error_response(0x59, request.response_handle_index(), 0x8004_010F);
    }

    collapsed_categories.remove(&category_id);
    *expanded_count = (*expanded_count).max(1);
    let max_rows = request.expand_max_row_count();
    let selected = if max_rows == 0 {
        Vec::new()
    } else {
        leaf_rows.iter().take(max_rows).cloned().collect()
    };
    rop_expand_row_success_response(request, leaf_rows.len(), selected)
}

pub(in crate::mapi) fn rop_collapse_row_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    _snapshot: &MapiMailStoreSnapshot,
) -> Vec<u8> {
    let Some(category_id) = request.category_id() else {
        return rop_error_response(0x5A, request.response_handle_index(), 0x8004_0102);
    };
    let Some(MapiObject::ContentsTable {
        folder_id,
        associated,
        columns,
        sort_orders,
        category_count,
        collapsed_categories,
        restriction,
        ..
    }) = object
    else {
        return rop_error_response(0x5A, request.response_handle_index(), 0x8004_0102);
    };
    if *associated || *category_count == 0 || sort_orders.is_empty() {
        return rop_error_response(0x5A, request.response_handle_index(), 0x8004_0102);
    }

    let columns = if columns.is_empty() {
        default_contents_columns()
    } else {
        columns.clone()
    };
    let mut source_rows = emails_for_folder(*folder_id, mailboxes, emails);
    source_rows.retain(|email| restriction_matches_email(restriction.as_ref(), email));
    sort_emails(&mut source_rows, sort_orders);
    let rows = categorized_email_rows(
        *folder_id,
        source_rows,
        &columns,
        sort_orders,
        1,
        &HashSet::new(),
    );
    let collapsed_count = rows
        .iter()
        .find(|row| !row.leaf && row.category_id == category_id)
        .map(|row| row.leaf_count)
        .unwrap_or(0);
    if collapsed_count == 0 {
        return rop_error_response(0x5A, request.response_handle_index(), 0x8004_010F);
    }
    collapsed_categories.insert(category_id);
    rop_collapse_row_success_response(request, collapsed_count)
}

pub(in crate::mapi) fn rop_get_collapse_state_response(
    request: &RopRequest,
    object: Option<&MapiObject>,
) -> Vec<u8> {
    let Some(MapiObject::ContentsTable {
        folder_id,
        category_count,
        expanded_count,
        collapsed_categories,
        position,
        ..
    }) = object
    else {
        return rop_error_response(0x6B, request.response_handle_index(), 0x8004_0102);
    };
    if *category_count == 0 {
        return rop_error_response(0x6B, request.response_handle_index(), 0x8004_0102);
    }
    let mut state = Vec::new();
    state.extend_from_slice(COLLAPSE_STATE_MAGIC);
    write_u64(&mut state, *folder_id);
    write_u64(
        &mut state,
        request.collapse_state_row_id().unwrap_or_default(),
    );
    write_u32(&mut state, request.collapse_state_row_instance_number());
    write_u32(&mut state, (*position).min(u32::MAX as usize) as u32);
    write_u16(&mut state, *category_count);
    write_u16(&mut state, *expanded_count);
    write_u16(
        &mut state,
        collapsed_categories.len().min(u16::MAX as usize) as u16,
    );
    for category_id in collapsed_categories.iter().take(u16::MAX as usize) {
        write_u64(&mut state, *category_id);
    }
    rop_get_collapse_state_success_response(request, &state)
}

pub(in crate::mapi) fn rop_set_collapse_state_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
) -> Vec<u8> {
    let Some(object) = object else {
        return rop_error_response(0x6C, request.response_handle_index(), 0x8004_0102);
    };
    let state = request.collapse_state();
    if state.len() < 30 || state.get(..6) != Some(COLLAPSE_STATE_MAGIC.as_slice()) {
        return rop_error_response(0x6C, request.response_handle_index(), 0x8004_0102);
    }
    let mut offset = 6;
    let folder_id = read_u64_from(state, &mut offset).unwrap_or_default();
    let _row_id = read_u64_from(state, &mut offset).unwrap_or_default();
    let _row_instance = read_u32_from(state, &mut offset).unwrap_or_default();
    let position = read_u32_from(state, &mut offset).unwrap_or_default() as usize;
    let category_count = read_u16_from(state, &mut offset).unwrap_or_default();
    let expanded_count = read_u16_from(state, &mut offset).unwrap_or_default();
    let collapsed_count = read_u16_from(state, &mut offset).unwrap_or_default() as usize;
    let mut collapsed = HashSet::new();
    for _ in 0..collapsed_count.min(256) {
        if let Some(category_id) = read_u64_from(state, &mut offset) {
            collapsed.insert(category_id);
        }
    }

    let MapiObject::ContentsTable {
        folder_id: table_folder_id,
        category_count: table_category_count,
        expanded_count: table_expanded_count,
        collapsed_categories,
        position: table_position,
        bookmarks,
        next_bookmark,
        ..
    } = object
    else {
        return rop_error_response(0x6C, request.response_handle_index(), 0x8004_0102);
    };
    if *table_folder_id != folder_id || category_count == 0 {
        return rop_error_response(0x6C, request.response_handle_index(), 0x8004_0102);
    }
    *table_category_count = category_count;
    *table_expanded_count = expanded_count;
    *collapsed_categories = collapsed;
    *table_position = position;

    let bookmark_id = *next_bookmark;
    *next_bookmark = next_bookmark.saturating_add(1);
    let bookmark = bookmark_id.to_le_bytes().to_vec();
    bookmarks.insert(
        bookmark.clone(),
        TableBookmark {
            position,
            row_key: None,
        },
    );
    rop_set_collapse_state_success_response(request, &bookmark)
}

fn read_u16_from(bytes: &[u8], offset: &mut usize) -> Option<u16> {
    let value = u16::from_le_bytes(bytes.get(*offset..*offset + 2)?.try_into().ok()?);
    *offset += 2;
    Some(value)
}

fn read_u32_from(bytes: &[u8], offset: &mut usize) -> Option<u32> {
    let value = u32::from_le_bytes(bytes.get(*offset..*offset + 4)?.try_into().ok()?);
    *offset += 4;
    Some(value)
}

fn read_u64_from(bytes: &[u8], offset: &mut usize) -> Option<u64> {
    let value = u64::from_le_bytes(bytes.get(*offset..*offset + 8)?.try_into().ok()?);
    *offset += 8;
    Some(value)
}

pub(in crate::mapi) fn sort_emails(rows: &mut [&JmapEmail], sort_orders: &[MapiSortOrder]) {
    if sort_orders.is_empty() {
        return;
    }
    rows.sort_by(|left, right| {
        for sort_order in sort_orders {
            let ordering = match sort_order.property_tag {
                PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
                    compare_case_insensitive(&left.subject, &right.subject)
                }
                PID_TAG_SENDER_NAME_W => compare_case_insensitive(
                    left.from_display.as_deref().unwrap_or(&left.from_address),
                    right.from_display.as_deref().unwrap_or(&right.from_address),
                ),
                PID_TAG_SENDER_EMAIL_ADDRESS_W => {
                    compare_case_insensitive(&left.from_address, &right.from_address)
                }
                PID_TAG_DISPLAY_TO_W => {
                    compare_case_insensitive(&display_to(left), &display_to(right))
                }
                PID_TAG_MESSAGE_DELIVERY_TIME | PID_TAG_LAST_MODIFICATION_TIME => {
                    left.received_at.cmp(&right.received_at)
                }
                PID_TAG_MESSAGE_FLAGS => message_flags(left).cmp(&message_flags(right)),
                PID_TAG_MESSAGE_SIZE => left.size_octets.cmp(&right.size_octets),
                PID_TAG_HAS_ATTACHMENTS => left.has_attachments.cmp(&right.has_attachments),
                PID_TAG_MID => mapi_message_id(left).cmp(&mapi_message_id(right)),
                _ => Ordering::Equal,
            };
            let ordering = apply_sort_direction(ordering, sort_order.order);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        Ordering::Equal
    });
}

pub(in crate::mapi) fn sort_mapi_messages(
    rows: &mut [&crate::mapi_store::MapiMessage],
    sort_orders: &[MapiSortOrder],
) {
    if sort_orders.is_empty() {
        return;
    }
    rows.sort_by(|left, right| {
        for sort_order in sort_orders {
            let ordering = match sort_order.property_tag {
                PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
                    compare_case_insensitive(&left.email.subject, &right.email.subject)
                }
                PID_TAG_SENDER_NAME_W => compare_case_insensitive(
                    left.email
                        .from_display
                        .as_deref()
                        .unwrap_or(&left.email.from_address),
                    right
                        .email
                        .from_display
                        .as_deref()
                        .unwrap_or(&right.email.from_address),
                ),
                PID_TAG_SENDER_EMAIL_ADDRESS_W => {
                    compare_case_insensitive(&left.email.from_address, &right.email.from_address)
                }
                PID_TAG_DISPLAY_TO_W => {
                    compare_case_insensitive(&display_to(&left.email), &display_to(&right.email))
                }
                PID_TAG_MESSAGE_DELIVERY_TIME | PID_TAG_LAST_MODIFICATION_TIME => {
                    left.email.received_at.cmp(&right.email.received_at)
                }
                PID_TAG_MESSAGE_FLAGS => {
                    message_flags(&left.email).cmp(&message_flags(&right.email))
                }
                PID_TAG_MESSAGE_SIZE => left.email.size_octets.cmp(&right.email.size_octets),
                PID_TAG_HAS_ATTACHMENTS => {
                    left.email.has_attachments.cmp(&right.email.has_attachments)
                }
                PID_TAG_MID => left.id.cmp(&right.id),
                _ => Ordering::Equal,
            };
            let ordering = apply_sort_direction(ordering, sort_order.order);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        Ordering::Equal
    });
}

fn sort_associated_config_messages(
    rows: &mut [MapiAssociatedConfigMessage],
    sort_orders: &[MapiSortOrder],
) {
    if sort_orders.is_empty() {
        return;
    }
    rows.sort_by(|left, right| {
        for sort_order in sort_orders {
            let ordering = match sort_order.property_tag {
                PID_TAG_MESSAGE_CLASS_W => {
                    compare_case_insensitive(&left.message_class, &right.message_class)
                }
                PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
                    compare_case_insensitive(&left.subject, &right.subject)
                }
                PID_TAG_LAST_MODIFICATION_TIME | PID_TAG_MID => left.id.cmp(&right.id),
                _ => Ordering::Equal,
            };
            let ordering = apply_sort_direction(ordering, sort_order.order);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        left.id.cmp(&right.id)
    });
}

pub(in crate::mapi) fn sort_common_views_messages(
    rows: &mut [MapiCommonViewsMessage],
    sort_orders: &[MapiSortOrder],
) {
    if sort_orders.is_empty() {
        return;
    }
    rows.sort_by(|left, right| {
        for sort_order in sort_orders {
            let ordering = match (left, right) {
                (
                    MapiCommonViewsMessage::NavigationShortcut(left),
                    MapiCommonViewsMessage::NavigationShortcut(right),
                ) => compare_navigation_shortcuts(left, right, sort_order.property_tag),
            };
            let ordering = apply_sort_direction(ordering, sort_order.order);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        common_views_message_id(left).cmp(&common_views_message_id(right))
    });
}

fn compare_navigation_shortcuts(
    left: &MapiNavigationShortcutMessage,
    right: &MapiNavigationShortcutMessage,
    property_tag: u32,
) -> Ordering {
    match property_tag & 0xFFFF_0000 {
        0x684F_0000 => Ordering::Equal,
        0x6850_0000 => wlink_group_guid_bytes(left).cmp(&wlink_group_guid_bytes(right)),
        0x684B_0000 => {
            wlink_ordinal_debug_bytes(left.ordinal).cmp(&wlink_ordinal_debug_bytes(right.ordinal))
        }
        0x6849_0000 => left.shortcut_type.cmp(&right.shortcut_type),
        0x6852_0000 => left.section.cmp(&right.section),
        0x0037_0000 | 0x3001_0000 => compare_case_insensitive(&left.subject, &right.subject),
        0x674A_0000 => left.id.cmp(&right.id),
        _ => Ordering::Equal,
    }
}

fn common_views_message_id(message: &MapiCommonViewsMessage) -> u64 {
    match message {
        MapiCommonViewsMessage::NavigationShortcut(message) => message.id,
    }
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
    }
}

fn wlink_group_guid_bytes(message: &MapiNavigationShortcutMessage) -> [u8; 16] {
    message
        .group_header_id
        .map(|group_id| *group_id.as_bytes())
        .unwrap_or_else(default_wlink_group_guid)
}

fn wlink_ordinal_debug_bytes(value: u32) -> Vec<u8> {
    if value <= u8::MAX as u32 {
        vec![value as u8]
    } else {
        value
            .to_be_bytes()
            .into_iter()
            .skip_while(|byte| *byte == 0)
            .collect()
    }
}

pub(in crate::mapi) fn sort_recoverable_items(
    rows: &mut [&crate::mapi_store::MapiRecoverableItemMessage],
    sort_orders: &[MapiSortOrder],
) {
    if sort_orders.is_empty() {
        return;
    }
    rows.sort_by(|left, right| {
        for sort_order in sort_orders {
            let ordering = match sort_order.property_tag {
                PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
                    compare_case_insensitive(&left.item.subject, &right.item.subject)
                }
                PID_TAG_SENDER_NAME_W | PID_TAG_SENDER_EMAIL_ADDRESS_W => {
                    compare_case_insensitive(&left.item.sender_address, &right.item.sender_address)
                }
                PID_TAG_MESSAGE_DELIVERY_TIME | PID_TAG_LAST_MODIFICATION_TIME => {
                    left.item.received_at.cmp(&right.item.received_at)
                }
                PID_TAG_MESSAGE_SIZE => left.item.size_octets.cmp(&right.item.size_octets),
                PID_TAG_HAS_ATTACHMENTS => {
                    left.item.has_attachments.cmp(&right.item.has_attachments)
                }
                PID_TAG_MID => left.id.cmp(&right.id),
                _ => Ordering::Equal,
            };
            let ordering = apply_sort_direction(ordering, sort_order.order);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        Ordering::Equal
    });
}

pub(in crate::mapi) fn sort_attachments(
    rows: &mut [&MapiAttachment],
    sort_orders: &[MapiSortOrder],
) {
    if sort_orders.is_empty() {
        return;
    }
    rows.sort_by(|left, right| {
        for sort_order in sort_orders {
            let ordering = match sort_order.property_tag {
                PID_TAG_ATTACH_NUM => left.attach_num.cmp(&right.attach_num),
                PID_TAG_ATTACH_FILENAME_W | PID_TAG_ATTACH_LONG_FILENAME_W => {
                    compare_case_insensitive(&left.file_name, &right.file_name)
                }
                PID_TAG_ATTACH_MIME_TAG_W => {
                    compare_case_insensitive(&left.media_type, &right.media_type)
                }
                PID_TAG_ATTACH_SIZE => left.size_octets.cmp(&right.size_octets),
                _ => Ordering::Equal,
            };
            let ordering = apply_sort_direction(ordering, sort_order.order);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        Ordering::Equal
    });
}

pub(in crate::mapi) fn sort_contacts(
    rows: &mut [&crate::mapi_store::MapiContact],
    sort_orders: &[MapiSortOrder],
) {
    if sort_orders.is_empty() {
        return;
    }
    rows.sort_by(|left, right| {
        for sort_order in sort_orders {
            let ordering = match sort_order.property_tag {
                PID_TAG_DISPLAY_NAME_W | PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
                    compare_case_insensitive(&left.contact.name, &right.contact.name)
                }
                PID_TAG_EMAIL_ADDRESS_W | PID_TAG_SMTP_ADDRESS_W => {
                    compare_case_insensitive(&left.contact.email, &right.contact.email)
                }
                PID_TAG_COMPANY_NAME_W => {
                    compare_case_insensitive(&left.contact.team, &right.contact.team)
                }
                PID_TAG_MID => left.id.cmp(&right.id),
                _ => Ordering::Equal,
            };
            let ordering = apply_sort_direction(ordering, sort_order.order);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        Ordering::Equal
    });
}

pub(in crate::mapi) fn sort_events(
    rows: &mut [&crate::mapi_store::MapiEvent],
    sort_orders: &[MapiSortOrder],
) {
    if sort_orders.is_empty() {
        return;
    }
    rows.sort_by(|left, right| {
        for sort_order in sort_orders {
            let ordering = match sort_order.property_tag {
                PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
                    compare_case_insensitive(&left.event.title, &right.event.title)
                }
                PID_TAG_START_DATE
                | PID_TAG_MESSAGE_DELIVERY_TIME
                | PID_TAG_LAST_MODIFICATION_TIME => {
                    event_start_sort_key(&left.event).cmp(&event_start_sort_key(&right.event))
                }
                PID_TAG_LOCATION_W => {
                    compare_case_insensitive(&left.event.location, &right.event.location)
                }
                PID_TAG_MID => left.id.cmp(&right.id),
                _ => Ordering::Equal,
            };
            let ordering = apply_sort_direction(ordering, sort_order.order);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        Ordering::Equal
    });
}

pub(in crate::mapi) fn sort_tasks(
    rows: &mut [&crate::mapi_store::MapiTask],
    sort_orders: &[MapiSortOrder],
) {
    if sort_orders.is_empty() {
        return;
    }
    rows.sort_by(|left, right| {
        for sort_order in sort_orders {
            let ordering = match sort_order.property_tag {
                PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W | PID_TAG_DISPLAY_NAME_W => {
                    compare_case_insensitive(&left.task.title, &right.task.title)
                }
                PID_TAG_LAST_MODIFICATION_TIME | PID_TAG_LOCAL_COMMIT_TIME => {
                    left.task.updated_at.cmp(&right.task.updated_at)
                }
                PID_TAG_MID => left.id.cmp(&right.id),
                _ => Ordering::Equal,
            };
            let ordering = apply_sort_direction(ordering, sort_order.order);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        Ordering::Equal
    });
}

pub(in crate::mapi) fn sort_notes(
    rows: &mut [&crate::mapi_store::MapiNote],
    sort_orders: &[MapiSortOrder],
) {
    if sort_orders.is_empty() {
        return;
    }
    rows.sort_by(|left, right| {
        for sort_order in sort_orders {
            let ordering = match sort_order.property_tag {
                PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W | PID_TAG_DISPLAY_NAME_W => {
                    compare_case_insensitive(&left.note.title, &right.note.title)
                }
                PID_TAG_LAST_MODIFICATION_TIME | PID_TAG_LOCAL_COMMIT_TIME => {
                    left.note.updated_at.cmp(&right.note.updated_at)
                }
                PID_TAG_MID => left.id.cmp(&right.id),
                _ => Ordering::Equal,
            };
            let ordering = apply_sort_direction(ordering, sort_order.order);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        Ordering::Equal
    });
}

pub(in crate::mapi) fn sort_journal_entries(
    rows: &mut [&crate::mapi_store::MapiJournalEntry],
    sort_orders: &[MapiSortOrder],
) {
    if sort_orders.is_empty() {
        return;
    }
    rows.sort_by(|left, right| {
        for sort_order in sort_orders {
            let ordering = match sort_order.property_tag {
                PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W | PID_TAG_DISPLAY_NAME_W => {
                    compare_case_insensitive(&left.entry.subject, &right.entry.subject)
                }
                PID_TAG_START_DATE | PID_TAG_MESSAGE_DELIVERY_TIME => {
                    journal_entry_start_sort_key(&left.entry)
                        .cmp(&journal_entry_start_sort_key(&right.entry))
                }
                PID_TAG_LAST_MODIFICATION_TIME | PID_TAG_LOCAL_COMMIT_TIME => {
                    left.entry.updated_at.cmp(&right.entry.updated_at)
                }
                PID_TAG_MID => left.id.cmp(&right.id),
                _ => Ordering::Equal,
            };
            let ordering = apply_sort_direction(ordering, sort_order.order);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        Ordering::Equal
    });
}

fn todo_search_content_rows<'a>(
    snapshot: &'a MapiMailStoreSnapshot,
    restriction: Option<&MapiRestriction>,
) -> Vec<SearchContentRow<'a>> {
    let mut rows = snapshot
        .todo_search_messages()
        .into_iter()
        .filter(|message| restriction_matches_email(restriction, &message.email))
        .map(SearchContentRow::Message)
        .collect::<Vec<_>>();
    rows.extend(
        snapshot
            .todo_search_results()
            .into_iter()
            .filter(|task| restriction_matches_task(restriction, &task.task))
            .map(SearchContentRow::Task),
    );
    rows
}

fn reminder_search_content_rows<'a>(
    snapshot: &'a MapiMailStoreSnapshot,
    restriction: Option<&MapiRestriction>,
) -> Vec<SearchContentRow<'a>> {
    let mut rows = snapshot
        .reminder_events()
        .into_iter()
        .filter(|event| restriction_matches_event(restriction, &event.event))
        .map(SearchContentRow::Event)
        .collect::<Vec<_>>();
    rows.extend(
        snapshot
            .reminder_tasks()
            .into_iter()
            .filter(|task| restriction_matches_task(restriction, &task.task))
            .map(SearchContentRow::Task),
    );
    rows.extend(
        snapshot
            .reminder_messages()
            .into_iter()
            .filter(|message| restriction_matches_email(restriction, &message.email))
            .map(SearchContentRow::Message),
    );
    rows
}

fn sort_search_content_rows(rows: &mut [SearchContentRow<'_>], sort_orders: &[MapiSortOrder]) {
    if sort_orders.is_empty() {
        return;
    }
    rows.sort_by(|left, right| {
        for sort_order in sort_orders {
            let ordering = match canonical_property_storage_tag(sort_order.property_tag) {
                PID_TAG_DISPLAY_NAME_W | PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
                    compare_case_insensitive(
                        search_content_row_subject(left),
                        search_content_row_subject(right),
                    )
                }
                PID_TAG_MESSAGE_DELIVERY_TIME
                | PID_TAG_LAST_MODIFICATION_TIME
                | PID_TAG_LOCAL_COMMIT_TIME => {
                    search_content_row_time(left).cmp(&search_content_row_time(right))
                }
                PID_TAG_MESSAGE_CLASS_W | PID_TAG_CONTAINER_CLASS_W => {
                    search_content_row_class(left).cmp(search_content_row_class(right))
                }
                PID_TAG_MID => search_content_row_id(left).cmp(&search_content_row_id(right)),
                _ => Ordering::Equal,
            };
            let ordering = apply_sort_direction(ordering, sort_order.order);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        search_content_row_id(left).cmp(&search_content_row_id(right))
    });
}

fn search_content_row_id(row: &SearchContentRow<'_>) -> u64 {
    match row {
        SearchContentRow::Event(event) => event.id,
        SearchContentRow::Message(message) => message.id,
        SearchContentRow::Task(task) => task.id,
    }
}

fn search_content_row_subject<'a>(row: &'a SearchContentRow<'a>) -> &'a str {
    match row {
        SearchContentRow::Event(event) => &event.event.title,
        SearchContentRow::Message(message) => &message.email.subject,
        SearchContentRow::Task(task) => &task.task.title,
    }
}

fn search_content_row_class(row: &SearchContentRow<'_>) -> &'static str {
    match row {
        SearchContentRow::Event(_) => "IPM.Appointment",
        SearchContentRow::Message(_) => "IPM.Note",
        SearchContentRow::Task(_) => "IPM.Task",
    }
}

fn search_content_row_time(row: &SearchContentRow<'_>) -> String {
    match row {
        SearchContentRow::Event(event) => event_start_sort_key(&event.event),
        SearchContentRow::Message(message) => message.email.received_at.clone(),
        SearchContentRow::Task(task) => task.task.updated_at.clone(),
    }
}

fn serialize_search_content_row(
    row: SearchContentRow<'_>,
    snapshot: &MapiMailStoreSnapshot,
    columns: &[u32],
    reminder_projection: bool,
) -> Vec<u8> {
    match row {
        SearchContentRow::Event(event) if reminder_projection => serialize_reminder_event_row(
            event,
            snapshot.reminder_for_source("calendar", event.canonical_id),
            columns,
        ),
        SearchContentRow::Event(event) => serialize_event_row_with_attachments(
            &event.event,
            event.id,
            event.folder_id,
            !event.attachments.is_empty(),
            columns,
        ),
        SearchContentRow::Message(message) => serialize_message_row(&message.email, columns),
        SearchContentRow::Task(task) if reminder_projection => serialize_reminder_task_row(
            task,
            snapshot.reminder_for_source("task", task.canonical_id),
            columns,
        ),
        SearchContentRow::Task(task) => {
            serialize_task_row(&task.task, task.id, TODO_SEARCH_FOLDER_ID, columns)
        }
    }
}

pub(in crate::mapi) fn apply_sort_direction(ordering: Ordering, sort_order: u8) -> Ordering {
    if sort_order == 0x01 {
        ordering.reverse()
    } else {
        ordering
    }
}

pub(in crate::mapi) fn compare_case_insensitive(left: &str, right: &str) -> Ordering {
    left.to_ascii_lowercase().cmp(&right.to_ascii_lowercase())
}

pub(in crate::mapi) fn table_view_signature(
    sort_orders: &[MapiSortOrder],
    restriction: Option<&MapiRestriction>,
) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

    fn push_bytes(hash: &mut u64, bytes: &[u8]) {
        for byte in bytes {
            *hash = (*hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME);
        }
    }

    fn push_restriction(hash: &mut u64, restriction: &MapiRestriction) {
        match restriction {
            MapiRestriction::And(children) => {
                push_bytes(hash, b"and");
                for child in children {
                    push_restriction(hash, child);
                }
            }
            MapiRestriction::Or(children) => {
                push_bytes(hash, b"or");
                for child in children {
                    push_restriction(hash, child);
                }
            }
            MapiRestriction::Not(child) => {
                push_bytes(hash, b"not");
                push_restriction(hash, child);
            }
            MapiRestriction::Content {
                property_tag,
                value,
            } => {
                push_bytes(hash, b"content");
                push_bytes(hash, &property_tag.to_le_bytes());
                push_bytes(hash, value.as_bytes());
            }
            MapiRestriction::Property {
                relop,
                property_tag,
                value,
            } => {
                push_bytes(hash, b"property");
                push_bytes(hash, &[*relop]);
                push_bytes(hash, &property_tag.to_le_bytes());
                push_bytes(hash, format!("{value:?}").as_bytes());
            }
            MapiRestriction::Bitmask {
                property_tag,
                mask,
                must_be_nonzero,
            } => {
                push_bytes(hash, b"bitmask");
                push_bytes(hash, &property_tag.to_le_bytes());
                push_bytes(hash, &mask.to_le_bytes());
                push_bytes(hash, &[*must_be_nonzero as u8]);
            }
            MapiRestriction::Size {
                relop,
                property_tag,
                size,
            } => {
                push_bytes(hash, b"size");
                push_bytes(hash, &[*relop]);
                push_bytes(hash, &property_tag.to_le_bytes());
                push_bytes(hash, &size.to_le_bytes());
            }
            MapiRestriction::Exist { property_tag } => {
                push_bytes(hash, b"exist");
                push_bytes(hash, &property_tag.to_le_bytes());
            }
        }
    }

    let mut hash = FNV_OFFSET;
    for sort_order in sort_orders {
        push_bytes(&mut hash, &sort_order.property_tag.to_le_bytes());
        push_bytes(&mut hash, &[sort_order.order]);
    }
    if let Some(restriction) = restriction {
        push_restriction(&mut hash, restriction);
    }
    hash
}

pub(in crate::mapi) fn serialize_attachment_row(
    attachment: &MapiAttachment,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match *column {
            PID_TAG_ATTACH_NUM => write_u32(&mut row, attachment.attach_num),
            PID_TAG_ATTACH_FILENAME_W | PID_TAG_ATTACH_LONG_FILENAME_W => {
                write_utf16z(&mut row, &attachment.file_name)
            }
            PID_TAG_ATTACH_MIME_TAG_W => write_utf16z(&mut row, &attachment.media_type),
            PID_TAG_ATTACH_SIZE => {
                write_u32(&mut row, attachment.size_octets.min(u32::MAX as u64) as u32)
            }
            PID_TAG_ATTACH_METHOD => write_u32(&mut row, ATTACH_BY_VALUE),
            PID_TAG_RENDERING_POSITION => write_u32(&mut row, u32::MAX),
            PID_TAG_ENTRY_ID => {
                write_u16_prefixed_bytes(&mut row, attachment.canonical_id.as_bytes())
            }
            PID_TAG_INSTANCE_KEY => {
                write_u16_prefixed_bytes(&mut row, attachment.file_reference.as_bytes())
            }
            _ => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(in crate::mapi) fn serialize_rule_row(rule: &MapiRule, columns: &[u32]) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match *column {
            PID_TAG_RULE_ID => write_u64(&mut row, rule.id),
            PID_TAG_RULE_SEQUENCE => write_u32(&mut row, rule_sequence(rule.id)),
            PID_TAG_RULE_STATE => write_u32(&mut row, if rule.is_active { ST_ENABLED } else { 0 }),
            PID_TAG_RULE_USER_FLAGS | PID_TAG_RULE_LEVEL => write_u32(&mut row, 0),
            PID_TAG_RULE_PROVIDER => write_utf16z(&mut row, "LPE Sieve"),
            PID_TAG_RULE_NAME => write_utf16z(&mut row, &rule.name),
            PID_TAG_RULE_PROVIDER_DATA => {
                let data = serde_json::json!({
                    "sourceKind": "sieve_script",
                    "conditionSummary": rule.condition_summary,
                    "actionSummary": rule.action_summary,
                    "updatedAt": rule.updated_at,
                })
                .to_string();
                write_u16_prefixed_bytes(&mut row, data.as_bytes());
            }
            PID_TAG_RULE_CONDITION | PID_TAG_RULE_ACTIONS => {
                write_property_default(&mut row, *column)
            }
            _ => write_property_default(&mut row, *column),
        }
    }
    row
}

fn rule_sequence(rule_id: u64) -> u32 {
    crate::mapi::identity::global_counter_from_store_id(rule_id)
        .unwrap_or(rule_id)
        .min(u64::from(u32::MAX)) as u32
}

pub(in crate::mapi) fn rop_get_status_response(
    request: &RopRequest,
    object: Option<&MapiObject>,
) -> Vec<u8> {
    if !object.is_some_and(|object| {
        is_table_object(object) || matches!(object, MapiObject::Folder { .. })
    }) {
        return rop_error_response(0x16, request.response_handle_index(), 0x8004_0102);
    }

    let mut response = vec![0x16, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(0);
    response
}

pub(in crate::mapi) fn is_table_object(object: &MapiObject) -> bool {
    matches!(
        object,
        MapiObject::HierarchyTable { .. }
            | MapiObject::ContentsTable { .. }
            | MapiObject::AttachmentTable { .. }
            | MapiObject::PermissionTable { .. }
            | MapiObject::RuleTable { .. }
    )
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
    let Some(object) = object else {
        return rop_error_response(0x18, request.response_handle_index(), 0x8004_0102);
    };
    let total_rows =
        table_position_and_count(Some(object), mailboxes, emails, snapshot, mailbox_guid).1;
    let Some(position) = table_position_mut(object) else {
        return rop_error_response(0x18, request.response_handle_index(), 0x8004_0102);
    };

    let requested_rows = request.seek_row_count().unwrap_or(0);
    let base_position = match request.seek_origin().unwrap_or(1) {
        0 => 0isize,
        2 => total_rows as isize,
        _ => *position as isize,
    };
    let requested_position = base_position.saturating_add(requested_rows as isize);
    let new_position = requested_position.clamp(0, total_rows as isize);
    let rows_sought = (new_position - base_position) as i32;
    *position = new_position as usize;

    let mut response = vec![0x18, request.response_handle_index()];
    write_u32(&mut response, 0);
    let want_row_moved_count = request.want_row_moved_count();
    response.push((want_row_moved_count && rows_sought != requested_rows) as u8);
    response.extend_from_slice(&if want_row_moved_count { rows_sought } else { 0 }.to_le_bytes());
    response
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
        .copied();
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
    let Some(object) = object else {
        return rop_error_response(0x19, request.response_handle_index(), 0x8004_0102);
    };
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
                row_no_longer_visible = true;
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

pub(in crate::mapi) fn rop_seek_row_fractional_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    mailbox_guid: Uuid,
) -> Vec<u8> {
    let Some(object) = object else {
        return rop_error_response(0x1A, request.response_handle_index(), 0x8004_0102);
    };
    let total_rows =
        table_position_and_count(Some(object), mailboxes, emails, snapshot, mailbox_guid).1;
    let Some(position) = table_position_mut(object) else {
        return rop_error_response(0x1A, request.response_handle_index(), 0x8004_0102);
    };
    let Some((numerator, denominator)) = request.fractional_position() else {
        return rop_error_response(0x1A, request.response_handle_index(), 0x8004_0102);
    };
    if denominator == 0 {
        return rop_error_response(0x1A, request.response_handle_index(), 0x8004_0102);
    }
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

pub(in crate::mapi) fn rop_free_bookmark_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
) -> Vec<u8> {
    let Some(object) = object else {
        return rop_error_response(0x89, request.response_handle_index(), 0x8004_0102);
    };
    let Some((_position, bookmarks, _next_bookmark)) = table_bookmark_state_mut(object) else {
        return rop_error_response(0x89, request.response_handle_index(), 0x8004_0102);
    };
    bookmarks.remove(request.bookmark());

    let mut response = vec![0x89, request.response_handle_index()];
    write_u32(&mut response, 0);
    response
}

pub(in crate::mapi) fn rop_find_row_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    mailbox_guid: Uuid,
) -> Vec<u8> {
    let Ok(restriction) = request.restriction() else {
        return rop_error_response(0x4F, request.response_handle_index(), 0x8004_0102);
    };
    let Some(restriction) = restriction else {
        return rop_error_response(0x4F, request.response_handle_index(), 0x8004_0102);
    };

    let Some(object) = object else {
        return rop_error_response(0x4F, request.response_handle_index(), 0x8004_0102);
    };
    let mut response = vec![0x4F, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(0);

    match object {
        MapiObject::HierarchyTable {
            folder_id,
            columns,
            sort_orders,
            restriction: table_restriction,
            position,
            ..
        } if is_queryable_hierarchy_folder(*folder_id) => {
            let columns = if columns.is_empty() {
                default_hierarchy_columns()
            } else {
                columns.clone()
            };
            let rows = hierarchy_rows(
                *folder_id,
                mailboxes,
                snapshot,
                table_restriction.as_ref(),
                sort_orders,
                mailbox_guid,
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
                    &serialize_hierarchy_row(row, mailboxes, &columns, mailbox_guid),
                );
            } else {
                return rop_error_response(0x4F, request.response_handle_index(), 0x8004_010F);
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
                } else {
                    default_contents_columns()
                }
            } else {
                columns.clone()
            };
            if *associated && *folder_id == COMMON_VIEWS_FOLDER_ID {
                let mut rows = snapshot.common_views_table_messages().collect::<Vec<_>>();
                sort_common_views_messages(&mut rows, sort_orders);
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
                    return rop_error_response(0x4F, request.response_handle_index(), 0x8004_010F);
                }
            } else if *associated && *folder_id == CONVERSATION_ACTION_SETTINGS_FOLDER_ID {
                let rows = snapshot
                    .conversation_action_messages()
                    .iter()
                    .collect::<Vec<_>>();
                if let Some((index, message)) =
                    find_row(rows.as_slice(), *position, request, |_message| true)
                {
                    *position = index;
                    response.push(1);
                    write_standard_property_row(
                        &mut response,
                        &serialize_conversation_action_row(message, &columns),
                    );
                } else {
                    return rop_error_response(0x4F, request.response_handle_index(), 0x8004_010F);
                }
            } else if *associated
                && !snapshot
                    .associated_config_messages_for_folder(*folder_id)
                    .is_empty()
            {
                let mut rows = snapshot.associated_config_messages_for_folder(*folder_id);
                sort_associated_config_messages(&mut rows, sort_orders);
                let rows = rows.iter().collect::<Vec<_>>();
                if let Some((index, message)) =
                    find_row(rows.as_slice(), *position, request, |message| {
                        restriction_matches_associated_config(Some(&restriction), message)
                    })
                {
                    *position = index;
                    response.push(1);
                    write_standard_property_row(
                        &mut response,
                        &serialize_associated_config_row_with_mailbox_guid(
                            message,
                            mailbox_guid,
                            &columns,
                        ),
                    );
                } else {
                    return rop_error_response(0x4F, request.response_handle_index(), 0x8004_010F);
                }
            } else {
                let mut rows = emails_for_folder(*folder_id, mailboxes, emails);
                rows.retain(|email| restriction_matches_email(table_restriction.as_ref(), email));
                sort_emails(&mut rows, sort_orders);
                if let Some((index, email)) =
                    find_row(rows.as_slice(), *position, request, |email| {
                        restriction_matches_email(Some(&restriction), email)
                    })
                {
                    *position = index;
                    response.push(1);
                    write_standard_property_row(
                        &mut response,
                        &serialize_message_row(email, &columns),
                    );
                } else {
                    return rop_error_response(0x4F, request.response_handle_index(), 0x8004_010F);
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
                return rop_error_response(0x4F, request.response_handle_index(), 0x8004_010F);
            }
        }
        _ => return rop_error_response(0x4F, request.response_handle_index(), 0x8004_0102),
    }

    response
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
    match object {
        Some(MapiObject::HierarchyTable {
            folder_id,
            position,
            restriction,
            sort_orders,
            ..
        }) if is_queryable_hierarchy_folder(*folder_id) => {
            let total = hierarchy_rows(
                *folder_id,
                mailboxes,
                snapshot,
                restriction.as_ref(),
                sort_orders,
                mailbox_guid,
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
            ..
        }) => {
            let total = if *associated {
                associated_folder_message_count(*folder_id, snapshot) as usize
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
                        restriction_matches_contact(restriction.as_ref(), &contact.contact)
                    })
                    .count()
            } else if *folder_id == TODO_SEARCH_FOLDER_ID {
                snapshot
                    .todo_search_messages()
                    .into_iter()
                    .filter(|message| {
                        restriction_matches_email(restriction.as_ref(), &message.email)
                    })
                    .count()
                    + snapshot
                        .todo_search_results()
                        .into_iter()
                        .filter(|task| restriction_matches_task(restriction.as_ref(), &task.task))
                        .count()
            } else if *folder_id == TRACKED_MAIL_PROCESSING_FOLDER_ID {
                snapshot
                    .tracked_mail_processing_messages()
                    .into_iter()
                    .filter(|message| {
                        restriction_matches_email(restriction.as_ref(), &message.email)
                    })
                    .count()
            } else if *folder_id == REMINDERS_FOLDER_ID {
                snapshot
                    .reminder_events()
                    .into_iter()
                    .filter(|event| restriction_matches_event(restriction.as_ref(), &event.event))
                    .count()
                    + snapshot
                        .reminder_tasks()
                        .into_iter()
                        .filter(|task| restriction_matches_task(restriction.as_ref(), &task.task))
                        .count()
                    + snapshot
                        .reminder_messages()
                        .into_iter()
                        .filter(|message| {
                            restriction_matches_email(restriction.as_ref(), &message.email)
                        })
                        .count()
            } else if *folder_id == JOURNAL_FOLDER_ID {
                snapshot
                    .journal_entries_for_folder(*folder_id)
                    .into_iter()
                    .filter(|entry| {
                        restriction_matches_journal_entry(restriction.as_ref(), &entry.entry)
                    })
                    .count()
            } else if crate::mapi_store::recoverable_storage_folder(*folder_id).is_some() {
                snapshot.recoverable_items_for_folder(*folder_id).len()
            } else {
                snapshot
                    .content_table_total(
                        *folder_id,
                        table_view_signature(sort_orders, restriction.as_ref()),
                    )
                    .unwrap_or_else(|| {
                        emails_for_folder(*folder_id, mailboxes, emails)
                            .into_iter()
                            .filter(|email| restriction_matches_email(restriction.as_ref(), email))
                            .count()
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
        }) => (
            *position,
            snapshot
                .attachments_for_message(*folder_id, *message_id)
                .unwrap_or_default()
                .iter()
                .filter(|attachment| {
                    restriction_matches_attachment(restriction.as_ref(), attachment)
                })
                .count(),
        ),
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
    }
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
            ..
        } if is_queryable_hierarchy_folder(*folder_id) => hierarchy_rows(
            *folder_id,
            mailboxes,
            snapshot,
            restriction.as_ref(),
            sort_orders,
            mailbox_guid,
        )
        .into_iter()
        .map(|row| hierarchy_row_id(&row))
        .collect(),
        MapiObject::ContentsTable {
            folder_id,
            sort_orders,
            restriction,
            ..
        } => {
            if *folder_id == NOTES_FOLDER_ID {
                let mut rows = snapshot.notes_for_folder(*folder_id);
                rows.retain(|note| restriction_matches_note(restriction.as_ref(), &note.note));
                sort_notes(&mut rows, sort_orders);
                return rows.into_iter().map(|note| note.id).collect();
            }
            if *folder_id == CONTACTS_SEARCH_FOLDER_ID {
                let mut rows = snapshot.contacts_search_results();
                rows.retain(|contact| {
                    restriction_matches_contact(restriction.as_ref(), &contact.contact)
                });
                sort_contacts(&mut rows, sort_orders);
                return rows.into_iter().map(|contact| contact.id).collect();
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
                rows.retain(|message| {
                    restriction_matches_email(restriction.as_ref(), &message.email)
                });
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
            let mut rows = emails_for_folder(*folder_id, mailboxes, emails);
            rows.retain(|email| restriction_matches_email(restriction.as_ref(), email));
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
            rows.retain(|attachment| {
                restriction_matches_attachment(restriction.as_ref(), attachment)
            });
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

pub(in crate::mapi) fn is_root_hierarchy_folder(folder_id: u64) -> bool {
    matches!(
        folder_id,
        ROOT_FOLDER_ID | IPM_SUBTREE_FOLDER_ID | PUBLIC_FOLDERS_ROOT_FOLDER_ID
    )
}

fn is_queryable_hierarchy_folder(folder_id: u64) -> bool {
    is_root_hierarchy_folder(folder_id) || folder_id == SYNC_ISSUES_FOLDER_ID
}

pub(in crate::mapi) fn is_advertised_special_folder(folder_id: u64) -> bool {
    matches!(
        folder_id,
        ROOT_FOLDER_ID
            | IPM_SUBTREE_FOLDER_ID
            | DEFERRED_ACTION_FOLDER_ID
            | SPOOLER_QUEUE_FOLDER_ID
            | COMMON_VIEWS_FOLDER_ID
            | SCHEDULE_FOLDER_ID
            | SEARCH_FOLDER_ID
            | VIEWS_FOLDER_ID
            | SHORTCUTS_FOLDER_ID
            | FREEBUSY_DATA_FOLDER_ID
            | RECOVERABLE_ITEMS_ROOT_FOLDER_ID
            | RECOVERABLE_ITEMS_DELETIONS_FOLDER_ID
            | RECOVERABLE_ITEMS_VERSIONS_FOLDER_ID
            | RECOVERABLE_ITEMS_PURGES_FOLDER_ID
            | PUBLIC_FOLDERS_ROOT_FOLDER_ID
    ) || role_for_folder_id(folder_id).is_some()
}

pub(in crate::mapi) fn role_for_folder_id(folder_id: u64) -> Option<&'static str> {
    match folder_id {
        INBOX_FOLDER_ID => Some("inbox"),
        DRAFTS_FOLDER_ID => Some("drafts"),
        SENT_FOLDER_ID => Some("sent"),
        TRASH_FOLDER_ID => Some("trash"),
        OUTBOX_FOLDER_ID => Some("outbox"),
        CONTACTS_FOLDER_ID => Some("contacts"),
        CALENDAR_FOLDER_ID => Some("calendar"),
        JOURNAL_FOLDER_ID => Some("journal"),
        NOTES_FOLDER_ID => Some("notes"),
        TASKS_FOLDER_ID => Some("tasks"),
        REMINDERS_FOLDER_ID => Some("reminders"),
        PUBLIC_FOLDERS_ROOT_FOLDER_ID => Some("public_folders_root"),
        SUGGESTED_CONTACTS_FOLDER_ID => Some("suggested_contacts"),
        QUICK_CONTACTS_FOLDER_ID => Some("quick_contacts"),
        IM_CONTACT_LIST_FOLDER_ID => Some("im_contact_list"),
        CONTACTS_SEARCH_FOLDER_ID => Some("contacts_search"),
        DOCUMENT_LIBRARIES_FOLDER_ID => Some("document_libraries"),
        SYNC_ISSUES_FOLDER_ID => Some("sync_issues"),
        CONFLICTS_FOLDER_ID => Some("conflicts"),
        LOCAL_FAILURES_FOLDER_ID => Some("local_failures"),
        SERVER_FAILURES_FOLDER_ID => Some("server_failures"),
        JUNK_FOLDER_ID => Some("junk"),
        RSS_FEEDS_FOLDER_ID => Some("rss_feeds"),
        TRACKED_MAIL_PROCESSING_FOLDER_ID => Some("tracked_mail_processing"),
        TODO_SEARCH_FOLDER_ID => Some("todo_search"),
        CONVERSATION_ACTION_SETTINGS_FOLDER_ID => Some("conversation_action_settings"),
        QUICK_STEP_SETTINGS_FOLDER_ID => Some("quick_step_settings"),
        ARCHIVE_FOLDER_ID => Some("archive"),
        CONVERSATION_HISTORY_FOLDER_ID => Some("conversation_history"),
        _ => None,
    }
}

pub(in crate::mapi) fn advertised_special_folder_id_for_create(
    parent_folder_id: u64,
    display_name: &str,
) -> Option<u64> {
    [
        INBOX_FOLDER_ID,
        OUTBOX_FOLDER_ID,
        SENT_FOLDER_ID,
        TRASH_FOLDER_ID,
        DRAFTS_FOLDER_ID,
        CONTACTS_FOLDER_ID,
        CALENDAR_FOLDER_ID,
        JOURNAL_FOLDER_ID,
        NOTES_FOLDER_ID,
        TASKS_FOLDER_ID,
        SUGGESTED_CONTACTS_FOLDER_ID,
        QUICK_CONTACTS_FOLDER_ID,
        IM_CONTACT_LIST_FOLDER_ID,
        CONTACTS_SEARCH_FOLDER_ID,
        DOCUMENT_LIBRARIES_FOLDER_ID,
        SYNC_ISSUES_FOLDER_ID,
        CONFLICTS_FOLDER_ID,
        LOCAL_FAILURES_FOLDER_ID,
        SERVER_FAILURES_FOLDER_ID,
        JUNK_FOLDER_ID,
        RSS_FEEDS_FOLDER_ID,
        TRACKED_MAIL_PROCESSING_FOLDER_ID,
        TODO_SEARCH_FOLDER_ID,
        CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
        QUICK_STEP_SETTINGS_FOLDER_ID,
        ARCHIVE_FOLDER_ID,
        FREEBUSY_DATA_FOLDER_ID,
        CONVERSATION_HISTORY_FOLDER_ID,
    ]
    .into_iter()
    .find(|folder_id| {
        let (name, parent_id, _, _) = special_folder_metadata(*folder_id);
        parent_id == parent_folder_id && name.eq_ignore_ascii_case(display_name)
    })
}

pub(in crate::mapi) fn serialize_special_folder_row(
    folder_id: u64,
    mailboxes: &[JmapMailbox],
    columns: &[u32],
    principal: Option<&AccountPrincipal>,
) -> Vec<u8> {
    match folder_id {
        IPM_SUBTREE_FOLDER_ID => serialize_ipm_subtree_folder_row(mailboxes, columns, principal),
        ROOT_FOLDER_ID => serialize_root_folder_row(mailboxes, columns, principal),
        _ => serialize_advertised_special_folder_row(folder_id, columns, principal),
    }
}

fn serialize_advertised_special_folder_row(
    folder_id: u64,
    columns: &[u32],
    principal: Option<&AccountPrincipal>,
) -> Vec<u8> {
    serialize_advertised_special_folder_row_with_mailbox_guid(
        folder_id,
        columns,
        principal
            .map(|principal| principal.account_id)
            .unwrap_or_default(),
    )
}

fn serialize_advertised_special_folder_row_with_mailbox_guid(
    folder_id: u64,
    columns: &[u32],
    mailbox_guid: Uuid,
) -> Vec<u8> {
    let mut row = Vec::new();
    let (display_name, parent_folder_id, message_class, has_subfolders) =
        special_folder_metadata(folder_id);
    let change_number = mapi_mailstore::change_number_for_store_id(folder_id);
    for column in columns {
        match *column {
            PID_TAG_DISPLAY_NAME_W => write_utf16z(&mut row, display_name),
            PID_TAG_ENTRY_ID => {
                let entry_id =
                    crate::mapi::identity::folder_entry_id_from_object_id(mailbox_guid, folder_id)
                        .unwrap_or_else(|| {
                            crate::mapi::identity::instance_key_for_object_id(folder_id)
                        });
                write_u16_prefixed_bytes(&mut row, &entry_id);
            }
            PID_TAG_INSTANCE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &crate::mapi::identity::instance_key_for_object_id(folder_id),
            ),
            PID_TAG_FOLDER_ID => write_object_id(&mut row, folder_id),
            PID_TAG_PARENT_FOLDER_ID => write_object_id(&mut row, parent_folder_id),
            PID_TAG_FOLDER_TYPE => write_u32(&mut row, special_folder_type(folder_id)),
            PID_TAG_ACCESS => write_u32(&mut row, MAPI_FOLDER_ACCESS),
            PID_TAG_CONTENT_COUNT | PID_TAG_CONTENT_UNREAD_COUNT | PID_TAG_DELETED_COUNT_TOTAL => {
                write_u32(&mut row, 0)
            }
            PID_TAG_SUBFOLDERS => row.push(has_subfolders as u8),
            PID_TAG_ATTRIBUTE_HIDDEN => {
                row.push((folder_id == CONVERSATION_ACTION_SETTINGS_FOLDER_ID) as u8)
            }
            PID_TAG_CONTAINER_CLASS_W | PID_TAG_MESSAGE_CLASS_W if message_class.is_empty() => {
                write_property_default(&mut row, *column)
            }
            PID_TAG_CONTAINER_CLASS_W => write_utf16z(&mut row, message_class),
            PID_TAG_MESSAGE_CLASS_W => write_utf16z(&mut row, message_class),
            PID_TAG_DEFAULT_POST_MESSAGE_CLASS_STRING8 if message_class == "IPF.Appointment" => {
                write_ascii_z(&mut row, "IPM.Appointment")
            }
            PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W if message_class == "IPF.Appointment" => {
                write_utf16z(&mut row, "IPM.Appointment")
            }
            PID_TAG_LAST_MODIFICATION_TIME
            | PID_TAG_LOCAL_COMMIT_TIME
            | PID_TAG_LOCAL_COMMIT_TIME_MAX
            | PID_TAG_HIER_REV => write_u64(
                &mut row,
                mapi_mailstore::filetime_from_change_number(change_number),
            ),
            PID_TAG_SERIALIZED_REPLID_GUID_MAP => {
                write_u16_prefixed_bytes(&mut row, &serialized_replid_guid_map())
            }
            PID_TAG_HIERARCHY_CHANGE_NUMBER => {
                write_u32(&mut row, change_number.min(u64::from(u32::MAX)) as u32)
            }
            PID_TAG_SOURCE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::source_key_for_store_id(folder_id),
            ),
            PID_TAG_PARENT_SOURCE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::source_key_for_store_id(parent_folder_id),
            ),
            PID_TAG_CHANGE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::change_key_for_change_number(change_number),
            ),
            PID_TAG_PREDECESSOR_CHANGE_LIST => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::predecessor_change_list(change_number),
            ),
            PID_TAG_CHANGE_NUMBER => write_u64(&mut row, change_number),
            _ if folder_id == INBOX_FOLDER_ID => {
                match special_folder_identification_property_value(mailbox_guid, *column) {
                    Some(value) => write_mapi_value(&mut row, *column, &value),
                    None => write_property_default(&mut row, *column),
                }
            }
            _ => write_property_default(&mut row, *column),
        }
    }
    row
}

fn special_folder_metadata(folder_id: u64) -> (&'static str, u64, &'static str, bool) {
    match folder_id {
        ROOT_FOLDER_ID => ("Root", 0, "", true),
        IPM_SUBTREE_FOLDER_ID => ("Top of Information Store", ROOT_FOLDER_ID, "IPF.Note", true),
        DEFERRED_ACTION_FOLDER_ID => ("Deferred Action", ROOT_FOLDER_ID, "", false),
        SPOOLER_QUEUE_FOLDER_ID => ("Spooler Queue", ROOT_FOLDER_ID, "", false),
        INBOX_FOLDER_ID => ("Inbox", IPM_SUBTREE_FOLDER_ID, "IPF.Note", false),
        OUTBOX_FOLDER_ID => ("Outbox", IPM_SUBTREE_FOLDER_ID, "IPF.Note", false),
        SENT_FOLDER_ID => ("Sent", IPM_SUBTREE_FOLDER_ID, "IPF.Note", false),
        TRASH_FOLDER_ID => ("Deleted Items", IPM_SUBTREE_FOLDER_ID, "IPF.Note", false),
        COMMON_VIEWS_FOLDER_ID => ("Common Views", ROOT_FOLDER_ID, "", false),
        SCHEDULE_FOLDER_ID => ("Schedule", ROOT_FOLDER_ID, "", false),
        SEARCH_FOLDER_ID => ("Search", ROOT_FOLDER_ID, "", false),
        VIEWS_FOLDER_ID => ("Personal Views", ROOT_FOLDER_ID, "", false),
        SHORTCUTS_FOLDER_ID => ("Shortcuts", ROOT_FOLDER_ID, "IPF.ShortcutFolder", false),
        DRAFTS_FOLDER_ID => ("Drafts", IPM_SUBTREE_FOLDER_ID, "IPF.Note", false),
        CONTACTS_FOLDER_ID => ("Contacts", IPM_SUBTREE_FOLDER_ID, "IPF.Contact", false),
        CALENDAR_FOLDER_ID => ("Calendar", IPM_SUBTREE_FOLDER_ID, "IPF.Appointment", false),
        JOURNAL_FOLDER_ID => ("Journal", IPM_SUBTREE_FOLDER_ID, "IPF.Journal", false),
        NOTES_FOLDER_ID => ("Notes", IPM_SUBTREE_FOLDER_ID, "IPF.StickyNote", false),
        TASKS_FOLDER_ID => ("Tasks", IPM_SUBTREE_FOLDER_ID, "IPF.Task", false),
        SUGGESTED_CONTACTS_FOLDER_ID => (
            "Suggested Contacts",
            IPM_SUBTREE_FOLDER_ID,
            "IPF.Contact",
            false,
        ),
        QUICK_CONTACTS_FOLDER_ID => (
            "Quick Contacts",
            IPM_SUBTREE_FOLDER_ID,
            "IPF.Contact.MOC.QuickContacts",
            false,
        ),
        IM_CONTACT_LIST_FOLDER_ID => (
            "IM Contact List",
            IPM_SUBTREE_FOLDER_ID,
            "IPF.Contact.MOC.ImContactList",
            false,
        ),
        CONTACTS_SEARCH_FOLDER_ID => (
            "Contacts Search",
            IPM_SUBTREE_FOLDER_ID,
            "IPF.Contact",
            false,
        ),
        DOCUMENT_LIBRARIES_FOLDER_ID => (
            "Document Libraries",
            ROOT_FOLDER_ID,
            "IPF.ShortcutFolder",
            false,
        ),
        SYNC_ISSUES_FOLDER_ID => ("Sync Issues", IPM_SUBTREE_FOLDER_ID, "IPF.Note", true),
        CONFLICTS_FOLDER_ID => ("Conflicts", SYNC_ISSUES_FOLDER_ID, "IPF.Note", false),
        LOCAL_FAILURES_FOLDER_ID => ("Local Failures", SYNC_ISSUES_FOLDER_ID, "IPF.Note", false),
        SERVER_FAILURES_FOLDER_ID => ("Server Failures", SYNC_ISSUES_FOLDER_ID, "IPF.Note", false),
        JUNK_FOLDER_ID => ("Junk E-mail", IPM_SUBTREE_FOLDER_ID, "IPF.Note", false),
        RSS_FEEDS_FOLDER_ID => (
            "RSS Feeds",
            IPM_SUBTREE_FOLDER_ID,
            "IPF.Note.OutlookHomepage",
            false,
        ),
        TRACKED_MAIL_PROCESSING_FOLDER_ID => {
            ("Tracked Mail Processing", ROOT_FOLDER_ID, "IPF.Note", false)
        }
        TODO_SEARCH_FOLDER_ID => ("To-Do", ROOT_FOLDER_ID, "IPF.Task", false),
        RECOVERABLE_ITEMS_ROOT_FOLDER_ID => ("Recoverable Items", ROOT_FOLDER_ID, "IPF.Note", true),
        RECOVERABLE_ITEMS_DELETIONS_FOLDER_ID => (
            "Deletions",
            RECOVERABLE_ITEMS_ROOT_FOLDER_ID,
            "IPF.Note",
            false,
        ),
        RECOVERABLE_ITEMS_VERSIONS_FOLDER_ID => (
            "Versions",
            RECOVERABLE_ITEMS_ROOT_FOLDER_ID,
            "IPF.Note",
            false,
        ),
        RECOVERABLE_ITEMS_PURGES_FOLDER_ID => (
            "Purges",
            RECOVERABLE_ITEMS_ROOT_FOLDER_ID,
            "IPF.Note",
            false,
        ),
        CONVERSATION_ACTION_SETTINGS_FOLDER_ID => (
            "Conversation Action Settings",
            IPM_SUBTREE_FOLDER_ID,
            "IPF.Configuration",
            false,
        ),
        QUICK_STEP_SETTINGS_FOLDER_ID => (
            "Quick Step Settings",
            IPM_SUBTREE_FOLDER_ID,
            "IPF.Configuration",
            false,
        ),
        ARCHIVE_FOLDER_ID => ("Archive", IPM_SUBTREE_FOLDER_ID, "IPF.Note", false),
        FREEBUSY_DATA_FOLDER_ID => ("FreeBusy Data", ROOT_FOLDER_ID, "", false),
        CONVERSATION_HISTORY_FOLDER_ID => (
            "Conversation History",
            IPM_SUBTREE_FOLDER_ID,
            "IPF.Note",
            false,
        ),
        REMINDERS_FOLDER_ID => ("Reminders", ROOT_FOLDER_ID, "Outlook.Reminder", false),
        PUBLIC_FOLDERS_ROOT_FOLDER_ID => ("Public Folders", 0, "IPF.Note", true),
        _ => ("Root", 0, "", true),
    }
}

fn special_folder_type(folder_id: u64) -> u32 {
    match folder_id {
        ROOT_FOLDER_ID | PUBLIC_FOLDERS_ROOT_FOLDER_ID => FOLDER_ROOT,
        CONTACTS_SEARCH_FOLDER_ID
        | REMINDERS_FOLDER_ID
        | TRACKED_MAIL_PROCESSING_FOLDER_ID
        | TODO_SEARCH_FOLDER_ID => FOLDER_SEARCH,
        _ => FOLDER_GENERIC,
    }
}

pub(in crate::mapi) fn serialize_root_folder_row(
    _mailboxes: &[JmapMailbox],
    columns: &[u32],
    principal: Option<&AccountPrincipal>,
) -> Vec<u8> {
    let mut row = Vec::new();
    let change_number = mapi_mailstore::change_number_for_store_id(ROOT_FOLDER_ID);
    for column in columns {
        match *column {
            PID_TAG_DISPLAY_NAME_W => write_utf16z(&mut row, "Root"),
            PID_TAG_ENTRY_ID => {
                let mailbox_guid = principal
                    .map(|principal| principal.account_id)
                    .unwrap_or_default();
                let entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
                    mailbox_guid,
                    ROOT_FOLDER_ID,
                )
                .unwrap_or_else(|| {
                    crate::mapi::identity::instance_key_for_object_id(ROOT_FOLDER_ID)
                });
                write_u16_prefixed_bytes(&mut row, &entry_id);
            }
            PID_TAG_INSTANCE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &crate::mapi::identity::instance_key_for_object_id(ROOT_FOLDER_ID),
            ),
            PID_TAG_FOLDER_ID => write_object_id(&mut row, ROOT_FOLDER_ID),
            PID_TAG_PARENT_FOLDER_ID => write_object_id(&mut row, 0),
            PID_TAG_FOLDER_TYPE => write_u32(&mut row, FOLDER_ROOT),
            PID_TAG_ACCESS => write_u32(&mut row, MAPI_FOLDER_ACCESS),
            PID_TAG_CONTENT_COUNT | PID_TAG_CONTENT_UNREAD_COUNT | PID_TAG_DELETED_COUNT_TOTAL => {
                write_u32(&mut row, 0)
            }
            PID_TAG_SUBFOLDERS => row.push(1),
            PID_TAG_CONTAINER_CLASS_W | PID_TAG_MESSAGE_CLASS_W => {
                write_property_default(&mut row, *column)
            }
            PID_TAG_LAST_MODIFICATION_TIME
            | PID_TAG_LOCAL_COMMIT_TIME
            | PID_TAG_LOCAL_COMMIT_TIME_MAX
            | PID_TAG_HIER_REV => write_u64(
                &mut row,
                mapi_mailstore::filetime_from_change_number(change_number),
            ),
            PID_TAG_SERIALIZED_REPLID_GUID_MAP => {
                write_u16_prefixed_bytes(&mut row, &serialized_replid_guid_map())
            }
            PID_TAG_HIERARCHY_CHANGE_NUMBER => {
                write_u32(&mut row, change_number.min(u64::from(u32::MAX)) as u32)
            }
            PID_TAG_SOURCE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::source_key_for_store_id(ROOT_FOLDER_ID),
            ),
            PID_TAG_PARENT_SOURCE_KEY => write_u16_prefixed_bytes(&mut row, &[]),
            PID_TAG_CHANGE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::change_key_for_change_number(change_number),
            ),
            PID_TAG_PREDECESSOR_CHANGE_LIST => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::predecessor_change_list(change_number),
            ),
            PID_TAG_CHANGE_NUMBER => write_u64(&mut row, change_number),
            _ => match special_folder_identification_property_value(
                principal
                    .map(|principal| principal.account_id)
                    .unwrap_or_default(),
                *column,
            ) {
                Some(value) => write_mapi_value(&mut row, *column, &value),
                None => write_property_default(&mut row, *column),
            },
        }
    }
    row
}

pub(in crate::mapi) fn serialize_ipm_subtree_folder_row(
    _mailboxes: &[JmapMailbox],
    columns: &[u32],
    principal: Option<&AccountPrincipal>,
) -> Vec<u8> {
    let mut row = Vec::new();
    let change_number = mapi_mailstore::change_number_for_store_id(IPM_SUBTREE_FOLDER_ID);
    for column in columns {
        match *column {
            PID_TAG_DISPLAY_NAME_W => write_utf16z(&mut row, "Top of Information Store"),
            PID_TAG_ENTRY_ID => {
                let mailbox_guid = principal
                    .map(|principal| principal.account_id)
                    .unwrap_or_default();
                let entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
                    mailbox_guid,
                    IPM_SUBTREE_FOLDER_ID,
                )
                .unwrap_or_else(|| {
                    crate::mapi::identity::instance_key_for_object_id(IPM_SUBTREE_FOLDER_ID)
                });
                write_u16_prefixed_bytes(&mut row, &entry_id);
            }
            PID_TAG_INSTANCE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &crate::mapi::identity::instance_key_for_object_id(IPM_SUBTREE_FOLDER_ID),
            ),
            PID_TAG_FOLDER_ID => write_object_id(&mut row, IPM_SUBTREE_FOLDER_ID),
            PID_TAG_PARENT_FOLDER_ID => write_object_id(&mut row, ROOT_FOLDER_ID),
            PID_TAG_FOLDER_TYPE => write_u32(&mut row, FOLDER_GENERIC),
            PID_TAG_ACCESS => write_u32(&mut row, MAPI_FOLDER_ACCESS),
            PID_TAG_CONTENT_COUNT | PID_TAG_CONTENT_UNREAD_COUNT | PID_TAG_DELETED_COUNT_TOTAL => {
                write_u32(&mut row, 0)
            }
            PID_TAG_SUBFOLDERS => row.push(1),
            PID_TAG_CONTAINER_CLASS_W => write_utf16z(&mut row, "IPF.Note"),
            PID_TAG_MESSAGE_CLASS_W => write_utf16z(&mut row, "IPF.Note"),
            PID_TAG_LAST_MODIFICATION_TIME
            | PID_TAG_LOCAL_COMMIT_TIME
            | PID_TAG_LOCAL_COMMIT_TIME_MAX
            | PID_TAG_HIER_REV => write_u64(
                &mut row,
                mapi_mailstore::filetime_from_change_number(change_number),
            ),
            PID_TAG_SERIALIZED_REPLID_GUID_MAP => {
                write_u16_prefixed_bytes(&mut row, &serialized_replid_guid_map())
            }
            PID_TAG_HIERARCHY_CHANGE_NUMBER => {
                write_u32(&mut row, change_number.min(u64::from(u32::MAX)) as u32)
            }
            PID_TAG_SOURCE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::source_key_for_store_id(IPM_SUBTREE_FOLDER_ID),
            ),
            PID_TAG_PARENT_SOURCE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::source_key_for_store_id(ROOT_FOLDER_ID),
            ),
            PID_TAG_CHANGE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::change_key_for_change_number(change_number),
            ),
            PID_TAG_PREDECESSOR_CHANGE_LIST => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::predecessor_change_list(change_number),
            ),
            PID_TAG_CHANGE_NUMBER => write_u64(&mut row, change_number),
            PID_TAG_OST_OSTID => write_u16_prefixed_bytes(
                &mut row,
                &principal.map(ipm_subtree_ost_ostid).unwrap_or_default(),
            ),
            _ => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(in crate::mapi) fn write_standard_property_row(response: &mut Vec<u8>, values: &[u8]) {
    response.push(0);
    response.extend_from_slice(values);
}

fn write_query_rows_property_row(response: &mut Vec<u8>, columns: &[u32], values: &[u8]) {
    response.extend_from_slice(&query_rows_property_row_bytes(columns, values));
}

pub(in crate::mapi) fn query_rows_property_row_bytes(columns: &[u32], values: &[u8]) -> Vec<u8> {
    if columns.is_empty() {
        return standard_property_row_bytes(values);
    }

    let mut cursor = Cursor::new(values);
    let mut row = Vec::with_capacity(values.len().saturating_add(columns.len() + 1));
    row.push(1);
    for column in columns {
        let value_start = cursor.position();
        if parse_mapi_property_value(&mut cursor, *column).is_err() {
            return standard_property_row_bytes(values);
        }
        let value_end = cursor.position();
        row.push(0);
        row.extend_from_slice(&values[value_start..value_end]);
    }
    if !cursor.remaining_is_zero_padding() {
        return standard_property_row_bytes(values);
    }
    row
}

pub(in crate::mapi) fn standard_property_row_bytes(values: &[u8]) -> Vec<u8> {
    let mut row = Vec::with_capacity(values.len().saturating_add(1));
    write_standard_property_row(&mut row, values);
    row
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapi::wire::MapiRestrictionType;
    use crate::mapi::wire::RopId;
    use lpe_storage::{
        AccessibleContact, CollaborationCollection, CollaborationRights, MailboxRule,
        SearchFolderDefinition,
    };

    fn exchange_builtin_excluded_folder_roles() -> Vec<String> {
        [
            "trash",
            "junk",
            "drafts",
            "outbox",
            "conflicts",
            "local_failures",
            "server_failures",
            "sync_issues",
        ]
        .into_iter()
        .map(str::to_string)
        .collect()
    }

    #[test]
    fn default_hierarchy_columns_cover_table_projection_contract() {
        let columns = default_hierarchy_columns();
        for property_tag in [
            PID_TAG_DISPLAY_NAME_W,
            PID_TAG_ENTRY_ID,
            PID_TAG_INSTANCE_KEY,
            PID_TAG_FOLDER_ID,
            PID_TAG_PARENT_FOLDER_ID,
            PID_TAG_FOLDER_TYPE,
            PID_TAG_ACCESS,
            PID_TAG_SOURCE_KEY,
            PID_TAG_PARENT_SOURCE_KEY,
            PID_TAG_CHANGE_KEY,
            PID_TAG_PREDECESSOR_CHANGE_LIST,
            PID_TAG_CHANGE_NUMBER,
            PID_TAG_CONTENT_COUNT,
            PID_TAG_CONTENT_UNREAD_COUNT,
            PID_TAG_CONTAINER_CLASS_W,
            PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W,
            PID_TAG_SERIALIZED_REPLID_GUID_MAP,
            PID_TAG_SUBFOLDERS,
        ] {
            assert!(columns.contains(&property_tag));
        }
    }

    #[test]
    fn outlook_bootstrap_row_invariant_classifier_reports_consistency() {
        let folder_id = INBOX_FOLDER_ID;
        let parent_id = IPM_SUBTREE_FOLDER_ID;
        let mailbox_guid = Uuid::new_v4();
        let entry_id =
            crate::mapi::identity::folder_entry_id_from_object_id(mailbox_guid, folder_id).unwrap();
        let parent_entry_id =
            crate::mapi::identity::folder_entry_id_from_object_id(mailbox_guid, parent_id).unwrap();
        let source_key = mapi_mailstore::source_key_for_store_id(folder_id);
        let parent_source_key = mapi_mailstore::source_key_for_store_id(parent_id);
        let instance_key = crate::mapi::identity::instance_key_for_object_id(folder_id);

        let summary = classify_outlook_bootstrap_row_invariants(
            0,
            "hierarchy_folder",
            folder_id,
            Some(folder_id),
            Some(parent_id),
            Some("IPF.Note"),
            |tag| match canonical_property_storage_tag(tag) {
                PID_TAG_ENTRY_ID => Some(MapiValue::Binary(entry_id.clone())),
                PID_TAG_RECORD_KEY => Some(MapiValue::Binary(source_key.clone())),
                PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(source_key.clone())),
                PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(parent_source_key.clone())),
                PID_TAG_PARENT_ENTRY_ID => Some(MapiValue::Binary(parent_entry_id.clone())),
                PID_TAG_FOLDER_ID => Some(MapiValue::U64(folder_id)),
                PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(instance_key.clone())),
                PID_TAG_DISPLAY_NAME_W => Some(MapiValue::String("Inbox".to_string())),
                PID_TAG_CONTAINER_CLASS_W => Some(MapiValue::String("IPF.Note".to_string())),
                PID_TAG_FOLDER_TYPE => Some(MapiValue::U32(FOLDER_GENERIC)),
                PID_TAG_CONTENT_COUNT | PID_TAG_ASSOCIATED_CONTENT_COUNT => Some(MapiValue::U32(0)),
                _ => None,
            },
        );

        assert!(summary.contains("folder_id_consistent=true"));
        assert!(summary.contains("parent_id_consistent=true"));
        assert!(summary.contains("source_key_stable_non_empty=true"));
        assert!(summary.contains("record_key_stable_non_empty=true"));
        assert!(summary.contains("issues=none"));
    }

    #[test]
    fn outlook_bootstrap_expected_container_class_matches_special_rows() {
        for (folder_id, expected) in [
            (TASKS_FOLDER_ID, "IPF.Task"),
            (RSS_FEEDS_FOLDER_ID, "IPF.Note.OutlookHomepage"),
            (CONVERSATION_ACTION_SETTINGS_FOLDER_ID, "IPF.Configuration"),
            (QUICK_STEP_SETTINGS_FOLDER_ID, "IPF.Configuration"),
            (QUICK_CONTACTS_FOLDER_ID, "IPF.Contact.MOC.QuickContacts"),
            (IM_CONTACT_LIST_FOLDER_ID, "IPF.Contact.MOC.ImContactList"),
        ] {
            assert_eq!(debug_expected_container_class(folder_id), Some(expected));
        }
    }

    #[test]
    fn outlook_bootstrap_row_invariant_classifier_flags_missing_record_key() {
        let folder_id = INBOX_FOLDER_ID;
        let source_key = mapi_mailstore::source_key_for_store_id(folder_id);
        let summary = classify_outlook_bootstrap_row_invariants(
            0,
            "hierarchy_folder",
            folder_id,
            Some(folder_id),
            None,
            None,
            |tag| match canonical_property_storage_tag(tag) {
                PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(source_key.clone())),
                PID_TAG_FOLDER_ID => Some(MapiValue::U64(folder_id)),
                PID_TAG_CONTENT_COUNT | PID_TAG_ASSOCIATED_CONTENT_COUNT => Some(MapiValue::U32(0)),
                PID_TAG_FOLDER_TYPE => Some(MapiValue::U32(FOLDER_GENERIC)),
                _ => None,
            },
        );

        assert!(summary.contains("record_key_stable_non_empty=false"));
        assert!(summary.contains("issues="));
        assert!(summary.contains("record_key"));
    }

    #[test]
    fn default_contents_columns_cover_table_projection_contract() {
        let columns = default_contents_columns();
        for property_tag in [
            PID_TAG_MID,
            PID_TAG_ENTRY_ID,
            PID_TAG_INSTANCE_KEY,
            PID_TAG_SOURCE_KEY,
            PID_TAG_PARENT_SOURCE_KEY,
            PID_TAG_CHANGE_KEY,
            PID_TAG_PREDECESSOR_CHANGE_LIST,
            PID_TAG_CHANGE_NUMBER,
            PID_TAG_SUBJECT_W,
            PID_TAG_NORMALIZED_SUBJECT_W,
            PID_TAG_MESSAGE_DELIVERY_TIME,
            PID_TAG_CLIENT_SUBMIT_TIME,
            PID_TAG_SENDER_NAME_W,
            PID_TAG_SENDER_EMAIL_ADDRESS_W,
            PID_TAG_DISPLAY_TO_W,
            PID_TAG_DISPLAY_CC_W,
            PID_TAG_MESSAGE_FLAGS,
            PID_TAG_READ,
            PID_TAG_MESSAGE_CLASS_W,
            PID_TAG_MESSAGE_SIZE,
            PID_TAG_HAS_ATTACHMENTS,
        ] {
            assert!(columns.contains(&property_tag));
        }
    }

    #[test]
    fn contacts_search_folder_message_count_matches_projected_results() {
        let account_id = Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap();
        let collection = CollaborationCollection {
            id: "default".to_string(),
            kind: "contacts".to_string(),
            owner_account_id: account_id,
            owner_email: "test@example.test".to_string(),
            owner_display_name: "Test".to_string(),
            display_name: "Contacts".to_string(),
            is_owned: true,
            rights: CollaborationRights {
                may_read: true,
                may_write: true,
                may_delete: true,
                may_share: true,
            },
        };
        let contact_id = Uuid::parse_str("71717171-7171-7171-7171-717171717171").unwrap();
        crate::mapi::identity::remember_mapi_identity(
            contact_id,
            crate::mapi::identity::mapi_store_id(67),
        );
        let contact = AccessibleContact {
            id: contact_id,
            collection_id: collection.id.clone(),
            owner_account_id: account_id,
            owner_email: "test@example.test".to_string(),
            owner_display_name: "Test".to_string(),
            rights: collection.rights.clone(),
            name: "Denis Ducret".to_string(),
            role: String::new(),
            email: "denis@example.test".to_string(),
            phone: String::new(),
            team: String::new(),
            notes: String::new(),
        };
        let snapshot = MapiMailStoreSnapshot::new(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec![collection],
            Vec::new(),
            Vec::new(),
            vec![contact],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .with_search_folder_definitions(vec![SearchFolderDefinition {
            id: Uuid::parse_str("34343434-3434-4434-8434-343434343402").unwrap(),
            account_id,
            role: "contacts_search".to_string(),
            display_name: "Contacts Search".to_string(),
            definition_kind: "exchange_builtin".to_string(),
            result_object_kind: "contact".to_string(),
            scope_json: serde_json::json!({"scope": "contacts"}),
            restriction_json: serde_json::json!({"kind": "contacts_search"}),
            excluded_folder_roles: Vec::new(),
            is_builtin: true,
        }]);

        assert_eq!(
            folder_message_count(CONTACTS_SEARCH_FOLDER_ID, &[], &[], &snapshot),
            1
        );
    }

    #[test]
    fn get_status_accepts_folder_handles_after_sync_import() {
        let request = RopRequest {
            rop_id: RopId::GetStatus.as_u8(),
            input_handle_index: Some(1),
            output_handle_index: None,
            payload: Vec::new(),
        };
        let folder = MapiObject::Folder {
            folder_id: CONTACTS_SEARCH_FOLDER_ID,
            properties: HashMap::new(),
        };

        assert_eq!(
            rop_get_status_response(&request, Some(&folder)),
            vec![RopId::GetStatus.as_u8(), 1, 0, 0, 0, 0, 0]
        );
    }

    #[test]
    fn special_folder_rows_use_global_counters_for_change_xids() {
        let row = serialize_special_folder_row(
            INBOX_FOLDER_ID,
            &[],
            &[PID_TAG_CHANGE_NUMBER, PID_TAG_CHANGE_KEY],
            None,
        );
        let change_number = u64::from_le_bytes(row[0..8].try_into().unwrap());
        let change_key_len = u16::from_le_bytes(row[8..10].try_into().unwrap()) as usize;
        let change_key = &row[10..10 + change_key_len];

        assert_eq!(change_number, crate::mapi::identity::INBOX_FOLDER_COUNTER);
        assert_eq!(change_key_len, 22);
        assert_eq!(
            &change_key[16..22],
            &crate::mapi::identity::globcnt_bytes(change_number)
        );
    }

    #[test]
    fn special_folder_rows_project_deleted_count_total() {
        let row = serialize_special_folder_row(
            COMMON_VIEWS_FOLDER_ID,
            &[],
            &[
                PID_TAG_LOCAL_COMMIT_TIME_MAX,
                PID_TAG_DELETED_COUNT_TOTAL,
                PID_TAG_CONTENT_UNREAD_COUNT,
                PID_TAG_CONTENT_COUNT,
            ],
            None,
        );

        assert_eq!(row.len(), 20);
        assert_eq!(u32::from_le_bytes(row[8..12].try_into().unwrap()), 0);
        assert_eq!(u32::from_le_bytes(row[12..16].try_into().unwrap()), 0);
        assert_eq!(u32::from_le_bytes(row[16..20].try_into().unwrap()), 0);
    }

    #[test]
    fn quick_step_settings_is_projected_as_leaf_configuration_folder() {
        assert_eq!(
            special_folder_property_value(
                QUICK_STEP_SETTINGS_FOLDER_ID,
                PID_TAG_SUBFOLDERS,
                Uuid::nil()
            ),
            Some(MapiValue::Bool(false))
        );

        let row = serialize_special_folder_row(
            QUICK_STEP_SETTINGS_FOLDER_ID,
            &[],
            &[PID_TAG_SUBFOLDERS],
            None,
        );
        assert_eq!(row, vec![0]);
    }

    #[test]
    fn conversation_action_settings_projects_hidden_attribute() {
        assert_eq!(
            special_folder_property_value(
                CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
                PID_TAG_ATTRIBUTE_HIDDEN,
                Uuid::nil()
            ),
            Some(MapiValue::Bool(true))
        );
        assert_eq!(
            special_folder_property_value(
                QUICK_STEP_SETTINGS_FOLDER_ID,
                PID_TAG_ATTRIBUTE_HIDDEN,
                Uuid::nil()
            ),
            Some(MapiValue::Bool(false))
        );

        let row = serialize_special_folder_row(
            CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
            &[],
            &[PID_TAG_ATTRIBUTE_HIDDEN],
            None,
        );
        assert_eq!(row, vec![1]);
    }

    #[test]
    fn sync_issues_hierarchy_table_projects_special_child_folders() {
        let snapshot = MapiMailStoreSnapshot::empty();
        let inbox = JmapMailbox {
            id: Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap(),
            parent_id: None,
            role: "inbox".to_string(),
            name: "INBOX".to_string(),
            sort_order: 0,
            modseq: 1,
            total_emails: 18,
            unread_emails: 0,
            is_subscribed: true,
        };
        let mailboxes = [inbox];
        let rows = hierarchy_rows(
            SYNC_ISSUES_FOLDER_ID,
            &mailboxes,
            &snapshot,
            None,
            &[],
            Uuid::nil(),
        );
        let row_ids = rows.iter().map(hierarchy_row_id).collect::<Vec<_>>();

        assert_eq!(
            row_ids,
            vec![
                CONFLICTS_FOLDER_ID,
                LOCAL_FAILURES_FOLDER_ID,
                SERVER_FAILURES_FOLDER_ID
            ]
        );
        for row in rows {
            assert_eq!(hierarchy_row_parent_id(&row, &[]), SYNC_ISSUES_FOLDER_ID);
        }
    }

    #[test]
    fn sync_issues_query_rows_projects_special_child_folders() {
        let snapshot = MapiMailStoreSnapshot::empty();
        let inbox = JmapMailbox {
            id: Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap(),
            parent_id: None,
            role: "inbox".to_string(),
            name: "INBOX".to_string(),
            sort_order: 0,
            modseq: 1,
            total_emails: 18,
            unread_emails: 0,
            is_subscribed: true,
        };
        let mailboxes = [inbox];
        let mut table = MapiObject::HierarchyTable {
            folder_id: SYNC_ISSUES_FOLDER_ID,
            columns: vec![PID_TAG_DISPLAY_NAME_W, PID_TAG_FOLDER_ID],
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        };
        let request = RopRequest {
            rop_id: RopId::QueryRows.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: vec![0, 1, 10, 0],
        };

        let response = rop_query_rows_response(
            &request,
            Some(&mut table),
            &mailboxes,
            &[],
            &snapshot,
            Uuid::nil(),
        );

        assert_eq!(response[0], RopId::QueryRows.as_u8());
        assert_eq!(u16::from_le_bytes(response[7..9].try_into().unwrap()), 3);
        assert!(utf16_position(&response, "INBOX").is_none());
        assert_response_contains_utf16(&response, "Conflicts");
        assert_response_contains_utf16(&response, "Local Failures");
        assert_response_contains_utf16(&response, "Server Failures");
        assert_eq!(table_position(&table), Some(3));
    }

    #[test]
    fn query_rows_origin_tracks_cursor_boundary() {
        let snapshot = MapiMailStoreSnapshot::empty();
        let inbox = JmapMailbox {
            id: Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap(),
            parent_id: None,
            role: "inbox".to_string(),
            name: "INBOX".to_string(),
            sort_order: 0,
            modseq: 1,
            total_emails: 18,
            unread_emails: 0,
            is_subscribed: true,
        };
        let mailboxes = [inbox];
        let mut table = MapiObject::HierarchyTable {
            folder_id: SYNC_ISSUES_FOLDER_ID,
            columns: vec![PID_TAG_DISPLAY_NAME_W],
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        };
        let request = RopRequest {
            rop_id: RopId::QueryRows.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: vec![0, 1, 1, 0],
        };

        let response = rop_query_rows_response(
            &request,
            Some(&mut table),
            &mailboxes,
            &[],
            &snapshot,
            Uuid::nil(),
        );

        assert_eq!(response[0], RopId::QueryRows.as_u8());
        assert_eq!(response[6], 0x01);
        assert_eq!(u16::from_le_bytes(response[7..9].try_into().unwrap()), 1);
        assert_eq!(table_position(&table), Some(1));

        let response = rop_query_rows_response(
            &RopRequest {
                payload: vec![0, 1, 10, 0],
                ..request
            },
            Some(&mut table),
            &mailboxes,
            &[],
            &snapshot,
            Uuid::nil(),
        );

        assert_eq!(response[6], 0x02);
        assert_eq!(u16::from_le_bytes(response[7..9].try_into().unwrap()), 2);
        assert_eq!(table_position(&table), Some(3));
    }

    #[test]
    fn mapi_hierarchy_row_projects_inbox_display_name() {
        let inbox = JmapMailbox {
            id: Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap(),
            parent_id: None,
            role: "inbox".to_string(),
            name: "INBOX".to_string(),
            sort_order: 0,
            modseq: 1,
            total_emails: 18,
            unread_emails: 0,
            is_subscribed: true,
        };

        let row =
            serialize_folder_row_with_context(&inbox, &[], &[PID_TAG_DISPLAY_NAME_W], Uuid::nil());

        assert!(utf16_position(&row, "INBOX").is_none());
        assert_response_contains_utf16(&row, "Inbox");
    }

    #[test]
    fn ipm_subtree_row_projects_principal_ost_identity_when_available() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap(),
            account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
            email: "test@l-p-e.ch".to_string(),
            display_name: "test".to_string(),
        };
        let row =
            serialize_special_folder_row(IPM_SUBTREE_FOLDER_ID, &[], &[PID_TAG_OST_OSTID], None);
        assert_eq!(u16::from_le_bytes(row[0..2].try_into().unwrap()), 0);

        let row = serialize_special_folder_row(
            IPM_SUBTREE_FOLDER_ID,
            &[],
            &[PID_TAG_OST_OSTID],
            Some(&principal),
        );
        assert_eq!(u16::from_le_bytes(row[0..2].try_into().unwrap()), 20);
        assert_eq!(&row[2..18], principal.account_id.as_bytes());
        assert_eq!(u32::from_le_bytes(row[18..22].try_into().unwrap()), 1);
    }

    #[test]
    fn root_and_ipm_subtree_rows_project_entry_id_identity() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap(),
            account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
            email: "test@l-p-e.ch".to_string(),
            display_name: "test".to_string(),
        };

        for folder_id in [ROOT_FOLDER_ID, IPM_SUBTREE_FOLDER_ID] {
            let row = serialize_special_folder_row(
                folder_id,
                &[],
                &[PID_TAG_ENTRY_ID, PID_TAG_INSTANCE_KEY],
                Some(&principal),
            );
            let entry_id_len = u16::from_le_bytes(row[0..2].try_into().unwrap()) as usize;
            let entry_id = &row[2..2 + entry_id_len];
            let instance_key_offset = 2 + entry_id_len;
            let instance_key_len = u16::from_le_bytes(
                row[instance_key_offset..instance_key_offset + 2]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let instance_key =
                &row[instance_key_offset + 2..instance_key_offset + 2 + instance_key_len];

            assert_eq!(entry_id_len, 46);
            assert_eq!(
                crate::mapi::identity::object_id_from_folder_entry_id(entry_id),
                Some(folder_id)
            );
            assert_eq!(
                instance_key,
                crate::mapi::identity::instance_key_for_object_id(folder_id)
            );
        }
    }

    #[test]
    fn ipm_subtree_hierarchy_restrictions_match_serialized_display_name() {
        let restriction = MapiRestriction::Content {
            property_tag: PID_TAG_DISPLAY_NAME_W,
            value: "Top of Information Store".to_string(),
        };

        assert!(special_hierarchy_row_matches(
            IPM_SUBTREE_FOLDER_ID,
            Some(&restriction),
            Uuid::nil()
        ));
        assert_eq!(
            special_folder_property_value(
                IPM_SUBTREE_FOLDER_ID,
                PID_TAG_DISPLAY_NAME_W,
                Uuid::nil()
            ),
            Some(MapiValue::String("Top of Information Store".to_string()))
        );
    }

    #[test]
    fn folder_type_rows_follow_microsoft_values() {
        let mailbox = JmapMailbox {
            id: Uuid::nil(),
            parent_id: None,
            role: "inbox".to_string(),
            name: "Inbox".to_string(),
            sort_order: 0,
            modseq: 1,
            total_emails: 0,
            unread_emails: 0,
            is_subscribed: true,
        };

        let mailbox_row =
            serialize_folder_row_with_context(&mailbox, &[], &[PID_TAG_FOLDER_TYPE], Uuid::nil());
        assert_eq!(
            u32::from_le_bytes(mailbox_row.try_into().unwrap()),
            FOLDER_GENERIC
        );

        let root_row =
            serialize_special_folder_row(ROOT_FOLDER_ID, &[], &[PID_TAG_FOLDER_TYPE], None);
        assert_eq!(
            u32::from_le_bytes(root_row.try_into().unwrap()),
            FOLDER_ROOT
        );

        let ipm_row =
            serialize_special_folder_row(IPM_SUBTREE_FOLDER_ID, &[], &[PID_TAG_FOLDER_TYPE], None);
        assert_eq!(
            u32::from_le_bytes(ipm_row.try_into().unwrap()),
            FOLDER_GENERIC
        );

        let finder_root_row =
            serialize_special_folder_row(SEARCH_FOLDER_ID, &[], &[PID_TAG_FOLDER_TYPE], None);
        assert_eq!(
            u32::from_le_bytes(finder_root_row.try_into().unwrap()),
            FOLDER_GENERIC
        );

        for folder_id in [
            CONTACTS_SEARCH_FOLDER_ID,
            REMINDERS_FOLDER_ID,
            TRACKED_MAIL_PROCESSING_FOLDER_ID,
            TODO_SEARCH_FOLDER_ID,
        ] {
            let search_row =
                serialize_special_folder_row(folder_id, &[], &[PID_TAG_FOLDER_TYPE], None);
            assert_eq!(
                u32::from_le_bytes(search_row.try_into().unwrap()),
                FOLDER_SEARCH
            );
        }
    }

    #[test]
    fn hierarchy_table_projects_user_saved_search_folder() {
        let definition_id = Uuid::parse_str("aaaaaaaa-3333-4111-8111-aaaaaaaaaaaa").unwrap();
        let folder_id = crate::mapi::identity::mapi_store_id(124);
        crate::mapi::identity::remember_mapi_identity(definition_id, folder_id);
        let snapshot = MapiMailStoreSnapshot::new(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .with_search_folder_definitions(vec![SearchFolderDefinition {
            id: definition_id,
            account_id: Uuid::nil(),
            role: "custom".to_string(),
            display_name: "Unread from Alice".to_string(),
            definition_kind: "user_saved".to_string(),
            result_object_kind: "message".to_string(),
            scope_json: serde_json::json!({"scope": "top_of_personal_folders"}),
            restriction_json: serde_json::json!({"kind": "text", "query": "alice"}),
            excluded_folder_roles: vec!["trash".to_string()],
            is_builtin: false,
        }]);
        let mailboxes = snapshot.mailboxes();
        let rows = hierarchy_rows(
            IPM_SUBTREE_FOLDER_ID,
            &mailboxes,
            &snapshot,
            None,
            &[],
            Uuid::nil(),
        );
        let row = rows
            .iter()
            .find(|row| hierarchy_row_id(row) == folder_id)
            .expect("search folder hierarchy row");

        assert_eq!(hierarchy_row_display_name(row), "Unread from Alice");
        let serialized = serialize_hierarchy_row(
            *row,
            &mailboxes,
            &[
                PID_TAG_FOLDER_TYPE,
                PID_TAG_PARENT_FOLDER_ID,
                PID_TAG_CONTAINER_CLASS_W,
            ],
            Uuid::nil(),
        );
        assert_eq!(
            u32::from_le_bytes(serialized[0..4].try_into().unwrap()),
            FOLDER_SEARCH
        );
        let mailbox = match row {
            HierarchyRow::Mailbox(mailbox) => mailbox,
            _ => panic!("expected mailbox-backed search folder row"),
        };
        assert_eq!(mapi_parent_folder_id(mailbox), IPM_SUBTREE_FOLDER_ID);
        let class = "IPF.Note"
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect::<Vec<_>>();
        assert!(serialized
            .windows(class.len())
            .any(|window| window == class));
    }

    #[test]
    fn ipm_subtree_hierarchy_suppresses_mail_folders_shadowing_outlook_special_folders() {
        let shadow_id = Uuid::parse_str("aaaaaaaa-5555-4111-8111-aaaaaaaaaaaa").unwrap();
        let suggested_shadow_id = Uuid::parse_str("aaaaaaaa-6666-4111-8111-aaaaaaaaaaaa").unwrap();
        let quick_contacts_shadow_id =
            Uuid::parse_str("aaaaaaaa-7777-4111-8111-aaaaaaaaaaaa").unwrap();
        let im_contacts_shadow_id =
            Uuid::parse_str("aaaaaaaa-8888-4111-8111-aaaaaaaaaaaa").unwrap();
        let tasks_shadow_id = Uuid::parse_str("aaaaaaaa-9999-4111-8111-aaaaaaaaaaaa").unwrap();
        let quick_step_shadow_id = Uuid::parse_str("aaaaaaaa-aaaa-4111-8111-aaaaaaaaaaaa").unwrap();
        let shadow_folder_id = crate::mapi::identity::mapi_store_id(0x4f);
        let suggested_shadow_folder_id = crate::mapi::identity::mapi_store_id(0x54);
        let quick_contacts_shadow_folder_id = crate::mapi::identity::mapi_store_id(0x55);
        let im_contacts_shadow_folder_id = crate::mapi::identity::mapi_store_id(0x56);
        let tasks_shadow_folder_id = crate::mapi::identity::mapi_store_id(0x57);
        let quick_step_shadow_folder_id = crate::mapi::identity::mapi_store_id(0x58);
        crate::mapi::identity::remember_mapi_identity(shadow_id, shadow_folder_id);
        crate::mapi::identity::remember_mapi_identity(
            suggested_shadow_id,
            suggested_shadow_folder_id,
        );
        crate::mapi::identity::remember_mapi_identity(
            quick_contacts_shadow_id,
            quick_contacts_shadow_folder_id,
        );
        crate::mapi::identity::remember_mapi_identity(
            im_contacts_shadow_id,
            im_contacts_shadow_folder_id,
        );
        crate::mapi::identity::remember_mapi_identity(tasks_shadow_id, tasks_shadow_folder_id);
        crate::mapi::identity::remember_mapi_identity(
            quick_step_shadow_id,
            quick_step_shadow_folder_id,
        );
        let mailboxes = vec![
            JmapMailbox {
                id: shadow_id,
                parent_id: None,
                role: String::new(),
                name: "Calendar".to_string(),
                sort_order: 0,
                modseq: 1,
                total_emails: 0,
                unread_emails: 0,
                is_subscribed: true,
            },
            JmapMailbox {
                id: suggested_shadow_id,
                parent_id: None,
                role: String::new(),
                name: "Suggested Contacts".to_string(),
                sort_order: 0,
                modseq: 1,
                total_emails: 0,
                unread_emails: 0,
                is_subscribed: true,
            },
            JmapMailbox {
                id: quick_contacts_shadow_id,
                parent_id: None,
                role: "contacts".to_string(),
                name: "Quick Contacts".to_string(),
                sort_order: 0,
                modseq: 1,
                total_emails: 0,
                unread_emails: 0,
                is_subscribed: true,
            },
            JmapMailbox {
                id: im_contacts_shadow_id,
                parent_id: None,
                role: "contacts".to_string(),
                name: "IM Contact List".to_string(),
                sort_order: 0,
                modseq: 1,
                total_emails: 0,
                unread_emails: 0,
                is_subscribed: true,
            },
            JmapMailbox {
                id: tasks_shadow_id,
                parent_id: None,
                role: "tasks".to_string(),
                name: "Tasks".to_string(),
                sort_order: 0,
                modseq: 1,
                total_emails: 0,
                unread_emails: 0,
                is_subscribed: true,
            },
            JmapMailbox {
                id: quick_step_shadow_id,
                parent_id: None,
                role: String::new(),
                name: "Quick Step Settings".to_string(),
                sort_order: 0,
                modseq: 1,
                total_emails: 0,
                unread_emails: 0,
                is_subscribed: true,
            },
        ];
        let task_collection = CollaborationCollection {
            id: "default".to_string(),
            kind: "tasks".to_string(),
            owner_account_id: Uuid::nil(),
            owner_email: "test@example.test".to_string(),
            owner_display_name: "Test".to_string(),
            display_name: "Tasks".to_string(),
            is_owned: true,
            rights: CollaborationRights {
                may_read: true,
                may_write: true,
                may_delete: true,
                may_share: true,
            },
        };
        let snapshot = MapiMailStoreSnapshot::new(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec![task_collection],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );

        let rows = hierarchy_rows(
            IPM_SUBTREE_FOLDER_ID,
            &mailboxes,
            &snapshot,
            None,
            &[],
            Uuid::nil(),
        );
        let row_ids = rows.iter().map(hierarchy_row_id).collect::<Vec<_>>();
        assert!(row_ids.contains(&CALENDAR_FOLDER_ID));
        assert!(row_ids.contains(&SUGGESTED_CONTACTS_FOLDER_ID));
        assert!(row_ids.contains(&QUICK_CONTACTS_FOLDER_ID));
        assert!(row_ids.contains(&IM_CONTACT_LIST_FOLDER_ID));
        assert!(row_ids.contains(&TASKS_FOLDER_ID));
        assert!(!row_ids.contains(&CONVERSATION_ACTION_SETTINGS_FOLDER_ID));
        assert!(!row_ids.contains(&QUICK_STEP_SETTINGS_FOLDER_ID));
        assert!(!row_ids.contains(&shadow_folder_id));
        assert!(!row_ids.contains(&suggested_shadow_folder_id));
        assert!(!row_ids.contains(&quick_contacts_shadow_folder_id));
        assert!(!row_ids.contains(&im_contacts_shadow_folder_id));
        assert!(!row_ids.contains(&tasks_shadow_folder_id));
        assert!(!row_ids.contains(&quick_step_shadow_folder_id));
        assert_eq!(
            rows.iter()
                .filter(|row| hierarchy_row_display_name(row) == "Tasks")
                .count(),
            1
        );

        let sync_ids = sync_mailboxes_for(IPM_SUBTREE_FOLDER_ID, 0x02, &mailboxes)
            .iter()
            .map(mapi_folder_id)
            .collect::<Vec<_>>();
        assert!(sync_ids.contains(&CALENDAR_FOLDER_ID));
        assert!(sync_ids.contains(&SUGGESTED_CONTACTS_FOLDER_ID));
        assert!(sync_ids.contains(&QUICK_CONTACTS_FOLDER_ID));
        assert!(sync_ids.contains(&IM_CONTACT_LIST_FOLDER_ID));
        assert!(sync_ids.contains(&TASKS_FOLDER_ID));
        assert!(!sync_ids.contains(&CONVERSATION_ACTION_SETTINGS_FOLDER_ID));
        assert!(!sync_ids.contains(&QUICK_STEP_SETTINGS_FOLDER_ID));
        assert!(!sync_ids.contains(&shadow_folder_id));
        assert!(!sync_ids.contains(&suggested_shadow_folder_id));
        assert!(!sync_ids.contains(&quick_contacts_shadow_folder_id));
        assert!(!sync_ids.contains(&im_contacts_shadow_folder_id));
        assert!(!sync_ids.contains(&tasks_shadow_folder_id));
        assert!(!sync_ids.contains(&quick_step_shadow_folder_id));

        let calendar_row = rows
            .iter()
            .find(|row| hierarchy_row_id(row) == CALENDAR_FOLDER_ID)
            .expect("calendar special folder row");
        let serialized = serialize_hierarchy_row(
            *calendar_row,
            &mailboxes,
            &[PID_TAG_CONTAINER_CLASS_W],
            Uuid::nil(),
        );
        let class = "IPF.Appointment"
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect::<Vec<_>>();
        assert!(serialized
            .windows(class.len())
            .any(|window| window == class));

        for (folder_id, expected) in [
            (QUICK_CONTACTS_FOLDER_ID, "IPF.Contact.MOC.QuickContacts"),
            (IM_CONTACT_LIST_FOLDER_ID, "IPF.Contact.MOC.ImContactList"),
            (TASKS_FOLDER_ID, "IPF.Task"),
        ] {
            let row = rows
                .iter()
                .find(|row| hierarchy_row_id(row) == folder_id)
                .expect("special folder row");
            let serialized = serialize_hierarchy_row(
                *row,
                &mailboxes,
                &[PID_TAG_CONTAINER_CLASS_W],
                Uuid::nil(),
            );
            let class = expected
                .encode_utf16()
                .flat_map(u16::to_le_bytes)
                .collect::<Vec<_>>();
            assert!(serialized
                .windows(class.len())
                .any(|window| window == class));
        }

        assert_eq!(
            folder_message_class(
                &sync_mailboxes_for(IPM_SUBTREE_FOLDER_ID, 0x02, &[])
                    .into_iter()
                    .find(|mailbox| mapi_folder_id(mailbox) == QUICK_CONTACTS_FOLDER_ID)
                    .expect("quick contacts virtual mailbox")
            ),
            "IPF.Contact.MOC.QuickContacts"
        );
    }

    #[test]
    fn rule_table_projects_canonical_sieve_rule() {
        let rule_id = Uuid::parse_str("aaaaaaaa-4444-4111-8111-aaaaaaaaaaaa").unwrap();
        let object_id = crate::mapi::identity::mapi_store_id(125);
        crate::mapi::identity::remember_mapi_identity(rule_id, object_id);
        let snapshot = MapiMailStoreSnapshot::new(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .with_rules(vec![MailboxRule {
            id: rule_id,
            name: "Reports".to_string(),
            is_active: true,
            source_kind: "sieve_script".to_string(),
            condition_summary: "header Subject contains report".to_string(),
            action_summary: "fileinto Reports".to_string(),
            supported_outlook_projection: true,
            unsupported_exchange_features: vec!["deferred_action_messages".to_string()],
            size_octets: 128,
            updated_at: "2026-05-28T08:00:00Z".to_string(),
        }]);

        let row = serialize_rule_row(
            &snapshot.rules()[0],
            &[
                PID_TAG_RULE_ID,
                PID_TAG_RULE_STATE,
                PID_TAG_RULE_PROVIDER,
                PID_TAG_RULE_NAME,
                PID_TAG_RULE_PROVIDER_DATA,
            ],
        );
        assert_eq!(u64::from_le_bytes(row[0..8].try_into().unwrap()), object_id);
        assert_eq!(
            u32::from_le_bytes(row[8..12].try_into().unwrap()),
            ST_ENABLED
        );
        let provider = "LPE Sieve"
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect::<Vec<_>>();
        let name = "Reports"
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect::<Vec<_>>();
        assert!(row.windows(provider.len()).any(|window| window == provider));
        assert!(row.windows(name.len()).any(|window| window == name));
        assert!(String::from_utf8_lossy(&row).contains("fileinto Reports"));
    }

    #[test]
    fn common_views_associated_contents_do_not_project_lpe_synthetic_fai() {
        let definition_id = Uuid::parse_str("aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa").unwrap();
        crate::mapi::identity::remember_mapi_identity(
            definition_id,
            crate::mapi::identity::mapi_store_id(123),
        );
        let snapshot = MapiMailStoreSnapshot::new(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .with_search_folder_definitions(vec![SearchFolderDefinition {
            id: definition_id,
            account_id: Uuid::nil(),
            role: "reminders".to_string(),
            display_name: "Reminders".to_string(),
            definition_kind: "exchange_builtin".to_string(),
            result_object_kind: "mixed".to_string(),
            scope_json: serde_json::json!({"scope": "top_of_personal_folders"}),
            restriction_json: serde_json::json!({"kind": "exchange_reminders"}),
            excluded_folder_roles: exchange_builtin_excluded_folder_roles(),
            is_builtin: true,
        }]);
        let mut table = MapiObject::ContentsTable {
            folder_id: COMMON_VIEWS_FOLDER_ID,
            associated: true,
            columns: vec![
                PID_TAG_MID,
                PID_TAG_ASSOCIATED,
                PID_TAG_MESSAGE_CLASS_W,
                0x6845_0102,
            ],
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        };
        let request = RopRequest {
            rop_id: 0x15,
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: vec![0, 1, 1, 0],
        };

        assert_eq!(
            associated_folder_message_count(COMMON_VIEWS_FOLDER_ID, &snapshot),
            2
        );
        let response =
            rop_query_rows_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil());

        assert_eq!(response[0], 0x15);
        assert_eq!(u16::from_le_bytes(response[7..9].try_into().unwrap()), 1);
        let mut shortcut_class = Vec::new();
        for code_unit in "IPM.Microsoft.WunderBar.Link".encode_utf16() {
            shortcut_class.extend_from_slice(&code_unit.to_le_bytes());
        }
        let mut search_class = Vec::new();
        for code_unit in "IPM.Microsoft.WunderBar.SFInfo".encode_utf16() {
            search_class.extend_from_slice(&code_unit.to_le_bytes());
        }
        assert!(response
            .windows(shortcut_class.len())
            .any(|window| window == shortcut_class.as_slice()));
        assert!(!response
            .windows(search_class.len())
            .any(|window| window == search_class.as_slice()));
    }

    #[test]
    fn common_views_default_columns_are_navigation_shortcut_columns() {
        let columns = default_navigation_shortcut_property_tags();

        assert!(columns.contains(&PID_TAG_WLINK_ENTRY_ID));
        assert!(columns.contains(&PID_TAG_WLINK_FOLDER_TYPE));
        assert!(!columns.contains(&0x6842_0003));
        assert!(!columns.contains(&0x6845_0102));
    }

    #[test]
    fn common_views_find_row_honors_restriction() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let shortcut_id = Uuid::from_u128(0x6d617069_776c_496e_8000_000000000002);
        crate::mapi::identity::remember_mapi_identity(
            shortcut_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 102,
            ),
        );
        let snapshot = MapiMailStoreSnapshot::empty().with_navigation_shortcuts(vec![
            crate::store::MapiNavigationShortcutRecord {
                id: shortcut_id,
                account_id,
                subject: "Inbox".to_string(),
                target_folder_id: Some(INBOX_FOLDER_ID),
                shortcut_type: 0,
                flags: 0,
                section: 0,
                ordinal: 0x81,
                group_header_id: Some(default_wlink_group_uuid()),
                group_name: "Mail".to_string(),
            },
        ]);
        let mut table = MapiObject::ContentsTable {
            folder_id: COMMON_VIEWS_FOLDER_ID,
            associated: true,
            columns: vec![PID_TAG_SUBJECT_W],
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        };
        let mut restriction = vec![MapiRestrictionType::Property as u8, 0x04];
        restriction.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
        restriction.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
        write_utf16z(&mut restriction, "Archive");
        let mut payload = vec![0];
        payload.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
        payload.extend_from_slice(&restriction);
        payload.push(1);
        payload.extend_from_slice(&0u16.to_le_bytes());
        let request = RopRequest {
            rop_id: RopId::FindRow.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload,
        };

        let response =
            rop_find_row_response(&request, Some(&mut table), &[], &[], &snapshot, account_id);

        assert_eq!(response[0], RopId::FindRow.as_u8());
        assert_eq!(
            u32::from_le_bytes(response[2..6].try_into().unwrap()),
            0x8004_010F
        );
        assert_eq!(table_position(&table), Some(0));
    }

    #[test]
    fn common_views_query_rows_uses_account_bound_wlink_entry_ids() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let snapshot = common_views_sort_snapshot(account_id);
        let mut table = MapiObject::ContentsTable {
            folder_id: COMMON_VIEWS_FOLDER_ID,
            associated: true,
            columns: vec![PID_TAG_WLINK_ENTRY_ID],
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        };
        let request = RopRequest {
            rop_id: RopId::QueryRows.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: vec![0, 1, 10, 0],
        };

        let response =
            rop_query_rows_response(&request, Some(&mut table), &[], &[], &snapshot, account_id);

        let expected =
            crate::mapi::identity::folder_entry_id_from_object_id(account_id, INBOX_FOLDER_ID)
                .unwrap();
        let zero_guid_entry_id =
            crate::mapi::identity::folder_entry_id_from_object_id(Uuid::nil(), INBOX_FOLDER_ID)
                .unwrap();
        assert!(response
            .windows(expected.len())
            .any(|window| window == expected.as_slice()));
        assert!(!response
            .windows(zero_guid_entry_id.len())
            .any(|window| window == zero_guid_entry_id.as_slice()));
    }

    #[test]
    fn common_views_query_rows_uses_wlink_sort_order() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let snapshot = common_views_sort_snapshot(account_id);
        let mut table = MapiObject::ContentsTable {
            folder_id: COMMON_VIEWS_FOLDER_ID,
            associated: true,
            columns: vec![PID_TAG_SUBJECT_W],
            sort_orders: vec![
                MapiSortOrder {
                    property_tag: 0x684F_0102,
                    order: 0,
                },
                MapiSortOrder {
                    property_tag: 0x6850_0102,
                    order: 0,
                },
                MapiSortOrder {
                    property_tag: 0x684B_0102,
                    order: 0,
                },
            ],
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        };
        let request = RopRequest {
            rop_id: RopId::QueryRows.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: vec![0, 1, 10, 0],
        };

        let response =
            rop_query_rows_response(&request, Some(&mut table), &[], &[], &snapshot, account_id);

        let alpha = utf16_position(&response, "Alpha").unwrap();
        let zulu = utf16_position(&response, "Zulu").unwrap();
        assert!(alpha < zulu);
    }

    #[test]
    fn inbox_associated_find_row_returns_outlook_eas_config() {
        assert_inbox_associated_find_row_returns_message_class("IPM.Configuration.EAS");
    }

    #[test]
    fn inbox_associated_find_row_returns_outlook_elc_config() {
        assert_inbox_associated_find_row_returns_message_class("IPM.Configuration.ELC");
    }

    #[test]
    fn inbox_associated_find_row_uses_sort_order() {
        let snapshot = inbox_associated_sort_snapshot();
        let mut table = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: true,
            columns: vec![PID_TAG_MESSAGE_CLASS_W],
            sort_orders: vec![MapiSortOrder {
                property_tag: PID_TAG_MESSAGE_CLASS_W,
                order: 0,
            }],
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        };
        let mut restriction = vec![MapiRestrictionType::Property as u8, 0x02];
        restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
        restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
        write_utf16z(&mut restriction, "IPM.Configuration.");
        let mut payload = vec![0];
        payload.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
        payload.extend_from_slice(&restriction);
        payload.push(1);
        payload.extend_from_slice(&0u16.to_le_bytes());
        let request = RopRequest {
            rop_id: RopId::FindRow.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload,
        };

        let response =
            rop_find_row_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil());

        assert_eq!(response[0], RopId::FindRow.as_u8());
        assert_eq!(response[7], 1);
        assert_eq!(response[8], 0);
        assert_response_contains_utf16(&response, "IPM.Configuration.AccountPrefs");
    }

    #[test]
    fn inbox_associated_query_rows_uses_sort_order() {
        let snapshot = inbox_associated_sort_snapshot();
        let mut table = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: true,
            columns: vec![PID_TAG_MESSAGE_CLASS_W],
            sort_orders: vec![MapiSortOrder {
                property_tag: PID_TAG_MESSAGE_CLASS_W,
                order: 0,
            }],
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        };
        let request = RopRequest {
            rop_id: RopId::QueryRows.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: vec![0, 1, 50, 0],
        };

        let response =
            rop_query_rows_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil());

        assert_eq!(response[0], RopId::QueryRows.as_u8());
        assert_eq!(response[9], 1);
        assert_eq!(response[10], 0);
        let account_prefs = utf16_position(&response, "IPM.Configuration.AccountPrefs").unwrap();
        let eas = utf16_position(&response, "IPM.Configuration.EAS").unwrap();
        let elc = utf16_position(&response, "IPM.Configuration.ELC").unwrap();
        let message_list =
            utf16_position(&response, "IPM.Configuration.MessageListSettings").unwrap();
        assert!(account_prefs < eas);
        assert!(eas < elc);
        assert!(elc < message_list);
    }

    #[test]
    fn inbox_associated_query_rows_uses_flagged_property_rows() {
        let snapshot = inbox_associated_sort_snapshot();
        let columns = vec![
            PID_TAG_FOLDER_ID,
            PID_TAG_MID,
            PID_TAG_INST_ID,
            PID_TAG_INSTANCE_NUM,
            PID_TAG_ROAMING_DATATYPES,
            PID_TAG_MESSAGE_CLASS_W,
            0x685D_0003,
            PID_TAG_LAST_MODIFICATION_TIME,
        ];
        let mut table = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: true,
            columns: columns.clone(),
            sort_orders: vec![
                MapiSortOrder {
                    property_tag: PID_TAG_MESSAGE_CLASS_W,
                    order: 0,
                },
                MapiSortOrder {
                    property_tag: PID_TAG_LAST_MODIFICATION_TIME,
                    order: 0,
                },
            ],
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        };
        let request = RopRequest {
            rop_id: RopId::QueryRows.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: vec![0, 1, 1, 0],
        };

        let response =
            rop_query_rows_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil());

        assert_eq!(response[0], RopId::QueryRows.as_u8());
        assert_eq!(u16::from_le_bytes([response[7], response[8]]), 1);
        let mut cursor = Cursor::new(&response[9..]);
        assert_eq!(cursor.read_u8().unwrap(), 1);
        for column in columns {
            assert_eq!(cursor.read_u8().unwrap(), 0);
            parse_mapi_property_value(&mut cursor, column).unwrap();
        }
        assert!(cursor.remaining_is_zero_padding());
    }

    #[test]
    fn inbox_associated_rows_project_folder_id_and_last_modification_time() {
        let message = MapiAssociatedConfigMessage {
            id: crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 91,
            ),
            folder_id: INBOX_FOLDER_ID,
            canonical_id: Uuid::nil(),
            message_class: "IPM.Configuration.MessageListSettings".to_string(),
            subject: "Message list settings".to_string(),
            properties_json: serde_json::json!({}),
        };
        let change_number = mapi_mailstore::change_number_for_store_id(message.id);

        assert_eq!(
            associated_config_property_value(&message, PID_TAG_FOLDER_ID),
            Some(MapiValue::U64(INBOX_FOLDER_ID))
        );
        assert_eq!(
            associated_config_property_value(&message, PID_TAG_INST_ID),
            Some(MapiValue::U64(message.id))
        );
        assert_eq!(
            associated_config_property_value(&message, PID_TAG_INSTANCE_NUM),
            Some(MapiValue::U32(0))
        );
        let entry_id = crate::mapi::identity::message_entry_id_from_object_ids(
            Uuid::nil(),
            INBOX_FOLDER_ID,
            message.id,
        )
        .unwrap();
        assert_eq!(
            associated_config_property_value(&message, PID_TAG_ENTRY_ID),
            Some(MapiValue::Binary(entry_id))
        );
        let source_key = mapi_mailstore::source_key_for_store_id(message.id);
        assert_eq!(
            associated_config_property_value(&message, PID_TAG_SOURCE_KEY),
            Some(MapiValue::Binary(source_key.clone()))
        );
        assert_eq!(
            associated_config_property_value(&message, PID_TAG_RECORD_KEY),
            Some(MapiValue::Binary(source_key))
        );
        assert_eq!(
            associated_config_property_value(&message, PID_TAG_LAST_MODIFICATION_TIME),
            Some(MapiValue::I64(
                mapi_mailstore::filetime_from_change_number(change_number) as i64
            ))
        );
        assert_eq!(
            associated_config_property_value(&message, PID_TAG_ROAMING_DATATYPES),
            Some(MapiValue::U32(4))
        );
        assert!(matches!(
            associated_config_property_value(&message, PID_TAG_ROAMING_DICTIONARY),
            Some(MapiValue::Binary(value)) if value.starts_with(br#"<?xml version="1.0" encoding="utf-8"?>"#)
        ));
        assert!(matches!(
            associated_config_property_value(&message, 0x685D_0003),
            Some(MapiValue::U32(value)) if value != 0
        ));
        let explicit_marker = MapiAssociatedConfigMessage {
            properties_json: serde_json::json!({
                "0x685d0003": {"type": "u32", "value": 42}
            }),
            ..message.clone()
        };
        assert_eq!(
            associated_config_property_value(&explicit_marker, 0x685D_0003),
            Some(MapiValue::U32(42))
        );

        let row = serialize_associated_config_row(
            &message,
            &[
                PID_TAG_FOLDER_ID,
                PID_TAG_MID,
                PID_TAG_INST_ID,
                PID_TAG_INSTANCE_NUM,
                PID_TAG_ROAMING_DATATYPES,
                0x685D_0003,
                PID_TAG_LAST_MODIFICATION_TIME,
            ],
        );

        assert_eq!(row.len(), 44);
    }

    fn assert_inbox_associated_find_row_returns_message_class(message_class: &str) {
        let snapshot = MapiMailStoreSnapshot::empty();
        let mut table = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: true,
            columns: vec![PID_TAG_MESSAGE_CLASS_W],
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        };
        let mut restriction = vec![MapiRestrictionType::Property as u8, 0x04];
        restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
        restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
        write_utf16z(&mut restriction, message_class);
        let mut payload = vec![0];
        payload.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
        payload.extend_from_slice(&restriction);
        payload.push(1);
        payload.extend_from_slice(&0u16.to_le_bytes());
        let request = RopRequest {
            rop_id: RopId::FindRow.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload,
        };

        let response =
            rop_find_row_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil());

        assert_eq!(response[0], RopId::FindRow.as_u8());
        assert_eq!(response[7], 1);
        let mut encoded_message_class = Vec::new();
        write_utf16z(&mut encoded_message_class, message_class);
        assert!(response
            .windows(encoded_message_class.len())
            .any(|window| window == encoded_message_class.as_slice()));
    }

    fn inbox_associated_sort_snapshot() -> MapiMailStoreSnapshot {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let persisted_id = Uuid::from_u128(0x6d617069_6163_6350_8000_000000000001);
        crate::mapi::identity::remember_mapi_identity(
            persisted_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 81,
            ),
        );
        MapiMailStoreSnapshot::empty().with_associated_configs(vec![
            crate::store::MapiAssociatedConfigRecord {
                id: persisted_id,
                account_id,
                folder_id: INBOX_FOLDER_ID,
                message_class: "IPM.Configuration.AccountPrefs".to_string(),
                subject: "Account prefs".to_string(),
                properties_json: serde_json::json!({}),
            },
        ])
    }

    fn common_views_sort_snapshot(account_id: Uuid) -> MapiMailStoreSnapshot {
        let zulu_id = Uuid::from_u128(0x6d617069_776c_5a75_8000_000000000001);
        let alpha_id = Uuid::from_u128(0x6d617069_776c_416c_8000_000000000001);
        crate::mapi::identity::remember_mapi_identity(
            zulu_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 111,
            ),
        );
        crate::mapi::identity::remember_mapi_identity(
            alpha_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 112,
            ),
        );
        let group_header_id = Some(default_wlink_group_uuid());
        MapiMailStoreSnapshot::empty().with_navigation_shortcuts(vec![
            crate::store::MapiNavigationShortcutRecord {
                id: zulu_id,
                account_id,
                subject: "Zulu".to_string(),
                target_folder_id: Some(SENT_FOLDER_ID),
                shortcut_type: 0,
                flags: 0,
                section: 1,
                ordinal: 0x20,
                group_header_id,
                group_name: "Mail".to_string(),
            },
            crate::store::MapiNavigationShortcutRecord {
                id: alpha_id,
                account_id,
                subject: "Alpha".to_string(),
                target_folder_id: Some(INBOX_FOLDER_ID),
                shortcut_type: 0,
                flags: 0,
                section: 1,
                ordinal: 0x10,
                group_header_id,
                group_name: "Mail".to_string(),
            },
        ])
    }

    fn assert_response_contains_utf16(response: &[u8], value: &str) {
        assert!(
            utf16_position(response, value).is_some(),
            "response did not contain {value}"
        );
    }

    fn utf16_position(response: &[u8], value: &str) -> Option<usize> {
        let mut encoded = Vec::new();
        write_utf16z(&mut encoded, value);
        response
            .windows(encoded.len())
            .position(|window| window == encoded.as_slice())
    }

    #[test]
    fn access_rows_follow_microsoft_flags() {
        let mailbox = JmapMailbox {
            id: Uuid::nil(),
            parent_id: None,
            role: "inbox".to_string(),
            name: "Inbox".to_string(),
            sort_order: 0,
            modseq: 1,
            total_emails: 0,
            unread_emails: 0,
            is_subscribed: true,
        };

        let mailbox_row =
            serialize_folder_row_with_context(&mailbox, &[], &[PID_TAG_ACCESS], Uuid::nil());
        assert_eq!(
            u32::from_le_bytes(mailbox_row.try_into().unwrap()),
            MAPI_FOLDER_ACCESS
        );

        let root_row = serialize_special_folder_row(ROOT_FOLDER_ID, &[], &[PID_TAG_ACCESS], None);
        assert_eq!(
            u32::from_le_bytes(root_row.try_into().unwrap()),
            MAPI_FOLDER_ACCESS
        );
    }

    #[test]
    fn reminders_folder_projects_reminder_container_class() {
        let row = serialize_special_folder_row(
            REMINDERS_FOLDER_ID,
            &[],
            &[PID_TAG_CONTAINER_CLASS_W, PID_TAG_MESSAGE_CLASS_W],
            None,
        );
        let expected = utf16z_test_bytes("Outlook.Reminder");

        assert_eq!(&row[..expected.len()], expected.as_slice());
        assert_eq!(&row[expected.len()..], expected.as_slice());
    }

    #[test]
    fn special_folder_property_projects_record_key() {
        assert_eq!(
            special_folder_property_value(INBOX_FOLDER_ID, PID_TAG_RECORD_KEY, Uuid::nil()),
            Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
                INBOX_FOLDER_ID
            )))
        );
    }

    #[test]
    fn ms_oxosfld_none_container_classes_serialize_as_empty_strings() {
        for folder_id in [
            ROOT_FOLDER_ID,
            DEFERRED_ACTION_FOLDER_ID,
            SPOOLER_QUEUE_FOLDER_ID,
            COMMON_VIEWS_FOLDER_ID,
            VIEWS_FOLDER_ID,
            FREEBUSY_DATA_FOLDER_ID,
        ] {
            let row =
                serialize_special_folder_row(folder_id, &[], &[PID_TAG_CONTAINER_CLASS_W], None);
            assert_eq!(row, utf16z_test_bytes(""));
        }
    }

    #[test]
    fn attachment_rows_use_by_value_method() {
        let attachment = MapiAttachment {
            attach_num: 0,
            canonical_id: Uuid::nil(),
            file_reference: "file-ref".to_string(),
            file_name: "report.pdf".to_string(),
            media_type: "application/pdf".to_string(),
            size_octets: 16,
        };

        let row = serialize_attachment_row(&attachment, &[PID_TAG_ATTACH_METHOD]);
        assert_eq!(u32::from_le_bytes(row.try_into().unwrap()), ATTACH_BY_VALUE);
    }

    fn utf16z_test_bytes(value: &str) -> Vec<u8> {
        value
            .encode_utf16()
            .chain(std::iter::once(0))
            .flat_map(u16::to_le_bytes)
            .collect()
    }
}

pub(in crate::mapi) fn write_logon_property_row(
    response: &mut Vec<u8>,
    principal: &AccountPrincipal,
    columns: &[u32],
) {
    if columns
        .iter()
        .all(|column| logon_property_value(principal, *column).is_some())
    {
        write_standard_property_row(response, &serialize_logon_row(principal, columns));
        return;
    }

    response.push(1);
    for column in columns {
        match logon_property_value(principal, *column) {
            Some(value) => {
                response.push(0);
                write_mapi_value(response, *column, &value);
            }
            None => {
                response.push(0x0A);
                write_u32(response, ROP_ERROR_NOT_SUPPORTED);
            }
        }
    }
}

pub(in crate::mapi) fn serialize_logon_row(
    principal: &AccountPrincipal,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match logon_property_value(principal, *column) {
            Some(value) => write_mapi_value(&mut row, *column, &value),
            None => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(in crate::mapi) fn serialized_replid_guid_map() -> Vec<u8> {
    let mut value = Vec::with_capacity(18);
    value.extend_from_slice(&(STORE_REPLICA_ID as u16).to_le_bytes());
    value.extend_from_slice(&crate::mapi::identity::STORE_REPLICA_GUID);
    value
}

pub(in crate::mapi) fn serialize_pending_attachment_row(
    attach_num: u32,
    properties: &HashMap<u32, MapiValue>,
    data: &[u8],
    columns: &[u32],
) -> Vec<u8> {
    let file_name = pending_attachment_file_name(attach_num, properties);
    let media_type = pending_attachment_media_type(properties);
    let size = data.len().min(u32::MAX as usize) as u32;
    let mut row = Vec::new();
    for column in columns {
        if let Some(value) = properties.get(column) {
            write_mapi_value(&mut row, *column, value);
            continue;
        }
        match *column {
            PID_TAG_ATTACH_NUM => write_u32(&mut row, attach_num),
            PID_TAG_ATTACH_FILENAME_W | PID_TAG_ATTACH_LONG_FILENAME_W => {
                write_utf16z(&mut row, &file_name)
            }
            PID_TAG_ATTACH_MIME_TAG_W => write_utf16z(&mut row, &media_type),
            PID_TAG_ATTACH_SIZE => write_u32(&mut row, size),
            PID_TAG_ATTACH_METHOD => write_u32(&mut row, ATTACH_BY_VALUE),
            PID_TAG_RENDERING_POSITION => write_u32(&mut row, u32::MAX),
            PID_TAG_ATTACH_DATA_BINARY => write_u16_prefixed_bytes(&mut row, data),
            _ => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(in crate::mapi) fn serialize_saved_attachment_row(
    attach_num: u32,
    file_reference: &str,
    file_name: &str,
    media_type: &str,
    size_octets: u64,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match *column {
            PID_TAG_ATTACH_NUM => write_u32(&mut row, attach_num),
            PID_TAG_ATTACH_FILENAME_W | PID_TAG_ATTACH_LONG_FILENAME_W => {
                write_utf16z(&mut row, file_name)
            }
            PID_TAG_ATTACH_MIME_TAG_W => write_utf16z(&mut row, media_type),
            PID_TAG_ATTACH_SIZE => write_u32(&mut row, size_octets.min(u32::MAX as u64) as u32),
            PID_TAG_ATTACH_METHOD => write_u32(&mut row, ATTACH_BY_VALUE),
            PID_TAG_RENDERING_POSITION => write_u32(&mut row, u32::MAX),
            PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => {
                write_u16_prefixed_bytes(&mut row, file_reference.as_bytes())
            }
            _ => write_property_default(&mut row, *column),
        }
    }
    row
}

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

pub(in crate::mapi) fn serialize_collaboration_folder_row(
    folder: &MapiCollaborationFolder,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match *column {
            PID_TAG_DISPLAY_NAME_W => write_utf16z(&mut row, &folder.collection.display_name),
            PID_TAG_FOLDER_ID => write_object_id(&mut row, folder.id),
            PID_TAG_PARENT_FOLDER_ID => write_object_id(&mut row, IPM_SUBTREE_FOLDER_ID),
            PID_TAG_CONTENT_COUNT => write_u32(&mut row, folder.item_count),
            PID_TAG_CONTENT_UNREAD_COUNT => write_u32(&mut row, 0),
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
    let mut row = Vec::new();
    for column in columns {
        match *column {
            PID_TAG_MID => write_object_id(&mut row, mapi_message_id(email)),
            PID_TAG_INST_ID => write_u64(&mut row, mapi_message_id(email)),
            PID_TAG_INSTANCE_NUM => write_u32(&mut row, 0),
            PID_TAG_ROW_TYPE => write_u32(&mut row, 0),
            PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
                write_utf16z(&mut row, &email.subject)
            }
            PID_TAG_MESSAGE_CLASS_W => write_utf16z(&mut row, "IPM.Note"),
            PID_TAG_MESSAGE_DELIVERY_TIME
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
            PID_TAG_MESSAGE_FLAGS => write_u32(&mut row, message_flags(email)),
            PID_TAG_READ => row.push((!email.unread) as u8),
            PID_TAG_MESSAGE_SIZE => {
                write_u32(&mut row, email.size_octets.clamp(0, u32::MAX as i64) as u32)
            }
            PID_TAG_SENDER_NAME_W => write_utf16z(
                &mut row,
                email.from_display.as_deref().unwrap_or(&email.from_address),
            ),
            PID_TAG_SENDER_EMAIL_ADDRESS_W => write_utf16z(&mut row, &email.from_address),
            PID_TAG_DISPLAY_TO_W => write_utf16z(&mut row, &display_to(email)),
            PID_TAG_DISPLAY_CC_W => write_utf16z(&mut row, &display_cc(email)),
            PID_TAG_HAS_ATTACHMENTS => row.push(email.has_attachments as u8),
            PID_TAG_BODY_W => write_utf16z(&mut row, &email.body_text),
            PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &crate::mapi::identity::instance_key_for_object_id(mapi_message_id(email)),
            ),
            PID_TAG_INTERNET_MESSAGE_ID_W => {
                write_utf16z(&mut row, email.internet_message_id.as_deref().unwrap_or(""))
            }
            _ => match email_property_value(email, *column) {
                Some(value) => write_mapi_value(&mut row, *column, &value),
                None => write_property_default(&mut row, *column),
            },
        }
    }
    row
}

pub(in crate::mapi) fn serialize_public_folder_item_row(
    item: &MapiPublicFolderItem,
    columns: &[u32],
) -> Vec<u8> {
    let change_number = mapi_mailstore::change_number_for_store_id(item.id);
    let message_class = if item.item.message_class.trim().is_empty() {
        "IPM.Post"
    } else {
        item.item.message_class.as_str()
    };
    let body_text = item.item.body_text.as_str();
    let mut row = Vec::new();
    for column in columns {
        match canonical_property_storage_tag(*column) {
            PID_TAG_MID => write_object_id(&mut row, item.id),
            PID_TAG_INST_ID => write_u64(&mut row, item.id),
            PID_TAG_INSTANCE_NUM => write_u32(&mut row, 0),
            PID_TAG_ROW_TYPE => write_u32(&mut row, 0),
            PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
                write_utf16z(&mut row, &item.item.subject)
            }
            PID_TAG_MESSAGE_CLASS_W => write_utf16z(&mut row, message_class),
            PID_TAG_ACCESS => write_u32(&mut row, MAPI_MESSAGE_ACCESS),
            PID_TAG_MESSAGE_FLAGS => write_u32(&mut row, 0),
            PID_TAG_READ => row.push(item.item.is_read as u8),
            PID_TAG_MESSAGE_SIZE => write_u32(
                &mut row,
                body_text
                    .len()
                    .saturating_add(item.item.subject.len())
                    .min(u32::MAX as usize) as u32,
            ),
            PID_TAG_HAS_ATTACHMENTS => row.push(0),
            PID_TAG_BODY_W => write_utf16z(&mut row, body_text),
            PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &crate::mapi::identity::instance_key_for_object_id(item.id),
            ),
            PID_TAG_PARENT_SOURCE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::source_key_for_store_id(item.folder_id),
            ),
            PID_TAG_SOURCE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::source_key_for_store_id(item.id),
            ),
            PID_TAG_CHANGE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::change_key_for_change_number(change_number),
            ),
            PID_TAG_PREDECESSOR_CHANGE_LIST => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::predecessor_change_list(change_number),
            ),
            PID_TAG_CHANGE_NUMBER => write_u64(&mut row, change_number),
            _ => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(in crate::mapi) fn serialize_recoverable_item_row(
    item: &crate::mapi_store::MapiRecoverableItemMessage,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match canonical_property_storage_tag(*column) {
            PID_TAG_MID => write_object_id(&mut row, item.id),
            PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
                write_utf16z(&mut row, &item.item.subject)
            }
            PID_TAG_MESSAGE_CLASS_W => write_utf16z(&mut row, "IPM.Note"),
            PID_TAG_MESSAGE_DELIVERY_TIME
            | PID_TAG_LAST_MODIFICATION_TIME
            | PID_TAG_LOCAL_COMMIT_TIME => write_u64(
                &mut row,
                mapi_mailstore::filetime_from_rfc3339_utc(&item.item.received_at),
            ),
            PID_TAG_ACCESS => write_u32(&mut row, MAPI_MESSAGE_ACCESS),
            PID_TAG_MESSAGE_FLAGS => write_u32(&mut row, MSGFLAG_READ),
            PID_TAG_READ => row.push(1),
            PID_TAG_MESSAGE_SIZE => write_u32(
                &mut row,
                item.item.size_octets.clamp(0, u32::MAX as i64) as u32,
            ),
            PID_TAG_SENDER_NAME_W | PID_TAG_SENDER_EMAIL_ADDRESS_W => {
                write_utf16z(&mut row, &item.item.sender_address)
            }
            PID_TAG_DISPLAY_TO_W | PID_TAG_DISPLAY_CC_W | PID_TAG_BODY_W => {
                write_utf16z(&mut row, "")
            }
            PID_TAG_HAS_ATTACHMENTS => row.push(item.item.has_attachments as u8),
            PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &crate::mapi::identity::instance_key_for_object_id(item.id),
            ),
            PID_TAG_SOURCE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::source_key_for_store_id(item.id),
            ),
            PID_TAG_PARENT_SOURCE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::source_key_for_store_id(item.folder_id),
            ),
            PID_TAG_CHANGE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::change_key_for_change_number(
                    mapi_mailstore::change_number_for_store_id(item.id),
                ),
            ),
            PID_TAG_PREDECESSOR_CHANGE_LIST => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::predecessor_change_list(
                    mapi_mailstore::change_number_for_store_id(item.id),
                ),
            ),
            PID_TAG_CHANGE_NUMBER => write_u64(
                &mut row,
                mapi_mailstore::change_number_for_store_id(item.id),
            ),
            _ => write_property_default(&mut row, *column),
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
    }
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

pub(in crate::mapi) fn serialize_associated_config_row(
    message: &MapiAssociatedConfigMessage,
    columns: &[u32],
) -> Vec<u8> {
    serialize_associated_config_row_with_mailbox_guid(message, Uuid::nil(), columns)
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
            PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
                Some(MapiValue::String(message.subject.clone()))
            }
            PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String(message.message_class.clone())),
            PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(0x0000_0040)),
            PID_TAG_ASSOCIATED => Some(MapiValue::Bool(true)),
            PID_TAG_MESSAGE_SIZE => Some(MapiValue::I64(
                message
                    .subject
                    .len()
                    .saturating_add(message.message_class.len())
                    .saturating_add(message.properties_json.to_string().len())
                    .min(i64::MAX as usize) as i64,
            )),
            PID_TAG_FOLDER_ID => Some(MapiValue::U64(message.folder_id)),
            PID_TAG_PARENT_FOLDER_ID => Some(MapiValue::U64(message.folder_id)),
            PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
                message.id,
            ))),
            PID_TAG_RECORD_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
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
                mapi_mailstore::filetime_from_change_number(change_number) as i64,
            )),
            PID_TAG_ROAMING_DATATYPES
                if message.message_class.starts_with("IPM.Configuration.") =>
            {
                Some(MapiValue::U32(4))
            }
            PID_TAG_ROAMING_DICTIONARY
                if message.message_class.starts_with("IPM.Configuration.") =>
            {
                Some(MapiValue::Binary(minimal_roaming_dictionary_stream()))
            }
            0x685D_0003 if message.message_class.starts_with("IPM.Configuration.") => {
                Some(MapiValue::U32(outlook_configuration_stamp(message)))
            }
            PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_MESSAGE_ACCESS)),
            _ => None,
        }
    })
}

fn minimal_roaming_dictionary_stream() -> Vec<u8> {
    br#"<?xml version="1.0" encoding="utf-8"?><UserConfiguration xmlns="dictionary.xsd"><Info version="LPE.1"/><Data/></UserConfiguration>"#.to_vec()
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
        PID_TAG_MESSAGE_SIZE => Some(MapiValue::I64(
            message
                .message
                .subject
                .len()
                .saturating_add(message.message.body_text.len())
                .saturating_add(message.message.payload_json.len())
                .min(i64::MAX as usize) as i64,
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
        PID_TAG_LOCAL_COMMIT_TIME | PID_TAG_MESSAGE_DELIVERY_TIME => Some(MapiValue::I64(
            mapi_mailstore::filetime_from_rfc3339_utc(&message.message.updated_at) as i64,
        )),
        PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_MESSAGE_ACCESS)),
        _ => None,
    }
}

pub(in crate::mapi) fn serialize_pending_navigation_shortcut_row(
    properties: &HashMap<u32, MapiValue>,
    principal: &AccountPrincipal,
    columns: &[u32],
) -> Vec<u8> {
    let shortcut = navigation_shortcut_from_mapi_properties(principal.account_id, None, properties);
    serialize_navigation_shortcut_row(&shortcut, Some(principal), columns)
}

pub(in crate::mapi) fn navigation_shortcut_from_mapi_properties(
    _account_id: Uuid,
    id: Option<Uuid>,
    properties: &HashMap<u32, MapiValue>,
) -> MapiNavigationShortcutMessage {
    let entry_target = properties
        .get(&PID_TAG_WLINK_ENTRY_ID)
        .and_then(|value| match value {
            MapiValue::Binary(bytes) => {
                crate::mapi::identity::object_id_from_folder_entry_id(bytes)
            }
            _ => None,
        });
    let subject = properties
        .get(&PID_TAG_SUBJECT_W)
        .or_else(|| properties.get(&PID_TAG_NORMALIZED_SUBJECT_W))
        .and_then(|value| match value {
            MapiValue::String(value) => Some(value.clone()),
            _ => None,
        })
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "Shortcut".to_string());
    let shortcut_id = id.unwrap_or_else(Uuid::new_v4);
    let shortcut_type = properties
        .get(&PID_TAG_WLINK_TYPE)
        .and_then(MapiValue::as_i64)
        .map(|value| value as u32)
        .unwrap_or(2);
    let group_header_id = properties
        .get(if shortcut_type == 4 {
            &PID_TAG_WLINK_GROUP_HEADER_ID
        } else {
            &PID_TAG_WLINK_GROUP_CLSID
        })
        .and_then(|value| match value {
            MapiValue::Guid(value) => Some(Uuid::from_bytes(*value)),
            _ => None,
        });
    let group_name = properties
        .get(&PID_TAG_WLINK_GROUP_NAME_W)
        .and_then(|value| match value {
            MapiValue::String(value) => Some(value.clone()),
            _ => None,
        })
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| {
            if shortcut_type == 4 {
                subject.clone()
            } else {
                "Mail".to_string()
            }
        });
    MapiNavigationShortcutMessage {
        id: crate::mapi::identity::mapped_mapi_object_id(&shortcut_id)
            .unwrap_or_else(|| crate::mapi::identity::mapi_store_id(0x7fff)),
        folder_id: COMMON_VIEWS_FOLDER_ID,
        canonical_id: shortcut_id,
        subject,
        target_folder_id: entry_target,
        shortcut_type,
        flags: properties
            .get(&PID_TAG_WLINK_FLAGS)
            .and_then(MapiValue::as_i64)
            .map(|value| value as u32)
            .unwrap_or(0),
        section: properties
            .get(&PID_TAG_WLINK_SECTION)
            .and_then(MapiValue::as_i64)
            .map(|value| value as u32)
            .unwrap_or(0),
        ordinal: properties
            .get(&PID_TAG_WLINK_ORDINAL)
            .and_then(|value| match value {
                MapiValue::Binary(bytes) => Some(
                    bytes
                        .iter()
                        .take(4)
                        .fold(0u32, |value, byte| (value << 8) | u32::from(*byte)),
                ),
                _ => None,
            })
            .or_else(|| {
                properties
                    .get(&0x684B_0003)
                    .and_then(MapiValue::as_i64)
                    .map(|value| value as u32)
            })
            .or_else(|| {
                properties
                    .get(&PID_TAG_WLINK_ORDINAL)
                    .and_then(MapiValue::as_i64)
                    .map(|value| value as u32)
            })
            .unwrap_or(0),
        group_header_id,
        group_name,
    }
}

pub(in crate::mapi) fn serialize_contact_row(
    contact: &AccessibleContact,
    item_id: u64,
    folder_id: u64,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match contact_property_value(contact, item_id, folder_id, *column) {
            Some(value) => write_mapi_value(&mut row, *column, &value),
            None => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(in crate::mapi) fn serialize_event_row(
    event: &AccessibleEvent,
    item_id: u64,
    folder_id: u64,
    columns: &[u32],
) -> Vec<u8> {
    serialize_event_row_with_attachments(event, item_id, folder_id, false, columns)
}

pub(in crate::mapi) fn serialize_event_row_with_attachments(
    event: &AccessibleEvent,
    item_id: u64,
    folder_id: u64,
    has_attachments: bool,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match if canonical_property_storage_tag(*column) == PID_TAG_HAS_ATTACHMENTS {
            Some(MapiValue::Bool(has_attachments))
        } else {
            event_property_value(event, item_id, folder_id, *column)
        } {
            Some(value) => write_mapi_value(&mut row, *column, &value),
            None => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(in crate::mapi) fn serialize_task_row(
    task: &ClientTask,
    item_id: u64,
    folder_id: u64,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match task_property_value(task, item_id, folder_id, *column) {
            Some(value) => write_mapi_value(&mut row, *column, &value),
            None => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(in crate::mapi) fn serialize_reminder_event_row(
    event: &crate::mapi_store::MapiEvent,
    reminder: Option<&lpe_storage::ClientReminder>,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match event_property_value_with_reminder(
            &event.event,
            event.id,
            REMINDERS_FOLDER_ID,
            *column,
            reminder,
        ) {
            Some(value) => write_mapi_value(&mut row, *column, &value),
            None => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(in crate::mapi) fn serialize_reminder_task_row(
    task: &crate::mapi_store::MapiTask,
    reminder: Option<&lpe_storage::ClientReminder>,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match task_property_value_with_reminder(
            &task.task,
            task.id,
            REMINDERS_FOLDER_ID,
            *column,
            reminder,
        ) {
            Some(value) => write_mapi_value(&mut row, *column, &value),
            None => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(in crate::mapi) fn serialize_pending_note_row(
    principal: &AccountPrincipal,
    properties: &HashMap<u32, MapiValue>,
    columns: &[u32],
) -> Vec<u8> {
    let note = note_input_from_mapi(
        principal.account_id,
        None,
        &default_note_for_mapping(),
        properties,
    );
    let item_id = properties
        .get(&PID_TAG_MID)
        .and_then(MapiValue::as_i64)
        .and_then(|value| u64::try_from(value).ok())
        .unwrap_or_default();
    let note = ClientNote {
        id: Uuid::nil(),
        title: note.title,
        body_text: note.body_text,
        color: note.color,
        categories_json: note.categories_json,
        created_at: "1970-01-01T00:00:00Z".to_string(),
        updated_at: "1970-01-01T00:00:00Z".to_string(),
    };
    serialize_note_row(&note, item_id, NOTES_FOLDER_ID, columns)
}

pub(in crate::mapi) fn serialize_pending_journal_entry_row(
    principal: &AccountPrincipal,
    properties: &HashMap<u32, MapiValue>,
    columns: &[u32],
) -> Vec<u8> {
    let entry = journal_entry_input_from_mapi(
        principal.account_id,
        None,
        &default_journal_entry_for_mapping(),
        properties,
    );
    let item_id = properties
        .get(&PID_TAG_MID)
        .and_then(MapiValue::as_i64)
        .and_then(|value| u64::try_from(value).ok())
        .unwrap_or_default();
    let entry = JournalEntry {
        id: Uuid::nil(),
        subject: entry.subject,
        body_text: entry.body_text,
        entry_type: entry.entry_type,
        message_class: entry.message_class,
        starts_at: entry.starts_at,
        ends_at: entry.ends_at,
        occurred_at: entry.occurred_at,
        companies_json: entry.companies_json,
        contacts_json: entry.contacts_json,
        created_at: "1970-01-01T00:00:00Z".to_string(),
        updated_at: "1970-01-01T00:00:00Z".to_string(),
    };
    serialize_journal_entry_row(&entry, item_id, JOURNAL_FOLDER_ID, columns)
}

pub(in crate::mapi) fn serialize_pending_conversation_action_row(
    properties: &HashMap<u32, MapiValue>,
    columns: &[u32],
) -> Vec<u8> {
    let action = conversation_action_from_mapi_properties(properties);
    let item_id = properties
        .get(&PID_TAG_MID)
        .and_then(MapiValue::as_i64)
        .and_then(|value| u64::try_from(value).ok())
        .unwrap_or_default();
    let message = MapiConversationActionMessage {
        id: item_id,
        folder_id: CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
        canonical_id: Uuid::nil(),
        action,
    };
    serialize_conversation_action_row(&message, columns)
}

pub(in crate::mapi) fn conversation_action_from_mapi_properties(
    properties: &HashMap<u32, MapiValue>,
) -> lpe_storage::ConversationAction {
    let conversation_id = properties
        .get(&PID_TAG_CONVERSATION_INDEX)
        .and_then(|value| match value {
            MapiValue::Binary(bytes) => conversation_id_from_index(bytes),
            _ => None,
        })
        .unwrap_or_else(Uuid::nil);
    lpe_storage::ConversationAction {
        id: conversation_id,
        conversation_id,
        subject: properties
            .get(&PID_TAG_SUBJECT_W)
            .or_else(|| properties.get(&PID_TAG_NORMALIZED_SUBJECT_W))
            .and_then(|value| value.as_text())
            .unwrap_or("Conv.Action")
            .to_string(),
        categories_json: match properties.get(&PID_NAME_KEYWORDS_TAG) {
            Some(MapiValue::MultiString(values)) => {
                serde_json::to_string(values).unwrap_or_else(|_| "[]".to_string())
            }
            _ => "[]".to_string(),
        },
        move_folder_entry_id: match properties.get(&PID_LID_CONVERSATION_ACTION_MOVE_FOLDER_EID_TAG)
        {
            Some(MapiValue::Binary(value)) => Some(value.clone()),
            _ => None,
        },
        move_store_entry_id: match properties.get(&PID_LID_CONVERSATION_ACTION_MOVE_STORE_EID_TAG) {
            Some(MapiValue::Binary(value)) => Some(value.clone()),
            _ => None,
        },
        move_target_mailbox_id: None,
        max_delivery_time: properties
            .get(&PID_LID_CONVERSATION_ACTION_MAX_DELIVERY_TIME_TAG)
            .and_then(MapiValue::as_i64)
            .and_then(filetime_to_rfc3339_utc),
        last_applied_time: properties
            .get(&PID_LID_CONVERSATION_ACTION_LAST_APPLIED_TIME_TAG)
            .and_then(MapiValue::as_i64)
            .and_then(filetime_to_rfc3339_utc),
        version: properties
            .get(&PID_LID_CONVERSATION_ACTION_VERSION_TAG)
            .and_then(MapiValue::as_i64)
            .and_then(|value| i32::try_from(value).ok())
            .unwrap_or(lpe_storage::CONVERSATION_ACTION_VERSION),
        processed: properties
            .get(&PID_LID_CONVERSATION_PROCESSED_TAG)
            .and_then(MapiValue::as_i64)
            .and_then(|value| i32::try_from(value).ok())
            .unwrap_or_default(),
        created_at: "1970-01-01T00:00:00Z".to_string(),
        updated_at: "1970-01-01T00:00:00Z".to_string(),
    }
}

pub(in crate::mapi) fn serialize_note_row(
    note: &ClientNote,
    item_id: u64,
    folder_id: u64,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match note_property_value(note, item_id, folder_id, *column) {
            Some(value) => write_mapi_value(&mut row, *column, &value),
            None => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(in crate::mapi) fn serialize_journal_entry_row(
    entry: &JournalEntry,
    item_id: u64,
    folder_id: u64,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match journal_entry_property_value(entry, item_id, folder_id, *column) {
            Some(value) => write_mapi_value(&mut row, *column, &value),
            None => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(in crate::mapi) fn serialize_pending_message_row(
    principal: &AccountPrincipal,
    properties: &HashMap<u32, MapiValue>,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        if let Some(value) = pending_message_property_value(principal, properties, *column) {
            write_mapi_value(&mut row, *column, &value);
        } else {
            write_property_default(&mut row, *column);
        }
    }
    row
}

pub(in crate::mapi) fn pending_message_property_value(
    principal: &AccountPrincipal,
    properties: &HashMap<u32, MapiValue>,
    property_tag: u32,
) -> Option<MapiValue> {
    let lookup_tag = canonical_property_storage_tag(property_tag);
    properties
        .get(&lookup_tag)
        .cloned()
        .or_else(|| match lookup_tag {
            PID_TAG_NORMALIZED_SUBJECT_W => properties.get(&PID_TAG_SUBJECT_W).cloned(),
            PID_TAG_SUBJECT_W => properties.get(&PID_TAG_NORMALIZED_SUBJECT_W).cloned(),
            PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String("IPM.Note".to_string())),
            PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_MESSAGE_ACCESS)),
            PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(MSGFLAG_UNSENT)),
            PID_TAG_HAS_ATTACHMENTS => Some(MapiValue::Bool(false)),
            PID_TAG_MESSAGE_SIZE => Some(MapiValue::I64(pending_message_size(properties))),
            PID_TAG_SENDER_NAME_W => Some(MapiValue::String(principal.display_name.clone())),
            PID_TAG_SENDER_EMAIL_ADDRESS_W => Some(MapiValue::String(principal.email.clone())),
            _ => None,
        })
}

pub(in crate::mapi) fn serialize_pending_contact_row(
    principal: &AccountPrincipal,
    properties: &HashMap<u32, MapiValue>,
    columns: &[u32],
) -> Vec<u8> {
    let contact = contact_input_from_mapi(
        principal.account_id,
        None,
        &default_contact_for_mapping(principal.account_id, "default"),
        properties,
    );
    let contact = AccessibleContact {
        id: Uuid::nil(),
        collection_id: "default".to_string(),
        owner_account_id: principal.account_id,
        owner_email: principal.email.clone(),
        owner_display_name: principal.display_name.clone(),
        rights: default_mapping_rights(),
        name: contact.name,
        role: contact.role,
        email: contact.email,
        phone: contact.phone,
        team: contact.team,
        notes: contact.notes,
    };
    serialize_contact_row(&contact, 0, CONTACTS_FOLDER_ID, columns)
}

pub(in crate::mapi) fn serialize_pending_event_row(
    principal: &AccountPrincipal,
    properties: &HashMap<u32, MapiValue>,
    columns: &[u32],
) -> Vec<u8> {
    let event = event_input_from_mapi(
        principal.account_id,
        None,
        &default_event_for_mapping(principal.account_id, "default"),
        properties,
    )
    .unwrap_or_else(|_| default_event_input(principal.account_id, None));
    let event = AccessibleEvent {
        id: Uuid::nil(),
        uid: Uuid::nil().to_string(),
        collection_id: "default".to_string(),
        owner_account_id: principal.account_id,
        owner_email: principal.email.clone(),
        owner_display_name: principal.display_name.clone(),
        rights: default_mapping_rights(),
        date: event.date,
        time: event.time,
        time_zone: event.time_zone,
        duration_minutes: event.duration_minutes,
        all_day: event.all_day,
        status: event.status,
        sequence: event.sequence,
        recurrence_rule: event.recurrence_rule,
        recurrence_json: event.recurrence_json,
        recurrence_exceptions_json: event.recurrence_exceptions_json,
        title: event.title,
        location: event.location,
        organizer_json: event.organizer_json,
        attendees: event.attendees,
        attendees_json: event.attendees_json,
        notes: event.notes,
        body_html: event.body_html,
    };
    serialize_event_row(&event, 0, CALENDAR_FOLDER_ID, columns)
}

pub(in crate::mapi) fn serialize_pending_task_row(
    principal: &AccountPrincipal,
    properties: &HashMap<u32, MapiValue>,
    columns: &[u32],
) -> Vec<u8> {
    let task = task_input_from_mapi(
        principal.account_id,
        None,
        &default_task_for_mapping(principal.account_id, "default"),
        Some("default"),
        properties,
    );
    let task = ClientTask {
        id: Uuid::nil(),
        owner_account_id: principal.account_id,
        owner_email: principal.email.clone(),
        owner_display_name: principal.display_name.clone(),
        is_owned: true,
        rights: default_mapping_rights(),
        task_list_id: task.task_list_id.unwrap_or_else(Uuid::nil),
        task_list_sort_order: 0,
        title: task.title,
        description: task.description,
        status: task.status,
        due_at: task.due_at,
        completed_at: task.completed_at,
        recurrence_rule: task.recurrence_rule,
        sort_order: task.sort_order,
        updated_at: "1970-01-01T00:00:00Z".to_string(),
    };
    serialize_task_row(&task, 0, TASKS_FOLDER_ID, columns)
}

pub(in crate::mapi) fn display_to(email: &JmapEmail) -> String {
    email
        .to
        .iter()
        .map(|address| {
            address
                .display_name
                .as_deref()
                .unwrap_or(&address.address)
                .to_string()
        })
        .collect::<Vec<_>>()
        .join("; ")
}

pub(in crate::mapi) fn display_cc(email: &JmapEmail) -> String {
    email
        .cc
        .iter()
        .map(|address| {
            address
                .display_name
                .as_deref()
                .unwrap_or(&address.address)
                .to_string()
        })
        .collect::<Vec<_>>()
        .join("; ")
}

pub(in crate::mapi) struct MapiRecipient<'a> {
    pub(in crate::mapi) recipient_type: u8,
    pub(in crate::mapi) address: &'a JmapEmailAddress,
}

pub(in crate::mapi) fn message_recipients(email: &JmapEmail) -> Vec<MapiRecipient<'_>> {
    email
        .to
        .iter()
        .map(|address| MapiRecipient {
            recipient_type: 0x01,
            address,
        })
        .chain(email.cc.iter().map(|address| MapiRecipient {
            recipient_type: 0x02,
            address,
        }))
        .chain(
            message_can_expose_bcc(email)
                .then_some(email.bcc.iter())
                .into_iter()
                .flatten()
                .map(|address| MapiRecipient {
                    recipient_type: 0x03,
                    address,
                }),
        )
        .collect()
}

pub(in crate::mapi) fn message_can_expose_bcc(email: &JmapEmail) -> bool {
    matches!(email.mailbox_role.as_str(), "drafts" | "sent")
}

pub(in crate::mapi) fn serialize_recipient_row(address: &JmapEmailAddress) -> Vec<u8> {
    let mut row = Vec::new();
    let recipient_flags = 0x0200u16 | 0x0010 | 0x0008 | 0x0003;
    row.extend_from_slice(&recipient_flags.to_le_bytes());
    write_utf16z(&mut row, &address.address);
    write_utf16z(
        &mut row,
        address.display_name.as_deref().unwrap_or(&address.address),
    );
    row.extend_from_slice(&0u16.to_le_bytes());
    row
}

pub(in crate::mapi) fn serialize_pending_recipient_row(recipient: &PendingRecipient) -> Vec<u8> {
    let address = JmapEmailAddress {
        address: recipient.address.clone(),
        display_name: recipient.display_name.clone(),
    };
    serialize_recipient_row(&address)
}

pub(in crate::mapi) fn message_flags(email: &JmapEmail) -> u32 {
    mapi_mailstore::canonical_message_flags(email)
}

pub(in crate::mapi) fn contact_size(contact: &AccessibleContact) -> i64 {
    contact
        .name
        .len()
        .saturating_add(contact.email.len())
        .saturating_add(contact.phone.len())
        .saturating_add(contact.team.len())
        .saturating_add(contact.notes.len())
        .min(i64::MAX as usize) as i64
}

pub(in crate::mapi) fn event_size(event: &AccessibleEvent) -> i64 {
    event
        .title
        .len()
        .saturating_add(event.location.len())
        .saturating_add(event.notes.len())
        .min(i64::MAX as usize) as i64
}

pub(in crate::mapi) fn task_size(task: &ClientTask) -> i64 {
    task.title
        .len()
        .saturating_add(task.description.len())
        .min(i64::MAX as usize) as i64
}

pub(in crate::mapi) fn journal_entry_start_sort_key(entry: &JournalEntry) -> &str {
    entry
        .starts_at
        .as_deref()
        .or(entry.occurred_at.as_deref())
        .unwrap_or(&entry.updated_at)
}

pub(in crate::mapi) fn event_start_sort_key(event: &AccessibleEvent) -> String {
    format!("{}T{}", event.date, event.time)
}

pub(in crate::mapi) fn event_start_filetime(event: &AccessibleEvent) -> u64 {
    date_time_to_filetime(&event.date, &event.time)
}

pub(in crate::mapi) fn event_end_filetime(event: &AccessibleEvent) -> u64 {
    let start = event_start_filetime(event);
    let duration = event.duration_minutes.max(1) as u64 * 60 * 10_000_000;
    start.saturating_add(duration)
}

pub(in crate::mapi) fn date_time_to_filetime(date: &str, time: &str) -> u64 {
    let year = date
        .get(0..4)
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or(1970);
    let month = date
        .get(5..7)
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(1);
    let day = date
        .get(8..10)
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(1);
    let hour = time
        .get(0..2)
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(0);
    let minute = time
        .get(3..5)
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(0);
    let days = days_from_civil(year, month, day).max(0) as u64;
    let unix_seconds = days
        .saturating_mul(86_400)
        .saturating_add(u64::from(hour.min(23)) * 3_600)
        .saturating_add(u64::from(minute.min(59)) * 60);
    unix_seconds_to_filetime(unix_seconds)
}

pub(in crate::mapi) fn filetime_to_date_time(filetime: i64) -> Option<(String, String)> {
    let filetime = u64::try_from(filetime).ok()?;
    let unix_seconds = filetime_to_unix_seconds(filetime)?;
    let days = unix_seconds / 86_400;
    let seconds = unix_seconds % 86_400;
    let (year, month, day) = civil_from_unix_days(days as i64);
    let hour = seconds / 3_600;
    let minute = (seconds % 3_600) / 60;
    Some((
        format!("{year:04}-{month:02}-{day:02}"),
        format!("{hour:02}:{minute:02}"),
    ))
}

pub(in crate::mapi) fn unix_seconds_to_filetime(unix_seconds: u64) -> u64 {
    unix_seconds
        .saturating_add(11_644_473_600)
        .saturating_mul(10_000_000)
}

pub(in crate::mapi) fn filetime_to_unix_seconds(filetime: u64) -> Option<u64> {
    filetime
        .checked_div(10_000_000)?
        .checked_sub(11_644_473_600)
}

pub(in crate::mapi) fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let year = i64::from(year) - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month = i64::from(month);
    let day = i64::from(day);
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

pub(in crate::mapi) fn unread_from_read_flags(read_flags: Option<u8>) -> Option<bool> {
    match read_flags {
        Some(flags) if flags & 0x10 != 0 => None,
        Some(flags) if flags & 0x04 != 0 => Some(true),
        Some(_) => Some(false),
        None => Some(false),
    }
}

pub(in crate::mapi) fn folder_message_class(mailbox: &JmapMailbox) -> &'static str {
    match mailbox.role.as_str() {
        "__mapi_deferred_action"
        | "__mapi_spooler_queue"
        | "__mapi_common_views"
        | "__mapi_views"
        | "__mapi_shortcuts"
        | "__mapi_freebusy_data" => "",
        "__mapi_search_folder_contact" => "IPF.Contact",
        "__mapi_search_folder_task" => "IPF.Task",
        "__mapi_search_folder_mixed" | "__mapi_search_folder_message" => "IPF.Note",
        "suggested_contacts" | "contacts_search" => "IPF.Contact",
        "quick_contacts" => "IPF.Contact.MOC.QuickContacts",
        "im_contact_list" => "IPF.Contact.MOC.ImContactList",
        "contacts" => "IPF.Contact",
        "calendar" => "IPF.Appointment",
        "journal" => "IPF.Journal",
        "notes" => "IPF.StickyNote",
        "tasks" => "IPF.Task",
        _ => "IPF.Note",
    }
}

fn folder_type(mailbox: &JmapMailbox) -> u32 {
    if mailbox.role.starts_with("__mapi_search_folder_") {
        FOLDER_SEARCH
    } else {
        FOLDER_GENERIC
    }
}

pub(in crate::mapi) fn collaboration_folder_message_class(
    kind: MapiCollaborationFolderKind,
) -> &'static str {
    match kind {
        MapiCollaborationFolderKind::Contacts => "IPF.Contact",
        MapiCollaborationFolderKind::Calendar => "IPF.Appointment",
        MapiCollaborationFolderKind::Task => "IPF.Task",
    }
}

pub(in crate::mapi) fn write_property_default(row: &mut Vec<u8>, property_tag: u32) {
    match MapiPropertyTag::new(property_tag).property_type() {
        Some(MapiPropertyType::Integer16) => write_u16(row, 0),
        Some(MapiPropertyType::Integer32) | Some(MapiPropertyType::Error) => write_u32(row, 0),
        Some(MapiPropertyType::Boolean) => row.push(0),
        Some(MapiPropertyType::Integer64) | Some(MapiPropertyType::Time) => write_u64(row, 0),
        Some(MapiPropertyType::String8) => write_ascii_z(row, ""),
        Some(MapiPropertyType::String) => write_utf16z(row, ""),
        Some(MapiPropertyType::Guid) => row.extend_from_slice(Uuid::nil().as_bytes()),
        Some(MapiPropertyType::Binary) => write_rop_binary(row, &[]),
        Some(
            MapiPropertyType::MultipleInteger16
            | MapiPropertyType::MultipleInteger32
            | MapiPropertyType::MultipleInteger64
            | MapiPropertyType::MultipleString8
            | MapiPropertyType::MultipleString
            | MapiPropertyType::MultipleGuid
            | MapiPropertyType::MultipleBinary,
        ) => write_u32(row, 0),
        _ => write_u32(row, 0x8004_0102),
    }
}

pub(in crate::mapi) fn mapi_folder_id(mailbox: &JmapMailbox) -> u64 {
    try_mapi_folder_id(mailbox).expect("MAPI folder identity mapping missing")
}

pub(in crate::mapi) fn try_mapi_folder_id(mailbox: &JmapMailbox) -> Option<u64> {
    match mailbox.role.as_str() {
        "__mapi_ipm_subtree" => Some(IPM_SUBTREE_FOLDER_ID),
        "__mapi_deferred_action" => Some(DEFERRED_ACTION_FOLDER_ID),
        "__mapi_spooler_queue" => Some(SPOOLER_QUEUE_FOLDER_ID),
        "__mapi_common_views" => Some(COMMON_VIEWS_FOLDER_ID),
        "__mapi_schedule" => Some(SCHEDULE_FOLDER_ID),
        "__mapi_search" => Some(SEARCH_FOLDER_ID),
        "__mapi_views" => Some(VIEWS_FOLDER_ID),
        "__mapi_shortcuts" => Some(SHORTCUTS_FOLDER_ID),
        "__mapi_freebusy_data" => Some(FREEBUSY_DATA_FOLDER_ID),
        "inbox" => Some(INBOX_FOLDER_ID),
        "drafts" => Some(DRAFTS_FOLDER_ID),
        "outbox" => Some(OUTBOX_FOLDER_ID),
        "sent" => Some(SENT_FOLDER_ID),
        "trash" => Some(TRASH_FOLDER_ID),
        "contacts" => Some(CONTACTS_FOLDER_ID),
        "calendar" => Some(CALENDAR_FOLDER_ID),
        "journal" => Some(JOURNAL_FOLDER_ID),
        "notes" => Some(NOTES_FOLDER_ID),
        "tasks" => Some(TASKS_FOLDER_ID),
        "reminders" => Some(REMINDERS_FOLDER_ID),
        "suggested_contacts" => Some(SUGGESTED_CONTACTS_FOLDER_ID),
        "quick_contacts" => Some(QUICK_CONTACTS_FOLDER_ID),
        "im_contact_list" => Some(IM_CONTACT_LIST_FOLDER_ID),
        "contacts_search" => Some(CONTACTS_SEARCH_FOLDER_ID),
        "document_libraries" => Some(DOCUMENT_LIBRARIES_FOLDER_ID),
        "sync_issues" => Some(SYNC_ISSUES_FOLDER_ID),
        "conflicts" => Some(CONFLICTS_FOLDER_ID),
        "local_failures" => Some(LOCAL_FAILURES_FOLDER_ID),
        "server_failures" => Some(SERVER_FAILURES_FOLDER_ID),
        "junk" => Some(JUNK_FOLDER_ID),
        "rss_feeds" => Some(RSS_FEEDS_FOLDER_ID),
        "tracked_mail_processing" => Some(TRACKED_MAIL_PROCESSING_FOLDER_ID),
        "todo_search" => Some(TODO_SEARCH_FOLDER_ID),
        "conversation_action_settings" => Some(CONVERSATION_ACTION_SETTINGS_FOLDER_ID),
        "quick_step_settings" => Some(QUICK_STEP_SETTINGS_FOLDER_ID),
        "archive" => Some(ARCHIVE_FOLDER_ID),
        "conversation_history" => Some(CONVERSATION_HISTORY_FOLDER_ID),
        _ => crate::mapi::identity::mapped_mapi_object_id(&mailbox.id),
    }
}

fn mapi_parent_folder_id(mailbox: &JmapMailbox) -> u64 {
    match mailbox.role.as_str() {
        "conflicts" | "local_failures" | "server_failures" => SYNC_ISSUES_FOLDER_ID,
        _ => mailbox
            .parent_id
            .and_then(|parent_id| crate::mapi::identity::mapped_mapi_object_id(&parent_id))
            .unwrap_or(IPM_SUBTREE_FOLDER_ID),
    }
}

fn mailbox_has_subfolders(mailbox: &JmapMailbox, mailboxes: &[JmapMailbox]) -> bool {
    !mailboxes.is_empty()
        && mailboxes
            .iter()
            .any(|candidate| candidate.parent_id == Some(mailbox.id))
}

pub(in crate::mapi) fn mapi_message_id(email: &JmapEmail) -> u64 {
    mapi_item_id(&email.id)
}

pub(in crate::mapi) fn mapi_item_id(id: &Uuid) -> u64 {
    crate::mapi::identity::mapped_mapi_object_id(id).expect("MAPI item identity mapping missing")
}
