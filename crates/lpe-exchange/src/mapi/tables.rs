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
    MapiNavigationShortcutMessage, MapiPublicFolder, MapiPublicFolderItem, MapiRule, MapiTask,
};
use lpe_storage::SearchFolderDefinition;

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
            existing
                .message_class
                .eq_ignore_ascii_case(&message.message_class)
                && (!is_empty_inbox_configuration_placeholder(existing)
                    || is_modeled_inbox_broad_startup_config(existing))
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

pub(in crate::mapi) fn default_associated_config_columns() -> Vec<u32> {
    let mut columns = default_contents_columns();
    columns.push(PID_TAG_FOLDER_ID);
    columns.push(PID_TAG_ROAMING_DATATYPES);
    columns
}

const COLLAPSE_STATE_MAGIC: &[u8; 6] = b"LPECS1";

#[derive(Clone)]
struct CategorizedTableRow {
    category_id: u64,
    leaf_count: usize,
    row: Vec<u8>,
    leaf: bool,
}

const TABLE_LEAF_ROW: u32 = 0x0000_0001;
const TABLE_EXPANDED_CATEGORY: u32 = 0x0000_0003;
const TABLE_COLLAPSED_CATEGORY: u32 = 0x0000_0004;

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

fn category_values_for_email(email: &JmapEmail, property_tag: u32) -> Vec<String> {
    let storage_tag = canonical_property_storage_tag(property_tag);
    if named_property_id_matches(storage_tag, PID_NAME_KEYWORDS_TAG) {
        let values = email
            .categories
            .iter()
            .map(|category| category.trim())
            .filter(|category| !category.is_empty())
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        if values.is_empty() {
            vec![String::new()]
        } else {
            values
        }
    } else {
        match email_property_value(email, storage_tag) {
            Some(value) => category_values_from_mapi_value(value),
            None => vec![String::new()],
        }
    }
}

fn category_values_from_mapi_value(value: MapiValue) -> Vec<String> {
    match value {
        MapiValue::MultiString(values) => {
            let values = values
                .into_iter()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .collect::<Vec<_>>();
            if values.is_empty() {
                vec![String::new()]
            } else {
                values
            }
        }
        value => vec![category_value_to_string(&value)],
    }
}

fn named_property_id_matches(left: u32, right: u32) -> bool {
    (left & 0xFFFF_0000) == (right & 0xFFFF_0000)
}

fn category_value_to_string(value: &MapiValue) -> String {
    match value {
        MapiValue::Bool(value) => value.to_string(),
        MapiValue::I16(value) => value.to_string(),
        MapiValue::I32(value) => value.to_string(),
        MapiValue::I64(value) => value.to_string(),
        MapiValue::F64(value) => f64::from_bits(*value).to_string(),
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
    let mut groups: Vec<(u64, String, Vec<(&JmapEmail, u32)>)> = Vec::new();
    for email in emails {
        for (instance, value) in category_values_for_email(email, category_sort.property_tag)
            .into_iter()
            .enumerate()
        {
            let category_id = category_id_for_value(folder_id, category_sort.property_tag, &value);
            let instance_num = instance.saturating_add(1).min(u32::MAX as usize) as u32;
            if let Some((_, _, rows)) = groups.iter_mut().find(|(id, _, _)| *id == category_id) {
                rows.push((email, instance_num));
            } else {
                groups.push((category_id, value, vec![(email, instance_num)]));
            }
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
        let expanded = expanded_count > 0 && !collapsed_categories.contains(&category_id);
        let unread_count = leaves.iter().filter(|(email, _)| email.unread).count();
        rows.push(CategorizedTableRow {
            category_id,
            leaf_count: leaves.len(),
            row: serialize_category_header_row(
                category_id,
                &value,
                leaves.len(),
                unread_count,
                category_sort.property_tag,
                expanded,
                columns,
            ),
            leaf: false,
        });
        if expanded {
            rows.extend(
                leaves
                    .into_iter()
                    .map(|(email, instance_num)| CategorizedTableRow {
                        category_id,
                        leaf_count: 1,
                        row: serialize_categorized_message_row(
                            email,
                            columns,
                            category_sort.property_tag,
                            &value,
                            instance_num,
                        ),
                        leaf: true,
                    }),
            );
        }
    }
    rows
}

fn serialize_category_header_row(
    category_id: u64,
    value: &str,
    leaf_count: usize,
    unread_count: usize,
    category_property_tag: u32,
    expanded: bool,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match canonical_property_storage_tag(*column) {
            PID_TAG_INST_ID => write_u64(&mut row, category_id),
            PID_TAG_INSTANCE_NUM => write_u32(&mut row, 0),
            PID_TAG_ROW_TYPE => write_u32(
                &mut row,
                if expanded {
                    TABLE_EXPANDED_CATEGORY
                } else {
                    TABLE_COLLAPSED_CATEGORY
                },
            ),
            PID_TAG_DEPTH => write_u32(&mut row, 0),
            PID_TAG_CONTENT_COUNT => write_u32(&mut row, leaf_count.min(u32::MAX as usize) as u32),
            PID_TAG_CONTENT_UNREAD_COUNT => {
                write_u32(&mut row, unread_count.min(u32::MAX as usize) as u32)
            }
            tag if tag == canonical_property_storage_tag(category_property_tag) => {
                write_category_instance_value(&mut row, *column, value)
            }
            _ => write_property_default(&mut row, *column),
        }
    }
    row
}

fn write_category_instance_value(row: &mut Vec<u8>, property_tag: u32, value: &str) {
    let value = match MapiPropertyTag::new(property_tag).property_type() {
        Some(MapiPropertyType::MultipleString | MapiPropertyType::MultipleString8) => {
            MapiValue::MultiString(vec![value.to_string()])
        }
        _ => MapiValue::String(value.to_string()),
    };
    write_mapi_value(row, property_tag, &value);
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
        PID_TAG_WLINK_CALENDAR_COLOR,
        PID_TAG_WLINK_ADDRESS_BOOK_EID,
        PID_TAG_WLINK_ADDRESS_BOOK_STORE_EID,
        PID_TAG_WLINK_CLIENT_ID,
        PID_TAG_WLINK_RO_GROUP_TYPE,
        PID_NAME_SHARING_CALENDAR_GROUP_ENTRY_ASSOCIATED_LOCAL_FOLDER_ID_TAG,
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
        PID_TAG_DISPLAY_NAME_W,
        PID_TAG_ATTACH_LONG_FILENAME_W,
        PID_TAG_ATTACH_FILENAME_W,
        PID_TAG_ATTACH_EXTENSION_W,
        PID_TAG_ATTACH_MIME_TAG_W,
        PID_TAG_ATTACH_SIZE,
        PID_TAG_ATTACH_METHOD,
        PID_TAG_RENDERING_POSITION,
        PID_TAG_ATTACHMENT_FLAGS,
        PID_TAG_ATTACH_FLAGS,
        PID_TAG_ATTACHMENT_LINK_ID,
        PID_TAG_ATTACHMENT_HIDDEN,
        PID_TAG_ATTACH_CONTENT_ID_W,
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
        PID_TAG_REM_OFFLINE_ENTRY_ID,
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
        PID_TAG_REM_OFFLINE_ENTRY_ID,
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

#[derive(Clone, Copy)]
enum SearchContentRow<'a> {
    Message(&'a MapiMessage),
    Task(&'a MapiTask),
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
        "row={row_index} kind={row_kind} object_id={} display_name={} entry_id={} entry_id_decoded={} record_key={} source_key={} source_key_decoded={} parent_source_key={} parent_source_key_decoded={} parent_entry_id={} parent_entry_id_decoded={} folder_id={} instance_key={} container_class={} expected_container_class={} folder_type={} content_count={} associated_content_count={} folder_id_consistent={} parent_id_consistent={} source_key_stable_non_empty={} record_key_stable_non_empty={} instance_key_stable_non_empty={} folder_type_valid={} counts_present_non_negative={} container_class_status={} issues={}",
        format_debug_u64(Some(object_id)),
        display_name.unwrap_or_default(),
        format_debug_binary(entry_id.as_deref()),
        format_debug_u64(entry_id_decoded),
        format_debug_binary(record_key.as_deref()),
        format_debug_binary(source_key.as_deref()),
        format_debug_u64(source_key_decoded),
        format_debug_binary(parent_source_key.as_deref()),
        format_debug_u64(parent_source_key_decoded),
        format_debug_binary(parent_entry_id.as_deref()),
        format_debug_u64(parent_entry_id_decoded),
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
        Some(bytes) => {
            let head = bytes
                .iter()
                .take(12)
                .map(|byte| format!("{byte:02x}"))
                .collect::<Vec<_>>()
                .join("");
            let tail = bytes
                .iter()
                .skip(bytes.len().saturating_sub(6))
                .map(|byte| format!("{byte:02x}"))
                .collect::<Vec<_>>()
                .join("");
            format!("present:{}:{}..{}", bytes.len(), head, tail)
        }
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

pub(in crate::mapi) fn rop_expand_row_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u8> {
    let Some(category_id) = request.category_id() else {
        return rop_error_response(0x59, request.response_handle_index(), 0x8004_0102);
    };
    let Some(MapiObject::ContentsTable {
        folder_id,
        associated,
        columns,
        columns_set,
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
    if !*columns_set && columns.is_empty() {
        return rop_error_response(0x59, request.response_handle_index(), 0x0000_04B9);
    }
    if *associated || *category_count == 0 || sort_orders.is_empty() {
        return rop_error_response(0x59, request.response_handle_index(), 0x8004_0102);
    }

    let columns = columns.clone();
    let mut source_rows = emails_for_folder(*folder_id, mailboxes, emails);
    source_rows.retain(|email| {
        restriction_matches_email_in_snapshot(restriction.as_ref(), email, *folder_id, snapshot)
    });
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
    if *expanded_count != 0 && !collapsed_categories.contains(&category_id) {
        return rop_error_response(0x59, request.response_handle_index(), 0x0000_04F8);
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
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u8> {
    let Some(category_id) = request.category_id() else {
        return rop_error_response(0x5A, request.response_handle_index(), 0x8004_0102);
    };
    let Some(MapiObject::ContentsTable {
        folder_id,
        associated,
        columns,
        columns_set,
        sort_orders,
        category_count,
        expanded_count,
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
    if !*columns_set && columns.is_empty() {
        return rop_error_response(0x5A, request.response_handle_index(), 0x0000_04B9);
    }

    let columns = columns.clone();
    let mut source_rows = emails_for_folder(*folder_id, mailboxes, emails);
    source_rows.retain(|email| {
        restriction_matches_email_in_snapshot(restriction.as_ref(), email, *folder_id, snapshot)
    });
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
    if *expanded_count == 0 || collapsed_categories.contains(&category_id) {
        return rop_error_response(0x5A, request.response_handle_index(), 0x0000_04F7);
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
        columns,
        columns_set,
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
    if !*columns_set && columns.is_empty() {
        return rop_error_response(0x6B, request.response_handle_index(), 0x0000_04B9);
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
    if !table_columns_are_available(object) {
        return rop_error_response(0x6C, request.response_handle_index(), 0x0000_04B9);
    }
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
                PID_TAG_SENDER_NAME_W => {
                    compare_case_insensitive(email_sender_name(left), email_sender_name(right))
                }
                PID_TAG_SENDER_EMAIL_ADDRESS_W => compare_case_insensitive(
                    email_sender_address(left),
                    email_sender_address(right),
                ),
                PID_TAG_DISPLAY_TO_W => {
                    compare_case_insensitive(&display_to(left), &display_to(right))
                }
                PID_TAG_MESSAGE_DELIVERY_TIME | PID_TAG_LAST_MODIFICATION_TIME => {
                    left.received_at.cmp(&right.received_at)
                }
                PID_TAG_MESSAGE_FLAGS => message_flags(left).cmp(&message_flags(right)),
                PID_TAG_MESSAGE_SIZE | PID_TAG_MESSAGE_SIZE_EXTENDED => {
                    left.size_octets.cmp(&right.size_octets)
                }
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
                    email_sender_name(&left.email),
                    email_sender_name(&right.email),
                ),
                PID_TAG_SENDER_EMAIL_ADDRESS_W => compare_case_insensitive(
                    email_sender_address(&left.email),
                    email_sender_address(&right.email),
                ),
                PID_TAG_DISPLAY_TO_W => {
                    compare_case_insensitive(&display_to(&left.email), &display_to(&right.email))
                }
                PID_TAG_MESSAGE_DELIVERY_TIME | PID_TAG_LAST_MODIFICATION_TIME => {
                    left.email.received_at.cmp(&right.email.received_at)
                }
                PID_TAG_MESSAGE_FLAGS => {
                    message_flags(&left.email).cmp(&message_flags(&right.email))
                }
                PID_TAG_MESSAGE_SIZE | PID_TAG_MESSAGE_SIZE_EXTENDED => {
                    left.email.size_octets.cmp(&right.email.size_octets)
                }
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

fn sort_associated_table_rows(
    rows: &mut [AssociatedTableRow],
    sort_orders: &[MapiSortOrder],
    mailbox_guid: Uuid,
) {
    if sort_orders.is_empty() {
        return;
    }
    rows.sort_by(|left, right| {
        for sort_order in sort_orders {
            let left_value =
                associated_table_row_property_value(left, mailbox_guid, sort_order.property_tag);
            let right_value =
                associated_table_row_property_value(right, mailbox_guid, sort_order.property_tag);
            let ordering = compare_optional_mapi_values(left_value, right_value);
            let ordering = apply_sort_direction(ordering, sort_order.order);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        associated_table_row_id(left).cmp(&associated_table_row_id(right))
    });
}

fn compare_optional_mapi_values(left: Option<MapiValue>, right: Option<MapiValue>) -> Ordering {
    match (left, right) {
        (Some(MapiValue::String(left)), Some(MapiValue::String(right))) => {
            compare_case_insensitive(&left, &right)
        }
        (Some(MapiValue::U64(left)), Some(MapiValue::U64(right))) => left.cmp(&right),
        (Some(MapiValue::I64(left)), Some(MapiValue::I64(right))) => left.cmp(&right),
        (Some(MapiValue::U32(left)), Some(MapiValue::U32(right))) => left.cmp(&right),
        (Some(MapiValue::I32(left)), Some(MapiValue::I32(right))) => left.cmp(&right),
        _ => Ordering::Equal,
    }
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
                (
                    MapiCommonViewsMessage::NamedView(left),
                    MapiCommonViewsMessage::NamedView(right),
                ) => compare_common_view_named_views(left, right, sort_order.property_tag),
                (
                    MapiCommonViewsMessage::SearchFolderDefinition(left),
                    MapiCommonViewsMessage::SearchFolderDefinition(right),
                ) => compare_search_folder_definitions(left, right, sort_order.property_tag),
                (
                    MapiCommonViewsMessage::NavigationShortcut(_),
                    MapiCommonViewsMessage::NamedView(_),
                ) => Ordering::Less,
                (
                    MapiCommonViewsMessage::NamedView(_),
                    MapiCommonViewsMessage::NavigationShortcut(_),
                ) => Ordering::Greater,
                (
                    MapiCommonViewsMessage::NavigationShortcut(_)
                    | MapiCommonViewsMessage::NamedView(_),
                    MapiCommonViewsMessage::SearchFolderDefinition(_),
                ) => Ordering::Less,
                (
                    MapiCommonViewsMessage::SearchFolderDefinition(_),
                    MapiCommonViewsMessage::NavigationShortcut(_)
                    | MapiCommonViewsMessage::NamedView(_),
                ) => Ordering::Greater,
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

fn compare_common_view_named_views(
    left: &MapiCommonViewNamedViewMessage,
    right: &MapiCommonViewNamedViewMessage,
    property_tag: u32,
) -> Ordering {
    match property_tag & 0xFFFF_0000 {
        0x0037_0000 | 0x3001_0000 => compare_case_insensitive(&left.name, &right.name),
        0x6834_0000 => left.view_flags.cmp(&right.view_flags),
        0x683A_0000 => left.view_type.cmp(&right.view_type),
        0x674A_0000 => left.id.cmp(&right.id),
        _ => Ordering::Equal,
    }
}

fn compare_search_folder_definitions(
    left: &SearchFolderDefinition,
    right: &SearchFolderDefinition,
    property_tag: u32,
) -> Ordering {
    match property_tag & 0xFFFF_0000 {
        0x0037_0000 | 0x3001_0000 => {
            compare_case_insensitive(&left.display_name, &right.display_name)
        }
        0x6841_0000 => left.role.cmp(&right.role),
        0x674A_0000 => left.id.cmp(&right.id),
        _ => Ordering::Equal,
    }
}

fn common_views_message_id(message: &MapiCommonViewsMessage) -> u64 {
    match message {
        MapiCommonViewsMessage::NavigationShortcut(message) => message.id,
        MapiCommonViewsMessage::NamedView(message) => message.id,
        MapiCommonViewsMessage::SearchFolderDefinition(message) => {
            crate::mapi::identity::mapped_mapi_object_id(&message.id).unwrap_or_default()
        }
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
        MapiCommonViewsMessage::NamedView(message) => {
            common_view_named_view_property_value(message, mailbox_guid, property_tag)
        }
        MapiCommonViewsMessage::SearchFolderDefinition(message) => {
            search_folder_definition_message_property_value(message, mailbox_guid, property_tag)
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
    wlink_ordinal_bytes(value)
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
                PID_TAG_MESSAGE_SIZE | PID_TAG_MESSAGE_SIZE_EXTENDED => {
                    left.item.size_octets.cmp(&right.item.size_octets)
                }
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
                PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W | PID_TAG_DISPLAY_NAME_W => {
                    compare_case_insensitive(&left.event.title, &right.event.title)
                }
                PID_TAG_LOCATION_W | PID_LID_LOCATION_W_TAG => {
                    compare_case_insensitive(&left.event.location, &right.event.location)
                }
                PID_TAG_START_DATE
                | PID_LID_COMMON_START_TAG
                | PID_LID_APPOINTMENT_START_WHOLE_TAG
                | PID_TAG_MESSAGE_DELIVERY_TIME
                | PID_TAG_LAST_MODIFICATION_TIME => {
                    (left.event.date.as_str(), left.event.time.as_str())
                        .cmp(&(right.event.date.as_str(), right.event.time.as_str()))
                }
                PID_TAG_END_DATE | PID_LID_COMMON_END_TAG | PID_LID_APPOINTMENT_END_WHOLE_TAG => (
                    left.event.date.as_str(),
                    left.event.time.as_str(),
                    left.event.duration_minutes,
                )
                    .cmp(&(
                        right.event.date.as_str(),
                        right.event.time.as_str(),
                        right.event.duration_minutes,
                    )),
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
    let mut rows = Vec::new();
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

fn search_content_row_matches(
    row: &SearchContentRow<'_>,
    restriction: Option<&MapiRestriction>,
) -> bool {
    match row {
        SearchContentRow::Message(message) => {
            restriction_matches_email(restriction, &message.email)
        }
        SearchContentRow::Task(task) => restriction_matches_task(restriction, &task.task),
    }
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
        SearchContentRow::Message(message) => message.id,
        SearchContentRow::Task(task) => task.id,
    }
}

fn search_content_row_subject<'a>(row: &'a SearchContentRow<'a>) -> &'a str {
    match row {
        SearchContentRow::Message(message) => &message.email.subject,
        SearchContentRow::Task(task) => &task.task.title,
    }
}

fn search_content_row_class(row: &SearchContentRow<'_>) -> &'static str {
    match row {
        SearchContentRow::Message(_) => "IPM.Note",
        SearchContentRow::Task(_) => "IPM.Task",
    }
}

fn search_content_row_time(row: &SearchContentRow<'_>) -> String {
    match row {
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
            MapiRestriction::InvalidTableRestriction => {
                push_bytes(hash, b"invalid");
            }
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
            MapiRestriction::Count { count, child } => {
                push_bytes(hash, b"count");
                push_bytes(hash, &count.to_le_bytes());
                push_restriction(hash, child);
            }
            MapiRestriction::SubObject { subobject, child } => {
                push_bytes(hash, b"subobject");
                push_bytes(hash, &subobject.to_le_bytes());
                push_restriction(hash, child);
            }
            MapiRestriction::Content {
                property_tag,
                value,
                fuzzy_level_low,
                fuzzy_level_high,
            } => {
                push_bytes(hash, b"content");
                push_bytes(hash, &property_tag.to_le_bytes());
                push_bytes(hash, &fuzzy_level_low.to_le_bytes());
                push_bytes(hash, &fuzzy_level_high.to_le_bytes());
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
            MapiRestriction::CompareProperties {
                relop,
                left_property_tag,
                right_property_tag,
            } => {
                push_bytes(hash, b"compare_properties");
                push_bytes(hash, &[*relop]);
                push_bytes(hash, &left_property_tag.to_le_bytes());
                push_bytes(hash, &right_property_tag.to_le_bytes());
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
            PID_TAG_DISPLAY_NAME_W | PID_TAG_ATTACH_FILENAME_W | PID_TAG_ATTACH_LONG_FILENAME_W => {
                write_utf16z(&mut row, &attachment.file_name)
            }
            PID_TAG_ATTACH_EXTENSION_W => {
                write_utf16z(&mut row, &attachment_file_extension(&attachment.file_name))
            }
            PID_TAG_ATTACH_MIME_TAG_W => write_utf16z(&mut row, &attachment.media_type),
            PID_TAG_ATTACH_SIZE => {
                write_u32(&mut row, attachment.size_octets.min(u32::MAX as u64) as u32)
            }
            PID_TAG_ATTACH_METHOD => write_u32(&mut row, attachment_method_value(attachment)),
            PID_TAG_RENDERING_POSITION => write_u32(&mut row, u32::MAX),
            PID_TAG_ATTACHMENT_FLAGS | PID_TAG_ATTACHMENT_LINK_ID => write_u32(&mut row, 0),
            PID_TAG_ATTACH_FLAGS => write_u32(
                &mut row,
                if attachment.content_id.is_some() {
                    4
                } else {
                    0
                },
            ),
            PID_TAG_ATTACHMENT_HIDDEN => row.push(if attachment_is_inline(attachment) {
                1
            } else {
                0
            }),
            PID_TAG_ATTACH_CONTENT_ID_W => {
                write_utf16z(&mut row, attachment.content_id.as_deref().unwrap_or(""))
            }
            PID_TAG_ATTACH_RENDERING => write_u16_prefixed_bytes(&mut row, &[]),
            PID_TAG_CREATION_TIME | PID_TAG_LAST_MODIFICATION_TIME => write_u64(&mut row, 0),
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
    if !object.is_some_and(is_table_object) {
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

fn table_columns_are_available(object: &MapiObject) -> bool {
    match object {
        MapiObject::HierarchyTable {
            columns,
            columns_set,
            sort_orders,
            restriction,
            ..
        }
        | MapiObject::ContentsTable {
            columns,
            columns_set,
            sort_orders,
            restriction,
            ..
        }
        | MapiObject::AttachmentTable {
            columns,
            columns_set,
            sort_orders,
            restriction,
            ..
        } => {
            (*columns_set || !columns.is_empty())
                && !table_sort_is_invalid(sort_orders)
                && !table_restriction_is_invalid(restriction.as_ref())
        }
        MapiObject::PermissionTable {
            columns,
            columns_set,
            ..
        } => *columns_set || !columns.is_empty(),
        MapiObject::RuleTable {
            columns,
            columns_set,
            ..
        } => *columns_set || !columns.is_empty(),
        _ => false,
    }
}

pub(in crate::mapi) fn invalid_table_sort_orders() -> Vec<MapiSortOrder> {
    vec![MapiSortOrder {
        property_tag: 0,
        order: u8::MAX,
    }]
}

fn table_sort_is_invalid(sort_orders: &[MapiSortOrder]) -> bool {
    sort_orders
        .first()
        .is_some_and(|sort| sort.property_tag == 0 && sort.order == u8::MAX)
}

fn table_restriction_is_invalid(restriction: Option<&MapiRestriction>) -> bool {
    matches!(restriction, Some(MapiRestriction::InvalidTableRestriction))
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
    if restriction.is_none() || is_broad_outlook_configuration_restriction(restriction) {
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

fn format_table_property_tags(tags: &[u32]) -> String {
    tags.iter()
        .map(|tag| format!("0x{tag:08x}"))
        .collect::<Vec<_>>()
        .join(",")
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
                if let Some((index, message)) = find_row(
                    row_refs.as_slice(),
                    *position,
                    request,
                    |message| {
                        if broad_outlook_configuration_probe {
                            let Some(config) = associated_table_row_config(message) else {
                                return false;
                            };
                            if crate::mapi_store::is_outlook_inbox_virtual_only_associated_config_id(
                                config.id,
                            ) {
                                return false;
                            }
                        }
                        associated_table_row_matches(message, Some(&restriction), mailbox_guid)
                    },
                ) {
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
    if folder_id == CONVERSATION_HISTORY_FOLDER_ID {
        return false;
    }
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
            PID_TAG_SUBFOLDERS => {
                row.push((has_subfolders && folder_id != SYNC_ISSUES_FOLDER_ID) as u8)
            }
            PID_TAG_ATTRIBUTE_HIDDEN => row.push(matches!(
                folder_id,
                CONVERSATION_ACTION_SETTINGS_FOLDER_ID | QUICK_STEP_SETTINGS_FOLDER_ID
            ) as u8),
            PID_TAG_CONTAINER_CLASS_W | PID_TAG_MESSAGE_CLASS_W if message_class.is_empty() => {
                write_property_default(&mut row, *column)
            }
            PID_TAG_CONTAINER_CLASS_W => write_utf16z(&mut row, message_class),
            PID_TAG_MESSAGE_CLASS_W => write_utf16z(&mut row, message_class),
            PID_TAG_DEFAULT_POST_MESSAGE_CLASS_STRING8 => {
                match default_post_message_class_for_container_class(message_class) {
                    Some(default_class) => write_ascii_z(&mut row, default_class),
                    None => write_property_default(&mut row, *column),
                }
            }
            PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W => {
                match default_post_message_class_for_container_class(message_class) {
                    Some(default_class) => write_utf16z(&mut row, default_class),
                    None => write_property_default(&mut row, *column),
                }
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
        SEARCH_FOLDER_ID => ("Search", ROOT_FOLDER_ID, "IPF.Note", false),
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
        CONTACTS_SEARCH_FOLDER_ID => ("Contacts Search", SEARCH_FOLDER_ID, "IPF.Contact", false),
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
        SEARCH_FOLDER_ID
        | CONTACTS_SEARCH_FOLDER_ID
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
            PID_TAG_DEFAULT_POST_MESSAGE_CLASS_STRING8 => write_ascii_z(&mut row, "IPM.Note"),
            PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W => write_utf16z(&mut row, "IPM.Note"),
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

const QUERY_ROWS_MAX_PROPERTY_VALUE_BYTES: usize = 510;

fn write_query_rows_property_row(response: &mut Vec<u8>, columns: &[u32], values: &[u8]) {
    response.push(0);
    let mut offset = 0usize;
    for column in columns {
        match write_query_rows_property_value(response, *column, values, offset) {
            Some(next_offset) => offset = next_offset,
            None => {
                response.extend_from_slice(values.get(offset..).unwrap_or_default());
                return;
            }
        }
    }
    response.extend_from_slice(values.get(offset..).unwrap_or_default());
}

pub(in crate::mapi) fn query_rows_property_row_bytes(_columns: &[u32], values: &[u8]) -> Vec<u8> {
    standard_property_row_bytes(values)
}

pub(in crate::mapi) fn standard_property_row_bytes(values: &[u8]) -> Vec<u8> {
    let mut row = Vec::with_capacity(values.len().saturating_add(1));
    write_standard_property_row(&mut row, values);
    row
}

fn write_query_rows_property_value(
    response: &mut Vec<u8>,
    property_tag: u32,
    values: &[u8],
    offset: usize,
) -> Option<usize> {
    let property_type = MapiPropertyTag::new(property_tag).property_type()?;
    match property_type {
        MapiPropertyType::Integer16 => {
            write_fixed_query_rows_property_value(response, values, offset, 2)
        }
        MapiPropertyType::Integer32 | MapiPropertyType::Floating32 | MapiPropertyType::Error => {
            write_fixed_query_rows_property_value(response, values, offset, 4)
        }
        MapiPropertyType::Boolean => {
            write_fixed_query_rows_property_value(response, values, offset, 1)
        }
        MapiPropertyType::Floating64 | MapiPropertyType::Integer64 | MapiPropertyType::Time => {
            write_fixed_query_rows_property_value(response, values, offset, 8)
        }
        MapiPropertyType::Guid => {
            write_fixed_query_rows_property_value(response, values, offset, 16)
        }
        MapiPropertyType::String8 => write_query_rows_string8_value(response, values, offset),
        MapiPropertyType::String => write_query_rows_utf16_value(response, values, offset),
        MapiPropertyType::ServerId | MapiPropertyType::Binary => {
            write_query_rows_binary_value(response, values, offset)
        }
        MapiPropertyType::MultipleInteger16 => {
            write_counted_fixed_query_rows_property_values(response, values, offset, 2)
        }
        MapiPropertyType::MultipleInteger32 => {
            write_counted_fixed_query_rows_property_values(response, values, offset, 4)
        }
        MapiPropertyType::MultipleInteger64 => {
            write_counted_fixed_query_rows_property_values(response, values, offset, 8)
        }
        MapiPropertyType::MultipleGuid => {
            write_counted_fixed_query_rows_property_values(response, values, offset, 16)
        }
        MapiPropertyType::MultipleString8 => {
            write_counted_query_rows_string_values(response, values, offset, false)
        }
        MapiPropertyType::MultipleString => {
            write_counted_query_rows_string_values(response, values, offset, true)
        }
        MapiPropertyType::MultipleBinary => {
            write_counted_query_rows_binary_values(response, values, offset)
        }
    }
}

fn write_fixed_query_rows_property_value(
    response: &mut Vec<u8>,
    values: &[u8],
    offset: usize,
    size: usize,
) -> Option<usize> {
    let end = offset.checked_add(size)?;
    response.extend_from_slice(values.get(offset..end)?);
    Some(end)
}

fn write_query_rows_string8_value(
    response: &mut Vec<u8>,
    values: &[u8],
    offset: usize,
) -> Option<usize> {
    let remaining = values.get(offset..)?;
    let end = remaining
        .iter()
        .position(|byte| *byte == 0)
        .map(|position| offset + position + 1)
        .unwrap_or(values.len());
    let segment = values.get(offset..end)?;
    if segment.len() <= QUERY_ROWS_MAX_PROPERTY_VALUE_BYTES {
        response.extend_from_slice(segment);
    } else {
        response.extend_from_slice(&segment[..QUERY_ROWS_MAX_PROPERTY_VALUE_BYTES - 1]);
        response.push(0);
    }
    Some(end)
}

fn write_query_rows_utf16_value(
    response: &mut Vec<u8>,
    values: &[u8],
    offset: usize,
) -> Option<usize> {
    let remaining = values.get(offset..)?;
    let mut relative_end = remaining.len();
    let mut index = 0usize;
    while index + 1 < remaining.len() {
        if remaining[index] == 0 && remaining[index + 1] == 0 {
            relative_end = index + 2;
            break;
        }
        index += 2;
    }
    let end = offset.checked_add(relative_end)?;
    let segment = values.get(offset..end)?;
    if segment.len() <= QUERY_ROWS_MAX_PROPERTY_VALUE_BYTES {
        response.extend_from_slice(segment);
    } else {
        response.extend_from_slice(&segment[..QUERY_ROWS_MAX_PROPERTY_VALUE_BYTES - 2]);
        response.extend_from_slice(&0u16.to_le_bytes());
    }
    Some(end)
}

fn write_query_rows_binary_value(
    response: &mut Vec<u8>,
    values: &[u8],
    offset: usize,
) -> Option<usize> {
    let size_bytes = values.get(offset..offset + 2)?;
    let size = u16::from_le_bytes(size_bytes.try_into().ok()?) as usize;
    let value_offset = offset + 2;
    let end = value_offset.checked_add(size)?;
    let value = values.get(value_offset..end)?;
    let truncated_size = value.len().min(QUERY_ROWS_MAX_PROPERTY_VALUE_BYTES);
    response.extend_from_slice(&(truncated_size as u16).to_le_bytes());
    response.extend_from_slice(&value[..truncated_size]);
    Some(end)
}

fn write_counted_fixed_query_rows_property_values(
    response: &mut Vec<u8>,
    values: &[u8],
    offset: usize,
    value_size: usize,
) -> Option<usize> {
    let count_bytes = values.get(offset..offset + 4)?;
    let count = u32::from_le_bytes(count_bytes.try_into().ok()?) as usize;
    let size = 4usize.checked_add(count.checked_mul(value_size)?)?;
    write_fixed_query_rows_property_value(response, values, offset, size)
}

fn write_counted_query_rows_string_values(
    response: &mut Vec<u8>,
    values: &[u8],
    offset: usize,
    unicode: bool,
) -> Option<usize> {
    let count_bytes = values.get(offset..offset + 4)?;
    let count = u32::from_le_bytes(count_bytes.try_into().ok()?) as usize;
    response.extend_from_slice(count_bytes);
    let mut current = offset + 4;
    for _ in 0..count {
        current = if unicode {
            write_query_rows_utf16_value(response, values, current)?
        } else {
            write_query_rows_string8_value(response, values, current)?
        };
    }
    Some(current)
}

fn write_counted_query_rows_binary_values(
    response: &mut Vec<u8>,
    values: &[u8],
    offset: usize,
) -> Option<usize> {
    let count_bytes = values.get(offset..offset + 4)?;
    let count = u32::from_le_bytes(count_bytes.try_into().ok()?) as usize;
    response.extend_from_slice(count_bytes);
    let mut current = offset + 4;
    for _ in 0..count {
        current = write_query_rows_binary_value(response, values, current)?;
    }
    Some(current)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapi::wire::MapiRestrictionType;
    use crate::mapi::wire::RopId;
    use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
    use lpe_storage::{
        AccessibleContact, AccessibleEvent, CollaborationCollection, CollaborationRights,
        MailboxRule, SearchFolderDefinition,
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
    fn default_store_identity_columns_include_offline_reminders_entry_id() {
        assert!(default_store_property_tags().contains(&PID_TAG_REM_OFFLINE_ENTRY_ID));
        assert!(default_folder_identity_property_tags().contains(&PID_TAG_REM_OFFLINE_ENTRY_ID));
    }

    #[test]
    fn property_defaults_serialize_floating_types_with_wire_widths() {
        let mut single = Vec::new();
        write_property_default(&mut single, 0x80BF_0004);
        assert_eq!(single, 0.0f32.to_le_bytes());

        let mut double = Vec::new();
        write_property_default(&mut double, 0x80BF_0005);
        assert_eq!(double, 0.0f64.to_le_bytes());
    }

    #[test]
    fn property_defaults_serialize_server_ids_as_empty_counted_binary() {
        let mut row = Vec::new();
        write_property_default(&mut row, PID_TAG_SENT_MAIL_SVR_EID);
        assert_eq!(row, 0u16.to_le_bytes());
    }

    #[test]
    fn property_defaults_serialize_multi_value_instance_columns() {
        let mut row = Vec::new();
        write_property_default(&mut row, 0x8031_3003);
        assert_eq!(row, 0u32.to_le_bytes());
    }

    #[test]
    fn microsoft_read_flags_validation_matches_message_protocol_rules() {
        for flags in [0x00, 0x01, 0x05, 0x10, 0x20, 0x40, 0x0A] {
            assert!(read_flags_are_valid(Some(flags), true));
        }
        for flags in [0x01, 0x05, 0x10, 0x20, 0x40] {
            assert!(read_flags_are_valid(Some(flags), false));
        }

        assert!(!read_flags_are_valid(Some(0x00), false));
        assert!(!read_flags_are_valid(Some(0x0A), false));
        assert!(!read_flags_are_valid(Some(0x04), true));
        assert!(!read_flags_are_valid(Some(0x11), true));
        assert!(!read_flags_are_valid(Some(0x60), true));
        assert!(!read_flags_are_valid(Some(0x80), true));
        assert!(!read_flags_are_valid(None, true));
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
    fn inbox_associated_invariant_uses_mailbox_guid_entry_id() {
        let mailbox_guid = Uuid::parse_str("bc737006-4413-49b9-aefc-3cb6e0088492").unwrap();
        let object = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: true,
            columns: Vec::new(),
            columns_set: false,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: std::collections::HashSet::new(),
            restriction: None,
            bookmarks: std::collections::HashMap::new(),
            next_bookmark: 1,
            position: 0,
        };
        let summaries = outlook_bootstrap_row_invariant_summaries(
            Some(&object),
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
            mailbox_guid,
            true,
            1,
        );

        assert_eq!(summaries.len(), 1, "{summaries:?}");
        assert!(
            summaries[0].contains("row_kind=inbox_associated")
                || summaries[0].contains("kind=inbox_associated"),
            "{summaries:?}"
        );
        assert!(summaries[0].contains("issues=none"), "{summaries:?}");
    }

    #[test]
    fn common_views_invariant_reports_decoded_row_identity() {
        let mailbox_guid = Uuid::parse_str("bc737006-4413-49b9-aefc-3cb6e0088492").unwrap();
        let object = MapiObject::ContentsTable {
            folder_id: COMMON_VIEWS_FOLDER_ID,
            associated: true,
            columns: Vec::new(),
            columns_set: false,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: std::collections::HashSet::new(),
            restriction: None,
            bookmarks: std::collections::HashMap::new(),
            next_bookmark: 1,
            position: 0,
        };
        let summaries = outlook_bootstrap_row_invariant_summaries(
            Some(&object),
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
            mailbox_guid,
            true,
            1,
        );

        assert_eq!(summaries.len(), 1);
        assert!(summaries[0].contains("kind=common_views_associated"));
        assert!(summaries[0].contains("source_key_decoded=0x"));
        assert!(summaries[0].contains("parent_source_key_decoded=0x0000000000090001"));
        assert!(summaries[0].contains("issues=none"));
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
            (FREEBUSY_DATA_FOLDER_ID, "IPF.Note"),
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
    fn pending_message_projects_non_empty_change_identity() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id: Uuid::nil(),
            email: "test@example.test".to_string(),
            display_name: "Test".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };
        let mut properties = HashMap::new();
        properties.insert(
            PID_TAG_MID,
            MapiValue::U64(crate::mapi::identity::mapi_store_id(42)),
        );

        assert_eq!(
            pending_message_property_value(&principal, &properties, PID_TAG_CHANGE_NUMBER),
            Some(MapiValue::U64(42))
        );
        assert_eq!(
            pending_message_property_value(&principal, &properties, PID_TAG_CHANGE_KEY),
            Some(MapiValue::Binary(
                mapi_mailstore::change_key_for_change_number(42)
            ))
        );
        assert_eq!(
            pending_message_property_value(
                &principal,
                &properties,
                PID_TAG_PREDECESSOR_CHANGE_LIST
            ),
            Some(MapiValue::Binary(mapi_mailstore::predecessor_change_list(
                42
            )))
        );
    }

    #[test]
    fn pending_associated_message_projects_configuration_defaults() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::nil(),
            account_id: Uuid::nil(),
            email: "test@example.test".to_string(),
            display_name: "Test".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };
        let mut properties = HashMap::new();
        properties.insert(
            PID_TAG_CHANGE_KEY,
            MapiValue::Binary(vec![0x11, 0x22, 0x33]),
        );

        assert_eq!(
            pending_associated_message_property_value(
                &principal,
                &properties,
                PID_TAG_MESSAGE_CLASS_W
            ),
            Some(MapiValue::String("IPM.Configuration".to_string()))
        );
        assert!(matches!(
            pending_associated_message_property_value(
                &principal,
                &properties,
                PID_TAG_ROAMING_DICTIONARY
            ),
            Some(MapiValue::Binary(value))
                if value.starts_with(br#"<?xml version="1.0" encoding="utf-8"?>"#)
                    && value.windows(b"18-OLPrefsVersion".len()).any(|window| window == b"18-OLPrefsVersion")
        ));
        assert_eq!(
            pending_associated_message_property_value(&principal, &properties, PID_TAG_CHANGE_KEY),
            Some(MapiValue::Binary(vec![0x11, 0x22, 0x33]))
        );
    }

    #[test]
    fn default_associated_config_columns_cover_required_configuration_contract() {
        let columns = default_associated_config_columns();
        for property_tag in [
            PID_TAG_FOLDER_ID,
            PID_TAG_MID,
            PID_TAG_MESSAGE_CLASS_W,
            PID_TAG_ROAMING_DATATYPES,
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
            ..Default::default()
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
    fn default_contacts_contents_table_uses_contact_rows_and_columns() {
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
        let contact_id = Uuid::parse_str("81818181-8181-4181-8181-818181818181").unwrap();
        crate::mapi::identity::remember_mapi_identity(
            contact_id,
            crate::mapi::identity::mapi_store_id(681),
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
            ..Default::default()
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
        .with_collaboration_folder_item_count(CONTACTS_FOLDER_ID, 0);
        let mut table = MapiObject::ContentsTable {
            folder_id: CONTACTS_FOLDER_ID,
            associated: false,
            columns: Vec::new(),
            columns_set: false,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        };

        assert_eq!(
            folder_message_count(CONTACTS_FOLDER_ID, &[], &[], &snapshot),
            1
        );

        let position_response = rop_query_position_response(
            &RopRequest {
                rop_id: RopId::QueryPosition.as_u8(),
                input_handle_index: Some(0),
                output_handle_index: None,
                payload: Vec::new(),
            },
            Some(&table),
            &[],
            &[],
            &snapshot,
            account_id,
        );
        assert_eq!(
            u32::from_le_bytes(position_response[10..14].try_into().unwrap()),
            1
        );

        let rows_response = rop_query_rows_response(
            &RopRequest {
                rop_id: RopId::QueryRows.as_u8(),
                input_handle_index: Some(0),
                output_handle_index: None,
                payload: vec![0, 1, 1, 0],
            },
            Some(&mut table),
            &[],
            &[],
            &snapshot,
            account_id,
        );

        assert_eq!(
            u16::from_le_bytes(rows_response[7..9].try_into().unwrap()),
            1
        );
        assert_response_contains_utf16(&rows_response, "Denis Ducret");
        assert_response_contains_utf16(&rows_response, "denis@example.test");
    }

    #[test]
    fn contact_table_projects_missing_secondary_email_slots_as_empty_strings() {
        let account_id = Uuid::parse_str("11111111-1111-4111-8111-111111111112").unwrap();
        let contact = AccessibleContact {
            id: Uuid::parse_str("81818181-8181-4181-8181-818181818182").unwrap(),
            collection_id: "default".to_string(),
            owner_account_id: account_id,
            owner_email: "test@example.test".to_string(),
            owner_display_name: "Test".to_string(),
            name: "Denis Ducret".to_string(),
            email: "denis@example.test".to_string(),
            ..Default::default()
        };
        let columns = [
            PID_LID_EMAIL1_EMAIL_ADDRESS_W_TAG,
            PID_LID_EMAIL1_ADDRESS_TYPE_W_TAG,
            PID_LID_EMAIL2_EMAIL_ADDRESS_W_TAG,
            PID_LID_EMAIL2_ADDRESS_TYPE_W_TAG,
            PID_LID_EMAIL3_EMAIL_ADDRESS_W_TAG,
            PID_LID_EMAIL3_ADDRESS_TYPE_W_TAG,
        ];

        assert_eq!(
            contact_property_value(
                &contact,
                1,
                CONTACTS_FOLDER_ID,
                PID_LID_EMAIL2_EMAIL_ADDRESS_W_TAG
            ),
            None
        );
        assert_eq!(
            contact_table_property_value(
                &contact,
                1,
                CONTACTS_FOLDER_ID,
                PID_LID_EMAIL2_EMAIL_ADDRESS_W_TAG
            ),
            Some(MapiValue::String(String::new()))
        );
        assert_eq!(
            contact_table_property_value(
                &contact,
                1,
                CONTACTS_FOLDER_ID,
                PID_LID_EMAIL3_ADDRESS_TYPE_W_TAG
            ),
            Some(MapiValue::String(String::new()))
        );

        let row = serialize_contact_row(&contact, 1, CONTACTS_FOLDER_ID, &columns);
        assert_response_contains_utf16(&row, "denis@example.test");
        assert_response_contains_utf16(&row, "SMTP");
    }

    #[test]
    fn get_status_rejects_folder_handles_matching_microsoft_table_scope() {
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
            vec![RopId::GetStatus.as_u8(), 1, 0x02, 0x01, 0x04, 0x80]
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
    fn configuration_folders_project_hidden_attribute() {
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
            Some(MapiValue::Bool(true))
        );

        let row = serialize_special_folder_row(
            CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
            &[],
            &[PID_TAG_ATTRIBUTE_HIDDEN],
            None,
        );
        assert_eq!(row, vec![1]);

        let row = serialize_special_folder_row(
            QUICK_STEP_SETTINGS_FOLDER_ID,
            &[],
            &[PID_TAG_ATTRIBUTE_HIDDEN],
            None,
        );
        assert_eq!(row, vec![1]);
    }

    #[test]
    fn sync_issues_hierarchy_table_is_leaf_until_backed() {
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
            size_octets: 0,
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

        assert!(row_ids.is_empty());
    }

    #[test]
    fn ipm_subtree_hierarchy_does_not_duplicate_sync_issues_children() {
        let snapshot = MapiMailStoreSnapshot::empty();
        let rows = hierarchy_rows(
            IPM_SUBTREE_FOLDER_ID,
            &[],
            &snapshot,
            None,
            &[],
            Uuid::nil(),
        );
        let row_ids = rows.iter().map(hierarchy_row_id).collect::<HashSet<_>>();

        assert!(row_ids.contains(&SYNC_ISSUES_FOLDER_ID));
        assert!(!row_ids.contains(&CONFLICTS_FOLDER_ID));
        assert!(!row_ids.contains(&LOCAL_FAILURES_FOLDER_ID));
        assert!(!row_ids.contains(&SERVER_FAILURES_FOLDER_ID));
    }

    #[test]
    fn contacts_search_hierarchy_row_belongs_to_search_folder() {
        let snapshot = MapiMailStoreSnapshot::empty();
        let ipm_rows = hierarchy_rows(
            IPM_SUBTREE_FOLDER_ID,
            &[],
            &snapshot,
            None,
            &[],
            Uuid::nil(),
        );
        let search_rows = hierarchy_rows(SEARCH_FOLDER_ID, &[], &snapshot, None, &[], Uuid::nil());

        assert!(!ipm_rows
            .iter()
            .any(|row| hierarchy_row_id(row) == CONTACTS_SEARCH_FOLDER_ID));
        let row = search_rows
            .iter()
            .find(|row| hierarchy_row_id(row) == CONTACTS_SEARCH_FOLDER_ID)
            .expect("contacts search row under Search");
        assert_eq!(hierarchy_row_parent_id(row, &[]), SEARCH_FOLDER_ID);
    }

    #[test]
    fn sync_issues_query_rows_returns_no_children_until_backed() {
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
            size_octets: 0,
            is_subscribed: true,
        };
        let mailboxes = [inbox];
        let mut table = MapiObject::HierarchyTable {
            folder_id: SYNC_ISSUES_FOLDER_ID,
            columns: vec![PID_TAG_DISPLAY_NAME_W, PID_TAG_FOLDER_ID],
            columns_set: true,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            deleted_advertised_special_folders: HashSet::new(),
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
        assert_eq!(u16::from_le_bytes(response[7..9].try_into().unwrap()), 0);
        assert!(utf16_position(&response, "INBOX").is_none());
        assert!(utf16_position(&response, "Conflicts").is_none());
        assert!(utf16_position(&response, "Local Failures").is_none());
        assert!(utf16_position(&response, "Server Failures").is_none());
        assert_eq!(table_position(&table), Some(0));
    }

    #[test]
    fn persisted_sync_issues_roles_stay_leaf_in_startup_hierarchy() {
        let sync_id = Uuid::parse_str("11111111-1111-1111-1111-11111111111a").unwrap();
        let mailboxes = vec![
            JmapMailbox {
                id: sync_id,
                parent_id: None,
                role: "sync_issues".to_string(),
                name: "Sync Issues".to_string(),
                sort_order: 90,
                modseq: 1,
                total_emails: 0,
                unread_emails: 0,
                size_octets: 0,
                is_subscribed: true,
            },
            JmapMailbox {
                id: Uuid::parse_str("11111111-1111-1111-1111-11111111111b").unwrap(),
                parent_id: Some(sync_id),
                role: "conflicts".to_string(),
                name: "Conflicts".to_string(),
                sort_order: 91,
                modseq: 1,
                total_emails: 0,
                unread_emails: 0,
                size_octets: 0,
                is_subscribed: true,
            },
            JmapMailbox {
                id: Uuid::parse_str("11111111-1111-1111-1111-11111111111c").unwrap(),
                parent_id: Some(sync_id),
                role: "local_failures".to_string(),
                name: "Local Failures".to_string(),
                sort_order: 92,
                modseq: 1,
                total_emails: 0,
                unread_emails: 0,
                size_octets: 0,
                is_subscribed: true,
            },
            JmapMailbox {
                id: Uuid::parse_str("11111111-1111-1111-1111-11111111111d").unwrap(),
                parent_id: Some(sync_id),
                role: "server_failures".to_string(),
                name: "Server Failures".to_string(),
                sort_order: 93,
                modseq: 1,
                total_emails: 0,
                unread_emails: 0,
                size_octets: 0,
                is_subscribed: true,
            },
        ];
        let snapshot = MapiMailStoreSnapshot::empty();
        let rows = hierarchy_rows(
            IPM_SUBTREE_FOLDER_ID,
            &mailboxes,
            &snapshot,
            None,
            &[],
            Uuid::nil(),
        );
        let row_ids = rows.iter().map(hierarchy_row_id).collect::<HashSet<_>>();

        assert!(row_ids.contains(&SYNC_ISSUES_FOLDER_ID));
        assert!(!row_ids.contains(&CONFLICTS_FOLDER_ID));
        assert!(!row_ids.contains(&LOCAL_FAILURES_FOLDER_ID));
        assert!(!row_ids.contains(&SERVER_FAILURES_FOLDER_ID));
        let sync_row = rows
            .iter()
            .find(|row| hierarchy_row_id(row) == SYNC_ISSUES_FOLDER_ID)
            .expect("sync issues startup row");
        assert_eq!(
            serialize_hierarchy_row(
                *sync_row,
                &mailboxes,
                &snapshot,
                &[PID_TAG_SUBFOLDERS],
                Uuid::nil(),
            ),
            vec![0]
        );
        assert!(!mailbox_has_subfolders(&mailboxes[0], &mailboxes));
        assert!(hierarchy_rows(
            SYNC_ISSUES_FOLDER_ID,
            &mailboxes,
            &snapshot,
            None,
            &[],
            Uuid::nil(),
        )
        .is_empty());
    }

    #[test]
    fn query_rows_request_validation_matches_microsoft_flags() {
        fn request(flags: u8, forward_read: u8) -> RopRequest {
            RopRequest {
                rop_id: RopId::QueryRows.as_u8(),
                input_handle_index: Some(0),
                output_handle_index: None,
                payload: vec![flags, forward_read, 1, 0],
            }
        }
        fn table() -> MapiObject {
            MapiObject::HierarchyTable {
                folder_id: SYNC_ISSUES_FOLDER_ID,
                columns: vec![PID_TAG_DISPLAY_NAME_W],
                columns_set: true,
                sort_orders: Vec::new(),
                category_count: 0,
                expanded_count: 0,
                collapsed_categories: HashSet::new(),
                deleted_advertised_special_folders: HashSet::new(),
                restriction: None,
                bookmarks: HashMap::new(),
                next_bookmark: 1,
                position: 0,
            }
        }

        for valid in [
            request(0x00, 0x00),
            request(0x00, 0x01),
            request(0x01, 0x01),
            request(0x02, 0x01),
            request(0x03, 0x01),
        ] {
            let mut table = table();
            let response = rop_query_rows_response(
                &valid,
                Some(&mut table),
                &[],
                &[],
                &MapiMailStoreSnapshot::empty(),
                Uuid::nil(),
            );
            assert_eq!(&response[..6], &[0x15, 0x00, 0, 0, 0, 0]);
        }

        for invalid in [request(0x04, 0x01), request(0x00, 0x02)] {
            let mut table = table();
            let response = rop_query_rows_response(
                &invalid,
                Some(&mut table),
                &[],
                &[],
                &MapiMailStoreSnapshot::empty(),
                Uuid::nil(),
            );
            assert_eq!(&response[..2], &[0x15, 0x00]);
            assert_eq!(
                u32::from_le_bytes(response[2..6].try_into().unwrap()),
                0x8007_0057
            );
        }
    }

    #[test]
    fn query_rows_truncates_variable_property_values_to_microsoft_limit() {
        let mut row = Vec::new();
        write_utf16z(&mut row, &"A".repeat(400));
        write_u16_prefixed_bytes(&mut row, &vec![0x42; 700]);

        let mut response = Vec::new();
        write_query_rows_property_row(&mut response, &[PID_TAG_SUBJECT_W, PID_TAG_ENTRY_ID], &row);

        assert_eq!(response[0], 0);
        let mut cursor = Cursor::new(&response[1..]);
        let subject = parse_mapi_property_value(&mut cursor, PID_TAG_SUBJECT_W).unwrap();
        assert_eq!(subject, MapiValue::String("A".repeat(254)));
        assert_eq!(cursor.position(), QUERY_ROWS_MAX_PROPERTY_VALUE_BYTES);

        let entry_id = parse_mapi_property_value(&mut cursor, PID_TAG_ENTRY_ID).unwrap();
        let MapiValue::Binary(entry_id) = entry_id else {
            panic!("entry id should be binary");
        };
        assert_eq!(entry_id.len(), QUERY_ROWS_MAX_PROPERTY_VALUE_BYTES);
        assert!(entry_id.iter().all(|byte| *byte == 0x42));
        assert_eq!(
            cursor.position(),
            QUERY_ROWS_MAX_PROPERTY_VALUE_BYTES * 2 + 2
        );
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
            size_octets: 0,
            is_subscribed: true,
        };
        let mailboxes = [inbox];
        let mut table = MapiObject::HierarchyTable {
            folder_id: SYNC_ISSUES_FOLDER_ID,
            columns: vec![PID_TAG_DISPLAY_NAME_W],
            columns_set: false,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            deleted_advertised_special_folders: HashSet::new(),
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
        assert_eq!(u16::from_le_bytes(response[7..9].try_into().unwrap()), 0);
        assert_eq!(table_position(&table), Some(3));
    }

    #[test]
    fn query_rows_origin_uses_global_position_for_windowed_content_tables() {
        let mailbox_id = Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap();
        let mailboxes = vec![JmapMailbox {
            id: mailbox_id,
            parent_id: None,
            role: "inbox".to_string(),
            name: "Inbox".to_string(),
            sort_order: 0,
            modseq: 1,
            total_emails: 4,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        }];
        let first_id = Uuid::parse_str("33333333-3333-4333-8333-333333333333").unwrap();
        let second_id = Uuid::parse_str("44444444-4444-4444-8444-444444444444").unwrap();
        crate::mapi::identity::remember_mapi_identity(
            first_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 501,
            ),
        );
        crate::mapi::identity::remember_mapi_identity(
            second_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 502,
            ),
        );
        let snapshot = MapiMailStoreSnapshot::new(
            mailboxes.clone(),
            vec![
                test_table_email(first_id, mailbox_id, "Window A"),
                test_table_email(second_id, mailbox_id, "Window B"),
            ],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .with_content_windows(vec![crate::mapi_store::MapiContentTableWindow {
            folder_id: INBOX_FOLDER_ID,
            view_signature: table_view_signature(&[], None),
            offset: 2,
            total: 4,
            message_ids: vec![first_id, second_id],
        }]);
        let mut table = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: false,
            columns: vec![PID_TAG_SUBJECT_W],
            columns_set: false,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 2,
        };
        let request = RopRequest {
            rop_id: RopId::QueryRows.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: vec![0, 1, 2, 0],
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
        assert_eq!(response[6], 0x02);
        assert_eq!(u16::from_le_bytes(response[7..9].try_into().unwrap()), 2);
        assert_eq!(table_position(&table), Some(4));
    }

    #[test]
    fn query_rows_ignores_incomplete_windowed_content_table_rows() {
        let mailbox_id = Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap();
        let mailboxes = vec![JmapMailbox {
            id: mailbox_id,
            parent_id: None,
            role: "inbox".to_string(),
            name: "Inbox".to_string(),
            sort_order: 0,
            modseq: 1,
            total_emails: 2,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        }];
        let first_id = Uuid::parse_str("33333333-3333-4333-8333-333333333333").unwrap();
        let missing_id = Uuid::parse_str("55555555-5555-4555-8555-555555555555").unwrap();
        crate::mapi::identity::remember_mapi_identity(
            first_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 801,
            ),
        );
        let snapshot = MapiMailStoreSnapshot::new(
            mailboxes.clone(),
            vec![test_table_email(first_id, mailbox_id, "Only stored row")],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .with_content_windows(vec![crate::mapi_store::MapiContentTableWindow {
            folder_id: INBOX_FOLDER_ID,
            view_signature: table_view_signature(&[], None),
            offset: 0,
            total: 2,
            message_ids: vec![first_id, missing_id],
        }]);
        let mut table = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: false,
            columns: vec![PID_TAG_SUBJECT_W],
            columns_set: false,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        };
        let response = rop_query_rows_response(
            &RopRequest {
                rop_id: RopId::QueryRows.as_u8(),
                input_handle_index: Some(0),
                output_handle_index: None,
                payload: vec![0, 1, 2, 0],
            },
            Some(&mut table),
            &mailboxes,
            &snapshot.emails(),
            &snapshot,
            Uuid::nil(),
        );

        assert_eq!(response[0], RopId::QueryRows.as_u8());
        assert_eq!(u16::from_le_bytes(response[7..9].try_into().unwrap()), 1);
        assert_eq!(table_position(&table), Some(1));
        assert_response_contains_utf16(&response, "Only stored row");

        let position_response = rop_query_position_response(
            &RopRequest {
                rop_id: RopId::QueryPosition.as_u8(),
                input_handle_index: Some(0),
                output_handle_index: None,
                payload: Vec::new(),
            },
            Some(&table),
            &mailboxes,
            &snapshot.emails(),
            &snapshot,
            Uuid::nil(),
        );
        assert_eq!(position_response[0], RopId::QueryPosition.as_u8());
        assert_eq!(
            u32::from_le_bytes(position_response[6..10].try_into().unwrap()),
            1
        );
        assert_eq!(
            u32::from_le_bytes(position_response[10..14].try_into().unwrap()),
            1
        );
    }

    #[test]
    fn bookmark_seek_preserves_global_position_for_windowed_content_tables() {
        let mailbox_id = Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap();
        let mailboxes = vec![JmapMailbox {
            id: mailbox_id,
            parent_id: None,
            role: "inbox".to_string(),
            name: "Inbox".to_string(),
            sort_order: 0,
            modseq: 1,
            total_emails: 4,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        }];
        let first_id = Uuid::parse_str("33333333-3333-4333-8333-333333333333").unwrap();
        let second_id = Uuid::parse_str("44444444-4444-4444-8444-444444444444").unwrap();
        crate::mapi::identity::remember_mapi_identity(
            first_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 601,
            ),
        );
        crate::mapi::identity::remember_mapi_identity(
            second_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 602,
            ),
        );
        let snapshot = MapiMailStoreSnapshot::new(
            mailboxes.clone(),
            vec![
                test_table_email(first_id, mailbox_id, "Window A"),
                test_table_email(second_id, mailbox_id, "Window B"),
            ],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .with_content_windows(vec![crate::mapi_store::MapiContentTableWindow {
            folder_id: INBOX_FOLDER_ID,
            view_signature: table_view_signature(&[], None),
            offset: 2,
            total: 4,
            message_ids: vec![first_id, second_id],
        }]);
        let mut table = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: false,
            columns: vec![PID_TAG_SUBJECT_W],
            columns_set: false,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 2,
        };
        let create_response = rop_create_bookmark_response(
            &RopRequest {
                rop_id: RopId::CreateBookmark.as_u8(),
                input_handle_index: Some(0),
                output_handle_index: None,
                payload: Vec::new(),
            },
            Some(&mut table),
            &mailboxes,
            &[],
            &snapshot,
            Uuid::nil(),
        );
        let bookmark_size = u16::from_le_bytes(create_response[6..8].try_into().unwrap()) as usize;
        let bookmark = create_response[8..8 + bookmark_size].to_vec();
        let mut seek_payload = Vec::new();
        seek_payload.extend_from_slice(&(bookmark.len() as u16).to_le_bytes());
        seek_payload.extend_from_slice(&bookmark);
        seek_payload.extend_from_slice(&1i32.to_le_bytes());
        seek_payload.push(1);

        let seek_response = rop_seek_row_bookmark_response(
            &RopRequest {
                rop_id: RopId::SeekRowBookmark.as_u8(),
                input_handle_index: Some(0),
                output_handle_index: None,
                payload: seek_payload,
            },
            Some(&mut table),
            &mailboxes,
            &[],
            &snapshot,
            Uuid::nil(),
        );

        assert_eq!(seek_response[0], RopId::SeekRowBookmark.as_u8());
        assert_eq!(seek_response[6], 0);
        assert_eq!(
            i32::from_le_bytes(seek_response[8..12].try_into().unwrap()),
            1
        );
        assert_eq!(table_position(&table), Some(3));
    }

    #[test]
    fn bookmark_seek_does_not_mark_sparse_window_unknown_row_deleted() {
        let mailbox_id = Uuid::parse_str("22222222-2222-4222-8222-222222222223").unwrap();
        let mailboxes = vec![JmapMailbox {
            id: mailbox_id,
            parent_id: None,
            role: "inbox".to_string(),
            name: "Inbox".to_string(),
            sort_order: 0,
            modseq: 1,
            total_emails: 4,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        }];
        let first_id = Uuid::parse_str("33333333-3333-4333-8333-333333333334").unwrap();
        let second_id = Uuid::parse_str("44444444-4444-4444-8444-444444444445").unwrap();
        crate::mapi::identity::remember_mapi_identity(
            first_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 603,
            ),
        );
        crate::mapi::identity::remember_mapi_identity(
            second_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 604,
            ),
        );
        let first_email = test_table_email(first_id, mailbox_id, "Window A");
        let second_email = test_table_email(second_id, mailbox_id, "Window B");
        let snapshot = MapiMailStoreSnapshot::new(
            mailboxes.clone(),
            vec![second_email],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .with_content_windows(vec![crate::mapi_store::MapiContentTableWindow {
            folder_id: INBOX_FOLDER_ID,
            view_signature: table_view_signature(&[], None),
            offset: 3,
            total: 4,
            message_ids: vec![second_id],
        }]);
        let bookmark = 7u32.to_le_bytes().to_vec();
        let mut bookmarks = HashMap::new();
        bookmarks.insert(
            bookmark.clone(),
            TableBookmark {
                position: 2,
                row_key: Some(mapi_message_id(&first_email)),
            },
        );
        let mut table = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: false,
            columns: vec![PID_TAG_SUBJECT_W],
            columns_set: false,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks,
            next_bookmark: 8,
            position: 0,
        };
        let mut seek_payload = Vec::new();
        seek_payload.extend_from_slice(&(bookmark.len() as u16).to_le_bytes());
        seek_payload.extend_from_slice(&bookmark);
        seek_payload.extend_from_slice(&1i32.to_le_bytes());
        seek_payload.push(1);

        let seek_response = rop_seek_row_bookmark_response(
            &RopRequest {
                rop_id: RopId::SeekRowBookmark.as_u8(),
                input_handle_index: Some(0),
                output_handle_index: None,
                payload: seek_payload,
            },
            Some(&mut table),
            &mailboxes,
            &[],
            &snapshot,
            Uuid::nil(),
        );

        assert_eq!(seek_response[0], RopId::SeekRowBookmark.as_u8());
        assert_eq!(seek_response[6], 0);
        assert_eq!(
            i32::from_le_bytes(seek_response[8..12].try_into().unwrap()),
            1
        );
        assert_eq!(table_position(&table), Some(3));
    }

    #[test]
    fn find_row_uses_windowed_content_table_rows_with_global_position() {
        let mailbox_id = Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap();
        let mailboxes = vec![JmapMailbox {
            id: mailbox_id,
            parent_id: None,
            role: "inbox".to_string(),
            name: "Inbox".to_string(),
            sort_order: 0,
            modseq: 1,
            total_emails: 4,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        }];
        let first_id = Uuid::parse_str("33333333-3333-4333-8333-333333333333").unwrap();
        let second_id = Uuid::parse_str("44444444-4444-4444-8444-444444444444").unwrap();
        crate::mapi::identity::remember_mapi_identity(
            first_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 701,
            ),
        );
        crate::mapi::identity::remember_mapi_identity(
            second_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 702,
            ),
        );
        let snapshot = MapiMailStoreSnapshot::new(
            mailboxes.clone(),
            vec![
                test_table_email(first_id, mailbox_id, "Window A"),
                test_table_email(second_id, mailbox_id, "Window B"),
            ],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .with_content_windows(vec![crate::mapi_store::MapiContentTableWindow {
            folder_id: INBOX_FOLDER_ID,
            view_signature: table_view_signature(&[], None),
            offset: 2,
            total: 4,
            message_ids: vec![first_id, second_id],
        }]);
        let mut table = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: false,
            columns: vec![PID_TAG_SUBJECT_W],
            columns_set: false,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 2,
        };
        let mut restriction = vec![MapiRestrictionType::Property as u8, 0x04];
        restriction.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
        restriction.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
        write_utf16z(&mut restriction, "Window B");
        let mut payload = vec![0];
        payload.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
        payload.extend_from_slice(&restriction);
        payload.push(1);
        payload.extend_from_slice(&0u16.to_le_bytes());

        let response = rop_find_row_response(
            &RopRequest {
                rop_id: RopId::FindRow.as_u8(),
                input_handle_index: Some(0),
                output_handle_index: None,
                payload,
            },
            Some(&mut table),
            &mailboxes,
            &[],
            &snapshot,
            Uuid::nil(),
        );

        assert_eq!(response[0], RopId::FindRow.as_u8());
        assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
        assert_eq!(response[7], 1);
        assert_eq!(table_position(&table), Some(3));
        assert_response_contains_utf16(&response, "Window B");
    }

    #[test]
    fn find_row_beginning_origin_keeps_windowed_global_position() {
        let mailbox_id = Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap();
        let mailboxes = vec![JmapMailbox {
            id: mailbox_id,
            parent_id: None,
            role: "inbox".to_string(),
            name: "Inbox".to_string(),
            sort_order: 0,
            modseq: 1,
            total_emails: 4,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        }];
        let first_id = Uuid::parse_str("33333333-3333-4333-8333-333333333333").unwrap();
        let second_id = Uuid::parse_str("44444444-4444-4444-8444-444444444444").unwrap();
        crate::mapi::identity::remember_mapi_identity(
            first_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 711,
            ),
        );
        crate::mapi::identity::remember_mapi_identity(
            second_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 712,
            ),
        );
        let snapshot = MapiMailStoreSnapshot::new(
            mailboxes.clone(),
            vec![
                test_table_email(first_id, mailbox_id, "Window A"),
                test_table_email(second_id, mailbox_id, "Window B"),
            ],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .with_content_windows(vec![crate::mapi_store::MapiContentTableWindow {
            folder_id: INBOX_FOLDER_ID,
            view_signature: table_view_signature(&[], None),
            offset: 2,
            total: 4,
            message_ids: vec![first_id, second_id],
        }]);
        let mut table = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: false,
            columns: vec![PID_TAG_SUBJECT_W],
            columns_set: false,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 2,
        };
        let mut restriction = vec![MapiRestrictionType::Property as u8, 0x04];
        restriction.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
        restriction.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
        write_utf16z(&mut restriction, "Window A");
        let mut payload = vec![0];
        payload.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
        payload.extend_from_slice(&restriction);
        payload.push(0);
        payload.extend_from_slice(&0u16.to_le_bytes());

        let response = rop_find_row_response(
            &RopRequest {
                rop_id: RopId::FindRow.as_u8(),
                input_handle_index: Some(0),
                output_handle_index: None,
                payload,
            },
            Some(&mut table),
            &mailboxes,
            &[],
            &snapshot,
            Uuid::nil(),
        );

        assert_eq!(response[0], RopId::FindRow.as_u8());
        assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
        assert_eq!(response[7], 1);
        assert_eq!(table_position(&table), Some(2));
        assert_response_contains_utf16(&response, "Window A");
    }

    #[test]
    fn find_row_beginning_origin_falls_back_when_complete_rows_are_loaded() {
        let mailbox_id = Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap();
        let mailboxes = vec![JmapMailbox {
            id: mailbox_id,
            parent_id: None,
            role: "inbox".to_string(),
            name: "Inbox".to_string(),
            sort_order: 0,
            modseq: 1,
            total_emails: 4,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        }];
        let first_id = Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap();
        let second_id = Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap();
        let third_id = Uuid::parse_str("33333333-3333-4333-8333-333333333333").unwrap();
        let fourth_id = Uuid::parse_str("44444444-4444-4444-8444-444444444444").unwrap();
        for (index, id) in [first_id, second_id, third_id, fourth_id]
            .into_iter()
            .enumerate()
        {
            crate::mapi::identity::remember_mapi_identity(
                id,
                crate::mapi::identity::mapi_store_id(
                    crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 820 + index as u64,
                ),
            );
        }
        let snapshot = MapiMailStoreSnapshot::new(
            mailboxes.clone(),
            vec![
                test_table_email(first_id, mailbox_id, "Earlier A"),
                test_table_email(second_id, mailbox_id, "Earlier B"),
                test_table_email(third_id, mailbox_id, "Window A"),
                test_table_email(fourth_id, mailbox_id, "Window B"),
            ],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .with_content_windows(vec![crate::mapi_store::MapiContentTableWindow {
            folder_id: INBOX_FOLDER_ID,
            view_signature: table_view_signature(&[], None),
            offset: 2,
            total: 4,
            message_ids: vec![third_id, fourth_id],
        }]);
        let mut table = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: false,
            columns: vec![PID_TAG_SUBJECT_W],
            columns_set: false,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 2,
        };
        let mut restriction = vec![MapiRestrictionType::Property as u8, 0x04];
        restriction.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
        restriction.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
        write_utf16z(&mut restriction, "Earlier B");
        let mut payload = vec![0];
        payload.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
        payload.extend_from_slice(&restriction);
        payload.push(0);
        payload.extend_from_slice(&0u16.to_le_bytes());

        let response = rop_find_row_response(
            &RopRequest {
                rop_id: RopId::FindRow.as_u8(),
                input_handle_index: Some(0),
                output_handle_index: None,
                payload,
            },
            Some(&mut table),
            &mailboxes,
            &snapshot.emails(),
            &snapshot,
            Uuid::nil(),
        );

        assert_eq!(response[0], RopId::FindRow.as_u8());
        assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
        assert_eq!(response[7], 1);
        assert_eq!(table_position(&table), Some(1));
        assert_response_contains_utf16(&response, "Earlier B");
    }

    #[test]
    fn query_position_clamps_stale_cursor_to_current_row_count() {
        let snapshot = MapiMailStoreSnapshot::empty();
        let expected_count = snapshot
            .associated_config_messages_for_folder(INBOX_FOLDER_ID)
            .len() as u32;
        let mut table = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: true,
            columns: vec![PID_TAG_SUBJECT_W],
            columns_set: false,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 50,
        };
        let request = RopRequest {
            rop_id: RopId::QueryPosition.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: Vec::new(),
        };

        let response = rop_query_position_response(
            &request,
            Some(&mut table),
            &[],
            &[],
            &snapshot,
            Uuid::nil(),
        );

        assert_eq!(response[0], RopId::QueryPosition.as_u8());
        assert_eq!(
            u32::from_le_bytes(response[6..10].try_into().unwrap()),
            expected_count
        );
        assert_eq!(
            u32::from_le_bytes(response[10..14].try_into().unwrap()),
            expected_count
        );
    }

    #[test]
    fn restricted_associated_query_position_reports_filtered_row_count() {
        let snapshot = MapiMailStoreSnapshot::empty();
        let restriction = MapiRestriction::Property {
            relop: 0x04,
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            value: MapiValue::String("IPM.Configuration.ExtensionMasterTable".to_string()),
        };
        let mut table = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: true,
            columns: vec![PID_TAG_MESSAGE_CLASS_W],
            columns_set: false,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: Some(restriction),
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        };
        let query_position = RopRequest {
            rop_id: RopId::QueryPosition.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: Vec::new(),
        };

        let response = rop_query_position_response(
            &query_position,
            Some(&table),
            &[],
            &[],
            &snapshot,
            Uuid::nil(),
        );

        assert_eq!(response[0], RopId::QueryPosition.as_u8());
        assert_eq!(u32::from_le_bytes(response[6..10].try_into().unwrap()), 0);
        assert_eq!(u32::from_le_bytes(response[10..14].try_into().unwrap()), 0);

        let query_rows = RopRequest {
            rop_id: RopId::QueryRows.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: vec![0, 1, 36, 0],
        };
        let response = rop_query_rows_response(
            &query_rows,
            Some(&mut table),
            &[],
            &[],
            &snapshot,
            Uuid::nil(),
        );

        assert_eq!(response[0], RopId::QueryRows.as_u8());
        assert_eq!(response[6], 0x00);
        assert_eq!(u16::from_le_bytes(response[7..9].try_into().unwrap()), 0);
    }

    #[test]
    fn calendar_contents_table_projects_canonical_events() {
        let account_id = Uuid::from_u128(0xbc737006441349b9aefc3cb6e0088492);
        let event_id = Uuid::from_u128(0xbd6a6c500b7f4fad83d93b9ea082d726);
        crate::mapi::identity::remember_mapi_identity(
            event_id,
            crate::mapi::identity::mapi_store_id(0x42),
        );
        let event = AccessibleEvent {
            id: event_id,
            uid: "zero-duration".to_string(),
            collection_id: "default".to_string(),
            owner_account_id: account_id,
            owner_email: "test@l-p-e.ch".to_string(),
            owner_display_name: "test".to_string(),
            rights: CollaborationRights {
                may_read: true,
                may_write: true,
                may_delete: true,
                may_share: false,
            },
            date: "2026-06-01".to_string(),
            time: "10:00".to_string(),
            time_zone: String::new(),
            duration_minutes: 0,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Test".to_string(),
            location: String::new(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: "[]".to_string(),
            notes: String::new(),
            body_html: String::new(),
        };
        let snapshot = MapiMailStoreSnapshot::new(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec![event],
            Vec::new(),
            Vec::new(),
        );
        let mut table = MapiObject::ContentsTable {
            folder_id: CALENDAR_FOLDER_ID,
            associated: false,
            columns: vec![PID_TAG_MID, PID_TAG_SUBJECT_W],
            columns_set: true,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        };

        assert_eq!(
            folder_message_count(CALENDAR_FOLDER_ID, &[], &[], &snapshot),
            1
        );

        let position_response = rop_query_position_response(
            &RopRequest {
                rop_id: RopId::QueryPosition.as_u8(),
                input_handle_index: Some(0),
                output_handle_index: None,
                payload: Vec::new(),
            },
            Some(&table),
            &[],
            &[],
            &snapshot,
            account_id,
        );
        assert_eq!(position_response[0], RopId::QueryPosition.as_u8());
        assert_eq!(
            u32::from_le_bytes(position_response[10..14].try_into().unwrap()),
            1
        );

        let rows_response = rop_query_rows_response(
            &RopRequest {
                rop_id: RopId::QueryRows.as_u8(),
                input_handle_index: Some(0),
                output_handle_index: None,
                payload: vec![0, 1, 10, 0],
            },
            Some(&mut table),
            &[],
            &[],
            &snapshot,
            account_id,
        );
        assert_eq!(rows_response[0], RopId::QueryRows.as_u8());
        assert_eq!(
            u16::from_le_bytes(rows_response[7..9].try_into().unwrap()),
            1
        );
        assert_response_contains_utf16(&rows_response, "Test");
    }

    #[test]
    fn query_rows_clamps_stale_cursor_to_current_row_count() {
        let snapshot = MapiMailStoreSnapshot::empty();
        let expected_count = snapshot
            .associated_config_messages_for_folder(INBOX_FOLDER_ID)
            .len();
        let mut table = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: true,
            columns: vec![PID_TAG_SUBJECT_W],
            columns_set: false,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 50,
        };
        let request = RopRequest {
            rop_id: RopId::QueryRows.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: vec![0, 1, 10, 0],
        };

        let response =
            rop_query_rows_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil());

        assert_eq!(response[0], RopId::QueryRows.as_u8());
        assert_eq!(response[6], 0x02);
        assert_eq!(u16::from_le_bytes(response[7..9].try_into().unwrap()), 0);
        assert_eq!(table_position(&table), Some(expected_count));
    }

    #[test]
    fn seek_row_clamps_stale_current_position_to_row_count() {
        let snapshot = MapiMailStoreSnapshot::empty();
        let expected_count = snapshot
            .associated_config_messages_for_folder(INBOX_FOLDER_ID)
            .len();
        let mut table = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: true,
            columns: vec![PID_TAG_SUBJECT_W],
            columns_set: false,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 50,
        };
        let response = rop_seek_row_response(
            &RopRequest {
                rop_id: RopId::SeekRow.as_u8(),
                input_handle_index: Some(0),
                output_handle_index: None,
                payload: vec![1, 0, 0, 0, 0, 1],
            },
            Some(&mut table),
            &[],
            &[],
            &snapshot,
            Uuid::nil(),
        );

        assert_eq!(response[0], RopId::SeekRow.as_u8());
        assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
        assert_eq!(response[6], 0);
        assert_eq!(i32::from_le_bytes(response[7..11].try_into().unwrap()), 0);
        assert_eq!(table_position(&table), Some(expected_count));
    }

    #[test]
    fn seek_row_request_validation_matches_microsoft_bookmark_and_boolean_values() {
        fn table() -> MapiObject {
            MapiObject::ContentsTable {
                folder_id: INBOX_FOLDER_ID,
                associated: true,
                columns: vec![PID_TAG_SUBJECT_W],
                columns_set: true,
                sort_orders: Vec::new(),
                category_count: 0,
                expanded_count: 0,
                collapsed_categories: HashSet::new(),
                restriction: None,
                bookmarks: HashMap::new(),
                next_bookmark: 1,
                position: 1,
            }
        }
        fn request(origin: u8, want_row_moved_count: u8) -> RopRequest {
            let mut payload = vec![origin];
            payload.extend_from_slice(&0i32.to_le_bytes());
            payload.push(want_row_moved_count);
            RopRequest {
                rop_id: RopId::SeekRow.as_u8(),
                input_handle_index: Some(0),
                output_handle_index: None,
                payload,
            }
        }

        for valid in [
            request(0x00, 0x00),
            request(0x01, 0x01),
            request(0x02, 0x01),
        ] {
            let mut table = table();
            let response = rop_seek_row_response(
                &valid,
                Some(&mut table),
                &[],
                &[],
                &MapiMailStoreSnapshot::empty(),
                Uuid::nil(),
            );
            assert_eq!(&response[..6], &[0x18, 0x00, 0, 0, 0, 0]);
        }

        for invalid in [request(0x03, 0x01), request(0x01, 0x02)] {
            let mut table = table();
            let response = rop_seek_row_response(
                &invalid,
                Some(&mut table),
                &[],
                &[],
                &MapiMailStoreSnapshot::empty(),
                Uuid::nil(),
            );
            assert_eq!(&response[..2], &[0x18, 0x00]);
            assert_eq!(
                u32::from_le_bytes(response[2..6].try_into().unwrap()),
                0x8007_0057
            );
            assert_eq!(table_position(&table), Some(1));
        }
    }

    #[test]
    fn seek_row_bookmark_request_validation_matches_microsoft_boolean_values() {
        let bookmark = 1u32.to_le_bytes().to_vec();
        let mut bookmarks = HashMap::new();
        bookmarks.insert(
            bookmark.clone(),
            TableBookmark {
                position: 1,
                row_key: None,
            },
        );
        let mut table = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: true,
            columns: vec![PID_TAG_SUBJECT_W],
            columns_set: true,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks,
            next_bookmark: 2,
            position: 1,
        };
        let mut payload = Vec::new();
        payload.extend_from_slice(&(bookmark.len() as u16).to_le_bytes());
        payload.extend_from_slice(&bookmark);
        payload.extend_from_slice(&0i32.to_le_bytes());
        payload.push(0x02);
        let request = RopRequest {
            rop_id: RopId::SeekRowBookmark.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload,
        };

        let response = rop_seek_row_bookmark_response(
            &request,
            Some(&mut table),
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
            Uuid::nil(),
        );

        assert_eq!(&response[..2], &[0x19, 0x00]);
        assert_eq!(
            u32::from_le_bytes(response[2..6].try_into().unwrap()),
            0x8007_0057
        );
        assert_eq!(table_position(&table), Some(1));
    }

    #[test]
    fn query_position_counts_categorized_content_rows() {
        let mailbox_id = Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap();
        let first_id = Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap();
        let second_id = Uuid::parse_str("33333333-3333-4333-8333-333333333333").unwrap();
        crate::mapi::identity::remember_mapi_identity(
            first_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 901,
            ),
        );
        crate::mapi::identity::remember_mapi_identity(
            second_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 902,
            ),
        );
        let mut first = test_table_email(first_id, mailbox_id, "Alpha");
        first.categories = vec!["Blue".to_string()];
        let mut second = test_table_email(second_id, mailbox_id, "Beta");
        second.categories = vec!["Green".to_string()];
        let mailboxes = vec![JmapMailbox {
            id: mailbox_id,
            parent_id: None,
            role: "inbox".to_string(),
            name: "Inbox".to_string(),
            sort_order: 0,
            modseq: 1,
            total_emails: 2,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        }];
        let emails = vec![first, second];
        let snapshot = MapiMailStoreSnapshot::empty();
        let table = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: false,
            columns: vec![PID_TAG_SUBJECT_W],
            columns_set: true,
            sort_orders: vec![MapiSortOrder {
                property_tag: PID_NAME_KEYWORDS_TAG,
                order: 0,
            }],
            category_count: 1,
            expanded_count: 1,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        };

        let response = rop_query_position_response(
            &RopRequest {
                rop_id: RopId::QueryPosition.as_u8(),
                input_handle_index: Some(0),
                output_handle_index: None,
                payload: Vec::new(),
            },
            Some(&table),
            &mailboxes,
            &emails,
            &snapshot,
            Uuid::nil(),
        );

        assert_eq!(response[0], RopId::QueryPosition.as_u8());
        assert_eq!(u32::from_le_bytes(response[6..10].try_into().unwrap()), 0);
        assert_eq!(u32::from_le_bytes(response[10..14].try_into().unwrap()), 4);
    }

    #[test]
    fn categorized_keywords_project_multivalue_instances_and_table_row_metadata() {
        let mailbox_id = Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap();
        let email_id = Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap();
        crate::mapi::identity::remember_mapi_identity(
            email_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 903,
            ),
        );
        let mut email = test_table_email(email_id, mailbox_id, "Categorized");
        email.categories = vec!["Blue".to_string(), "Customer".to_string()];
        email.unread = true;
        let columns = [
            PID_TAG_INST_ID,
            PID_TAG_INSTANCE_NUM,
            PID_TAG_ROW_TYPE,
            PID_TAG_DEPTH,
            PID_TAG_CONTENT_COUNT,
            PID_TAG_CONTENT_UNREAD_COUNT,
            PID_NAME_KEYWORDS_TAG,
            PID_TAG_SUBJECT_W,
        ];

        let rows = categorized_email_rows(
            INBOX_FOLDER_ID,
            vec![&email],
            &columns,
            &[MapiSortOrder {
                property_tag: PID_NAME_KEYWORDS_TAG,
                order: 0,
            }],
            1,
            &HashSet::new(),
        );

        assert_eq!(rows.len(), 4);
        assert_category_header_row(&rows[0].row, "Blue", 1, 1, TABLE_EXPANDED_CATEGORY);
        assert_category_leaf_row(&rows[1].row, &email, 1, "Blue");
        assert_category_header_row(&rows[2].row, "Customer", 1, 1, TABLE_EXPANDED_CATEGORY);
        assert_category_leaf_row(&rows[3].row, &email, 2, "Customer");
    }

    #[test]
    fn microsoft_oxctabl_category_values_preserve_all_multistring_instances() {
        assert_eq!(
            category_values_from_mapi_value(MapiValue::MultiString(vec![
                " Category1 ".to_string(),
                String::new(),
                "Category2".to_string(),
            ])),
            vec!["Category1".to_string(), "Category2".to_string()]
        );
        assert_eq!(
            category_values_from_mapi_value(MapiValue::MultiString(vec![
                String::new(),
                " ".to_string(),
            ])),
            vec![String::new()]
        );
    }

    fn assert_category_header_row(
        row: &[u8],
        category: &str,
        content_count: u32,
        unread_count: u32,
        row_type: u32,
    ) {
        let mut cursor = Cursor::new(row);
        parse_mapi_property_value(&mut cursor, PID_TAG_INST_ID).unwrap();
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_INSTANCE_NUM).unwrap(),
            MapiValue::I32(0)
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_ROW_TYPE).unwrap(),
            MapiValue::I32(row_type as i32)
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_DEPTH).unwrap(),
            MapiValue::I32(0)
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_CONTENT_COUNT).unwrap(),
            MapiValue::I32(content_count as i32)
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_CONTENT_UNREAD_COUNT).unwrap(),
            MapiValue::I32(unread_count as i32)
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_NAME_KEYWORDS_TAG).unwrap(),
            MapiValue::MultiString(vec![category.to_string()])
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_SUBJECT_W).unwrap(),
            MapiValue::String(String::new())
        );
    }

    fn assert_category_leaf_row(row: &[u8], email: &JmapEmail, instance_num: u32, category: &str) {
        let mut cursor = Cursor::new(row);
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_INST_ID).unwrap(),
            MapiValue::I64(mapi_message_id(email) as i64)
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_INSTANCE_NUM).unwrap(),
            MapiValue::I32(instance_num as i32)
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_ROW_TYPE).unwrap(),
            MapiValue::I32(TABLE_LEAF_ROW as i32)
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_DEPTH).unwrap(),
            MapiValue::I32(1)
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_CONTENT_COUNT).unwrap(),
            MapiValue::I32(0)
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_CONTENT_UNREAD_COUNT).unwrap(),
            MapiValue::I32(0)
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_NAME_KEYWORDS_TAG).unwrap(),
            MapiValue::MultiString(vec![category.to_string()])
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_SUBJECT_W).unwrap(),
            MapiValue::String("Categorized".to_string())
        );
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
            size_octets: 0,
            is_subscribed: true,
        };

        let row =
            serialize_folder_row_with_context(&inbox, &[], &[PID_TAG_DISPLAY_NAME_W], Uuid::nil());

        assert!(utf16_position(&row, "INBOX").is_none());
        assert_response_contains_utf16(&row, "Inbox");
    }

    #[test]
    fn microsoft_oxcfold_hierarchy_row_projects_folder_message_size_columns() {
        let inbox = JmapMailbox {
            id: Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap(),
            parent_id: None,
            role: "inbox".to_string(),
            name: "Inbox".to_string(),
            sort_order: 0,
            modseq: 1,
            total_emails: 18,
            unread_emails: 0,
            size_octets: u64::from(u32::MAX) + 10,
            is_subscribed: true,
        };

        let row = serialize_folder_row_with_context(
            &inbox,
            &[],
            &[PID_TAG_MESSAGE_SIZE, PID_TAG_MESSAGE_SIZE_EXTENDED],
            Uuid::nil(),
        );
        let mut cursor = Cursor::new(&row);

        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_MESSAGE_SIZE).unwrap(),
            MapiValue::I32(-1)
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_MESSAGE_SIZE_EXTENDED).unwrap(),
            MapiValue::I64(i64::from(u32::MAX) + 10)
        );
    }

    #[test]
    fn ipm_subtree_row_projects_principal_ost_identity_when_available() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap(),
            account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
            email: "test@l-p-e.ch".to_string(),
            display_name: "test".to_string(),
            quota_mb: None,
            quota_used_octets: None,
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
            quota_mb: None,
            quota_used_octets: None,
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
            fuzzy_level_low: 0x0001,
            fuzzy_level_high: 0x0001,
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
            size_octets: 0,
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
            FOLDER_SEARCH
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
        let definition_id = Uuid::parse_str("aaaaaaaa-5556-4111-8111-aaaaaaaaaaaa").unwrap();
        let folder_id = crate::mapi::identity::mapi_store_id(0x7FFF_1000_1124);
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
            &snapshot,
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
    fn custom_collaboration_folders_are_only_ipm_subtree_children() {
        let folder_id = crate::mapi::identity::mapi_store_id(0x7FFF_1000_1130);
        let collection = CollaborationCollection {
            id: "project-calendar".to_string(),
            kind: "calendar".to_string(),
            owner_account_id: Uuid::nil(),
            owner_email: "test@example.test".to_string(),
            owner_display_name: "Test".to_string(),
            display_name: "Project Calendar".to_string(),
            is_owned: true,
            rights: CollaborationRights {
                may_read: true,
                may_write: true,
                may_delete: true,
                may_share: true,
            },
        };
        crate::mapi::identity::remember_mapi_identity(
            crate::mapi_store::collaboration_folder_identity_canonical_id(
                crate::mapi_store::MapiCollaborationFolderKind::Calendar,
                &collection,
            )
            .unwrap(),
            folder_id,
        );
        let snapshot = MapiMailStoreSnapshot::new(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec![collection],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );
        let mailboxes = snapshot.mailboxes();

        let ipm_rows = hierarchy_rows(
            IPM_SUBTREE_FOLDER_ID,
            &mailboxes,
            &snapshot,
            None,
            &[],
            Uuid::nil(),
        );
        assert!(ipm_rows
            .iter()
            .any(|row| hierarchy_row_id(row) == folder_id));

        let root_rows = hierarchy_rows(
            ROOT_FOLDER_ID,
            &mailboxes,
            &snapshot,
            None,
            &[],
            Uuid::nil(),
        );
        assert!(!root_rows
            .iter()
            .any(|row| hierarchy_row_id(row) == folder_id));
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
        let conversation_history_shadow_id =
            Uuid::parse_str("aaaaaaaa-bbbb-4111-8111-aaaaaaaaaaaa").unwrap();
        let shadow_folder_id = crate::mapi::identity::mapi_store_id(0x4f);
        let suggested_shadow_folder_id = crate::mapi::identity::mapi_store_id(0x54);
        let quick_contacts_shadow_folder_id = crate::mapi::identity::mapi_store_id(0x55);
        let im_contacts_shadow_folder_id = crate::mapi::identity::mapi_store_id(0x56);
        let tasks_shadow_folder_id = crate::mapi::identity::mapi_store_id(0x57);
        let quick_step_shadow_folder_id = crate::mapi::identity::mapi_store_id(0x58);
        let conversation_history_shadow_folder_id = crate::mapi::identity::mapi_store_id(0x59);
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
        crate::mapi::identity::remember_mapi_identity(
            conversation_history_shadow_id,
            conversation_history_shadow_folder_id,
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
                size_octets: 0,
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
                size_octets: 0,
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
                size_octets: 0,
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
                size_octets: 0,
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
                size_octets: 0,
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
                size_octets: 0,
                is_subscribed: true,
            },
            JmapMailbox {
                id: conversation_history_shadow_id,
                parent_id: None,
                role: "conversation_history".to_string(),
                name: "Conversation History".to_string(),
                sort_order: 0,
                modseq: 1,
                total_emails: 0,
                unread_emails: 0,
                size_octets: 0,
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
        assert!(row_ids.contains(&TASKS_FOLDER_ID));
        assert!(!row_ids.contains(&QUICK_CONTACTS_FOLDER_ID));
        assert!(!row_ids.contains(&IM_CONTACT_LIST_FOLDER_ID));
        assert!(!row_ids.contains(&CONVERSATION_ACTION_SETTINGS_FOLDER_ID));
        assert!(!row_ids.contains(&QUICK_STEP_SETTINGS_FOLDER_ID));
        assert!(!row_ids.contains(&shadow_folder_id));
        assert!(!row_ids.contains(&suggested_shadow_folder_id));
        assert!(!row_ids.contains(&quick_contacts_shadow_folder_id));
        assert!(!row_ids.contains(&im_contacts_shadow_folder_id));
        assert!(!row_ids.contains(&tasks_shadow_folder_id));
        assert!(!row_ids.contains(&quick_step_shadow_folder_id));
        assert!(!row_ids.contains(&conversation_history_shadow_folder_id));
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
        assert!(sync_ids.contains(&TASKS_FOLDER_ID));
        assert!(!sync_ids.contains(&QUICK_CONTACTS_FOLDER_ID));
        assert!(!sync_ids.contains(&IM_CONTACT_LIST_FOLDER_ID));
        assert!(!sync_ids.contains(&CONVERSATION_ACTION_SETTINGS_FOLDER_ID));
        assert!(!sync_ids.contains(&QUICK_STEP_SETTINGS_FOLDER_ID));
        assert!(!sync_ids.contains(&shadow_folder_id));
        assert!(!sync_ids.contains(&suggested_shadow_folder_id));
        assert!(!sync_ids.contains(&quick_contacts_shadow_folder_id));
        assert!(!sync_ids.contains(&im_contacts_shadow_folder_id));
        assert!(!sync_ids.contains(&tasks_shadow_folder_id));
        assert!(!sync_ids.contains(&quick_step_shadow_folder_id));
        assert!(!sync_ids.contains(&conversation_history_shadow_folder_id));

        let calendar_row = rows
            .iter()
            .find(|row| hierarchy_row_id(row) == CALENDAR_FOLDER_ID)
            .expect("calendar special folder row");
        let serialized = serialize_hierarchy_row(
            *calendar_row,
            &mailboxes,
            &snapshot,
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

        for (folder_id, expected) in [(TASKS_FOLDER_ID, "IPF.Task")] {
            let row = rows
                .iter()
                .find(|row| hierarchy_row_id(row) == folder_id)
                .expect("special folder row");
            let serialized = serialize_hierarchy_row(
                *row,
                &mailboxes,
                &snapshot,
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
        for (folder_id, expected) in [
            (QUICK_CONTACTS_FOLDER_ID, "IPF.Contact.MOC.QuickContacts"),
            (IM_CONTACT_LIST_FOLDER_ID, "IPF.Contact.MOC.ImContactList"),
        ] {
            let serialized = serialize_advertised_special_folder_row_with_mailbox_guid(
                folder_id,
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
                &mapi_mailstore::virtual_special_mailbox(QUICK_CONTACTS_FOLDER_ID)
                    .expect("quick contacts virtual mailbox")
            ),
            "IPF.Contact.MOC.QuickContacts"
        );
    }

    #[test]
    fn deleted_advertised_quick_step_folder_unshadows_real_folder_in_hierarchy() {
        let quick_step_id = Uuid::parse_str("99999999-9999-4999-9999-999999999999").unwrap();
        let quick_step_folder_id = crate::mapi::identity::mapi_store_id(0x99);
        crate::mapi::identity::remember_mapi_identity(quick_step_id, quick_step_folder_id);
        let quick_step = JmapMailbox {
            id: quick_step_id,
            parent_id: None,
            role: "custom".to_string(),
            name: "Quick Step Settings".to_string(),
            sort_order: 40,
            modseq: 40,
            total_emails: 0,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        };
        let mailboxes = [quick_step];
        let snapshot = MapiMailStoreSnapshot::empty();
        let mut deleted = HashSet::new();
        deleted.insert(QUICK_STEP_SETTINGS_FOLDER_ID);

        let rows = hierarchy_rows_excluding_deleted(
            IPM_SUBTREE_FOLDER_ID,
            &mailboxes,
            &snapshot,
            None,
            &[],
            Uuid::nil(),
            &deleted,
        );
        let row_ids = rows.iter().map(hierarchy_row_id).collect::<Vec<_>>();

        assert!(!row_ids.contains(&QUICK_STEP_SETTINGS_FOLDER_ID));
        assert!(row_ids.contains(&quick_step_folder_id));
    }

    #[test]
    fn real_quick_step_folder_projects_configuration_class() {
        let quick_step_id = Uuid::parse_str("99999999-9999-4999-9999-999999999998").unwrap();
        crate::mapi::identity::remember_mapi_identity(
            quick_step_id,
            crate::mapi::identity::mapi_store_id(0x97),
        );
        let quick_step = JmapMailbox {
            id: quick_step_id,
            parent_id: None,
            role: "custom".to_string(),
            name: "Quick Step Settings".to_string(),
            sort_order: 40,
            modseq: 40,
            total_emails: 0,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        };

        assert_eq!(folder_message_class(&quick_step), "IPF.Configuration");
        assert_eq!(
            mailbox_property_value_with_context(
                &quick_step,
                std::slice::from_ref(&quick_step),
                PID_TAG_ATTRIBUTE_HIDDEN,
            ),
            Some(MapiValue::Bool(true))
        );
        assert_eq!(
            mailbox_property_value_with_context(
                &quick_step,
                std::slice::from_ref(&quick_step),
                PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W,
            ),
            Some(MapiValue::String("IPM.Configuration".to_string()))
        );

        let row = serialize_folder_row_with_context(
            &quick_step,
            std::slice::from_ref(&quick_step),
            &[
                PID_TAG_ATTRIBUTE_HIDDEN,
                PID_TAG_CONTAINER_CLASS_W,
                PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W,
            ],
            Uuid::nil(),
        );
        let container_class = "IPF.Configuration"
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect::<Vec<_>>();
        let default_post_class = "IPM.Configuration"
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect::<Vec<_>>();

        assert_eq!(row.first(), Some(&1));
        assert!(row
            .windows(container_class.len())
            .any(|window| window == container_class));
        assert!(row
            .windows(default_post_class.len())
            .any(|window| window == default_post_class));
    }

    #[test]
    fn deleted_advertised_quick_step_folder_is_excluded_from_hierarchy_sync() {
        let quick_step_id = Uuid::parse_str("88888888-8888-4888-8888-888888888888").unwrap();
        let quick_step_folder_id = crate::mapi::identity::mapi_store_id(0x98);
        crate::mapi::identity::remember_mapi_identity(quick_step_id, quick_step_folder_id);
        let quick_step = JmapMailbox {
            id: quick_step_id,
            parent_id: None,
            role: "custom".to_string(),
            name: "Quick Step Settings".to_string(),
            sort_order: 40,
            modseq: 40,
            total_emails: 0,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        };
        let mailboxes = [quick_step];
        let mut deleted = HashSet::new();
        deleted.insert(QUICK_STEP_SETTINGS_FOLDER_ID);

        let sync_ids =
            sync_mailboxes_for_excluding_deleted(IPM_SUBTREE_FOLDER_ID, 0x02, &mailboxes, &deleted)
                .iter()
                .map(mapi_folder_id)
                .collect::<Vec<_>>();

        assert!(!sync_ids.contains(&QUICK_STEP_SETTINGS_FOLDER_ID));
        assert!(sync_ids.contains(&quick_step_folder_id));
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
    fn microsoft_oxosrch_common_views_projects_search_folder_definition_messages() {
        let definition_id = Uuid::parse_str("aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa").unwrap();
        crate::mapi::identity::remember_mapi_identity(
            definition_id,
            crate::mapi::identity::mapi_store_id(123),
        );
        let mut definition_blob = vec![
            0x04, 0x10, 0x00, 0x00, 0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
        ];
        definition_blob.extend_from_slice(&1u32.to_le_bytes());
        definition_blob.push(0xAA);
        definition_blob.extend_from_slice(&0u32.to_le_bytes());
        definition_blob.push(0xBB);
        definition_blob.extend_from_slice(&0u32.to_le_bytes());
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
            restriction_json: serde_json::json!({
                "kind": "exchange_reminders",
                "pidTagSearchFolderDefinition": BASE64_STANDARD.encode(&definition_blob)
            }),
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
                PID_TAG_SEARCH_FOLDER_ID,
                PID_TAG_SEARCH_FOLDER_TEMPLATE_ID,
                PID_TAG_SEARCH_FOLDER_STORAGE_TYPE,
                PID_TAG_SEARCH_FOLDER_TAG,
                PID_TAG_SEARCH_FOLDER_EFP_FLAGS,
                PID_TAG_SEARCH_FOLDER_DEFINITION,
            ],
            columns_set: true,
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
            payload: vec![0, 1, 20, 0],
        };

        assert_eq!(
            associated_folder_message_count(COMMON_VIEWS_FOLDER_ID, &snapshot),
            7
        );
        let response =
            rop_query_rows_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil());

        assert_eq!(response[0], 0x15);
        assert_eq!(u16::from_le_bytes(response[7..9].try_into().unwrap()), 7);
        let mut shortcut_class = Vec::new();
        for code_unit in "IPM.Microsoft.WunderBar.Link".encode_utf16() {
            shortcut_class.extend_from_slice(&code_unit.to_le_bytes());
        }
        let mut search_class = Vec::new();
        for code_unit in "IPM.Microsoft.WunderBar.SFInfo".encode_utf16() {
            search_class.extend_from_slice(&code_unit.to_le_bytes());
        }
        let mut named_view_class = Vec::new();
        for code_unit in "IPM.Microsoft.FolderDesign.NamedView".encode_utf16() {
            named_view_class.extend_from_slice(&code_unit.to_le_bytes());
        }
        assert!(response
            .windows(shortcut_class.len())
            .any(|window| window == shortcut_class.as_slice()));
        assert!(response
            .windows(search_class.len())
            .any(|window| window == search_class.as_slice()));
        assert!(response
            .windows(16)
            .any(|window| window == definition_id.as_bytes()));
        assert!(response
            .windows(4)
            .any(|window| window == 0x48u32.to_le_bytes()));
    }

    #[test]
    fn common_views_default_columns_are_navigation_shortcut_columns() {
        let columns = default_navigation_shortcut_property_tags();

        assert!(columns.contains(&PID_TAG_WLINK_ENTRY_ID));
        assert!(columns.contains(&PID_TAG_WLINK_FOLDER_TYPE));
        assert!(columns.contains(&PID_TAG_WLINK_CALENDAR_COLOR));
        assert!(columns.contains(&PID_TAG_WLINK_ADDRESS_BOOK_EID));
        assert!(columns.contains(&PID_TAG_WLINK_ADDRESS_BOOK_STORE_EID));
        assert!(columns.contains(&PID_TAG_WLINK_CLIENT_ID));
        assert!(columns.contains(&PID_TAG_WLINK_RO_GROUP_TYPE));
        assert!(
            columns.contains(&PID_NAME_SHARING_CALENDAR_GROUP_ENTRY_ASSOCIATED_LOCAL_FOLDER_ID_TAG)
        );
        assert!(!columns.contains(&0x6842_0003));
        assert!(!columns.contains(&0x6845_0102));
    }

    #[test]
    fn navigation_shortcut_parser_accepts_binary_wlink_group_ids() {
        let group_id = Uuid::from_bytes([0x33; 16]);
        let mut header_properties = HashMap::new();
        header_properties.insert(
            PID_TAG_SUBJECT_W,
            MapiValue::String("My Calendars".to_string()),
        );
        header_properties.insert(PID_TAG_WLINK_TYPE, MapiValue::U32(4));
        header_properties.insert(0x6842_0102, MapiValue::Binary(group_id.as_bytes().to_vec()));

        let header =
            navigation_shortcut_from_mapi_properties(Uuid::nil(), None, &header_properties);

        assert_eq!(header.shortcut_type, 4);
        assert_eq!(header.group_header_id, Some(group_id));

        let mut link_properties = HashMap::new();
        link_properties.insert(PID_TAG_SUBJECT_W, MapiValue::String("Calendar".to_string()));
        link_properties.insert(PID_TAG_WLINK_TYPE, MapiValue::U32(0));
        link_properties.insert(0x6850_0102, MapiValue::Binary(group_id.as_bytes().to_vec()));

        let link = navigation_shortcut_from_mapi_properties(Uuid::nil(), None, &link_properties);

        assert_eq!(link.shortcut_type, 0);
        assert_eq!(link.group_header_id, Some(group_id));
    }

    #[test]
    fn navigation_shortcut_parser_decodes_typed_and_wrapped_entry_id() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let inbox_entry_id =
            crate::mapi::identity::folder_entry_id_from_object_id(account_id, INBOX_FOLDER_ID)
                .unwrap();
        let mut wrapped_entry_id = vec![0xaa; 17];
        wrapped_entry_id.extend_from_slice(&inbox_entry_id);
        wrapped_entry_id.extend_from_slice(&[0xbb; 13]);
        let mut properties = HashMap::new();
        properties.insert(PID_TAG_SUBJECT_W, MapiValue::String("Inbox".to_string()));
        properties.insert(PID_TAG_WLINK_TYPE, MapiValue::U32(0));
        properties.insert(0x684C_0102, MapiValue::Binary(wrapped_entry_id));
        properties.insert(0x6850_0102, MapiValue::Binary([0x44; 16].to_vec()));

        let shortcut = navigation_shortcut_from_mapi_properties(account_id, None, &properties);

        assert_eq!(shortcut.target_folder_id, Some(INBOX_FOLDER_ID));
        assert_eq!(shortcut.group_header_id, Some(Uuid::from_bytes([0x44; 16])));
    }

    #[test]
    fn find_row_request_validation_matches_microsoft_flags() {
        fn request(flags: u8) -> RopRequest {
            let mut restriction = vec![MapiRestrictionType::Exist as u8];
            restriction.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
            let mut payload = vec![flags];
            payload.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
            payload.extend_from_slice(&restriction);
            payload.push(1);
            payload.extend_from_slice(&0u16.to_le_bytes());
            RopRequest {
                rop_id: RopId::FindRow.as_u8(),
                input_handle_index: Some(0),
                output_handle_index: None,
                payload,
            }
        }
        fn table() -> MapiObject {
            MapiObject::HierarchyTable {
                folder_id: SYNC_ISSUES_FOLDER_ID,
                columns: vec![PID_TAG_DISPLAY_NAME_W],
                columns_set: true,
                sort_orders: Vec::new(),
                category_count: 0,
                expanded_count: 0,
                collapsed_categories: HashSet::new(),
                deleted_advertised_special_folders: HashSet::new(),
                restriction: None,
                bookmarks: HashMap::new(),
                next_bookmark: 1,
                position: 0,
            }
        }

        for valid in [request(0x00), request(0x01)] {
            let mut table = table();
            let response = rop_find_row_response(
                &valid,
                Some(&mut table),
                &[],
                &[],
                &MapiMailStoreSnapshot::empty(),
                Uuid::nil(),
            );
            assert_ne!(
                u32::from_le_bytes(response[2..6].try_into().unwrap()),
                0x8007_0057
            );
        }

        for invalid in [request(0x02), request(0x80)] {
            let mut table = table();
            let response = rop_find_row_response(
                &invalid,
                Some(&mut table),
                &[],
                &[],
                &MapiMailStoreSnapshot::empty(),
                Uuid::nil(),
            );
            assert_eq!(&response[..2], &[0x4F, 0x00]);
            assert_eq!(
                u32::from_le_bytes(response[2..6].try_into().unwrap()),
                0x8007_0057
            );
        }
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
                subject: "Pinned Inbox".to_string(),
                target_folder_id: Some(INBOX_FOLDER_ID),
                shortcut_type: 0,
                flags: 0,
                save_stamp: 0,
                section: 1,
                ordinal: 0x81,
                group_header_id: Some(default_wlink_group_uuid()),
                group_name: "Mail".to_string(),
            },
        ]);
        let mut table = MapiObject::ContentsTable {
            folder_id: COMMON_VIEWS_FOLDER_ID,
            associated: true,
            columns: vec![PID_TAG_SUBJECT_W],
            columns_set: false,
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
        assert_eq!(response.len(), 6);
        assert_eq!(table_position(&table), Some(0));
    }

    #[test]
    fn contents_find_row_matches_message_search_key() {
        let mailbox_id = Uuid::from_u128(0x3333);
        let email_id = Uuid::from_u128(0x4444);
        crate::mapi::identity::remember_mapi_identity(
            email_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 444,
            ),
        );
        let search_key = crate::mapi_mailstore::source_key_for_uuid(&email_id);
        let email = JmapEmail {
            id: email_id,
            thread_id: Uuid::from_u128(0x5555),
            mailbox_id,
            mailbox_role: "sent".to_string(),
            mailbox_name: "Sent".to_string(),
            modseq: 7,
            mailbox_ids: vec![mailbox_id],
            mailbox_states: vec![lpe_storage::JmapEmailMailboxState {
                mailbox_id,
                role: "sent".to_string(),
                name: "Sent".to_string(),
                modseq: 7,
                unread: false,
                flagged: false,
                followup_flag_status: "none".to_string(),
                followup_icon: 0,
                todo_item_flags: 0,
                followup_request: String::new(),
                followup_start_at: None,
                followup_due_at: None,
                followup_completed_at: None,
                reminder_set: false,
                reminder_at: None,
                reminder_dismissed_at: None,
                swapped_todo_store_id: None,
                swapped_todo_data: None,
                categories: Vec::new(),
                draft: false,
            }],
            received_at: "2026-05-20T10:00:00Z".to_string(),
            sent_at: Some("2026-05-20T10:00:00Z".to_string()),
            from_address: "sender@example.test".to_string(),
            from_display: Some("Sender".to_string()),
            sender_address: None,
            sender_display: None,
            sender_authorization_kind: "self".to_string(),
            submitted_by_account_id: Uuid::nil(),
            to: Vec::new(),
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: "Search key probe".to_string(),
            preview: "Preview".to_string(),
            body_text: "Body".to_string(),
            body_html_sanitized: None,
            unread: false,
            flagged: false,
            followup_flag_status: "none".to_string(),
            followup_icon: 0,
            todo_item_flags: 0,
            followup_request: String::new(),
            followup_start_at: None,
            followup_due_at: None,
            followup_completed_at: None,
            reminder_set: false,
            reminder_at: None,
            reminder_dismissed_at: None,
            swapped_todo_store_id: None,
            swapped_todo_data: None,
            categories: Vec::new(),
            has_attachments: false,
            size_octets: 128,
            internet_message_id: Some("<search-key-probe@example.test>".to_string()),
            mime_blob_ref: None,
            delivery_status: "stored".to_string(),
        };
        let mut table = MapiObject::ContentsTable {
            folder_id: SENT_FOLDER_ID,
            associated: false,
            columns: vec![PID_TAG_SEARCH_KEY, PID_TAG_SUBJECT_W],
            columns_set: true,
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
        restriction.extend_from_slice(&PID_TAG_SEARCH_KEY.to_le_bytes());
        restriction.extend_from_slice(&PID_TAG_SEARCH_KEY.to_le_bytes());
        write_rop_binary(&mut restriction, &search_key);
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

        let response = rop_find_row_response(
            &request,
            Some(&mut table),
            &[],
            &[email],
            &MapiMailStoreSnapshot::empty(),
            Uuid::nil(),
        );

        assert_eq!(response[0], RopId::FindRow.as_u8());
        assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
        assert_eq!(response[7], 1);
        assert!(response
            .windows(search_key.len())
            .any(|window| window == search_key.as_slice()));
    }

    #[test]
    fn contacts_contents_find_row_matches_display_name() {
        let account_id = Uuid::from_u128(0x7171);
        let contact_id = Uuid::from_u128(0x7172);
        crate::mapi::identity::remember_mapi_identity(
            contact_id,
            crate::mapi::identity::mapi_store_id(0x7172),
        );
        let rights = CollaborationRights {
            may_read: true,
            may_write: true,
            may_delete: true,
            may_share: true,
        };
        let collection = CollaborationCollection {
            id: "default".to_string(),
            kind: "contacts".to_string(),
            owner_account_id: account_id,
            owner_email: "test@example.test".to_string(),
            owner_display_name: "Test".to_string(),
            display_name: "Contacts".to_string(),
            is_owned: true,
            rights: rights.clone(),
        };
        let contact = AccessibleContact {
            id: contact_id,
            collection_id: collection.id.clone(),
            owner_account_id: account_id,
            owner_email: "test@example.test".to_string(),
            owner_display_name: "Test".to_string(),
            rights,
            name: "Denis Ducret".to_string(),
            role: String::new(),
            email: "denis@example.test".to_string(),
            phone: String::new(),
            team: String::new(),
            notes: String::new(),
            ..Default::default()
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
        );
        let mut table = MapiObject::ContentsTable {
            folder_id: CONTACTS_FOLDER_ID,
            associated: false,
            columns: vec![PID_TAG_MID, PID_TAG_DISPLAY_NAME_W, PID_TAG_MESSAGE_CLASS_W],
            columns_set: true,
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
        restriction.extend_from_slice(&PID_TAG_DISPLAY_NAME_W.to_le_bytes());
        restriction.extend_from_slice(&PID_TAG_DISPLAY_NAME_W.to_le_bytes());
        write_utf16z(&mut restriction, "Denis Ducret");
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
        assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
        assert_eq!(response[7], 1);
        let name = "Denis Ducret"
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect::<Vec<_>>();
        assert!(response
            .windows(name.len())
            .any(|window| window == name.as_slice()));
    }

    #[test]
    fn calendar_contents_find_row_matches_outlook_date_window() {
        let account_id = Uuid::from_u128(0x8181);
        let event_id = Uuid::from_u128(0x8182);
        crate::mapi::identity::remember_mapi_identity(
            event_id,
            crate::mapi::identity::mapi_store_id(0x8182),
        );
        let mut event = default_event_for_mapping(account_id, "default");
        event.id = event_id;
        event.title = "Project review".to_string();
        event.date = "2026-06-01".to_string();
        event.time = "10:00".to_string();
        event.duration_minutes = 60;
        let start = match event_property_value(
            &event,
            mapi_item_id(&event.id),
            CALENDAR_FOLDER_ID,
            PID_LID_APPOINTMENT_START_WHOLE_TAG,
        ) {
            Some(MapiValue::I64(value)) => value,
            _ => panic!("event start filetime missing"),
        };
        let end = match event_property_value(
            &event,
            mapi_item_id(&event.id),
            CALENDAR_FOLDER_ID,
            PID_LID_APPOINTMENT_END_WHOLE_TAG,
        ) {
            Some(MapiValue::I64(value)) => value,
            _ => panic!("event end filetime missing"),
        };
        let snapshot = MapiMailStoreSnapshot::new(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec![event],
            Vec::new(),
            Vec::new(),
        );
        let mut table = MapiObject::ContentsTable {
            folder_id: CALENDAR_FOLDER_ID,
            associated: false,
            columns: vec![PID_TAG_MID, PID_TAG_SUBJECT_W, PID_TAG_MESSAGE_CLASS_W],
            columns_set: true,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        };
        let mut restriction = vec![MapiRestrictionType::Or as u8];
        restriction.extend_from_slice(&2u16.to_le_bytes());
        restriction.push(MapiRestrictionType::Property as u8);
        restriction.push(0x04);
        restriction.extend_from_slice(&0x8021_000Bu32.to_le_bytes());
        restriction.extend_from_slice(&0x8021_000Bu32.to_le_bytes());
        restriction.push(1);
        restriction.push(MapiRestrictionType::And as u8);
        restriction.extend_from_slice(&2u16.to_le_bytes());
        restriction.push(MapiRestrictionType::Property as u8);
        restriction.push(0x03);
        restriction.extend_from_slice(&PID_LID_APPOINTMENT_START_WHOLE_TAG.to_le_bytes());
        restriction.extend_from_slice(&PID_LID_APPOINTMENT_START_WHOLE_TAG.to_le_bytes());
        restriction.extend_from_slice(&(start - 1).to_le_bytes());
        restriction.push(MapiRestrictionType::Property as u8);
        restriction.push(0x01);
        restriction.extend_from_slice(&PID_LID_APPOINTMENT_END_WHOLE_TAG.to_le_bytes());
        restriction.extend_from_slice(&PID_LID_APPOINTMENT_END_WHOLE_TAG.to_le_bytes());
        restriction.extend_from_slice(&(end + 1).to_le_bytes());
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
        assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
        assert_eq!(response[7], 1);
        assert_response_contains_utf16(&response, "Project review");
        assert_response_contains_utf16(&response, "IPM.Appointment");
    }

    #[test]
    fn common_views_find_row_returns_default_compact_named_view() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let snapshot = MapiMailStoreSnapshot::empty();
        let mut table = MapiObject::ContentsTable {
            folder_id: COMMON_VIEWS_FOLDER_ID,
            associated: true,
            columns: vec![
                PID_TAG_SUBJECT_W,
                PID_TAG_MESSAGE_CLASS_W,
                PID_TAG_VIEW_DESCRIPTOR_FLAGS,
                PID_TAG_VIEW_DESCRIPTOR_VERSION,
                PID_TAG_VIEW_DESCRIPTOR_FOLDER_TYPE,
            ],
            columns_set: true,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        };
        let mut restriction = vec![MapiRestrictionType::And as u8];
        restriction.extend_from_slice(&5u16.to_le_bytes());
        restriction.push(MapiRestrictionType::Property as u8);
        restriction.push(0x04);
        restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
        restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
        write_utf16z(&mut restriction, "IPM.Microsoft.FolderDesign.NamedView");
        restriction.push(MapiRestrictionType::Bitmask as u8);
        restriction.push(1);
        restriction.extend_from_slice(&PID_TAG_VIEW_DESCRIPTOR_FLAGS.to_le_bytes());
        restriction.extend_from_slice(&1u32.to_le_bytes());
        restriction.push(MapiRestrictionType::Property as u8);
        restriction.push(0x04);
        restriction.extend_from_slice(&PID_TAG_VIEW_DESCRIPTOR_VERSION.to_le_bytes());
        restriction.extend_from_slice(&PID_TAG_VIEW_DESCRIPTOR_VERSION.to_le_bytes());
        restriction.extend_from_slice(&8u32.to_le_bytes());
        restriction.push(MapiRestrictionType::Or as u8);
        restriction.extend_from_slice(&2u16.to_le_bytes());
        restriction.push(MapiRestrictionType::Content as u8);
        restriction.extend_from_slice(&0u32.to_le_bytes());
        restriction.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
        restriction.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
        write_utf16z(&mut restriction, "Compact");
        restriction.push(MapiRestrictionType::Property as u8);
        restriction.push(0x04);
        restriction.extend_from_slice(&PID_TAG_VIEW_DESCRIPTOR_FLAGS.to_le_bytes());
        restriction.extend_from_slice(&PID_TAG_VIEW_DESCRIPTOR_FLAGS.to_le_bytes());
        restriction.extend_from_slice(&14_745_605u32.to_le_bytes());
        restriction.push(MapiRestrictionType::Property as u8);
        restriction.push(0x04);
        restriction.extend_from_slice(&PID_TAG_VIEW_DESCRIPTOR_FOLDER_TYPE.to_le_bytes());
        restriction.extend_from_slice(&PID_TAG_VIEW_DESCRIPTOR_FOLDER_TYPE.to_le_bytes());
        restriction.extend_from_slice(&16u16.to_le_bytes());
        restriction.extend_from_slice(&[
            0x00, 0x78, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x46,
        ]);
        let mut payload = vec![0];
        payload.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
        payload.extend_from_slice(&restriction);
        payload.push(0);
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
        assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
        assert_response_contains_utf16(&response, "Compact");
        assert!(response
            .windows(4)
            .any(|window| window == 14_745_605u32.to_le_bytes()));
    }

    #[test]
    fn common_views_find_row_returns_default_sent_to_named_view() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let snapshot = MapiMailStoreSnapshot::empty();
        let mut table = MapiObject::ContentsTable {
            folder_id: COMMON_VIEWS_FOLDER_ID,
            associated: true,
            columns: vec![
                PID_TAG_MID,
                PID_TAG_INST_ID,
                PID_TAG_SUBJECT_W,
                PID_TAG_MESSAGE_CLASS_W,
                PID_TAG_VIEW_DESCRIPTOR_FLAGS,
                PID_TAG_VIEW_DESCRIPTOR_VERSION,
                PID_TAG_VIEW_DESCRIPTOR_FOLDER_TYPE,
            ],
            columns_set: true,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        };
        let mut restriction = vec![MapiRestrictionType::And as u8];
        restriction.extend_from_slice(&5u16.to_le_bytes());
        restriction.push(MapiRestrictionType::Property as u8);
        restriction.push(0x04);
        restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
        restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
        write_utf16z(&mut restriction, "IPM.Microsoft.FolderDesign.NamedView");
        restriction.push(MapiRestrictionType::Bitmask as u8);
        restriction.push(1);
        restriction.extend_from_slice(&PID_TAG_VIEW_DESCRIPTOR_FLAGS.to_le_bytes());
        restriction.extend_from_slice(&1u32.to_le_bytes());
        restriction.push(MapiRestrictionType::Property as u8);
        restriction.push(0x04);
        restriction.extend_from_slice(&PID_TAG_VIEW_DESCRIPTOR_VERSION.to_le_bytes());
        restriction.extend_from_slice(&PID_TAG_VIEW_DESCRIPTOR_VERSION.to_le_bytes());
        restriction.extend_from_slice(&8u32.to_le_bytes());
        restriction.push(MapiRestrictionType::Or as u8);
        restriction.extend_from_slice(&2u16.to_le_bytes());
        restriction.push(MapiRestrictionType::Content as u8);
        restriction.extend_from_slice(&0u32.to_le_bytes());
        restriction.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
        restriction.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
        write_utf16z(&mut restriction, "Sent To");
        restriction.push(MapiRestrictionType::Property as u8);
        restriction.push(0x04);
        restriction.extend_from_slice(&PID_TAG_VIEW_DESCRIPTOR_FLAGS.to_le_bytes());
        restriction.extend_from_slice(&PID_TAG_VIEW_DESCRIPTOR_FLAGS.to_le_bytes());
        restriction.extend_from_slice(&15_269_893u32.to_le_bytes());
        restriction.push(MapiRestrictionType::Property as u8);
        restriction.push(0x04);
        restriction.extend_from_slice(&PID_TAG_VIEW_DESCRIPTOR_FOLDER_TYPE.to_le_bytes());
        restriction.extend_from_slice(&PID_TAG_VIEW_DESCRIPTOR_FOLDER_TYPE.to_le_bytes());
        restriction.extend_from_slice(&16u16.to_le_bytes());
        restriction.extend_from_slice(&[
            0x00, 0x78, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x46,
        ]);
        let mut payload = vec![0];
        payload.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
        payload.extend_from_slice(&restriction);
        payload.push(0);
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
        assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
        assert_response_contains_utf16(&response, "Sent To");
        assert!(response
            .windows(4)
            .any(|window| window == 15_269_893u32.to_le_bytes()));
    }

    #[test]
    fn common_views_find_row_matches_mail_wlink_folder_type() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let shortcut_id = Uuid::from_u128(0x6d617069_776c_496e_8000_000000000003);
        crate::mapi::identity::remember_mapi_identity(
            shortcut_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 103,
            ),
        );
        let snapshot = MapiMailStoreSnapshot::empty().with_navigation_shortcuts(vec![
            crate::store::MapiNavigationShortcutRecord {
                id: shortcut_id,
                account_id,
                subject: "Pinned Inbox".to_string(),
                target_folder_id: Some(INBOX_FOLDER_ID),
                shortcut_type: 0,
                flags: 0,
                save_stamp: 0,
                section: 1,
                ordinal: 0x81,
                group_header_id: Some(default_wlink_group_uuid()),
                group_name: "Mail".to_string(),
            },
        ]);
        let mut table = MapiObject::ContentsTable {
            folder_id: COMMON_VIEWS_FOLDER_ID,
            associated: true,
            columns: vec![PID_TAG_SUBJECT_W, 0x684F_0102],
            columns_set: true,
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
        restriction.extend_from_slice(&0x684F_0102u32.to_le_bytes());
        restriction.extend_from_slice(&0x684F_0102u32.to_le_bytes());
        restriction.extend_from_slice(&16u16.to_le_bytes());
        restriction.extend_from_slice(&[
            0x0C, 0x78, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x46,
        ]);
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
        assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
        assert_eq!(response[7], 1);
        assert_response_contains_utf16(&response, "Pinned Inbox");
    }

    #[test]
    fn common_views_query_rows_uses_account_bound_wlink_entry_ids() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let snapshot = common_views_sort_snapshot(account_id);
        let mut table = MapiObject::ContentsTable {
            folder_id: COMMON_VIEWS_FOLDER_ID,
            associated: true,
            columns: vec![PID_TAG_WLINK_ENTRY_ID],
            columns_set: false,
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
    fn common_views_wlink_query_rows_include_named_views() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let snapshot = common_views_sort_snapshot(account_id);
        let mut table = MapiObject::ContentsTable {
            folder_id: COMMON_VIEWS_FOLDER_ID,
            associated: true,
            columns: default_navigation_shortcut_property_tags(),
            columns_set: true,
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

        let (_, projected_total) =
            table_position_and_count(Some(&table), &[], &[], &snapshot, account_id);
        assert_eq!(
            projected_total,
            snapshot.common_views_table_messages().count()
        );

        let response =
            rop_query_rows_response(&request, Some(&mut table), &[], &[], &snapshot, account_id);

        assert_response_contains_utf16(&response, "Alpha");
        assert_response_contains_utf16(&response, "IPM.Microsoft.FolderDesign.NamedView");
        assert_response_contains_utf16(&response, "Compact");
    }

    #[test]
    fn common_views_restricted_named_view_query_rows_remain_available() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let snapshot = common_views_sort_snapshot(account_id);
        let mut table = MapiObject::ContentsTable {
            folder_id: COMMON_VIEWS_FOLDER_ID,
            associated: true,
            columns: default_navigation_shortcut_property_tags(),
            columns_set: true,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: Some(MapiRestriction::Property {
                relop: 0x04,
                property_tag: PID_TAG_MESSAGE_CLASS_W,
                value: MapiValue::String("IPM.Microsoft.FolderDesign.NamedView".to_string()),
            }),
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

        assert_response_contains_utf16(&response, "IPM.Microsoft.FolderDesign.NamedView");
        assert_response_contains_utf16(&response, "Compact");
    }

    #[test]
    fn common_views_query_rows_uses_wlink_sort_order() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let snapshot = common_views_sort_snapshot(account_id);
        let mut table = MapiObject::ContentsTable {
            folder_id: COMMON_VIEWS_FOLDER_ID,
            associated: true,
            columns: vec![PID_TAG_SUBJECT_W],
            columns_set: true,
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
    fn inbox_associated_find_row_suppresses_outlook_eas_config() {
        assert_inbox_associated_find_row_no_match_for_message_class("IPM.Configuration.EAS");
    }

    #[test]
    fn inbox_associated_find_row_returns_outlook_elc_config() {
        assert_inbox_associated_find_row_returns_message_class("IPM.Configuration.ELC");
    }

    #[test]
    fn inbox_associated_find_row_returns_folder_default_named_view() {
        let response = inbox_associated_find_row_response_for_message_class(
            "IPM.Microsoft.FolderDesign.NamedView",
        );

        assert_eq!(response[0], RopId::FindRow.as_u8());
        assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
        assert_response_contains_utf16(&response, "IPM.Microsoft.FolderDesign.NamedView");
    }

    #[test]
    fn inbox_associated_find_row_returns_outlook_sharing_configuration() {
        assert_inbox_associated_find_row_returns_message_class("IPM.Sharing.Configuration");
    }

    #[test]
    fn inbox_associated_exact_virtual_find_row_filters_followup_query_rows() {
        let snapshot = MapiMailStoreSnapshot::empty();
        let mut table = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: true,
            columns: vec![PID_TAG_MESSAGE_CLASS_W],
            columns_set: true,
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
        let mut restriction = vec![MapiRestrictionType::Content as u8];
        restriction.extend_from_slice(&0u32.to_le_bytes());
        restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
        restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
        write_utf16z(&mut restriction, "IPM.Sharing.Configuration");
        let mut payload = vec![0];
        payload.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
        payload.extend_from_slice(&restriction);
        payload.push(1);
        payload.extend_from_slice(&0u16.to_le_bytes());
        let find_request = RopRequest {
            rop_id: RopId::FindRow.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload,
        };

        let find_response = rop_find_row_response(
            &find_request,
            Some(&mut table),
            &[],
            &[],
            &snapshot,
            Uuid::nil(),
        );

        assert_eq!(find_response[0], RopId::FindRow.as_u8());
        assert_eq!(find_response[7], 1);
        assert_response_contains_utf16(&find_response, "IPM.Sharing.Configuration");

        let query_request = RopRequest {
            rop_id: RopId::QueryRows.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: vec![0, 1, 10, 0],
        };
        let query_response = rop_query_rows_response(
            &query_request,
            Some(&mut table),
            &[],
            &[],
            &snapshot,
            Uuid::nil(),
        );

        assert_eq!(query_response[0], RopId::QueryRows.as_u8());
        assert_eq!(
            u16::from_le_bytes([query_response[7], query_response[8]]),
            1
        );
        assert_response_contains_utf16(&query_response, "IPM.Sharing.Configuration");
    }

    #[test]
    fn inbox_associated_find_row_does_not_return_empty_virtual_rule_organizer() {
        assert_inbox_associated_find_row_no_match_for_message_class("IPM.RuleOrganizer");
    }

    #[test]
    fn inbox_associated_find_row_returns_outlook_sharing_index() {
        assert_inbox_associated_find_row_returns_message_class("IPM.Sharing.Index");
    }

    #[test]
    fn inbox_associated_find_row_returns_outlook_aggregation_config() {
        let snapshot = MapiMailStoreSnapshot::empty();
        let mut table = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: true,
            columns: vec![
                PID_TAG_FOLDER_ID,
                PID_TAG_MID,
                PID_TAG_INST_ID,
                PID_TAG_INSTANCE_NUM,
                PID_TAG_MESSAGE_CLASS_W,
                0x81AB_001F,
                0x81AC_001F,
                0x81A1_0048,
                0x81ED_0003,
                0x8AA6_0003,
            ],
            columns_set: true,
            sort_orders: vec![
                MapiSortOrder {
                    property_tag: PID_TAG_MESSAGE_CLASS_W,
                    order: 0,
                },
                MapiSortOrder {
                    property_tag: PID_TAG_LAST_MODIFICATION_TIME,
                    order: 1,
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
        let mut restriction = vec![MapiRestrictionType::Content as u8];
        restriction.extend_from_slice(&0u32.to_le_bytes());
        restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
        restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
        write_utf16z(&mut restriction, "IPM.Aggregation");
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
        assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
        assert_eq!(response[6], 0);
        assert_eq!(response[7], 1);
        assert_response_contains_utf16(&response, "IPM.Aggregation");
    }

    #[test]
    fn inbox_associated_find_row_returns_sharing_index_private_defaults() {
        let snapshot = MapiMailStoreSnapshot::empty();
        assert!(snapshot
            .associated_config_messages_for_folder(INBOX_FOLDER_ID)
            .into_iter()
            .all(|message| message.message_class != "IPM.Sharing.Index"));
    }

    #[test]
    fn inbox_associated_broad_configuration_find_row_ignores_virtual_defaults() {
        let snapshot = MapiMailStoreSnapshot::empty();
        let mut table = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: true,
            columns: vec![
                PID_TAG_FOLDER_ID,
                PID_TAG_MID,
                PID_TAG_INST_ID,
                PID_TAG_INSTANCE_NUM,
                PID_TAG_ROAMING_DATATYPES,
                PID_TAG_MESSAGE_CLASS_W,
                0x685D_0003,
                PID_TAG_LAST_MODIFICATION_TIME,
            ],
            columns_set: true,
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
        assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
        assert_response_contains_utf16(&response, "IPM.Configuration.AccountPrefs");
        assert!(utf16_position(&response, "IPM.Configuration.MessageListSettings").is_none());
        assert!(utf16_position(&response, "IPM.Configuration.EAS").is_none());
        assert!(utf16_position(&response, "IPM.Configuration.ELC").is_none());
        assert_eq!(table_position(&table), Some(0));
    }

    #[test]
    fn quick_step_associated_find_row_returns_custom_action_config() {
        let snapshot = MapiMailStoreSnapshot::empty();
        let mut table = MapiObject::ContentsTable {
            folder_id: QUICK_STEP_SETTINGS_FOLDER_ID,
            associated: true,
            columns: vec![PID_TAG_MESSAGE_CLASS_W, 0x7C08_0102],
            columns_set: true,
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
        let mut restriction = vec![MapiRestrictionType::Property as u8, 0x04];
        restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
        restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
        write_utf16z(&mut restriction, "IPM.Microsoft.CustomAction");
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
        write_utf16z(&mut encoded_message_class, "IPM.Microsoft.CustomAction");
        assert!(response
            .windows(encoded_message_class.len())
            .any(|window| window == encoded_message_class.as_slice()));
        assert!(response
            .windows(b"<?xml version=\"1.0\" encoding=\"utf-8\"?>".len())
            .any(|window| window == b"<?xml version=\"1.0\" encoding=\"utf-8\"?>"));
    }

    #[test]
    fn contacts_associated_find_row_returns_osc_contact_sync_config() {
        assert_contact_folder_associated_find_row_returns_osc_contact_sync(CONTACTS_FOLDER_ID);
    }

    #[test]
    fn contacts_associated_find_row_returns_contact_link_timestamp_config() {
        assert_contact_folder_associated_find_row_returns_config(
            CONTACTS_FOLDER_ID,
            "IPM.Microsoft.ContactLink.TimeStamp",
            &MapiMailStoreSnapshot::empty(),
        );
    }

    #[test]
    fn contacts_associated_find_row_preserves_table_position_for_contact_link_timestamp() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let contact_prefs_id = Uuid::from_u128(0x6d617069_6370_7266_8000_000000000001);
        crate::mapi::identity::remember_mapi_identity(
            contact_prefs_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 83,
            ),
        );
        let snapshot = MapiMailStoreSnapshot::empty().with_associated_configs(vec![
            crate::store::MapiAssociatedConfigRecord {
                id: contact_prefs_id,
                account_id,
                folder_id: CONTACTS_FOLDER_ID,
                message_class: "IPM.Configuration.ContactPrefs".to_string(),
                subject: "IPM.Configuration.ContactPrefs".to_string(),
                properties_json: serde_json::json!({}),
            },
        ]);
        let mut table = MapiObject::ContentsTable {
            folder_id: CONTACTS_FOLDER_ID,
            associated: true,
            columns: vec![PID_TAG_MESSAGE_CLASS_W],
            columns_set: true,
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
        let mut restriction = vec![MapiRestrictionType::Property as u8, 0x04];
        restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
        restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
        write_utf16z(&mut restriction, "IPM.Microsoft.ContactLink.TimeStamp");
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
        assert_response_contains_utf16(&response, "IPM.Microsoft.ContactLink.TimeStamp");
        assert_eq!(table_position(&table), Some(1));

        let position_response = rop_query_position_response(
            &RopRequest {
                rop_id: RopId::QueryPosition.as_u8(),
                input_handle_index: Some(0),
                output_handle_index: None,
                payload: Vec::new(),
            },
            Some(&table),
            &[],
            &[],
            &snapshot,
            Uuid::nil(),
        );
        assert_eq!(position_response[0], RopId::QueryPosition.as_u8());
        assert_eq!(
            u32::from_le_bytes(position_response[6..10].try_into().unwrap()),
            1
        );
        assert_eq!(
            u32::from_le_bytes(position_response[10..14].try_into().unwrap()),
            4
        );
    }

    #[test]
    fn suggested_contacts_associated_find_row_does_not_return_empty_osc_contact_sync_config() {
        assert_contact_folder_associated_find_row_does_not_return_config(
            SUGGESTED_CONTACTS_FOLDER_ID,
            "IPM.Microsoft.OSC.ContactSync",
            &MapiMailStoreSnapshot::empty(),
        );
    }

    #[test]
    fn suggested_contacts_associated_table_does_not_expose_folder_default_named_view() {
        let rows = associated_table_rows(
            SUGGESTED_CONTACTS_FOLDER_ID,
            &MapiMailStoreSnapshot::empty(),
            None,
            Uuid::nil(),
        );

        assert_eq!(rows.len(), 1);
        assert!(matches!(rows[0], AssociatedTableRow::Config(_)));
        assert_eq!(
            associated_folder_message_count(
                SUGGESTED_CONTACTS_FOLDER_ID,
                &MapiMailStoreSnapshot::empty()
            ),
            1
        );
    }

    #[test]
    fn inbox_associated_table_exposes_folder_default_named_view() {
        let restriction = MapiRestriction::Property {
            relop: 0x04,
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            value: MapiValue::String(
                crate::mapi_store::OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_CLASS.to_string(),
            ),
        };
        let rows = associated_table_rows(
            INBOX_FOLDER_ID,
            &MapiMailStoreSnapshot::empty(),
            Some(&restriction),
            Uuid::nil(),
        );

        assert_eq!(rows.len(), 1);
        assert!(matches!(rows[0], AssociatedTableRow::NamedView(_)));
        assert_eq!(
            associated_table_row_id(&rows[0]),
            crate::mapi_store::OUTLOOK_DEFAULT_FOLDER_NAMED_VIEW_ID
        );
        assert_eq!(
            restricted_associated_folder_message_count(
                INBOX_FOLDER_ID,
                &MapiMailStoreSnapshot::empty(),
                Some(&restriction),
                Uuid::nil()
            ),
            1
        );
    }

    #[test]
    fn quick_contacts_associated_find_row_returns_osc_contact_sync_config() {
        assert_contact_folder_associated_find_row_returns_osc_contact_sync(
            QUICK_CONTACTS_FOLDER_ID,
        );
    }

    #[test]
    fn im_contact_list_associated_find_row_returns_osc_contact_sync_config() {
        assert_contact_folder_associated_find_row_returns_osc_contact_sync(
            IM_CONTACT_LIST_FOLDER_ID,
        );
    }

    #[test]
    fn dynamic_contacts_associated_find_row_returns_osc_contact_sync_config() {
        let folder_id = crate::mapi::identity::mapi_store_id(0x4e);
        let collection = CollaborationCollection {
            id: "outlook-log-dynamic-contacts-table".to_string(),
            kind: "contacts".to_string(),
            display_name: "Contacts".to_string(),
            owner_account_id: Uuid::from_u128(0x4e),
            owner_email: "owner@example.test".to_string(),
            owner_display_name: "Owner".to_string(),
            is_owned: true,
            rights: CollaborationRights {
                may_read: true,
                may_write: true,
                may_delete: true,
                may_share: true,
            },
        };
        crate::mapi::identity::remember_mapi_identity(
            crate::mapi_store::collaboration_folder_identity_canonical_id(
                crate::mapi_store::MapiCollaborationFolderKind::Contacts,
                &collection,
            )
            .unwrap(),
            folder_id,
        );
        let snapshot = MapiMailStoreSnapshot::new(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec![collection],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );

        assert_contact_folder_associated_find_row_returns_osc_contact_sync_for_snapshot(
            folder_id, &snapshot,
        );

        let folder = snapshot
            .collaboration_folder_for_id(folder_id)
            .expect("dynamic contacts folder");
        assert_eq!(
            hierarchy_row_expected_container_class(&HierarchyRow::Collaboration(folder)),
            Some("IPF.Contact")
        );
        let row = serialize_hierarchy_row(
            HierarchyRow::Collaboration(folder),
            &[],
            &snapshot,
            &[PID_TAG_ASSOCIATED_CONTENT_COUNT],
            Uuid::nil(),
        );

        assert_eq!(u32::from_le_bytes(row.try_into().unwrap()), 2);
    }

    #[test]
    fn mailbox_backed_quick_contacts_associated_find_row_returns_osc_contact_sync_config() {
        let folder_id = crate::mapi::identity::mapi_store_id(0x55);
        let mailbox_id = Uuid::parse_str("aaaaaaaa-7777-4111-8111-aaaaaaaaaaaa").unwrap();
        crate::mapi::identity::remember_mapi_identity(mailbox_id, folder_id);
        let snapshot = MapiMailStoreSnapshot::new(
            vec![JmapMailbox {
                id: mailbox_id,
                parent_id: None,
                role: String::new(),
                name: "Quick Contacts".to_string(),
                sort_order: 0,
                modseq: 1,
                total_emails: 0,
                unread_emails: 0,
                size_octets: 0,
                is_subscribed: true,
            }],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );

        assert_contact_folder_associated_find_row_returns_osc_contact_sync_for_snapshot(
            folder_id, &snapshot,
        );
    }

    #[test]
    fn empty_conversation_action_settings_find_row_returns_default_action() {
        let snapshot = MapiMailStoreSnapshot::empty();
        let mut table = MapiObject::ContentsTable {
            folder_id: CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
            associated: true,
            columns: vec![PID_TAG_MESSAGE_CLASS_W],
            columns_set: false,
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
        write_utf16z(&mut restriction, "IPM.ConversationAction");
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
        write_utf16z(&mut encoded_message_class, "IPM.ConversationAction");
        assert!(response
            .windows(encoded_message_class.len())
            .any(|window| window == encoded_message_class.as_slice()));
    }

    #[test]
    fn conversation_action_settings_find_row_honors_restriction() {
        let snapshot = MapiMailStoreSnapshot::empty();
        let mut table = MapiObject::ContentsTable {
            folder_id: CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
            associated: true,
            columns: vec![PID_TAG_MESSAGE_CLASS_W],
            columns_set: false,
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
        write_utf16z(&mut restriction, "IPM.NotConversationAction");
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
        assert_eq!(
            u32::from_le_bytes(response[2..6].try_into().unwrap()),
            0x8004_010F
        );
        assert_eq!(response.len(), 6);
        assert!(utf16_position(&response, "IPM.Configuration.AccountPrefs").is_none());
        assert!(utf16_position(&response, "IPM.ExtendedRule.Message").is_none());
    }

    #[test]
    fn microsoft_conversation_action_example_round_trips_fai_properties() {
        let conversation_id = Uuid::from_bytes([
            0xb7, 0xa2, 0xb5, 0xc4, 0xaa, 0x65, 0x1c, 0xf2, 0xd3, 0x8c, 0x62, 0x8c, 0x0e, 0xaf,
            0x56, 0xc4,
        ]);
        let move_folder_entry_id = vec![
            0x00, 0x00, 0x00, 0x00, 0x0c, 0x99, 0xf4, 0xed, 0xa2, 0xf1, 0xe4, 0x41, 0xb1, 0x5b,
            0x9b, 0x25, 0x10, 0x91, 0x3e, 0x9d, 0x02, 0x81, 0x00, 0x00,
        ];
        let move_store_entry_id = vec![
            0x00, 0x00, 0x00, 0x00, 0x38, 0xa1, 0xbb, 0x10, 0x05, 0xe5, 0x10, 0x1a, 0xa1, 0xbb,
            0x08, 0x00, 0x2b, 0x2a, 0x56, 0xc2, 0x00, 0x00, 0x6d, 0x73, 0x70, 0x73, 0x74, 0x2e,
            0x64, 0x6c, 0x6c, 0x00,
        ];
        let max_delivery_time = mapi_mailstore::filetime_from_rfc3339_utc("2009-02-17T23:31:42Z");
        let last_applied_time = mapi_mailstore::filetime_from_rfc3339_utc("2009-02-17T23:51:11Z");
        let mut properties = HashMap::new();
        properties.insert(
            PID_TAG_CONVERSATION_INDEX,
            MapiValue::Binary(conversation_index_for_uuid(conversation_id)),
        );
        properties.insert(
            PID_TAG_SUBJECT_W,
            MapiValue::String(
                "Conv.Action: Solidifying our proposal to Fabrikam, Inc.".to_string(),
            ),
        );
        properties.insert(
            PID_NAME_KEYWORDS_TAG,
            MapiValue::MultiString(vec![
                "Fabrikam".to_string(),
                "Business Proposals".to_string(),
            ]),
        );
        properties.insert(
            PID_LID_CONVERSATION_ACTION_MOVE_FOLDER_EID_TAG,
            MapiValue::Binary(move_folder_entry_id.clone()),
        );
        properties.insert(
            PID_LID_CONVERSATION_ACTION_MOVE_STORE_EID_TAG,
            MapiValue::Binary(move_store_entry_id.clone()),
        );
        properties.insert(
            PID_LID_CONVERSATION_ACTION_MAX_DELIVERY_TIME_TAG,
            MapiValue::U64(max_delivery_time),
        );
        properties.insert(
            PID_LID_CONVERSATION_ACTION_LAST_APPLIED_TIME_TAG,
            MapiValue::U64(last_applied_time),
        );
        properties.insert(
            PID_LID_CONVERSATION_ACTION_VERSION_TAG,
            MapiValue::I32(lpe_storage::CONVERSATION_ACTION_VERSION),
        );
        properties.insert(PID_LID_CONVERSATION_PROCESSED_TAG, MapiValue::I32(7));

        let action = conversation_action_from_mapi_properties(&properties);
        assert_eq!(action.id, conversation_id);
        assert_eq!(action.conversation_id, conversation_id);
        assert_eq!(
            action.subject,
            "Conv.Action: Solidifying our proposal to Fabrikam, Inc."
        );
        assert_eq!(
            action.move_folder_entry_id,
            Some(move_folder_entry_id.clone())
        );
        assert_eq!(
            action.move_store_entry_id,
            Some(move_store_entry_id.clone())
        );
        assert_eq!(
            action.max_delivery_time.as_deref(),
            Some("2009-02-17T23:31:00Z")
        );
        assert_eq!(
            action.last_applied_time.as_deref(),
            Some("2009-02-17T23:51:00Z")
        );
        assert_eq!(action.version, lpe_storage::CONVERSATION_ACTION_VERSION);
        assert_eq!(action.processed, 7);
        let categories: Vec<String> = serde_json::from_str(&action.categories_json).unwrap();
        assert_eq!(
            categories,
            vec!["Fabrikam".to_string(), "Business Proposals".to_string()]
        );

        let message = MapiConversationActionMessage {
            id: crate::mapi::identity::mapi_store_id(0x7fff_ffff_ffe8),
            folder_id: CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
            canonical_id: action.id,
            action,
        };

        assert_eq!(
            conversation_action_property_value(&message, PID_TAG_MESSAGE_CLASS_W),
            Some(MapiValue::String("IPM.ConversationAction".to_string()))
        );
        assert_eq!(
            conversation_action_property_value(&message, PID_TAG_CONVERSATION_INDEX),
            Some(MapiValue::Binary(conversation_index_for_uuid(
                conversation_id
            )))
        );
        assert_eq!(
            conversation_action_property_value(
                &message,
                PID_LID_CONVERSATION_ACTION_MOVE_FOLDER_EID_TAG
            ),
            Some(MapiValue::Binary(move_folder_entry_id))
        );
        assert_eq!(
            conversation_action_property_value(
                &message,
                PID_LID_CONVERSATION_ACTION_MOVE_STORE_EID_TAG
            ),
            Some(MapiValue::Binary(move_store_entry_id))
        );
        assert_eq!(
            conversation_action_property_value(&message, PID_LID_CONVERSATION_PROCESSED_TAG),
            Some(MapiValue::I32(7))
        );
    }

    #[test]
    fn inbox_associated_find_row_uses_sort_order() {
        let snapshot = inbox_associated_sort_snapshot();
        let mut table = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: true,
            columns: vec![PID_TAG_MESSAGE_CLASS_W],
            columns_set: true,
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
        assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
        assert_response_contains_utf16(&response, "IPM.Configuration.AccountPrefs");
        assert!(utf16_position(&response, "IPM.Configuration.MessageListSettings").is_none());
        assert!(utf16_position(&response, "IPM.ExtendedRule.Message").is_none());
    }

    #[test]
    fn inbox_associated_broad_configuration_find_row_filters_followup_query_rows() {
        let snapshot = inbox_associated_sort_snapshot();
        let mut table = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: true,
            columns: vec![PID_TAG_MESSAGE_CLASS_W],
            columns_set: true,
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
        let find_request = RopRequest {
            rop_id: RopId::FindRow.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload,
        };

        let find_response = rop_find_row_response(
            &find_request,
            Some(&mut table),
            &[],
            &[],
            &snapshot,
            Uuid::nil(),
        );
        assert_eq!(find_response[0], RopId::FindRow.as_u8());
        assert_eq!(
            u32::from_le_bytes(find_response[2..6].try_into().unwrap()),
            0
        );
        assert_response_contains_utf16(&find_response, "IPM.Configuration.AccountPrefs");
        assert!(utf16_position(&find_response, "IPM.Configuration.MessageListSettings").is_none());

        let query_request = RopRequest {
            rop_id: RopId::QueryRows.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: vec![0, 1, 50, 0],
        };
        let query_response = rop_query_rows_response(
            &query_request,
            Some(&mut table),
            &[],
            &[],
            &snapshot,
            Uuid::nil(),
        );

        assert_eq!(query_response[0], RopId::QueryRows.as_u8());
        assert_eq!(
            u16::from_le_bytes([query_response[7], query_response[8]]),
            2
        );
        assert!(utf16_position(&query_response, "IPM.Configuration.AccountPrefs").is_some());
        assert!(utf16_position(&query_response, "IPM.Configuration.EAS").is_none());
        assert!(utf16_position(&query_response, "IPM.Configuration.ELC").is_none());
        assert!(utf16_position(&query_response, "IPM.Configuration.MessageListSettings").is_some());
        assert!(utf16_position(&query_response, "IPM.RuleOrganizer").is_none());
        assert!(utf16_position(&query_response, "IPM.Sharing.Configuration").is_none());
        assert!(utf16_position(&query_response, "IPM.Microsoft.FolderDesign.NamedView").is_none());
    }

    #[test]
    fn inbox_associated_broad_find_row_resets_cursor_for_restricted_followup() {
        let account_id = Uuid::from_u128(0x73a6_121f_9c0d_423b_8fcb_7174f28e1608);
        let earlier_id = Uuid::from_u128(0x73a6_121f_9c0d_423b_8fcb_7174f28e1609);
        let persisted_id = Uuid::from_u128(0x73a6_121f_9c0d_423b_8fcb_7174f28e1610);
        crate::mapi::identity::remember_mapi_identity(
            earlier_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 800,
            ),
        );
        crate::mapi::identity::remember_mapi_identity(
            persisted_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 801,
            ),
        );
        let snapshot = MapiMailStoreSnapshot::empty().with_associated_configs(vec![
            crate::store::MapiAssociatedConfigRecord {
                id: earlier_id,
                account_id,
                folder_id: INBOX_FOLDER_ID,
                message_class: "IPM.Configuration.ClientOptions".to_string(),
                subject: "ClientOptions".to_string(),
                properties_json: serde_json::json!({}),
            },
            crate::store::MapiAssociatedConfigRecord {
                id: persisted_id,
                account_id,
                folder_id: INBOX_FOLDER_ID,
                message_class: "IPM.Configuration.MessageListSettings".to_string(),
                subject: "IPM.Configuration.MessageListSettings".to_string(),
                properties_json: serde_json::json!({
                    "0x001a001f": {
                        "type": "string",
                        "value": "IPM.Configuration.MessageListSettings"
                    },
                    "0x7c070102": {
                        "type": "binary",
                        "value": "3c786d6c2f3e"
                    }
                }),
            },
        ]);
        let mut table = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: true,
            columns: vec![PID_TAG_MESSAGE_CLASS_W],
            columns_set: true,
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
        let mut find_payload = vec![0];
        find_payload.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
        find_payload.extend_from_slice(&restriction);
        find_payload.push(1);
        find_payload.extend_from_slice(&0u16.to_le_bytes());
        let find_response = rop_find_row_response(
            &RopRequest {
                rop_id: RopId::FindRow.as_u8(),
                input_handle_index: Some(0),
                output_handle_index: None,
                payload: find_payload,
            },
            Some(&mut table),
            &[],
            &[],
            &snapshot,
            Uuid::nil(),
        );

        assert_eq!(find_response[0], RopId::FindRow.as_u8());
        assert_eq!(
            u32::from_le_bytes(find_response[2..6].try_into().unwrap()),
            0
        );
        assert_response_contains_utf16(&find_response, "IPM.Configuration.AccountPrefs");
        assert!(utf16_position(&find_response, "IPM.Configuration.MessageListSettings").is_none());
        assert_eq!(table_position(&table), Some(0));

        let query_response = rop_query_rows_response(
            &RopRequest {
                rop_id: RopId::QueryRows.as_u8(),
                input_handle_index: Some(0),
                output_handle_index: None,
                payload: vec![0, 1, 50, 0],
            },
            Some(&mut table),
            &[],
            &[],
            &snapshot,
            Uuid::nil(),
        );

        assert_eq!(query_response[0], RopId::QueryRows.as_u8());
        assert_eq!(
            u16::from_le_bytes([query_response[7], query_response[8]]),
            2
        );
        assert!(utf16_position(&query_response, "IPM.Configuration.ClientOptions").is_none());
        assert_response_contains_utf16(&query_response, "IPM.Configuration.AccountPrefs");
        assert_response_contains_utf16(&query_response, "IPM.Configuration.MessageListSettings");
    }

    #[test]
    fn inbox_associated_broad_configuration_restriction_projects_startup_configs() {
        let snapshot = MapiMailStoreSnapshot::empty();
        let restriction = MapiRestriction::Property {
            relop: 0x02,
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            value: MapiValue::String("IPM.Configuration.".to_string()),
        };

        let rows =
            associated_table_rows(INBOX_FOLDER_ID, &snapshot, Some(&restriction), Uuid::nil());
        let classes = rows
            .iter()
            .filter_map(associated_table_row_config)
            .map(|message| message.message_class.as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            classes,
            vec![
                "IPM.Configuration.AccountPrefs",
                "IPM.Configuration.MessageListSettings"
            ]
        );
    }

    #[test]
    fn inbox_associated_broad_configuration_restriction_projects_persisted_outlook_configs() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let autocomplete_id = Uuid::from_u128(0x6d617069_6175_746f_8000_000000000101);
        crate::mapi::identity::remember_mapi_identity(
            autocomplete_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 185,
            ),
        );
        let snapshot = MapiMailStoreSnapshot::empty().with_associated_configs(vec![
            crate::store::MapiAssociatedConfigRecord {
                id: autocomplete_id,
                account_id,
                folder_id: INBOX_FOLDER_ID,
                message_class: "IPM.Configuration.Autocomplete".to_string(),
                subject: "Autocomplete".to_string(),
                properties_json: serde_json::json!({
                    "0x7c070102": {"type": "binary", "value": "3c786d6c2f3e"}
                }),
            },
        ]);
        let restriction = MapiRestriction::Property {
            relop: 0x02,
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            value: MapiValue::String("IPM.Configuration.".to_string()),
        };

        let rows =
            associated_table_rows(INBOX_FOLDER_ID, &snapshot, Some(&restriction), Uuid::nil());
        let classes = rows
            .iter()
            .filter_map(associated_table_row_config)
            .map(|message| message.message_class.as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            classes,
            vec![
                "IPM.Configuration.Autocomplete",
                "IPM.Configuration.AccountPrefs",
                "IPM.Configuration.MessageListSettings"
            ]
        );
    }

    #[test]
    fn inbox_associated_broad_configuration_find_row_ignores_extended_rule_message() {
        let snapshot = inbox_associated_extended_rule_snapshot();
        let mut table = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: true,
            columns: vec![PID_TAG_MESSAGE_CLASS_W],
            columns_set: true,
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
        assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
        assert_response_contains_utf16(&response, "IPM.Configuration.AccountPrefs");
        assert!(utf16_position(&response, "IPM.Configuration.MessageListSettings").is_none());
        assert!(utf16_position(&response, "IPM.ExtendedRule.Message").is_none());
    }

    #[test]
    fn inbox_associated_query_rows_uses_sort_order() {
        let snapshot = inbox_associated_sort_snapshot();
        let mut table = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: true,
            columns: vec![PID_TAG_MESSAGE_CLASS_W],
            columns_set: true,
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
        assert_eq!(u16::from_le_bytes([response[7], response[8]]), 3);
        assert!(utf16_position(&response, "IPM.Configuration.AccountPrefs").is_some());
        assert!(utf16_position(&response, "IPM.Configuration.UMOLK.UserOptions").is_none());
        assert!(utf16_position(&response, "IPM.Microsoft.FolderDesign.NamedView").is_some());
        assert!(utf16_position(&response, "IPM.Configuration.MessageListSettings").is_some());
        assert!(utf16_position(&response, "IPM.Configuration.EAS").is_none());
        assert!(utf16_position(&response, "IPM.Configuration.ELC").is_none());
        assert!(utf16_position(&response, "IPM.Sharing.Configuration").is_none());
    }

    #[test]
    fn inbox_associated_query_rows_suppresses_extended_rule_message() {
        let snapshot = inbox_associated_extended_rule_snapshot();
        let mut table = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: true,
            columns: vec![PID_TAG_MESSAGE_CLASS_W],
            columns_set: true,
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
        assert_eq!(u16::from_le_bytes([response[7], response[8]]), 3);
        assert!(utf16_position(&response, "IPM.ExtendedRule.Message").is_none());
        assert!(utf16_position(&response, "IPM.Configuration.AccountPrefs").is_some());
        assert!(utf16_position(&response, "IPM.Microsoft.FolderDesign.NamedView").is_some());
        assert!(utf16_position(&response, "IPM.Configuration.MessageListSettings").is_some());
    }

    #[test]
    fn inbox_associated_query_rows_suppresses_duplicate_persisted_compact_named_view() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let account_prefs_id = Uuid::from_u128(0x6d617069_6163_6350_8000_000000000101);
        let persisted_view_id = Uuid::from_u128(0x6d617069_696e_4e76_8000_000000000101);
        let account_prefs_object_id = crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 181,
        );
        let persisted_view_object_id = crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 182,
        );
        crate::mapi::identity::remember_mapi_identity(account_prefs_id, account_prefs_object_id);
        crate::mapi::identity::remember_mapi_identity(persisted_view_id, persisted_view_object_id);
        let snapshot = MapiMailStoreSnapshot::empty().with_associated_configs(vec![
            crate::store::MapiAssociatedConfigRecord {
                id: account_prefs_id,
                account_id,
                folder_id: INBOX_FOLDER_ID,
                message_class: "IPM.Configuration.AccountPrefs".to_string(),
                subject: "Account prefs".to_string(),
                properties_json: serde_json::json!({
                    "0x7c070102": {"type": "binary", "value": "3c786d6c2f3e"}
                }),
            },
            crate::store::MapiAssociatedConfigRecord {
                id: persisted_view_id,
                account_id,
                folder_id: INBOX_FOLDER_ID,
                message_class: "IPM.Microsoft.FolderDesign.NamedView".to_string(),
                subject: "Compact".to_string(),
                properties_json: serde_json::json!({
                    "0x0e0b0102": {"type": "binary", "value": "010203"}
                }),
            },
        ]);
        let mut table = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: true,
            columns: vec![PID_TAG_MID, PID_TAG_SUBJECT_W, PID_TAG_MESSAGE_CLASS_W],
            columns_set: true,
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
        assert_eq!(u16::from_le_bytes([response[7], response[8]]), 4);
        assert!(utf16_position(&response, "IPM.Configuration.AccountPrefs").is_some());
        assert!(utf16_position(&response, "IPM.Microsoft.FolderDesign.NamedView").is_some());
        assert!(utf16_position(&response, "IPM.Configuration.MessageListSettings").is_some());
    }

    #[test]
    fn inbox_associated_query_rows_replaces_empty_persisted_compact_named_view() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let account_prefs_id = Uuid::from_u128(0x6d617069_6163_6350_8000_000000000111);
        let persisted_view_id = Uuid::from_u128(0x6d617069_696e_4e76_8000_000000000111);
        crate::mapi::identity::remember_mapi_identity(
            account_prefs_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 183,
            ),
        );
        crate::mapi::identity::remember_mapi_identity(
            persisted_view_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 184,
            ),
        );
        let snapshot = MapiMailStoreSnapshot::empty().with_associated_configs(vec![
            crate::store::MapiAssociatedConfigRecord {
                id: account_prefs_id,
                account_id,
                folder_id: INBOX_FOLDER_ID,
                message_class: "IPM.Configuration.AccountPrefs".to_string(),
                subject: "Account prefs".to_string(),
                properties_json: serde_json::json!({
                    "0x7c070102": {"type": "binary", "value": "3c786d6c2f3e"}
                }),
            },
            crate::store::MapiAssociatedConfigRecord {
                id: persisted_view_id,
                account_id,
                folder_id: INBOX_FOLDER_ID,
                message_class: "IPM.Microsoft.FolderDesign.NamedView".to_string(),
                subject: "Compact".to_string(),
                properties_json: serde_json::json!({}),
            },
        ]);
        let mut table = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: true,
            columns: vec![PID_TAG_MID, PID_TAG_SUBJECT_W, PID_TAG_MESSAGE_CLASS_W],
            columns_set: true,
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
        assert_eq!(u16::from_le_bytes([response[7], response[8]]), 3);
        assert!(utf16_position(&response, "IPM.Configuration.AccountPrefs").is_some());
        assert!(utf16_position(&response, "IPM.Microsoft.FolderDesign.NamedView").is_some());
        assert!(utf16_position(&response, "IPM.Configuration.MessageListSettings").is_some());
    }

    #[test]
    fn junk_associated_query_rows_exposes_default_named_view() {
        let snapshot = MapiMailStoreSnapshot::empty();
        let mut table = MapiObject::ContentsTable {
            folder_id: JUNK_FOLDER_ID,
            associated: true,
            columns: vec![PID_TAG_MESSAGE_CLASS_W],
            columns_set: true,
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
        assert_eq!(u16::from_le_bytes([response[7], response[8]]), 1);
        assert!(utf16_position(&response, "IPM.Microsoft.FolderDesign.NamedView").is_some());
    }

    #[test]
    fn contacts_associated_query_rows_expose_contact_default_named_view() {
        let snapshot = MapiMailStoreSnapshot::empty();
        let mut table = MapiObject::ContentsTable {
            folder_id: CONTACTS_FOLDER_ID,
            associated: true,
            columns: vec![PID_TAG_MESSAGE_CLASS_W],
            columns_set: true,
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
        assert_eq!(u16::from_le_bytes([response[7], response[8]]), 3);
        assert!(utf16_position(&response, "IPM.Microsoft.FolderDesign.NamedView").is_some());
    }

    #[test]
    fn calendar_associated_query_rows_expose_calendar_default_named_view() {
        let snapshot = MapiMailStoreSnapshot::empty();
        let mut table = MapiObject::ContentsTable {
            folder_id: CALENDAR_FOLDER_ID,
            associated: true,
            columns: vec![PID_TAG_MESSAGE_CLASS_W, PID_TAG_SUBJECT_W],
            columns_set: true,
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
        assert_eq!(u16::from_le_bytes([response[7], response[8]]), 1);
        assert!(utf16_position(&response, "IPM.Microsoft.FolderDesign.NamedView").is_some());
        assert_response_contains_utf16(&response, "Calendar");
        assert!(utf16_position(&response, "Compact").is_none());
    }

    #[test]
    fn inbox_associated_query_rows_suppresses_prefix_configuration_with_stored_stream() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let config_id = Uuid::from_u128(0x6d617069_6d6c_7343_8000_000000000099);
        crate::mapi::identity::remember_mapi_identity(
            config_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 82,
            ),
        );
        let snapshot = MapiMailStoreSnapshot::empty().with_associated_configs(vec![
            crate::store::MapiAssociatedConfigRecord {
                id: config_id,
                account_id,
                folder_id: INBOX_FOLDER_ID,
                message_class: "IPM.Configuration.MessageListSettings".to_string(),
                subject: "Message list settings".to_string(),
                properties_json: serde_json::json!({
                    "0x7c070102": {"type": "binary", "value": "3c786d6c2f3e"}
                }),
            },
        ]);
        let mut table = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: true,
            columns: vec![PID_TAG_MESSAGE_CLASS_W],
            columns_set: false,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: Some(outlook_configuration_prefix_restriction()),
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
        assert_eq!(u16::from_le_bytes([response[7], response[8]]), 2);
        assert!(utf16_position(&response, "IPM.Configuration.AccountPrefs").is_some());
        assert!(utf16_position(&response, "IPM.Configuration.MessageListSettings").is_some());
    }

    #[test]
    fn inbox_associated_query_rows_uses_standard_property_rows_for_complete_rows() {
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
            columns_set: true,
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
            restriction: Some(MapiRestriction::Property {
                relop: 0x04,
                property_tag: PID_TAG_MESSAGE_CLASS_W,
                value: MapiValue::String("IPM.Configuration.AccountPrefs".to_string()),
            }),
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
        assert_eq!(cursor.read_u8().unwrap(), 0);
        for column in columns {
            parse_mapi_property_value(&mut cursor, column).unwrap();
        }
        assert!(cursor.remaining_is_zero_padding());
    }

    #[test]
    fn inbox_associated_query_rows_returns_umolk_user_options_default() {
        let snapshot = MapiMailStoreSnapshot::empty();
        let mut table = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: true,
            columns: vec![PID_TAG_MID, PID_TAG_MESSAGE_CLASS_W, PID_TAG_SUBJECT_W],
            columns_set: true,
            sort_orders: vec![MapiSortOrder {
                property_tag: PID_TAG_LAST_MODIFICATION_TIME,
                order: 1,
            }],
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: Some(MapiRestriction::Property {
                relop: 0x04,
                property_tag: PID_TAG_MESSAGE_CLASS_W,
                value: MapiValue::String("IPM.Configuration.UMOLK.UserOptions".to_string()),
            }),
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        };
        let request = RopRequest {
            rop_id: RopId::QueryRows.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: vec![0, 1, 2, 0],
        };

        let response =
            rop_query_rows_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil());

        assert_eq!(response[0], RopId::QueryRows.as_u8());
        assert_eq!(u16::from_le_bytes([response[7], response[8]]), 1);
        assert!(utf16_position(&response, "IPM.Configuration.UMOLK.UserOptions").is_some());
    }

    #[test]
    fn microsoft_oxocfg_inbox_mrm_configuration_uses_xml_stream() {
        let snapshot = MapiMailStoreSnapshot::empty();
        let mut table = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: true,
            columns: vec![
                PID_TAG_MESSAGE_CLASS_W,
                PID_TAG_ROAMING_DATATYPES,
                PID_TAG_ROAMING_XML_STREAM,
            ],
            columns_set: true,
            sort_orders: vec![MapiSortOrder {
                property_tag: PID_TAG_LAST_MODIFICATION_TIME,
                order: 1,
            }],
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: Some(MapiRestriction::Property {
                relop: 0x04,
                property_tag: PID_TAG_MESSAGE_CLASS_W,
                value: MapiValue::String("IPM.Configuration.MRM".to_string()),
            }),
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        };
        let request = RopRequest {
            rop_id: RopId::QueryRows.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: vec![0, 1, 2, 0],
        };

        let response =
            rop_query_rows_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil());

        assert_eq!(response[0], RopId::QueryRows.as_u8());
        assert_eq!(u16::from_le_bytes([response[7], response[8]]), 1);
        assert!(utf16_position(&response, "IPM.Configuration.MRM").is_some());
        assert!(response
            .windows(4)
            .any(|window| window == 2u32.to_le_bytes()));
        assert!(response
            .windows(b"RetentionHold".len())
            .any(|window| window == b"RetentionHold"));
    }

    #[test]
    fn inbox_associated_query_rows_does_not_return_empty_virtual_rule_organizer() {
        let snapshot = MapiMailStoreSnapshot::empty();
        let mut table = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: true,
            columns: vec![
                PID_TAG_FOLDER_ID,
                PID_TAG_MID,
                PID_TAG_INST_ID,
                PID_TAG_INSTANCE_NUM,
                PID_TAG_MESSAGE_CLASS_W,
                PID_TAG_LAST_MODIFICATION_TIME,
            ],
            columns_set: true,
            sort_orders: vec![MapiSortOrder {
                property_tag: PID_TAG_LAST_MODIFICATION_TIME,
                order: 1,
            }],
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: Some(MapiRestriction::Property {
                relop: 0x04,
                property_tag: PID_TAG_MESSAGE_CLASS_W,
                value: MapiValue::String("IPM.RuleOrganizer".to_string()),
            }),
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        };
        let request = RopRequest {
            rop_id: RopId::QueryRows.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: vec![0, 1, 35, 0],
        };

        let response =
            rop_query_rows_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil());

        assert_eq!(response[0], RopId::QueryRows.as_u8());
        assert_eq!(u16::from_le_bytes([response[7], response[8]]), 0);
    }

    #[test]
    fn rule_organizer_without_client_payload_has_no_synthetic_stream_property() {
        let message = MapiAssociatedConfigMessage {
            id: crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFED),
            folder_id: INBOX_FOLDER_ID,
            canonical_id: Uuid::from_u128(0x6d617069_7275_6c65_8000_000000000001),
            message_class: crate::mapi_store::OUTLOOK_INBOX_RULE_ORGANIZER_CONFIG_CLASS.to_string(),
            subject: crate::mapi_store::OUTLOOK_INBOX_RULE_ORGANIZER_CONFIG_CLASS.to_string(),
            properties_json: serde_json::json!({}),
        };

        assert_eq!(
            associated_config_property_value(&message, OUTLOOK_RULE_ORGANIZER_BINARY_6802),
            None
        );
    }

    #[test]
    fn delegate_freebusy_projects_outlook_view_probe_properties() {
        let message = MapiDelegateFreeBusyMessage {
            id: crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFE4),
            folder_id: FREEBUSY_DATA_FOLDER_ID,
            canonical_id: Uuid::from_u128(0x6d617069_6672_4266_8000_000000000001),
            message: lpe_storage::DelegateFreeBusyMessageObject {
                id: Uuid::from_u128(0x6d617069_6672_4266_8000_000000000001),
                account_id: Uuid::nil(),
                owner_account_id: Uuid::nil(),
                owner_email: String::new(),
                message_kind: "freebusy".to_string(),
                subject: "LocalFreebusy".to_string(),
                body_text: String::new(),
                starts_at: None,
                ends_at: None,
                busy_status: None,
                payload_json: "{}".to_string(),
                updated_at: "1970-01-01T00:00:00Z".to_string(),
            },
        };

        for tag in [
            0x6841_0003,
            0x6842_000B,
            0x6843_000B,
            0x684A_101F,
            0x6845_1102,
            0x686B_1003,
            0x6870_1102,
            0x6871_1003,
            0x6872_001F,
            0x686D_000B,
            0x686E_000B,
            0x686F_000B,
            0x684B_000B,
            0x6844_101F,
            0x3008_0040,
            0x0E0B_0102,
        ] {
            assert!(
                delegate_freebusy_property_value(&message, tag).is_some(),
                "missing modeled freebusy property 0x{tag:08x}"
            );
        }
        assert_ne!(
            delegate_freebusy_property_value(&message, PID_TAG_LAST_MODIFICATION_TIME),
            Some(MapiValue::I64(0))
        );
    }

    #[test]
    fn inbox_associated_query_rows_default_columns_cover_required_configuration_contract() {
        let snapshot = inbox_associated_sort_snapshot();
        let columns = default_associated_config_columns();
        let mut table = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: true,
            columns: Vec::new(),
            columns_set: true,
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
            restriction: Some(MapiRestriction::Property {
                relop: 0x04,
                property_tag: PID_TAG_MESSAGE_CLASS_W,
                value: MapiValue::String("IPM.Configuration.AccountPrefs".to_string()),
            }),
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
        assert_eq!(cursor.read_u8().unwrap(), 0);
        for column in columns {
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
        let mailbox_guid = Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap();
        let mailbox_entry_id = crate::mapi::identity::message_entry_id_from_object_ids(
            mailbox_guid,
            INBOX_FOLDER_ID,
            message.id,
        )
        .unwrap();
        assert_eq!(
            associated_config_property_value_with_mailbox_guid(
                &message,
                mailbox_guid,
                PID_TAG_ENTRY_ID
            ),
            Some(MapiValue::Binary(mailbox_entry_id.clone()))
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
            associated_config_property_value(&message, PID_TAG_CONVERSATION_TOPIC_W),
            Some(MapiValue::String("Message list settings".to_string()))
        );
        assert_eq!(
            associated_config_property_value(&message, PID_TAG_MESSAGE_CLASS_W),
            Some(MapiValue::String(
                "IPM.Configuration.MessageListSettings".to_string()
            ))
        );
        assert_eq!(
            associated_config_property_value(&message, PID_TAG_ORIGINAL_MESSAGE_CLASS_W),
            Some(MapiValue::String(
                "IPM.Configuration.MessageListSettings".to_string()
            ))
        );
        assert_eq!(
            associated_config_property_value(&message, PID_TAG_MESSAGE_STATUS),
            Some(MapiValue::U32(0))
        );
        assert_eq!(
            associated_config_property_value(&message, PID_TAG_ACCESS_LEVEL),
            Some(MapiValue::U32(1))
        );
        assert_eq!(
            associated_config_property_value(&message, PID_TAG_SENT_MAIL_SVR_EID),
            Some(MapiValue::Binary(Vec::new()))
        );
        assert_eq!(
            associated_config_property_value(&message, PID_TAG_SEARCH_KEY),
            Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
                message.id
            )))
        );
        assert_eq!(
            associated_config_property_value(&message, PID_TAG_PARENT_SOURCE_KEY),
            Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
                INBOX_FOLDER_ID
            )))
        );
        assert_eq!(
            associated_config_property_value_with_mailbox_guid(
                &message,
                mailbox_guid,
                PID_TAG_PARENT_ENTRY_ID
            ),
            crate::mapi::identity::folder_entry_id_from_object_id(mailbox_guid, INBOX_FOLDER_ID)
                .map(MapiValue::Binary)
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
            Some(MapiValue::Binary(value))
                if value.starts_with(br#"<?xml version="1.0" encoding="utf-8"?>"#)
                    && value.windows(b"18-OLPrefsVersion".len()).any(|window| window == b"18-OLPrefsVersion")
                    && value.windows(b"9-1".len()).any(|window| window == b"9-1")
        ));
        assert!(matches!(
            associated_config_property_value(&message, 0x685D_0003),
            Some(MapiValue::U32(value)) if value != 0
        ));
        assert_eq!(
            associated_config_property_value(&message, OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B),
            Some(MapiValue::Binary(Vec::new()))
        );
        assert_eq!(
            associated_config_property_value(&message, PID_NAME_CONTENT_CLASS_W_TAG),
            Some(MapiValue::String("urn:content-classes:message".to_string()))
        );
        assert_eq!(
            associated_config_property_value(&message, PID_NAME_CONTENT_TYPE_W_TAG),
            Some(MapiValue::String("text/xml".to_string()))
        );
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
        let xml_only = MapiAssociatedConfigMessage {
            properties_json: serde_json::json!({
                "0x7c080102": {"type": "binary", "value": "3c786d6c2f3e"}
            }),
            ..message.clone()
        };
        assert_eq!(
            associated_config_property_value(&xml_only, PID_TAG_ROAMING_DATATYPES),
            Some(MapiValue::U32(2))
        );
        assert_eq!(
            associated_config_property_value(&xml_only, PID_TAG_ROAMING_XML_STREAM),
            Some(MapiValue::Binary(b"<xml/>".to_vec()))
        );
        let binary_only = MapiAssociatedConfigMessage {
            properties_json: serde_json::json!({
                "0x7c090102": {"type": "binary", "value": "010203"}
            }),
            ..message.clone()
        };
        assert_eq!(
            associated_config_property_value(&binary_only, PID_TAG_ROAMING_DATATYPES),
            Some(MapiValue::U32(1))
        );
        assert_eq!(
            associated_config_property_value(&binary_only, 0x7C09_0102),
            Some(MapiValue::Binary(vec![1, 2, 3]))
        );
        let explicit_no_streams = MapiAssociatedConfigMessage {
            properties_json: serde_json::json!({
                "0x7c060003": {"type": "i32", "value": 0}
            }),
            ..message.clone()
        };
        assert_eq!(
            associated_config_property_value(&explicit_no_streams, PID_TAG_ROAMING_DATATYPES),
            Some(MapiValue::I32(0))
        );
        assert_eq!(
            associated_config_property_value(&explicit_no_streams, PID_TAG_ROAMING_DICTIONARY),
            None
        );
        let work_hours = MapiAssociatedConfigMessage {
            id: crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 93,
            ),
            folder_id: CALENDAR_FOLDER_ID,
            canonical_id: Uuid::nil(),
            message_class: "IPM.Configuration.WorkHours".to_string(),
            subject: "WorkHours".to_string(),
            properties_json: serde_json::json!({}),
        };
        assert_eq!(
            associated_config_property_value(&work_hours, PID_TAG_ROAMING_DATATYPES),
            Some(MapiValue::U32(2))
        );
        assert!(matches!(
            associated_config_property_value(&work_hours, PID_TAG_ROAMING_XML_STREAM),
            Some(MapiValue::Binary(value))
                if value.windows(b"WorkingHours.xsd".len()).any(|window| window == b"WorkingHours.xsd")
                    && value.windows(b"WorkHoursVersion1".len()).any(|window| window == b"WorkHoursVersion1")
        ));
        assert_eq!(
            associated_config_property_value(&work_hours, PID_TAG_ROAMING_DICTIONARY),
            None
        );
        let category_list = MapiAssociatedConfigMessage {
            id: crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 94,
            ),
            folder_id: CALENDAR_FOLDER_ID,
            canonical_id: Uuid::nil(),
            message_class: "IPM.Configuration.CategoryList".to_string(),
            subject: "CategoryList".to_string(),
            properties_json: serde_json::json!({}),
        };
        assert_eq!(
            associated_config_property_value(&category_list, PID_TAG_ROAMING_DATATYPES),
            Some(MapiValue::U32(2))
        );
        assert!(matches!(
            associated_config_property_value(&category_list, PID_TAG_ROAMING_XML_STREAM),
            Some(MapiValue::Binary(value))
                if value.windows(b"CategoryList.xsd".len()).any(|window| window == b"CategoryList.xsd")
                    && value.windows(b"Red Category".len()).any(|window| window == b"Red Category")
        ));
        assert_eq!(
            associated_config_property_value(&category_list, PID_TAG_ROAMING_DICTIONARY),
            None
        );
        let quick_step = MapiAssociatedConfigMessage {
            id: crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 92,
            ),
            folder_id: QUICK_STEP_SETTINGS_FOLDER_ID,
            canonical_id: Uuid::nil(),
            message_class: crate::mapi_store::OUTLOOK_QUICK_STEP_CUSTOM_ACTION_CLASS.to_string(),
            subject: crate::mapi_store::OUTLOOK_QUICK_STEP_CUSTOM_ACTION_CLASS.to_string(),
            properties_json: serde_json::json!({}),
        };
        assert_eq!(
            associated_config_property_value(&quick_step, PID_TAG_ROAMING_DATATYPES),
            Some(MapiValue::U32(2))
        );
        assert!(matches!(
            associated_config_property_value(&quick_step, PID_TAG_ROAMING_XML_STREAM),
            Some(MapiValue::Binary(value))
                if value.starts_with(br#"<?xml version="1.0" encoding="utf-8"?>"#)
                    && value.windows(b"customActions".len()).any(|window| window == b"customActions")
        ));
        assert_eq!(
            associated_config_property_value(&quick_step, OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B),
            Some(MapiValue::Binary(Vec::new()))
        );

        let row = serialize_associated_config_row_with_mailbox_guid(
            &message,
            mailbox_guid,
            &[
                PID_TAG_FOLDER_ID,
                PID_TAG_MID,
                PID_TAG_INST_ID,
                PID_TAG_INSTANCE_NUM,
                PID_TAG_ROAMING_DATATYPES,
                0x685D_0003,
                OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B,
                PID_TAG_LAST_MODIFICATION_TIME,
            ],
        );

        assert_eq!(row.len(), 46);
        let mut row_cursor = Cursor::new(&row);
        for column in [
            PID_TAG_FOLDER_ID,
            PID_TAG_MID,
            PID_TAG_INST_ID,
            PID_TAG_INSTANCE_NUM,
            PID_TAG_ROAMING_DATATYPES,
            0x685D_0003,
        ] {
            parse_mapi_property_value(&mut row_cursor, column).unwrap();
        }
        assert_eq!(
            parse_mapi_property_value(&mut row_cursor, OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B)
                .unwrap(),
            MapiValue::Binary(Vec::new())
        );

        let entry_id_row = serialize_associated_config_row_with_mailbox_guid(
            &message,
            mailbox_guid,
            &[PID_TAG_ENTRY_ID],
        );
        assert!(entry_id_row
            .windows(mailbox_entry_id.len())
            .any(|window| window == mailbox_entry_id));
    }

    #[test]
    fn contact_link_timestamp_config_projects_outlook_osc_defaults() {
        let message = MapiAssociatedConfigMessage {
            id: crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFEC),
            folder_id: CONTACTS_FOLDER_ID,
            canonical_id: Uuid::nil(),
            message_class: "IPM.Microsoft.ContactLink.TimeStamp".to_string(),
            subject: "IPM.Microsoft.ContactLink.TimeStamp".to_string(),
            properties_json: serde_json::json!({}),
        };

        assert_eq!(
            associated_config_property_value(&message, PID_NAME_OSC_CONTACT_SOURCES_TAG),
            Some(MapiValue::MultiString(Vec::new()))
        );
        assert_eq!(
            associated_config_property_value(
                &message,
                (PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80E1 << 16) | 0x0102
            ),
            Some(MapiValue::Binary(Vec::new()))
        );
        assert_eq!(
            associated_config_property_value(
                &message,
                (PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80E1 << 16) | 0x0040
            ),
            Some(MapiValue::I64(0))
        );
        assert_eq!(
            associated_config_property_value(
                &message,
                (PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80EC << 16) | 0x001F
            ),
            Some(MapiValue::String(String::new()))
        );
        assert_eq!(
            associated_config_property_value(
                &message,
                (PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80EA << 16) | 0x0003
            ),
            Some(MapiValue::U32(0))
        );
        assert_eq!(
            associated_config_property_value(
                &message,
                (PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80ED << 16) | 0x000B
            ),
            Some(MapiValue::Bool(false))
        );
        assert_eq!(
            associated_config_property_value(&message, OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B),
            Some(MapiValue::Binary(Vec::new()))
        );
    }

    #[test]
    fn contacts_helper_associated_configs_project_table_config_columns() {
        for message_class in [
            "IPM.Microsoft.ContactLink.TimeStamp",
            "IPM.Microsoft.OSC.ContactSync",
        ] {
            let message = MapiAssociatedConfigMessage {
                id: crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFEC),
                folder_id: CONTACTS_FOLDER_ID,
                canonical_id: Uuid::nil(),
                message_class: message_class.to_string(),
                subject: message_class.to_string(),
                properties_json: serde_json::json!({}),
            };

            assert_eq!(
                associated_config_property_value(&message, PID_TAG_ROAMING_DATATYPES),
                Some(MapiValue::U32(0))
            );
            assert!(matches!(
                associated_config_property_value(&message, 0x685D_0003),
                Some(MapiValue::U32(value)) if value != 0
            ));
            assert_eq!(
                associated_config_property_value(&message, PID_NAME_OSC_CONTACT_SOURCES_TAG),
                Some(MapiValue::MultiString(Vec::new()))
            );
            assert_eq!(
                associated_config_property_value(
                    &message,
                    (PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80EC << 16) | 0x0003
                ),
                Some(MapiValue::U32(0))
            );

            let row = serialize_associated_config_row_with_mailbox_guid(
                &message,
                Uuid::nil(),
                &[
                    PID_TAG_FOLDER_ID,
                    PID_TAG_MID,
                    PID_TAG_INST_ID,
                    PID_TAG_INSTANCE_NUM,
                    PID_TAG_ROAMING_DATATYPES,
                    PID_TAG_MESSAGE_CLASS_W,
                    0x685D_0003,
                    PID_TAG_LAST_MODIFICATION_TIME,
                ],
            );
            let mut row_cursor = Cursor::new(&row);
            for column in [
                PID_TAG_FOLDER_ID,
                PID_TAG_MID,
                PID_TAG_INST_ID,
                PID_TAG_INSTANCE_NUM,
            ] {
                parse_mapi_property_value(&mut row_cursor, column).unwrap();
            }
            assert_eq!(
                parse_mapi_property_value(&mut row_cursor, PID_TAG_ROAMING_DATATYPES).unwrap(),
                MapiValue::I32(0)
            );
            parse_mapi_property_value(&mut row_cursor, PID_TAG_MESSAGE_CLASS_W).unwrap();
            assert!(matches!(
                parse_mapi_property_value(&mut row_cursor, 0x685D_0003).unwrap(),
                MapiValue::I32(value) if value != 0
            ));
            parse_mapi_property_value(&mut row_cursor, PID_TAG_LAST_MODIFICATION_TIME).unwrap();
            assert_eq!(row_cursor.position() as usize, row.len());
        }
    }

    #[test]
    fn inbox_named_view_associated_row_projects_view_descriptor_properties() {
        let message = MapiAssociatedConfigMessage {
            id: crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 91,
            ),
            folder_id: INBOX_FOLDER_ID,
            canonical_id: Uuid::from_u128(0x6d617069_696e_5669_8000_000000000001),
            message_class: crate::mapi_store::OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_CLASS.to_string(),
            subject: "Compact".to_string(),
            properties_json: serde_json::json!({}),
        };

        assert_eq!(
            associated_config_property_value(&message, PID_TAG_MESSAGE_CLASS_W),
            Some(MapiValue::String(
                "IPM.Microsoft.FolderDesign.NamedView".to_string()
            ))
        );
        assert_eq!(
            associated_config_property_value(&message, PID_TAG_SUBJECT_W),
            Some(MapiValue::String("Compact".to_string()))
        );
        assert_eq!(
            associated_config_property_value(&message, PID_TAG_VIEW_DESCRIPTOR_VERSION),
            Some(MapiValue::U32(8))
        );
        assert_eq!(
            associated_config_property_value(&message, PID_TAG_VIEW_DESCRIPTOR_VERSION_CANONICAL),
            Some(MapiValue::U32(8))
        );
        assert_eq!(
            associated_config_property_value(&message, PID_TAG_VIEW_DESCRIPTOR_NAME_W),
            Some(MapiValue::String("Compact".to_string()))
        );
        assert_eq!(
            associated_config_property_value(&message, PID_TAG_VIEW_DESCRIPTOR_STRINGS_W),
            Some(MapiValue::String(
                "\nImportance\nReminder\nIcon\nFlag Status\nAttachment\nFrom\nSubject\nReceived\nSize\nCategories\n".to_string()
            ))
        );
        assert_eq!(
            associated_config_property_value(&message, PID_TAG_VIEW_DESCRIPTOR_VIEW_MODE),
            Some(MapiValue::U32(0))
        );
        assert!(matches!(
            associated_config_property_value(&message, PID_TAG_VIEW_DESCRIPTOR_FLAGS),
            Some(MapiValue::U32(value)) if value != 0
        ));
        assert!(matches!(
            associated_config_property_value(&message, PID_TAG_VIEW_DESCRIPTOR_BINARY),
            Some(MapiValue::Binary(value)) if !value.is_empty()
        ));
        assert_eq!(
            associated_config_property_value(&message, OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B),
            associated_config_property_value(&message, PID_TAG_VIEW_DESCRIPTOR_BINARY)
        );
        assert_eq!(
            associated_config_property_value(&message, OUTLOOK_COMMON_VIEW_DESCRIPTOR_STRINGS_683C),
            Some(MapiValue::Binary(view_descriptor_strings_binary(
                &outlook_mail_view_definition("Compact")
            )))
        );
        assert_eq!(
            associated_config_property_value(&message, PID_TAG_VIEW_DESCRIPTOR_CLSID),
            Some(MapiValue::Guid(*message.canonical_id.as_bytes()))
        );
        assert_eq!(
            associated_config_property_value(&message, 0x6833_0102),
            Some(MapiValue::Binary(message.canonical_id.as_bytes().to_vec()))
        );
        assert_eq!(
            associated_config_property_value(&message, 0x6842_0102),
            Some(MapiValue::Binary(default_wlink_group_guid().to_vec()))
        );

        let row = serialize_associated_config_row_with_mailbox_guid(
            &message,
            Uuid::nil(),
            &[
                PID_TAG_MID,
                PID_TAG_INST_ID,
                PID_TAG_INSTANCE_NUM,
                PID_TAG_SUBJECT_W,
                PID_TAG_VIEW_DESCRIPTOR_CLSID,
                PID_TAG_VIEW_DESCRIPTOR_FLAGS,
                PID_TAG_VIEW_DESCRIPTOR_VERSION,
                PID_TAG_VIEW_DESCRIPTOR_VIEW_MODE,
                0x6842_0102,
                PID_TAG_LAST_MODIFICATION_TIME,
                PID_TAG_MESSAGE_CLASS_W,
            ],
        );
        let mut row_cursor = Cursor::new(&row);
        for column in [
            PID_TAG_MID,
            PID_TAG_INST_ID,
            PID_TAG_INSTANCE_NUM,
            PID_TAG_SUBJECT_W,
            PID_TAG_VIEW_DESCRIPTOR_CLSID,
            PID_TAG_VIEW_DESCRIPTOR_FLAGS,
            PID_TAG_VIEW_DESCRIPTOR_VERSION,
            PID_TAG_VIEW_DESCRIPTOR_VIEW_MODE,
            0x6842_0102,
            PID_TAG_LAST_MODIFICATION_TIME,
            PID_TAG_MESSAGE_CLASS_W,
        ] {
            parse_mapi_property_value(&mut row_cursor, column).unwrap();
        }
        assert!(row_cursor.remaining_is_zero_padding());
    }

    #[test]
    fn microsoft_oxocfg_associated_config_sort_uses_persisted_last_modification_time() {
        let older_id = crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 111,
        );
        let newer_id = crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 110,
        );
        let older = MapiAssociatedConfigMessage {
            id: older_id,
            folder_id: INBOX_FOLDER_ID,
            canonical_id: Uuid::nil(),
            message_class: "IPM.Configuration.ClientOptions".to_string(),
            subject: "Older client options".to_string(),
            properties_json: serde_json::json!({
                "__lpe_updated_at": "2026-01-01T00:00:00Z"
            }),
        };
        let newer = MapiAssociatedConfigMessage {
            id: newer_id,
            folder_id: INBOX_FOLDER_ID,
            canonical_id: Uuid::nil(),
            message_class: "IPM.Configuration.ClientOptions".to_string(),
            subject: "Newer client options".to_string(),
            properties_json: serde_json::json!({
                "__lpe_updated_at": "2026-06-01T00:00:00Z"
            }),
        };

        assert_eq!(
            associated_config_property_value(&newer, PID_TAG_LAST_MODIFICATION_TIME),
            Some(MapiValue::I64(
                mapi_mailstore::filetime_from_rfc3339_utc("2026-06-01T00:00:00Z") as i64
            ))
        );

        let mut rows = vec![
            AssociatedTableRow::Config(older),
            AssociatedTableRow::Config(newer),
        ];
        sort_associated_table_rows(
            &mut rows,
            &[
                MapiSortOrder {
                    property_tag: PID_TAG_MESSAGE_CLASS_W,
                    order: 0,
                },
                MapiSortOrder {
                    property_tag: PID_TAG_LAST_MODIFICATION_TIME,
                    order: 1,
                },
            ],
            Uuid::nil(),
        );

        assert_eq!(associated_table_row_id(&rows[0]), newer_id);
        assert_eq!(associated_table_row_id(&rows[1]), older_id);
    }

    fn assert_inbox_associated_find_row_no_match_for_message_class(message_class: &str) {
        let response = inbox_associated_find_row_response_for_message_class(message_class);

        assert_eq!(response[0], RopId::FindRow.as_u8());
        assert_eq!(
            u32::from_le_bytes(response[2..6].try_into().unwrap()),
            0x8004_010F
        );
        assert_eq!(response.len(), 6);
    }

    fn assert_inbox_associated_find_row_returns_message_class(message_class: &str) {
        let response = inbox_associated_find_row_response_for_message_class(message_class);

        assert_eq!(response[0], RopId::FindRow.as_u8());
        assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
        assert_eq!(response[6], 0);
        assert_eq!(response[7], 1);
        assert_response_contains_utf16(&response, message_class);
    }

    fn inbox_associated_find_row_response_for_message_class(message_class: &str) -> Vec<u8> {
        let snapshot = MapiMailStoreSnapshot::empty();
        let mut table = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: true,
            columns: vec![PID_TAG_MESSAGE_CLASS_W],
            columns_set: false,
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

        rop_find_row_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil())
    }

    fn assert_contact_folder_associated_find_row_returns_osc_contact_sync(folder_id: u64) {
        let snapshot = MapiMailStoreSnapshot::empty();
        assert_contact_folder_associated_find_row_returns_osc_contact_sync_for_snapshot(
            folder_id, &snapshot,
        );
    }

    fn assert_contact_folder_associated_find_row_returns_osc_contact_sync_for_snapshot(
        folder_id: u64,
        snapshot: &MapiMailStoreSnapshot,
    ) {
        assert_contact_folder_associated_find_row_returns_config(
            folder_id,
            "IPM.Microsoft.OSC.ContactSync",
            snapshot,
        );
    }

    fn assert_contact_folder_associated_find_row_returns_config(
        folder_id: u64,
        message_class: &str,
        snapshot: &MapiMailStoreSnapshot,
    ) {
        let response =
            contact_folder_associated_find_row_response(folder_id, message_class, snapshot);

        assert_eq!(response[0], RopId::FindRow.as_u8());
        assert_eq!(u32::from_le_bytes(response[3..7].try_into().unwrap()), 0);
        assert_eq!(response[7], 1);
        let mut encoded_message_class = Vec::new();
        write_utf16z(&mut encoded_message_class, message_class);
        assert!(response
            .windows(encoded_message_class.len())
            .any(|window| window == encoded_message_class.as_slice()));
    }

    fn assert_contact_folder_associated_find_row_does_not_return_config(
        folder_id: u64,
        message_class: &str,
        snapshot: &MapiMailStoreSnapshot,
    ) {
        let response =
            contact_folder_associated_find_row_response(folder_id, message_class, snapshot);

        assert_eq!(response[0], RopId::FindRow.as_u8());
        assert_eq!(
            u32::from_le_bytes(response[2..6].try_into().unwrap()),
            0x8004_010F
        );
        assert_eq!(response.len(), 6);
    }

    fn contact_folder_associated_find_row_response(
        folder_id: u64,
        message_class: &str,
        snapshot: &MapiMailStoreSnapshot,
    ) -> Vec<u8> {
        let mut table = MapiObject::ContentsTable {
            folder_id,
            associated: true,
            columns: vec![
                PID_TAG_FOLDER_ID,
                PID_TAG_MID,
                PID_TAG_INST_ID,
                PID_TAG_INSTANCE_NUM,
                PID_TAG_MESSAGE_CLASS_W,
            ],
            columns_set: true,
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

        rop_find_row_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil())
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
                properties_json: serde_json::json!({
                    "0x7c070102": {"type": "binary", "value": "3c786d6c2f3e"}
                }),
            },
        ])
    }

    fn inbox_associated_extended_rule_snapshot() -> MapiMailStoreSnapshot {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let persisted_id = Uuid::from_u128(0x6d617069_6578_5275_8000_000000000101);
        crate::mapi::identity::remember_mapi_identity(
            persisted_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 83,
            ),
        );
        MapiMailStoreSnapshot::empty().with_associated_configs(vec![
            crate::store::MapiAssociatedConfigRecord {
                id: persisted_id,
                account_id,
                folder_id: INBOX_FOLDER_ID,
                message_class: "IPM.ExtendedRule.Message".to_string(),
                subject: "Junk E-mail Rule".to_string(),
                properties_json: serde_json::json!({
                    "0x7c060003": {"type": "u32", "value": 4},
                    "0x7c070102": {"type": "binary", "value": "392d30"}
                }),
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
                save_stamp: 0,
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
                save_stamp: 0,
                section: 1,
                ordinal: 0x10,
                group_header_id,
                group_name: "Mail".to_string(),
            },
        ])
    }

    fn test_table_email(id: Uuid, mailbox_id: Uuid, subject: &str) -> JmapEmail {
        JmapEmail {
            id,
            thread_id: Uuid::from_u128(0x5555),
            mailbox_id,
            mailbox_role: "inbox".to_string(),
            mailbox_name: "Inbox".to_string(),
            modseq: 1,
            mailbox_ids: vec![mailbox_id],
            mailbox_states: vec![lpe_storage::JmapEmailMailboxState {
                mailbox_id,
                role: "inbox".to_string(),
                name: "Inbox".to_string(),
                modseq: 1,
                unread: false,
                flagged: false,
                followup_flag_status: "none".to_string(),
                followup_icon: 0,
                todo_item_flags: 0,
                followup_request: String::new(),
                followup_start_at: None,
                followup_due_at: None,
                followup_completed_at: None,
                reminder_set: false,
                reminder_at: None,
                reminder_dismissed_at: None,
                swapped_todo_store_id: None,
                swapped_todo_data: None,
                categories: Vec::new(),
                draft: false,
            }],
            received_at: "2026-06-09T20:00:00Z".to_string(),
            sent_at: Some("2026-06-09T20:00:00Z".to_string()),
            from_address: "sender@example.test".to_string(),
            from_display: Some("Sender".to_string()),
            sender_address: None,
            sender_display: None,
            sender_authorization_kind: "self".to_string(),
            submitted_by_account_id: Uuid::nil(),
            to: Vec::new(),
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: subject.to_string(),
            preview: String::new(),
            body_text: String::new(),
            body_html_sanitized: None,
            unread: false,
            flagged: false,
            followup_flag_status: "none".to_string(),
            followup_icon: 0,
            todo_item_flags: 0,
            followup_request: String::new(),
            followup_start_at: None,
            followup_due_at: None,
            followup_completed_at: None,
            reminder_set: false,
            reminder_at: None,
            reminder_dismissed_at: None,
            swapped_todo_store_id: None,
            swapped_todo_data: None,
            categories: Vec::new(),
            has_attachments: false,
            size_octets: 128,
            internet_message_id: Some(format!("<{}@example.test>", id)),
            mime_blob_ref: None,
            delivery_status: "stored".to_string(),
        }
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
    fn message_row_projects_containing_folder_ids() {
        let email_id = Uuid::from_u128(0x7171);
        crate::mapi::identity::remember_mapi_identity(
            email_id,
            crate::mapi::identity::mapi_store_id(0x81),
        );
        let mut email = test_table_email(email_id, Uuid::from_u128(0x8181), "Test Draft");
        email.mailbox_role = "drafts".to_string();

        let row = serialize_message_row(
            &email,
            &[PID_TAG_FOLDER_ID, PID_TAG_PARENT_FOLDER_ID, PID_TAG_MID],
        );

        assert_eq!(
            crate::mapi::identity::object_id_from_wire_id(&row[0..8]),
            Some(DRAFTS_FOLDER_ID)
        );
        assert_eq!(
            crate::mapi::identity::object_id_from_wire_id(&row[8..16]),
            Some(DRAFTS_FOLDER_ID)
        );
        assert_eq!(
            crate::mapi::identity::object_id_from_wire_id(&row[16..24]),
            Some(mapi_message_id(&email))
        );
    }

    #[test]
    fn normal_message_row_projects_outlook_inbox_view_columns() {
        let email_id = Uuid::from_u128(0x7172);
        let mut email = test_table_email(email_id, Uuid::from_u128(0x8182), "Inbox row");
        email.received_at = "2026-06-20T16:28:38Z".to_string();
        email.from_display = Some("Denis Ducret".to_string());
        email.from_address = "denis.ducret@sdic.ch".to_string();
        email.sender_display = Some("Delegate Sender".to_string());
        email.sender_address = Some("delegate@example.test".to_string());
        email.size_octets = 2048;
        email.has_attachments = true;
        email.followup_flag_status = "flagged".to_string();
        email.reminder_set = true;
        email.categories = vec!["Blue".to_string(), "Customer".to_string()];
        let expected_time = mapi_mailstore::filetime_from_rfc3339_utc(&email.received_at);
        let columns = [
            PID_TAG_CREATION_TIME,
            PID_TAG_IMPORTANCE,
            PID_TAG_PRIORITY,
            PID_TAG_SENSITIVITY,
            PID_LID_REMINDER_SET_TAG,
            PID_TAG_MESSAGE_CLASS_W,
            PID_TAG_SUBJECT_PREFIX_W,
            PID_TAG_FLAG_STATUS,
            PID_TAG_HAS_ATTACHMENTS,
            PID_TAG_SENDER_NAME_W,
            PID_TAG_SENDER_EMAIL_ADDRESS_W,
            PID_TAG_SENT_REPRESENTING_NAME_W,
            PID_TAG_SENT_REPRESENTING_ADDRESS_TYPE_W,
            PID_TAG_SENT_REPRESENTING_EMAIL_ADDRESS_W,
            PID_TAG_MESSAGE_DELIVERY_TIME,
            PID_TAG_MESSAGE_SIZE,
            PID_TAG_MESSAGE_SIZE_EXTENDED,
            PID_NAME_KEYWORDS_TAG,
            PID_LID_OUTLOOK_COMMON_8514_TAG,
            0x8017_000B,
            PID_LID_OUTLOOK_APPOINTMENT_8F07_TAG,
            PID_NAME_CONTENT_CLASS_W_TAG,
        ];

        let row = serialize_message_row(&email, &columns);
        let mut cursor = Cursor::new(&row);

        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_CREATION_TIME).unwrap(),
            MapiValue::I64(expected_time as i64)
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_IMPORTANCE).unwrap(),
            MapiValue::I32(1)
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_PRIORITY).unwrap(),
            MapiValue::I32(0)
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_SENSITIVITY).unwrap(),
            MapiValue::I32(0)
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_LID_REMINDER_SET_TAG).unwrap(),
            MapiValue::Bool(true)
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_MESSAGE_CLASS_W).unwrap(),
            MapiValue::String("IPM.Note".to_string())
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_SUBJECT_PREFIX_W).unwrap(),
            MapiValue::String(String::new())
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_FLAG_STATUS).unwrap(),
            MapiValue::I32(2)
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_HAS_ATTACHMENTS).unwrap(),
            MapiValue::Bool(true)
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_SENDER_NAME_W).unwrap(),
            MapiValue::String("Delegate Sender".to_string())
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_SENDER_EMAIL_ADDRESS_W).unwrap(),
            MapiValue::String("delegate@example.test".to_string())
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_SENT_REPRESENTING_NAME_W).unwrap(),
            MapiValue::String("Denis Ducret".to_string())
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_SENT_REPRESENTING_ADDRESS_TYPE_W)
                .unwrap(),
            MapiValue::String("SMTP".to_string())
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_SENT_REPRESENTING_EMAIL_ADDRESS_W)
                .unwrap(),
            MapiValue::String("denis.ducret@sdic.ch".to_string())
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_MESSAGE_DELIVERY_TIME).unwrap(),
            MapiValue::I64(expected_time as i64)
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_MESSAGE_SIZE).unwrap(),
            MapiValue::I32(2048)
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_MESSAGE_SIZE_EXTENDED).unwrap(),
            MapiValue::I64(2048)
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_NAME_KEYWORDS_TAG).unwrap(),
            MapiValue::MultiString(vec!["Blue".to_string(), "Customer".to_string()])
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_LID_OUTLOOK_COMMON_8514_TAG).unwrap(),
            MapiValue::Bool(false)
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, 0x8017_000B).unwrap(),
            MapiValue::Bool(false)
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_LID_OUTLOOK_APPOINTMENT_8F07_TAG).unwrap(),
            MapiValue::Bool(false)
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_NAME_CONTENT_CLASS_W_TAG).unwrap(),
            MapiValue::String("urn:content-classes:message".to_string())
        );
    }

    #[test]
    fn normal_message_row_projects_microsoft_view_descriptor_string8_columns() {
        let email_id = Uuid::from_u128(0x7173);
        let mut email = test_table_email(email_id, Uuid::from_u128(0x8183), "ANSI subject");
        email.from_display = Some("Denis Ducret".to_string());
        email.categories = vec!["Blue".to_string(), "Customer".to_string()];
        let message_class_a = (PID_TAG_MESSAGE_CLASS_W & 0xFFFF_0000) | 0x001E;
        let sent_representing_name_a = (PID_TAG_SENT_REPRESENTING_NAME_W & 0xFFFF_0000) | 0x001E;
        let subject_a = (PID_TAG_SUBJECT_W & 0xFFFF_0000) | 0x001E;
        let keywords_a = (PID_NAME_KEYWORDS_TAG & 0xFFFF_0000) | 0x101E;

        let row = serialize_message_row(
            &email,
            &[
                message_class_a,
                sent_representing_name_a,
                subject_a,
                keywords_a,
            ],
        );
        let mut cursor = Cursor::new(&row);

        assert_eq!(
            parse_mapi_property_value(&mut cursor, message_class_a).unwrap(),
            MapiValue::String("IPM.Note".to_string())
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, sent_representing_name_a).unwrap(),
            MapiValue::String("Denis Ducret".to_string())
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, subject_a).unwrap(),
            MapiValue::String("ANSI subject".to_string())
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, keywords_a).unwrap(),
            MapiValue::MultiString(vec!["Blue".to_string(), "Customer".to_string()])
        );
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
            size_octets: 0,
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
        assert_eq!(
            special_folder_property_value(INBOX_FOLDER_ID, PID_TAG_ACCESS, Uuid::nil()),
            Some(MapiValue::U32(MAPI_FOLDER_ACCESS))
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
    fn reminders_folder_projects_default_post_message_class() {
        assert_eq!(
            special_folder_property_value(
                REMINDERS_FOLDER_ID,
                PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W,
                Uuid::nil()
            ),
            Some(MapiValue::String("IPM.Note".to_string()))
        );

        let row = serialize_special_folder_row(
            REMINDERS_FOLDER_ID,
            &[],
            &[PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W],
            None,
        );

        assert_eq!(row, utf16z_test_bytes("IPM.Note"));
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
    fn special_folder_property_projects_empty_archive_policy_defaults() {
        assert_eq!(
            special_folder_property_value(INBOX_FOLDER_ID, PID_TAG_ARCHIVE_TAG, Uuid::nil()),
            Some(MapiValue::Binary(Vec::new()))
        );
        assert_eq!(
            special_folder_property_value(INBOX_FOLDER_ID, PID_TAG_POLICY_TAG, Uuid::nil()),
            Some(MapiValue::Binary(Vec::new()))
        );
        assert_eq!(
            special_folder_property_value(INBOX_FOLDER_ID, PID_TAG_RETENTION_PERIOD, Uuid::nil()),
            Some(MapiValue::U32(0))
        );
        assert_eq!(
            special_folder_property_value(INBOX_FOLDER_ID, PID_TAG_RETENTION_FLAGS, Uuid::nil()),
            Some(MapiValue::U32(0))
        );
        assert_eq!(
            special_folder_property_value(INBOX_FOLDER_ID, PID_TAG_ARCHIVE_PERIOD, Uuid::nil()),
            Some(MapiValue::U32(0))
        );
    }

    #[test]
    fn special_folder_property_projects_view_defaults_for_outlook_folders() {
        let account_id = Uuid::from_u128(0xaaaaaaaa_aaaa_4aaa_8aaa_aaaaaaaaaaaa);
        for folder_id in [
            INBOX_FOLDER_ID,
            OUTBOX_FOLDER_ID,
            SENT_FOLDER_ID,
            TRASH_FOLDER_ID,
            DRAFTS_FOLDER_ID,
            JUNK_FOLDER_ID,
            ARCHIVE_FOLDER_ID,
            CONVERSATION_HISTORY_FOLDER_ID,
            CONTACTS_SEARCH_FOLDER_ID,
            CONTACTS_FOLDER_ID,
            QUICK_CONTACTS_FOLDER_ID,
            IM_CONTACT_LIST_FOLDER_ID,
            CALENDAR_FOLDER_ID,
        ] {
            assert!(matches!(
                special_folder_property_value(folder_id, PID_TAG_DEFAULT_VIEW_ENTRY_ID, account_id),
                Some(MapiValue::Binary(value)) if !value.is_empty()
            ));
        }
        for folder_id in [
            DEFERRED_ACTION_FOLDER_ID,
            FREEBUSY_DATA_FOLDER_ID,
            TRACKED_MAIL_PROCESSING_FOLDER_ID,
            IPM_SUBTREE_FOLDER_ID,
            JOURNAL_FOLDER_ID,
            NOTES_FOLDER_ID,
            TASKS_FOLDER_ID,
            TODO_SEARCH_FOLDER_ID,
            SYNC_ISSUES_FOLDER_ID,
            CONFLICTS_FOLDER_ID,
            LOCAL_FAILURES_FOLDER_ID,
            SERVER_FAILURES_FOLDER_ID,
            RSS_FEEDS_FOLDER_ID,
            QUICK_STEP_SETTINGS_FOLDER_ID,
        ] {
            assert_eq!(
                special_folder_property_value(folder_id, PID_TAG_DEFAULT_VIEW_ENTRY_ID, account_id),
                None
            );
        }
        assert_eq!(
            special_folder_property_value(INBOX_FOLDER_ID, PID_TAG_FOLDER_FORM_FLAGS, Uuid::nil()),
            Some(MapiValue::U32(0))
        );
        assert_eq!(
            special_folder_property_value(INBOX_FOLDER_ID, PID_TAG_FOLDER_WEBVIEWINFO, Uuid::nil()),
            Some(MapiValue::Binary(Vec::new()))
        );
        assert_eq!(
            special_folder_property_value(INBOX_FOLDER_ID, PID_TAG_FOLDER_XVIEWINFO_E, Uuid::nil()),
            Some(MapiValue::Binary(Vec::new()))
        );
        assert_eq!(
            special_folder_property_value(INBOX_FOLDER_ID, PID_TAG_FOLDER_VIEWS_ONLY, Uuid::nil()),
            Some(MapiValue::U32(0))
        );
        assert_eq!(
            special_folder_property_value(
                INBOX_FOLDER_ID,
                PID_TAG_DEFAULT_FORM_NAME_W,
                Uuid::nil()
            ),
            Some(MapiValue::String(String::new()))
        );
        assert_eq!(
            special_folder_property_value(
                INBOX_FOLDER_ID,
                PID_TAG_FOLDER_FORM_STORAGE,
                Uuid::nil()
            ),
            Some(MapiValue::Binary(Vec::new()))
        );
        assert_eq!(
            special_folder_property_value(INBOX_FOLDER_ID, PID_TAG_ACL_MEMBER_NAME_W, Uuid::nil()),
            Some(MapiValue::String(String::new()))
        );
        assert_eq!(
            special_folder_property_value(
                INBOX_FOLDER_ID,
                PID_TAG_FOLDER_VIEWLIST_FLAGS,
                Uuid::nil()
            ),
            Some(MapiValue::U32(0))
        );
        assert_eq!(
            special_folder_property_value(
                FREEBUSY_DATA_FOLDER_ID,
                PID_TAG_CONTAINER_CLASS_W,
                account_id
            ),
            Some(MapiValue::String("IPF.Note".to_string()))
        );
        assert_eq!(
            special_folder_property_value(
                FREEBUSY_DATA_FOLDER_ID,
                PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W,
                account_id
            ),
            Some(MapiValue::String("IPM.Note".to_string()))
        );
    }

    #[test]
    fn configuration_special_folder_projects_default_post_message_class() {
        assert_eq!(
            special_folder_property_value(
                QUICK_STEP_SETTINGS_FOLDER_ID,
                PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W,
                Uuid::nil()
            ),
            Some(MapiValue::String("IPM.Configuration".to_string()))
        );
        assert_eq!(
            special_folder_property_value(
                QUICK_STEP_SETTINGS_FOLDER_ID,
                PID_TAG_DEFAULT_POST_MESSAGE_CLASS_STRING8,
                Uuid::nil()
            ),
            Some(MapiValue::String("IPM.Configuration".to_string()))
        );

        let row = serialize_special_folder_row(
            QUICK_STEP_SETTINGS_FOLDER_ID,
            &[],
            &[
                PID_TAG_DEFAULT_POST_MESSAGE_CLASS_STRING8,
                PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W,
            ],
            None,
        );
        let ascii = b"IPM.Configuration\0";
        assert!(row.windows(ascii.len()).any(|window| window == ascii));
        assert!(row
            .windows(utf16z_test_bytes("IPM.Configuration").len())
            .any(|window| window == utf16z_test_bytes("IPM.Configuration")));
    }

    #[test]
    fn ipm_subtree_row_projects_default_post_message_class() {
        let row = serialize_special_folder_row(
            IPM_SUBTREE_FOLDER_ID,
            &[],
            &[
                PID_TAG_DEFAULT_POST_MESSAGE_CLASS_STRING8,
                PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W,
            ],
            None,
        );
        let ascii = b"IPM.Note\0";
        assert!(row.windows(ascii.len()).any(|window| window == ascii));
        assert!(row
            .windows(utf16z_test_bytes("IPM.Note").len())
            .any(|window| window == utf16z_test_bytes("IPM.Note")));
    }

    #[test]
    fn ms_oxosfld_none_container_classes_serialize_as_empty_strings() {
        for folder_id in [
            ROOT_FOLDER_ID,
            DEFERRED_ACTION_FOLDER_ID,
            SPOOLER_QUEUE_FOLDER_ID,
            COMMON_VIEWS_FOLDER_ID,
            VIEWS_FOLDER_ID,
        ] {
            let row =
                serialize_special_folder_row(folder_id, &[], &[PID_TAG_CONTAINER_CLASS_W], None);
            assert_eq!(row, utf16z_test_bytes(""));
        }

        let row = serialize_special_folder_row(
            FREEBUSY_DATA_FOLDER_ID,
            &[],
            &[PID_TAG_CONTAINER_CLASS_W],
            None,
        );
        assert_eq!(row, utf16z_test_bytes("IPF.Note"));
    }

    #[test]
    fn attachment_rows_use_by_value_method() {
        let attachment = MapiAttachment {
            attach_num: 0,
            canonical_id: Uuid::nil(),
            file_reference: "file-ref".to_string(),
            file_name: "report.pdf".to_string(),
            media_type: "application/pdf".to_string(),
            disposition: None,
            content_id: None,
            size_octets: 16,
        };

        let row = serialize_attachment_row(&attachment, &[PID_TAG_ATTACH_METHOD]);
        assert_eq!(u32::from_le_bytes(row.try_into().unwrap()), ATTACH_BY_VALUE);
    }

    #[test]
    fn attachment_row_projects_microsoft_message_attachment_example_columns() {
        let attachment = MapiAttachment {
            attach_num: 1,
            canonical_id: Uuid::parse_str("11111111-2222-4333-8444-555555555555").unwrap(),
            file_reference: "attachment:message:one".to_string(),
            file_name: "test.txt".to_string(),
            media_type: "text/plain".to_string(),
            disposition: None,
            content_id: None,
            size_octets: 512,
        };
        let columns = [
            PID_TAG_ATTACH_METHOD,
            PID_TAG_RENDERING_POSITION,
            PID_TAG_ATTACHMENT_FLAGS,
            PID_TAG_DISPLAY_NAME_W,
            PID_TAG_ATTACHMENT_LINK_ID,
            PID_TAG_ATTACH_FLAGS,
            PID_TAG_ATTACHMENT_HIDDEN,
            PID_TAG_ATTACH_LONG_FILENAME_W,
            PID_TAG_ATTACH_FILENAME_W,
            PID_TAG_ATTACH_EXTENSION_W,
            PID_TAG_ATTACH_MIME_TAG_W,
            PID_TAG_ATTACH_CONTENT_ID_W,
            PID_TAG_ATTACH_RENDERING,
            PID_TAG_CREATION_TIME,
            PID_TAG_LAST_MODIFICATION_TIME,
        ];

        let row = serialize_attachment_row(&attachment, &columns);
        let mut cursor = Cursor::new(&row);

        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_ATTACH_METHOD).unwrap(),
            MapiValue::I32(ATTACH_BY_VALUE as i32)
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_RENDERING_POSITION).unwrap(),
            MapiValue::I32(-1)
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_ATTACHMENT_FLAGS).unwrap(),
            MapiValue::I32(0)
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_DISPLAY_NAME_W).unwrap(),
            MapiValue::String("test.txt".to_string())
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_ATTACHMENT_LINK_ID).unwrap(),
            MapiValue::I32(0)
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_ATTACH_FLAGS).unwrap(),
            MapiValue::I32(0)
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_ATTACHMENT_HIDDEN).unwrap(),
            MapiValue::Bool(false)
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_ATTACH_LONG_FILENAME_W).unwrap(),
            MapiValue::String("test.txt".to_string())
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_ATTACH_FILENAME_W).unwrap(),
            MapiValue::String("test.txt".to_string())
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_ATTACH_EXTENSION_W).unwrap(),
            MapiValue::String(".txt".to_string())
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_ATTACH_MIME_TAG_W).unwrap(),
            MapiValue::String("text/plain".to_string())
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_ATTACH_CONTENT_ID_W).unwrap(),
            MapiValue::String(String::new())
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_ATTACH_RENDERING).unwrap(),
            MapiValue::Binary(Vec::new())
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_CREATION_TIME).unwrap(),
            MapiValue::I64(0)
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_LAST_MODIFICATION_TIME).unwrap(),
            MapiValue::I64(0)
        );
    }

    #[test]
    fn attachment_row_projects_microsoft_inline_image_example_columns() {
        let attachment = MapiAttachment {
            attach_num: 1,
            canonical_id: Uuid::parse_str("11111111-2222-4333-8444-555555555555").unwrap(),
            file_reference: "attachment:message:inline-image".to_string(),
            file_name: "image001.PNG".to_string(),
            media_type: "image/png".to_string(),
            disposition: Some("inline".to_string()),
            content_id: Some("image001.PNG@01C86E1C.F1954390".to_string()),
            size_octets: 1024,
        };
        let columns = [
            PID_TAG_ATTACH_METHOD,
            PID_TAG_RENDERING_POSITION,
            PID_TAG_ATTACHMENT_FLAGS,
            PID_TAG_DISPLAY_NAME_W,
            PID_TAG_ATTACHMENT_LINK_ID,
            PID_TAG_ATTACH_FLAGS,
            PID_TAG_ATTACHMENT_HIDDEN,
            PID_TAG_ATTACH_LONG_FILENAME_W,
            PID_TAG_ATTACH_FILENAME_W,
            PID_TAG_ATTACH_EXTENSION_W,
            PID_TAG_ATTACH_MIME_TAG_W,
            PID_TAG_ATTACH_CONTENT_ID_W,
            PID_TAG_ATTACH_RENDERING,
        ];

        for row in [
            serialize_attachment_row(&attachment, &columns),
            serialize_saved_attachment_row(
                attachment.attach_num,
                &attachment.file_reference,
                &attachment.file_name,
                &attachment.media_type,
                attachment.disposition.as_deref(),
                attachment.content_id.as_deref(),
                attachment.size_octets,
                &columns,
            ),
            serialize_pending_attachment_row(
                0,
                &HashMap::from([
                    (PID_TAG_ATTACH_METHOD, MapiValue::U32(ATTACH_BY_VALUE)),
                    (PID_TAG_RENDERING_POSITION, MapiValue::U32(u32::MAX)),
                    (PID_TAG_ATTACHMENT_FLAGS, MapiValue::U32(0)),
                    (
                        PID_TAG_DISPLAY_NAME_W,
                        MapiValue::String("image001.PNG".to_string()),
                    ),
                    (PID_TAG_ATTACHMENT_LINK_ID, MapiValue::U32(0)),
                    (PID_TAG_ATTACH_FLAGS, MapiValue::U32(4)),
                    (PID_TAG_ATTACHMENT_HIDDEN, MapiValue::Bool(true)),
                    (
                        PID_TAG_ATTACH_LONG_FILENAME_W,
                        MapiValue::String("image001.PNG".to_string()),
                    ),
                    (
                        PID_TAG_ATTACH_FILENAME_W,
                        MapiValue::String("image001.PNG".to_string()),
                    ),
                    (
                        PID_TAG_ATTACH_EXTENSION_W,
                        MapiValue::String(".PNG".to_string()),
                    ),
                    (
                        PID_TAG_ATTACH_MIME_TAG_W,
                        MapiValue::String("image/png".to_string()),
                    ),
                    (
                        PID_TAG_ATTACH_CONTENT_ID_W,
                        MapiValue::String("image001.PNG@01C86E1C.F1954390".to_string()),
                    ),
                    (PID_TAG_ATTACH_RENDERING, MapiValue::Binary(Vec::new())),
                ]),
                &[],
                &columns,
            ),
        ] {
            let mut cursor = Cursor::new(&row);
            assert_eq!(
                parse_mapi_property_value(&mut cursor, PID_TAG_ATTACH_METHOD).unwrap(),
                MapiValue::I32(ATTACH_BY_VALUE as i32)
            );
            assert_eq!(
                parse_mapi_property_value(&mut cursor, PID_TAG_RENDERING_POSITION).unwrap(),
                MapiValue::I32(-1)
            );
            assert_eq!(
                parse_mapi_property_value(&mut cursor, PID_TAG_ATTACHMENT_FLAGS).unwrap(),
                MapiValue::I32(0)
            );
            assert_eq!(
                parse_mapi_property_value(&mut cursor, PID_TAG_DISPLAY_NAME_W).unwrap(),
                MapiValue::String("image001.PNG".to_string())
            );
            assert_eq!(
                parse_mapi_property_value(&mut cursor, PID_TAG_ATTACHMENT_LINK_ID).unwrap(),
                MapiValue::I32(0)
            );
            assert_eq!(
                parse_mapi_property_value(&mut cursor, PID_TAG_ATTACH_FLAGS).unwrap(),
                MapiValue::I32(4)
            );
            assert_eq!(
                parse_mapi_property_value(&mut cursor, PID_TAG_ATTACHMENT_HIDDEN).unwrap(),
                MapiValue::Bool(true)
            );
            assert_eq!(
                parse_mapi_property_value(&mut cursor, PID_TAG_ATTACH_LONG_FILENAME_W).unwrap(),
                MapiValue::String("image001.PNG".to_string())
            );
            assert_eq!(
                parse_mapi_property_value(&mut cursor, PID_TAG_ATTACH_FILENAME_W).unwrap(),
                MapiValue::String("image001.PNG".to_string())
            );
            assert_eq!(
                parse_mapi_property_value(&mut cursor, PID_TAG_ATTACH_EXTENSION_W).unwrap(),
                MapiValue::String(".PNG".to_string())
            );
            assert_eq!(
                parse_mapi_property_value(&mut cursor, PID_TAG_ATTACH_MIME_TAG_W).unwrap(),
                MapiValue::String("image/png".to_string())
            );
            assert_eq!(
                parse_mapi_property_value(&mut cursor, PID_TAG_ATTACH_CONTENT_ID_W).unwrap(),
                MapiValue::String("image001.PNG@01C86E1C.F1954390".to_string())
            );
            assert_eq!(
                parse_mapi_property_value(&mut cursor, PID_TAG_ATTACH_RENDERING).unwrap(),
                MapiValue::Binary(Vec::new())
            );
        }
    }

    #[test]
    fn categorized_table_expand_collapse_require_set_columns() {
        let category_id = category_id_for_value(INBOX_FOLDER_ID, PID_TAG_SUBJECT_W, "Alpha");
        let mut table = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: false,
            columns: Vec::new(),
            columns_set: false,
            sort_orders: vec![MapiSortOrder {
                property_tag: PID_TAG_SUBJECT_W,
                order: 0,
            }],
            category_count: 1,
            expanded_count: 1,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        };
        let mut expand_payload = 1u16.to_le_bytes().to_vec();
        expand_payload.extend_from_slice(&category_id.to_le_bytes());
        let expand = rop_expand_row_response(
            &RopRequest {
                rop_id: RopId::ExpandRow.as_u8(),
                input_handle_index: Some(0),
                output_handle_index: None,
                payload: expand_payload,
            },
            Some(&mut table),
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
        );
        assert_eq!(expand[0], RopId::ExpandRow.as_u8());
        assert_eq!(
            u32::from_le_bytes(expand[2..6].try_into().unwrap()),
            0x0000_04B9
        );

        let collapse = rop_collapse_row_response(
            &RopRequest {
                rop_id: RopId::CollapseRow.as_u8(),
                input_handle_index: Some(0),
                output_handle_index: None,
                payload: category_id.to_le_bytes().to_vec(),
            },
            Some(&mut table),
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
        );
        assert_eq!(collapse[0], RopId::CollapseRow.as_u8());
        assert_eq!(
            u32::from_le_bytes(collapse[2..6].try_into().unwrap()),
            0x0000_04B9
        );
    }

    #[test]
    fn microsoft_categorized_expand_collapse_report_current_state_errors() {
        let mailbox_id = Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap();
        let email_id = Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap();
        crate::mapi::identity::remember_mapi_identity(
            email_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 904,
            ),
        );
        let email = test_table_email(email_id, mailbox_id, "Alpha");
        let mailboxes = vec![JmapMailbox {
            id: mailbox_id,
            parent_id: None,
            role: "inbox".to_string(),
            name: "Inbox".to_string(),
            sort_order: 0,
            modseq: 1,
            total_emails: 1,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        }];
        let emails = vec![email];
        let category_id = category_id_for_value(INBOX_FOLDER_ID, PID_TAG_SUBJECT_W, "Alpha");
        let mut expanded_table = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: false,
            columns: vec![PID_TAG_SUBJECT_W],
            columns_set: true,
            sort_orders: vec![MapiSortOrder {
                property_tag: PID_TAG_SUBJECT_W,
                order: 0,
            }],
            category_count: 1,
            expanded_count: 1,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        };
        let mut expand_payload = 1u16.to_le_bytes().to_vec();
        expand_payload.extend_from_slice(&category_id.to_le_bytes());
        let expand = rop_expand_row_response(
            &RopRequest {
                rop_id: RopId::ExpandRow.as_u8(),
                input_handle_index: Some(0),
                output_handle_index: None,
                payload: expand_payload,
            },
            Some(&mut expanded_table),
            &mailboxes,
            &emails,
            &MapiMailStoreSnapshot::empty(),
        );
        assert_eq!(expand[0], RopId::ExpandRow.as_u8());
        assert_eq!(
            u32::from_le_bytes(expand[2..6].try_into().unwrap()),
            0x0000_04F8
        );

        let mut collapsed_categories = HashSet::new();
        collapsed_categories.insert(category_id);
        let mut collapsed_table = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: false,
            columns: vec![PID_TAG_SUBJECT_W],
            columns_set: true,
            sort_orders: vec![MapiSortOrder {
                property_tag: PID_TAG_SUBJECT_W,
                order: 0,
            }],
            category_count: 1,
            expanded_count: 1,
            collapsed_categories,
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        };
        let collapse = rop_collapse_row_response(
            &RopRequest {
                rop_id: RopId::CollapseRow.as_u8(),
                input_handle_index: Some(0),
                output_handle_index: None,
                payload: category_id.to_le_bytes().to_vec(),
            },
            Some(&mut collapsed_table),
            &mailboxes,
            &emails,
            &MapiMailStoreSnapshot::empty(),
        );
        assert_eq!(collapse[0], RopId::CollapseRow.as_u8());
        assert_eq!(
            u32::from_le_bytes(collapse[2..6].try_into().unwrap()),
            0x0000_04F7
        );
    }

    #[test]
    fn microsoft_table_bookmark_and_collapse_rops_require_set_columns() {
        fn table() -> MapiObject {
            let bookmark = 1u32.to_le_bytes().to_vec();
            let mut bookmarks = HashMap::new();
            bookmarks.insert(
                bookmark,
                TableBookmark {
                    position: 0,
                    row_key: None,
                },
            );
            MapiObject::ContentsTable {
                folder_id: INBOX_FOLDER_ID,
                associated: false,
                columns: Vec::new(),
                columns_set: false,
                sort_orders: Vec::new(),
                category_count: 1,
                expanded_count: 1,
                collapsed_categories: HashSet::new(),
                restriction: None,
                bookmarks,
                next_bookmark: 2,
                position: 0,
            }
        }

        let bookmark = 1u32.to_le_bytes().to_vec();
        let mut seek_payload = Vec::new();
        seek_payload.extend_from_slice(&(bookmark.len() as u16).to_le_bytes());
        seek_payload.extend_from_slice(&bookmark);
        seek_payload.extend_from_slice(&0i32.to_le_bytes());
        seek_payload.push(0);
        let mut seek_table = table();
        let seek = rop_seek_row_bookmark_response(
            &RopRequest {
                rop_id: RopId::SeekRowBookmark.as_u8(),
                input_handle_index: Some(0),
                output_handle_index: None,
                payload: seek_payload,
            },
            Some(&mut seek_table),
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
            Uuid::nil(),
        );
        assert_eq!(seek[0], RopId::SeekRowBookmark.as_u8());
        assert_eq!(
            u32::from_le_bytes(seek[2..6].try_into().unwrap()),
            0x0000_04B9
        );

        let mut free_payload = Vec::new();
        free_payload.extend_from_slice(&(bookmark.len() as u16).to_le_bytes());
        free_payload.extend_from_slice(&bookmark);
        let mut free_table = table();
        let free = rop_free_bookmark_response(
            &RopRequest {
                rop_id: RopId::FreeBookmark.as_u8(),
                input_handle_index: Some(0),
                output_handle_index: None,
                payload: free_payload,
            },
            Some(&mut free_table),
        );
        assert_eq!(free[0], RopId::FreeBookmark.as_u8());
        assert_eq!(
            u32::from_le_bytes(free[2..6].try_into().unwrap()),
            0x0000_04B9
        );

        let get = rop_get_collapse_state_response(
            &RopRequest {
                rop_id: RopId::GetCollapseState.as_u8(),
                input_handle_index: Some(0),
                output_handle_index: None,
                payload: Vec::new(),
            },
            Some(&table()),
        );
        assert_eq!(get[0], RopId::GetCollapseState.as_u8());
        assert_eq!(
            u32::from_le_bytes(get[2..6].try_into().unwrap()),
            0x0000_04B9
        );

        let mut collapse_state = Vec::new();
        collapse_state.extend_from_slice(COLLAPSE_STATE_MAGIC);
        write_u64(&mut collapse_state, INBOX_FOLDER_ID);
        write_u64(&mut collapse_state, 0);
        write_u32(&mut collapse_state, 0);
        write_u32(&mut collapse_state, 0);
        write_u16(&mut collapse_state, 1);
        write_u16(&mut collapse_state, 1);
        write_u16(&mut collapse_state, 0);
        let mut set_payload = Vec::new();
        set_payload.extend_from_slice(&(collapse_state.len() as u16).to_le_bytes());
        set_payload.extend_from_slice(&collapse_state);
        let mut set_table = table();
        let set = rop_set_collapse_state_response(
            &RopRequest {
                rop_id: RopId::SetCollapseState.as_u8(),
                input_handle_index: Some(0),
                output_handle_index: None,
                payload: set_payload,
            },
            Some(&mut set_table),
        );
        assert_eq!(set[0], RopId::SetCollapseState.as_u8());
        assert_eq!(
            u32::from_le_bytes(set[2..6].try_into().unwrap()),
            0x0000_04B9
        );
    }

    #[test]
    fn microsoft_contents_table_query_find_and_expand_require_set_columns() {
        let mut table = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: false,
            columns: Vec::new(),
            columns_set: false,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        };

        let query = rop_query_rows_response(
            &RopRequest {
                rop_id: RopId::QueryRows.as_u8(),
                input_handle_index: Some(0),
                output_handle_index: None,
                payload: vec![0, 1, 1, 0],
            },
            Some(&mut table),
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
            Uuid::nil(),
        );
        assert_eq!(query[0], RopId::QueryRows.as_u8());
        assert_eq!(
            u32::from_le_bytes(query[2..6].try_into().unwrap()),
            0x0000_04B9
        );

        let mut restriction = vec![MapiRestrictionType::Property as u8, 0x04];
        restriction.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
        restriction.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
        write_utf16z(&mut restriction, "Alpha");
        let mut find_payload = vec![0];
        find_payload.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
        find_payload.extend_from_slice(&restriction);
        find_payload.push(1);
        find_payload.extend_from_slice(&0u16.to_le_bytes());
        let find = rop_find_row_response(
            &RopRequest {
                rop_id: RopId::FindRow.as_u8(),
                input_handle_index: Some(0),
                output_handle_index: None,
                payload: find_payload,
            },
            Some(&mut table),
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
            Uuid::nil(),
        );
        assert_eq!(find[0], RopId::FindRow.as_u8());
        assert_eq!(
            u32::from_le_bytes(find[2..6].try_into().unwrap()),
            0x0000_04B9
        );

        let mut expand_payload = 1u16.to_le_bytes().to_vec();
        expand_payload.extend_from_slice(&0u64.to_le_bytes());
        let expand = rop_expand_row_response(
            &RopRequest {
                rop_id: RopId::ExpandRow.as_u8(),
                input_handle_index: Some(0),
                output_handle_index: None,
                payload: expand_payload,
            },
            Some(&mut table),
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
        );
        assert_eq!(expand[0], RopId::ExpandRow.as_u8());
        assert_eq!(
            u32::from_le_bytes(expand[2..6].try_into().unwrap()),
            0x0000_04B9
        );
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
    let content_id = pending_attachment_content_id(properties);
    let hidden = pending_attachment_hidden(properties);
    let size = data.len().min(u32::MAX as usize) as u32;
    let mut row = Vec::new();
    for column in columns {
        if let Some(value) = properties.get(column) {
            write_mapi_value(&mut row, *column, value);
            continue;
        }
        match *column {
            PID_TAG_ATTACH_NUM => write_u32(&mut row, attach_num),
            PID_TAG_DISPLAY_NAME_W | PID_TAG_ATTACH_FILENAME_W | PID_TAG_ATTACH_LONG_FILENAME_W => {
                write_utf16z(&mut row, &file_name)
            }
            PID_TAG_ATTACH_EXTENSION_W => {
                write_utf16z(&mut row, &attachment_file_extension(&file_name))
            }
            PID_TAG_ATTACH_MIME_TAG_W => write_utf16z(&mut row, &media_type),
            PID_TAG_ATTACH_SIZE => write_u32(&mut row, size),
            PID_TAG_ATTACH_METHOD => write_u32(
                &mut row,
                attachment_method_value_from_metadata(&media_type, &file_name),
            ),
            PID_TAG_RENDERING_POSITION => write_u32(&mut row, u32::MAX),
            PID_TAG_ATTACHMENT_FLAGS | PID_TAG_ATTACHMENT_LINK_ID => write_u32(&mut row, 0),
            PID_TAG_ATTACH_FLAGS => {
                write_u32(&mut row, if content_id.is_some() || hidden { 4 } else { 0 })
            }
            PID_TAG_ATTACHMENT_HIDDEN => {
                row.push(if content_id.is_some() || hidden { 1 } else { 0 })
            }
            PID_TAG_ATTACH_CONTENT_ID_W => {
                write_utf16z(&mut row, content_id.as_deref().unwrap_or(""))
            }
            PID_TAG_ATTACH_RENDERING => write_u16_prefixed_bytes(&mut row, &[]),
            PID_TAG_CREATION_TIME | PID_TAG_LAST_MODIFICATION_TIME => write_u64(&mut row, 0),
            PID_TAG_ATTACH_DATA_BINARY => write_u16_prefixed_bytes(&mut row, data),
            _ => write_property_default(&mut row, *column),
        }
    }
    row
}

fn pending_attachment_content_id(properties: &HashMap<u32, MapiValue>) -> Option<String> {
    optional_pending_text_property(properties, &[PID_TAG_ATTACH_CONTENT_ID_W])
        .map(|value| value.trim().trim_matches(['<', '>']).to_string())
        .filter(|value| !value.is_empty())
}

fn pending_attachment_hidden(properties: &HashMap<u32, MapiValue>) -> bool {
    properties
        .get(&PID_TAG_ATTACHMENT_HIDDEN)
        .and_then(MapiValue::as_bool)
        .unwrap_or(false)
}

pub(in crate::mapi) fn serialize_saved_attachment_row(
    attach_num: u32,
    file_reference: &str,
    file_name: &str,
    media_type: &str,
    disposition: Option<&str>,
    content_id: Option<&str>,
    size_octets: u64,
    columns: &[u32],
) -> Vec<u8> {
    let is_inline = disposition.is_some_and(|value| value.eq_ignore_ascii_case("inline"))
        || content_id.is_some();
    let mut row = Vec::new();
    for column in columns {
        match *column {
            PID_TAG_ATTACH_NUM => write_u32(&mut row, attach_num),
            PID_TAG_DISPLAY_NAME_W | PID_TAG_ATTACH_FILENAME_W | PID_TAG_ATTACH_LONG_FILENAME_W => {
                write_utf16z(&mut row, file_name)
            }
            PID_TAG_ATTACH_EXTENSION_W => {
                write_utf16z(&mut row, &attachment_file_extension(file_name))
            }
            PID_TAG_ATTACH_MIME_TAG_W => write_utf16z(&mut row, media_type),
            PID_TAG_ATTACH_SIZE => write_u32(&mut row, size_octets.min(u32::MAX as u64) as u32),
            PID_TAG_ATTACH_METHOD => write_u32(&mut row, ATTACH_BY_VALUE),
            PID_TAG_RENDERING_POSITION => write_u32(&mut row, u32::MAX),
            PID_TAG_ATTACHMENT_FLAGS | PID_TAG_ATTACHMENT_LINK_ID => write_u32(&mut row, 0),
            PID_TAG_ATTACH_FLAGS => write_u32(&mut row, if is_inline { 4 } else { 0 }),
            PID_TAG_ATTACHMENT_HIDDEN => row.push(if is_inline { 1 } else { 0 }),
            PID_TAG_ATTACH_CONTENT_ID_W => write_utf16z(&mut row, content_id.unwrap_or("")),
            PID_TAG_ATTACH_RENDERING => write_u16_prefixed_bytes(&mut row, &[]),
            PID_TAG_CREATION_TIME | PID_TAG_LAST_MODIFICATION_TIME => write_u64(&mut row, 0),
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
            PID_TAG_ROW_TYPE => write_u32(&mut row, TABLE_LEAF_ROW),
            PID_TAG_DEPTH => write_u32(&mut row, 0),
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
            PID_TAG_MESSAGE_SIZE_EXTENDED => write_u64(
                &mut row,
                body_text
                    .len()
                    .saturating_add(item.item.subject.len())
                    .min(i64::MAX as usize) as u64,
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

fn public_folder_item_size(item: &MapiPublicFolderItem) -> i64 {
    item.item
        .body_text
        .len()
        .saturating_add(item.item.subject.len())
        .min(i64::MAX as usize) as i64
}

fn public_folder_item_property_value(
    item: &MapiPublicFolderItem,
    property_tag: u32,
) -> Option<MapiValue> {
    let change_number = mapi_mailstore::change_number_for_store_id(item.id);
    let message_class = if item.item.message_class.trim().is_empty() {
        "IPM.Post"
    } else {
        item.item.message_class.as_str()
    };
    match canonical_property_storage_tag(property_tag) {
        PID_TAG_MID | PID_TAG_INST_ID => Some(MapiValue::U64(item.id)),
        PID_TAG_INSTANCE_NUM | PID_TAG_DEPTH => Some(MapiValue::U32(0)),
        PID_TAG_ROW_TYPE => Some(MapiValue::U32(TABLE_LEAF_ROW)),
        PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W | PID_TAG_DISPLAY_NAME_W => {
            Some(MapiValue::String(item.item.subject.clone()))
        }
        PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String(message_class.to_string())),
        PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_MESSAGE_ACCESS)),
        PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(0)),
        PID_TAG_READ => Some(MapiValue::Bool(item.item.is_read)),
        PID_TAG_MESSAGE_SIZE => Some(mapi_message_size_value(public_folder_item_size(item))),
        PID_TAG_MESSAGE_SIZE_EXTENDED => Some(mapi_message_size_extended_value(
            public_folder_item_size(item),
        )),
        PID_TAG_HAS_ATTACHMENTS => Some(MapiValue::Bool(false)),
        PID_TAG_BODY_W => Some(MapiValue::String(item.item.body_text.clone())),
        PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
            crate::mapi::identity::instance_key_for_object_id(item.id),
        )),
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(item.folder_id),
        )),
        PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
            item.id,
        ))),
        PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::change_key_for_change_number(change_number),
        )),
        PID_TAG_PREDECESSOR_CHANGE_LIST => Some(MapiValue::Binary(
            mapi_mailstore::predecessor_change_list(change_number),
        )),
        PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(change_number)),
        _ => None,
    }
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
            PID_TAG_MESSAGE_SIZE_EXTENDED => {
                write_u64(&mut row, item.item.size_octets.max(0) as u64)
            }
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

fn recoverable_item_property_value(
    item: &crate::mapi_store::MapiRecoverableItemMessage,
    property_tag: u32,
) -> Option<MapiValue> {
    let change_number = mapi_mailstore::change_number_for_store_id(item.id);
    match canonical_property_storage_tag(property_tag) {
        PID_TAG_MID | PID_TAG_INST_ID => Some(MapiValue::U64(item.id)),
        PID_TAG_INSTANCE_NUM | PID_TAG_DEPTH => Some(MapiValue::U32(0)),
        PID_TAG_ROW_TYPE => Some(MapiValue::U32(TABLE_LEAF_ROW)),
        PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
            Some(MapiValue::String(item.item.subject.clone()))
        }
        PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String("IPM.Note".to_string())),
        PID_TAG_MESSAGE_DELIVERY_TIME
        | PID_TAG_LAST_MODIFICATION_TIME
        | PID_TAG_LOCAL_COMMIT_TIME => Some(MapiValue::I64(
            mapi_mailstore::filetime_from_rfc3339_utc(&item.item.received_at) as i64,
        )),
        PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_MESSAGE_ACCESS)),
        PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(MSGFLAG_READ)),
        PID_TAG_READ => Some(MapiValue::Bool(true)),
        PID_TAG_MESSAGE_SIZE => Some(MapiValue::U32(
            item.item.size_octets.clamp(0, u32::MAX as i64) as u32,
        )),
        PID_TAG_MESSAGE_SIZE_EXTENDED => {
            Some(mapi_message_size_extended_value(item.item.size_octets))
        }
        PID_TAG_SENDER_NAME_W | PID_TAG_SENDER_EMAIL_ADDRESS_W => {
            Some(MapiValue::String(item.item.sender_address.clone()))
        }
        PID_TAG_DISPLAY_TO_W | PID_TAG_DISPLAY_CC_W | PID_TAG_BODY_W => {
            Some(MapiValue::String(String::new()))
        }
        PID_TAG_HAS_ATTACHMENTS => Some(MapiValue::Bool(item.item.has_attachments)),
        PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
            crate::mapi::identity::instance_key_for_object_id(item.id),
        )),
        PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
            item.id,
        ))),
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(item.folder_id),
        )),
        PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::change_key_for_change_number(change_number),
        )),
        PID_TAG_PREDECESSOR_CHANGE_LIST => Some(MapiValue::Binary(
            mapi_mailstore::predecessor_change_list(change_number),
        )),
        PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(change_number)),
        _ => None,
    }
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

fn property_tag_id_matches(left: u32, right: u32) -> bool {
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
    let entry_target = navigation_shortcut_property_by_id(properties, &PID_TAG_WLINK_ENTRY_ID)
        .and_then(|value| match value {
            MapiValue::Binary(bytes) => navigation_shortcut_folder_id_from_entry_id(bytes),
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
    let group_header_id = navigation_shortcut_property_by_id(
        properties,
        if shortcut_type == 4 {
            &PID_TAG_WLINK_GROUP_HEADER_ID
        } else {
            &PID_TAG_WLINK_GROUP_CLSID
        },
    )
    .and_then(navigation_shortcut_guid_value);
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
        save_stamp: properties
            .get(&PID_TAG_WLINK_SAVE_STAMP)
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

fn navigation_shortcut_folder_id_from_entry_id(bytes: &[u8]) -> Option<u64> {
    crate::mapi::identity::object_id_from_folder_identifier_bytes(bytes).or_else(|| {
        bytes
            .windows(46)
            .find_map(crate::mapi::identity::object_id_from_folder_entry_id)
    })
}

fn navigation_shortcut_property_by_id<'a>(
    properties: &'a HashMap<u32, MapiValue>,
    property_tag: &u32,
) -> Option<&'a MapiValue> {
    properties.get(property_tag).or_else(|| {
        properties
            .iter()
            .find(|(tag, _)| property_tag_id_matches(**tag, *property_tag))
            .map(|(_, value)| value)
    })
}

fn navigation_shortcut_guid_value(value: &MapiValue) -> Option<Uuid> {
    match value {
        MapiValue::Guid(value) => Some(Uuid::from_bytes(*value)),
        MapiValue::Binary(value) => value
            .get(..16)
            .and_then(|bytes| <[u8; 16]>::try_from(bytes).ok())
            .map(Uuid::from_bytes),
        _ => None,
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
        match contact_table_property_value(contact, item_id, folder_id, *column) {
            Some(value) => write_mapi_value(&mut row, *column, &value),
            None => write_property_default(&mut row, *column),
        }
    }
    row
}

fn contact_table_property_value(
    contact: &AccessibleContact,
    item_id: u64,
    folder_id: u64,
    property_tag: u32,
) -> Option<MapiValue> {
    contact_property_value(contact, item_id, folder_id, property_tag).or_else(|| {
        outlook_contact_empty_email_table_value(canonical_property_storage_tag(property_tag))
    })
}

fn outlook_contact_empty_email_table_value(property_tag: u32) -> Option<MapiValue> {
    match property_tag {
        PID_LID_EMAIL2_ADDRESS_TYPE_W_TAG
        | PID_LID_EMAIL2_DISPLAY_NAME_W_TAG
        | PID_LID_EMAIL2_EMAIL_ADDRESS_W_TAG
        | PID_LID_EMAIL2_ORIGINAL_DISPLAY_NAME_W_TAG
        | PID_LID_EMAIL3_ADDRESS_TYPE_W_TAG
        | PID_LID_EMAIL3_DISPLAY_NAME_W_TAG
        | PID_LID_EMAIL3_EMAIL_ADDRESS_W_TAG
        | PID_LID_EMAIL3_ORIGINAL_DISPLAY_NAME_W_TAG => Some(MapiValue::String(String::new())),
        _ => None,
    }
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

pub(in crate::mapi) fn serialize_pending_associated_message_row(
    principal: &AccountPrincipal,
    properties: &HashMap<u32, MapiValue>,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        if let Some(value) =
            pending_associated_message_property_value(principal, properties, *column)
        {
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
            PID_TAG_ACCESS_LEVEL => Some(MapiValue::U32(1)),
            PID_TAG_IMPORTANCE => Some(MapiValue::U32(1)),
            PID_TAG_PRIORITY | PID_TAG_SENSITIVITY => Some(MapiValue::U32(0)),
            PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(MSGFLAG_UNSENT | MSGFLAG_READ)),
            PID_TAG_HAS_ATTACHMENTS => Some(MapiValue::Bool(false)),
            PID_TAG_TRUST_SENDER => Some(MapiValue::U32(1)),
            PID_TAG_HAS_NAMED_PROPERTIES => Some(MapiValue::Bool(false)),
            PID_TAG_DISPLAY_BCC_W | PID_TAG_DISPLAY_CC_W | PID_TAG_DISPLAY_TO_W => {
                Some(MapiValue::String(String::new()))
            }
            PID_TAG_MESSAGE_SIZE => Some(mapi_message_size_value(pending_message_size(properties))),
            PID_TAG_MESSAGE_SIZE_EXTENDED => Some(mapi_message_size_extended_value(
                pending_message_size(properties),
            )),
            PID_TAG_CREATION_TIME | PID_TAG_LAST_MODIFICATION_TIME => properties
                .get(&PID_TAG_CREATION_TIME)
                .cloned()
                .or_else(|| properties.get(&PID_TAG_LAST_MODIFICATION_TIME).cloned())
                .or(Some(MapiValue::U64(0))),
            PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
                mapi_mailstore::change_key_for_change_number(pending_message_change_number(
                    properties,
                )),
            )),
            PID_TAG_PREDECESSOR_CHANGE_LIST => Some(MapiValue::Binary(
                mapi_mailstore::predecessor_change_list(pending_message_change_number(properties)),
            )),
            PID_TAG_CHANGE_NUMBER => {
                Some(MapiValue::U64(pending_message_change_number(properties)))
            }
            PID_TAG_SEARCH_KEY => Some(MapiValue::Binary(pending_message_search_key(properties))),
            PID_TAG_MESSAGE_LOCALE_ID => Some(MapiValue::U32(0x0409)),
            PID_TAG_LOCALE_ID => Some(MapiValue::U32(0x0409)),
            PID_TAG_CREATOR_NAME_W | PID_TAG_LAST_MODIFIER_NAME_W => {
                Some(MapiValue::String(principal.display_name.clone()))
            }
            PID_TAG_CREATOR_ENTRY_ID | PID_TAG_LAST_MODIFIER_ENTRY_ID => {
                Some(MapiValue::Binary(mailbox_owner_entry_id(principal)))
            }
            PID_TAG_SENDER_NAME_W => Some(MapiValue::String(principal.display_name.clone())),
            PID_TAG_SENDER_EMAIL_ADDRESS_W => Some(MapiValue::String(principal.email.clone())),
            _ => None,
        })
}

fn pending_associated_message_property_value(
    principal: &AccountPrincipal,
    properties: &HashMap<u32, MapiValue>,
    property_tag: u32,
) -> Option<MapiValue> {
    let lookup_tag = canonical_property_storage_tag(property_tag);
    properties
        .get(&lookup_tag)
        .cloned()
        .or_else(|| match lookup_tag {
            PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String("IPM.Configuration".to_string())),
            PID_TAG_ROAMING_DICTIONARY => {
                Some(MapiValue::Binary(minimal_roaming_dictionary_stream()))
            }
            _ => pending_message_property_value(principal, properties, property_tag),
        })
}

fn pending_message_search_key(properties: &HashMap<u32, MapiValue>) -> Vec<u8> {
    properties
        .get(&PID_TAG_MID)
        .and_then(mapi_value_u64)
        .filter(|message_id| {
            crate::mapi::identity::global_counter_from_store_id(*message_id).is_some()
        })
        .map(mapi_mailstore::source_key_for_store_id)
        .unwrap_or_else(|| {
            mapi_mailstore::change_key_for_change_number(pending_message_change_number(properties))
        })
}

fn pending_message_change_number(properties: &HashMap<u32, MapiValue>) -> u64 {
    properties
        .get(&PID_TAG_CHANGE_NUMBER)
        .and_then(mapi_value_u64)
        .or_else(|| {
            properties
                .get(&PID_TAG_MID)
                .and_then(mapi_value_u64)
                .and_then(crate::mapi::identity::global_counter_from_store_id)
        })
        .or_else(|| {
            properties
                .get(&PID_TAG_SOURCE_KEY)
                .and_then(|value| match value {
                    MapiValue::Binary(value) => {
                        crate::mapi::identity::object_id_from_source_key(value)
                            .and_then(crate::mapi::identity::global_counter_from_store_id)
                    }
                    _ => None,
                })
        })
        .unwrap_or(1)
}

fn mapi_value_u64(value: &MapiValue) -> Option<u64> {
    match value {
        MapiValue::I16(value) => u64::try_from(*value).ok(),
        MapiValue::I32(value) => u64::try_from(*value).ok(),
        MapiValue::I64(value) => u64::try_from(*value).ok(),
        MapiValue::U32(value) => Some(u64::from(*value)),
        MapiValue::U64(value) => Some(*value),
        _ => None,
    }
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
        ..Default::default()
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

pub(in crate::mapi) fn display_bcc(email: &JmapEmail) -> String {
    email
        .bcc
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
    pub(in crate::mapi) order: u32,
    pub(in crate::mapi) address: &'a JmapEmailAddress,
}

pub(in crate::mapi) fn message_recipients(email: &JmapEmail) -> Vec<MapiRecipient<'_>> {
    let recipients = email
        .to
        .iter()
        .map(|address| MapiRecipient {
            recipient_type: 0x01,
            order: 0,
            address,
        })
        .chain(email.cc.iter().map(|address| MapiRecipient {
            recipient_type: 0x02,
            order: 0,
            address,
        }))
        .chain(
            message_can_expose_bcc(email)
                .then_some(email.bcc.iter())
                .into_iter()
                .flatten()
                .map(|address| MapiRecipient {
                    recipient_type: 0x03,
                    order: 0,
                    address,
                }),
        )
        .collect::<Vec<_>>();
    recipients
        .into_iter()
        .enumerate()
        .map(|(order, mut recipient)| {
            recipient.order = order.min(u32::MAX as usize) as u32;
            recipient
        })
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

pub(in crate::mapi) fn read_flags_are_valid(read_flags: Option<u8>, allow_default: bool) -> bool {
    let Some(flags) = read_flags else {
        return false;
    };
    const RF_SUPPRESS_RECEIPT: u8 = 0x01;
    const RF_RESERVED: u8 = 0x0A;
    const RF_CLEAR_READ_FLAG: u8 = 0x04;
    const RF_GENERATE_RECEIPT_ONLY: u8 = 0x10;
    const RF_CLEAR_NOTIFY_READ: u8 = 0x20;
    const RF_CLEAR_NOTIFY_UNREAD: u8 = 0x40;
    const RF_KNOWN_MASK: u8 = RF_SUPPRESS_RECEIPT
        | RF_RESERVED
        | RF_CLEAR_READ_FLAG
        | RF_GENERATE_RECEIPT_ONLY
        | RF_CLEAR_NOTIFY_READ
        | RF_CLEAR_NOTIFY_UNREAD;

    if flags & !RF_KNOWN_MASK != 0 {
        return false;
    }
    let effective = flags & !RF_RESERVED;
    let valid = matches!(effective, 0x00 | 0x01 | 0x05 | 0x10 | 0x20 | 0x40);
    valid && (allow_default || effective != 0)
}

pub(in crate::mapi) fn folder_message_class(mailbox: &JmapMailbox) -> &'static str {
    if let Some(folder_id) = mailbox_advertised_special_folder_id(mailbox) {
        return special_folder_metadata(folder_id).2;
    }
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

pub(in crate::mapi) fn mailbox_projects_hidden_attribute(mailbox: &JmapMailbox) -> bool {
    matches!(
        mailbox_advertised_special_folder_id(mailbox),
        Some(CONVERSATION_ACTION_SETTINGS_FOLDER_ID | QUICK_STEP_SETTINGS_FOLDER_ID)
    )
}

fn mailbox_advertised_special_folder_id(mailbox: &JmapMailbox) -> Option<u64> {
    if mapi_parent_folder_id(mailbox) != IPM_SUBTREE_FOLDER_ID {
        return None;
    }
    advertised_special_folder_id_for_create(IPM_SUBTREE_FOLDER_ID, mailbox.name.trim())
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
        Some(MapiPropertyType::Floating32) => row.extend_from_slice(&0.0f32.to_le_bytes()),
        Some(MapiPropertyType::Floating64) => row.extend_from_slice(&0.0f64.to_le_bytes()),
        Some(MapiPropertyType::Boolean) => row.push(0),
        Some(MapiPropertyType::Integer64) | Some(MapiPropertyType::Time) => write_u64(row, 0),
        Some(MapiPropertyType::String8) => write_ascii_z(row, ""),
        Some(MapiPropertyType::String) => write_utf16z(row, ""),
        Some(MapiPropertyType::Guid) => row.extend_from_slice(Uuid::nil().as_bytes()),
        Some(MapiPropertyType::ServerId | MapiPropertyType::Binary) => write_rop_binary(row, &[]),
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
    try_mapi_folder_id_for_role(&mailbox.role)
        .or_else(|| crate::mapi::identity::mapped_mapi_object_id(&mailbox.id))
}

fn try_mapi_folder_id_for_role(role: &str) -> Option<u64> {
    match role {
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
        _ => None,
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
    if mapi_folder_id(mailbox) == SYNC_ISSUES_FOLDER_ID {
        return false;
    }
    !mailboxes.is_empty()
        && mailboxes
            .iter()
            .any(|candidate| candidate.parent_id == Some(mailbox.id))
}

pub(in crate::mapi) fn mapi_message_id(email: &JmapEmail) -> u64 {
    mapi_item_id(&email.id)
}

pub(in crate::mapi) fn mapi_folder_id_for_email(email: &JmapEmail) -> u64 {
    try_mapi_folder_id_for_role(&email.mailbox_role)
        .or_else(|| crate::mapi::identity::mapped_mapi_object_id(&email.mailbox_id))
        .unwrap_or(IPM_SUBTREE_FOLDER_ID)
}

pub(in crate::mapi) fn mapi_item_id(id: &Uuid) -> u64 {
    crate::mapi::identity::mapped_mapi_object_id(id).expect("MAPI item identity mapping missing")
}
