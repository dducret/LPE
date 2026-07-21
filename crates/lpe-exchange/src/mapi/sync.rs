use super::rop::*;
use super::session::*;
use super::tables::*;
use super::*;

use crate::mapi::properties::*;
use crate::mapi::wire::RopId;
use lpe_storage::SearchFolderDefinition;

pub(in crate::mapi) use super::identity::{
    ARCHIVE_FOLDER_ID, CALENDAR_FOLDER_ID, COMMON_VIEWS_FOLDER_ID, CONFLICTS_FOLDER_ID,
    CONTACTS_FOLDER_ID, CONTACTS_SEARCH_FOLDER_ID, CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
    CONVERSATION_HISTORY_FOLDER_ID, DEFERRED_ACTION_FOLDER_ID, DOCUMENT_LIBRARIES_FOLDER_ID,
    DRAFTS_FOLDER_ID, FREEBUSY_DATA_FOLDER_ID, IM_CONTACT_LIST_FOLDER_ID, INBOX_FOLDER_ID,
    IPM_SUBTREE_FOLDER_ID, JOURNAL_FOLDER_ID, JUNK_FOLDER_ID, LOCAL_FAILURES_FOLDER_ID,
    NOTES_FOLDER_ID, OUTBOX_FOLDER_ID, PUBLIC_FOLDERS_ROOT_FOLDER_ID, QUICK_CONTACTS_FOLDER_ID,
    QUICK_STEP_SETTINGS_FOLDER_ID, REMINDERS_FOLDER_ID, ROOT_FOLDER_ID, RSS_FEEDS_FOLDER_ID,
    SCHEDULE_FOLDER_ID, SEARCH_FOLDER_ID, SENT_FOLDER_ID, SERVER_FAILURES_FOLDER_ID,
    SHORTCUTS_FOLDER_ID, SPOOLER_QUEUE_FOLDER_ID, STORE_REPLICA_ID, SUGGESTED_CONTACTS_FOLDER_ID,
    SYNC_ISSUES_FOLDER_ID, TASKS_FOLDER_ID, TODO_SEARCH_FOLDER_ID,
    TRACKED_MAIL_PROCESSING_FOLDER_ID, TRASH_FOLDER_ID, VIEWS_FOLDER_ID,
};

mod responses;
mod scope;

pub(in crate::mapi) use responses::*;
pub(in crate::mapi) use scope::*;

pub(in crate::mapi) const PID_TAG_ROAMING_DATATYPES: u32 = 0x7C06_0003;
pub(in crate::mapi) const PID_TAG_ROAMING_DICTIONARY: u32 = 0x7C07_0102;
pub(in crate::mapi) const PID_TAG_ROAMING_XML_STREAM: u32 = 0x7C08_0102;

pub(in crate::mapi) const PRIVATE_LOGON_SPECIAL_FOLDER_IDS: [u64; 13] = [
    ROOT_FOLDER_ID,
    DEFERRED_ACTION_FOLDER_ID,
    SPOOLER_QUEUE_FOLDER_ID,
    IPM_SUBTREE_FOLDER_ID,
    INBOX_FOLDER_ID,
    OUTBOX_FOLDER_ID,
    SENT_FOLDER_ID,
    TRASH_FOLDER_ID,
    COMMON_VIEWS_FOLDER_ID,
    SCHEDULE_FOLDER_ID,
    SEARCH_FOLDER_ID,
    VIEWS_FOLDER_ID,
    SHORTCUTS_FOLDER_ID,
];

pub(in crate::mapi) const PUBLIC_LOGON_SPECIAL_FOLDER_IDS: [u64; 1] =
    [PUBLIC_FOLDERS_ROOT_FOLDER_ID];

pub(in crate::mapi) fn sync_emails_for(
    folder_id: u64,
    sync_type: u8,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
) -> Vec<JmapEmail> {
    if sync_type == 0x02 {
        return Vec::new();
    }

    emails_for_folder(folder_id, mailboxes, emails)
        .into_iter()
        .cloned()
        .collect()
}

pub(in crate::mapi) fn sync_checkpoint_kind(sync_type: u8) -> MapiCheckpointKind {
    if sync_type == 0x02 {
        MapiCheckpointKind::Hierarchy
    } else {
        MapiCheckpointKind::Content
    }
}

pub(in crate::mapi) fn sync_checkpoint_mailbox_id(
    folder_id: u64,
    sync_type: u8,
    mailboxes: &[JmapMailbox],
) -> Option<Uuid> {
    if sync_type == 0x02 {
        return None;
    }
    mailboxes
        .iter()
        .find(|mailbox| mapi_folder_id(mailbox) == folder_id)
        .map(|mailbox| mailbox.id)
        .or_else(|| {
            crate::mapi_mailstore::virtual_special_mailbox(folder_id).map(|mailbox| mailbox.id)
        })
}

pub(in crate::mapi) fn changed_sync_mailboxes(
    mailboxes: Vec<JmapMailbox>,
    changed_ids: &[Uuid],
) -> Vec<JmapMailbox> {
    if changed_ids.is_empty() {
        return Vec::new();
    }
    mailboxes
        .into_iter()
        .filter(|mailbox| changed_ids.contains(&mailbox.id))
        .collect()
}

pub(in crate::mapi) fn changed_sync_emails(
    emails: Vec<JmapEmail>,
    changed_ids: &[Uuid],
) -> Vec<JmapEmail> {
    if changed_ids.is_empty() {
        return Vec::new();
    }
    emails
        .into_iter()
        .filter(|email| changed_ids.contains(&email.id))
        .collect()
}

pub(in crate::mapi) fn special_sync_objects_for(
    folder_id: u64,
    sync_type: u8,
    snapshot: &MapiMailStoreSnapshot,
    principal: &AccountPrincipal,
) -> Vec<mapi_mailstore::SpecialMessageSyncFact> {
    if sync_type == 0x02 {
        return Vec::new();
    }
    let mut objects = Vec::new();
    if folder_id == TRASH_FOLDER_ID {
        objects.extend(
            snapshot
                .events_for_folder(folder_id)
                .into_iter()
                .map(|event| calendar_sync_object(event, None)),
        );
    } else if folder_id == CALENDAR_FOLDER_ID
        || snapshot
            .collaboration_folder_for_id(folder_id)
            .is_some_and(|folder| {
                folder.kind == crate::mapi_store::MapiCollaborationFolderKind::Calendar
            })
    {
        objects.extend(
            snapshot
                .events_for_folder(folder_id)
                .into_iter()
                .map(|event| {
                    calendar_sync_object(
                        event,
                        snapshot.reminder_for_source("calendar", event.canonical_id),
                    )
                }),
        );
    } else if snapshot
        .collaboration_folder_for_id(folder_id)
        .is_some_and(|folder| {
            folder.kind == crate::mapi_store::MapiCollaborationFolderKind::Contacts
        })
    {
        objects.extend(
            snapshot
                .contacts_for_folder(folder_id)
                .into_iter()
                .map(|contact| contact_sync_object(contact, principal.account_id)),
        );
    } else if snapshot
        .collaboration_folder_for_id(folder_id)
        .is_some_and(|folder| folder.kind == crate::mapi_store::MapiCollaborationFolderKind::Task)
    {
        objects.extend(
            snapshot
                .tasks_for_folder(folder_id)
                .into_iter()
                .map(|task| {
                    task_sync_object(
                        task,
                        snapshot.reminder_for_source("task", task.canonical_id),
                    )
                }),
        );
    } else if snapshot.public_folder_for_id(folder_id).is_some() {
        objects.extend(
            snapshot
                .public_folder_items_for_folder(folder_id)
                .into_iter()
                .map(public_folder_item_sync_object),
        );
    } else {
        objects.extend(match folder_id {
            CONTACTS_SEARCH_FOLDER_ID => snapshot
                .contacts_search_results()
                .into_iter()
                .map(|contact| {
                    sync_object_projected_to_folder(
                        contact_sync_object(contact, principal.account_id),
                        CONTACTS_SEARCH_FOLDER_ID,
                    )
                })
                .collect(),
            TODO_SEARCH_FOLDER_ID => snapshot
                .todo_search_results()
                .into_iter()
                .map(|task| {
                    sync_object_projected_to_folder(
                        task_sync_object(
                            task,
                            snapshot.reminder_for_source("task", task.canonical_id),
                        ),
                        TODO_SEARCH_FOLDER_ID,
                    )
                })
                .collect(),
            REMINDERS_FOLDER_ID => snapshot
                .reminder_tasks()
                .into_iter()
                .map(|task| {
                    sync_object_projected_to_folder(
                        task_sync_object(
                            task,
                            snapshot.reminder_for_source("task", task.canonical_id),
                        ),
                        REMINDERS_FOLDER_ID,
                    )
                })
                .collect(),
            NOTES_FOLDER_ID => snapshot
                .notes_for_folder(folder_id)
                .into_iter()
                .map(|note| mapi_mailstore::SpecialMessageSyncFact {
                    folder_id: note.folder_id,
                    item_id: note.id,
                    canonical_id: note.canonical_id,
                    associated: false,
                    subject: note.note.title.clone(),
                    body_text: note.note.body_text.clone(),
                    message_class: "IPM.StickyNote".to_string(),
                    last_modified_filetime: mapi_mailstore::filetime_from_rfc3339_utc(
                        &note.note.updated_at,
                    ),
                    message_size: note_size(&note.note),
                    read_state: None,
                    named_properties: vec![
                        (
                            PID_LID_NOTE_COLOR_TAG,
                            mapi_mailstore::SpecialMessagePropertyValue::I32(
                                note_property_value(
                                    &note.note,
                                    note.id,
                                    note.folder_id,
                                    PID_LID_NOTE_COLOR_TAG,
                                )
                                .and_then(|value| value.as_i64())
                                .unwrap_or(3) as i32,
                            ),
                        ),
                        (
                            PID_LID_NOTE_HEIGHT_TAG,
                            mapi_mailstore::SpecialMessagePropertyValue::I32(200),
                        ),
                        (
                            PID_LID_NOTE_WIDTH_TAG,
                            mapi_mailstore::SpecialMessagePropertyValue::I32(166),
                        ),
                        (
                            PID_LID_NOTE_X_TAG,
                            mapi_mailstore::SpecialMessagePropertyValue::I32(80),
                        ),
                        (
                            PID_LID_NOTE_Y_TAG,
                            mapi_mailstore::SpecialMessagePropertyValue::I32(80),
                        ),
                    ],
                    named_property_definitions: HashMap::new(),
                })
                .collect(),
            JOURNAL_FOLDER_ID => snapshot
                .journal_entries_for_folder(folder_id)
                .into_iter()
                .map(|entry| journal_sync_object(entry))
                .collect(),
            COMMON_VIEWS_FOLDER_ID => common_views_sync_messages(snapshot)
                .into_iter()
                .map(|message| common_views_sync_object(message, principal))
                .collect(),
            CONVERSATION_ACTION_SETTINGS_FOLDER_ID => snapshot
                .conversation_action_table_messages()
                .iter()
                .map(conversation_action_sync_object)
                .collect(),
            FREEBUSY_DATA_FOLDER_ID => snapshot
                .delegate_freebusy_messages()
                .iter()
                .map(delegate_freebusy_sync_object)
                .collect(),
            _ => Vec::new(),
        });
    }
    if folder_id != COMMON_VIEWS_FOLDER_ID {
        objects.extend(
            snapshot
                .associated_config_sync_messages_for_folder(folder_id)
                .iter()
                .map(associated_config_sync_object),
        );
    }
    for object in &mut objects {
        populate_special_message_named_property_definitions(object, snapshot);
    }
    objects
}

fn populate_special_message_named_property_definitions(
    object: &mut mapi_mailstore::SpecialMessageSyncFact,
    snapshot: &MapiMailStoreSnapshot,
) {
    object.named_property_definitions = object
        .named_properties
        .iter()
        .filter_map(|(property_tag, _value)| {
            let property_id = MapiPropertyTag::new(*property_tag).property_id();
            if property_id < 0x8000 {
                return None;
            }
            snapshot
                .named_property_for_id(property_id)
                .cloned()
                .or_else(|| {
                    fast_transfer_named_property_for_message_tag(
                        &object.message_class,
                        *property_tag,
                    )
                })
                .map(|property| (property_id, property))
        })
        .collect();
}

fn special_message_with_named_property_definitions(
    mut object: mapi_mailstore::SpecialMessageSyncFact,
    snapshot: &MapiMailStoreSnapshot,
) -> mapi_mailstore::SpecialMessageSyncFact {
    populate_special_message_named_property_definitions(&mut object, snapshot);
    object
}

fn common_views_sync_messages(
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<crate::mapi_store::MapiCommonViewsMessage> {
    snapshot.common_views_messages().collect()
}

fn sync_object_projected_to_folder(
    mut object: mapi_mailstore::SpecialMessageSyncFact,
    folder_id: u64,
) -> mapi_mailstore::SpecialMessageSyncFact {
    object.folder_id = folder_id;
    object
}

fn public_folder_item_sync_object(
    item: &crate::mapi_store::MapiPublicFolderItem,
) -> mapi_mailstore::SpecialMessageSyncFact {
    let message_class = if item.item.message_class.trim().is_empty() {
        "IPM.Post".to_string()
    } else {
        item.item.message_class.clone()
    };
    let message_size = item
        .item
        .subject
        .len()
        .saturating_add(item.item.body_text.len())
        .min(i64::MAX as usize) as i64;

    mapi_mailstore::SpecialMessageSyncFact {
        folder_id: item.folder_id,
        item_id: item.id,
        canonical_id: item.item.id,
        associated: false,
        subject: item.item.subject.clone(),
        body_text: item.item.body_text.clone(),
        message_class,
        last_modified_filetime: mapi_mailstore::filetime_from_rfc3339_utc(&item.item.updated_at),
        message_size,
        read_state: Some(item.item.is_read),
        named_properties: vec![
            (
                PID_TAG_ACCESS,
                mapi_mailstore::SpecialMessagePropertyValue::U32(MAPI_MESSAGE_ACCESS),
            ),
            (
                PID_TAG_HAS_ATTACHMENTS,
                mapi_mailstore::SpecialMessagePropertyValue::Bool(false),
            ),
            (
                PID_TAG_READ,
                mapi_mailstore::SpecialMessagePropertyValue::Bool(item.item.is_read),
            ),
        ],
        named_property_definitions: HashMap::new(),
    }
}

fn contact_sync_object(
    contact: &crate::mapi_store::MapiContact,
    mailbox_guid: Uuid,
) -> mapi_mailstore::SpecialMessageSyncFact {
    let mut properties = Vec::new();
    for property_tag in [
        PID_TAG_DISPLAY_NAME_W,
        PID_TAG_EMAIL_ADDRESS_W,
        PID_TAG_SMTP_ADDRESS_W,
        PID_TAG_MOBILE_TELEPHONE_NUMBER_W,
        PID_TAG_BUSINESS_TELEPHONE_NUMBER_W,
        PID_TAG_HOME_TELEPHONE_NUMBER_W,
        PID_TAG_COMPANY_NAME_W,
        PID_TAG_TITLE_W,
        PID_TAG_ACCESS,
        PID_TAG_HAS_ATTACHMENTS,
        PID_TAG_SOURCE_KEY,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_CHANGE_NUMBER,
        PID_TAG_LAST_MODIFICATION_TIME,
    ] {
        if let Some(value) = contact_property_value_with_identity(
            &contact.contact,
            contact.id,
            contact.folder_id,
            mailbox_guid,
            contact.durable_identity.as_ref(),
            property_tag,
        )
        .and_then(special_message_property_value)
        {
            properties.push((property_tag, value));
        }
    }
    let change_number = contact
        .durable_identity
        .as_ref()
        .map(|identity| identity.change_number)
        .unwrap_or_else(|| mapi_mailstore::change_number_for_store_id(contact.id));

    mapi_mailstore::SpecialMessageSyncFact {
        folder_id: contact.folder_id,
        item_id: contact.id,
        canonical_id: contact.canonical_id,
        associated: false,
        subject: contact.contact.name.clone(),
        body_text: contact.contact.notes.clone(),
        message_class: "IPM.Contact".to_string(),
        last_modified_filetime: contact
            .durable_identity
            .as_ref()
            .map(|identity| identity.last_modification_time)
            .unwrap_or_else(|| mapi_mailstore::filetime_from_change_number(change_number)),
        message_size: contact_size(&contact.contact),
        read_state: None,
        named_properties: properties,
        named_property_definitions: HashMap::new(),
    }
}

fn task_sync_object(
    task: &crate::mapi_store::MapiTask,
    reminder: Option<&lpe_storage::ClientReminder>,
) -> mapi_mailstore::SpecialMessageSyncFact {
    let mut properties = Vec::new();
    for property_tag in [
        PID_TAG_FLAG_STATUS,
        PID_TAG_ACCESS,
        PID_TAG_HAS_ATTACHMENTS,
        PID_TAG_LAST_MODIFICATION_TIME,
        PID_TAG_LOCAL_COMMIT_TIME,
        PID_LID_REMINDER_SET_TAG,
        PID_LID_REMINDER_DELTA_TAG,
        PID_LID_REMINDER_TIME_TAG,
        PID_LID_REMINDER_SIGNAL_TIME_TAG,
        PID_LID_REMINDER_OVERRIDE_TAG,
        PID_LID_REMINDER_PLAY_SOUND_TAG,
        PID_LID_REMINDER_FILE_PARAMETER_W_TAG,
    ] {
        if let Some(value) = task_property_value_with_reminder(
            &task.task,
            task.id,
            task.folder_id,
            property_tag,
            reminder,
        )
        .and_then(special_message_property_value)
        {
            properties.push((property_tag, value));
        }
    }

    mapi_mailstore::SpecialMessageSyncFact {
        folder_id: task.folder_id,
        item_id: task.id,
        canonical_id: task.canonical_id,
        associated: false,
        subject: task.task.title.clone(),
        body_text: task.task.description.clone(),
        message_class: "IPM.Task".to_string(),
        last_modified_filetime: mapi_mailstore::filetime_from_rfc3339_utc(&task.task.updated_at),
        message_size: task_size(&task.task),
        read_state: None,
        named_properties: properties,
        named_property_definitions: HashMap::new(),
    }
}

pub(in crate::mapi) fn changed_special_sync_objects(
    objects: Vec<mapi_mailstore::SpecialMessageSyncFact>,
    changed_ids: &[Uuid],
) -> Vec<mapi_mailstore::SpecialMessageSyncFact> {
    if changed_ids.is_empty() {
        return Vec::new();
    }
    objects
        .into_iter()
        .filter(|object| changed_ids.contains(&object.canonical_id))
        .collect()
}

fn journal_sync_object(
    entry: &crate::mapi_store::MapiJournalEntry,
) -> mapi_mailstore::SpecialMessageSyncFact {
    let companies = journal_entry_property_value(
        &entry.entry,
        entry.id,
        entry.folder_id,
        PID_LID_COMPANIES_TAG,
    )
    .and_then(|value| match value {
        MapiValue::MultiString(values) => Some(values),
        _ => None,
    })
    .unwrap_or_default();
    let contacts = journal_entry_property_value(
        &entry.entry,
        entry.id,
        entry.folder_id,
        PID_LID_CONTACTS_TAG,
    )
    .and_then(|value| match value {
        MapiValue::MultiString(values) => Some(values),
        _ => None,
    })
    .unwrap_or_default();
    let mut named_properties = vec![
        (
            PID_LID_LOG_TYPE_W_TAG,
            mapi_mailstore::SpecialMessagePropertyValue::String(entry.entry.entry_type.clone()),
        ),
        (
            PID_LID_LOG_TYPE_DESC_W_TAG,
            mapi_mailstore::SpecialMessagePropertyValue::String(entry.entry.entry_type.clone()),
        ),
        (
            PID_LID_COMPANIES_TAG,
            mapi_mailstore::SpecialMessagePropertyValue::MultiString(companies),
        ),
        (
            PID_LID_CONTACTS_TAG,
            mapi_mailstore::SpecialMessagePropertyValue::MultiString(contacts),
        ),
        (
            PID_LID_LOG_DURATION_TAG,
            mapi_mailstore::SpecialMessagePropertyValue::I32(0),
        ),
        (
            PID_LID_LOG_FLAGS_TAG,
            mapi_mailstore::SpecialMessagePropertyValue::I32(0),
        ),
    ];
    if let Some(starts_at) = entry
        .entry
        .starts_at
        .as_deref()
        .or(entry.entry.occurred_at.as_deref())
    {
        named_properties.push((
            PID_LID_COMMON_START_TAG,
            mapi_mailstore::SpecialMessagePropertyValue::Time(starts_at.to_string()),
        ));
        named_properties.push((
            PID_LID_LOG_START_TAG,
            mapi_mailstore::SpecialMessagePropertyValue::Time(starts_at.to_string()),
        ));
    }
    if let Some(ends_at) = entry.entry.ends_at.as_deref() {
        named_properties.push((
            PID_LID_COMMON_END_TAG,
            mapi_mailstore::SpecialMessagePropertyValue::Time(ends_at.to_string()),
        ));
        named_properties.push((
            PID_LID_LOG_END_TAG,
            mapi_mailstore::SpecialMessagePropertyValue::Time(ends_at.to_string()),
        ));
    }

    mapi_mailstore::SpecialMessageSyncFact {
        folder_id: entry.folder_id,
        item_id: entry.id,
        canonical_id: entry.canonical_id,
        associated: false,
        subject: entry.entry.subject.clone(),
        body_text: entry.entry.body_text.clone(),
        message_class: entry.entry.message_class.clone(),
        last_modified_filetime: mapi_mailstore::filetime_from_rfc3339_utc(&entry.entry.updated_at),
        message_size: journal_entry_size(&entry.entry),
        read_state: None,
        named_properties,
        named_property_definitions: HashMap::new(),
    }
}

fn navigation_shortcut_sync_object(
    message: &crate::mapi_store::MapiNavigationShortcutMessage,
    principal: &AccountPrincipal,
) -> mapi_mailstore::SpecialMessageSyncFact {
    let mut named_properties = Vec::new();
    for property_tag in [
        PID_TAG_WLINK_TYPE,
        PID_TAG_WLINK_FLAGS,
        PID_TAG_WLINK_SAVE_STAMP,
        PID_TAG_WLINK_ORDINAL,
        PID_TAG_WLINK_ENTRY_ID,
        PID_TAG_WLINK_RECORD_KEY,
        PID_TAG_WLINK_STORE_ENTRY_ID,
        PID_TAG_WLINK_FOLDER_TYPE,
        PID_TAG_WLINK_GROUP_HEADER_ID,
        PID_TAG_WLINK_GROUP_CLSID,
        PID_TAG_WLINK_GROUP_NAME_W,
        PID_TAG_WLINK_SECTION,
        // [MS-OXOCFG] sections 2.2.9.15 through 2.2.9.19: these
        // client-written optional values are part of the WLink FAI message
        // and must use the same canonical projection in ICS and tables.
        PID_TAG_WLINK_CALENDAR_COLOR,
        PID_TAG_WLINK_ADDRESS_BOOK_EID,
        PID_TAG_WLINK_ADDRESS_BOOK_STORE_EID,
        PID_TAG_WLINK_CLIENT_ID,
        PID_TAG_WLINK_RO_GROUP_TYPE,
        PID_TAG_SOURCE_KEY,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_CHANGE_NUMBER,
    ] {
        if let Some(value) =
            navigation_shortcut_property_value_for_principal(message, principal, property_tag)
                .and_then(special_message_property_value)
        {
            named_properties.push((property_tag, value));
        }
    }
    let change_number = message
        .durable_identity
        .as_ref()
        .map(|identity| identity.change_number)
        .unwrap_or_else(|| mapi_mailstore::change_number_for_store_id(message.id));

    mapi_mailstore::SpecialMessageSyncFact {
        folder_id: message.folder_id,
        item_id: message.id,
        canonical_id: message.canonical_id,
        associated: true,
        subject: message.subject.clone(),
        body_text: String::new(),
        message_class: "IPM.Microsoft.WunderBar.Link".to_string(),
        last_modified_filetime: message
            .durable_identity
            .as_ref()
            .map(|identity| identity.last_modification_time)
            .unwrap_or_else(|| mapi_mailstore::filetime_from_change_number(change_number)),
        message_size: 128,
        read_state: None,
        named_properties,
        named_property_definitions: HashMap::new(),
    }
}

fn common_views_sync_object(
    message: crate::mapi_store::MapiCommonViewsMessage,
    principal: &AccountPrincipal,
) -> mapi_mailstore::SpecialMessageSyncFact {
    match message {
        crate::mapi_store::MapiCommonViewsMessage::NavigationShortcut(message) => {
            navigation_shortcut_sync_object(&message, principal)
        }
        crate::mapi_store::MapiCommonViewsMessage::NamedView(message) => {
            common_view_named_view_sync_object(&message, principal.account_id)
        }
        crate::mapi_store::MapiCommonViewsMessage::SearchFolderDefinition(message) => {
            search_folder_definition_sync_object(&message, principal.account_id)
        }
        crate::mapi_store::MapiCommonViewsMessage::AssociatedConfig(message) => {
            associated_config_sync_object(&message)
        }
    }
}

fn search_folder_definition_sync_object(
    message: &SearchFolderDefinition,
    account_id: Uuid,
) -> mapi_mailstore::SpecialMessageSyncFact {
    let item_id = crate::mapi::identity::mapped_mapi_object_id(&message.id)
        .expect("projected search-folder FAI identity");
    let mut named_properties = Vec::new();
    for property_tag in [
        PID_TAG_SEARCH_FOLDER_ID,
        PID_TAG_SEARCH_FOLDER_TEMPLATE_ID,
        PID_TAG_SEARCH_FOLDER_TAG,
        PID_TAG_SEARCH_FOLDER_LAST_USED,
        PID_TAG_SEARCH_FOLDER_EXPIRATION,
        PID_TAG_SEARCH_FOLDER_STORAGE_TYPE,
        PID_TAG_SEARCH_FOLDER_EFP_FLAGS,
        PID_TAG_SEARCH_FOLDER_DEFINITION,
        PID_TAG_SOURCE_KEY,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_CHANGE_NUMBER,
    ] {
        if let Some(value) =
            search_folder_definition_message_property_value(message, account_id, property_tag)
                .and_then(special_message_property_value)
        {
            named_properties.push((property_tag, value));
        }
    }
    let change_number =
        search_folder_definition_message_property_value(message, account_id, PID_TAG_CHANGE_NUMBER)
            .and_then(|value| value.as_i64())
            .and_then(|value| u64::try_from(value).ok())
            .unwrap_or_else(|| mapi_mailstore::change_number_for_store_id(item_id));
    let last_modified_filetime = search_folder_definition_message_property_value(
        message,
        account_id,
        PID_TAG_LAST_MODIFICATION_TIME,
    )
    .and_then(|value| value.as_i64())
    .and_then(|value| u64::try_from(value).ok())
    .unwrap_or_else(|| mapi_mailstore::filetime_from_change_number(change_number));

    mapi_mailstore::SpecialMessageSyncFact {
        folder_id: COMMON_VIEWS_FOLDER_ID,
        item_id,
        canonical_id: message.id,
        associated: true,
        subject: message.display_name.clone(),
        body_text: String::new(),
        message_class: "IPM.Microsoft.WunderBar.SFInfo".to_string(),
        last_modified_filetime,
        message_size: 128,
        read_state: None,
        named_properties,
        named_property_definitions: HashMap::new(),
    }
}

fn common_view_named_view_sync_object(
    message: &crate::mapi_store::MapiCommonViewNamedViewMessage,
    account_id: Uuid,
) -> mapi_mailstore::SpecialMessageSyncFact {
    let mut named_properties = Vec::new();
    for property_tag in [
        PID_TAG_VIEW_DESCRIPTOR_CLSID,
        PID_TAG_VIEW_DESCRIPTOR_FLAGS,
        OUTLOOK_COMMON_VIEW_DESCRIPTOR_BINARY_6835,
        OUTLOOK_COMMON_VIEW_DESCRIPTOR_STRINGS_683C,
        PID_TAG_VIEW_DESCRIPTOR_VERSION,
        PID_TAG_VIEW_DESCRIPTOR_VERSION_CANONICAL,
        PID_TAG_VIEW_DESCRIPTOR_NAME_W,
        PID_TAG_VIEW_DESCRIPTOR_STRINGS_W,
        PID_TAG_VIEW_DESCRIPTOR_FOLDER_TYPE,
        PID_TAG_VIEW_DESCRIPTOR_VIEW_MODE,
        PID_TAG_VIEW_DESCRIPTOR_BINARY,
        PID_TAG_WLINK_GROUP_HEADER_ID,
    ] {
        if let Some(value) =
            common_view_named_view_property_value(message, account_id, property_tag)
                .and_then(special_message_property_value)
        {
            named_properties.push((property_tag, value));
        }
    }
    let change_number = mapi_mailstore::change_number_for_store_id(message.id);

    mapi_mailstore::SpecialMessageSyncFact {
        folder_id: message.folder_id,
        item_id: message.id,
        canonical_id: message.canonical_id,
        associated: true,
        subject: message.name.clone(),
        body_text: String::new(),
        message_class: "IPM.Microsoft.FolderDesign.NamedView".to_string(),
        last_modified_filetime: mapi_mailstore::filetime_from_change_number(change_number),
        message_size: 128,
        read_state: None,
        named_properties,
        named_property_definitions: HashMap::new(),
    }
}

fn conversation_action_sync_object(
    message: &crate::mapi_store::MapiConversationActionMessage,
) -> mapi_mailstore::SpecialMessageSyncFact {
    let mut named_properties = Vec::new();
    for property_tag in [
        PID_TAG_CONVERSATION_INDEX,
        PID_LID_CONVERSATION_ACTION_MOVE_FOLDER_EID_TAG,
        PID_LID_CONVERSATION_ACTION_MOVE_STORE_EID_TAG,
        PID_LID_CONVERSATION_ACTION_MAX_DELIVERY_TIME_TAG,
        PID_LID_CONVERSATION_PROCESSED_TAG,
        PID_LID_CONVERSATION_ACTION_LAST_APPLIED_TIME_TAG,
        PID_LID_CONVERSATION_ACTION_VERSION_TAG,
        PID_NAME_KEYWORDS_TAG,
    ] {
        if let Some(value) = conversation_action_property_value(message, property_tag)
            .and_then(special_message_property_value)
        {
            named_properties.push((property_tag, value));
        }
    }
    let change_number = mapi_mailstore::change_number_for_store_id(message.id);
    let message_size = conversation_action_property_value(message, PID_TAG_MESSAGE_SIZE)
        .and_then(|value| match value {
            MapiValue::I32(value) => Some(value),
            _ => None,
        })
        .unwrap_or(0) as i64;

    mapi_mailstore::SpecialMessageSyncFact {
        folder_id: message.folder_id,
        item_id: message.id,
        canonical_id: message.canonical_id,
        associated: true,
        subject: conversation_action_subject(&message.action),
        body_text: String::new(),
        message_class: "IPM.ConversationAction".to_string(),
        last_modified_filetime: mapi_mailstore::filetime_from_change_number(change_number),
        message_size,
        read_state: None,
        named_properties,
        named_property_definitions: HashMap::new(),
    }
}

fn delegate_freebusy_sync_object(
    message: &crate::mapi_store::MapiDelegateFreeBusyMessage,
) -> mapi_mailstore::SpecialMessageSyncFact {
    let change_number = mapi_mailstore::change_number_for_store_id(message.id);
    let message_size = message
        .message
        .subject
        .len()
        .saturating_add(message.message.body_text.len())
        .saturating_add(message.message.payload_json.len())
        .min(i64::MAX as usize) as i64;

    mapi_mailstore::SpecialMessageSyncFact {
        folder_id: message.folder_id,
        item_id: message.id,
        canonical_id: message.canonical_id,
        associated: true,
        subject: message.message.subject.clone(),
        body_text: message.message.body_text.clone(),
        message_class: if message.message.message_kind == "delegate" {
            "IPM.Microsoft.Delegate".to_string()
        } else {
            "IPM.Microsoft.ScheduleData.FreeBusy".to_string()
        },
        last_modified_filetime: mapi_mailstore::filetime_from_change_number(change_number),
        message_size,
        read_state: None,
        named_properties: Vec::new(),
        named_property_definitions: HashMap::new(),
    }
}

fn associated_config_sync_object(
    message: &crate::mapi_store::MapiAssociatedConfigMessage,
) -> mapi_mailstore::SpecialMessageSyncFact {
    let mut named_properties = Vec::new();
    let stored_properties = mapi_properties_from_json(&message.properties_json);
    for (tag, value) in stored_properties.clone() {
        if associated_config_standard_sync_tag(tag) {
            continue;
        }
        if let Some(value) = special_message_property_value(value) {
            named_properties.push((tag, value));
        }
    }
    for &tag in associated_config_default_sync_tags(message, &stored_properties) {
        let canonical_tag = canonical_property_storage_tag(tag);
        if associated_config_standard_sync_tag(canonical_tag)
            || stored_properties.contains_key(&canonical_tag)
        {
            continue;
        }
        if let Some(value) =
            associated_config_property_value(message, tag).and_then(special_message_property_value)
        {
            named_properties.push((tag, value));
        }
    }
    let change_number = stored_properties
        .get(&PID_TAG_CHANGE_NUMBER)
        .and_then(MapiValue::as_i64)
        .and_then(|value| u64::try_from(value).ok())
        .unwrap_or_else(|| mapi_mailstore::change_number_for_store_id(message.id));
    let last_modified_filetime = stored_properties
        .get(&PID_TAG_LAST_MODIFICATION_TIME)
        .and_then(MapiValue::as_i64)
        .and_then(|value| u64::try_from(value).ok())
        .unwrap_or_else(|| mapi_mailstore::filetime_from_change_number(change_number));
    let message_size = message
        .subject
        .len()
        .saturating_add(message.message_class.len())
        .saturating_add(message.properties_json.to_string().len())
        .min(i64::MAX as usize) as i64;

    mapi_mailstore::SpecialMessageSyncFact {
        folder_id: message.folder_id,
        item_id: message.id,
        canonical_id: message.canonical_id,
        associated: true,
        subject: message.subject.clone(),
        body_text: associated_config_text_property(message, PID_TAG_BODY_W),
        message_class: message.message_class.clone(),
        last_modified_filetime,
        message_size,
        read_state: None,
        named_properties,
        named_property_definitions: HashMap::new(),
    }
}

fn associated_config_default_sync_tags(
    message: &crate::mapi_store::MapiAssociatedConfigMessage,
    stored_properties: &HashMap<u32, MapiValue>,
) -> &'static [u32] {
    if crate::mapi_store::is_outlook_configuration_message_class(&message.message_class) {
        // [MS-OXOCFG] sections 2.2.2.1 and 2.2.5.1: a persisted
        // PidTagRoamingDatatypes value is the client's complete declaration of
        // the streams that exist. LPE therefore preserves the client-owned bag
        // in CopyTo/ICS instead of adding absent compatibility properties.
        if stored_properties.contains_key(&PID_TAG_ROAMING_DATATYPES) {
            return &[];
        }
        &[
            PID_TAG_ROAMING_DATATYPES,
            PID_TAG_ROAMING_DICTIONARY,
            OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B,
            PID_NAME_CONTENT_CLASS_W_TAG,
            PID_NAME_CONTENT_TYPE_W_TAG,
        ]
    } else if message
        .message_class
        .eq_ignore_ascii_case(crate::mapi_store::OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_CLASS)
    {
        &[
            PID_TAG_VIEW_DESCRIPTOR_CLSID,
            PID_TAG_VIEW_DESCRIPTOR_FLAGS,
            PID_TAG_VIEW_DESCRIPTOR_VERSION,
            PID_TAG_VIEW_DESCRIPTOR_VERSION_CANONICAL,
            PID_TAG_VIEW_DESCRIPTOR_NAME_W,
            PID_TAG_VIEW_DESCRIPTOR_STRINGS_W,
            PID_TAG_VIEW_DESCRIPTOR_FOLDER_TYPE,
            PID_TAG_VIEW_DESCRIPTOR_VIEW_MODE,
            PID_TAG_VIEW_DESCRIPTOR_BINARY,
            OUTLOOK_COMMON_VIEW_DESCRIPTOR_BINARY_6835,
            OUTLOOK_COMMON_VIEW_DESCRIPTOR_STRINGS_683C,
            OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B,
        ]
    } else {
        &[]
    }
}

fn associated_config_standard_sync_tag(tag: u32) -> bool {
    matches!(
        canonical_property_storage_tag(tag),
        PID_TAG_FOLDER_ID
            | PID_TAG_MID
            | PID_TAG_INST_ID
            | PID_TAG_INSTANCE_NUM
            | PID_TAG_ENTRY_ID
            | PID_TAG_INSTANCE_KEY
            | PID_TAG_ASSOCIATED
            | PID_TAG_MESSAGE_SIZE
            | PID_TAG_MESSAGE_FLAGS
            | PID_TAG_SUBJECT_W
            | PID_TAG_NORMALIZED_SUBJECT_W
            | PID_TAG_MESSAGE_CLASS_W
            | PID_TAG_BODY_W
            | PID_TAG_LAST_MODIFICATION_TIME
            | PID_TAG_LOCAL_COMMIT_TIME
            | PID_TAG_MESSAGE_DELIVERY_TIME
            | PID_TAG_ACCESS
    )
}

fn associated_config_text_property(
    message: &crate::mapi_store::MapiAssociatedConfigMessage,
    tag: u32,
) -> String {
    mapi_properties_from_json(&message.properties_json)
        .remove(&tag)
        .and_then(MapiValue::into_text)
        .unwrap_or_default()
}

fn special_message_property_value(
    value: MapiValue,
) -> Option<mapi_mailstore::SpecialMessagePropertyValue> {
    match value {
        MapiValue::Binary(value) => {
            Some(mapi_mailstore::SpecialMessagePropertyValue::Binary(value))
        }
        MapiValue::Bool(value) => Some(mapi_mailstore::SpecialMessagePropertyValue::Bool(value)),
        MapiValue::Guid(value) => Some(mapi_mailstore::SpecialMessagePropertyValue::Guid(value)),
        MapiValue::I32(value) => Some(mapi_mailstore::SpecialMessagePropertyValue::I32(value)),
        MapiValue::I64(value) => Some(mapi_mailstore::SpecialMessagePropertyValue::I64(value)),
        MapiValue::U32(value) => Some(mapi_mailstore::SpecialMessagePropertyValue::U32(value)),
        MapiValue::U64(value) => Some(mapi_mailstore::SpecialMessagePropertyValue::U64(value)),
        MapiValue::String(value) => {
            Some(mapi_mailstore::SpecialMessagePropertyValue::String(value))
        }
        MapiValue::MultiString(values) => Some(
            mapi_mailstore::SpecialMessagePropertyValue::MultiString(values),
        ),
        _ => None,
    }
}

fn calendar_sync_object(
    event: &crate::mapi_store::MapiEvent,
    reminder: Option<&lpe_storage::ClientReminder>,
) -> mapi_mailstore::SpecialMessageSyncFact {
    let mut properties = Vec::new();
    for property_tag in [
        PID_TAG_START_DATE,
        PID_TAG_END_DATE,
        PID_TAG_MESSAGE_DELIVERY_TIME,
        PID_LID_COMMON_START_TAG,
        PID_LID_COMMON_END_TAG,
        PID_LID_BUSY_STATUS_TAG,
        PID_LID_LOCATION_W_TAG,
        PID_LID_APPOINTMENT_START_WHOLE_TAG,
        PID_LID_APPOINTMENT_END_WHOLE_TAG,
        PID_LID_APPOINTMENT_DURATION_TAG,
        PID_LID_APPOINTMENT_SUB_TYPE_TAG,
        PID_LID_APPOINTMENT_RECUR_TAG,
        PID_LID_APPOINTMENT_STATE_FLAGS_TAG,
        PID_LID_TIME_ZONE_STRUCT_TAG,
        PID_LID_TIME_ZONE_DESCRIPTION_W_TAG,
        PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_START_DISPLAY_TAG,
        PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_END_DISPLAY_TAG,
        PID_LID_GLOBAL_OBJECT_ID_TAG,
        PID_LID_CLEAN_GLOBAL_OBJECT_ID_TAG,
        PID_TAG_BODY_HTML_W,
        PID_TAG_SENDER_NAME_W,
        PID_TAG_SENDER_EMAIL_ADDRESS_W,
        PID_TAG_DISPLAY_TO_W,
        PID_TAG_DISPLAY_CC_W,
        PID_LID_ALL_ATTENDEES_STRING_W_TAG,
        PID_LID_TO_ATTENDEES_STRING_W_TAG,
        PID_LID_CC_ATTENDEES_STRING_W_TAG,
        PID_TAG_ACCESS,
        PID_TAG_HAS_ATTACHMENTS,
        PID_TAG_SOURCE_KEY,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_CHANGE_NUMBER,
        PID_TAG_LOCAL_COMMIT_TIME,
        PID_LID_REMINDER_SET_TAG,
        PID_LID_REMINDER_DELTA_TAG,
        PID_LID_REMINDER_TIME_TAG,
        PID_LID_REMINDER_SIGNAL_TIME_TAG,
        PID_LID_REMINDER_OVERRIDE_TAG,
        PID_LID_REMINDER_PLAY_SOUND_TAG,
        PID_LID_REMINDER_FILE_PARAMETER_W_TAG,
    ] {
        let value = if property_tag == PID_TAG_HAS_ATTACHMENTS {
            Some(mapi_mailstore::SpecialMessagePropertyValue::Bool(
                !event.attachments.is_empty(),
            ))
        } else {
            versioned_event_property_value_with_reminder(event, property_tag, reminder)
                .and_then(special_message_property_value)
        };
        if let Some(value) = value {
            properties.push((property_tag, value));
        }
    }

    mapi_mailstore::SpecialMessageSyncFact {
        folder_id: event.folder_id,
        item_id: event.id,
        canonical_id: event.canonical_id,
        associated: false,
        subject: event.event.title.clone(),
        body_text: event.event.notes.clone(),
        message_class: "IPM.Appointment".to_string(),
        last_modified_filetime: mapi_mailstore::filetime_from_rfc3339_utc(
            &event.version.updated_at,
        ),
        message_size: event_size(&event.event),
        read_state: None,
        named_properties: properties,
        named_property_definitions: HashMap::new(),
    }
}

pub(in crate::mapi) fn sync_attachment_facts_for(
    folder_id: u64,
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<mapi_mailstore::MessageAttachmentSyncFacts> {
    let mut facts = emails
        .iter()
        .filter_map(|email| {
            let message_id = mapi_message_id(email);
            let attachments = snapshot
                .attachments_for_message(folder_id, message_id)
                .or_else(|| {
                    let canonical_folder_id = mapi_folder_id_for_email(email);
                    (canonical_folder_id != folder_id)
                        .then(|| snapshot.attachments_for_message(canonical_folder_id, message_id))
                        .flatten()
                })
                .unwrap_or_default();
            if attachments.is_empty() {
                return None;
            }
            Some(mapi_mailstore::MessageAttachmentSyncFacts {
                message_id: email.id,
                attachments: attachments
                    .iter()
                    .map(|attachment| mapi_mailstore::AttachmentSyncFact {
                        id: attachment.canonical_id,
                        file_reference: attachment.file_reference.clone(),
                        file_name: attachment.file_name.clone(),
                        media_type: attachment.media_type.clone(),
                        size_octets: attachment.size_octets,
                        embedded_message_blob: None,
                    })
                    .collect(),
            })
        })
        .collect::<Vec<_>>();
    facts.extend(
        snapshot
            .events_for_folder(folder_id)
            .into_iter()
            .filter(|event| !event.attachments.is_empty())
            .map(|event| mapi_mailstore::MessageAttachmentSyncFacts {
                message_id: event.canonical_id,
                attachments: event
                    .attachments
                    .iter()
                    .map(|attachment| mapi_mailstore::AttachmentSyncFact {
                        id: attachment.canonical_id,
                        file_reference: attachment.file_reference.clone(),
                        file_name: attachment.file_name.clone(),
                        media_type: attachment.media_type.clone(),
                        size_octets: attachment.size_octets,
                        embedded_message_blob: None,
                    })
                    .collect(),
            }),
    );
    facts
}

fn fast_transfer_message_children(
    rop_id: u8,
    level: u8,
    property_tags: &[u32],
) -> mapi_mailstore::FastTransferMessageChildren {
    // [MS-OXCFXICS] sections 3.2.5.8.1.1, 3.2.5.8.1.2, and 3.2.5.10:
    // CopyTo lists exclusions, CopyProperties lists inclusions, and a nonzero
    // Level excludes all descendant subobjects.
    if level != 0 {
        return mapi_mailstore::FastTransferMessageChildren::new(false, false);
    }

    let includes = |property_tag| property_tags.contains(&property_tag);
    match RopId::from_u8(rop_id) {
        Some(RopId::FastTransferSourceCopyTo) => mapi_mailstore::FastTransferMessageChildren::new(
            !includes(PID_TAG_MESSAGE_RECIPIENTS),
            !includes(PID_TAG_MESSAGE_ATTACHMENTS),
        ),
        Some(RopId::FastTransferSourceCopyProperties) => {
            mapi_mailstore::FastTransferMessageChildren::new(
                includes(PID_TAG_MESSAGE_RECIPIENTS),
                includes(PID_TAG_MESSAGE_ATTACHMENTS),
            )
        }
        _ => mapi_mailstore::FastTransferMessageChildren::all(),
    }
}

pub(in crate::mapi) fn fast_transfer_manifest_for_object(
    rop_id: u8,
    send_options: u8,
    level: u8,
    property_tags: &[u32],
    object: &MapiObject,
    principal: &AccountPrincipal,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Option<(u64, Vec<u8>)> {
    let message_children = fast_transfer_message_children(rop_id, level, property_tags);
    match object {
        MapiObject::Folder { folder_id, .. } => {
            if RopId::from_u8(rop_id) == Some(RopId::FastTransferSourceCopyFolder) {
                let copy_mailboxes = sync_mailboxes_for_excluding_deleted(
                    *folder_id,
                    0x02,
                    mailboxes,
                    &HashSet::new(),
                );
                let mut attachment_facts = Vec::new();
                for mailbox in &copy_mailboxes {
                    let copied_folder_id = mapi_folder_id(mailbox);
                    let folder_messages =
                        emails_for_folder(copied_folder_id, &copy_mailboxes, emails)
                            .into_iter()
                            .cloned()
                            .collect::<Vec<_>>();
                    attachment_facts.extend(sync_attachment_facts_for(
                        copied_folder_id,
                        &folder_messages,
                        snapshot,
                    ));
                }
                return Some((
                    *folder_id,
                    mapi_mailstore::fast_transfer_top_folder_buffer_with_attachments(
                        *folder_id,
                        &copy_mailboxes,
                        emails,
                        &attachment_facts,
                    ),
                ));
            }
            let folder = folder_row_for_id(*folder_id, mailboxes)
                .cloned()
                .into_iter()
                .collect::<Vec<_>>();
            let messages = emails_for_folder(*folder_id, mailboxes, emails)
                .into_iter()
                .cloned()
                .collect::<Vec<_>>();
            let attachment_facts = sync_attachment_facts_for(*folder_id, &messages, snapshot);
            Some((
                *folder_id,
                mapi_mailstore::fast_transfer_manifest_buffer_with_attachments(
                    *folder_id,
                    &folder,
                    &messages,
                    &attachment_facts,
                ),
            ))
        }
        MapiObject::Message {
            folder_id,
            message_id,
            saved_email,
            ..
        } => {
            let message = message_for_id(*folder_id, *message_id, mailboxes, emails)
                .or(saved_email.as_ref().map(|saved| &saved.email))?
                .clone();
            let attachment_facts =
                sync_attachment_facts_for(*folder_id, std::slice::from_ref(&message), snapshot);
            Some((
                *folder_id,
                mapi_mailstore::fast_transfer_message_content_buffer_with_attachments(
                    &message,
                    &attachment_facts,
                    message_children,
                ),
            ))
        }
        MapiObject::AssociatedConfig {
            folder_id,
            config_id,
            saved_message,
        } => {
            let message = snapshot
                .associated_config_message_for_id(*config_id)
                .or_else(|| saved_message.clone())
                .filter(|message| message.folder_id == *folder_id)?;
            Some((
                *folder_id,
                mapi_mailstore::fast_transfer_message_content_buffer_with_special_object(
                    *folder_id,
                    &special_message_with_named_property_definitions(
                        associated_config_sync_object(&message),
                        snapshot,
                    ),
                    send_options,
                    message_children,
                ),
            ))
        }
        MapiObject::ConversationAction {
            folder_id,
            conversation_action_id,
        } => {
            let message =
                snapshot.conversation_action_table_message_for_id(*conversation_action_id)?;
            if message.folder_id != *folder_id {
                return None;
            }
            Some((
                *folder_id,
                mapi_mailstore::fast_transfer_message_content_buffer_with_special_object(
                    *folder_id,
                    &special_message_with_named_property_definitions(
                        conversation_action_sync_object(&message),
                        snapshot,
                    ),
                    send_options,
                    message_children,
                ),
            ))
        }
        MapiObject::NavigationShortcut {
            folder_id,
            shortcut_id,
            ..
        } => {
            let message = snapshot.navigation_shortcut_message_for_id(*shortcut_id)?;
            if message.folder_id != *folder_id {
                return None;
            }
            Some((
                *folder_id,
                mapi_mailstore::fast_transfer_message_content_buffer_with_special_object(
                    *folder_id,
                    &special_message_with_named_property_definitions(
                        navigation_shortcut_sync_object(&message, principal),
                        snapshot,
                    ),
                    send_options,
                    message_children,
                ),
            ))
        }
        MapiObject::CommonViewNamedView { folder_id, view_id } => {
            let message = snapshot.named_view_message_for_folder_and_id(*folder_id, *view_id)?;
            Some((
                *folder_id,
                mapi_mailstore::fast_transfer_message_content_buffer_with_special_object(
                    *folder_id,
                    &special_message_with_named_property_definitions(
                        common_view_named_view_sync_object(&message, principal.account_id),
                        snapshot,
                    ),
                    send_options,
                    message_children,
                ),
            ))
        }
        MapiObject::DelegateFreeBusyMessage {
            folder_id,
            message_id,
            ..
        } => {
            let message = snapshot.delegate_freebusy_message_for_id(*message_id)?;
            if message.folder_id != *folder_id {
                return None;
            }
            Some((
                *folder_id,
                mapi_mailstore::fast_transfer_message_content_buffer_with_special_object(
                    *folder_id,
                    &special_message_with_named_property_definitions(
                        delegate_freebusy_sync_object(&message),
                        snapshot,
                    ),
                    send_options,
                    message_children,
                ),
            ))
        }
        MapiObject::PublicFolderItem {
            folder_id, item_id, ..
        } => {
            let item = snapshot.public_folder_item_for_id(*folder_id, *item_id)?;
            Some((
                *folder_id,
                mapi_mailstore::fast_transfer_message_content_buffer_with_special_object(
                    *folder_id,
                    &special_message_with_named_property_definitions(
                        public_folder_item_sync_object(&item),
                        snapshot,
                    ),
                    send_options,
                    message_children,
                ),
            ))
        }
        _ => None,
    }
}

pub(in crate::mapi) fn message_for_id<'a>(
    folder_id: u64,
    message_id: u64,
    mailboxes: &[JmapMailbox],
    emails: &'a [JmapEmail],
) -> Option<&'a JmapEmail> {
    emails.iter().find(|email| {
        mapi_item_id_matches(&email.id, message_id)
            && email_matches_folder(email, folder_id, mailboxes)
    })
}

pub(in crate::mapi) fn mapi_item_id_matches(canonical_id: &Uuid, object_id: u64) -> bool {
    crate::mapi::identity::object_id_matches(canonical_id, object_id)
}

pub(in crate::mapi) fn next_pending_attachment_num(
    session: &MapiSession,
    folder_id: u64,
    message_id: u64,
    snapshot: &MapiMailStoreSnapshot,
) -> u32 {
    let snapshot_max = snapshot
        .attachments_for_message(folder_id, message_id)
        .unwrap_or_default()
        .iter()
        .map(|attachment| attachment.attach_num)
        .max();
    let session_max = session
        .handles
        .values()
        .filter_map(|object| match object {
            MapiObject::PendingAttachment {
                folder_id: pending_folder_id,
                message_id: pending_message_id,
                attach_num,
                ..
            }
            | MapiObject::SavedAttachment {
                folder_id: pending_folder_id,
                message_id: pending_message_id,
                attach_num,
                ..
            } if *pending_folder_id == folder_id && *pending_message_id == message_id => {
                Some(*attach_num)
            }
            _ => None,
        })
        .max();
    snapshot_max
        .into_iter()
        .chain(session_max)
        .max()
        .map(|value| value.saturating_add(1))
        .unwrap_or(0)
}

pub(in crate::mapi) fn email_matches_folder(
    email: &JmapEmail,
    folder_id: u64,
    mailboxes: &[JmapMailbox],
) -> bool {
    if let Some(role) = role_for_folder_id(folder_id) {
        return email.mailbox_states.iter().any(|state| state.role == role)
            || email.mailbox_role == role;
    }

    mailboxes
        .iter()
        .find(|mailbox| mapi_folder_id(mailbox) == folder_id)
        .is_some_and(|mailbox| {
            email
                .mailbox_states
                .iter()
                .any(|state| state.mailbox_id == mailbox.id)
                || email.mailbox_id == mailbox.id
        })
}

#[cfg(test)]
mod tests;
