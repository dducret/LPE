use super::*;
use crate::store::ExchangeAddressBookEntryDetails;

const FOLDER_IPM_SUBTREE_VALID: u32 = 0x0000_0001;
const FOLDER_IPM_INBOX_VALID: u32 = 0x0000_0002;
const FOLDER_IPM_OUTBOX_VALID: u32 = 0x0000_0004;
const FOLDER_IPM_WASTEBASKET_VALID: u32 = 0x0000_0008;
const FOLDER_IPM_SENTMAIL_VALID: u32 = 0x0000_0010;
const FOLDER_VIEWS_VALID: u32 = 0x0000_0020;
const FOLDER_COMMON_VIEWS_VALID: u32 = 0x0000_0040;
const FOLDER_FINDER_VALID: u32 = 0x0000_0080;

pub(in crate::mapi) fn logon_property_value(
    principal: &AccountPrincipal,
    property_tag: u32,
) -> Option<MapiValue> {
    match property_tag {
        PID_TAG_SERIALIZED_REPLID_GUID_MAP => Some(MapiValue::Binary(serialized_replid_guid_map())),
        PID_TAG_VALID_FOLDER_MASK => Some(MapiValue::U32(valid_folder_mask())),
        PID_TAG_RESOURCE_FLAGS => Some(MapiValue::U32(0)),
        PID_TAG_USER_ENTRY_ID => Some(MapiValue::Binary(mailbox_owner_entry_id(principal))),
        PID_TAG_MAILBOX_OWNER_ENTRY_ID => {
            Some(MapiValue::Binary(mailbox_owner_entry_id(principal)))
        }
        PID_TAG_MAILBOX_OWNER_NAME_W => Some(MapiValue::String(principal.display_name.clone())),
        PID_TAG_IPM_PUBLIC_FOLDERS_ENTRY_ID => Some(MapiValue::Binary(
            crate::mapi::identity::public_folder_entry_id_from_object_id(
                PUBLIC_FOLDERS_ROOT_FOLDER_ID,
            )
            .expect("public-folder root uses a valid MAPI folder ID"),
        )),
        PID_TAG_SERVER_TYPE_DISPLAY_NAME_W => Some(MapiValue::String("LPE".to_string())),
        PID_TAG_SERVER_CONNECTED_ICON | PID_TAG_SERVER_ACCOUNT_ICON => {
            Some(MapiValue::Binary(OUTLOOK_STORE_ICON_ICO.to_vec()))
        }
        PID_TAG_OUTLOOK_STORE_STATE => Some(MapiValue::U32(0)),
        PID_TAG_PRIVATE => Some(MapiValue::Bool(true)),
        PID_TAG_USER_GUID => Some(MapiValue::Binary(principal.account_id.as_bytes().to_vec())),
        PID_TAG_MESSAGE_SIZE_EXTENDED => principal
            .quota_used_octets
            .map(|value| MapiValue::I64(value.min(i64::MAX as u64) as i64)),
        PID_TAG_PROHIBIT_RECEIVE_QUOTA
        | PID_TAG_PROHIBIT_SEND_QUOTA
        | PID_TAG_STORAGE_QUOTA_LIMIT => principal
            .quota_mb
            .map(|value| MapiValue::U32(value.saturating_mul(1024))),
        PID_TAG_MAX_SUBMIT_MESSAGE_SIZE | PID_TAG_EXTENDED_RULE_SIZE_LIMIT => {
            Some(MapiValue::U32(35 * 1024))
        }
        PID_TAG_PST_PATH_W => Some(MapiValue::String(String::new())),
        _ => special_folder_identification_property_value(principal.account_id, property_tag),
    }
}

pub(in crate::mapi) fn special_folder_identification_property_value(
    mailbox_guid: Uuid,
    property_tag: u32,
) -> Option<MapiValue> {
    match canonical_property_storage_tag(property_tag) {
        PID_TAG_VALID_FOLDER_MASK => Some(MapiValue::U32(valid_folder_mask())),
        PID_TAG_IPM_SUBTREE_ENTRY_ID => Some(special_folder_entry_id_value(
            mailbox_guid,
            IPM_SUBTREE_FOLDER_ID,
        )),
        PID_TAG_IPM_OUTBOX_ENTRY_ID => Some(special_folder_entry_id_value(
            mailbox_guid,
            OUTBOX_FOLDER_ID,
        )),
        PID_TAG_IPM_WASTEBASKET_ENTRY_ID => {
            Some(special_folder_entry_id_value(mailbox_guid, TRASH_FOLDER_ID))
        }
        PID_TAG_IPM_SENTMAIL_ENTRY_ID => {
            Some(special_folder_entry_id_value(mailbox_guid, SENT_FOLDER_ID))
        }
        PID_TAG_VIEWS_ENTRY_ID => {
            Some(special_folder_entry_id_value(mailbox_guid, VIEWS_FOLDER_ID))
        }
        PID_TAG_COMMON_VIEWS_ENTRY_ID => Some(special_folder_entry_id_value(
            mailbox_guid,
            COMMON_VIEWS_FOLDER_ID,
        )),
        PID_TAG_FINDER_ENTRY_ID => Some(special_folder_entry_id_value(
            mailbox_guid,
            SEARCH_FOLDER_ID,
        )),
        PID_TAG_IPM_ARCHIVE_ENTRY_ID => Some(special_folder_entry_id_value(
            mailbox_guid,
            ARCHIVE_FOLDER_ID,
        )),
        PID_TAG_IPM_APPOINTMENT_ENTRY_ID => Some(special_folder_entry_id_value(
            mailbox_guid,
            CALENDAR_FOLDER_ID,
        )),
        PID_TAG_IPM_CONTACT_ENTRY_ID => Some(special_folder_entry_id_value(
            mailbox_guid,
            CONTACTS_FOLDER_ID,
        )),
        PID_TAG_IPM_JOURNAL_ENTRY_ID => Some(special_folder_entry_id_value(
            mailbox_guid,
            JOURNAL_FOLDER_ID,
        )),
        PID_TAG_IPM_NOTE_ENTRY_ID => {
            Some(special_folder_entry_id_value(mailbox_guid, NOTES_FOLDER_ID))
        }
        PID_TAG_IPM_TASK_ENTRY_ID => {
            Some(special_folder_entry_id_value(mailbox_guid, TASKS_FOLDER_ID))
        }
        PID_TAG_REM_ONLINE_ENTRY_ID => Some(special_folder_entry_id_value(
            mailbox_guid,
            REMINDERS_FOLDER_ID,
        )),
        PID_TAG_REM_OFFLINE_ENTRY_ID => Some(special_folder_entry_id_value(
            mailbox_guid,
            REMINDERS_FOLDER_ID,
        )),
        PID_TAG_IPM_DRAFTS_ENTRY_ID => Some(special_folder_entry_id_value(
            mailbox_guid,
            DRAFTS_FOLDER_ID,
        )),
        PID_TAG_ADDITIONAL_REN_ENTRY_IDS => Some(MapiValue::MultiBinary(additional_ren_entry_ids(
            mailbox_guid,
        ))),
        PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX => {
            Some(MapiValue::Binary(additional_ren_entry_ids_ex(mailbox_guid)))
        }
        PID_TAG_FREE_BUSY_ENTRY_IDS => {
            Some(MapiValue::MultiBinary(free_busy_entry_ids(mailbox_guid)))
        }
        _ => None,
    }
}

pub(in crate::mapi) fn is_default_folder_identification_property_tag(property_tag: u32) -> bool {
    is_scalar_default_folder_entry_id_property_tag(property_tag)
        || matches!(
            canonical_property_storage_tag(property_tag),
            PID_TAG_ADDITIONAL_REN_ENTRY_IDS
                | PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX
                | PID_TAG_FREE_BUSY_ENTRY_IDS
        )
}

pub(in crate::mapi) fn is_scalar_default_folder_entry_id_property_tag(property_tag: u32) -> bool {
    matches!(
        canonical_property_storage_tag(property_tag),
        PID_TAG_IPM_SUBTREE_ENTRY_ID
            | PID_TAG_IPM_OUTBOX_ENTRY_ID
            | PID_TAG_IPM_WASTEBASKET_ENTRY_ID
            | PID_TAG_IPM_SENTMAIL_ENTRY_ID
            | PID_TAG_VIEWS_ENTRY_ID
            | PID_TAG_COMMON_VIEWS_ENTRY_ID
            | PID_TAG_FINDER_ENTRY_ID
            | PID_TAG_IPM_ARCHIVE_ENTRY_ID
            | PID_TAG_IPM_APPOINTMENT_ENTRY_ID
            | PID_TAG_IPM_CONTACT_ENTRY_ID
            | PID_TAG_IPM_JOURNAL_ENTRY_ID
            | PID_TAG_IPM_NOTE_ENTRY_ID
            | PID_TAG_IPM_TASK_ENTRY_ID
            | PID_TAG_REM_ONLINE_ENTRY_ID
            | PID_TAG_REM_OFFLINE_ENTRY_ID
            | PID_TAG_IPM_DRAFTS_ENTRY_ID
    )
}

pub(in crate::mapi) fn ipm_subtree_ost_ostid(principal: &AccountPrincipal) -> Vec<u8> {
    let mut value = Vec::with_capacity(20);
    value.extend_from_slice(principal.account_id.as_bytes());
    value.extend_from_slice(&1u32.to_le_bytes());
    value
}

fn valid_folder_mask() -> u32 {
    FOLDER_IPM_SUBTREE_VALID
        | FOLDER_IPM_INBOX_VALID
        | FOLDER_IPM_OUTBOX_VALID
        | FOLDER_IPM_WASTEBASKET_VALID
        | FOLDER_IPM_SENTMAIL_VALID
        | FOLDER_VIEWS_VALID
        | FOLDER_COMMON_VIEWS_VALID
        | FOLDER_FINDER_VALID
}

fn special_folder_entry_id_value(mailbox_guid: Uuid, folder_id: u64) -> MapiValue {
    MapiValue::Binary(special_folder_entry_id(mailbox_guid, folder_id))
}

fn additional_ren_entry_ids(mailbox_guid: Uuid) -> Vec<Vec<u8>> {
    [
        CONFLICTS_FOLDER_ID,
        SYNC_ISSUES_FOLDER_ID,
        LOCAL_FAILURES_FOLDER_ID,
        SERVER_FAILURES_FOLDER_ID,
        JUNK_FOLDER_ID,
    ]
    .into_iter()
    .map(|folder_id| special_folder_entry_id(mailbox_guid, folder_id))
    .collect()
}

fn additional_ren_entry_ids_ex(mailbox_guid: Uuid) -> Vec<u8> {
    let entries = [
        (0x8001u16, RSS_FEEDS_FOLDER_ID),
        (0x8002, TRACKED_MAIL_PROCESSING_FOLDER_ID),
        (0x8004, TODO_SEARCH_FOLDER_ID),
        (0x8006, CONVERSATION_ACTION_SETTINGS_FOLDER_ID),
        (0x8007, QUICK_STEP_SETTINGS_FOLDER_ID),
        (0x8008, SUGGESTED_CONTACTS_FOLDER_ID),
        (0x8009, CONTACTS_SEARCH_FOLDER_ID),
        (0x800A, IM_CONTACT_LIST_FOLDER_ID),
        (0x800B, QUICK_CONTACTS_FOLDER_ID),
        (0x800F, ARCHIVE_FOLDER_ID),
    ];
    let mut value = Vec::new();
    for (persist_id, folder_id) in entries {
        let entry_id = special_folder_entry_id(mailbox_guid, folder_id);
        let data_size = 4usize.saturating_add(entry_id.len());
        value.extend_from_slice(&persist_id.to_le_bytes());
        value.extend_from_slice(&(data_size.min(u16::MAX as usize) as u16).to_le_bytes());
        value.extend_from_slice(&0x0001u16.to_le_bytes());
        value.extend_from_slice(&(entry_id.len().min(u16::MAX as usize) as u16).to_le_bytes());
        value.extend_from_slice(&entry_id);
    }
    value.extend_from_slice(&0u16.to_le_bytes());
    value.extend_from_slice(&0u16.to_le_bytes());
    value
}

fn free_busy_entry_ids(mailbox_guid: Uuid) -> Vec<Vec<u8>> {
    vec![
        Vec::new(),
        Vec::new(),
        Vec::new(),
        special_folder_entry_id(mailbox_guid, FREEBUSY_DATA_FOLDER_ID),
    ]
}

fn special_folder_entry_id(mailbox_guid: Uuid, folder_id: u64) -> Vec<u8> {
    crate::mapi::identity::folder_entry_id_from_object_id(mailbox_guid, folder_id)
        .expect("special folders use valid MAPI folder IDs")
}

pub(crate) fn mailbox_owner_entry_id(principal: &AccountPrincipal) -> Vec<u8> {
    let entry = super::nspi::principal_address_book_entry(principal);
    let legacy_dn = super::nspi::nspi_entry_unprefixed_legacy_dn(&entry);
    let mut value = Vec::with_capacity(28 + legacy_dn.len() + 1);
    value.extend_from_slice(&[0, 0, 0, 0]);
    value.extend_from_slice(&NSPI_PERMANENT_ENTRY_ID_PROVIDER_UID);
    value.extend_from_slice(&1u32.to_le_bytes());
    value.extend_from_slice(&super::nspi::nspi_entry_display_type(&entry).to_le_bytes());
    value.extend_from_slice(legacy_dn.as_bytes());
    value.push(0);
    value
}

pub(in crate::mapi) fn sent_representing_entry_id(email: &JmapEmail) -> Vec<u8> {
    let entry = ExchangeAddressBookEntry {
        id: email.submitted_by_account_id,
        display_name: email_sent_representing_name(email).to_string(),
        email: email_sent_representing_address(email).to_string(),
        entry_kind: ExchangeAddressBookEntryKind::Account,
        directory_kind: ExchangeAddressBookDirectoryKind::Person,
        member_emails: Vec::new(),
        details: ExchangeAddressBookEntryDetails::default(),
    };
    super::nspi::nspi_entry_permanent_entry_id(&entry)
}

pub(in crate::mapi) fn hierarchy_display_name(
    hierarchy_values: &[(u32, MapiValue)],
    property_values: &[(u32, MapiValue)],
) -> Option<String> {
    let display_name = |values: &[(u32, MapiValue)]| {
        values.iter().rev().find_map(|(tag, value)| {
            (*tag == PID_TAG_DISPLAY_NAME_W)
                .then(|| value.as_text().map(str::trim).map(str::to_string))
                .flatten()
        })
    };
    // [MS-OXCFXICS] section 3.2.5.9.4.3: PropertyValues entries
    // duplicated in HierarchyValues are ignored.
    display_name(hierarchy_values)
        .or_else(|| display_name(property_values))
        .filter(|value| !value.is_empty())
}

pub(in crate::mapi) fn imported_hierarchy_existing_mailbox<'a>(
    hierarchy_values: &[(u32, MapiValue)],
    display_name: &str,
    mailboxes: &'a [JmapMailbox],
) -> Option<&'a JmapMailbox> {
    let source_key = hierarchy_values
        .iter()
        .find_map(|(tag, value)| match (tag, value) {
            (tag, MapiValue::Binary(value)) if *tag == PID_TAG_SOURCE_KEY => Some(value.as_slice()),
            _ => None,
        });
    if let Some(source_key) = source_key {
        if let Some(mailbox) = mailboxes.iter().find(|mailbox| {
            mapi_mailstore::source_key_for_mailbox_folder(mailbox) == source_key
                || mapi_mailstore::source_key_for_uuid(&mailbox.id) == source_key
        }) {
            return Some(mailbox);
        }
    }

    mailboxes
        .iter()
        .find(|mailbox| mailbox.name.eq_ignore_ascii_case(display_name))
}

pub(in crate::mapi) fn system_folder_display_name(display_name: &str) -> bool {
    matches!(
        display_name.trim().to_ascii_lowercase().as_str(),
        "inbox"
            | "drafts"
            | "sent"
            | "sent items"
            | "deleted"
            | "deleted items"
            | "trash"
            | "outbox"
            | "sync issues"
            | "conflicts"
            | "local failures"
            | "server failures"
            | "junk e-mail"
            | "junk email"
            | "rss feeds"
            | "archive"
            | "conversation history"
    )
}
