use super::*;

pub(crate) const STORE_REPLICA_ID: u64 = 1;
pub(crate) const MAX_PERSISTED_GLOBAL_COUNTER: u64 = 0x7FFF_FFFF_FFFF;
pub(crate) const STORE_REPLICA_GUID: [u8; 16] = [
    0x74, 0x1f, 0x6f, 0xd3, 0x8e, 0x1a, 0x65, 0x4f, 0x9d, 0x42, 0x2d, 0xfb, 0x45, 0x1c, 0x8f, 0x10,
];

static MAPI_OBJECT_IDS: OnceLock<Mutex<HashMap<Uuid, MapiIdentityMaterial>>> = OnceLock::new();

#[derive(Debug, Clone)]
struct MapiIdentityMaterial {
    object_id: u64,
    source_key: Option<Vec<u8>>,
}

pub(crate) const ROOT_FOLDER_COUNTER: u64 = 1;
pub(crate) const DEFERRED_ACTION_FOLDER_COUNTER: u64 = 2;
pub(crate) const SPOOLER_QUEUE_FOLDER_COUNTER: u64 = 3;
pub(crate) const IPM_SUBTREE_FOLDER_COUNTER: u64 = 4;
pub(crate) const INBOX_FOLDER_COUNTER: u64 = 5;
pub(crate) const OUTBOX_FOLDER_COUNTER: u64 = 6;
pub(crate) const SENT_FOLDER_COUNTER: u64 = 7;
pub(crate) const TRASH_FOLDER_COUNTER: u64 = 8;
pub(crate) const COMMON_VIEWS_FOLDER_COUNTER: u64 = 9;
pub(crate) const SCHEDULE_FOLDER_COUNTER: u64 = 10;
pub(crate) const SEARCH_FOLDER_COUNTER: u64 = 11;
pub(crate) const VIEWS_FOLDER_COUNTER: u64 = 12;
pub(crate) const SHORTCUTS_FOLDER_COUNTER: u64 = 13;
pub(crate) const DRAFTS_FOLDER_COUNTER: u64 = 14;
pub(crate) const CONTACTS_FOLDER_COUNTER: u64 = 15;
pub(crate) const CALENDAR_FOLDER_COUNTER: u64 = 16;
pub(crate) const JOURNAL_FOLDER_COUNTER: u64 = 17;
pub(crate) const NOTES_FOLDER_COUNTER: u64 = 18;
pub(crate) const TASKS_FOLDER_COUNTER: u64 = 19;
pub(crate) const REMINDERS_FOLDER_COUNTER: u64 = 20;
pub(crate) const SUGGESTED_CONTACTS_FOLDER_COUNTER: u64 = 21;
pub(crate) const QUICK_CONTACTS_FOLDER_COUNTER: u64 = 22;
pub(crate) const IM_CONTACT_LIST_FOLDER_COUNTER: u64 = 23;
pub(crate) const CONTACTS_SEARCH_FOLDER_COUNTER: u64 = 24;
pub(crate) const DOCUMENT_LIBRARIES_FOLDER_COUNTER: u64 = 25;
pub(crate) const SYNC_ISSUES_FOLDER_COUNTER: u64 = 26;
pub(crate) const CONFLICTS_FOLDER_COUNTER: u64 = 27;
pub(crate) const LOCAL_FAILURES_FOLDER_COUNTER: u64 = 28;
pub(crate) const SERVER_FAILURES_FOLDER_COUNTER: u64 = 29;
pub(crate) const JUNK_FOLDER_COUNTER: u64 = 30;
pub(crate) const RSS_FEEDS_FOLDER_COUNTER: u64 = 31;
pub(crate) const TRACKED_MAIL_PROCESSING_FOLDER_COUNTER: u64 = 32;
pub(crate) const TODO_SEARCH_FOLDER_COUNTER: u64 = 33;
pub(crate) const CONVERSATION_ACTION_SETTINGS_FOLDER_COUNTER: u64 = 34;
pub(crate) const ARCHIVE_FOLDER_COUNTER: u64 = 35;
pub(crate) const FREEBUSY_DATA_FOLDER_COUNTER: u64 = 36;
pub(crate) const CONVERSATION_HISTORY_FOLDER_COUNTER: u64 = 37;
pub(crate) const FIRST_DYNAMIC_GLOBAL_COUNTER: u64 = CONVERSATION_HISTORY_FOLDER_COUNTER + 1;

pub(crate) const ROOT_FOLDER_ID: u64 = mapi_store_id(ROOT_FOLDER_COUNTER);
pub(crate) const DEFERRED_ACTION_FOLDER_ID: u64 = mapi_store_id(DEFERRED_ACTION_FOLDER_COUNTER);
pub(crate) const SPOOLER_QUEUE_FOLDER_ID: u64 = mapi_store_id(SPOOLER_QUEUE_FOLDER_COUNTER);
pub(crate) const IPM_SUBTREE_FOLDER_ID: u64 = mapi_store_id(IPM_SUBTREE_FOLDER_COUNTER);
pub(crate) const INBOX_FOLDER_ID: u64 = mapi_store_id(INBOX_FOLDER_COUNTER);
pub(crate) const OUTBOX_FOLDER_ID: u64 = mapi_store_id(OUTBOX_FOLDER_COUNTER);
pub(crate) const SENT_FOLDER_ID: u64 = mapi_store_id(SENT_FOLDER_COUNTER);
pub(crate) const TRASH_FOLDER_ID: u64 = mapi_store_id(TRASH_FOLDER_COUNTER);
pub(crate) const COMMON_VIEWS_FOLDER_ID: u64 = mapi_store_id(COMMON_VIEWS_FOLDER_COUNTER);
pub(crate) const SCHEDULE_FOLDER_ID: u64 = mapi_store_id(SCHEDULE_FOLDER_COUNTER);
pub(crate) const SEARCH_FOLDER_ID: u64 = mapi_store_id(SEARCH_FOLDER_COUNTER);
pub(crate) const VIEWS_FOLDER_ID: u64 = mapi_store_id(VIEWS_FOLDER_COUNTER);
pub(crate) const SHORTCUTS_FOLDER_ID: u64 = mapi_store_id(SHORTCUTS_FOLDER_COUNTER);
pub(crate) const DRAFTS_FOLDER_ID: u64 = mapi_store_id(DRAFTS_FOLDER_COUNTER);
pub(crate) const CONTACTS_FOLDER_ID: u64 = mapi_store_id(CONTACTS_FOLDER_COUNTER);
pub(crate) const CALENDAR_FOLDER_ID: u64 = mapi_store_id(CALENDAR_FOLDER_COUNTER);
pub(crate) const JOURNAL_FOLDER_ID: u64 = mapi_store_id(JOURNAL_FOLDER_COUNTER);
pub(crate) const NOTES_FOLDER_ID: u64 = mapi_store_id(NOTES_FOLDER_COUNTER);
pub(crate) const TASKS_FOLDER_ID: u64 = mapi_store_id(TASKS_FOLDER_COUNTER);
pub(crate) const REMINDERS_FOLDER_ID: u64 = mapi_store_id(REMINDERS_FOLDER_COUNTER);
pub(crate) const SUGGESTED_CONTACTS_FOLDER_ID: u64 =
    mapi_store_id(SUGGESTED_CONTACTS_FOLDER_COUNTER);
pub(crate) const QUICK_CONTACTS_FOLDER_ID: u64 = mapi_store_id(QUICK_CONTACTS_FOLDER_COUNTER);
pub(crate) const IM_CONTACT_LIST_FOLDER_ID: u64 = mapi_store_id(IM_CONTACT_LIST_FOLDER_COUNTER);
pub(crate) const CONTACTS_SEARCH_FOLDER_ID: u64 = mapi_store_id(CONTACTS_SEARCH_FOLDER_COUNTER);
pub(crate) const DOCUMENT_LIBRARIES_FOLDER_ID: u64 =
    mapi_store_id(DOCUMENT_LIBRARIES_FOLDER_COUNTER);
pub(crate) const SYNC_ISSUES_FOLDER_ID: u64 = mapi_store_id(SYNC_ISSUES_FOLDER_COUNTER);
pub(crate) const CONFLICTS_FOLDER_ID: u64 = mapi_store_id(CONFLICTS_FOLDER_COUNTER);
pub(crate) const LOCAL_FAILURES_FOLDER_ID: u64 = mapi_store_id(LOCAL_FAILURES_FOLDER_COUNTER);
pub(crate) const SERVER_FAILURES_FOLDER_ID: u64 = mapi_store_id(SERVER_FAILURES_FOLDER_COUNTER);
pub(crate) const JUNK_FOLDER_ID: u64 = mapi_store_id(JUNK_FOLDER_COUNTER);
pub(crate) const RSS_FEEDS_FOLDER_ID: u64 = mapi_store_id(RSS_FEEDS_FOLDER_COUNTER);
pub(crate) const TRACKED_MAIL_PROCESSING_FOLDER_ID: u64 =
    mapi_store_id(TRACKED_MAIL_PROCESSING_FOLDER_COUNTER);
pub(crate) const TODO_SEARCH_FOLDER_ID: u64 = mapi_store_id(TODO_SEARCH_FOLDER_COUNTER);
pub(crate) const CONVERSATION_ACTION_SETTINGS_FOLDER_ID: u64 =
    mapi_store_id(CONVERSATION_ACTION_SETTINGS_FOLDER_COUNTER);
pub(crate) const ARCHIVE_FOLDER_ID: u64 = mapi_store_id(ARCHIVE_FOLDER_COUNTER);
pub(crate) const FREEBUSY_DATA_FOLDER_ID: u64 = mapi_store_id(FREEBUSY_DATA_FOLDER_COUNTER);
pub(crate) const CONVERSATION_HISTORY_FOLDER_ID: u64 =
    mapi_store_id(CONVERSATION_HISTORY_FOLDER_COUNTER);

pub(crate) const fn mapi_store_id(global_counter: u64) -> u64 {
    ((global_counter & 0x0000_FFFF_FFFF_FFFF) << 16) | STORE_REPLICA_ID
}

pub(crate) fn global_counter_from_store_id(store_id: u64) -> Option<u64> {
    if store_id & 0xFFFF != STORE_REPLICA_ID {
        return None;
    }
    let counter = store_id >> 16;
    (counter != 0).then_some(counter)
}

pub(crate) fn globcnt_bytes(value: u64) -> [u8; 6] {
    let bytes = (value & 0x0000_FFFF_FFFF_FFFF).to_be_bytes();
    [bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7]]
}

pub(crate) fn global_counter_from_globcnt(bytes: &[u8]) -> Option<u64> {
    let bytes: [u8; 6] = bytes.try_into().ok()?;
    let global_counter = u64::from_be_bytes([
        0, 0, bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5],
    ]);
    (global_counter != 0).then_some(global_counter)
}

pub(crate) fn object_id_from_wire_id(bytes: &[u8]) -> Option<u64> {
    if bytes.len() != 8 {
        return None;
    }
    let replica_id = u16::from_le_bytes(bytes[..2].try_into().ok()?);
    if u64::from(replica_id) != STORE_REPLICA_ID {
        return None;
    }
    global_counter_from_globcnt(&bytes[2..8]).map(mapi_store_id)
}

pub(crate) fn object_id_from_trailing_replid_wire_id(bytes: &[u8]) -> Option<u64> {
    if bytes.len() != 8 {
        return None;
    }
    let replica_id = u16::from_le_bytes(bytes[6..8].try_into().ok()?);
    if u64::from(replica_id) != STORE_REPLICA_ID {
        return None;
    }
    global_counter_from_globcnt(&bytes[..6]).map(mapi_store_id)
}

pub(crate) fn wire_id_bytes_from_object_id(object_id: u64) -> Option<[u8; 8]> {
    let global_counter = global_counter_from_store_id(object_id)?;
    let mut bytes = [0; 8];
    bytes[..2].copy_from_slice(&(STORE_REPLICA_ID as u16).to_le_bytes());
    bytes[2..8].copy_from_slice(&globcnt_bytes(global_counter));
    Some(bytes)
}

#[allow(dead_code)]
pub(crate) fn remember_mapi_identity(canonical_id: Uuid, object_id: u64) {
    remember_mapi_identity_with_source_key(canonical_id, object_id, None);
}

pub(crate) fn remember_mapi_identity_with_source_key(
    canonical_id: Uuid,
    object_id: u64,
    source_key: Option<Vec<u8>>,
) {
    let mut ids = MAPI_OBJECT_IDS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    ids.insert(
        canonical_id,
        MapiIdentityMaterial {
            object_id,
            source_key,
        },
    );
}

pub(crate) fn mapped_mapi_object_id(canonical_id: &Uuid) -> Option<u64> {
    MAPI_OBJECT_IDS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(canonical_id)
        .map(|identity| identity.object_id)
}

pub(crate) fn mapped_mapi_source_key(canonical_id: &Uuid) -> Option<Vec<u8>> {
    MAPI_OBJECT_IDS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(canonical_id)
        .and_then(|identity| identity.source_key.clone())
}

pub(crate) fn long_term_id_from_object_id(object_id: u64) -> Option<[u8; 24]> {
    let global_counter = global_counter_from_store_id(object_id)?;
    let mut long_term_id = [0; 24];
    long_term_id[..16].copy_from_slice(&STORE_REPLICA_GUID);
    long_term_id[16..22].copy_from_slice(&globcnt_bytes(global_counter));
    Some(long_term_id)
}

pub(crate) fn object_id_from_long_term_id(long_term_id: &[u8]) -> Option<u64> {
    object_id_from_long_term_id_with_replica_guids(long_term_id, &[])
}

pub(crate) fn object_id_from_long_term_id_with_replica_guids(
    long_term_id: &[u8],
    replica_guid_aliases: &[[u8; 16]],
) -> Option<u64> {
    if long_term_id.len() != 24 || long_term_id[22..24] != [0, 0] {
        return None;
    }
    let replica_guid: [u8; 16] = long_term_id[..16].try_into().ok()?;
    if replica_guid != STORE_REPLICA_GUID && !replica_guid_aliases.contains(&replica_guid) {
        return None;
    }
    global_counter_from_globcnt(&long_term_id[16..22]).map(mapi_store_id)
}

pub(crate) fn folder_entry_id_from_object_id(
    mailbox_guid: Uuid,
    object_id: u64,
) -> Option<Vec<u8>> {
    let global_counter = global_counter_from_store_id(object_id)?;
    let mut entry_id = Vec::with_capacity(46);
    entry_id.extend_from_slice(&0u32.to_le_bytes());
    entry_id.extend_from_slice(&mailbox_guid.to_bytes_le());
    entry_id.extend_from_slice(&1u16.to_le_bytes());
    entry_id.extend_from_slice(&STORE_REPLICA_GUID);
    entry_id.extend_from_slice(&globcnt_bytes(global_counter));
    entry_id.extend_from_slice(&0u16.to_le_bytes());
    Some(entry_id)
}

pub(crate) fn object_id_from_folder_entry_id(entry_id: &[u8]) -> Option<u64> {
    if entry_id.len() != 46
        || entry_id[0..4] != [0, 0, 0, 0]
        || entry_id[20..22] != 1u16.to_le_bytes()
        || entry_id[44..46] != [0, 0]
    {
        return None;
    }
    let global_counter = global_counter_from_globcnt(&entry_id[38..44])?;
    let object_id = mapi_store_id(global_counter);
    if entry_id[22..38] == STORE_REPLICA_GUID || is_advertised_special_folder_id(object_id) {
        Some(object_id)
    } else {
        None
    }
}

pub(crate) fn object_id_from_folder_identifier_bytes(bytes: &[u8]) -> Option<u64> {
    object_id_from_folder_entry_id(bytes)
        .or_else(|| object_id_from_long_term_id(bytes))
        .or_else(|| stale_special_folder_object_id_from_long_term_id(bytes))
}

fn stale_special_folder_object_id_from_long_term_id(long_term_id: &[u8]) -> Option<u64> {
    if long_term_id.len() != 24 || long_term_id[22..24] != [0, 0] {
        return None;
    }
    let global_counter = global_counter_from_globcnt(&long_term_id[16..22])?;
    let object_id = mapi_store_id(global_counter);
    is_advertised_special_folder_id(object_id).then_some(object_id)
}

fn is_advertised_special_folder_id(object_id: u64) -> bool {
    matches!(
        object_id,
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
            | INBOX_FOLDER_ID
            | DRAFTS_FOLDER_ID
            | SENT_FOLDER_ID
            | TRASH_FOLDER_ID
            | OUTBOX_FOLDER_ID
            | CONTACTS_FOLDER_ID
            | CALENDAR_FOLDER_ID
            | TASKS_FOLDER_ID
            | NOTES_FOLDER_ID
            | JOURNAL_FOLDER_ID
            | REMINDERS_FOLDER_ID
            | JUNK_FOLDER_ID
            | ARCHIVE_FOLDER_ID
            | RSS_FEEDS_FOLDER_ID
            | TRACKED_MAIL_PROCESSING_FOLDER_ID
            | TODO_SEARCH_FOLDER_ID
            | CONVERSATION_ACTION_SETTINGS_FOLDER_ID
            | SUGGESTED_CONTACTS_FOLDER_ID
            | CONTACTS_SEARCH_FOLDER_ID
            | DOCUMENT_LIBRARIES_FOLDER_ID
            | IM_CONTACT_LIST_FOLDER_ID
            | QUICK_CONTACTS_FOLDER_ID
            | SYNC_ISSUES_FOLDER_ID
            | CONFLICTS_FOLDER_ID
            | LOCAL_FAILURES_FOLDER_ID
            | SERVER_FAILURES_FOLDER_ID
            | CONVERSATION_HISTORY_FOLDER_ID
    )
}

pub(crate) fn message_entry_id_from_object_ids(
    mailbox_guid: Uuid,
    folder_id: u64,
    message_id: u64,
) -> Option<Vec<u8>> {
    let folder_counter = global_counter_from_store_id(folder_id)?;
    let message_counter = global_counter_from_store_id(message_id)?;
    let mut entry_id = Vec::with_capacity(70);
    entry_id.extend_from_slice(&0u32.to_le_bytes());
    entry_id.extend_from_slice(&mailbox_guid.to_bytes_le());
    entry_id.extend_from_slice(&0x0007u16.to_le_bytes());
    entry_id.extend_from_slice(&STORE_REPLICA_GUID);
    entry_id.extend_from_slice(&globcnt_bytes(folder_counter));
    entry_id.extend_from_slice(&0u16.to_le_bytes());
    entry_id.extend_from_slice(&STORE_REPLICA_GUID);
    entry_id.extend_from_slice(&globcnt_bytes(message_counter));
    entry_id.extend_from_slice(&0u16.to_le_bytes());
    Some(entry_id)
}

pub(crate) fn source_key_for_object_id(object_id: u64) -> Vec<u8> {
    let mut key = STORE_REPLICA_GUID.to_vec();
    let global_counter = global_counter_from_store_id(object_id)
        .expect("source keys require a MAPI object id with the store replica id");
    key.extend_from_slice(&globcnt_bytes(global_counter));
    key
}

#[allow(dead_code)]
pub(crate) fn object_id_from_source_key(source_key: &[u8]) -> Option<u64> {
    if source_key.len() != 22 || source_key[..16] != STORE_REPLICA_GUID {
        return None;
    }
    let global_counter = global_counter_from_globcnt(source_key.get(16..22)?)?;
    if global_counter > MAX_PERSISTED_GLOBAL_COUNTER {
        return None;
    }
    Some(mapi_store_id(global_counter))
}

pub(crate) fn change_key_for_change_number(change_number: u64) -> Vec<u8> {
    let mut key = STORE_REPLICA_GUID.to_vec();
    key.extend_from_slice(&globcnt_bytes(change_number.max(1)));
    key
}

pub(crate) fn instance_key_for_object_id(object_id: u64) -> Vec<u8> {
    source_key_for_object_id(object_id)
}

pub(crate) fn persisted_identity_material(global_counter: u64) -> (u64, Vec<u8>, Vec<u8>, Vec<u8>) {
    let object_id = mapi_store_id(global_counter);
    let source_key = source_key_for_object_id(object_id);
    let change_key = change_key_for_change_number(global_counter);
    let instance_key = instance_key_for_object_id(object_id);
    debug_assert_eq!(source_key.len(), 22);
    debug_assert_eq!(change_key.len(), 22);
    debug_assert_eq!(instance_key.len(), 22);
    (object_id, source_key, change_key, instance_key)
}

#[allow(dead_code)]
pub(crate) fn legacy_migration_object_id(canonical_id: &Uuid) -> u64 {
    let bytes = canonical_id.as_bytes();
    let value = u64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]) & 0x0000_FFFF_FFFF_FFFF;
    mapi_store_id(value.max(0x100))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn long_term_id_round_trips_object_id() {
        let object_id = mapi_store_id(0x1234_5678_9abc);
        let long_term_id = long_term_id_from_object_id(object_id).unwrap();

        assert_eq!(long_term_id.len(), 24);
        assert_eq!(&long_term_id[16..22], &[0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc]);
        assert_eq!(object_id_from_long_term_id(&long_term_id), Some(object_id));
    }

    #[test]
    fn folder_entry_id_round_trips_object_id() {
        let mailbox_guid = Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap();
        let object_id = CALENDAR_FOLDER_ID;
        let entry_id = folder_entry_id_from_object_id(mailbox_guid, object_id).unwrap();

        assert_eq!(entry_id.len(), 46);
        assert_eq!(&entry_id[..4], &0u32.to_le_bytes());
        assert_eq!(&entry_id[4..20], &mailbox_guid.to_bytes_le());
        assert_eq!(&entry_id[20..22], &1u16.to_le_bytes());
        assert_eq!(&entry_id[22..38], &STORE_REPLICA_GUID);
        assert_eq!(&entry_id[38..44], &globcnt_bytes(CALENDAR_FOLDER_COUNTER));
        assert_eq!(&entry_id[44..46], &0u16.to_le_bytes());
        assert_eq!(object_id_from_folder_entry_id(&entry_id), Some(object_id));
        assert_eq!(
            object_id_from_folder_identifier_bytes(&entry_id),
            Some(object_id)
        );
    }

    #[test]
    fn message_entry_id_uses_private_mailbox_shape_with_source_key_counters() {
        let mailbox_guid = Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap();
        let message_id = mapi_store_id(FIRST_DYNAMIC_GLOBAL_COUNTER + 7);
        let entry_id =
            message_entry_id_from_object_ids(mailbox_guid, CALENDAR_FOLDER_ID, message_id)
                .expect("message EntryID");

        assert_eq!(entry_id.len(), 70);
        assert_eq!(&entry_id[..4], &0u32.to_le_bytes());
        assert_eq!(&entry_id[4..20], &mailbox_guid.to_bytes_le());
        assert_eq!(&entry_id[20..22], &0x0007u16.to_le_bytes());
        assert_eq!(&entry_id[22..38], &STORE_REPLICA_GUID);
        assert_eq!(&entry_id[38..44], &globcnt_bytes(CALENDAR_FOLDER_COUNTER));
        assert_eq!(&entry_id[44..46], &0u16.to_le_bytes());
        assert_eq!(&entry_id[46..62], &STORE_REPLICA_GUID);
        assert_eq!(
            &entry_id[62..68],
            &source_key_for_object_id(message_id)[16..22]
        );
        assert_eq!(&entry_id[68..70], &0u16.to_le_bytes());
    }

    #[test]
    fn stale_cached_special_folder_identifiers_normalize_to_canonical_ids() {
        let mailbox_guid = Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap();
        let mut entry_id =
            folder_entry_id_from_object_id(mailbox_guid, CALENDAR_FOLDER_ID).unwrap();
        entry_id[22..38].copy_from_slice(&[0xA5; 16]);
        assert_eq!(
            object_id_from_folder_identifier_bytes(&entry_id),
            Some(CALENDAR_FOLDER_ID)
        );

        let mut long_term_id = long_term_id_from_object_id(CALENDAR_FOLDER_ID).unwrap();
        long_term_id[..16].copy_from_slice(&[0xA5; 16]);
        assert_eq!(
            object_id_from_folder_identifier_bytes(&long_term_id),
            Some(CALENDAR_FOLDER_ID)
        );
    }

    #[test]
    fn stale_cached_normal_item_identifiers_are_not_accepted_as_special_folders() {
        let mailbox_guid = Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap();
        let object_id = mapi_store_id(FIRST_DYNAMIC_GLOBAL_COUNTER);
        let mut entry_id = folder_entry_id_from_object_id(mailbox_guid, object_id).unwrap();
        entry_id[22..38].copy_from_slice(&[0xA5; 16]);
        assert_eq!(object_id_from_folder_identifier_bytes(&entry_id), None);

        let mut long_term_id = long_term_id_from_object_id(object_id).unwrap();
        long_term_id[..16].copy_from_slice(&[0xA5; 16]);
        assert_eq!(object_id_from_folder_identifier_bytes(&long_term_id), None);
    }

    #[test]
    fn wire_id_round_trips_replica_id_and_big_endian_global_counter() {
        let wire_id = [0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10];

        assert_eq!(object_id_from_wire_id(&wire_id), Some(CALENDAR_FOLDER_ID));
        assert_eq!(
            wire_id_bytes_from_object_id(CALENDAR_FOLDER_ID),
            Some(wire_id)
        );
    }

    #[test]
    fn source_change_and_instance_keys_are_replica_scoped() {
        let object_id = mapi_store_id(42);
        assert_eq!(
            source_key_for_object_id(object_id),
            instance_key_for_object_id(object_id)
        );
        assert_eq!(source_key_for_object_id(object_id).len(), 22);
        assert_eq!(change_key_for_change_number(7).len(), 22);
        assert_eq!(
            &source_key_for_object_id(object_id)[16..22],
            &[0, 0, 0, 0, 0, 42]
        );
        assert_eq!(
            &change_key_for_change_number(7)[16..22],
            &[0, 0, 0, 0, 0, 7]
        );
        assert_eq!(
            object_id_from_source_key(&source_key_for_object_id(object_id)),
            Some(object_id)
        );
        assert!(source_key_for_object_id(object_id).starts_with(&STORE_REPLICA_GUID));
        assert!(change_key_for_change_number(7).starts_with(&STORE_REPLICA_GUID));
    }

    #[test]
    fn source_key_rejects_counters_outside_persisted_object_id_range() {
        let mut source_key = STORE_REPLICA_GUID.to_vec();
        source_key.extend_from_slice(&globcnt_bytes(MAX_PERSISTED_GLOBAL_COUNTER + 1));

        assert_eq!(object_id_from_source_key(&source_key), None);
    }

    #[test]
    fn dynamic_counters_start_after_reserved_special_folders() {
        assert_eq!(
            FIRST_DYNAMIC_GLOBAL_COUNTER,
            CONVERSATION_HISTORY_FOLDER_COUNTER + 1
        );
        assert!(FIRST_DYNAMIC_GLOBAL_COUNTER > CONVERSATION_HISTORY_FOLDER_COUNTER);
    }

    #[test]
    fn persisted_identity_material_matches_schema_key_lengths() {
        for global_counter in [
            ROOT_FOLDER_COUNTER,
            INBOX_FOLDER_COUNTER,
            REMINDERS_FOLDER_COUNTER,
            FIRST_DYNAMIC_GLOBAL_COUNTER,
            FIRST_DYNAMIC_GLOBAL_COUNTER + 42,
        ] {
            let (object_id, source_key, change_key, instance_key) =
                persisted_identity_material(global_counter);
            assert_eq!(
                global_counter_from_store_id(object_id),
                Some(global_counter)
            );
            assert_eq!(source_key.len(), 22);
            assert_eq!(change_key.len(), 22);
            assert_eq!(instance_key.len(), 22);
            assert_eq!(&source_key[16..22], &globcnt_bytes(global_counter));
            assert_eq!(&change_key[16..22], &globcnt_bytes(global_counter));
            assert_eq!(&instance_key[16..22], &globcnt_bytes(global_counter));
        }
    }

    #[test]
    #[should_panic(expected = "source keys require a MAPI object id with the store replica id")]
    fn source_key_rejects_non_mapi_object_id_instead_of_emitting_guid_only_xid() {
        let _ = source_key_for_object_id(42);
    }
}
