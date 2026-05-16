use super::*;

pub(crate) const STORE_REPLICA_ID: u64 = 1;
pub(crate) const STORE_REPLICA_GUID: [u8; 16] = [
    0x74, 0x1f, 0x6f, 0xd3, 0x8e, 0x1a, 0x65, 0x4f, 0x9d, 0x42, 0x2d, 0xfb, 0x45, 0x1c, 0x8f, 0x10,
];

static MAPI_OBJECT_IDS: OnceLock<Mutex<HashMap<Uuid, u64>>> = OnceLock::new();

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

pub(crate) fn remember_mapi_identity(canonical_id: Uuid, object_id: u64) {
    let mut ids = MAPI_OBJECT_IDS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    ids.insert(canonical_id, object_id);
}

pub(crate) fn mapped_mapi_object_id(canonical_id: &Uuid) -> Option<u64> {
    MAPI_OBJECT_IDS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(canonical_id)
        .copied()
}

pub(crate) fn long_term_id_from_object_id(object_id: u64) -> Option<[u8; 24]> {
    let global_counter = global_counter_from_store_id(object_id)?;
    let mut long_term_id = [0; 24];
    long_term_id[..16].copy_from_slice(&STORE_REPLICA_GUID);
    long_term_id[16..22].copy_from_slice(&globcnt_bytes(global_counter));
    Some(long_term_id)
}

pub(crate) fn object_id_from_long_term_id(long_term_id: &[u8]) -> Option<u64> {
    if long_term_id.len() != 24
        || long_term_id[..16] != STORE_REPLICA_GUID
        || long_term_id[22..24] != [0, 0]
    {
        return None;
    }
    global_counter_from_globcnt(&long_term_id[16..22]).map(mapi_store_id)
}

pub(crate) fn source_key_for_object_id(object_id: u64) -> Vec<u8> {
    let mut key = STORE_REPLICA_GUID.to_vec();
    let Some(global_counter) = global_counter_from_store_id(object_id) else {
        return key;
    };
    key.extend_from_slice(&globcnt_bytes(global_counter));
    key
}

#[allow(dead_code)]
pub(crate) fn object_id_from_source_key(source_key: &[u8]) -> Option<u64> {
    if source_key.len() != 22 || source_key[..16] != STORE_REPLICA_GUID {
        return None;
    }
    global_counter_from_globcnt(source_key.get(16..22)?).map(mapi_store_id)
}

pub(crate) fn change_key_for_change_number(change_number: u64) -> Vec<u8> {
    let mut key = STORE_REPLICA_GUID.to_vec();
    key.extend_from_slice(&globcnt_bytes(change_number.max(1)));
    key
}

pub(crate) fn instance_key_for_object_id(object_id: u64) -> Vec<u8> {
    source_key_for_object_id(object_id)
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
}
