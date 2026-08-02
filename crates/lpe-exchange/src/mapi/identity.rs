use super::*;
use crate::store::{MapiIdentityObjectKind, MapiIdentityRecord, MapiIdentityRequest};
use anyhow::{anyhow, Result};

pub(crate) const STORE_REPLICA_ID: u64 = 1;
pub(crate) const MAX_PERSISTED_GLOBAL_COUNTER: u64 = 0x7FFF_FFFF_FFFF;
pub(crate) const FIRST_RESERVED_HIGH_GLOBAL_COUNTER: u64 = 0x7FFF_FE00_0000;
pub(crate) const STORE_REPLICA_GUID: [u8; 16] = [
    0x74, 0x1f, 0x6f, 0xd3, 0x8e, 0x1a, 0x65, 0x4f, 0x9d, 0x42, 0x2d, 0xfb, 0x45, 0x1c, 0x8f, 0x10,
];
// [MS-OXCDATA] section 2.2.4.1: Folder EntryIDs in the public message
// store use this provider UID instead of the private mailbox GUID.
pub(crate) const PUBLIC_FOLDER_PROVIDER_UID: [u8; 16] = [
    0x1a, 0x44, 0x73, 0x90, 0xaa, 0x66, 0x11, 0xcd, 0x9b, 0xc8, 0x00, 0xaa, 0x00, 0x2f, 0xc4, 0x5a,
];
// [MS-OXCDATA] section 2.2.4.3: Store Object EntryIDs use the
// MAPI store provider wrapper and the mailbox-store provider UID.
const STORE_OBJECT_PROVIDER_UID: [u8; 16] = [
    0x38, 0xa1, 0xbb, 0x10, 0x05, 0xe5, 0x10, 0x1a, 0xa1, 0xbb, 0x08, 0x00, 0x2b, 0x2a, 0x56, 0xc2,
];
const MAILBOX_STORE_PROVIDER_UID: [u8; 16] = [
    0x1b, 0x55, 0xfa, 0x20, 0xaa, 0x66, 0x11, 0xcd, 0x9b, 0xc8, 0x00, 0xaa, 0x00, 0x2f, 0xc4, 0x5a,
];

static MAPI_OBJECT_IDS: OnceLock<Mutex<HashMap<Uuid, MapiIdentityMaterial>>> = OnceLock::new();

tokio::task_local! {
    static CURRENT_MAPI_IDENTITY_CODEC: MapiIdentityCodec;
}

pub(crate) async fn with_current_mapi_identity_codec<T>(
    codec: MapiIdentityCodec,
    future: impl std::future::Future<Output = T>,
) -> T {
    CURRENT_MAPI_IDENTITY_CODEC.scope(codec, future).await
}

fn current_mapi_identity_codec<T>(mapper: impl FnOnce(&MapiIdentityCodec) -> T) -> Option<T> {
    CURRENT_MAPI_IDENTITY_CODEC.try_with(mapper).ok()
}

pub(crate) fn current_store_replica_guid() -> [u8; 16] {
    current_mapi_identity_codec(MapiIdentityCodec::replica_guid).unwrap_or(STORE_REPLICA_GUID)
}

pub(crate) fn durable_object_id(object_id: u64) -> Option<u64> {
    current_mapi_identity_codec(|codec| codec.actual_object_id(object_id))
        .unwrap_or(Some(object_id))
}

#[derive(Debug, Clone)]
struct MapiIdentityMaterial {
    object_id: u64,
    source_key: Option<Vec<u8>>,
}

/// Translates the stable logical special-folder IDs used by the mailbox model
/// into the durable IDs allocated for one database/account request scope.
///
/// The logical IDs intentionally remain outside the durable allocation range.
/// [MS-OXCSTOR] section 2.2.1.8.2 defines the wire FID as a REPLID/GLOBCNT
/// value, so the codec owns every conversion between that wire identity and
/// the internal special-folder role IDs.
#[derive(Debug, Clone)]
pub(crate) struct MapiIdentityCodec {
    replica_guid: [u8; 16],
    logical_to_actual: HashMap<u64, u64>,
    actual_to_logical: HashMap<u64, u64>,
    special_canonical_ids: HashSet<Uuid>,
}

impl MapiIdentityCodec {
    pub(crate) fn legacy_for_tests() -> Self {
        let mut logical_to_actual = HashMap::new();
        let mut actual_to_logical = HashMap::new();
        for logical_id in logical_special_folder_ids() {
            logical_to_actual.insert(logical_id, logical_id);
            actual_to_logical.insert(logical_id, logical_id);
        }
        Self {
            replica_guid: STORE_REPLICA_GUID,
            logical_to_actual,
            actual_to_logical,
            special_canonical_ids: HashSet::new(),
        }
    }

    pub(crate) fn from_special_folder_identity_records(
        replica_guid: Uuid,
        requests: &[MapiIdentityRequest],
        records: &[MapiIdentityRecord],
    ) -> Result<Self> {
        let mut logical_ids_by_canonical_id = HashMap::new();
        for request in requests {
            let Some(counter) = request.reserved_global_counter else {
                continue;
            };
            if !(ROOT_FOLDER_COUNTER..FIRST_DYNAMIC_GLOBAL_COUNTER).contains(&counter) {
                continue;
            }
            let logical_id = mapi_store_id(counter);
            if logical_ids_by_canonical_id
                .insert(request.canonical_id, logical_id)
                .is_some()
            {
                return Err(anyhow!(
                    "duplicate logical MAPI special-folder identity request for {}",
                    request.canonical_id
                ));
            }
        }
        let mut logical_to_actual = HashMap::new();
        let mut actual_to_logical = HashMap::new();
        let mut special_canonical_ids = HashSet::new();
        for record in records {
            if record.object_kind != MapiIdentityObjectKind::Mailbox {
                continue;
            }
            let Some(logical_id) = logical_ids_by_canonical_id
                .get(&record.canonical_id)
                .copied()
            else {
                continue;
            };
            let actual_counter =
                global_counter_from_store_id(record.object_id).ok_or_else(|| {
                    anyhow!(
                        "MAPI special-folder identity {} has an invalid object ID {:#018x}",
                        record.canonical_id,
                        record.object_id
                    )
                })?;
            if actual_counter < FIRST_DYNAMIC_GLOBAL_COUNTER {
                return Err(anyhow!(
                    "MAPI special-folder identity {} reused logical counter {}",
                    record.canonical_id,
                    actual_counter
                ));
            }
            if logical_to_actual
                .insert(logical_id, record.object_id)
                .is_some()
                || actual_to_logical
                    .insert(record.object_id, logical_id)
                    .is_some()
            {
                return Err(anyhow!(
                    "duplicate durable MAPI special-folder identity for {:#018x}",
                    logical_id
                ));
            }
            special_canonical_ids.insert(record.canonical_id);
        }
        for logical_id in logical_special_folder_ids() {
            if !logical_to_actual.contains_key(&logical_id) {
                return Err(anyhow!(
                    "MAPI special-folder identity is missing for logical folder {logical_id:#018x}"
                ));
            }
        }
        Ok(Self {
            replica_guid: *replica_guid.as_bytes(),
            logical_to_actual,
            actual_to_logical,
            special_canonical_ids,
        })
    }

    pub(crate) fn replica_guid(&self) -> [u8; 16] {
        self.replica_guid
    }

    pub(crate) fn is_special_canonical_id(&self, canonical_id: &Uuid) -> bool {
        self.special_canonical_ids.contains(canonical_id)
    }

    pub(crate) fn actual_object_id(&self, object_id: u64) -> Option<u64> {
        if is_logical_special_folder_id(object_id) {
            self.logical_to_actual.get(&object_id).copied()
        } else {
            Some(object_id)
        }
    }

    pub(crate) fn logical_object_id(&self, object_id: u64) -> Option<u64> {
        self.actual_to_logical
            .get(&object_id)
            .copied()
            .or_else(|| (!is_logical_special_folder_id(object_id)).then_some(object_id))
    }

    pub(crate) fn object_id_from_wire_id(&self, bytes: &[u8]) -> Option<u64> {
        self.logical_object_id(raw_object_id_from_wire_id(bytes)?)
    }

    pub(crate) fn object_id_from_trailing_replid_wire_id(&self, bytes: &[u8]) -> Option<u64> {
        self.logical_object_id(raw_object_id_from_trailing_replid_wire_id(bytes)?)
    }

    pub(crate) fn wire_id_bytes_from_object_id(&self, object_id: u64) -> Option<[u8; 8]> {
        raw_wire_id_bytes_from_object_id(self.actual_object_id(object_id)?)
    }

    pub(crate) fn source_key_for_object_id(&self, object_id: u64) -> Option<Vec<u8>> {
        let actual_object_id = self.actual_object_id(object_id)?;
        let global_counter = global_counter_from_store_id(actual_object_id)?;
        let mut key = self.replica_guid.to_vec();
        key.extend_from_slice(&globcnt_bytes(global_counter));
        Some(key)
    }

    pub(crate) fn object_id_from_source_key(&self, source_key: &[u8]) -> Option<u64> {
        if source_key.len() != 22 || source_key[..16] != self.replica_guid {
            return None;
        }
        let global_counter = global_counter_from_globcnt(source_key.get(16..22)?)?;
        (global_counter <= MAX_PERSISTED_GLOBAL_COUNTER)
            .then_some(mapi_store_id(global_counter))
            .and_then(|object_id| self.logical_object_id(object_id))
    }

    pub(crate) fn long_term_id_from_object_id(&self, object_id: u64) -> Option<[u8; 24]> {
        let actual_object_id = self.actual_object_id(object_id)?;
        let global_counter = global_counter_from_store_id(actual_object_id)?;
        let mut long_term_id = [0; 24];
        long_term_id[..16].copy_from_slice(&self.replica_guid);
        long_term_id[16..22].copy_from_slice(&globcnt_bytes(global_counter));
        Some(long_term_id)
    }

    pub(crate) fn object_id_from_long_term_id(&self, long_term_id: &[u8]) -> Option<u64> {
        if long_term_id.len() != 24
            || long_term_id[22..24] != [0, 0]
            || long_term_id[..16] != self.replica_guid
        {
            return None;
        }
        global_counter_from_globcnt(&long_term_id[16..22])
            .map(mapi_store_id)
            .and_then(|object_id| self.logical_object_id(object_id))
    }

    pub(crate) fn folder_entry_id_from_object_id(
        &self,
        mailbox_guid: Uuid,
        object_id: u64,
    ) -> Option<Vec<u8>> {
        self.folder_entry_id_with_provider(mailbox_guid.to_bytes_le(), object_id, 1)
    }

    pub(crate) fn outlook_message_list_settings_entry_id(
        &self,
        mailbox_guid: Uuid,
        object_id: u64,
    ) -> Option<Vec<u8>> {
        self.folder_entry_id_with_provider(mailbox_guid.to_bytes_le(), object_id, 0x000D)
    }

    pub(crate) fn public_folder_entry_id_from_object_id(&self, object_id: u64) -> Option<Vec<u8>> {
        self.folder_entry_id_with_provider(PUBLIC_FOLDER_PROVIDER_UID, object_id, 1)
    }

    fn folder_entry_id_with_provider(
        &self,
        provider_uid: [u8; 16],
        object_id: u64,
        entry_type: u16,
    ) -> Option<Vec<u8>> {
        let actual_object_id = self.actual_object_id(object_id)?;
        let global_counter = global_counter_from_store_id(actual_object_id)?;
        let mut entry_id = Vec::with_capacity(46);
        entry_id.extend_from_slice(&0u32.to_le_bytes());
        entry_id.extend_from_slice(&provider_uid);
        entry_id.extend_from_slice(&entry_type.to_le_bytes());
        entry_id.extend_from_slice(&self.replica_guid);
        entry_id.extend_from_slice(&globcnt_bytes(global_counter));
        entry_id.extend_from_slice(&0u16.to_le_bytes());
        Some(entry_id)
    }

    pub(crate) fn object_id_from_folder_entry_id(&self, entry_id: &[u8]) -> Option<u64> {
        if entry_id.len() != 46
            || entry_id[0..4] != [0, 0, 0, 0]
            || entry_id[20..22] != 1u16.to_le_bytes()
            || entry_id[22..38] != self.replica_guid
            || entry_id[44..46] != [0, 0]
        {
            return None;
        }
        global_counter_from_globcnt(&entry_id[38..44])
            .map(mapi_store_id)
            .and_then(|object_id| self.logical_object_id(object_id))
    }

    pub(crate) fn object_id_from_folder_identifier_bytes(&self, bytes: &[u8]) -> Option<u64> {
        self.object_id_from_folder_entry_id(bytes)
            .or_else(|| self.object_id_from_long_term_id(bytes))
    }

    pub(crate) fn message_entry_id_from_object_ids(
        &self,
        mailbox_guid: Uuid,
        folder_id: u64,
        message_id: u64,
    ) -> Option<Vec<u8>> {
        let folder_id = self.actual_object_id(folder_id)?;
        let message_id = self.actual_object_id(message_id)?;
        let folder_counter = global_counter_from_store_id(folder_id)?;
        let message_counter = global_counter_from_store_id(message_id)?;
        let mut entry_id = Vec::with_capacity(70);
        entry_id.extend_from_slice(&0u32.to_le_bytes());
        entry_id.extend_from_slice(&mailbox_guid.to_bytes_le());
        entry_id.extend_from_slice(&0x0007u16.to_le_bytes());
        entry_id.extend_from_slice(&self.replica_guid);
        entry_id.extend_from_slice(&globcnt_bytes(folder_counter));
        entry_id.extend_from_slice(&0u16.to_le_bytes());
        entry_id.extend_from_slice(&self.replica_guid);
        entry_id.extend_from_slice(&globcnt_bytes(message_counter));
        entry_id.extend_from_slice(&0u16.to_le_bytes());
        Some(entry_id)
    }

    pub(crate) fn object_ids_from_message_entry_id(
        &self,
        mailbox_guid: Uuid,
        entry_id: &[u8],
    ) -> Option<(u64, u64)> {
        if entry_id.len() != 70
            || entry_id[0..4] != [0, 0, 0, 0]
            || entry_id[4..20] != mailbox_guid.to_bytes_le()
            || entry_id[20..22] != 0x0007u16.to_le_bytes()
            || entry_id[22..38] != self.replica_guid
            || entry_id[44..46] != [0, 0]
            || entry_id[46..62] != self.replica_guid
            || entry_id[68..70] != [0, 0]
        {
            return None;
        }
        let folder_id = global_counter_from_globcnt(&entry_id[38..44])
            .filter(|counter| *counter <= MAX_PERSISTED_GLOBAL_COUNTER)
            .map(mapi_store_id)
            .and_then(|object_id| self.logical_object_id(object_id))?;
        let message_id = global_counter_from_globcnt(&entry_id[62..68])
            .filter(|counter| *counter <= MAX_PERSISTED_GLOBAL_COUNTER)
            .map(mapi_store_id)?;
        Some((folder_id, message_id))
    }

    pub(crate) fn change_key_for_change_number(&self, change_number: u64) -> Vec<u8> {
        let mut key = self.replica_guid.to_vec();
        key.extend_from_slice(&globcnt_bytes(change_number.max(1)));
        key
    }

    pub(crate) fn instance_key_for_object_id(&self, object_id: u64) -> Option<Vec<u8>> {
        self.source_key_for_object_id(object_id)
    }
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
pub(crate) const RECOVERABLE_ITEMS_ROOT_FOLDER_COUNTER: u64 = 38;
pub(crate) const RECOVERABLE_ITEMS_DELETIONS_FOLDER_COUNTER: u64 = 39;
pub(crate) const RECOVERABLE_ITEMS_VERSIONS_FOLDER_COUNTER: u64 = 40;
pub(crate) const RECOVERABLE_ITEMS_PURGES_FOLDER_COUNTER: u64 = 41;
pub(crate) const QUICK_STEP_SETTINGS_FOLDER_COUNTER: u64 = 42;
pub(crate) const PUBLIC_FOLDERS_ROOT_FOLDER_COUNTER: u64 = 0x7FFF_FFFF_FFFE;
pub(crate) const FIRST_DYNAMIC_GLOBAL_COUNTER: u64 = QUICK_STEP_SETTINGS_FOLDER_COUNTER + 1;

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
pub(crate) const RECOVERABLE_ITEMS_ROOT_FOLDER_ID: u64 =
    mapi_store_id(RECOVERABLE_ITEMS_ROOT_FOLDER_COUNTER);
pub(crate) const RECOVERABLE_ITEMS_DELETIONS_FOLDER_ID: u64 =
    mapi_store_id(RECOVERABLE_ITEMS_DELETIONS_FOLDER_COUNTER);
pub(crate) const RECOVERABLE_ITEMS_VERSIONS_FOLDER_ID: u64 =
    mapi_store_id(RECOVERABLE_ITEMS_VERSIONS_FOLDER_COUNTER);
pub(crate) const RECOVERABLE_ITEMS_PURGES_FOLDER_ID: u64 =
    mapi_store_id(RECOVERABLE_ITEMS_PURGES_FOLDER_COUNTER);
pub(crate) const QUICK_STEP_SETTINGS_FOLDER_ID: u64 =
    mapi_store_id(QUICK_STEP_SETTINGS_FOLDER_COUNTER);
pub(crate) const PUBLIC_FOLDERS_ROOT_FOLDER_ID: u64 =
    mapi_store_id(PUBLIC_FOLDERS_ROOT_FOLDER_COUNTER);
pub(crate) const CONVERSATION_MEMBERS_CONTENTS_TABLE_ID: u64 =
    mapi_store_id(FIRST_RESERVED_HIGH_GLOBAL_COUNTER + 0x80);

pub(crate) fn logical_special_folder_ids() -> impl Iterator<Item = u64> {
    (ROOT_FOLDER_COUNTER..FIRST_DYNAMIC_GLOBAL_COUNTER).map(mapi_store_id)
}

pub(crate) fn is_logical_special_folder_id(object_id: u64) -> bool {
    logical_special_folder_ids().any(|logical_id| logical_id == object_id)
}

pub(crate) const fn mapi_store_id(global_counter: u64) -> u64 {
    ((global_counter & 0x0000_FFFF_FFFF_FFFF) << 16) | STORE_REPLICA_ID
}

pub(crate) fn mailbox_store_object_entry_id(server_shortname: &str, mailbox_dn: &str) -> Vec<u8> {
    let server_shortname = server_shortname.trim();
    let mailbox_dn = mailbox_dn.trim();
    let mut entry_id = Vec::with_capacity(
        62usize
            .saturating_add(server_shortname.len())
            .saturating_add(mailbox_dn.len()),
    );
    entry_id.extend_from_slice(&0u32.to_le_bytes());
    entry_id.extend_from_slice(&STORE_OBJECT_PROVIDER_UID);
    entry_id.push(0);
    entry_id.push(0);
    entry_id.extend_from_slice(b"EMSMDB.DLL\0\0\0\0");
    entry_id.extend_from_slice(&0u32.to_le_bytes());
    entry_id.extend_from_slice(&MAILBOX_STORE_PROVIDER_UID);
    entry_id.extend_from_slice(&0x0000_000Cu32.to_le_bytes());
    entry_id.extend_from_slice(server_shortname.as_bytes());
    entry_id.push(0);
    entry_id.extend_from_slice(mailbox_dn.as_bytes());
    entry_id.push(0);
    entry_id
}

pub(crate) fn principal_mailbox_store_entry_id(principal: &AccountPrincipal) -> Vec<u8> {
    let entry = super::nspi::principal_address_book_entry(principal);
    let mailbox_dn = super::nspi::nspi_entry_unprefixed_legacy_dn(&entry);
    mailbox_store_object_entry_id(&principal.email, &mailbox_dn)
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

fn raw_object_id_from_wire_id(bytes: &[u8]) -> Option<u64> {
    if bytes.len() != 8 {
        return None;
    }
    let replica_id = u16::from_le_bytes(bytes[..2].try_into().ok()?);
    if u64::from(replica_id) != STORE_REPLICA_ID {
        return None;
    }
    global_counter_from_globcnt(&bytes[2..8]).map(mapi_store_id)
}

fn raw_object_id_from_trailing_replid_wire_id(bytes: &[u8]) -> Option<u64> {
    if bytes.len() != 8 {
        return None;
    }
    let replica_id = u16::from_le_bytes(bytes[6..8].try_into().ok()?);
    if u64::from(replica_id) != STORE_REPLICA_ID {
        return None;
    }
    global_counter_from_globcnt(&bytes[..6]).map(mapi_store_id)
}

fn raw_wire_id_bytes_from_object_id(object_id: u64) -> Option<[u8; 8]> {
    let global_counter = global_counter_from_store_id(object_id)?;
    let mut bytes = [0; 8];
    bytes[..2].copy_from_slice(&(STORE_REPLICA_ID as u16).to_le_bytes());
    bytes[2..8].copy_from_slice(&globcnt_bytes(global_counter));
    Some(bytes)
}

pub(crate) fn object_id_from_wire_id(bytes: &[u8]) -> Option<u64> {
    current_mapi_identity_codec(|codec| codec.object_id_from_wire_id(bytes))
        .unwrap_or_else(|| raw_object_id_from_wire_id(bytes))
}

pub(crate) fn object_id_from_trailing_replid_wire_id(bytes: &[u8]) -> Option<u64> {
    current_mapi_identity_codec(|codec| codec.object_id_from_trailing_replid_wire_id(bytes))
        .unwrap_or_else(|| raw_object_id_from_trailing_replid_wire_id(bytes))
}

pub(crate) fn wire_id_bytes_from_object_id(object_id: u64) -> Option<[u8; 8]> {
    current_mapi_identity_codec(|codec| codec.wire_id_bytes_from_object_id(object_id))
        .unwrap_or_else(|| raw_wire_id_bytes_from_object_id(object_id))
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

pub(crate) fn forget_mapi_identity(canonical_id: &Uuid) {
    MAPI_OBJECT_IDS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(canonical_id);
}

pub(crate) fn mapped_mapi_object_id(canonical_id: &Uuid) -> Option<u64> {
    MAPI_OBJECT_IDS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(canonical_id)
        .map(|identity| identity.object_id)
}

pub(crate) fn object_id_matches(canonical_id: &Uuid, object_id: u64) -> bool {
    mapped_mapi_object_id(canonical_id) == Some(object_id)
        || legacy_migration_object_id(canonical_id) == object_id
}

pub(crate) fn mapped_mapi_source_key(canonical_id: &Uuid) -> Option<Vec<u8>> {
    MAPI_OBJECT_IDS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(canonical_id)
        .and_then(|identity| identity.source_key.clone())
}

fn raw_long_term_id_from_object_id(object_id: u64) -> Option<[u8; 24]> {
    let global_counter = global_counter_from_store_id(object_id)?;
    let mut long_term_id = [0; 24];
    long_term_id[..16].copy_from_slice(&STORE_REPLICA_GUID);
    long_term_id[16..22].copy_from_slice(&globcnt_bytes(global_counter));
    Some(long_term_id)
}

fn raw_object_id_from_long_term_id(long_term_id: &[u8]) -> Option<u64> {
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

fn raw_folder_entry_id_from_object_id(mailbox_guid: Uuid, object_id: u64) -> Option<Vec<u8>> {
    folder_entry_id_with_provider(mailbox_guid.to_bytes_le(), object_id)
}

fn raw_outlook_message_list_settings_entry_id(
    mailbox_guid: Uuid,
    object_id: u64,
) -> Option<Vec<u8>> {
    folder_entry_id_with_provider_and_type(mailbox_guid.to_bytes_le(), object_id, 0x000D)
}

fn raw_public_folder_entry_id_from_object_id(object_id: u64) -> Option<Vec<u8>> {
    folder_entry_id_with_provider(PUBLIC_FOLDER_PROVIDER_UID, object_id)
}

fn folder_entry_id_with_provider(provider_uid: [u8; 16], object_id: u64) -> Option<Vec<u8>> {
    folder_entry_id_with_provider_and_type(provider_uid, object_id, 1)
}

fn folder_entry_id_with_provider_and_type(
    provider_uid: [u8; 16],
    object_id: u64,
    entry_type: u16,
) -> Option<Vec<u8>> {
    let global_counter = global_counter_from_store_id(object_id)?;
    let mut entry_id = Vec::with_capacity(46);
    entry_id.extend_from_slice(&0u32.to_le_bytes());
    entry_id.extend_from_slice(&provider_uid);
    entry_id.extend_from_slice(&entry_type.to_le_bytes());
    entry_id.extend_from_slice(&STORE_REPLICA_GUID);
    entry_id.extend_from_slice(&globcnt_bytes(global_counter));
    entry_id.extend_from_slice(&0u16.to_le_bytes());
    Some(entry_id)
}

fn raw_object_id_from_folder_entry_id(entry_id: &[u8]) -> Option<u64> {
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

fn raw_object_id_from_folder_identifier_bytes(bytes: &[u8]) -> Option<u64> {
    raw_object_id_from_folder_entry_id(bytes)
        .or_else(|| raw_object_id_from_long_term_id(bytes))
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
            | QUICK_STEP_SETTINGS_FOLDER_ID
            | SUGGESTED_CONTACTS_FOLDER_ID
            | CONTACTS_SEARCH_FOLDER_ID
            | DOCUMENT_LIBRARIES_FOLDER_ID
            | IM_CONTACT_LIST_FOLDER_ID
            | QUICK_CONTACTS_FOLDER_ID
            | SYNC_ISSUES_FOLDER_ID
            | CONFLICTS_FOLDER_ID
            | LOCAL_FAILURES_FOLDER_ID
            | SERVER_FAILURES_FOLDER_ID
            | RECOVERABLE_ITEMS_ROOT_FOLDER_ID
            | RECOVERABLE_ITEMS_DELETIONS_FOLDER_ID
            | RECOVERABLE_ITEMS_VERSIONS_FOLDER_ID
            | RECOVERABLE_ITEMS_PURGES_FOLDER_ID
            | PUBLIC_FOLDERS_ROOT_FOLDER_ID
    )
}

fn raw_message_entry_id_from_object_ids(
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

fn raw_object_ids_from_message_entry_id(mailbox_guid: Uuid, entry_id: &[u8]) -> Option<(u64, u64)> {
    if entry_id.len() != 70
        || entry_id[0..4] != [0, 0, 0, 0]
        || entry_id[4..20] != mailbox_guid.to_bytes_le()
        || entry_id[20..22] != 0x0007u16.to_le_bytes()
        || entry_id[22..38] != STORE_REPLICA_GUID
        || entry_id[44..46] != [0, 0]
        || entry_id[46..62] != STORE_REPLICA_GUID
        || entry_id[68..70] != [0, 0]
    {
        return None;
    }
    let folder_id = global_counter_from_globcnt(&entry_id[38..44])
        .filter(|counter| *counter <= MAX_PERSISTED_GLOBAL_COUNTER)
        .map(mapi_store_id)?;
    let message_id = global_counter_from_globcnt(&entry_id[62..68])
        .filter(|counter| *counter <= MAX_PERSISTED_GLOBAL_COUNTER)
        .map(mapi_store_id)?;
    Some((folder_id, message_id))
}

fn raw_source_key_for_object_id(object_id: u64) -> Vec<u8> {
    let mut key = STORE_REPLICA_GUID.to_vec();
    let global_counter = global_counter_from_store_id(object_id)
        .expect("source keys require a MAPI object id with the store replica id");
    key.extend_from_slice(&globcnt_bytes(global_counter));
    key
}

pub(crate) fn generated_message_search_key(canonical_id: &Uuid) -> Vec<u8> {
    // [MS-OXCPRPT] section 2.2.1.9 defines the stable, unique Message
    // search identity. Microsoft MAPI represents PidTagSearchKey on Messages
    // as a 16-byte MAPIUID, not as the 22-byte SourceKey XID.
    canonical_id.as_bytes().to_vec()
}

#[allow(dead_code)]
fn raw_object_id_from_source_key(source_key: &[u8]) -> Option<u64> {
    if source_key.len() != 22 || source_key[..16] != STORE_REPLICA_GUID {
        return None;
    }
    let global_counter = global_counter_from_globcnt(source_key.get(16..22)?)?;
    if global_counter > MAX_PERSISTED_GLOBAL_COUNTER {
        return None;
    }
    Some(mapi_store_id(global_counter))
}

fn raw_change_key_for_change_number(change_number: u64) -> Vec<u8> {
    let mut key = STORE_REPLICA_GUID.to_vec();
    key.extend_from_slice(&globcnt_bytes(change_number.max(1)));
    key
}

fn raw_instance_key_for_object_id(object_id: u64) -> Vec<u8> {
    raw_source_key_for_object_id(object_id)
}

pub(crate) fn long_term_id_from_object_id(object_id: u64) -> Option<[u8; 24]> {
    current_mapi_identity_codec(|codec| codec.long_term_id_from_object_id(object_id))
        .unwrap_or_else(|| raw_long_term_id_from_object_id(object_id))
}

pub(crate) fn object_id_from_long_term_id(long_term_id: &[u8]) -> Option<u64> {
    current_mapi_identity_codec(|codec| codec.object_id_from_long_term_id(long_term_id))
        .unwrap_or_else(|| raw_object_id_from_long_term_id(long_term_id))
}

pub(crate) fn folder_entry_id_from_object_id(
    mailbox_guid: Uuid,
    object_id: u64,
) -> Option<Vec<u8>> {
    current_mapi_identity_codec(|codec| {
        codec.folder_entry_id_from_object_id(mailbox_guid, object_id)
    })
    .unwrap_or_else(|| raw_folder_entry_id_from_object_id(mailbox_guid, object_id))
}

pub(crate) fn outlook_message_list_settings_entry_id(
    mailbox_guid: Uuid,
    object_id: u64,
) -> Option<Vec<u8>> {
    current_mapi_identity_codec(|codec| {
        codec.outlook_message_list_settings_entry_id(mailbox_guid, object_id)
    })
    .unwrap_or_else(|| raw_outlook_message_list_settings_entry_id(mailbox_guid, object_id))
}

pub(crate) fn public_folder_entry_id_from_object_id(object_id: u64) -> Option<Vec<u8>> {
    current_mapi_identity_codec(|codec| codec.public_folder_entry_id_from_object_id(object_id))
        .unwrap_or_else(|| raw_public_folder_entry_id_from_object_id(object_id))
}

pub(crate) fn object_id_from_folder_entry_id(entry_id: &[u8]) -> Option<u64> {
    current_mapi_identity_codec(|codec| codec.object_id_from_folder_entry_id(entry_id))
        .unwrap_or_else(|| raw_object_id_from_folder_entry_id(entry_id))
}

pub(crate) fn object_id_from_folder_identifier_bytes(bytes: &[u8]) -> Option<u64> {
    current_mapi_identity_codec(|codec| codec.object_id_from_folder_identifier_bytes(bytes))
        .unwrap_or_else(|| raw_object_id_from_folder_identifier_bytes(bytes))
}

pub(crate) fn message_entry_id_from_object_ids(
    mailbox_guid: Uuid,
    folder_id: u64,
    message_id: u64,
) -> Option<Vec<u8>> {
    current_mapi_identity_codec(|codec| {
        codec.message_entry_id_from_object_ids(mailbox_guid, folder_id, message_id)
    })
    .unwrap_or_else(|| raw_message_entry_id_from_object_ids(mailbox_guid, folder_id, message_id))
}

pub(crate) fn object_ids_from_message_entry_id(
    mailbox_guid: Uuid,
    entry_id: &[u8],
) -> Option<(u64, u64)> {
    current_mapi_identity_codec(|codec| {
        codec.object_ids_from_message_entry_id(mailbox_guid, entry_id)
    })
    .unwrap_or_else(|| raw_object_ids_from_message_entry_id(mailbox_guid, entry_id))
}

pub(crate) fn source_key_for_object_id(object_id: u64) -> Vec<u8> {
    current_mapi_identity_codec(|codec| {
        codec
            .source_key_for_object_id(object_id)
            .expect("source keys require a scoped MAPI object identity")
    })
    .unwrap_or_else(|| raw_source_key_for_object_id(object_id))
}

#[allow(dead_code)]
pub(crate) fn object_id_from_source_key(source_key: &[u8]) -> Option<u64> {
    current_mapi_identity_codec(|codec| codec.object_id_from_source_key(source_key))
        .unwrap_or_else(|| raw_object_id_from_source_key(source_key))
}

pub(crate) fn change_key_for_change_number(change_number: u64) -> Vec<u8> {
    current_mapi_identity_codec(|codec| codec.change_key_for_change_number(change_number))
        .unwrap_or_else(|| raw_change_key_for_change_number(change_number))
}

pub(crate) fn instance_key_for_object_id(object_id: u64) -> Vec<u8> {
    current_mapi_identity_codec(|codec| {
        codec
            .instance_key_for_object_id(object_id)
            .expect("instance keys require a scoped MAPI object identity")
    })
    .unwrap_or_else(|| raw_instance_key_for_object_id(object_id))
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

    #[tokio::test]
    async fn scoped_codec_maps_logical_default_folder_ids_to_durable_ids() {
        let replica_guid = Uuid::from_u128(0x11223344_5566_7788_99aa_bbccddeeff00);
        let mut requests = Vec::new();
        let mut records = Vec::new();
        for counter in ROOT_FOLDER_COUNTER..FIRST_DYNAMIC_GLOBAL_COUNTER {
            let canonical_id = Uuid::from_u128(counter as u128 + 1);
            let object_id = mapi_store_id(FIRST_DYNAMIC_GLOBAL_COUNTER + counter - 1);
            requests.push(MapiIdentityRequest {
                object_kind: MapiIdentityObjectKind::Mailbox,
                canonical_id,
                reserved_global_counter: Some(counter),
                source_key: None,
            });
            records.push(MapiIdentityRecord {
                object_kind: MapiIdentityObjectKind::Mailbox,
                canonical_id,
                object_id,
                change_number: 1,
                source_key: Vec::new(),
                change_key: Vec::new(),
                predecessor_change_list: Vec::new(),
                last_modification_time: 0,
            });
        }

        let codec = MapiIdentityCodec::from_special_folder_identity_records(
            replica_guid,
            &requests,
            &records,
        )
        .unwrap();
        let inbox_actual = mapi_store_id(FIRST_DYNAMIC_GLOBAL_COUNTER + INBOX_FOLDER_COUNTER - 1);
        assert_eq!(codec.actual_object_id(INBOX_FOLDER_ID), Some(inbox_actual));
        assert_eq!(codec.logical_object_id(inbox_actual), Some(INBOX_FOLDER_ID));

        let (
            wire_id,
            decoded_wire_id,
            source_key,
            decoded_source_key,
            long_term_id,
            decoded_long_term_id,
            folder_entry_id,
            new_mail_folder_id,
        ) = with_current_mapi_identity_codec(codec.clone(), async {
            let wire_id = wire_id_bytes_from_object_id(INBOX_FOLDER_ID).unwrap();
            let source_key = source_key_for_object_id(INBOX_FOLDER_ID);
            let long_term_id = long_term_id_from_object_id(INBOX_FOLDER_ID).unwrap();
            let folder_entry_id =
                folder_entry_id_from_object_id(Uuid::nil(), INBOX_FOLDER_ID).unwrap();
            let new_mail = crate::mapi::notifications::MapiNotificationEvent::canonical(
                crate::mapi::notifications::MapiNotificationKind::Content,
                crate::mapi::wire::MapiNotificationEventMask::NewMail.as_u16(),
                INBOX_FOLDER_ID,
                Some(mapi_store_id(FIRST_DYNAMIC_GLOBAL_COUNTER + 0x100)),
                None,
                1,
                1,
                None,
                None,
                "created".to_string(),
                None,
                None,
                None,
                Some("IPM.Note".to_string()),
            );
            let new_mail_response =
                crate::mapi::notifications::rop_notify_response(&codec, 0x1a, 0, &new_mail)
                    .expect("complete NewMail notification serializes");
            (
                wire_id,
                object_id_from_wire_id(&wire_id),
                source_key.clone(),
                object_id_from_source_key(&source_key),
                long_term_id,
                object_id_from_long_term_id(&long_term_id),
                folder_entry_id,
                new_mail_response[8..16].to_vec(),
            )
        })
        .await;

        assert_eq!(
            wire_id,
            raw_wire_id_bytes_from_object_id(inbox_actual).unwrap()
        );
        assert_eq!(
            new_mail_folder_id,
            raw_wire_id_bytes_from_object_id(inbox_actual).unwrap()
        );
        assert_eq!(decoded_wire_id, Some(INBOX_FOLDER_ID));
        assert_eq!(&source_key[..16], replica_guid.as_bytes());
        assert_eq!(
            &source_key[16..22],
            &globcnt_bytes(FIRST_DYNAMIC_GLOBAL_COUNTER + INBOX_FOLDER_COUNTER - 1)
        );
        assert_eq!(decoded_source_key, Some(INBOX_FOLDER_ID));
        assert_eq!(&long_term_id[..16], replica_guid.as_bytes());
        assert_eq!(decoded_long_term_id, Some(INBOX_FOLDER_ID));
        assert_eq!(&folder_entry_id[22..38], replica_guid.as_bytes());
        assert_eq!(
            codec.object_id_from_folder_entry_id(&folder_entry_id),
            Some(INBOX_FOLDER_ID)
        );

        let public_folder_entry_id = codec
            .public_folder_entry_id_from_object_id(PUBLIC_FOLDERS_ROOT_FOLDER_ID)
            .unwrap();
        assert_eq!(&public_folder_entry_id[22..38], replica_guid.as_bytes());
        assert_eq!(
            codec.object_id_from_folder_entry_id(&public_folder_entry_id),
            Some(PUBLIC_FOLDERS_ROOT_FOLDER_ID)
        );
    }

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
    fn message_list_settings_entry_id_matches_exchange_private_shape() {
        let mailbox_guid = Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap();
        let entry_id =
            outlook_message_list_settings_entry_id(mailbox_guid, INBOX_FOLDER_ID).unwrap();

        assert_eq!(entry_id.len(), 46);
        assert_eq!(&entry_id[..4], &0u32.to_le_bytes());
        assert_eq!(&entry_id[4..20], &mailbox_guid.to_bytes_le());
        assert_eq!(&entry_id[20..22], &0x000D_u16.to_le_bytes());
        assert_eq!(&entry_id[22..38], &STORE_REPLICA_GUID);
        assert_eq!(&entry_id[38..44], &globcnt_bytes(INBOX_FOLDER_COUNTER));
        assert_eq!(&entry_id[44..46], &0u16.to_le_bytes());
        assert_eq!(object_id_from_folder_entry_id(&entry_id), None);
    }

    #[test]
    fn public_folder_entry_id_uses_public_store_provider_uid() {
        let entry_id =
            public_folder_entry_id_from_object_id(PUBLIC_FOLDERS_ROOT_FOLDER_ID).unwrap();

        assert_eq!(entry_id.len(), 46);
        assert_eq!(&entry_id[4..20], &PUBLIC_FOLDER_PROVIDER_UID);
        assert_eq!(&entry_id[22..38], &STORE_REPLICA_GUID);
        assert_eq!(
            object_id_from_folder_entry_id(&entry_id),
            Some(PUBLIC_FOLDERS_ROOT_FOLDER_ID)
        );
    }

    #[test]
    fn mailbox_store_object_entry_id_matches_outlook_wlink_shape() {
        let mailbox_dn = "/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn=test-l-p-e-ch";
        let entry_id = mailbox_store_object_entry_id("test@l-p-e.ch", mailbox_dn);

        assert_eq!(entry_id.len(), 145);
        assert_eq!(&entry_id[..4], &0u32.to_le_bytes());
        assert_eq!(&entry_id[4..20], &STORE_OBJECT_PROVIDER_UID);
        assert_eq!(&entry_id[20..22], &[0, 0]);
        assert_eq!(&entry_id[22..36], b"EMSMDB.DLL\0\0\0\0");
        assert_eq!(&entry_id[36..40], &0u32.to_le_bytes());
        assert_eq!(&entry_id[40..56], &MAILBOX_STORE_PROVIDER_UID);
        assert_eq!(&entry_id[56..60], &0x0000_000Cu32.to_le_bytes());
        assert_eq!(&entry_id[60..74], b"test@l-p-e.ch\0");
        assert_eq!(&entry_id[74..144], mailbox_dn.as_bytes());
        assert_eq!(entry_id[144], 0);
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
    fn stale_cached_conversation_history_identifier_is_not_advertised() {
        let mailbox_guid = Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap();
        let mut entry_id =
            folder_entry_id_from_object_id(mailbox_guid, CONVERSATION_HISTORY_FOLDER_ID).unwrap();
        entry_id[22..38].copy_from_slice(&[0xA5; 16]);
        assert_eq!(object_id_from_folder_identifier_bytes(&entry_id), None);

        let mut long_term_id = long_term_id_from_object_id(CONVERSATION_HISTORY_FOLDER_ID).unwrap();
        long_term_id[..16].copy_from_slice(&[0xA5; 16]);
        assert_eq!(object_id_from_folder_identifier_bytes(&long_term_id), None);
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
            QUICK_STEP_SETTINGS_FOLDER_COUNTER + 1
        );
        assert!(FIRST_DYNAMIC_GLOBAL_COUNTER > QUICK_STEP_SETTINGS_FOLDER_COUNTER);
        assert!(FIRST_DYNAMIC_GLOBAL_COUNTER > RECOVERABLE_ITEMS_PURGES_FOLDER_COUNTER);
    }

    #[test]
    fn forgotten_mapi_identity_is_not_mapped() {
        let canonical_id = Uuid::parse_str("aaaaaaaa-9999-4999-8999-aaaaaaaaaaaa").unwrap();
        let object_id = mapi_store_id(FIRST_DYNAMIC_GLOBAL_COUNTER + 90);
        remember_mapi_identity(canonical_id, object_id);

        assert_eq!(mapped_mapi_object_id(&canonical_id), Some(object_id));

        forget_mapi_identity(&canonical_id);

        assert_eq!(mapped_mapi_object_id(&canonical_id), None);
    }

    #[test]
    #[should_panic(expected = "source keys require a MAPI object id with the store replica id")]
    fn source_key_rejects_non_mapi_object_id_instead_of_emitting_guid_only_xid() {
        let _ = source_key_for_object_id(42);
    }
}
