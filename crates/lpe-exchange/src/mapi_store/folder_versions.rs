use super::*;
use crate::mapi_mailstore;

#[derive(Debug, Clone, Default)]
pub(super) struct MapiFolderVersions {
    versions: HashMap<u64, MapiFolderVersion>,
}

impl MapiFolderVersions {
    pub(super) fn from_identity_records(records: &[MapiIdentityRecord]) -> Self {
        Self {
            versions: records
                .iter()
                .filter(|record| record.object_kind == MapiIdentityObjectKind::Mailbox)
                .map(|record| {
                    (
                        record.object_id,
                        MapiFolderVersion {
                            folder_id: record.object_id,
                            change_number: record.change_number,
                            change_key: record.change_key.clone(),
                            predecessor_change_list: record.predecessor_change_list.clone(),
                            last_modification_time: record.last_modification_time,
                        },
                    )
                })
                .collect(),
        }
    }

    pub(super) fn version(&self, folder_id: u64) -> Option<&MapiFolderVersion> {
        self.versions.get(&folder_id)
    }

    pub(super) fn all(&self) -> Vec<MapiFolderVersion> {
        self.versions.values().cloned().collect()
    }

    pub(super) fn upsert(&mut self, version: MapiFolderVersion) {
        self.versions.insert(version.folder_id, version);
    }

    pub(super) fn change_number(&self, folder_id: u64) -> Option<u64> {
        self.version(folder_id).map(|version| version.change_number)
    }
}

pub(crate) fn mapi_folder_identity_requests(mailboxes: &[JmapMailbox]) -> Vec<MapiIdentityRequest> {
    let mut reserved_counters = HashSet::new();
    let mut requests = mailboxes
        .iter()
        .filter(|mailbox| !is_virtual_special_mailbox(mailbox))
        .map(|mailbox| {
            let reserved_global_counter = reserved_folder_counter_for_role(&mailbox.role);
            if let Some(counter) = reserved_global_counter {
                reserved_counters.insert(counter);
            }
            MapiIdentityRequest {
                object_kind: MapiIdentityObjectKind::Mailbox,
                canonical_id: mailbox.id,
                reserved_global_counter,
                source_key: None,
            }
        })
        .collect::<Vec<_>>();

    // [MS-OXCFXICS] section 3.1.5.3: every server object and every server
    // change consume separate counters. Persist one version identity for each
    // reserved private-store folder that has no canonical mailbox row.
    for counter in crate::mapi::identity::ROOT_FOLDER_COUNTER
        ..crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER
    {
        if !reserved_counters.insert(counter) {
            continue;
        }
        let folder_id = crate::mapi::identity::mapi_store_id(counter);
        requests.push(MapiIdentityRequest {
            object_kind: MapiIdentityObjectKind::Mailbox,
            canonical_id: mapi_mailstore::virtual_special_mailbox_id(folder_id),
            reserved_global_counter: Some(counter),
            source_key: None,
        });
    }
    requests
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_mailbox_owns_reserved_fid_and_virtual_folders_fill_the_gaps() {
        let inbox = JmapMailbox {
            id: Uuid::parse_str("55555555-5555-4555-8555-555555555555").unwrap(),
            parent_id: None,
            role: "inbox".to_string(),
            name: "Inbox".to_string(),
            sort_order: 10,
            modseq: 1,
            total_emails: 0,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        };

        let requests = mapi_folder_identity_requests(&[inbox.clone()]);
        let inbox_requests = requests
            .iter()
            .filter(|request| {
                request.reserved_global_counter == Some(crate::mapi::identity::INBOX_FOLDER_COUNTER)
            })
            .collect::<Vec<_>>();
        assert_eq!(inbox_requests.len(), 1);
        assert_eq!(inbox_requests[0].canonical_id, inbox.id);
        assert!(requests.iter().any(|request| {
            request.reserved_global_counter == Some(crate::mapi::identity::CONTACTS_FOLDER_COUNTER)
                && request.canonical_id
                    == mapi_mailstore::virtual_special_mailbox_id(
                        crate::mapi::identity::CONTACTS_FOLDER_ID,
                    )
        }));
    }
}
