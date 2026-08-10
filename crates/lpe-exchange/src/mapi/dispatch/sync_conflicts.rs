use std::collections::BTreeMap;

use anyhow::{anyhow, bail, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SyncImportVersionRelation {
    Newer,
    OlderOrSame,
    Conflict,
}

pub(super) fn sync_import_version_relation(
    incoming_pcl: &[u8],
    current_pcl: &[u8],
) -> Result<SyncImportVersionRelation> {
    // [MS-OXCFXICS] section 3.1.5.6.1: PCL inclusion determines newer,
    // older/equal, and conflicting ICS-upload versions.
    let incoming = parse_predecessor_change_list(incoming_pcl)?;
    let current = parse_predecessor_change_list(current_pcl)?;
    let incoming_includes_current = predecessor_map_includes(&incoming, &current)?;
    let current_includes_incoming = predecessor_map_includes(&current, &incoming)?;
    Ok(if incoming_includes_current && !current_includes_incoming {
        SyncImportVersionRelation::Newer
    } else if current_includes_incoming {
        SyncImportVersionRelation::OlderOrSame
    } else {
        SyncImportVersionRelation::Conflict
    })
}

pub(super) fn merge_sync_predecessor_change_lists(first: &[u8], second: &[u8]) -> Result<Vec<u8>> {
    // [MS-OXCFXICS] sections 2.2.2.3 and 3.1.5.6.2: a resolved
    // version succeeds both inputs and serializes one greatest XID per GUID.
    let mut merged = parse_predecessor_change_list(first)?;
    for (guid, local_id) in parse_predecessor_change_list(second)? {
        match merged.get(&guid) {
            Some(existing) if existing.len() != local_id.len() => {
                bail!("MAPI PCL LocalIds for one replica have inconsistent lengths")
            }
            Some(existing) if existing >= &local_id => {}
            _ => {
                merged.insert(guid, local_id);
            }
        }
    }
    serialize_predecessor_change_list(&merged)
}

pub(super) fn imported_version_wins_last_writer(
    incoming_last_modification_time: u64,
    incoming_change_key: &[u8],
    current_last_modification_time: u64,
    current_change_key: &[u8],
) -> Result<bool> {
    // [MS-OXCFXICS] section 3.1.5.6.2.2: compare modification time,
    // then the ChangeKey NamespaceGuid; equal GUIDs favor the import.
    let (incoming_guid, _) = split_xid(incoming_change_key)?;
    let (current_guid, _) = split_xid(current_change_key)?;
    Ok(
        match incoming_last_modification_time.cmp(&current_last_modification_time) {
            std::cmp::Ordering::Greater => true,
            std::cmp::Ordering::Less => false,
            std::cmp::Ordering::Equal => incoming_guid >= current_guid,
        },
    )
}

fn predecessor_map_includes(
    candidate: &BTreeMap<[u8; 16], Vec<u8>>,
    predecessor: &BTreeMap<[u8; 16], Vec<u8>>,
) -> Result<bool> {
    for (guid, predecessor_local_id) in predecessor {
        let Some(candidate_local_id) = candidate.get(guid) else {
            return Ok(false);
        };
        if candidate_local_id.len() != predecessor_local_id.len() {
            bail!("MAPI PCL LocalIds for one replica have inconsistent lengths");
        }
        if candidate_local_id < predecessor_local_id {
            return Ok(false);
        }
    }
    Ok(true)
}

fn parse_predecessor_change_list(bytes: &[u8]) -> Result<BTreeMap<[u8; 16], Vec<u8>>> {
    let mut entries = BTreeMap::new();
    let mut offset = 0usize;
    let mut previous_guid = None;
    while offset < bytes.len() {
        let size = usize::from(
            *bytes
                .get(offset)
                .ok_or_else(|| anyhow!("truncated MAPI PCL SizedXid"))?,
        );
        offset += 1;
        let end = offset
            .checked_add(size)
            .ok_or_else(|| anyhow!("MAPI PCL SizedXid length overflow"))?;
        let xid = bytes
            .get(offset..end)
            .ok_or_else(|| anyhow!("truncated MAPI PCL XID"))?;
        offset = end;
        let (guid, local_id) = split_xid(xid)?;
        if previous_guid.is_some_and(|previous| previous >= guid) {
            bail!("MAPI PCL XIDs are not strictly sorted by replica GUID");
        }
        previous_guid = Some(guid);
        entries.insert(guid, local_id.to_vec());
    }
    if entries.is_empty() {
        bail!("MAPI PCL must contain at least one SizedXid");
    }
    Ok(entries)
}

fn split_xid(bytes: &[u8]) -> Result<([u8; 16], &[u8])> {
    if !(17..=24).contains(&bytes.len()) {
        bail!("MAPI XID length must be between 17 and 24 bytes");
    }
    let guid = bytes[..16]
        .try_into()
        .map_err(|_| anyhow!("MAPI XID replica GUID is malformed"))?;
    Ok((guid, &bytes[16..]))
}

fn serialize_predecessor_change_list(entries: &BTreeMap<[u8; 16], Vec<u8>>) -> Result<Vec<u8>> {
    let mut serialized = Vec::new();
    for (guid, local_id) in entries {
        let size = 16usize
            .checked_add(local_id.len())
            .ok_or_else(|| anyhow!("MAPI PCL SizedXid length overflow"))?;
        serialized.push(u8::try_from(size).map_err(|_| anyhow!("MAPI PCL XID is too large"))?);
        serialized.extend_from_slice(guid);
        serialized.extend_from_slice(local_id);
    }
    Ok(serialized)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn xid(replica_byte: u8, counter: u64) -> Vec<u8> {
        let mut xid = vec![replica_byte; 16];
        xid.extend_from_slice(&counter.to_be_bytes()[2..]);
        xid
    }

    fn pcl(xids: &[&[u8]]) -> Vec<u8> {
        let mut pcl = Vec::new();
        for xid in xids {
            pcl.push(xid.len() as u8);
            pcl.extend_from_slice(xid);
        }
        pcl
    }

    #[test]
    fn microsoft_oxcfxics_3_1_5_6_1_classifies_pcl_relations() {
        let older = xid(0x31, 10);
        let newer = xid(0x31, 11);
        let other_replica = xid(0x42, 1);

        assert_eq!(
            sync_import_version_relation(&pcl(&[&newer]), &pcl(&[&older])).unwrap(),
            SyncImportVersionRelation::Newer
        );
        assert_eq!(
            sync_import_version_relation(&pcl(&[&older]), &pcl(&[&newer])).unwrap(),
            SyncImportVersionRelation::OlderOrSame
        );
        assert_eq!(
            sync_import_version_relation(&pcl(&[&other_replica]), &pcl(&[&newer])).unwrap(),
            SyncImportVersionRelation::Conflict
        );
    }

    #[test]
    fn microsoft_oxcfxics_3_1_5_6_2_merges_conflicting_predecessors() {
        let first = xid(0x11, 5);
        let second = xid(0x22, 7);
        assert_eq!(
            merge_sync_predecessor_change_lists(&pcl(&[&first]), &pcl(&[&second])).unwrap(),
            pcl(&[&first, &second])
        );
    }

    #[test]
    fn microsoft_oxcfxics_3_1_5_6_2_2_applies_last_writer_wins() {
        let server = xid(0x11, 5);
        let client = xid(0x22, 7);
        assert!(imported_version_wins_last_writer(11, &client, 10, &server).unwrap());
        assert!(!imported_version_wins_last_writer(9, &client, 10, &server).unwrap());
        assert!(imported_version_wins_last_writer(10, &client, 10, &server).unwrap());
    }
}
