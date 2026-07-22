use std::collections::BTreeMap;

use super::*;

const SYNC_FLAG_NO_DELETIONS: u16 = 0x0002;
const SYNC_FLAG_IGNORE_NO_LONGER_IN_SCOPE: u16 = 0x0004;
const META_TAG_IDSET_NO_LONGER_IN_SCOPE: u32 = 0x4021_0102;
const META_TAG_IDSET_EXPIRED: u32 = 0x6793_0102;
const PROGRESS_PROPERTY_TAG: u32 = 0x0000_0102;
const PROGRESS_MESSAGE_SIZE_TAG: u32 = 0x0000_0003;
const PROGRESS_ASSOCIATED_TAG: u32 = 0x0000_000B;

#[derive(Clone, Default)]
struct CounterSet {
    ranges: Vec<(u64, u64)>,
}

#[derive(Clone, Default)]
struct ReplicaCounterSets {
    replicas: BTreeMap<[u8; 16], CounterSet>,
}

#[derive(Clone, Default)]
struct SyncStateSets {
    idset_given: ReplicaCounterSets,
    cnset_seen: ReplicaCounterSets,
    cnset_seen_fai: ReplicaCounterSets,
    cnset_read: ReplicaCounterSets,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DownloadChangeFact {
    pub(crate) object_id: u64,
    pub(crate) change_number: u64,
    pub(crate) associated: bool,
    pub(crate) source_key: Vec<u8>,
}

pub(crate) fn download_change_facts(
    sync_type: u8,
    sync_flags: u16,
    folder_id: u64,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    attachment_facts: &[MessageAttachmentSyncFacts],
    special_objects: &[SpecialMessageSyncFact],
    folder_versions: &[crate::mapi_store::MapiFolderVersion],
) -> Vec<DownloadChangeFact> {
    if sync_type == SYNC_TYPE_HIERARCHY {
        return mailboxes
            .iter()
            .filter_map(|mailbox| {
                let object_id = mapi_folder_id_for_mailbox(mailbox, folder_id);
                (object_id != folder_id).then(|| DownloadChangeFact {
                    object_id,
                    change_number: folder_versions
                        .iter()
                        .find(|version| version.folder_id == object_id)
                        .map(|version| version.change_number)
                        .unwrap_or_else(|| canonical_hierarchy_change_number(folder_id, mailbox)),
                    associated: false,
                    source_key: source_key_for_store_id(object_id),
                })
            })
            .collect();
    }
    if sync_type != SYNC_TYPE_CONTENTS {
        return Vec::new();
    }

    let mut facts = if content_sync_includes_normal(sync_type, sync_flags) {
        emails
            .iter()
            .filter_map(|email| {
                Some(DownloadChangeFact {
                    object_id: crate::mapi::identity::mapped_mapi_object_id(&email.id)?,
                    change_number: manifest::canonical_message_change_number_with_attachments(
                        email,
                        attachments_for_message(email.id, attachment_facts),
                    ),
                    associated: false,
                    source_key: source_key_for_uuid(&email.id),
                })
            })
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let default_include_associated =
        default_content_sync_includes_associated(emails, special_objects);
    facts.extend(special_objects.iter().filter_map(|object| {
        content_sync_includes_associated(
            sync_type,
            sync_flags,
            object.associated,
            default_include_associated,
        )
        .then(|| DownloadChangeFact {
            object_id: object.item_id,
            change_number: special_message::special_message_change_number(object),
            associated: object.associated,
            source_key: special_message::special_message_sync_source_key(object, sync_flags),
        })
    }));
    facts
}

struct ParsedProperty<'a> {
    tag: u32,
    value: &'a [u8],
    next_offset: usize,
}

#[derive(Clone, Copy)]
struct ProgressPerMessage {
    message_size: u64,
    associated: bool,
}

struct ManifestChange {
    start: usize,
    end: usize,
    source_key: Vec<u8>,
    associated: bool,
    progress: Option<ProgressPerMessage>,
}

struct ParsedManifest {
    changes: Vec<ManifestChange>,
    deleted_ids: CounterSet,
    no_longer_in_scope_ids: CounterSet,
    expired_ids: CounterSet,
    final_state: SyncStateSets,
    progress_prefix: Option<[u8; 4]>,
}

impl CounterSet {
    fn from_ranges(mut ranges: Vec<(u64, u64)>) -> Result<Self, String> {
        if ranges
            .iter()
            .any(|(low, high)| *low == 0 || low > high || *high > 0x0000_FFFF_FFFF_FFFF)
        {
            return Err("GLOBSET range is outside the 48-bit GLOBCNT domain".to_string());
        }
        ranges.sort_unstable();
        let mut merged: Vec<(u64, u64)> = Vec::with_capacity(ranges.len());
        for (low, high) in ranges {
            if let Some((_, previous_high)) = merged.last_mut() {
                if low <= previous_high.saturating_add(1) {
                    *previous_high = (*previous_high).max(high);
                    continue;
                }
            }
            merged.push((low, high));
        }
        Ok(Self { ranges: merged })
    }

    fn contains(&self, value: u64) -> bool {
        self.ranges
            .binary_search_by(|(low, high)| {
                if value < *low {
                    std::cmp::Ordering::Greater
                } else if value > *high {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .is_ok()
    }

    fn insert(&mut self, value: u64) {
        debug_assert!((1..=0x0000_FFFF_FFFF_FFFF).contains(&value));
        let index = self
            .ranges
            .partition_point(|(_, high)| high.saturating_add(1) < value);
        if index == self.ranges.len() {
            self.ranges.push((value, value));
            return;
        }

        let (low, high) = self.ranges[index];
        if value.saturating_add(1) < low {
            self.ranges.insert(index, (value, value));
            return;
        }
        if (low..=high).contains(&value) {
            return;
        }

        self.ranges[index] = (low.min(value), high.max(value));
        while index + 1 < self.ranges.len()
            && self.ranges[index + 1].0 <= self.ranges[index].1.saturating_add(1)
        {
            let next_high = self.ranges.remove(index + 1).1;
            self.ranges[index].1 = self.ranges[index].1.max(next_high);
        }
    }

    fn union_with(&mut self, other: &Self) {
        let mut ranges = Vec::with_capacity(self.ranges.len() + other.ranges.len());
        ranges.extend_from_slice(&self.ranges);
        ranges.extend_from_slice(&other.ranges);
        *self = Self::from_ranges(ranges).expect("validated GLOBSET ranges remain valid");
    }

    fn difference(&self, other: &Self) -> Self {
        let mut output = Vec::new();
        let mut other_index = 0usize;
        for &(low, high) in &self.ranges {
            let mut cursor = low;
            while other_index < other.ranges.len() && other.ranges[other_index].1 < cursor {
                other_index += 1;
            }
            let mut index = other_index;
            while index < other.ranges.len() && other.ranges[index].0 <= high {
                let (other_low, other_high) = other.ranges[index];
                if other_low > cursor {
                    output.push((cursor, high.min(other_low - 1)));
                }
                if other_high >= high {
                    cursor = high.saturating_add(1);
                    break;
                }
                cursor = cursor.max(other_high.saturating_add(1));
                index += 1;
            }
            if cursor <= high {
                output.push((cursor, high));
            }
        }
        Self { ranges: output }
    }

    fn intersection(&self, other: &Self) -> Self {
        let mut output = Vec::new();
        let (mut left, mut right) = (0usize, 0usize);
        while left < self.ranges.len() && right < other.ranges.len() {
            let (left_low, left_high) = self.ranges[left];
            let (right_low, right_high) = other.ranges[right];
            let low = left_low.max(right_low);
            let high = left_high.min(right_high);
            if low <= high {
                output.push((low, high));
            }
            if left_high < right_high {
                left += 1;
            } else {
                right += 1;
            }
        }
        Self { ranges: output }
    }

    fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }
}

impl ReplicaCounterSets {
    fn local(&self) -> Option<&CounterSet> {
        self.replicas.get(&STORE_REPLICA_GUID)
    }

    fn local_mut(&mut self) -> &mut CounterSet {
        self.replicas.entry(STORE_REPLICA_GUID).or_default()
    }

    fn insert(&mut self, replica_guid: [u8; 16], counter: u64) {
        self.replicas
            .entry(replica_guid)
            .or_default()
            .insert(counter);
    }
}

fn source_key_replica_counter(source_key: &[u8]) -> Option<([u8; 16], u64)> {
    let replica_guid = source_key.get(..16)?.try_into().ok()?;
    let counter = crate::mapi::identity::global_counter_from_globcnt(source_key.get(16..22)?)?;
    (source_key.len() == 22 && counter != 0).then_some((replica_guid, counter))
}

pub(super) fn replguid_idset_from_source_keys<'a>(
    source_keys: impl IntoIterator<Item = (&'a [u8], u64)>,
) -> Vec<u8> {
    let mut identities = ReplicaCounterSets::default();
    for (source_key, fallback_object_id) in source_keys {
        let identity = source_key_replica_counter(source_key).or_else(|| {
            crate::mapi::identity::global_counter_from_store_id(fallback_object_id)
                .map(|counter| (STORE_REPLICA_GUID, counter))
        });
        if let Some((replica_guid, counter)) = identity {
            identities.insert(replica_guid, counter);
        }
    }
    encode_replguid_sets(&identities)
}

pub(crate) fn validate_download_state_property(
    sync_type: u8,
    property_tag: u32,
    value: &[u8],
) -> Result<(), String> {
    let valid = matches!(
        property_tag,
        META_TAG_IDSET_GIVEN | META_TAG_IDSET_GIVEN_BINARY
    ) || property_tag == META_TAG_CNSET_SEEN
        || (sync_type == SYNC_TYPE_CONTENTS
            && matches!(property_tag, META_TAG_CNSET_SEEN_FAI | META_TAG_CNSET_READ));
    if !valid {
        return Err(format!(
            "property 0x{property_tag:08x} is not valid for ICS sync type 0x{sync_type:02x}"
        ));
    }
    decode_replguid_set(value).map(|_| ())
}

/// Selects an already generated LPE ICS download manifest from the state the
/// client uploaded to the synchronization context.
///
/// [MS-OXCFXICS] sections 2.2.1.1.1 through 2.2.1.1.4 define the four
/// client state sets. Section 3.2.5.3 requires the server to derive the
/// download from those sets, rather than from server-side per-client state.
pub(crate) fn select_download_manifest_for_client_state(
    sync_type: u8,
    sync_flags: u16,
    full_manifest: &[u8],
    client_state: &[u8],
    change_facts: &[DownloadChangeFact],
    resident_hierarchy_alias_counters: &[u64],
) -> Result<(Vec<u8>, Vec<u8>), String> {
    if !matches!(sync_type, SYNC_TYPE_CONTENTS | SYNC_TYPE_HIERARCHY) {
        return Err(format!(
            "unsupported ICS synchronization type 0x{sync_type:02x}"
        ));
    }

    let client_state = parse_standalone_state(client_state, sync_type, "client")?;
    let manifest = parse_manifest(full_manifest, sync_type, sync_flags)?;
    let mut changes_by_source_key = BTreeMap::new();
    for fact in change_facts {
        let source_identity = source_key_replica_counter(&fact.source_key).ok_or_else(|| {
            format!(
                "invalid SourceKey GID in ICS change facts for object 0x{:016x}",
                fact.object_id
            )
        })?;
        if changes_by_source_key
            .insert(fact.source_key.as_slice(), (fact, source_identity))
            .is_some()
        {
            return Err(format!(
                "duplicate SourceKey in ICS change facts for object 0x{:016x}",
                fact.object_id
            ));
        }
    }
    let mut retained = Vec::new();

    for change in &manifest.changes {
        let &(fact, _) = changes_by_source_key
            .get(change.source_key.as_slice())
            .ok_or_else(|| {
                format!(
                    "ICS manifest SourceKey ({} bytes) has no canonical change fact",
                    change.source_key.len()
                )
            })?;
        if fact.associated != change.associated {
            return Err(format!(
                "ICS manifest SourceKey ({} bytes) has inconsistent associated state",
                change.source_key.len()
            ));
        }
        let already_seen = if sync_type == SYNC_TYPE_CONTENTS && change.associated {
            client_state
                .cnset_seen_fai
                .local()
                .is_some_and(|seen| seen.contains(fact.change_number))
        } else {
            client_state
                .cnset_seen
                .local()
                .is_some_and(|seen| seen.contains(fact.change_number))
        };
        if !already_seen {
            retained.push(change);
        }
    }

    let mut selected = Vec::with_capacity(full_manifest.len());
    if let Some(prefix) = manifest.progress_prefix {
        write_selected_progress_mode(&mut selected, prefix, &retained)?;
    }
    for change in &retained {
        selected.extend_from_slice(
            full_manifest
                .get(change.start..change.end)
                .ok_or_else(|| "ICS change range overruns manifest".to_string())?,
        );
    }

    let mut final_state = client_state.clone();
    for change in &retained {
        let &(fact, (replica_guid, object_counter)) = changes_by_source_key
            .get(change.source_key.as_slice())
            .expect("retained changes were matched above");
        // [MS-OXCFXICS] sections 2.2.1.1.1, 2.2.1.2.5, 2.2.2.4.2,
        // and 3.2.5.3: IdsetGivenC adds change.Id, which is the GID in the
        // SourceKey actually emitted for this change, including its REPLGUID.
        final_state.idset_given.insert(replica_guid, object_counter);
        if sync_type == SYNC_TYPE_CONTENTS && change.associated {
            final_state
                .cnset_seen_fai
                .local_mut()
                .insert(fact.change_number);
        } else {
            final_state
                .cnset_seen
                .local_mut()
                .insert(fact.change_number);
        }
    }

    if sync_flags & SYNC_FLAG_NO_DELETIONS == 0 {
        let mut resident_server_ids = manifest
            .final_state
            .idset_given
            .local()
            .cloned()
            .unwrap_or_default();
        if sync_type == SYNC_TYPE_HIERARCHY {
            // [MS-OXCFXICS] sections 2.2.3.2.4.3.1, 3.2.5.3, and
            // 3.3.5.8.8: a successfully imported folder SourceKey remains a
            // server object and the client adds its FID to IdsetGiven.
            // Count durable aliases only when comparing this client's
            // IdsetGiven; they are not new changes to advertise to other OSTs.
            for counter in resident_hierarchy_alias_counters {
                resident_server_ids.insert(*counter);
            }
        }
        let missing = client_state
            .idset_given
            .local()
            .map(|client| client.difference(&resident_server_ids))
            .unwrap_or_default();
        let mut no_longer_in_scope = missing.intersection(&manifest.no_longer_in_scope_ids);
        let expired = missing.intersection(&manifest.expired_ids);
        let explicitly_deleted = if sync_type == SYNC_TYPE_HIERARCHY {
            manifest.deleted_ids.clone()
        } else {
            missing.intersection(&manifest.deleted_ids)
        };
        let mut deleted = missing.difference(&no_longer_in_scope).difference(&expired);
        deleted.union_with(&explicitly_deleted);
        if sync_flags & SYNC_FLAG_IGNORE_NO_LONGER_IN_SCOPE != 0 {
            no_longer_in_scope = CounterSet::default();
        }
        write_deletion_section(&mut selected, &deleted, &no_longer_in_scope, &expired);
        let mut removed = deleted.clone();
        removed.union_with(&no_longer_in_scope);
        removed.union_with(&expired);
        let remaining_ids = final_state
            .idset_given
            .local()
            .map(|ids| ids.difference(&removed))
            .unwrap_or_default();
        *final_state.idset_given.local_mut() = remaining_ids;
    }

    let mut selected_final_state = Vec::new();
    write_state(&mut selected_final_state, sync_type, &final_state);
    selected.extend_from_slice(&selected_final_state);
    write_u32(&mut selected, INCR_SYNC_END);
    Ok((selected, selected_final_state))
}

fn parse_manifest(bytes: &[u8], sync_type: u8, sync_flags: u16) -> Result<ParsedManifest, String> {
    let progress_expected = sync_type == SYNC_TYPE_CONTENTS && sync_flags & SYNC_FLAG_PROGRESS != 0;
    let mut offset = 0usize;
    let progress_prefix = if read_u32(bytes, offset).ok() == Some(INCR_SYNC_PROGRESS_MODE) {
        if !progress_expected {
            return Err("unexpected IncrSyncProgressMode in ICS manifest".to_string());
        }
        let (prefix, next_offset) = parse_progress_mode(bytes, offset)?;
        offset = next_offset;
        Some(prefix)
    } else {
        if progress_expected {
            return Err("missing IncrSyncProgressMode in progress ICS manifest".to_string());
        }
        None
    };

    let mut changes = Vec::new();
    let mut deleted_ids = CounterSet::default();
    let mut no_longer_in_scope_ids = CounterSet::default();
    let mut expired_ids = CounterSet::default();
    let final_state = loop {
        let tag = read_u32(bytes, offset)?;
        let (start, progress, change_offset) = if tag == INCR_SYNC_PROGRESS_PER_MSG {
            if !progress_expected {
                return Err("unexpected IncrSyncProgressPerMsg in ICS manifest".to_string());
            }
            let (progress, change_offset) = parse_progress_per_message(bytes, offset)?;
            if read_u32(bytes, change_offset)? != INCR_SYNC_CHG {
                return Err("IncrSyncProgressPerMsg is not followed by IncrSyncChg".to_string());
            }
            (offset, Some(progress), change_offset)
        } else {
            (offset, None, offset)
        };

        match read_u32(bytes, change_offset)? {
            INCR_SYNC_CHG => {
                if progress_expected && progress.is_none() {
                    return Err("progress ICS change is missing IncrSyncProgressPerMsg".to_string());
                }
                let mut change = parse_change(bytes, change_offset, sync_type)?;
                if let Some(progress) = progress {
                    if progress.associated != change.associated {
                        return Err(
                            "progress associated flag differs from ICS change header".to_string()
                        );
                    }
                    change.progress = Some(progress);
                }
                change.start = start;
                offset = change.end;
                changes.push(change);
            }
            INCR_SYNC_DEL => {
                let (deleted, no_longer, expired, next_offset) =
                    parse_deletion_section(bytes, change_offset)?;
                deleted_ids.union_with(&deleted);
                no_longer_in_scope_ids.union_with(&no_longer);
                expired_ids.union_with(&expired);
                offset = next_offset;
            }
            INCR_SYNC_READ => {
                let (_, _, next_offset) = parse_read_state_section(bytes, change_offset)?;
                offset = next_offset;
            }
            INCR_SYNC_STATE_BEGIN => {
                let (final_state, final_state_end) =
                    parse_state(bytes, change_offset, sync_type, "manifest final")?;
                if read_u32(bytes, final_state_end)? != INCR_SYNC_END {
                    return Err("ICS final state is not followed by IncrSyncEnd".to_string());
                }
                if final_state_end.checked_add(4) != Some(bytes.len()) {
                    return Err("trailing bytes after IncrSyncEnd".to_string());
                }
                break final_state;
            }
            other => {
                return Err(format!(
                    "unexpected top-level FastTransfer atom 0x{other:08x} at offset {change_offset}"
                ))
            }
        }
    };

    Ok(ParsedManifest {
        changes,
        deleted_ids,
        no_longer_in_scope_ids,
        expired_ids,
        final_state,
        progress_prefix,
    })
}

fn parse_change(bytes: &[u8], start: usize, sync_type: u8) -> Result<ManifestChange, String> {
    let mut offset = start
        .checked_add(4)
        .ok_or_else(|| "ICS change offset overflow".to_string())?;
    let mut in_header = true;
    let mut message_marker_seen = false;
    let mut source_key = None;
    let mut associated = None;

    while offset < bytes.len() {
        let tag = read_u32(bytes, offset)?;
        if is_change_boundary(tag) {
            break;
        }
        if is_fast_transfer_marker(tag) {
            if tag == INCR_SYNC_MESSAGE {
                if sync_type != SYNC_TYPE_CONTENTS || message_marker_seen {
                    return Err("invalid IncrSyncMessage placement in ICS change".to_string());
                }
                message_marker_seen = true;
                in_header = false;
            }
            offset += 4;
            continue;
        }

        let property = parse_property(bytes, offset)?;
        if in_header {
            match property.tag {
                PID_TAG_SOURCE_KEY => {
                    if source_key.replace(property.value.to_vec()).is_some() {
                        return Err("ICS change contains duplicate PidTagSourceKey".to_string());
                    }
                }
                PID_TAG_ASSOCIATED => associated = Some(parse_bool(property.value)?),
                _ => {}
            }
        }
        offset = property.next_offset;
    }

    if sync_type == SYNC_TYPE_CONTENTS && !message_marker_seen {
        return Err("content ICS change is missing IncrSyncMessage".to_string());
    }
    let associated = if sync_type == SYNC_TYPE_CONTENTS {
        associated.ok_or_else(|| "content ICS change is missing PidTagAssociated".to_string())?
    } else {
        false
    };
    let source_key =
        source_key.ok_or_else(|| "ICS change does not contain PidTagSourceKey".to_string())?;
    Ok(ManifestChange {
        start,
        end: offset,
        source_key,
        associated,
        progress: None,
    })
}

fn parse_standalone_state(
    bytes: &[u8],
    sync_type: u8,
    label: &str,
) -> Result<SyncStateSets, String> {
    let (state, end) = parse_state(bytes, 0, sync_type, label)?;
    if end != bytes.len() {
        return Err(format!("trailing bytes after {label} ICS state"));
    }
    Ok(state)
}

fn parse_state(
    bytes: &[u8],
    start: usize,
    sync_type: u8,
    label: &str,
) -> Result<(SyncStateSets, usize), String> {
    if read_u32(bytes, start)? != INCR_SYNC_STATE_BEGIN {
        return Err(format!("{label} ICS state is missing IncrSyncStateBegin"));
    }
    let mut offset = start + 4;
    let mut raw_idset_given = None;
    let mut raw_cnset_seen = None;
    let mut raw_cnset_seen_fai = None;
    let mut raw_cnset_read = None;

    loop {
        let tag = read_u32(bytes, offset)?;
        if tag == INCR_SYNC_STATE_END {
            offset += 4;
            break;
        }
        let property = parse_property(bytes, offset)?;
        let target = match property.tag {
            META_TAG_IDSET_GIVEN | META_TAG_IDSET_GIVEN_BINARY => &mut raw_idset_given,
            META_TAG_CNSET_SEEN => &mut raw_cnset_seen,
            META_TAG_CNSET_SEEN_FAI if sync_type == SYNC_TYPE_CONTENTS => &mut raw_cnset_seen_fai,
            META_TAG_CNSET_READ if sync_type == SYNC_TYPE_CONTENTS => &mut raw_cnset_read,
            _ => {
                return Err(format!(
                    "unexpected property 0x{:08x} in {label} ICS state",
                    property.tag
                ))
            }
        };
        if target.replace(property.value).is_some() {
            return Err(format!(
                "duplicate property 0x{:08x} in {label} ICS state",
                property.tag
            ));
        }
        offset = property.next_offset;
    }

    let idset_given = decode_replguid_set(required_state_value(
        raw_idset_given,
        META_TAG_IDSET_GIVEN,
        label,
    )?)?;
    let cnset_seen = decode_replguid_set(required_state_value(
        raw_cnset_seen,
        META_TAG_CNSET_SEEN,
        label,
    )?)?;
    let (cnset_seen_fai, cnset_read) = if sync_type == SYNC_TYPE_CONTENTS {
        (
            decode_replguid_set(required_state_value(
                raw_cnset_seen_fai,
                META_TAG_CNSET_SEEN_FAI,
                label,
            )?)?,
            decode_replguid_set(required_state_value(
                raw_cnset_read,
                META_TAG_CNSET_READ,
                label,
            )?)?,
        )
    } else {
        (ReplicaCounterSets::default(), ReplicaCounterSets::default())
    };

    Ok((
        SyncStateSets {
            idset_given,
            cnset_seen,
            cnset_seen_fai,
            cnset_read,
        },
        offset,
    ))
}

fn required_state_value<'a>(
    value: Option<&'a [u8]>,
    tag: u32,
    label: &str,
) -> Result<&'a [u8], String> {
    value.ok_or_else(|| format!("missing property 0x{tag:08x} in {label} ICS state"))
}

fn decode_replguid_set(value: &[u8]) -> Result<ReplicaCounterSets, String> {
    if value.is_empty() {
        return Ok(ReplicaCounterSets::default());
    }
    let mut offset = 0usize;
    let mut replicas = BTreeMap::new();
    let mut previous_guid = None;
    while offset < value.len() {
        let guid: [u8; 16] = value
            .get(offset..offset.saturating_add(16))
            .ok_or_else(|| "REPLGUID/GLOBSET is missing a replica GUID".to_string())?
            .try_into()
            .unwrap();
        offset += 16;
        if previous_guid.is_some_and(|previous| previous >= guid) {
            return Err("REPLGUID/GLOBSET replica GUIDs are not strictly ordered".to_string());
        }
        let (ranges, next_offset) = decode_globset_range_prefix(value, offset)?;
        offset = next_offset;
        replicas.insert(guid, CounterSet::from_ranges(ranges)?);
        previous_guid = Some(guid);
    }
    Ok(ReplicaCounterSets { replicas })
}

fn decode_replid_set(value: &[u8]) -> Result<CounterSet, String> {
    if value.is_empty() {
        return Ok(CounterSet::default());
    }
    let mut offset = 0usize;
    let mut local = CounterSet::default();
    let mut previous_replid = None;
    while offset < value.len() {
        let replid = u16::from_le_bytes(
            value
                .get(offset..offset.saturating_add(2))
                .ok_or_else(|| "REPLID/GLOBSET is missing its replica id".to_string())?
                .try_into()
                .unwrap(),
        );
        offset += 2;
        if previous_replid.is_some_and(|previous| previous >= replid) {
            return Err("REPLID/GLOBSET replica IDs are not strictly ordered".to_string());
        }
        let (ranges, next_offset) = decode_globset_range_prefix(value, offset)?;
        offset = next_offset;
        if u64::from(replid) == crate::mapi::identity::STORE_REPLICA_ID {
            local.union_with(&CounterSet::from_ranges(ranges)?);
        }
        previous_replid = Some(replid);
    }
    Ok(local)
}

fn decode_globset_range_prefix(
    value: &[u8],
    mut offset: usize,
) -> Result<(Vec<(u64, u64)>, usize), String> {
    let mut stack = Vec::new();
    let mut push_lengths = Vec::new();
    let mut ranges = Vec::new();
    while offset < value.len() {
        let command = value[offset];
        offset += 1;
        match command {
            GLOBSET_END_COMMAND => {
                if !stack.is_empty() {
                    return Err("GLOBSET ended with a non-empty prefix stack".to_string());
                }
                return Ok((ranges, offset));
            }
            1..=6 => {
                let push_len = command as usize;
                let end = offset
                    .checked_add(push_len)
                    .filter(|end| *end <= value.len())
                    .ok_or_else(|| "truncated GLOBSET push command".to_string())?;
                if stack.len().saturating_add(push_len) > 6 {
                    return Err("GLOBSET push exceeds a 6-byte GLOBCNT".to_string());
                }
                stack.extend_from_slice(&value[offset..end]);
                offset = end;
                if stack.len() == 6 {
                    let counter = crate::mapi::identity::global_counter_from_globcnt(&stack)
                        .ok_or_else(|| "invalid pushed GLOBCNT".to_string())?;
                    ranges.push((counter, counter));
                    stack.truncate(stack.len().saturating_sub(push_len));
                } else {
                    push_lengths.push(push_len);
                }
            }
            GLOBSET_POP_COMMAND => {
                let pop_len = push_lengths
                    .pop()
                    .ok_or_else(|| "GLOBSET pop has no matching push".to_string())?;
                if pop_len > stack.len() {
                    return Err("GLOBSET pop exceeds the prefix stack".to_string());
                }
                stack.truncate(stack.len() - pop_len);
            }
            GLOBSET_BITMASK_COMMAND => {
                if stack.len() != 5 {
                    return Err("GLOBSET bitmask requires a 5-byte prefix".to_string());
                }
                let starting_value = *value
                    .get(offset)
                    .ok_or_else(|| "truncated GLOBSET bitmask start".to_string())?;
                let bitmask = *value
                    .get(offset + 1)
                    .ok_or_else(|| "truncated GLOBSET bitmask".to_string())?;
                offset += 2;
                let mut values = vec![starting_value];
                for bit in 0..8 {
                    if bitmask & (1 << bit) != 0 {
                        let next = u16::from(starting_value) + 1 + bit;
                        if next > u16::from(u8::MAX) {
                            return Err("GLOBSET bitmask value overflows".to_string());
                        }
                        values.push(next as u8);
                    }
                }
                values.sort_unstable();
                values.dedup();
                let mut low = values[0];
                let mut high = low;
                for value in values.into_iter().skip(1) {
                    if value == high.saturating_add(1) {
                        high = value;
                    } else {
                        ranges.push(globcnt_suffix_range(&stack, low, high)?);
                        low = value;
                        high = value;
                    }
                }
                ranges.push(globcnt_suffix_range(&stack, low, high)?);
            }
            GLOBSET_RANGE_COMMAND => {
                let suffix_len = 6usize.saturating_sub(stack.len());
                let low_suffix = value
                    .get(offset..offset.saturating_add(suffix_len))
                    .ok_or_else(|| "truncated GLOBSET range low value".to_string())?;
                let high_suffix = value
                    .get(offset.saturating_add(suffix_len)..offset.saturating_add(suffix_len * 2))
                    .ok_or_else(|| "truncated GLOBSET range high value".to_string())?;
                let mut low = stack.clone();
                low.extend_from_slice(low_suffix);
                let mut high = stack.clone();
                high.extend_from_slice(high_suffix);
                let low = crate::mapi::identity::global_counter_from_globcnt(&low)
                    .ok_or_else(|| "invalid GLOBSET range low value".to_string())?;
                let high = crate::mapi::identity::global_counter_from_globcnt(&high)
                    .ok_or_else(|| "invalid GLOBSET range high value".to_string())?;
                if high < low {
                    return Err("GLOBSET range is descending".to_string());
                }
                ranges.push((low, high));
                offset += suffix_len * 2;
            }
            _ => return Err(format!("unsupported GLOBSET command 0x{command:02x}")),
        }
    }
    Err("GLOBSET is missing its end command".to_string())
}

fn globcnt_suffix_range(prefix: &[u8], low: u8, high: u8) -> Result<(u64, u64), String> {
    let mut low_bytes = prefix.to_vec();
    low_bytes.push(low);
    let mut high_bytes = prefix.to_vec();
    high_bytes.push(high);
    Ok((
        crate::mapi::identity::global_counter_from_globcnt(&low_bytes)
            .ok_or_else(|| "invalid GLOBSET bitmask low value".to_string())?,
        crate::mapi::identity::global_counter_from_globcnt(&high_bytes)
            .ok_or_else(|| "invalid GLOBSET bitmask high value".to_string())?,
    ))
}

fn parse_progress_mode(bytes: &[u8], start: usize) -> Result<([u8; 4], usize), String> {
    let property = parse_property(bytes, start + 4)?;
    if property.tag != PROGRESS_PROPERTY_TAG || property.value.len() != 32 {
        return Err("invalid IncrSyncProgressMode payload".to_string());
    }
    Ok((
        property.value[..4].try_into().unwrap(),
        property.next_offset,
    ))
}

fn parse_progress_per_message(
    bytes: &[u8],
    start: usize,
) -> Result<(ProgressPerMessage, usize), String> {
    let size = parse_property(bytes, start + 4)?;
    if size.tag != PROGRESS_MESSAGE_SIZE_TAG || size.value.len() != 4 {
        return Err("invalid IncrSyncProgressPerMsg message size".to_string());
    }
    let size_value = i32::from_le_bytes(size.value.try_into().unwrap());
    if size_value < 0 {
        return Err("negative IncrSyncProgressPerMsg message size".to_string());
    }
    let associated = parse_property(bytes, size.next_offset)?;
    if associated.tag != PROGRESS_ASSOCIATED_TAG {
        return Err("invalid IncrSyncProgressPerMsg associated flag".to_string());
    }
    Ok((
        ProgressPerMessage {
            message_size: size_value as u64,
            associated: parse_bool(associated.value)?,
        },
        associated.next_offset,
    ))
}

fn write_selected_progress_mode(
    output: &mut Vec<u8>,
    prefix: [u8; 4],
    retained: &[&ManifestChange],
) -> Result<(), String> {
    let mut fai_count = 0u32;
    let mut fai_size = 0u64;
    let mut normal_count = 0u32;
    let mut normal_size = 0u64;
    for change in retained {
        let progress = change
            .progress
            .ok_or_else(|| "retained progress change has no progress payload".to_string())?;
        if progress.associated {
            fai_count = fai_count.saturating_add(1);
            fai_size = fai_size.saturating_add(progress.message_size);
        } else {
            normal_count = normal_count.saturating_add(1);
            normal_size = normal_size.saturating_add(progress.message_size);
        }
    }

    let mut value = Vec::with_capacity(32);
    value.extend_from_slice(&prefix);
    value.extend_from_slice(&fai_count.to_le_bytes());
    value.extend_from_slice(&fai_size.to_le_bytes());
    value.extend_from_slice(&normal_count.to_le_bytes());
    value.extend_from_slice(&0u32.to_le_bytes());
    value.extend_from_slice(&normal_size.to_le_bytes());
    write_u32(output, INCR_SYNC_PROGRESS_MODE);
    write_binary_property(output, PROGRESS_PROPERTY_TAG, &value);
    Ok(())
}

fn parse_read_state_section(
    bytes: &[u8],
    start: usize,
) -> Result<(CounterSet, CounterSet, usize), String> {
    let mut offset = start + 4;
    let mut read = None;
    let mut unread = None;
    while !is_change_boundary(read_u32(bytes, offset)?) {
        let property = parse_property(bytes, offset)?;
        let target = match property.tag {
            META_TAG_IDSET_READ => &mut read,
            META_TAG_IDSET_UNREAD => &mut unread,
            _ => {
                return Err(format!(
                    "unexpected property 0x{:08x} in IncrSyncRead",
                    property.tag
                ))
            }
        };
        if target.replace(decode_replid_set(property.value)?).is_some() {
            return Err(format!(
                "duplicate property 0x{:08x} in IncrSyncRead",
                property.tag
            ));
        }
        offset = property.next_offset;
    }
    let read = read.unwrap_or_default();
    let unread = unread.unwrap_or_default();
    if !read.intersection(&unread).is_empty() {
        return Err("the same object is both read and unread in ICS manifest".to_string());
    }
    Ok((read, unread, offset))
}

fn parse_deletion_section(
    bytes: &[u8],
    start: usize,
) -> Result<(CounterSet, CounterSet, CounterSet, usize), String> {
    let mut offset = start + 4;
    let mut deleted = None;
    let mut no_longer_in_scope = None;
    let mut expired = None;
    while !is_change_boundary(read_u32(bytes, offset)?) {
        let property = parse_property(bytes, offset)?;
        let target = match property.tag {
            META_TAG_IDSET_DELETED => &mut deleted,
            META_TAG_IDSET_NO_LONGER_IN_SCOPE => &mut no_longer_in_scope,
            META_TAG_IDSET_EXPIRED => &mut expired,
            _ => {
                return Err(format!(
                    "invalid property 0x{:08x} in IncrSyncDel",
                    property.tag
                ))
            }
        };
        if target.replace(decode_replid_set(property.value)?).is_some() {
            return Err(format!(
                "duplicate property 0x{:08x} in IncrSyncDel",
                property.tag
            ));
        }
        offset = property.next_offset;
    }
    if deleted.is_none() && no_longer_in_scope.is_none() && expired.is_none() {
        return Err("IncrSyncDel has no IDSET property".to_string());
    }
    Ok((
        deleted.unwrap_or_default(),
        no_longer_in_scope.unwrap_or_default(),
        expired.unwrap_or_default(),
        offset,
    ))
}

fn write_deletion_section(
    output: &mut Vec<u8>,
    deleted: &CounterSet,
    no_longer_in_scope: &CounterSet,
    expired: &CounterSet,
) {
    if deleted.is_empty() && no_longer_in_scope.is_empty() && expired.is_empty() {
        return;
    }
    write_u32(output, INCR_SYNC_DEL);
    write_replid_idset_property(output, META_TAG_IDSET_DELETED, deleted);
    write_replid_idset_property(
        output,
        META_TAG_IDSET_NO_LONGER_IN_SCOPE,
        no_longer_in_scope,
    );
    write_replid_idset_property(output, META_TAG_IDSET_EXPIRED, expired);
}

fn write_replid_idset_property(output: &mut Vec<u8>, property_tag: u32, counters: &CounterSet) {
    if counters.is_empty() {
        return;
    }
    let mut value = Vec::new();
    value.extend_from_slice(&(crate::mapi::identity::STORE_REPLICA_ID as u16).to_le_bytes());
    write_globset_ranges(&mut value, &counters.ranges);
    write_binary_property(output, property_tag, &value);
}

fn write_state(output: &mut Vec<u8>, sync_type: u8, state: &SyncStateSets) {
    write_u32(output, INCR_SYNC_STATE_BEGIN);
    write_binary_property(
        output,
        META_TAG_IDSET_GIVEN,
        &encode_replguid_sets(&state.idset_given),
    );
    write_binary_property(
        output,
        META_TAG_CNSET_SEEN,
        &encode_replguid_sets(&state.cnset_seen),
    );
    if sync_type == SYNC_TYPE_CONTENTS {
        write_binary_property(
            output,
            META_TAG_CNSET_SEEN_FAI,
            &encode_replguid_sets(&state.cnset_seen_fai),
        );
        write_binary_property(
            output,
            META_TAG_CNSET_READ,
            &encode_replguid_sets(&state.cnset_read),
        );
    }
    write_u32(output, INCR_SYNC_STATE_END);
}

fn encode_replguid_sets(sets: &ReplicaCounterSets) -> Vec<u8> {
    let mut value = Vec::new();
    for (guid, counters) in &sets.replicas {
        if counters.is_empty() {
            continue;
        }
        value.extend_from_slice(guid);
        write_globset_ranges(&mut value, &counters.ranges);
    }
    value
}

fn parse_property(bytes: &[u8], offset: usize) -> Result<ParsedProperty<'_>, String> {
    let tag = read_u32(bytes, offset)?;
    if is_fast_transfer_marker(tag) {
        return Err(format!(
            "FastTransfer marker 0x{tag:08x} appears where a property is required"
        ));
    }
    let value_info = offset
        .checked_add(4)
        .ok_or_else(|| "FastTransfer property offset overflow".to_string())?;
    let value_start = fast_transfer_property_value_start(bytes, tag, value_info)?;
    let property_type = tag & 0x0000_FFFF;
    let (payload_start, payload_len, next_offset) = match property_type {
        _ if tag == META_TAG_IDSET_GIVEN => variable_property_range(bytes, value_start)?,
        0x0002 => fixed_property_range(bytes, value_start, 2)?,
        0x0003 => fixed_property_range(bytes, value_start, 4)?,
        0x000B => fixed_property_range(bytes, value_start, 2)?,
        0x0014 | 0x0040 => fixed_property_range(bytes, value_start, 8)?,
        0x0048 => fixed_property_range(bytes, value_start, 16)?,
        0x001E | 0x001F | 0x0102 => variable_property_range(bytes, value_start)?,
        0x101E | 0x101F => multi_string_property_range(bytes, value_start)?,
        _ => {
            return Err(format!(
                "unsupported LPE FastTransfer property type in 0x{tag:08x}"
            ))
        }
    };
    let value = bytes
        .get(payload_start..payload_start + payload_len)
        .ok_or_else(|| format!("FastTransfer property 0x{tag:08x} overruns stream"))?;
    Ok(ParsedProperty {
        tag,
        value,
        next_offset,
    })
}

fn fixed_property_range(
    bytes: &[u8],
    value_start: usize,
    len: usize,
) -> Result<(usize, usize, usize), String> {
    let end = value_start
        .checked_add(len)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| "fixed FastTransfer property overruns stream".to_string())?;
    Ok((value_start, len, end))
}

fn variable_property_range(
    bytes: &[u8],
    value_start: usize,
) -> Result<(usize, usize, usize), String> {
    let len = read_u32(bytes, value_start)? as usize;
    let payload_start = value_start
        .checked_add(4)
        .ok_or_else(|| "variable FastTransfer property offset overflow".to_string())?;
    let end = payload_start
        .checked_add(len)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| "variable FastTransfer property overruns stream".to_string())?;
    Ok((payload_start, len, end))
}

fn multi_string_property_range(
    bytes: &[u8],
    value_start: usize,
) -> Result<(usize, usize, usize), String> {
    let count = read_u32(bytes, value_start)? as usize;
    let mut end = value_start + 4;
    for _ in 0..count {
        let len = read_u32(bytes, end)? as usize;
        end = end
            .checked_add(4)
            .and_then(|offset| offset.checked_add(len))
            .filter(|offset| *offset <= bytes.len())
            .ok_or_else(|| "multi-string FastTransfer property overruns stream".to_string())?;
    }
    Ok((value_start, end - value_start, end))
}

fn parse_bool(value: &[u8]) -> Result<bool, String> {
    match value {
        [0, 0] => Ok(false),
        [1, 0] => Ok(true),
        _ => Err("invalid FastTransfer Boolean value".to_string()),
    }
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, String> {
    let value = bytes
        .get(offset..offset.saturating_add(4))
        .ok_or_else(|| format!("FastTransfer atom at offset {offset} overruns stream"))?;
    Ok(u32::from_le_bytes(value.try_into().unwrap()))
}

fn is_change_boundary(tag: u32) -> bool {
    matches!(
        tag,
        INCR_SYNC_PROGRESS_PER_MSG
            | INCR_SYNC_CHG
            | INCR_SYNC_DEL
            | INCR_SYNC_READ
            | INCR_SYNC_STATE_BEGIN
            | INCR_SYNC_END
    )
}

fn is_fast_transfer_marker(tag: u32) -> bool {
    matches!(
        tag,
        NEW_ATTACH
            | START_EMBED
            | END_EMBED
            | START_RECIP
            | END_TO_RECIP
            | START_TOP_FLD
            | START_SUB_FLD
            | END_FOLDER
            | START_MESSAGE
            | END_MESSAGE
            | END_ATTACH
            | INCR_SYNC_CHG
            | INCR_SYNC_DEL
            | INCR_SYNC_END
            | INCR_SYNC_MESSAGE
            | INCR_SYNC_READ
            | INCR_SYNC_STATE_BEGIN
            | INCR_SYNC_STATE_END
            | INCR_SYNC_PROGRESS_MODE
            | INCR_SYNC_PROGRESS_PER_MSG
            | 0x4010_0003 // StartFAIMsg
    )
}
