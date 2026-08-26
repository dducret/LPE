use super::*;
use anyhow::bail;

const EVENT_SUBJECT_ALIASES: &[u32] = &[PID_TAG_SUBJECT_W, PID_TAG_NORMALIZED_SUBJECT_W];
const EVENT_LOCATION_ALIASES: &[u32] = &[PID_LID_LOCATION_W_TAG];
const EVENT_HTML_ALIASES: &[u32] = &[PID_TAG_BODY_HTML_W, PID_TAG_HTML_BINARY];
const EVENT_TEMPORAL_PROPERTIES: &[u32] = &[
    PID_TAG_START_DATE,
    PID_TAG_END_DATE,
    PID_LID_COMMON_START_TAG,
    PID_LID_COMMON_END_TAG,
    PID_LID_APPOINTMENT_START_WHOLE_TAG,
    PID_LID_APPOINTMENT_END_WHOLE_TAG,
    PID_LID_APPOINTMENT_DURATION_TAG,
];
const EVENT_REMINDER_PROPERTIES: &[u32] = &[
    PID_LID_REMINDER_SET_TAG,
    PID_LID_REMINDER_TIME_TAG,
    PID_LID_REMINDER_SIGNAL_TIME_TAG,
    PID_LID_REMINDER_DELTA_TAG,
];
const EVENT_RESPONSE_PROPERTIES: &[u32] = &[PID_TAG_RESPONSE_REQUESTED, PID_TAG_REPLY_REQUESTED];
const EVENT_SERVER_MANAGED_PROPERTIES: &[u32] = &[
    PID_TAG_ENTRY_ID,
    PID_TAG_PARENT_ENTRY_ID,
    PID_TAG_INSTANCE_KEY,
    PID_TAG_RECORD_KEY,
    PID_TAG_FOLDER_ID,
    PID_TAG_PARENT_FOLDER_ID,
    PID_TAG_MID,
    PID_TAG_INST_ID,
    PID_TAG_INSTANCE_NUM,
    PID_TAG_PARENT_SOURCE_KEY,
    PID_TAG_LAST_MODIFICATION_TIME,
    PID_TAG_LOCAL_COMMIT_TIME,
    PID_TAG_SOURCE_KEY,
    PID_TAG_SEARCH_KEY,
    PID_TAG_CHANGE_KEY,
    PID_TAG_PREDECESSOR_CHANGE_LIST,
    PID_TAG_CHANGE_NUMBER,
    PID_TAG_ACCESS,
    PID_TAG_ACCESS_LEVEL,
    PID_TAG_HAS_ATTACHMENTS,
    PID_TAG_MESSAGE_SIZE,
    PID_TAG_MESSAGE_SIZE_EXTENDED,
    PID_TAG_DISPLAY_NAME_W,
];

pub(super) fn imported_event_identity_from_properties(
    properties: &HashMap<u32, MapiValue>,
) -> Result<Option<MapiEventImportedIdentity>> {
    let Some(source_key) = properties.get(&PID_TAG_SOURCE_KEY) else {
        return Ok(None);
    };
    let MapiValue::Binary(source_key) = source_key else {
        bail!("imported Event SourceKey is not binary");
    };
    if persistable_import_source_key_global_counter(source_key).is_none() {
        bail!("imported Event SourceKey is outside the local dynamic identity range");
    }
    let change_key = match properties.get(&PID_TAG_CHANGE_KEY) {
        Some(MapiValue::Binary(change_key)) => change_key.clone(),
        _ => bail!("imported Event ChangeKey is missing or not binary"),
    };
    let predecessor_change_list = match properties.get(&PID_TAG_PREDECESSOR_CHANGE_LIST) {
        Some(MapiValue::Binary(predecessor_change_list)) => predecessor_change_list.clone(),
        _ => bail!("imported Event PCL is missing or not binary"),
    };
    let last_modification_time = properties
        .get(&PID_TAG_LAST_MODIFICATION_TIME)
        .and_then(MapiValue::as_i64)
        .ok_or_else(|| anyhow!("imported Event LastModificationTime is missing or invalid"))?;
    let last_modification_time = u64::try_from(last_modification_time)
        .map_err(|_| anyhow!("imported Event LastModificationTime cannot be negative"))?;
    let last_modification_time = last_modification_time - last_modification_time % 10;
    Ok(Some(MapiEventImportedIdentity {
        source_key: source_key.clone(),
        change_key,
        predecessor_change_list,
        last_modification_time,
    }))
}

pub(super) fn stage_event_property_values(
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    snapshot: &MapiMailStoreSnapshot,
    values: Vec<(u32, MapiValue)>,
) -> Result<Vec<(usize, u32, u32)>> {
    let Some(MapiObject::Event {
        folder_id,
        event_id,
        transaction,
    }) = input_object_mut(session, handle_slots, request)
    else {
        bail!("MAPI Event handle was not found");
    };
    let event = snapshot
        .event_for_id(*folder_id, *event_id)
        .ok_or_else(|| anyhow!("canonical MAPI calendar event was not found"))?;
    if !event_handle_is_writable(transaction.open_mode_flags, event.event.rights.may_write) {
        bail!("MAPI Event handle is not writable");
    }
    if transaction.import_disposition != MapiEventImportDisposition::Apply {
        return Ok(Vec::new());
    }
    let is_imported_event = transaction.imported_identity.is_some();

    let values = values
        .into_iter()
        .enumerate()
        .map(|(index, (tag, value))| {
            (
                index,
                tag,
                canonical_calendar_property_storage_tag(tag),
                value,
            )
        })
        .collect::<Vec<_>>();
    let mut problems = values
        .iter()
        .filter(|(_, _, storage_tag, _)| {
            event_property_reports_server_managed_problem(*storage_tag, is_imported_event)
                || is_unsupported_calendar_passthrough_property_tag(*storage_tag)
        })
        .map(|(index, tag, _, _)| (*index, *tag, 0x8004_0102))
        .collect::<Vec<_>>();
    let values = values
        .into_iter()
        .filter(|(_, _, storage_tag, _)| !event_property_is_server_managed(*storage_tag))
        .filter(|(_, _, storage_tag, _)| {
            !is_unsupported_calendar_passthrough_property_tag(*storage_tag)
        })
        .collect::<Vec<_>>();
    if values.is_empty() {
        return Ok(problems);
    }
    let staged_values = values
        .iter()
        .map(|(_, _, storage_tag, value)| (*storage_tag, value.clone()))
        .collect::<Vec<_>>();
    let mut merged = transaction.pending_properties.clone();
    let mut merged_deleted = transaction.deleted_properties.clone();
    apply_event_property_values(&mut merged, &mut merged_deleted, &staged_values);
    if validate_staged_event_property_values(event, merged, &merged_deleted).is_ok() {
        apply_event_property_values(
            &mut transaction.pending_properties,
            &mut transaction.deleted_properties,
            &staged_values,
        );
        return Ok(problems);
    }

    let mut staged_indices = HashSet::new();
    for coupled_tags in [
        EVENT_TEMPORAL_PROPERTIES,
        EVENT_REMINDER_PROPERTIES,
        EVENT_RESPONSE_PROPERTIES,
    ] {
        let coupled = values
            .iter()
            .filter(|(_, _, storage_tag, _)| coupled_tags.contains(storage_tag))
            .collect::<Vec<_>>();
        if coupled.is_empty() {
            continue;
        }
        let candidates = coupled
            .iter()
            .map(|(_, _, storage_tag, value)| (*storage_tag, value.clone()))
            .collect::<Vec<_>>();
        let mut merged = transaction.pending_properties.clone();
        let mut merged_deleted = transaction.deleted_properties.clone();
        apply_event_property_values(&mut merged, &mut merged_deleted, &candidates);
        if validate_staged_event_property_values(event, merged, &merged_deleted).is_ok() {
            apply_event_property_values(
                &mut transaction.pending_properties,
                &mut transaction.deleted_properties,
                &candidates,
            );
        } else {
            for (index, tag, _, _) in &coupled {
                problems.push((*index, *tag, 0x8004_0102));
            }
        }
        staged_indices.extend(coupled.into_iter().map(|(index, _, _, _)| *index));
    }
    for (index, tag, storage_tag, value) in values {
        if staged_indices.contains(&index) {
            continue;
        }
        let candidate = [(storage_tag, value)];
        let mut merged = transaction.pending_properties.clone();
        let mut merged_deleted = transaction.deleted_properties.clone();
        apply_event_property_values(&mut merged, &mut merged_deleted, &candidate);
        if validate_staged_event_property_values(event, merged, &merged_deleted).is_err() {
            problems.push((index, tag, 0x8004_0102));
            continue;
        }
        apply_event_property_values(
            &mut transaction.pending_properties,
            &mut transaction.deleted_properties,
            &candidate,
        );
    }
    problems.sort_unstable_by_key(|(index, _, _)| *index);
    Ok(problems)
}

pub(super) fn stage_pending_event_property_values(
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    principal: &AccountPrincipal,
    values: Vec<(u32, MapiValue)>,
) -> Result<Vec<(usize, u32, u32)>> {
    let Some(MapiObject::PendingEvent { properties, .. }) =
        input_object_mut(session, handle_slots, request)
    else {
        bail!("MAPI PendingEvent handle was not found");
    };
    let is_imported_event = imported_event_identity_from_properties(properties)
        .ok()
        .flatten()
        .is_some();
    let values = values
        .into_iter()
        .enumerate()
        .map(|(index, (tag, value))| {
            (
                index,
                tag,
                canonical_calendar_property_storage_tag(tag),
                value,
            )
        })
        .collect::<Vec<_>>();
    let mut problems = values
        .iter()
        .filter(|(_, _, storage_tag, _)| {
            event_property_reports_server_managed_problem(*storage_tag, is_imported_event)
                || is_unsupported_calendar_passthrough_property_tag(*storage_tag)
        })
        .map(|(index, tag, _, _)| (*index, *tag, 0x8004_0102))
        .collect::<Vec<_>>();
    let values = values
        .into_iter()
        .filter(|(_, _, storage_tag, _)| {
            !event_property_is_server_managed(*storage_tag)
                || (is_imported_event && *storage_tag == PID_TAG_SEARCH_KEY)
        })
        .filter(|(_, _, storage_tag, _)| {
            !is_unsupported_calendar_passthrough_property_tag(*storage_tag)
        })
        .collect::<Vec<_>>();
    if values.is_empty() {
        return Ok(problems);
    }
    let staged_values = values
        .iter()
        .map(|(_, _, storage_tag, value)| (*storage_tag, value.clone()))
        .collect::<Vec<_>>();
    let mut merged = properties.clone();
    apply_event_property_values(&mut merged, &mut HashSet::new(), &staged_values);
    if validate_pending_event_property_values(principal.account_id, merged).is_ok() {
        apply_event_property_values(properties, &mut HashSet::new(), &staged_values);
        return Ok(problems);
    }

    let mut staged_indices = HashSet::new();
    for coupled_tags in [
        EVENT_TEMPORAL_PROPERTIES,
        EVENT_REMINDER_PROPERTIES,
        EVENT_RESPONSE_PROPERTIES,
    ] {
        let coupled = values
            .iter()
            .filter(|(_, _, storage_tag, _)| coupled_tags.contains(storage_tag))
            .collect::<Vec<_>>();
        if coupled.is_empty() {
            continue;
        }
        let candidates = coupled
            .iter()
            .map(|(_, _, storage_tag, value)| (*storage_tag, value.clone()))
            .collect::<Vec<_>>();
        let mut merged = properties.clone();
        apply_event_property_values(&mut merged, &mut HashSet::new(), &candidates);
        if validate_pending_event_property_values(principal.account_id, merged).is_ok() {
            apply_event_property_values(properties, &mut HashSet::new(), &candidates);
        } else {
            problems.extend(
                coupled
                    .iter()
                    .map(|(index, tag, _, _)| (*index, *tag, 0x8004_0102)),
            );
        }
        staged_indices.extend(coupled.into_iter().map(|(index, _, _, _)| *index));
    }
    for (index, tag, storage_tag, value) in values {
        if staged_indices.contains(&index) {
            continue;
        }
        let candidate = [(storage_tag, value)];
        let mut merged = properties.clone();
        apply_event_property_values(&mut merged, &mut HashSet::new(), &candidate);
        if validate_pending_event_property_values(principal.account_id, merged).is_err() {
            problems.push((index, tag, 0x8004_0102));
            continue;
        }
        apply_event_property_values(properties, &mut HashSet::new(), &candidate);
    }
    problems.sort_unstable_by_key(|(index, _, _)| *index);
    Ok(problems)
}

pub(super) fn pending_event_import_properties(
    values: Vec<(u32, MapiValue)>,
) -> Result<HashMap<u32, MapiValue>> {
    let values = values
        .into_iter()
        .map(|(tag, value)| (canonical_calendar_property_storage_tag(tag), value))
        .collect::<Vec<_>>();
    if values
        .iter()
        .any(|(tag, _)| is_unsupported_calendar_passthrough_property_tag(*tag))
    {
        bail!("unsupported Calendar property in ImportMessageChange");
    }
    let mut properties = HashMap::new();
    apply_event_property_values(&mut properties, &mut HashSet::new(), &values);
    validate_calendar_passthrough_property_values(&properties)?;
    Ok(properties)
}

pub(super) fn apply_pending_event_fast_transfer_property_values(
    account_id: Uuid,
    properties: &mut HashMap<u32, MapiValue>,
    values: Vec<(u32, MapiValue)>,
) -> Result<()> {
    let is_imported_event = imported_event_identity_from_properties(properties)
        .ok()
        .flatten()
        .is_some();
    let mut staged = Vec::new();
    for (tag, value) in values {
        let storage_tag = canonical_calendar_property_storage_tag(tag);
        if is_unsupported_calendar_passthrough_property_tag(storage_tag) {
            bail!("unsupported Calendar property in FastTransfer upload");
        }
        if event_property_is_server_managed(storage_tag) {
            if !event_property_has_valid_server_managed_type(storage_tag) {
                bail!("invalid server-managed Calendar property type in FastTransfer upload");
            }
            if is_imported_event && storage_tag == PID_TAG_SEARCH_KEY {
                staged.push((storage_tag, value));
            }
            continue;
        }
        staged.push((storage_tag, value));
    }
    let mut merged = properties.clone();
    apply_event_property_values(&mut merged, &mut HashSet::new(), &staged);
    validate_incremental_fast_transfer_event_property_values(account_id, merged.clone())?;
    *properties = merged;
    Ok(())
}

fn validate_incremental_fast_transfer_event_property_values(
    account_id: Uuid,
    merged: HashMap<u32, MapiValue>,
) -> Result<()> {
    for tag in EVENT_RESPONSE_PROPERTIES {
        if let Some(value) = merged.get(tag) {
            if !matches!(value, MapiValue::Bool(_)) {
                bail!("invalid Calendar response-request property value");
            }
        }
    }
    let mut independently_validated = merged.clone();
    for tag in EVENT_RESPONSE_PROPERTIES {
        independently_validated.remove(tag);
    }
    validate_calendar_passthrough_property_values(&independently_validated)?;
    validate_pending_event_canonical_property_values(account_id, merged)
}

fn validate_pending_event_property_values(
    account_id: Uuid,
    merged: HashMap<u32, MapiValue>,
) -> Result<()> {
    validate_calendar_passthrough_property_values(&merged)?;
    validate_pending_event_canonical_property_values(account_id, merged)
}

fn validate_calendar_passthrough_property_values(
    properties: &HashMap<u32, MapiValue>,
) -> Result<()> {
    if let Some(value) = properties.get(&PID_LID_APPOINTMENT_COLOR_TAG) {
        if !matches!(value, MapiValue::I32(color) if (0..=10).contains(color)) {
            bail!("invalid MAPI appointment color property value");
        }
    }
    validate_calendar_passthrough_invariants(properties)
}

fn validate_pending_event_canonical_property_values(
    account_id: Uuid,
    merged: HashMap<u32, MapiValue>,
) -> Result<()> {
    let (property_values, _, _) = split_reminder_property_values(merged.into_iter().collect())?;
    let (canonical_values, _): (Vec<_>, Vec<_>) = property_values
        .into_iter()
        .partition(|(tag, _)| !is_calendar_passthrough_property_tag(*tag));
    validate_calendar_event_input_for_staging(
        account_id,
        DEFAULT_CALENDAR_COLLECTION_ID,
        &canonical_values.into_iter().collect(),
    )
}

pub(super) fn stage_pending_event_property_deletions(
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    principal: &AccountPrincipal,
    property_tags: &[u32],
) -> Result<Vec<(usize, u32, u32)>> {
    let Some(MapiObject::PendingEvent { properties, .. }) =
        input_object_mut(session, handle_slots, request)
    else {
        bail!("MAPI PendingEvent handle was not found");
    };
    let values = property_tags
        .iter()
        .copied()
        .enumerate()
        .map(|(index, tag)| (index, tag, canonical_calendar_property_storage_tag(tag)))
        .collect::<Vec<_>>();
    let mut problems = values
        .iter()
        .filter(|(_, _, storage_tag)| {
            event_property_is_server_managed(*storage_tag)
                || is_calendar_named_passthrough_property_id(*storage_tag)
                    && !is_calendar_passthrough_property_tag(*storage_tag)
                || is_unsupported_calendar_passthrough_property_tag(*storage_tag)
                    && !is_custom_property_tag(*storage_tag)
        })
        .map(|(index, tag, _)| (*index, *tag, 0x8004_0102))
        .collect::<Vec<_>>();
    let values = values
        .into_iter()
        .filter(|(_, _, storage_tag)| {
            !event_property_is_server_managed(*storage_tag)
                && (!is_calendar_named_passthrough_property_id(*storage_tag)
                    || is_calendar_passthrough_property_tag(*storage_tag))
                && (!is_unsupported_calendar_passthrough_property_tag(*storage_tag)
                    || is_custom_property_tag(*storage_tag))
        })
        .collect::<Vec<_>>();
    if values.is_empty() {
        return Ok(problems);
    }

    let mut merged = properties.clone();
    remove_pending_event_property_values(
        &mut merged,
        &values
            .iter()
            .map(|(_, _, storage_tag)| *storage_tag)
            .collect::<Vec<_>>(),
    );
    if validate_pending_event_property_values(principal.account_id, merged.clone()).is_ok() {
        *properties = merged;
        return Ok(problems);
    }

    let mut staged_indices = HashSet::new();
    for coupled_tags in [
        EVENT_TEMPORAL_PROPERTIES,
        EVENT_REMINDER_PROPERTIES,
        EVENT_RESPONSE_PROPERTIES,
    ] {
        let coupled = values
            .iter()
            .filter(|(_, _, storage_tag)| coupled_tags.contains(storage_tag))
            .collect::<Vec<_>>();
        if coupled.is_empty() {
            continue;
        }
        let mut candidate = properties.clone();
        remove_pending_event_property_values(
            &mut candidate,
            &coupled
                .iter()
                .map(|(_, _, storage_tag)| *storage_tag)
                .collect::<Vec<_>>(),
        );
        if validate_pending_event_property_values(principal.account_id, candidate.clone()).is_ok() {
            *properties = candidate;
        } else {
            problems.extend(
                coupled
                    .iter()
                    .map(|(index, tag, _)| (*index, *tag, 0x8004_0102)),
            );
        }
        staged_indices.extend(coupled.into_iter().map(|(index, _, _)| *index));
    }
    for (index, tag, storage_tag) in values {
        if staged_indices.contains(&index) {
            continue;
        }
        let mut candidate = properties.clone();
        remove_pending_event_property_values(&mut candidate, &[storage_tag]);
        if validate_pending_event_property_values(principal.account_id, candidate.clone()).is_ok() {
            *properties = candidate;
        } else {
            problems.push((index, tag, 0x8004_0102));
        }
    }
    problems.sort_unstable_by_key(|(index, _, _)| *index);
    Ok(problems)
}

fn remove_pending_event_property_values(properties: &mut HashMap<u32, MapiValue>, tags: &[u32]) {
    for storage_tag in tags {
        let aliases = if EVENT_SUBJECT_ALIASES.contains(storage_tag) {
            EVENT_SUBJECT_ALIASES
        } else if EVENT_LOCATION_ALIASES.contains(storage_tag) {
            EVENT_LOCATION_ALIASES
        } else if EVENT_HTML_ALIASES.contains(storage_tag) {
            EVENT_HTML_ALIASES
        } else {
            std::slice::from_ref(storage_tag)
        };
        for alias in aliases {
            properties.remove(alias);
        }
    }
}

fn apply_event_property_values(
    pending: &mut HashMap<u32, MapiValue>,
    deleted: &mut HashSet<u32>,
    values: &[(u32, MapiValue)],
) {
    for aliases in [
        EVENT_SUBJECT_ALIASES,
        EVENT_LOCATION_ALIASES,
        EVENT_HTML_ALIASES,
    ] {
        if values.iter().any(|(tag, _)| aliases.contains(tag)) {
            for tag in aliases {
                pending.remove(tag);
                deleted.remove(tag);
            }
        }
    }
    for (tag, value) in values {
        pending.insert(*tag, value.clone());
        deleted.remove(tag);
    }
    if let Some((_, value)) = values
        .iter()
        .rev()
        .find(|(tag, _)| *tag == PID_TAG_SUBJECT_W)
        .or_else(|| {
            values
                .iter()
                .rev()
                .find(|(tag, _)| *tag == PID_TAG_NORMALIZED_SUBJECT_W)
        })
    {
        for tag in EVENT_SUBJECT_ALIASES {
            pending.insert(*tag, value.clone());
        }
    }
    if let Some((_, value)) = values
        .iter()
        .rev()
        .find(|(tag, _)| EVENT_LOCATION_ALIASES.contains(tag))
    {
        for tag in EVENT_LOCATION_ALIASES {
            pending.insert(*tag, value.clone());
        }
    }
    if let Some((_, value)) = values
        .iter()
        .rev()
        .find(|(tag, _)| EVENT_HTML_ALIASES.contains(tag))
    {
        match value {
            MapiValue::String(value) => {
                pending.insert(PID_TAG_BODY_HTML_W, MapiValue::String(value.clone()));
                pending.insert(
                    PID_TAG_HTML_BINARY,
                    MapiValue::Binary(value.as_bytes().to_vec()),
                );
            }
            MapiValue::Binary(value) => {
                pending.insert(PID_TAG_HTML_BINARY, MapiValue::Binary(value.clone()));
                if let Ok(value) = String::from_utf8(value.clone()) {
                    pending.insert(PID_TAG_BODY_HTML_W, MapiValue::String(value));
                }
            }
            _ => {}
        }
    }
}

pub(in crate::mapi) fn event_property_is_server_managed(tag: u32) -> bool {
    let property_id = tag & 0xFFFF_0000;
    EVENT_SERVER_MANAGED_PROPERTIES
        .iter()
        .any(|managed_tag| *managed_tag & 0xFFFF_0000 == property_id)
}

fn event_property_has_valid_server_managed_type(tag: u32) -> bool {
    EVENT_SERVER_MANAGED_PROPERTIES.contains(&canonical_calendar_property_storage_tag(tag))
}

// [MS-OXCFXICS] 2.2.3.2.4.2.1 and 3.3.4.3.3.2.2.1 require the
// import header's modification time and then a full property copy. Outlook
// repeats that value and the appointment SearchKey. [MS-OXCPRPT] 3.2.5.4
// permits ignoring read-only changes; [MS-OXCMSG] 2.2 product note <1>
// records Exchange's SearchKey exception. LPE bounds that compatibility to
// the pre-first-Save value observed in the Probe B/C/D traces.
fn event_property_reports_server_managed_problem(tag: u32, is_imported_event: bool) -> bool {
    event_property_is_server_managed(tag)
        && !(is_imported_event
            && event_property_has_valid_server_managed_type(tag)
            && matches!(tag, PID_TAG_LAST_MODIFICATION_TIME | PID_TAG_SEARCH_KEY))
}

fn validate_staged_event_property_values(
    event: &crate::mapi_store::MapiEvent,
    merged: HashMap<u32, MapiValue>,
    deleted: &HashSet<u32>,
) -> Result<()> {
    validate_effective_calendar_passthrough_invariants(event, &merged, deleted)?;
    let (property_values, _, _) = split_reminder_property_values(merged.into_iter().collect())?;
    let (canonical_values, _): (Vec<_>, Vec<_>) = property_values
        .into_iter()
        .partition(|(tag, _)| !is_calendar_passthrough_property_tag(*tag));
    let canonical_values = canonical_values.into_iter().collect::<HashMap<_, _>>();
    if !canonical_values.is_empty()
        && !bounded_meeting_cancellation_from_mapi(&canonical_values)?
        && meeting_response_event_input_from_mapi(
            event.event.owner_account_id,
            Some(event.canonical_id),
            &event.event,
            &canonical_values,
        )?
        .is_none()
    {
        event_input_from_mapi(
            event.event.owner_account_id,
            Some(event.canonical_id),
            &event.event,
            &canonical_values,
        )?;
    }

    Ok(())
}

fn validate_effective_calendar_passthrough_invariants(
    event: &crate::mapi_store::MapiEvent,
    pending: &HashMap<u32, MapiValue>,
    deleted: &HashSet<u32>,
) -> Result<()> {
    let mut effective = HashMap::new();
    for value in &event.stored_properties {
        if !matches!(
            value.property_tag,
            PID_TAG_RESPONSE_REQUESTED | PID_TAG_REPLY_REQUESTED | PID_LID_APPOINTMENT_COLOR_TAG
        ) || deleted.contains(&value.property_tag)
        {
            continue;
        }
        let mut cursor = Cursor::new(&value.property_value);
        if let Ok(parsed) = parse_mapi_property_value(&mut cursor, value.property_tag) {
            if cursor.remaining() == 0 {
                effective.insert(value.property_tag, parsed);
            }
        }
    }
    effective.extend(pending.iter().filter_map(|(tag, value)| {
        matches!(
            *tag,
            PID_TAG_RESPONSE_REQUESTED | PID_TAG_REPLY_REQUESTED | PID_LID_APPOINTMENT_COLOR_TAG
        )
        .then(|| (*tag, value.clone()))
    }));
    validate_calendar_passthrough_property_values(&effective)
}

pub(super) fn stage_event_property_deletions(
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    snapshot: &MapiMailStoreSnapshot,
    property_tags: &[u32],
) -> Result<Vec<(usize, u32, u32)>> {
    let Some(MapiObject::Event {
        folder_id,
        event_id,
        transaction,
    }) = input_object_mut(session, handle_slots, request)
    else {
        bail!("MAPI Event handle was not found");
    };
    let event = snapshot
        .event_for_id(*folder_id, *event_id)
        .ok_or_else(|| anyhow!("canonical MAPI calendar event was not found"))?;
    if !event_handle_is_writable(transaction.open_mode_flags, event.event.rights.may_write) {
        bail!("MAPI Event handle is not writable");
    }
    if transaction.import_disposition != MapiEventImportDisposition::Apply {
        return Ok(Vec::new());
    }

    let reminder = snapshot.reminder_for_source("calendar", event.canonical_id);
    let mut problems = Vec::new();
    let requested_response_deletions = property_tags
        .iter()
        .enumerate()
        .filter(|(_, tag)| EVENT_RESPONSE_PROPERTIES.contains(tag))
        .map(|(index, tag)| (index, *tag))
        .collect::<Vec<_>>();
    let reject_response_deletions = if requested_response_deletions.is_empty() {
        false
    } else {
        let mut candidate_pending = transaction.pending_properties.clone();
        let mut candidate_deleted = transaction.deleted_properties.clone();
        for (_, tag) in &requested_response_deletions {
            candidate_pending.remove(tag);
            candidate_deleted.insert(*tag);
        }
        validate_effective_calendar_passthrough_invariants(
            event,
            &candidate_pending,
            &candidate_deleted,
        )
        .is_err()
    };
    for (index, tag) in property_tags.iter().enumerate() {
        let storage_tag = canonical_calendar_property_storage_tag(*tag);
        if reject_response_deletions && EVENT_RESPONSE_PROPERTIES.contains(&storage_tag) {
            problems.push((index, *tag, 0x8004_0102));
            continue;
        }
        if event_property_is_server_managed(storage_tag) {
            problems.push((index, *tag, 0x8004_0102));
            continue;
        }
        if is_calendar_named_passthrough_property_id(storage_tag)
            && !is_calendar_passthrough_property_tag(storage_tag)
        {
            problems.push((index, *tag, 0x8004_0102));
            continue;
        }
        if stage_clearable_event_property_deletion(transaction, storage_tag) {
            continue;
        }
        if matches!(
            storage_tag,
            PID_LID_REMINDER_DELTA_TAG
                | PID_LID_REMINDER_TIME_TAG
                | PID_LID_REMINDER_SIGNAL_TIME_TAG
        ) {
            if staged_event_reminder_is_active(transaction, reminder) {
                problems.push((index, *tag, 0x8004_0102));
            } else {
                transaction.pending_properties.remove(&storage_tag);
                transaction.deleted_properties.insert(storage_tag);
            }
            continue;
        }
        if !is_custom_property_tag(storage_tag)
            && event_property_value_with_reminder(
                &event.event,
                event.id,
                event.folder_id,
                storage_tag,
                reminder,
            )
            .is_some()
        {
            problems.push((index, *tag, 0x8004_0102));
            continue;
        }
        transaction.pending_properties.remove(&storage_tag);
        transaction.deleted_properties.insert(storage_tag);
    }
    Ok(problems)
}

fn stage_clearable_event_property_deletion(
    transaction: &mut MapiEventTransaction,
    storage_tag: u32,
) -> bool {
    let clear_text = MapiValue::String(String::new());
    match storage_tag {
        PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
            for tag in EVENT_SUBJECT_ALIASES {
                transaction
                    .pending_properties
                    .insert(*tag, clear_text.clone());
                transaction.deleted_properties.insert(*tag);
            }
        }
        PID_LID_LOCATION_W_TAG => {
            transaction
                .pending_properties
                .insert(PID_LID_LOCATION_W_TAG, clear_text);
            transaction
                .deleted_properties
                .insert(PID_LID_LOCATION_W_TAG);
        }
        PID_TAG_BODY_W => {
            transaction
                .pending_properties
                .insert(PID_TAG_BODY_W, clear_text);
            transaction.deleted_properties.insert(PID_TAG_BODY_W);
        }
        PID_TAG_BODY_HTML_W | PID_TAG_HTML_BINARY => {
            transaction
                .pending_properties
                .insert(PID_TAG_BODY_HTML_W, clear_text);
            transaction
                .pending_properties
                .insert(PID_TAG_HTML_BINARY, MapiValue::Binary(Vec::new()));
            transaction
                .deleted_properties
                .extend([PID_TAG_BODY_HTML_W, PID_TAG_HTML_BINARY]);
        }
        PID_LID_REMINDER_SET_TAG => {
            transaction
                .pending_properties
                .insert(PID_LID_REMINDER_SET_TAG, MapiValue::Bool(false));
            // The canonical Event can represent disabling a reminder, but not
            // independently removing Delta/Time/Signal while keeping it active.
            transaction
                .deleted_properties
                .insert(PID_LID_REMINDER_SET_TAG);
        }
        _ => return false,
    }
    true
}

fn staged_event_reminder_is_active(
    transaction: &MapiEventTransaction,
    reminder: Option<&lpe_storage::ClientReminder>,
) -> bool {
    transaction
        .pending_properties
        .get(&PID_LID_REMINDER_SET_TAG)
        .and_then(MapiValue::as_bool)
        .unwrap_or(reminder.is_some())
}

pub(super) fn event_handle_is_writable(open_mode_flags: u8, may_write: bool) -> bool {
    may_write && matches!(open_mode_flags & 0x03, 0x01 | 0x03)
}

pub(super) fn event_open_mode_after_save(disposition: SaveDisposition) -> Option<u8> {
    match disposition {
        SaveDisposition::Default => None,
        SaveDisposition::KeepOpenReadOnly => Some(0x00),
        SaveDisposition::KeepOpenReadWrite | SaveDisposition::ForceSave => Some(0x01),
    }
}

#[cfg(test)]
mod calendar_search_key_tests {
    use super::*;

    #[test]
    fn saved_calendar_search_key_is_immutable_and_repeated_import_is_ignored() {
        assert!(event_property_is_server_managed(PID_TAG_SEARCH_KEY));
        for property_tag in [
            PID_TAG_ENTRY_ID,
            PID_TAG_PARENT_ENTRY_ID,
            PID_TAG_INSTANCE_KEY,
            PID_TAG_RECORD_KEY,
            PID_TAG_FOLDER_ID,
            PID_TAG_PARENT_FOLDER_ID,
            PID_TAG_MID,
            PID_TAG_INST_ID,
            PID_TAG_INSTANCE_NUM,
            PID_TAG_PARENT_SOURCE_KEY,
            PID_TAG_ACCESS,
            PID_TAG_ACCESS_LEVEL,
            PID_TAG_HAS_ATTACHMENTS,
            PID_TAG_MESSAGE_SIZE,
            PID_TAG_MESSAGE_SIZE_EXTENDED,
        ] {
            assert!(event_property_is_server_managed(property_tag));
        }
        assert!(event_property_reports_server_managed_problem(
            PID_TAG_SEARCH_KEY,
            false
        ));
        assert!(!event_property_reports_server_managed_problem(
            PID_TAG_SEARCH_KEY,
            true
        ));
        assert!(event_property_reports_server_managed_problem(
            (PID_TAG_SEARCH_KEY & 0xFFFF_0000) | 0x001F,
            true
        ));
        assert!(event_property_is_server_managed(
            (PID_TAG_ENTRY_ID & 0xFFFF_0000) | 0x001F
        ));
        assert!(event_property_is_server_managed(
            (PID_TAG_DISPLAY_NAME_W & 0xFFFF_0000) | 0x0102
        ));
        assert!(event_property_has_valid_server_managed_type(
            (PID_TAG_DISPLAY_NAME_W & 0xFFFF_0000) | 0x001E
        ));
        assert!(!event_property_has_valid_server_managed_type(
            (PID_TAG_DISPLAY_NAME_W & 0xFFFF_0000) | 0x0102
        ));
    }

    #[test]
    fn calendar_set_and_delete_reject_owned_wrong_types_and_preserve_valid_aliases() {
        let principal = super::tests::test_principal();
        let mut session = super::tests::test_mapi_session();
        let event_handle = session.allocate_output_handle(
            Some(0),
            MapiObject::PendingEvent {
                folder_id: CALENDAR_FOLDER_ID,
                properties: HashMap::new(),
                recipients: Vec::new(),
                recipients_modified: false,
                fail_on_conflict: false,
            },
        );
        let set_request = RopRequest {
            rop_id: RopId::SetProperties.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: Vec::new(),
        };
        let subject_string8 = (PID_TAG_SUBJECT_W & 0xFFFF_0000) | 0x001E;
        let color_string = (PID_LID_APPOINTMENT_COLOR_TAG & 0xFFFF_0000) | 0x001F;
        let search_key_string = (PID_TAG_SEARCH_KEY & 0xFFFF_0000) | 0x001F;
        let entry_id_string = (PID_TAG_ENTRY_ID & 0xFFFF_0000) | 0x001F;
        let display_name_binary = (PID_TAG_DISPLAY_NAME_W & 0xFFFF_0000) | 0x0102;

        let problems = stage_pending_event_property_values(
            &mut session,
            &[event_handle],
            &set_request,
            &principal,
            vec![
                (PID_LID_APPOINTMENT_COLOR_TAG, MapiValue::I32(7)),
                (
                    subject_string8,
                    MapiValue::String("String8 alias".to_string()),
                ),
                (PID_LID_APPOINTMENT_COLOR_TAG, MapiValue::I32(11)),
                (PID_LID_APPOINTMENT_COLOR_TAG, MapiValue::Bool(true)),
                (color_string, MapiValue::String("invalid color".to_string())),
                (
                    search_key_string,
                    MapiValue::String("invalid key".to_string()),
                ),
                (
                    entry_id_string,
                    MapiValue::String("invalid identity".to_string()),
                ),
                (display_name_binary, MapiValue::Binary(vec![1, 2, 3])),
            ],
        )
        .unwrap();
        assert_eq!(
            problems,
            vec![
                (2, PID_LID_APPOINTMENT_COLOR_TAG, 0x8004_0102),
                (3, PID_LID_APPOINTMENT_COLOR_TAG, 0x8004_0102),
                (4, color_string, 0x8004_0102),
                (5, search_key_string, 0x8004_0102),
                (6, entry_id_string, 0x8004_0102),
                (7, display_name_binary, 0x8004_0102),
            ]
        );
        let MapiObject::PendingEvent { properties, .. } =
            session.handles.get(&event_handle).unwrap()
        else {
            panic!("pending Calendar handle changed kind")
        };
        assert_eq!(
            properties.get(&PID_LID_APPOINTMENT_COLOR_TAG),
            Some(&MapiValue::I32(7))
        );
        assert_eq!(
            properties.get(&PID_TAG_SUBJECT_W),
            Some(&MapiValue::String("String8 alias".to_string()))
        );

        let delete_request = RopRequest {
            rop_id: RopId::DeleteProperties.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: Vec::new(),
        };
        let problems = stage_pending_event_property_deletions(
            &mut session,
            &[event_handle],
            &delete_request,
            &principal,
            &[
                color_string,
                search_key_string,
                entry_id_string,
                display_name_binary,
                PID_LID_APPOINTMENT_COLOR_TAG,
            ],
        )
        .unwrap();
        assert_eq!(
            problems,
            vec![
                (0, color_string, 0x8004_0102),
                (1, search_key_string, 0x8004_0102),
                (2, entry_id_string, 0x8004_0102),
                (3, display_name_binary, 0x8004_0102),
            ]
        );
        let MapiObject::PendingEvent { properties, .. } =
            session.handles.get(&event_handle).unwrap()
        else {
            panic!("pending Calendar handle changed kind")
        };
        assert!(!properties.contains_key(&PID_LID_APPOINTMENT_COLOR_TAG));
    }

    #[test]
    fn calendar_response_and_reply_flags_must_match_effective_stored_state() {
        let canonical_id = Uuid::from_u128(0x202608111137);
        let item_id = crate::mapi::identity::mapi_store_id(0x1137);
        let change_number = mapi_mailstore::change_number_for_store_id(item_id);
        let event = crate::mapi_store::MapiEvent {
            id: item_id,
            source_key: mapi_mailstore::source_key_for_store_id(item_id),
            folder_id: CALENDAR_FOLDER_ID,
            canonical_id,
            event: default_event_for_mapping(Uuid::nil(), DEFAULT_CALENDAR_COLLECTION_ID),
            version: lpe_storage::MapiEventVersion {
                event_id: canonical_id,
                canonical_modseq: 1,
                change_number,
                search_key: None,
                change_key: mapi_mailstore::change_key_for_change_number(change_number),
                predecessor_change_list: mapi_mailstore::predecessor_change_list(change_number),
                last_modification_time: mapi_mailstore::filetime_from_rfc3339_utc(
                    "2026-08-11T11:37:00Z",
                ),
                created_at: "2026-08-11T11:37:00Z".to_string(),
                updated_at: "2026-08-11T11:37:00Z".to_string(),
            },
            attachments: Vec::new(),
            stored_properties: vec![crate::store::MapiCustomPropertyValue {
                property_tag: PID_TAG_REPLY_REQUESTED,
                property_type: MapiPropertyType::Boolean.as_u16(),
                property_value: vec![1],
            }],
        };

        assert!(validate_effective_calendar_passthrough_invariants(
            &event,
            &HashMap::from([(PID_TAG_RESPONSE_REQUESTED, MapiValue::Bool(false))]),
            &HashSet::new(),
        )
        .is_err());
        assert!(validate_effective_calendar_passthrough_invariants(
            &event,
            &HashMap::from([(PID_TAG_RESPONSE_REQUESTED, MapiValue::Bool(true))]),
            &HashSet::new(),
        )
        .is_ok());
        assert!(validate_effective_calendar_passthrough_invariants(
            &event,
            &HashMap::new(),
            &HashSet::from([PID_TAG_RESPONSE_REQUESTED]),
        )
        .is_err());
        assert!(validate_effective_calendar_passthrough_invariants(
            &event,
            &HashMap::new(),
            &HashSet::from([PID_TAG_RESPONSE_REQUESTED, PID_TAG_REPLY_REQUESTED]),
        )
        .is_ok());
    }

    #[test]
    fn pending_calendar_rsvp_pair_survives_unrelated_property_problem() {
        let principal = super::tests::test_principal();
        let mut session = super::tests::test_mapi_session();
        let event_handle = session.allocate_output_handle(
            Some(0),
            MapiObject::PendingEvent {
                folder_id: CALENDAR_FOLDER_ID,
                properties: HashMap::new(),
                recipients: Vec::new(),
                recipients_modified: false,
                fail_on_conflict: false,
            },
        );
        let request = RopRequest {
            rop_id: RopId::SetProperties.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: Vec::new(),
        };
        let values = vec![
            (PID_TAG_RESPONSE_REQUESTED, MapiValue::Bool(true)),
            (PID_TAG_REPLY_REQUESTED, MapiValue::Bool(true)),
            (0x9100_1003, MapiValue::MultiI32(vec![1, 2])),
        ];

        let problems = stage_pending_event_property_values(
            &mut session,
            &[event_handle],
            &request,
            &principal,
            values,
        )
        .unwrap();

        assert_eq!(problems, vec![(2, 0x9100_1003, 0x8004_0102)]);
        let MapiObject::PendingEvent { properties, .. } =
            session.handles.get(&event_handle).unwrap()
        else {
            panic!("pending Calendar handle changed kind")
        };
        assert_eq!(
            properties.get(&PID_TAG_RESPONSE_REQUESTED),
            Some(&MapiValue::Bool(true))
        );
        assert_eq!(
            properties.get(&PID_TAG_REPLY_REQUESTED),
            Some(&MapiValue::Bool(true))
        );
        assert!(!properties.contains_key(&0x9100_1003));
    }

    #[test]
    fn pending_calendar_deletes_normalize_aliases_and_keep_rsvp_atomic() {
        let principal = super::tests::test_principal();
        let mut session = super::tests::test_mapi_session();
        let event_handle = session.allocate_output_handle(
            Some(0),
            MapiObject::PendingEvent {
                folder_id: CALENDAR_FOLDER_ID,
                properties: HashMap::from([
                    (
                        PID_LID_LOCATION_W_TAG,
                        MapiValue::String("Geneva".to_string()),
                    ),
                    (PID_TAG_RESPONSE_REQUESTED, MapiValue::Bool(true)),
                    (PID_TAG_REPLY_REQUESTED, MapiValue::Bool(true)),
                ]),
                recipients: Vec::new(),
                recipients_modified: false,
                fail_on_conflict: false,
            },
        );
        let request = RopRequest {
            rop_id: RopId::DeleteProperties.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: Vec::new(),
        };
        let location_string8 = (PID_LID_LOCATION_W_TAG & 0xFFFF_0000) | 0x001E;

        let problems = stage_pending_event_property_deletions(
            &mut session,
            &[event_handle],
            &request,
            &principal,
            &[location_string8, PID_TAG_RESPONSE_REQUESTED],
        )
        .unwrap();
        assert_eq!(problems, vec![(1, PID_TAG_RESPONSE_REQUESTED, 0x8004_0102)]);
        let MapiObject::PendingEvent { properties, .. } =
            session.handles.get(&event_handle).unwrap()
        else {
            panic!("pending Calendar handle changed kind")
        };
        assert!(!properties.contains_key(&PID_LID_LOCATION_W_TAG));
        assert_eq!(
            properties.get(&PID_TAG_RESPONSE_REQUESTED),
            Some(&MapiValue::Bool(true))
        );
        assert_eq!(
            properties.get(&PID_TAG_REPLY_REQUESTED),
            Some(&MapiValue::Bool(true))
        );

        let problems = stage_pending_event_property_deletions(
            &mut session,
            &[event_handle],
            &request,
            &principal,
            &[PID_TAG_RESPONSE_REQUESTED, PID_TAG_REPLY_REQUESTED],
        )
        .unwrap();
        assert!(problems.is_empty());
        let MapiObject::PendingEvent { properties, .. } =
            session.handles.get(&event_handle).unwrap()
        else {
            panic!("pending Calendar handle changed kind")
        };
        assert!(!properties.contains_key(&PID_TAG_RESPONSE_REQUESTED));
        assert!(!properties.contains_key(&PID_TAG_REPLY_REQUESTED));
    }

    #[test]
    fn calendar_fast_transfer_normalizes_string8_and_rejects_wrong_types() {
        let account_id = Uuid::nil();
        let location_string8 = (PID_LID_LOCATION_W_TAG & 0xFFFF_0000) | 0x001E;
        for value in [MapiValue::I32(11), MapiValue::Bool(true)] {
            assert!(
                pending_event_import_properties(vec![(PID_LID_APPOINTMENT_COLOR_TAG, value,)])
                    .is_err()
            );
        }
        let mut imported = pending_event_import_properties(vec![
            (location_string8, MapiValue::String("Geneva".to_string())),
            (0x9100_001E, MapiValue::String("opaque".to_string())),
        ])
        .unwrap();
        assert!(!imported.contains_key(&location_string8));
        assert_eq!(
            imported.get(&PID_LID_LOCATION_W_TAG),
            Some(&MapiValue::String("Geneva".to_string()))
        );
        assert_eq!(
            imported.get(&0x9100_001F),
            Some(&MapiValue::String("opaque".to_string()))
        );

        apply_pending_event_fast_transfer_property_values(
            account_id,
            &mut imported,
            vec![(0x9101_001E, MapiValue::String("stream".to_string()))],
        )
        .unwrap();
        assert_eq!(
            imported.get(&0x9101_001F),
            Some(&MapiValue::String("stream".to_string()))
        );
        let display_name_string8 = (PID_TAG_DISPLAY_NAME_W & 0xFFFF_0000) | 0x001E;
        apply_pending_event_fast_transfer_property_values(
            account_id,
            &mut imported,
            vec![(
                display_name_string8,
                MapiValue::String("ignored alias".to_string()),
            )],
        )
        .unwrap();
        assert!(!imported.contains_key(&PID_TAG_DISPLAY_NAME_W));
        assert!(apply_pending_event_fast_transfer_property_values(
            account_id,
            &mut imported,
            vec![(
                (PID_LID_LOCATION_W_TAG & 0xFFFF_0000) | 0x0102,
                MapiValue::Binary(vec![1]),
            ),],
        )
        .is_err());
        for property_tag in [
            (PID_TAG_SEARCH_KEY & 0xFFFF_0000) | 0x001F,
            (PID_TAG_ENTRY_ID & 0xFFFF_0000) | 0x001F,
            (PID_TAG_DISPLAY_NAME_W & 0xFFFF_0000) | 0x0102,
            (PID_LID_APPOINTMENT_COLOR_TAG & 0xFFFF_0000) | 0x001F,
        ] {
            assert!(apply_pending_event_fast_transfer_property_values(
                account_id,
                &mut imported,
                vec![(property_tag, MapiValue::String("wrong type".to_string()))],
            )
            .is_err());
        }
    }

    #[test]
    fn calendar_fast_transfer_defers_only_cross_chunk_rsvp_pair_validation() {
        let account_id = Uuid::nil();
        let mut properties = HashMap::new();

        apply_pending_event_fast_transfer_property_values(
            account_id,
            &mut properties,
            vec![(PID_TAG_RESPONSE_REQUESTED, MapiValue::Bool(true))],
        )
        .unwrap();
        assert_eq!(
            properties.get(&PID_TAG_RESPONSE_REQUESTED),
            Some(&MapiValue::Bool(true))
        );
        assert!(validate_pending_event_property_values(account_id, properties.clone()).is_err());

        for value in [MapiValue::I32(11), MapiValue::Bool(true)] {
            assert!(apply_pending_event_fast_transfer_property_values(
                account_id,
                &mut properties,
                vec![(PID_LID_APPOINTMENT_COLOR_TAG, value)],
            )
            .is_err());
        }
        assert!(!properties.contains_key(&PID_LID_APPOINTMENT_COLOR_TAG));
        assert!(apply_pending_event_fast_transfer_property_values(
            account_id,
            &mut properties,
            vec![(PID_TAG_RESPONSE_REQUESTED, MapiValue::I32(1))],
        )
        .is_err());

        apply_pending_event_fast_transfer_property_values(
            account_id,
            &mut properties,
            vec![
                (PID_TAG_REPLY_REQUESTED, MapiValue::Bool(true)),
                (PID_LID_APPOINTMENT_COLOR_TAG, MapiValue::I32(7)),
            ],
        )
        .unwrap();
        assert!(validate_pending_event_property_values(account_id, properties.clone()).is_ok());
        assert_eq!(
            properties.get(&PID_LID_APPOINTMENT_COLOR_TAG),
            Some(&MapiValue::I32(7))
        );
    }

    #[test]
    fn saved_calendar_commit_deletes_legacy_unsupported_property_rows() {
        let principal = super::tests::test_principal();
        let canonical_id = Uuid::from_u128(0x202608121010);
        let item_id = crate::mapi::identity::mapi_store_id(0x1210);
        let change_number = mapi_mailstore::change_number_for_store_id(item_id);
        let event = crate::mapi_store::MapiEvent {
            id: item_id,
            source_key: mapi_mailstore::source_key_for_store_id(item_id),
            folder_id: CALENDAR_FOLDER_ID,
            canonical_id,
            event: default_event_for_mapping(principal.account_id, DEFAULT_CALENDAR_COLLECTION_ID),
            version: lpe_storage::MapiEventVersion {
                event_id: canonical_id,
                canonical_modseq: 1,
                change_number,
                search_key: None,
                change_key: mapi_mailstore::change_key_for_change_number(change_number),
                predecessor_change_list: mapi_mailstore::predecessor_change_list(change_number),
                last_modification_time: mapi_mailstore::filetime_from_rfc3339_utc(
                    "2026-08-12T10:10:00Z",
                ),
                created_at: "2026-08-12T10:10:00Z".to_string(),
                updated_at: "2026-08-12T10:10:00Z".to_string(),
            },
            attachments: Vec::new(),
            stored_properties: Vec::new(),
        };
        let mut transaction = MapiEventTransaction::new(0x01, event.version.canonical_modseq);
        transaction
            .deleted_properties
            .extend([0x9100_1003, (PID_LID_LOCATION_W_TAG & 0xFFFF_0000) | 0x0102]);

        let commit = staged_event_commit_input(&principal, &event, &transaction, None, false)
            .unwrap()
            .unwrap();
        assert_eq!(
            commit.custom_property_deletes,
            vec![(PID_LID_LOCATION_W_TAG & 0xFFFF_0000) | 0x0102, 0x9100_1003,]
        );
    }
}

pub(super) fn staged_event_commit_input(
    principal: &AccountPrincipal,
    event: &crate::mapi_store::MapiEvent,
    transaction: &MapiEventTransaction,
    reminder: Option<&lpe_storage::ClientReminder>,
    force_save: bool,
) -> Result<Option<MapiEventCommitInput>> {
    if transaction.import_disposition == MapiEventImportDisposition::IgnoreOlderOrSame {
        return Ok(None);
    }
    let keep_server_content =
        transaction.import_disposition == MapiEventImportDisposition::KeepServerContent;
    let (property_values, reminder_set, mut reminder_at) =
        split_reminder_property_values(if keep_server_content {
            Vec::new()
        } else {
            transaction.pending_properties.clone().into_iter().collect()
        })?;
    if reminder_set == Some(true) && reminder_at.is_none() {
        reminder_at = reminder.map(|reminder| reminder.reminder_at.clone());
    }
    validate_effective_calendar_passthrough_invariants(
        event,
        &transaction.pending_properties,
        &transaction.deleted_properties,
    )?;
    let property_values = property_values.into_iter().collect::<HashMap<_, _>>();
    let (canonical_values, custom_values): (Vec<_>, Vec<_>) = property_values
        .into_iter()
        .partition(|(tag, _)| !is_calendar_passthrough_property_tag(*tag));
    let canonical_values = canonical_values.into_iter().collect::<HashMap<_, _>>();
    let mut event_input = if canonical_values.is_empty() {
        None
    } else {
        if bounded_meeting_cancellation_from_mapi(&canonical_values)? {
            bail!("MAPI meeting cancellation requires the canonical Event delete path");
        }
        Some(
            if let Some(input) = meeting_response_event_input_from_mapi(
                event.event.owner_account_id,
                Some(event.canonical_id),
                &event.event,
                &canonical_values,
            )? {
                input
            } else {
                event_input_from_mapi(
                    event.event.owner_account_id,
                    Some(event.canonical_id),
                    &event.event,
                    &canonical_values,
                )?
            },
        )
    };
    if !keep_server_content {
        if let Some(recipients) = transaction.pending_recipients.as_deref() {
            let input = event_input.get_or_insert_with(|| {
                event_input_from_mapi(
                    event.event.owner_account_id,
                    Some(event.canonical_id),
                    &event.event,
                    &HashMap::new(),
                )
                .expect("empty Calendar property projection is valid")
            });
            apply_calendar_pending_recipients(input, &event.event, &canonical_values, recipients);
        }
    }
    if transaction.imported_identity.is_none() {
        if let Some(input) = event_input.as_mut() {
            materialize_owner_meeting_organizer(
                input,
                &event.event.owner_email,
                &event.event.owner_display_name,
            );
        }
    }
    let mut custom_property_upserts = custom_values
        .into_iter()
        .map(|(property_tag, value)| {
            let mut property_value = Vec::new();
            write_mapi_value(&mut property_value, property_tag, &value);
            MapiEventCustomPropertyValue {
                property_tag,
                property_type: MapiPropertyTag::new(property_tag).property_type_code(),
                property_value,
            }
        })
        .collect::<Vec<_>>();
    custom_property_upserts.sort_by_key(|value| value.property_tag);
    let mut custom_property_deletes = if keep_server_content {
        Vec::new()
    } else {
        transaction
            .deleted_properties
            .iter()
            .copied()
            // Unsupported named values are rejected on new writes, but older
            // rows must remain deletable instead of producing a successful
            // DeleteProperties followed by a no-op Save.
            .filter(|tag| {
                is_calendar_passthrough_property_tag(*tag)
                    || is_custom_property_tag(*tag)
                    || is_invalid_calendar_canonical_named_property_tag(*tag)
                    || crate::store::is_mapi_calendar_standard_passthrough_property_id(*tag)
            })
            .collect::<Vec<_>>()
    };
    custom_property_deletes.sort_unstable();

    let reminder = MapiEventReminderPatch {
        reminder_set,
        reminder_at,
        reminder_dismissed_at: None,
    };
    // [MS-OXCMSG] section 3.2.5.3 requires every Save on an independently
    // opened handle to perform the optimistic-version check. A no-op save must
    // therefore still reach the canonical commit boundary; ForceSave remains
    // the only way to bypass ecObjectModified.
    Ok(Some(MapiEventCommitInput {
        principal_account_id: principal.account_id,
        event_id: event.canonical_id,
        expected_modseq: transaction.base_modseq,
        force_save,
        imported_identity: transaction.imported_identity.clone(),
        event: event_input,
        reminder,
        custom_property_upserts,
        custom_property_deletes,
        attachment_changes: MapiEventAttachmentChanges::default(),
    }))
}

pub(super) fn event_after_commit(
    mut event: lpe_storage::AccessibleEvent,
    input: Option<&lpe_storage::UpsertClientEventInput>,
) -> lpe_storage::AccessibleEvent {
    let Some(input) = input else {
        return event;
    };
    event.uid = input.uid.clone();
    event.date = input.date.clone();
    event.time = input.time.clone();
    event.time_zone = input.time_zone.clone();
    event.duration_minutes = input.duration_minutes;
    event.all_day = input.all_day;
    event.status = input.status.clone();
    event.sequence = input.sequence;
    event.recurrence_rule = input.recurrence_rule.clone();
    event.recurrence_json = input.recurrence_json.clone();
    event.recurrence_exceptions_json = input.recurrence_exceptions_json.clone();
    event.title = input.title.clone();
    event.location = input.location.clone();
    event.organizer_json = input.organizer_json.clone();
    event.attendees = input.attendees.clone();
    event.attendees_json = input.attendees_json.clone();
    event.notes = input.notes.clone();
    event.body_html = input.body_html.clone();
    event
}
