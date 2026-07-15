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

    let values = values
        .into_iter()
        .enumerate()
        .map(|(index, (tag, value))| (index, tag, canonical_property_storage_tag(tag), value))
        .collect::<Vec<_>>();
    let mut problems = values
        .iter()
        .filter(|(_, _, storage_tag, _)| event_property_is_server_managed(*storage_tag))
        .map(|(index, tag, _, _)| (*index, *tag, 0x8004_0102))
        .collect::<Vec<_>>();
    let values = values
        .into_iter()
        .filter(|(_, _, storage_tag, _)| !event_property_is_server_managed(*storage_tag))
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
    if validate_staged_event_property_values(event, merged).is_ok() {
        apply_event_property_values(
            &mut transaction.pending_properties,
            &mut transaction.deleted_properties,
            &staged_values,
        );
        return Ok(problems);
    }

    let mut staged_indices = HashSet::new();
    for coupled_tags in [EVENT_TEMPORAL_PROPERTIES, EVENT_REMINDER_PROPERTIES] {
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
        if validate_staged_event_property_values(event, merged).is_ok() {
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
        if validate_staged_event_property_values(event, merged).is_err() {
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
    let values = values
        .into_iter()
        .enumerate()
        .map(|(index, (tag, value))| (index, tag, canonical_property_storage_tag(tag), value))
        .collect::<Vec<_>>();
    let mut problems = values
        .iter()
        .filter(|(_, _, storage_tag, _)| event_property_is_server_managed(*storage_tag))
        .map(|(index, tag, _, _)| (*index, *tag, 0x8004_0102))
        .collect::<Vec<_>>();
    let values = values
        .into_iter()
        .filter(|(_, _, storage_tag, _)| !event_property_is_server_managed(*storage_tag))
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
    for coupled_tags in [EVENT_TEMPORAL_PROPERTIES, EVENT_REMINDER_PROPERTIES] {
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

fn validate_pending_event_property_values(
    account_id: Uuid,
    merged: HashMap<u32, MapiValue>,
) -> Result<()> {
    let (property_values, _, _) = split_reminder_property_values(merged.into_iter().collect())?;
    let (canonical_values, _) = split_custom_property_values(property_values.into_iter().collect());
    event_input_from_mapi(
        account_id,
        None,
        &default_event_for_mapping(account_id, DEFAULT_CALENDAR_COLLECTION_ID),
        &canonical_values.into_iter().collect(),
    )?;
    Ok(())
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

fn event_property_is_server_managed(tag: u32) -> bool {
    matches!(
        tag,
        PID_TAG_LAST_MODIFICATION_TIME
            | PID_TAG_LOCAL_COMMIT_TIME
            | PID_TAG_CHANGE_KEY
            | PID_TAG_PREDECESSOR_CHANGE_LIST
            | PID_TAG_CHANGE_NUMBER
            | PID_TAG_DISPLAY_NAME_W
    )
}

fn validate_staged_event_property_values(
    event: &crate::mapi_store::MapiEvent,
    merged: HashMap<u32, MapiValue>,
) -> Result<()> {
    let (property_values, _, _) = split_reminder_property_values(merged.into_iter().collect())?;
    let (canonical_values, _) = split_custom_property_values(property_values.into_iter().collect());
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

    let reminder = snapshot.reminder_for_source("calendar", event.canonical_id);
    let mut problems = Vec::new();
    for (index, tag) in property_tags.iter().enumerate() {
        let storage_tag = canonical_property_storage_tag(*tag);
        if event_property_is_server_managed(storage_tag) {
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

pub(super) fn staged_event_commit_input(
    principal: &AccountPrincipal,
    event: &crate::mapi_store::MapiEvent,
    transaction: &MapiEventTransaction,
    reminder: Option<&lpe_storage::ClientReminder>,
    force_save: bool,
) -> Result<Option<MapiEventCommitInput>> {
    let (property_values, reminder_set, mut reminder_at) = split_reminder_property_values(
        transaction.pending_properties.clone().into_iter().collect(),
    )?;
    if reminder_set == Some(true) && reminder_at.is_none() {
        reminder_at = reminder.map(|reminder| reminder.reminder_at.clone());
    }
    let (canonical_values, custom_values) =
        split_custom_property_values(property_values.into_iter().collect());
    let canonical_values = canonical_values.into_iter().collect::<HashMap<_, _>>();
    let event_input = if canonical_values.is_empty() {
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
    let mut custom_property_deletes = transaction
        .deleted_properties
        .iter()
        .copied()
        .filter(|tag| is_custom_property_tag(*tag))
        .collect::<Vec<_>>();
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
