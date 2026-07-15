use super::*;
use anyhow::bail;

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
    let mut merged = transaction.pending_properties.clone();
    for (_, _, storage_tag, value) in &values {
        merged.insert(*storage_tag, value.clone());
    }
    if validate_staged_event_property_values(event, merged).is_ok() {
        for (_, _, storage_tag, value) in values {
            transaction.deleted_properties.remove(&storage_tag);
            transaction.pending_properties.insert(storage_tag, value);
        }
        return Ok(Vec::new());
    }

    let mut problems = Vec::new();
    let mut staged = HashSet::new();
    let temporal = values
        .iter()
        .filter(|(_, _, storage_tag, _)| event_temporal_property_is_coupled(*storage_tag))
        .collect::<Vec<_>>();
    if !temporal.is_empty() {
        let mut merged = transaction.pending_properties.clone();
        for (_, _, storage_tag, value) in &temporal {
            merged.insert(*storage_tag, value.clone());
        }
        if validate_staged_event_property_values(event, merged).is_ok() {
            for (_, _, storage_tag, value) in temporal {
                transaction.deleted_properties.remove(storage_tag);
                transaction
                    .pending_properties
                    .insert(*storage_tag, value.clone());
                staged.insert(*storage_tag);
            }
        } else {
            for (index, tag, storage_tag, _) in temporal {
                problems.push((*index, *tag, 0x8004_0102));
                staged.insert(*storage_tag);
            }
        }
    }
    for (index, tag, storage_tag, value) in values {
        if staged.contains(&storage_tag) {
            continue;
        }
        let mut merged = transaction.pending_properties.clone();
        merged.insert(storage_tag, value.clone());
        if validate_staged_event_property_values(event, merged).is_err() {
            problems.push((index, tag, 0x8004_0102));
            continue;
        }
        transaction.deleted_properties.remove(&storage_tag);
        transaction.pending_properties.insert(storage_tag, value);
    }
    problems.sort_unstable_by_key(|(index, _, _)| *index);
    Ok(problems)
}

fn event_temporal_property_is_coupled(tag: u32) -> bool {
    matches!(
        tag,
        PID_TAG_START_DATE
            | PID_TAG_END_DATE
            | PID_LID_COMMON_START_TAG
            | PID_LID_COMMON_END_TAG
            | PID_LID_APPOINTMENT_START_WHOLE_TAG
            | PID_LID_APPOINTMENT_END_WHOLE_TAG
            | PID_LID_APPOINTMENT_DURATION_TAG
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
        if stage_clearable_event_property_deletion(transaction, storage_tag) {
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
        PID_TAG_LOCATION_W | PID_LID_LOCATION_W_TAG => {
            transaction
                .pending_properties
                .insert(PID_TAG_LOCATION_W, clear_text.clone());
            transaction
                .pending_properties
                .insert(PID_LID_LOCATION_W_TAG, clear_text);
            transaction
                .deleted_properties
                .extend([PID_TAG_LOCATION_W, PID_LID_LOCATION_W_TAG]);
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
        PID_LID_REMINDER_SET_TAG
        | PID_LID_REMINDER_TIME_TAG
        | PID_LID_REMINDER_SIGNAL_TIME_TAG
        | PID_LID_REMINDER_DELTA_TAG => {
            transaction
                .pending_properties
                .insert(PID_LID_REMINDER_SET_TAG, MapiValue::Bool(false));
            transaction.deleted_properties.extend([
                PID_LID_REMINDER_SET_TAG,
                PID_LID_REMINDER_TIME_TAG,
                PID_LID_REMINDER_SIGNAL_TIME_TAG,
                PID_LID_REMINDER_DELTA_TAG,
            ]);
        }
        _ => return false,
    }
    true
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
