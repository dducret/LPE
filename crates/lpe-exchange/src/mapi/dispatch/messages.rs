use super::*;

pub(super) fn canonical_message_folder_id(email: &JmapEmail, mailboxes: &[JmapMailbox]) -> u64 {
    email
        .mailbox_states
        .iter()
        .find_map(|state| {
            mailboxes
                .iter()
                .find(|mailbox| mailbox.id == state.mailbox_id)
                .map(mapi_folder_id)
        })
        .or_else(|| {
            mailboxes
                .iter()
                .find(|mailbox| mailbox.id == email.mailbox_id)
                .map(mapi_folder_id)
        })
        .unwrap_or_else(|| {
            mailboxes
                .iter()
                .find(|mailbox| mailbox.role == email.mailbox_role)
                .map(mapi_folder_id)
                .unwrap_or(INBOX_FOLDER_ID)
        })
}

pub(super) fn fallback_open_message_folder_id(
    requested_folder_id: u64,
    email: &JmapEmail,
    mailboxes: &[JmapMailbox],
) -> u64 {
    if email_matches_folder(email, requested_folder_id, mailboxes) {
        requested_folder_id
    } else {
        canonical_message_folder_id(email, mailboxes)
    }
}

pub(super) fn open_message_folder_id(request: &RopRequest, message_id: u64) -> u64 {
    request.folder_id().unwrap_or_else(|| {
        if crate::mapi_store::is_outlook_local_freebusy_message_id(message_id) {
            FREEBUSY_DATA_FOLDER_ID
        } else {
            INBOX_FOLDER_ID
        }
    })
}

pub(super) fn unique_message_for_id(message_id: u64, emails: &[JmapEmail]) -> Option<&JmapEmail> {
    let mut matches = emails
        .iter()
        .filter(|email| mapi_item_id_matches(&email.id, message_id));
    let email = matches.next()?;
    matches.next().is_none().then_some(email)
}

pub(super) fn persisted_message_delete_is_best_effort(object: Option<&MapiObject>) -> bool {
    matches!(object, Some(MapiObject::Message { .. }))
}

pub(super) fn append_save_changes_message_response(
    responses: &mut Vec<u8>,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    handle: u32,
    message_id: u64,
) {
    set_handle_slot(handle_slots, Some(request.response_handle_index()), handle);
    responses.extend_from_slice(&rop_save_changes_message_response(request, message_id));
}

fn copyable_message_followup_property_tag(tag: u32) -> bool {
    matches!(
        canonical_property_storage_tag(tag),
        PID_TAG_MESSAGE_FLAGS
            | PID_TAG_FLAG_STATUS
            | PID_TAG_FOLLOWUP_ICON
            | PID_TAG_TODO_ITEM_FLAGS
            | PID_TAG_FLAG_COMPLETE_TIME
            | PID_LID_TASK_START_DATE_TAG
            | PID_LID_TASK_DUE_DATE_TAG
            | PID_LID_REMINDER_SET_TAG
            | PID_LID_REMINDER_TIME_TAG
            | PID_LID_REMINDER_SIGNAL_TIME_TAG
            | PID_LID_FLAG_REQUEST_W_TAG
            | PID_TAG_SWAPPED_TODO_STORE
            | PID_TAG_SWAPPED_TODO_DATA
            | PID_NAME_KEYWORDS_TAG
    )
}

pub(super) async fn copy_message_followup_property_values_for_request<S>(
    store: &S,
    principal: &AccountPrincipal,
    source: Option<&MapiObject>,
    destination: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    property_tags: &[u32],
) -> Result<Option<Vec<(usize, u32, u32)>>>
where
    S: ExchangeStore,
{
    if property_tags.is_empty()
        || !property_tags
            .iter()
            .copied()
            .all(copyable_message_followup_property_tag)
    {
        return Ok(None);
    }
    let Some(MapiObject::Message {
        folder_id: source_folder_id,
        message_id: source_message_id,
        ..
    }) = source
    else {
        return Ok(None);
    };
    let Some(destination_object @ MapiObject::Message { .. }) = destination else {
        return Ok(None);
    };
    let Some(email) = message_for_id(*source_folder_id, *source_message_id, mailboxes, emails)
    else {
        return Ok(None);
    };
    let mut values = Vec::new();
    let mut problems = Vec::new();
    for (index, property_tag) in property_tags.iter().copied().enumerate() {
        match normal_message_debug_property_value(email, property_tag) {
            Some(value) => values.push((canonical_property_storage_tag(property_tag), value)),
            None => problems.push((index, property_tag, 0x8004_010F)),
        }
    }
    if !values.is_empty() {
        apply_supported_object_property_values(
            store,
            principal,
            destination_object,
            values,
            mailboxes,
            emails,
            snapshot,
        )
        .await?;
    }
    Ok(Some(problems))
}

pub(super) async fn copy_all_message_followup_property_values_for_request<S>(
    store: &S,
    principal: &AccountPrincipal,
    source: Option<&MapiObject>,
    destination: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    excluded_property_tags: &[u32],
) -> Result<bool>
where
    S: ExchangeStore,
{
    let Some(MapiObject::Message {
        folder_id: source_folder_id,
        message_id: source_message_id,
        ..
    }) = source
    else {
        return Ok(false);
    };
    let Some(destination_object @ MapiObject::Message { .. }) = destination else {
        return Ok(false);
    };
    let Some(email) = message_for_id(*source_folder_id, *source_message_id, mailboxes, emails)
    else {
        return Ok(false);
    };
    let excluded = excluded_property_tags
        .iter()
        .copied()
        .map(canonical_property_storage_tag)
        .collect::<HashSet<_>>();
    let mut values = Vec::new();
    for property_tag in [
        PID_TAG_MESSAGE_FLAGS,
        PID_TAG_FLAG_STATUS,
        PID_TAG_FOLLOWUP_ICON,
        PID_TAG_TODO_ITEM_FLAGS,
        PID_TAG_FLAG_COMPLETE_TIME,
        PID_LID_TASK_START_DATE_TAG,
        PID_LID_TASK_DUE_DATE_TAG,
        PID_LID_REMINDER_SET_TAG,
        PID_LID_REMINDER_TIME_TAG,
        PID_LID_REMINDER_SIGNAL_TIME_TAG,
        PID_LID_FLAG_REQUEST_W_TAG,
        PID_TAG_SWAPPED_TODO_STORE,
        PID_TAG_SWAPPED_TODO_DATA,
        PID_NAME_KEYWORDS_TAG,
    ] {
        if excluded.contains(&property_tag) {
            continue;
        }
        if let Some(value) = normal_message_debug_property_value(email, property_tag) {
            values.push((property_tag, value));
        }
    }
    if values.is_empty() {
        return Ok(false);
    }
    apply_supported_object_property_values(
        store,
        principal,
        destination_object,
        values,
        mailboxes,
        emails,
        snapshot,
    )
    .await?;
    Ok(true)
}
