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

pub(super) fn stage_message_property_values(
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    values: Vec<(u32, MapiValue)>,
) -> Result<()> {
    let handle = input_handle(handle_slots, request).ok_or_else(|| anyhow!("missing handle"))?;
    let object = session
        .handles
        .get_mut(&handle)
        .ok_or_else(|| anyhow!("missing message object"))?;
    let MapiObject::Message {
        pending_properties, ..
    } = object
    else {
        return Err(anyhow!("MAPI object is not a message"));
    };
    let (canonical_values, _custom_values) = split_custom_property_values(values.clone());
    let followup_values = canonical_values
        .into_iter()
        .filter(|(tag, _)| {
            !matches!(
                *tag,
                PID_TAG_SUBJECT_W
                    | PID_TAG_NORMALIZED_SUBJECT_W
                    | PID_TAG_BODY_W
                    | PID_TAG_SOURCE_KEY
                    | PID_TAG_CHANGE_KEY
                    | PID_TAG_PREDECESSOR_CHANGE_LIST
            )
        })
        .collect::<Vec<_>>();
    message_followup_update_from_mapi_values(followup_values)?;
    apply_mapi_property_values_to_map(pending_properties, values);
    Ok(())
}

pub(super) async fn apply_staged_message_property_values<S>(
    store: &S,
    principal: &AccountPrincipal,
    folder_id: u64,
    message_id: u64,
    pending_properties: HashMap<u32, MapiValue>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Result<()>
where
    S: ExchangeStore,
{
    let values = pending_properties.into_iter().collect::<Vec<_>>();
    let object = MapiObject::Message {
        folder_id,
        message_id,
        saved_email: None,
        pending_properties: HashMap::new(),
    };
    apply_supported_object_property_values(
        store, principal, &object, values, mailboxes, emails, snapshot,
    )
    .await
}

pub(super) async fn apply_staged_message_recipient_replacement<S>(
    store: &S,
    principal: &AccountPrincipal,
    folder_id: u64,
    message_id: u64,
    recipients: &[PendingRecipient],
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
) -> Result<()>
where
    S: ExchangeStore,
{
    let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails) else {
        return Err(anyhow::anyhow!("canonical message not found"));
    };
    let (to, cc, bcc) = submitted_recipients_from_pending(recipients);
    store
        .replace_message_recipients(
            principal.account_id,
            email.id,
            &to,
            &cc,
            &bcc,
            AuditEntryInput {
                actor: principal.email.clone(),
                action: "mapi-replace-message-recipients".to_string(),
                subject: format!("message:{}", email.id),
            },
        )
        .await?;
    Ok(())
}

pub(super) async fn delete_canonical_message_text_properties<S>(
    store: &S,
    principal: &AccountPrincipal,
    object: Option<&MapiObject>,
    property_tags: &[u32],
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Result<bool>
where
    S: ExchangeStore,
{
    let Some(MapiObject::Message {
        folder_id,
        message_id,
        ..
    }) = object
    else {
        return Ok(false);
    };
    let mut values = Vec::new();
    for tag in property_tags {
        match *tag {
            PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
                values.push((PID_TAG_SUBJECT_W, MapiValue::String(String::new())));
            }
            PID_TAG_BODY_W => values.push((PID_TAG_BODY_W, MapiValue::String(String::new()))),
            _ => {}
        }
    }
    if values.is_empty() {
        return Ok(false);
    }
    values.sort_by_key(|(tag, _)| *tag);
    values.dedup_by_key(|(tag, _)| *tag);
    let target = MapiObject::Message {
        folder_id: *folder_id,
        message_id: *message_id,
        saved_email: None,
        pending_properties: HashMap::new(),
    };
    apply_supported_object_property_values(
        store, principal, &target, values, mailboxes, emails, snapshot,
    )
    .await?;
    Ok(true)
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
