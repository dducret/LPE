use super::*;
use std::collections::HashSet;

pub(super) struct SavedMessageSubmissionOverlay {
    pub(super) input: SubmitMessageInput,
    pub(super) custom_property_upserts: Vec<MapiCustomPropertyValue>,
    pub(super) followup_update: Option<lpe_storage::JmapEmailFollowupUpdate>,
    pub(super) replaced_scheduling_attachment_id: Option<Uuid>,
}

pub(super) async fn saved_message_submission_overlay<S>(
    store: &S,
    principal: &AccountPrincipal,
    email: &JmapEmail,
    pending_properties: &HashMap<u32, MapiValue>,
    deleted_property_tags: &HashSet<u32>,
    recipient_replacement: Option<&[PendingRecipient]>,
    mut added_attachments: Vec<AttachmentUploadInput>,
    selected_scheduling_attachment_deleted: bool,
) -> Result<SavedMessageSubmissionOverlay>
where
    S: ExchangeStore,
{
    let protected_emails = store
        .fetch_jmap_emails_with_protected_bcc(principal.account_id, &[email.id])
        .await?;
    let email = protected_emails
        .iter()
        .find(|loaded| loaded.id == email.id)
        .unwrap_or(email);

    let content_changed = selected_scheduling_attachment_deleted
        || recipient_replacement.is_some()
        || pending_properties
            .keys()
            .copied()
            .any(saved_submission_content_property_tag)
        || deleted_property_tags
            .iter()
            .copied()
            .any(saved_submission_content_property_tag);
    let mut input = if content_changed {
        let mut properties = saved_submission_property_values(email);
        for tag in deleted_property_tags {
            properties.remove(&canonical_property_storage_tag(*tag));
        }
        if pending_properties.contains_key(&PID_TAG_BODY_W) {
            properties.remove(&PID_TAG_RTF_COMPRESSED);
        } else if pending_properties.contains_key(&PID_TAG_BODY_HTML_W)
            || pending_properties.contains_key(&PID_TAG_HTML_BINARY)
        {
            properties.remove(&PID_TAG_BODY_W);
            properties.remove(&PID_TAG_RTF_COMPRESSED);
        } else if pending_properties.contains_key(&PID_TAG_RTF_COMPRESSED) {
            properties.remove(&PID_TAG_BODY_W);
            properties.remove(&PID_TAG_BODY_HTML_W);
            properties.remove(&PID_TAG_HTML_BINARY);
        }
        properties.extend(
            pending_properties
                .iter()
                .map(|(tag, value)| (canonical_property_storage_tag(*tag), value.clone())),
        );
        let effective_scheduling = effective_message_is_scheduling(&properties);
        let source_was_scheduling =
            email.calendar_meeting_request.is_some() || email.calendar_meeting_response.is_some();
        if !effective_scheduling
            && !source_was_scheduling
            && pending_properties
                .keys()
                .chain(deleted_property_tags.iter())
                .copied()
                .any(meeting_scheduling_only_property_tag)
        {
            return Err(anyhow!(
                "ordinary saved Message has unsupported meeting-only property overlay"
            ));
        }
        if let Some(message_class) =
            optional_pending_text_property(&properties, &[PID_TAG_MESSAGE_CLASS_W])
        {
            let message_class = message_class.trim();
            if !message_class.eq_ignore_ascii_case("IPM.Note")
                && !message_class
                    .to_ascii_lowercase()
                    .starts_with("ipm.schedule.meeting.")
            {
                return Err(anyhow!(
                    "saved Message class is not supported by canonical submission"
                ));
            }
        }
        let recipients = recipient_replacement
            .map(<[PendingRecipient]>::to_vec)
            .unwrap_or_else(|| {
                if effective_scheduling {
                    pending_recipients_from_email(email)
                } else {
                    ordinary_pending_recipients_from_email(email)
                }
            });
        let mut input = mapi_submit_from_pending_message(principal, &properties, &recipients)?;
        input.draft_message_id = Some(email.id);
        input
    } else {
        mapi_submit_from_email(principal, email, Vec::new())
    };

    let followup_values = pending_properties
        .iter()
        .filter(|(tag, _)| copyable_message_followup_property_tag(**tag))
        .map(|(tag, value)| (*tag, value.clone()))
        .collect::<Vec<_>>();
    let mut update = message_followup_update_from_mapi_values(followup_values)?;
    apply_message_followup_property_deletions(&mut update, deleted_property_tags);
    let followup_update = if message_followup_update_is_empty(&update) {
        None
    } else {
        if let Some(unread) = update.unread {
            input.unread = Some(unread);
        }
        if let Some(flagged) = update.flagged {
            input.flagged = Some(flagged);
        }
        Some(update)
    };

    let replaced_scheduling_attachment_id = if content_changed {
        email
            .calendar_meeting_request
            .as_ref()
            .and_then(|request| request.transport_attachment_id)
            .or_else(|| {
                email
                    .calendar_meeting_response
                    .as_ref()
                    .and_then(|response| response.transport_attachment_id)
            })
    } else {
        input.attachments.clear();
        None
    };
    input.attachments.append(&mut added_attachments);

    let mut custom_property_upserts = pending_properties
        .iter()
        .filter(|(tag, _)| is_custom_property_tag(**tag))
        .map(|(property_tag, value)| {
            let mut property_value = Vec::new();
            write_mapi_value(&mut property_value, *property_tag, value);
            MapiCustomPropertyValue {
                property_tag: *property_tag,
                property_type: MapiPropertyTag::new(*property_tag).property_type_code(),
                property_value,
            }
        })
        .collect::<Vec<_>>();
    custom_property_upserts.sort_by_key(|value| value.property_tag);

    Ok(SavedMessageSubmissionOverlay {
        input,
        custom_property_upserts,
        followup_update,
        replaced_scheduling_attachment_id,
    })
}

fn effective_message_is_scheduling(properties: &HashMap<u32, MapiValue>) -> bool {
    optional_pending_text_property(properties, &[PID_TAG_MESSAGE_CLASS_W]).is_some_and(
        |message_class| {
            message_class
                .trim()
                .to_ascii_lowercase()
                .starts_with("ipm.schedule.meeting.")
        },
    ) || is_mapi_meeting_request(properties)
}

fn ordinary_pending_recipients_from_email(email: &JmapEmail) -> Vec<PendingRecipient> {
    email
        .to
        .iter()
        .map(|recipient| (0x01, recipient))
        .chain(email.cc.iter().map(|recipient| (0x02, recipient)))
        .chain(email.bcc.iter().map(|recipient| (0x03, recipient)))
        .enumerate()
        .map(|(row_id, (recipient_type, recipient))| PendingRecipient {
            row_id: row_id.min(u32::MAX as usize) as u32,
            address: recipient.address.clone(),
            display_name: recipient.display_name.clone(),
            recipient_type,
            recipient_flags: 0x0000_0001,
        })
        .collect()
}

fn saved_submission_property_values(email: &JmapEmail) -> HashMap<u32, MapiValue> {
    let mut tags = vec![
        PID_TAG_SUBJECT_W,
        PID_TAG_NORMALIZED_SUBJECT_W,
        PID_TAG_BODY_W,
        PID_TAG_BODY_HTML_W,
        PID_TAG_HTML_BINARY,
        PID_TAG_MESSAGE_CLASS_W,
        PID_TAG_SENDER_NAME_W,
        PID_TAG_SENDER_EMAIL_ADDRESS_W,
        PID_TAG_SENDER_SMTP_ADDRESS_W,
        PID_TAG_SENT_REPRESENTING_NAME_W,
        PID_TAG_SENT_REPRESENTING_EMAIL_ADDRESS_W,
        PID_TAG_SENT_REPRESENTING_SMTP_ADDRESS_W,
        PID_TAG_INTERNET_MESSAGE_ID_W,
        PID_TAG_MESSAGE_FLAGS,
        PID_TAG_FLAG_STATUS,
        PID_TAG_CLIENT_SUBMIT_TIME,
        PID_TAG_CREATION_TIME,
        PID_TAG_LAST_MODIFICATION_TIME,
    ];
    tags.extend(email_meeting_property_tags(email));
    tags.push(PID_TAG_RTF_COMPRESSED);
    tags.sort_unstable();
    tags.dedup();
    tags.into_iter()
        .filter_map(|tag| email_property_value(email, tag).map(|value| (tag, value)))
        .collect()
}

pub(super) fn saved_submission_content_property_tag(tag: u32) -> bool {
    let tag = canonical_property_storage_tag(tag);
    meeting_scheduling_input_property_tag(tag) || matches!(tag, PID_TAG_INTERNET_MESSAGE_ID_W)
}

pub(super) fn saved_submission_property_deletion_is_supported(tag: u32) -> bool {
    (saved_submission_content_property_tag(tag)
        && canonical_property_storage_tag(tag) != PID_TAG_RTF_COMPRESSED)
        || is_custom_property_tag(tag)
        || message_followup_property_deletion_is_supported(tag)
}

fn meeting_scheduling_only_property_tag(tag: u32) -> bool {
    matches!(
        canonical_property_storage_tag(tag),
        PID_TAG_START_DATE
            | PID_TAG_END_DATE
            | PID_TAG_CLIENT_SUBMIT_TIME
            | PID_TAG_LAST_MODIFICATION_TIME
            | PID_TAG_CREATION_TIME
            | PID_LID_COMMON_START_TAG
            | PID_LID_COMMON_END_TAG
            | PID_LID_APPOINTMENT_START_WHOLE_TAG
            | PID_LID_APPOINTMENT_END_WHOLE_TAG
            | PID_LID_LOCATION_W_TAG
            | PID_LID_APPOINTMENT_SEQUENCE_TAG
            | PID_LID_APPOINTMENT_STATE_FLAGS_TAG
            | PID_LID_GLOBAL_OBJECT_ID_TAG
            | PID_LID_CLEAN_GLOBAL_OBJECT_ID_TAG
            | PID_LID_ATTENDEE_CRITICAL_CHANGE_TAG
            | PID_LID_APPOINTMENT_COUNTER_PROPOSAL_TAG
            | PID_LID_APPOINTMENT_PROPOSED_START_WHOLE_TAG
            | PID_LID_APPOINTMENT_PROPOSED_END_WHOLE_TAG
    )
}

pub(super) fn saved_submission_property_upsert_is_supported(tag: u32) -> bool {
    saved_submission_content_property_tag(tag)
        // [MS-OXCFXICS] section 4.3.1: a partial item upload sets the
        // updated PCL on the existing Message before SaveChangesMessage;
        // [MS-OXCPRPT] section 2.2.6 gives NoReplicate the same Message
        // semantics as RopSetProperties.
        || canonical_property_storage_tag(tag) == PID_TAG_PREDECESSOR_CHANGE_LIST
        || is_custom_property_tag(tag)
        || copyable_message_followup_property_tag(tag)
}

fn message_followup_property_deletion_is_supported(tag: u32) -> bool {
    matches!(
        canonical_property_storage_tag(tag),
        PID_TAG_FLAG_STATUS
            | PID_TAG_FOLLOWUP_ICON
            | PID_TAG_TODO_ITEM_FLAGS
            | PID_LID_TASK_START_DATE_TAG
            | PID_LID_TASK_DUE_DATE_TAG
            | PID_LID_REMINDER_SET_TAG
            | PID_LID_REMINDER_TIME_TAG
            | PID_LID_REMINDER_SIGNAL_TIME_TAG
            | PID_LID_FLAG_REQUEST_W_TAG
            | PID_NAME_KEYWORDS_TAG
    )
}

pub(super) fn apply_message_followup_property_deletions(
    update: &mut lpe_storage::JmapEmailFollowupUpdate,
    deleted_property_tags: &HashSet<u32>,
) {
    for tag in deleted_property_tags
        .iter()
        .copied()
        .map(canonical_property_storage_tag)
    {
        match tag {
            PID_TAG_FLAG_STATUS => {
                update.flagged = Some(false);
                update.followup_flag_status = Some("none".to_string());
            }
            PID_TAG_FOLLOWUP_ICON => update.followup_icon = Some(0),
            PID_TAG_TODO_ITEM_FLAGS => update.todo_item_flags = Some(0),
            PID_LID_TASK_START_DATE_TAG => update.followup_start_at = Some(String::new()),
            PID_LID_TASK_DUE_DATE_TAG => update.followup_due_at = Some(String::new()),
            PID_LID_REMINDER_SET_TAG => update.reminder_set = Some(false),
            PID_LID_REMINDER_TIME_TAG | PID_LID_REMINDER_SIGNAL_TIME_TAG => {
                update.reminder_at = Some(String::new());
            }
            PID_LID_FLAG_REQUEST_W_TAG => update.followup_request = Some(String::new()),
            PID_NAME_KEYWORDS_TAG => update.categories = Some(Vec::new()),
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_transport_content_properties_require_scheduling_regeneration() {
        assert!(saved_submission_content_property_tag(PID_TAG_SUBJECT_W));
        assert!(saved_submission_content_property_tag(PID_TAG_BODY_HTML_W));
        assert!(saved_submission_content_property_tag(
            PID_LID_APPOINTMENT_PROPOSED_START_WHOLE_TAG
        ));
        assert!(saved_submission_content_property_tag(
            PID_LID_APPOINTMENT_SEQUENCE_TAG
        ));
        assert!(!saved_submission_content_property_tag(PID_TAG_FLAG_STATUS));
        assert!(!saved_submission_content_property_tag(0x9001_001F));
    }

    #[test]
    fn saved_message_deletions_fail_closed_outside_the_submit_overlay_contract() {
        assert!(saved_submission_property_deletion_is_supported(
            PID_TAG_SUBJECT_W
        ));
        assert!(saved_submission_property_deletion_is_supported(
            PID_LID_APPOINTMENT_SEQUENCE_TAG
        ));
        assert!(saved_submission_property_deletion_is_supported(
            PID_TAG_FLAG_STATUS
        ));
        assert!(saved_submission_property_deletion_is_supported(0x9001_001F));
        assert!(!saved_submission_property_deletion_is_supported(
            PID_TAG_DISPLAY_NAME_W
        ));
        assert!(!saved_submission_property_deletion_is_supported(
            PID_TAG_MESSAGE_FLAGS
        ));
        assert!(!saved_submission_property_deletion_is_supported(
            PID_TAG_RTF_COMPRESSED
        ));
        assert!(!saved_submission_property_upsert_is_supported(
            PID_TAG_DISPLAY_NAME_W
        ));
        assert!(saved_submission_property_upsert_is_supported(
            PID_TAG_PREDECESSOR_CHANGE_LIST
        ));
    }
}
