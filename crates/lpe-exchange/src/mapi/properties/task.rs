use super::*;

pub(in crate::mapi) fn task_property_value(
    task: &ClientTask,
    item_id: u64,
    folder_id: u64,
    property_tag: u32,
) -> Option<MapiValue> {
    task_property_value_with_reminder(task, item_id, folder_id, property_tag, None)
}

pub(in crate::mapi) fn task_property_value_with_reminder(
    task: &ClientTask,
    item_id: u64,
    folder_id: u64,
    property_tag: u32,
    reminder: Option<&lpe_storage::ClientReminder>,
) -> Option<MapiValue> {
    if let Some(value) = task_reminder_property_value(reminder, property_tag) {
        return Some(value);
    }
    let property_tag = canonical_property_storage_tag(property_tag);
    let change_number = mapi_mailstore::change_number_for_store_id(item_id);
    match property_tag {
        PID_TAG_MID => Some(MapiValue::U64(item_id)),
        PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W | PID_TAG_DISPLAY_NAME_W => {
            Some(MapiValue::String(task.title.clone()))
        }
        PID_TAG_BODY_W => Some(MapiValue::String(task.description.clone())),
        PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String("IPM.Task".to_string())),
        PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_MESSAGE_ACCESS)),
        PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(MSGFLAG_READ)),
        PID_TAG_FLAG_STATUS => Some(MapiValue::U32(task_flag_status(task))),
        PID_LID_PERCENT_COMPLETE_TAG => Some(MapiValue::F64(task_percent_complete(task).to_bits())),
        PID_LID_RECURRING_TAG => Some(MapiValue::Bool(!task.recurrence_rule.trim().is_empty())),
        PID_TAG_HAS_ATTACHMENTS => Some(MapiValue::Bool(false)),
        PID_TAG_MESSAGE_SIZE => Some(mapi_message_size_value(task_size(task))),
        PID_TAG_MESSAGE_SIZE_EXTENDED => Some(mapi_message_size_extended_value(task_size(task))),
        PID_TAG_LAST_MODIFICATION_TIME | PID_TAG_LOCAL_COMMIT_TIME => Some(MapiValue::U64(
            mapi_mailstore::filetime_from_rfc3339_utc(&task.updated_at),
        )),
        PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
            crate::mapi::identity::instance_key_for_object_id(item_id),
        )),
        PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_uuid(
            &task.id,
        ))),
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(folder_id),
        )),
        PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::change_key_for_change_number(change_number),
        )),
        PID_TAG_PREDECESSOR_CHANGE_LIST => Some(MapiValue::Binary(
            mapi_mailstore::predecessor_change_list(change_number),
        )),
        PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(change_number)),
        _ => None,
    }
}

fn task_reminder_property_value(
    reminder: Option<&lpe_storage::ClientReminder>,
    property_tag: u32,
) -> Option<MapiValue> {
    let reminder = reminder?;
    match property_tag {
        PID_LID_REMINDER_SET_TAG => Some(MapiValue::Bool(true)),
        PID_LID_REMINDER_DELTA_TAG => Some(MapiValue::I32(
            reminder
                .due_at
                .as_deref()
                .map(|due_at| {
                    reminder_delta_minutes(
                        mapi_mailstore::filetime_from_rfc3339_utc(due_at),
                        &reminder.reminder_at,
                    )
                })
                .unwrap_or_default(),
        )),
        PID_LID_REMINDER_OVERRIDE_TAG | PID_LID_REMINDER_PLAY_SOUND_TAG => {
            Some(MapiValue::Bool(false))
        }
        PID_LID_REMINDER_FILE_PARAMETER_W_TAG => Some(MapiValue::String(String::new())),
        PID_LID_REMINDER_TIME_TAG | PID_LID_REMINDER_SIGNAL_TIME_TAG => Some(MapiValue::U64(
            mapi_mailstore::filetime_from_rfc3339_utc(&reminder.reminder_at),
        )),
        _ => None,
    }
}

pub(super) fn reminder_delta_minutes(anchor_filetime: u64, reminder_at: &str) -> i32 {
    let reminder_filetime = mapi_mailstore::filetime_from_rfc3339_utc(reminder_at);
    if anchor_filetime <= reminder_filetime {
        return 0;
    }
    ((anchor_filetime - reminder_filetime) / 600_000_000).min(i32::MAX as u64) as i32
}

fn task_flag_status(task: &ClientTask) -> u32 {
    if task.status == "completed" {
        FOLLOWUP_COMPLETE
    } else {
        FOLLOWUP_FLAGGED
    }
}

fn task_percent_complete(task: &ClientTask) -> f64 {
    if task.status == "completed" {
        1.0
    } else {
        0.0
    }
}

pub(in crate::mapi) fn default_task_for_mapping(
    account_id: Uuid,
    collection_id: &str,
) -> ClientTask {
    ClientTask {
        id: Uuid::nil(),
        owner_account_id: account_id,
        owner_email: String::new(),
        owner_display_name: String::new(),
        is_owned: true,
        rights: default_mapping_rights(),
        task_list_id: Uuid::nil(),
        task_list_sort_order: 0,
        title: String::new(),
        description: String::new(),
        status: "needs-action".to_string(),
        due_at: None,
        completed_at: None,
        recurrence_rule: String::new(),
        sort_order: if matches!(collection_id, "tasks" | "default") {
            0
        } else {
            10
        },
        updated_at: "1970-01-01T00:00:00Z".to_string(),
    }
}

pub(in crate::mapi) fn task_input_from_mapi(
    account_id: Uuid,
    id: Option<Uuid>,
    existing: &ClientTask,
    collection_id: Option<&str>,
    properties: &HashMap<u32, MapiValue>,
) -> UpsertClientTaskInput {
    let title = optional_pending_text_property(
        properties,
        &[
            PID_TAG_SUBJECT_W,
            PID_TAG_NORMALIZED_SUBJECT_W,
            PID_TAG_DISPLAY_NAME_W,
        ],
    )
    .unwrap_or_else(|| existing.title.clone());
    let status = properties
        .get(&PID_TAG_FLAG_STATUS)
        .and_then(MapiValue::as_i64)
        .map(|value| {
            if value == FOLLOWUP_COMPLETE as i64 {
                "completed"
            } else {
                "needs-action"
            }
        })
        .unwrap_or(&existing.status)
        .to_string();
    let due_at = properties
        .get(&PID_TAG_END_DATE)
        .and_then(MapiValue::as_i64)
        .and_then(filetime_to_date_time)
        .map(|(date, time)| format!("{date}T{time}:00Z"))
        .or_else(|| existing.due_at.clone());
    UpsertClientTaskInput {
        id,
        principal_account_id: account_id,
        account_id,
        task_list_id: collection_id
            .and_then(|value| Uuid::parse_str(value).ok())
            .or_else(|| (existing.task_list_id != Uuid::nil()).then_some(existing.task_list_id)),
        title,
        description: optional_pending_text_property(properties, &[PID_TAG_BODY_W])
            .unwrap_or_else(|| existing.description.clone()),
        status,
        due_at,
        completed_at: existing.completed_at.clone(),
        recurrence_rule: existing.recurrence_rule.clone(),
        sort_order: existing.sort_order,
    }
}

fn reject_unsupported_mapi_task_properties(properties: &HashMap<u32, MapiValue>) -> Result<()> {
    for tag in properties.keys() {
        let supported = matches!(
            canonical_property_storage_tag(*tag),
            PID_TAG_SUBJECT_W
                | PID_TAG_NORMALIZED_SUBJECT_W
                | PID_TAG_DISPLAY_NAME_W
                | PID_TAG_BODY_W
                | PID_TAG_FLAG_STATUS
                | PID_TAG_END_DATE
        );
        if !supported {
            return Err(anyhow!(
                "MAPI task property {tag:#010X} is outside the canonical task subset"
            ));
        }
    }
    Ok(())
}

pub(in crate::mapi) async fn apply_canonical_task_property_values<S>(
    store: &S,
    principal: &AccountPrincipal,
    folder_id: u64,
    task_id: u64,
    values: Vec<(u32, MapiValue)>,
    snapshot: &MapiMailStoreSnapshot,
) -> Result<()>
where
    S: ExchangeStore,
{
    let task = snapshot
        .task_for_id(folder_id, task_id)
        .ok_or_else(|| anyhow!("canonical MAPI task was not found"))?;
    let (properties, reminder_set, reminder_at) = split_reminder_property_values(values)?;
    if reminder_set.is_some() || reminder_at.is_some() {
        store
            .update_accessible_task_reminder(
                principal.account_id,
                task.canonical_id,
                reminder_set,
                reminder_at,
                None,
                None,
            )
            .await?;
    }
    if properties.is_empty() {
        return Ok(());
    }
    reject_unsupported_mapi_task_properties(&properties)?;
    let input = task_input_from_mapi(
        principal.account_id,
        Some(task.canonical_id),
        &task.task,
        None,
        &properties,
    );
    store
        .update_accessible_task(principal.account_id, task.canonical_id, input)
        .await?;
    Ok(())
}
