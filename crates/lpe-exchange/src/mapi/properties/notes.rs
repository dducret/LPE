use super::*;

pub(in crate::mapi) fn note_property_value(
    note: &ClientNote,
    item_id: u64,
    folder_id: u64,
    property_tag: u32,
) -> Option<MapiValue> {
    if let Some(value) = note_named_property_value(note, property_tag) {
        return Some(value);
    }
    let property_tag = canonical_property_storage_tag(property_tag);
    let change_number = mapi_mailstore::change_number_for_store_id(item_id);
    match property_tag {
        PID_TAG_MID => Some(MapiValue::U64(item_id)),
        PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W | PID_TAG_DISPLAY_NAME_W => {
            Some(MapiValue::String(note.title.clone()))
        }
        PID_TAG_BODY_W => Some(MapiValue::String(note.body_text.clone())),
        PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String("IPM.StickyNote".to_string())),
        PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_MESSAGE_ACCESS)),
        PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(MSGFLAG_READ)),
        PID_TAG_HAS_ATTACHMENTS => Some(MapiValue::Bool(false)),
        PID_TAG_MESSAGE_SIZE => Some(mapi_message_size_value(note_size(note))),
        PID_TAG_MESSAGE_SIZE_EXTENDED => Some(mapi_message_size_extended_value(note_size(note))),
        PID_TAG_LAST_MODIFICATION_TIME | PID_TAG_LOCAL_COMMIT_TIME => Some(MapiValue::U64(
            mapi_mailstore::filetime_from_rfc3339_utc(&note.updated_at),
        )),
        PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
            crate::mapi::identity::instance_key_for_object_id(item_id),
        )),
        PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_uuid(
            &note.id,
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

pub(in crate::mapi) fn journal_entry_property_value(
    entry: &JournalEntry,
    item_id: u64,
    folder_id: u64,
    property_tag: u32,
) -> Option<MapiValue> {
    if let Some(value) = journal_entry_named_property_value(entry, property_tag) {
        return Some(value);
    }
    let property_tag = canonical_property_storage_tag(property_tag);
    let change_number = mapi_mailstore::change_number_for_store_id(item_id);
    match property_tag {
        PID_TAG_MID => Some(MapiValue::U64(item_id)),
        PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W | PID_TAG_DISPLAY_NAME_W => {
            Some(MapiValue::String(entry.subject.clone()))
        }
        PID_TAG_BODY_W => Some(MapiValue::String(entry.body_text.clone())),
        PID_TAG_START_DATE | PID_TAG_MESSAGE_DELIVERY_TIME => entry
            .starts_at
            .as_deref()
            .or(entry.occurred_at.as_deref())
            .map(|value| MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(value))),
        PID_TAG_END_DATE => entry
            .ends_at
            .as_deref()
            .map(|value| MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(value))),
        PID_TAG_LAST_MODIFICATION_TIME | PID_TAG_LOCAL_COMMIT_TIME => Some(MapiValue::U64(
            mapi_mailstore::filetime_from_rfc3339_utc(&entry.updated_at),
        )),
        PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String(entry.message_class.clone())),
        PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_MESSAGE_ACCESS)),
        PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(MSGFLAG_READ)),
        PID_TAG_HAS_ATTACHMENTS => Some(MapiValue::Bool(false)),
        PID_TAG_MESSAGE_SIZE => Some(mapi_message_size_value(journal_entry_size(entry))),
        PID_TAG_MESSAGE_SIZE_EXTENDED => {
            Some(mapi_message_size_extended_value(journal_entry_size(entry)))
        }
        PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
            crate::mapi::identity::instance_key_for_object_id(item_id),
        )),
        PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_uuid(
            &entry.id,
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

pub(in crate::mapi) fn note_named_property_value(
    note: &ClientNote,
    property_tag: u32,
) -> Option<MapiValue> {
    match property_tag {
        PID_LID_NOTE_COLOR_TAG => Some(MapiValue::I32(note_color_value(&note.color))),
        _ => None,
    }
}

pub(in crate::mapi) fn journal_entry_named_property_value(
    entry: &JournalEntry,
    property_tag: u32,
) -> Option<MapiValue> {
    match property_tag {
        PID_LID_COMMON_START_TAG | PID_LID_LOG_START_TAG => entry
            .starts_at
            .as_deref()
            .or(entry.occurred_at.as_deref())
            .map(|value| MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(value))),
        PID_LID_COMMON_END_TAG | PID_LID_LOG_END_TAG => entry
            .ends_at
            .as_deref()
            .map(|value| MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(value))),
        PID_LID_COMPANIES_TAG => Some(MapiValue::MultiString(json_string_array(
            &entry.companies_json,
        ))),
        PID_LID_CONTACTS_TAG => Some(MapiValue::MultiString(json_string_array(
            &entry.contacts_json,
        ))),
        PID_LID_CONTACT_LINK_NAME_W_TAG | PID_LID_CONTACT_LINK_NAME_STRING8_TAG => {
            let names = json_string_array(&entry.contacts_json);
            (!names.is_empty()).then(|| MapiValue::String(names.join("; ")))
        }
        PID_LID_CONTACT_LINK_ENTRY_TAG => Some(MapiValue::Binary(empty_contact_link_entry_blob())),
        PID_LID_CONTACT_LINK_SEARCH_KEY_TAG => {
            Some(MapiValue::Binary(empty_contact_link_search_key_blob()))
        }
        PID_LID_LOG_TYPE_W_TAG | PID_LID_LOG_TYPE_STRING8_TAG => {
            Some(MapiValue::String(entry.entry_type.clone()))
        }
        PID_LID_LOG_TYPE_DESC_W_TAG | PID_LID_LOG_TYPE_DESC_STRING8_TAG => {
            Some(MapiValue::String(entry.entry_type.clone()))
        }
        PID_LID_LOG_DURATION_TAG => Some(MapiValue::I32(0)),
        PID_LID_LOG_FLAGS_TAG => Some(MapiValue::I32(0)),
        _ => None,
    }
}

fn note_color_value(color: &str) -> i32 {
    match color.trim().to_ascii_lowercase().as_str() {
        "blue" => 0,
        "green" => 1,
        "pink" => 2,
        "white" => 4,
        _ => 3,
    }
}

fn note_color_name(value: i64) -> &'static str {
    match value {
        0 => "blue",
        1 => "green",
        2 => "pink",
        4 => "white",
        _ => "yellow",
    }
}

pub(in crate::mapi) fn json_string_array(value: &str) -> Vec<String> {
    serde_json::from_str::<Vec<String>>(value).unwrap_or_default()
}

fn contact_names_from_link_name(value: &str) -> Vec<String> {
    value
        .split(';')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect()
}

pub(in crate::mapi) fn empty_contact_link_entry_blob() -> Vec<u8> {
    0u32.to_le_bytes().to_vec()
}

pub(in crate::mapi) fn empty_contact_link_search_key_blob() -> Vec<u8> {
    0u16.to_le_bytes().to_vec()
}

fn json_from_mapi_multi_string_value(
    properties: &HashMap<u32, MapiValue>,
    tag: u32,
) -> Option<String> {
    match properties.get(&tag) {
        Some(MapiValue::MultiString(values)) => serde_json::to_string(values).ok(),
        Some(MapiValue::String(value)) if !value.trim().is_empty() => {
            serde_json::to_string(&vec![value.clone()]).ok()
        }
        _ => None,
    }
}

fn json_from_mapi_multi_string(
    properties: &HashMap<u32, MapiValue>,
    tag: u32,
    existing: &str,
) -> String {
    json_from_mapi_multi_string_value(properties, tag).unwrap_or_else(|| existing.to_string())
}

pub(in crate::mapi) fn note_size(note: &ClientNote) -> i64 {
    note.title
        .len()
        .saturating_add(note.body_text.len())
        .min(i64::MAX as usize) as i64
}

pub(in crate::mapi) fn journal_entry_size(entry: &JournalEntry) -> i64 {
    entry
        .subject
        .len()
        .saturating_add(entry.body_text.len())
        .saturating_add(entry.entry_type.len())
        .saturating_add(entry.companies_json.len())
        .saturating_add(entry.contacts_json.len())
        .min(i64::MAX as usize) as i64
}

pub(in crate::mapi) fn default_note_for_mapping() -> ClientNote {
    ClientNote {
        id: Uuid::nil(),
        title: String::new(),
        body_text: String::new(),
        color: "yellow".to_string(),
        categories_json: "[]".to_string(),
        created_at: "1970-01-01T00:00:00Z".to_string(),
        updated_at: "1970-01-01T00:00:00Z".to_string(),
    }
}

pub(in crate::mapi) fn default_journal_entry_for_mapping() -> JournalEntry {
    JournalEntry {
        id: Uuid::nil(),
        subject: String::new(),
        body_text: String::new(),
        entry_type: String::new(),
        message_class: "IPM.Activity".to_string(),
        starts_at: None,
        ends_at: None,
        occurred_at: None,
        companies_json: "[]".to_string(),
        contacts_json: "[]".to_string(),
        created_at: "1970-01-01T00:00:00Z".to_string(),
        updated_at: "1970-01-01T00:00:00Z".to_string(),
    }
}

pub(in crate::mapi) fn note_input_from_mapi(
    account_id: Uuid,
    id: Option<Uuid>,
    existing: &ClientNote,
    properties: &HashMap<u32, MapiValue>,
) -> UpsertClientNoteInput {
    UpsertClientNoteInput {
        id,
        account_id,
        title: optional_pending_text_property(
            properties,
            &[
                PID_TAG_SUBJECT_W,
                PID_TAG_NORMALIZED_SUBJECT_W,
                PID_TAG_DISPLAY_NAME_W,
            ],
        )
        .unwrap_or_else(|| existing.title.clone()),
        body_text: optional_pending_text_property(properties, &[PID_TAG_BODY_W])
            .unwrap_or_else(|| existing.body_text.clone()),
        color: properties
            .get(&PID_LID_NOTE_COLOR_TAG)
            .and_then(MapiValue::as_i64)
            .map(note_color_name)
            .unwrap_or(&existing.color)
            .to_string(),
        categories_json: existing.categories_json.clone(),
    }
}

fn reject_unsupported_mapi_note_properties(properties: &HashMap<u32, MapiValue>) -> Result<()> {
    for tag in properties.keys() {
        let supported = matches!(
            canonical_property_storage_tag(*tag),
            PID_TAG_SUBJECT_W
                | PID_TAG_NORMALIZED_SUBJECT_W
                | PID_TAG_DISPLAY_NAME_W
                | PID_TAG_BODY_W
                | PID_LID_NOTE_COLOR_TAG
        );
        if !supported {
            return Err(anyhow!(
                "MAPI note property {tag:#010X} is outside the canonical note subset"
            ));
        }
    }
    Ok(())
}

pub(in crate::mapi) fn journal_entry_input_from_mapi(
    account_id: Uuid,
    id: Option<Uuid>,
    existing: &JournalEntry,
    properties: &HashMap<u32, MapiValue>,
) -> UpsertJournalEntryInput {
    UpsertJournalEntryInput {
        id,
        account_id,
        subject: optional_pending_text_property(
            properties,
            &[
                PID_TAG_SUBJECT_W,
                PID_TAG_NORMALIZED_SUBJECT_W,
                PID_TAG_DISPLAY_NAME_W,
            ],
        )
        .unwrap_or_else(|| existing.subject.clone()),
        body_text: optional_pending_text_property(properties, &[PID_TAG_BODY_W])
            .unwrap_or_else(|| existing.body_text.clone()),
        entry_type: optional_pending_text_property(
            properties,
            &[
                PID_LID_LOG_TYPE_W_TAG,
                PID_LID_LOG_TYPE_STRING8_TAG,
                PID_LID_LOG_TYPE_DESC_W_TAG,
                PID_LID_LOG_TYPE_DESC_STRING8_TAG,
            ],
        )
        .unwrap_or_else(|| existing.entry_type.clone()),
        message_class: optional_pending_text_property(properties, &[PID_TAG_MESSAGE_CLASS_W])
            .unwrap_or_else(|| existing.message_class.clone()),
        starts_at: properties
            .get(&PID_TAG_START_DATE)
            .or_else(|| properties.get(&PID_LID_COMMON_START_TAG))
            .or_else(|| properties.get(&PID_LID_LOG_START_TAG))
            .and_then(MapiValue::as_i64)
            .and_then(filetime_to_date_time)
            .map(|(date, time)| format!("{date}T{time}:00Z"))
            .or_else(|| existing.starts_at.clone()),
        ends_at: properties
            .get(&PID_TAG_END_DATE)
            .or_else(|| properties.get(&PID_LID_COMMON_END_TAG))
            .or_else(|| properties.get(&PID_LID_LOG_END_TAG))
            .and_then(MapiValue::as_i64)
            .and_then(filetime_to_date_time)
            .map(|(date, time)| format!("{date}T{time}:00Z"))
            .or_else(|| existing.ends_at.clone()),
        occurred_at: existing.occurred_at.clone(),
        companies_json: json_from_mapi_multi_string(
            properties,
            PID_LID_COMPANIES_TAG,
            &existing.companies_json,
        ),
        contacts_json: json_from_mapi_multi_string_value(properties, PID_LID_CONTACTS_TAG)
            .or_else(|| {
                optional_pending_text_property(
                    properties,
                    &[
                        PID_LID_CONTACT_LINK_NAME_W_TAG,
                        PID_LID_CONTACT_LINK_NAME_STRING8_TAG,
                    ],
                )
                .and_then(|value| {
                    let names = contact_names_from_link_name(&value);
                    (!names.is_empty()).then(|| serde_json::to_string(&names).ok())
                })
                .flatten()
            })
            .unwrap_or_else(|| existing.contacts_json.clone()),
    }
}

fn reject_unsupported_mapi_journal_entry_properties(
    properties: &HashMap<u32, MapiValue>,
) -> Result<()> {
    for tag in properties.keys() {
        let supported = matches!(
            canonical_property_storage_tag(*tag),
            PID_TAG_SUBJECT_W
                | PID_TAG_NORMALIZED_SUBJECT_W
                | PID_TAG_DISPLAY_NAME_W
                | PID_TAG_BODY_W
                | PID_TAG_MESSAGE_CLASS_W
                | PID_TAG_START_DATE
                | PID_TAG_END_DATE
                | PID_LID_COMMON_START_TAG
                | PID_LID_COMMON_END_TAG
                | PID_LID_LOG_START_TAG
                | PID_LID_LOG_END_TAG
                | PID_LID_LOG_TYPE_W_TAG
                | PID_LID_LOG_TYPE_STRING8_TAG
                | PID_LID_LOG_TYPE_DESC_W_TAG
                | PID_LID_LOG_TYPE_DESC_STRING8_TAG
                | PID_LID_COMPANIES_TAG
                | PID_LID_CONTACTS_TAG
                | PID_LID_CONTACT_LINK_NAME_W_TAG
                | PID_LID_CONTACT_LINK_NAME_STRING8_TAG
                | PID_LID_CONTACT_LINK_ENTRY_TAG
                | PID_LID_CONTACT_LINK_SEARCH_KEY_TAG
        );
        if !supported {
            return Err(anyhow!(
                "MAPI journal property {tag:#010X} is outside the canonical journal subset"
            ));
        }
    }
    Ok(())
}

pub(in crate::mapi) async fn apply_canonical_note_property_values<S>(
    store: &S,
    principal: &AccountPrincipal,
    folder_id: u64,
    note_id: u64,
    values: Vec<(u32, MapiValue)>,
    snapshot: &MapiMailStoreSnapshot,
) -> Result<()>
where
    S: ExchangeStore,
{
    let note = snapshot
        .note_for_id(folder_id, note_id)
        .ok_or_else(|| anyhow!("canonical MAPI note was not found"))?;
    let properties = values.into_iter().collect::<HashMap<_, _>>();
    reject_unsupported_mapi_note_properties(&properties)?;
    let input = note_input_from_mapi(
        principal.account_id,
        Some(note.canonical_id),
        &note.note,
        &properties,
    );
    store.upsert_mapi_note(input).await?;
    Ok(())
}

pub(in crate::mapi) async fn apply_canonical_journal_entry_property_values<S>(
    store: &S,
    principal: &AccountPrincipal,
    folder_id: u64,
    journal_entry_id: u64,
    values: Vec<(u32, MapiValue)>,
    snapshot: &MapiMailStoreSnapshot,
) -> Result<()>
where
    S: ExchangeStore,
{
    let entry = snapshot
        .journal_entry_for_id(folder_id, journal_entry_id)
        .ok_or_else(|| anyhow!("canonical MAPI journal entry was not found"))?;
    let properties = values.into_iter().collect::<HashMap<_, _>>();
    reject_unsupported_mapi_journal_entry_properties(&properties)?;
    let input = journal_entry_input_from_mapi(
        principal.account_id,
        Some(entry.canonical_id),
        &entry.entry,
        &properties,
    );
    store.upsert_mapi_journal_entry(input).await?;
    Ok(())
}
