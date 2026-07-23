use super::*;

pub(super) fn default_folder_entry_id_expected_folder_id(tag: u32) -> Option<u64> {
    match canonical_property_storage_tag(tag) {
        PID_TAG_IPM_SUBTREE_ENTRY_ID => Some(IPM_SUBTREE_FOLDER_ID),
        PID_TAG_IPM_OUTBOX_ENTRY_ID => Some(OUTBOX_FOLDER_ID),
        PID_TAG_IPM_WASTEBASKET_ENTRY_ID => Some(TRASH_FOLDER_ID),
        PID_TAG_IPM_SENTMAIL_ENTRY_ID => Some(SENT_FOLDER_ID),
        PID_TAG_VIEWS_ENTRY_ID => Some(VIEWS_FOLDER_ID),
        PID_TAG_COMMON_VIEWS_ENTRY_ID => Some(COMMON_VIEWS_FOLDER_ID),
        PID_TAG_FINDER_ENTRY_ID => Some(SEARCH_FOLDER_ID),
        PID_TAG_IPM_ARCHIVE_ENTRY_ID => Some(ARCHIVE_FOLDER_ID),
        PID_TAG_IPM_APPOINTMENT_ENTRY_ID => Some(CALENDAR_FOLDER_ID),
        PID_TAG_IPM_CONTACT_ENTRY_ID => Some(CONTACTS_FOLDER_ID),
        PID_TAG_IPM_JOURNAL_ENTRY_ID => Some(JOURNAL_FOLDER_ID),
        PID_TAG_IPM_NOTE_ENTRY_ID => Some(NOTES_FOLDER_ID),
        PID_TAG_IPM_TASK_ENTRY_ID => Some(TASKS_FOLDER_ID),
        PID_TAG_REM_ONLINE_ENTRY_ID => Some(REMINDERS_FOLDER_ID),
        PID_TAG_REM_OFFLINE_ENTRY_ID => Some(REMINDERS_FOLDER_ID),
        PID_TAG_IPM_DRAFTS_ENTRY_ID => Some(DRAFTS_FOLDER_ID),
        tag if is_scalar_default_folder_entry_id_property_tag(tag) => None,
        _ => None,
    }
}

pub(super) fn folder_set_property_problems(
    object: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    values: &[(u32, MapiValue)],
) -> Vec<(usize, u32, u32)> {
    let Some(MapiObject::Folder { folder_id, .. }) = object else {
        return Vec::new();
    };
    values
        .iter()
        .enumerate()
        .filter_map(|(index, (tag, value))| {
            let storage_tag = canonical_property_storage_tag(*tag);
            if *folder_id == IPM_SUBTREE_FOLDER_ID && storage_tag == PID_TAG_OST_OSTID {
                return match value {
                    MapiValue::Binary(bytes) if !bytes.is_empty() => None,
                    _ => Some((index, *tag, 0x8004_0102)),
                };
            }
            if storage_tag == PID_TAG_ADDITIONAL_REN_ENTRY_IDS {
                if !matches!(*folder_id, ROOT_FOLDER_ID | INBOX_FOLDER_ID) {
                    return Some((index, *tag, 0x8004_0102));
                }
                return match value {
                    MapiValue::MultiBinary(values)
                        if !values.is_empty()
                            && additional_ren_entry_ids_profile_bytes(value).is_some() =>
                    {
                        None
                    }
                    _ => Some((index, *tag, 0x8004_0102)),
                };
            }
            if storage_tag == PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX {
                if !matches!(*folder_id, ROOT_FOLDER_ID | INBOX_FOLDER_ID) {
                    return Some((index, *tag, 0x8004_0102));
                }
                return match value {
                    MapiValue::Binary(bytes) if !bytes.is_empty() => None,
                    _ => Some((index, *tag, 0x8004_0102)),
                };
            }
            if storage_tag == PID_TAG_FREE_BUSY_ENTRY_IDS {
                if !matches!(*folder_id, ROOT_FOLDER_ID | INBOX_FOLDER_ID) {
                    return Some((index, *tag, 0x8004_0102));
                }
                let MapiValue::MultiBinary(values) = value else {
                    return Some((index, *tag, 0x8004_0102));
                };
                return match values.get(3).and_then(|bytes| {
                    crate::mapi::identity::object_id_from_folder_identifier_bytes(bytes)
                }) {
                    Some(folder_id)
                        if default_folder_id_matches_or_is_persistable_alias_candidate(
                            folder_id,
                            FREEBUSY_DATA_FOLDER_ID,
                        ) =>
                    {
                        None
                    }
                    _ => Some((index, *tag, 0x8004_0102)),
                };
            }
            if storage_tag == PID_TAG_EXTENDED_FOLDER_FLAGS {
                return match value {
                    MapiValue::Binary(bytes) if !bytes.is_empty() && bytes.len() <= 4096 => None,
                    _ => Some((index, *tag, 0x8004_0102)),
                };
            }
            let hidden_configuration_class =
                hidden_configuration_folder_message_class(*folder_id, mailboxes);
            if storage_tag == PID_TAG_ATTRIBUTE_HIDDEN && hidden_configuration_class.is_some() {
                return match value {
                    MapiValue::Bool(_) => None,
                    _ => Some((index, *tag, 0x8004_0102)),
                };
            }
            if storage_tag == PID_TAG_CONTAINER_CLASS_W {
                if let Some(expected_class) = hidden_configuration_class {
                    return match value {
                        MapiValue::String(value) if value.eq_ignore_ascii_case(expected_class) => {
                            None
                        }
                        _ => Some((index, *tag, 0x8004_0102)),
                    };
                }
            }
            if !matches!(*folder_id, ROOT_FOLDER_ID | INBOX_FOLDER_ID) {
                return Some((index, *tag, 0x8004_0102));
            }
            if !is_scalar_default_folder_entry_id_property_tag(storage_tag) {
                return Some((index, *tag, 0x8004_0102));
            }
            let Some(expected_folder_id) = default_folder_entry_id_expected_folder_id(storage_tag)
            else {
                return Some((index, *tag, 0x8004_0102));
            };
            let MapiValue::Binary(bytes) = value else {
                return Some((index, *tag, 0x8004_0102));
            };
            match crate::mapi::identity::object_id_from_folder_identifier_bytes(bytes) {
                Some(folder_id)
                    if default_folder_id_matches_or_is_persistable_alias_candidate(
                        folder_id,
                        expected_folder_id,
                    ) =>
                {
                    None
                }
                _ => Some((index, *tag, 0x8004_0102)),
            }
        })
        .collect()
}

fn default_folder_id_matches_or_is_persistable_alias_candidate(
    folder_id: u64,
    expected_folder_id: u64,
) -> bool {
    folder_id == expected_folder_id
        || crate::mapi::identity::global_counter_from_store_id(folder_id).is_some_and(|counter| {
            (crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER
                ..crate::mapi::identity::FIRST_RESERVED_HIGH_GLOBAL_COUNTER)
                .contains(&counter)
        })
}

pub(super) fn default_folder_identification_safe_property_values(
    principal: &AccountPrincipal,
    object: Option<&MapiObject>,
    values: Vec<(u32, MapiValue)>,
) -> Vec<(u32, MapiValue)> {
    if !strips_any_default_folder_identification_values(object) {
        return values;
    }
    values
        .into_iter()
        .filter_map(|(tag, value)| {
            default_folder_identification_safe_property_value(principal, object, tag, value)
        })
        .collect()
}

fn default_folder_identification_safe_property_value(
    principal: &AccountPrincipal,
    object: Option<&MapiObject>,
    tag: u32,
    value: MapiValue,
) -> Option<(u32, MapiValue)> {
    if !strips_default_folder_identification_value(object, tag) {
        return Some((tag, value));
    }
    match canonical_property_storage_tag(tag) {
        storage_tag if is_scalar_default_folder_entry_id_property_tag(storage_tag) => {
            special_folder_identification_property_value(principal.account_id, storage_tag)
                .map(|value| (storage_tag, value))
        }
        PID_TAG_ADDITIONAL_REN_ENTRY_IDS => Some((canonical_property_storage_tag(tag), value)),
        PID_TAG_FREE_BUSY_ENTRY_IDS => {
            merge_indexed_special_folder_entry_ids(principal, tag, value)
                .map(|value| (canonical_property_storage_tag(tag), value))
        }
        _ => None,
    }
}

fn merge_indexed_special_folder_entry_ids(
    principal: &AccountPrincipal,
    tag: u32,
    value: MapiValue,
) -> Option<MapiValue> {
    let MapiValue::MultiBinary(client_values) = value else {
        return None;
    };
    let Some(MapiValue::MultiBinary(mut canonical_values)) =
        special_folder_identification_property_value(principal.account_id, tag)
    else {
        return None;
    };
    let canonical_len = canonical_values.len();
    if client_values.len() > canonical_len {
        canonical_values.extend(client_values.into_iter().skip(canonical_len));
    }
    Some(MapiValue::MultiBinary(canonical_values))
}

// [MS-OXOSFLD] section 2.2.4 requires preserving values at unassigned
// AdditionalRenEntryIds indexes. [MS-OXCSPAM] section 3.2.4.1.2 assigns
// Outlook's Junk Email Move Stamp to index 5, so retain the complete wire
// value rather than synthesizing only the five documented folder entries.
pub(super) fn additional_ren_entry_ids_profile_bytes(value: &MapiValue) -> Option<Vec<u8>> {
    let MapiValue::MultiBinary(values) = value else {
        return None;
    };
    if values.is_empty() || values.iter().any(|value| value.len() > u16::MAX as usize) {
        return None;
    }
    let mut bytes = Vec::new();
    write_mapi_value(&mut bytes, PID_TAG_ADDITIONAL_REN_ENTRY_IDS, value);
    (bytes.len() <= 4096).then_some(bytes)
}

pub(super) fn additional_ren_entry_ids_from_profile_bytes(bytes: &[u8]) -> Option<MapiValue> {
    let mut cursor = Cursor::new(bytes);
    let value = parse_mapi_property_value(&mut cursor, PID_TAG_ADDITIONAL_REN_ENTRY_IDS).ok()?;
    matches!(&value, MapiValue::MultiBinary(values) if !values.is_empty())
        .then_some(value)
        .filter(|_| cursor.remaining() == 0)
}

pub(super) fn default_folder_entry_id_aliases(
    object: Option<&MapiObject>,
    values: &[(u32, MapiValue)],
) -> Vec<MapiSpecialFolderAlias> {
    if !strips_any_default_folder_identification_values(object) {
        return Vec::new();
    }
    let mut aliases = Vec::new();
    for (tag, value) in values {
        let storage_tag = canonical_property_storage_tag(*tag);
        if storage_tag == PID_TAG_ADDITIONAL_REN_ENTRY_IDS {
            aliases.extend(indexed_special_folder_aliases(
                value,
                &[
                    CONFLICTS_FOLDER_ID,
                    SYNC_ISSUES_FOLDER_ID,
                    LOCAL_FAILURES_FOLDER_ID,
                    SERVER_FAILURES_FOLDER_ID,
                    JUNK_FOLDER_ID,
                ],
            ));
        } else if storage_tag == PID_TAG_FREE_BUSY_ENTRY_IDS {
            aliases.extend(indexed_special_folder_aliases(
                value,
                &[0, 0, 0, FREEBUSY_DATA_FOLDER_ID],
            ));
        } else if is_scalar_default_folder_entry_id_property_tag(storage_tag) {
            let Some(expected_folder_id) = default_folder_entry_id_expected_folder_id(storage_tag)
            else {
                continue;
            };
            let MapiValue::Binary(bytes) = value else {
                continue;
            };
            if let Some(alias) = special_folder_alias(bytes, expected_folder_id) {
                aliases.push(alias);
            }
        }
    }
    aliases
}

fn indexed_special_folder_aliases(
    value: &MapiValue,
    expected_folder_ids: &[u64],
) -> Vec<MapiSpecialFolderAlias> {
    let MapiValue::MultiBinary(values) = value else {
        return Vec::new();
    };
    let mut aliases = Vec::new();
    for (index, bytes) in values.iter().enumerate() {
        let Some(expected_folder_id) = expected_folder_ids.get(index).copied() else {
            continue;
        };
        if expected_folder_id == 0 {
            continue;
        }
        if let Some(alias) = special_folder_alias(bytes, expected_folder_id) {
            aliases.push(alias);
        }
    }
    aliases
}

fn special_folder_alias(bytes: &[u8], expected_folder_id: u64) -> Option<MapiSpecialFolderAlias> {
    let Some(alias_id) = crate::mapi::identity::object_id_from_folder_identifier_bytes(bytes)
    else {
        return None;
    };
    if alias_id == expected_folder_id {
        return None;
    }
    Some(MapiSpecialFolderAlias {
        alias_folder_id: alias_id,
        canonical_folder_id: expected_folder_id,
        source_key: crate::mapi::identity::source_key_for_object_id(alias_id),
    })
}

pub(super) fn default_folder_identification_values_stripped_by_safe_values(
    object: Option<&MapiObject>,
    property_tags: &[u32],
) -> bool {
    property_tags
        .iter()
        .any(|tag| strips_default_folder_identification_value(object, *tag))
}

fn strips_default_folder_identification_value(object: Option<&MapiObject>, tag: u32) -> bool {
    match object {
        Some(MapiObject::Folder { folder_id, .. }) => {
            strips_default_folder_identification_value_for_folder_id(*folder_id, tag)
        }
        _ => false,
    }
}

pub(super) fn strips_default_folder_identification_value_for_folder_id(
    folder_id: u64,
    tag: u32,
) -> bool {
    if !is_default_folder_identification_property_tag(tag) {
        return false;
    }
    matches!(folder_id, ROOT_FOLDER_ID | INBOX_FOLDER_ID)
        && (matches!(
            canonical_property_storage_tag(tag),
            PID_TAG_ADDITIONAL_REN_ENTRY_IDS
                | PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX
                | PID_TAG_FREE_BUSY_ENTRY_IDS
        ) || is_scalar_default_folder_entry_id_property_tag(tag))
}

fn strips_any_default_folder_identification_values(object: Option<&MapiObject>) -> bool {
    matches!(
        object,
        Some(MapiObject::Folder {
            folder_id: ROOT_FOLDER_ID | INBOX_FOLDER_ID,
            ..
        })
    )
}

fn hidden_configuration_folder_message_class(
    folder_id: u64,
    mailboxes: &[JmapMailbox],
) -> Option<&'static str> {
    if let Some(mailbox) = mailboxes.iter().find(|mailbox| {
        try_mapi_folder_id(mailbox) == Some(folder_id)
            || mailbox.role == role_for_folder_id(folder_id).unwrap_or_default()
    }) {
        if mailbox_projects_hidden_attribute(mailbox) {
            return Some(folder_message_class(mailbox));
        }
    }
    if is_advertised_special_folder(folder_id)
        && matches!(
            folder_id,
            CONVERSATION_ACTION_SETTINGS_FOLDER_ID | QUICK_STEP_SETTINGS_FOLDER_ID
        )
    {
        return Some("IPF.Configuration");
    }
    None
}
