use super::*;

#[derive(Debug)]
pub(super) struct BoundedSearchCriteria {
    pub(super) scope_json: Value,
    pub(super) restriction_json: Value,
}

pub(super) fn search_folder_handle_properties(
    definition: &SearchFolderDefinition,
    folder_id: u64,
    mailbox_guid: Uuid,
) -> HashMap<u32, MapiValue> {
    [
        PID_TAG_DISPLAY_NAME_W,
        PID_TAG_FOLDER_ID,
        PID_TAG_PARENT_FOLDER_ID,
        PID_TAG_FOLDER_TYPE,
        PID_TAG_CONTENT_COUNT,
        PID_TAG_CONTENT_UNREAD_COUNT,
        PID_TAG_ASSOCIATED_CONTENT_COUNT,
        PID_TAG_SUBFOLDERS,
        PID_TAG_ACCESS,
        PID_TAG_RIGHTS,
        PID_TAG_CONTAINER_CLASS_W,
        PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W,
        PID_TAG_DEFAULT_FORM_NAME_W,
        PID_TAG_EXTENDED_FOLDER_FLAGS,
        PID_TAG_FOLDER_FORM_FLAGS,
        PID_TAG_FOLDER_WEBVIEWINFO,
        PID_TAG_FOLDER_XVIEWINFO_E,
        PID_TAG_FOLDER_VIEWS_ONLY,
        PID_TAG_FOLDER_VIEWLIST_FLAGS,
        PID_TAG_ARCHIVE_TAG,
        PID_TAG_POLICY_TAG,
        PID_TAG_RETENTION_PERIOD,
        PID_TAG_RETENTION_FLAGS,
        PID_TAG_ARCHIVE_PERIOD,
        PID_TAG_ENTRY_ID,
        PID_TAG_RECORD_KEY,
        PID_TAG_SOURCE_KEY,
        PID_TAG_PARENT_SOURCE_KEY,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_CHANGE_NUMBER,
        PID_TAG_HIERARCHY_CHANGE_NUMBER,
    ]
    .into_iter()
    .filter_map(|tag| {
        search_folder_definition_property_value(definition, folder_id, tag, mailbox_guid)
            .map(|value| (tag, value))
    })
    .collect()
}

pub(super) fn bounded_search_criteria_from_rop(
    request: &RopRequest,
    search_folder_id: u64,
    previous_definition: Option<&SearchFolderDefinition>,
    mailboxes: &[JmapMailbox],
) -> Result<BoundedSearchCriteria, u32> {
    let restriction_bytes = request
        .search_criteria_restriction_bytes()
        .ok_or(EC_SEARCH_INVALID_PARAMETER)?;
    let restriction = if restriction_bytes.is_empty() {
        None
    } else {
        Some(parse_mapi_restriction(restriction_bytes).map_err(|_| EC_SEARCH_UNSUPPORTED)?)
    };
    let folder_ids = request
        .search_criteria_folder_ids()
        .ok_or(EC_SEARCH_INVALID_PARAMETER)?;
    let flags = request
        .search_criteria_flags()
        .ok_or(EC_SEARCH_INVALID_PARAMETER)?;
    if !set_search_criteria_flags_are_valid(flags) {
        return Err(EC_SEARCH_INVALID_PARAMETER);
    }

    let restriction_json = if let Some(restriction) = restriction {
        if microsoft_oxcdata_reminders_restriction(&restriction) {
            json!({
                "kind": "exchange_reminders",
                "match": "reminder_set_or_recurring",
                "recurrenceHorizonDays": 90,
                "occurrenceDismissals": true
            })
        } else {
            let clauses = bounded_search_restriction_clauses(&restriction)?;
            if clauses.is_empty() {
                return Err(EC_SEARCH_INVALID_PARAMETER);
            }
            json!({
                "kind": "mapi_bounded",
                "all": clauses
            })
        }
    } else {
        previous_mapi_bounded_restriction_json(previous_definition)
            .ok_or(EC_SEARCH_INVALID_PARAMETER)?
    };

    let scope_json = if folder_ids.is_empty() {
        let mut scope = previous_mapi_bounded_scope_json(previous_definition)
            .ok_or(EC_SEARCH_NOT_INITIALIZED)?;
        if let Some(scope) = scope.as_object_mut() {
            scope.insert(
                "recursive".to_string(),
                Value::Bool(flags & SEARCH_RECURSIVE_FLAG != 0),
            );
        }
        scope
    } else {
        let mut canonical_folder_ids = Vec::new();
        let mut folder_roles = Vec::new();
        for folder_id in folder_ids {
            if folder_id == search_folder_id {
                return Err(EC_SEARCH_SCOPE_VIOLATION);
            }
            if let Some(mailbox) = folder_row_for_id(folder_id, mailboxes) {
                canonical_folder_ids.push(mailbox.id.to_string());
                if !mailbox.role.is_empty() {
                    folder_roles.push(mailbox.role.clone());
                }
            } else if let Some(role) = role_for_folder_id(folder_id) {
                folder_roles.push(role.to_string());
            } else {
                return Err(EC_SEARCH_NOT_FOUND);
            }
        }
        folder_roles.sort();
        folder_roles.dedup();
        json!({
            "kind": "mapi_bounded",
            "scope": "folders",
            "recursive": flags & SEARCH_RECURSIVE_FLAG != 0,
            "folderIds": canonical_folder_ids,
            "folderRoles": folder_roles
        })
    };
    Ok(BoundedSearchCriteria {
        scope_json,
        restriction_json,
    })
}

fn previous_mapi_bounded_restriction_json(
    definition: Option<&SearchFolderDefinition>,
) -> Option<Value> {
    let restriction = &definition?.restriction_json;
    if restriction.get("kind").and_then(Value::as_str) != Some("mapi_bounded")
        || restriction
            .get("all")
            .and_then(Value::as_array)
            .is_none_or(Vec::is_empty)
    {
        return None;
    }
    Some(restriction.clone())
}

fn previous_mapi_bounded_scope_json(definition: Option<&SearchFolderDefinition>) -> Option<Value> {
    let scope = &definition?.scope_json;
    if scope.get("kind").and_then(Value::as_str) != Some("mapi_bounded")
        || (scope
            .get("folderIds")
            .and_then(Value::as_array)
            .is_none_or(Vec::is_empty)
            && scope
                .get("folderRoles")
                .and_then(Value::as_array)
                .is_none_or(Vec::is_empty))
    {
        return None;
    }
    Some(scope.clone())
}

fn set_search_criteria_flags_are_valid(flags: u32) -> bool {
    flags & !SET_SEARCH_VALID_FLAGS == 0
        && flags & (SET_SEARCH_STOP_FLAG | SET_SEARCH_RESTART_FLAG)
            != (SET_SEARCH_STOP_FLAG | SET_SEARCH_RESTART_FLAG)
        && flags & (SEARCH_RECURSIVE_FLAG | SET_SEARCH_SHALLOW_FLAG)
            != (SEARCH_RECURSIVE_FLAG | SET_SEARCH_SHALLOW_FLAG)
        && flags & (SET_SEARCH_CONTENT_INDEXED_FLAG | SET_SEARCH_NON_CONTENT_INDEXED_FLAG)
            != (SET_SEARCH_CONTENT_INDEXED_FLAG | SET_SEARCH_NON_CONTENT_INDEXED_FLAG)
}

fn bounded_search_restriction_clauses(restriction: &MapiRestriction) -> Result<Vec<Value>, u32> {
    match restriction {
        MapiRestriction::InvalidTableRestriction => Err(EC_SEARCH_UNSUPPORTED),
        MapiRestriction::And(children) => {
            let mut clauses = Vec::new();
            for child in children {
                clauses.extend(bounded_search_restriction_clauses(child)?);
            }
            Ok(clauses)
        }
        MapiRestriction::Content {
            property_tag,
            value,
            fuzzy_level_low,
            ..
        } => bounded_search_content_clause(*property_tag, value, *fuzzy_level_low)
            .map(|clause| vec![clause]),
        MapiRestriction::Not(child) => bounded_search_not_clause(child).map(|clause| vec![clause]),
        MapiRestriction::Property {
            relop,
            property_tag,
            value,
        } => {
            bounded_search_property_clause(*relop, *property_tag, value).map(|clause| vec![clause])
        }
        MapiRestriction::Bitmask {
            property_tag,
            mask,
            must_be_nonzero,
        } if *property_tag == PID_TAG_MESSAGE_FLAGS && *mask == MSGFLAG_READ => Ok(vec![json!({
            "field": "unread",
            "equals": !*must_be_nonzero
        })]),
        MapiRestriction::Exist { property_tag }
            if canonical_property_storage_tag(*property_tag) == PID_TAG_HAS_ATTACHMENTS =>
        {
            Ok(vec![json!({
                "field": "hasAttachment",
                "equals": true
            })])
        }
        _ => Err(EC_SEARCH_UNSUPPORTED),
    }
}

fn microsoft_oxcdata_reminders_restriction(restriction: &MapiRestriction) -> bool {
    match restriction {
        MapiRestriction::And(children) => {
            children
                .iter()
                .any(microsoft_oxcdata_excluded_parent_folders_restriction)
                && children
                    .iter()
                    .any(microsoft_oxcdata_reminder_core_restriction)
        }
        _ => microsoft_oxcdata_reminder_core_restriction(restriction),
    }
}

fn microsoft_oxcdata_excluded_parent_folders_restriction(restriction: &MapiRestriction) -> bool {
    match restriction {
        MapiRestriction::And(children) if children.len() >= 4 => children.iter().all(|child| {
            matches!(
                child,
                MapiRestriction::Property {
                    relop: 0x05,
                    property_tag,
                    value: MapiValue::Binary(_),
                } if canonical_property_storage_tag(*property_tag) == PID_TAG_PARENT_ENTRY_ID
            )
        }),
        _ => false,
    }
}

fn microsoft_oxcdata_reminder_core_restriction(restriction: &MapiRestriction) -> bool {
    const MSGFLAG_SUBMIT: u32 = 0x0000_0004;

    let MapiRestriction::And(children) = restriction else {
        return false;
    };

    children
        .iter()
        .any(microsoft_oxcdata_not_schedule_message_class)
        && children.iter().any(|child| {
            matches!(
                child,
                MapiRestriction::Bitmask {
                    property_tag,
                    mask: MSGFLAG_SUBMIT,
                    must_be_nonzero: false,
                } if canonical_property_storage_tag(*property_tag) == PID_TAG_MESSAGE_FLAGS
            )
        })
        && children.iter().any(microsoft_oxcdata_reminder_or_recurring)
}

fn microsoft_oxcdata_not_schedule_message_class(restriction: &MapiRestriction) -> bool {
    let MapiRestriction::Not(child) = restriction else {
        return false;
    };
    let MapiRestriction::And(children) = child.as_ref() else {
        return false;
    };
    children.iter().any(|child| {
        matches!(
            child,
            MapiRestriction::Exist { property_tag }
                if canonical_property_storage_tag(*property_tag) == PID_TAG_MESSAGE_CLASS_W
        )
    }) && children.iter().any(|child| {
        matches!(
            child,
            MapiRestriction::Content {
                property_tag,
                value,
                fuzzy_level_low: 0x0002,
                ..
            } if canonical_property_storage_tag(*property_tag) == PID_TAG_MESSAGE_CLASS_W
                && value.eq_ignore_ascii_case("IPM.Schedule")
        )
    })
}

fn microsoft_oxcdata_reminder_or_recurring(restriction: &MapiRestriction) -> bool {
    let MapiRestriction::Or(children) = restriction else {
        return false;
    };

    children
        .iter()
        .any(microsoft_oxcdata_reminder_set_true_property)
        && children
            .iter()
            .any(microsoft_oxcdata_recurring_exists_and_true)
}

fn microsoft_oxcdata_reminder_set_true_property(restriction: &MapiRestriction) -> bool {
    matches!(
        restriction,
        MapiRestriction::Property {
            relop: 0x04,
            property_tag,
            value: MapiValue::Bool(true),
        } if canonical_property_storage_tag(*property_tag) == PID_LID_REMINDER_SET_TAG
    )
}

fn microsoft_oxcdata_recurring_exists_and_true(restriction: &MapiRestriction) -> bool {
    let MapiRestriction::And(children) = restriction else {
        return false;
    };
    children.iter().any(|child| {
        matches!(
            child,
            MapiRestriction::Exist { property_tag }
                if canonical_property_storage_tag(*property_tag) == PID_LID_RECURRING_TAG
        )
    }) && children.iter().any(|child| {
        matches!(
            child,
            MapiRestriction::Property {
                relop: 0x04,
                property_tag,
                value: MapiValue::Bool(true),
            } if canonical_property_storage_tag(*property_tag) == PID_LID_RECURRING_TAG
        )
    })
}

fn bounded_search_content_clause(
    property_tag: u32,
    value: &str,
    fuzzy_level_low: u16,
) -> Result<Value, u32> {
    let field = match canonical_property_storage_tag(property_tag) {
        PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => "subject",
        PID_TAG_BODY_W | PID_TAG_BODY_STRING8 | PID_TAG_BODY_HTML_W => "body",
        PID_TAG_SENDER_NAME_W | PID_TAG_SENDER_EMAIL_ADDRESS_W => "sender",
        _ => return Err(EC_SEARCH_UNSUPPORTED),
    };
    let operator = match fuzzy_level_low {
        0x0000 => "equals",
        0x0001 => "contains",
        _ => return Err(EC_SEARCH_UNSUPPORTED),
    };
    Ok(json!({
        "field": field,
        operator: value
    }))
}

fn bounded_search_not_clause(restriction: &MapiRestriction) -> Result<Value, u32> {
    match restriction {
        MapiRestriction::Content {
            property_tag,
            value,
            fuzzy_level_low,
            ..
        } if canonical_property_storage_tag(*property_tag) == PID_TAG_MESSAGE_CLASS_W => {
            let operator = match fuzzy_level_low {
                0x0000 => "notEquals",
                0x0002 => "notPrefix",
                _ => return Err(EC_SEARCH_UNSUPPORTED),
            };
            Ok(json!({
                "field": "messageClass",
                operator: value
            }))
        }
        _ => Err(EC_SEARCH_UNSUPPORTED),
    }
}

fn bounded_search_property_clause(
    relop: u8,
    property_tag: u32,
    value: &MapiValue,
) -> Result<Value, u32> {
    match canonical_property_storage_tag(property_tag) {
        PID_TAG_READ if relop == 0x04 => Ok(json!({
            "field": "unread",
            "equals": !value.as_bool().ok_or(EC_SEARCH_UNSUPPORTED)?
        })),
        PID_TAG_FLAG_STATUS if relop == 0x04 => Ok(json!({
            "field": "flagged",
            "equals": value.as_i64().ok_or(EC_SEARCH_UNSUPPORTED)? == FOLLOWUP_FLAGGED as i64
        })),
        PID_TAG_HAS_ATTACHMENTS if relop == 0x04 => Ok(json!({
            "field": "hasAttachment",
            "equals": value.as_bool().ok_or(EC_SEARCH_UNSUPPORTED)?
        })),
        PID_TAG_IMPORTANCE if relop == 0x04 => Ok(json!({
            "field": "importance",
            "equals": value.as_i64().ok_or(EC_SEARCH_UNSUPPORTED)?
        })),
        PID_NAME_KEYWORDS_TAG if relop == 0x04 => Ok(json!({
            "field": "category",
            "equals": match value {
                MapiValue::String(value) => value.clone(),
                MapiValue::MultiString(values) if values.len() == 1 => values[0].clone(),
                _ => return Err(EC_SEARCH_UNSUPPORTED),
            }
        })),
        PID_TAG_SENDER_NAME_W | PID_TAG_SENDER_EMAIL_ADDRESS_W if relop == 0x04 => Ok(json!({
            "field": "sender",
            "equals": value.clone().into_text().ok_or(EC_SEARCH_UNSUPPORTED)?
        })),
        PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W if relop == 0x04 => Ok(json!({
            "field": "subject",
            "equals": value.clone().into_text().ok_or(EC_SEARCH_UNSUPPORTED)?
        })),
        PID_TAG_BODY_W | PID_TAG_BODY_STRING8 | PID_TAG_BODY_HTML_W if relop == 0x04 => Ok(json!({
            "field": "body",
            "equals": value.clone().into_text().ok_or(EC_SEARCH_UNSUPPORTED)?
        })),
        PID_TAG_CLIENT_SUBMIT_TIME | PID_TAG_MESSAGE_DELIVERY_TIME => {
            let value = filetime_to_rfc3339_utc(value.as_i64().ok_or(EC_SEARCH_UNSUPPORTED)?)
                .ok_or(EC_SEARCH_UNSUPPORTED)?;
            match relop {
                0x01 => "beforeOrAt",
                0x03 => "afterOrAt",
                0x04 => "equals",
                _ => return Err(EC_SEARCH_UNSUPPORTED),
            };
            let mut clause = serde_json::Map::new();
            clause.insert("field".to_string(), Value::String("receivedAt".to_string()));
            clause.insert(
                match relop {
                    0x01 => "beforeOrAt",
                    0x03 => "afterOrAt",
                    _ => "equals",
                }
                .to_string(),
                Value::String(value),
            );
            Ok(Value::Object(clause))
        }
        _ => Err(EC_SEARCH_UNSUPPORTED),
    }
}

pub(super) fn bounded_search_criteria_to_rop(
    definition: &lpe_storage::SearchFolderDefinition,
    mailboxes: &[JmapMailbox],
    use_unicode: bool,
) -> Result<(Vec<u8>, Vec<u64>, u32), u32> {
    if definition
        .restriction_json
        .get("kind")
        .and_then(Value::as_str)
        != Some("mapi_bounded")
    {
        return Err(EC_SEARCH_UNSUPPORTED);
    }
    let clauses = definition
        .restriction_json
        .get("all")
        .and_then(Value::as_array)
        .ok_or(EC_SEARCH_UNSUPPORTED)?;
    let mut message_class_restrictions = Vec::new();
    let mut other_restrictions = Vec::new();
    for clause in clauses {
        let restriction = rop_restriction_from_json_clause(clause, use_unicode)?;
        if is_message_class_exclusion_clause(clause) {
            message_class_restrictions.push(restriction);
        } else {
            other_restrictions.push(restriction);
        }
    }
    let restriction = if !message_class_restrictions.is_empty() && !other_restrictions.is_empty() {
        and_restriction(
            vec![
                and_restriction(message_class_restrictions, true),
                and_restriction(other_restrictions, true),
            ],
            true,
        )
    } else {
        message_class_restrictions.extend(other_restrictions);
        and_restriction(message_class_restrictions, false)
    };
    let mut folder_ids = Vec::new();
    if let Some(ids) = definition
        .scope_json
        .get("folderIds")
        .and_then(Value::as_array)
    {
        for id in ids {
            if let Some(id) = id.as_str().and_then(|id| uuid::Uuid::parse_str(id).ok()) {
                if let Some(mailbox) = mailboxes.iter().find(|mailbox| mailbox.id == id) {
                    if let Some(folder_id) =
                        crate::mapi::identity::mapped_mapi_object_id(&mailbox.id)
                    {
                        folder_ids.push(folder_id);
                    }
                }
            }
        }
    }
    if let Some(roles) = definition
        .scope_json
        .get("folderRoles")
        .and_then(Value::as_array)
    {
        for role in roles {
            if let Some(role) = role.as_str().and_then(folder_id_for_role) {
                folder_ids.push(role);
            }
        }
    }
    folder_ids.sort();
    folder_ids.dedup();
    let mut flags = SEARCH_RUNNING_FLAG;
    if definition
        .scope_json
        .get("recursive")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        flags |= SEARCH_RECURSIVE_FLAG;
    }
    Ok((restriction, folder_ids, flags))
}

fn is_message_class_exclusion_clause(clause: &Value) -> bool {
    clause.get("field").and_then(Value::as_str) == Some("messageClass")
        && (clause.get("notEquals").is_some() || clause.get("notPrefix").is_some())
}

fn and_restriction(mut restrictions: Vec<Vec<u8>>, force_wrapper: bool) -> Vec<u8> {
    if restrictions.is_empty() {
        Vec::new()
    } else if restrictions.len() == 1 && !force_wrapper {
        restrictions.remove(0)
    } else {
        let mut bytes = vec![0x00];
        bytes.extend_from_slice(&(restrictions.len() as u16).to_le_bytes());
        for child in restrictions {
            bytes.extend_from_slice(&child);
        }
        bytes
    }
}

pub(super) fn builtin_search_criteria_to_rop(
    definition: &lpe_storage::SearchFolderDefinition,
) -> Option<(Vec<u8>, Vec<u64>, u32)> {
    if !definition.is_builtin {
        return None;
    }
    let folder_ids = builtin_search_scope_folder_ids(definition.role.as_str())?;
    Some((
        Vec::new(),
        folder_ids,
        SEARCH_RUNNING_FLAG | SEARCH_RECURSIVE_FLAG,
    ))
}

fn builtin_search_scope_folder_ids(role: &str) -> Option<Vec<u64>> {
    match role {
        "contacts_search" => Some(vec![CONTACTS_FOLDER_ID]),
        "todo_search" => Some(vec![TASKS_FOLDER_ID]),
        "reminders" => Some(vec![CALENDAR_FOLDER_ID, TASKS_FOLDER_ID]),
        "tracked_mail_processing" => Some(vec![IPM_SUBTREE_FOLDER_ID]),
        _ => None,
    }
}

pub(super) fn builtin_search_role_for_folder_id(folder_id: u64) -> Option<&'static str> {
    match folder_id {
        CONTACTS_SEARCH_FOLDER_ID => Some("contacts_search"),
        TODO_SEARCH_FOLDER_ID => Some("todo_search"),
        REMINDERS_FOLDER_ID => Some("reminders"),
        TRACKED_MAIL_PROCESSING_FOLDER_ID => Some("tracked_mail_processing"),
        _ => None,
    }
}

pub(super) fn builtin_search_criteria_to_rop_for_folder_id(
    folder_id: u64,
) -> Option<(Vec<u8>, Vec<u64>, u32)> {
    builtin_search_scope_folder_ids(builtin_search_role_for_folder_id(folder_id)?).map(
        |folder_ids| {
            (
                Vec::new(),
                folder_ids,
                SEARCH_RUNNING_FLAG | SEARCH_RECURSIVE_FLAG,
            )
        },
    )
}

fn rop_restriction_from_json_clause(clause: &Value, use_unicode: bool) -> Result<Vec<u8>, u32> {
    let field = clause
        .get("field")
        .and_then(Value::as_str)
        .ok_or(EC_SEARCH_UNSUPPORTED)?;
    if let Some(value) = clause.get("contains").and_then(Value::as_str) {
        return Ok(rop_content_restriction(
            property_tag_for_search_field(field, use_unicode)?,
            value,
        ));
    }
    if let Some(value) = clause.get("equals") {
        return rop_property_restriction(field, 0x04, value, use_unicode);
    }
    for (key, fuzzy_level_low) in [("notEquals", 0x0000), ("notPrefix", 0x0002)] {
        if let Some(value) = clause.get(key).and_then(Value::as_str) {
            if field != "messageClass" {
                return Err(EC_SEARCH_UNSUPPORTED);
            }
            return Ok(rop_not_content_restriction(
                string_search_property_tag(PID_TAG_MESSAGE_CLASS_W, use_unicode),
                fuzzy_level_low,
                value,
            ));
        }
    }
    for (key, relop) in [("beforeOrAt", 0x01), ("afterOrAt", 0x03)] {
        if let Some(value) = clause.get(key) {
            return rop_property_restriction(field, relop, value, use_unicode);
        }
    }
    Err(EC_SEARCH_UNSUPPORTED)
}

fn rop_content_restriction(property_tag: u32, value: &str) -> Vec<u8> {
    let mut bytes = vec![0x03];
    bytes.extend_from_slice(&0x0001u16.to_le_bytes());
    bytes.extend_from_slice(&0x0001u16.to_le_bytes());
    bytes.extend_from_slice(&property_tag.to_le_bytes());
    bytes.extend_from_slice(&property_tag.to_le_bytes());
    write_mapi_value(
        &mut bytes,
        property_tag,
        &MapiValue::String(value.to_string()),
    );
    bytes
}

fn rop_not_content_restriction(property_tag: u32, fuzzy_level_low: u16, value: &str) -> Vec<u8> {
    let mut bytes = vec![0x02, 0x03];
    bytes.extend_from_slice(&fuzzy_level_low.to_le_bytes());
    bytes.extend_from_slice(&0x0001u16.to_le_bytes());
    bytes.extend_from_slice(&property_tag.to_le_bytes());
    bytes.extend_from_slice(&property_tag.to_le_bytes());
    write_mapi_value(
        &mut bytes,
        property_tag,
        &MapiValue::String(value.to_string()),
    );
    bytes
}

fn rop_property_restriction(
    field: &str,
    relop: u8,
    value: &Value,
    use_unicode: bool,
) -> Result<Vec<u8>, u32> {
    let property_tag = property_tag_for_search_field(field, use_unicode)?;
    let mapi_value = match field {
        "unread" => MapiValue::Bool(!value.as_bool().ok_or(EC_SEARCH_UNSUPPORTED)?),
        "flagged" => MapiValue::U32(if value.as_bool().ok_or(EC_SEARCH_UNSUPPORTED)? {
            FOLLOWUP_FLAGGED
        } else {
            0
        }),
        "hasAttachment" => MapiValue::Bool(value.as_bool().ok_or(EC_SEARCH_UNSUPPORTED)?),
        "category" => MapiValue::MultiString(vec![value
            .as_str()
            .ok_or(EC_SEARCH_UNSUPPORTED)?
            .to_string()]),
        "receivedAt" => {
            let value = value.as_str().ok_or(EC_SEARCH_UNSUPPORTED)?;
            MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(value))
        }
        "importance" => MapiValue::U32(
            value
                .as_u64()
                .and_then(|value| u32::try_from(value).ok())
                .ok_or(EC_SEARCH_UNSUPPORTED)?,
        ),
        _ => MapiValue::String(value.as_str().ok_or(EC_SEARCH_UNSUPPORTED)?.to_string()),
    };
    let mut bytes = vec![0x04, relop];
    bytes.extend_from_slice(&property_tag.to_le_bytes());
    bytes.extend_from_slice(&property_tag.to_le_bytes());
    write_mapi_value(&mut bytes, property_tag, &mapi_value);
    Ok(bytes)
}

fn property_tag_for_search_field(field: &str, use_unicode: bool) -> Result<u32, u32> {
    match field {
        "subject" => Ok(string_search_property_tag(PID_TAG_SUBJECT_W, use_unicode)),
        "body" => Ok(string_search_property_tag(PID_TAG_BODY_W, use_unicode)),
        "sender" => Ok(string_search_property_tag(
            PID_TAG_SENDER_EMAIL_ADDRESS_W,
            use_unicode,
        )),
        "category" => Ok(multiple_string_search_property_tag(
            PID_NAME_KEYWORDS_TAG,
            use_unicode,
        )),
        "unread" => Ok(PID_TAG_READ),
        "flagged" => Ok(PID_TAG_FLAG_STATUS),
        "hasAttachment" => Ok(PID_TAG_HAS_ATTACHMENTS),
        "receivedAt" => Ok(PID_TAG_MESSAGE_DELIVERY_TIME),
        "importance" => Ok(PID_TAG_IMPORTANCE),
        "messageClass" => Ok(string_search_property_tag(
            PID_TAG_MESSAGE_CLASS_W,
            use_unicode,
        )),
        _ => Err(EC_SEARCH_UNSUPPORTED),
    }
}

fn string_search_property_tag(property_tag: u32, use_unicode: bool) -> u32 {
    if use_unicode {
        property_tag
    } else {
        (property_tag & 0xFFFF_0000) | 0x001E
    }
}

fn multiple_string_search_property_tag(property_tag: u32, use_unicode: bool) -> u32 {
    if use_unicode {
        property_tag
    } else {
        (property_tag & 0xFFFF_0000) | 0x101E
    }
}

fn folder_id_for_role(role: &str) -> Option<u64> {
    match role {
        "inbox" => Some(INBOX_FOLDER_ID),
        "sent" => Some(SENT_FOLDER_ID),
        "trash" => Some(TRASH_FOLDER_ID),
        "drafts" => Some(DRAFTS_FOLDER_ID),
        "junk" => Some(JUNK_FOLDER_ID),
        "archive" => Some(ARCHIVE_FOLDER_ID),
        "outbox" => Some(OUTBOX_FOLDER_ID),
        _ => None,
    }
}
