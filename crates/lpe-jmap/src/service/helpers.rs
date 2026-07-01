use super::*;

pub(crate) fn api_request_exceeds_call_limit(request: &JmapApiRequest) -> bool {
    request.method_calls.len() > MAX_CALLS_IN_REQUEST as usize
}

pub(super) fn requested_account_id_from_arguments(
    arguments: &Value,
    account: &AuthenticatedAccount,
) -> Result<Uuid> {
    session::requested_account_id(arguments.get("accountId").and_then(Value::as_str), account)
}

pub(super) fn string_ids_from_arguments(arguments: &Value, field: &str) -> Option<Vec<String>> {
    arguments.get(field).and_then(Value::as_array).map(|ids| {
        ids.iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect()
    })
}

pub(super) fn property_names_from_arguments(arguments: &Value) -> Option<HashSet<String>> {
    arguments
        .get("properties")
        .and_then(Value::as_array)
        .map(|properties| {
            properties
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect::<HashSet<_>>()
        })
}

pub(super) fn project_get_properties(object: Value, properties: Option<&HashSet<String>>) -> Value {
    let Some(properties) = properties else {
        return object;
    };
    let Value::Object(map) = object else {
        return object;
    };
    let mut projected = Map::new();
    if let Some(id) = map.get("id") {
        projected.insert("id".to_string(), id.clone());
    }
    for property in properties {
        if property == "id" {
            continue;
        }
        if let Some(value) = map.get(property) {
            projected.insert(property.clone(), value.clone());
        }
    }
    Value::Object(projected)
}

pub(super) fn object_keys(arguments: &Value, field: &str) -> Vec<String> {
    arguments
        .get(field)
        .and_then(Value::as_object)
        .map(|objects| objects.keys().cloned().collect())
        .unwrap_or_default()
}

pub(super) fn canonical_create_ids(arguments: &Value) -> Vec<String> {
    let ids = object_keys(arguments, "create");
    if ids.is_empty() {
        object_keys(arguments, "emails")
    } else {
        ids
    }
}

pub(super) fn parse_reminder_id(id: &str) -> Result<(String, Uuid, Option<String>)> {
    let parts = id.splitn(3, ':').collect::<Vec<_>>();
    if parts.len() < 2 || parts[0].is_empty() || parts[1].is_empty() {
        bail!("reminder id must be sourceType:sourceId");
    }
    Ok((
        parts[0].to_string(),
        parse_uuid(parts[1])?,
        parts.get(2).map(|value| (*value).to_string()),
    ))
}

pub(super) fn parse_share_input(owner_account_id: Uuid, value: &Value) -> Result<JmapShareInput> {
    let share_type = value
        .get("type")
        .or_else(|| value.get("shareType"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("share type is required"))?;
    let rights = value.get("rights").and_then(Value::as_object);
    Ok(JmapShareInput {
        owner_account_id,
        share_type: share_type.to_string(),
        grantee_email: value
            .get("granteeEmail")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("granteeEmail is required"))?
            .to_string(),
        calendar_id: value
            .get("calendarId")
            .and_then(Value::as_str)
            .map(parse_uuid)
            .transpose()?,
        task_list_id: value
            .get("taskListId")
            .and_then(Value::as_str)
            .map(parse_uuid)
            .transpose()?,
        sender_right: value
            .get("senderRight")
            .and_then(Value::as_str)
            .map(str::to_string),
        may_read: rights
            .and_then(|rights| rights.get("mayRead"))
            .or_else(|| value.get("mayRead"))
            .and_then(Value::as_bool)
            .unwrap_or(true),
        may_write: rights
            .and_then(|rights| rights.get("mayWrite"))
            .or_else(|| value.get("mayWrite"))
            .and_then(Value::as_bool)
            .unwrap_or(false),
        may_delete: rights
            .and_then(|rights| rights.get("mayDelete"))
            .or_else(|| value.get("mayDelete"))
            .and_then(Value::as_bool)
            .unwrap_or(false),
        may_share: rights
            .and_then(|rights| rights.get("mayShare"))
            .or_else(|| value.get("mayShare"))
            .and_then(Value::as_bool)
            .unwrap_or(false),
    })
}

pub(super) fn share_audit(
    account: &AuthenticatedAccount,
    action: &str,
    subject: &str,
) -> AuditEntryInput {
    AuditEntryInput {
        actor: account.email.clone(),
        action: action.to_string(),
        subject: subject.to_string(),
    }
}

pub(super) fn rule_to_value(rule: MailboxRule) -> Value {
    json!({
        "id": rule.id.to_string(),
        "@type": "Rule",
        "name": rule.name,
        "isActive": rule.is_active,
        "sourceKind": rule.source_kind,
        "conditionSummary": rule.condition_summary,
        "actionSummary": rule.action_summary,
        "supportedOutlookProjection": rule.supported_outlook_projection,
        "unsupportedExchangeFeatures": rule.unsupported_exchange_features,
        "sizeOctets": rule.size_octets,
        "updatedAt": rule.updated_at,
    })
}

pub(super) fn outlook_profile_state_to_value(profile: OutlookProfileState) -> Value {
    json!({
        "id": profile.id,
        "@type": "OutlookProfile",
        "accountId": profile.account_id.to_string(),
        "messagesBackedByCanonicalMailbox": profile.messages_backed_by_canonical_mailbox,
        "contactsBackedByCanonicalStore": profile.contacts_backed_by_canonical_store,
        "calendarsBackedByCanonicalStore": profile.calendars_backed_by_canonical_store,
        "tasksBackedByCanonicalStore": profile.tasks_backed_by_canonical_store,
        "notesBackedByCanonicalStore": profile.notes_backed_by_canonical_store,
        "journalsBackedByCanonicalStore": profile.journals_backed_by_canonical_store,
        "searchFoldersCount": profile.search_folders_count,
        "rulesCount": profile.rules_count,
        "senderIdentitiesCount": profile.sender_identities_count,
        "mapiNamedPropertiesCount": profile.mapi_named_properties_count,
        "mapiCustomPropertiesCount": profile.mapi_custom_properties_count,
        "mapiNavigationShortcutsCount": profile.mapi_navigation_shortcuts_count,
        "mapiSyncCheckpointsCount": profile.mapi_sync_checkpoints_count,
        "mapiProfileSettingsPresent": profile.mapi_profile_settings_present,
        "ipmSubtreeOstIdPresent": profile.ipm_subtree_ost_id_present,
        "ipmSubtreeOstIdSizeOctets": profile.ipm_subtree_ost_id_size_octets,
        "profileSettingsUpdatedAt": profile.profile_settings_updated_at,
        "unsupportedClientLocalState": profile.unsupported_client_local_state,
    })
}

pub(super) fn canonical_query_state_method(data_type: &str) -> String {
    match data_type {
        "Reminder" => "Reminder".to_string(),
        _ => format!("{data_type}/query"),
    }
}

pub(super) fn canonical_query_filter(data_type: &str, arguments: &Value) -> Option<Value> {
    if data_type == "Reminder" {
        Some(json!({
            "includeInactive": arguments
                .get("includeInactive")
                .and_then(Value::as_bool)
                .unwrap_or(false)
        }))
    } else {
        None
    }
}

pub(super) fn search_folder_to_value(folder: SearchFolderDefinition) -> Value {
    json!({
        "id": folder.id.to_string(),
        "@type": "SearchFolder",
        "role": folder.role,
        "displayName": folder.display_name,
        "definitionKind": folder.definition_kind,
        "resultObjectKind": folder.result_object_kind,
        "scope": folder.scope_json,
        "restriction": folder.restriction_json,
        "excludedFolderRoles": folder.excluded_folder_roles,
        "isBuiltin": folder.is_builtin,
    })
}

pub(super) fn search_folder_input_from_value(
    id: Option<Uuid>,
    account_id: Uuid,
    value: &Value,
) -> Result<UpsertSearchFolderInput> {
    let display_name = value
        .get("displayName")
        .and_then(Value::as_str)
        .or_else(|| value.get("name").and_then(Value::as_str))
        .ok_or_else(|| anyhow!("displayName is required"))?
        .to_string();
    let result_object_kind = value
        .get("resultObjectKind")
        .and_then(Value::as_str)
        .unwrap_or("message")
        .to_string();
    let scope_json = value
        .get("scope")
        .cloned()
        .unwrap_or_else(|| json!({"scope": "top_of_personal_folders", "recursive": true}));
    let restriction_json = value
        .get("restriction")
        .cloned()
        .unwrap_or_else(|| json!({"kind": "user_saved"}));
    let excluded_folder_roles = value
        .get("excludedFolderRoles")
        .and_then(Value::as_array)
        .map(|roles| {
            roles
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    Ok(UpsertSearchFolderInput {
        id,
        account_id,
        display_name,
        result_object_kind,
        scope_json,
        restriction_json,
        excluded_folder_roles,
    })
}

pub(crate) fn validate_declared_capabilities(request: &JmapApiRequest) -> Result<()> {
    for capability in &request.using_capabilities {
        if !is_supported_capability(capability) {
            bail!("JMAP request declares unsupported capability: {capability}");
        }
    }
    Ok(())
}

pub(super) fn is_supported_capability(capability: &str) -> bool {
    matches!(
        capability,
        JMAP_CORE_CAPABILITY
            | JMAP_MAIL_CAPABILITY
            | JMAP_SUBMISSION_CAPABILITY
            | JMAP_BLOB_CAPABILITY
            | JMAP_CONTACTS_CAPABILITY
            | JMAP_CALENDARS_CAPABILITY
            | JMAP_TASKS_CAPABILITY
            | JMAP_LPE_OUTLOOK_CAPABILITY
            | JMAP_VACATION_RESPONSE_CAPABILITY
            | JMAP_WEBSOCKET_CAPABILITY
    )
}

pub(super) fn method_capability(method_name: &str) -> Option<&'static str> {
    match method_name {
        "Mailbox/get"
        | "Mailbox/query"
        | "Mailbox/queryChanges"
        | "Mailbox/changes"
        | "Mailbox/set"
        | "Mailbox/import"
        | "Mailbox/copy"
        | "Email/query"
        | "Email/queryChanges"
        | "Email/get"
        | "Email/changes"
        | "Email/set"
        | "Email/copy"
        | "Email/import"
        | "Thread/query"
        | "Thread/queryChanges"
        | "Thread/get"
        | "Thread/changes"
        | "Thread/set"
        | "Thread/import"
        | "Thread/copy"
        | "Quota/get"
        | "SearchSnippet/get" => Some(JMAP_MAIL_CAPABILITY),
        "EmailSubmission/get"
        | "EmailSubmission/changes"
        | "EmailSubmission/query"
        | "EmailSubmission/queryChanges"
        | "EmailSubmission/set"
        | "EmailSubmission/import"
        | "EmailSubmission/copy"
        | "Identity/get"
        | "Identity/query"
        | "Identity/queryChanges"
        | "Identity/changes"
        | "Identity/set"
        | "Identity/import"
        | "Identity/copy" => Some(JMAP_SUBMISSION_CAPABILITY),
        "AddressBook/get"
        | "AddressBook/query"
        | "AddressBook/queryChanges"
        | "AddressBook/changes"
        | "AddressBook/set"
        | "AddressBook/import"
        | "AddressBook/copy"
        | "ContactCard/get"
        | "ContactCard/query"
        | "ContactCard/queryChanges"
        | "ContactCard/changes"
        | "ContactCard/set"
        | "ContactCard/import"
        | "ContactCard/copy" => Some(JMAP_CONTACTS_CAPABILITY),
        "Calendar/get"
        | "Calendar/query"
        | "Calendar/queryChanges"
        | "Calendar/changes"
        | "Calendar/set"
        | "Calendar/import"
        | "Calendar/copy"
        | "CalendarEvent/get"
        | "CalendarEvent/query"
        | "CalendarEvent/queryChanges"
        | "CalendarEvent/changes"
        | "CalendarEvent/set"
        | "CalendarEvent/import"
        | "CalendarEvent/copy" => Some(JMAP_CALENDARS_CAPABILITY),
        "TaskList/get"
        | "TaskList/query"
        | "TaskList/queryChanges"
        | "TaskList/changes"
        | "TaskList/set"
        | "TaskList/import"
        | "TaskList/copy"
        | "Task/get"
        | "Task/query"
        | "Task/queryChanges"
        | "Task/changes"
        | "Task/set"
        | "Task/import"
        | "Task/copy" => Some(JMAP_TASKS_CAPABILITY),
        "Note/get"
        | "Note/query"
        | "Note/queryChanges"
        | "Note/changes"
        | "Note/set"
        | "Note/import"
        | "Note/copy"
        | "JournalEntry/get"
        | "JournalEntry/query"
        | "JournalEntry/queryChanges"
        | "JournalEntry/changes"
        | "JournalEntry/set"
        | "JournalEntry/import"
        | "JournalEntry/copy"
        | "Reminder/get"
        | "Reminder/query"
        | "Reminder/queryChanges"
        | "Reminder/changes"
        | "Reminder/set"
        | "Reminder/import"
        | "Reminder/copy"
        | "Rule/get"
        | "Rule/query"
        | "Rule/queryChanges"
        | "Rule/changes"
        | "Rule/set"
        | "Rule/import"
        | "Rule/copy"
        | "OutlookProfile/get"
        | "OutlookProfile/query"
        | "OutlookProfile/queryChanges"
        | "OutlookProfile/changes"
        | "OutlookProfile/set"
        | "OutlookProfile/import"
        | "OutlookProfile/copy"
        | "SearchFolder/get"
        | "SearchFolder/query"
        | "SearchFolder/queryChanges"
        | "SearchFolder/changes"
        | "SearchFolder/set"
        | "SearchFolder/import"
        | "SearchFolder/copy"
        | "Share/get"
        | "Share/query"
        | "Share/queryChanges"
        | "Share/changes"
        | "Share/set"
        | "Share/import"
        | "Share/copy"
        | "DurableChange/get"
        | "DurableChange/query"
        | "DurableChange/queryChanges"
        | "DurableChange/changes"
        | "DurableChange/set"
        | "DurableChange/import"
        | "DurableChange/copy"
        | "RecipientSuggestion/query" => Some(JMAP_LPE_OUTLOOK_CAPABILITY),
        "Blob/upload" | "Blob/get" | "Blob/query" | "Blob/queryChanges" | "Blob/changes"
        | "Blob/set" | "Blob/import" | "Blob/lookup" => Some(JMAP_BLOB_CAPABILITY),
        "Blob/copy" => Some(JMAP_CORE_CAPABILITY),
        "VacationResponse/get" | "VacationResponse/set" => Some(JMAP_VACATION_RESPONSE_CAPABILITY),
        _ => None,
    }
}

pub(super) fn is_method_error_payload(payload: &Value) -> bool {
    payload
        .as_object()
        .and_then(|object| object.get("type"))
        .and_then(Value::as_str)
        .is_some()
}

pub(super) fn resolve_result_references(
    arguments: Value,
    previous_results: &HashMap<String, (String, Value)>,
) -> std::result::Result<Value, Value> {
    let Value::Object(mut object) = arguments else {
        return Ok(arguments);
    };
    let references = object
        .iter()
        .filter_map(|(key, value)| {
            key.strip_prefix('#')
                .map(|property| (key.clone(), property.to_string(), value.clone()))
        })
        .collect::<Vec<_>>();

    for (reference_key, property, reference) in references {
        if object.contains_key(&property) {
            return Err(result_reference_error(&format!(
                "result reference {reference_key} conflicts with explicit {property}"
            )));
        }
        let reference = reference.as_object().ok_or_else(|| {
            result_reference_error(&format!(
                "result reference {reference_key} must be an object"
            ))
        })?;
        let result_of = reference
            .get("resultOf")
            .and_then(Value::as_str)
            .ok_or_else(|| result_reference_error("result reference is missing resultOf"))?;
        let expected_name = reference
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| result_reference_error("result reference is missing name"))?;
        let path = reference
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| result_reference_error("result reference is missing path"))?;
        let (actual_name, payload) = previous_results.get(result_of).ok_or_else(|| {
            result_reference_error(&format!(
                "result reference target {result_of} is not available"
            ))
        })?;
        if actual_name != expected_name {
            return Err(result_reference_error(&format!(
                "result reference target {result_of} is {actual_name}, not {expected_name}"
            )));
        }
        let resolved = payload.pointer(path).ok_or_else(|| {
            result_reference_error(&format!(
                "result reference path {path} is not available on {result_of}"
            ))
        })?;
        object.remove(&reference_key);
        object.insert(property, resolved.clone());
    }

    Ok(Value::Object(object))
}

pub(super) fn result_reference_error(description: &str) -> Value {
    method_error("resultReference", description)
}

pub(super) fn method_object_limit_error(method_name: &str, arguments: &Value) -> Option<Value> {
    let object_count = match method_name {
        "Mailbox/get"
        | "Email/get"
        | "EmailSubmission/get"
        | "Identity/get"
        | "Thread/get"
        | "Quota/get"
        | "AddressBook/get"
        | "ContactCard/get"
        | "Calendar/get"
        | "CalendarEvent/get"
        | "TaskList/get"
        | "Task/get"
        | "Note/get"
        | "JournalEntry/get"
        | "Reminder/get"
        | "Rule/get"
        | "OutlookProfile/get"
        | "SearchFolder/get"
        | "Share/get"
        | "DurableChange/get"
        | "Blob/get"
        | "VacationResponse/get" => object_array_len(arguments, "ids"),
        "SearchSnippet/get" => object_array_len(arguments, "emailIds"),
        "Blob/lookup" => object_array_len(arguments, "ids"),
        "Mailbox/set"
        | "Email/set"
        | "EmailSubmission/set"
        | "ContactCard/set"
        | "AddressBook/set"
        | "Calendar/set"
        | "CalendarEvent/set"
        | "TaskList/set"
        | "Task/set"
        | "Note/set"
        | "JournalEntry/set"
        | "Reminder/set"
        | "Rule/set"
        | "OutlookProfile/set"
        | "SearchFolder/set"
        | "Identity/set"
        | "Thread/set"
        | "Blob/set"
        | "Share/set"
        | "DurableChange/set"
        | "VacationResponse/set" => set_object_count(arguments),
        "Email/copy"
        | "Mailbox/copy"
        | "Thread/copy"
        | "EmailSubmission/copy"
        | "OutlookProfile/copy"
        | "AddressBook/copy"
        | "Calendar/copy"
        | "ContactCard/copy"
        | "CalendarEvent/copy"
        | "TaskList/copy"
        | "Task/copy"
        | "Note/copy"
        | "JournalEntry/copy"
        | "Reminder/copy"
        | "Rule/copy"
        | "SearchFolder/copy"
        | "Identity/copy"
        | "Share/copy"
        | "DurableChange/copy" => object_map_len(arguments, "create"),
        "Email/import"
        | "Mailbox/import"
        | "Thread/import"
        | "EmailSubmission/import"
        | "AddressBook/import"
        | "Calendar/import"
        | "ContactCard/import"
        | "CalendarEvent/import"
        | "TaskList/import"
        | "Task/import"
        | "Note/import"
        | "JournalEntry/import"
        | "Reminder/import"
        | "Rule/import"
        | "SearchFolder/import"
        | "Identity/import"
        | "Blob/import"
        | "Share/import"
        | "DurableChange/import" => {
            object_map_len(arguments, "emails").or_else(|| object_map_len(arguments, "create"))
        }
        "Blob/upload" => object_map_len(arguments, "create"),
        "Blob/copy" => object_array_len(arguments, "blobIds"),
        _ => None,
    };

    let limit = if method_name.ends_with("/get")
        || matches!(method_name, "SearchSnippet/get" | "Blob/lookup")
    {
        MAX_OBJECTS_IN_GET
    } else {
        MAX_OBJECTS_IN_SET
    };

    object_count
        .filter(|count| *count > limit as usize)
        .map(|count| {
            method_error(
                "tooManyObjects",
                &format!("{method_name} includes {count} objects; limit is {limit}"),
            )
        })
}

pub(super) fn object_array_len(arguments: &Value, field: &str) -> Option<usize> {
    arguments.get(field).and_then(Value::as_array).map(Vec::len)
}

pub(super) fn object_map_len(arguments: &Value, field: &str) -> Option<usize> {
    arguments
        .get(field)
        .and_then(Value::as_object)
        .map(serde_json::Map::len)
}

pub(super) fn set_object_count(arguments: &Value) -> Option<usize> {
    let count = object_map_len(arguments, "create").unwrap_or(0)
        + object_map_len(arguments, "update").unwrap_or(0)
        + object_array_len(arguments, "destroy").unwrap_or(0);
    (count > 0).then_some(count)
}

pub(super) fn authorization_header(headers: &HeaderMap) -> Option<String> {
    headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .map(ToString::to_string)
}

pub(super) fn bearer_token(authorization: Option<&str>) -> Option<&str> {
    authorization
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

pub(crate) fn collection_state_fingerprint(collection: &CollaborationCollection) -> String {
    opaque_state_fingerprint(&format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
        collection.kind,
        collection.owner_account_id,
        collection.owner_email,
        collection.owner_display_name,
        collection.display_name,
        collection.is_owned,
        collection.rights.may_read,
        collection.rights.may_write,
        collection.rights.may_delete,
        collection.rights.may_share
    ))
}

pub(super) fn email_submission_state_fingerprint(submission: &JmapEmailSubmission) -> String {
    opaque_state_fingerprint(&format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}",
        submission.email_id,
        submission.thread_id,
        submission.identity_id,
        submission.identity_email,
        submission.envelope_mail_from,
        submission.envelope_rcpt_to.join(","),
        submission.send_at,
        submission.undo_status,
        submission.delivery_status
    ))
}

pub(super) fn identity_state_fingerprint(identity: &SenderIdentity) -> String {
    opaque_state_fingerprint(&format!(
        "{}|{}|{}|{}|{}|{}",
        identity.owner_account_id,
        identity.email,
        identity.display_name,
        identity.authorization_kind,
        identity.sender_address.as_deref().unwrap_or_default(),
        identity.sender_display.as_deref().unwrap_or_default()
    ))
}

pub(super) fn mailbox_state_fingerprint(
    mailbox: &JmapMailbox,
    access: Option<&MailboxAccountAccess>,
) -> String {
    let is_drafts = mailbox.role == "drafts";
    let (may_read, may_write, may_draft, may_submit) = access
        .map(|access| {
            let may_write = crate::mailboxes::mailbox_account_may_write(access);
            let may_submit = crate::mailboxes::mailbox_account_may_submit(access);
            (
                access.may_read,
                may_write,
                is_drafts && may_write && may_submit,
                is_drafts && may_submit,
            )
        })
        .unwrap_or((true, true, is_drafts, false));
    opaque_state_fingerprint(&format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
        mailbox
            .parent_id
            .map(|id| id.to_string())
            .unwrap_or_default(),
        mailbox.role,
        mailbox.name,
        mailbox.sort_order,
        mailbox.total_emails,
        mailbox.unread_emails,
        mailbox.is_subscribed,
        may_read,
        may_draft,
        may_draft,
        may_write,
        may_write,
        may_submit,
    ))
}

pub(super) fn contact_state_fingerprint(contact: &AccessibleContact) -> String {
    opaque_state_fingerprint(&format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
        contact.collection_id,
        contact.owner_account_id,
        contact.owner_email,
        contact.owner_display_name,
        contact.name,
        contact.role,
        contact.email,
        contact.phone,
        contact.team,
        contact.notes,
        contact.rights.may_write,
        contact.rights.may_delete
    ))
}

pub(super) fn event_state_fingerprint(event: &AccessibleEvent) -> String {
    opaque_state_fingerprint(&format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
        event.collection_id,
        event.owner_account_id,
        event.owner_email,
        event.owner_display_name,
        event.date,
        event.time,
        event.time_zone,
        event.duration_minutes,
        event.recurrence_rule,
        event.title,
        event.location,
        event.attendees,
        event.attendees_json,
        event.notes,
        event.rights.may_write
    ))
}

pub(super) fn task_state_fingerprint(task: &ClientTask) -> String {
    opaque_state_fingerprint(&format!(
        "{}|{}|{}|{}|{}|{}|{}|{}",
        task.task_list_id,
        task.title,
        task.description,
        task.status,
        task.due_at.as_deref().unwrap_or_default(),
        task.completed_at.as_deref().unwrap_or_default(),
        task.sort_order,
        task.updated_at
    ))
}

pub(super) fn task_list_state_fingerprint(task_list: &ClientTaskList) -> String {
    opaque_state_fingerprint(&format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
        task_list.owner_account_id,
        task_list.owner_email,
        task_list.owner_display_name,
        task_list.is_owned,
        task_list.rights.may_read,
        task_list.rights.may_write,
        task_list.rights.may_delete,
        task_list.rights.may_share,
        task_list.name,
        task_list.role.clone().unwrap_or_default(),
        task_list.sort_order,
        task_list.updated_at
    ))
}

pub(super) fn email_state_fingerprint(email: &JmapEmail, include_bcc: bool) -> String {
    opaque_state_fingerprint(
        &(format!(
            "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
            email.thread_id,
            format_mailbox_ids(&email.mailbox_ids),
            format_mailbox_states(&email.mailbox_states),
            email.received_at,
            email.sent_at.as_deref().unwrap_or_default(),
            email.from_display.as_deref().unwrap_or_default(),
            email.from_address,
            format_addresses(&email.to),
            format_addresses(&email.cc),
            include_bcc
                .then(|| format_addresses(&email.bcc))
                .unwrap_or_default(),
            email.subject,
            email.preview,
            email.unread,
            email.flagged,
            email.delivery_status,
        ) + &format!(
            "|{}|{}|{}|{}|{}",
            email.body_text,
            email.body_html_sanitized.as_deref().unwrap_or_default(),
            email.has_attachments,
            email.size_octets,
            email.internet_message_id.as_deref().unwrap_or_default(),
        )),
    )
}

pub(super) fn format_mailbox_ids(mailbox_ids: &[Uuid]) -> String {
    let mut values = mailbox_ids.iter().map(Uuid::to_string).collect::<Vec<_>>();
    values.sort();
    values.join(",")
}

pub(super) fn format_mailbox_states(states: &[lpe_storage::JmapEmailMailboxState]) -> String {
    let mut values = states
        .iter()
        .map(|state| {
            format!(
                "{}:{}:{}:{}:{}:{}",
                state.mailbox_id, state.role, state.name, state.unread, state.flagged, state.draft
            )
        })
        .collect::<Vec<_>>();
    values.sort();
    values.join("|")
}

pub(crate) fn opaque_state_fingerprint(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
}

pub(crate) fn trim_snippet(value: &str, max_chars: usize) -> String {
    let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.chars().count() <= max_chars {
        normalized
    } else {
        normalized.chars().take(max_chars).collect::<String>()
    }
}
