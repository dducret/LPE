use super::super::*;

pub(in crate::mapi::dispatch) fn should_log_outlook_surface_getprops_info(
    object: Option<&MapiObject>,
) -> bool {
    if matches!(
        object,
        Some(MapiObject::Logon | MapiObject::PublicFolderLogon)
    ) {
        return true;
    }
    matches!(
        object.and_then(MapiObject::folder_id),
        Some(
            INBOX_FOLDER_ID
                | DEFERRED_ACTION_FOLDER_ID
                | CONTACTS_FOLDER_ID
                | CALENDAR_FOLDER_ID
                | JOURNAL_FOLDER_ID
                | NOTES_FOLDER_ID
                | TASKS_FOLDER_ID
        )
    )
}

pub(in crate::mapi::dispatch) fn format_outlook_surface_folder_getprops_trace(
    request_id: &str,
    request: &RopRequest,
    object: Option<&MapiObject>,
    property_response: &[u8],
) -> Option<String> {
    let Some(MapiObject::Folder { folder_id, .. }) = object else {
        return None;
    };
    if !should_log_outlook_surface_getprops_info(object) {
        return None;
    }
    let property_tags = request.property_tags();
    let response = getprops_contract_response_summary(&property_tags, property_response);
    Some(format!(
        "getprops_folder:request_id={request_id};handle={};folder=0x{folder_id:016x};role={};tags={};names={};returned={};problems={};zero_defaults={};values={};response={}",
        request.input_handle_index().unwrap_or(0),
        debug_role_for_folder_id(*folder_id),
        format_debug_property_tags(&property_tags),
        format_set_property_names_for_debug(&property_tags),
        response.returned_tags,
        response.problem_tags,
        response.zero_default_tags,
        truncate_debug_field(&response.value_shapes, 512),
        response.result
    ))
}

fn truncate_debug_field(value: &str, limit: usize) -> String {
    if value.len() <= limit {
        value.to_string()
    } else {
        format!("{}...", &value[..limit])
    }
}

pub(in crate::mapi::dispatch) fn log_set_properties_specific_debug(
    principal: &AccountPrincipal,
    request_id: &str,
    request: &RopRequest,
    object: Option<&MapiObject>,
    probe: &SetPropertiesProbeRequest,
) {
    let default_folder_identification_values_stripped =
        default_folder_identification_values_stripped_by_safe_values(object, &probe.property_tags);
    let default_folder_entry_id_storage_mode = if default_folder_identification_values_stripped {
        "accepted_canonical_projection_stripped"
    } else if probe.default_folder_entry_id_values.is_empty() {
        "not_default_folder_entry_ids"
    } else if matches!(
        object,
        Some(MapiObject::Folder {
            folder_id: ROOT_FOLDER_ID | INBOX_FOLDER_ID,
            ..
        })
    ) {
        "accepted_canonical_projection_not_persisted"
    } else {
        "normal_property_validation"
    };
    let folder_profile_property_storage = folder_profile_property_storage_mode_for_debug(
        object,
        &probe.property_tags,
        &probe.property_value_shapes,
    );
    tracing::debug!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        mapi_request_id = request_id,
        request_rop_id = %rop_id_hex(request.rop_id),
        input_handle_index = request.input_handle_index().unwrap_or(0),
        response_handle_index = request.response_handle_index(),
        object_kind = mapi_object_debug_kind(object),
        folder_id = %mapi_object_debug_folder_id(object),
        property_tag_count = probe.property_tags.len(),
        property_tags = %format_debug_property_tags(&probe.property_tags),
        property_names = %format_set_property_names_for_debug(&probe.property_tags),
        property_value_shapes = %probe.property_value_shapes,
        associated_config_stream_summary = %probe.associated_config_stream_summary,
        default_folder_entry_id_values = %probe.default_folder_entry_id_values,
        default_folder_identification_values_stripped = default_folder_identification_values_stripped,
        default_folder_entry_id_storage_mode = default_folder_entry_id_storage_mode,
        folder_profile_property_storage = %folder_profile_property_storage,
        parse_error = %probe.parse_error,
        "rca debug mapi set properties specific"
    );
}

fn folder_profile_property_storage_mode_for_debug(
    object: Option<&MapiObject>,
    property_tags: &[u32],
    property_value_shapes: &str,
) -> String {
    let Some(MapiObject::Folder { folder_id, .. }) = object else {
        return String::new();
    };
    let supported = property_tags
        .iter()
        .copied()
        .map(canonical_property_storage_tag)
        .filter(|tag| *tag == PID_TAG_EXTENDED_FOLDER_FLAGS)
        .map(|tag| format!("{tag:#010x}:durable_folder_profile_property"))
        .collect::<Vec<_>>()
        .join(",");
    if supported.is_empty() {
        return String::new();
    }
    format!("folder=0x{folder_id:016x};{supported};values={property_value_shapes}")
}

pub(in crate::mapi::dispatch) fn log_get_properties_default_folder_response_debug(
    principal: &AccountPrincipal,
    request_id: &str,
    request: &RopRequest,
    object: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    property_response: &[u8],
) {
    let property_tags = request.property_tags();
    if !property_tags
        .iter()
        .copied()
        .any(is_default_folder_identification_property_tag)
    {
        return;
    }
    let probe = GetPropertiesSpecificProbeRequest {
        input_handle_index: request.input_handle_index().unwrap_or(0),
        property_tags,
    };
    let response_shape = summarize_get_properties_probe_response(property_response, 0, &probe);
    let decoded_values =
        default_folder_getprops_response_values_for_debug(&probe.property_tags, property_response);
    let default_folder_projection =
        default_folder_hierarchy_projection_for_debug(principal, mailboxes, emails, snapshot);
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        mapi_request_id = request_id,
        request_rop_id = "0x07",
        input_handle_index = request.input_handle_index().unwrap_or(0),
        response_handle_index = request.response_handle_index(),
        object_kind = mapi_object_debug_kind(object),
        folder_id = %mapi_object_debug_folder_id(object),
        property_tag_count = probe.property_tags.len(),
        property_tags = %format_debug_property_tags(&probe.property_tags),
        property_names = %format_set_property_names_for_debug(&probe.property_tags),
        response_shape = %response_shape,
        default_folder_entry_id_values = %decoded_values,
        default_folder_hierarchy_projection = %default_folder_projection,
        "rca debug mapi default folder getprops response"
    );
}

pub(in crate::mapi::dispatch) fn log_get_properties_specific_response_debug(
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    request_id: &str,
    request: &RopRequest,
    object: Option<&MapiObject>,
    property_response: &[u8],
) {
    let property_tags = request.property_tags();
    let probe = GetPropertiesSpecificProbeRequest {
        input_handle_index: request.input_handle_index().unwrap_or(0),
        property_tags,
    };
    let response_shape = summarize_get_properties_probe_response(property_response, 0, &probe);
    let response_values =
        get_properties_specific_response_values_for_debug(&probe.property_tags, property_response);
    let associated_config_debug = associated_config_debug_identity(object);
    record_outlook_umolk_getprops_materialization(
        principal,
        session,
        request_id,
        request,
        object,
        associated_config_debug.as_ref(),
        &probe.property_tags,
        property_response,
        &response_shape,
    );
    let contacts_associated_named_probe = matches!(
        object,
        Some(MapiObject::AssociatedConfig { folder_id, .. })
            if mapi_folder_is_outlook_contacts_surface(*folder_id)
    ) && probe
        .property_tags
        .iter()
        .any(|tag| MapiPropertyTag::new(*tag).property_id() >= FIRST_NAMED_PROPERTY_ID);
    if contacts_associated_named_probe {
        let (config_id, message_class, subject) = match object {
            Some(MapiObject::AssociatedConfig {
                config_id,
                saved_message,
                ..
            }) => saved_message
                .as_ref()
                .map(|message| {
                    (
                        format!("0x{:016x}", message.id),
                        message.message_class.as_str(),
                        message.subject.as_str(),
                    )
                })
                .unwrap_or_else(|| (format!("0x{config_id:016x}"), "missing", "missing")),
            _ => ("none".to_string(), "none", "none"),
        };
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            mailbox = %principal.email,
            request_type = "Execute",
            mapi_request_id = request_id,
            request_rop_id = "0x07",
            input_handle_index = request.input_handle_index().unwrap_or(0),
            response_handle_index = request.response_handle_index(),
            object_kind = mapi_object_debug_kind(object),
            folder_id = %mapi_object_debug_folder_id(object),
            associated_config_id = %config_id,
            associated_config_class = message_class,
            associated_config_subject = subject,
            property_tag_count = probe.property_tags.len(),
            property_tags = %format_debug_property_tags(&probe.property_tags),
            property_names = %format_set_property_names_for_debug(&probe.property_tags),
            response_shape = %response_shape,
            response_values = %response_values,
            "rca debug mapi contacts associated getprops response"
        );
    } else {
        tracing::debug!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            mailbox = %principal.email,
            request_type = "Execute",
            mapi_request_id = request_id,
            request_rop_id = "0x07",
            input_handle_index = request.input_handle_index().unwrap_or(0),
            response_handle_index = request.response_handle_index(),
            object_kind = mapi_object_debug_kind(object),
            folder_id = %mapi_object_debug_folder_id(object),
            property_tag_count = probe.property_tags.len(),
            property_tags = %format_debug_property_tags(&probe.property_tags),
            property_names = %format_set_property_names_for_debug(&probe.property_tags),
            response_shape = %response_shape,
            response_values = %response_values,
            "rca debug mapi getprops specific response"
        );
    }
}

fn associated_config_debug_identity(
    object: Option<&MapiObject>,
) -> Option<(String, String, String)> {
    let Some(MapiObject::AssociatedConfig {
        config_id,
        saved_message,
        ..
    }) = object
    else {
        return None;
    };
    Some(
        saved_message
            .as_ref()
            .map(|message| {
                (
                    format!("0x{:016x}", message.id),
                    message.message_class.clone(),
                    message.subject.clone(),
                )
            })
            .unwrap_or_else(|| {
                (
                    format!("0x{config_id:016x}"),
                    "missing".into(),
                    "missing".into(),
                )
            }),
    )
}

fn record_outlook_umolk_getprops_materialization(
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    request_id: &str,
    request: &RopRequest,
    object: Option<&MapiObject>,
    associated_config_debug: Option<&(String, String, String)>,
    property_tags: &[u32],
    property_response: &[u8],
    response_shape: &str,
) {
    let Some(MapiObject::AssociatedConfig {
        folder_id: INBOX_FOLDER_ID,
        ..
    }) = object
    else {
        return;
    };
    let Some((config_id, message_class, subject)) = associated_config_debug else {
        return;
    };
    if !crate::mapi_store::is_outlook_umolk_user_options_message_class(&message_class) {
        return;
    }
    if session
        .post_hierarchy_actions
        .last_outlook_umolk_named_property_probe_context
        .is_empty()
        && !property_tags
            .iter()
            .any(|tag| MapiPropertyTag::new(*tag).property_id() >= FIRST_NAMED_PROPERTY_ID)
    {
        return;
    }
    let materialization =
        summarize_flagged_getprops_materialization(property_tags, property_response);
    let dictionary_shape = classify_umolk_dictionary_shape(response_shape);
    let dictionary_contract =
        summarize_umolk_roaming_dictionary_contract(property_tags, property_response);
    session
        .post_hierarchy_actions
        .outlook_umolk_getprops_not_found_count = materialization.not_found_count;
    session
        .post_hierarchy_actions
        .last_outlook_umolk_getprops_materialization_context = format!(
        "request_id={request_id};handle={};config={config_id};class={message_class};subject={subject};property_tag_count={};returned_value_count={};problem_count={};not_found_count={};first_problem_tags={};dictionary_shape={dictionary_shape};dictionary_olprefs_version={};dictionary_olprefs_value={};dictionary_info_version={};response_shape={};response_bytes={}",
        request.input_handle_index().unwrap_or(0),
        property_tags.len(),
        materialization.returned_value_count,
        materialization.problem_count,
        materialization.not_found_count,
        materialization.first_problem_tags,
        dictionary_contract.olprefs_version,
        dictionary_contract.olprefs_value,
        dictionary_contract.info_version,
        truncate_debug_field(response_shape, 1024),
        property_response.len(),
    );
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        mapi_request_id = request_id,
        request_rop_id = "0x07",
        input_handle_index = request.input_handle_index().unwrap_or(0),
        response_handle_index = request.response_handle_index(),
        object_kind = mapi_object_debug_kind(object),
        associated_config_id = config_id,
        associated_config_class = message_class,
        associated_config_subject = subject,
        umolk_named_property_probe_context = %debug_context_or_none(
            &session
                .post_hierarchy_actions
                .last_outlook_umolk_named_property_probe_context
        ),
        umolk_getprops_materialization_context = %session
            .post_hierarchy_actions
            .last_outlook_umolk_getprops_materialization_context,
        "rca debug mapi umolk getprops materialization"
    );
    session.record_outlook_view_failure_trace_event(format!(
        "umolk_getprops_materialization:{}",
        session
            .post_hierarchy_actions
            .last_outlook_umolk_getprops_materialization_context
    ));
}

fn classify_umolk_dictionary_shape(response_shape: &str) -> &'static str {
    if !response_shape.contains("0x7c070102:binary") {
        return "dictionary_not_returned";
    }
    if response_shape.contains("0x7c070102:binary:bytes=0") {
        return "empty_dictionary";
    }
    if response_shape.contains("preview=3c786d6c2f3e") {
        return "stale_xml_placeholder";
    }
    if response_shape.contains("preview=3c3f786d6c")
        && response_shape.contains("55736572436f6e66696775726174696f6e")
    {
        return "xml_user_configuration_dictionary";
    }
    if response_shape.contains("preview=3c3f786d6c") {
        return "xml_dictionary";
    }
    "unknown_binary_dictionary"
}

#[derive(Debug, PartialEq, Eq)]
struct UmolkRoamingDictionaryContractSummary {
    olprefs_version: &'static str,
    olprefs_value: String,
    info_version: String,
}

impl Default for UmolkRoamingDictionaryContractSummary {
    fn default() -> Self {
        Self {
            olprefs_version: "not_returned",
            olprefs_value: "none".to_string(),
            info_version: "none".to_string(),
        }
    }
}

fn summarize_umolk_roaming_dictionary_contract(
    property_tags: &[u32],
    response: &[u8],
) -> UmolkRoamingDictionaryContractSummary {
    let Some(dictionary) =
        extract_getprops_binary_value(property_tags, response, PID_TAG_ROAMING_DICTIONARY)
    else {
        return UmolkRoamingDictionaryContractSummary::default();
    };
    summarize_umolk_roaming_dictionary_xml(&dictionary)
}

fn extract_getprops_binary_value(
    property_tags: &[u32],
    response: &[u8],
    target_tag: u32,
) -> Option<Vec<u8>> {
    let row_kind = response.get(6).copied()?;
    let mut cursor = Cursor::new(response.get(7..).unwrap_or_default());
    for tag in property_tags {
        let storage_tag = canonical_property_storage_tag(*tag);
        if row_kind == 1 {
            match cursor.read_u8().ok()? {
                0 => {}
                0x0A => {
                    let _ = cursor.read_u32().ok()?;
                    continue;
                }
                _ => return None,
            }
        } else if row_kind != 0 {
            return None;
        }
        let value = parse_property_value_for_tag(&mut cursor, *tag).ok()?;
        if storage_tag == target_tag {
            if let MapiValue::Binary(bytes) = value {
                return Some(bytes);
            }
            return None;
        }
    }
    None
}

fn summarize_umolk_roaming_dictionary_xml(bytes: &[u8]) -> UmolkRoamingDictionaryContractSummary {
    let text = String::from_utf8_lossy(bytes);
    let info_version = xml_attr_value(text.as_ref(), "Info", "version")
        .as_deref()
        .map(sanitize_debug_token)
        .unwrap_or_else(|| "missing".to_string());
    let Some(raw_value) = xml_element_attr_by_key(text.as_ref(), "e", "18-OLPrefsVersion", "v")
    else {
        return UmolkRoamingDictionaryContractSummary {
            olprefs_version: "missing",
            olprefs_value: "missing".to_string(),
            info_version,
        };
    };
    let olprefs_value = sanitize_debug_token(&raw_value);
    let olprefs_version = classify_olprefs_version_value(&raw_value);
    UmolkRoamingDictionaryContractSummary {
        olprefs_version,
        olprefs_value,
        info_version,
    }
}

fn classify_olprefs_version_value(raw_value: &str) -> &'static str {
    let version_part = raw_value
        .rsplit_once('-')
        .map_or(raw_value, |(_, value)| value);
    match version_part.trim().parse::<i64>() {
        Ok(value) if value > 0 => "positive",
        Ok(_) => "zero_or_negative",
        Err(_) => "invalid",
    }
}

fn xml_element_attr_by_key(text: &str, element: &str, key: &str, attr: &str) -> Option<String> {
    let key_attr = format!(r#"k="{key}""#);
    let mut remaining = text;
    while let Some(start) = remaining.find(&format!("<{element} ")) {
        remaining = &remaining[start..];
        let Some(end) = remaining.find('>') else {
            return None;
        };
        let tag = &remaining[..=end];
        if tag.contains(&key_attr) {
            return xml_attr_value(tag, element, attr);
        }
        remaining = &remaining[end + 1..];
    }
    None
}

fn xml_attr_value(text: &str, element: &str, attr: &str) -> Option<String> {
    let element_start = format!("<{element}");
    let attr_start = format!(r#"{attr}=""#);
    let start = text.find(&element_start)?;
    let tag = &text[start..];
    let end = tag.find('>')?;
    let tag = &tag[..end];
    let attr_start_index = tag.find(&attr_start)? + attr_start.len();
    let attr_tail = &tag[attr_start_index..];
    let attr_end = attr_tail.find('"')?;
    Some(attr_tail[..attr_end].to_string())
}

fn sanitize_debug_token(value: &str) -> String {
    let sanitized: String = value
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-'))
        .take(64)
        .collect();
    if sanitized.is_empty() {
        "invalid".to_string()
    } else {
        sanitized
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct GetPropsMaterializationSummary {
    returned_value_count: usize,
    problem_count: usize,
    not_found_count: usize,
    first_problem_tags: String,
}

fn summarize_flagged_getprops_materialization(
    property_tags: &[u32],
    response: &[u8],
) -> GetPropsMaterializationSummary {
    let mut summary = GetPropsMaterializationSummary::default();
    let Some(row_kind) = response.get(6).copied() else {
        summary.first_problem_tags = "truncated".to_string();
        return summary;
    };
    if row_kind == 0 {
        summary.returned_value_count = property_tags.len();
        return summary;
    }
    if row_kind != 1 {
        summary.first_problem_tags = format!("unsupported_row_kind={row_kind}");
        return summary;
    }
    let mut cursor = Cursor::new(response.get(7..).unwrap_or_default());
    let mut first_problem_tags = Vec::new();
    for tag in property_tags {
        let Ok(flag) = cursor.read_u8() else {
            first_problem_tags.push(format!("{tag:#010x}:truncated"));
            break;
        };
        match flag {
            0 => match parse_property_value_for_tag(&mut cursor, *tag) {
                Ok(_) => summary.returned_value_count += 1,
                Err(error) => {
                    summary.problem_count += 1;
                    if first_problem_tags.len() < 12 {
                        first_problem_tags.push(format!("{tag:#010x}:parse_error={error}"));
                    }
                    break;
                }
            },
            0x0A => {
                let error = cursor.read_u32().unwrap_or(0);
                summary.problem_count += 1;
                if error == 0x8004_010F {
                    summary.not_found_count += 1;
                }
                if first_problem_tags.len() < 12 {
                    first_problem_tags.push(format!("{tag:#010x}:{error:#010x}"));
                }
            }
            other => {
                summary.problem_count += 1;
                if first_problem_tags.len() < 12 {
                    first_problem_tags.push(format!("{tag:#010x}:flag={other:#04x}"));
                }
                break;
            }
        }
    }
    summary.first_problem_tags = first_problem_tags.join(",");
    summary
}

pub(in crate::mapi::dispatch) fn log_get_properties_view_response_debug(
    principal: &AccountPrincipal,
    request_id: &str,
    request: &RopRequest,
    object: Option<&MapiObject>,
    property_response: &[u8],
) {
    let property_tags = request.property_tags();
    if !property_tags
        .iter()
        .copied()
        .any(is_outlook_view_property_tag_for_debug)
    {
        return;
    }
    let response_values =
        get_properties_view_response_values_for_debug(&property_tags, property_response);
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        mapi_request_id = request_id,
        request_rop_id = "0x07",
        input_handle_index = request.input_handle_index().unwrap_or(0),
        response_handle_index = request.response_handle_index(),
        object_kind = mapi_object_debug_kind(object),
        folder_id = %mapi_object_debug_folder_id(object),
        property_tag_count = property_tags.len(),
        property_tags = %format_debug_property_tags(&property_tags),
        property_names = %format_set_property_names_for_debug(&property_tags),
        view_response_values = %response_values,
        "rca debug mapi outlook view getprops response"
    );
}

pub(in crate::mapi::dispatch) fn associated_config_stream_write_summary(
    values: &[(u32, MapiValue)],
) -> String {
    let mut parts = Vec::new();
    for (tag, value) in values {
        match canonical_property_storage_tag(*tag) {
            PID_TAG_ROAMING_DATATYPES
            | PID_TAG_ROAMING_DICTIONARY
            | PID_TAG_ROAMING_XML_STREAM
            | 0x7C09_0102
            | 0x685D_0003 => parts.push(format!(
                "{}={}",
                set_property_debug_name(*tag),
                mapi_value_debug_shape(value)
            )),
            _ => {}
        }
    }
    parts.join(",")
}

pub(in crate::mapi::dispatch) fn get_properties_specific_response_values_for_debug(
    property_tags: &[u32],
    response: &[u8],
) -> String {
    if response.get(6).copied() != Some(0) {
        return "not-standard-row".to_string();
    }
    let mut cursor = Cursor::new(response.get(7..).unwrap_or_default());
    let mut values = Vec::new();
    for tag in property_tags {
        let storage_tag = canonical_property_storage_tag(*tag);
        match parse_property_value_for_tag(&mut cursor, *tag) {
            Ok(value) => values.push(get_properties_specific_value_for_debug(storage_tag, &value)),
            Err(error) => {
                values.push(format!(
                    "{storage_tag:#010x}:{}:parse_error={error}",
                    set_property_debug_name(storage_tag)
                ));
                break;
            }
        }
    }
    values.join(",")
}

fn get_properties_specific_value_for_debug(tag: u32, value: &MapiValue) -> String {
    let storage_tag = canonical_property_storage_tag(tag);
    if is_default_folder_identification_property_tag(storage_tag)
        || storage_tag == PID_TAG_DEFAULT_VIEW_ENTRY_ID
    {
        return default_folder_getprops_value_for_debug(storage_tag, value);
    }
    let decoded = match value {
        MapiValue::Binary(bytes) => {
            let decoded_folder_id =
                crate::mapi::identity::object_id_from_folder_identifier_bytes(bytes);
            match decoded_folder_id {
                Some(folder_id) => format!(
                    ";decoded_folder_id=0x{folder_id:016x};decoded_name={}",
                    post_hierarchy_probe_folder_name(folder_id)
                ),
                None => format!(";preview={}", hex_preview(bytes, 32)),
            }
        }
        MapiValue::Guid(bytes) => format!(";guid={}", bytes_to_hex(bytes)),
        _ => String::new(),
    };
    format!(
        "{storage_tag:#010x}:{}:{}{}",
        set_property_debug_name(storage_tag),
        mapi_value_debug_shape(value),
        decoded
    )
}

fn is_outlook_view_property_tag_for_debug(tag: u32) -> bool {
    matches!(
        canonical_property_storage_tag(tag),
        PID_TAG_DEFAULT_VIEW_ENTRY_ID
            | PID_TAG_FOLDER_FORM_FLAGS
            | PID_TAG_FOLDER_WEBVIEWINFO
            | PID_TAG_FOLDER_XVIEWINFO_E
            | PID_TAG_FOLDER_VIEWS_ONLY
            | PID_TAG_FOLDER_VIEWLIST_FLAGS
            | PID_TAG_VIEWS_ENTRY_ID
            | PID_TAG_COMMON_VIEWS_ENTRY_ID
    )
}

pub(in crate::mapi::dispatch) fn get_properties_view_response_values_for_debug(
    property_tags: &[u32],
    response: &[u8],
) -> String {
    if response.get(6).copied() != Some(0) {
        return "not-standard-row".to_string();
    }
    let mut cursor = Cursor::new(response.get(7..).unwrap_or_default());
    let mut values = Vec::new();
    for tag in property_tags {
        let storage_tag = canonical_property_storage_tag(*tag);
        match parse_property_value_for_tag(&mut cursor, *tag) {
            Ok(value) if is_outlook_view_property_tag_for_debug(storage_tag) => {
                values.push(get_properties_specific_value_for_debug(storage_tag, &value));
            }
            Ok(_) => {}
            Err(error) => {
                values.push(format!(
                    "{storage_tag:#010x}:{}:parse_error={error}",
                    set_property_debug_name(storage_tag)
                ));
                break;
            }
        }
    }
    values.join(",")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn umolk_dictionary_shape_classifies_current_modeled_xml_dictionary() {
        let shape = concat!(
            "values=0x7c070102:binary:bytes=170:",
            "preview=3c3f786d6c2076657273696f6e3d22312e3022;",
            "tail=55736572436f6e66696775726174696f6e"
        );

        assert_eq!(
            classify_umolk_dictionary_shape(shape),
            "xml_user_configuration_dictionary"
        );
    }

    #[test]
    fn umolk_dictionary_shape_classifies_stale_placeholder() {
        let shape = "values=0x7c070102:binary:bytes=6:preview=3c786d6c2f3e";

        assert_eq!(
            classify_umolk_dictionary_shape(shape),
            "stale_xml_placeholder"
        );
    }

    #[test]
    fn umolk_dictionary_contract_classifies_positive_olprefs_version() {
        let summary = summarize_umolk_roaming_dictionary_xml(
            br#"<?xml version="1.0"?><UserConfiguration><Info version="Outlook.16"/><Data><e k="18-OLPrefsVersion" v="9-1"/></Data></UserConfiguration>"#,
        );

        assert_eq!(
            summary,
            UmolkRoamingDictionaryContractSummary {
                olprefs_version: "positive",
                olprefs_value: "9-1".to_string(),
                info_version: "Outlook.16".to_string(),
            }
        );
    }

    #[test]
    fn umolk_dictionary_contract_classifies_zero_olprefs_version() {
        let summary = summarize_umolk_roaming_dictionary_xml(
            br#"<?xml version="1.0"?><UserConfiguration><Info version="LPE.1"/><Data><e k="18-OLPrefsVersion" v="9-0"/></Data></UserConfiguration>"#,
        );

        assert_eq!(summary.olprefs_version, "zero_or_negative");
        assert_eq!(summary.olprefs_value, "9-0");
        assert_eq!(summary.info_version, "LPE.1");
    }

    #[test]
    fn umolk_dictionary_contract_reports_missing_olprefs_version() {
        let summary = summarize_umolk_roaming_dictionary_xml(
            br#"<?xml version="1.0"?><UserConfiguration><Info version="Outlook.16"/></UserConfiguration>"#,
        );

        assert_eq!(summary.olprefs_version, "missing");
        assert_eq!(summary.olprefs_value, "missing");
        assert_eq!(summary.info_version, "Outlook.16");
    }

    #[test]
    fn umolk_dictionary_contract_reports_invalid_olprefs_version() {
        let summary = summarize_umolk_roaming_dictionary_xml(
            br#"<?xml version="1.0"?><UserConfiguration><Info version="Outlook.16"/><Data><e k="18-OLPrefsVersion" v="bad"/></Data></UserConfiguration>"#,
        );

        assert_eq!(summary.olprefs_version, "invalid");
        assert_eq!(summary.olprefs_value, "bad");
        assert_eq!(summary.info_version, "Outlook.16");
    }
}
