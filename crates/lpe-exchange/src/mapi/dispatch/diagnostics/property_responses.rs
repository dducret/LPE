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
    if is_default_folder_identification_property_tag(storage_tag) {
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

fn get_properties_view_response_values_for_debug(property_tags: &[u32], response: &[u8]) -> String {
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
