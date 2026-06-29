use super::super::*;

pub(in crate::mapi::dispatch) fn summarize_first_post_hierarchy_probe(
    request_rop_buffer: &[u8],
    response_rop_buffer: &[u8],
) -> FirstPostHierarchyProbeDebugSummary {
    let mut summary = FirstPostHierarchyProbeDebugSummary::default();
    let Some((requests, _request_handle_table)) = split_rop_buffer(request_rop_buffer) else {
        summary.parse_error = "invalid request ROP buffer".to_string();
        return summary;
    };
    let mut request_cursor = Cursor::new(requests);
    let mut request_rop_ids = Vec::new();
    let mut open_folder_requests = Vec::new();
    let mut get_properties_requests = Vec::new();
    let mut set_properties_requests = Vec::new();
    while request_cursor.remaining() > 0 {
        let request = match read_rop_request(&mut request_cursor) {
            Ok(request) => request,
            Err(error) => {
                summary.parse_error = error.to_string();
                break;
            }
        };
        let rop_id = request.typed().rop_id();
        request_rop_ids.push(rop_id);
        match rop_id {
            0x02 => open_folder_requests.push(OpenFolderProbeRequest {
                output_handle_index: request.output_handle_index.unwrap_or(0),
                folder_id: request.folder_id().unwrap_or(ROOT_FOLDER_ID),
            }),
            0x07 => get_properties_requests.push(GetPropertiesSpecificProbeRequest {
                input_handle_index: request.input_handle_index().unwrap_or(0),
                property_tags: request.property_tags(),
            }),
            0x0A | 0x79 => set_properties_requests.push(set_properties_probe_request(&request)),
            _ => {}
        }
    }

    summary.open_folder_request_count = open_folder_requests.len();
    summary.open_folder_requests = open_folder_requests
        .iter()
        .map(|request| {
            format!(
                "out={};folder=0x{:016x};name={}",
                request.output_handle_index,
                request.folder_id,
                post_hierarchy_probe_folder_name(request.folder_id)
            )
        })
        .collect::<Vec<_>>()
        .join("|");
    summary.get_properties_specific_request_count = get_properties_requests.len();
    summary.get_properties_specific_requests = get_properties_requests
        .iter()
        .map(|request| {
            format!(
                "in={};tags={}",
                request.input_handle_index,
                format_debug_property_tags(&request.property_tags)
            )
        })
        .collect::<Vec<_>>()
        .join("|");
    summary.set_properties_request_count = set_properties_requests.len();
    summary.set_properties_requests = set_properties_requests
        .iter()
        .map(|request| {
            format!(
                "in={};tags={};values={};default_folder_entry_ids={};parse_error={}",
                request.input_handle_index,
                format_debug_property_tags(&request.property_tags),
                request.property_value_shapes,
                request.default_folder_entry_id_values,
                request.parse_error
            )
        })
        .collect::<Vec<_>>()
        .join("|");

    let Some((responses, _response_handle_table)) = split_rop_buffer(response_rop_buffer) else {
        if summary.parse_error.is_empty() {
            summary.parse_error = "invalid response ROP buffer".to_string();
        }
        return summary;
    };
    let mut response_offset = 0usize;
    let mut open_folder_index = 0usize;
    let mut get_properties_index = 0usize;
    let mut set_properties_index = 0usize;
    let mut open_folder_responses = Vec::new();
    let mut get_properties_responses = Vec::new();
    let mut set_properties_responses = Vec::new();
    for rop_id in request_rop_ids {
        if rop_has_no_response(rop_id) {
            continue;
        }
        let Some(found) = responses
            .get(response_offset..)
            .and_then(|remaining| remaining.iter().position(|candidate| *candidate == rop_id))
        else {
            break;
        };
        response_offset += found;
        match rop_id {
            0x02 => {
                if let Some(request) = open_folder_requests.get(open_folder_index) {
                    open_folder_responses.push(summarize_open_folder_probe_response(
                        responses,
                        response_offset,
                        request,
                    ));
                }
                open_folder_index = open_folder_index.saturating_add(1);
            }
            0x07 => {
                if let Some(request) = get_properties_requests.get(get_properties_index) {
                    get_properties_responses.push(summarize_get_properties_probe_response(
                        responses,
                        response_offset,
                        request,
                    ));
                }
                get_properties_index = get_properties_index.saturating_add(1);
            }
            0x0A | 0x79 => {
                if let Some(request) = set_properties_requests.get(set_properties_index) {
                    set_properties_responses.push(summarize_set_properties_probe_response(
                        responses,
                        response_offset,
                        request,
                    ));
                }
                set_properties_index = set_properties_index.saturating_add(1);
            }
            _ => {}
        }
        response_offset = response_offset.saturating_add(6);
    }
    summary.open_folder_response_shapes = open_folder_responses.join("|");
    summary.get_properties_specific_response_shapes = get_properties_responses.join("|");
    summary.set_properties_response_shapes = set_properties_responses.join("|");
    summary
}

pub(in crate::mapi::dispatch) fn set_properties_probe_request(
    request: &RopRequest,
) -> SetPropertiesProbeRequest {
    match request.property_values() {
        Ok(values) => SetPropertiesProbeRequest {
            input_handle_index: request.input_handle_index().unwrap_or(0),
            property_tags: values.iter().map(|(tag, _value)| *tag).collect(),
            property_value_shapes: values
                .iter()
                .map(|(tag, value)| format!("{tag:#010x}:{}", mapi_value_debug_shape(value)))
                .collect::<Vec<_>>()
                .join(","),
            associated_config_stream_summary: associated_config_stream_write_summary(&values),
            default_folder_entry_id_values: default_folder_entry_id_values_for_debug(&values),
            parse_error: String::new(),
        },
        Err(error) => SetPropertiesProbeRequest {
            input_handle_index: request.input_handle_index().unwrap_or(0),
            property_tags: Vec::new(),
            property_value_shapes: String::new(),
            associated_config_stream_summary: String::new(),
            default_folder_entry_id_values: String::new(),
            parse_error: error.to_string(),
        },
    }
}

pub(in crate::mapi::dispatch) fn summarize_open_folder_probe_response(
    responses: &[u8],
    offset: usize,
    request: &OpenFolderProbeRequest,
) -> String {
    let result = read_response_error_code(responses, offset)
        .map(|code| format!("{code:#010x}"))
        .unwrap_or_else(|| "truncated".to_string());
    let has_rules = responses
        .get(offset + 6)
        .map(|value| value.to_string())
        .unwrap_or_else(|| "truncated".to_string());
    let is_ghosted = responses
        .get(offset + 7)
        .map(|value| value.to_string())
        .unwrap_or_else(|| "truncated".to_string());
    format!(
        "out={};folder=0x{:016x};name={};result={result};has_rules={has_rules};is_ghosted={is_ghosted}",
        request.output_handle_index,
        request.folder_id,
        post_hierarchy_probe_folder_name(request.folder_id)
    )
}

pub(in crate::mapi::dispatch) fn summarize_get_properties_probe_response(
    responses: &[u8],
    offset: usize,
    request: &GetPropertiesSpecificProbeRequest,
) -> String {
    let result = read_response_error_code(responses, offset)
        .map(|code| format!("{code:#010x}"))
        .unwrap_or_else(|| "truncated".to_string());
    let row_shape = match responses.get(offset + 6).copied() {
        Some(0) => "standard",
        Some(1) => "flagged",
        Some(_) => "unknown",
        None => "truncated",
    };
    let values = summarize_get_properties_probe_response_values(responses, offset, request);
    format!(
        "in={};result={result};row={row_shape};tags={};values={values}",
        request.input_handle_index,
        format_debug_property_tags(&request.property_tags)
    )
}

fn summarize_get_properties_probe_response_values(
    responses: &[u8],
    offset: usize,
    request: &GetPropertiesSpecificProbeRequest,
) -> String {
    if responses.get(offset + 6).copied() != Some(0) {
        return "not-standard-row".to_string();
    }
    let mut cursor = Cursor::new(responses.get(offset + 7..).unwrap_or_default());
    let mut values = Vec::new();
    for tag in &request.property_tags {
        match parse_property_value_for_tag(&mut cursor, *tag) {
            Ok(value) => values.push(format!("{tag:#010x}:{}", mapi_value_debug_shape(&value))),
            Err(error) => {
                values.push(format!("{tag:#010x}:parse_error={error}"));
                break;
            }
        }
    }
    values.join(",")
}

pub(in crate::mapi::dispatch) fn summarize_set_properties_probe_response(
    responses: &[u8],
    offset: usize,
    request: &SetPropertiesProbeRequest,
) -> String {
    let result = read_response_error_code(responses, offset)
        .map(|code| format!("{code:#010x}"))
        .unwrap_or_else(|| "truncated".to_string());
    let property_problem_count = responses
        .get(offset + 6..offset + 8)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u16::from_le_bytes)
        .map(|count| count.to_string())
        .unwrap_or_else(|| "truncated".to_string());
    format!(
        "in={};result={result};property_problem_count={property_problem_count};tags={}",
        request.input_handle_index,
        format_debug_property_tags(&request.property_tags)
    )
}
