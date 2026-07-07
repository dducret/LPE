use super::*;

pub(super) fn is_named_property_rop(rop_id: RopId) -> bool {
    matches!(
        rop_id,
        RopId::GetNamesFromPropertyIds
            | RopId::GetPropertyIdsFromNames
            | RopId::QueryNamedProperties
    )
}

pub(super) fn contains_outlook_osc_contact_source_probe(properties: &[MapiNamedProperty]) -> bool {
    properties.iter().any(|property| {
        property.guid == PS_PUBLIC_STRINGS_GUID
            && match &property.kind {
                MapiNamedPropertyKind::Name(name) => name.eq_ignore_ascii_case("OscContactSources"),
                MapiNamedPropertyKind::Lid(lid) => matches!(
                    *lid,
                    PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80E1
                        | PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80EA
                        | PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80EC
                        | PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80ED
                ),
            }
    })
}

pub(super) fn contains_outlook_view_descriptor_probe(properties: &[MapiNamedProperty]) -> bool {
    properties.iter().any(|property| {
        matches!(
            (&property.guid, &property.kind),
            (guid, MapiNamedPropertyKind::Lid(lid))
                if (*guid == PSETID_COMMON_GUID
                    && matches!(*lid, PID_LID_COMMON_START | PID_LID_COMMON_END))
                    || (*guid == PSETID_APPOINTMENT_GUID
                        && matches!(*lid, PID_LID_LOCATION | PID_LID_BUSY_STATUS))
                    || (*guid == PSETID_ADDRESS_GUID
                        && matches!(*lid, PID_LID_EMAIL1_EMAIL_ADDRESS))
                    || (*guid == PSETID_TASK_GUID
                        && matches!(
                            *lid,
                            PID_LID_TASK_DUE_DATE
                                | PID_LID_TASK_START_DATE
                                | PID_LID_PERCENT_COMPLETE
                        ))
                    || (*guid == PSETID_NOTE_GUID && matches!(*lid, PID_LID_NOTE_COLOR))
                    || (*guid == PSETID_LOG_GUID
                        && matches!(
                            *lid,
                            PID_LID_LOG_START | PID_LID_LOG_DURATION | PID_LID_LOG_TYPE
                        ))
        )
    })
}

pub(super) fn cache_named_property_mapping_and_return_property_id(
    session: &mut MapiSession,
    property_id: u16,
    property: MapiNamedProperty,
) -> u16 {
    let property = normalize_named_property(property);
    if is_reserved_named_property_id(property_id)
        && well_known_named_property_id(&property).is_none()
    {
        return session
            .property_id_for_name(property, true)
            .unwrap_or(property_id);
    }
    let existing_property = session
        .named_property_ids
        .get(&property_id)
        .cloned()
        .or_else(|| well_known_named_property_for_id(property_id));
    if existing_property
        .as_ref()
        .is_some_and(|existing| existing != &property)
    {
        return session
            .property_id_for_name(property, true)
            .unwrap_or(property_id);
    }
    let property_for_lookup = property.clone();
    session.cache_named_property(property_id, property);
    session
        .property_id_for_name(property_for_lookup, false)
        .unwrap_or(property_id)
}

pub(super) async fn append_get_names_from_property_ids_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    request: &RopRequest,
    responses: &mut Vec<u8>,
) where
    S: ExchangeStore,
{
    let property_ids = request.property_ids();
    let missing_property_ids = property_ids
        .iter()
        .copied()
        .filter(|property_id| !session.named_property_ids.contains_key(property_id))
        .collect::<Vec<_>>();
    if !missing_property_ids.is_empty() {
        if let Ok(mappings) = store
            .fetch_mapi_named_properties_by_ids(principal.account_id, &missing_property_ids)
            .await
        {
            for mapping in mappings {
                session.cache_named_property(mapping.property_id, mapping.property);
            }
        }
    }
    responses.extend_from_slice(&rop_get_names_from_property_ids_response(request, session));
}

pub(super) async fn append_get_property_ids_from_names_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    request_id: &str,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    responses: &mut Vec<u8>,
) where
    S: ExchangeStore,
{
    let properties = match request.named_property_names() {
        Ok(properties) => properties,
        Err(_) => {
            responses.extend_from_slice(&rop_error_response(
                0x56,
                request.response_handle_index(),
                0x8004_0102,
            ));
            return;
        }
    };
    if properties.is_empty()
        && matches!(
            input_object(session, handle_slots, request),
            Some(MapiObject::Logon | MapiObject::PublicFolderLogon)
        )
    {
        if let Ok(mappings) = store
            .fetch_mapi_named_properties(principal.account_id, None)
            .await
        {
            for mapping in mappings {
                session.cache_named_property(mapping.property_id, mapping.property);
            }
        }
        let property_ids = session
            .named_properties_for_query(None)
            .into_iter()
            .map(|(property_id, _property)| property_id)
            .collect::<Vec<_>>();
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            mailbox = %principal.email,
            request_type = "Execute",
            request_rop_id = "0x56",
            input_handle_index = request.input_handle_index().unwrap_or(0),
            response_handle_index = request.response_handle_index(),
            object_kind = "logon",
            create_missing = request.named_property_create(),
            requested_named_property_count = 0,
            requested_named_properties = "",
            missing_named_property_count = 0,
            missing_named_properties = "",
            returned_property_id_count = property_ids.len(),
            returned_property_ids = %format_debug_property_ids(&property_ids),
            message = "rca debug mapi get property ids from names",
        );
        responses.extend_from_slice(&rop_get_property_ids_from_names_response(
            request,
            &property_ids,
        ));
        return;
    }
    let requested_named_properties = format_debug_named_properties(&properties);
    let mut property_ids = Vec::with_capacity(properties.len());
    let mut property_id_sources = Vec::with_capacity(properties.len());
    let mut missing = Vec::new();
    for (index, property) in properties.iter().cloned().enumerate() {
        let normalized = normalize_named_property(property.clone());
        let well_known = well_known_named_property_id(&normalized);
        match session.property_id_for_name(normalized, false) {
            Some(property_id) => {
                property_ids.push(property_id);
                property_id_sources.push(if well_known == Some(property_id) {
                    "well_known"
                } else {
                    "session_cached"
                });
            }
            None => {
                property_ids.push(0);
                property_id_sources.push("missing");
                missing.push((index, property));
            }
        }
    }
    let missing_properties = missing
        .iter()
        .map(|(_index, property)| property.clone())
        .collect::<Vec<_>>();
    let post_calendar_query_position_probe = !session
        .post_hierarchy_actions
        .last_calendar_normal_contents_table_query_position_context
        .is_empty()
        && !session
            .post_hierarchy_actions
            .calendar_normal_contents_table_query_rows_observed;
    if post_calendar_query_position_probe && !missing.is_empty() {
        if let Ok(mappings) = store
            .fetch_mapi_named_properties(principal.account_id, None)
            .await
        {
            let db_mapping_by_property = mappings
                .into_iter()
                .map(|mapping| {
                    (
                        normalize_named_property(mapping.property),
                        mapping.property_id,
                    )
                })
                .collect::<std::collections::HashMap<_, _>>();
            missing.retain(|(index, property)| {
                let normalized = normalize_named_property(property.clone());
                let Some(property_id) = db_mapping_by_property.get(&normalized).copied() else {
                    return true;
                };
                session.cache_named_property(property_id, normalized);
                property_ids[*index] = property_id;
                property_id_sources[*index] = "db_existing";
                false
            });
        }
    }
    if !missing.is_empty() {
        let allocatable_properties = missing
            .iter()
            .map(|(_index, property)| property.clone())
            .collect::<Vec<_>>();
        match store
            .fetch_or_allocate_mapi_named_property_ids(
                principal.account_id,
                &allocatable_properties,
                request.named_property_create(),
            )
            .await
        {
            Ok(mappings) => {
                for (missing_index, (index, property)) in missing.into_iter().enumerate() {
                    let mapping = mappings.get(missing_index).cloned().flatten();
                    let property_id = mapping
                        .map(|mapping| {
                            cache_named_property_mapping_and_return_property_id(
                                session,
                                mapping.property_id,
                                mapping.property,
                            )
                        })
                        .or_else(|| {
                            session.property_id_for_name(property, request.named_property_create())
                        });
                    property_ids[index] = property_id.unwrap_or(0);
                    property_id_sources[index] = if property_id.is_some() {
                        if post_calendar_query_position_probe {
                            "newly_allocated"
                        } else {
                            "store_existing_or_allocated"
                        }
                    } else {
                        "unresolved"
                    };
                }
            }
            Err(_) if request.named_property_create() => {
                responses.extend_from_slice(&rop_error_response(
                    0x56,
                    request.response_handle_index(),
                    0x8007_000E,
                ));
                return;
            }
            Err(_) => {}
        }
    }
    if !request.named_property_create() && property_ids.contains(&0) {
        let duplicate_summary = summarize_named_property_id_duplicates(&properties, &property_ids);
        let property_family_summary = format_named_property_family_summary(&properties);
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            mailbox = %principal.email,
            request_type = "Execute",
            request_rop_id = "0x56",
            input_handle_index = request.input_handle_index().unwrap_or(0),
            response_handle_index = request.response_handle_index(),
            object_kind = mapi_object_debug_kind(input_object(session, handle_slots, request)),
            create_missing = request.named_property_create(),
            requested_named_property_count = properties.len(),
            requested_named_properties = %requested_named_properties,
            requested_named_property_family_summary = %property_family_summary,
            missing_named_property_count = missing_properties.len(),
            missing_named_properties = %format_debug_named_properties(&missing_properties),
            returned_property_id_count = property_ids.len(),
            returned_property_ids = %format_debug_property_ids(&property_ids),
            duplicate_requested_named_property_count = duplicate_summary.0,
            duplicate_returned_property_id_count = duplicate_summary.1,
            returned_property_id_collision_count = duplicate_summary.2,
            returned_property_id_collisions = %duplicate_summary.3,
            rop_return_value = "0x8004010f",
            message = "rca debug mapi get property ids from names",
        );
        responses.extend_from_slice(&rop_error_response(
            0x56,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return;
    }
    let duplicate_summary = summarize_named_property_id_duplicates(&properties, &property_ids);
    let unresolved_properties = unresolved_named_properties(&properties, &property_ids);
    let unresolved_named_property_count = unresolved_properties.len();
    let property_id_source_summary = format_named_property_id_sources(&property_id_sources);
    let property_family_summary = format_named_property_family_summary(&properties);
    let property_id_mapping_summary =
        format_named_property_resolution_mappings(&properties, &property_ids, &property_id_sources);
    let allocated_or_store_resolved_named_property_count = property_id_sources
        .iter()
        .filter(|source| matches!(**source, "store_existing_or_allocated" | "newly_allocated"))
        .count();
    let legacy_low_dynamic_property_id_count = legacy_low_dynamic_property_id_count(&property_ids);
    let named_property_response = rop_get_property_ids_from_names_response(request, &property_ids);
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = "0x56",
        input_handle_index = request.input_handle_index().unwrap_or(0),
        response_handle_index = request.response_handle_index(),
        object_kind = mapi_object_debug_kind(input_object(session, handle_slots, request)),
        create_missing = request.named_property_create(),
        requested_named_property_count = properties.len(),
        requested_named_properties = %requested_named_properties,
        requested_named_property_family_summary = %property_family_summary,
        pre_resolution_missing_named_property_count = missing_properties.len(),
        missing_named_property_count = missing_properties.len(),
        missing_named_properties = %format_debug_named_properties(&missing_properties),
        allocated_or_store_resolved_named_property_count,
        unresolved_returned_property_id_count = unresolved_named_property_count,
        unresolved_returned_named_properties = %format_debug_named_properties(&unresolved_properties),
        legacy_low_dynamic_property_id_count,
        returned_property_id_count = property_ids.len(),
        returned_property_ids = %format_debug_property_ids(&property_ids),
        returned_property_id_sources = %property_id_source_summary,
        returned_named_property_mappings = %property_id_mapping_summary,
        response_rop_payload_bytes = named_property_response.len(),
        duplicate_requested_named_property_count = duplicate_summary.0,
        duplicate_returned_property_id_count = duplicate_summary.1,
        returned_property_id_collision_count = duplicate_summary.2,
        returned_property_id_collisions = %duplicate_summary.3,
        message = "rca debug mapi get property ids from names",
    );
    record_post_calendar_query_position_named_property_probe(
        session,
        handle_slots,
        request,
        request_id,
        properties.len(),
        missing_properties.len(),
        allocated_or_store_resolved_named_property_count,
        unresolved_named_property_count,
        legacy_low_dynamic_property_id_count,
        property_ids.len(),
        duplicate_summary.0,
        duplicate_summary.1,
        duplicate_summary.2,
        &duplicate_summary.3,
        &missing_properties,
        &property_id_source_summary,
        &property_id_mapping_summary,
        named_property_response.len(),
    );
    record_outlook_umolk_named_property_probe(
        session,
        handle_slots,
        request,
        request_id,
        properties.len(),
        missing_properties.len(),
        allocated_or_store_resolved_named_property_count,
        unresolved_named_property_count,
        legacy_low_dynamic_property_id_count,
        property_ids.len(),
        duplicate_summary.0,
        duplicate_summary.1,
        duplicate_summary.2,
        &duplicate_summary.3,
        &property_id_source_summary,
        &property_family_summary,
        &property_id_mapping_summary,
        named_property_response.len(),
    );
    if contains_outlook_osc_contact_source_probe(&properties) {
        session.record_outlook_view_failure_trace_event(format!(
            "resolve_osc_contact_sources:request_id={request_id};object={};create_missing={};requested={};returned={}",
            mapi_object_debug_kind(input_object(session, handle_slots, request)),
            request.named_property_create(),
            requested_named_properties,
            format_debug_property_ids(&property_ids)
        ));
    }
    if contains_outlook_view_descriptor_probe(&properties) {
        session.record_outlook_view_failure_trace_event(format!(
            "resolve_outlook_view_descriptor_props:request_id={request_id};object={};create_missing={};requested={};returned={}",
            mapi_object_debug_kind(input_object(session, handle_slots, request)),
            request.named_property_create(),
            requested_named_properties,
            format_debug_property_ids(&property_ids)
        ));
    }
    responses.extend_from_slice(&named_property_response);
}

fn record_outlook_umolk_named_property_probe(
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    request_id: &str,
    requested_count: usize,
    missing_count: usize,
    allocated_or_store_resolved_count: usize,
    unresolved_count: usize,
    legacy_low_dynamic_property_id_count: usize,
    returned_count: usize,
    duplicate_requested_count: usize,
    duplicate_returned_id_count: usize,
    returned_id_collision_count: usize,
    returned_id_collisions: &str,
    property_id_source_summary: &str,
    property_family_summary: &str,
    property_id_mapping_summary: &str,
    response_rop_payload_bytes: usize,
) {
    if !request.named_property_create() || requested_count <= 50 {
        return;
    }
    let Some((config_id, message_class)) =
        input_object(session, handle_slots, request).and_then(|object| match object {
            MapiObject::AssociatedConfig {
                folder_id: INBOX_FOLDER_ID,
                config_id,
                saved_message: Some(saved_message),
            } if crate::mapi_store::is_outlook_umolk_user_options_message_class(
                &saved_message.message_class,
            ) =>
            {
                Some((*config_id, saved_message.message_class.clone()))
            }
            _ => None,
        })
    else {
        return;
    };
    session
        .post_hierarchy_actions
        .outlook_umolk_named_property_probe_count = session
        .post_hierarchy_actions
        .outlook_umolk_named_property_probe_count
        .saturating_add(1);
    session
        .post_hierarchy_actions
        .last_outlook_umolk_named_property_probe_context = format!(
        "request_id={request_id};handle={};config=0x{config_id:016x};class={};create_missing=true;requested={requested_count};pre_resolution_missing={missing_count};allocated_or_store_resolved={allocated_or_store_resolved_count};unresolved={unresolved_count};legacy_low_dynamic_ids={legacy_low_dynamic_property_id_count};returned={returned_count};duplicate_requested={duplicate_requested_count};duplicate_returned_ids={duplicate_returned_id_count};returned_id_collisions={returned_id_collision_count};returned_id_collision_detail={};families={};sources={};mappings={};response_rop_payload_bytes={response_rop_payload_bytes}",
        request.input_handle_index().unwrap_or(0),
        message_class,
        debug_context_or_none(returned_id_collisions),
        property_family_summary,
        property_id_source_summary,
        truncate_named_property_debug_field(property_id_mapping_summary, 2048),
    );
    session.record_outlook_view_failure_trace_event(format!(
        "umolk_named_property_burst:{}",
        session
            .post_hierarchy_actions
            .last_outlook_umolk_named_property_probe_context
    ));
}

fn truncate_named_property_debug_field(value: &str, limit: usize) -> String {
    if value.len() <= limit {
        value.to_string()
    } else {
        format!("{}...", &value[..limit])
    }
}

fn record_post_calendar_query_position_named_property_probe(
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    request_id: &str,
    requested_count: usize,
    missing_count: usize,
    allocated_or_store_resolved_count: usize,
    unresolved_count: usize,
    legacy_low_dynamic_property_id_count: usize,
    returned_count: usize,
    duplicate_requested_count: usize,
    duplicate_returned_id_count: usize,
    returned_id_collision_count: usize,
    returned_id_collisions: &str,
    missing_properties: &[MapiNamedProperty],
    property_id_source_summary: &str,
    property_id_mapping_summary: &str,
    response_rop_payload_bytes: usize,
) {
    if session
        .post_hierarchy_actions
        .last_calendar_normal_contents_table_query_position_context
        .is_empty()
        || session
            .post_hierarchy_actions
            .calendar_normal_contents_table_query_rows_observed
    {
        return;
    }
    let object_kind = mapi_object_debug_kind(input_object(session, handle_slots, request));
    let calendar_query_position_context = session
        .post_hierarchy_actions
        .last_calendar_normal_contents_table_query_position_context
        .clone();
    let inbox_normal_contents_table_observed = session
        .post_hierarchy_actions
        .inbox_normal_contents_table_observed;
    let inbox_normal_contents_table_setcolumns_observed = session
        .post_hierarchy_actions
        .inbox_normal_contents_table_setcolumns_observed;
    let inbox_normal_contents_table_query_rows_observed = session
        .post_hierarchy_actions
        .inbox_normal_contents_table_query_rows_observed;
    let last_inbox_normal_contents_table_context = session
        .post_hierarchy_actions
        .last_inbox_contents_table_context
        .clone();
    let last_inbox_normal_contents_table_setcolumns_context = session
        .post_hierarchy_actions
        .last_inbox_normal_contents_table_setcolumns_context
        .clone();
    let last_inbox_normal_contents_table_query_position_context = session
        .post_hierarchy_actions
        .last_inbox_normal_contents_table_query_position_context
        .clone();
    let last_inbox_normal_contents_table_query_rows_context = session
        .post_hierarchy_actions
        .last_inbox_normal_contents_table_query_rows_context
        .clone();
    let missing_named_property_sample = format_debug_named_property_sample(missing_properties, 32);
    let visible_inbox_release_without_query_rows =
        crate::mapi::transport::visible_inbox_release_without_query_rows_observed(
            &session.post_hierarchy_actions,
        );
    let input_handle_table_summary = format_debug_handle_table(handle_slots);
    let live_handle_summaries = format_live_handle_debug_summary(session);
    let next_debug_focus = "calendar_query_rows_missing_after_named_property_probe";
    let context = format!(
        "request_id={request_id};object={object_kind};create_missing={};requested={requested_count};pre_resolution_missing={missing_count};missing_sample={missing_named_property_sample};allocated_or_store_resolved={allocated_or_store_resolved_count};unresolved_returned={unresolved_count};legacy_low_dynamic_property_ids={legacy_low_dynamic_property_id_count};returned={returned_count};property_id_sources={property_id_source_summary};response_rop_payload_bytes={response_rop_payload_bytes};input_handle_table={input_handle_table_summary};live_handles={live_handle_summaries};duplicate_requested={duplicate_requested_count};duplicate_returned_ids={duplicate_returned_id_count};returned_id_collisions={returned_id_collision_count};collision_summary={returned_id_collisions};visible_inbox_release_without_query_rows={visible_inbox_release_without_query_rows};inbox_normal_contents_table_observed={inbox_normal_contents_table_observed};inbox_normal_contents_table_setcolumns_observed={inbox_normal_contents_table_setcolumns_observed};inbox_normal_contents_table_query_rows_observed={inbox_normal_contents_table_query_rows_observed};last_inbox_normal_contents_table={last_inbox_normal_contents_table_context};last_inbox_normal_setcolumns={last_inbox_normal_contents_table_setcolumns_context};last_inbox_normal_query_position={last_inbox_normal_contents_table_query_position_context};last_inbox_normal_query_rows={last_inbox_normal_contents_table_query_rows_context};after_calendar_query_position={calendar_query_position_context}",
        request.named_property_create()
    );
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        request_type = "Execute",
        request_rop_id = "0x56",
        request_id,
        object_kind,
        create_missing = request.named_property_create(),
        requested_named_property_count = requested_count,
        pre_resolution_missing_named_property_count = missing_count,
        missing_named_property_count = missing_count,
        missing_named_property_sample = %missing_named_property_sample,
        allocated_or_store_resolved_named_property_count = allocated_or_store_resolved_count,
        unresolved_returned_property_id_count = unresolved_count,
        legacy_low_dynamic_property_id_count,
        returned_property_id_count = returned_count,
        returned_property_id_sources = %property_id_source_summary,
        returned_named_property_mappings = %property_id_mapping_summary,
        response_rop_payload_bytes,
        input_handle_table_summary = %input_handle_table_summary,
        live_handle_summaries = %live_handle_summaries,
        duplicate_requested_named_property_count = duplicate_requested_count,
        duplicate_returned_property_id_count = duplicate_returned_id_count,
        returned_property_id_collision_count = returned_id_collision_count,
        returned_property_id_collisions = returned_id_collisions,
        inbox_normal_contents_table_observed,
        inbox_normal_contents_table_setcolumns_observed,
        inbox_normal_contents_table_query_rows_observed,
        visible_inbox_release_without_query_rows,
        last_inbox_normal_contents_table_context =
            %last_inbox_normal_contents_table_context,
        last_inbox_normal_contents_table_setcolumns_context =
            %last_inbox_normal_contents_table_setcolumns_context,
        last_inbox_normal_contents_table_query_position_context =
            %last_inbox_normal_contents_table_query_position_context,
        last_inbox_normal_contents_table_query_rows_context =
            %last_inbox_normal_contents_table_query_rows_context,
        calendar_query_position_context = %calendar_query_position_context,
        next_debug_focus,
        "rca debug mapi post calendar query position named property probe"
    );
    session.record_post_calendar_query_position_named_property_probe(context);
}

fn format_named_property_id_sources(sources: &[&str]) -> String {
    let mut counts = std::collections::BTreeMap::<&str, usize>::new();
    for source in sources {
        *counts.entry(*source).or_default() += 1;
    }
    counts
        .into_iter()
        .map(|(source, count)| format!("{source}={count}"))
        .collect::<Vec<_>>()
        .join(";")
}

pub(super) fn format_named_property_family_summary(properties: &[MapiNamedProperty]) -> String {
    let mut counts = std::collections::BTreeMap::<String, usize>::new();
    for property in properties.iter().cloned().map(normalize_named_property) {
        let kind = match property.kind {
            MapiNamedPropertyKind::Lid(lid) => format!("lid_{:#04x}", lid & 0xff00),
            MapiNamedPropertyKind::Name(_) => "name".to_string(),
        };
        let key = format!("{}:{kind}", hex_preview(&property.guid, 16));
        *counts.entry(key).or_default() += 1;
    }
    counts
        .into_iter()
        .map(|(family, count)| format!("{family}={count}"))
        .collect::<Vec<_>>()
        .join(";")
}

fn unresolved_named_properties(
    properties: &[MapiNamedProperty],
    property_ids: &[u16],
) -> Vec<MapiNamedProperty> {
    properties
        .iter()
        .zip(property_ids.iter().copied())
        .filter_map(|(property, property_id)| (property_id == 0).then_some(property.clone()))
        .collect()
}

fn legacy_low_dynamic_property_id_count(property_ids: &[u16]) -> usize {
    property_ids
        .iter()
        .copied()
        .filter(|property_id| {
            (FIRST_NAMED_PROPERTY_ID..DYNAMIC_NAMED_PROPERTY_ID_START).contains(property_id)
                && !is_reserved_named_property_id(*property_id)
        })
        .count()
}

fn format_named_property_resolution_mappings(
    properties: &[MapiNamedProperty],
    property_ids: &[u16],
    sources: &[&str],
) -> String {
    properties
        .iter()
        .zip(property_ids.iter().copied())
        .zip(sources.iter().copied())
        .enumerate()
        .map(|(index, ((property, property_id), source))| {
            format!(
                "{index}:0x{property_id:04x}:{source}:{}",
                format_debug_named_properties(std::slice::from_ref(property))
            )
        })
        .collect::<Vec<_>>()
        .join("|")
}

fn format_debug_handle_table(handle_slots: &[u32]) -> String {
    let handles = handle_slots
        .iter()
        .map(|handle| format!("{handle:#010x}"))
        .collect::<Vec<_>>()
        .join(",");
    format!("count={};handles={handles}", handle_slots.len())
}

pub(super) fn format_debug_named_property_sample(
    properties: &[MapiNamedProperty],
    limit: usize,
) -> String {
    let mut sample = properties.iter().take(limit).cloned().collect::<Vec<_>>();
    let mut formatted = format_debug_named_properties(&sample);
    if properties.len() > sample.len() {
        if !formatted.is_empty() {
            formatted.push('|');
        }
        formatted.push_str(&format!("...{} more", properties.len() - sample.len()));
    }
    sample.clear();
    formatted
}

pub(super) fn summarize_named_property_id_duplicates(
    properties: &[MapiNamedProperty],
    property_ids: &[u16],
) -> (usize, usize, usize, String) {
    let normalized_properties = properties
        .iter()
        .cloned()
        .map(normalize_named_property)
        .collect::<Vec<_>>();
    let mut request_counts = std::collections::HashMap::<String, usize>::new();
    for property in &normalized_properties {
        let key = format_debug_named_properties(std::slice::from_ref(property));
        *request_counts.entry(key).or_insert(0) += 1;
    }
    let duplicate_requested_count = request_counts
        .values()
        .map(|count| count.saturating_sub(1))
        .sum::<usize>();

    let mut id_counts = std::collections::HashMap::<u16, usize>::new();
    let mut id_to_names =
        std::collections::HashMap::<u16, std::collections::HashSet<String>>::new();
    for (property, property_id) in normalized_properties
        .iter()
        .zip(property_ids.iter().copied())
    {
        *id_counts.entry(property_id).or_insert(0) += 1;
        id_to_names
            .entry(property_id)
            .or_default()
            .insert(format_debug_named_properties(std::slice::from_ref(
                property,
            )));
    }
    let duplicate_returned_id_count = id_counts
        .values()
        .map(|count| count.saturating_sub(1))
        .sum::<usize>();
    let mut collisions = id_to_names
        .into_iter()
        .filter_map(|(property_id, names)| {
            (names.len() > 1).then_some(format!("0x{property_id:04x}:{}", names.len()))
        })
        .collect::<Vec<_>>();
    collisions.sort();
    (
        duplicate_requested_count,
        duplicate_returned_id_count,
        collisions.len(),
        collisions.join(","),
    )
}

pub(super) async fn append_query_named_properties_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    request: &RopRequest,
    responses: &mut Vec<u8>,
) where
    S: ExchangeStore,
{
    if let Ok(mappings) = store
        .fetch_mapi_named_properties(principal.account_id, request.named_property_query_guid())
        .await
    {
        for mapping in mappings {
            session.cache_named_property(mapping.property_id, mapping.property);
        }
    }
    responses.extend_from_slice(&rop_query_named_properties_response(request, session));
}

pub(super) async fn append_named_property_dispatch_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    request_id: &str,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    responses: &mut Vec<u8>,
) -> bool
where
    S: ExchangeStore,
{
    match RopId::from_u8(request.rop_id) {
        Some(RopId::GetNamesFromPropertyIds) => {
            append_get_names_from_property_ids_response(
                store, principal, session, request, responses,
            )
            .await;
            false
        }
        Some(RopId::GetPropertyIdsFromNames) => {
            append_get_property_ids_from_names_response(
                store,
                principal,
                request_id,
                session,
                handle_slots,
                request,
                responses,
            )
            .await;
            true
        }
        Some(RopId::QueryNamedProperties) => {
            append_query_named_properties_response(store, principal, session, request, responses)
                .await;
            false
        }
        _ => false,
    }
}
