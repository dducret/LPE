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

pub(super) fn cache_named_property_mapping_and_return_property_id(
    session: &mut MapiSession,
    property_id: u16,
    property: MapiNamedProperty,
) -> u16 {
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
    let mut missing = Vec::new();
    for (index, property) in properties.iter().cloned().enumerate() {
        match session.property_id_for_name(property.clone(), false) {
            Some(property_id) => property_ids.push(property_id),
            None => {
                property_ids.push(0);
                missing.push((index, property));
            }
        }
    }
    let missing_properties = missing
        .iter()
        .map(|(_index, property)| property.clone())
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        match store
            .fetch_or_allocate_mapi_named_property_ids(
                principal.account_id,
                &missing_properties,
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
            missing_named_property_count = missing_properties.len(),
            missing_named_properties = %format_debug_named_properties(&missing_properties),
            returned_property_id_count = property_ids.len(),
            returned_property_ids = %format_debug_property_ids(&property_ids),
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
        missing_named_property_count = missing_properties.len(),
        missing_named_properties = %format_debug_named_properties(&missing_properties),
        returned_property_id_count = property_ids.len(),
        returned_property_ids = %format_debug_property_ids(&property_ids),
        message = "rca debug mapi get property ids from names",
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
    responses.extend_from_slice(&rop_get_property_ids_from_names_response(
        request,
        &property_ids,
    ));
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
