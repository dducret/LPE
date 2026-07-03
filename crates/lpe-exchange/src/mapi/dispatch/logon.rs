use super::*;

pub(super) fn is_logon_dispatch_rop(rop_id: RopId) -> bool {
    matches!(rop_id, RopId::Logon | RopId::GetAddressTypes)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn append_logon_dispatch_response(
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    typed_request: &TypedRopRequest,
    principal: &AccountPrincipal,
    request_id: &str,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
    output_handles: &mut Vec<u32>,
) -> bool {
    match RopId::from_u8(request.rop_id) {
        Some(RopId::Logon) => {
            append_logon_response(
                session,
                handle_slots,
                request,
                typed_request,
                principal,
                request_id,
                mailboxes,
                emails,
                snapshot,
                responses,
                output_handles,
            );
            false
        }
        Some(RopId::GetAddressTypes) => append_address_types_dispatch_response(
            principal,
            session,
            handle_slots,
            request,
            responses,
        ),
        _ => false,
    }
}

pub(super) struct LogonResponseContext {
    pub(super) handle: u32,
    pub(super) is_private_logon: bool,
    pub(super) special_folder_ids: String,
}

pub(super) fn allocate_logon_response_context(
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    principal: &AccountPrincipal,
    request: &RopRequest,
) -> LogonResponseContext {
    let is_private_logon = request.payload.first().copied().unwrap_or(0) & 0x01 != 0;
    let logon_object = if is_private_logon {
        MapiObject::Logon
    } else {
        MapiObject::PublicFolderLogon
    };
    let handle = session.allocate_output_handle(request.output_handle_index, logon_object);
    set_handle_slot(handle_slots, request.output_handle_index, handle);
    let special_folder_ids = if is_private_logon {
        PRIVATE_LOGON_SPECIAL_FOLDER_IDS.as_slice()
    } else {
        PUBLIC_LOGON_SPECIAL_FOLDER_IDS.as_slice()
    }
    .iter()
    .map(|folder_id| format!("{folder_id:#018x}"))
    .collect::<Vec<_>>()
    .join(",");
    session.record_logon_identity(MapiLogonIdentityDebug {
        mailbox_guid: principal.account_id.to_string(),
        replid: STORE_REPLICA_ID.to_string(),
        replica_guid: bytes_to_hex(&crate::mapi::identity::STORE_REPLICA_GUID),
        response_flags: if is_private_logon { "0x07" } else { "0x00" }.to_string(),
        special_folder_ids: special_folder_ids.clone(),
    });
    LogonResponseContext {
        handle,
        is_private_logon,
        special_folder_ids,
    }
}

pub(super) fn append_logon_response(
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    typed_request: &TypedRopRequest,
    principal: &AccountPrincipal,
    request_id: &str,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
    output_handles: &mut Vec<u32>,
) {
    if let TypedRopRequest::Logon(logon_request) = typed_request {
        log_rop_logon_request_identity(principal, request_id, logon_request);
    }
    let logon_context = allocate_logon_response_context(session, handle_slots, principal, request);
    if logon_context.is_private_logon {
        responses.extend_from_slice(&rop_logon_response_body(principal, request));
        log_default_folder_discovery_contract(
            principal,
            request_id,
            "private_logon_response",
            "0xfe",
            mailboxes,
            emails,
            snapshot,
        );
    } else {
        responses.extend_from_slice(&rop_public_folder_logon_response_body(principal, request));
    }
    log_outlook_bootstrap_phase(
        principal,
        "logon_default_folder_ids_returned",
        "0xfe",
        None,
        false,
        None,
        None,
        Some(logon_context.handle),
        &logon_context.special_folder_ids,
    );
    output_handles.push(logon_context.handle);
}

pub(super) fn private_logon_request_handle(
    session: &MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
) -> bool {
    let object = input_object(session, handle_slots, request);
    matches!(object, Some(MapiObject::Logon))
        || (object.is_none()
            && request.input_handle_index() == Some(0)
            && input_handle(handle_slots, request).is_some())
}

pub(super) fn logon_request_handle(
    session: &MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
) -> bool {
    private_logon_request_handle(session, handle_slots, request)
        || matches!(
            input_object(session, handle_slots, request),
            Some(MapiObject::PublicFolderLogon)
        )
}

pub(super) fn address_types_response(request: &RopRequest, has_input_object: bool) -> Vec<u8> {
    if has_input_object {
        rop_get_address_types_response(request)
    } else {
        rop_error_response(0x49, request.response_handle_index(), 0x8004_0102)
    }
}

pub(super) fn append_address_types_response(
    principal: &AccountPrincipal,
    session: &MapiSession,
    object: Option<&MapiObject>,
    request: &RopRequest,
    responses: &mut Vec<u8>,
) {
    if object.is_none() {
        responses.extend_from_slice(&address_types_response(request, false));
        return;
    }
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = "0x49",
        input_handle_index = request.input_handle_index().unwrap_or(0),
        response_handle_index = request.response_handle_index(),
        object_kind = mapi_object_debug_kind(object),
        address_type_count = 2,
        address_types = "EX,SMTP",
        inbox_associated_query_rows_returned_non_empty =
            session.post_hierarchy_actions.inbox_associated_query_rows_returned_non_empty,
        inbox_normal_contents_table_observed =
            session.post_hierarchy_actions.inbox_normal_contents_table_observed,
        last_inbox_associated_query = %debug_context_or_none(
            &session.post_hierarchy_actions.last_inbox_associated_query_context
        ),
        message = "rca debug mapi get address types",
    );
    responses.extend_from_slice(&address_types_response(request, true));
}

pub(super) fn append_address_types_dispatch_response(
    principal: &AccountPrincipal,
    session: &MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    responses: &mut Vec<u8>,
) -> bool {
    append_address_types_response(
        principal,
        session,
        input_object(session, handle_slots, request),
        request,
        responses,
    );
    true
}

pub(super) fn store_state_response(request: &RopRequest, has_input_handle: bool) -> Vec<u8> {
    if has_input_handle {
        rop_get_store_state_response(request)
    } else {
        rop_error_response(0x7B, request.response_handle_index(), 0x8004_0102)
    }
}

pub(super) fn append_store_state_response(
    handle_slots: &[u32],
    request: &RopRequest,
    responses: &mut Vec<u8>,
) {
    let has_input_handle = input_handle(handle_slots, request).is_some();
    responses.extend_from_slice(&store_state_response(request, has_input_handle));
}
