use super::*;

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
        message = "rca debug mapi get address types",
    );
    responses.extend_from_slice(&address_types_response(request, true));
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
