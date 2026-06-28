use super::*;

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
