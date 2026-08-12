use super::*;

pub(super) struct PropertyDispatchFlow {
    pub(super) stop_batch: bool,
    pub(super) echo_input_handle_table: bool,
}

impl PropertyDispatchFlow {
    fn continue_batch() -> Self {
        Self {
            stop_batch: false,
            echo_input_handle_table: false,
        }
    }

    fn echo_input_handle_table() -> Self {
        Self {
            stop_batch: false,
            echo_input_handle_table: true,
        }
    }

    fn stop_with_echo_input_handle_table() -> Self {
        Self {
            stop_batch: true,
            echo_input_handle_table: true,
        }
    }
}

pub(super) fn is_property_dispatch_rop(rop_id: RopId) -> bool {
    matches!(
        rop_id,
        RopId::GetPropertiesSpecific
            | RopId::GetPropertiesAll
            | RopId::GetPropertiesList
            | RopId::SetProperties
            | RopId::SetPropertiesNoReplicate
            | RopId::DeleteProperties
            | RopId::DeletePropertiesNoReplicate
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn append_property_dispatch_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    request_id: &str,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    created_emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    response_size_limit: usize,
    responses: &mut Vec<u8>,
) -> PropertyDispatchFlow
where
    S: ExchangeStore,
{
    let rop_id = RopId::from_u8(request.rop_id);
    if input_object(session, handle_slots, request)
        .is_some_and(|object| !object_supports_property_reads(object))
    {
        responses.extend_from_slice(&rop_error_response(
            request.rop_id,
            request.response_handle_index(),
            MapiError::NotSupported.as_u32(),
        ));
        return match rop_id {
            Some(
                RopId::GetPropertiesSpecific
                | RopId::SetProperties
                | RopId::SetPropertiesNoReplicate,
            ) => PropertyDispatchFlow::echo_input_handle_table(),
            _ => PropertyDispatchFlow::continue_batch(),
        };
    }

    match rop_id {
        Some(RopId::GetPropertiesSpecific) => {
            append_get_properties_specific_response(
                store,
                principal,
                session,
                handle_slots,
                request,
                request_id,
                mailboxes,
                emails,
                created_emails,
                snapshot,
                response_size_limit,
                responses,
            )
            .await;
            PropertyDispatchFlow::echo_input_handle_table()
        }
        Some(RopId::GetPropertiesAll) => {
            append_get_properties_all_response(
                store,
                principal,
                session,
                handle_slots,
                request,
                mailboxes,
                emails,
                snapshot,
                responses,
            )
            .await;
            PropertyDispatchFlow::continue_batch()
        }
        Some(RopId::GetPropertiesList) => {
            append_get_properties_list_response(
                session,
                handle_slots,
                request,
                snapshot,
                responses,
            );
            PropertyDispatchFlow::continue_batch()
        }
        Some(RopId::SetProperties | RopId::SetPropertiesNoReplicate) => {
            let flow = append_set_properties_response(
                store,
                principal,
                session,
                handle_slots,
                request,
                request_id,
                mailboxes,
                emails,
                snapshot,
                responses,
            )
            .await;
            if matches!(flow, PropertyMutationFlow::StopBatch) {
                PropertyDispatchFlow::stop_with_echo_input_handle_table()
            } else {
                PropertyDispatchFlow::echo_input_handle_table()
            }
        }
        Some(RopId::DeleteProperties | RopId::DeletePropertiesNoReplicate) => {
            append_delete_properties_response(
                store,
                principal,
                session,
                handle_slots,
                request,
                mailboxes,
                emails,
                snapshot,
                responses,
            )
            .await;
            PropertyDispatchFlow::continue_batch()
        }
        _ => PropertyDispatchFlow::continue_batch(),
    }
}
