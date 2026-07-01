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
    responses: &mut Vec<u8>,
) -> PropertyDispatchFlow
where
    S: ExchangeStore,
{
    match RopId::from_u8(request.rop_id) {
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
                responses,
            )
            .await;
            PropertyDispatchFlow::echo_input_handle_table()
        }
        Some(RopId::GetPropertiesAll) => {
            append_get_properties_all_response(
                principal,
                session,
                handle_slots,
                request,
                mailboxes,
                emails,
                snapshot,
                responses,
            );
            PropertyDispatchFlow::continue_batch()
        }
        Some(RopId::GetPropertiesList) => {
            append_get_properties_list_response(session, handle_slots, request, responses);
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
