use super::*;

pub(super) fn is_message_dispatch_rop(rop_id: RopId) -> bool {
    matches!(
        rop_id,
        RopId::OpenMessage
            | RopId::CreateMessage
            | RopId::SaveChangesMessage
            | RopId::DeleteMessages
            | RopId::HardDeleteMessages
            | RopId::GetMessageStatus
            | RopId::SetMessageStatus
            | RopId::MoveCopyMessages
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn append_message_dispatch_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    request_id: &str,
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
    output_handles: &mut Vec<u32>,
    created_emails: &mut Vec<JmapEmail>,
) where
    S: ExchangeStore,
{
    match RopId::from_u8(request.rop_id) {
        Some(RopId::OpenMessage) => {
            append_open_message_response(
                principal,
                request_id,
                session,
                handle_slots,
                request,
                mailboxes,
                emails,
                snapshot,
                responses,
                output_handles,
            );
        }
        Some(RopId::CreateMessage) => {
            append_create_message_response(
                principal,
                session,
                handle_slots,
                request,
                mailboxes,
                snapshot,
                responses,
                output_handles,
            );
        }
        Some(RopId::SaveChangesMessage) => {
            append_save_changes_message_route_response(
                store,
                principal,
                session,
                handle_slots,
                request,
                mailboxes,
                emails,
                snapshot,
                responses,
                created_emails,
            )
            .await;
        }
        Some(RopId::DeleteMessages | RopId::HardDeleteMessages) => {
            append_delete_messages_response(
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
        }
        Some(RopId::GetMessageStatus | RopId::SetMessageStatus) => {
            append_message_status_response(
                session,
                handle_slots,
                request,
                mailboxes,
                emails,
                snapshot,
                responses,
            );
        }
        Some(RopId::MoveCopyMessages) => {
            append_move_copy_messages_response(
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
        }
        _ => {}
    }
}
