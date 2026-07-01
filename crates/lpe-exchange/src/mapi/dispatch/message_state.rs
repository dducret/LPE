use super::*;

pub(super) fn is_message_state_rop(rop_id: RopId) -> bool {
    matches!(
        rop_id,
        RopId::ReloadCachedInformation | RopId::SetMessageReadFlag | RopId::SetReadFlags
    )
}

pub(super) async fn append_message_state_dispatch_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) where
    S: ExchangeStore,
{
    match RopId::from_u8(request.rop_id) {
        Some(RopId::ReloadCachedInformation) => {
            append_reload_cached_information_response(
                session,
                handle_slots,
                request,
                mailboxes,
                emails,
                snapshot,
                responses,
            );
        }
        Some(RopId::SetMessageReadFlag) => {
            append_set_message_read_flag_response(
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
        Some(RopId::SetReadFlags) => {
            append_set_read_flags_response(
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
