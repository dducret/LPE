use super::*;

pub(super) fn is_stream_dispatch_rop(rop_id: RopId) -> bool {
    matches!(
        rop_id,
        RopId::OpenStream
            | RopId::ReadStream
            | RopId::SeekStream
            | RopId::SetStreamSize
            | RopId::WriteStream
            | RopId::WriteAndCommitStream
            | RopId::WriteStreamExtended
            | RopId::CopyToStream
            | RopId::GetStreamSize
            | RopId::CloneStream
            | RopId::LockRegionStream
            | RopId::UnlockRegionStream
            | RopId::CopyTo
            | RopId::CopyProperties
            | RopId::CommitStream
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn append_stream_dispatch_response<S>(
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
) where
    S: ExchangeStore,
{
    match RopId::from_u8(request.rop_id) {
        Some(
            RopId::OpenStream
            | RopId::ReadStream
            | RopId::SeekStream
            | RopId::SetStreamSize
            | RopId::WriteStream
            | RopId::WriteAndCommitStream
            | RopId::WriteStreamExtended
            | RopId::CopyToStream
            | RopId::GetStreamSize
            | RopId::CloneStream
            | RopId::LockRegionStream
            | RopId::UnlockRegionStream,
        ) => {
            append_stream_response(
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
                output_handles,
            )
            .await;
        }
        Some(RopId::CopyTo) => {
            append_copy_to_response(
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
        Some(RopId::CopyProperties) => {
            append_copy_properties_response(
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
        Some(RopId::CommitStream) => {
            append_commit_stream_response(
                store,
                principal,
                session,
                handle_slots,
                request,
                request_id,
                responses,
            )
            .await;
        }
        _ => {}
    }
}
