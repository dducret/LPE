use super::*;

pub(super) fn is_sync_transfer_rop(rop_id: RopId) -> bool {
    matches!(
        rop_id,
        RopId::SynchronizationConfigure
            | RopId::FastTransferSourceCopyMessages
            | RopId::FastTransferDestinationConfigure
            | RopId::FastTransferDestinationPutBuffer
            | RopId::FastTransferDestinationPutBufferExtended
            | RopId::FastTransferSourceCopyFolder
            | RopId::FastTransferSourceCopyTo
            | RopId::FastTransferSourceCopyProperties
            | RopId::FastTransferSourceGetBuffer
            | RopId::TellVersion
            | RopId::SynchronizationUploadStateStreamBegin
            | RopId::SynchronizationUploadStateStreamContinue
            | RopId::SynchronizationUploadStateStreamEnd
            | RopId::SynchronizationOpenCollector
            | RopId::SynchronizationGetTransferState
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn append_sync_transfer_dispatch_response<S>(
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
    completed_hierarchy_sync: &mut Option<(u64, String, String)>,
    content_sync_configure_observed: &mut bool,
) -> bool
where
    S: ExchangeStore,
{
    match RopId::from_u8(request.rop_id) {
        Some(RopId::SynchronizationConfigure) => {
            matches!(
                append_synchronization_configure_response(
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
                    content_sync_configure_observed,
                )
                .await,
                SyncConfigureFlow::StopBatch
            )
        }
        Some(RopId::FastTransferSourceCopyMessages) => {
            append_fast_transfer_source_copy_messages_response(
                session,
                handle_slots,
                request,
                mailboxes,
                emails,
                snapshot,
                responses,
                output_handles,
            );
            false
        }
        Some(RopId::FastTransferDestinationConfigure) => {
            append_fast_transfer_destination_configure_response(
                session,
                handle_slots,
                request,
                responses,
                output_handles,
            );
            false
        }
        Some(RopId::FastTransferDestinationPutBuffer)
        | Some(RopId::FastTransferDestinationPutBufferExtended) => {
            append_fast_transfer_destination_put_buffer_response(
                session,
                handle_slots,
                request,
                responses,
            )
        }
        Some(RopId::FastTransferSourceCopyFolder)
        | Some(RopId::FastTransferSourceCopyTo)
        | Some(RopId::FastTransferSourceCopyProperties) => {
            append_fast_transfer_source_copy_response(
                session,
                handle_slots,
                request,
                principal.account_id,
                mailboxes,
                emails,
                snapshot,
                responses,
                output_handles,
            );
            false
        }
        Some(RopId::FastTransferSourceGetBuffer) => {
            *completed_hierarchy_sync = append_fast_transfer_source_get_buffer_response(
                store,
                principal,
                request_id,
                session,
                handle_slots,
                request,
                responses,
            )
            .await;
            false
        }
        Some(RopId::TellVersion) => {
            append_tell_version_response(session, handle_slots, request, responses);
            false
        }
        Some(RopId::SynchronizationUploadStateStreamBegin) => {
            append_upload_state_stream_begin_response(
                session,
                handle_slots,
                request,
                &principal.email,
                request_id,
                responses,
            );
            false
        }
        Some(RopId::SynchronizationUploadStateStreamContinue) => {
            append_upload_state_stream_continue_response(
                session,
                handle_slots,
                request,
                &principal.email,
                request_id,
                responses,
            );
            false
        }
        Some(RopId::SynchronizationUploadStateStreamEnd) => {
            append_upload_state_stream_end_response(
                session,
                handle_slots,
                request,
                &principal.email,
                request_id,
                responses,
            );
            false
        }
        Some(RopId::SynchronizationOpenCollector) => {
            append_synchronization_open_collector_response(
                session,
                handle_slots,
                request,
                mailboxes,
                responses,
                output_handles,
            );
            false
        }
        Some(RopId::SynchronizationGetTransferState) => {
            append_synchronization_get_transfer_state_response(
                session,
                handle_slots,
                request,
                mailboxes,
                emails,
                snapshot,
                responses,
                output_handles,
            );
            false
        }
        _ => false,
    }
}
