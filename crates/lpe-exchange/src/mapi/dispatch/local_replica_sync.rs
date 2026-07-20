use super::*;
use crate::mapi::wire::MapiError;
use crate::store::MAX_MAPI_LOCAL_REPLICA_ID_COUNT;

pub(super) async fn append_set_local_replica_midset_deleted_response<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    session: &MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    responses: &mut Vec<u8>,
) {
    // [MS-OXCFXICS] section 2.2.3.2.4.8.1 and [MS-OXCROPS] sections
    // 2.2.13.12.1, 2.2.13.12.1.1, and 2.2.13.12.2 require a Folder
    // InputServerObject and define the LongTermIdRange and response shapes.
    let Some(input_handle) = input_handle(handle_slots, request) else {
        responses.extend_from_slice(&rop_error_response(
            request.rop_id,
            request.response_handle_index(),
            MapiError::NullObject.as_u32(),
        ));
        return;
    };
    let Some(input_object) = session.handles.get(&input_handle) else {
        responses.extend_from_slice(&rop_error_response(
            request.rop_id,
            request.response_handle_index(),
            MapiError::NullObject.as_u32(),
        ));
        return;
    };
    let MapiObject::Folder { folder_id, .. } = input_object else {
        responses.extend_from_slice(&rop_error_response(
            request.rop_id,
            request.response_handle_index(),
            ROP_ERROR_NOT_SUPPORTED,
        ));
        return;
    };
    let Some(ranges) = request.local_replica_deleted_ranges() else {
        responses.extend_from_slice(&rop_error_response(
            request.rop_id,
            request.response_handle_index(),
            MapiError::InvalidParameter.as_u32(),
        ));
        return;
    };

    match store
        .add_mapi_local_replica_deleted_ranges(principal.account_id, *folder_id, &ranges)
        .await
    {
        Ok(()) => responses.extend_from_slice(&rop_simple_success_response(request)),
        Err(error) => {
            tracing::warn!(
                adapter = "mapi",
                endpoint = "emsmdb",
                account_id = %principal.account_id,
                folder_id = format_args!("0x{folder_id:016x}"),
                range_count = ranges.len(),
                error = %error,
                "rejected MAPI local-replica deleted-item ranges"
            );
            responses.extend_from_slice(&rop_error_response(
                request.rop_id,
                request.response_handle_index(),
                MapiError::InvalidParameter.as_u32(),
            ));
        }
    }
}

pub(super) async fn append_get_local_replica_ids_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    session: &MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    responses: &mut Vec<u8>,
) where
    S: ExchangeStore,
{
    // [MS-OXCROPS] section 3.2.5.4 requires ecNullObject for an unassigned
    // handle. [MS-OXCFXICS] section 2.2.3.2.4.7.1 then requires the assigned
    // InputServerObject to be a Logon object.
    let Some(input_handle) = input_handle(handle_slots, request) else {
        responses.extend_from_slice(&rop_error_response(
            request.rop_id,
            request.response_handle_index(),
            MapiError::NullObject.as_u32(),
        ));
        return;
    };
    let Some(input_object) = session.handles.get(&input_handle) else {
        responses.extend_from_slice(&rop_error_response(
            request.rop_id,
            request.response_handle_index(),
            MapiError::NullObject.as_u32(),
        ));
        return;
    };
    if !matches!(input_object, MapiObject::Logon) {
        responses.extend_from_slice(&rop_error_response(
            request.rop_id,
            request.response_handle_index(),
            ROP_ERROR_NOT_SUPPORTED,
        ));
        return;
    }

    let id_count = request.local_replica_id_count();
    if !(1..=MAX_MAPI_LOCAL_REPLICA_ID_COUNT).contains(&id_count) {
        responses.extend_from_slice(&rop_error_response(
            request.rop_id,
            request.response_handle_index(),
            MapiError::InvalidParameter.as_u32(),
        ));
        return;
    }

    match store
        .reserve_mapi_local_replica_ids(principal.account_id, id_count)
        .await
    {
        Ok(first_global_counter) => responses.extend_from_slice(
            &rop_get_local_replica_ids_response(request, first_global_counter),
        ),
        Err(error) => {
            tracing::error!(
                adapter = "mapi",
                endpoint = "emsmdb",
                mailbox = %principal.email,
                account_id = %principal.account_id,
                id_count,
                error = %error,
                "failed to reserve MAPI local replica ID range"
            );
            responses.extend_from_slice(&rop_error_response(
                request.rop_id,
                request.response_handle_index(),
                MapiError::GeneralFailure.as_u32(),
            ));
        }
    }
}

pub(super) async fn append_local_replica_dispatch_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    responses: &mut Vec<u8>,
) -> bool
where
    S: ExchangeStore,
{
    match RopId::from_u8(request.rop_id) {
        Some(RopId::SetLocalReplicaMidsetDeleted) => {
            append_set_local_replica_midset_deleted_response(
                store,
                principal,
                session,
                handle_slots,
                request,
                responses,
            )
            .await;
            false
        }
        Some(RopId::GetLocalReplicaIds) => {
            append_get_local_replica_ids_response(
                store,
                principal,
                session,
                handle_slots,
                request,
                responses,
            )
            .await;
            true
        }
        _ => false,
    }
}
