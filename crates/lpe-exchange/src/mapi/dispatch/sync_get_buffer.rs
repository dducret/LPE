use super::*;

const FAST_TRANSFER_SOURCE_GET_BUFFER_RESULT_OVERHEAD: usize = 15;
pub(super) const PACKED_FAST_TRANSFER_RESPONSE_FRAME_MAXIMUM: u32 = 32 * 1024;
const PACKED_FAST_TRANSFER_MINIMUM_REMAINING_RESPONSE_BYTES: usize = 8 * 1024;
const PACKED_FAST_TRANSFER_MAXIMUM_RESPONSE_FRAMES: usize = 96;

pub(super) fn fast_transfer_source_get_buffer_transfer_size(
    request: &RopRequest,
    residual_rop_out_size: usize,
) -> usize {
    let requested = request.fast_transfer_buffer_size();
    if request.fast_transfer_uses_server_determined_buffer_size() {
        // [MS-OXCFXICS] section 3.2.5.8.1.5: BufferSize 0xBABE selects the
        // smaller of MaximumBufferSize and the residual output buffer after
        // the RopFastTransferSourceGetBuffer response structure.
        requested.min(
            residual_rop_out_size.saturating_sub(FAST_TRANSFER_SOURCE_GET_BUFFER_RESULT_OVERHEAD),
        )
    } else {
        requested.clamp(1, u16::MAX as usize)
    }
}

pub(super) fn fast_transfer_source_get_buffer_response_is_partial(response: &[u8]) -> bool {
    response.len() >= 8
        && response[0] == RopId::FastTransferSourceGetBuffer.as_u8()
        && response[2..6] == [0, 0, 0, 0]
        && u16::from_le_bytes([response[6], response[7]]) == 0x0001
}

fn fast_transfer_source_transfer_position(
    session: &MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
) -> Option<usize> {
    match input_object(session, handle_slots, request) {
        Some(MapiObject::SynchronizationSource {
            transfer_position, ..
        }) => Some(*transfer_position),
        _ => None,
    }
}

pub(super) async fn packed_fast_transfer_source_get_buffer_response_payloads<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    request_id: &str,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    initial_rop_buffer_size: usize,
    max_rop_out: u32,
    response_handles: &[u32],
) -> (Vec<Vec<u8>>, Option<(u64, String, String)>) {
    let mut response_payloads = Vec::new();
    let mut response_rop_buffer_size = initial_rop_buffer_size;
    let mut completed_hierarchy_sync = None;

    // [MS-OXCRPC] section 3.1.4.2.1.2.2 permits synthetic GetBuffer
    // responses after a chained terminal GetBuffer request. Each has its own
    // RPC_HEADER_EXT and complete response-handle table.
    for _ in 1..PACKED_FAST_TRANSFER_MAXIMUM_RESPONSE_FRAMES {
        let Some(transfer_position_before) =
            fast_transfer_source_transfer_position(session, handle_slots, request)
        else {
            break;
        };
        let total_response_residual = if max_rop_out == 0 {
            usize::MAX
        } else {
            (max_rop_out as usize).saturating_sub(response_rop_buffer_size)
        };
        if total_response_residual < PACKED_FAST_TRANSFER_MINIMUM_REMAINING_RESPONSE_BYTES {
            break;
        }
        let frame_size_limit =
            total_response_residual.min(PACKED_FAST_TRANSFER_RESPONSE_FRAME_MAXIMUM as usize);
        let residual_rop_out_size = frame_size_limit
            .saturating_sub(8)
            .saturating_sub(2)
            .saturating_sub(response_handles.len().saturating_mul(4));
        if residual_rop_out_size <= FAST_TRANSFER_SOURCE_GET_BUFFER_RESULT_OVERHEAD {
            break;
        }

        let mut response = Vec::new();
        let completed = append_fast_transfer_source_get_buffer_response(
            store,
            principal,
            request_id,
            session,
            handle_slots,
            request,
            residual_rop_out_size,
            &mut response,
        )
        .await;
        if response.is_empty() {
            break;
        }
        let response_is_partial = fast_transfer_source_get_buffer_response_is_partial(&response);
        let response_payload = rop_buffer_with_response_spec(response, response_handles);
        let response_frame_size = 8 + response_payload.len();
        debug_assert!(response_frame_size <= frame_size_limit);
        response_rop_buffer_size += response_frame_size;
        response_payloads.push(response_payload);
        if completed.is_some() {
            completed_hierarchy_sync = completed;
        }
        let transfer_progressed =
            fast_transfer_source_transfer_position(session, handle_slots, request).is_some_and(
                |transfer_position_after| transfer_position_after > transfer_position_before,
            );
        if !response_is_partial || !transfer_progressed {
            break;
        }
    }

    (response_payloads, completed_hierarchy_sync)
}

pub(super) async fn append_fast_transfer_source_get_buffer_response<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    request_id: &str,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    residual_rop_out_size: usize,
    responses: &mut Vec<u8>,
) -> Option<(u64, String, String)> {
    let mut completed_hierarchy_sync = None;
    match input_object_mut(session, &handle_slots, &request) {
        Some(MapiObject::SynchronizationSource {
            folder_id,
            mailbox_id,
            checkpoint_kind,
            checkpoint_change_sequence,
            checkpoint_modseq,
            checkpoint_store_allowed,
            checkpoint_skip_reason,
            checkpoint_zero_delta,
            sync_type,
            sync_flags,
            transfer_state_source,
            initial_state,
            state,
            state_upload_buffer,
            client_state_uploaded_bytes,
            client_state_uploaded_marker_mask,
            client_state_selection_enabled,
            client_state_selection_invalidated,
            client_state_selection_applied,
            download_change_facts,
            resident_hierarchy_alias_counters,
            incremental_transfer_buffer,
            transfer_buffer,
            transfer_position,
            ..
        }) => {
            if *client_state_selection_invalidated {
                // [MS-OXCFXICS] sections 3.1.5.4.3.2 and 3.1.5.4.3.2.4:
                // a malformed GLOBSET is rejected with RpcFormat. It cannot
                // subsequently serve as the ICS-state basis described in
                // section 3.2.5.2 for this configured download context.
                responses.extend_from_slice(&rop_error_response(
                    0x4E,
                    request.response_handle_index(),
                    0x0000_04B6,
                ));
                return None;
            }
            let checkpoint_delta_available_before_client_selection =
                incremental_transfer_buffer.is_some();
            if *client_state_selection_enabled
                && !*client_state_selection_applied
                && matches!(*sync_type, 0x01 | 0x02)
            {
                // [MS-OXCFXICS] sections 3.2.5.2 and 3.2.5.3: the uploaded
                // client state, including valid zero-length streams, is the
                // sole semantic input to the download delta. Checkpoints are
                // retained only as completion telemetry.
                match mapi_mailstore::select_download_manifest_for_client_state(
                    *sync_type,
                    *sync_flags,
                    transfer_buffer,
                    initial_state,
                    download_change_facts,
                    resident_hierarchy_alias_counters,
                ) {
                    Ok((selected, selected_final_state)) => {
                        *transfer_buffer = selected;
                        *state = selected_final_state;
                        *transfer_position = 0;
                        *client_state_selection_applied = true;
                        incremental_transfer_buffer.take();
                    }
                    Err(error) => {
                        tracing::error!(
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            mailbox = %principal.email,
                            mapi_request_id = request_id,
                            request_rop_id = "0x4e",
                            folder_id = format_args!("0x{:016x}", *folder_id),
                            sync_type = format_args!("0x{:02x}", *sync_type),
                            error = %error,
                            "cannot select ICS download from uploaded client state"
                        );
                        responses.extend_from_slice(&rop_error_response(
                            0x4E,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        return None;
                    }
                }
            }
            let requested_buffer_bytes =
                fast_transfer_source_get_buffer_transfer_size(request, residual_rop_out_size);
            let previous_transfer_position = *transfer_position;
            let empty_content_sync_state_only =
                *sync_type == 0x01 && transfer_buffer.len() == state.len().saturating_add(4);
            let upload_state_has_delta_anchor =
                uploaded_state_has_delta_anchor(*client_state_uploaded_marker_mask);
            let checkpoint_delta_available_before_get_buffer =
                incremental_transfer_buffer.is_some();
            let active_transfer_selection = if *client_state_selection_applied {
                "uploaded_client_state_delta"
            } else {
                "full_without_uploaded_client_state"
            };
            let response = rop_fast_transfer_source_get_buffer_response(
                &request,
                transfer_buffer,
                transfer_position,
                requested_buffer_bytes,
                *transfer_state_source,
            );
            let completed = *transfer_position >= transfer_buffer.len();
            let response_debug = summarize_fast_transfer_get_buffer_response(&response, completed);
            if completed && *sync_type == 0x02 {
                let hierarchy_close_summary = mapi_mailstore::hierarchy_transfer_close_summary(
                    *sync_type,
                    *folder_id,
                    transfer_buffer,
                );
                let default_folder_hierarchy_membership_summary =
                    mapi_mailstore::default_folder_hierarchy_membership_summary(
                        *sync_type,
                        *folder_id,
                        transfer_buffer,
                    );
                completed_hierarchy_sync = Some((
                    *folder_id,
                    format!(
                        "folder=0x{:016x};checkpoint_kind={};checkpoint_mailbox={};seq={};modseq={};state={};state_summary={};upload_buffer={};client_state={};upload_delta_anchor={};incremental={};checkpoint_candidate_before_selection={};selection={};requested={};response={};payload={};status={};completed={};position={}/{};{}",
                        *folder_id,
                        checkpoint_kind.as_str(),
                        (*mailbox_id).map(|id| id.to_string()).unwrap_or_default(),
                        *checkpoint_change_sequence,
                        *checkpoint_modseq,
                        state.len(),
                        mapi_mailstore::final_sync_state_debug_summary(state),
                        state_upload_buffer.len(),
                        *client_state_uploaded_bytes,
                        upload_state_has_delta_anchor,
                        incremental_transfer_buffer.is_some(),
                        checkpoint_delta_available_before_client_selection,
                        active_transfer_selection,
                        requested_buffer_bytes,
                        response.len(),
                        response_debug.transfer_payload_bytes,
                        response_debug.transfer_status,
                        completed,
                        *transfer_position,
                        transfer_buffer.len(),
                        hierarchy_close_summary
                    ),
                    default_folder_hierarchy_membership_summary,
                ));
            }
            tracing::info!(
                rca_debug = true,
                adapter = "mapi",
                endpoint = "emsmdb",
                mailbox = %principal.email,
                request_type = "Execute",
                mapi_request_id = request_id,
                request_rop_id = "0x4e",
                folder_id = format_args!("0x{:016x}", *folder_id),
                folder_role = debug_role_for_folder_id(*folder_id),
                folder_container_class = debug_container_class_for_folder_id(*folder_id),
                sync_type = format_args!("0x{:02x}", *sync_type),
                checkpoint_kind = checkpoint_kind.as_str(),
                checkpoint_zero_delta = *checkpoint_zero_delta,
                checkpoint_mailbox_id = (*mailbox_id)
                    .map(|id| id.to_string())
                    .unwrap_or_default(),
                checkpoint_change_sequence = *checkpoint_change_sequence,
                checkpoint_modseq = *checkpoint_modseq,
                sync_state_bytes = state.len(),
                sync_state_summary =
                    %mapi_mailstore::final_sync_state_debug_summary(state),
                upload_state_buffer_bytes = state_upload_buffer.len(),
                upload_state_client_bytes = *client_state_uploaded_bytes,
                upload_state_marker_mask =
                    format_args!("0x{:02x}", *client_state_uploaded_marker_mask),
                upload_state_markers =
                    %uploaded_state_marker_summary(*client_state_uploaded_marker_mask),
                upload_state_has_delta_anchor =
                    upload_state_has_delta_anchor,
                incremental_transfer_available = incremental_transfer_buffer.is_some(),
                incremental_transfer_buffer_bytes = incremental_transfer_buffer
                    .as_ref()
                    .map(|buffer| buffer.len())
                    .unwrap_or_default(),
                checkpoint_delta_selection_gate = "disabled_client_state_is_authoritative",
                checkpoint_delta_available_before_client_selection,
                checkpoint_delta_available_before_get_buffer,
                active_transfer_selection,
                requested_buffer_bytes,
                transfer_position_before = previous_transfer_position,
                transfer_position_after = *transfer_position,
                transfer_buffer_bytes = transfer_buffer.len(),
                empty_content_sync_state_only,
                outlook_no_current_item_candidate = empty_content_sync_state_only,
                transfer_chunk_bytes =
                    (*transfer_position).saturating_sub(previous_transfer_position),
                transfer_completed = completed,
                transfer_status = if completed { "0x0003" } else { "0x0001" },
                get_buffer_response_bytes = response.len(),
                get_buffer_response_header_bytes = response_debug.header_bytes,
                get_buffer_response_rop_id = %response_debug.rop_id,
                get_buffer_response_rop_id_matches = response_debug.rop_id_matches,
                get_buffer_response_handle_index = response_debug.handle_index,
                get_buffer_return_value = %response_debug.return_value,
                get_buffer_transfer_status_wire = %response_debug.transfer_status,
                get_buffer_transfer_status_matches_completed =
                    response_debug.transfer_status_matches_completed,
                get_buffer_in_progress_count = response_debug.in_progress_count,
                get_buffer_total_step_count = response_debug.total_step_count,
                get_buffer_reserved_byte = response_debug.reserved_byte,
                get_buffer_reserved_zero = response_debug.reserved_zero,
                get_buffer_transfer_buffer_size_wire =
                    response_debug.transfer_buffer_size,
                get_buffer_transfer_payload_bytes = response_debug.transfer_payload_bytes,
                get_buffer_transfer_buffer_size_matches_payload =
                    response_debug.transfer_buffer_size_matches_payload,
                get_buffer_transfer_payload_preview_hex =
                    %response_debug.transfer_payload_preview_hex,
                get_buffer_transfer_payload_tail_hex =
                    %response_debug.transfer_payload_tail_hex,
                get_buffer_response_parse_error = %response_debug.parse_error,
                "rca debug mapi fast transfer get buffer"
            );
            mapi_mailstore::log_hierarchy_get_buffer_payload_summary(
                *sync_type,
                *folder_id,
                if completed { "0x0003" } else { "0x0001" },
                transfer_buffer,
            );
            let checkpoint = (
                *mailbox_id,
                *checkpoint_kind,
                *checkpoint_change_sequence,
                *checkpoint_modseq,
                *sync_type,
                *folder_id,
            );
            responses.extend_from_slice(&response);
            if completed && matches!(checkpoint.4, 0x01 | 0x02) {
                let mut cursor_json = serde_json::json!({
                    "syncType": checkpoint.4,
                    "syncRootFolderId": checkpoint.5,
                    "source": "emsmdb-ics-download"
                });
                if checkpoint.1 == MapiCheckpointKind::Hierarchy {
                    cursor_json["hierarchySyncVersion"] =
                        serde_json::json!(HIERARCHY_SYNC_CURSOR_VERSION);
                }
                if checkpoint.1 != MapiCheckpointKind::Hierarchy && checkpoint.0.is_none() {
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %principal.email,
                        request_type = "Execute",
                        mapi_request_id = request_id,
                        request_rop_id = "0x4e",
                        folder_id = format_args!("0x{:016x}", *folder_id),
                        folder_role = debug_role_for_folder_id(*folder_id),
                        folder_container_class =
                            debug_container_class_for_folder_id(*folder_id),
                        sync_type = format_args!("0x{:02x}", checkpoint.4),
                        checkpoint_kind = checkpoint.1.as_str(),
                        checkpoint_mailbox_id = "",
                        checkpoint_change_sequence = checkpoint.2,
                        checkpoint_modseq = checkpoint.3,
                        sync_state_bytes = state.len(),
                        upload_state_buffer_bytes = state_upload_buffer.len(),
                        upload_state_client_bytes = *client_state_uploaded_bytes,
                        incremental_transfer_available = incremental_transfer_buffer.is_some(),
                        transfer_buffer_bytes = transfer_buffer.len(),
                        transfer_position = *transfer_position,
                        checkpoint_store_status = "skipped_no_mailbox_id",
                        checkpoint_skip_reason =
                            "content_or_read_state_sync_without_canonical_mailbox_id",
                        "rca debug mapi sync checkpoint store"
                    );
                    session.record_completed_sync_checkpoint(
                        checkpoint.5,
                        debug_role_for_folder_id(checkpoint.5),
                        debug_container_class_for_folder_id(checkpoint.5),
                        checkpoint.1.as_str(),
                        checkpoint.4,
                        "skipped_no_mailbox_id",
                    );
                } else if !*checkpoint_store_allowed {
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %principal.email,
                        request_type = "Execute",
                        mapi_request_id = request_id,
                        request_rop_id = "0x4e",
                        folder_id = format_args!("0x{:016x}", *folder_id),
                        folder_role = debug_role_for_folder_id(*folder_id),
                        folder_container_class =
                            debug_container_class_for_folder_id(*folder_id),
                        sync_type = format_args!("0x{:02x}", checkpoint.4),
                        checkpoint_kind = checkpoint.1.as_str(),
                        checkpoint_mailbox_id = checkpoint
                            .0
                            .map(|id| id.to_string())
                            .unwrap_or_default(),
                        checkpoint_change_sequence = checkpoint.2,
                        checkpoint_modseq = checkpoint.3,
                        sync_state_bytes = state.len(),
                        upload_state_buffer_bytes = state_upload_buffer.len(),
                        upload_state_client_bytes = *client_state_uploaded_bytes,
                        incremental_transfer_available = incremental_transfer_buffer.is_some(),
                        transfer_buffer_bytes = transfer_buffer.len(),
                        transfer_position = *transfer_position,
                        checkpoint_store_status = "not_stored_partial_scope",
                        checkpoint_skip_reason = *checkpoint_skip_reason,
                        "rca debug mapi sync checkpoint store"
                    );
                    session.record_completed_sync_checkpoint(
                        checkpoint.5,
                        debug_role_for_folder_id(checkpoint.5),
                        debug_container_class_for_folder_id(checkpoint.5),
                        checkpoint.1.as_str(),
                        checkpoint.4,
                        "ok_partial_scope_no_checkpoint",
                    );
                } else {
                    let checkpoint_result = store
                        .store_mapi_sync_checkpoint(
                            principal.account_id,
                            checkpoint.0,
                            checkpoint.1,
                            checkpoint.2,
                            checkpoint.3,
                            cursor_json,
                        )
                        .await;
                    match checkpoint_result {
                        Ok(stored_checkpoint) => {
                            tracing::info!(
                                rca_debug = true,
                                adapter = "mapi",
                                endpoint = "emsmdb",
                                mailbox = %principal.email,
                                request_type = "Execute",
                                mapi_request_id = request_id,
                                request_rop_id = "0x4e",
                                folder_id = format_args!("0x{:016x}", *folder_id),
                                folder_role = debug_role_for_folder_id(*folder_id),
                                folder_container_class =
                                    debug_container_class_for_folder_id(*folder_id),
                                sync_type = format_args!("0x{:02x}", checkpoint.4),
                                checkpoint_kind = checkpoint.1.as_str(),
                                checkpoint_mailbox_id = checkpoint
                                    .0
                                    .map(|id| id.to_string())
                                    .unwrap_or_default(),
                                checkpoint_change_sequence = checkpoint.2,
                                checkpoint_modseq = checkpoint.3,
                                stored_change_sequence = stored_checkpoint.last_change_sequence,
                                stored_modseq = stored_checkpoint.last_modseq,
                                sync_state_bytes = state.len(),
                                upload_state_buffer_bytes = state_upload_buffer.len(),
                                upload_state_client_bytes = *client_state_uploaded_bytes,
                                incremental_transfer_available = incremental_transfer_buffer.is_some(),
                                transfer_buffer_bytes = transfer_buffer.len(),
                                transfer_position = *transfer_position,
                                checkpoint_store_status = "ok",
                                checkpoint_skip_reason = "",
                                "rca debug mapi sync checkpoint store"
                            );
                            session.record_completed_sync_checkpoint(
                                checkpoint.5,
                                debug_role_for_folder_id(checkpoint.5),
                                debug_container_class_for_folder_id(checkpoint.5),
                                checkpoint.1.as_str(),
                                checkpoint.4,
                                "ok",
                            );
                        }
                        Err(error) => {
                            tracing::warn!(
                                rca_debug = true,
                                adapter = "mapi",
                                endpoint = "emsmdb",
                                mailbox = %principal.email,
                                request_type = "Execute",
                                mapi_request_id = request_id,
                                request_rop_id = "0x4e",
                                folder_id = format_args!("0x{:016x}", *folder_id),
                                folder_role = debug_role_for_folder_id(*folder_id),
                                folder_container_class =
                                    debug_container_class_for_folder_id(*folder_id),
                                sync_type = format_args!("0x{:02x}", checkpoint.4),
                                checkpoint_kind = checkpoint.1.as_str(),
                                checkpoint_mailbox_id = checkpoint
                                    .0
                                    .map(|id| id.to_string())
                                    .unwrap_or_default(),
                                checkpoint_change_sequence = checkpoint.2,
                                checkpoint_modseq = checkpoint.3,
                                sync_state_bytes = state.len(),
                                upload_state_buffer_bytes = state_upload_buffer.len(),
                                upload_state_client_bytes = *client_state_uploaded_bytes,
                                incremental_transfer_available = incremental_transfer_buffer.is_some(),
                                transfer_buffer_bytes = transfer_buffer.len(),
                                transfer_position = *transfer_position,
                                checkpoint_store_status = "error",
                                checkpoint_skip_reason = "",
                                error = %error,
                                "rca debug mapi sync checkpoint store"
                            );
                            session.record_completed_sync_checkpoint(
                                checkpoint.5,
                                debug_role_for_folder_id(checkpoint.5),
                                debug_container_class_for_folder_id(checkpoint.5),
                                checkpoint.1.as_str(),
                                checkpoint.4,
                                "error",
                            );
                        }
                    }
                }
            }
        }
        _ => responses.extend_from_slice(&rop_error_response(
            0x4E,
            request.response_handle_index(),
            0x8004_0102,
        )),
    }
    completed_hierarchy_sync
}
