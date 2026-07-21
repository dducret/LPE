use super::*;

pub(super) fn append_upload_state_stream_begin_response(
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    mailbox_email: &str,
    request_id: &str,
    responses: &mut Vec<u8>,
) {
    match input_object_mut(session, handle_slots, request) {
        Some(MapiObject::SynchronizationSource {
            folder_id,
            mailbox_id,
            checkpoint_kind,
            sync_type,
            state_upload_property_tag,
            state_upload_buffer,
            ..
        }) => {
            let property_tag = request.upload_state_property_tag().unwrap_or_default();
            let declared_bytes = request.upload_state_transfer_size().unwrap_or_default();
            *state_upload_property_tag = Some(property_tag);
            state_upload_buffer.clear();
            tracing::info!(
                rca_debug = true,
                adapter = "mapi",
                endpoint = "emsmdb",
                mailbox = %mailbox_email,
                request_type = "Execute",
                mapi_request_id = request_id,
                request_rop_id = "0x75",
                sync_context_kind = "source",
                folder_id = format_args!("0x{:016x}", *folder_id),
                folder_role = debug_role_for_folder_id(*folder_id),
                folder_container_class = debug_container_class_for_folder_id(*folder_id),
                sync_type = format_args!("0x{:02x}", *sync_type),
                checkpoint_kind = checkpoint_kind.as_str(),
                checkpoint_mailbox_id = (*mailbox_id)
                    .map(|id| id.to_string())
                    .unwrap_or_default(),
                upload_state_property_tag = format_args!("0x{property_tag:08x}"),
                upload_state_property_name = upload_state_property_name(property_tag),
                upload_state_declared_bytes = declared_bytes,
                upload_state_empty_declared = declared_bytes == 0,
                "rca debug mapi sync upload state begin"
            );
            responses.extend_from_slice(&rop_upload_state_success_response(request));
        }
        Some(MapiObject::SynchronizationCollector {
            folder_id,
            mailbox_id,
            checkpoint_kind,
            sync_type,
            state_upload_property_tag,
            state_upload_buffer,
            client_state_uploaded_bytes,
            client_state_uploaded_marker_mask,
            ..
        }) => {
            let property_tag = request.upload_state_property_tag().unwrap_or_default();
            let declared_bytes = request.upload_state_transfer_size().unwrap_or_default();
            *state_upload_property_tag = Some(property_tag);
            state_upload_buffer.clear();
            tracing::info!(
                rca_debug = true,
                adapter = "mapi",
                endpoint = "emsmdb",
                mailbox = %mailbox_email,
                request_type = "Execute",
                mapi_request_id = request_id,
                request_rop_id = "0x75",
                sync_context_kind = "collector",
                folder_id = format_args!("0x{:016x}", *folder_id),
                folder_role = debug_role_for_folder_id(*folder_id),
                folder_container_class = debug_container_class_for_folder_id(*folder_id),
                sync_type = format_args!("0x{:02x}", *sync_type),
                checkpoint_kind = checkpoint_kind.as_str(),
                checkpoint_mailbox_id = (*mailbox_id)
                    .map(|id| id.to_string())
                    .unwrap_or_default(),
                upload_state_property_tag = format_args!("0x{property_tag:08x}"),
                upload_state_property_name = upload_state_property_name(property_tag),
                upload_state_declared_bytes = declared_bytes,
                upload_state_empty_declared = declared_bytes == 0,
                upload_state_client_bytes = *client_state_uploaded_bytes,
                upload_state_marker_mask =
                    format_args!("0x{:02x}", *client_state_uploaded_marker_mask),
                "rca debug mapi sync upload state begin"
            );
            responses.extend_from_slice(&rop_upload_state_success_response(request));
        }
        _ => responses.extend_from_slice(&rop_error_response(
            0x75,
            request.response_handle_index(),
            0x8004_0102,
        )),
    }
}

pub(super) fn append_upload_state_stream_continue_response(
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    mailbox_email: &str,
    request_id: &str,
    responses: &mut Vec<u8>,
) {
    match input_object_mut(session, handle_slots, request) {
        Some(MapiObject::SynchronizationSource {
            folder_id,
            mailbox_id,
            checkpoint_kind,
            sync_type,
            state_upload_buffer,
            ..
        }) => {
            let stream_data = request.stream_data();
            state_upload_buffer.extend_from_slice(stream_data);
            tracing::info!(
                rca_debug = true,
                adapter = "mapi",
                endpoint = "emsmdb",
                mailbox = %mailbox_email,
                request_type = "Execute",
                mapi_request_id = request_id,
                request_rop_id = "0x76",
                sync_context_kind = "source",
                folder_id = format_args!("0x{:016x}", *folder_id),
                folder_role = debug_role_for_folder_id(*folder_id),
                folder_container_class = debug_container_class_for_folder_id(*folder_id),
                sync_type = format_args!("0x{:02x}", *sync_type),
                checkpoint_kind = checkpoint_kind.as_str(),
                checkpoint_mailbox_id = (*mailbox_id)
                    .map(|id| id.to_string())
                    .unwrap_or_default(),
                upload_state_chunk_bytes = stream_data.len(),
                upload_state_chunk_preview = %hex_preview(stream_data, 16),
                upload_state_buffer_bytes = state_upload_buffer.len(),
                "rca debug mapi sync upload state continue"
            );
            responses.extend_from_slice(&rop_upload_state_success_response(request));
        }
        Some(MapiObject::SynchronizationCollector {
            folder_id,
            mailbox_id,
            checkpoint_kind,
            sync_type,
            state_upload_buffer,
            client_state_uploaded_bytes,
            client_state_uploaded_marker_mask,
            ..
        }) => {
            let stream_data = request.stream_data();
            state_upload_buffer.extend_from_slice(stream_data);
            tracing::info!(
                rca_debug = true,
                adapter = "mapi",
                endpoint = "emsmdb",
                mailbox = %mailbox_email,
                request_type = "Execute",
                mapi_request_id = request_id,
                request_rop_id = "0x76",
                sync_context_kind = "collector",
                folder_id = format_args!("0x{:016x}", *folder_id),
                folder_role = debug_role_for_folder_id(*folder_id),
                folder_container_class = debug_container_class_for_folder_id(*folder_id),
                sync_type = format_args!("0x{:02x}", *sync_type),
                checkpoint_kind = checkpoint_kind.as_str(),
                checkpoint_mailbox_id = (*mailbox_id)
                    .map(|id| id.to_string())
                    .unwrap_or_default(),
                upload_state_chunk_bytes = stream_data.len(),
                upload_state_chunk_preview = %hex_preview(stream_data, 16),
                upload_state_buffer_bytes = state_upload_buffer.len(),
                upload_state_client_bytes = *client_state_uploaded_bytes,
                upload_state_marker_mask =
                    format_args!("0x{:02x}", *client_state_uploaded_marker_mask),
                "rca debug mapi sync upload state continue"
            );
            responses.extend_from_slice(&rop_upload_state_success_response(request));
        }
        _ => responses.extend_from_slice(&rop_error_response(
            0x76,
            request.response_handle_index(),
            0x8004_0102,
        )),
    }
}

pub(super) fn append_upload_state_stream_end_response(
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    mailbox_email: &str,
    request_id: &str,
    responses: &mut Vec<u8>,
) {
    match input_object_mut(session, handle_slots, request) {
        Some(MapiObject::SynchronizationSource {
            folder_id,
            mailbox_id,
            checkpoint_kind,
            checkpoint_store_allowed,
            checkpoint_skip_reason,
            checkpoint_zero_delta,
            sync_type,
            initial_state,
            state,
            state_upload_property_tag,
            state_upload_buffer,
            client_state_uploaded_bytes,
            client_state_uploaded_marker_mask,
            client_state_selection_invalidated,
            incremental_transfer_buffer,
            transfer_buffer,
            transfer_position,
            ..
        }) => {
            let uploaded_bytes = state_upload_buffer.len();
            let upload_state_stream_summary = if uploaded_bytes == 0 {
                "bytes=0;empty=true".to_string()
            } else {
                mapi_mailstore::replguid_globset_debug_summary(state_upload_buffer)
            };
            let property_tag = state_upload_property_tag.take().unwrap_or_default();
            let upload_state_empty_stream_after_client_state =
                uploaded_bytes == 0 && *client_state_uploaded_bytes > 0;
            let validation = mapi_mailstore::validate_download_state_property(
                *sync_type,
                property_tag,
                state_upload_buffer,
            );
            let valid_state_property = validation.is_ok();
            if valid_state_property {
                mark_uploaded_state_stream(client_state_uploaded_marker_mask, property_tag);
                let updated_initial_state =
                    mapi_mailstore::sync_state_stream_with_uploaded_property(
                        *sync_type,
                        initial_state,
                        property_tag,
                        state_upload_buffer,
                    );
                *initial_state = updated_initial_state;
                *client_state_uploaded_bytes =
                    (*client_state_uploaded_bytes).saturating_add(uploaded_bytes);
            } else {
                *checkpoint_store_allowed = false;
                *checkpoint_skip_reason = "invalid_uploaded_client_state";
                *client_state_selection_invalidated = true;
            }
            state_upload_buffer.clear();
            let has_delta_anchor =
                uploaded_state_has_delta_anchor(*client_state_uploaded_marker_mask);
            let checkpoint_delta_available_before_upload_state =
                incremental_transfer_buffer.is_some();
            let client_state_validation_error = validation.err().unwrap_or_default();
            tracing::info!(
                rca_debug = true,
                adapter = "mapi",
                endpoint = "emsmdb",
                mailbox = %mailbox_email,
                request_type = "Execute",
                mapi_request_id = request_id,
                request_rop_id = "0x77",
                sync_context_kind = "source",
                folder_id = format_args!("0x{:016x}", *folder_id),
                folder_role = debug_role_for_folder_id(*folder_id),
                folder_container_class = debug_container_class_for_folder_id(*folder_id),
                sync_type = format_args!("0x{:02x}", *sync_type),
                checkpoint_kind = checkpoint_kind.as_str(),
                checkpoint_zero_delta = *checkpoint_zero_delta,
                checkpoint_mailbox_id = (*mailbox_id)
                    .map(|id| id.to_string())
                    .unwrap_or_default(),
                upload_state_total_bytes = state.len(),
                upload_state_stream_bytes = uploaded_bytes,
                upload_state_empty_stream = uploaded_bytes == 0,
                upload_state_empty_stream_expected = uploaded_bytes == 0,
                upload_state_empty_stream_after_client_state,
                upload_state_property_tag = format_args!("0x{property_tag:08x}"),
                upload_state_property_name = upload_state_property_name(property_tag),
                upload_state_stream_summary = %upload_state_stream_summary,
                upload_state_client_bytes = *client_state_uploaded_bytes,
                upload_state_marker_mask =
                    format_args!("0x{:02x}", *client_state_uploaded_marker_mask),
                upload_state_markers =
                    %uploaded_state_marker_summary(*client_state_uploaded_marker_mask),
                upload_state_has_delta_anchor = has_delta_anchor,
                checkpoint_delta_available_before_upload_state,
                upload_state_property_valid = valid_state_property,
                upload_state_client_state_validation_error = %client_state_validation_error,
                checkpoint_delta_selection_gate = "disabled_client_state_is_authoritative",
                active_transfer_selection = "full_pending_get_buffer_client_state_selection",
                transfer_buffer_bytes = transfer_buffer.len(),
                transfer_position = *transfer_position,
                "rca debug mapi sync upload state end"
            );
            if valid_state_property {
                responses.extend_from_slice(&rop_upload_state_success_response(request));
            } else {
                // [MS-OXCFXICS] section 3.1.5.4.3.2 recommends
                // RpcFormat when an IDSET/GLOBSET cannot be decoded.
                responses.extend_from_slice(&rop_error_response(
                    0x77,
                    request.response_handle_index(),
                    0x0000_04B6,
                ));
            }
        }
        Some(MapiObject::SynchronizationCollector {
            folder_id,
            mailbox_id,
            checkpoint_kind,
            state,
            state_upload_property_tag,
            state_upload_buffer,
            client_state_uploaded_bytes,
            client_state_uploaded_marker_mask,
            sync_type,
            uploaded_normal_change_numbers,
            uploaded_fai_change_numbers,
            uploaded_read_change_numbers,
            ..
        }) => {
            let uploaded_bytes = state_upload_buffer.len();
            let property_tag = state_upload_property_tag.take().unwrap_or_default();
            let idset_given = matches!(property_tag, 0x4017_0003 | 0x4017_0102);
            let valid_state_property = upload_state_marker_bit(property_tag) != 0;
            if valid_state_property {
                if !idset_given {
                    mark_uploaded_state_stream(client_state_uploaded_marker_mask, property_tag);
                    *state = mapi_mailstore::upload_sync_state_stream_with_uploaded_property(
                        *sync_type,
                        state,
                        property_tag,
                        state_upload_buffer,
                    );
                    if let Ok(counters) =
                        mapi_mailstore::replguid_globset_counters(state_upload_buffer)
                    {
                        let values = match property_tag {
                            0x6796_0102 => Some(&mut *uploaded_normal_change_numbers),
                            0x67DA_0102 => Some(&mut *uploaded_fai_change_numbers),
                            0x67D2_0102 => Some(&mut *uploaded_read_change_numbers),
                            _ => None,
                        };
                        if let Some(values) = values {
                            for counter in counters {
                                if !values.contains(&counter) {
                                    values.push(counter);
                                }
                            }
                        }
                    }
                }
            }
            state_upload_buffer.clear();
            *client_state_uploaded_bytes =
                (*client_state_uploaded_bytes).saturating_add(uploaded_bytes);
            tracing::info!(
                rca_debug = true,
                adapter = "mapi",
                endpoint = "emsmdb",
                mailbox = %mailbox_email,
                request_type = "Execute",
                mapi_request_id = request_id,
                request_rop_id = "0x77",
                sync_context_kind = "collector",
                folder_id = format_args!("0x{:016x}", *folder_id),
                folder_role = debug_role_for_folder_id(*folder_id),
                folder_container_class = debug_container_class_for_folder_id(*folder_id),
                sync_type = format_args!("0x{:02x}", *sync_type),
                checkpoint_kind = checkpoint_kind.as_str(),
                checkpoint_mailbox_id = (*mailbox_id)
                    .map(|id| id.to_string())
                    .unwrap_or_default(),
                upload_state_total_bytes = state.len(),
                upload_state_stream_bytes = uploaded_bytes,
                upload_state_empty_stream = uploaded_bytes == 0,
                upload_state_property_tag = format_args!("0x{property_tag:08x}"),
                upload_state_property_name = upload_state_property_name(property_tag),
                upload_state_client_bytes = *client_state_uploaded_bytes,
                upload_state_marker_mask =
                    format_args!("0x{:02x}", *client_state_uploaded_marker_mask),
                upload_state_markers =
                    %uploaded_state_marker_summary(*client_state_uploaded_marker_mask),
                upload_state_server_state_preserved = true,
                "rca debug mapi sync upload state end"
            );
            responses.extend_from_slice(&rop_upload_state_success_response(request));
        }
        _ => responses.extend_from_slice(&rop_error_response(
            0x77,
            request.response_handle_index(),
            0x8004_0102,
        )),
    }
}
