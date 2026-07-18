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
            if uploaded_bytes > 0 {
                mark_uploaded_state_stream(client_state_uploaded_marker_mask, property_tag);
                let updated_initial_state =
                    mapi_mailstore::sync_state_stream_with_uploaded_property(
                        *sync_type,
                        initial_state,
                        property_tag,
                        state_upload_buffer,
                    );
                *initial_state = updated_initial_state;
            }
            state_upload_buffer.clear();
            *client_state_uploaded_bytes =
                (*client_state_uploaded_bytes).saturating_add(uploaded_bytes);
            let has_delta_anchor =
                uploaded_state_has_delta_anchor(*client_state_uploaded_marker_mask);
            if *client_state_uploaded_bytes > 0 && !has_delta_anchor {
                *checkpoint_store_allowed = false;
                *checkpoint_skip_reason = "uploaded_client_state_transfer";
            } else if has_delta_anchor
                && *checkpoint_skip_reason == "uploaded_client_state_transfer"
            {
                *checkpoint_store_allowed = true;
                *checkpoint_skip_reason = "";
            }
            let mut selected_checkpoint_delta = false;
            let checkpoint_delta_available_before_upload_state =
                incremental_transfer_buffer.is_some();
            if has_delta_anchor {
                if let Some(buffer) = incremental_transfer_buffer.take() {
                    *transfer_buffer = buffer;
                    *transfer_position = 0;
                    selected_checkpoint_delta = true;
                }
            }
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
                upload_state_selected_checkpoint_delta = selected_checkpoint_delta,
                checkpoint_delta_selection_gate = "upload_state_delta_anchor",
                checkpoint_delta_selection_blocked_by_missing_upload_state_delta_anchor =
                    checkpoint_delta_available_before_upload_state && !has_delta_anchor,
                active_transfer_selection = if selected_checkpoint_delta {
                    "checkpoint_delta_after_upload_state_delta_anchor"
                } else if *checkpoint_zero_delta && !checkpoint_delta_available_before_upload_state
                {
                    "checkpoint_delta_zero_delta_initial"
                } else if checkpoint_delta_available_before_upload_state {
                    "full_pending_upload_state_delta_anchor"
                } else {
                    "full_or_static"
                },
                transfer_buffer_bytes = transfer_buffer.len(),
                transfer_position = *transfer_position,
                "rca debug mapi sync upload state end"
            );
            responses.extend_from_slice(&rop_upload_state_success_response(request));
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
            if uploaded_bytes > 0 {
                let idset_given = matches!(property_tag, 0x4017_0003 | 0x4017_0102);
                if !idset_given {
                    mark_uploaded_state_stream(client_state_uploaded_marker_mask, property_tag);
                }
                *state = mapi_mailstore::upload_sync_state_stream_with_uploaded_property(
                    *sync_type,
                    state,
                    property_tag,
                    state_upload_buffer,
                );
                if !idset_given {
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
