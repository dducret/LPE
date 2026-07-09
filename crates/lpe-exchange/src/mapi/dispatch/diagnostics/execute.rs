use super::*;
use crate::mapi::outlook_startup::{
    normalized_rop_sequence_signature, outlook_startup_gate_summary,
};
use crate::mapi::session::PostHierarchyExecuteObservation;
use crate::mapi::transport::post_hierarchy_action_summary;

pub(in crate::mapi::dispatch) fn log_execute_rop_debug(
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    headers: &HeaderMap,
    _session_id: &str,
    request_id: &str,
    request: &RopRequestDebugSummary,
    request_rop_buffer: &[u8],
    response_rop_buffer: &[u8],
    session: &MapiSession,
    post_hierarchy_observation: PostHierarchyExecuteObservation,
) {
    let response = summarize_response_rop_buffer_with_expected_handles(
        response_rop_buffer,
        &request.full_ids,
        &request.full_response_handle_indexes,
    );
    let logon = summarize_logon_response_rop(response_rop_buffer, &request.full_ids);
    let endpoint = match endpoint {
        MapiEndpoint::Emsmdb => "emsmdb",
        MapiEndpoint::Nspi => "nspi",
    };
    let client_request_id = safe_header(headers, "client-request-id").unwrap_or_default();
    let client_application = safe_header(headers, "x-clientapplication").unwrap_or_default();
    let client_info = safe_header(headers, "x-clientinfo").unwrap_or_default();
    let trace_id = safe_header(headers, "x-trace-id").unwrap_or_default();
    let post_hierarchy = post_hierarchy_action_summary(session, false);
    let startup_gates = outlook_startup_gate_summary(session);
    let sequence_signature = normalized_rop_sequence_signature(&request.names_csv);
    let message = "rca debug mapi execute rops";

    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = endpoint,
        tenant_id = %principal.tenant_id,
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = "Execute",
        mapi_request_id = request_id,
        client_request_id = %client_request_id,
        client_application = %client_application,
        client_info = %client_info,
        trace_id = %trace_id,
        request_rop_ids = %request.ids_csv,
        request_rop_names = %request.names_csv,
        request_rop_sequence_signature = %sequence_signature,
        request_rop_count = request.total_count,
        request_rop_debug_entry_count = request.ids.len(),
        request_rop_debug_truncated = request.truncated,
        request_rop_tail_ids = %request.tail_ids_csv,
        request_rop_tail_names = %request.tail_names_csv,
        request_non_release_rops = %request.non_release_rops,
        request_all_rops_are_release = request.all_release,
        request_handle_count = request.handle_count,
        input_handle_table_summary = %request.handle_table_summary,
        request_extended_rop_buffer = request.extended,
        request_rop_parse_error = %request.parse_error,
        response_rop_ids = %response.ids_csv,
        response_rop_names = %response.names_csv,
        response_rop_results_best_effort = %response.results_csv,
        response_rop_count = response.count,
        response_handle_count = response.handle_count,
        output_handle_table_summary = %response.handle_table_summary,
        response_rop_frames = %response.frames,
        response_extended_rop_buffer = response.extended,
        response_rop_parse_error = %response.parse_error,
        last_completed_hierarchy_sync_root = %post_hierarchy.last_completed_hierarchy_sync_root,
        content_sync_started_after_hierarchy =
            post_hierarchy.content_sync_configure_observed,
        post_hierarchy_close_kind = post_hierarchy.close_kind,
        outlook_bootstrap_phase = post_hierarchy.outlook_bootstrap_phase,
        outlook_bootstrap_phase_name = post_hierarchy.outlook_bootstrap_phase_name,
        outlook_bootstrap_stall_code = post_hierarchy.outlook_bootstrap_stall_code,
        outlook_bootstrap_stall_name = post_hierarchy.outlook_bootstrap_stall_name,
        outlook_bootstrap_next_expected_phase =
            post_hierarchy.outlook_bootstrap_next_expected_phase,
        outlook_startup_last_successful_gate = startup_gates.last_successful_gate,
        outlook_startup_first_missing_gate = startup_gates.first_missing_gate,
        outlook_startup_gate_count = startup_gates.gate_count,
        outlook_startup_passed_gate_count = startup_gates.passed_count,
        outlook_startup_gates = %startup_gates.gates,
        outlook_abandoned_immediately_after_fai =
            startup_gates.abandoned_immediately_after_fai,
        inbox_associated_broad_ipm_configuration_findrow_matched =
            session
                .post_hierarchy_actions
                .inbox_associated_broad_ipm_configuration_findrow_matched,
        inbox_associated_exact_ipm_configuration_findrow_matched =
            session
                .post_hierarchy_actions
                .inbox_associated_exact_ipm_configuration_findrow_matched,
        default_view_normal_query_rows_observed =
            session
                .post_hierarchy_actions
                .default_view_normal_contents_table_query_rows_observed,
        last_default_view_normal_query_rows_context =
            %debug_context_or_none(
                &session
                    .post_hierarchy_actions
                    .last_default_view_normal_contents_table_query_rows_context
            ),
        outlook_smart_input_variant = %session.outlook_smart_input_variant,
        post_hierarchy_execute_count = post_hierarchy.execute_count,
        post_hierarchy_rop_ids_seen = %post_hierarchy.rop_ids_seen,
        post_visible_inbox_release_create_save_batch_count =
            post_hierarchy.post_visible_inbox_release_create_save_batch_count,
        last_post_visible_inbox_release_create_save_batch_context =
            %debug_context_or_none(
                &post_hierarchy.last_post_visible_inbox_release_create_save_batch_context
            ),
        visible_inbox_open_create_save_batch_count =
            post_hierarchy.visible_inbox_open_create_save_batch_count,
        last_visible_inbox_open_create_save_batch_context =
            %debug_context_or_none(
                &post_hierarchy.last_visible_inbox_open_create_save_batch_context
            ),
        last_post_hierarchy_create_save_object_context =
            %debug_context_or_none(
                &post_hierarchy.last_post_hierarchy_create_save_object_context
            ),
        post_hierarchy_submit_attempt_count =
            post_hierarchy.post_hierarchy_submit_attempt_count,
        last_post_hierarchy_submit_attempt_context =
            %debug_context_or_none(
                &post_hierarchy.last_post_hierarchy_submit_attempt_context
            ),
        post_visible_release_hierarchy_query_position_count =
            post_hierarchy.post_visible_release_hierarchy_query_position_count,
        first_post_visible_release_hierarchy_query_position_context =
            %debug_context_or_none(
                &post_hierarchy
                    .first_post_visible_release_hierarchy_query_position_context
            ),
        outlook_umolk_named_property_probe_count =
            session
                .post_hierarchy_actions
                .outlook_umolk_named_property_probe_count,
        last_outlook_umolk_named_property_probe_context =
            %debug_context_or_none(
                &session
                    .post_hierarchy_actions
                    .last_outlook_umolk_named_property_probe_context
            ),
        outlook_umolk_getprops_not_found_count =
            session
                .post_hierarchy_actions
                .outlook_umolk_getprops_not_found_count,
        last_outlook_umolk_getprops_materialization_context =
            %debug_context_or_none(
                &session
                    .post_hierarchy_actions
                    .last_outlook_umolk_getprops_materialization_context
            ),
        outlook_view_trace_events = %post_hierarchy.outlook_view_trace_events,
        logon_response_present = logon.present,
        logon_error_code = %logon.error_code,
        logon_parse_error = %logon.parse_error,
        request_rop_buffer_bytes = request_rop_buffer.len(),
        response_rop_buffer_bytes = response_rop_buffer.len(),
        response_rop_buffer_preview = %hex_preview(response_rop_buffer, 160),
        message = message,
    );

    if should_log_execute_stalled_before_content_sync(
        endpoint,
        &post_hierarchy.last_completed_hierarchy_sync_root,
        post_hierarchy.content_sync_configure_observed,
        post_hierarchy.close_kind,
    ) {
        tracing::warn!(
            rca_debug = true,
            rca_warning = %post_hierarchy.close_kind,
            adapter = "mapi",
            endpoint = endpoint,
            tenant_id = %principal.tenant_id,
            account_id = %principal.account_id,
            mailbox = %principal.email,
            request_type = "Execute",
            mapi_request_id = request_id,
            client_request_id = %client_request_id,
            client_application = %client_application,
            client_info = %client_info,
            trace_id = %trace_id,
            request_rop_ids = %request.ids_csv,
            request_rop_names = %request.names_csv,
            request_rop_sequence_signature = %sequence_signature,
            response_rop_ids = %response.ids_csv,
            response_rop_results_best_effort = %response.results_csv,
            post_hierarchy_close_kind = %post_hierarchy.close_kind,
            outlook_bootstrap_phase = post_hierarchy.outlook_bootstrap_phase,
            outlook_bootstrap_phase_name = post_hierarchy.outlook_bootstrap_phase_name,
            outlook_bootstrap_stall_code = post_hierarchy.outlook_bootstrap_stall_code,
            outlook_bootstrap_stall_name = post_hierarchy.outlook_bootstrap_stall_name,
            outlook_bootstrap_next_expected_phase =
                post_hierarchy.outlook_bootstrap_next_expected_phase,
            post_hierarchy_last_hierarchy_query_position_context =
                %debug_context_or_none(
                    &post_hierarchy.last_hierarchy_table_query_position_context
                ),
            post_visible_release_hierarchy_query_position_count =
                post_hierarchy.post_visible_release_hierarchy_query_position_count,
            first_post_visible_release_hierarchy_query_position_context =
                %debug_context_or_none(
                    &post_hierarchy
                        .first_post_visible_release_hierarchy_query_position_context
                ),
            post_visible_findrow_release_hierarchy_query_position_count =
                post_hierarchy.post_visible_findrow_release_hierarchy_query_position_count,
            first_post_visible_findrow_release_hierarchy_query_position_context =
                %debug_context_or_none(
                    &post_hierarchy
                        .first_post_visible_findrow_release_hierarchy_query_position_context
                ),
            post_visible_inbox_release_create_save_batch_count =
                post_hierarchy.post_visible_inbox_release_create_save_batch_count,
            last_post_visible_inbox_release_create_save_batch_context =
                %debug_context_or_none(
                    &post_hierarchy.last_post_visible_inbox_release_create_save_batch_context
                ),
            visible_inbox_open_create_save_batch_count =
                post_hierarchy.visible_inbox_open_create_save_batch_count,
            last_visible_inbox_open_create_save_batch_context =
                %debug_context_or_none(
                    &post_hierarchy.last_visible_inbox_open_create_save_batch_context
                ),
            last_post_hierarchy_create_save_object_context =
                %debug_context_or_none(
                    &post_hierarchy.last_post_hierarchy_create_save_object_context
                ),
            post_hierarchy_submit_attempt_count =
                post_hierarchy.post_hierarchy_submit_attempt_count,
            last_post_hierarchy_submit_attempt_context =
                %debug_context_or_none(
                    &post_hierarchy.last_post_hierarchy_submit_attempt_context
                ),
            outlook_umolk_named_property_probe_count =
                session
                    .post_hierarchy_actions
                    .outlook_umolk_named_property_probe_count,
            last_outlook_umolk_named_property_probe_context =
                %debug_context_or_none(
                    &session
                        .post_hierarchy_actions
                        .last_outlook_umolk_named_property_probe_context
                ),
            outlook_umolk_getprops_not_found_count =
                session
                    .post_hierarchy_actions
                    .outlook_umolk_getprops_not_found_count,
            last_outlook_umolk_getprops_materialization_context =
                %debug_context_or_none(
                    &session
                        .post_hierarchy_actions
                        .last_outlook_umolk_getprops_materialization_context
                ),
            outlook_view_trace_events = %post_hierarchy.outlook_view_trace_events,
            next_debug_focus = "outlook_execute_stall_before_content_sync",
            "rca warn mapi outlook execute stalled before content sync"
        );
    }

    if logon.present {
        let response_store_identity_matches_session = logon.mailbox_guid
            == principal.account_id.to_string()
            && logon.replica_guid == bytes_to_hex(&crate::mapi::identity::STORE_REPLICA_GUID);
        tracing::debug!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = endpoint,
            tenant_id = %principal.tenant_id,
            account_id = %principal.account_id,
            mailbox = %principal.email,
            request_type = "Execute",
            mapi_request_id = request_id,
            client_request_id = %client_request_id,
            client_application = %client_application,
            client_info = %client_info,
            trace_id = %trace_id,
            output_handle_index = %logon.output_handle_index,
            logon_error_code = %logon.error_code,
            logon_flags = %logon.logon_flags,
            response_flags = %logon.response_flags,
            special_folder_ids = %logon.special_folder_ids,
            special_folder_contract = %logon.special_folder_contract,
            special_folder_contract_issues = %logon.special_folder_contract_issues,
            default_folder_identification_contract =
                %default_folder_identification_contract_for_debug(principal),
            mailbox_guid = %logon.mailbox_guid,
            expected_mailbox_guid = %principal.account_id,
            replid = %logon.replid,
            replica_guid = %logon.replica_guid,
            expected_replica_guid = %bytes_to_hex(&crate::mapi::identity::STORE_REPLICA_GUID),
            response_store_identity_matches_session,
            parse_error = %logon.parse_error,
            message = "rca debug mapi logon response",
        );
    }

    if endpoint == "emsmdb" && !request.parse_error.is_empty() {
        tracing::debug!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = endpoint,
            tenant_id = %principal.tenant_id,
            account_id = %principal.account_id,
            mailbox = %principal.email,
            request_type = "Execute",
            mapi_request_id = request_id,
            client_request_id = %client_request_id,
            client_application = %client_application,
            client_info = %client_info,
            trace_id = %trace_id,
            request_rop_ids = %request.ids_csv,
            request_rop_names = %request.names_csv,
            request_rop_parse_error = %request.parse_error,
            request_rop_payload_bytes = request.request_payload_bytes,
            request_handle_table_bytes = request.handle_table_bytes,
            request_handle_count = request.handle_count,
            input_handle_table_summary = %request.handle_table_summary,
            request_rop_raw_frame_count = request.raw_frame_count,
            request_rop_raw_frames = %request.raw_frames,
            "rca debug mapi execute request framing"
        );
    }

    if let Some(response_framing_context) =
        execute_response_framing_context(&request.full_ids).filter(|_| endpoint == "emsmdb")
    {
        tracing::debug!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = endpoint,
            tenant_id = %principal.tenant_id,
            account_id = %principal.account_id,
            mailbox = %principal.email,
            request_type = "Execute",
            mapi_request_id = request_id,
            client_request_id = %client_request_id,
            client_application = %client_application,
            client_info = %client_info,
            trace_id = %trace_id,
            response_framing_context = response_framing_context,
            request_rop_ids = %request.ids_csv,
            request_rop_names = %request.names_csv,
            request_rop_tail_ids = %request.tail_ids_csv,
            request_rop_tail_names = %request.tail_names_csv,
            request_non_release_rops = %request.non_release_rops,
            response_rop_ids = %response.ids_csv,
            response_rop_names = %response.names_csv,
            response_rop_results_best_effort = %response.results_csv,
            response_rop_buffer_layout = %response.buffer_layout,
            response_rop_buffer_size_word = %response.buffer_size_word,
            response_rop_payload_bytes = response.response_payload_bytes,
            response_handle_table_bytes = response.handle_table_bytes,
            response_handle_count = response.handle_count,
            output_handle_table_summary = %response.handle_table_summary,
            response_rop_frame_count = response.count,
            response_rop_frames = %response.frames,
            response_rop_parse_error = %response.parse_error,
            "rca debug mapi execute response framing"
        );
    }

    if endpoint == "emsmdb"
        && request.full_ids.contains(&RopId::SetColumns.as_u8())
        && request.full_ids.contains(&RopId::Release.as_u8())
    {
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = endpoint,
            tenant_id = %principal.tenant_id,
            account_id = %principal.account_id,
            mailbox = %principal.email,
            request_type = "Execute",
            mapi_request_id = request_id,
            client_request_id = %client_request_id,
            client_application = %client_application,
            client_info = %client_info,
            trace_id = %trace_id,
            response_framing_context = "setcolumns_release_batch",
            request_full_rop_ids = %rop_ids_csv(&request.full_ids),
            request_full_rop_names = %rop_names_csv(&request.full_ids),
            request_rop_ids = %request.ids_csv,
            request_rop_names = %request.names_csv,
            request_non_release_rops = %request.non_release_rops,
            request_rop_raw_frame_count = request.raw_frame_count,
            request_rop_raw_frames = %request.raw_frames,
            response_rop_ids = %response.ids_csv,
            response_rop_names = %response.names_csv,
            response_rop_results_best_effort = %response.results_csv,
            response_rop_buffer_layout = %response.buffer_layout,
            response_rop_buffer_size_word = %response.buffer_size_word,
            response_rop_payload_bytes = response.response_payload_bytes,
            response_handle_table_bytes = response.handle_table_bytes,
            response_handle_count = response.handle_count,
            output_handle_table_summary = %response.handle_table_summary,
            response_rop_frame_count = response.count,
            response_rop_frames = %response.frames,
            response_rop_parse_error = %response.parse_error,
            "rca debug mapi setcolumns release response framing"
        );
    }

    if endpoint == "emsmdb"
        && request.all_release
        && request.total_count != 0
        && response.count == 0
        && response.handle_count == 0
    {
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = endpoint,
            tenant_id = %principal.tenant_id,
            account_id = %principal.account_id,
            mailbox = %principal.email,
            request_type = "Execute",
            mapi_request_id = request_id,
            client_request_id = %client_request_id,
            client_application = %client_application,
            client_info = %client_info,
            trace_id = %trace_id,
            outlook_event_provider = "Outlook",
            outlook_event_id = 19,
            outlook_event_operation = "EcDoRpcExt",
            outlook_event_hresult = "0x800704d3",
            outlook_event_data_code = 78,
            correlation_basis = "release_only_no_rop_response_with_handle_table",
            release_response_spec_compliant = true,
            request_rop_ids = %request.ids_csv,
            request_rop_names = %request.names_csv,
            request_rop_payload_bytes = request.request_payload_bytes,
            request_handle_table_bytes = request.handle_table_bytes,
            request_handle_count = request.handle_count,
            input_handle_table_summary = %request.handle_table_summary,
            request_rop_raw_frame_count = request.raw_frame_count,
            request_rop_raw_frames = %request.raw_frames,
            response_rop_ids = %response.ids_csv,
            response_rop_names = %response.names_csv,
            response_rop_buffer_layout = %response.buffer_layout,
            response_rop_buffer_size_word = %response.buffer_size_word,
            response_rop_payload_bytes = response.response_payload_bytes,
            response_handle_table_bytes = response.handle_table_bytes,
            response_handle_count = response.handle_count,
            output_handle_table_summary = %response.handle_table_summary,
            response_rop_frame_count = response.count,
            response_rop_parse_error = %response.parse_error,
            live_handle_count = session.handles.len(),
            last_completed_hierarchy_sync_root =
                %post_hierarchy.last_completed_hierarchy_sync_root,
            content_sync_started_after_hierarchy =
                post_hierarchy.content_sync_configure_observed,
            post_hierarchy_execute_count = post_hierarchy.execute_count,
            post_hierarchy_rop_ids_seen = %post_hierarchy.rop_ids_seen,
            message = "rca debug mapi outlook event 19 candidate"
        );
    }

    if endpoint == "emsmdb" && execute_batch_has_same_save_getprops_not_found(request, &response) {
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = endpoint,
            tenant_id = %principal.tenant_id,
            account_id = %principal.account_id,
            mailbox = %principal.email,
            request_type = "Execute",
            mapi_request_id = request_id,
            client_request_id = %client_request_id,
            client_application = %client_application,
            client_info = %client_info,
            trace_id = %trace_id,
            outlook_event_provider = "Outlook",
            outlook_event_id = 19,
            outlook_event_hresult_candidate = "0x800704d3",
            outlook_event_data_code_observed = 1343,
            correlation_basis =
                "same_execute_batch_savechanges_success_then_getprops_object_not_found",
            request_rop_ids = %request.ids_csv,
            request_rop_names = %request.names_csv,
            request_rop_payload_bytes = request.request_payload_bytes,
            request_handle_table_bytes = request.handle_table_bytes,
            request_handle_count = request.handle_count,
            input_handle_table_summary = %request.handle_table_summary,
            request_rop_raw_frame_count = request.raw_frame_count,
            request_rop_raw_frames = %request.raw_frames,
            response_rop_ids = %response.ids_csv,
            response_rop_names = %response.names_csv,
            response_rop_results_best_effort = %response.results_csv,
            response_rop_buffer_layout = %response.buffer_layout,
            response_rop_buffer_size_word = %response.buffer_size_word,
            response_rop_payload_bytes = response.response_payload_bytes,
            response_handle_table_bytes = response.handle_table_bytes,
            response_handle_count = response.handle_count,
            output_handle_table_summary = %response.handle_table_summary,
            response_rop_frame_count = response.count,
            response_rop_frames = %response.frames,
            response_rop_parse_error = %response.parse_error,
            live_handle_count = session.handles.len(),
            message = "rca debug mapi outlook event 19 candidate"
        );
    }

    if endpoint == "emsmdb"
        && (post_hierarchy_observation.first_execute
            || post_hierarchy_observation.first_bootstrap_probe
            || post_hierarchy_observation.first_set_properties_probe)
    {
        let probe = super::super::summarize_first_post_hierarchy_probe(
            request_rop_buffer,
            response_rop_buffer,
        );
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = endpoint,
            tenant_id = %principal.tenant_id,
            account_id = %principal.account_id,
            mailbox = %principal.email,
            request_type = "Execute",
            mapi_request_id = request_id,
            client_request_id = %client_request_id,
            client_application = %client_application,
            client_info = %client_info,
            trace_id = %trace_id,
            last_completed_hierarchy_sync_root =
                %post_hierarchy.last_completed_hierarchy_sync_root,
            first_post_hierarchy_execute = post_hierarchy_observation.first_execute,
            first_post_hierarchy_bootstrap_probe =
                post_hierarchy_observation.first_bootstrap_probe,
            first_post_hierarchy_set_properties_probe =
                post_hierarchy_observation.first_set_properties_probe,
            request_rop_ids = %request.ids_csv,
            request_rop_names = %request.names_csv,
            response_rop_results_best_effort = %response.results_csv,
            open_folder_request_count = probe.open_folder_request_count,
            open_folder_requests = %probe.open_folder_requests,
            open_folder_response_shapes = %probe.open_folder_response_shapes,
            get_properties_specific_request_count = probe.get_properties_specific_request_count,
            get_properties_specific_requests = %probe.get_properties_specific_requests,
            get_properties_specific_response_shapes =
                %probe.get_properties_specific_response_shapes,
            set_properties_request_count = probe.set_properties_request_count,
            set_properties_requests = %probe.set_properties_requests,
            set_properties_response_shapes = %probe.set_properties_response_shapes,
            probe_parse_error = %probe.parse_error,
            "rca debug mapi post hierarchy execute probe"
        );
    }
}

pub(in crate::mapi::dispatch) fn should_log_execute_stalled_before_content_sync(
    endpoint: &str,
    last_completed_hierarchy_sync_root: &str,
    content_sync_configure_observed: bool,
    post_hierarchy_close_kind: &str,
) -> bool {
    endpoint == "emsmdb"
        && !last_completed_hierarchy_sync_root.is_empty()
        && !content_sync_configure_observed
        && !matches!(
            post_hierarchy_close_kind,
            "post_hierarchy_no_close" | "outlook_post_hierarchy_execute_before_content_sync"
        )
}

pub(in crate::mapi::dispatch) fn log_execute_dispatch_start_debug(
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    _headers: &HeaderMap,
    request_id: &str,
    mailbox_count: usize,
    email_count: usize,
) {
    let endpoint = match endpoint {
        MapiEndpoint::Emsmdb => "emsmdb",
        MapiEndpoint::Nspi => "nspi",
    };
    let message = "rca debug mapi execute dispatch start";

    tracing::debug!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = endpoint,
        tenant_id = %principal.tenant_id,
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = "Execute",
        mapi_request_id = request_id,
        mailbox_count = mailbox_count,
        email_count = email_count,
        message = message,
    );
}

pub(in crate::mapi::dispatch) fn log_execute_parse_failure_debug(
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    headers: &HeaderMap,
    request_id: &str,
    body: &[u8],
    error: &anyhow::Error,
) {
    let client_request_id = safe_header(headers, "client-request-id").unwrap_or_default();
    let client_application = safe_header(headers, "x-clientapplication").unwrap_or_default();
    let client_info = safe_header(headers, "x-clientinfo").unwrap_or_default();
    let trace_id = safe_header(headers, "x-trace-id").unwrap_or_default();
    let endpoint = match endpoint {
        MapiEndpoint::Emsmdb => "emsmdb",
        MapiEndpoint::Nspi => "nspi",
    };
    let flags = read_le_u32_at(body, 0);
    let rop_buffer_size = read_le_u32_at(body, 4);
    let rop_buffer_end = rop_buffer_size.and_then(|size| 8usize.checked_add(size as usize));
    let max_rop_out = rop_buffer_end.and_then(|offset| read_le_u32_at(body, offset));
    let auxiliary_buffer_size =
        rop_buffer_end.and_then(|offset| read_le_u32_at(body, offset.saturating_add(4)));
    let expected_body_bytes = match (rop_buffer_end, auxiliary_buffer_size) {
        (Some(offset), Some(auxiliary_buffer_size)) => offset
            .checked_add(8)
            .and_then(|offset| offset.checked_add(auxiliary_buffer_size as usize)),
        _ => None,
    };
    tracing::warn!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = endpoint,
        tenant_id = %principal.tenant_id,
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = "Execute",
        mapi_request_id = request_id,
        client_request_id = %client_request_id,
        client_application = %client_application,
        client_info = %client_info,
        trace_id = %trace_id,
        request_body_bytes = body.len(),
        execute_flags = flags.map(format_hex_u32).unwrap_or_default(),
        declared_rop_buffer_size = rop_buffer_size
            .map(|value| value.to_string())
            .unwrap_or_default(),
        max_rop_out = max_rop_out.map(|value| value.to_string()).unwrap_or_default(),
        declared_auxiliary_buffer_size = auxiliary_buffer_size
            .map(|value| value.to_string())
            .unwrap_or_default(),
        expected_body_bytes = expected_body_bytes
            .map(|value| value.to_string())
            .unwrap_or_default(),
        body_preview_hex = %debug_payload_preview_hex(body),
        parse_error = %error,
        "rca debug mapi execute parse failure"
    );
}

fn read_le_u32_at(bytes: &[u8], offset: usize) -> Option<u32> {
    let value = bytes.get(offset..offset.checked_add(4)?)?;
    Some(u32::from_le_bytes(value.try_into().ok()?))
}

fn format_hex_u32(value: u32) -> String {
    format!("0x{value:08x}")
}
