use super::super::*;
use super::*;

#[test]
fn execute_max_rop_out_returns_buffer_too_small_response() {
    let request = [
        0x09, 0x00, 0x15, 0x01, 0x01, 0x02, 0x01, 0xFF, 0x0F, 0x6D, 0x00, 0x00, 0x00, 0x56, 0x00,
        0x00, 0x00,
    ];
    let response = rop_buffer_with_response_spec(vec![0x15, 0x01, 0, 0, 0, 0, 0, 0], &[0x56]);

    let capped = apply_execute_max_rop_out("test-request", &request, response.clone(), 4);

    assert_ne!(capped, response);
    assert_eq!(&capped[..3], &[0x0C, 0x00, 0xFF]);
    assert_eq!(&capped[3..5], &(response.len() as u16).to_le_bytes());
    assert_eq!(&capped[5..12], &[0x15, 0x01, 0x01, 0x02, 0x01, 0xFF, 0x0F]);
    assert_eq!(
        &capped[12..],
        &[0x6D, 0x00, 0x00, 0x00, 0x56, 0x00, 0x00, 0x00]
    );
}

#[test]
fn parse_execute_request_keeps_max_rop_out() {
    let rop_buffer = [0x02, 0x00];
    let mut body = Vec::new();
    body.extend_from_slice(&0u32.to_le_bytes());
    body.extend_from_slice(&(rop_buffer.len() as u32).to_le_bytes());
    body.extend_from_slice(&rop_buffer);
    body.extend_from_slice(&0x1234u32.to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes());

    let parsed = parse_execute_request(&body).unwrap();

    assert_eq!(parsed.rop_buffer, rop_buffer);
    assert_eq!(parsed.max_rop_out, 0x1234);
}

#[test]
fn execute_stall_warning_requires_specific_post_hierarchy_pre_sync_stop() {
    assert!(should_log_execute_stalled_before_content_sync(
        "emsmdb",
        "0x0000000000040001",
        false,
        "outlook_umolk_named_property_burst_before_content_sync",
    ));

    assert!(!should_log_execute_stalled_before_content_sync(
        "nspi",
        "0x0000000000040001",
        false,
        "outlook_umolk_named_property_burst_before_content_sync",
    ));
    assert!(!should_log_execute_stalled_before_content_sync(
        "emsmdb",
        "",
        false,
        "outlook_umolk_named_property_burst_before_content_sync",
    ));
    assert!(!should_log_execute_stalled_before_content_sync(
        "emsmdb",
        "0x0000000000040001",
        true,
        "outlook_umolk_named_property_burst_before_content_sync",
    ));
    assert!(!should_log_execute_stalled_before_content_sync(
        "emsmdb",
        "0x0000000000040001",
        false,
        "post_hierarchy_no_close",
    ));
    assert!(!should_log_execute_stalled_before_content_sync(
        "emsmdb",
        "0x0000000000040001",
        false,
        "outlook_post_hierarchy_execute_before_content_sync",
    ));
}

#[tokio::test]
async fn execute_active_session_acquire_waits_for_short_outlook_overlap() {
    let session_id = format!("test-overlap-{}", Uuid::new_v4());
    let active = begin_active_session_request(&session_id).unwrap();
    let release = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        drop(active);
    });

    let acquired = acquire_execute_active_session_request(&session_id).await;

    assert!(acquired.is_some());
    drop(acquired);
    release.await.unwrap();
    assert!(!session_request_is_active(&session_id));
}

#[test]
fn release_only_execute_batch_is_store_independent() {
    let release_only = rop_buffer_with_response(vec![0x01, 0x00, 0x00], &[0x34]);
    assert!(rop_buffer_is_store_independent_release_only(&release_only));

    let mut release_then_getprops = vec![0x01, 0x00, 0x00];
    release_then_getprops.extend_from_slice(&[0x07, 0x00, 0x01]);
    release_then_getprops.extend_from_slice(&4096u16.to_le_bytes());
    release_then_getprops.extend_from_slice(&1u16.to_le_bytes());
    release_then_getprops.extend_from_slice(&0x3601_0003u32.to_le_bytes());
    let mixed = rop_buffer_with_response(release_then_getprops, &[0x34, 0xff]);
    assert!(!rop_buffer_is_store_independent_release_only(&mixed));
}

#[test]
fn release_only_execute_response_echoes_input_handle_table() {
    let response_handles = execute_response_handle_table(&[], &[u32::MAX], &[], &[], true, true);

    assert_eq!(response_handles, vec![u32::MAX]);
}

#[test]
fn mixed_release_execute_response_preserves_sparse_output_handle_index() {
    let response_handles = execute_response_handle_table(
        &[0x02, 0x01, 0, 0, 0, 0, 0, 0],
        &[u32::MAX, 77],
        &[77],
        &[1],
        true,
        true,
    );

    assert_eq!(response_handles, vec![0, 77]);
}

#[test]
fn mixed_create_save_batch_preserves_save_response_folder_handle_slot() {
    let response_handles = execute_response_handle_table(
        &[
            0x05, 0x02, 0, 0, 0, 0, 0, 0, 0, 0x06, 0x03, 0, 0, 0, 0, 0, 0x0c, 0x01, 0, 0, 0, 0,
        ],
        &[u32::MAX, 6, 28, 29],
        &[28, 29],
        &[2, 3, 1],
        true,
        true,
    );

    assert_eq!(response_handles, vec![0, 6, 28, 29]);
}

#[test]
fn mixed_setcolumns_release_response_omits_release_only_handle_slots() {
    let response_handles =
        execute_response_handle_table(&[0x12, 0x01, 0, 0, 0, 0, 0], &[25], &[], &[0], true, true);

    assert_eq!(response_handles, vec![25]);
}

#[test]
fn mixed_setcolumns_release_response_trims_snapshot_to_response_handle_index() {
    let response_handles = execute_response_handle_table(
        &[0x12, 0x00, 0, 0, 0, 0, 0],
        &[27, 75, 74],
        &[],
        &[0],
        true,
        true,
    );

    assert_eq!(response_handles, vec![27]);
}

#[test]
fn mixed_setcolumns_trailing_release_returns_invalid_released_handle() {
    let response_handles = execute_response_handle_table(
        &[0x12, 0x00, 0, 0, 0, 0, 0],
        &[u32::MAX, 75, 74],
        &[],
        &[0],
        true,
        true,
    );

    assert_eq!(response_handles, vec![0]);
}

#[test]
fn outlook_setcolumns_then_release_same_slot_uses_response_boundary_handle_snapshot() {
    let response_handles = execute_response_handle_table(
        &[0x12, 0x00, 0, 0, 0, 0, 0],
        &[28, 80, 79],
        &[],
        &[0],
        true,
        true,
    );

    assert_eq!(response_handles, vec![28]);
}

#[test]
fn non_release_echo_response_keeps_output_placeholders() {
    let response_handles = execute_response_handle_table(
        &[0x07, 0x01, 0, 0, 0, 0, 0],
        &[25, u32::MAX],
        &[],
        &[1],
        true,
        false,
    );

    assert_eq!(response_handles, vec![25, u32::MAX]);
}

#[test]
fn execute_rop_debug_summary_decodes_ids_and_return_codes() {
    let mut request_bytes = vec![0x02, 0, 0, 1];
    request_bytes.extend_from_slice(
        &crate::mapi::identity::wire_id_bytes_from_object_id(ROOT_FOLDER_ID).unwrap(),
    );
    request_bytes.push(0);
    let request_buffer = rop_buffer_with_response(request_bytes, &[0]);
    let request_summary = summarize_request_rop_buffer(&request_buffer);

    assert_eq!(request_summary.ids, vec![0x02]);
    assert_eq!(request_summary.ids_csv, "0x02");
    assert_eq!(request_summary.names_csv, "OpenFolder");
    assert_eq!(request_summary.handle_count, 1);
    assert!(request_summary.parse_error.is_empty());

    let request = RopRequest {
        rop_id: 0x02,
        input_handle_index: Some(0),
        output_handle_index: Some(1),
        payload: Vec::new(),
    };
    let response_buffer =
        rop_buffer_with_response(rop_open_folder_response(&request, false), &[42]);
    let response_summary = summarize_response_rop_buffer(&response_buffer, &request_summary.ids);

    assert_eq!(response_summary.ids_csv, "0x02");
    assert_eq!(response_summary.names_csv, "OpenFolder");
    assert_eq!(response_summary.results_csv, "0x02:0x00000000");
    assert_eq!(response_summary.count, 1);
    assert_eq!(response_summary.handle_count, 1);
    assert!(response_summary.parse_error.is_empty());
}

#[test]
fn execute_rop_debug_summary_skips_false_getprops_inside_findrow_payload() {
    let mut request_bytes = vec![
        RopId::FindRow.as_u8(),
        0,
        3,
        0,
        0,
        0,
        1,
        0,
        0,
        RopId::GetPropertiesSpecific.as_u8(),
        0,
        5,
    ];
    request_bytes.extend_from_slice(&0u16.to_le_bytes());
    request_bytes.extend_from_slice(&1u16.to_le_bytes());
    request_bytes.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
    let request_buffer = rop_buffer_with_response(request_bytes, &[0]);
    let request_summary = summarize_request_rop_buffer(&request_buffer);

    let mut responses = vec![RopId::FindRow.as_u8(), 3];
    responses.extend_from_slice(&0u32.to_le_bytes());
    responses.push(0);
    responses.push(1);
    responses.extend_from_slice(&[0, 1, 0, 0, 0, 0]);
    responses.extend_from_slice(&[RopId::GetPropertiesSpecific.as_u8(), 0x0e, 0x1a, 0, 0, 0]);
    responses.extend_from_slice(&[RopId::GetPropertiesSpecific.as_u8(), 5, 0, 0, 0, 0, 0]);
    for unit in "Subject\0".encode_utf16() {
        responses.extend_from_slice(&unit.to_le_bytes());
    }
    let response_buffer = rop_buffer_with_response(responses, &[0]);
    let response_summary = summarize_response_rop_buffer_with_expected_handles(
        &response_buffer,
        &request_summary.full_ids,
        &request_summary.full_response_handle_indexes,
    );

    assert_eq!(response_summary.ids_csv, "0x4f,0x07");
    assert_eq!(
        response_summary.results_csv,
        "0x4f:0x00000000,0x07:0x00000000"
    );
    assert!(response_summary.frames.contains("0x07@"));
    assert!(!response_summary.results_csv.contains("0x0000001a"));
}

#[test]
fn execute_rop_debug_summary_uses_output_handle_for_open_folder_response() {
    let mut request_bytes = vec![RopId::OpenFolder.as_u8(), 0, 0, 1];
    request_bytes.extend_from_slice(
        &crate::mapi::identity::wire_id_bytes_from_object_id(ROOT_FOLDER_ID).unwrap(),
    );
    request_bytes.push(0);
    request_bytes.extend_from_slice(&[RopId::GetPropertiesSpecific.as_u8(), 0, 1, 0, 0, 0, 0]);
    let request_buffer = rop_buffer_with_response(request_bytes, &[0, u32::MAX]);
    let request_summary = summarize_request_rop_buffer(&request_buffer);

    let request = RopRequest {
        rop_id: RopId::OpenFolder.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: Some(1),
        payload: Vec::new(),
    };
    let mut responses = rop_open_folder_response(&request, false);
    responses.extend_from_slice(&[RopId::GetPropertiesSpecific.as_u8(), 1]);
    responses.extend_from_slice(&0u32.to_le_bytes());
    responses.extend_from_slice(&0u16.to_le_bytes());
    let response_buffer = rop_buffer_with_response(responses, &[ROOT_FOLDER_ID as u32, 42]);
    let response_summary = summarize_response_rop_buffer_with_expected_handles(
        &response_buffer,
        &request_summary.full_ids,
        &request_summary.full_response_handle_indexes,
    );

    assert_eq!(response_summary.ids_csv, "0x02,0x07");
    assert_eq!(
        response_summary.results_csv,
        "0x02:0x00000000,0x07:0x00000000"
    );
}

#[test]
fn execute_rop_debug_summary_uses_output_handle_for_open_stream_response() {
    let mut request_bytes = vec![RopId::OpenMessage.as_u8(), 0, 0, 1];
    request_bytes.extend_from_slice(&0u16.to_le_bytes());
    request_bytes.extend_from_slice(&ROOT_FOLDER_ID.to_le_bytes());
    request_bytes.push(0);
    request_bytes.extend_from_slice(&0x7fff_ffff_ffed_0001u64.to_le_bytes());
    request_bytes.extend_from_slice(&[RopId::OpenStream.as_u8(), 0, 1, 2]);
    request_bytes.extend_from_slice(&0x6802_0102u32.to_le_bytes());
    request_bytes.push(0);
    request_bytes.extend_from_slice(&[RopId::ReadStream.as_u8(), 0, 2]);
    request_bytes.extend_from_slice(&0xffffu16.to_le_bytes());
    let request_buffer = rop_buffer_with_response(request_bytes, &[0, u32::MAX, u32::MAX]);
    let request_summary = summarize_request_rop_buffer(&request_buffer);

    let open_message_request = RopRequest {
        rop_id: RopId::OpenMessage.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: Some(1),
        payload: Vec::new(),
    };
    let open_stream_request = RopRequest {
        rop_id: RopId::OpenStream.as_u8(),
        input_handle_index: Some(1),
        output_handle_index: Some(2),
        payload: Vec::new(),
    };
    let mut responses = rop_open_message_response(&open_message_request, "IPM.RuleOrganizer", 0);
    responses.extend_from_slice(&rop_open_stream_response(&open_stream_request, 0));
    responses.extend_from_slice(&[RopId::ReadStream.as_u8(), 2]);
    responses.extend_from_slice(&0u32.to_le_bytes());
    responses.extend_from_slice(&0u16.to_le_bytes());
    let response_buffer = rop_buffer_with_response(responses, &[ROOT_FOLDER_ID as u32, 42, 43]);
    let response_summary = summarize_response_rop_buffer_with_expected_handles(
        &response_buffer,
        &request_summary.full_ids,
        &request_summary.full_response_handle_indexes,
    );

    assert_eq!(response_summary.ids_csv, "0x03,0x2b,0x2c");
    assert_eq!(
        response_summary.results_csv,
        "0x03:0x00000000,0x2b:0x00000000,0x2c:0x00000000"
    );
}

#[test]
fn execute_rop_debug_summary_distinguishes_truncated_release_prefix() {
    let mut request_bytes = Vec::new();
    for index in 0..MAX_ROP_DEBUG_ENTRIES {
        request_bytes.extend_from_slice(&[RopId::Release.as_u8(), 0, index as u8]);
    }
    request_bytes.extend_from_slice(&[RopId::OpenFolder.as_u8(), 0, 0, 1]);
    request_bytes.extend_from_slice(
        &crate::mapi::identity::wire_id_bytes_from_object_id(ROOT_FOLDER_ID).unwrap(),
    );
    request_bytes.push(0);
    let request_buffer =
        rop_buffer_with_response(request_bytes, &vec![u32::MAX; MAX_ROP_DEBUG_ENTRIES + 1]);

    let request_summary = summarize_request_rop_buffer(&request_buffer);

    assert_eq!(request_summary.full_ids.len(), MAX_ROP_DEBUG_ENTRIES + 1);
    assert_eq!(request_summary.ids.len(), MAX_ROP_DEBUG_ENTRIES);
    assert_eq!(request_summary.total_count, MAX_ROP_DEBUG_ENTRIES + 1);
    assert!(request_summary.truncated);
    assert!(!request_summary.all_release);
    assert!(request_summary.ids.iter().all(|rop_id| *rop_id == 0x01));
    assert!(request_summary.tail_ids_csv.ends_with("0x02"));
    assert!(request_summary.tail_names_csv.ends_with("OpenFolder"));
    assert_eq!(
        request_summary.non_release_rops,
        format!("{}:OpenFolder", MAX_ROP_DEBUG_ENTRIES)
    );
    assert!(request_summary.parse_error.is_empty());
}

#[test]
fn execute_rop_response_summary_uses_full_truncated_request_ids() {
    let mut request_bytes = Vec::new();
    for index in 0..MAX_ROP_DEBUG_ENTRIES {
        request_bytes.extend_from_slice(&[RopId::Release.as_u8(), 0, index as u8]);
    }
    request_bytes.extend_from_slice(&[RopId::GetPropertyIdsFromNames.as_u8(), 0, 45, 0x02]);
    request_bytes.extend_from_slice(&1u16.to_le_bytes());
    request_bytes.push(0x00);
    request_bytes.extend_from_slice(&[0x02; 16]);
    request_bytes.extend_from_slice(&0x820du32.to_le_bytes());
    let request_buffer =
        rop_buffer_with_response(request_bytes, &vec![u32::MAX; MAX_ROP_DEBUG_ENTRIES + 1]);
    let request_summary = summarize_request_rop_buffer(&request_buffer);
    let property_ids_request = RopRequest {
        rop_id: RopId::GetPropertyIdsFromNames.as_u8(),
        input_handle_index: Some(45),
        output_handle_index: Some(45),
        payload: Vec::new(),
    };
    let response_buffer = rpc_header_ext_rop_buffer(rop_buffer_with_response_spec(
        rop_get_property_ids_from_names_response(&property_ids_request, &[0x820d]),
        &[0],
    ));

    let truncated_response = summarize_response_rop_buffer(&response_buffer, &request_summary.ids);
    let full_response = summarize_response_rop_buffer(&response_buffer, &request_summary.full_ids);

    assert_eq!(truncated_response.count, 0);
    assert_eq!(full_response.ids_csv, "0x56");
    assert_eq!(full_response.names_csv, "GetPropertyIdsFromNames");
    assert_eq!(full_response.results_csv, "0x56:0x00000000");
    assert!(full_response.parse_error.is_empty());
}

#[test]
fn get_buffer_response_debug_exposes_wire_framing() {
    let mut response = vec![0x4e, 0x03];
    response.extend_from_slice(&0u32.to_le_bytes());
    response.extend_from_slice(&0x0003u16.to_le_bytes());
    response.extend_from_slice(&2u16.to_le_bytes());
    response.extend_from_slice(&2u16.to_le_bytes());
    response.push(0);
    response.extend_from_slice(&4u16.to_le_bytes());
    response.extend_from_slice(&[0x40, 0x12, 0x00, 0x03]);

    let debug = summarize_fast_transfer_get_buffer_response(&response, true);

    assert_eq!(debug.rop_id, "0x4e");
    assert!(debug.rop_id_matches);
    assert_eq!(debug.handle_index, 3);
    assert_eq!(debug.return_value, "0x00000000");
    assert_eq!(debug.transfer_status, "0x0003");
    assert!(debug.transfer_status_matches_completed);
    assert_eq!(debug.in_progress_count, 2);
    assert_eq!(debug.total_step_count, 2);
    assert!(debug.reserved_zero);
    assert_eq!(debug.transfer_buffer_size, 4);
    assert_eq!(debug.transfer_payload_bytes, 4);
    assert!(debug.transfer_buffer_size_matches_payload);
    assert_eq!(debug.transfer_payload_preview_hex, "40120003");
    assert!(debug.parse_error.is_empty());
}

#[test]
fn execute_rop_debug_summary_skips_release_rops_without_responses() {
    let request = RopRequest {
        rop_id: 0x7F,
        input_handle_index: Some(1),
        output_handle_index: None,
        payload: 2u32.to_le_bytes().to_vec(),
    };
    let response_buffer =
        rop_buffer_with_response(rop_get_local_replica_ids_response(&request, 42), &[42]);
    let response_summary = summarize_response_rop_buffer(&response_buffer, &[0x01, 0x7F]);

    assert_eq!(response_summary.ids_csv, "0x7f");
    assert_eq!(response_summary.results_csv, "0x7f:0x00000000");
    assert_eq!(response_summary.count, 1);
    assert_eq!(response_summary.handle_count, 1);
    assert!(response_summary.parse_error.is_empty());
}

#[test]
fn execute_rop_response_summary_keeps_get_address_types_frame_boundary() {
    let address_types_request = RopRequest {
        rop_id: 0x49,
        input_handle_index: Some(0),
        output_handle_index: Some(0),
        payload: Vec::new(),
    };
    let open_folder_request = RopRequest {
        rop_id: 0x02,
        input_handle_index: Some(1),
        output_handle_index: Some(2),
        payload: Vec::new(),
    };
    let mut responses = rop_get_address_types_response(&address_types_request);
    responses.extend_from_slice(&rop_open_folder_response(&open_folder_request, false));
    responses.extend_from_slice(&[0x07, 0x02, 0, 0, 0, 0, 1, 1, 0, 0, 0]);

    let response_buffer =
        rpc_header_ext_rop_buffer(rop_buffer_with_response_spec(responses, &[1, 5, 20]));
    let response_summary = summarize_response_rop_buffer(&response_buffer, &[0x49, 0x02, 0x07]);

    assert_eq!(response_summary.ids_csv, "0x49,0x02,0x07");
    assert_eq!(
        response_summary.results_csv,
        "0x49:0x00000000,0x02:0x00000000,0x07:0x00000000"
    );
    assert!(response_summary
        .frames
        .contains("0x49@0..18:len=18:out=0:rv=0x00000000"));
    assert!(response_summary
        .frames
        .contains("0x02@18..26:len=8:out=2:rv=0x00000000"));
    assert!(response_summary
        .frames
        .contains("0x07@26..37:len=11:out=2:rv=0x00000000"));
    assert!(response_summary.parse_error.is_empty());
}

#[test]
fn execute_rop_response_summary_keeps_get_property_ids_frame_boundary() {
    let property_ids_request = RopRequest {
        rop_id: 0x56,
        input_handle_index: Some(0),
        output_handle_index: Some(0),
        payload: Vec::new(),
    };
    let open_folder_request = RopRequest {
        rop_id: 0x02,
        input_handle_index: Some(1),
        output_handle_index: Some(2),
        payload: Vec::new(),
    };
    let mut responses =
        rop_get_property_ids_from_names_response(&property_ids_request, &[0x8003, 0x8004]);
    responses.extend_from_slice(&rop_open_folder_response(&open_folder_request, false));
    responses.extend_from_slice(&[0x07, 0x02, 0, 0, 0, 0, 1, 1, 0, 0, 0]);

    let response_buffer =
        rpc_header_ext_rop_buffer(rop_buffer_with_response_spec(responses, &[13, 4, 17]));
    let response_summary = summarize_response_rop_buffer(&response_buffer, &[0x56, 0x02, 0x07]);

    assert_eq!(response_summary.ids_csv, "0x56,0x02,0x07");
    assert_eq!(
        response_summary.results_csv,
        "0x56:0x00000000,0x02:0x00000000,0x07:0x00000000"
    );
    assert!(response_summary
        .frames
        .contains("0x56@0..12:len=12:out=0:rv=0x00000000"));
    assert!(response_summary
        .frames
        .contains("0x02@12..20:len=8:out=2:rv=0x00000000"));
    assert!(response_summary
        .frames
        .contains("0x07@20..31:len=11:out=2:rv=0x00000000"));
    assert!(response_summary.parse_error.is_empty());
}

#[test]
fn execute_rop_response_summary_keeps_contents_table_frame_boundary() {
    let table_request = RopRequest {
        rop_id: 0x05,
        input_handle_index: Some(0),
        output_handle_index: Some(1),
        payload: vec![0x02],
    };
    let set_columns_request = RopRequest {
        rop_id: 0x12,
        input_handle_index: Some(1),
        output_handle_index: None,
        payload: Vec::new(),
    };
    let sort_table_request = RopRequest {
        rop_id: 0x13,
        input_handle_index: Some(1),
        output_handle_index: None,
        payload: Vec::new(),
    };
    let seek_row_response = vec![0x18, 0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    let mut responses = rop_get_contents_table_response(&table_request, 0x12);
    responses.extend_from_slice(&rop_set_columns_response(&set_columns_request));
    responses.extend_from_slice(&rop_sort_table_response(&sort_table_request));
    responses.extend_from_slice(&seek_row_response);
    responses.extend_from_slice(&rop_get_contents_table_response(&table_request, 13));

    let response_buffer =
        rpc_header_ext_rop_buffer(rop_buffer_with_response_spec(responses, &[42, 43]));
    let response_summary =
        summarize_response_rop_buffer(&response_buffer, &[0x05, 0x12, 0x13, 0x18, 0x05]);

    assert_eq!(response_summary.ids_csv, "0x05,0x12,0x13,0x18,0x05");
    assert_eq!(
        response_summary.results_csv,
        "0x05:0x00000000,0x12:0x00000000,0x13:0x00000000,0x18:0x00000000,0x05:0x00000000"
    );
    assert!(response_summary
        .frames
        .contains("0x05@0..10:len=10:out=1:rv=0x00000000"));
    assert!(response_summary
        .frames
        .contains("0x12@10..17:len=7:out=1:rv=0x00000000"));
    assert!(response_summary
        .frames
        .contains("0x13@17..24:len=7:out=1:rv=0x00000000"));
    assert!(response_summary
        .frames
        .contains("0x18@24..35:len=11:out=1:rv=0x00000000"));
    assert!(response_summary
        .frames
        .contains("0x05@35..45:len=10:out=1:rv=0x00000000"));
    assert!(response_summary.parse_error.is_empty());
}

#[test]
fn execute_rop_response_summary_skips_implausible_query_rows_payload_marker() {
    let table_request = RopRequest {
        rop_id: 0x05,
        input_handle_index: Some(0),
        output_handle_index: Some(1),
        payload: vec![0x02],
    };
    let set_columns_request = RopRequest {
        rop_id: 0x12,
        input_handle_index: Some(1),
        output_handle_index: None,
        payload: Vec::new(),
    };
    let sort_table_request = RopRequest {
        rop_id: 0x13,
        input_handle_index: Some(1),
        output_handle_index: None,
        payload: Vec::new(),
    };
    let seek_row_response = vec![0x18, 0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    let mut find_row_response = vec![0x4f, 0x01, 0, 0, 0, 0, 0, 1];
    find_row_response.extend_from_slice(&[0x01, 0x00, 0x15, 0x49, 0x00, 0x50, 0x00, 0x46]);
    let query_rows_response = vec![0x15, 0x03, 0, 0, 0, 0, 0, 0, 0];
    let mut responses = rop_get_contents_table_response(&table_request, 2);
    responses.extend_from_slice(&rop_set_columns_response(&set_columns_request));
    responses.extend_from_slice(&rop_sort_table_response(&sort_table_request));
    responses.extend_from_slice(&seek_row_response);
    responses.extend_from_slice(&find_row_response);
    responses.extend_from_slice(&query_rows_response);

    let response_buffer =
        rpc_header_ext_rop_buffer(rop_buffer_with_response_spec(responses, &[1, 2, 3, 4]));
    let response_summary =
        summarize_response_rop_buffer(&response_buffer, &[0x05, 0x12, 0x13, 0x18, 0x4f, 0x15]);

    assert_eq!(response_summary.ids_csv, "0x05,0x12,0x13,0x18,0x4f,0x15");
    assert_eq!(
            response_summary.results_csv,
            "0x05:0x00000000,0x12:0x00000000,0x13:0x00000000,0x18:0x00000000,0x4f:0x00000000,0x15:0x00000000"
        );
    assert!(response_summary
        .frames
        .contains("0x4f@35..51:len=16:out=1:rv=0x00000000"));
    assert!(response_summary
        .frames
        .contains("0x15@51..60:len=9:out=3:rv=0x00000000"));
    assert!(!response_summary.results_csv.contains("0x15:0x46005000"));
    assert!(response_summary.parse_error.is_empty());
}

#[test]
fn execute_rop_response_summary_keeps_create_setprops_save_frame_boundary() {
    let mut responses = vec![0x06, 0x02];
    responses.extend_from_slice(&0u32.to_le_bytes());
    responses.push(0);
    responses.extend_from_slice(&[0x29, 0x03]);
    responses.extend_from_slice(&0u32.to_le_bytes());
    responses.extend_from_slice(&[0x0a, 0x04]);
    responses.extend_from_slice(&0u32.to_le_bytes());
    responses.extend_from_slice(&0u16.to_le_bytes());
    responses.extend_from_slice(&[0x07, 0x04]);
    responses.extend_from_slice(&0u32.to_le_bytes());
    responses.extend_from_slice(&1u32.to_le_bytes());
    responses.push(0);
    responses.extend_from_slice(&[0x0a, 0x04]);
    responses.extend_from_slice(&0u32.to_le_bytes());
    responses.extend_from_slice(&1u16.to_le_bytes());
    responses.extend_from_slice(&1u16.to_le_bytes());
    responses.extend_from_slice(&PID_TAG_NORMALIZED_SUBJECT_W.to_le_bytes());
    responses.extend_from_slice(&0x8004_0102u32.to_le_bytes());
    responses.extend_from_slice(&[0x0c, 0x05]);
    responses.extend_from_slice(&0u32.to_le_bytes());
    responses.push(4);
    responses.extend_from_slice(&0x0000_0000_0000_1234u64.to_le_bytes());

    let response_buffer =
        rpc_header_ext_rop_buffer(rop_buffer_with_response_spec(responses, &[1, 4, 5, 6]));
    let response_summary = summarize_response_rop_buffer(
        &response_buffer,
        &[0x01, 0x06, 0x29, 0x0a, 0x07, 0x0a, 0x0c],
    );

    assert_eq!(response_summary.ids_csv, "0x06,0x29,0x0a,0x07,0x0a,0x0c");
    assert_eq!(
            response_summary.results_csv,
            "0x06:0x00000000,0x29:0x00000000,0x0a:0x00000000,0x07:0x00000000,0x0a:0x00000000,0x0c:0x00000000"
        );
    assert!(response_summary
        .frames
        .contains("0x06@0..7:len=7:out=2:rv=0x00000000"));
    assert!(response_summary
        .frames
        .contains("0x29@7..13:len=6:out=3:rv=0x00000000"));
    assert!(response_summary
        .frames
        .contains("0x0a@13..21:len=8:out=4:rv=0x00000000"));
    assert!(response_summary
        .frames
        .contains("0x07@21..32:len=11:out=4:rv=0x00000000"));
    assert!(response_summary
        .frames
        .contains("0x0a@32..50:len=18:out=4:rv=0x00000000"));
    assert!(response_summary
        .frames
        .contains("0x0c@50..65:len=15:out=5:rv=0x00000000"));
    assert!(response_summary.parse_error.is_empty());
}

#[test]
fn execute_rop_response_summary_does_not_treat_find_row_payload_as_next_rop() {
    let table_request = RopRequest {
        rop_id: 0x05,
        input_handle_index: Some(0),
        output_handle_index: Some(2),
        payload: vec![0x02],
    };
    let set_columns_request = RopRequest {
        rop_id: 0x12,
        input_handle_index: Some(2),
        output_handle_index: None,
        payload: Vec::new(),
    };

    let mut responses = vec![0x4F, 0x01];
    responses.extend_from_slice(&0u32.to_le_bytes());
    responses.push(0);
    responses.push(1);
    responses.extend_from_slice(&[
        0x00, 0x01, 0x00, 0x01, 0x05, 0x01, 0x00, 0x7f, 0xff, 0x00, 0x44, 0x55,
    ]);
    let find_row_end = responses.len();
    responses.extend_from_slice(&rop_get_contents_table_response(&table_request, 3));
    responses.extend_from_slice(&rop_set_columns_response(&set_columns_request));

    let response_buffer =
        rpc_header_ext_rop_buffer(rop_buffer_with_response_spec(responses, &[42, 43]));
    let response_summary = summarize_response_rop_buffer(&response_buffer, &[0x4F, 0x05, 0x12]);

    assert_eq!(response_summary.ids_csv, "0x4f,0x05,0x12");
    assert_eq!(
        response_summary.results_csv,
        "0x4f:0x00000000,0x05:0x00000000,0x12:0x00000000"
    );
    assert!(response_summary.frames.contains(&format!(
        "0x4f@0..{find_row_end}:len={find_row_end}:out=1:rv=0x00000000"
    )));
    assert!(!response_summary.results_csv.contains("0xffff7f00"));
    assert!(response_summary.parse_error.is_empty());
}

#[test]
fn execute_rop_response_summary_skips_implausible_getprops_payload_rop_marker() {
    let mut responses = vec![0x07, 0x01];
    responses.extend_from_slice(&0u32.to_le_bytes());
    responses.extend_from_slice(&1u16.to_le_bytes());
    responses.extend_from_slice(&OUTLOOK_COMMON_VIEW_DESCRIPTOR_BINARY_6835.to_le_bytes());
    responses.extend_from_slice(&8u32.to_le_bytes());
    responses.extend_from_slice(&[0x01, 0x02, 0x07, 0x74, 0x1f, 0x6f, 0xd3, 0x03]);
    let first_getprops_end = responses.len();
    responses.extend_from_slice(&[0x07, 0x01]);
    responses.extend_from_slice(&0u32.to_le_bytes());
    responses.extend_from_slice(&1u16.to_le_bytes());
    responses.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    responses.extend_from_slice(&0u16.to_le_bytes());

    let response_buffer =
        rpc_header_ext_rop_buffer(rop_buffer_with_response_spec(responses, &[0x0000_0001]));
    let response_summary = summarize_response_rop_buffer(&response_buffer, &[0x07, 0x07]);

    assert_eq!(response_summary.ids_csv, "0x07,0x07");
    assert_eq!(
        response_summary.results_csv,
        "0x07:0x00000000,0x07:0x00000000"
    );
    assert!(response_summary.frames.contains(&format!(
        "0x07@0..{first_getprops_end}:len={first_getprops_end}:out=1:rv=0x00000000"
    )));
    assert!(!response_summary.results_csv.contains("0xd36f1f74"));
    assert!(response_summary.parse_error.is_empty());
}

#[test]
fn execute_rop_response_summary_skips_bare_warning_getprops_payload_marker() {
    let mut responses = vec![0x4F, 0x01];
    responses.extend_from_slice(&0u32.to_le_bytes());
    responses.push(0);
    responses.push(1);
    responses.extend_from_slice(&[0x00, 0x01, 0x07, 0x00, 0x00, 0x00, 0x04, 0x00]);
    let find_row_end = responses.len();
    responses.extend_from_slice(&[0x07, 0x02]);
    responses.extend_from_slice(&0u32.to_le_bytes());
    responses.push(0);

    let response_buffer =
        rpc_header_ext_rop_buffer(rop_buffer_with_response_spec(responses, &[0x0000_0001]));
    let response_summary = summarize_response_rop_buffer(&response_buffer, &[0x4F, 0x07]);

    assert_eq!(response_summary.ids_csv, "0x4f,0x07");
    assert_eq!(
        response_summary.results_csv,
        "0x4f:0x00000000,0x07:0x00000000"
    );
    assert!(response_summary.frames.contains(&format!(
        "0x4f@0..{find_row_end}:len={find_row_end}:out=1:rv=0x00000000"
    )));
    assert!(!response_summary.results_csv.contains("0x07:0x00040000"));
    assert!(response_summary.parse_error.is_empty());
}

#[test]
fn execute_rop_response_framing_summary_marks_multi_rop_boundaries() {
    let mut responses = Vec::new();
    responses.push(0x02);
    responses.push(1);
    responses.extend_from_slice(&0u32.to_le_bytes());
    responses.push(0);
    responses.push(0);
    for rop_id in [0x70, 0x75, 0x77, 0x75, 0x77] {
        responses.push(rop_id);
        responses.push(1);
        responses.extend_from_slice(&0u32.to_le_bytes());
    }
    responses.push(0x4E);
    responses.push(2);
    responses.extend_from_slice(&0u32.to_le_bytes());
    responses.extend_from_slice(&0x0003u16.to_le_bytes());
    responses.extend_from_slice(&1u16.to_le_bytes());
    responses.extend_from_slice(&1u16.to_le_bytes());
    responses.push(0);
    responses.extend_from_slice(&4u16.to_le_bytes());
    responses.extend_from_slice(&[0x03, 0x00, 0x14, 0x40]);

    let response_buffer =
        rpc_header_ext_rop_buffer(rop_buffer_with_response_spec(responses, &[1, 4, 3]));
    let response_summary = summarize_response_rop_buffer(
        &response_buffer,
        &[0x02, 0x70, 0x75, 0x77, 0x75, 0x77, 0x4E],
    );

    assert_eq!(response_summary.buffer_layout, "rpc_header_ext_spec");
    assert_eq!(response_summary.response_payload_bytes, 57);
    assert_eq!(response_summary.handle_table_bytes, 12);
    assert_eq!(response_summary.count, 7);
    assert_eq!(
            response_summary.results_csv,
            "0x02:0x00000000,0x70:0x00000000,0x75:0x00000000,0x77:0x00000000,0x75:0x00000000,0x77:0x00000000,0x4e:0x00000000"
        );
    assert!(response_summary
        .frames
        .contains("0x02@0..8:len=8:out=1:rv=0x00000000"));
    assert!(response_summary
        .frames
        .contains("0x4e@38..57:len=19:out=2:rv=0x00000000"));
    assert!(response_summary.parse_error.is_empty());
}

#[test]
fn execute_response_framing_context_includes_bootstrap_getprops_batches() {
    assert_eq!(
        execute_response_framing_context(&[0x07]),
        Some("getprops_or_release_getprops")
    );
    assert_eq!(
        execute_response_framing_context(&[0x01, 0x07]),
        Some("getprops_or_release_getprops")
    );
    assert_eq!(
        execute_response_framing_context(&[0x01, 0x01]),
        Some("release_only")
    );
    assert_eq!(
        execute_response_framing_context(&[0x02, 0x70, 0x4E]),
        Some("hierarchy_sync")
    );
    assert_eq!(
        execute_response_framing_context(&[0x49, 0x02, 0x07]),
        Some("named_props_openfolder_getprops")
    );
    assert_eq!(
        execute_response_framing_context(&[0x56, 0x02, 0x07]),
        Some("named_props_openfolder_getprops")
    );
    assert_eq!(
        execute_response_framing_context(&[0x05, 0x12, 0x13, 0x18, 0x4F]),
        Some("contents_table_probe")
    );
    assert_eq!(
        execute_response_framing_context(&[
            0x05, 0x12, 0x13, 0x18, 0x4F, 0x56, 0x05, 0x12, 0x13, 0x4F,
        ]),
        Some("contents_table_probe")
    );
    assert_eq!(
        execute_response_framing_context(&[
            0x05, 0x12, 0x13, 0x18, 0x4F, 0x56, 0x04, 0x12, 0x15, 0x29, 0x07, 0x14,
        ]),
        Some("contents_table_batch")
    );
    assert_eq!(
        execute_response_framing_context(&[0x12, 0x01, 0x01, 0x01]),
        Some("setcolumns_release_batch")
    );
    assert_eq!(
        execute_response_framing_context(&[0x01, 0x02, 0x07]),
        Some("openfolder_getprops_probe")
    );
    assert_eq!(execute_response_framing_context(&[0x0A]), Some("setprops"));
    assert_eq!(execute_response_framing_context(&[0x79]), Some("setprops"));
    assert_eq!(
        execute_response_framing_context(&[0x03, 0x07, 0x01, 0x0a]),
        Some("open_message_getprops_setprops")
    );
    assert_eq!(
        execute_response_framing_context(&[0x01, 0x06, 0x29, 0x0a, 0x07, 0x0a, 0x0c]),
        Some("create_message_setprops_save")
    );
    assert_eq!(
        execute_response_framing_context(&[0x02, 0x07]),
        Some("openfolder_getprops_probe")
    );
}
