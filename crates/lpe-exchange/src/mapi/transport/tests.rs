use super::*;
use crate::mapi::transport::diagnostics::post_hierarchy_close_kind;
use crate::mapi::wire::RopId;

fn test_session(handles: HashMap<u32, MapiObject>) -> MapiSession {
    MapiSession {
        endpoint: MapiEndpoint::Emsmdb,
        tenant_id: Uuid::from_u128(0xaaaaaaaa_aaaa_aaaa_aaaa_aaaaaaaaaaaa),
        account_id: Uuid::from_u128(0xbbbbbbbb_bbbb_bbbb_bbbb_bbbbbbbbbbbb),
        email: "user@example.test".to_string(),
        created_at: SystemTime::now(),
        last_seen_at: SystemTime::now(),
        first_request_type: "Connect".to_string(),
        first_request_id: "test:1".to_string(),
        last_request_type: "Connect".to_string(),
        last_request_id: "test:1".to_string(),
        request_count: 1,
        execute_request_count: 0,
        next_handle: 1,
        handles,
        message_statuses: HashMap::new(),
        message_save_generations: HashMap::new(),
        message_handle_generations: HashMap::new(),
        pending_message_recipient_replacements: HashMap::new(),
        pending_message_attachments: HashMap::new(),
        pending_attachment_parent_messages: HashMap::new(),
        pending_attachment_deletions: HashSet::new(),
        pending_embedded_message_ids: HashMap::new(),
        pending_embedded_message_attachments: HashMap::new(),
        saved_embedded_messages: HashMap::new(),
        saved_search_folder_definitions: HashMap::new(),
        special_folder_aliases: HashMap::new(),
        deleted_advertised_special_folders: HashSet::new(),
        deleted_search_folder_definitions: HashSet::new(),
        named_properties: HashMap::new(),
        named_property_ids: HashMap::new(),
        next_named_property_id: crate::mapi::properties::FIRST_NAMED_PROPERTY_ID,
        next_local_replica_sequence: 1,
        notification_cursor: None,
        pending_notifications: VecDeque::new(),
        completed_execute_requests: HashMap::new(),
        completed_execute_request_order: VecDeque::new(),
        post_hierarchy_actions: PostHierarchyActionState::default(),
        inbox_associated_config_stream_handles: HashSet::new(),
        inbox_rule_organizer_stream_handles: HashSet::new(),
        logon_identity: None,
        outlook_smart_input_variant: "none".to_string(),
        outlook_smart_input_variant_applied: false,
    }
}

pub(in crate::mapi) fn test_session_for_outlook_startup() -> MapiSession {
    test_session(HashMap::new())
}

fn test_principal() -> AccountPrincipal {
    AccountPrincipal {
        tenant_id: Uuid::from_u128(0xaaaaaaaa_aaaa_aaaa_aaaa_aaaaaaaaaaaa),
        account_id: Uuid::from_u128(0xbbbbbbbb_bbbb_bbbb_bbbb_bbbbbbbbbbbb),
        email: "user@example.test".to_string(),
        display_name: "User".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    }
}

#[test]
fn request_type_recognizes_get_hierarchy_info_as_nspi_request() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-requesttype",
        HeaderValue::from_static("GetHierarchyInfo"),
    );

    let request_type = request_type(&headers).unwrap();

    assert_eq!(request_type, MapiRequestType::GetHierarchyInfo);
    assert!(request_type.requires_nspi_session());
    assert_eq!(request_type.header_value(), "GetHierarchyInfo");
}

#[test]
fn connect_body_debug_summary_decodes_fields() {
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    write_u32(&mut body, 60_000);
    write_u32(&mut body, 6);
    write_u32(&mut body, 10_000);
    body.extend_from_slice(b"/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn=\0");
    write_utf16z(&mut body, "Alice");
    let auxiliary_buffer = connect_auxiliary_buffer();
    write_u32(&mut body, auxiliary_buffer.len() as u32);
    body.extend_from_slice(&auxiliary_buffer);

    let summary = summarize_connect_body(&body);

    assert_eq!(summary.status_code, 0);
    assert_eq!(summary.error_code, 0);
    assert_eq!(summary.polls_max, 60_000);
    assert_eq!(summary.retry_count, 6);
    assert_eq!(summary.retry_delay_ms, 10_000);
    assert_eq!(
        summary.dn_prefix,
        "/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn="
    );
    assert_eq!(summary.display_name, "Alice");
    assert_eq!(
        summary.auxiliary_buffer_bytes,
        auxiliary_buffer.len() as u32
    );
    assert!(summary.parse_error.is_empty());
}

#[test]
fn mapi_http_date_formats_imf_fixdate_in_gmt() {
    assert_eq!(
        mapi_http_date(SystemTime::UNIX_EPOCH + Duration::from_secs(0)),
        "Thu, 01 Jan 1970 00:00:00 GMT"
    );
    assert_eq!(
        mapi_http_date(SystemTime::UNIX_EPOCH + Duration::from_secs(1_780_144_640)),
        "Sat, 30 May 2026 12:37:20 GMT"
    );
}

#[tokio::test]
async fn mapi_response_start_time_uses_current_http_date_not_sentinel() {
    let response = mapi_response("Execute", "request:1", 0, Vec::new(), None);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();

    assert!(body.contains("\r\nX-StartTime: "));
    assert!(body.contains(" GMT\r\n\r\n"));
    assert!(!body.contains("Mon, 01 Jan 2001 00:00:00 GMT"));
}

#[tokio::test]
async fn notification_wait_empty_response_reports_success_with_empty_body() {
    let response = notification_wait_empty_response(MapiEndpoint::Emsmdb, "request:43", "abc");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response_header(&response, "x-requesttype").unwrap(),
        "NotificationWait"
    );
    assert_eq!(response_header(&response, "x-responsecode").unwrap(), "0");
    let set_cookies = response
        .headers()
        .get_all("set-cookie")
        .iter()
        .map(|value| value.to_str().unwrap().to_string())
        .collect::<Vec<_>>();
    assert_eq!(set_cookies.len(), 2);
    assert!(set_cookies
        .iter()
        .any(|cookie| cookie.starts_with("MapiContext=abc")));
    assert!(set_cookies
        .iter()
        .any(|cookie| cookie.starts_with("MapiSequence=abc")));

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert!(body.starts_with(b"PROCESSING\r\nDONE\r\nX-ResponseCode: 0\r\n"));
    assert!(body.ends_with(&[0; 16]));
}

#[test]
fn notification_wait_empty_delay_uses_long_poll_when_subscription_exists() {
    let short_session = test_session(HashMap::new());

    assert_eq!(
        notification_wait_empty_delay_millis(&short_session),
        MAPI_NOTIFICATION_WAIT_EMPTY_DELAY_MILLIS
    );

    let mut logged_on_session = test_session(HashMap::new());
    logged_on_session.logon_identity = Some(MapiLogonIdentityDebug::default());

    assert_eq!(
        notification_wait_empty_delay_millis(&logged_on_session),
        MAPI_NOTIFICATION_WAIT_SUBSCRIPTION_EMPTY_DELAY_MILLIS
    );

    let mut handles = HashMap::new();
    handles.insert(
        7,
        MapiObject::NotificationSubscription {
            registration: MapiNotificationRegistration {
                notification_types: 0x0078,
                folder_id: Some(crate::mapi::identity::INBOX_FOLDER_ID),
            },
        },
    );
    let subscription_session = test_session(handles);

    assert_eq!(
        notification_wait_empty_delay_millis(&subscription_session),
        MAPI_NOTIFICATION_WAIT_SUBSCRIPTION_EMPTY_DELAY_MILLIS
    );
}

#[tokio::test]
async fn notification_wait_active_session_acquire_waits_for_short_outlook_overlap() {
    let session_id = "notification-overlap-session".to_string();
    let active = begin_active_session_request(&session_id).unwrap();
    let release = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        drop(active);
    });

    let acquired = acquire_notification_wait_active_session_request(&session_id).await;

    assert!(acquired.is_some());
    drop(acquired);
    release.await.unwrap();
    assert!(!session_request_is_active(&session_id));
}

#[test]
fn session_cookie_lookup_debug_reports_sanitized_latest_cookie_selection() {
    let principal = test_principal();
    let session_id = create_session(MapiEndpoint::Emsmdb, &principal, "Connect", "test:1");
    let stale_id = "00000000-0000-0000-0000-000000000000";
    let mut headers = HeaderMap::new();
    headers.append(
        "cookie",
        HeaderValue::from_str(&format!("MapiContext={stale_id}; MapiSequence={stale_id}")).unwrap(),
    );
    headers.append(
        "cookie",
        HeaderValue::from_str(&format!(
            "MapiContext={session_id}; MapiSequence={session_id}"
        ))
        .unwrap(),
    );

    let summary = session_cookie_lookup_debug(MapiEndpoint::Emsmdb, &principal, &headers);

    assert_eq!(summary.cookie_header_count, 2);
    assert_eq!(summary.context_candidate_count, 2);
    assert_eq!(summary.sequence_candidate_count, 2);
    assert_eq!(
        summary.selected_context.suffix,
        cookie_value_suffix(&session_id)
    );
    assert_eq!(
        summary.selected_sequence.suffix,
        cookie_value_suffix(&session_id)
    );
    assert_eq!(
        summary.selected_context.hash,
        format!("{:016x}", mapi_payload_fingerprint(session_id.as_bytes()))
    );
    assert_eq!(summary.selected_context.hash.len(), 16);
    assert_ne!(summary.selected_context.hash, session_id);
    assert_ne!(summary.selected_sequence.hash, session_id);
    assert!(summary.selected_session_exists);
    assert!(summary.selected_session_endpoint_matches);
    assert!(summary.selected_session_principal_matches);
    remove_session(&session_id);
}

#[test]
fn session_cookie_lookup_debug_reports_endpoint_and_principal_mismatch() {
    let principal = test_principal();
    let session_id = create_session(MapiEndpoint::Nspi, &principal, "Bind", "test:1");
    let mut headers = HeaderMap::new();
    headers.insert(
        "cookie",
        HeaderValue::from_str(&format!(
            "MapiContext={session_id}; MapiSequence={session_id}"
        ))
        .unwrap(),
    );

    let summary = session_cookie_lookup_debug(MapiEndpoint::Emsmdb, &principal, &headers);

    assert!(summary.selected_session_exists);
    assert!(!summary.selected_session_endpoint_matches);
    assert!(summary.selected_session_principal_matches);
    remove_session(&session_id);

    let session_id = create_session(MapiEndpoint::Emsmdb, &principal, "Connect", "test:1");
    let other_principal = AccountPrincipal {
        account_id: Uuid::from_u128(0xcccccccc_cccc_cccc_cccc_cccccccccccc),
        email: "other@example.test".to_string(),
        ..principal
    };
    let mut headers = HeaderMap::new();
    headers.insert(
        "cookie",
        HeaderValue::from_str(&format!(
            "MapiContext={session_id}; MapiSequence={session_id}"
        ))
        .unwrap(),
    );

    let summary = session_cookie_lookup_debug(MapiEndpoint::Emsmdb, &other_principal, &headers);

    assert!(summary.selected_session_exists);
    assert!(summary.selected_session_endpoint_matches);
    assert!(!summary.selected_session_principal_matches);
    remove_session(&session_id);
}

#[test]
fn post_hierarchy_action_summary_stays_empty_before_completed_hierarchy() {
    let mut session = test_session(HashMap::new());

    session.record_execute_after_hierarchy_completion(
        &[0x01, 0x70],
        "Release,SynchronizationImportMessageChange",
    );
    let summary = post_hierarchy_action_summary(&session, true);

    assert_eq!(summary.execute_count, 0);
    assert_eq!(summary.rop_ids_seen, "");
    assert!(!summary.content_sync_configure_observed);
    assert!(!summary.release_client_initiated);
    assert!(!summary.logoff_client_initiated);
    assert!(!summary.disconnect_client_initiated);
    assert_eq!(summary.close_kind, "post_hierarchy_no_close");
    assert_eq!(summary.last_completed_hierarchy_sync_root, "");
    assert_eq!(summary.last_successful_hierarchy_get_buffer_summary, "");
}

#[test]
fn post_hierarchy_action_summary_records_execute_rops_and_client_actions() {
    let mut session = test_session(HashMap::new());

    session.record_completed_hierarchy_sync(
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
        "folder=0x0000000000040001;status=0x0003".to_string(),
        "calendar:row_present=true".to_string(),
    );
    let first = session.record_execute_after_hierarchy_completion(
        &[0x02, 0x70, 0x4e],
        "OpenFolder,SynchronizationImportMessageChange,OpenStream",
    );
    let second = session.record_execute_after_hierarchy_completion(
        &[0x01, 0x70],
        "Release,SynchronizationImportMessageChange",
    );
    session.record_content_sync_configure();
    session.record_logoff_after_hierarchy_completion();
    let summary = post_hierarchy_action_summary(&session, true);

    assert!(first.first_execute);
    assert!(first.first_bootstrap_probe);
    assert!(!first.first_set_properties_probe);
    assert!(!second.first_execute);
    assert!(!second.first_bootstrap_probe);
    assert!(!second.first_set_properties_probe);
    assert_eq!(summary.execute_count, 2);
    assert_eq!(summary.rop_ids_seen, "0x02,0x70,0x4e,0x01");
    assert!(summary.content_sync_configure_observed);
    assert!(summary.release_client_initiated);
    assert!(summary.logoff_client_initiated);
    assert!(summary.disconnect_client_initiated);
    assert_eq!(summary.close_kind, "post_hierarchy_content_sync_observed");
    assert_eq!(
        summary.last_completed_hierarchy_sync_root,
        format!("0x{:016x}", crate::mapi::identity::IPM_SUBTREE_FOLDER_ID)
    );
    assert_eq!(
        summary.last_successful_hierarchy_get_buffer_summary,
        "folder=0x0000000000040001;status=0x0003"
    );
    assert_eq!(
        summary.last_default_folder_hierarchy_membership_summary,
        "calendar:row_present=true"
    );
}

#[test]
fn post_hierarchy_action_summary_records_last_request_contracts() {
    let mut session = test_session(HashMap::new());

    session.record_completed_hierarchy_sync(
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
        "folder=0x0000000000040001;status=0x0003".to_string(),
        "calendar:row_present=true".to_string(),
    );
    session.record_post_hierarchy_request_contract(
        "GetReceiveFolder(message_class=IPM;folder=0x0000000000060001)->ok".to_string(),
    );
    session.record_post_hierarchy_getprops_contract(
            "GetPropertiesSpecific(kind=folder;folder=0x0000000000100001;probe=calendar_probe;tags=0x3613001f;returned_tags=0x3613001f)".to_string(),
        );
    session.record_post_hierarchy_request_contract(
        "GetPropertiesSpecific(calendar props)->ok".to_string(),
    );
    session.record_post_hierarchy_setprops_contract(
            "SetProperties(kind=folder;folder=0x0000000000010001;probe=root_default_folder_bootstrap;tags=0x36d00102;write_mode=ignored_canonical_projection)".to_string(),
        );
    session.record_post_hierarchy_request_contract("SetProperties(root defaults)->ok".to_string());

    let summary = post_hierarchy_action_summary(&session, true);

    assert!(summary
        .last_getprops_request_contract
        .contains("probe=calendar_probe"));
    assert!(summary
        .last_setprops_request_contract
        .contains("write_mode=ignored_canonical_projection"));
    assert_eq!(
            summary.request_contract_sequence,
            "1:GetReceiveFolder(message_class=IPM;folder=0x0000000000060001)->ok|2:GetPropertiesSpecific(calendar props)->ok|3:SetProperties(root defaults)->ok"
        );
}

#[test]
fn partial_scope_checkpoint_not_stored_count_counts_expected_partial_scope_summaries() {
    let mut session = test_session(HashMap::new());

    session.record_completed_sync_checkpoint(
        crate::mapi::identity::TRASH_FOLDER_ID,
        "trash",
        "IPF.Note",
        "content",
        0x01,
        "ok_partial_scope_no_checkpoint",
    );
    session.record_completed_sync_checkpoint(
        crate::mapi::identity::CALENDAR_FOLDER_ID,
        "calendar",
        "IPF.Appointment",
        "content",
        0x01,
        "ok",
    );

    assert_eq!(
        partial_scope_checkpoint_not_stored_count(&session.post_hierarchy_actions),
        1
    );
}

#[test]
fn post_fai_inbox_probe_loop_terminal_summary_requires_no_normal_contents() {
    let mut state = PostHierarchyActionState {
        post_inbox_fai_folder_type_probe_loop_logged: true,
        inbox_associated_contents_table_observed: true,
        inbox_open_folder_probe_count: 4,
        inbox_folder_type_getprops_probe_count: 4,
        last_inbox_open_folder_context: "output_handle=19".to_string(),
        last_inbox_folder_type_getprops_context: "folder_type=1".to_string(),
        last_inbox_associated_query_context: "returned=6".to_string(),
        last_inbox_related_release_context: "handle=13;role=ipm_subtree".to_string(),
        recent_probe_actions: vec![
            "OpenFolder(in=1,handle=8,out=19,folder=0x0000000000050001)".to_string(),
            "GetPropertiesSpecific(in=2,handle=19,tags=0x36010003)".to_string(),
        ],
        ..PostHierarchyActionState::default()
    };

    let summary = post_fai_inbox_probe_loop_terminal_summary(&state).unwrap();

    assert!(summary.contains("open_folder_count=4"));
    assert!(summary.contains("folder_type_getprops_count=4"));
    assert!(summary.contains("normal_contents_table_observed=false"));
    assert!(summary.contains("last_folder_type_getprops=folder_type=1"));
    assert!(summary
        .contains("next_expected_client_step=open_inbox_normal_contents_table_or_sync_configure"));

    state.inbox_normal_contents_table_observed = true;

    assert!(post_fai_inbox_probe_loop_terminal_summary(&state).is_none());
}

#[test]
fn outlook_bootstrap_phase_classifies_current_wall_and_successful_progress() {
    let mut state = PostHierarchyActionState {
        post_inbox_fai_handoff_logged: true,
        post_inbox_fai_reopen_logged: true,
        post_inbox_fai_folder_type_probe_loop_logged: true,
        inbox_associated_contents_table_observed: true,
        inbox_open_folder_probe_count: 6,
        inbox_folder_type_getprops_probe_count: 6,
        last_inbox_hierarchy_query_context: "rows=16".to_string(),
        ..PostHierarchyActionState::default()
    };

    assert_eq!(outlook_bootstrap_phase(&state), 7);
    assert_eq!(
        outlook_bootstrap_phase_name(outlook_bootstrap_phase(&state)),
        "repeated_inbox_folder_type_probe_loop"
    );
    assert_eq!(outlook_bootstrap_stall_code(&state), 3);
    assert_eq!(
        outlook_bootstrap_stall_name(outlook_bootstrap_stall_code(&state)),
        "repeated_inbox_folder_type_probe_without_contents"
    );
    assert_eq!(
        outlook_bootstrap_next_expected_phase(&state),
        "open_inbox_normal_contents_table_or_sync_configure"
    );

    state.inbox_normal_contents_table_observed = true;

    assert_eq!(outlook_bootstrap_phase(&state), 8);
    assert_eq!(
        outlook_bootstrap_phase_name(outlook_bootstrap_phase(&state)),
        "inbox_normal_contents_table_opened"
    );
}

#[test]
fn outlook_bootstrap_stall_classifies_post_common_views_notification_handoff() {
    let mut state = PostHierarchyActionState {
        last_common_views_inbox_shortcut_context: "target_folder=0x0000000000050001".to_string(),
        last_inbox_notification_registration_context: "notification_folder=0x0000000000050001"
            .to_string(),
        ..PostHierarchyActionState::default()
    };

    assert_eq!(outlook_bootstrap_stall_code(&state), 4);
    assert_eq!(
        outlook_bootstrap_stall_name(outlook_bootstrap_stall_code(&state)),
        "after_common_views_inbox_notification_without_contents"
    );

    state.inbox_normal_contents_table_observed = true;

    assert_eq!(outlook_bootstrap_stall_code(&state), 0);
}

#[test]
fn outlook_bootstrap_stall_classifies_exact_fai_findrow_without_open() {
    let mut state = PostHierarchyActionState {
        inbox_associated_contents_table_observed: true,
        inbox_associated_exact_ipm_configuration_findrow_matched: true,
        ..PostHierarchyActionState::default()
    };

    assert_eq!(outlook_bootstrap_stall_code(&state), 5);
    assert_eq!(
        outlook_bootstrap_stall_name(outlook_bootstrap_stall_code(&state)),
        "after_inbox_fai_exact_config_findrow_without_open"
    );

    state.inbox_associated_config_open_observed = true;

    assert_eq!(outlook_bootstrap_stall_code(&state), 0);
}

#[test]
fn post_hierarchy_close_kind_classifies_visible_inbox_query_position_without_query_rows() {
    let mut state = PostHierarchyActionState {
        inbox_normal_contents_table_observed: true,
        inbox_normal_contents_table_setcolumns_observed: true,
        last_inbox_normal_contents_table_query_position_context: "handle=140;response_row_count=1"
            .to_string(),
        ..PostHierarchyActionState::default()
    };

    assert_eq!(
        post_hierarchy_close_kind(&state, false),
        "outlook_visible_inbox_query_position_before_query_rows"
    );

    state.inbox_normal_contents_table_query_rows_observed = true;

    assert_eq!(
        post_hierarchy_close_kind(&state, false),
        "post_hierarchy_no_close"
    );
}

#[test]
fn post_hierarchy_close_kind_prioritizes_visible_inbox_release_without_query_rows() {
    let mut state = PostHierarchyActionState {
        inbox_normal_contents_table_observed: true,
        inbox_normal_contents_table_setcolumns_observed: true,
        last_inbox_related_release_context:
            "visible_inbox_release_without_query_rows=true;handle=27".to_string(),
        last_calendar_normal_contents_table_query_position_context:
            "handle=140;response_row_count=1".to_string(),
        post_calendar_query_position_named_property_probe_count: 4,
        ..PostHierarchyActionState::default()
    };

    assert!(visible_inbox_release_without_query_rows_observed(&state));
    assert_eq!(
        post_hierarchy_close_kind(&state, false),
        "outlook_visible_inbox_release_after_setcolumns_before_query_rows"
    );

    state.inbox_normal_contents_table_query_rows_observed = true;

    assert!(!visible_inbox_release_without_query_rows_observed(&state));
    assert_eq!(
        post_hierarchy_close_kind(&state, false),
        "outlook_calendar_query_position_named_property_burst_before_query_rows"
    );
}

#[test]
fn post_hierarchy_summary_tracks_create_save_after_visible_inbox_release() {
    let mut session = test_session(HashMap::new());
    session.record_completed_hierarchy_sync(
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
        "folder=0x0000000000040001;status=0x0003".to_string(),
        "calendar:row_present=true".to_string(),
    );
    session
        .post_hierarchy_actions
        .inbox_normal_contents_table_observed = true;
    session
        .post_hierarchy_actions
        .inbox_normal_contents_table_setcolumns_observed = true;
    session
        .post_hierarchy_actions
        .last_inbox_related_release_context =
        "visible_inbox_release_without_query_rows=true;handle=27".to_string();

    session.record_execute_after_hierarchy_completion(
        &[
            RopId::CreateMessage.as_u8(),
            RopId::SetProperties.as_u8(),
            RopId::SaveChangesMessage.as_u8(),
        ],
        "CreateMessage,SetProperties,SaveChangesMessage",
    );

    let summary = post_hierarchy_action_summary(&session, false);

    assert_eq!(
        summary.post_visible_inbox_release_create_save_batch_count,
        1
    );
    assert!(summary
        .last_post_visible_inbox_release_create_save_batch_context
        .contains("request_rops=CreateMessage,SetProperties,SaveChangesMessage"));
    assert_eq!(
        summary.close_kind,
        "outlook_create_save_after_visible_inbox_release_before_query_rows"
    );
}

#[test]
fn default_view_query_rows_does_not_clear_visible_inbox_release_diagnostic() {
    let mut state = PostHierarchyActionState {
        inbox_normal_contents_table_observed: true,
        inbox_normal_contents_table_setcolumns_observed: true,
        default_view_normal_contents_table_query_rows_observed: true,
        last_default_view_normal_contents_table_query_rows_context:
            "folder=0x00000000000e0001;role=drafts".to_string(),
        last_inbox_related_release_context:
            "visible_inbox_release_without_query_rows=true;handle=27".to_string(),
        ..PostHierarchyActionState::default()
    };

    assert!(visible_inbox_release_without_query_rows_observed(&state));
    assert_eq!(
        post_hierarchy_close_kind(&state, false),
        "outlook_visible_inbox_release_after_setcolumns_before_query_rows"
    );

    state.inbox_normal_contents_table_query_rows_observed = true;

    assert!(!visible_inbox_release_without_query_rows_observed(&state));
    assert_eq!(
        post_hierarchy_close_kind(&state, false),
        "post_hierarchy_no_close"
    );
}

#[test]
fn post_hierarchy_close_kind_classifies_calendar_query_position_without_query_rows() {
    let mut state = PostHierarchyActionState {
        inbox_normal_contents_table_observed: true,
        inbox_normal_contents_table_setcolumns_observed: true,
        last_inbox_normal_contents_table_query_position_context: "handle=27;response_row_count=1"
            .to_string(),
        last_calendar_normal_contents_table_query_position_context:
            "handle=55;response_row_count=1".to_string(),
        ..PostHierarchyActionState::default()
    };

    assert_eq!(
        post_hierarchy_close_kind(&state, false),
        "outlook_calendar_query_position_before_query_rows"
    );

    state.calendar_normal_contents_table_query_rows_observed = true;

    assert_eq!(
        post_hierarchy_close_kind(&state, false),
        "outlook_visible_inbox_query_position_before_query_rows"
    );
}

#[test]
fn post_hierarchy_close_kind_classifies_calendar_named_property_burst_without_query_rows() {
    let mut state = PostHierarchyActionState {
        last_calendar_normal_contents_table_query_position_context:
            "handle=134;response_row_count=1".to_string(),
        post_calendar_query_position_named_property_probe_count: 3,
        last_post_calendar_query_position_named_property_context:
            "request_id={A}:215;requested=140;missing=111;returned=140".to_string(),
        ..PostHierarchyActionState::default()
    };

    assert_eq!(
        post_hierarchy_close_kind(&state, false),
        "outlook_calendar_query_position_named_property_burst_before_query_rows"
    );

    state.calendar_normal_contents_table_query_rows_observed = true;

    assert_eq!(
        post_hierarchy_close_kind(&state, false),
        "post_hierarchy_no_close"
    );
}

#[test]
fn post_hierarchy_close_kind_classifies_umolk_named_property_burst() {
    let mut state = PostHierarchyActionState {
        outlook_umolk_named_property_probe_count: 1,
        last_outlook_umolk_named_property_probe_context:
            "request_id={A}:112;requested=218;returned=218".to_string(),
        ..PostHierarchyActionState::default()
    };

    assert_eq!(
        post_hierarchy_close_kind(&state, false),
        "outlook_umolk_named_property_burst_before_content_sync"
    );

    state.last_outlook_umolk_getprops_materialization_context =
        "request_id={A}:113;problem_count=206;not_found_count=206".to_string();
    state.outlook_umolk_getprops_not_found_count = 206;

    assert_eq!(
        post_hierarchy_close_kind(&state, false),
        "outlook_umolk_getprops_mostly_not_found_before_content_sync"
    );
}

#[test]
fn post_hierarchy_close_kind_classifies_visible_inbox_message_faults() {
    let mut state = PostHierarchyActionState {
        visible_inbox_message_open_missing_count: 1,
        last_visible_inbox_message_open_context:
            "request_id={A}:121;folder=0x0000000000050001;source=missing".to_string(),
        ..PostHierarchyActionState::default()
    };

    assert_eq!(
        post_hierarchy_close_kind(&state, false),
        "outlook_visible_inbox_message_open_missing_before_content_sync"
    );

    state.visible_inbox_message_open_missing_count = 0;
    state.visible_inbox_message_getprops_not_found_count = 87;
    state.last_visible_inbox_message_getprops_context =
        "request_id={A}:122;problem_count=87;not_found_count=87".to_string();

    assert_eq!(
        post_hierarchy_close_kind(&state, false),
        "outlook_visible_inbox_message_getprops_mostly_not_found_before_content_sync"
    );

    state.visible_inbox_message_getprops_not_found_count = 0;
    state.last_visible_inbox_message_row_context =
        "request_id={A}:123;row_summary=returned=1".to_string();
    state.last_visible_inbox_message_open_context.clear();

    assert_eq!(
        post_hierarchy_close_kind(&state, false),
        "outlook_visible_inbox_row_returned_without_message_open_before_content_sync"
    );
}

#[test]
fn post_hierarchy_close_kind_classifies_default_view_sweep_before_inbox_query_rows() {
    let mut state = PostHierarchyActionState {
        inbox_normal_contents_table_observed: true,
        default_view_normal_contents_table_query_rows_observed: true,
        last_default_view_normal_contents_table_query_rows_context:
            "folder=0x00000000000e0001;role=drafts".to_string(),
        ..PostHierarchyActionState::default()
    };

    assert_eq!(
        post_hierarchy_close_kind(&state, false),
        "outlook_default_view_sweep_before_visible_inbox_query_rows"
    );

    state.inbox_normal_contents_table_query_rows_observed = true;

    assert_eq!(
        post_hierarchy_close_kind(&state, false),
        "post_hierarchy_no_close"
    );
}

#[test]
fn post_hierarchy_close_kind_classifies_default_view_followup() {
    let state = PostHierarchyActionState {
        inbox_normal_contents_table_observed: true,
        default_view_normal_contents_table_query_rows_observed: true,
        last_default_view_normal_contents_table_query_rows_context:
            "folder=0x00000000000e0001;role=drafts".to_string(),
        last_successful_non_release_execute_context:
            "request_id={A}:168;request_rops=GetHierarchyTable,SetColumns,QueryPosition;response_rops=GetHierarchyTable,SetColumns,QueryPosition;response_results=0x00000000,0x00000000,0x00000000;response_rop_bytes=22;cached=false".to_string(),
        ..PostHierarchyActionState::default()
    };

    assert_eq!(
        post_hierarchy_close_kind(&state, false),
        "outlook_default_view_followup_after_visible_inbox_handoff"
    );

    let state = PostHierarchyActionState {
        inbox_normal_contents_table_observed: true,
        default_view_normal_contents_table_query_rows_observed: true,
        last_default_view_normal_contents_table_query_rows_context:
            "folder=0x00000000000e0001;role=drafts".to_string(),
        last_successful_non_release_execute_context:
            "request_id={A}:109;request_rops=Release,OpenMessage,GetPropertiesSpecific;response_rops=OpenMessage,GetPropertiesSpecific;response_results=0x00000000,0x00000000;response_rop_bytes=395;cached=false".to_string(),
        ..PostHierarchyActionState::default()
    };

    assert_eq!(
        post_hierarchy_close_kind(&state, false),
        "outlook_default_view_followup_after_visible_inbox_handoff"
    );
}

#[test]
fn records_default_view_normal_query_rows_without_marking_inbox_complete() {
    let mut session = test_session(HashMap::new());

    session.record_default_view_normal_contents_table_query_rows(
        Some(42),
        "folder=0x0000000000070001;role=sent;response_row_count=2".to_string(),
    );

    assert!(
        session
            .post_hierarchy_actions
            .default_view_normal_contents_table_query_rows_observed
    );
    assert_eq!(
        session
            .post_hierarchy_actions
            .last_default_view_normal_contents_table_query_rows_handle,
        Some(42)
    );
    assert!(
        !session
            .post_hierarchy_actions
            .inbox_normal_contents_table_query_rows_observed
    );
}

#[test]
fn post_hierarchy_action_summary_exports_bootstrap_phase_scoreboard() {
    let mut session = test_session(HashMap::new());
    session.post_hierarchy_actions.post_inbox_fai_handoff_logged = true;
    session.post_hierarchy_actions.post_inbox_fai_reopen_logged = true;
    session
        .post_hierarchy_actions
        .post_inbox_fai_folder_type_probe_loop_logged = true;
    session
        .post_hierarchy_actions
        .inbox_associated_contents_table_observed = true;
    session.post_hierarchy_actions.inbox_open_folder_probe_count = 4;
    session
        .post_hierarchy_actions
        .inbox_folder_type_getprops_probe_count = 4;
    session
        .post_hierarchy_actions
        .last_inbox_hierarchy_query_context = "rows=16".to_string();

    let summary = post_hierarchy_action_summary(&session, false);

    assert_eq!(summary.outlook_bootstrap_phase, 7);
    assert_eq!(
        summary.outlook_bootstrap_phase_name,
        "repeated_inbox_folder_type_probe_loop"
    );
    assert_eq!(summary.outlook_bootstrap_stall_code, 3);
    assert_eq!(
        summary.outlook_bootstrap_stall_name,
        "repeated_inbox_folder_type_probe_without_contents"
    );
    assert_eq!(
        summary.outlook_bootstrap_next_expected_phase,
        "open_inbox_normal_contents_table_or_sync_configure"
    );
}

#[test]
fn special_folder_contract_summary_reports_conversation_history() {
    let session = test_session(HashMap::new());
    let summary = special_folder_contract_summary(&session);

    assert!(summary.contains("conversation_history=0x0000000000250001;source=additional_ren"));
    assert!(summary.contains("archive=0x0000000000230001;source=additional_ren"));
}

#[test]
fn required_default_folder_disconnect_coverage_reports_calendar_contacts_gap() {
    let mut handles = HashMap::new();
    handles.insert(
        5,
        MapiObject::Folder {
            folder_id: crate::mapi::identity::CALENDAR_FOLDER_ID,
            properties: HashMap::new(),
        },
    );
    let mut session = test_session(handles);
    session.record_completed_hierarchy_sync(
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
        "folder=0x0000000000040001;status=0x0003".to_string(),
        "calendar:row_present=true;contacts:row_present=true".to_string(),
    );
    session.record_opened_folder(crate::mapi::identity::CALENDAR_FOLDER_ID);
    session.record_post_hierarchy_request_contract(
        "GetPropertiesSpecific(kind=folder;folder=0x0000000000100001;role=calendar)->ok"
            .to_string(),
    );
    session.record_completed_sync_checkpoint(
        crate::mapi::identity::INBOX_FOLDER_ID,
        "inbox",
        "IPF.Note",
        "content",
        0x01,
        "ok",
    );

    let coverage = required_default_folder_disconnect_coverage_summary(&session);

    assert!(coverage.contains("calendar:folder=0x0000000000100001"));
    assert!(coverage.contains("calendar:folder=0x0000000000100001;advertised_source=default_ipm;parent=0x0000000000040001;hierarchy_row_expected_present=true;opened=true;pre_content_contract_seen=true;content_checkpointed=false;live_handle_count=1"));
    assert!(coverage.contains("contacts:folder=0x00000000000f0001;advertised_source=default_ipm;parent=0x0000000000040001;hierarchy_row_expected_present=true;opened=false;pre_content_contract_seen=false;content_checkpointed=false;live_handle_count=0"));
    assert!(coverage.contains("inbox:folder=0x0000000000050001"));
    assert!(coverage.contains("content_checkpointed=true"));
}

#[test]
fn post_hierarchy_action_summary_classifies_release_logoff_without_content_sync() {
    let mut session = test_session(HashMap::new());

    session.record_completed_hierarchy_sync(
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
        "folder=0x0000000000040001;status=0x0003".to_string(),
        "calendar:row_present=true".to_string(),
    );
    session.record_execute_after_hierarchy_completion(&[0x01], "Release");
    session.record_logoff_after_hierarchy_completion();
    let summary = post_hierarchy_action_summary(&session, true);

    assert_eq!(summary.execute_count, 1);
    assert_eq!(summary.rop_ids_seen, "0x01");
    assert!(summary.release_client_initiated);
    assert!(summary.logoff_client_initiated);
    assert!(!summary.content_sync_configure_observed);
    assert_eq!(
        summary.close_kind,
        "outlook_release_logoff_before_content_sync"
    );
}

#[test]
fn post_hierarchy_observation_logs_first_execute_and_later_first_bootstrap_probe() {
    let mut session = test_session(HashMap::new());

    session.record_completed_hierarchy_sync(
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
        "folder=0x0000000000040001;status=0x0003".to_string(),
        "calendar:row_present=true".to_string(),
    );
    let receive_folder_probe = session
        .record_execute_after_hierarchy_completion(&[0x01, 0x27], "Release,GetReceiveFolder");
    let default_folder_probe = session.record_execute_after_hierarchy_completion(
        &[0x02, 0x07],
        "OpenFolder,GetPropertiesSpecific",
    );
    let later_default_folder_probe = session
        .record_execute_after_hierarchy_completion(&[0x02, 0x0a], "OpenFolder,SetProperties");
    let second_set_properties_probe = session
        .record_execute_after_hierarchy_completion(&[0x02, 0x0a], "OpenFolder,SetProperties");

    assert!(receive_folder_probe.first_execute);
    assert!(!receive_folder_probe.first_bootstrap_probe);
    assert!(!receive_folder_probe.first_set_properties_probe);
    assert!(!default_folder_probe.first_execute);
    assert!(default_folder_probe.first_bootstrap_probe);
    assert!(!default_folder_probe.first_set_properties_probe);
    assert!(!later_default_folder_probe.first_execute);
    assert!(!later_default_folder_probe.first_bootstrap_probe);
    assert!(later_default_folder_probe.first_set_properties_probe);
    assert!(!second_set_properties_probe.first_execute);
    assert!(!second_set_properties_probe.first_bootstrap_probe);
    assert!(!second_set_properties_probe.first_set_properties_probe);
}
