use super::*;

#[tokio::test]
async fn mapi_over_http_shared_task_read_only_rights_reject_mutations() {
    let account = FakeStore::account();
    let owner_account_id = Uuid::parse_str("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb").unwrap();
    let task_id = Uuid::parse_str("cccccccc-cccc-4ccc-8ccc-cccccccccccc").unwrap();
    let task_list_id = "dddddddd-dddd-4ddd-8ddd-dddddddddddd";
    let mut shared_tasks = FakeStore::collection(task_list_id, "tasks", "Shared Readonly Tasks");
    shared_tasks.owner_account_id = owner_account_id;
    shared_tasks.owner_email = "owner@example.test".to_string();
    shared_tasks.owner_display_name = "Owner".to_string();
    shared_tasks.is_owned = false;
    shared_tasks.rights.may_write = false;
    shared_tasks.rights.may_delete = false;
    shared_tasks.rights.may_share = false;
    let mut shared_task = FakeStore::task(
        "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
        task_list_id,
        "Shared readonly before",
    );
    shared_task.owner_account_id = owner_account_id;
    shared_task.owner_email = shared_tasks.owner_email.clone();
    shared_task.owner_display_name = shared_tasks.owner_display_name.clone();
    shared_task.is_owned = false;
    shared_task.rights = shared_tasks.rights.clone();
    let store = FakeStore {
        session: Some(account.clone()),
        task_collections: Arc::new(Mutex::new(vec![shared_tasks.clone()])),
        tasks: Arc::new(Mutex::new(vec![shared_task])),
        ..Default::default()
    };
    let snapshot = store
        .load_mapi_mail_store(account.account_id, 100)
        .await
        .unwrap();
    let shared_folder_id = snapshot
        .collaboration_folders()
        .iter()
        .find(|folder| folder.collection.id == shared_tasks.id)
        .expect("shared tasks folder projected")
        .id;
    let task_mapi_id = snapshot
        .tasks_for_folder(shared_folder_id)
        .into_iter()
        .find(|task| task.canonical_id == task_id)
        .expect("shared task projected")
        .id;
    let tasks = store.tasks.clone();
    let service = ExchangeService::new(store);
    let (execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

    let mut properties = Vec::new();
    append_mapi_utf16_property(&mut properties, 0x0037_001F, "Forbidden update");
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, shared_folder_id);
    append_rop_open_message(&mut rops, 1, 2, shared_folder_id, task_mapi_id);
    append_rop_get_properties_specific(&mut rops, 2, &[0x0FF4_0003]);
    append_rop_set_properties(&mut rops, 2, 1, &properties);
    append_rop_delete_messages(&mut rops, 1, &[task_mapi_id]);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[logon_handle, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &0x0000_0002u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x0A, 0x02, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x1E, 0x01, 0x05, 0x00, 0x07, 0x80]
    ));
    let tasks = tasks.lock().unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].title, "Shared readonly before");
}
