use super::*;

#[tokio::test]
async fn find_folder_lists_contact_and_calendar_folders() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "contacts", "Contacts",
        )])),
        calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "calendar", "Calendar",
        )])),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "44444444-4444-4444-4444-444444444444",
            "custom",
            "RCA Sync",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:FindFolder /></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<t:ServerVersionInfo"));
    assert!(body.contains("<m:FindFolderResponse>"));
    assert!(body.contains("<t:FolderClass>IPF.Contacts</t:FolderClass>"));
    assert!(body.contains("<t:FolderClass>IPF.Calendar</t:FolderClass>"));
    assert!(body.contains("<t:ContactsFolder>"));
    assert!(body.contains("<t:CalendarFolder>"));
    assert!(body.contains("<t:FolderId Id=\"default\" ChangeKey=\"ck-v1-"));
    assert!(body.contains(
        "<t:FolderId Id=\"mailbox:44444444-4444-4444-4444-444444444444\" ChangeKey=\"ck-v1-"
    ));
    assert!(
        body.find("<t:FolderId Id=\"mailbox:44444444-4444-4444-4444-444444444444\"")
            .unwrap()
            < body.find("<t:ContactsFolder>").unwrap()
    );
    assert!(body.contains("<t:ParentFolderId Id=\"msgfolderroot\" ChangeKey=\"root\"/>"));
    assert!(body.contains("<t:DisplayName>RCA Sync</t:DisplayName>"));
    assert!(body.contains("<t:TotalCount>0</t:TotalCount>"));
    assert!(body.contains("<t:ChildFolderCount>0</t:ChildFolderCount>"));
    assert!(body.contains("<t:EffectiveRights>"));
    assert!(body.contains("<t:UnreadCount>0</t:UnreadCount>"));
}

#[tokio::test]
async fn find_folder_projects_collaboration_effective_rights() {
    let mut writable_delegate = FakeStore::collection("delegate-calendar", "calendar", "Team");
    writable_delegate.is_owned = false;
    writable_delegate.rights = CollaborationRights {
        may_read: true,
        may_write: true,
        may_delete: true,
        may_share: false,
    };
    let mut read_only_delegate = FakeStore::collection("delegate-tasks", "tasks", "Tasks");
    read_only_delegate.is_owned = false;
    read_only_delegate.rights = CollaborationRights {
        may_read: true,
        may_write: false,
        may_delete: false,
        may_share: false,
    };
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "contacts", "Contacts",
        )])),
        calendar_collections: Arc::new(Mutex::new(vec![writable_delegate])),
        task_collections: Arc::new(Mutex::new(vec![read_only_delegate])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:FindFolder /></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert_eq!(
        effective_rights_for_folder(&body, "default"),
        concat!(
            "<t:EffectiveRights>",
            "<t:CreateAssociated>false</t:CreateAssociated>",
            "<t:CreateContents>true</t:CreateContents>",
            "<t:CreateHierarchy>true</t:CreateHierarchy>",
            "<t:Delete>true</t:Delete>",
            "<t:Modify>true</t:Modify>",
            "<t:Read>true</t:Read>",
            "<t:ViewPrivateItems>true</t:ViewPrivateItems>",
            "</t:EffectiveRights>"
        )
    );
    assert_eq!(
        effective_rights_for_folder(&body, "delegate-calendar"),
        concat!(
            "<t:EffectiveRights>",
            "<t:CreateAssociated>false</t:CreateAssociated>",
            "<t:CreateContents>true</t:CreateContents>",
            "<t:CreateHierarchy>false</t:CreateHierarchy>",
            "<t:Delete>true</t:Delete>",
            "<t:Modify>true</t:Modify>",
            "<t:Read>true</t:Read>",
            "<t:ViewPrivateItems>true</t:ViewPrivateItems>",
            "</t:EffectiveRights>"
        )
    );
    assert_eq!(
        effective_rights_for_folder(&body, "delegate-tasks"),
        concat!(
            "<t:EffectiveRights>",
            "<t:CreateAssociated>false</t:CreateAssociated>",
            "<t:CreateContents>false</t:CreateContents>",
            "<t:CreateHierarchy>false</t:CreateHierarchy>",
            "<t:Delete>false</t:Delete>",
            "<t:Modify>false</t:Modify>",
            "<t:Read>true</t:Read>",
            "<t:ViewPrivateItems>false</t:ViewPrivateItems>",
            "</t:EffectiveRights>"
        )
    );
}

fn effective_rights_for_folder<'a>(body: &'a str, folder_id: &str) -> &'a str {
    let folder = body
        .split_once(&format!(r#"<t:FolderId Id="{folder_id}""#))
        .expect("folder should be present in the EWS response")
        .1;
    let start = folder
        .find("<t:EffectiveRights>")
        .expect("folder should include effective rights");
    let end = start
        + folder[start..]
            .find("</t:EffectiveRights>")
            .expect("effective rights should be complete")
        + "</t:EffectiveRights>".len();
    &folder[start..end]
}

#[tokio::test]
async fn sync_folder_hierarchy_lists_contact_and_calendar_folders() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "contacts", "Contacts",
        )])),
        calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "calendar", "Calendar",
        )])),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "44444444-4444-4444-4444-444444444444",
            "custom",
            "RCA Sync",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderHierarchy /></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:SyncFolderHierarchyResponse>"));
    assert!(body.contains("<m:IncludesLastFolderInRange>true</m:IncludesLastFolderInRange>"));
    assert!(body.contains("<t:Create><t:ContactsFolder>"));
    assert!(body.contains("<t:Create><t:CalendarFolder>"));
    assert!(body.contains("<t:Create><t:Folder>"));
    assert!(body.contains("<t:FolderClass>IPF.Contacts</t:FolderClass>"));
    assert!(body.contains("<t:FolderClass>IPF.Calendar</t:FolderClass>"));
    assert!(body.contains(
        "<t:FolderId Id=\"mailbox:44444444-4444-4444-4444-444444444444\" ChangeKey=\"ck-v1-"
    ));
    assert!(
        body.find("<t:Create><t:Folder>").unwrap()
            < body.find("<t:Create><t:ContactsFolder>").unwrap()
    );
    assert!(body.contains("<t:ParentFolderId Id=\"msgfolderroot\" ChangeKey=\"root\"/>"));
    assert!(body.contains("<t:DisplayName>RCA Sync</t:DisplayName>"));
    assert!(body.contains("<t:UnreadCount>0</t:UnreadCount>"));
}

#[tokio::test]
async fn sync_folder_hierarchy_replays_canonical_mailbox_changes() {
    let mailbox_id = "44444444-4444-4444-4444-444444444444";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            mailbox_id, "custom", "RCA Sync",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderHierarchy /></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let initial = response_text(response).await;
    let initial_state = test_xml_text(&initial, "SyncState").unwrap();

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:SyncFolderHierarchy><m:SyncState>{initial_state}</m:SyncState></m:SyncFolderHierarchy></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(!body.contains("<t:Create>"));
    assert!(!body.contains("<t:Update>"));
    assert!(!body.contains("<t:Delete>"));

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:UpdateFolder><m:FolderChanges><t:FolderChange><t:FolderId Id="mailbox:{mailbox_id}"/><t:Updates><t:SetFolderField><t:FieldURI FieldURI="folder:DisplayName"/><t:Folder><t:DisplayName>Renamed RCA Sync</t:DisplayName></t:Folder></t:SetFolderField></t:Updates></t:FolderChange></m:FolderChanges></m:UpdateFolder></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    assert!(response_text(response)
        .await
        .contains("<m:ResponseCode>NoError</m:ResponseCode>"));

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:SyncFolderHierarchy><m:SyncState>{initial_state}</m:SyncState></m:SyncFolderHierarchy></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let renamed = response_text(response).await;
    assert!(renamed.contains("<t:Update><t:Folder>"));
    assert!(renamed.contains("<t:DisplayName>Renamed RCA Sync</t:DisplayName>"));
    let renamed_state = test_xml_text(&renamed, "SyncState").unwrap();

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:DeleteFolder DeleteType="HardDelete"><m:FolderIds><t:FolderId Id="mailbox:{mailbox_id}"/></m:FolderIds></m:DeleteFolder></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    assert!(response_text(response)
        .await
        .contains("<m:ResponseCode>NoError</m:ResponseCode>"));

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:SyncFolderHierarchy><m:SyncState>{renamed_state}</m:SyncState></m:SyncFolderHierarchy></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let deleted = response_text(response).await;
    assert!(deleted.contains(&format!(
        "<t:Delete><t:FolderId Id=\"mailbox:{mailbox_id}\""
    )));
}

#[tokio::test]
async fn create_folder_path_creates_nested_mailboxes_and_sync_reports_changes() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store.clone());

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope><s:Body>
              <m:CreateFolderPath>
                <m:ParentFolderId><t:DistinguishedFolderId Id="msgfolderroot"/></m:ParentFolderId>
                <m:RelativeFolderPath>
                  <t:Folder><t:DisplayName>Projects</t:DisplayName></t:Folder>
                  <t:Folder><t:DisplayName>RCA Sync</t:DisplayName></t:Folder>
                </m:RelativeFolderPath>
              </m:CreateFolderPath>
            </s:Body></s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:CreateFolderPathResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:DisplayName>Projects</t:DisplayName>"));
    assert!(body.contains("<t:DisplayName>RCA Sync</t:DisplayName>"));
    assert!(body.contains("<t:ParentFolderId Id=\"mailbox:44444444-4444-4444-4444-444444444444\" ChangeKey=\"ck-44444444-4444-4444-4444-444444444444\"/>"));
    let created = store.created_mailboxes.lock().unwrap().clone();
    assert_eq!(created.len(), 2);
    assert_eq!(created[0].name, "Projects");
    assert_eq!(created[1].name, "RCA Sync");
    assert!(created[1].parent_id.is_some());
    assert_eq!(
        store
            .mapi_sync_changes
            .lock()
            .unwrap()
            .changed_mailbox_ids
            .len(),
        2
    );

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderHierarchy /></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:DisplayName>Projects</t:DisplayName>"));
    assert!(body.contains("<t:DisplayName>RCA Sync</t:DisplayName>"));

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetFolder><m:FolderIds><t:FolderId Id="mailbox:44444444-4444-4444-4444-444444444402"/></m:FolderIds></m:GetFolder></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:ParentFolderId Id=\"mailbox:44444444-4444-4444-4444-444444444444\" ChangeKey=\"ck-44444444-4444-4444-4444-444444444444\"/>"));
}

#[tokio::test]
async fn create_folder_path_rejects_an_empty_segment_without_creating_mailboxes() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let created_mailboxes = store.created_mailboxes.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope><s:Body><m:CreateFolderPath>
              <m:ParentFolderId><t:DistinguishedFolderId Id="msgfolderroot"/></m:ParentFolderId>
              <m:RelativeFolderPath>
                <t:Folder><t:DisplayName>Projects</t:DisplayName></t:Folder>
                <t:Folder><t:DisplayName> </t:DisplayName></t:Folder>
              </m:RelativeFolderPath>
            </m:CreateFolderPath></s:Body></s:Envelope>
            "#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:CreateFolderPathResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(created_mailboxes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn copy_move_and_update_folder_use_canonical_mailbox_changes() {
    let source_id = "11111111-1111-1111-1111-111111111111";
    let target_id = "22222222-2222-2222-2222-222222222222";
    let child_id = "33333333-3333-3333-3333-333333333333";
    let child = FakeStore::mailbox(child_id, "custom", "Child");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(source_id, "custom", "Source"),
            FakeStore::mailbox(target_id, "custom", "Target"),
            child,
        ])),
        ..Default::default()
    };
    let service = ExchangeService::new(store.clone());

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:CopyFolder><m:ToFolderId><t:FolderId Id="mailbox:{target_id}"/></m:ToFolderId><m:FolderIds><t:FolderId Id="mailbox:{source_id}"/></m:FolderIds></m:CopyFolder></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:CopyFolderResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert_eq!(store.created_mailboxes.lock().unwrap().len(), 1);
    assert!(store.copied_emails.lock().unwrap().is_empty());

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:MoveFolder><m:ToFolderId><t:FolderId Id="mailbox:{target_id}"/></m:ToFolderId><m:FolderIds><t:FolderId Id="mailbox:{child_id}"/></m:FolderIds></m:MoveFolder></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:MoveFolderResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert_eq!(
        store.updated_mailboxes.lock().unwrap()[0].parent_id,
        Some(Some(Uuid::parse_str(target_id).unwrap()))
    );

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:GetFolder><m:FolderIds><t:FolderId Id="mailbox:{child_id}"/></m:FolderIds></m:GetFolder></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains(&format!(
        "<t:ParentFolderId Id=\"mailbox:{target_id}\" ChangeKey=\"ck-{target_id}\"/>"
    )));

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:UpdateFolder><m:FolderChanges><t:FolderChange><t:FolderId Id="mailbox:{source_id}"/><t:Updates><t:SetFolderField><t:FieldURI FieldURI="folder:DisplayName"/><t:Folder><t:DisplayName>Renamed Source</t:DisplayName></t:Folder></t:SetFolderField></t:Updates></t:FolderChange></m:FolderChanges></m:UpdateFolder></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:UpdateFolderResponse>"));
    assert!(body.contains("<t:DisplayName>Renamed Source</t:DisplayName>"));
    assert!(store
        .mapi_sync_changes
        .lock()
        .unwrap()
        .changed_mailbox_ids
        .contains(&Uuid::parse_str(source_id).unwrap()));
}

#[tokio::test]
async fn move_folder_rejects_a_batch_without_mutating_any_source() {
    let first_id = "11111111-1111-1111-1111-111111111111";
    let second_id = "22222222-2222-2222-2222-222222222222";
    let target_id = "33333333-3333-3333-3333-333333333333";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(first_id, "custom", "First"),
            FakeStore::mailbox(second_id, "custom", "Second"),
            FakeStore::mailbox(target_id, "custom", "Target"),
        ])),
        ..Default::default()
    };
    let updated_mailboxes = store.updated_mailboxes.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:MoveFolder><m:ToFolderId><t:FolderId Id="mailbox:{target_id}"/></m:ToFolderId><m:FolderIds><t:FolderId Id="mailbox:{first_id}"/><t:FolderId Id="mailbox:{second_id}"/></m:FolderIds></m:MoveFolder></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:MoveFolderResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(updated_mailboxes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn copy_folder_rejects_mixed_public_and_mailbox_sources_without_mutation() {
    let mailbox_id = "11111111-1111-1111-1111-111111111111";
    let public_folder_id = "aaaaaaaa-1111-1111-1111-111111111111";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            mailbox_id, "custom", "Private",
        )])),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            public_folder_id,
            None,
            "Shared",
        )])),
        ..Default::default()
    };
    let created_mailboxes = store.created_mailboxes.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:CopyFolder><m:ToFolderId><t:FolderId Id="public-folder:{public_folder_id}"/></m:ToFolderId><m:FolderIds><t:FolderId Id="mailbox:{mailbox_id}"/><t:FolderId Id="public-folder:{public_folder_id}"/></m:FolderIds></m:CopyFolder></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:CopyFolderResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(created_mailboxes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn folder_change_keys_follow_canonical_revisions_and_reject_stale_updates() {
    let source_id = "11111111-1111-1111-1111-111111111111";
    let target_id = "22222222-2222-2222-2222-222222222222";
    let public_folder_id = "aaaaaaaa-1111-1111-1111-111111111111";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(source_id, "custom", "Source"),
            FakeStore::mailbox(target_id, "custom", "Target"),
        ])),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            public_folder_id,
            None,
            "Shared",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store.clone());

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:GetFolder><m:FolderIds><t:FolderId Id="mailbox:{source_id}"/></m:FolderIds></m:GetFolder></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let initial = response_text(response).await;
    let initial_key = test_item_change_key(&initial, &format!("mailbox:{source_id}"));

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:UpdateFolder><m:FolderChanges><t:FolderChange><t:FolderId Id="mailbox:{source_id}" ChangeKey="{initial_key}"/><t:Updates><t:SetFolderField><t:FieldURI FieldURI="folder:DisplayName"/><t:Folder><t:DisplayName>Renamed Source</t:DisplayName></t:Folder></t:SetFolderField></t:Updates></t:FolderChange></m:FolderChanges></m:UpdateFolder></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let renamed = response_text(response).await;
    let renamed_key = test_item_change_key(&renamed, &format!("mailbox:{source_id}"));
    assert_ne!(renamed_key, initial_key);

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:MoveFolder><m:ToFolderId><t:FolderId Id="mailbox:{target_id}"/></m:ToFolderId><m:FolderIds><t:FolderId Id="mailbox:{source_id}"/></m:FolderIds></m:MoveFolder></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let moved = response_text(response).await;
    let moved_key = test_item_change_key(&moved, &format!("mailbox:{source_id}"));
    assert_ne!(moved_key, renamed_key);

    let update_count = store.updated_mailboxes.lock().unwrap().len();
    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:UpdateFolder><m:FolderChanges><t:FolderChange><t:FolderId Id="mailbox:{source_id}" ChangeKey="{initial_key}"/><t:Updates><t:SetFolderField><t:FieldURI FieldURI="folder:DisplayName"/><t:Folder><t:DisplayName>Stale Rename</t:DisplayName></t:Folder></t:SetFolderField></t:Updates></t:FolderChange></m:FolderChanges></m:UpdateFolder></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>ErrorIrresolvableConflict</m:ResponseCode>"));
    assert_eq!(store.updated_mailboxes.lock().unwrap().len(), update_count);
    let source = store
        .mailboxes
        .lock()
        .unwrap()
        .iter()
        .find(|mailbox| mailbox.id == Uuid::parse_str(source_id).unwrap())
        .unwrap()
        .clone();
    assert_eq!(source.name, "Renamed Source");
    assert_eq!(source.parent_id, Some(Uuid::parse_str(target_id).unwrap()));

    store
        .update_jmap_mailbox(
            JmapMailboxUpdateInput {
                account_id: FakeStore::account().account_id,
                mailbox_id: Uuid::parse_str(source_id).unwrap(),
                name: Some("JMAP Rename".to_string()),
                parent_id: None,
                sort_order: None,
                is_subscribed: None,
            },
            lpe_storage::AuditEntryInput {
                actor: "alice@example.test".to_string(),
                action: "jmap-mailbox-set".to_string(),
                subject: source_id.to_string(),
            },
        )
        .await
        .unwrap();
    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:GetFolder><m:FolderIds><t:FolderId Id="mailbox:{source_id}"/></m:FolderIds></m:GetFolder></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let jmap_updated = response_text(response).await;
    assert_ne!(
        test_item_change_key(&jmap_updated, &format!("mailbox:{source_id}")),
        moved_key
    );

    let update_count = store.updated_mailboxes.lock().unwrap().len();
    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:UpdateFolder><m:FolderChanges><t:FolderChange><t:FolderId Id="mailbox:{source_id}" ChangeKey="{moved_key}"/><t:Updates><t:SetFolderField><t:FieldURI FieldURI="folder:DisplayName"/><t:Folder><t:DisplayName>Stale JMAP Rename</t:DisplayName></t:Folder></t:SetFolderField></t:Updates></t:FolderChange></m:FolderChanges></m:UpdateFolder></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>ErrorIrresolvableConflict</m:ResponseCode>"));
    assert_eq!(store.updated_mailboxes.lock().unwrap().len(), update_count);
    assert!(store
        .mailboxes
        .lock()
        .unwrap()
        .iter()
        .any(|mailbox| mailbox.id == Uuid::parse_str(source_id).unwrap()
            && mailbox.name == "JMAP Rename"));

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:GetFolder><m:FolderIds><t:FolderId Id="public-folder:{public_folder_id}"/></m:FolderIds></m:GetFolder></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let initial = response_text(response).await;
    let initial_key = test_item_change_key(&initial, &format!("public-folder:{public_folder_id}"));
    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:UpdateFolder><m:FolderChanges><t:FolderChange><t:FolderId Id="public-folder:{public_folder_id}" ChangeKey="{initial_key}"/><t:Updates><t:SetFolderField><t:FieldURI FieldURI="folder:DisplayName"/><t:Folder><t:DisplayName>Renamed Shared</t:DisplayName></t:Folder></t:SetFolderField></t:Updates></t:FolderChange></m:FolderChanges></m:UpdateFolder></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let renamed = response_text(response).await;
    assert_ne!(
        test_item_change_key(&renamed, &format!("public-folder:{public_folder_id}")),
        initial_key
    );

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:UpdateFolder><m:FolderChanges><t:FolderChange><t:FolderId Id="public-folder:{public_folder_id}" ChangeKey="{initial_key}"/><t:Updates><t:SetFolderField><t:FieldURI FieldURI="folder:DisplayName"/><t:Folder><t:DisplayName>Stale Shared</t:DisplayName></t:Folder></t:SetFolderField></t:Updates></t:FolderChange></m:FolderChanges></m:UpdateFolder></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>ErrorIrresolvableConflict</m:ResponseCode>"));
    assert!(store
        .public_folders
        .lock()
        .unwrap()
        .iter()
        .any(
            |folder| folder.id == Uuid::parse_str(public_folder_id).unwrap()
                && folder.display_name == "Renamed Shared"
        ));
}

#[tokio::test]
async fn empty_folder_deletes_messages_and_subfolders_through_canonical_paths() {
    let parent_id = "11111111-1111-1111-1111-111111111111";
    let child_id = "22222222-2222-2222-2222-222222222222";
    let parent_message_id = "33333333-3333-3333-3333-333333333333";
    let child_message_id = "44444444-4444-4444-4444-444444444444";
    let mut child = FakeStore::mailbox(child_id, "custom", "Child");
    child.parent_id = Some(Uuid::parse_str(parent_id).unwrap());
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(parent_id, "custom", "Parent"),
            child,
        ])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(parent_message_id, parent_id, "custom", "Parent payload"),
            FakeStore::email(child_message_id, child_id, "custom", "Child payload"),
        ])),
        ..Default::default()
    };
    let service = ExchangeService::new(store.clone());

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:EmptyFolder DeleteSubFolders="true" DeleteType="HardDelete"><m:FolderIds><t:FolderId Id="mailbox:{parent_id}"/></m:FolderIds></m:EmptyFolder></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:EmptyFolderResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert_eq!(
        store.deleted_emails.lock().unwrap().as_slice(),
        &[
            Uuid::parse_str(parent_message_id).unwrap(),
            Uuid::parse_str(child_message_id).unwrap()
        ]
    );
    assert_eq!(
        store.destroyed_mailboxes.lock().unwrap().as_slice(),
        &[Uuid::parse_str(child_id).unwrap()]
    );
}

#[tokio::test]
async fn folder_operations_preserve_system_and_public_folder_boundaries() {
    let inbox_id = "11111111-1111-1111-1111-111111111111";
    let target_id = "22222222-2222-2222-2222-222222222222";
    let public_root_id = "aaaaaaaa-1111-1111-1111-111111111111";
    let public_child_id = "aaaaaaaa-2222-2222-2222-222222222222";
    let public_item_id = "aaaaaaaa-3333-3333-3333-333333333333";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(inbox_id, "inbox", "Inbox"),
            FakeStore::mailbox(target_id, "custom", "Target"),
        ])),
        public_folders: Arc::new(Mutex::new(vec![
            FakeStore::public_folder(public_root_id, None, "Shared"),
            FakeStore::public_folder(public_child_id, Some(public_root_id), "Team"),
        ])),
        public_folder_items: Arc::new(Mutex::new(vec![FakeStore::public_folder_item(
            public_item_id,
            public_child_id,
            "Public payload",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store.clone());

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:UpdateFolder><m:FolderChanges><t:FolderChange><t:FolderId Id="mailbox:{inbox_id}"/><t:Updates><t:SetFolderField><t:FieldURI FieldURI="folder:DisplayName"/><t:Folder><t:DisplayName>Inbox2</t:DisplayName></t:Folder></t:SetFolderField></t:Updates></t:FolderChange></m:FolderChanges></m:UpdateFolder></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:UpdateFolderResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(store.updated_mailboxes.lock().unwrap().is_empty());

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:UpdateFolder><m:FolderChanges><t:FolderChange><t:FolderId Id="public-folder:{public_child_id}"/><t:Updates><t:SetFolderField><t:FieldURI FieldURI="folder:DisplayName"/><t:Folder><t:DisplayName>Renamed Team</t:DisplayName></t:Folder></t:SetFolderField></t:Updates></t:FolderChange></m:FolderChanges></m:UpdateFolder></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:UpdateFolderResponse>"));
    assert!(body.contains("<t:DisplayName>Renamed Team</t:DisplayName>"));

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:EmptyFolder DeleteSubFolders="false" DeleteType="HardDelete"><m:FolderIds><t:FolderId Id="public-folder:{public_child_id}"/></m:FolderIds></m:EmptyFolder></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:EmptyFolderResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert_eq!(
        store.deleted_public_folder_items.lock().unwrap().as_slice(),
        &[Uuid::parse_str(public_item_id).unwrap()]
    );

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:MoveFolder><m:ToFolderId><t:FolderId Id="public-folder:{public_root_id}"/></m:ToFolderId><m:FolderIds><t:FolderId Id="public-folder:{public_child_id}"/></m:FolderIds></m:MoveFolder></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:MoveFolderResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(body.contains("canonical public-folder reparenting"));
}

#[tokio::test]
async fn get_folder_returns_msgfolderroot() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetFolder><m:FolderIds><t:DistinguishedFolderId Id="msgfolderroot"/></m:FolderIds></m:GetFolder></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:GetFolderResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:FolderId Id=\"msgfolderroot\" ChangeKey=\"root\"/>"));
    assert!(body.contains("<t:DisplayName>Root</t:DisplayName>"));
    assert!(body.contains("<t:TotalCount>0</t:TotalCount>"));
    assert!(body.contains("<t:ChildFolderCount>0</t:ChildFolderCount>"));
    assert!(body.contains("<t:EffectiveRights>"));
    assert!(body.contains("<t:UnreadCount>0</t:UnreadCount>"));
}

#[tokio::test]
async fn get_folder_root_reports_child_folders_for_client_bootstrap() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "contacts", "Contacts",
        )])),
        calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "calendar", "Calendar",
        )])),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Inbox",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetFolder><m:FolderIds><t:DistinguishedFolderId Id="msgfolderroot"/></m:FolderIds></m:GetFolder></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:GetFolderResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:FolderId Id=\"msgfolderroot\" ChangeKey=\"root\"/>"));
    assert!(body.contains("<t:ChildFolderCount>3</t:ChildFolderCount>"));
}

#[tokio::test]
async fn get_folder_returns_multiple_supported_folder_kinds() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "contacts", "Contacts",
        )])),
        calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "calendar", "Calendar",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetFolder><m:FolderIds><t:DistinguishedFolderId Id="contacts"/><t:DistinguishedFolderId Id="calendar"/></m:FolderIds></m:GetFolder></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:FolderClass>IPF.Contacts</t:FolderClass>"));
    assert!(body.contains("<t:FolderClass>IPF.Calendar</t:FolderClass>"));
}

#[tokio::test]
async fn get_folder_preserves_exact_supported_targets_in_request_order() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "calendar", "Calendar",
        )])),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "44444444-4444-4444-4444-444444444444",
            "custom",
            "RCA Sync",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetFolder><m:FolderIds><t:FolderId Id="mailbox:44444444-4444-4444-4444-444444444444"/><t:DistinguishedFolderId Id="msgfolderroot"/><t:DistinguishedFolderId Id="calendar"/></m:FolderIds></m:GetFolder></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    let mailbox = body.find("<t:DisplayName>RCA Sync</t:DisplayName>").unwrap();
    let root = body.find("<t:DisplayName>Root</t:DisplayName>").unwrap();
    let calendar = body.find("<t:CalendarFolder>").unwrap();
    assert!(mailbox < root && root < calendar);
}

#[tokio::test]
async fn get_folder_rejects_missing_duplicate_or_misplaced_folder_ids_wrapper() {
    let service = ExchangeService::new(FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    });

    for request in [
        br#"<s:Envelope><s:Body><m:GetFolder><t:DistinguishedFolderId Id="msgfolderroot"/></m:GetFolder></s:Body></s:Envelope>"# as &[u8],
        br#"<s:Envelope><s:Body><m:GetFolder><m:FolderIds><t:DistinguishedFolderId Id="msgfolderroot"/></m:FolderIds><m:FolderIds><t:DistinguishedFolderId Id="calendar"/></m:FolderIds></m:GetFolder></s:Body></s:Envelope>"#,
        br#"<s:Envelope><s:Body><m:GetFolder><m:FolderIds><t:DistinguishedFolderId Id="msgfolderroot"/></m:FolderIds><t:DistinguishedFolderId Id="calendar"/></m:GetFolder></s:Body></s:Envelope>"#,
    ] {
        let response = service.handle(&bearer_headers(), request).await.unwrap();
        let body = response_text(response).await;
        assert!(body.contains("<m:GetFolderResponseMessage ResponseClass=\"Error\">"));
        assert!(body.contains("<m:ResponseCode>ErrorFolderNotFound</m:ResponseCode>"));
    }
}

#[tokio::test]
async fn get_folder_returns_only_the_requested_collaboration_collection() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        calendar_collections: Arc::new(Mutex::new(vec![
            FakeStore::collection("shared-calendar-alice", "calendar", "Alice Calendar"),
            FakeStore::collection("shared-calendar-bob", "calendar", "Bob Calendar"),
        ])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetFolder><m:FolderIds><t:FolderId Id="shared-calendar-bob"/></m:FolderIds></m:GetFolder></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("Id=\"shared-calendar-bob\""));
    assert!(!body.contains("Id=\"shared-calendar-alice\""));
}

#[tokio::test]
async fn create_folder_uses_canonical_mailbox_store() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let created_mailboxes = store.created_mailboxes.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:CreateFolder>
                  <m:ParentFolderId><t:DistinguishedFolderId Id="msgfolderroot"/></m:ParentFolderId>
                  <m:Folders><t:Folder><t:DisplayName>RCA Sync</t:DisplayName></t:Folder></m:Folders>
                </m:CreateFolder>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:CreateFolderResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains(
        "<t:FolderId Id=\"mailbox:44444444-4444-4444-4444-444444444444\" ChangeKey=\"ck-v1-"
    ));
    assert!(body.contains("<t:TotalCount>0</t:TotalCount>"));
    assert_eq!(created_mailboxes.lock().unwrap()[0].name, "RCA Sync");
}

#[tokio::test]
// [MS-OXWSFOLD] section 3.1.4.2: CreateFolder uses the requested existing parent.
async fn create_folder_uses_the_requested_custom_mailbox_parent() {
    let parent_id = "44444444-4444-4444-4444-444444444444";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            parent_id, "custom", "Projects",
        )])),
        ..Default::default()
    };
    let created_mailboxes = store.created_mailboxes.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:CreateFolder><m:ParentFolderId><t:FolderId Id="mailbox:{parent_id}"/></m:ParentFolderId><m:Folders><t:Folder><t:DisplayName>Quarterly Reviews</t:DisplayName><t:FolderClass>IPF.Note</t:FolderClass></t:Folder></m:Folders></m:CreateFolder></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains(&format!("<t:ParentFolderId Id=\"mailbox:{parent_id}\"")));
    assert_eq!(
        created_mailboxes.lock().unwrap()[0].parent_id,
        Some(Uuid::parse_str(parent_id).unwrap())
    );
}

#[tokio::test]
async fn create_and_delete_folder_reject_unsupported_or_batch_shapes_without_mutation() {
    let first_id = "44444444-4444-4444-4444-444444444444";
    let second_id = "55555555-5555-5555-5555-555555555555";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(first_id, "custom", "First"),
            FakeStore::mailbox(second_id, "custom", "Second"),
        ])),
        ..Default::default()
    };
    let created_mailboxes = store.created_mailboxes.clone();
    let destroyed_mailboxes = store.destroyed_mailboxes.clone();
    let service = ExchangeService::new(store);

    for request in [
        r#"<s:Envelope><s:Body><m:CreateFolder><m:ParentFolderId><t:DistinguishedFolderId Id="msgfolderroot"/></m:ParentFolderId><m:Folders><t:Folder><t:DisplayName>Inbox</t:DisplayName></t:Folder></m:Folders></m:CreateFolder></s:Body></s:Envelope>"#,
        r#"<s:Envelope><s:Body><m:CreateFolder><m:ParentFolderId><t:FolderId Id="public-folder:not-a-uuid"/></m:ParentFolderId><m:Folders><t:Folder><t:DisplayName>Escape</t:DisplayName></t:Folder></m:Folders></m:CreateFolder></s:Body></s:Envelope>"#,
        r#"<s:Envelope><s:Body><m:CreateFolder><m:ParentFolderId><t:DistinguishedFolderId Id="msgfolderroot"/></m:ParentFolderId><m:Folders><t:CalendarFolder><t:DisplayName>Calendar Copy</t:DisplayName></t:CalendarFolder></m:Folders></m:CreateFolder></s:Body></s:Envelope>"#,
        r#"<s:Envelope><s:Body><m:CreateFolder><m:ParentFolderId><t:DistinguishedFolderId Id="msgfolderroot"/></m:ParentFolderId><m:Folders><t:Folder><t:DisplayName>First</t:DisplayName></t:Folder></m:Folders><m:Folders><t:Folder><t:DisplayName>Second</t:DisplayName></t:Folder></m:Folders></m:CreateFolder></s:Body></s:Envelope>"#,
        r#"<s:Envelope><s:Body><m:CreateFolder><m:ParentFolderId><t:DistinguishedFolderId Id="msgfolderroot"/></m:ParentFolderId><m:ParentFolderId><t:FolderId Id="mailbox:44444444-4444-4444-4444-444444444444"/></m:ParentFolderId><m:Folders><t:Folder><t:DisplayName>Duplicate Parent</t:DisplayName></t:Folder></m:Folders></m:CreateFolder></s:Body></s:Envelope>"#,
    ] {
        let response = service
            .handle(&bearer_headers(), request.as_bytes())
            .await
            .unwrap();
        let body = response_text(response).await;
        assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    }
    assert!(created_mailboxes.lock().unwrap().is_empty());

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:DeleteFolder DeleteType="HardDelete"><m:FolderIds><t:FolderId Id="mailbox:{first_id}"/><t:FolderId Id="mailbox:{second_id}"/></m:FolderIds></m:DeleteFolder></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>ErrorFolderNotFound</m:ResponseCode>"));
    assert!(destroyed_mailboxes.lock().unwrap().is_empty());

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:DeleteFolder DeleteType="HardDelete"><m:FolderIds><t:FolderId Id="mailbox:{first_id}"/></m:FolderIds><m:FolderIds><t:FolderId Id="mailbox:{second_id}"/></m:FolderIds></m:DeleteFolder></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>ErrorFolderNotFound</m:ResponseCode>"));
    assert!(destroyed_mailboxes.lock().unwrap().is_empty());

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:FindFolder><m:ParentFolderIds><t:DistinguishedFolderId Id="msgfolderroot"/></m:ParentFolderIds><m:ParentFolderIds><t:DistinguishedFolderId Id="inbox"/></m:ParentFolderIds></m:FindFolder></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:FindFolderResponseMessage ResponseClass=\"Error\">"));
    assert!(body.contains("<m:ResponseCode>ErrorFolderNotFound</m:ResponseCode>"));
}

#[tokio::test]
async fn create_folder_uses_canonical_public_folder_store() {
    let parent_folder_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            None,
            "Public Root",
        )])),
        ..Default::default()
    };
    let public_folders = store.public_folders.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:CreateFolder>
                  <m:ParentFolderId><t:FolderId Id="public-folder:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"/></m:ParentFolderId>
                  <m:Folders><t:Folder><t:DisplayName>Team Posts</t:DisplayName><t:FolderClass>IPF.Note</t:FolderClass></t:Folder></m:Folders>
                </m:CreateFolder>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:CreateFolderResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("public-folder:cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcdcd"));
    assert!(body.contains("<t:DisplayName>Team Posts</t:DisplayName>"));
    let folders = public_folders.lock().unwrap();
    assert_eq!(folders.len(), 2);
    assert_eq!(folders[1].parent_folder_id, Some(parent_folder_id));
    assert_eq!(folders[1].display_name, "Team Posts");
}

#[tokio::test]
// [MS-OXWSSRCH] section 3.1.4.1 and [MS-OXWSFOLD] sections 3.1.4.4 and 3.1.4.6.
async fn public_child_folder_find_get_and_hierarchy_share_canonical_projection() {
    let root_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let child_id = "cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcdcd";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            root_id,
            None,
            "Public Root",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store.clone());

    let initial_response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderHierarchy /></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let initial = response_text(initial_response).await;
    let initial_state = test_xml_text(&initial, "SyncState").unwrap();

    let create_response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:CreateFolder><m:ParentFolderId><t:FolderId Id="public-folder:{root_id}"/></m:ParentFolderId><m:Folders><t:Folder><t:DisplayName>Team Posts</t:DisplayName><t:FolderClass>IPF.Note</t:FolderClass></t:Folder></m:Folders></m:CreateFolder></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    assert!(response_text(create_response)
        .await
        .contains("<m:ResponseCode>NoError</m:ResponseCode>"));

    let find_response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:FindFolder><m:ParentFolderIds><t:FolderId Id="public-folder:{root_id}"/></m:ParentFolderIds></m:FindFolder></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let find = response_text(find_response).await;
    assert!(find.contains(&format!("public-folder:{child_id}")));
    let find_key = test_item_change_key(&find, &format!("public-folder:{child_id}"));
    let find_rights = effective_rights_for_folder(&find, &format!("public-folder:{child_id}"));

    let get_response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:GetFolder><m:FolderIds><t:FolderId Id="public-folder:{child_id}"/></m:FolderIds></m:GetFolder></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let get = response_text(get_response).await;
    assert_eq!(
        test_item_change_key(&get, &format!("public-folder:{child_id}")),
        find_key
    );
    assert_eq!(
        effective_rights_for_folder(&get, &format!("public-folder:{child_id}")),
        find_rights
    );

    let created_response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:SyncFolderHierarchy><m:SyncState>{initial_state}</m:SyncState></m:SyncFolderHierarchy></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let created = response_text(created_response).await;
    assert_eq!(
        created
            .matches(&format!(
                "<t:Create><t:Folder><t:FolderId Id=\"public-folder:{child_id}\""
            ))
            .count(),
        1
    );
    let created_state = test_xml_text(&created, "SyncState").unwrap();

    let no_change_response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:SyncFolderHierarchy><m:SyncState>{created_state}</m:SyncState></m:SyncFolderHierarchy></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    assert!(!response_text(no_change_response)
        .await
        .contains("<t:Create>"));

    let delete_response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:DeleteFolder DeleteType="HardDelete"><m:FolderIds><t:FolderId Id="public-folder:{child_id}"/></m:FolderIds></m:DeleteFolder></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    assert!(response_text(delete_response)
        .await
        .contains("<m:ResponseCode>NoError</m:ResponseCode>"));

    let deleted_response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:SyncFolderHierarchy><m:SyncState>{created_state}</m:SyncState></m:SyncFolderHierarchy></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let deleted = response_text(deleted_response).await;
    assert_eq!(
        deleted
            .matches(&format!(
                "<t:Delete><t:FolderId Id=\"public-folder:{child_id}\""
            ))
            .count(),
        1
    );
}

#[tokio::test]
async fn delete_folder_uses_canonical_mailbox_destroy() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "44444444-4444-4444-4444-444444444444",
            "custom",
            "RCA Sync",
        )])),
        ..Default::default()
    };
    let destroyed_mailboxes = store.destroyed_mailboxes.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:DeleteFolder DeleteType="HardDelete">
                  <m:FolderIds><t:FolderId Id="mailbox:44444444-4444-4444-4444-444444444444"/></m:FolderIds>
                </m:DeleteFolder>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:DeleteFolderResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert_eq!(
        destroyed_mailboxes.lock().unwrap().as_slice(),
        &[Uuid::parse_str("44444444-4444-4444-4444-444444444444").unwrap()]
    );
}

#[tokio::test]
async fn create_folder_rejects_missing_public_folder_parent() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:CreateFolder>
                  <m:ParentFolderId><t:FolderId Id="public-folder:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"/></m:ParentFolderId>
                  <m:Folders><t:Folder><t:DisplayName>Team Posts</t:DisplayName></t:Folder></m:Folders>
                </m:CreateFolder>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:CreateFolderResponse>"));
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert!(body.contains("public folder not found"));
}

#[tokio::test]
async fn create_folder_rejects_blank_public_folder_display_name() {
    // [MS-OXWSFOLD] sections 3.1.4.2 and 3.1.4.2.3.2: reject malformed
    // CreateFolder input before it reaches the canonical public-folder tree.
    let public_folders = Arc::new(Mutex::new(vec![FakeStore::public_folder(
        "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        None,
        "Public Root",
    )]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: public_folders.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:CreateFolder>
                  <m:ParentFolderId><t:FolderId Id="public-folder:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"/></m:ParentFolderId>
                  <m:Folders><t:Folder><t:DisplayName>   </t:DisplayName></t:Folder></m:Folders>
                </m:CreateFolder>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:CreateFolderResponse>"));
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert!(body.contains("CreateFolder is missing DisplayName"));
    assert_eq!(public_folders.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn create_folder_rejects_non_owner_public_folder_structural_change() {
    let non_owner = AuthenticatedAccount {
        tenant_id: FakeStore::account().tenant_id,
        account_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
        email: "delegate@example.test".to_string(),
        display_name: "Public Delegate".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let store = FakeStore {
        session: Some(non_owner),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            None,
            "Public Root",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:CreateFolder>
                  <m:ParentFolderId><t:FolderId Id="public-folder:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"/></m:ParentFolderId>
                  <m:Folders><t:Folder><t:DisplayName>Team Posts</t:DisplayName></t:Folder></m:Folders>
                </m:CreateFolder>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:CreateFolderResponse>"));
    assert!(body.contains("<m:ResponseCode>ErrorAccessDenied</m:ResponseCode>"));
    assert!(body.contains("public folder structural changes require tree owner access"));
}

#[tokio::test]
async fn delete_folder_uses_canonical_public_folder_store() {
    let folder_id = Uuid::parse_str("bbbbbbbb-cccc-dddd-eeee-ffffffffffff").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![
            FakeStore::public_folder("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", None, "Public Root"),
            FakeStore::public_folder(
                "bbbbbbbb-cccc-dddd-eeee-ffffffffffff",
                Some("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
                "Public Child",
            ),
        ])),
        ..Default::default()
    };
    let deleted_public_folders = store.deleted_public_folders.clone();
    let public_folders = store.public_folders.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:DeleteFolder DeleteType="HardDelete">
                  <m:FolderIds><t:FolderId Id="public-folder:bbbbbbbb-cccc-dddd-eeee-ffffffffffff"/></m:FolderIds>
                </m:DeleteFolder>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:DeleteFolderResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert_eq!(
        deleted_public_folders.lock().unwrap().as_slice(),
        &[folder_id]
    );
    let folders = public_folders.lock().unwrap();
    let deleted = folders
        .iter()
        .find(|folder| folder.id == folder_id)
        .expect("child public folder should still exist as a lifecycle row");
    assert_eq!(deleted.lifecycle_state, "deleted");
}

#[tokio::test]
async fn delete_folder_rejects_public_folder_tree_root() {
    let folder_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            None,
            "Public Root",
        )])),
        ..Default::default()
    };
    let deleted_public_folders = store.deleted_public_folders.clone();
    let public_folders = store.public_folders.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:DeleteFolder DeleteType="HardDelete">
                  <m:FolderIds><t:FolderId Id="public-folder:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"/></m:FolderIds>
                </m:DeleteFolder>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:DeleteFolderResponse>"));
    assert!(body.contains("<m:ResponseCode>ErrorFolderNotFound</m:ResponseCode>"));
    assert!(body.contains("public folder tree root cannot be deleted"));
    assert!(deleted_public_folders.lock().unwrap().is_empty());
    let folders = public_folders.lock().unwrap();
    let root = folders
        .iter()
        .find(|folder| folder.id == folder_id)
        .expect("root public folder should remain active");
    assert_eq!(root.lifecycle_state, "active");
}

#[tokio::test]
async fn delete_folder_rejects_non_owner_public_folder_structural_change() {
    let folder_id = Uuid::parse_str("bbbbbbbb-cccc-dddd-eeee-ffffffffffff").unwrap();
    let non_owner = AuthenticatedAccount {
        tenant_id: FakeStore::account().tenant_id,
        account_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
        email: "delegate@example.test".to_string(),
        display_name: "Public Delegate".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let store = FakeStore {
        session: Some(non_owner),
        public_folders: Arc::new(Mutex::new(vec![
            FakeStore::public_folder("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", None, "Public Root"),
            FakeStore::public_folder(
                "bbbbbbbb-cccc-dddd-eeee-ffffffffffff",
                Some("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
                "Public Child",
            ),
        ])),
        ..Default::default()
    };
    let deleted_public_folders = store.deleted_public_folders.clone();
    let public_folders = store.public_folders.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:DeleteFolder DeleteType="HardDelete">
                  <m:FolderIds><t:FolderId Id="public-folder:bbbbbbbb-cccc-dddd-eeee-ffffffffffff"/></m:FolderIds>
                </m:DeleteFolder>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:DeleteFolderResponse>"));
    assert!(body.contains("<m:ResponseCode>ErrorAccessDenied</m:ResponseCode>"));
    assert!(body.contains("public folder structural changes require tree owner access"));
    assert!(deleted_public_folders.lock().unwrap().is_empty());
    let folders = public_folders.lock().unwrap();
    let folder = folders
        .iter()
        .find(|folder| folder.id == folder_id)
        .expect("public folder should remain active");
    assert_eq!(folder.lifecycle_state, "active");
}

#[tokio::test]
async fn delete_folder_rejects_public_folder_with_active_children() {
    let parent_id = Uuid::parse_str("bbbbbbbb-cccc-dddd-eeee-ffffffffffff").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![
            FakeStore::public_folder("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", None, "Public Root"),
            FakeStore::public_folder(
                "bbbbbbbb-cccc-dddd-eeee-ffffffffffff",
                Some("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
                "Public Parent",
            ),
            FakeStore::public_folder(
                "cccccccc-dddd-eeee-ffff-000000000000",
                Some("bbbbbbbb-cccc-dddd-eeee-ffffffffffff"),
                "Public Child",
            ),
        ])),
        ..Default::default()
    };
    let deleted_public_folders = store.deleted_public_folders.clone();
    let public_folders = store.public_folders.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:DeleteFolder DeleteType="HardDelete">
                  <m:FolderIds><t:FolderId Id="public-folder:bbbbbbbb-cccc-dddd-eeee-ffffffffffff"/></m:FolderIds>
                </m:DeleteFolder>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:DeleteFolderResponse>"));
    assert!(body.contains("<m:ResponseCode>ErrorFolderNotFound</m:ResponseCode>"));
    assert!(body.contains("public folder with active children cannot be deleted"));
    assert!(deleted_public_folders.lock().unwrap().is_empty());
    let folders = public_folders.lock().unwrap();
    let parent = folders
        .iter()
        .find(|folder| folder.id == parent_id)
        .expect("parent public folder should remain active");
    assert_eq!(parent.lifecycle_state, "active");
}

#[tokio::test]
async fn delete_folder_rejects_public_folder_with_active_items() {
    let folder_id = Uuid::parse_str("bbbbbbbb-cccc-dddd-eeee-ffffffffffff").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![
            FakeStore::public_folder("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", None, "Public Root"),
            FakeStore::public_folder(
                "bbbbbbbb-cccc-dddd-eeee-ffffffffffff",
                Some("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
                "Public Child",
            ),
        ])),
        public_folder_items: Arc::new(Mutex::new(vec![FakeStore::public_folder_item(
            "dddddddd-eeee-ffff-0000-111111111111",
            "bbbbbbbb-cccc-dddd-eeee-ffffffffffff",
            "Active post",
        )])),
        ..Default::default()
    };
    let deleted_public_folders = store.deleted_public_folders.clone();
    let public_folders = store.public_folders.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:DeleteFolder DeleteType="HardDelete">
                  <m:FolderIds><t:FolderId Id="public-folder:bbbbbbbb-cccc-dddd-eeee-ffffffffffff"/></m:FolderIds>
                </m:DeleteFolder>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:DeleteFolderResponse>"));
    assert!(body.contains("<m:ResponseCode>ErrorFolderNotFound</m:ResponseCode>"));
    assert!(body.contains("public folder with active items cannot be deleted"));
    assert!(deleted_public_folders.lock().unwrap().is_empty());
    let folders = public_folders.lock().unwrap();
    let folder = folders
        .iter()
        .find(|folder| folder.id == folder_id)
        .expect("public folder with active item should remain active");
    assert_eq!(folder.lifecycle_state, "active");
}

#[tokio::test]
async fn get_folder_returns_ews_error_for_unsupported_folder_ids() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetFolder><m:FolderIds><t:DistinguishedFolderId Id="inbox"/></m:FolderIds></m:GetFolder></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:GetFolderResponseMessage ResponseClass=\"Error\">"));
    assert!(body.contains("<m:ResponseCode>ErrorFolderNotFound</m:ResponseCode>"));
    assert!(body.contains("<t:ServerVersionInfo"));
}

#[tokio::test]
async fn get_folder_returns_system_mailbox_by_distinguished_id() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Inbox",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetFolder><m:FolderIds><t:DistinguishedFolderId Id="inbox"/></m:FolderIds></m:GetFolder></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:GetFolderResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains(
        "<t:FolderId Id=\"mailbox:55555555-5555-5555-5555-555555555555\" ChangeKey=\"ck-v1-"
    ));
    assert!(body.contains("<t:DisplayName>Inbox</t:DisplayName>"));
}

#[tokio::test]
async fn get_server_time_zones_returns_minimal_definitions() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetServerTimeZones ReturnFullTimeZoneData="false"/></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:GetServerTimeZonesResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:TimeZoneDefinition Id=\"UTC\""));
    assert!(body.contains("<t:TimeZoneDefinition Id=\"Europe/Berlin\""));
}

#[tokio::test]
async fn get_server_time_zones_projects_only_requested_compact_catalog_entries() {
    let service = ExchangeService::new(FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    });
    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetServerTimeZones ReturnFullTimeZoneData="false"><t:Ids><t:Id>UTC</t:Id></t:Ids></m:GetServerTimeZones></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:TimeZoneDefinition Id=\"UTC\""));
    assert!(!body.contains("Europe/Berlin"));

    let invalid = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetServerTimeZones ReturnFullTimeZoneData="false"><t:Ids><t:Id>Unknown/Zone</t:Id></t:Ids></m:GetServerTimeZones></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let invalid_body = response_text(invalid).await;
    assert!(invalid_body.contains("<m:ResponseCode>ErrorInvalidRequest</m:ResponseCode>"));
    assert!(!invalid_body.contains("TimeZoneDefinition Id="));
}

#[tokio::test]
async fn get_server_time_zones_requires_one_exact_case_catalog_id_when_filtered() {
    let service = ExchangeService::new(FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    });

    for request in [
        br#"<s:Envelope><s:Body><m:GetServerTimeZones /></s:Body></s:Envelope>"#.as_slice(),
        br#"<s:Envelope><s:Body><m:GetServerTimeZones ReturnFullTimeZoneData="true" /></s:Body></s:Envelope>"#.as_slice(),
        br#"<s:Envelope><s:Body><m:GetServerTimeZones ReturnFullTimeZoneData="false"><t:Ids /></m:GetServerTimeZones></s:Body></s:Envelope>"#.as_slice(),
        br#"<s:Envelope><s:Body><m:GetServerTimeZones ReturnFullTimeZoneData="false"><t:Ids><t:Id>utc</t:Id></t:Ids></m:GetServerTimeZones></s:Body></s:Envelope>"#.as_slice(),
        br#"<s:Envelope><s:Body><m:GetServerTimeZones ReturnFullTimeZoneData="false"><t:Ids><t:Id>UTC</t:Id><t:Id>utc</t:Id></t:Ids></m:GetServerTimeZones></s:Body></s:Envelope>"#.as_slice(),
        br#"<s:Envelope><s:Body><m:GetServerTimeZones ReturnFullTimeZoneData="false"><t:Ids><t:Id>UTC</t:Id><t:Id>Europe/Berlin</t:Id></t:Ids></m:GetServerTimeZones></s:Body></s:Envelope>"#.as_slice(),
        br#"<s:Envelope><s:Body><m:GetServerTimeZones ReturnFullTimeZoneData="false"><t:Ids><t:Id>UTC</t:Id></t:Ids><t:Ids><t:Id>Europe/Berlin</t:Id></t:Ids></m:GetServerTimeZones></s:Body></s:Envelope>"#.as_slice(),
    ] {
        let response = service.handle(&bearer_headers(), request).await.unwrap();
        let body = response_text(response).await;
        assert!(body.contains("<m:ResponseCode>ErrorInvalidRequest</m:ResponseCode>"));
        assert!(!body.contains("TimeZoneDefinition Id="));
    }
}

#[tokio::test]
async fn resolve_names_returns_authenticated_mailbox_match() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:ResolveNames><m:UnresolvedEntry>alice</m:UnresolvedEntry></m:ResolveNames></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:ResolveNamesResponseMessage ResponseClass=\"Success\">"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<m:ResolutionSet TotalItemsInView=\"1\""));
    assert!(body.contains("<t:Name>Alice</t:Name>"));
    assert!(body.contains("<t:EmailAddress>alice@example.test</t:EmailAddress>"));
    assert!(body.contains("<t:MailboxType>Mailbox</t:MailboxType>"));
    assert!(body.contains("<t:ServerVersionInfo"));
}

#[tokio::test]
async fn resolve_names_returns_tenant_directory_account_match() {
    let mut bob = FakeStore::account();
    bob.account_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    bob.email = "bob@example.test".to_string();
    bob.display_name = "Bob Tenant".to_string();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        directory_accounts: Arc::new(Mutex::new(vec![bob])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:ResolveNames><m:UnresolvedEntry>bob@example.test</m:UnresolvedEntry></m:ResolveNames></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:ResolveNamesResponseMessage ResponseClass=\"Success\">"));
    assert!(body.contains("<t:Name>Bob Tenant</t:Name>"));
    assert!(body.contains("<t:EmailAddress>bob@example.test</t:EmailAddress>"));
    assert!(!body.contains("mallory@other.test"));
}

#[tokio::test]
async fn resolve_names_returns_accessible_contact_match() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "contacts", "Contacts",
        )])),
        contacts: Arc::new(Mutex::new(vec![FakeStore::contact(
            "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "Bob Contact",
            "bob@example.test",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:ResolveNames><m:UnresolvedEntry>Bob Contact</m:UnresolvedEntry></m:ResolveNames></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:ResolveNamesResponseMessage ResponseClass=\"Success\">"));
    assert!(body.contains("<t:Name>Bob Contact</t:Name>"));
    assert!(body.contains("<t:EmailAddress>bob@example.test</t:EmailAddress>"));
    assert!(body.contains("<t:MailboxType>Contact</t:MailboxType>"));
}

#[tokio::test]
async fn resolve_names_hidden_authenticated_account_can_resolve_self() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        omit_principal_from_directory: true,
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:ResolveNames><m:UnresolvedEntry>alice@example.test</m:UnresolvedEntry></m:ResolveNames></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:ResolveNamesResponseMessage ResponseClass=\"Success\">"));
    assert!(body.contains("<t:EmailAddress>alice@example.test</t:EmailAddress>"));
}

#[tokio::test]
async fn resolve_names_returns_no_results_for_non_directory_names() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:ResolveNames><m:UnresolvedEntry>bob</m:UnresolvedEntry></m:ResolveNames></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:ResolveNamesResponseMessage ResponseClass=\"Error\">"));
    assert!(body.contains("<m:ResponseCode>ErrorNameResolutionNoResults</m:ResponseCode>"));
    assert!(!body.contains("bob@example.test"));
}

#[tokio::test]
async fn resolve_names_projects_canonical_distribution_lists_only_for_active_directory() {
    // [MS-OXWSRSLNM] sections 3.1.4.1, 3.1.4.1.3.5, and 3.1.4.1.4.1:
    // ActiveDirectory resolution projects visible canonical distribution lists; Contacts does not.
    let service = ExchangeService::new(FakeStore {
        session: Some(FakeStore::account()),
        group_aliases: Arc::new(Mutex::new(vec![(
            Uuid::parse_str("abababab-abab-abab-abab-abababababab").unwrap(),
            "All Hands".to_string(),
            "all@example.test".to_string(),
        )])),
        ..Default::default()
    });

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:ResolveNames><m:UnresolvedEntry>all@example.test</m:UnresolvedEntry></m:ResolveNames></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:ResolveNamesResponseMessage ResponseClass=\"Success\">"));
    assert!(body.contains("<t:Name>All Hands</t:Name>"));
    assert!(body.contains("<t:EmailAddress>all@example.test</t:EmailAddress>"));
    assert!(body.contains("<t:MailboxType>PublicDL</t:MailboxType>"));

    let contacts_response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:ResolveNames SearchScope="Contacts"><m:UnresolvedEntry>all@example.test</m:UnresolvedEntry></m:ResolveNames></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let contacts_body = response_text(contacts_response).await;
    assert!(contacts_body.contains("<m:ResponseCode>ErrorNameResolutionNoResults</m:ResponseCode>"));
    assert!(!contacts_body.contains("all@example.test"));
}

#[tokio::test]
async fn resolve_names_rejects_ambiguous_and_inaccessible_contact_results() {
    // [MS-OXWSRSLNM] sections 3.1.4.1, 3.1.4.1.3.5, and 3.1.4.1.4.1:
    // visible ambiguity stays explicit while inaccessible contacts are absent.
    let hidden_owner = Uuid::parse_str("ffffffff-ffff-ffff-ffff-ffffffffffff").unwrap();
    let mut hidden_collection = FakeStore::collection("hidden", "contacts", "Hidden Contacts");
    hidden_collection.owner_account_id = hidden_owner;
    hidden_collection.is_owned = false;
    hidden_collection.rights.may_read = false;
    let mut hidden_contact = FakeStore::contact(
        "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee",
        "Bob Hidden",
        "hidden@example.test",
    );
    hidden_contact.collection_id = "hidden".to_string();
    hidden_contact.owner_account_id = hidden_owner;
    hidden_contact.rights.may_read = false;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![hidden_collection])),
        contacts: Arc::new(Mutex::new(vec![hidden_contact])),
        directory_accounts: Arc::new(Mutex::new(vec![
            {
                let mut account = FakeStore::account();
                account.account_id =
                    Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
                account.display_name = "Bob Tenant".to_string();
                account.email = "bob@example.test".to_string();
                account
            },
            {
                let mut account = FakeStore::account();
                account.account_id =
                    Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap();
                account.display_name = "Bob Other".to_string();
                account.email = "bob.other@example.test".to_string();
                account
            },
            {
                let mut account = FakeStore::account();
                account.tenant_id = Uuid::from_u128(0xdddddddddddddddddddddddddddddddd);
                account.account_id =
                    Uuid::parse_str("dddddddd-dddd-dddd-dddd-dddddddddddd").unwrap();
                account.display_name = "Foreign Directory".to_string();
                account.email = "foreign@example.test".to_string();
                account
            },
        ])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let ambiguous = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:ResolveNames><m:UnresolvedEntry>bob</m:UnresolvedEntry></m:ResolveNames></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let ambiguous_body = response_text(ambiguous).await;
    assert!(ambiguous_body
        .contains("<m:ResponseCode>ErrorNameResolutionMultipleResults</m:ResponseCode>"));
    assert!(!ambiguous_body.contains("bob@example.test"));

    let hidden = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:ResolveNames SearchScope="Contacts"><m:UnresolvedEntry>Bob Hidden</m:UnresolvedEntry></m:ResolveNames></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let hidden_body = response_text(hidden).await;
    assert!(hidden_body.contains("<m:ResponseCode>ErrorNameResolutionNoResults</m:ResponseCode>"));
    assert!(!hidden_body.contains("hidden@example.test"));

    let foreign = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:ResolveNames><m:UnresolvedEntry>Foreign Directory</m:UnresolvedEntry></m:ResolveNames></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let foreign_body = response_text(foreign).await;
    assert!(foreign_body.contains("<m:ResponseCode>ErrorNameResolutionNoResults</m:ResponseCode>"));
    assert!(!foreign_body.contains("foreign@example.test"));
}

#[tokio::test]
async fn find_people_projects_canonical_accounts_and_contacts() {
    let mut bob = FakeStore::account();
    bob.account_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    bob.email = "bob@example.test".to_string();
    bob.display_name = "Bob Tenant".to_string();
    let mut mallory = FakeStore::account();
    mallory.tenant_id = Uuid::from_u128(0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb);
    mallory.account_id = Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap();
    mallory.email = "mallory@other.test".to_string();
    mallory.display_name = "Mallory Foreign".to_string();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        directory_accounts: Arc::new(Mutex::new(vec![bob, mallory])),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "contacts", "Contacts",
        )])),
        contacts: Arc::new(Mutex::new(vec![FakeStore::contact(
            "dddddddd-dddd-dddd-dddd-dddddddddddd",
            "Bob Contact",
            "bob.contact@example.test",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:FindPeople><m:QueryString>bob</m:QueryString></m:FindPeople></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:FindPeopleResponseMessage ResponseClass=\"Success\">"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(
        body.contains("<t:PersonaId Id=\"persona:account:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb\"/>")
    );
    assert!(body.contains("<t:PersonaType>Person</t:PersonaType>"));
    assert!(body.contains("<t:DisplayName>Bob Tenant</t:DisplayName>"));
    assert!(body.contains("<t:EmailAddress>bob@example.test</t:EmailAddress>"));
    assert!(
        body.contains("<t:PersonaId Id=\"persona:contact:dddddddd-dddd-dddd-dddd-dddddddddddd\"/>")
    );
    assert!(body.contains("<t:PersonaType>Contact</t:PersonaType>"));
    assert!(body.contains("<t:DisplayName>Bob Contact</t:DisplayName>"));
    assert!(body.contains("<t:EmailAddress>bob.contact@example.test</t:EmailAddress>"));
    assert!(!body.contains("mallory@other.test"));
    assert!(!body.contains("PublicDL"));
}

#[tokio::test]
async fn get_persona_resolves_only_visible_stateless_persona_ids() {
    let mut foreign = FakeStore::contact(
        "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee",
        "Hidden Contact",
        "hidden@example.test",
    );
    foreign.owner_account_id = Uuid::parse_str("ffffffff-ffff-ffff-ffff-ffffffffffff").unwrap();
    foreign.collection_id = "foreign".to_string();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "contacts", "Contacts",
        )])),
        contacts: Arc::new(Mutex::new(vec![
            FakeStore::contact(
                "dddddddd-dddd-dddd-dddd-dddddddddddd",
                "Bob Contact",
                "bob.contact@example.test",
            ),
            foreign,
        ])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetPersona><m:PersonaId Id="persona:contact:dddddddd-dddd-dddd-dddd-dddddddddddd"/></m:GetPersona></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:GetPersonaResponseMessage ResponseClass=\"Success\">"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(
        body.contains("<t:PersonaId Id=\"persona:contact:dddddddd-dddd-dddd-dddd-dddddddddddd\"/>")
    );
    assert!(body.contains("<t:DisplayName>Bob Contact</t:DisplayName>"));
    assert!(body.contains("<t:EmailAddress>bob.contact@example.test</t:EmailAddress>"));

    let foreign_response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetPersona><m:PersonaId Id="persona:contact:eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"/></m:GetPersona></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let foreign_body = response_text(foreign_response).await;
    assert!(foreign_body.contains("<m:GetPersonaResponseMessage ResponseClass=\"Error\">"));
    assert!(foreign_body.contains("<m:ResponseCode>ErrorItemNotFound</m:ResponseCode>"));
    assert!(!foreign_body.contains("hidden@example.test"));
}

#[tokio::test]
async fn get_user_availability_returns_canonical_busy_events() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "calendar", "Calendar",
        )])),
        events: Arc::new(Mutex::new(vec![
            AccessibleEvent {
                id: Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap(),
                uid: "cccccccc-cccc-cccc-cccc-cccccccccccc".to_string(),
                collection_id: "default".to_string(),
                owner_account_id: FakeStore::account().account_id,
                owner_email: "alice@example.test".to_string(),
                owner_display_name: "Alice".to_string(),
                rights: FakeStore::rights(),
                date: "2026-05-04".to_string(),
                time: "09:30".to_string(),
                time_zone: "UTC".to_string(),
                duration_minutes: 45,
                all_day: false,
                status: "confirmed".to_string(),
                sequence: 0,
                recurrence_rule: String::new(),
                recurrence_json: "{}".to_string(),
                recurrence_exceptions_json: "[]".to_string(),
                title: "Planning".to_string(),
                location: "Room 1".to_string(),
                organizer_json: "{}".to_string(),
                attendees: String::new(),
                attendees_json: String::new(),
                notes: "Agenda".to_string(),
                body_html: String::new(),
            },
            AccessibleEvent {
                id: Uuid::parse_str("ffffffff-ffff-ffff-ffff-ffffffffffff").unwrap(),
                uid: "ffffffff-ffff-ffff-ffff-ffffffffffff".to_string(),
                collection_id: "default".to_string(),
                owner_account_id: FakeStore::account().account_id,
                owner_email: "alice@example.test".to_string(),
                owner_display_name: "Alice".to_string(),
                rights: FakeStore::rights(),
                date: "2026-05-07".to_string(),
                time: "09:30".to_string(),
                time_zone: "UTC".to_string(),
                duration_minutes: 45,
                all_day: false,
                status: "confirmed".to_string(),
                sequence: 0,
                recurrence_rule: String::new(),
                recurrence_json: "{}".to_string(),
                recurrence_exceptions_json: "[]".to_string(),
                title: "Outside window".to_string(),
                location: String::new(),
                organizer_json: "{}".to_string(),
                attendees: String::new(),
                attendees_json: String::new(),
                notes: String::new(),
                body_html: String::new(),
            },
        ])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:GetUserAvailabilityRequest>
                  <m:MailboxDataArray>
                    <t:MailboxData>
                      <t:Email><t:Address>alice@example.test</t:Address></t:Email>
                    </t:MailboxData>
                  </m:MailboxDataArray>
                  <t:FreeBusyViewOptions>
                    <t:TimeWindow>
                      <t:StartTime>2026-05-04T00:00:00Z</t:StartTime>
                      <t:EndTime>2026-05-05T00:00:00Z</t:EndTime>
                    </t:TimeWindow>
                  </t:FreeBusyViewOptions>
                </m:GetUserAvailabilityRequest>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:GetUserAvailabilityResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<m:FreeBusyView>"));
    assert!(body.contains("</m:FreeBusyView>"));
    assert!(!body.contains("<t:FreeBusyView>"));
    assert!(body.contains("<t:FreeBusyViewType>Detailed</t:FreeBusyViewType>"));
    assert!(body.contains("<t:CalendarEventArray><t:CalendarEvent>"));
    assert!(body.contains("<t:StartTime>2026-05-04T09:30:00Z</t:StartTime>"));
    assert!(body.contains("<t:EndTime>2026-05-04T10:15:00Z</t:EndTime>"));
    assert!(!body.contains("2026-05-07T09:30:00Z"));
    assert!(body.contains("<t:ServerVersionInfo"));
}

#[tokio::test]
async fn get_user_availability_returns_suggestions_when_requested() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "calendar", "Calendar",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:GetUserAvailabilityRequest>
                  <m:MailboxDataArray>
                    <t:MailboxData>
                      <t:Email><t:Address>alice@example.test</t:Address></t:Email>
                    </t:MailboxData>
                  </m:MailboxDataArray>
                  <t:FreeBusyViewOptions>
                    <t:TimeWindow>
                      <t:StartTime>2026-05-15T00:00:00</t:StartTime>
                      <t:EndTime>2026-05-17T00:00:00</t:EndTime>
                    </t:TimeWindow>
                    <t:RequestedView>Detailed</t:RequestedView>
                  </t:FreeBusyViewOptions>
                  <t:SuggestionsViewOptions>
                    <t:MeetingDurationInMinutes>60</t:MeetingDurationInMinutes>
                    <t:DetailedSuggestionsWindow>
                      <t:StartTime>2026-05-15T00:00:00</t:StartTime>
                      <t:EndTime>2026-05-17T00:00:00</t:EndTime>
                    </t:DetailedSuggestionsWindow>
                  </t:SuggestionsViewOptions>
                </m:GetUserAvailabilityRequest>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:GetUserAvailabilityResponse>"));
    assert!(body.contains("<m:FreeBusyResponseArray>"));
    assert!(body.contains("<m:FreeBusyView>"));
    assert!(body.contains("<m:SuggestionsResponse>"));
    assert!(body.contains("<m:SuggestionDayResultArray>"));
    assert!(body.contains("<t:SuggestionDayResult>"));
    assert!(body.contains("<t:Date>2026-05-15T00:00:00Z</t:Date>"));
    assert!(body.contains("<t:SuggestionArray></t:SuggestionArray>"));
}

#[tokio::test]
// [MS-OXWAVLS] §3.1.4.1: one response is returned for each MailboxData entry.
async fn get_user_availability_returns_ordered_responses_for_multiple_readable_mailboxes() {
    let alice = FakeStore::account();
    let alice_calendar = FakeStore::collection("default", "calendar", "Calendar");
    let mut bob_calendar = FakeStore::collection(
        "shared-calendar-bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        "calendar",
        "Bob Calendar",
    );
    bob_calendar.owner_account_id =
        Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    bob_calendar.owner_email = "bob@example.test".to_string();
    bob_calendar.owner_display_name = "Bob".to_string();
    bob_calendar.is_owned = false;
    let alice_event = AccessibleEvent {
        id: Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap(),
        uid: "alice-availability".to_string(),
        collection_id: alice_calendar.id.clone(),
        owner_account_id: alice.account_id,
        owner_email: alice.email.clone(),
        owner_display_name: "Alice".to_string(),
        rights: FakeStore::rights(),
        date: "2026-05-04".to_string(),
        time: "09:00".to_string(),
        time_zone: "UTC".to_string(),
        duration_minutes: 30,
        all_day: false,
        status: "confirmed".to_string(),
        sequence: 0,
        recurrence_rule: String::new(),
        recurrence_json: "{}".to_string(),
        recurrence_exceptions_json: "[]".to_string(),
        title: "Alice private event".to_string(),
        location: String::new(),
        organizer_json: "{}".to_string(),
        attendees: String::new(),
        attendees_json: "[]".to_string(),
        notes: String::new(),
        body_html: String::new(),
    };
    let mut bob_event = alice_event.clone();
    bob_event.id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbc").unwrap();
    bob_event.uid = "bob-availability".to_string();
    bob_event.collection_id = bob_calendar.id.clone();
    bob_event.owner_account_id = bob_calendar.owner_account_id;
    bob_event.owner_email = bob_calendar.owner_email.clone();
    bob_event.owner_display_name = bob_calendar.owner_display_name.clone();
    bob_event.time = "11:00".to_string();
    bob_event.title = "Bob private event".to_string();
    let service = ExchangeService::new(FakeStore {
        session: Some(alice),
        calendar_collections: Arc::new(Mutex::new(vec![alice_calendar, bob_calendar])),
        events: Arc::new(Mutex::new(vec![alice_event, bob_event])),
        ..Default::default()
    });

    let request = br#"<s:Envelope><s:Body><m:GetUserAvailabilityRequest><m:MailboxDataArray><t:MailboxData><t:Email><t:Address>alice@example.test</t:Address></t:Email></t:MailboxData><t:MailboxData><t:Email><t:Address>bob@example.test</t:Address></t:Email></t:MailboxData></m:MailboxDataArray><t:FreeBusyViewOptions><t:TimeWindow><t:StartTime>2026-05-04T00:00:00Z</t:StartTime><t:EndTime>2026-05-05T00:00:00Z</t:EndTime></t:TimeWindow></t:FreeBusyViewOptions></m:GetUserAvailabilityRequest></s:Body></s:Envelope>"#;
    let response = service.handle(&bearer_headers(), request).await.unwrap();
    let body = response_text(response).await;

    assert_eq!(body.matches("<m:FreeBusyResponse>").count(), 2);
    let alice_busy = body.find("2026-05-04T09:00:00Z").unwrap();
    let bob_busy = body.find("2026-05-04T11:00:00Z").unwrap();
    assert!(alice_busy < bob_busy);
    assert!(!body.contains("Alice private event"));
    assert!(!body.contains("Bob private event"));
}

#[tokio::test]
async fn get_user_availability_redacts_an_unreadable_mailbox_without_hiding_other_results() {
    let alice = FakeStore::account();
    let alice_calendar = FakeStore::collection("default", "calendar", "Calendar");
    let mut bob_calendar = FakeStore::collection(
        "shared-calendar-bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        "calendar",
        "Bob Calendar",
    );
    bob_calendar.owner_account_id =
        Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    bob_calendar.owner_email = "bob@example.test".to_string();
    bob_calendar.owner_display_name = "Bob".to_string();
    bob_calendar.is_owned = false;
    bob_calendar.rights.may_read = false;
    let alice_event = AccessibleEvent {
        id: Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap(),
        uid: "alice-availability".to_string(),
        collection_id: alice_calendar.id.clone(),
        owner_account_id: alice.account_id,
        owner_email: alice.email.clone(),
        owner_display_name: "Alice".to_string(),
        rights: FakeStore::rights(),
        date: "2026-05-04".to_string(),
        time: "09:00".to_string(),
        time_zone: "UTC".to_string(),
        duration_minutes: 30,
        all_day: false,
        status: "confirmed".to_string(),
        sequence: 0,
        recurrence_rule: String::new(),
        recurrence_json: "{}".to_string(),
        recurrence_exceptions_json: "[]".to_string(),
        title: "Alice private event".to_string(),
        location: String::new(),
        organizer_json: "{}".to_string(),
        attendees: String::new(),
        attendees_json: "[]".to_string(),
        notes: String::new(),
        body_html: String::new(),
    };
    let service = ExchangeService::new(FakeStore {
        session: Some(alice),
        calendar_collections: Arc::new(Mutex::new(vec![alice_calendar, bob_calendar])),
        events: Arc::new(Mutex::new(vec![alice_event])),
        ..Default::default()
    });

    let request = br#"<s:Envelope><s:Body><m:GetUserAvailabilityRequest><m:MailboxDataArray><t:MailboxData><t:Email><t:Address>alice@example.test</t:Address></t:Email></t:MailboxData><t:MailboxData><t:Email><t:Address>bob@example.test</t:Address></t:Email></t:MailboxData></m:MailboxDataArray><t:FreeBusyViewOptions><t:TimeWindow><t:StartTime>2026-05-04T00:00:00Z</t:StartTime><t:EndTime>2026-05-05T00:00:00Z</t:EndTime></t:TimeWindow></t:FreeBusyViewOptions></m:GetUserAvailabilityRequest></s:Body></s:Envelope>"#;
    let response = service.handle(&bearer_headers(), request).await.unwrap();
    let body = response_text(response).await;

    assert_eq!(body.matches("<m:FreeBusyResponse>").count(), 2);
    assert!(body.contains("2026-05-04T09:00:00Z"));
    assert!(body.contains("<m:ResponseCode>ErrorFreeBusyGenerationFailed</m:ResponseCode>"));
    assert!(!body.contains("Bob Calendar"));
}

#[tokio::test]
async fn get_user_availability_enforces_calendar_grants_and_redacts_failures() {
    let mut shared_calendar = FakeStore::collection(
        "shared-calendar-bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        "calendar",
        "Bob Calendar",
    );
    shared_calendar.owner_account_id =
        Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    shared_calendar.owner_email = "bob@example.test".to_string();
    shared_calendar.owner_display_name = "Bob".to_string();
    shared_calendar.is_owned = false;
    let mut event = AccessibleEvent {
        id: Uuid::parse_str("dddddddd-dddd-dddd-dddd-dddddddddddd").unwrap(),
        uid: "shared-busy".to_string(),
        collection_id: shared_calendar.id.clone(),
        owner_account_id: shared_calendar.owner_account_id,
        owner_email: shared_calendar.owner_email.clone(),
        owner_display_name: shared_calendar.owner_display_name.clone(),
        rights: shared_calendar.rights.clone(),
        date: "2026-05-04".to_string(),
        time: "09:00".to_string(),
        time_zone: "UTC".to_string(),
        duration_minutes: 30,
        all_day: false,
        status: "confirmed".to_string(),
        sequence: 0,
        recurrence_rule: String::new(),
        recurrence_json: "{}".to_string(),
        recurrence_exceptions_json: "[]".to_string(),
        title: "Protected subject".to_string(),
        location: "Protected room".to_string(),
        organizer_json: "{}".to_string(),
        attendees: "protected@example.test".to_string(),
        attendees_json: "[]".to_string(),
        notes: "Protected body".to_string(),
        body_html: String::new(),
    };
    let store = FakeStore {
        session: Some(FakeStore::account()),
        calendar_collections: Arc::new(Mutex::new(vec![shared_calendar.clone()])),
        events: Arc::new(Mutex::new(vec![event.clone()])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let request = br#"<s:Envelope><s:Body><m:GetUserAvailabilityRequest><m:MailboxDataArray><t:MailboxData><t:Email><t:Address>bob@example.test</t:Address></t:Email></t:MailboxData></m:MailboxDataArray><t:FreeBusyViewOptions><t:TimeWindow><t:StartTime>2026-05-04T00:00:00Z</t:StartTime><t:EndTime>2026-05-05T00:00:00Z</t:EndTime></t:TimeWindow></t:FreeBusyViewOptions></m:GetUserAvailabilityRequest></s:Body></s:Envelope>"#;
    let response = service.handle(&bearer_headers(), request).await.unwrap();
    let body = response_text(response).await;
    assert!(body.contains("2026-05-04T09:00:00Z"));
    assert!(!body.contains("Protected subject"));
    assert!(!body.contains("protected@example.test"));

    shared_calendar.rights.may_read = false;
    event.rights.may_read = false;
    let denied = ExchangeService::new(FakeStore {
        session: Some(FakeStore::account()),
        calendar_collections: Arc::new(Mutex::new(vec![shared_calendar])),
        events: Arc::new(Mutex::new(vec![event])),
        ..Default::default()
    })
    .handle(&bearer_headers(), request)
    .await
    .unwrap();
    let denied_body = response_text(denied).await;
    assert!(denied_body.contains("<m:ResponseCode>ErrorFreeBusyGenerationFailed</m:ResponseCode>"));
    assert!(!denied_body.contains("Protected subject"));
}

#[tokio::test]
async fn get_user_availability_uses_only_the_default_readable_calendar() {
    let default_calendar = FakeStore::collection("default", "calendar", "Calendar");
    let custom_calendar = FakeStore::collection(
        "cccccccc-cccc-cccc-cccc-cccccccccccc",
        "calendar",
        "Private planning",
    );
    let default_event = AccessibleEvent {
        id: Uuid::parse_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeee1").unwrap(),
        uid: "default-calendar-busy".to_string(),
        collection_id: default_calendar.id.clone(),
        owner_account_id: FakeStore::account().account_id,
        owner_email: "alice@example.test".to_string(),
        owner_display_name: "Alice".to_string(),
        rights: FakeStore::rights(),
        date: "2026-05-04".to_string(),
        time: "09:00".to_string(),
        time_zone: "UTC".to_string(),
        duration_minutes: 30,
        all_day: false,
        status: "confirmed".to_string(),
        sequence: 0,
        recurrence_rule: String::new(),
        recurrence_json: "{}".to_string(),
        recurrence_exceptions_json: "[]".to_string(),
        title: "Default calendar busy".to_string(),
        location: String::new(),
        organizer_json: "{}".to_string(),
        attendees: String::new(),
        attendees_json: "[]".to_string(),
        notes: String::new(),
        body_html: String::new(),
    };
    let mut custom_event = default_event.clone();
    custom_event.id = Uuid::parse_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeee2").unwrap();
    custom_event.collection_id = custom_calendar.id.clone();
    custom_event.time = "12:00".to_string();
    let service = ExchangeService::new(FakeStore {
        session: Some(FakeStore::account()),
        calendar_collections: Arc::new(Mutex::new(vec![default_calendar, custom_calendar])),
        events: Arc::new(Mutex::new(vec![default_event, custom_event])),
        ..Default::default()
    });

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetUserAvailabilityRequest><m:MailboxDataArray><t:MailboxData><t:Email><t:Address>alice@example.test</t:Address></t:Email></t:MailboxData></m:MailboxDataArray><t:FreeBusyViewOptions><t:TimeWindow><t:StartTime>2026-05-04T00:00:00Z</t:StartTime><t:EndTime>2026-05-05T00:00:00Z</t:EndTime></t:TimeWindow></t:FreeBusyViewOptions></m:GetUserAvailabilityRequest></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("2026-05-04T09:00:00Z"));
    assert!(!body.contains("2026-05-04T12:00:00Z"));
}

#[tokio::test]
async fn get_user_availability_expands_supported_recurrence_in_utc_across_berlin_dst() {
    let calendar = FakeStore::collection("default", "calendar", "Calendar");
    let event = AccessibleEvent {
        id: Uuid::parse_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").unwrap(),
        uid: "berlin-recurring".to_string(),
        collection_id: "default".to_string(),
        owner_account_id: FakeStore::account().account_id,
        owner_email: "alice@example.test".to_string(),
        owner_display_name: "Alice".to_string(),
        rights: FakeStore::rights(),
        date: "2026-03-29".to_string(),
        time: "09:00".to_string(),
        time_zone: "Europe/Berlin".to_string(),
        duration_minutes: 60,
        all_day: false,
        status: "confirmed".to_string(),
        sequence: 0,
        recurrence_rule: "FREQ=DAILY;COUNT=2".to_string(),
        recurrence_json: "{}".to_string(),
        recurrence_exceptions_json: "[]".to_string(),
        title: "Busy".to_string(),
        location: String::new(),
        organizer_json: "{}".to_string(),
        attendees: String::new(),
        attendees_json: "[]".to_string(),
        notes: String::new(),
        body_html: String::new(),
    };
    let service = ExchangeService::new(FakeStore {
        session: Some(FakeStore::account()),
        calendar_collections: Arc::new(Mutex::new(vec![calendar])),
        events: Arc::new(Mutex::new(vec![event])),
        ..Default::default()
    });
    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetUserAvailabilityRequest><m:MailboxDataArray><t:MailboxData><t:Email><t:Address>alice@example.test</t:Address></t:Email></t:MailboxData></m:MailboxDataArray><t:FreeBusyViewOptions><t:TimeWindow><t:StartTime>2026-03-29T00:00:00Z</t:StartTime><t:EndTime>2026-03-31T00:00:00Z</t:EndTime></t:TimeWindow></t:FreeBusyViewOptions></m:GetUserAvailabilityRequest></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("2026-03-29T07:00:00Z"));
    assert!(body.contains("2026-03-30T07:00:00Z"));
}

#[tokio::test]
async fn get_user_availability_rejects_invalid_time_window_without_event_detail() {
    let service = ExchangeService::new(FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    });
    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetUserAvailabilityRequest><m:MailboxDataArray><t:MailboxData><t:Email><t:Address>alice@example.test</t:Address></t:Email></t:MailboxData></m:MailboxDataArray><t:FreeBusyViewOptions><t:TimeWindow><t:StartTime>2026-05-05T00:00:00Z</t:StartTime><t:EndTime>2026-05-04T00:00:00Z</t:EndTime></t:TimeWindow></t:FreeBusyViewOptions></m:GetUserAvailabilityRequest></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>ErrorFreeBusyGenerationFailed</m:ResponseCode>"));
    assert!(!body.contains("CalendarEvent"));
}

#[tokio::test]
async fn update_folder_requires_supported_change_payload() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    for operation in ["UpdateFolder"] {
        let request = format!("<s:Envelope><s:Body><m:{operation} /></s:Body></s:Envelope>");
        let response = service
            .handle(&bearer_headers(), request.as_bytes())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_text(response).await;
        assert!(body.contains(&format!("<m:{operation}Response>")));
        assert!(body.contains("ResponseClass=\"Error\""));
        assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
        assert!(body.contains("<t:ServerVersionInfo"));
    }
}

#[tokio::test]
async fn update_item_rejects_unsupported_item_ids() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:UpdateItem><m:ItemChanges><t:ItemChange><t:ItemId Id="message:dddddddd-dddd-dddd-dddd-dddddddddddd"/><t:Updates/></t:ItemChange></m:ItemChanges></m:UpdateItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:UpdateItemResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
}

#[tokio::test]
async fn update_item_updates_message_read_and_flag_state() {
    let mut email = FakeStore::email(
        "dddddddd-dddd-dddd-dddd-dddddddddddd",
        "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee",
        "inbox",
        "Mailbox message",
    );
    email.unread = true;
    email.flagged = false;
    let emails = Arc::new(Mutex::new(vec![email]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        emails: emails.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:UpdateItem>
                  <m:ItemChanges>
                    <t:ItemChange>
                      <t:ItemId Id="message:dddddddd-dddd-dddd-dddd-dddddddddddd"/>
                      <t:Updates>
                        <t:SetItemField>
                          <t:FieldURI FieldURI="message:IsRead"/>
                          <t:Message><t:IsRead>true</t:IsRead></t:Message>
                        </t:SetItemField>
                        <t:SetItemField>
                          <t:FieldURI FieldURI="message:Flag"/>
                          <t:Message><t:Flag><t:FlagStatus>Flagged</t:FlagStatus></t:Flag></t:Message>
                        </t:SetItemField>
                      </t:Updates>
                    </t:ItemChange>
                  </m:ItemChanges>
                </m:UpdateItem>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:UpdateItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:IsRead>true</t:IsRead>"));
    let updated = emails.lock().unwrap()[0].clone();
    assert!(!updated.unread);
    assert!(updated.flagged);
}

#[tokio::test]
async fn update_item_rejects_stale_change_key_without_mutating_message() {
    let mut email = FakeStore::email(
        "dddddddd-dddd-dddd-dddd-dddddddddddd",
        "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee",
        "inbox",
        "Mailbox message",
    );
    email.unread = true;
    let emails = Arc::new(Mutex::new(vec![email]));
    let service = ExchangeService::new(FakeStore {
        session: Some(FakeStore::account()),
        emails: emails.clone(),
        ..Default::default()
    });

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope><s:Body><m:UpdateItem><m:ItemChanges><t:ItemChange>
              <t:ItemId Id="message:dddddddd-dddd-dddd-dddd-dddddddddddd" ChangeKey="stale"/>
              <t:Updates><t:SetItemField><t:FieldURI FieldURI="message:IsRead"/>
                <t:Message><t:IsRead>true</t:IsRead></t:Message>
              </t:SetItemField></t:Updates>
            </t:ItemChange></m:ItemChanges></m:UpdateItem></s:Body></s:Envelope>
            "#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>ErrorIrresolvableConflict</m:ResponseCode>"));
    assert!(emails.lock().unwrap()[0].unread);
}

#[tokio::test]
async fn update_item_updates_public_folder_item() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            None,
            "Public Root",
        )])),
        public_folder_items: Arc::new(Mutex::new(vec![FakeStore::public_folder_item(
            "abababab-abab-abab-abab-abababababab",
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "Public post",
        )])),
        ..Default::default()
    };
    let public_folder_items = store.public_folder_items.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:UpdateItem ConflictResolution="AlwaysOverwrite">
                  <m:ItemChanges>
                    <t:ItemChange>
                      <t:ItemId Id="public-folder-item:abababab-abab-abab-abab-abababababab"/>
                      <t:Updates>
                        <t:SetItemField>
                          <t:FieldURI FieldURI="item:Subject"/>
                          <t:Message>
                            <t:Subject>Updated public post</t:Subject>
                            <t:Body BodyType="Text">Updated public body</t:Body>
                          </t:Message>
                        </t:SetItemField>
                      </t:Updates>
                    </t:ItemChange>
                  </m:ItemChanges>
                </m:UpdateItem>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:UpdateItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("public-folder-item:abababab-abab-abab-abab-abababababab"));
    assert!(body.contains("<t:Subject>Updated public post</t:Subject>"));
    assert!(body.contains("<t:Body BodyType=\"Text\">Updated public body</t:Body>"));
    let updated = public_folder_items.lock().unwrap()[0].clone();
    assert_eq!(updated.subject, "Updated public post");
    assert_eq!(updated.body_text, "Updated public body");
    assert_eq!(updated.change_counter, 2);
}

#[tokio::test]
async fn update_item_rejects_public_folder_item_without_write_access() {
    let non_owner = AuthenticatedAccount {
        tenant_id: FakeStore::account().tenant_id,
        account_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
        email: "delegate@example.test".to_string(),
        display_name: "Public Delegate".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let mut public_folder =
        FakeStore::public_folder("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", None, "Public Root");
    public_folder.rights.may_write = false;
    public_folder.rights.may_delete = false;
    public_folder.rights.may_share = false;
    let store = FakeStore {
        session: Some(non_owner),
        public_folders: Arc::new(Mutex::new(vec![public_folder])),
        public_folder_items: Arc::new(Mutex::new(vec![FakeStore::public_folder_item(
            "abababab-abab-abab-abab-abababababab",
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "Public post",
        )])),
        ..Default::default()
    };
    let public_folder_items = store.public_folder_items.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:UpdateItem ConflictResolution="AlwaysOverwrite">
                  <m:ItemChanges>
                    <t:ItemChange>
                      <t:ItemId Id="public-folder-item:abababab-abab-abab-abab-abababababab"/>
                      <t:Updates>
                        <t:SetItemField>
                          <t:FieldURI FieldURI="item:Subject"/>
                          <t:Message>
                            <t:Subject>Updated public post</t:Subject>
                            <t:Body BodyType="Text">Updated public body</t:Body>
                          </t:Message>
                        </t:SetItemField>
                      </t:Updates>
                    </t:ItemChange>
                  </m:ItemChanges>
                </m:UpdateItem>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:UpdateItemResponse>"));
    assert!(body.contains("<m:ResponseCode>ErrorAccessDenied</m:ResponseCode>"));
    assert!(body.contains("public folder write access is not granted"));
    let item = public_folder_items.lock().unwrap()[0].clone();
    assert_eq!(item.subject, "Public post");
    assert_eq!(item.change_counter, 1);
}

#[tokio::test]
async fn delete_item_hard_deletes_canonical_message() {
    let message_id = Uuid::parse_str("dddddddd-dddd-dddd-dddd-dddddddddddd").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            "dddddddd-dddd-dddd-dddd-dddddddddddd",
            "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee",
            "drafts",
            "Draft from EWS",
        )])),
        ..Default::default()
    };
    let deleted_emails = store.deleted_emails.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:DeleteItem DeleteType="HardDelete">
                  <m:ItemIds><t:ItemId Id="message:dddddddd-dddd-dddd-dddd-dddddddddddd"/></m:ItemIds>
                </m:DeleteItem>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:DeleteItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert_eq!(deleted_emails.lock().unwrap().as_slice(), &[message_id]);
}

#[tokio::test]
async fn delete_item_deletes_canonical_task() {
    let task_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        tasks: Arc::new(Mutex::new(vec![FakeStore::task(
            "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "aaaaaaaa-0000-0000-0000-000000000001",
            "Review task",
        )])),
        ..Default::default()
    };
    let deleted_tasks = store.deleted_tasks.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:DeleteItem><m:ItemIds><t:ItemId Id="task:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"/></m:ItemIds></m:DeleteItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:DeleteItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert_eq!(deleted_tasks.lock().unwrap().as_slice(), &[task_id]);
}

#[tokio::test]
async fn delete_item_deletes_public_folder_item() {
    let folder_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let item_id = Uuid::parse_str("abababab-abab-abab-abab-abababababab").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            folder_id,
            None,
            "Public Root",
        )])),
        public_folder_items: Arc::new(Mutex::new(vec![FakeStore::public_folder_item(
            "abababab-abab-abab-abab-abababababab",
            folder_id,
            "Public post",
        )])),
        ..Default::default()
    };
    let deleted_public_folder_items = store.deleted_public_folder_items.clone();
    let public_folder_items = store.public_folder_items.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:DeleteItem><m:ItemIds><t:ItemId Id="public-folder-item:abababab-abab-abab-abab-abababababab"/></m:ItemIds></m:DeleteItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:DeleteItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert_eq!(
        deleted_public_folder_items.lock().unwrap().as_slice(),
        &[item_id]
    );
    assert_eq!(
        public_folder_items.lock().unwrap()[0].lifecycle_state,
        "deleted"
    );
}

#[tokio::test]
async fn delete_item_rejects_public_folder_item_without_delete_access() {
    let folder_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let non_owner = AuthenticatedAccount {
        tenant_id: FakeStore::account().tenant_id,
        account_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
        email: "delegate@example.test".to_string(),
        display_name: "Public Delegate".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let mut public_folder = FakeStore::public_folder(folder_id, None, "Public Root");
    public_folder.rights.may_delete = false;
    let store = FakeStore {
        session: Some(non_owner),
        public_folders: Arc::new(Mutex::new(vec![public_folder])),
        public_folder_items: Arc::new(Mutex::new(vec![FakeStore::public_folder_item(
            "abababab-abab-abab-abab-abababababab",
            folder_id,
            "Public post",
        )])),
        ..Default::default()
    };
    let deleted_public_folder_items = store.deleted_public_folder_items.clone();
    let public_folder_items = store.public_folder_items.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:DeleteItem><m:ItemIds><t:ItemId Id="public-folder-item:abababab-abab-abab-abab-abababababab"/></m:ItemIds></m:DeleteItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:DeleteItemResponse>"));
    assert!(body.contains("<m:ResponseCode>ErrorAccessDenied</m:ResponseCode>"));
    assert!(body.contains("public folder delete access is not granted"));
    assert!(deleted_public_folder_items.lock().unwrap().is_empty());
    assert_eq!(
        public_folder_items.lock().unwrap()[0].lifecycle_state,
        "active"
    );
}

#[tokio::test]
async fn create_update_task_round_trips_through_sync_folder_items() {
    let task_list_id = Uuid::parse_str("aaaaaaaa-0000-0000-0000-000000000001").unwrap();
    let collection =
        FakeStore::collection("aaaaaaaa-0000-0000-0000-000000000001", "tasks", "Tasks");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        task_collections: Arc::new(Mutex::new(vec![collection])),
        ..Default::default()
    };
    let tasks = store.tasks.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:CreateItem>
                  <m:SavedItemFolderId><t:FolderId Id="aaaaaaaa-0000-0000-0000-000000000001"/></m:SavedItemFolderId>
                  <m:Items>
                    <t:Task>
                      <t:Subject>Review JMAP parity</t:Subject>
                      <t:Body BodyType="Text">Check EWS task coverage</t:Body>
                      <t:Status>InProgress</t:Status>
                      <t:StartDate>2026-05-06T08:00:00Z</t:StartDate>
                      <t:DueDate>2026-05-06T09:00:00Z</t:DueDate>
                      <t:Importance>High</t:Importance>
                    </t:Task>
                  </m:Items>
                </m:CreateItem>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:CreateItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("task:eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"));
    assert_eq!(tasks.lock().unwrap()[0].task_list_id, task_list_id);
    assert_eq!(tasks.lock().unwrap()[0].status, "in-progress");
    assert_eq!(
        tasks.lock().unwrap()[0].starts_at.as_deref(),
        Some("2026-05-06T08:00:00Z")
    );
    assert_eq!(tasks.lock().unwrap()[0].priority, 1);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:ItemShape><t:BaseShape>AllProperties</t:BaseShape></m:ItemShape><m:SyncFolderId><t:FolderId Id="aaaaaaaa-0000-0000-0000-000000000001"/></m:SyncFolderId><m:SyncState>tasks:aaaaaaaa-0000-0000-0000-000000000001:0</m:SyncState></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:Create><t:Task>"));
    assert!(body.contains("<t:Subject>Review JMAP parity</t:Subject>"));
    let old_sync_state = body
        .split("<m:SyncState>")
        .nth(1)
        .and_then(|rest| rest.split("</m:SyncState>").next())
        .unwrap()
        .to_string();

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:UpdateItem ConflictResolution="AlwaysOverwrite">
                  <m:ItemChanges>
                    <t:ItemChange>
                      <t:ItemId Id="task:eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"/>
                      <t:Updates>
                        <t:SetItemField>
                          <t:FieldURI FieldURI="task:Subject"/>
                          <t:Task>
                            <t:Subject>Complete JMAP parity review</t:Subject>
                            <t:Body BodyType="Text">Validated through EWS sync</t:Body>
                            <t:Status>Completed</t:Status>
                            <t:StartDate>2026-05-06T08:15:00Z</t:StartDate>
                            <t:Importance>Normal</t:Importance>
                            <t:CompleteDate>2026-05-06T10:00:00Z</t:CompleteDate>
                          </t:Task>
                        </t:SetItemField>
                      </t:Updates>
                    </t:ItemChange>
                  </m:ItemChanges>
                </m:UpdateItem>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:UpdateItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:Subject>Complete JMAP parity review</t:Subject>"));
    assert!(body.contains("<t:Status>Completed</t:Status>"));
    assert_eq!(
        tasks.lock().unwrap()[0].starts_at.as_deref(),
        Some("2026-05-06T08:15:00Z")
    );
    assert_eq!(tasks.lock().unwrap()[0].priority, 5);

    let request = format!(
        r#"<s:Envelope><s:Body><m:SyncFolderItems><m:ItemShape><t:BaseShape>AllProperties</t:BaseShape></m:ItemShape><m:SyncFolderId><t:FolderId Id="aaaaaaaa-0000-0000-0000-000000000001"/></m:SyncFolderId><m:SyncState>{old_sync_state}</m:SyncState></m:SyncFolderItems></s:Body></s:Envelope>"#
    );
    let response = service
        .handle(&bearer_headers(), request.as_bytes())
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:Update><t:Task>"));
    assert!(body.contains("<t:Subject>Complete JMAP parity review</t:Subject>"));
    assert!(body.contains("<t:StartDate>2026-05-06T08:15:00Z</t:StartDate>"));
    assert!(body.contains("<t:CompleteDate>2026-05-06T10:00:00Z</t:CompleteDate>"));
    assert!(body.contains("<t:Importance>Normal</t:Importance>"));
}

#[tokio::test]
async fn sync_folder_items_pages_task_changes() {
    let task_list_id = "aaaaaaaa-0000-0000-0000-000000000001";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        tasks: Arc::new(Mutex::new(vec![
            FakeStore::task(
                "11111111-1111-1111-1111-111111111111",
                task_list_id,
                "First task",
            ),
            FakeStore::task(
                "22222222-2222-2222-2222-222222222222",
                task_list_id,
                "Second task",
            ),
        ])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:DistinguishedFolderId Id="tasks"/></m:SyncFolderId><m:MaxChangesReturned>1</m:MaxChangesReturned></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let first_page = response_text(response).await;
    assert!(first_page.contains("<m:IncludesLastItemInRange>false</m:IncludesLastItemInRange>"));
    assert_eq!(first_page.matches("<t:Create>").count(), 1);
    let sync_state = test_xml_text(&first_page, "SyncState").unwrap();

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:DistinguishedFolderId Id="tasks"/></m:SyncFolderId><m:SyncState>{sync_state}</m:SyncState><m:MaxChangesReturned>1</m:MaxChangesReturned></m:SyncFolderItems></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let last_page = response_text(response).await;
    assert!(last_page.contains("<m:IncludesLastItemInRange>true</m:IncludesLastItemInRange>"));
    assert_eq!(last_page.matches("<t:Create>").count(), 1);
}

#[tokio::test]
async fn delete_item_rejects_unsupported_item_ids() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:DeleteItem><m:ItemIds><t:ItemId Id="note:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"/></m:ItemIds></m:DeleteItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:DeleteItemResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
}

#[tokio::test]
async fn delete_item_moves_canonical_message_to_trash_by_default() {
    let message_id = Uuid::parse_str("88888888-8888-8888-8888-888888888888").unwrap();
    let trash_id = Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "77777777-7777-7777-7777-777777777777",
            "trash",
            "Deleted",
        )])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            "88888888-8888-8888-8888-888888888888",
            "66666666-6666-6666-6666-666666666666",
            "inbox",
            "Inbox message",
        )])),
        ..Default::default()
    };
    let moved_emails = store.moved_emails.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:DeleteItem><m:ItemIds><t:ItemId Id="message:88888888-8888-8888-8888-888888888888"/></m:ItemIds></m:DeleteItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:DeleteItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert_eq!(
        moved_emails.lock().unwrap().as_slice(),
        &[(message_id, trash_id)]
    );
}

#[tokio::test]
async fn delete_item_rejects_soft_delete_without_mutating_canonical_mail() {
    let message_id = Uuid::parse_str("88888888-8888-8888-8888-888888888888").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "77777777-7777-7777-7777-777777777777",
            "trash",
            "Deleted",
        )])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            "88888888-8888-8888-8888-888888888888",
            "66666666-6666-6666-6666-666666666666",
            "inbox",
            "Inbox message",
        )])),
        ..Default::default()
    };
    let emails = store.emails.clone();
    let moved_emails = store.moved_emails.clone();
    let deleted_emails = store.deleted_emails.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:DeleteItem DeleteType="SoftDelete"><m:ItemIds><t:ItemId Id="message:88888888-8888-8888-8888-888888888888"/></m:ItemIds></m:DeleteItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:DeleteItemResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert!(moved_emails.lock().unwrap().is_empty());
    assert!(deleted_emails.lock().unwrap().is_empty());
    assert!(emails.lock().unwrap().iter().any(|email| email.id == message_id));
}

#[tokio::test]
async fn create_item_saveonly_stores_message_as_canonical_draft() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let saved_drafts = store.saved_drafts.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:CreateItem MessageDisposition="SaveOnly">
                  <m:SavedItemFolderId><t:DistinguishedFolderId Id="drafts"/></m:SavedItemFolderId>
                  <m:Items>
                    <t:Message>
                      <t:Subject>Draft from EWS</t:Subject>
                      <t:Body BodyType="Text">Hello from EWS</t:Body>
                      <t:ToRecipients>
                        <t:Mailbox><t:Name>Bob</t:Name><t:EmailAddress>bob@example.test</t:EmailAddress></t:Mailbox>
                      </t:ToRecipients>
                    </t:Message>
                  </m:Items>
                </m:CreateItem>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:CreateItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("message:dddddddd-dddd-dddd-dddd-dddddddddddd"));
    let create_change_key =
        test_item_change_key(&body, "message:dddddddd-dddd-dddd-dddd-dddddddddddd");
    assert!(create_change_key.starts_with("ck-v1-"));
    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetItem><m:ItemIds><t:ItemId Id="message:dddddddd-dddd-dddd-dddd-dddddddddddd"/></m:ItemIds></m:GetItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let get_body = response_text(response).await;
    assert_eq!(
        test_item_change_key(&get_body, "message:dddddddd-dddd-dddd-dddd-dddddddddddd"),
        create_change_key
    );
    let recorded = saved_drafts.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].source, "ews-createitem");
    assert_eq!(recorded[0].subject, "Draft from EWS");
    assert_eq!(recorded[0].body_text, "Hello from EWS");
    assert_eq!(recorded[0].from_address, "alice@example.test");
    assert_eq!(recorded[0].to[0].address, "bob@example.test");
    assert_eq!(recorded[0].to[0].display_name.as_deref(), Some("Bob"));
}

#[tokio::test]
async fn create_item_rejects_embedded_attachments_before_saving_a_draft() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let saved_drafts = store.saved_drafts.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:CreateItem MessageDisposition="SaveOnly">
                  <m:Items>
                    <t:Message>
                      <t:Subject>Attachment must not be discarded</t:Subject>
                      <t:Attachments>
                        <t:FileAttachment>
                          <t:Name>brief.txt</t:Name>
                          <t:Content>cGF5bG9hZA==</t:Content>
                        </t:FileAttachment>
                      </t:Attachments>
                    </t:Message>
                  </m:Items>
                </m:CreateItem>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:CreateItemResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert!(body.contains("CreateItem does not support embedded attachments"));
    assert!(saved_drafts.lock().unwrap().is_empty());
}

#[tokio::test]
async fn create_item_saveonly_stores_public_folder_post() {
    let public_folder_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            None,
            "Public Root",
        )])),
        ..Default::default()
    };
    let public_folder_items = store.public_folder_items.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:CreateItem MessageDisposition="SaveOnly">
                  <m:SavedItemFolderId><t:FolderId Id="public-folder:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"/></m:SavedItemFolderId>
                  <m:Items>
                    <t:Message>
                      <t:Subject>Public post from EWS</t:Subject>
                      <t:Body BodyType="Text">Visible in public folders</t:Body>
                    </t:Message>
                  </m:Items>
                </m:CreateItem>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:CreateItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("public-folder-item:efefefef-efef-efef-efef-efefefefefef"));
    assert!(body.contains("public-folder:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"));
    let recorded = public_folder_items.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].public_folder_id, public_folder_id);
    assert_eq!(recorded[0].subject, "Public post from EWS");
    assert_eq!(recorded[0].body_text, "Visible in public folders");
    assert_eq!(recorded[0].message_class, "IPM.Post");
}

#[tokio::test]
async fn create_item_rejects_public_folder_post_without_write_access() {
    let non_owner = AuthenticatedAccount {
        tenant_id: FakeStore::account().tenant_id,
        account_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
        email: "delegate@example.test".to_string(),
        display_name: "Public Delegate".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let mut public_folder =
        FakeStore::public_folder("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", None, "Public Root");
    public_folder.rights.may_write = false;
    public_folder.rights.may_delete = false;
    public_folder.rights.may_share = false;
    let store = FakeStore {
        session: Some(non_owner),
        public_folders: Arc::new(Mutex::new(vec![public_folder])),
        ..Default::default()
    };
    let public_folder_items = store.public_folder_items.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:CreateItem MessageDisposition="SaveOnly">
                  <m:SavedItemFolderId><t:FolderId Id="public-folder:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"/></m:SavedItemFolderId>
                  <m:Items>
                    <t:Message>
                      <t:Subject>Public post from EWS</t:Subject>
                      <t:Body BodyType="Text">Visible in public folders</t:Body>
                    </t:Message>
                  </m:Items>
                </m:CreateItem>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:CreateItemResponse>"));
    assert!(body.contains("<m:ResponseCode>ErrorAccessDenied</m:ResponseCode>"));
    assert!(body.contains("public folder write access is not granted"));
    assert!(public_folder_items.lock().unwrap().is_empty());
}

#[tokio::test]
async fn create_item_send_and_save_uses_canonical_submission() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let submitted_messages = store.submitted_messages.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:CreateItem MessageDisposition="SendAndSaveCopy">
                  <m:Items>
                    <t:Message>
                      <t:Subject>Send from EWS</t:Subject>
                      <t:Body BodyType="HTML">&lt;p&gt;Hello&lt;/p&gt;</t:Body>
                      <t:ToRecipients>
                        <t:Mailbox><t:EmailAddress>carol@example.test</t:EmailAddress></t:Mailbox>
                      </t:ToRecipients>
                    </t:Message>
                  </m:Items>
                </m:CreateItem>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:CreateItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("message:ffffffff-ffff-ffff-ffff-ffffffffffff"));
    let recorded = submitted_messages.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].source, "ews-createitem");
    assert_eq!(recorded[0].subject, "Send from EWS");
    assert_eq!(recorded[0].body_text, "Hello");
    assert_eq!(
        recorded[0].body_html_sanitized.as_deref(),
        Some("<p>Hello</p>")
    );
    assert_eq!(recorded[0].to[0].address, "carol@example.test");
}

#[tokio::test]
async fn create_item_send_and_save_rejects_an_unsupported_saved_copy_target_before_submission() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let submitted_messages = store.submitted_messages.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:CreateItem MessageDisposition="SendAndSaveCopy">
                  <m:SavedItemFolderId><t:FolderId Id="mailbox:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"/></m:SavedItemFolderId>
                  <m:Items>
                    <t:Message>
                      <t:Subject>Unsupported saved copy target</t:Subject>
                      <t:ToRecipients><t:Mailbox><t:EmailAddress>carol@example.test</t:EmailAddress></t:Mailbox></t:ToRecipients>
                    </t:Message>
                  </m:Items>
                </m:CreateItem>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:CreateItemResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert!(submitted_messages.lock().unwrap().is_empty());
}

#[tokio::test]
async fn get_item_returns_ews_error_for_unsupported_message_ids() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:GetItem>
                  <m:ItemShape><t:BaseShape>Default</t:BaseShape></m:ItemShape>
                  <m:ItemIds><t:ItemId Id="message:dddddddd-dddd-dddd-dddd-dddddddddddd"/></m:ItemIds>
                </m:GetItem>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:GetItemResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(body.contains("<m:ResponseCode>ErrorItemNotFound</m:ResponseCode>"));
    assert!(body.contains("<t:ServerVersionInfo"));
}

#[tokio::test]
async fn get_sharing_metadata_returns_owned_calendar_metadata_without_exchange_tokens() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "contacts", "Contacts",
        )])),
        calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default",
            "calendar",
            "Calendar",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:GetSharingMetadata>
                  <m:IdOfFolderToShare><t:DistinguishedFolderId Id="calendar"/></m:IdOfFolderToShare>
                </m:GetSharingMetadata>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:GetSharingMetadataResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:DataType>Calendar</t:DataType>"));
    assert!(
        body.contains("<t:FolderId Id=\"default\" ChangeKey=\"ck-default\"/>")
    );
    assert!(body.contains("<t:OwnerSmtpAddress>alice@example.test</t:OwnerSmtpAddress>"));
    assert!(!body.contains("<t:DataType>Contacts</t:DataType>"));
    assert!(!body.to_ascii_lowercase().contains("token"));
}

#[tokio::test]
async fn accept_sharing_invitation_creates_same_tenant_calendar_grant() {
    let alice = FakeStore::account();
    let bob = AuthenticatedAccount {
        tenant_id: alice.tenant_id,
        account_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
        email: "bob@example.test".to_string(),
        display_name: "Bob".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let grants = Arc::new(Mutex::new(Vec::new()));
    let store = FakeStore {
        session: Some(alice.clone()),
        directory_accounts: Arc::new(Mutex::new(vec![bob.clone()])),
        ews_sharing_grants: grants.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:CreateItem>
                  <m:Items>
                    <t:AcceptSharingInvitation>
                      <t:SharingInvitationData>
                        <t:DataType>Calendar</t:DataType>
                        <t:SharedFolderOwner>
                          <t:Mailbox><t:EmailAddress>bob@example.test</t:EmailAddress></t:Mailbox>
                        </t:SharedFolderOwner>
                        <t:PermissionLevel>Editor</t:PermissionLevel>
                      </t:SharingInvitationData>
                    </t:AcceptSharingInvitation>
                  </m:Items>
                </m:CreateItem>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    let change_key = body
        .split("ChangeKey=\"")
        .nth(1)
        .and_then(|value| value.split('\"').next())
        .expect("sharing acceptance must return a ChangeKey");
    assert!(change_key.starts_with("ck-v1-"));
    assert!(body.contains("<m:CreateItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:AcceptSharingInvitation>"));
    assert!(body.contains("<t:DataType>Calendar</t:DataType>"));
    assert!(body.contains("<t:PermissionLevel>Editor</t:PermissionLevel>"));

    let grants = grants.lock().unwrap();
    assert_eq!(grants.len(), 1);
    assert_eq!(grants[0].kind, "calendar");
    assert_eq!(grants[0].owner_account_id, bob.account_id);
    assert_eq!(grants[0].grantee_account_id, alice.account_id);
    assert!(grants[0].rights.may_read);
    assert!(grants[0].rights.may_write);
    assert!(grants[0].rights.may_delete);
    assert!(!grants[0].rights.may_share);
}

#[tokio::test]
async fn accept_sharing_invitation_updates_one_canonical_grant_and_rejects_mixed_items() {
    let alice = FakeStore::account();
    let bob = AuthenticatedAccount {
        tenant_id: alice.tenant_id,
        account_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
        email: "bob@example.test".to_string(),
        display_name: "Bob".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let charlie = AuthenticatedAccount {
        tenant_id: alice.tenant_id,
        account_id: Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap(),
        email: "charlie@example.test".to_string(),
        display_name: "Charlie".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let grants = Arc::new(Mutex::new(Vec::new()));
    let store = FakeStore {
        session: Some(alice),
        directory_accounts: Arc::new(Mutex::new(vec![bob.clone(), charlie])),
        ews_sharing_grants: grants.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let invitation = |permission| {
        format!(
            r#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body><m:CreateItem><m:Items><t:AcceptSharingInvitation>
                <t:SharingInvitationData><t:DataType>Calendar</t:DataType>
                  <t:SharedFolderOwner><t:Mailbox><t:EmailAddress>bob@example.test</t:EmailAddress></t:Mailbox></t:SharedFolderOwner>
                  <t:PermissionLevel>{permission}</t:PermissionLevel>
                </t:SharingInvitationData>
              </t:AcceptSharingInvitation></m:Items></m:CreateItem></s:Body>
            </s:Envelope>
            "#,
        )
    };

    for permission in ["Reviewer", "Editor"] {
        let response = service
            .handle(&bearer_headers(), invitation(permission).as_bytes())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response_text(response)
            .await
            .contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    }
    {
        let grants = grants.lock().unwrap();
        assert_eq!(grants.len(), 1);
        assert!(grants[0].rights.may_write);
    }

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body><m:CreateItem>
                <t:SharedFolderOwner><t:Mailbox><t:EmailAddress>charlie@example.test</t:EmailAddress></t:Mailbox></t:SharedFolderOwner>
                <m:Items><t:AcceptSharingInvitation><t:SharingInvitationData><t:DataType>Calendar</t:DataType><t:SharedFolderOwner><t:Mailbox><t:EmailAddress>bob@example.test</t:EmailAddress></t:Mailbox></t:SharedFolderOwner><t:PermissionLevel>Editor</t:PermissionLevel></t:SharingInvitationData></t:AcceptSharingInvitation></m:Items>
              </m:CreateItem></s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    assert!(response_text(response)
        .await
        .contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    {
        let grants = grants.lock().unwrap();
        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0].owner_account_id, bob.account_id);
    }

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body><m:CreateItem><m:Items>
                <t:AcceptSharingInvitation><t:SharingInvitationData><t:DataType>Calendar</t:DataType><t:SharedFolderOwner><t:Mailbox><t:EmailAddress>bob@example.test</t:EmailAddress></t:Mailbox></t:SharedFolderOwner></t:SharingInvitationData></t:AcceptSharingInvitation>
                <t:Message><t:Subject>must not be created</t:Subject></t:Message>
              </m:Items></m:CreateItem></s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    let grants = grants.lock().unwrap();
    assert_eq!(grants.len(), 1);
    assert!(grants[0].rights.may_write);
}

#[tokio::test]
async fn get_sharing_metadata_rejects_shared_collection_without_leaking_owner_data() {
    let alice = FakeStore::account();
    let mut shared_calendar =
        FakeStore::collection("shared-calendar-bob", "calendar", "Bob Calendar");
    shared_calendar.owner_account_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    shared_calendar.owner_email = "bob@example.test".to_string();
    shared_calendar.owner_display_name = "Bob".to_string();
    shared_calendar.is_owned = false;
    let store = FakeStore {
        session: Some(alice),
        calendar_collections: Arc::new(Mutex::new(vec![shared_calendar])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body><m:GetSharingMetadata>
                <m:IdOfFolderToShare><t:FolderId Id="shared-calendar-bob"/></m:IdOfFolderToShare>
              </m:GetSharingMetadata></s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:GetSharingMetadataResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert!(!body.contains("bob@example.test"));
    assert!(!body.to_ascii_lowercase().contains("token"));
}

#[tokio::test]
async fn accept_sharing_invitation_rejects_cross_tenant_owner_without_grant() {
    let alice = FakeStore::account();
    let bob = AuthenticatedAccount {
        tenant_id: Uuid::from_u128(0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb),
        account_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
        email: "bob@example.test".to_string(),
        display_name: "Bob".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let grants = Arc::new(Mutex::new(Vec::new()));
    let store = FakeStore {
        session: Some(alice),
        directory_accounts: Arc::new(Mutex::new(vec![bob])),
        ews_sharing_grants: grants.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:CreateItem>
                  <m:Items>
                    <t:AcceptSharingInvitation>
                      <t:SharingInvitationData>
                        <t:DataType>Contacts</t:DataType>
                        <t:SharedFolderOwner>
                          <t:Mailbox><t:EmailAddress>bob@example.test</t:EmailAddress></t:Mailbox>
                        </t:SharedFolderOwner>
                      </t:SharingInvitationData>
                    </t:AcceptSharingInvitation>
                  </m:Items>
                </m:CreateItem>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:CreateItemResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert!(body.contains("same tenant"));
    assert!(grants.lock().unwrap().is_empty());
}

#[tokio::test]
async fn get_sharing_folder_returns_accessible_same_tenant_calendar_grant() {
    let alice = FakeStore::account();
    let bob = AuthenticatedAccount {
        tenant_id: alice.tenant_id,
        account_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
        email: "bob@example.test".to_string(),
        display_name: "Bob".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let mut shared_calendar =
        FakeStore::collection("shared-calendar-bob", "calendar", "Bob Calendar");
    shared_calendar.owner_account_id = bob.account_id;
    shared_calendar.owner_email = bob.email.clone();
    shared_calendar.owner_display_name = bob.display_name.clone();
    shared_calendar.is_owned = false;
    shared_calendar.rights = CollaborationRights {
        may_read: true,
        may_write: false,
        may_delete: false,
        may_share: false,
    };
    let store = FakeStore {
        session: Some(alice),
        directory_accounts: Arc::new(Mutex::new(vec![bob])),
        calendar_collections: Arc::new(Mutex::new(vec![shared_calendar])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:GetSharingFolder>
                  <m:SharingFolderRequest>
                    <t:DataType>Calendar</t:DataType>
                    <t:SharedFolderOwner>
                      <t:Mailbox><t:EmailAddress>bob@example.test</t:EmailAddress></t:Mailbox>
                    </t:SharedFolderOwner>
                  </m:SharingFolderRequest>
                </m:GetSharingFolder>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:GetSharingFolderResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body
        .contains("<t:FolderId Id=\"shared-calendar-bob\" ChangeKey=\"ck-shared-calendar-bob\"/>"));
    assert!(body.contains("<t:OwnerSmtpAddress>bob@example.test</t:OwnerSmtpAddress>"));
    assert!(body.contains("<t:PermissionLevel>Reviewer</t:PermissionLevel>"));
}

#[tokio::test]
async fn get_sharing_folder_rejects_shared_folder_id_selector_without_projection() {
    let alice = FakeStore::account();
    let bob = AuthenticatedAccount {
        tenant_id: alice.tenant_id,
        account_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
        email: "bob@example.test".to_string(),
        display_name: "Bob".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let mut shared_calendar =
        FakeStore::collection("shared-calendar-bob", "calendar", "Bob Calendar");
    shared_calendar.owner_account_id = bob.account_id;
    shared_calendar.owner_email = bob.email.clone();
    shared_calendar.owner_display_name = bob.display_name.clone();
    shared_calendar.is_owned = false;
    let service = ExchangeService::new(FakeStore {
        session: Some(alice),
        directory_accounts: Arc::new(Mutex::new(vec![bob])),
        calendar_collections: Arc::new(Mutex::new(vec![shared_calendar])),
        ..Default::default()
    });

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:GetSharingFolder>
                  <m:SharingFolderRequest>
                    <t:DataType>Calendar</t:DataType>
                    <t:SharedFolderId>exchange-only-shared-folder</t:SharedFolderId>
                    <t:SharedFolderOwner><t:Mailbox><t:EmailAddress>bob@example.test</t:EmailAddress></t:Mailbox></t:SharedFolderOwner>
                  </m:SharingFolderRequest>
                </m:GetSharingFolder>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:GetSharingFolderResponse>"));
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert!(!body.contains("shared-calendar-bob"));
    assert!(!body.contains("bob@example.test"));
}

#[tokio::test]
async fn get_sharing_folder_rejects_ungranted_same_tenant_calendar() {
    let alice = FakeStore::account();
    let bob = AuthenticatedAccount {
        tenant_id: alice.tenant_id,
        account_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
        email: "bob@example.test".to_string(),
        display_name: "Bob".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let store = FakeStore {
        session: Some(alice),
        directory_accounts: Arc::new(Mutex::new(vec![bob])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:GetSharingFolder>
                  <m:SharingFolderRequest>
                    <t:DataType>Calendar</t:DataType>
                    <t:SharedFolderOwner>
                      <t:Mailbox><t:EmailAddress>bob@example.test</t:EmailAddress></t:Mailbox>
                    </t:SharedFolderOwner>
                  </m:SharingFolderRequest>
                </m:GetSharingFolder>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:GetSharingFolderResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert!(body.contains("not accessible"));
}

#[tokio::test]
async fn get_sharing_folder_stops_projecting_a_revoked_canonical_grant() {
    let alice = FakeStore::account();
    let bob = AuthenticatedAccount {
        tenant_id: alice.tenant_id,
        account_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
        email: "bob@example.test".to_string(),
        display_name: "Bob".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let mut shared_calendar =
        FakeStore::collection("shared-calendar-bob", "calendar", "Bob Calendar");
    shared_calendar.owner_account_id = bob.account_id;
    shared_calendar.owner_email = bob.email.clone();
    shared_calendar.owner_display_name = bob.display_name.clone();
    shared_calendar.is_owned = false;
    let collections = Arc::new(Mutex::new(vec![shared_calendar]));
    let service = ExchangeService::new(FakeStore {
        session: Some(alice),
        directory_accounts: Arc::new(Mutex::new(vec![bob])),
        calendar_collections: collections.clone(),
        ..Default::default()
    });
    let request = br#"
        <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
          <s:Body><m:GetSharingFolder><m:SharingFolderRequest>
            <t:DataType>Calendar</t:DataType>
            <t:SharedFolderOwner><t:Mailbox><t:EmailAddress>bob@example.test</t:EmailAddress></t:Mailbox></t:SharedFolderOwner>
          </m:SharingFolderRequest></m:GetSharingFolder></s:Body>
        </s:Envelope>
        "#;

    let response = service.handle(&bearer_headers(), request).await.unwrap();
    assert!(response_text(response)
        .await
        .contains("<m:ResponseCode>NoError</m:ResponseCode>"));

    collections.lock().unwrap().clear();
    let response = service.handle(&bearer_headers(), request).await.unwrap();
    let body = response_text(response).await;
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert!(!body.contains("shared-calendar-bob"));
}

#[tokio::test]
async fn refresh_sharing_folder_verifies_accessible_shared_contacts_folder() {
    let alice = FakeStore::account();
    let bob = AuthenticatedAccount {
        tenant_id: alice.tenant_id,
        account_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
        email: "bob@example.test".to_string(),
        display_name: "Bob".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let mut shared_contacts =
        FakeStore::collection("shared-contacts-bob", "contacts", "Bob Contacts");
    shared_contacts.owner_account_id = bob.account_id;
    shared_contacts.owner_email = bob.email;
    shared_contacts.owner_display_name = bob.display_name;
    shared_contacts.is_owned = false;
    shared_contacts.rights = CollaborationRights {
        may_read: true,
        may_write: false,
        may_delete: false,
        may_share: false,
    };
    let store = FakeStore {
        session: Some(alice),
        contact_collections: Arc::new(Mutex::new(vec![shared_contacts])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:RefreshSharingFolder>
                  <m:SharingFolderId><t:FolderId Id="shared-contacts-bob"/></m:SharingFolderId>
                </m:RefreshSharingFolder>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:RefreshSharingFolderResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body
        .contains("<t:FolderId Id=\"shared-contacts-bob\" ChangeKey=\"ck-shared-contacts-bob\"/>"));
}

#[tokio::test]
async fn expand_dl_projects_same_tenant_directory_group_members() {
    let bob = AuthenticatedAccount {
        tenant_id: FakeStore::account().tenant_id,
        account_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
        email: "bob@example.test".to_string(),
        display_name: "Bob".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let carol_id = Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap();
    let foreign_id = Uuid::parse_str("dddddddd-dddd-dddd-dddd-dddddddddddd").unwrap();
    let hidden_id = Uuid::parse_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").unwrap();
    let group_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap();
    let extra_address_book_entries = Arc::new(Mutex::new(vec![
        ExchangeAddressBookEntry {
            id: group_id,
            display_name: "Engineering".to_string(),
            email: "engineering@example.test".to_string(),
            entry_kind: ExchangeAddressBookEntryKind::DistributionList,
            directory_kind: ExchangeAddressBookDirectoryKind::Person,
            member_emails: vec![
                "bob@example.test".to_string(),
                "carol@example.test".to_string(),
                "mallory@example.test".to_string(),
                "hidden@example.test".to_string(),
            ],
            details: ExchangeAddressBookEntryDetails::default(),
        },
        ExchangeAddressBookEntry {
            id: carol_id,
            display_name: "Carol Contact".to_string(),
            email: "carol@example.test".to_string(),
            entry_kind: ExchangeAddressBookEntryKind::Contact,
            directory_kind: ExchangeAddressBookDirectoryKind::Person,
            member_emails: Vec::new(),
            details: ExchangeAddressBookEntryDetails::default(),
        },
        ExchangeAddressBookEntry {
            id: foreign_id,
            display_name: "Mallory".to_string(),
            email: "mallory@example.test".to_string(),
            entry_kind: ExchangeAddressBookEntryKind::Contact,
            directory_kind: ExchangeAddressBookDirectoryKind::Person,
            member_emails: Vec::new(),
            details: ExchangeAddressBookEntryDetails::default(),
        },
        ExchangeAddressBookEntry {
            id: hidden_id,
            display_name: "Hidden".to_string(),
            email: "hidden@example.test".to_string(),
            entry_kind: ExchangeAddressBookEntryKind::Contact,
            directory_kind: ExchangeAddressBookDirectoryKind::Person,
            member_emails: Vec::new(),
            details: ExchangeAddressBookEntryDetails::default(),
        },
    ]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        directory_accounts: Arc::new(Mutex::new(vec![bob])),
        extra_address_book_entries: extra_address_book_entries.clone(),
        extra_address_book_entry_tenants: Arc::new(Mutex::new(HashMap::from([(
            foreign_id,
            Uuid::from_u128(0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb),
        )]))),
        hidden_address_book_entry_ids: Arc::new(Mutex::new(vec![hidden_id])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:ExpandDL>
                  <m:Mailbox><t:EmailAddress>engineering@example.test</t:EmailAddress></m:Mailbox>
                </m:ExpandDL>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:ExpandDLResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(
        body.contains("<m:DLExpansion TotalItemsInView=\"2\" IncludesLastItemInRange=\"true\">")
    );
    assert!(body.contains("<t:EmailAddress>bob@example.test</t:EmailAddress>"));
    assert!(body.contains("<t:EmailAddress>carol@example.test</t:EmailAddress>"));
    assert!(!body.contains("mallory@example.test"));
    assert!(!body.contains("hidden@example.test"));
}

#[tokio::test]
async fn expand_dl_returns_parseable_gap_for_unknown_distribution_list() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:ExpandDL>
                  <m:Mailbox><t:EmailAddress>missing@example.test</t:EmailAddress></m:Mailbox>
                </m:ExpandDL>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:ExpandDLResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(body.contains("<m:ResponseCode>ErrorNameResolutionNoResults</m:ResponseCode>"));
}

#[tokio::test]
async fn get_user_photo_returns_parseable_canonical_photo_gap() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages">
              <s:Body>
                <m:GetUserPhoto>
                  <m:Email>alice@example.test</m:Email>
                  <m:SizeRequested>HR48x48</m:SizeRequested>
                </m:GetUserPhoto>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:GetUserPhotoResponse ResponseClass=\"Error\">"));
    assert!(body.contains("<m:ResponseCode>ErrorItemNotFound</m:ResponseCode>"));
    assert!(body.contains("<m:HasChanged>false</m:HasChanged>"));
}

#[tokio::test]
async fn get_password_expiration_date_returns_parseable_canonical_account_gap() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages">
              <s:Body>
                <m:GetPasswordExpirationDate>
                  <m:MailboxSmtpAddress>alice@example.test</m:MailboxSmtpAddress>
                </m:GetPasswordExpirationDate>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:GetPasswordExpirationDateResponse ResponseClass=\"Error\">"));
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert!(body.contains("no canonical account password expiration date"));
}

#[tokio::test]
async fn mark_as_junk_moves_messages_to_canonical_junk_mailbox() {
    let inbox_id = "44444444-4444-4444-4444-444444444444";
    let junk_id = "55555555-5555-5555-5555-555555555555";
    let message_id = "66666666-6666-6666-6666-666666666666";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(inbox_id, "inbox", "Inbox"),
            FakeStore::mailbox(junk_id, "junk", "Junk Email"),
        ])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            message_id,
            inbox_id,
            "inbox",
            "Suspicious",
        )])),
        ..Default::default()
    };
    let moved_emails = store.moved_emails.clone();
    let emails = store.emails.clone();
    let service = ExchangeService::new(store);
    let request = format!(
        r#"
        <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
          <s:Body>
            <m:MarkAsJunk IsJunk="true" MoveItem="true">
              <m:ItemIds><t:ItemId Id="message:{message_id}"/></m:ItemIds>
            </m:MarkAsJunk>
          </s:Body>
        </s:Envelope>
        "#
    );

    let response = service
        .handle(&bearer_headers(), request.as_bytes())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:MarkAsJunkResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains(&format!("<m:MovedItemId Id=\"message:{message_id}\"")));
    assert_eq!(
        moved_emails.lock().unwrap().as_slice(),
        &[(
            Uuid::parse_str(message_id).unwrap(),
            Uuid::parse_str(junk_id).unwrap()
        )]
    );
    assert_eq!(emails.lock().unwrap()[0].mailbox_role, "junk");
}

#[tokio::test]
async fn mark_as_junk_keeps_exchange_only_block_sender_behavior_parseable() {
    let inbox_id = "44444444-4444-4444-4444-444444444444";
    let junk_id = "55555555-5555-5555-5555-555555555555";
    let message_id = "66666666-6666-6666-6666-666666666666";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(inbox_id, "inbox", "Inbox"),
            FakeStore::mailbox(junk_id, "junk", "Junk Email"),
        ])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            message_id,
            inbox_id,
            "inbox",
            "Suspicious",
        )])),
        ..Default::default()
    };
    let moved_emails = store.moved_emails.clone();
    let service = ExchangeService::new(store);
    let request = format!(
        r#"
        <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
          <s:Body>
            <m:MarkAsJunk IsJunk="true" MoveItem="false">
              <m:ItemIds><t:ItemId Id="message:{message_id}"/></m:ItemIds>
            </m:MarkAsJunk>
          </s:Body>
        </s:Envelope>
        "#
    );

    let response = service
        .handle(&bearer_headers(), request.as_bytes())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:MarkAsJunkResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert!(body.contains("Exchange blocked-sender"));
    assert!(moved_emails.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mark_as_junk_preflights_visibility_before_moving_any_message() {
    let inbox_id = "44444444-4444-4444-4444-444444444444";
    let junk_id = "55555555-5555-5555-5555-555555555555";
    let visible_id = "66666666-6666-6666-6666-666666666666";
    let denied_id = "77777777-7777-7777-7777-777777777777";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(inbox_id, "inbox", "Inbox"),
            FakeStore::mailbox(junk_id, "junk", "Junk Email"),
        ])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(visible_id, inbox_id, "inbox", "Visible"),
            FakeStore::email(denied_id, inbox_id, "inbox", "Not visible"),
        ])),
        inaccessible_jmap_email_ids: Arc::new(Mutex::new(
            vec![Uuid::parse_str(denied_id).unwrap()],
        )),
        ..Default::default()
    };
    let moved_emails = store.moved_emails.clone();
    let service = ExchangeService::new(store);
    let request = format!(
        r#"
        <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
          <s:Body><m:MarkAsJunk IsJunk="true" MoveItem="true"><m:ItemIds>
            <t:ItemId Id="message:{visible_id}"/><t:ItemId Id="message:{denied_id}"/>
          </m:ItemIds></m:MarkAsJunk></s:Body>
        </s:Envelope>
        "#
    );

    let response = service
        .handle(&bearer_headers(), request.as_bytes())
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>ErrorItemNotFound</m:ResponseCode>"));
    assert!(moved_emails.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mark_as_junk_is_idempotent_after_the_message_is_already_in_junk() {
    let inbox_id = "44444444-4444-4444-4444-444444444444";
    let junk_id = "55555555-5555-5555-5555-555555555555";
    let message_id = "66666666-6666-6666-6666-666666666666";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(inbox_id, "inbox", "Inbox"),
            FakeStore::mailbox(junk_id, "junk", "Junk Email"),
        ])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            message_id,
            inbox_id,
            "inbox",
            "Suspicious",
        )])),
        ..Default::default()
    };
    let moved_emails = store.moved_emails.clone();
    let service = ExchangeService::new(store);
    let request = format!(
        r#"
        <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
          <s:Body><m:MarkAsJunk IsJunk="true" MoveItem="true"><m:ItemIds>
            <t:ItemId Id="message:{message_id}"/>
          </m:ItemIds></m:MarkAsJunk></s:Body>
        </s:Envelope>
        "#
    );

    let first = service
        .handle(&bearer_headers(), request.as_bytes())
        .await
        .unwrap();
    let second = service
        .handle(&bearer_headers(), request.as_bytes())
        .await
        .unwrap();

    assert!(response_text(first)
        .await
        .contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(response_text(second)
        .await
        .contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert_eq!(moved_emails.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn delegate_operations_use_canonical_permissions_and_preferences() {
    let delegate = AuthenticatedAccount {
        tenant_id: FakeStore::account().tenant_id,
        account_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
        email: "delegate@example.test".to_string(),
        display_name: "Delegate User".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let store = FakeStore {
        session: Some(FakeStore::account()),
        directory_accounts: Arc::new(Mutex::new(vec![delegate.clone()])),
        ..Default::default()
    };
    let service = ExchangeService::new(store.clone());

    let add_response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:AddDelegate>
                  <m:Mailbox><t:EmailAddress>alice@example.test</t:EmailAddress></m:Mailbox>
                  <m:DelegateUsers>
                    <t:DelegateUser>
                      <t:UserId><t:PrimarySmtpAddress>delegate@example.test</t:PrimarySmtpAddress></t:UserId>
                      <t:DelegatePermissions>
                        <t:CalendarFolderPermissionLevel>Editor</t:CalendarFolderPermissionLevel>
                        <t:InboxFolderPermissionLevel>Reviewer</t:InboxFolderPermissionLevel>
                      </t:DelegatePermissions>
                      <t:ReceiveCopiesOfMeetingMessages>true</t:ReceiveCopiesOfMeetingMessages>
                      <t:ViewPrivateItems>true</t:ViewPrivateItems>
                    </t:DelegateUser>
                  </m:DelegateUsers>
                  <m:DeliverMeetingRequests>DelegatesAndMe</m:DeliverMeetingRequests>
                </m:AddDelegate>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(add_response.status(), StatusCode::OK);
    let add_body = response_text(add_response).await;
    assert!(add_body.contains("<m:AddDelegateResponse>"));
    assert!(add_body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(add_body
        .contains("<t:CalendarFolderPermissionLevel>Editor</t:CalendarFolderPermissionLevel>"));
    assert!(
        add_body.contains("<t:InboxFolderPermissionLevel>Reviewer</t:InboxFolderPermissionLevel>")
    );

    {
        let delegates = store.ews_delegates.lock().unwrap();
        let stored = delegates.first().expect("delegate should be stored");
        assert_eq!(stored.grantee_account_id, delegate.account_id);
        assert!(stored.inbox_rights.may_read);
        assert!(!stored.inbox_rights.may_write);
        assert!(stored.calendar_rights.may_read);
        assert!(stored.calendar_rights.may_write);
        assert!(stored.calendar_rights.may_delete);
        assert!(stored.may_send_on_behalf);
        assert!(!stored.may_send_as);
        assert_eq!(
            stored.preferences.meeting_request_delivery,
            "delegate_and_owner"
        );
        assert!(stored.preferences.receives_meeting_request_copy);
        assert!(stored.preferences.may_view_private_items);
    }

    let get_response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:GetDelegate>
                  <m:Mailbox><t:EmailAddress>alice@example.test</t:EmailAddress></m:Mailbox>
                  <m:UserIds><t:UserId><t:PrimarySmtpAddress>delegate@example.test</t:PrimarySmtpAddress></t:UserId></m:UserIds>
                </m:GetDelegate>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let get_body = response_text(get_response).await;
    assert!(get_body.contains("<m:GetDelegateResponse>"));
    assert!(get_body.contains("<t:PrimarySmtpAddress>delegate@example.test</t:PrimarySmtpAddress>"));
    assert!(get_body.contains("<t:ViewPrivateItems>true</t:ViewPrivateItems>"));

    let update_response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:UpdateDelegate>
                  <m:DelegateUsers>
                    <t:DelegateUser>
                      <t:UserId><t:PrimarySmtpAddress>delegate@example.test</t:PrimarySmtpAddress></t:UserId>
                      <t:DelegatePermissions>
                        <t:CalendarFolderPermissionLevel>Reviewer</t:CalendarFolderPermissionLevel>
                        <t:InboxFolderPermissionLevel>Editor</t:InboxFolderPermissionLevel>
                      </t:DelegatePermissions>
                      <t:ReceiveCopiesOfMeetingMessages>false</t:ReceiveCopiesOfMeetingMessages>
                      <t:ViewPrivateItems>false</t:ViewPrivateItems>
                    </t:DelegateUser>
                  </m:DelegateUsers>
                  <m:DeliverMeetingRequests>NoForward</m:DeliverMeetingRequests>
                </m:UpdateDelegate>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let update_body = response_text(update_response).await;
    assert!(update_body.contains("<m:UpdateDelegateResponse>"));
    assert!(update_body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    {
        let delegates = store.ews_delegates.lock().unwrap();
        let stored = delegates.first().expect("delegate should remain stored");
        assert!(stored.inbox_rights.may_write);
        assert!(!stored.calendar_rights.may_write);
        assert_eq!(stored.preferences.meeting_request_delivery, "owner_only");
        assert!(!stored.preferences.receives_meeting_request_copy);
        assert!(!stored.preferences.may_view_private_items);
    }

    let remove_response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:RemoveDelegate>
                  <m:UserIds><t:UserId><t:PrimarySmtpAddress>delegate@example.test</t:PrimarySmtpAddress></t:UserId></m:UserIds>
                </m:RemoveDelegate>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let remove_body = response_text(remove_response).await;
    assert!(remove_body.contains("<m:RemoveDelegateResponse>"));
    assert!(remove_body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(store.ews_delegates.lock().unwrap().is_empty());
}

#[tokio::test]
async fn delegate_add_rejects_cross_tenant_delegate() {
    let other_tenant_delegate = AuthenticatedAccount {
        tenant_id: Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap(),
        account_id: Uuid::parse_str("dddddddd-dddd-dddd-dddd-dddddddddddd").unwrap(),
        email: "delegate@example.test".to_string(),
        display_name: "Other Tenant Delegate".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let store = FakeStore {
        session: Some(FakeStore::account()),
        directory_accounts: Arc::new(Mutex::new(vec![other_tenant_delegate])),
        ..Default::default()
    };
    let service = ExchangeService::new(store.clone());

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:AddDelegate>
                  <m:DelegateUsers>
                    <t:DelegateUser>
                      <t:UserId><t:PrimarySmtpAddress>delegate@example.test</t:PrimarySmtpAddress></t:UserId>
                      <t:DelegatePermissions><t:CalendarFolderPermissionLevel>Reviewer</t:CalendarFolderPermissionLevel></t:DelegatePermissions>
                    </t:DelegateUser>
                  </m:DelegateUsers>
                </m:AddDelegate>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:AddDelegateResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert!(store.ews_delegates.lock().unwrap().is_empty());
}

#[tokio::test]
async fn delegate_operations_reject_a_mailbox_other_than_the_authenticated_owner() {
    let delegate = AuthenticatedAccount {
        tenant_id: FakeStore::account().tenant_id,
        account_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
        email: "delegate@example.test".to_string(),
        display_name: "Delegate User".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let store = FakeStore {
        session: Some(FakeStore::account()),
        directory_accounts: Arc::new(Mutex::new(vec![delegate])),
        ..Default::default()
    };
    let service = ExchangeService::new(store.clone());

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:AddDelegate>
                  <m:Mailbox><t:EmailAddress>other@example.test</t:EmailAddress></m:Mailbox>
                  <m:DelegateUsers>
                    <t:DelegateUser>
                      <t:UserId><t:PrimarySmtpAddress>delegate@example.test</t:PrimarySmtpAddress></t:UserId>
                      <t:DelegatePermissions><t:InboxFolderPermissionLevel>Reviewer</t:InboxFolderPermissionLevel></t:DelegatePermissions>
                    </t:DelegateUser>
                  </m:DelegateUsers>
                </m:AddDelegate>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:AddDelegateResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert!(body.contains("owner-bound to the authenticated mailbox"));
    assert!(store.ews_delegates.lock().unwrap().is_empty());
}

#[tokio::test]
async fn delegate_batches_reject_invalid_members_without_partial_mutation() {
    let delegate = AuthenticatedAccount {
        tenant_id: FakeStore::account().tenant_id,
        account_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
        email: "delegate@example.test".to_string(),
        display_name: "Delegate User".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let store = FakeStore {
        session: Some(FakeStore::account()),
        directory_accounts: Arc::new(Mutex::new(vec![delegate])),
        ..Default::default()
    };
    let service = ExchangeService::new(store.clone());

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body><m:AddDelegate><m:DelegateUsers>
                <t:DelegateUser><t:UserId><t:PrimarySmtpAddress>delegate@example.test</t:PrimarySmtpAddress></t:UserId><t:DelegatePermissions><t:InboxFolderPermissionLevel>Reviewer</t:InboxFolderPermissionLevel></t:DelegatePermissions></t:DelegateUser>
                <t:DelegateUser><t:UserId><t:PrimarySmtpAddress>missing@example.test</t:PrimarySmtpAddress></t:UserId><t:DelegatePermissions><t:CalendarFolderPermissionLevel>Reviewer</t:CalendarFolderPermissionLevel></t:DelegatePermissions></t:DelegateUser>
              </m:DelegateUsers></m:AddDelegate></s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:AddDelegateResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(store.ews_delegates.lock().unwrap().is_empty());
}

#[tokio::test]
async fn delegate_remove_validates_the_full_batch_before_mutation() {
    let delegate = AuthenticatedAccount {
        tenant_id: FakeStore::account().tenant_id,
        account_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
        email: "delegate@example.test".to_string(),
        display_name: "Delegate User".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let store = FakeStore {
        session: Some(FakeStore::account()),
        directory_accounts: Arc::new(Mutex::new(vec![delegate])),
        ..Default::default()
    };
    let service = ExchangeService::new(store.clone());
    let add = service.handle(&bearer_headers(), br#"
        <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types"><s:Body><m:AddDelegate><m:DelegateUsers><t:DelegateUser><t:UserId><t:PrimarySmtpAddress>delegate@example.test</t:PrimarySmtpAddress></t:UserId><t:DelegatePermissions><t:InboxFolderPermissionLevel>Reviewer</t:InboxFolderPermissionLevel></t:DelegatePermissions></t:DelegateUser></m:DelegateUsers></m:AddDelegate></s:Body></s:Envelope>
    "#).await.unwrap();
    assert!(response_text(add)
        .await
        .contains("<m:ResponseCode>NoError</m:ResponseCode>"));

    let response = service.handle(&bearer_headers(), br#"
        <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types"><s:Body><m:RemoveDelegate><m:UserIds><t:UserId><t:PrimarySmtpAddress>delegate@example.test</t:PrimarySmtpAddress></t:UserId><t:UserId><t:PrimarySmtpAddress>missing@example.test</t:PrimarySmtpAddress></t:UserId></m:UserIds></m:RemoveDelegate></s:Body></s:Envelope>
    "#).await.unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>ErrorItemNotFound</m:ResponseCode>"));
    assert_eq!(store.ews_delegates.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn delegate_add_rejects_unsupported_exchange_only_permission_shapes() {
    let delegate = AuthenticatedAccount {
        tenant_id: FakeStore::account().tenant_id,
        account_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
        email: "delegate@example.test".to_string(),
        display_name: "Delegate User".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let store = FakeStore {
        session: Some(FakeStore::account()),
        directory_accounts: Arc::new(Mutex::new(vec![delegate])),
        ..Default::default()
    };
    let service = ExchangeService::new(store.clone());

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:AddDelegate>
                  <m:DelegateUsers>
                    <t:DelegateUser>
                      <t:UserId><t:PrimarySmtpAddress>delegate@example.test</t:PrimarySmtpAddress></t:UserId>
                      <t:DelegatePermissions>
                        <t:CalendarFolderPermissionLevel>Custom</t:CalendarFolderPermissionLevel>
                        <t:ContactsFolderPermissionLevel>Editor</t:ContactsFolderPermissionLevel>
                      </t:DelegatePermissions>
                    </t:DelegateUser>
                  </m:DelegateUsers>
                </m:AddDelegate>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:AddDelegateResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert!(body.contains("ContactsFolderPermissionLevel") || body.contains("Custom"));
    assert!(store.ews_delegates.lock().unwrap().is_empty());
}

#[tokio::test]
async fn user_configuration_create_get_update_and_delete_use_canonical_storage() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store.clone());

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:CreateUserConfiguration>
                  <m:UserConfiguration>
                    <t:UserConfigurationName Name="OWA.UserOptions"/>
                    <t:Dictionary>
                      <t:DictionaryEntry>
                        <t:DictionaryKey><t:Type>String</t:Type><t:Value>previewPane</t:Value></t:DictionaryKey>
                        <t:DictionaryValue><t:Type>String</t:Type><t:Value>right</t:Value></t:DictionaryValue>
                      </t:DictionaryEntry>
                      <t:DictionaryEntry>
                        <t:DictionaryKey><t:Type>String</t:Type><t:Value>theme</t:Value></t:DictionaryKey>
                        <t:DictionaryValue><t:Type>String</t:Type><t:Value>contrast</t:Value></t:DictionaryValue>
                      </t:DictionaryEntry>
                    </t:Dictionary>
                    <t:XmlData>PG9wdGlvbnMgdmVyc2lvbj0iMSIvPg==</t:XmlData>
                    <t:BinaryData>cHJvZmlsZS1jYWNoZQ==</t:BinaryData>
                  </m:UserConfiguration>
                </m:CreateUserConfiguration>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:CreateUserConfigurationResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    {
        let configurations = store.ews_user_configurations.lock().unwrap();
        assert_eq!(configurations.len(), 1);
        let configuration = &configurations[0];
        assert_eq!(configuration.scope_kind, "account");
        assert_eq!(configuration.config_name, "OWA.UserOptions");
        assert_eq!(configuration.config_class, "ews_user_configuration");
        assert_eq!(
            configuration.dictionary_json["previewPane"].as_str(),
            Some("right")
        );
        assert_eq!(
            configuration.xml_payload.as_deref(),
            Some("<options version=\"1\"/>")
        );
        assert_eq!(
            configuration.binary_payload.as_deref(),
            Some(b"profile-cache".as_slice())
        );
    }
    assert_eq!(
        store
            .ews_user_configuration_audits
            .lock()
            .unwrap()
            .iter()
            .map(|entry| entry.action.as_str())
            .collect::<Vec<_>>(),
        vec!["ews-create-user-configuration"]
    );

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:GetUserConfiguration>
                  <m:UserConfigurationName Name="OWA.UserOptions"/>
                  <m:UserConfigurationProperties>All</m:UserConfigurationProperties>
                </m:GetUserConfiguration>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:GetUserConfigurationResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:UserConfigurationName Name=\"OWA.UserOptions\"/>"));
    assert!(body.contains("<t:Value>previewPane</t:Value>"));
    assert!(body.contains("<t:Value>right</t:Value>"));
    assert!(body.contains("<t:XmlData>PG9wdGlvbnMgdmVyc2lvbj0iMSIvPg==</t:XmlData>"));
    assert!(body.contains("<t:BinaryData>cHJvZmlsZS1jYWNoZQ==</t:BinaryData>"));

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:UpdateUserConfiguration>
                  <m:UserConfiguration>
                    <t:UserConfigurationName Name="OWA.UserOptions"/>
                    <t:Dictionary>
                      <t:DictionaryEntry>
                        <t:DictionaryKey><t:Type>String</t:Type><t:Value>previewPane</t:Value></t:DictionaryKey>
                        <t:DictionaryValue><t:Type>String</t:Type><t:Value>bottom</t:Value></t:DictionaryValue>
                      </t:DictionaryEntry>
                    </t:Dictionary>
                    <t:XmlData>PG9wdGlvbnMgdmVyc2lvbj0iMiIvPg==</t:XmlData>
                  </m:UserConfiguration>
                </m:UpdateUserConfiguration>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:UpdateUserConfigurationResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    {
        let configurations = store.ews_user_configurations.lock().unwrap();
        assert_eq!(configurations.len(), 1);
        let configuration = &configurations[0];
        assert_eq!(configuration.modseq, 2);
        assert_eq!(
            configuration.dictionary_json["previewPane"].as_str(),
            Some("bottom")
        );
        assert_eq!(configuration.binary_payload, None);
    }
    assert_eq!(
        store
            .ews_user_configuration_audits
            .lock()
            .unwrap()
            .iter()
            .map(|entry| entry.action.as_str())
            .collect::<Vec<_>>(),
        vec![
            "ews-create-user-configuration",
            "ews-update-user-configuration"
        ]
    );

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:GetUserConfiguration>
                  <m:UserConfigurationName Name="OWA.UserOptions"/>
                  <m:UserConfigurationProperties>Dictionary</m:UserConfigurationProperties>
                </m:GetUserConfiguration>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:Value>bottom</t:Value>"));
    assert!(!body.contains("<t:XmlData>"));
    assert!(!body.contains("<t:BinaryData>"));

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:DeleteUserConfiguration>
                  <m:UserConfigurationName Name="OWA.UserOptions"/>
                </m:DeleteUserConfiguration>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:DeleteUserConfigurationResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(store.ews_user_configurations.lock().unwrap().is_empty());
    assert_eq!(
        store
            .ews_user_configuration_audits
            .lock()
            .unwrap()
            .iter()
            .map(|entry| entry.action.as_str())
            .collect::<Vec<_>>(),
        vec![
            "ews-create-user-configuration",
            "ews-update-user-configuration",
            "ews-delete-user-configuration"
        ]
    );
}

#[tokio::test]
async fn user_configuration_supports_mailbox_scoped_names_and_not_found_errors() {
    let mailbox_id = "44444444-4444-4444-4444-444444444444";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            mailbox_id, "inbox", "Inbox",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store.clone());

    let create = format!(
        r#"
        <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
          <s:Body>
            <m:CreateUserConfiguration>
              <m:UserConfiguration>
                <t:UserConfigurationName Name="IPM.Configuration.Calendar">
                  <t:FolderId Id="mailbox:{mailbox_id}"/>
                </t:UserConfigurationName>
                <t:Dictionary>
                  <t:DictionaryEntry>
                    <t:DictionaryKey><t:Type>String</t:Type><t:Value>view</t:Value></t:DictionaryKey>
                    <t:DictionaryValue><t:Type>String</t:Type><t:Value>work-week</t:Value></t:DictionaryValue>
                  </t:DictionaryEntry>
                </t:Dictionary>
              </m:UserConfiguration>
            </m:CreateUserConfiguration>
          </s:Body>
        </s:Envelope>
        "#
    );
    let response = service
        .handle(&bearer_headers(), create.as_bytes())
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:CreateUserConfigurationResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    {
        let configurations = store.ews_user_configurations.lock().unwrap();
        assert_eq!(configurations.len(), 1);
        assert_eq!(configurations[0].scope_kind, "mailbox");
        assert_eq!(
            configurations[0].mailbox_id,
            Some(Uuid::parse_str(mailbox_id).unwrap())
        );
    }

    let missing = r#"
        <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
          <s:Body>
            <m:GetUserConfiguration>
              <m:UserConfigurationName Name="Missing.Configuration"/>
              <m:UserConfigurationProperties>All</m:UserConfigurationProperties>
            </m:GetUserConfiguration>
          </s:Body>
        </s:Envelope>
    "#;
    let response = service
        .handle(&bearer_headers(), missing.as_bytes())
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:GetUserConfigurationResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(body.contains("<m:ResponseCode>ErrorItemNotFound</m:ResponseCode>"));
}

#[tokio::test]
async fn user_configuration_enforces_scope_name_payload_and_mutation_boundaries() {
    use base64::Engine as _;

    let mailbox_id = "44444444-4444-4444-4444-444444444445";
    let public_folder_id = "44444444-4444-4444-4444-444444444446";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            mailbox_id, "inbox", "Inbox",
        )])),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            public_folder_id,
            None,
            "Public",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store.clone());
    let create = |scope: &str, value: &str| {
        format!(
            r#"<s:Envelope><s:Body><m:CreateUserConfiguration><m:UserConfiguration><t:UserConfigurationName Name="Shared.Configuration">{scope}</t:UserConfigurationName><t:Dictionary><t:DictionaryEntry><t:DictionaryKey><t:Type>String</t:Type><t:Value>scope</t:Value></t:DictionaryKey><t:DictionaryValue><t:Type>String</t:Type><t:Value>{value}</t:Value></t:DictionaryValue></t:DictionaryEntry></t:Dictionary></m:UserConfiguration></m:CreateUserConfiguration></s:Body></s:Envelope>"#
        )
    };

    let mailbox_scope = format!("<t:FolderId Id=\"mailbox:{mailbox_id}\"/>");
    let public_folder_scope = format!("<t:FolderId Id=\"public-folder:{public_folder_id}\"/>");
    for (scope, value) in [
        ("", "account"),
        (mailbox_scope.as_str(), "mailbox"),
        (public_folder_scope.as_str(), "public"),
    ] {
        let response = service
            .handle(&bearer_headers(), create(scope, value).as_bytes())
            .await
            .unwrap();
        assert!(response_text(response)
            .await
            .contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    }
    assert_eq!(store.ews_user_configurations.lock().unwrap().len(), 3);

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:GetUserConfiguration><m:UserConfigurationName Name="Shared.Configuration"><t:FolderId Id="mailbox:{mailbox_id}"/></m:UserConfigurationName><m:UserConfigurationProperties>Dictionary</m:UserConfigurationProperties></m:GetUserConfiguration></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains(&format!("<t:FolderId Id=\"mailbox:{mailbox_id}\"/>")));
    assert!(body.contains("<t:Value>mailbox</t:Value>"));
    assert!(!body.contains("<t:ItemId "));

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:GetUserConfiguration><m:UserConfigurationName Name="Shared.Configuration"><t:FolderId Id="public-folder:{public_folder_id}"/></m:UserConfigurationName><m:UserConfigurationProperties>Id</m:UserConfigurationProperties></m:GetUserConfiguration></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains(&format!(
        "<t:FolderId Id=\"public-folder:{public_folder_id}\"/>"
    )));
    assert!(body.contains("<t:ItemId Id=\"user-configuration:"));
    assert!(!body.contains("<t:Dictionary>"));

    let response = service
        .handle(&bearer_headers(), create("", "duplicate").as_bytes())
        .await
        .unwrap();
    assert!(response_text(response)
        .await
        .contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert_eq!(store.ews_user_configurations.lock().unwrap().len(), 3);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:UpdateUserConfiguration><m:UserConfiguration><t:UserConfigurationName Name="Missing.Configuration"/></m:UserConfiguration></m:UpdateUserConfiguration></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    assert!(response_text(response)
        .await
        .contains("<m:ResponseCode>ErrorItemNotFound</m:ResponseCode>"));

    let oversized = base64::engine::general_purpose::STANDARD.encode(vec![b'x'; 65_537]);
    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:CreateUserConfiguration><m:UserConfiguration><t:UserConfigurationName Name="TooLarge"/><t:BinaryData>{oversized}</t:BinaryData></m:UserConfiguration></m:CreateUserConfiguration></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    assert!(response_text(response)
        .await
        .contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert_eq!(store.ews_user_configurations.lock().unwrap().len(), 3);

    for element in ["XmlData", "BinaryData"] {
        let response = service
            .handle(
                &bearer_headers(),
                format!(
                    r#"<s:Envelope><s:Body><m:CreateUserConfiguration><m:UserConfiguration><t:UserConfigurationName Name="Invalid{element}"/><t:{element}>not-base64!</t:{element}></m:UserConfiguration></m:CreateUserConfiguration></s:Body></s:Envelope>"#
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        assert!(response_text(response)
            .await
            .contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    }
    assert_eq!(store.ews_user_configurations.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn user_configuration_rejects_unreadable_public_scope_and_late_invalid_update() {
    let public_folder_id = "44444444-4444-4444-4444-444444444446";
    let non_owner = AuthenticatedAccount {
        tenant_id: FakeStore::account().tenant_id,
        account_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
        email: "delegate@example.test".to_string(),
        display_name: "Public Delegate".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let mut public_folder = FakeStore::public_folder(public_folder_id, None, "Public Root");
    public_folder.rights = PublicFolderRights {
        may_read: false,
        may_write: false,
        may_delete: false,
        may_share: false,
    };
    let store = FakeStore {
        session: Some(non_owner),
        public_folders: Arc::new(Mutex::new(vec![public_folder])),
        ..Default::default()
    };
    let service = ExchangeService::new(store.clone());

    let denied = format!(
        r#"<s:Envelope><s:Body><m:CreateUserConfiguration><m:UserConfiguration><t:UserConfigurationName Name="Public.Configuration"><t:FolderId Id="public-folder:{public_folder_id}"/></t:UserConfigurationName></m:UserConfiguration></m:CreateUserConfiguration></s:Body></s:Envelope>"#
    );
    let response = service
        .handle(&bearer_headers(), denied.as_bytes())
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:CreateUserConfigurationResponse>"));
    assert!(body.contains("<m:ResponseCode>ErrorAccessDenied</m:ResponseCode>"));
    assert!(store.ews_user_configurations.lock().unwrap().is_empty());
    assert!(store
        .ews_user_configuration_audits
        .lock()
        .unwrap()
        .is_empty());

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:CreateUserConfiguration><m:UserConfiguration><t:UserConfigurationName Name="Safe.Configuration"/><t:Dictionary><t:DictionaryEntry><t:DictionaryKey><t:Type>String</t:Type><t:Value>view</t:Value></t:DictionaryKey><t:DictionaryValue><t:Type>String</t:Type><t:Value>initial</t:Value></t:DictionaryValue></t:DictionaryEntry></t:Dictionary></m:UserConfiguration></m:CreateUserConfiguration></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    assert!(response_text(response)
        .await
        .contains("<m:ResponseCode>NoError</m:ResponseCode>"));

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:UpdateUserConfiguration><m:UserConfiguration><t:UserConfigurationName Name="Safe.Configuration"/><t:Dictionary><t:DictionaryEntry><t:DictionaryKey><t:Type>String</t:Type><t:Value>view</t:Value></t:DictionaryKey><t:DictionaryValue><t:Type>String</t:Type><t:Value>changed</t:Value></t:DictionaryValue></t:DictionaryEntry></t:Dictionary><t:BinaryData>not-base64!</t:BinaryData></m:UserConfiguration></m:UpdateUserConfiguration></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    assert!(response_text(response)
        .await
        .contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    let configurations = store.ews_user_configurations.lock().unwrap();
    assert_eq!(configurations.len(), 1);
    assert_eq!(
        configurations[0].dictionary_json["view"].as_str(),
        Some("initial")
    );
    assert_eq!(configurations[0].modseq, 1);
    drop(configurations);
    assert_eq!(store.ews_user_configuration_audits.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn pull_subscription_get_events_and_unsubscribe_return_status_flow() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "44444444-4444-4444-4444-444444444444",
            "inbox",
            "Inbox",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:Subscribe>
                  <m:PullSubscriptionRequest>
                    <t:SubscribeToAllFolders>true</t:SubscribeToAllFolders>
                    <t:EventTypes><t:EventType>NewMailEvent</t:EventType><t:EventType>DeletedEvent</t:EventType></t:EventTypes>
                    <t:Timeout>10</t:Timeout>
                  </m:PullSubscriptionRequest>
                </m:Subscribe>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:SubscribeResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    let subscription_id = body
        .split("<m:SubscriptionId>")
        .nth(1)
        .and_then(|rest| rest.split("</m:SubscriptionId>").next())
        .unwrap()
        .to_string();
    let watermark = body
        .split("<m:Watermark>")
        .nth(1)
        .and_then(|rest| rest.split("</m:Watermark>").next())
        .unwrap()
        .to_string();

    let request = format!(
        r#"<s:Envelope><s:Body><m:GetEvents><m:SubscriptionId>{subscription_id}</m:SubscriptionId><m:Watermark>{watermark}</m:Watermark></m:GetEvents></s:Body></s:Envelope>"#
    );
    let response = service
        .handle(&bearer_headers(), request.as_bytes())
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:GetEventsResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<m:Notification>"));
    assert!(body.contains(&format!(
        "<t:SubscriptionId>{subscription_id}</t:SubscriptionId>"
    )));
    assert!(body.contains(&format!(
        "<t:PreviousWatermark>{watermark}</t:PreviousWatermark>"
    )));
    assert!(body.contains("<t:MoreEvents>false</t:MoreEvents>"));
    assert!(body.contains("<t:StatusEvent>"));
    assert!(!body.contains("<t:StatusEvent><t:Watermark>") || !body.contains("<t:TimeStamp>"));

    let request = format!(
        r#"<s:Envelope><s:Body><m:Unsubscribe><m:SubscriptionId>{subscription_id}</m:SubscriptionId></m:Unsubscribe></s:Body></s:Envelope>"#
    );
    let response = service
        .handle(&bearer_headers(), request.as_bytes())
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:UnsubscribeResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
}

#[tokio::test]
async fn pull_subscription_get_events_does_not_synthesize_mailbox_events() {
    let mailbox_id = "12121212-1212-1212-1212-121212121212";
    let message_id = "14141414-1414-1414-1414-141414141414";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            mailbox_id, "inbox", "Inbox",
        )])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            message_id,
            mailbox_id,
            "inbox",
            "RCA Notification",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:Subscribe>
                  <m:PullSubscriptionRequest>
                    <t:FolderIds><t:DistinguishedFolderId Id="inbox"/></t:FolderIds>
                    <t:EventTypes><t:EventType>CreatedEvent</t:EventType></t:EventTypes>
                    <t:Timeout>10</t:Timeout>
                  </m:PullSubscriptionRequest>
                </m:Subscribe>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    let subscription_id = body
        .split("<m:SubscriptionId>")
        .nth(1)
        .and_then(|rest| rest.split("</m:SubscriptionId>").next())
        .unwrap()
        .to_string();
    let watermark = body
        .split("<m:Watermark>")
        .nth(1)
        .and_then(|rest| rest.split("</m:Watermark>").next())
        .unwrap()
        .to_string();

    let request = format!(
        r#"<s:Envelope><s:Body><m:GetEvents><m:SubscriptionId>{subscription_id}</m:SubscriptionId><m:Watermark>{watermark}</m:Watermark></m:GetEvents></s:Body></s:Envelope>"#
    );
    let response = service
        .handle(&bearer_headers(), request.as_bytes())
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:GetEventsResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:StatusEvent>"));
    assert!(!body.contains("<t:CreatedEvent>"));
    assert!(!body.contains("<t:NewMailEvent>"));
    assert!(!body.contains(&format!("<t:ItemId Id=\"message:{message_id}\"")));
}

#[tokio::test]
async fn pull_subscription_get_events_replays_canonical_changes_after_restart() {
    let mailbox_id = "55555555-5555-5555-5555-555555555555";
    let message_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap();
    let mailbox_uuid = Uuid::parse_str(mailbox_id).unwrap();
    let mapi_notification_cursor = Arc::new(Mutex::new(Some(7)));
    let mapi_notification_polls = Arc::new(Mutex::new(vec![MapiNotificationPoll {
        event_pending: true,
        cursor: Some(8),
        events: vec![MapiNotificationEvent::canonical(
            MapiNotificationKind::Content,
            1,
            1,
            Some(2),
            None,
            8,
            9,
            None,
            None,
            "created".to_string(),
            Some("Inbox".to_string()),
            None,
            Some("RCA pull create".to_string()),
            None,
        )
        .with_canonical_ids(Some(mailbox_uuid), Some(message_id))],
    }]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            mailbox_id, "inbox", "Inbox",
        )])),
        mapi_notification_cursor: mapi_notification_cursor.clone(),
        mapi_notification_polls: mapi_notification_polls.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store.clone());

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:Subscribe>
                  <m:PullSubscriptionRequest>
                    <t:FolderIds><t:FolderId Id="mailbox:55555555-5555-5555-5555-555555555555"/></t:FolderIds>
                    <t:EventTypes><t:EventType>CreatedEvent</t:EventType></t:EventTypes>
                    <t:Timeout>10</t:Timeout>
                  </m:PullSubscriptionRequest>
                </m:Subscribe>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    let subscription_id = test_xml_text(&body, "SubscriptionId").unwrap();
    let watermark = test_xml_text(&body, "Watermark").unwrap();
    // [MS-OXWSNTIF] sections 2.2.5.1-.2 and 3.1.4.1.3.2: clients replay
    // the opaque bookmark verbatim after reconnect/restart.
    assert!(!watermark.is_empty());

    let restarted_service = ExchangeService::new(store);
    let request = format!(
        r#"<s:Envelope><s:Body><m:GetEvents><m:SubscriptionId>{subscription_id}</m:SubscriptionId><m:Watermark>{watermark}</m:Watermark></m:GetEvents></s:Body></s:Envelope>"#
    );
    let response = restarted_service
        .handle(&bearer_headers(), request.as_bytes())
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:CreatedEvent>"));
    assert!(body.contains(&format!("<t:ItemId Id=\"message:{message_id}\"")));
    assert!(body.contains(&format!("<t:ParentFolderId Id=\"mailbox:{mailbox_id}\"")));
    assert!(!body.contains("<t:StatusEvent>"));
    assert!(mapi_notification_polls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn pull_subscription_get_events_replays_canonical_delete() {
    let mailbox_id = "66666666-6666-6666-6666-666666666666";
    let message_id = Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap();
    let mailbox_uuid = Uuid::parse_str(mailbox_id).unwrap();
    let mapi_notification_polls = Arc::new(Mutex::new(vec![MapiNotificationPoll {
        event_pending: true,
        cursor: Some(4),
        events: vec![MapiNotificationEvent::canonical(
            MapiNotificationKind::Content,
            1,
            1,
            Some(2),
            None,
            4,
            5,
            None,
            None,
            // [MS-OXWSNTIF] sections 2.2.4.5 and 2.2.4.8: canonical
            // destruction projects as a DeletedEvent.
            "destroyed".to_string(),
            Some("Inbox".to_string()),
            None,
            Some("RCA pull delete".to_string()),
            None,
        )
        .with_canonical_ids(Some(mailbox_uuid), Some(message_id))],
    }]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            mailbox_id, "inbox", "Inbox",
        )])),
        mapi_notification_cursor: Arc::new(Mutex::new(Some(3))),
        mapi_notification_polls,
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:Subscribe>
                  <m:PullSubscriptionRequest>
                    <t:FolderIds><t:FolderId Id="mailbox:66666666-6666-6666-6666-666666666666"/></t:FolderIds>
                    <t:EventTypes><t:EventType>DeletedEvent</t:EventType></t:EventTypes>
                    <t:Timeout>10</t:Timeout>
                  </m:PullSubscriptionRequest>
                </m:Subscribe>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    let subscription_id = test_xml_text(&body, "SubscriptionId").unwrap();
    let watermark = test_xml_text(&body, "Watermark").unwrap();

    let request = format!(
        r#"<s:Envelope><s:Body><m:GetEvents><m:SubscriptionId>{subscription_id}</m:SubscriptionId><m:Watermark>{watermark}</m:Watermark></m:GetEvents></s:Body></s:Envelope>"#
    );
    let response = service
        .handle(&bearer_headers(), request.as_bytes())
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:DeletedEvent>"));
    assert!(body.contains(&format!("<t:ItemId Id=\"message:{message_id}\"")));
    assert!(body.contains(&format!("<t:ParentFolderId Id=\"mailbox:{mailbox_id}\"")));
    assert!(!body.contains("<t:StatusEvent>"));
}

#[tokio::test]
async fn pull_subscription_expired_watermark_returns_parseable_error() {
    let mailbox_id = "88888888-8888-8888-8888-888888888888";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            mailbox_id, "inbox", "Inbox",
        )])),
        mapi_notification_cursor: Arc::new(Mutex::new(Some(7))),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:Subscribe><m:PullSubscriptionRequest><t:SubscribeToAllFolders>true</t:SubscribeToAllFolders><t:EventTypes><t:EventType>NewMailEvent</t:EventType></t:EventTypes><t:Timeout>10</t:Timeout></m:PullSubscriptionRequest></m:Subscribe></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let subscription = response_text(response).await;
    let subscription_id = test_xml_text(&subscription, "SubscriptionId").unwrap();
    let watermark = test_xml_text(&subscription, "Watermark").unwrap();
    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:GetEvents><m:SubscriptionId>{subscription_id}</m:SubscriptionId><m:Watermark>{watermark}</m:Watermark></m:GetEvents></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:GetEventsResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(body.contains("<m:ResponseCode>ErrorInvalidWatermark</m:ResponseCode>"));
    assert!(body.contains("canonical change-log retention"));
}

#[tokio::test]
async fn streaming_notification_replay_is_immediate_token_bound_and_inbox_filtered() {
    let inbox_id = Uuid::parse_str("88888888-8888-8888-8888-888888888811").unwrap();
    let custom_id = Uuid::parse_str("88888888-8888-8888-8888-888888888812").unwrap();
    let inbox_message_id = Uuid::parse_str("88888888-8888-8888-8888-888888888813").unwrap();
    let custom_message_id = Uuid::parse_str("88888888-8888-8888-8888-888888888814").unwrap();
    let cursor = Arc::new(Mutex::new(Some(7)));
    let polls = Arc::new(Mutex::new(vec![MapiNotificationPoll {
        event_pending: true,
        cursor: Some(9),
        events: vec![
            MapiNotificationEvent::canonical(
                MapiNotificationKind::Content,
                1,
                1,
                Some(2),
                None,
                8,
                8,
                None,
                None,
                "created".to_string(),
                Some("Inbox item".to_string()),
                None,
                Some("Inbox item".to_string()),
                None,
            )
            .with_canonical_ids(Some(inbox_id), Some(inbox_message_id)),
            MapiNotificationEvent::canonical(
                MapiNotificationKind::Content,
                1,
                1,
                Some(2),
                None,
                9,
                9,
                None,
                None,
                "created".to_string(),
                Some("Custom item".to_string()),
                None,
                Some("Custom item".to_string()),
                None,
            )
            .with_canonical_ids(Some(custom_id), Some(custom_message_id)),
        ],
    }]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox"),
            FakeStore::mailbox(&custom_id.to_string(), "custom", "Custom"),
        ])),
        mapi_notification_cursor: cursor.clone(),
        mapi_notification_polls: polls.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:Subscribe><m:PullSubscriptionRequest><t:SubscribeToAllFolders>true</t:SubscribeToAllFolders><t:EventTypes><t:EventType>NewMailEvent</t:EventType></t:EventTypes><t:Timeout>30</t:Timeout></m:PullSubscriptionRequest></m:Subscribe></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let subscription = response_text(response).await;
    let subscription_id = test_xml_text(&subscription, "SubscriptionId").unwrap();

    let response = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        service.handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:GetStreamingEvents><m:SubscriptionIds><t:SubscriptionId>{subscription_id}</t:SubscriptionId></m:SubscriptionIds><m:ConnectionTimeout>30</m:ConnectionTimeout></m:GetStreamingEvents></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        ),
    )
    .await
    .expect("one-shot streaming replay must not wait for ConnectionTimeout")
    .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:GetStreamingEventsResponse>"));
    assert!(body.contains("<t:NewMailEvent>"));
    assert!(body.contains(&format!("message:{inbox_message_id}")));
    assert!(!body.contains(&format!("message:{custom_message_id}")));
    assert_eq!(*cursor.lock().unwrap(), Some(7));
    assert!(polls.lock().unwrap().is_empty());

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetStreamingEvents><m:SubscriptionIds><t:SubscriptionId>invalid</t:SubscriptionId></m:SubscriptionIds><m:ConnectionTimeout>30</m:ConnectionTimeout></m:GetStreamingEvents></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>ErrorInvalidSubscription</m:ResponseCode>"));
    assert_eq!(*cursor.lock().unwrap(), Some(7));
}

#[tokio::test]
async fn streaming_notification_replay_reports_expired_canonical_cursor() {
    let mailbox_id = "88888888-8888-8888-8888-888888888821";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            mailbox_id, "inbox", "Inbox",
        )])),
        mapi_notification_cursor: Arc::new(Mutex::new(Some(7))),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:Subscribe><m:PullSubscriptionRequest><t:FolderIds><t:FolderId Id="mailbox:88888888-8888-8888-8888-888888888821"/></t:FolderIds><t:EventTypes><t:EventType>CreatedEvent</t:EventType></t:EventTypes><t:Timeout>10</t:Timeout></m:PullSubscriptionRequest></m:Subscribe></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let subscription = response_text(response).await;
    let subscription_id = test_xml_text(&subscription, "SubscriptionId").unwrap();
    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:GetStreamingEvents><m:SubscriptionIds><t:SubscriptionId>{subscription_id}</t:SubscriptionId></m:SubscriptionIds><m:ConnectionTimeout>1</m:ConnectionTimeout></m:GetStreamingEvents></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:GetStreamingEventsResponse>"));
    assert!(body.contains("<m:ResponseCode>ErrorInvalidWatermark</m:ResponseCode>"));
}

#[tokio::test]
async fn pull_notification_tokens_scope_changes_and_empty_replay_are_durable() {
    let mailbox_id = Uuid::parse_str("88888888-8888-8888-8888-888888888801").unwrap();
    let message_id = Uuid::parse_str("88888888-8888-8888-8888-888888888802").unwrap();
    let replays = Arc::new(Mutex::new(vec![
        EwsNotificationReplay {
            expired: false,
            current_cursor: Some(8),
            next_cursor: 8,
            more_events: false,
            events: Vec::new(),
        },
        EwsNotificationReplay {
            expired: false,
            current_cursor: Some(8),
            next_cursor: 8,
            more_events: false,
            events: vec![EwsNotificationLogEvent {
                cursor: 8,
                mailbox_id,
                message_id,
                change_kind: "created".to_string(),
                modseq: 8,
                created_at: "2026-08-16T12:00:00.000000Z".to_string(),
            }],
        },
    ]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &mailbox_id.to_string(),
            "inbox",
            "Inbox",
        )])),
        mapi_notification_cursor: Arc::new(Mutex::new(Some(7))),
        ews_notification_replays: replays,
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:Subscribe><m:PullSubscriptionRequest><t:FolderIds><t:FolderId Id="mailbox:{mailbox_id}"/></t:FolderIds><t:EventTypes><t:EventType>CreatedEvent</t:EventType></t:EventTypes><t:Timeout>10</t:Timeout></m:PullSubscriptionRequest></m:Subscribe></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let subscription = response_text(response).await;
    let subscription_id = test_xml_text(&subscription, "SubscriptionId").unwrap();
    let watermark = test_xml_text(&subscription, "Watermark").unwrap();

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:GetEvents><m:SubscriptionId>{subscription_id}</m:SubscriptionId><m:Watermark>{watermark}</m:Watermark></m:GetEvents></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:CreatedEvent>"));
    assert!(!body.contains("<t:NewMailEvent>"));
    assert!(body.contains("2026-08-16T12:00:00.000000Z"));
    let next_watermark = test_xml_text(&body, "Watermark").unwrap();

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:GetEvents><m:SubscriptionId>{subscription_id}</m:SubscriptionId><m:Watermark>{next_watermark}</m:Watermark></m:GetEvents></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:StatusEvent>"));
    assert!(!body.contains("<t:CreatedEvent>"));
    assert!(body.contains(&next_watermark));

    let tampered = next_watermark.replacen(".8.", ".9.", 1);
    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:GetEvents><m:SubscriptionId>{subscription_id}</m:SubscriptionId><m:Watermark>{tampered}</m:Watermark></m:GetEvents></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>ErrorInvalidWatermark</m:ResponseCode>"));
}

#[tokio::test]
async fn get_user_oof_settings_returns_disabled_without_active_vacation() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetUserOofSettingsRequest /></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:GetUserOofSettingsResponse>"));
    assert!(!body.contains("<m:GetUserOofSettingsRequestResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:OofSettings>"));
    assert!(body.contains("</t:OofSettings>"));
    assert!(!body.contains("<m:OofSettings>"));
    assert!(body.contains("<t:OofState>Disabled</t:OofState>"));
    assert!(body.contains("<t:ExternalAudience>None</t:ExternalAudience>"));
    assert!(body.contains("<m:AllowExternalOof>None</m:AllowExternalOof>"));
    assert!(body.contains("<t:ServerVersionInfo"));
}

#[tokio::test]
async fn get_user_oof_settings_projects_canonical_sieve_vacation() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        active_sieve_script: Arc::new(Mutex::new(Some(
            r#"require ["vacation"];
               vacation :subject "Out" :days 3 "Away until Monday";"#
                .to_string(),
        ))),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetUserOofSettings /></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:GetUserOofSettingsResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:OofState>Enabled</t:OofState>"));
    assert!(body.contains("<t:ExternalAudience>All</t:ExternalAudience>"));
    assert!(body.contains("<t:InternalReply><t:Message>Away until Monday</t:Message>"));
    assert!(body.contains("<t:ExternalReply><t:Message>Away until Monday</t:Message>"));
    assert!(body.contains("<t:ServerVersionInfo"));
}

#[tokio::test]
async fn set_user_oof_settings_writes_canonical_sieve_vacation() {
    let active_sieve_script = Arc::new(Mutex::new(None));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        active_sieve_script: active_sieve_script.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:SetUserOofSettings>
                  <t:OofSettings>
                    <t:OofState>Enabled</t:OofState>
                    <t:InternalReply><t:Message>Back next week</t:Message></t:InternalReply>
                  </t:OofSettings>
                </m:SetUserOofSettings>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:SetUserOofSettingsResponse>"));
    assert!(body.contains("<m:ResponseMessage ResponseClass=\"Success\">"));
    assert!(!body.contains("<m:ResponseMessages>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    let script = active_sieve_script.lock().unwrap().clone().unwrap();
    assert!(script.contains("vacation :days 7 \"Back next week\";"));
}

#[tokio::test]
async fn set_user_oof_settings_scheduled_round_trips_canonical_sieve_metadata() {
    let active_sieve_script = Arc::new(Mutex::new(None));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        active_sieve_script: active_sieve_script.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:SetUserOofSettingsRequest>
                  <t:Mailbox><t:Address>alice@example.test</t:Address></t:Mailbox>
                  <t:UserOofSettings>
                    <t:OofState>Scheduled</t:OofState>
                    <t:ExternalAudience>Known</t:ExternalAudience>
                    <t:Duration>
                      <t:StartTime>2026-05-15T00:00:00</t:StartTime>
                      <t:EndTime>2026-05-17T00:00:00</t:EndTime>
                    </t:Duration>
                    <t:InternalReply><t:Message>Back Monday</t:Message></t:InternalReply>
                    <t:ExternalReply><t:Message>Back Monday external</t:Message></t:ExternalReply>
                  </t:UserOofSettings>
                </m:SetUserOofSettingsRequest>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:SetUserOofSettingsResponse>"));
    assert!(body.contains("<m:ResponseMessage ResponseClass=\"Success\">"));
    assert!(!body.contains("<m:ResponseMessages>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));

    let script = active_sieve_script.lock().unwrap().clone().unwrap();
    assert!(script.contains("# LPE-EWS-OOF-State: Scheduled"));
    assert!(script.contains("# LPE-EWS-OOF-ExternalAudience: Known"));
    assert!(script.contains("# LPE-EWS-OOF-StartTime: 2026-05-15T00:00:00"));
    assert!(script.contains("# LPE-EWS-OOF-EndTime: 2026-05-17T00:00:00"));
    assert!(script.contains("vacation :days 7 \"Back Monday\";"));

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetUserOofSettingsRequest /></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:OofState>Scheduled</t:OofState>"));
    assert!(body.contains("<t:ExternalAudience>Known</t:ExternalAudience>"));
    assert!(body.contains("<t:Duration>"));
    assert!(body.contains("<t:StartTime>2026-05-15T00:00:00</t:StartTime>"));
    assert!(body.contains("<t:EndTime>2026-05-17T00:00:00</t:EndTime>"));
    assert!(body.contains("<t:InternalReply><t:Message>Back Monday</t:Message>"));
}

#[tokio::test]
async fn set_user_oof_settings_disables_active_sieve_script() {
    let active_sieve_script = Arc::new(Mutex::new(Some(
        "# LPE-EWS-OOF-State: Enabled\r\n# LPE-EWS-OOF-ExternalAudience: All\r\nrequire [\"vacation\"];\r\nvacation \"Away\";\r\n".to_string(),
    )));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        active_sieve_script: active_sieve_script.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SetUserOofSettings><t:OofSettings><t:OofState>Disabled</t:OofState></t:OofSettings></m:SetUserOofSettings></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:SetUserOofSettingsResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(active_sieve_script.lock().unwrap().is_none());
}

#[tokio::test]
async fn oof_settings_reject_a_foreign_mailbox_without_mutation() {
    let active_sieve_script = Arc::new(Mutex::new(None));
    let service = ExchangeService::new(FakeStore {
        session: Some(FakeStore::account()),
        active_sieve_script: active_sieve_script.clone(),
        ..Default::default()
    });

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SetUserOofSettings><t:Mailbox><t:EmailAddress>other@example.test</t:EmailAddress></t:Mailbox><t:OofSettings><t:OofState>Enabled</t:OofState><t:InternalReply><t:Message>Away</t:Message></t:InternalReply></t:OofSettings></m:SetUserOofSettings></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert!(active_sieve_script.lock().unwrap().is_none());
}

#[tokio::test]
async fn send_item_submits_existing_draft_through_canonical_submission() {
    let draft_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-000000000001").unwrap();
    let drafts_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-000000000002").unwrap();
    let sent_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-000000000003").unwrap();
    let submitted_draft_messages = Arc::new(Mutex::new(Vec::new()));
    let submitted_messages = Arc::new(Mutex::new(Vec::new()));
    let mut draft = FakeStore::email(
        &draft_id.to_string(),
        &drafts_id.to_string(),
        "drafts",
        "Draft to send",
    );
    draft.bcc.push(JmapEmailAddress {
        address: "protected@example.test".to_string(),
        display_name: Some("Protected Recipient".to_string()),
    });
    let emails = Arc::new(Mutex::new(vec![draft]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        emails: emails.clone(),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(&drafts_id.to_string(), "drafts", "Drafts"),
            FakeStore::mailbox(&sent_id.to_string(), "sent", "Sent"),
        ])),
        submitted_draft_messages: submitted_draft_messages.clone(),
        submitted_messages: submitted_messages.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:SendItem SaveItemToFolder="true"><m:ItemIds><t:ItemId Id="message:{draft_id}"/></m:ItemIds></m:SendItem></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:SendItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(!body.contains("protected@example.test"));
    assert_eq!(*submitted_draft_messages.lock().unwrap(), vec![draft_id]);
    let submitted = submitted_messages.lock().unwrap();
    assert_eq!(submitted.len(), 1);
    assert_eq!(submitted[0].draft_message_id, Some(draft_id));
    assert_eq!(submitted[0].source, "ews-senditem");
    assert_eq!(submitted[0].bcc.len(), 1);
    assert_eq!(submitted[0].bcc[0].address, "protected@example.test");
    let stored = emails.lock().unwrap();
    assert!(!stored.iter().any(|email| email.id == draft_id));
    assert!(stored.iter().any(|email| {
        email.mailbox_role == "sent"
            && email
                .bcc
                .iter()
                .any(|recipient| recipient.address == "protected@example.test")
    }));
    assert!(!stored.iter().any(|email| email.mailbox_role == "outbox"));
    drop(stored);

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:GetItem><m:ItemIds><t:ItemId Id="message:ffffffff-ffff-ffff-ffff-ffffffffffff"/></m:ItemIds></m:GetItem></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let sent = response_text(response).await;
    assert!(sent.contains("<m:GetItemResponse>"));
    assert!(sent.contains("message:ffffffff-ffff-ffff-ffff-ffffffffffff"));
}

#[tokio::test]
// [MS-OXWSMSG] section 3.1.4.2: unsupported multi-item canonical shapes
// fail before any contact or calendar creation begins.
async fn create_item_rejects_multiple_contact_or_calendar_items_without_mutation() {
    for items in [
        concat!(
            "<t:Contact><t:DisplayName>First</t:DisplayName></t:Contact>",
            "<t:Contact><t:DisplayName>Second</t:DisplayName></t:Contact>",
        ),
        concat!(
            "<t:CalendarItem><t:Subject>First</t:Subject><t:Start>2026-05-01T10:00:00Z</t:Start><t:End>2026-05-01T11:00:00Z</t:End></t:CalendarItem>",
            "<t:CalendarItem><t:Subject>Second</t:Subject><t:Start>2026-05-02T10:00:00Z</t:Start><t:End>2026-05-02T11:00:00Z</t:End></t:CalendarItem>",
        ),
    ] {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
                "default", "contacts", "Contacts",
            )])),
            calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
                "default", "calendar", "Calendar",
            )])),
            ..Default::default()
        };
        let contacts = store.contacts.clone();
        let events = store.events.clone();
        let service = ExchangeService::new(store);
        let request = format!(
            "<s:Envelope><s:Body><m:CreateItem><m:Items>{items}</m:Items></m:CreateItem></s:Body></s:Envelope>"
        );

        let response = service
            .handle(&bearer_headers(), request.as_bytes())
            .await
            .unwrap();

        let body = response_text(response).await;
        assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
        assert!(contacts.lock().unwrap().is_empty());
        assert!(events.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn get_item_projects_protected_bcc_for_owner_sent_item() {
    let message_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-000000000010").unwrap();
    let mailbox_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-000000000011").unwrap();
    let mut message = FakeStore::email(
        &message_id.to_string(),
        &mailbox_id.to_string(),
        "sent",
        "Protected recipients",
    );
    message.bcc.push(JmapEmailAddress {
        address: "protected@example.test".to_string(),
        display_name: Some("Protected Recipient".to_string()),
    });
    let service = ExchangeService::new(FakeStore {
        session: Some(FakeStore::account()),
        emails: Arc::new(Mutex::new(vec![message])),
        ..Default::default()
    });

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:GetItem><m:ItemIds><t:ItemId Id="message:{message_id}"/></m:ItemIds></m:GetItem></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:BccRecipients>"));
    assert!(body.contains("protected@example.test"));
}

#[tokio::test]
async fn send_item_rejects_an_invalid_batch_before_submitting_a_draft() {
    let draft_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-000000000012").unwrap();
    let drafts_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-000000000013").unwrap();
    let submitted_draft_messages = Arc::new(Mutex::new(Vec::new()));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            &draft_id.to_string(),
            &drafts_id.to_string(),
            "drafts",
            "Draft to preserve",
        )])),
        submitted_draft_messages: submitted_draft_messages.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                concat!(
                    r#"<s:Envelope><s:Body><m:SendItem><m:ItemIds>"#,
                    r#"<t:ItemId Id="message:{draft_id}"/>"#,
                    r#"<t:ItemId Id="contact:aaaaaaaa-bbbb-cccc-dddd-000000000014"/>"#,
                    r#"</m:ItemIds></m:SendItem></s:Body></s:Envelope>"#,
                ),
                draft_id = draft_id
            )
            .as_bytes(),
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert!(submitted_draft_messages.lock().unwrap().is_empty());
}

#[tokio::test]
async fn send_item_requires_one_direct_nonempty_item_ids_collection_before_submitting() {
    let draft_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-000000000021").unwrap();
    let drafts_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-000000000022").unwrap();

    for item_ids in [
        format!(
            r#"<m:ItemIds><t:ItemId Id="message:{draft_id}"/></m:ItemIds><m:ItemIds/>"#
        ),
        format!(r#"<t:ItemId Id="message:{draft_id}"/>"#),
        format!(
            r#"<m:ItemIds><t:ItemId Id="message:{draft_id}"/></m:ItemIds><m:Other><m:ItemIds/></m:Other>"#
        ),
    ] {
        let submitted_draft_messages = Arc::new(Mutex::new(Vec::new()));
        let service = ExchangeService::new(FakeStore {
            session: Some(FakeStore::account()),
            emails: Arc::new(Mutex::new(vec![FakeStore::email(
                &draft_id.to_string(),
                &drafts_id.to_string(),
                "drafts",
                "Draft to preserve",
            )])),
            submitted_draft_messages: submitted_draft_messages.clone(),
            ..Default::default()
        });

        let response = service
            .handle(
                &bearer_headers(),
                format!(
                    r#"<s:Envelope><s:Body><m:SendItem>{item_ids}</m:SendItem></s:Body></s:Envelope>"#
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let body = response_text(response).await;

        assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
        assert!(submitted_draft_messages.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn delete_item_requires_one_direct_nonempty_item_ids_collection_before_deleting() {
    let message_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-000000000023").unwrap();
    let mailbox_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-000000000024").unwrap();

    for item_ids in [
        format!(
            r#"<m:ItemIds><t:ItemId Id="message:{message_id}"/></m:ItemIds><m:ItemIds/>"#
        ),
        format!(r#"<t:ItemId Id="message:{message_id}"/>"#),
        format!(
            r#"<m:ItemIds><t:ItemId Id="message:{message_id}"/></m:ItemIds><m:Other><m:ItemIds/></m:Other>"#
        ),
    ] {
        let deleted_emails = Arc::new(Mutex::new(Vec::new()));
        let service = ExchangeService::new(FakeStore {
            session: Some(FakeStore::account()),
            emails: Arc::new(Mutex::new(vec![FakeStore::email(
                &message_id.to_string(),
                &mailbox_id.to_string(),
                "inbox",
                "Do not delete",
            )])),
            deleted_emails: deleted_emails.clone(),
            ..Default::default()
        });

        let response = service
            .handle(
                &bearer_headers(),
                format!(
                    r#"<s:Envelope><s:Body><m:DeleteItem DeleteType="HardDelete">{item_ids}</m:DeleteItem></s:Body></s:Envelope>"#
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let body = response_text(response).await;

        assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
        assert!(deleted_emails.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn send_item_rejects_multiple_accessible_drafts_without_submitting_either() {
    let first_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-000000000028").unwrap();
    let second_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-000000000029").unwrap();
    let drafts_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-000000000030").unwrap();
    let submitted_draft_messages = Arc::new(Mutex::new(Vec::new()));
    let service = ExchangeService::new(FakeStore {
        session: Some(FakeStore::account()),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(&first_id.to_string(), &drafts_id.to_string(), "drafts", "First"),
            FakeStore::email(&second_id.to_string(), &drafts_id.to_string(), "drafts", "Second"),
        ])),
        submitted_draft_messages: submitted_draft_messages.clone(),
        ..Default::default()
    });

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:SendItem><m:ItemIds><t:ItemId Id="message:{first_id}"/><t:ItemId Id="message:{second_id}"/></m:ItemIds></m:SendItem></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;

    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert!(submitted_draft_messages.lock().unwrap().is_empty());
}

#[tokio::test]
async fn delete_item_rejects_multiple_accessible_items_without_deleting_either() {
    let first_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-000000000031").unwrap();
    let second_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-000000000032").unwrap();
    let mailbox_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-000000000033").unwrap();
    let deleted_emails = Arc::new(Mutex::new(Vec::new()));
    let service = ExchangeService::new(FakeStore {
        session: Some(FakeStore::account()),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(&first_id.to_string(), &mailbox_id.to_string(), "inbox", "First"),
            FakeStore::email(&second_id.to_string(), &mailbox_id.to_string(), "inbox", "Second"),
        ])),
        deleted_emails: deleted_emails.clone(),
        ..Default::default()
    });

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:DeleteItem DeleteType="HardDelete"><m:ItemIds><t:ItemId Id="message:{first_id}"/><t:ItemId Id="message:{second_id}"/></m:ItemIds></m:DeleteItem></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;

    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert!(deleted_emails.lock().unwrap().is_empty());
}

#[tokio::test]
async fn update_item_rejects_multi_item_and_unsupported_message_fields_without_mutation() {
    let first_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-000000000025").unwrap();
    let second_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-000000000026").unwrap();
    let mailbox_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-000000000027").unwrap();
    let mut first = FakeStore::email(&first_id.to_string(), &mailbox_id.to_string(), "inbox", "First");
    first.unread = true;
    let mut second = FakeStore::email(&second_id.to_string(), &mailbox_id.to_string(), "inbox", "Second");
    second.unread = true;
    second.flagged = false;
    let emails = Arc::new(Mutex::new(vec![first, second]));
    let service = ExchangeService::new(FakeStore {
        session: Some(FakeStore::account()),
        emails: emails.clone(),
        ..Default::default()
    });

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                concat!(
                    r#"<s:Envelope><s:Body><m:UpdateItem><m:ItemChanges>"#,
                    r#"<t:ItemChange><t:ItemId Id="message:{first_id}"/><t:Updates>"#,
                    r#"<t:SetItemField><t:FieldURI FieldURI="message:IsRead"/>"#,
                    r#"<t:Message><t:IsRead>true</t:IsRead></t:Message>"#,
                    r#"</t:SetItemField></t:Updates></t:ItemChange>"#,
                    r#"<t:ItemChange><t:ItemId Id="message:{second_id}"/><t:Updates>"#,
                    r#"<t:SetItemField><t:FieldURI FieldURI="message:Flag"/>"#,
                    r#"<t:Message><t:Flag><t:FlagStatus>Flagged</t:FlagStatus></t:Flag></t:Message>"#,
                    r#"</t:SetItemField></t:Updates></t:ItemChange>"#,
                    r#"</m:ItemChanges></m:UpdateItem></s:Body></s:Envelope>"#,
                ),
                first_id = first_id,
                second_id = second_id,
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    let updated = emails.lock().unwrap();
    assert!(updated[0].unread);
    assert!(!updated[0].flagged);
    assert!(updated[1].unread);
    assert!(!updated[1].flagged);
    drop(updated);

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                concat!(
                    r#"<s:Envelope><s:Body><m:UpdateItem><m:ItemChanges>"#,
                    r#"<t:ItemChange><t:ItemId Id="message:{first_id}"/><t:Updates>"#,
                    r#"<t:SetItemField><t:FieldURI FieldURI="message:IsRead"/>"#,
                    r#"<t:Message><t:IsRead>true</t:IsRead></t:Message></t:SetItemField>"#,
                    r#"<t:SetItemField><t:FieldURI FieldURI="item:Subject"/>"#,
                    r#"<t:Message><t:Subject>Ignored today</t:Subject></t:Message></t:SetItemField>"#,
                    r#"</t:Updates></t:ItemChange></m:ItemChanges></m:UpdateItem></s:Body></s:Envelope>"#,
                ),
                first_id = first_id,
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert!(emails.lock().unwrap()[0].unread);
}

#[tokio::test]
async fn update_item_rejects_multiple_targets_before_updating_another_item() {
    let message_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-000000000015").unwrap();
    let mailbox_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-000000000016").unwrap();
    let emails = Arc::new(Mutex::new(vec![FakeStore::email(
        &message_id.to_string(),
        &mailbox_id.to_string(),
        "inbox",
        "Unread must remain unchanged",
    )]));
    let service = ExchangeService::new(FakeStore {
        session: Some(FakeStore::account()),
        emails: emails.clone(),
        ..Default::default()
    });

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                concat!(
                    r#"<s:Envelope><s:Body><m:UpdateItem><m:ItemChanges>"#,
                    r#"<t:ItemChange><t:ItemId Id="message:{message_id}"/><t:Updates>"#,
                    r#"<t:SetItemField><t:FieldURI FieldURI="message:IsRead"/>"#,
                    r#"<t:Message><t:IsRead>true</t:IsRead></t:Message>"#,
                    r#"</t:SetItemField></t:Updates></t:ItemChange>"#,
                    r#"<t:ItemChange><t:ItemId Id="public-folder-item:aaaaaaaa-bbbb-cccc-dddd-000000000017"/>"#,
                    r#"<t:Updates><t:SetItemField><t:FieldURI FieldURI="item:Subject"/>"#,
                    r#"<t:Message><t:Subject>Unavailable</t:Subject></t:Message>"#,
                    r#"</t:SetItemField></t:Updates></t:ItemChange>"#,
                    r#"</m:ItemChanges></m:UpdateItem></s:Body></s:Envelope>"#,
                ),
                message_id = message_id,
            )
            .as_bytes(),
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert!(!emails.lock().unwrap()[0].unread);
}

#[tokio::test]
async fn delete_item_rejects_multiple_targets_before_deleting_another_item() {
    let message_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-000000000018").unwrap();
    let mailbox_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-000000000019").unwrap();
    let deleted_emails = Arc::new(Mutex::new(Vec::new()));
    let service = ExchangeService::new(FakeStore {
        session: Some(FakeStore::account()),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            &message_id.to_string(),
            &mailbox_id.to_string(),
            "inbox",
            "Do not delete",
        )])),
        deleted_emails: deleted_emails.clone(),
        ..Default::default()
    });

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                concat!(
                    r#"<s:Envelope><s:Body><m:DeleteItem DeleteType="HardDelete"><m:ItemIds>"#,
                    r#"<t:ItemId Id="message:{message_id}"/>"#,
                    r#"<t:ItemId Id="public-folder-item:aaaaaaaa-bbbb-cccc-dddd-000000000020"/>"#,
                    r#"</m:ItemIds></m:DeleteItem></s:Body></s:Envelope>"#,
                ),
                message_id = message_id
            )
            .as_bytes(),
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert!(deleted_emails.lock().unwrap().is_empty());
}

#[tokio::test]
async fn inbox_rules_project_and_update_canonical_sieve_rules() {
    let mailbox_rules = Arc::new(Mutex::new(vec![MailboxRule {
        id: Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-000000000011").unwrap(),
        name: "Reports".to_string(),
        is_active: true,
        source_kind: "sieve_script".to_string(),
        condition_summary: "subject contains report".to_string(),
        action_summary: "fileinto Reports".to_string(),
        supported_outlook_projection: true,
        unsupported_exchange_features: Vec::new(),
        size_octets: 64,
        updated_at: "2026-05-07T12:00:00Z".to_string(),
    }]));
    let active_sieve_script = Arc::new(Mutex::new(None));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailbox_rules: mailbox_rules.clone(),
        active_sieve_script: active_sieve_script.clone(),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox("aaaaaaaa-bbbb-cccc-dddd-000000000013", "custom", "Invoices"),
            FakeStore::mailbox("aaaaaaaa-bbbb-cccc-dddd-000000000014", "custom", "Paid"),
        ])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetInboxRules /></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:GetInboxRulesResponse>"));
    assert!(!body.contains("Reports"));

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:UpdateInboxRules><m:Operations><t:CreateRuleOperation><t:Rule><t:DisplayName>Invoices</t:DisplayName><t:IsEnabled>true</t:IsEnabled><t:Conditions><t:SubjectContainsWords><t:String>invoice</t:String></t:SubjectContainsWords></t:Conditions><t:Actions><t:MoveToFolder><t:FolderId Id="mailbox:aaaaaaaa-bbbb-cccc-dddd-000000000013"/></t:MoveToFolder></t:Actions></t:Rule></t:CreateRuleOperation></m:Operations></m:UpdateInboxRules></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:UpdateInboxRulesResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    let sieve = active_sieve_script.lock().unwrap().clone().unwrap();
    assert!(sieve.contains(r#"header :contains "Subject" "invoice""#));
    assert!(sieve.contains(r#"fileinto "Invoices";"#));
    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetInboxRules /></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:DisplayName>Invoices</t:DisplayName>"));
    assert!(body.contains("mailbox:aaaaaaaa-bbbb-cccc-dddd-000000000013"));
    let rule_id = body
        .split("<t:RuleId>")
        .nth(1)
        .and_then(|value| value.split("</t:RuleId>").next())
        .unwrap()
        .to_string();

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:UpdateInboxRules><m:Operations><t:SetRuleOperation><t:Rule><t:RuleId>{rule_id}</t:RuleId><t:DisplayName>Invoices</t:DisplayName><t:IsEnabled>true</t:IsEnabled><t:Conditions><t:SubjectContainsWords><t:String>paid invoice</t:String></t:SubjectContainsWords></t:Conditions><t:Actions><t:MoveToFolder><t:FolderId Id="mailbox:aaaaaaaa-bbbb-cccc-dddd-000000000014"/></t:MoveToFolder></t:Actions></t:Rule></t:SetRuleOperation></m:Operations></m:UpdateInboxRules></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:UpdateInboxRulesResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    let sieve = active_sieve_script.lock().unwrap().clone().unwrap();
    assert!(sieve.contains(r#"header :contains "Subject" "paid invoice""#));
    assert!(sieve.contains(r#"fileinto "Paid";"#));

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetInboxRules /></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("paid invoice"));
    assert!(body.contains("mailbox:aaaaaaaa-bbbb-cccc-dddd-000000000014"));

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:UpdateInboxRules><m:Operations><t:DeleteRuleOperation><t:RuleId>{rule_id}</t:RuleId></t:DeleteRuleOperation></m:Operations></m:UpdateInboxRules></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:UpdateInboxRulesResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(active_sieve_script.lock().unwrap().is_none());
}

#[tokio::test]
async fn update_inbox_rules_rejects_exchange_only_rule_shapes_without_side_effects() {
    let mailbox_rules = Arc::new(Mutex::new(vec![MailboxRule {
        id: Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-000000000012").unwrap(),
        name: "Existing".to_string(),
        is_active: true,
        source_kind: "sieve_script".to_string(),
        condition_summary: "subject contains report".to_string(),
        action_summary: "fileinto Reports".to_string(),
        supported_outlook_projection: true,
        unsupported_exchange_features: Vec::new(),
        size_octets: 64,
        updated_at: "2026-05-07T12:00:00Z".to_string(),
    }]));
    let active_sieve_script = Arc::new(Mutex::new(None));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailbox_rules: mailbox_rules.clone(),
        active_sieve_script: active_sieve_script.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    for request in [
        br#"<s:Envelope><s:Body><m:UpdateInboxRules><m:Operations><t:CreateRuleOperation><t:Rule><t:DisplayName>Client only</t:DisplayName><t:IsClientOnly>true</t:IsClientOnly><t:Conditions><t:SubjectContainsWords><t:String>invoice</t:String></t:SubjectContainsWords></t:Conditions><t:Actions><t:MoveToFolder><t:DisplayName>Invoices</t:DisplayName></t:MoveToFolder></t:Actions></t:Rule></t:CreateRuleOperation></m:Operations></m:UpdateInboxRules></s:Body></s:Envelope>"#.as_slice(),
        br#"<s:Envelope><s:Body><m:UpdateInboxRules><m:Operations><t:CreateRuleOperation><t:Rule><t:DisplayName>Blob</t:DisplayName><t:RuleProviderData>AQID</t:RuleProviderData><t:Conditions><t:SubjectContainsWords><t:String>invoice</t:String></t:SubjectContainsWords></t:Conditions><t:Actions><t:MoveToFolder><t:DisplayName>Invoices</t:DisplayName></t:MoveToFolder></t:Actions></t:Rule></t:CreateRuleOperation></m:Operations></m:UpdateInboxRules></s:Body></s:Envelope>"#.as_slice(),
        br#"<s:Envelope><s:Body><m:UpdateInboxRules><m:Operations><t:CreateRuleOperation><t:Rule><t:DisplayName>Deferred</t:DisplayName><t:DeferredActionMessage>AQID</t:DeferredActionMessage><t:Conditions><t:SubjectContainsWords><t:String>invoice</t:String></t:SubjectContainsWords></t:Conditions><t:Actions><t:MoveToFolder><t:DisplayName>Invoices</t:DisplayName></t:MoveToFolder></t:Actions></t:Rule></t:CreateRuleOperation></m:Operations></m:UpdateInboxRules></s:Body></s:Envelope>"#.as_slice(),
        br#"<s:Envelope><s:Body><m:UpdateInboxRules><m:Operations><t:CreateRuleOperation><t:Rule><t:DisplayName>Valid first</t:DisplayName><t:Conditions><t:SubjectContainsWords><t:String>valid</t:String></t:SubjectContainsWords></t:Conditions><t:Actions><t:Delete/></t:Actions></t:Rule></t:CreateRuleOperation><t:CreateRuleOperation><t:Rule><t:DisplayName>Unsupported second</t:DisplayName><t:IsClientOnly>true</t:IsClientOnly></t:Rule></t:CreateRuleOperation></m:Operations></m:UpdateInboxRules></s:Body></s:Envelope>"#.as_slice(),
    ] {
        let response = service.handle(&bearer_headers(), request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response_text(response).await;
        assert!(body.contains("<m:UpdateInboxRulesResponse>"));
        assert!(body.contains("ResponseClass=\"Error\""));
        assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
        assert_eq!(mailbox_rules.lock().unwrap().len(), 1);
        assert!(mailbox_rules
            .lock()
            .unwrap()
            .iter()
            .all(|rule| rule.name == "Existing"));
        assert!(active_sieve_script.lock().unwrap().is_none());
    }
}

#[tokio::test]
async fn inbox_rules_are_scoped_to_the_authenticated_mailbox() {
    let active_sieve_script = Arc::new(Mutex::new(Some(
        "require [\"fileinto\"];\nif true { fileinto \"Private\"; }\n".to_string(),
    )));
    let service = ExchangeService::new(FakeStore {
        session: Some(FakeStore::account()),
        active_sieve_script: active_sieve_script.clone(),
        ..Default::default()
    });

    for request in [
        br#"<s:Envelope><s:Body><m:GetInboxRules><m:MailboxSmtpAddress>other@example.test</m:MailboxSmtpAddress></m:GetInboxRules></s:Body></s:Envelope>"#.as_slice(),
        br#"<s:Envelope><s:Body><m:UpdateInboxRules><m:MailboxSmtpAddress>other@example.test</m:MailboxSmtpAddress><m:Operations><t:CreateRuleOperation><t:Rule><t:DisplayName>Discard invoices</t:DisplayName><t:Conditions><t:SubjectContainsWords><t:String>invoice</t:String></t:SubjectContainsWords></t:Conditions><t:Actions><t:Delete/></t:Actions></t:Rule></t:CreateRuleOperation></m:Operations></m:UpdateInboxRules></s:Body></s:Envelope>"#.as_slice(),
    ] {
        let response = service.handle(&bearer_headers(), request).await.unwrap();
        let body = response_text(response).await;
        assert!(body.contains("ResponseClass=\"Error\""));
        assert!(body.contains("ErrorInvalidOperation"));
    }
    assert_eq!(
        active_sieve_script.lock().unwrap().as_deref(),
        Some("require [\"fileinto\"];\nif true { fileinto \"Private\"; }\n")
    );
}

#[tokio::test]
async fn update_inbox_rules_refuses_unmanaged_or_oof_sieve_without_mutation() {
    for script in [
        "require [\"fileinto\"];\nif true { fileinto \"Private\"; }\n",
        "require [\"vacation\"];\nvacation \"Away\";\n",
    ] {
        let active_sieve_script = Arc::new(Mutex::new(Some(script.to_string())));
        let service = ExchangeService::new(FakeStore {
            session: Some(FakeStore::account()),
            active_sieve_script: active_sieve_script.clone(),
            ..Default::default()
        });
        let response = service
            .handle(
                &bearer_headers(),
                br#"<s:Envelope><s:Body><m:UpdateInboxRules><m:Operations><t:CreateRuleOperation><t:Rule><t:DisplayName>Discard invoices</t:DisplayName><t:Conditions><t:SubjectContainsWords><t:String>invoice</t:String></t:SubjectContainsWords></t:Conditions><t:Actions><t:Delete/></t:Actions></t:Rule></t:CreateRuleOperation></m:Operations></m:UpdateInboxRules></s:Body></s:Envelope>"#,
            )
            .await
            .unwrap();
        let body = response_text(response).await;
        assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
        assert_eq!(active_sieve_script.lock().unwrap().as_deref(), Some(script));
    }
}

#[tokio::test]
async fn inbox_rules_require_the_exact_active_generated_script_name() {
    let script = concat!(
        "# lpe-ews-inbox-rules-v1\n",
        "require [\"discard\"];\n",
        "# lpe-ews-rule-v1 aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee 1 RGlzY2FyZCBpbnZvaWNlcw aW52b2ljZQ\n",
        "# lpe-ews-action-delete\n",
        "if header :contains \"Subject\" \"invoice\" {\n  discard;\n  stop;\n}\n"
    );
    let active_sieve_script = Arc::new(Mutex::new(Some(script.to_string())));
    let service = ExchangeService::new(FakeStore {
        session: Some(FakeStore::account()),
        active_sieve_script: active_sieve_script.clone(),
        active_sieve_script_name: Arc::new(Mutex::new(Some("other-script".to_string()))),
        ..Default::default()
    });

    let read = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetInboxRules /></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let read_body = response_text(read).await;
    assert!(read_body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(!read_body.contains("Discard invoices"));

    let update = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:UpdateInboxRules><m:Operations><t:CreateRuleOperation><t:Rule><t:DisplayName>Discard receipts</t:DisplayName><t:Conditions><t:SubjectContainsWords><t:String>receipt</t:String></t:SubjectContainsWords></t:Conditions><t:Actions><t:Delete/></t:Actions></t:Rule></t:CreateRuleOperation></m:Operations></m:UpdateInboxRules></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let update_body = response_text(update).await;
    assert!(update_body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert_eq!(active_sieve_script.lock().unwrap().as_deref(), Some(script));
}

#[tokio::test]
async fn update_inbox_rules_rolls_back_an_invalid_later_operation() {
    let active_sieve_script = Arc::new(Mutex::new(None));
    let service = ExchangeService::new(FakeStore {
        session: Some(FakeStore::account()),
        active_sieve_script: active_sieve_script.clone(),
        ..Default::default()
    });
    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:UpdateInboxRules><m:Operations><t:CreateRuleOperation><t:Rule><t:DisplayName>Discard invoices</t:DisplayName><t:Conditions><t:SubjectContainsWords><t:String>invoice</t:String></t:SubjectContainsWords></t:Conditions><t:Actions><t:Delete/></t:Actions></t:Rule></t:CreateRuleOperation></m:Operations></m:UpdateInboxRules></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    assert!(response_text(response)
        .await
        .contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    let before = active_sieve_script.lock().unwrap().clone();

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:UpdateInboxRules><m:Operations><t:CreateRuleOperation><t:Rule><t:DisplayName>Discard reports</t:DisplayName><t:Conditions><t:SubjectContainsWords><t:String>report</t:String></t:SubjectContainsWords></t:Conditions><t:Actions><t:Delete/></t:Actions></t:Rule></t:CreateRuleOperation><t:CreateRuleOperation><t:Rule><t:DisplayName>Move invoices</t:DisplayName><t:Conditions><t:SubjectContainsWords><t:String>invoice</t:String></t:SubjectContainsWords></t:Conditions><t:Actions><t:MoveToFolder><t:FolderId Id="mailbox:cccccccc-cccc-cccc-cccc-cccccccccccc"/></t:MoveToFolder></t:Actions></t:Rule></t:CreateRuleOperation></m:Operations></m:UpdateInboxRules></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert_eq!(*active_sieve_script.lock().unwrap(), before);
}

#[tokio::test]
async fn reminders_are_read_and_dismissed_from_canonical_reminder_state() {
    let reminder_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-000000000021").unwrap();
    let task_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-000000000022").unwrap();
    let reminders = Arc::new(Mutex::new(vec![
        ClientReminder {
            source_type: "calendar".to_string(),
            source_id: reminder_id,
            occurrence_start_at: Some("2026-05-08T09:00:00Z".to_string()),
            title: "Planning".to_string(),
            due_at: Some("2026-05-08T10:00:00Z".to_string()),
            reminder_at: "2026-05-08T08:45:00Z".to_string(),
            dismissed_at: None,
            completed_at: None,
            status: "active".to_string(),
        },
        ClientReminder {
            source_type: "calendar".to_string(),
            source_id: reminder_id,
            occurrence_start_at: Some("2026-05-09T09:00:00Z".to_string()),
            title: "Planning".to_string(),
            due_at: Some("2026-05-09T10:00:00Z".to_string()),
            reminder_at: "2026-05-09T08:45:00Z".to_string(),
            dismissed_at: None,
            completed_at: None,
            status: "active".to_string(),
        },
        ClientReminder {
            source_type: "task".to_string(),
            source_id: task_id,
            occurrence_start_at: None,
            title: "Follow up".to_string(),
            due_at: Some("2026-05-08T12:00:00Z".to_string()),
            reminder_at: "2026-05-08T11:30:00Z".to_string(),
            dismissed_at: None,
            completed_at: None,
            status: "active".to_string(),
        },
    ]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        reminders: reminders.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetReminders /></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:GetRemindersResponse>"));
    assert!(body.contains("<t:Subject>Planning</t:Subject>"));
    assert!(body.contains("<t:Subject>Follow up</t:Subject>"));
    assert!(body.contains("<t:ReminderTime>2026-05-08T08:45:00Z</t:ReminderTime>"));
    assert!(body.contains("<t:ReminderTime>2026-05-08T11:30:00Z</t:ReminderTime>"));
    assert!(body.contains(&format!("calendar:{reminder_id}:2026-05-08T09:00:00Z")));
    assert!(body.contains(&format!("task:{task_id}")));

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:PerformReminderAction><m:ReminderItemActions><t:ReminderItemAction><t:ActionType>Dismiss</t:ActionType><t:ItemId Id="calendar:{reminder_id}:2026-05-08T09:00:00Z"/></t:ReminderItemAction></m:ReminderItemActions></m:PerformReminderAction></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:PerformReminderActionResponse>"));
    let stored = reminders.lock().unwrap();
    assert_eq!(stored[0].status, "dismissed");
    assert_eq!(stored[0].dismissed_at.as_deref(), Some("now"));
    assert_eq!(stored[1].status, "active");
    assert!(stored[1].dismissed_at.is_none());
    assert_eq!(stored[2].status, "active");
    assert!(stored[2].dismissed_at.is_none());
}

#[tokio::test]
async fn perform_reminder_action_snoozes_calendar_and_task_canonical_reminders() {
    let account = FakeStore::account();
    let event_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-000000000023").unwrap();
    let task_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-000000000024").unwrap();
    let task_list_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-000000000025").unwrap();
    let reminders = Arc::new(Mutex::new(vec![
        ClientReminder {
            source_type: "calendar".to_string(),
            source_id: event_id,
            occurrence_start_at: Some("2026-05-09T09:00:00Z".to_string()),
            title: "Standup".to_string(),
            due_at: Some("2026-05-09T09:00:00Z".to_string()),
            reminder_at: "2026-05-09T08:45:00Z".to_string(),
            dismissed_at: Some("2026-05-09T08:46:00Z".to_string()),
            completed_at: None,
            status: "dismissed".to_string(),
        },
        ClientReminder {
            source_type: "calendar".to_string(),
            source_id: event_id,
            occurrence_start_at: Some("2026-05-10T09:00:00Z".to_string()),
            title: "Standup".to_string(),
            due_at: Some("2026-05-10T09:00:00Z".to_string()),
            reminder_at: "2026-05-10T08:45:00Z".to_string(),
            dismissed_at: None,
            completed_at: None,
            status: "pending".to_string(),
        },
        ClientReminder {
            source_type: "task".to_string(),
            source_id: task_id,
            occurrence_start_at: Some("2026-05-09T15:00:00Z".to_string()),
            title: "Ship notes".to_string(),
            due_at: Some("2026-05-09T15:00:00Z".to_string()),
            reminder_at: "2026-05-09T14:30:00Z".to_string(),
            dismissed_at: Some("2026-05-09T14:31:00Z".to_string()),
            completed_at: None,
            status: "dismissed".to_string(),
        },
    ]));
    let events = Arc::new(Mutex::new(vec![AccessibleEvent {
        id: event_id,
        uid: "event-uid".to_string(),
        collection_id: "default".to_string(),
        owner_account_id: account.account_id,
        owner_email: account.email.clone(),
        owner_display_name: account.display_name.clone(),
        rights: FakeStore::rights(),
        date: "2026-05-09".to_string(),
        time: "09:00".to_string(),
        time_zone: "UTC".to_string(),
        duration_minutes: 30,
        all_day: false,
        status: "confirmed".to_string(),
        sequence: 1,
        recurrence_rule: String::new(),
        recurrence_json: "{}".to_string(),
        recurrence_exceptions_json: "{}".to_string(),
        title: "Standup".to_string(),
        location: String::new(),
        organizer_json: "{}".to_string(),
        attendees: String::new(),
        attendees_json: "[]".to_string(),
        notes: String::new(),
        body_html: String::new(),
    }]));
    let tasks = Arc::new(Mutex::new(vec![FakeStore::task(
        &task_id.to_string(),
        &task_list_id.to_string(),
        "Ship notes",
    )]));
    let store = FakeStore {
        session: Some(account),
        reminders: reminders.clone(),
        events,
        tasks,
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:PerformReminderAction><m:ReminderItemActions><t:ReminderItemAction><t:ActionType>Snooze</t:ActionType><t:NewReminderTime>2026-05-09T16:00:00Z</t:NewReminderTime><t:ItemId Id="calendar:{event_id}:2026-05-09T09:00:00Z"/></t:ReminderItemAction></m:ReminderItemActions></m:PerformReminderAction></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:PerformReminderActionResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:PerformReminderAction><m:ReminderItemActions><t:ReminderItemAction><t:ActionType>Snooze</t:ActionType><t:NewReminderTime>2026-05-09T16:00:00Z</t:NewReminderTime><t:ItemId Id="task:{task_id}:2026-05-09T15:00:00Z"/></t:ReminderItemAction></m:ReminderItemActions></m:PerformReminderAction></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));

    let stored = reminders.lock().unwrap();
    assert_eq!(stored.len(), 3);
    assert_eq!(stored[0].reminder_at, "2026-05-09T16:00:00Z");
    assert_eq!(stored[1].reminder_at, "2026-05-10T08:45:00Z");
    assert_eq!(stored[2].reminder_at, "2026-05-09T16:00:00Z");
    assert!(stored[0].dismissed_at.is_none());
    assert!(stored[2].dismissed_at.is_none());
    assert_eq!(stored[0].status, "pending");
    assert_eq!(stored[1].status, "pending");
    assert_eq!(stored[2].status, "pending");
    drop(stored);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetReminders /></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:Subject>Standup</t:Subject>"));
    assert!(body.contains("<t:Subject>Ship notes</t:Subject>"));
    assert!(body.contains("<t:ReminderTime>2026-05-09T16:00:00Z</t:ReminderTime>"));
    assert!(body.contains("<t:ReminderTime>2026-05-10T08:45:00Z</t:ReminderTime>"));
}

#[tokio::test]
async fn get_mail_tips_projects_directory_and_oof_without_local_tip_state() {
    let recipient = AuthenticatedAccount {
        tenant_id: FakeStore::account().tenant_id,
        account_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
        email: "bob@example.test".to_string(),
        display_name: "Bob Recipient".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let store = FakeStore {
        session: Some(FakeStore::account()),
        directory_accounts: Arc::new(Mutex::new(vec![recipient])),
        active_sieve_script: Arc::new(Mutex::new(Some(
            r#"require ["vacation"]; vacation :days 3 "Back Monday";"#.to_string(),
        ))),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope><s:Body>
              <m:GetMailTips>
                <m:SendingAs><t:EmailAddress>alice@example.test</t:EmailAddress></m:SendingAs>
                <m:Recipients>
                  <t:Mailbox><t:EmailAddress>bob@example.test</t:EmailAddress></t:Mailbox>
                  <t:Mailbox><t:EmailAddress>missing@example.test</t:EmailAddress></t:Mailbox>
                </m:Recipients>
                <m:MailTipsRequested>InvalidRecipient OutOfOfficeMessage MailboxFull DeliveryRestricted ModerationStatus CustomMailTip</m:MailTipsRequested>
              </m:GetMailTips>
            </s:Body></s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:GetMailTipsResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:EmailAddress>bob@example.test</t:EmailAddress>"));
    assert!(body.contains("<t:Name>Bob Recipient</t:Name>"));
    assert!(body.contains("<t:MailboxType>Mailbox</t:MailboxType>"));
    assert!(body.contains("<t:Message>Back Monday</t:Message>"));
    assert!(body.contains("<t:EmailAddress>missing@example.test</t:EmailAddress>"));
    assert!(body.contains("<t:MailboxType>Unknown</t:MailboxType>"));
    assert!(body.contains("<t:InvalidRecipient>true</t:InvalidRecipient>"));
    for unsupported_tip in [
        "MailboxFull",
        "DeliveryRestricted",
        "IsModerated",
        "CustomMailTip",
        "Bcc",
    ] {
        assert!(!body.contains(unsupported_tip));
    }
}

#[tokio::test]
async fn get_service_configuration_reports_bounded_mail_tips_and_parseable_gaps() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope><s:Body>
              <m:GetServiceConfiguration>
                <m:RequestedConfiguration>
                  <t:ConfigurationName>MailTips</t:ConfigurationName>
                  <t:ConfigurationName>UnifiedMessagingConfiguration</t:ConfigurationName>
                  <t:ConfigurationName>ProtectionRules</t:ConfigurationName>
                  <t:ConfigurationName>PolicyTips</t:ConfigurationName>
                </m:RequestedConfiguration>
              </m:GetServiceConfiguration>
            </s:Body></s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:GetServiceConfigurationResponse>"));
    assert!(body.contains("<m:ConfigurationName>MailTips</m:ConfigurationName>"));
    assert!(body.contains("<m:MailTipsConfiguration>"));
    assert!(body.contains("<t:MailTipsEnabled>true</t:MailTipsEnabled>"));
    assert!(body.contains(
        "<t:MaxRecipientsPerGetMailTipsRequest>100</t:MaxRecipientsPerGetMailTipsRequest>"
    ));
    assert!(
        body.contains("<m:ConfigurationName>UnifiedMessagingConfiguration</m:ConfigurationName>")
    );
    assert!(body.contains("<m:ConfigurationName>ProtectionRules</m:ConfigurationName>"));
    assert!(body.contains("<m:ConfigurationName>PolicyTips</m:ConfigurationName>"));
    assert!(body.contains("Unified Messaging service configuration is not implemented by LPE."));
    assert!(body.contains("Protection Rules service configuration is not implemented by LPE."));
    assert!(body.contains("Policy Tips service configuration is not implemented by LPE."));
    assert_eq!(body.matches("ResponseClass=\"Success\"").count(), 1);
    assert_eq!(body.matches("ResponseClass=\"Error\"").count(), 3);
    assert_eq!(
        body.matches("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>")
            .count(),
        3
    );
}

#[tokio::test]
async fn get_service_configuration_returns_a_parseable_gap_for_each_unknown_request() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope><s:Body>
              <m:GetServiceConfiguration>
                <m:RequestedConfiguration>
                  <t:ConfigurationName>OrganizationRelationshipSettings</t:ConfigurationName>
                  <t:ConfigurationName>MailboxProtectionRules</t:ConfigurationName>
                </m:RequestedConfiguration>
              </m:GetServiceConfiguration>
            </s:Body></s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert_eq!(body.matches("ResponseClass=\"Error\"").count(), 2);
    assert_eq!(
        body.matches("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>")
            .count(),
        2
    );
    assert!(body.contains(
        "<m:ConfigurationName>OrganizationRelationshipSettings</m:ConfigurationName>"
    ));
    assert!(body.contains("<m:ConfigurationName>MailboxProtectionRules</m:ConfigurationName>"));
    assert!(!body.contains("<m:ConfigurationName>Unknown</m:ConfigurationName>"));
}

#[tokio::test]
async fn get_service_configuration_defaults_to_supported_mail_tips_config() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetServiceConfiguration /></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:GetServiceConfigurationResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<m:ConfigurationName>MailTips</m:ConfigurationName>"));
    assert!(!body.contains("Unified Messaging service configuration"));
}

#[tokio::test]
async fn get_user_retention_policy_tags_projects_same_tenant_assignment_visibility() {
    let account = FakeStore::account();
    let foreign_tenant_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    let visible_tag_id = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
    let assigned_hidden_tag_id = Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap();
    let hidden_unassigned_tag_id = Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap();
    let foreign_tag_id = Uuid::parse_str("44444444-4444-4444-4444-444444444444").unwrap();
    let disabled_tag_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let store = FakeStore {
        session: Some(account.clone()),
        disabled_ews_retention_policy_tag_ids: Arc::new(Mutex::new(vec![disabled_tag_id])),
        ews_retention_policy_tags: Arc::new(Mutex::new(vec![
            FakeRetentionPolicyTag {
                tenant_id: account.tenant_id,
                assigned_account_id: None,
                tag: retention_policy_tag(
                    visible_tag_id,
                    "Visible cleanup",
                    "personal",
                    "delete_and_allow_recovery",
                    Some(30),
                    true,
                    "User visible cleanup",
                ),
            },
            FakeRetentionPolicyTag {
                tenant_id: account.tenant_id,
                assigned_account_id: Some(account.account_id),
                tag: retention_policy_tag(
                    assigned_hidden_tag_id,
                    "Assigned archive",
                    "all",
                    "move_to_archive",
                    Some(730),
                    false,
                    "Assigned default archive",
                ),
            },
            FakeRetentionPolicyTag {
                tenant_id: account.tenant_id,
                assigned_account_id: None,
                tag: retention_policy_tag(
                    hidden_unassigned_tag_id,
                    "Hidden unassigned",
                    "all",
                    "permanently_delete",
                    Some(90),
                    false,
                    "Hidden tenant tag",
                ),
            },
            FakeRetentionPolicyTag {
                tenant_id: foreign_tenant_id,
                assigned_account_id: Some(account.account_id),
                tag: retention_policy_tag(
                    foreign_tag_id,
                    "Foreign tenant tag",
                    "personal",
                    "delete_and_allow_recovery",
                    Some(10),
                    true,
                    "Foreign tenant",
                ),
            },
            FakeRetentionPolicyTag {
                tenant_id: account.tenant_id,
                assigned_account_id: Some(account.account_id),
                tag: retention_policy_tag(
                    disabled_tag_id,
                    "Disabled assigned tag",
                    "all",
                    "delete_and_allow_recovery",
                    Some(365),
                    true,
                    "Disabled default tag",
                ),
            },
        ])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetUserRetentionPolicyTags /></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:GetUserRetentionPolicyTagsResponse ResponseClass=\"Success\">"));
    assert_eq!(body.matches("<t:RetentionPolicyTag>").count(), 2);
    assert!(body.contains("<t:DisplayName>Visible cleanup</t:DisplayName>"));
    assert!(body.contains("<t:RetentionId>11111111-1111-1111-1111-111111111111</t:RetentionId>"));
    assert!(body.contains("<t:OptedInto>false</t:OptedInto>"));
    assert!(body.contains("<t:DisplayName>Assigned archive</t:DisplayName>"));
    assert!(body.contains("<t:RetentionId>22222222-2222-2222-2222-222222222222</t:RetentionId>"));
    assert!(body.contains("<t:IsVisible>false</t:IsVisible>"));
    assert!(body.contains("<t:OptedInto>true</t:OptedInto>"));
    assert!(body.contains("<t:IsArchive>true</t:IsArchive>"));
    assert!(!body.contains("Hidden unassigned"));
    assert!(!body.contains("Foreign tenant tag"));
    assert!(!body.contains("Disabled assigned tag"));
}

#[tokio::test]
async fn get_user_retention_policy_tags_returns_documented_response_shape() {
    let account = FakeStore::account();
    let tag_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let store = FakeStore {
        session: Some(account.clone()),
        ews_retention_policy_tags: Arc::new(Mutex::new(vec![FakeRetentionPolicyTag {
            tenant_id: account.tenant_id,
            assigned_account_id: Some(account.account_id),
            tag: retention_policy_tag(
                tag_id,
                "Deleted Items purge",
                "deleted_items",
                "permanently_delete",
                Some(14),
                true,
                "Cleanup & recovery policy",
            ),
        }])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetUserRetentionPolicyTags /></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<m:RetentionPolicyTags>"));
    assert!(body.contains("<t:RetentionPolicyTag>"));
    assert!(body.contains("<t:DisplayName>Deleted Items purge</t:DisplayName>"));
    assert!(body.contains("<t:RetentionId>55555555-5555-5555-5555-555555555555</t:RetentionId>"));
    assert!(body.contains("<t:RetentionPeriod>14</t:RetentionPeriod>"));
    assert!(body.contains("<t:Type>DeletedItems</t:Type>"));
    assert!(body.contains("<t:RetentionAction>PermanentlyDelete</t:RetentionAction>"));
    assert!(body.contains("<t:Description>Cleanup &amp; recovery policy</t:Description>"));
    assert!(body.contains("<t:IsVisible>true</t:IsVisible>"));
    assert!(body.contains("<t:OptedInto>true</t:OptedInto>"));
    assert!(body.contains("<t:IsArchive>false</t:IsArchive>"));
}

#[tokio::test]
async fn rooms_are_projected_from_canonical_directory_entries() {
    let room_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-000000000031").unwrap();
    let equipment_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-000000000032").unwrap();
    let person_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-000000000033").unwrap();
    let hidden_room_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-000000000034").unwrap();
    let foreign_room_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-000000000035").unwrap();
    let foreign_tenant_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    let extra_address_book_entries = Arc::new(Mutex::new(vec![
        ExchangeAddressBookEntry {
            id: room_id,
            display_name: "Room 101".to_string(),
            email: "room101@example.test".to_string(),
            entry_kind: ExchangeAddressBookEntryKind::Account,
            directory_kind: ExchangeAddressBookDirectoryKind::Room,
            member_emails: Vec::new(),
            details: ExchangeAddressBookEntryDetails::default(),
        },
        ExchangeAddressBookEntry {
            id: equipment_id,
            display_name: "Projector A".to_string(),
            email: "projector-a@example.test".to_string(),
            entry_kind: ExchangeAddressBookEntryKind::Account,
            directory_kind: ExchangeAddressBookDirectoryKind::Equipment,
            member_emails: Vec::new(),
            details: ExchangeAddressBookEntryDetails::default(),
        },
        ExchangeAddressBookEntry {
            id: person_id,
            display_name: "Alice Person".to_string(),
            email: "alice@example.test".to_string(),
            entry_kind: ExchangeAddressBookEntryKind::Account,
            directory_kind: ExchangeAddressBookDirectoryKind::Person,
            member_emails: Vec::new(),
            details: ExchangeAddressBookEntryDetails::default(),
        },
        ExchangeAddressBookEntry {
            id: hidden_room_id,
            display_name: "Hidden Room".to_string(),
            email: "hidden-room@example.test".to_string(),
            entry_kind: ExchangeAddressBookEntryKind::Account,
            directory_kind: ExchangeAddressBookDirectoryKind::Room,
            member_emails: Vec::new(),
            details: ExchangeAddressBookEntryDetails::default(),
        },
        ExchangeAddressBookEntry {
            id: foreign_room_id,
            display_name: "Foreign Room".to_string(),
            email: "foreign-room@example.test".to_string(),
            entry_kind: ExchangeAddressBookEntryKind::Account,
            directory_kind: ExchangeAddressBookDirectoryKind::Room,
            member_emails: Vec::new(),
            details: ExchangeAddressBookEntryDetails::default(),
        },
    ]));
    let extra_address_book_entry_tenants = Arc::new(Mutex::new(HashMap::from([(
        foreign_room_id,
        foreign_tenant_id,
    )])));
    let hidden_address_book_entry_ids = Arc::new(Mutex::new(vec![hidden_room_id]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        extra_address_book_entries: extra_address_book_entries.clone(),
        extra_address_book_entry_tenants: extra_address_book_entry_tenants.clone(),
        hidden_address_book_entry_ids: hidden_address_book_entry_ids.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetRooms /></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:GetRoomsResponse>"));
    assert!(body.contains("<t:Name>Room 101</t:Name>"));
    assert!(body.contains("<t:EmailAddress>room101@example.test</t:EmailAddress>"));
    assert!(body.contains("<t:Name>Projector A</t:Name>"));
    assert!(body.contains("<t:EmailAddress>projector-a@example.test</t:EmailAddress>"));
    assert!(!body.contains("Alice Person"));
    assert!(!body.contains("alice@example.test"));
    assert!(!body.contains("Hidden Room"));
    assert!(!body.contains("hidden-room@example.test"));
    assert!(!body.contains("Foreign Room"));
    assert!(!body.contains("foreign-room@example.test"));

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetRoomLists /></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:GetRoomListsResponse>"));
    assert!(body.contains("<t:EmailAddress>rooms@example.test</t:EmailAddress>"));
    assert_eq!(extra_address_book_entries.lock().unwrap().len(), 5);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetRooms><m:RoomList><t:EmailAddress>rooms@example.test</t:EmailAddress></m:RoomList></m:GetRooms></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:GetRoomsResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetRooms><m:RoomList><t:EmailAddress>custom-list@example.test</t:EmailAddress></m:RoomList></m:GetRooms></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:GetRoomsResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert!(body.contains("explicit room-list membership is not supported"));
}

#[tokio::test]
async fn get_rooms_rejects_a_late_unsupported_room_list_without_projection() {
    let room_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-000000000036").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        extra_address_book_entries: Arc::new(Mutex::new(vec![ExchangeAddressBookEntry {
            id: room_id,
            display_name: "Room 102".to_string(),
            email: "room102@example.test".to_string(),
            entry_kind: ExchangeAddressBookEntryKind::Account,
            directory_kind: ExchangeAddressBookDirectoryKind::Room,
            member_emails: Vec::new(),
            details: ExchangeAddressBookEntryDetails::default(),
        }])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetRooms><m:RoomList><t:EmailAddress>rooms@example.test</t:EmailAddress></m:RoomList><m:RoomList><t:EmailAddress>custom-list@example.test</t:EmailAddress></m:RoomList></m:GetRooms></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:GetRoomsResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert!(!body.contains("<m:Rooms>"));
    assert!(!body.contains("Room 102"));
}

#[tokio::test]
async fn pull_and_streaming_notifications_replay_canonical_sql_change_cursor() {
    let mailbox_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-000000000041").unwrap();
    let message_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-000000000042").unwrap();
    let mapi_notification_cursor = Arc::new(Mutex::new(Some(7)));
    let mapi_notification_polls = Arc::new(Mutex::new(vec![
        MapiNotificationPoll {
            event_pending: true,
            cursor: Some(8),
            events: vec![MapiNotificationEvent::canonical(
                MapiNotificationKind::Content,
                1,
                1,
                Some(2),
                None,
                8,
                9,
                None,
                None,
                "created".to_string(),
                Some("Inbox".to_string()),
                None,
                Some("Hello".to_string()),
                None,
            )
            .with_canonical_ids(Some(mailbox_id), Some(message_id))],
        },
        MapiNotificationPoll {
            event_pending: true,
            cursor: Some(9),
            events: vec![MapiNotificationEvent::canonical(
                MapiNotificationKind::Content,
                1,
                1,
                Some(2),
                None,
                9,
                10,
                None,
                None,
                "created".to_string(),
                Some("Inbox".to_string()),
                None,
                Some("Hello again".to_string()),
                None,
            )
            .with_canonical_ids(Some(mailbox_id), Some(message_id))],
        },
    ]));
    let mut email = FakeStore::email(
        &message_id.to_string(),
        &mailbox_id.to_string(),
        "inbox",
        "Hello",
    );
    // [MS-OXWSNTIF] section 2.2.4.5: the item ChangeKey in the replay event
    // must name the same canonical message version GetItem subsequently reads.
    email.modseq = 10;
    email.mailbox_states[0].modseq = 10;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &mailbox_id.to_string(),
            "inbox",
            "Inbox",
        )])),
        emails: Arc::new(Mutex::new(vec![email])),
        mapi_notification_cursor: mapi_notification_cursor.clone(),
        mapi_notification_polls: mapi_notification_polls.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:Subscribe><m:PullSubscriptionRequest><t:SubscribeToAllFolders>true</t:SubscribeToAllFolders><t:EventTypes><t:EventType>NewMailEvent</t:EventType></t:EventTypes><t:Timeout>30</t:Timeout></m:PullSubscriptionRequest></m:Subscribe></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:SubscribeResponse>"));
    let subscription_id = test_xml_text(&body, "SubscriptionId").unwrap();
    let watermark = test_xml_text(&body, "Watermark").unwrap();
    // [MS-OXWSNTIF] sections 2.2.5.1-.2 and 3.1.4.2.3.1: the signed
    // opaque bookmark is replayed verbatim by pull and streaming projections.
    assert!(!watermark.is_empty());

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:GetEvents><m:SubscriptionId>{subscription_id}</m:SubscriptionId><m:Watermark>{watermark}</m:Watermark></m:GetEvents></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:GetEventsResponse>"));
    assert!(body.contains(&format!("<t:ItemId Id=\"message:{message_id}\"")));
    assert!(body.contains(&format!("<t:ParentFolderId Id=\"mailbox:{mailbox_id}\"")));
    let notification_change_key = test_item_change_key(&body, &format!("message:{message_id}"));
    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:GetItem><m:ItemIds><t:ItemId Id="message:{message_id}"/></m:ItemIds></m:GetItem></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let get_body = response_text(response).await;
    assert_eq!(
        test_item_change_key(&get_body, &format!("message:{message_id}")),
        notification_change_key
    );

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:GetStreamingEvents><m:SubscriptionIds><t:SubscriptionId>{subscription_id}</t:SubscriptionId></m:SubscriptionIds><m:ConnectionTimeout>1</m:ConnectionTimeout></m:GetStreamingEvents></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:GetStreamingEventsResponse>"));
    assert!(body.contains(&format!("<t:ItemId Id=\"message:{message_id}\"")));
    assert!(mapi_notification_polls.lock().unwrap().is_empty());
}

fn test_xml_text(xml: &str, local_name: &str) -> Option<String> {
    let open = format!(":{local_name}>");
    let close = format!("</");
    let start = xml.find(&open)? + open.len();
    let rest = &xml[start..];
    let end = rest.find(&close)?;
    Some(rest[..end].trim().to_string())
}

fn test_item_change_key(xml: &str, item_id: &str) -> String {
    let marker = format!("Id=\"{item_id}\" ChangeKey=\"");
    xml.split_once(&marker)
        .and_then(|(_, rest)| {
            rest.split_once('"')
                .map(|(change_key, _)| change_key.to_string())
        })
        .unwrap_or_else(|| panic!("missing EWS ChangeKey for {item_id}"))
}

#[tokio::test]
async fn set_user_oof_settings_errors_use_single_response_message_shape() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:SetUserOofSettingsRequest>
                  <t:UserOofSettings>
                    <t:OofState>Scheduled</t:OofState>
                    <t:InternalReply><t:Message>Back Monday</t:Message></t:InternalReply>
                  </t:UserOofSettings>
                </m:SetUserOofSettingsRequest>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:SetUserOofSettingsResponse>"));
    assert!(body.contains("<m:ResponseMessage ResponseClass=\"Error\">"));
    assert!(!body.contains("<m:ResponseMessages>"));
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert!(body.contains("Duration is required when OofState is Scheduled"));
}

#[tokio::test]
async fn convert_id_round_trips_supported_canonical_object_families() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let canonical_ids = [
        (
            "AlternateId",
            "message:99999999-9999-9999-9999-999999999999",
        ),
        (
            "AlternateId",
            "mailbox:55555555-5555-5555-5555-555555555555",
        ),
        (
            "AlternateId",
            "contact:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        ),
        ("AlternateId", "event:cccccccc-cccc-cccc-cccc-cccccccccccc"),
        ("AlternateId", "task:eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"),
        (
            "AlternateId",
            "attachment:99999999-9999-9999-9999-999999999999:abababab-abab-abab-abab-abababababab",
        ),
        (
            "AlternatePublicFolderId",
            "public-folder:bbbbbbbb-cccc-dddd-eeee-ffffffffffff",
        ),
        (
            "AlternatePublicFolderItemId",
            "public-folder-item:abababab-abab-abab-abab-abababababab",
        ),
    ];
    let source_ids = canonical_ids
        .iter()
        .map(|(element, id)| format!(r#"<t:{element} Format="EwsId" Id="{id}"/>"#))
        .collect::<String>();

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:ConvertId DestinationFormat="OwaId"><m:SourceIds>{source_ids}</m:SourceIds></m:ConvertId></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:ConvertIdResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert_eq!(
        body.matches("Format=\"OwaId\"").count(),
        canonical_ids.len()
    );
    assert!(body.contains("<t:AlternatePublicFolderId Format=\"OwaId\""));
    assert!(body.contains("<t:AlternatePublicFolderItemId Format=\"OwaId\""));
    assert!(body.contains("LPEEWS1."));
    for (_, id) in canonical_ids {
        assert!(!body.contains(&format!("Id=\"{id}\"")));
    }

    let source_ids = convert_id_response_sources(&body)
        .into_iter()
        .map(|(element, format, id)| format!(r#"<t:{element} Format="{format}" Id="{id}"/>"#))
        .collect::<String>();
    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:ConvertId DestinationFormat="EwsId"><m:SourceIds>{source_ids}</m:SourceIds></m:ConvertId></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:ConvertIdResponse>"));
    assert_eq!(
        body.matches("Format=\"EwsId\"").count(),
        canonical_ids.len()
    );
    for (_, id) in canonical_ids {
        assert!(body.contains(&format!("Id=\"{id}\"")));
    }
}

#[tokio::test]
async fn convert_id_round_trips_hex_entry_id_attachment_payload() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let attachment_id =
        "attachment:99999999-9999-9999-9999-999999999999:abababab-abab-abab-abab-abababababab";

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:ConvertId DestinationFormat="HexEntryId"><m:SourceIds><t:AlternateId Format="EwsId" Id="{attachment_id}"/></m:SourceIds></m:ConvertId></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ConvertIdResponse>"));
    assert!(body.contains("Format=\"HexEntryId\""));
    let (_, _, hex_id) = convert_id_response_sources(&body)
        .into_iter()
        .next()
        .expect("hex ConvertId response id");
    assert!(hex_id.chars().all(|value| value.is_ascii_hexdigit()));

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:ConvertId DestinationFormat="EwsId"><m:SourceIds><t:AlternateId Format="HexEntryId" Id="{hex_id}"/></m:SourceIds></m:ConvertId></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ConvertIdResponse>"));
    assert!(body.contains(&format!("Id=\"{attachment_id}\"")));
}

#[tokio::test]
async fn convert_id_rejects_malformed_and_cross_family_sources_without_lookup() {
    let service = ExchangeService::new(FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    });
    for source in [
        "<t:AlternatePublicFolderId Format=\"EwsId\" Id=\"message:99999999-9999-9999-9999-999999999999\"/>",
        "<t:AlternateId Format=\"EwsId\" Id=\"public-folder:bbbbbbbb-cccc-dddd-eeee-ffffffffffff\"/>",
        "<t:AlternateId Format=\"Unknown\" Id=\"message:99999999-9999-9999-9999-999999999999\"/>",
        "<t:AlternateId Format=\"EwsId\"/>",
    ] {
        let response = service
            .handle(
                &bearer_headers(),
                format!(
                    "<s:Envelope><s:Body><m:ConvertId DestinationFormat=\"EwsId\"><m:SourceIds>{source}</m:SourceIds></m:ConvertId></s:Body></s:Envelope>"
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let body = response_text(response).await;
        assert!(body.contains("<m:ResponseCode>ErrorInvalidId</m:ResponseCode>"));
        assert!(!body.contains("not found"));
    }
}

fn convert_id_response_sources(body: &str) -> Vec<(String, String, String)> {
    [
        "AlternateId",
        "AlternatePublicFolderId",
        "AlternatePublicFolderItemId",
    ]
    .into_iter()
    .flat_map(|element| {
        convert_id_response_sources_for_element(body, element)
            .into_iter()
            .map(move |(format, id)| (element.to_string(), format, id))
    })
    .collect()
}

fn convert_id_response_sources_for_element(body: &str, element: &str) -> Vec<(String, String)> {
    let mut values = Vec::new();
    let mut rest = body;
    while let Some(index) = rest.find(&format!("<t:{element}")) {
        rest = &rest[index..];
        let Some(end) = rest.find('>') else {
            break;
        };
        let tag = &rest[..end];
        let format = test_attr(tag, "Format").unwrap_or_default();
        let id = test_attr(tag, "Id").unwrap_or_default();
        if !format.is_empty() && !id.is_empty() {
            values.push((format, id));
        }
        rest = &rest[end + 1..];
    }
    values
}

fn test_attr(tag: &str, attr: &str) -> Option<String> {
    let start = tag.find(&format!("{attr}=\""))? + attr.len() + 2;
    let end = tag[start..].find('"')?;
    Some(tag[start..start + end].to_string())
}

#[tokio::test]
async fn unknown_ews_operations_return_parseable_invalid_operation_errors() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    for operation in ["UnsupportedOperation"] {
        let request = format!(
            concat!(
                "<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\" ",
                "xmlns:m=\"http://schemas.microsoft.com/exchange/services/2006/messages\">",
                "<s:Body><m:{operation} /></s:Body>",
                "</s:Envelope>"
            ),
            operation = operation
        );
        let response = service
            .handle(&bearer_headers(), request.as_bytes())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_text(response).await;
        assert!(body.contains(&format!("<m:{operation}Response>")));
        assert!(body.contains("ResponseClass=\"Error\""));
        assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
        assert!(body.contains("<t:ServerVersionInfo"));
    }
}

#[tokio::test]
async fn create_managed_folder_uses_canonical_retention_folder_api() {
    let account = FakeStore::account();
    let tag_id = Uuid::parse_str("66666666-6666-6666-6666-666666666666").unwrap();
    let store = FakeStore {
        session: Some(account.clone()),
        ews_retention_policy_tags: Arc::new(Mutex::new(vec![FakeRetentionPolicyTag {
            tenant_id: account.tenant_id,
            assigned_account_id: None,
            tag: retention_policy_tag(
                tag_id,
                "Managed Archive",
                "custom_folder",
                "delete_and_allow_recovery",
                Some(180),
                true,
                "Managed archive folder",
            ),
        }])),
        ..Default::default()
    };
    let service = ExchangeService::new(store.clone());

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:CreateManagedFolder><m:FolderNames><t:FolderName>Managed Archive</t:FolderName></m:FolderNames></m:CreateManagedFolder></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:CreateManagedFolderResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:DisplayName>Managed Archive</t:DisplayName>"));
    assert_eq!(store.created_mailboxes.lock().unwrap().len(), 1);
    assert_eq!(
        store.created_mailboxes.lock().unwrap()[0].name,
        "Managed Archive"
    );
}

#[tokio::test]
async fn create_managed_folder_rejects_unavailable_retention_tags() {
    let account = FakeStore::account();
    let foreign_tenant_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    let store = FakeStore {
        session: Some(account.clone()),
        ews_retention_policy_tags: Arc::new(Mutex::new(vec![
            FakeRetentionPolicyTag {
                tenant_id: account.tenant_id,
                assigned_account_id: None,
                tag: retention_policy_tag(
                    Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap(),
                    "Hidden Folder",
                    "custom_folder",
                    "delete_and_allow_recovery",
                    Some(90),
                    false,
                    "Hidden tag",
                ),
            },
            FakeRetentionPolicyTag {
                tenant_id: foreign_tenant_id,
                assigned_account_id: Some(account.account_id),
                tag: retention_policy_tag(
                    Uuid::parse_str("88888888-8888-8888-8888-888888888888").unwrap(),
                    "Foreign Folder",
                    "custom_folder",
                    "delete_and_allow_recovery",
                    Some(90),
                    true,
                    "Foreign tag",
                ),
            },
        ])),
        ..Default::default()
    };
    let service = ExchangeService::new(store.clone());

    let hidden_response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:CreateManagedFolder><m:FolderNames><t:FolderName>Hidden Folder</t:FolderName></m:FolderNames></m:CreateManagedFolder></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let hidden_body = response_text(hidden_response).await;
    assert!(hidden_body.contains("<m:CreateManagedFolderResponse>"));
    assert!(hidden_body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert!(hidden_body.contains("managed retention folder tag not found"));

    let foreign_response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:CreateManagedFolder><m:FolderNames><t:FolderName>Foreign Folder</t:FolderName></m:FolderNames></m:CreateManagedFolder></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let foreign_body = response_text(foreign_response).await;
    assert!(foreign_body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert!(store.created_mailboxes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn ucs_im_group_operations_use_canonical_contact_group_state() {
    let group_id = Uuid::parse_str("12121212-1212-1212-1212-121212121212").unwrap();
    let contact_id = Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "im_contact_list",
            "contacts",
            "IM Contact List",
        )])),
        contacts: Arc::new(Mutex::new(vec![FakeStore::contact(
            "22222222-2222-2222-2222-222222222222",
            "Bob Contact",
            "bob@example.test",
        )])),
        ..Default::default()
    };
    let groups = store.ews_im_groups.clone();
    let members = store.ews_im_group_members.clone();
    let contacts = store.contacts.clone();
    let service = ExchangeService::new(store);

    let add_group = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:AddImGroup><m:DisplayName>Engineering</m:DisplayName></m:AddImGroup></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let add_group_body = response_text(add_group).await;
    assert!(add_group_body.contains("<m:AddImGroupResponse>"));
    assert!(add_group_body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(add_group_body.contains("im-group:12121212-1212-1212-1212-121212121212"));
    assert_eq!(groups.lock().unwrap()[0].display_name, "Engineering");

    let set_group = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SetImGroup><m:ImGroupId Id="im-group:12121212-1212-1212-1212-121212121212"/><m:DisplayName>Platform</m:DisplayName></m:SetImGroup></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let set_group_body = response_text(set_group).await;
    assert!(set_group_body.contains("<m:SetImGroupResponse>"));
    assert!(set_group_body.contains("<t:DisplayName>Platform</t:DisplayName>"));

    let add_contact = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:AddImContactToGroup><m:ImGroupId Id="im-group:12121212-1212-1212-1212-121212121212"/><m:ContactId Id="contact:22222222-2222-2222-2222-222222222222"/><m:DisplayName>Bob Contact</m:DisplayName></m:AddImContactToGroup></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let add_contact_body = response_text(add_contact).await;
    assert!(add_contact_body.contains("<m:AddImContactToGroupResponse>"));
    assert!(add_contact_body.contains("<t:MemberKind>contact</t:MemberKind>"));
    assert_eq!(members.lock().unwrap()[0].contact_id, Some(contact_id));

    let add_new = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:AddNewImContactToGroup><m:ImGroupId Id="im-group:12121212-1212-1212-1212-121212121212"/><m:DisplayName>Carol IM</m:DisplayName><m:SmtpAddress>carol@example.test</m:SmtpAddress></m:AddNewImContactToGroup></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let add_new_body = response_text(add_new).await;
    assert!(add_new_body.contains("<m:AddNewImContactToGroupResponse>"));
    assert!(add_new_body.contains("<t:MemberKind>contact</t:MemberKind>"));
    assert!(contacts
        .lock()
        .unwrap()
        .iter()
        .any(|contact| contact.collection_id == "im_contact_list"
            && contact.email == "carol@example.test"));

    let remove_from_list = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:RemoveContactFromImList><m:ContactId Id="contact:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"/></m:RemoveContactFromImList></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let remove_from_list_body = response_text(remove_from_list).await;
    assert!(remove_from_list_body.contains("<m:RemoveContactFromImListResponse>"));
    assert!(remove_from_list_body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));

    let add_tel = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:AddNewTelUriContactToGroup><m:ImGroupId Id="im-group:12121212-1212-1212-1212-121212121212"/><m:DisplayName>Carol Mobile</m:DisplayName><m:TelUri>tel:+15550101</m:TelUri></m:AddNewTelUriContactToGroup></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let add_tel_body = response_text(add_tel).await;
    assert!(add_tel_body.contains("<m:AddNewTelUriContactToGroupResponse>"));
    assert!(add_tel_body.contains("<t:MemberKind>tel_uri</t:MemberKind>"));

    let list = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetImItemList /></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let list_body = response_text(list).await;
    assert!(list_body.contains("<m:GetImItemListResponse>"));
    assert!(list_body.contains("<t:DisplayName>Platform</t:DisplayName>"));
    assert!(list_body.contains("im-member:contact:22222222-2222-2222-2222-222222222222"));
    assert!(list_body.contains("tel:+15550101"));

    let items = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetImItems /></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let items_body = response_text(items).await;
    assert!(items_body.contains("<m:GetImItemsResponse>"));
    assert!(items_body.contains("<t:MemberKind>contact</t:MemberKind>"));

    let remove_contact = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:RemoveImContactFromGroup><m:ImGroupId Id="im-group:12121212-1212-1212-1212-121212121212"/><m:ContactId Id="contact:22222222-2222-2222-2222-222222222222"/></m:RemoveImContactFromGroup></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let remove_contact_body = response_text(remove_contact).await;
    assert!(remove_contact_body.contains("<m:RemoveImContactFromGroupResponse>"));
    assert!(remove_contact_body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(!members
        .lock()
        .unwrap()
        .iter()
        .any(|member| member.contact_id == Some(contact_id)));

    let remove_group = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:RemoveImGroup><m:ImGroupId Id="im-group:12121212-1212-1212-1212-121212121212"/></m:RemoveImGroup></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let remove_group_body = response_text(remove_group).await;
    assert!(remove_group_body.contains("<m:RemoveImGroupResponse>"));
    assert!(remove_group_body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(!groups
        .lock()
        .unwrap()
        .iter()
        .any(|group| group.id == group_id));
}

#[tokio::test]
async fn ucs_distribution_list_membership_stays_tenant_scoped() {
    let group_id = Uuid::parse_str("12121212-1212-1212-1212-121212121212").unwrap();
    let visible_dl_id = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap();
    let foreign_dl_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    let foreign_tenant_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ews_im_groups: Arc::new(Mutex::new(vec![EwsImGroup {
            id: group_id,
            display_name: "Platform".to_string(),
            modseq: 1,
        }])),
        extra_address_book_entries: Arc::new(Mutex::new(vec![
            ExchangeAddressBookEntry {
                id: visible_dl_id,
                display_name: "Visible DL".to_string(),
                email: "visible-dl@example.test".to_string(),
                entry_kind: ExchangeAddressBookEntryKind::DistributionList,
                directory_kind: ExchangeAddressBookDirectoryKind::Person,
                member_emails: vec!["bob@example.test".to_string()],
                details: ExchangeAddressBookEntryDetails::default(),
            },
            ExchangeAddressBookEntry {
                id: foreign_dl_id,
                display_name: "Foreign DL".to_string(),
                email: "foreign-dl@other.test".to_string(),
                entry_kind: ExchangeAddressBookEntryKind::DistributionList,
                directory_kind: ExchangeAddressBookDirectoryKind::Person,
                member_emails: vec!["mallory@other.test".to_string()],
                details: ExchangeAddressBookEntryDetails::default(),
            },
        ])),
        extra_address_book_entry_tenants: Arc::new(Mutex::new(HashMap::from([(
            foreign_dl_id,
            foreign_tenant_id,
        )]))),
        ..Default::default()
    };
    let members = store.ews_im_group_members.clone();
    let service = ExchangeService::new(store);

    let add_visible = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:AddDistributionGroupToImList><m:ImGroupId Id="im-group:12121212-1212-1212-1212-121212121212"/><m:SmtpAddress>visible-dl@example.test</m:SmtpAddress></m:AddDistributionGroupToImList></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let add_visible_body = response_text(add_visible).await;
    assert!(add_visible_body.contains("<m:AddDistributionGroupToImListResponse>"));
    assert!(add_visible_body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert_eq!(members.lock().unwrap().len(), 1);

    let add_foreign = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:AddDistributionGroupToImList><m:ImGroupId Id="im-group:12121212-1212-1212-1212-121212121212"/><m:SmtpAddress>foreign-dl@other.test</m:SmtpAddress></m:AddDistributionGroupToImList></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let add_foreign_body = response_text(add_foreign).await;
    assert!(add_foreign_body.contains("<m:AddDistributionGroupToImListResponse>"));
    assert!(add_foreign_body.contains("ResponseClass=\"Error\""));
    assert!(add_foreign_body.contains("distribution list not found"));
    assert_eq!(members.lock().unwrap().len(), 1);

    let remove_visible = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:RemoveDistributionGroupFromImList><m:SmtpAddress>visible-dl@example.test</m:SmtpAddress></m:RemoveDistributionGroupFromImList></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let remove_visible_body = response_text(remove_visible).await;
    assert!(remove_visible_body.contains("<m:RemoveDistributionGroupFromImListResponse>"));
    assert!(remove_visible_body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(members.lock().unwrap().is_empty());
}

#[tokio::test]
async fn message_tracking_reports_project_canonical_trace_state() {
    let account = FakeStore::account();
    let report_id = "aaaaaaaa-1111-2222-3333-444444444444";
    let store = FakeStore {
        session: Some(account.clone()),
        ews_message_tracking_reports: Arc::new(Mutex::new(vec![FakeMessageTrackingReport {
            tenant_id: account.tenant_id,
            report: EwsMessageTrackingReport {
                report_id: report_id.to_string(),
                account_id: account.account_id,
                sender: "alice@example.test".to_string(),
                recipients: vec!["bob@example.test".to_string()],
                subject: "Quarterly trace".to_string(),
                submitted_at: "2026-06-02T10:15:00Z".to_string(),
                status: "relayed".to_string(),
                trace_id: Some("lpe-ct-out-trace-1".to_string()),
                remote_message_ref: Some("remote-123".to_string()),
            },
        }])),
        ews_message_tracking_events: Arc::new(Mutex::new(HashMap::from([(
            report_id.to_string(),
            vec![
                EwsMessageTrackingEvent {
                    event_source: "lpe".to_string(),
                    event_kind: "handed_off".to_string(),
                    recipient_address: None,
                    recipient_is_protected: false,
                    timestamp: "2026-06-02T10:16:00Z".to_string(),
                    dsn_json: "{}".to_string(),
                },
                EwsMessageTrackingEvent {
                    event_source: "lpe-ct".to_string(),
                    event_kind: "relayed".to_string(),
                    recipient_address: Some("bob@example.test".to_string()),
                    recipient_is_protected: false,
                    timestamp: "2026-06-02T10:17:00Z".to_string(),
                    dsn_json: "{\"status\":\"2.0.0\"}".to_string(),
                },
                // [MS-OXWSMTRK] section 3.1.4.2: preserve the CT event but
                // never project canonical protected Bcc metadata to EWS.
                EwsMessageTrackingEvent {
                    event_source: "lpe-ct".to_string(),
                    event_kind: "relayed_bcc".to_string(),
                    recipient_address: Some("bcc@example.test".to_string()),
                    recipient_is_protected: true,
                    timestamp: "2026-06-02T10:18:00Z".to_string(),
                    dsn_json: "{\"recipient\":\"bcc@example.test\"}".to_string(),
                },
            ],
        )]))),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let find_response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:FindMessageTrackingReport><m:Subject>Quarterly</m:Subject></m:FindMessageTrackingReport></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(find_response.status(), StatusCode::OK);
    let find_body = response_text(find_response).await;
    assert!(find_body.contains("<m:FindMessageTrackingReportResponse>"));
    assert!(find_body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(find_body.contains("<t:MessageTrackingReportId>aaaaaaaa-1111-2222-3333-444444444444</t:MessageTrackingReportId>"));
    assert!(find_body.contains("<t:Sender>alice@example.test</t:Sender>"));
    assert!(find_body.contains("<t:SmtpAddress>bob@example.test</t:SmtpAddress>"));
    assert!(find_body.contains("<t:Subject>Quarterly trace</t:Subject>"));
    assert!(find_body.contains("<t:TraceId>lpe-ct-out-trace-1</t:TraceId>"));
    assert!(find_body.contains("<t:RemoteMessageReference>remote-123</t:RemoteMessageReference>"));

    let get_request = format!(
        concat!(
            "<s:Envelope><s:Body><m:GetMessageTrackingReport>",
            "<m:MessageTrackingReportId>{report_id}</m:MessageTrackingReportId>",
            "</m:GetMessageTrackingReport></s:Body></s:Envelope>"
        ),
        report_id = report_id
    );
    let get_response = service
        .handle(&bearer_headers(), get_request.as_bytes())
        .await
        .unwrap();

    assert_eq!(get_response.status(), StatusCode::OK);
    let get_body = response_text(get_response).await;
    assert!(get_body.contains("<m:GetMessageTrackingReportResponse>"));
    assert!(get_body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(get_body.contains("<t:RecipientTrackingEvents>"));
    assert!(get_body.contains("<t:EventDescription>handed_off</t:EventDescription>"));
    assert!(get_body.contains("<t:EventData>lpe</t:EventData>"));
    assert!(get_body.contains("<t:EventDescription>relayed</t:EventDescription>"));
    assert!(get_body.contains("<t:RecipientAddress>bob@example.test</t:RecipientAddress>"));
    assert!(get_body.contains("<t:EventDescription>relayed_bcc</t:EventDescription>"));
    assert!(!get_body.contains("bcc@example.test"));
}

#[tokio::test]
async fn message_tracking_reports_do_not_cross_tenant_boundaries() {
    let account = FakeStore::account();
    let foreign_tenant = Uuid::from_u128(0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb);
    let report_id = "foreign-report";
    let store = FakeStore {
        session: Some(account.clone()),
        ews_message_tracking_reports: Arc::new(Mutex::new(vec![FakeMessageTrackingReport {
            tenant_id: foreign_tenant,
            report: EwsMessageTrackingReport {
                report_id: report_id.to_string(),
                account_id: account.account_id,
                sender: "mallory@example.test".to_string(),
                recipients: vec!["victim@example.test".to_string()],
                subject: "Foreign trace".to_string(),
                submitted_at: "2026-06-02T10:15:00Z".to_string(),
                status: "relayed".to_string(),
                trace_id: Some("foreign-trace".to_string()),
                remote_message_ref: None,
            },
        }])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let find_response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:FindMessageTrackingReport><m:TraceId>foreign-trace</m:TraceId></m:FindMessageTrackingReport></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let find_body = response_text(find_response).await;
    assert!(find_body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(!find_body.contains("foreign-trace"));
    assert!(!find_body.contains("mallory@example.test"));

    let get_request = format!(
        concat!(
            "<s:Envelope><s:Body><m:GetMessageTrackingReport>",
            "<m:MessageTrackingReportId>{report_id}</m:MessageTrackingReportId>",
            "</m:GetMessageTrackingReport></s:Body></s:Envelope>"
        ),
        report_id = report_id
    );
    let get_response = service
        .handle(&bearer_headers(), get_request.as_bytes())
        .await
        .unwrap();
    let get_body = response_text(get_response).await;
    assert!(get_body.contains("<m:GetMessageTrackingReportResponse>"));
    assert!(get_body.contains("<m:ResponseCode>ErrorItemNotFound</m:ResponseCode>"));
    assert!(!get_body.contains("mallory@example.test"));
}

#[tokio::test]
async fn ediscovery_configuration_and_searchable_mailboxes_project_canonical_compliance_state() {
    let alice = FakeStore::account();
    let bob = AuthenticatedAccount {
        tenant_id: alice.tenant_id,
        account_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
        email: "bob@example.test".to_string(),
        display_name: "Bob".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let foreign = AuthenticatedAccount {
        tenant_id: Uuid::from_u128(0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb),
        account_id: Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap(),
        email: "mallory@example.test".to_string(),
        display_name: "Mallory".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let store = FakeStore {
        session: Some(alice),
        directory_accounts: Arc::new(Mutex::new(vec![bob, foreign])),
        ews_discovery_search_configs: Arc::new(Mutex::new(vec![EwsDiscoverySearchConfig {
            id: Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap(),
            display_name: "Case Alpha".to_string(),
            query_text: "subject:alpha".to_string(),
            updated_at: "2026-06-02T00:00:00Z".to_string(),
        }])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages">
              <s:Body><m:GetDiscoverySearchConfiguration /></s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:GetDiscoverySearchConfigurationResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:SearchName>Case Alpha</t:SearchName>"));
    assert!(body.contains("<t:SearchQuery>subject:alpha</t:SearchQuery>"));

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages">
              <s:Body><m:GetSearchableMailboxes /></s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:GetSearchableMailboxesResponse>"));
    assert!(body.contains("<t:PrimarySmtpAddress>alice@example.test</t:PrimarySmtpAddress>"));
    assert!(body.contains("<t:PrimarySmtpAddress>bob@example.test</t:PrimarySmtpAddress>"));
    assert!(!body.contains("mallory@example.test"));
}

#[tokio::test]
async fn search_mailboxes_records_canonical_discovery_search_results_without_bcc() {
    let mut email = FakeStore::email(
        "11111111-1111-1111-1111-111111111111",
        "44444444-4444-4444-4444-444444444444",
        "inbox",
        "Alpha investigation",
    );
    email.preview = "Alpha body preview".to_string();
    email.body_text = "Alpha body with searchable terms".to_string();
    email.bcc.push(JmapEmailAddress {
        display_name: Some("Hidden".to_string()),
        address: "hidden@example.test".to_string(),
    });
    let results = Arc::new(Mutex::new(Vec::new()));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        emails: Arc::new(Mutex::new(vec![email])),
        ews_discovery_search_results: results.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:SearchMailboxes>
                  <m:SearchQueries>
                    <t:MailboxQuery>
                      <t:Query>Alpha</t:Query>
                      <t:MailboxSearchScopes>
                        <t:MailboxSearchScope><t:Mailbox>alice@example.test</t:Mailbox></t:MailboxSearchScope>
                      </t:MailboxSearchScopes>
                    </t:MailboxQuery>
                  </m:SearchQueries>
                </m:SearchMailboxes>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:SearchMailboxesResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<m:ResultCount>1</m:ResultCount>"));
    assert!(body.contains("<t:Subject>Alpha investigation</t:Subject>"));
    assert!(!body.contains("hidden@example.test"));
    let results = results.lock().unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].result_count, 1);
}

#[tokio::test]
async fn hold_operations_use_canonical_compliance_hold_state() {
    let alice = FakeStore::account();
    let bob = AuthenticatedAccount {
        tenant_id: alice.tenant_id,
        account_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
        email: "bob@example.test".to_string(),
        display_name: "Bob".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let holds = Arc::new(Mutex::new(Vec::new()));
    let store = FakeStore {
        session: Some(alice),
        directory_accounts: Arc::new(Mutex::new(vec![bob])),
        ews_holds: holds.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:SetHoldOnMailboxes>
                  <m:Action>CreateHold</m:Action>
                  <m:HoldId>case-alpha</m:HoldId>
                  <m:Query>Alpha</m:Query>
                  <m:Mailboxes><t:Mailbox><t:EmailAddress>bob@example.test</t:EmailAddress></t:Mailbox></m:Mailboxes>
                </m:SetHoldOnMailboxes>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:SetHoldOnMailboxesResponse>"));
    assert!(body.contains("<m:Action>CreateHold</m:Action>"));
    assert!(body.contains("<t:Mailbox>bob@example.test</t:Mailbox>"));
    assert!(body.contains("<t:IsOnHold>true</t:IsOnHold>"));
    assert_eq!(holds.lock().unwrap().len(), 1);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:GetHoldOnMailboxes>
                  <m:Mailboxes><t:Mailbox><t:EmailAddress>bob@example.test</t:EmailAddress></t:Mailbox></m:Mailboxes>
                </m:GetHoldOnMailboxes>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:GetHoldOnMailboxesResponse>"));
    assert!(body.contains("<t:HoldName>case-alpha</t:HoldName>"));
    assert!(body.contains("<t:Query>Alpha</t:Query>"));
}

#[tokio::test]
async fn non_indexable_reports_project_canonical_search_diagnostics() {
    let alice = FakeStore::account();
    let foreign_account_id = Uuid::parse_str("ffffffff-ffff-ffff-ffff-ffffffffffff").unwrap();
    let store = FakeStore {
        session: Some(alice.clone()),
        ews_non_indexable_reports: Arc::new(Mutex::new(vec![
            EwsNonIndexableReport {
                id: Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap(),
                account_id: alice.account_id,
                email: alice.email,
                report_kind: "attachment".to_string(),
                reason: "unsupported attachment type".to_string(),
                message_id: Some(Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap()),
                attachment_id: Some(
                    Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap(),
                ),
                detected_at: "2026-06-02T00:00:00Z".to_string(),
                resolved: false,
            },
            EwsNonIndexableReport {
                id: Uuid::parse_str("44444444-4444-4444-4444-444444444444").unwrap(),
                account_id: foreign_account_id,
                email: "mallory@example.test".to_string(),
                report_kind: "message".to_string(),
                reason: "foreign".to_string(),
                message_id: Some(Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap()),
                attachment_id: None,
                detected_at: "2026-06-02T00:00:00Z".to_string(),
                resolved: false,
            },
        ])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetNonIndexableItemDetails /></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:GetNonIndexableItemDetailsResponse>"));
    assert!(body.contains("unsupported attachment type"));
    assert!(!body.contains("mallory@example.test"));

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetNonIndexableItemStatistics /></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:GetNonIndexableItemStatisticsResponse>"));
    assert!(body.contains("<t:Mailbox>alice@example.test</t:Mailbox>"));
    assert!(body.contains("<t:ItemCount>1</t:ItemCount>"));
}

#[tokio::test]
async fn bulk_transfer_operations_record_canonical_transfer_jobs() {
    let jobs = Arc::new(Mutex::new(Vec::new()));
    let mailbox_id = "55555555-5555-5555-5555-555555555555";
    let message_id = "11111111-1111-1111-1111-111111111111";
    let emails = Arc::new(Mutex::new(vec![FakeStore::email(
        message_id,
        mailbox_id,
        "inbox",
        "Exported item",
    )]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ews_transfer_jobs: jobs.clone(),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            mailbox_id,
            "inbox",
            "Inbox",
        )])),
        emails: emails.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new_with_validator(
        store,
        Validator::new(FakeDetector::rfc822(), 0.8),
    );

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"
                <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
                  <s:Body><m:ExportItems><m:ItemIds><t:ItemId Id="message:{message_id}"/></m:ItemIds></m:ExportItems></s:Body>
                </s:Envelope>
                "#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:ExportItemsResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains(&format!("<m:ItemId Id=\"message:{message_id}\"/>")));
    assert!(body.contains("<m:Data>"));

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:UploadItems>
                  <m:Items><t:Item>
                    <t:ParentFolderId><t:FolderId Id="mailbox:55555555-5555-5555-5555-555555555555"/></t:ParentFolderId>
                    <t:Data>RnJvbTogaW1wb3J0QGV4YW1wbGUudGVzdA0KVG86IGFsaWNlQGV4YW1wbGUudGVzdA0KU3ViamVjdDogSW1wb3J0ZWQgaXRlbQ0KDQpJbXBvcnQgYm9keQ==</t:Data>
                  </t:Item></m:Items>
                </m:UploadItems>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:UploadItemsResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<m:ItemId Id=\"message:99999999-9999-9999-9999-999999999999\"/>"));
    assert_eq!(emails.lock().unwrap().len(), 2);
    assert_eq!(emails.lock().unwrap()[1].subject, "Imported item");

    let jobs = jobs.lock().unwrap();
    assert_eq!(jobs.len(), 2);
    assert_eq!(jobs[0].direction, "export");
    assert_eq!(jobs[1].direction, "import");
}

#[tokio::test]
async fn bulk_transfer_rejects_arbitrary_packages_without_side_effects() {
    let jobs = Arc::new(Mutex::new(Vec::new()));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ews_transfer_jobs: jobs.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:UploadItems>
                  <m:Items><t:Item><t:Subject>Unsupported package</t:Subject></t:Item></m:Items>
                </m:UploadItems>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:UploadItemsResponse>"));
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert!(body.contains("UploadItems requires one ParentFolderId per item"));
    assert!(jobs.lock().unwrap().is_empty());

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:ExportItems /></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:ExportItemsResponse>"));
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert!(body.contains("ExportItems requires between one and 100 canonical ItemId values"));
    assert!(jobs.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mail_app_operations_use_canonical_catalog_install_and_token_state() {
    let account = FakeStore::account();
    let catalog_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeee00000001").unwrap();
    let other_tenant_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    let store = FakeStore {
        session: Some(account.clone()),
        ews_mail_app_manifests: Arc::new(Mutex::new(vec![
            FakeMailAppManifest {
                tenant_id: account.tenant_id,
                manifest: EwsMailAppManifest {
                    catalog_id,
                    app_id: "contoso-action".to_string(),
                    display_name: "Contoso Action".to_string(),
                    manifest_xml: "<OfficeApp><Id>contoso-action</Id></OfficeApp>".to_string(),
                    provider_name: "Contoso".to_string(),
                    version: "1.0.0".to_string(),
                    installation_status: None,
                },
            },
            FakeMailAppManifest {
                tenant_id: other_tenant_id,
                manifest: EwsMailAppManifest {
                    catalog_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-000000000001").unwrap(),
                    app_id: "other-tenant-app".to_string(),
                    display_name: "Other Tenant App".to_string(),
                    manifest_xml: "<OfficeApp><Id>other-tenant-app</Id></OfficeApp>".to_string(),
                    provider_name: "Other".to_string(),
                    version: "1.0.0".to_string(),
                    installation_status: None,
                },
            },
        ])),
        ews_app_marketplace_policy: Arc::new(Mutex::new(EwsAppMarketplacePolicy {
            enabled: false,
            url: None,
        })),
        ..Default::default()
    };
    let service = ExchangeService::new(store.clone());

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetAppManifests /></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:GetAppManifestsResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:AppId>contoso-action</t:AppId>"));
    assert!(body.contains("&lt;OfficeApp&gt;&lt;Id&gt;contoso-action&lt;/Id&gt;&lt;/OfficeApp&gt;"));
    assert!(!body.contains("other-tenant-app"));

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetAppMarketplaceUrl /></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:GetAppMarketplaceUrlResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(body.contains("Exchange marketplace federation is not enabled"));

    *store.ews_app_marketplace_policy.lock().unwrap() = EwsAppMarketplacePolicy {
        enabled: true,
        url: Some("https://apps.example.test/catalog".to_string()),
    };
    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetAppMarketplaceUrl /></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body
        .contains("<m:AppMarketplaceUrl>https://apps.example.test/catalog</m:AppMarketplaceUrl>"));

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:InstallApp><m:AppId>contoso-action</m:AppId></m:InstallApp></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:InstallAppResponse>"));
    assert!(body.contains("<m:Status>installed</m:Status>"));
    assert_eq!(
        store.ews_mail_app_installations.lock().unwrap()[0].status,
        "installed"
    );

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetClientAccessToken><m:AppId>contoso-action</m:AppId><m:TokenScope>ews</m:TokenScope></m:GetClientAccessToken></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:GetClientAccessTokenResponse>"));
    assert!(body.contains("<t:TokenValue>ews-app-token:"));
    assert!(body.contains("<t:Scope>ews</t:Scope>"));
    assert_eq!(store.ews_mail_app_token_events.lock().unwrap().len(), 1);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:DisableApp><m:AppId>contoso-action</m:AppId></m:DisableApp></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:DisableAppResponse>"));
    assert!(body.contains("<m:Status>disabled</m:Status>"));

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:UninstallApp><m:AppId>contoso-action</m:AppId></m:UninstallApp></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:UninstallAppResponse>"));
    assert!(body.contains("<m:Status>uninstalled</m:Status>"));
    assert!(store.ews_mail_app_token_events.lock().unwrap().is_empty());
}

#[tokio::test]
async fn unified_messaging_operations_use_canonical_call_state() {
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        ..Default::default()
    };
    let service = ExchangeService::new(store.clone());

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope><s:Body>
              <m:PlayOnPhone>
                <m:DialString>+15551234567</m:DialString>
                <m:ItemId><t:ItemId Id="message:11111111-2222-3333-4444-555555555555"/></m:ItemId>
              </m:PlayOnPhone>
            </s:Body></s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:PlayOnPhoneResponse>"));
    assert!(body.contains("<t:PhoneCallId>ews-call-1</t:PhoneCallId>"));
    assert!(body.contains("<t:CallState>requested</t:CallState>"));
    assert!(body.contains("<t:PhoneNumber>+15551234567</t:PhoneNumber>"));

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetPhoneCallInformation><m:PhoneCallId>ews-call-1</m:PhoneCallId></m:GetPhoneCallInformation></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:GetPhoneCallInformationResponse>"));
    assert!(body.contains("<t:CallState>requested</t:CallState>"));

    let other_account = AuthenticatedAccount {
        tenant_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
        account_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-000000000001").unwrap(),
        email: "mallory@other.test".to_string(),
        display_name: "Mallory".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let mut other_store = store.clone();
    other_store.session = Some(other_account);
    let other_service = ExchangeService::new(other_store);
    let response = other_service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetPhoneCallInformation><m:PhoneCallId>ews-call-1</m:PhoneCallId></m:GetPhoneCallInformation></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:GetPhoneCallInformationResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(body.contains("<m:ResponseCode>ErrorItemNotFound</m:ResponseCode>"));

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:DisconnectPhoneCall><m:PhoneCallId>ews-call-1</m:PhoneCallId></m:DisconnectPhoneCall></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:DisconnectPhoneCallResponse>"));
    assert!(body.contains("<t:CallState>cancelled</t:CallState>"));
    assert_eq!(
        store.ews_unified_messaging_calls.lock().unwrap()[0]
            .call
            .status,
        "cancelled"
    );
}

#[tokio::test]
async fn find_conversation_groups_messages_by_canonical_thread_in_folder() {
    let inbox_id = "44444444-4444-4444-4444-444444444444";
    let archive_id = "55555555-5555-5555-5555-555555555555";
    let thread_id = Uuid::parse_str("aaaaaaaa-1111-1111-1111-111111111111").unwrap();
    let other_thread_id = Uuid::parse_str("bbbbbbbb-2222-2222-2222-222222222222").unwrap();
    let mut first = FakeStore::email(
        "11111111-1111-1111-1111-111111111111",
        inbox_id,
        "inbox",
        "Quarterly planning",
    );
    first.thread_id = thread_id;
    first.unread = true;
    first.received_at = "2026-05-01T09:00:00Z".to_string();
    first.size_octets = 120;
    let mut reply = FakeStore::email(
        "22222222-2222-2222-2222-222222222222",
        inbox_id,
        "inbox",
        "RE: Quarterly planning",
    );
    reply.thread_id = thread_id;
    reply.received_at = "2026-05-01T10:00:00Z".to_string();
    reply.has_attachments = true;
    reply.size_octets = 250;
    let mut other = FakeStore::email(
        "33333333-3333-3333-3333-333333333333",
        inbox_id,
        "inbox",
        "Budget",
    );
    other.thread_id = other_thread_id;
    let mut archived = FakeStore::email(
        "44444444-4444-4444-4444-444444444444",
        archive_id,
        "archive",
        "Quarterly planning",
    );
    archived.thread_id = thread_id;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(inbox_id, "inbox", "Inbox"),
            FakeStore::mailbox(archive_id, "archive", "Archive"),
        ])),
        emails: Arc::new(Mutex::new(vec![first, reply, other, archived])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:FindConversation>
                  <m:IndexedPageItemView BasePoint="Beginning" MaxEntriesReturned="10" Offset="0"/>
                  <m:ParentFolderId><t:DistinguishedFolderId Id="inbox"/></m:ParentFolderId>
                </m:FindConversation>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:FindConversationResponse ResponseClass=\"Success\">"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert_eq!(body.matches("<t:ConversationId").count(), 2);
    assert!(body
        .contains("<t:ConversationId Id=\"conversation:aaaaaaaa-1111-1111-1111-111111111111\"/>"));
    assert!(body.contains("<t:MessageCount>2</t:MessageCount>"));
    assert!(body.contains("<t:UnreadCount>1</t:UnreadCount>"));
    assert!(body.contains("<t:HasAttachments>true</t:HasAttachments>"));
    assert!(body.contains("<t:Size>370</t:Size>"));
    assert!(body.contains("<t:ConversationTopic>Quarterly planning</t:ConversationTopic>"));
    assert!(!body.contains("44444444-4444-4444-4444-444444444444"));
}

#[tokio::test]
async fn get_conversation_items_returns_current_canonical_thread_nodes() {
    let inbox_id = "44444444-4444-4444-4444-444444444444";
    let trash_id = "55555555-5555-5555-5555-555555555555";
    let thread_id = Uuid::parse_str("aaaaaaaa-1111-1111-1111-111111111111").unwrap();
    let mut inbox = FakeStore::email(
        "11111111-1111-1111-1111-111111111111",
        inbox_id,
        "inbox",
        "Quarterly planning",
    );
    inbox.thread_id = thread_id;
    inbox.internet_message_id = Some("<planning-1@example.test>".to_string());
    let mut trash = FakeStore::email(
        "22222222-2222-2222-2222-222222222222",
        trash_id,
        "trash",
        "RE: Quarterly planning",
    );
    trash.thread_id = thread_id;
    trash.internet_message_id = Some("<planning-2@example.test>".to_string());
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(inbox_id, "inbox", "Inbox"),
            FakeStore::mailbox(trash_id, "trash", "Deleted Items"),
        ])),
        emails: Arc::new(Mutex::new(vec![inbox, trash])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:GetConversationItems>
                  <m:FoldersToIgnore><t:DistinguishedFolderId Id="deleteditems"/></m:FoldersToIgnore>
                  <m:SortOrder>TreeOrderAscending</m:SortOrder>
                  <m:Conversations>
                    <t:Conversation><t:ConversationId Id="conversation:aaaaaaaa-1111-1111-1111-111111111111"/></t:Conversation>
                  </m:Conversations>
                </m:GetConversationItems>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:GetConversationItemsResponse>"));
    assert!(body.contains("<m:GetConversationItemsResponseMessage ResponseClass=\"Success\">"));
    assert!(body.contains("<t:ConversationNodes>"));
    assert!(
        body.contains("<t:InternetMessageId>&lt;planning-1@example.test&gt;</t:InternetMessageId>")
    );
    assert!(body.contains("<t:ItemId Id=\"message:11111111-1111-1111-1111-111111111111\""));
    assert!(!body.contains("22222222-2222-2222-2222-222222222222"));
}

#[tokio::test]
async fn apply_conversation_action_moves_current_thread_messages() {
    let inbox_id = "44444444-4444-4444-4444-444444444444";
    let archive_id = "55555555-5555-5555-5555-555555555555";
    let thread_id = Uuid::parse_str("aaaaaaaa-1111-1111-1111-111111111111").unwrap();
    let mut first = FakeStore::email(
        "11111111-1111-1111-1111-111111111111",
        inbox_id,
        "inbox",
        "Quarterly planning",
    );
    first.thread_id = thread_id;
    let mut reply = FakeStore::email(
        "22222222-2222-2222-2222-222222222222",
        inbox_id,
        "inbox",
        "RE: Quarterly planning",
    );
    reply.thread_id = thread_id;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(inbox_id, "inbox", "Inbox"),
            FakeStore::mailbox(archive_id, "archive", "Archive"),
        ])),
        emails: Arc::new(Mutex::new(vec![first, reply])),
        ..Default::default()
    };
    let moved_emails = store.moved_emails.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:ApplyConversationAction>
                  <m:ConversationActions>
                    <t:ConversationAction>
                      <t:Action>Move</t:Action>
                      <t:ConversationId Id="conversation:aaaaaaaa-1111-1111-1111-111111111111"/>
                      <t:DestinationFolderId><t:FolderId Id="mailbox:55555555-5555-5555-5555-555555555555"/></t:DestinationFolderId>
                    </t:ConversationAction>
                  </m:ConversationActions>
                </m:ApplyConversationAction>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:ApplyConversationActionResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert_eq!(
        moved_emails.lock().unwrap().as_slice(),
        &[
            (
                Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap(),
                Uuid::parse_str(archive_id).unwrap()
            ),
            (
                Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap(),
                Uuid::parse_str(archive_id).unwrap()
            )
        ]
    );
}

#[tokio::test]
async fn apply_conversation_action_sets_current_thread_read_state() {
    let inbox_id = "44444444-4444-4444-4444-444444444444";
    let thread_id = Uuid::parse_str("aaaaaaaa-1111-1111-1111-111111111111").unwrap();
    let mut first = FakeStore::email(
        "11111111-1111-1111-1111-111111111111",
        inbox_id,
        "inbox",
        "Quarterly planning",
    );
    first.thread_id = thread_id;
    first.unread = true;
    let mut reply = FakeStore::email(
        "22222222-2222-2222-2222-222222222222",
        inbox_id,
        "inbox",
        "RE: Quarterly planning",
    );
    reply.thread_id = thread_id;
    reply.unread = true;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        emails: Arc::new(Mutex::new(vec![first, reply])),
        ..Default::default()
    };
    let emails = store.emails.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:ApplyConversationAction>
                  <m:ConversationActions>
                    <t:ConversationAction>
                      <t:Action>SetReadState</t:Action>
                      <t:ConversationId Id="conversation:aaaaaaaa-1111-1111-1111-111111111111"/>
                      <t:Read>true</t:Read>
                    </t:ConversationAction>
                  </m:ConversationActions>
                </m:ApplyConversationAction>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(emails.lock().unwrap().iter().all(|email| !email.unread));
}

#[tokio::test]
async fn apply_conversation_action_keeps_future_message_rules_parseable() {
    let inbox_id = "44444444-4444-4444-4444-444444444444";
    let archive_id = "55555555-5555-5555-5555-555555555555";
    let thread_id = Uuid::parse_str("aaaaaaaa-1111-1111-1111-111111111111").unwrap();
    let mut first = FakeStore::email(
        "11111111-1111-1111-1111-111111111111",
        inbox_id,
        "inbox",
        "Quarterly planning",
    );
    first.thread_id = thread_id;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(inbox_id, "inbox", "Inbox"),
            FakeStore::mailbox(archive_id, "archive", "Archive"),
        ])),
        emails: Arc::new(Mutex::new(vec![first])),
        ..Default::default()
    };
    let moved_emails = store.moved_emails.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:ApplyConversationAction>
                  <m:ConversationActions>
                    <t:ConversationAction>
                      <t:Action>AlwaysMove</t:Action>
                      <t:ConversationId Id="conversation:aaaaaaaa-1111-1111-1111-111111111111"/>
                      <t:DestinationFolderId><t:FolderId Id="mailbox:55555555-5555-5555-5555-555555555555"/></t:DestinationFolderId>
                    </t:ConversationAction>
                  </m:ConversationActions>
                </m:ApplyConversationAction>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:ApplyConversationActionResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert!(body.contains("Persistent future-message conversation actions are not supported"));
    assert!(moved_emails.lock().unwrap().is_empty());
}

#[tokio::test]
async fn apply_conversation_action_rejects_persistent_batch_without_mutation() {
    let inbox_id = "44444444-4444-4444-4444-444444444444";
    let archive_id = "55555555-5555-5555-5555-555555555555";
    let thread_id = Uuid::parse_str("aaaaaaaa-1111-1111-1111-111111111111").unwrap();
    let mut email = FakeStore::email(
        "11111111-1111-1111-1111-111111111111",
        inbox_id,
        "inbox",
        "Quarterly planning",
    );
    email.thread_id = thread_id;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(inbox_id, "inbox", "Inbox"),
            FakeStore::mailbox(archive_id, "archive", "Archive"),
        ])),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    let moved_emails = store.moved_emails.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:ApplyConversationAction>
                  <m:ConversationActions>
                    <t:ConversationAction>
                      <t:Action>Move</t:Action>
                      <t:ConversationId Id="conversation:aaaaaaaa-1111-1111-1111-111111111111"/>
                      <t:DestinationFolderId><t:FolderId Id="mailbox:55555555-5555-5555-5555-555555555555"/></t:DestinationFolderId>
                    </t:ConversationAction>
                    <t:ConversationAction>
                      <t:Action>AlwaysMove</t:Action>
                      <t:ConversationId Id="conversation:aaaaaaaa-1111-1111-1111-111111111111"/>
                      <t:DestinationFolderId><t:FolderId Id="mailbox:55555555-5555-5555-5555-555555555555"/></t:DestinationFolderId>
                    </t:ConversationAction>
                  </m:ConversationActions>
                </m:ApplyConversationAction>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:ApplyConversationActionResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert!(moved_emails.lock().unwrap().is_empty());
}

#[tokio::test]
async fn authentication_errors_return_basic_challenge() {
    let response = error_response(&anyhow::anyhow!("missing account authentication"));

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response
            .headers()
            .get("www-authenticate")
            .and_then(|value| value.to_str().ok()),
        Some("Basic realm=\"LPE EWS\"")
    );
    let body = response_text(response).await;
    assert!(body.contains("<s:Fault>"));
    assert!(body.contains("missing account authentication"));
}

#[tokio::test]
async fn sync_folder_items_returns_contacts_from_canonical_store() {
    let contact_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    let collection = FakeStore::collection("default", "contacts", "Contacts");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![collection.clone()])),
        contacts: Arc::new(Mutex::new(vec![AccessibleContact {
            id: contact_id,
            collection_id: collection.id.clone(),
            owner_account_id: collection.owner_account_id,
            owner_email: collection.owner_email.clone(),
            owner_display_name: collection.owner_display_name.clone(),
            rights: collection.rights.clone(),
            name: "Bob Example".to_string(),
            role: "Manager".to_string(),
            email: "bob@example.test".to_string(),
            phone: "+491234".to_string(),
            team: "Ops".to_string(),
            notes: "VIP".to_string(),
            ..Default::default()
        }])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:DistinguishedFolderId Id="contacts"/></m:SyncFolderId></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:SyncFolderItemsResponse>"));
    assert!(body.contains("<t:Contact>"));
    assert!(body.contains("contact:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"));
    assert!(body.contains("bob@example.test"));
}

#[tokio::test]
async fn create_delete_contact_round_trips_through_sync_folder_items() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "contacts", "Contacts",
        )])),
        ..Default::default()
    };
    let deleted_contacts = store.deleted_contacts.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:CreateItem>
                  <m:SavedItemFolderId><t:FolderId Id="default"/></m:SavedItemFolderId>
                  <m:Items>
                    <t:Contact>
                      <t:DisplayName>RCA Contact</t:DisplayName>
                      <t:GivenName>RCA</t:GivenName>
                      <t:Surname>Contact</t:Surname>
                      <t:EmailAddresses>
                        <t:Entry Key="EmailAddress1">rca@example.test</t:Entry>
                      </t:EmailAddresses>
                      <t:PhoneNumbers>
                        <t:Entry Key="MobilePhone">+41000000000</t:Entry>
                      </t:PhoneNumbers>
                      <t:CompanyName>LPE</t:CompanyName>
                      <t:JobTitle>Tester</t:JobTitle>
                      <t:Body BodyType="Text">Created by RCA</t:Body>
                    </t:Contact>
                  </m:Items>
                </m:CreateItem>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:CreateItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("contact:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"));

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:ItemShape><t:BaseShape>AllProperties</t:BaseShape></m:ItemShape><m:SyncFolderId><t:FolderId Id="default"/></m:SyncFolderId><m:SyncState>contacts:default:0</m:SyncState><m:MaxChangesReturned>10</m:MaxChangesReturned></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:Create><t:Contact>"));
    assert!(body.contains("contact:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"));
    assert!(body.contains("<t:DisplayName>RCA Contact</t:DisplayName>"));
    let sync_state = test_xml_text(&body, "SyncState").unwrap();
    assert!(sync_state.starts_with("lpe-ews-sync.v1.contacts."));
    let change_key = test_item_change_key(&body, "contact:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope><s:Body><m:UpdateItem><m:ItemChanges><t:ItemChange>
              <t:ItemId Id="contact:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"/>
              <t:Updates><t:SetItemField><t:FieldURI FieldURI="contacts:DisplayName"/>
                <t:Contact><t:DisplayName>Must Not Persist</t:DisplayName></t:Contact>
              </t:SetItemField></t:Updates>
            </t:ItemChange></m:ItemChanges></m:UpdateItem></s:Body></s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>ErrorIrresolvableConflict</m:ResponseCode>"));

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:DeleteItem DeleteType="HardDelete"><m:ItemIds><t:ItemId Id="contact:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb" ChangeKey="{change_key}"/></m:ItemIds></m:DeleteItem></s:Body></s:Envelope>"#,
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:DeleteItemResponse>"));
    assert_eq!(
        deleted_contacts.lock().unwrap().as_slice(),
        &[Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap()]
    );

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="default"/></m:SyncFolderId><m:SyncState>{sync_state}</m:SyncState></m:SyncFolderItems></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(
        body.contains("<t:Delete><t:ItemId Id=\"contact:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb\"")
    );
    assert!(test_xml_text(&body, "SyncState")
        .unwrap()
        .starts_with("lpe-ews-sync.v1.contacts."));
}

#[tokio::test]
async fn create_contact_rejects_magika_blocked_photo_before_persistence() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "contacts", "Contacts",
        )])),
        ..Default::default()
    };
    let contacts = store.contacts.clone();
    let service =
        ExchangeService::new_with_validator(store, Validator::new(FakeDetector::executable(), 0.8));

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope><s:Body><m:CreateItem>
              <m:SavedItemFolderId><t:FolderId Id="default"/></m:SavedItemFolderId>
              <m:Items><t:Contact>
                <t:DisplayName>Blocked Photo</t:DisplayName>
                <t:EmailAddresses><t:Entry Key="EmailAddress1">blocked@example.test</t:Entry></t:EmailAddresses>
                <t:Photo>aGVsbG8=</t:Photo>
              </t:Contact></m:Items>
            </m:CreateItem></s:Body></s:Envelope>
            "#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:CreateItemResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert!(contacts.lock().unwrap().is_empty());
}

#[tokio::test]
async fn create_contact_syncs_from_current_empty_rca_sync_state() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "contacts", "Contacts",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:CreateItem>
                  <m:SavedItemFolderId><t:FolderId Id="default" ChangeKey="ck-default"/></m:SavedItemFolderId>
                  <m:Items>
                    <t:Contact>
                      <t:DisplayName>RCA Contact</t:DisplayName>
                      <t:EmailAddresses>
                        <t:Entry Key="EmailAddress1">rca@example.test</t:Entry>
                      </t:EmailAddresses>
                    </t:Contact>
                  </m:Items>
                </m:CreateItem>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:ItemShape><t:BaseShape>AllProperties</t:BaseShape></m:ItemShape><m:SyncFolderId><t:FolderId Id="default" ChangeKey="ck-default"/></m:SyncFolderId><m:SyncState>contacts:default:v2:0</m:SyncState><m:MaxChangesReturned>10</m:MaxChangesReturned></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:Create><t:Contact>"));
    assert!(body.contains("contact:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"));
    assert!(body.contains("<t:DisplayName>RCA Contact</t:DisplayName>"));
    assert!(!body.contains("<m:SyncState>contacts:default:v2:0</m:SyncState>"));
}

#[tokio::test]
async fn create_contact_without_saved_folder_ignores_unrelated_folder_ids() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "contacts", "Contacts",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:CreateItem>
                  <m:Items>
                    <t:Contact>
                      <t:FolderId Id="shared-contacts-aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"/>
                      <t:DisplayName>Unscoped RCA Contact</t:DisplayName>
                      <t:EmailAddresses>
                        <t:Entry Key="EmailAddress1">unscoped@example.test</t:Entry>
                      </t:EmailAddresses>
                    </t:Contact>
                  </m:Items>
                </m:CreateItem>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="default" ChangeKey="ck-default"/></m:SyncFolderId><m:SyncState>contacts:default:v2:0</m:SyncState></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:Create><t:Contact>"));
    assert!(body.contains("<t:DisplayName>Unscoped RCA Contact</t:DisplayName>"));
}

#[tokio::test]
async fn sync_folder_items_rejects_nonempty_legacy_contact_sync_state() {
    let contact_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    let collection = FakeStore::collection("default", "contacts", "Contacts");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![collection.clone()])),
        contacts: Arc::new(Mutex::new(vec![AccessibleContact {
            id: contact_id,
            collection_id: collection.id.clone(),
            owner_account_id: collection.owner_account_id,
            owner_email: collection.owner_email.clone(),
            owner_display_name: collection.owner_display_name.clone(),
            rights: collection.rights.clone(),
            name: "Updated RCA Contact".to_string(),
            role: "Manager".to_string(),
            email: "rca@example.test".to_string(),
            phone: "+41000000000".to_string(),
            team: "LPE".to_string(),
            notes: "Changed after legacy sync state".to_string(),
            ..Default::default()
        }])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:ItemShape><t:BaseShape>AllProperties</t:BaseShape></m:ItemShape><m:SyncFolderId><t:FolderId Id="default" ChangeKey="ck-default"/></m:SyncFolderId><m:SyncState>contacts:default:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb</m:SyncState><m:MaxChangesReturned>10</m:MaxChangesReturned></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(
        body.contains("<m:ResponseClass>Error</m:ResponseClass>")
            || body.contains("ResponseClass=\"Error\"")
    );
    assert!(!body.contains("<t:Update><t:Contact>"));
}

#[tokio::test]
async fn sync_folder_items_rejects_keyed_legacy_contact_sync_state() {
    let contact_id = Uuid::parse_str("e77d919d-df4f-488d-bb4c-2defdfd8d6ec").unwrap();
    let collection = FakeStore::collection("default", "contacts", "Contacts");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![collection.clone()])),
        contacts: Arc::new(Mutex::new(vec![AccessibleContact {
            id: contact_id,
            collection_id: collection.id.clone(),
            owner_account_id: collection.owner_account_id,
            owner_email: collection.owner_email.clone(),
            owner_display_name: collection.owner_display_name.clone(),
            rights: collection.rights.clone(),
            name: "RCA Contact".to_string(),
            role: "Tester".to_string(),
            email: "rca@example.test".to_string(),
            phone: "+41000000000".to_string(),
            team: "LPE".to_string(),
            notes: "RCA sync verification".to_string(),
            ..Default::default()
        }])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:ItemShape><t:BaseShape>AllProperties</t:BaseShape></m:ItemShape><m:SyncFolderId><t:FolderId Id="default" ChangeKey="ck-default"/></m:SyncFolderId><m:SyncState>contacts:default:e77d919d-df4f-488d-bb4c-2defdfd8d6ec=ck-d21173e54e57cc77</m:SyncState><m:MaxChangesReturned>10</m:MaxChangesReturned></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(
        body.contains("<m:ResponseClass>Error</m:ResponseClass>")
            || body.contains("ResponseClass=\"Error\"")
    );
    assert!(!body.contains("<t:Update><t:Contact>"));
}

#[tokio::test]
async fn sync_folder_items_returns_no_contact_change_for_current_keyed_sync_state() {
    let contact_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    let collection = FakeStore::collection("default", "contacts", "Contacts");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![collection.clone()])),
        contacts: Arc::new(Mutex::new(vec![AccessibleContact {
            id: contact_id,
            collection_id: collection.id.clone(),
            owner_account_id: collection.owner_account_id,
            owner_email: collection.owner_email.clone(),
            owner_display_name: collection.owner_display_name.clone(),
            rights: collection.rights.clone(),
            name: "RCA Contact".to_string(),
            role: "Tester".to_string(),
            email: "rca@example.test".to_string(),
            phone: "+41000000000".to_string(),
            team: "LPE".to_string(),
            notes: "No change".to_string(),
            ..Default::default()
        }])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="default"/></m:SyncFolderId><m:SyncState>contacts:default:0</m:SyncState></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    let current_sync_state = body
        .split("<m:SyncState>")
        .nth(1)
        .and_then(|rest| rest.split("</m:SyncState>").next())
        .unwrap()
        .to_string();
    assert!(current_sync_state.starts_with("lpe-ews-sync.v1.contacts."));

    let request = format!(
        r#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="default"/></m:SyncFolderId><m:SyncState>{current_sync_state}</m:SyncState></m:SyncFolderItems></s:Body></s:Envelope>"#
    );
    let response = service
        .handle(&bearer_headers(), request.as_bytes())
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(!body.contains("<t:Create>"));
    assert!(!body.contains("<t:Update>"));
    assert!(!body.contains("<t:Delete>"));
    let next_sync_state = test_xml_text(&body, "SyncState").unwrap();
    assert!(next_sync_state.starts_with("lpe-ews-sync.v1.contacts."));
    assert_ne!(next_sync_state, current_sync_state);
}

#[tokio::test]
async fn ews_contact_change_key_is_stable_across_get_find_and_sync() {
    let contact_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    let unrelated_contact_id = Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap();
    let collection = FakeStore::collection("default", "contacts", "Contacts");
    let contact_versions = Arc::new(Mutex::new(HashMap::from([
        (contact_id, 41),
        (unrelated_contact_id, 9),
    ])));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![collection.clone()])),
        contacts: Arc::new(Mutex::new(vec![
            AccessibleContact {
                id: contact_id,
                collection_id: collection.id.clone(),
                owner_account_id: collection.owner_account_id,
                owner_email: collection.owner_email.clone(),
                owner_display_name: collection.owner_display_name.clone(),
                rights: collection.rights.clone(),
                name: "Stable Contact".to_string(),
                ..Default::default()
            },
            AccessibleContact {
                id: unrelated_contact_id,
                collection_id: collection.id.clone(),
                owner_account_id: collection.owner_account_id,
                owner_email: collection.owner_email.clone(),
                owner_display_name: collection.owner_display_name.clone(),
                rights: collection.rights.clone(),
                name: "Unrelated Contact".to_string(),
                ..Default::default()
            },
        ])),
        contact_versions: contact_versions.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let get_request = br#"<s:Envelope><s:Body><m:GetItem><m:ItemIds><t:ItemId Id="contact:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"/></m:ItemIds></m:GetItem></s:Body></s:Envelope>"#;

    let response = service
        .handle(&bearer_headers(), get_request)
        .await
        .unwrap();
    let body = response_text(response).await;
    let first_key = test_item_change_key(&body, "contact:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
    assert!(first_key.starts_with("ck-v1-"));

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:FindItem><m:ParentFolderIds><t:DistinguishedFolderId Id="contacts"/></m:ParentFolderIds></m:FindItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert_eq!(
        test_item_change_key(&body, "contact:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
        first_key
    );

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="default"/></m:SyncFolderId><m:SyncState>contacts:default:0</m:SyncState></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert_eq!(
        test_item_change_key(&body, "contact:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
        first_key
    );
    let sync_state = test_xml_text(&body, "SyncState").unwrap();

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="default"/></m:SyncFolderId><m:SyncState>{sync_state}</m:SyncState></m:SyncFolderItems></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(!body.contains("<t:Create>"));
    assert!(!body.contains("<t:Update>"));
    assert!(!body.contains("<t:Delete>"));

    contact_versions
        .lock()
        .unwrap()
        .insert(unrelated_contact_id, 10);
    let response = service
        .handle(&bearer_headers(), get_request)
        .await
        .unwrap();
    let body = response_text(response).await;
    assert_eq!(
        test_item_change_key(&body, "contact:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
        first_key
    );

    contact_versions.lock().unwrap().insert(contact_id, 42);
    let response = service
        .handle(&bearer_headers(), get_request)
        .await
        .unwrap();
    let body = response_text(response).await;
    let changed_key = test_item_change_key(&body, "contact:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
    assert_ne!(changed_key, first_key);

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="default"/></m:SyncFolderId><m:SyncState>{sync_state}</m:SyncState></m:SyncFolderItems></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:Update>"));
    assert_eq!(
        test_item_change_key(&body, "contact:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
        changed_key
    );
}

#[tokio::test]
async fn update_contact_round_trips_through_sync_folder_items() {
    let contact_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    let collection = FakeStore::collection("default", "contacts", "Contacts");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![collection.clone()])),
        contacts: Arc::new(Mutex::new(vec![AccessibleContact {
            id: contact_id,
            collection_id: collection.id.clone(),
            owner_account_id: collection.owner_account_id,
            owner_email: collection.owner_email.clone(),
            owner_display_name: collection.owner_display_name.clone(),
            rights: collection.rights.clone(),
            name: "RCA Contact".to_string(),
            role: "Tester".to_string(),
            email: "rca@example.test".to_string(),
            phone: "+41000000000".to_string(),
            team: "LPE".to_string(),
            notes: "Created by RCA".to_string(),
            ..Default::default()
        }])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="default"/></m:SyncFolderId><m:SyncState>contacts:default:0</m:SyncState></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    let old_sync_state = body
        .split("<m:SyncState>")
        .nth(1)
        .and_then(|rest| rest.split("</m:SyncState>").next())
        .unwrap()
        .to_string();
    let change_key = test_item_change_key(&body, "contact:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"
            <s:Envelope>
              <s:Body>
                <m:UpdateItem ConflictResolution="AlwaysOverwrite">
                  <m:ItemChanges>
                    <t:ItemChange>
                      <t:ItemId Id="contact:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb" ChangeKey="{change_key}"/>
                      <t:Updates>
                        <t:SetItemField>
                          <t:FieldURI FieldURI="contacts:DisplayName"/>
                          <t:Contact>
                            <t:DisplayName>Updated RCA Contact</t:DisplayName>
                            <t:JobTitle>Manager</t:JobTitle>
                            <t:Body BodyType="Text">Updated by RCA</t:Body>
                          </t:Contact>
                        </t:SetItemField>
                      </t:Updates>
                    </t:ItemChange>
                  </m:ItemChanges>
                </m:UpdateItem>
              </s:Body>
            </s:Envelope>
            "#,
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:UpdateItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:DisplayName>Updated RCA Contact</t:DisplayName>"));

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:UpdateItem><m:ItemChanges><t:ItemChange>
                  <t:ItemId Id="contact:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb" ChangeKey="{change_key}"/>
                  <t:Updates><t:SetItemField><t:FieldURI FieldURI="contacts:DisplayName"/>
                    <t:Contact><t:DisplayName>Stale Contact</t:DisplayName></t:Contact>
                  </t:SetItemField></t:Updates>
                </t:ItemChange></m:ItemChanges></m:UpdateItem></s:Body></s:Envelope>"#,
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>ErrorIrresolvableConflict</m:ResponseCode>"));

    let request = format!(
        r#"<s:Envelope><s:Body><m:SyncFolderItems><m:ItemShape><t:BaseShape>AllProperties</t:BaseShape></m:ItemShape><m:SyncFolderId><t:FolderId Id="default" ChangeKey="ck-default"/></m:SyncFolderId><m:SyncState>{old_sync_state}</m:SyncState><m:MaxChangesReturned>10</m:MaxChangesReturned></m:SyncFolderItems></s:Body></s:Envelope>"#
    );
    let response = service
        .handle(&bearer_headers(), request.as_bytes())
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:Update><t:Contact>"));
    assert!(body.contains("<t:DisplayName>Updated RCA Contact</t:DisplayName>"));
    assert!(body.contains("<t:JobTitle>Manager</t:JobTitle>"));
    assert!(body.contains("<t:Body BodyType=\"Text\">Updated by RCA</t:Body>"));
}

#[tokio::test]
async fn update_contact_unmapped_field_still_advances_sync_folder_items() {
    let contact_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    let collection = FakeStore::collection("default", "contacts", "Contacts");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![collection.clone()])),
        contacts: Arc::new(Mutex::new(vec![AccessibleContact {
            id: contact_id,
            collection_id: collection.id.clone(),
            owner_account_id: collection.owner_account_id,
            owner_email: collection.owner_email.clone(),
            owner_display_name: collection.owner_display_name.clone(),
            rights: collection.rights.clone(),
            name: "RCA Contact".to_string(),
            role: "Tester".to_string(),
            email: "rca@example.test".to_string(),
            phone: "+41000000000".to_string(),
            team: "LPE".to_string(),
            notes: "Created by RCA".to_string(),
            ..Default::default()
        }])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="default"/></m:SyncFolderId><m:SyncState>contacts:default:0</m:SyncState></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    let old_sync_state = body
        .split("<m:SyncState>")
        .nth(1)
        .and_then(|rest| rest.split("</m:SyncState>").next())
        .unwrap()
        .to_string();
    let change_key = test_item_change_key(&body, "contact:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"
            <s:Envelope>
              <s:Body>
                <m:UpdateItem ConflictResolution="AlwaysOverwrite">
                  <m:ItemChanges>
                    <t:ItemChange>
                      <t:ItemId Id="contact:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb" ChangeKey="{change_key}"/>
                      <t:Updates>
                        <t:SetItemField>
                          <t:FieldURI FieldURI="contacts:AssistantName"/>
                          <t:Contact>
                            <t:AssistantName>RCA Assistant</t:AssistantName>
                          </t:Contact>
                        </t:SetItemField>
                      </t:Updates>
                    </t:ItemChange>
                  </m:ItemChanges>
                </m:UpdateItem>
              </s:Body>
            </s:Envelope>
            "#,
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:UpdateItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));

    let request = format!(
        r#"<s:Envelope><s:Body><m:SyncFolderItems><m:ItemShape><t:BaseShape>AllProperties</t:BaseShape></m:ItemShape><m:SyncFolderId><t:FolderId Id="default" ChangeKey="ck-default"/></m:SyncFolderId><m:SyncState>{old_sync_state}</m:SyncState><m:MaxChangesReturned>10</m:MaxChangesReturned></m:SyncFolderItems></s:Body></s:Envelope>"#
    );
    let response = service
        .handle(&bearer_headers(), request.as_bytes())
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:Update><t:Contact>"));
    assert!(body.contains("<t:DisplayName>RCA Contact</t:DisplayName>"));
    assert!(!body.contains(&format!("<m:SyncState>{old_sync_state}</m:SyncState>")));
}

#[tokio::test]
async fn create_delete_calendar_item_round_trips_through_sync_folder_items() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "calendar", "Calendar",
        )])),
        ..Default::default()
    };
    let deleted_events = store.deleted_events.clone();
    let events = store.events.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Header>
                <t:TimeZoneContext><t:TimeZoneDefinition Id="UTC" /></t:TimeZoneContext>
              </s:Header>
              <s:Body>
                <m:CreateItem>
                  <m:SavedItemFolderId><t:FolderId Id="default"/></m:SavedItemFolderId>
                  <m:Items>
                    <t:CalendarItem>
                      <t:Subject>RCA Calendar</t:Subject>
                      <t:Location>Room 1</t:Location>
                      <t:Start>2026-05-04T23:30:00Z</t:Start>
                      <t:End>2026-05-05T01:30:00Z</t:End>
                      <t:Recurrence>
                        <t:WeeklyRecurrence>
                          <t:Interval>1</t:Interval>
                          <t:DaysOfWeek>Monday Wednesday</t:DaysOfWeek>
                        </t:WeeklyRecurrence>
                        <t:NumberedRecurrence>
                          <t:StartDate>2026-05-04</t:StartDate>
                          <t:NumberOfOccurrences>5</t:NumberOfOccurrences>
                        </t:NumberedRecurrence>
                      </t:Recurrence>
                      <t:RequiredAttendees>
                        <t:Attendee><t:Mailbox><t:Name>Bob</t:Name><t:EmailAddress>bob@example.test</t:EmailAddress></t:Mailbox><t:ResponseType>Accept</t:ResponseType></t:Attendee>
                      </t:RequiredAttendees>
                      <t:OptionalAttendees>
                        <t:Attendee><t:Mailbox><t:Name>Carol</t:Name><t:EmailAddress>carol@example.test</t:EmailAddress></t:Mailbox><t:ResponseType>Tentative</t:ResponseType></t:Attendee>
                      </t:OptionalAttendees>
                      <t:Body BodyType="Text">Created by RCA</t:Body>
                    </t:CalendarItem>
                  </m:Items>
                </m:CreateItem>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:CreateItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("event:cccccccc-cccc-cccc-cccc-cccccccccccc"));
    let created_events = events.lock().unwrap();
    assert_eq!(
        created_events[0].recurrence_rule,
        "FREQ=WEEKLY;BYDAY=MO,WE;COUNT=5"
    );
    assert_eq!(created_events[0].duration_minutes, 120);
    assert_eq!(created_events[0].attendees, "Bob, Carol");
    assert!(created_events[0]
        .attendees_json
        .contains("alice@example.test"));
    assert!(created_events[0]
        .attendees_json
        .contains("bob@example.test"));
    assert!(created_events[0]
        .attendees_json
        .contains("carol@example.test"));
    assert!(created_events[0].attendees_json.contains("accepted"));
    assert!(created_events[0].attendees_json.contains("tentative"));
    drop(created_events);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:ItemShape><t:BaseShape>AllProperties</t:BaseShape></m:ItemShape><m:SyncFolderId><t:FolderId Id="default"/></m:SyncFolderId><m:SyncState>calendar:default:0</m:SyncState><m:MaxChangesReturned>10</m:MaxChangesReturned></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:Create><t:CalendarItem>"));
    assert!(body.contains("event:cccccccc-cccc-cccc-cccc-cccccccccccc"));
    assert!(body.contains("<t:Subject>RCA Calendar</t:Subject>"));
    assert!(body.contains("<t:Start>2026-05-04T23:30:00Z</t:Start>"));
    assert!(body.contains("<t:End>2026-05-05T01:30:00Z</t:End>"));
    assert!(body.contains("<t:WeeklyRecurrence>"));
    assert!(body.contains("<t:DaysOfWeek>Monday Wednesday</t:DaysOfWeek>"));
    assert!(body.contains("<t:NumberOfOccurrences>5</t:NumberOfOccurrences>"));
    assert!(body.contains("<t:RequiredAttendees>"));
    assert!(body.contains("<t:OptionalAttendees>"));
    assert!(body.contains("<t:EmailAddress>bob@example.test</t:EmailAddress>"));
    assert!(body.contains("<t:EmailAddress>carol@example.test</t:EmailAddress>"));
    assert!(body.contains("<t:ResponseType>Accept</t:ResponseType>"));
    assert!(body.contains("<t:ResponseType>Tentative</t:ResponseType>"));
    let sync_state = test_xml_text(&body, "SyncState").unwrap();
    assert!(sync_state.starts_with("lpe-ews-sync.v1.calendar."));
    let change_key = test_item_change_key(&body, "event:cccccccc-cccc-cccc-cccc-cccccccccccc");

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:UpdateItem><m:ItemChanges><t:ItemChange>
              <t:ItemId Id="event:cccccccc-cccc-cccc-cccc-cccccccccccc"/>
              <t:Updates><t:SetItemField><t:FieldURI FieldURI="calendar:Subject"/>
                <t:CalendarItem><t:Subject>Must Not Persist</t:Subject></t:CalendarItem>
              </t:SetItemField></t:Updates>
            </t:ItemChange></m:ItemChanges></m:UpdateItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>ErrorIrresolvableConflict</m:ResponseCode>"));
    assert_eq!(events.lock().unwrap()[0].title, "RCA Calendar");

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:UpdateItem><m:ItemChanges><t:ItemChange>
                  <t:ItemId Id="event:cccccccc-cccc-cccc-cccc-cccccccccccc" ChangeKey="{change_key}"/>
                  <t:Updates><t:SetItemField><t:FieldURI FieldURI="calendar:Subject"/>
                    <t:CalendarItem><t:Subject>Updated RCA Calendar</t:Subject></t:CalendarItem>
                  </t:SetItemField></t:Updates>
                </t:ItemChange></m:ItemChanges></m:UpdateItem></s:Body></s:Envelope>"#,
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:Subject>Updated RCA Calendar</t:Subject>"));
    assert_eq!(events.lock().unwrap()[0].title, "Updated RCA Calendar");
    let updated_change_key =
        test_item_change_key(&body, "event:cccccccc-cccc-cccc-cccc-cccccccccccc");

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:UpdateItem><m:ItemChanges><t:ItemChange>
                  <t:ItemId Id="event:cccccccc-cccc-cccc-cccc-cccccccccccc" ChangeKey="{change_key}"/>
                  <t:Updates><t:SetItemField><t:FieldURI FieldURI="calendar:Subject"/>
                    <t:CalendarItem><t:Subject>Stale Calendar</t:Subject></t:CalendarItem>
                  </t:SetItemField></t:Updates>
                </t:ItemChange></m:ItemChanges></m:UpdateItem></s:Body></s:Envelope>"#,
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>ErrorIrresolvableConflict</m:ResponseCode>"));
    assert_eq!(events.lock().unwrap()[0].title, "Updated RCA Calendar");

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:DeleteItem DeleteType="HardDelete"><m:ItemIds><t:ItemId Id="event:cccccccc-cccc-cccc-cccc-cccccccccccc" ChangeKey="{updated_change_key}"/></m:ItemIds></m:DeleteItem></s:Body></s:Envelope>"#,
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:DeleteItemResponse>"));
    assert_eq!(
        deleted_events.lock().unwrap().as_slice(),
        &[Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap()]
    );

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="default"/></m:SyncFolderId><m:SyncState>{sync_state}</m:SyncState></m:SyncFolderItems></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:Delete><t:ItemId Id=\"event:cccccccc-cccc-cccc-cccc-cccccccccccc\""));
    assert!(test_xml_text(&body, "SyncState")
        .unwrap()
        .starts_with("lpe-ews-sync.v1.calendar."));
}

#[tokio::test]
async fn sync_folder_items_pages_calendar_changes() {
    let account = FakeStore::account();
    let event = |id: &str, title: &str| AccessibleEvent {
        id: Uuid::parse_str(id).unwrap(),
        uid: id.to_string(),
        collection_id: "default".to_string(),
        owner_account_id: account.account_id,
        owner_email: account.email.clone(),
        owner_display_name: account.display_name.clone(),
        rights: FakeStore::rights(),
        date: "2026-05-04".to_string(),
        time: "09:00".to_string(),
        time_zone: "UTC".to_string(),
        duration_minutes: 30,
        all_day: false,
        status: "confirmed".to_string(),
        sequence: 0,
        recurrence_rule: String::new(),
        recurrence_json: "{}".to_string(),
        recurrence_exceptions_json: "[]".to_string(),
        title: title.to_string(),
        location: String::new(),
        organizer_json: "{}".to_string(),
        attendees: String::new(),
        attendees_json: String::new(),
        notes: String::new(),
        body_html: String::new(),
    };
    let store = FakeStore {
        session: Some(account.clone()),
        calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "calendar", "Calendar",
        )])),
        events: Arc::new(Mutex::new(vec![
            event("11111111-1111-1111-1111-111111111111", "First event"),
            event("22222222-2222-2222-2222-222222222222", "Second event"),
        ])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="default"/></m:SyncFolderId><m:SyncState>calendar:default:v2:0</m:SyncState><m:MaxChangesReturned>1</m:MaxChangesReturned></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let first_page = response_text(response).await;
    assert!(first_page.contains("<m:IncludesLastItemInRange>false</m:IncludesLastItemInRange>"));
    assert_eq!(first_page.matches("<t:Create>").count(), 1);
    let sync_state = test_xml_text(&first_page, "SyncState").unwrap();

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="default"/></m:SyncFolderId><m:SyncState>{sync_state}</m:SyncState><m:MaxChangesReturned>1</m:MaxChangesReturned></m:SyncFolderItems></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let last_page = response_text(response).await;
    assert!(last_page.contains("<m:IncludesLastItemInRange>true</m:IncludesLastItemInRange>"));
    assert_eq!(last_page.matches("<t:Create>").count(), 1);
}

#[tokio::test]
async fn sync_folder_items_returns_empty_sync_for_custom_mailbox_folder() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "44444444-4444-4444-4444-444444444444",
            "custom",
            "RCA Sync",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="mailbox:44444444-4444-4444-4444-444444444444"/></m:SyncFolderId></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:SyncFolderItemsResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<m:SyncState>lpe-ews-sync.v1.mailbox."));
    assert!(body.contains("<m:Changes></m:Changes>"));
}

#[tokio::test]
async fn sync_folder_items_accepts_any_folder_id_namespace_prefix() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "44444444-4444-4444-4444-444444444444",
            "custom",
            "RCA Sync",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><x:FolderId Id="mailbox:44444444-4444-4444-4444-444444444444"/></m:SyncFolderId></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:SyncFolderItemsResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<m:SyncState>lpe-ews-sync.v1.mailbox."));
}

#[tokio::test]
async fn sync_folder_items_uses_bound_mailbox_id_when_folder_id_is_omitted() {
    let emails = Arc::new(Mutex::new(vec![FakeStore::email(
        "99999999-9999-9999-9999-999999999999",
        "44444444-4444-4444-4444-444444444444",
        "custom",
        "RCA folder item",
    )]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "44444444-4444-4444-4444-444444444444",
            "custom",
            "RCA Sync",
        )])),
        emails,
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="mailbox:44444444-4444-4444-4444-444444444444"/></m:SyncFolderId><m:MaxChangesReturned>1</m:MaxChangesReturned></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let state = test_xml_text(&response_text(response).await, "SyncState").unwrap();
    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncState>{state}</m:SyncState></m:SyncFolderItems></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(!body.contains("<t:Create><t:Message>"));
}

#[tokio::test]
async fn sync_folder_items_accepts_utf16_soap_requests() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "44444444-4444-4444-4444-444444444444",
            "custom",
            "RCA Sync",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let mut headers = bearer_headers();
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("text/xml; charset=utf-16"),
    );
    let request = r#"<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types"><s:Body><m:SyncFolderItems><m:ItemShape><t:BaseShape>IdOnly</t:BaseShape></m:ItemShape><m:SyncFolderId><t:FolderId Id="mailbox:44444444-4444-4444-4444-444444444444"/></m:SyncFolderId><m:MaxChangesReturned>512</m:MaxChangesReturned></m:SyncFolderItems></s:Body></s:Envelope>"#;
    let mut body = vec![0xff, 0xfe];
    body.extend(request.encode_utf16().flat_map(u16::to_le_bytes));

    let response = service.handle(&headers, &body).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:SyncFolderItemsResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
}

#[tokio::test]
async fn create_item_saveonly_imports_message_into_custom_mailbox_folder() {
    let mailbox_id = Uuid::parse_str("44444444-4444-4444-4444-444444444444").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "44444444-4444-4444-4444-444444444444",
            "custom",
            "RCA Sync",
        )])),
        ..Default::default()
    };
    let imported_emails = store.imported_emails.clone();
    let saved_drafts = store.saved_drafts.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:CreateItem MessageDisposition="SaveOnly">
                  <m:SavedItemFolderId><t:FolderId Id="mailbox:44444444-4444-4444-4444-444444444444"/></m:SavedItemFolderId>
                  <m:Items>
                    <t:Message>
                      <t:Subject>RCA folder item</t:Subject>
                      <t:Body BodyType="Text">Hello from EWS</t:Body>
                    </t:Message>
                  </m:Items>
                </m:CreateItem>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("message:99999999-9999-9999-9999-999999999999"));
    assert!(saved_drafts.lock().unwrap().is_empty());
    let recorded = imported_emails.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].mailbox_id, mailbox_id);
    assert_eq!(recorded[0].subject, "RCA folder item");
}

#[tokio::test]
async fn find_item_lists_custom_mailbox_messages() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "44444444-4444-4444-4444-444444444444",
            "custom",
            "RCA Sync",
        )])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            "99999999-9999-9999-9999-999999999999",
            "44444444-4444-4444-4444-444444444444",
            "custom",
            "RCA folder item",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:FindItem><m:ParentFolderIds><t:FolderId Id="mailbox:44444444-4444-4444-4444-444444444444"/></m:ParentFolderIds></m:FindItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:FindItemResponse>"));
    assert!(body.contains("<t:Message>"));
    assert!(body.contains("message:99999999-9999-9999-9999-999999999999"));
    assert!(body.contains("<t:Subject>RCA folder item</t:Subject>"));
}

#[tokio::test]
async fn find_item_rejects_items_lost_after_query_without_leaking_counts_or_bcc() {
    let mailbox_id = "44444444-4444-4444-4444-444444444444";
    let message_id = "99999999-9999-9999-9999-999999999999";
    let mut email = FakeStore::email(message_id, mailbox_id, "custom", "Revoked item");
    email.bcc.push(JmapEmailAddress {
        address: "protected@example.test".to_string(),
        display_name: Some("Protected recipient".to_string()),
    });
    let service = ExchangeService::new(FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            mailbox_id, "custom", "RCA Sync",
        )])),
        emails: Arc::new(Mutex::new(vec![email])),
        inaccessible_jmap_email_ids: Arc::new(Mutex::new(vec![
            Uuid::parse_str(message_id).unwrap()
        ])),
        ..Default::default()
    });

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:FindItem><m:ParentFolderIds><t:FolderId Id="mailbox:44444444-4444-4444-4444-444444444444"/></m:ParentFolderIds></m:FindItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:FindItemResponse>"));
    assert!(body.contains("<m:ResponseCode>ErrorFolderNotFound</m:ResponseCode>"));
    assert!(!body.contains("TotalItemsInView=\"1\""));
    assert!(!body.contains("Revoked item"));
    assert!(!body.contains("protected@example.test"));
}

#[tokio::test]
async fn find_item_rejects_unscoped_duplicate_or_inaccessible_parents_before_query() {
    let service = ExchangeService::new(FakeStore {
        session: Some(FakeStore::account()),
        fail_query_jmap_email_ids: true,
        ..Default::default()
    });

    for request in [
        br#"<s:Envelope><s:Body><m:FindItem/></s:Body></s:Envelope>"#.as_slice(),
        br#"<s:Envelope><s:Body><m:FindItem><m:ParentFolderIds><t:DistinguishedFolderId Id="inbox"/></m:ParentFolderIds><m:ParentFolderIds><t:DistinguishedFolderId Id="contacts"/></m:ParentFolderIds></m:FindItem></s:Body></s:Envelope>"#.as_slice(),
        br#"<s:Envelope><s:Body><m:FindItem><m:ParentFolderIds><t:FolderId Id="mailbox:not-a-uuid"/></m:ParentFolderIds></m:FindItem></s:Body></s:Envelope>"#.as_slice(),
        br#"<s:Envelope><s:Body><m:FindItem><m:ParentFolderIds><t:FolderId Id="unsupported"/></m:ParentFolderIds></m:FindItem></s:Body></s:Envelope>"#.as_slice(),
        br#"<s:Envelope><s:Body><m:FindItem><m:ParentFolderIds><t:FolderId Id="mailbox:44444444-4444-4444-4444-444444444444"/></m:ParentFolderIds></m:FindItem></s:Body></s:Envelope>"#.as_slice(),
    ] {
        let response = service.handle(&bearer_headers(), request).await.unwrap();
        let body = response_text(response).await;
        assert!(body.contains("<m:FindItemResponse>"));
        assert!(body.contains("<m:ResponseCode>ErrorFolderNotFound</m:ResponseCode>"));
    }
}

#[tokio::test]
async fn get_item_rejects_missing_duplicate_empty_or_malformed_item_id_shapes() {
    let service = ExchangeService::new(FakeStore {
        session: Some(FakeStore::account()),
        fail_fetch_all_jmap_email_ids: true,
        ..Default::default()
    });

    for request in [
        br#"<s:Envelope><s:Body><m:GetItem/></s:Body></s:Envelope>"#.as_slice(),
        br#"<s:Envelope><s:Body><m:GetItem><m:ItemIds/><m:ItemIds><t:ItemId Id="message:99999999-9999-9999-9999-999999999999"/></m:ItemIds></m:GetItem></s:Body></s:Envelope>"#.as_slice(),
        br#"<s:Envelope><s:Body><m:GetItem><m:ItemIds/></m:GetItem></s:Body></s:Envelope>"#.as_slice(),
        br#"<s:Envelope><s:Body><m:GetItem><m:ItemIds><t:ItemId Id="message:not-a-uuid"/></m:ItemIds></m:GetItem></s:Body></s:Envelope>"#.as_slice(),
    ] {
        let response = service.handle(&bearer_headers(), request).await.unwrap();
        let body = response_text(response).await;
        assert!(body.contains("<m:GetItemResponse>"));
        assert!(body.contains("<m:ResponseCode>ErrorItemNotFound</m:ResponseCode>"));
    }
}

#[tokio::test]
async fn find_item_pages_more_than_two_hundred_mailbox_messages() {
    let mailbox_id = "44444444-4444-4444-4444-444444444444";
    let emails = (1..=201)
        .map(|index| {
            FakeStore::email(
                &Uuid::from_u128(index).to_string(),
                mailbox_id,
                "custom",
                &format!("Mailbox item {index}"),
            )
        })
        .collect();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            mailbox_id,
            "custom",
            "Mailbox sync",
        )])),
        emails: Arc::new(Mutex::new(emails)),
        respect_jmap_query_pagination: true,
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:FindItem><m:IndexedPageItemView BasePoint="Beginning" Offset="0" MaxEntriesReturned="200"/><m:ParentFolderIds><t:FolderId Id="mailbox:{mailbox_id}"/></m:ParentFolderIds></m:FindItem></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let first_page = response_text(response).await;
    assert!(first_page.contains("TotalItemsInView=\"201\" IncludesLastItemInRange=\"false\""));
    assert_eq!(first_page.matches("<t:Message>").count(), 200);

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:FindItem><m:IndexedPageItemView BasePoint="Beginning" Offset="200" MaxEntriesReturned="5"/><m:ParentFolderIds><t:FolderId Id="mailbox:{mailbox_id}"/></m:ParentFolderIds></m:FindItem></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let last_page = response_text(response).await;
    assert!(last_page.contains("TotalItemsInView=\"201\" IncludesLastItemInRange=\"true\""));
    assert_eq!(last_page.matches("<t:Message>").count(), 1);
}

#[tokio::test]
async fn ews_mail_change_key_is_stable_for_a_non_primary_mailbox_membership() {
    let inbox_id = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
    let archive_id = Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap();
    let message_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap();
    let mut email = FakeStore::email(
        &message_id.to_string(),
        &inbox_id.to_string(),
        "inbox",
        "Multi-folder message",
    );
    let mut archive_state = email.mailbox_states[0].clone();
    archive_state.mailbox_id = archive_id;
    archive_state.role = "archive".to_string();
    archive_state.name = "Archive".to_string();
    archive_state.modseq = 42;
    archive_state.unread = true;
    email.mailbox_ids.push(archive_id);
    email.mailbox_states.push(archive_state);
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox"),
            FakeStore::mailbox(&archive_id.to_string(), "archive", "Archive"),
        ])),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:FindItem><m:ParentFolderIds><t:FolderId Id="mailbox:{archive_id}"/></m:ParentFolderIds></m:FindItem></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains(&format!("<t:ParentFolderId Id=\"mailbox:{archive_id}\"/>")));
    assert!(body.contains("<t:IsRead>false</t:IsRead>"));
    let find_key = test_item_change_key(&body, &format!("message:{message_id}"));

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:GetItem><m:ItemIds><t:ItemId Id="message:{message_id}"/></m:ItemIds></m:GetItem></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert_eq!(
        test_item_change_key(&body, &format!("message:{message_id}")),
        find_key
    );

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="mailbox:{archive_id}"/></m:SyncFolderId></m:SyncFolderItems></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains(&format!("<t:ParentFolderId Id=\"mailbox:{archive_id}\"/>")));
    assert_eq!(
        test_item_change_key(&body, &format!("message:{message_id}")),
        find_key
    );
}

#[tokio::test]
async fn find_item_lists_system_mailbox_messages_by_distinguished_id() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Inbox",
        )])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            "88888888-8888-8888-8888-888888888888",
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Inbox message",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:FindItem><m:ParentFolderIds><t:DistinguishedFolderId Id="inbox"/></m:ParentFolderIds></m:FindItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:FindItemResponse>"));
    assert!(body.contains("<t:Message>"));
    assert!(body.contains("message:88888888-8888-8888-8888-888888888888"));
    assert!(body.contains("<t:Subject>Inbox message</t:Subject>"));
}

#[tokio::test]
async fn sync_folder_items_reports_custom_mailbox_create_and_delete_changes() {
    let mailbox_id = Uuid::parse_str("44444444-4444-4444-4444-444444444444").unwrap();
    let message_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap();
    let emails = Arc::new(Mutex::new(vec![FakeStore::email(
        &message_id.to_string(),
        &mailbox_id.to_string(),
        "custom",
        "RCA folder item",
    )]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &mailbox_id.to_string(),
            "custom",
            "RCA folder",
        )])),
        emails: emails.clone(),
        mapi_notification_cursor: Arc::new(Mutex::new(Some(7))),
        ews_mailbox_item_sync_replays: Arc::new(Mutex::new(vec![EwsNotificationReplay {
            expired: false,
            current_cursor: Some(8),
            next_cursor: 8,
            more_events: false,
            events: vec![EwsNotificationLogEvent {
                cursor: 8,
                mailbox_id,
                message_id,
                change_kind: "destroyed".to_string(),
                modseq: 8,
                created_at: "2026-08-17T08:00:00.000000Z".to_string(),
            }],
        }])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="mailbox:44444444-4444-4444-4444-444444444444"/></m:SyncFolderId></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:Create><t:Message>"));
    assert!(body.contains("message:99999999-9999-9999-9999-999999999999"));
    assert!(body.contains("<m:SyncState>lpe-ews-sync.v1.mailbox."));
    let sync_state = test_xml_text(&body, "SyncState").unwrap();

    emails.lock().unwrap().clear();
    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncState>{sync_state}</m:SyncState><m:SyncFolderId><t:FolderId Id="mailbox:44444444-4444-4444-4444-444444444444"/></m:SyncFolderId></m:SyncFolderItems></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(
        body.contains("<t:Delete><t:ItemId Id=\"message:99999999-9999-9999-9999-999999999999\"")
    );
}

#[tokio::test]
async fn sync_folder_items_pages_more_than_two_hundred_mailbox_messages() {
    let mailbox_id = "44444444-4444-4444-4444-444444444444";
    let emails = (1..=201)
        .map(|index| {
            FakeStore::email(
                &Uuid::from_u128(index).to_string(),
                mailbox_id,
                "custom",
                &format!("Mailbox item {index}"),
            )
        })
        .collect();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            mailbox_id,
            "custom",
            "Mailbox sync",
        )])),
        emails: Arc::new(Mutex::new(emails)),
        respect_jmap_query_pagination: true,
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="mailbox:{mailbox_id}"/></m:SyncFolderId><m:MaxChangesReturned>200</m:MaxChangesReturned></m:SyncFolderItems></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let first_page = response_text(response).await;
    assert!(first_page.contains("<m:IncludesLastItemInRange>false</m:IncludesLastItemInRange>"));
    assert_eq!(first_page.matches("<t:Create>").count(), 200);
    let sync_state = test_xml_text(&first_page, "SyncState").unwrap();

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="mailbox:{mailbox_id}"/></m:SyncFolderId><m:SyncState>{sync_state}</m:SyncState><m:MaxChangesReturned>200</m:MaxChangesReturned></m:SyncFolderItems></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let last_page = response_text(response).await;
    assert!(last_page.contains("<m:IncludesLastItemInRange>true</m:IncludesLastItemInRange>"));
    assert_eq!(last_page.matches("<t:Create>").count(), 1);
}

#[tokio::test]
async fn sync_folder_items_rejects_malformed_and_folder_mismatched_cursor_tokens() {
    let first_mailbox_id = "44444444-4444-4444-4444-444444444444";
    let second_mailbox_id = "55555555-5555-5555-5555-555555555555";
    let service = ExchangeService::new(FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(first_mailbox_id, "custom", "First"),
            FakeStore::mailbox(second_mailbox_id, "custom", "Second"),
        ])),
        ..Default::default()
    });

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="mailbox:{first_mailbox_id}"/></m:SyncFolderId></m:SyncFolderItems></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let state = test_xml_text(&response_text(response).await, "SyncState").unwrap();

    for (sync_state, folder_id) in [
        ("not-a-token".to_string(), first_mailbox_id),
        (state, second_mailbox_id),
    ] {
        let response = service
            .handle(
                &bearer_headers(),
                format!(
                    r#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="mailbox:{folder_id}"/></m:SyncFolderId><m:SyncState>{sync_state}</m:SyncState></m:SyncFolderItems></s:Body></s:Envelope>"#
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let body = response_text(response).await;
        assert!(body.contains("ResponseClass=\"Error\""));
        assert!(!body.contains("<t:Message>"));
    }
}

#[tokio::test]
async fn sync_folder_items_rejects_expired_canonical_replay_cursor() {
    let mailbox_id = "66666666-6666-6666-6666-666666666666";
    let service = ExchangeService::new(FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            mailbox_id, "custom", "Replay",
        )])),
        mapi_notification_cursor: Arc::new(Mutex::new(Some(9))),
        ..Default::default()
    });
    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="mailbox:{mailbox_id}"/></m:SyncFolderId></m:SyncFolderItems></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let state = test_xml_text(&response_text(response).await, "SyncState").unwrap();
    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncState>{state}</m:SyncState></m:SyncFolderItems></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(body.contains("canonical change-log retention"));
    assert!(!body.contains("<t:Create>"));
}

#[tokio::test]
async fn sync_folder_items_honors_max_changes_returned() {
    let mailbox_id = "44444444-4444-4444-4444-444444444444";
    let emails = vec![
        FakeStore::email(
            "11111111-1111-1111-1111-111111111111",
            mailbox_id,
            "custom",
            "First item",
        ),
        FakeStore::email(
            "22222222-2222-2222-2222-222222222222",
            mailbox_id,
            "custom",
            "Second item",
        ),
    ];
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            mailbox_id,
            "custom",
            "Mailbox sync",
        )])),
        emails: Arc::new(Mutex::new(emails)),
        respect_jmap_query_pagination: true,
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="mailbox:{mailbox_id}"/></m:SyncFolderId><m:MaxChangesReturned>1</m:MaxChangesReturned></m:SyncFolderItems></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let first_page = response_text(response).await;
    assert!(first_page.contains("<m:IncludesLastItemInRange>false</m:IncludesLastItemInRange>"));
    assert_eq!(first_page.matches("<t:Create>").count(), 1);
    let sync_state = test_xml_text(&first_page, "SyncState").unwrap();

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="mailbox:{mailbox_id}"/></m:SyncFolderId><m:SyncState>{sync_state}</m:SyncState><m:MaxChangesReturned>1</m:MaxChangesReturned></m:SyncFolderItems></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let last_page = response_text(response).await;
    assert!(last_page.contains("<m:IncludesLastItemInRange>true</m:IncludesLastItemInRange>"));
    assert_eq!(last_page.matches("<t:Create>").count(), 1);
}

#[tokio::test]
async fn sync_folder_items_reports_system_mailbox_messages() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Inbox",
        )])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            "88888888-8888-8888-8888-888888888888",
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Inbox message",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:DistinguishedFolderId Id="inbox"/></m:SyncFolderId></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:SyncFolderItemsResponse>"));
    assert!(body.contains("<t:Create><t:Message>"));
    assert!(body.contains("message:88888888-8888-8888-8888-888888888888"));
    assert!(body.contains("<m:SyncState>lpe-ews-sync.v1.mailbox."));
}

#[tokio::test]
async fn sync_folder_items_excludes_protected_bcc_recipients() {
    let mut message = FakeStore::email(
        "88888888-8888-8888-8888-888888888888",
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Inbox message",
    );
    message.bcc.push(JmapEmailAddress {
        address: "protected@example.test".to_string(),
        display_name: Some("Protected Recipient".to_string()),
    });
    let service = ExchangeService::new(FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Inbox",
        )])),
        emails: Arc::new(Mutex::new(vec![message])),
        ..Default::default()
    });

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:DistinguishedFolderId Id="inbox"/></m:SyncFolderId></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<t:Create><t:Message>"));
    assert!(!body.contains("<t:BccRecipients>"));
    assert!(!body.contains("protected@example.test"));
}

#[tokio::test]
async fn find_item_lists_public_folder_items() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            None,
            "Public Root",
        )])),
        public_folder_items: Arc::new(Mutex::new(vec![FakeStore::public_folder_item(
            "abababab-abab-abab-abab-abababababab",
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "Public post",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:FindItem><m:ParentFolderIds><t:FolderId Id="public-folder:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"/></m:ParentFolderIds></m:FindItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:FindItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("public-folder-item:abababab-abab-abab-abab-abababababab"));
    assert!(body
        .contains("<t:ParentFolderId Id=\"public-folder:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee\"/>"));
    assert!(body.contains("<t:Subject>Public post</t:Subject>"));
}

#[tokio::test]
async fn find_item_rejects_public_folder_without_read_access() {
    let non_owner = AuthenticatedAccount {
        tenant_id: FakeStore::account().tenant_id,
        account_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
        email: "delegate@example.test".to_string(),
        display_name: "Public Delegate".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let mut public_folder =
        FakeStore::public_folder("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", None, "Public Root");
    public_folder.rights = PublicFolderRights {
        may_read: false,
        may_write: false,
        may_delete: false,
        may_share: false,
    };
    let store = FakeStore {
        session: Some(non_owner),
        public_folders: Arc::new(Mutex::new(vec![public_folder])),
        public_folder_items: Arc::new(Mutex::new(vec![FakeStore::public_folder_item(
            "abababab-abab-abab-abab-abababababab",
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "Public post",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:FindItem><m:ParentFolderIds><t:FolderId Id="public-folder:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"/></m:ParentFolderIds></m:FindItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:FindItemResponse>"));
    assert!(body.contains("<m:ResponseCode>ErrorAccessDenied</m:ResponseCode>"));
    assert!(body.contains("public folder read access is not granted"));
}

#[tokio::test]
async fn sync_folder_items_reports_public_folder_items() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            None,
            "Public Root",
        )])),
        public_folder_items: Arc::new(Mutex::new(vec![FakeStore::public_folder_item(
            "abababab-abab-abab-abab-abababababab",
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "Public post",
        )])),
        ..Default::default()
    };
    let public_folder_items = store.public_folder_items.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="public-folder:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"/></m:SyncFolderId></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:SyncFolderItemsResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:Create><t:Message>"));
    assert!(body.contains("public-folder-item:abababab-abab-abab-abab-abababababab"));
    let change_key = test_item_change_key(
        &body,
        "public-folder-item:abababab-abab-abab-abab-abababababab",
    );
    let first_sync_state = test_xml_text(&body, "SyncState").unwrap();
    assert!(first_sync_state.starts_with("lpe-ews-sync.v1.public-folder."));

    public_folder_items.lock().unwrap()[0].is_read = true;
    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="public-folder:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"/></m:SyncFolderId><m:SyncState>{first_sync_state}</m:SyncState></m:SyncFolderItems></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:ReadFlagChange>"));
    assert!(body.contains(&format!(
        "<t:ItemId Id=\"public-folder-item:abababab-abab-abab-abab-abababababab\" ChangeKey=\"{change_key}\"/>"
    )));
    assert!(body.contains("<t:IsRead>true</t:IsRead>"));
    assert!(!body.contains("<t:Update>"));
    let current_sync_state = test_xml_text(&body, "SyncState").unwrap();

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="public-folder:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"/></m:SyncFolderId><m:SyncState>{current_sync_state}</m:SyncState></m:SyncFolderItems></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(!body.contains("<t:Create>"));
    assert!(!body.contains("<t:Update>"));
    assert!(!body.contains("<t:ReadFlagChange>"));
}

#[tokio::test]
async fn sync_folder_items_rejects_public_folder_without_read_access() {
    let non_owner = AuthenticatedAccount {
        tenant_id: FakeStore::account().tenant_id,
        account_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
        email: "delegate@example.test".to_string(),
        display_name: "Public Delegate".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let mut public_folder =
        FakeStore::public_folder("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", None, "Public Root");
    public_folder.rights = PublicFolderRights {
        may_read: false,
        may_write: false,
        may_delete: false,
        may_share: false,
    };
    let store = FakeStore {
        session: Some(non_owner),
        public_folders: Arc::new(Mutex::new(vec![public_folder])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="public-folder:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"/></m:SyncFolderId></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:SyncFolderItemsResponse>"));
    assert!(body.contains("<m:ResponseCode>ErrorAccessDenied</m:ResponseCode>"));
    assert!(body.contains("public folder read access is not granted"));
}

#[tokio::test]
async fn get_item_returns_custom_mailbox_message_body() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            "99999999-9999-9999-9999-999999999999",
            "44444444-4444-4444-4444-444444444444",
            "custom",
            "RCA folder item",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetItem><m:ItemIds><t:ItemId Id="message:99999999-9999-9999-9999-999999999999"/></m:ItemIds></m:GetItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:GetItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:Body BodyType=\"Text\">Hello</t:Body>"));
}

#[tokio::test]
async fn get_item_returns_system_mailbox_message_body() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            "88888888-8888-8888-8888-888888888888",
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Inbox message",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetItem><m:ItemIds><t:ItemId Id="message:88888888-8888-8888-8888-888888888888"/></m:ItemIds></m:GetItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:GetItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:Subject>Inbox message</t:Subject>"));
    assert!(body.contains("<t:Body BodyType=\"Text\">Hello</t:Body>"));
}

#[tokio::test]
async fn get_item_returns_public_folder_item_body() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            None,
            "Public Root",
        )])),
        public_folder_items: Arc::new(Mutex::new(vec![FakeStore::public_folder_item(
            "abababab-abab-abab-abab-abababababab",
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "Public post",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetItem><m:ItemIds><t:ItemId Id="public-folder-item:abababab-abab-abab-abab-abababababab"/></m:ItemIds></m:GetItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:GetItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(
        body.contains("<t:ItemId Id=\"public-folder-item:abababab-abab-abab-abab-abababababab\"")
    );
    assert!(body
        .contains("<t:ParentFolderId Id=\"public-folder:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee\"/>"));
    assert!(body.contains("<t:Subject>Public post</t:Subject>"));
    assert!(body.contains("<t:Body BodyType=\"Text\">Public body</t:Body>"));
}

#[tokio::test]
async fn get_item_rejects_public_folder_item_without_read_access() {
    let non_owner = AuthenticatedAccount {
        tenant_id: FakeStore::account().tenant_id,
        account_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
        email: "delegate@example.test".to_string(),
        display_name: "Public Delegate".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let mut public_folder =
        FakeStore::public_folder("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", None, "Public Root");
    public_folder.rights = PublicFolderRights {
        may_read: false,
        may_write: false,
        may_delete: false,
        may_share: false,
    };
    let store = FakeStore {
        session: Some(non_owner),
        public_folders: Arc::new(Mutex::new(vec![public_folder])),
        public_folder_items: Arc::new(Mutex::new(vec![FakeStore::public_folder_item(
            "abababab-abab-abab-abab-abababababab",
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "Public post",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetItem><m:ItemIds><t:ItemId Id="public-folder-item:abababab-abab-abab-abab-abababababab"/></m:ItemIds></m:GetItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:GetItemResponse>"));
    assert!(body.contains("<m:ResponseCode>ErrorAccessDenied</m:ResponseCode>"));
    assert!(body.contains("public folder read access is not granted"));
}

#[tokio::test]
async fn get_item_returns_requested_mime_content_without_leaking_bcc_for_normal_mailbox() {
    let mut email = FakeStore::email(
        "99999999-9999-9999-9999-999999999999",
        "44444444-4444-4444-4444-444444444444",
        "custom",
        "RCA folder item",
    );
    email.bcc.push(JmapEmailAddress {
        address: "hidden@example.test".to_string(),
        display_name: Some("Hidden".to_string()),
    });
    let store = FakeStore {
        session: Some(FakeStore::account()),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetItem><m:ItemShape><t:AdditionalProperties><t:FieldURI FieldURI="item:MimeContent"/></t:AdditionalProperties></m:ItemShape><m:ItemIds><t:ItemId Id="message:99999999-9999-9999-9999-999999999999"/></m:ItemIds></m:GetItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<t:MimeContent CharacterSet=\"UTF-8\">"));
    let mime = decoded_mime_content(&body);
    assert!(mime.contains("Subject: RCA folder item"));
    assert!(mime.contains("Content-Type: text/plain; charset=UTF-8"));
    assert!(mime.ends_with("Hello"));
    assert!(!mime.contains("Bcc:"));
}

#[tokio::test]
async fn get_item_mime_content_hides_bcc_for_sent_message_default_fetch() {
    let mut email = FakeStore::email(
        "99999999-9999-9999-9999-999999999999",
        "44444444-4444-4444-4444-444444444444",
        "sent",
        "Sent folder item",
    );
    email.bcc.push(JmapEmailAddress {
        address: "hidden@example.test".to_string(),
        display_name: Some("Hidden".to_string()),
    });
    let store = FakeStore {
        session: Some(FakeStore::account()),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetItem><m:ItemShape><t:AdditionalProperties><t:FieldURI FieldURI="item:MimeContent"/></t:AdditionalProperties></m:ItemShape><m:ItemIds><t:ItemId Id="message:99999999-9999-9999-9999-999999999999"/></m:ItemIds></m:GetItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    let mime = decoded_mime_content(&body);
    assert!(mime.contains("Subject: Sent folder item"));
    assert!(!mime.contains("Bcc:"));
    assert!(!mime.contains("hidden@example.test"));
}

#[tokio::test]
async fn sync_folder_items_pages_contact_changes() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "contacts", "Contacts",
        )])),
        contacts: Arc::new(Mutex::new(vec![
            FakeStore::contact(
                "11111111-1111-1111-1111-111111111111",
                "First contact",
                "first@example.test",
            ),
            FakeStore::contact(
                "22222222-2222-2222-2222-222222222222",
                "Second contact",
                "second@example.test",
            ),
        ])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="default"/></m:SyncFolderId><m:MaxChangesReturned>1</m:MaxChangesReturned></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let first_page = response_text(response).await;
    assert!(first_page.contains("<m:IncludesLastItemInRange>false</m:IncludesLastItemInRange>"));
    assert_eq!(first_page.matches("<t:Create>").count(), 1);
    let sync_state = test_xml_text(&first_page, "SyncState").unwrap();

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="default"/></m:SyncFolderId><m:SyncState>{sync_state}</m:SyncState><m:MaxChangesReturned>1</m:MaxChangesReturned></m:SyncFolderItems></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let last_page = response_text(response).await;
    assert!(last_page.contains("<m:IncludesLastItemInRange>true</m:IncludesLastItemInRange>"));
    assert_eq!(last_page.matches("<t:Create>").count(), 1);
}

#[tokio::test]
async fn get_item_returns_recipients_html_and_owner_sent_bcc_only() {
    let mut sent = FakeStore::email(
        "99999999-9999-9999-9999-999999999999",
        "44444444-4444-4444-4444-444444444444",
        "sent",
        "Owner sent item",
    );
    sent.cc.push(JmapEmailAddress {
        address: "carol@example.test".to_string(),
        display_name: Some("Carol".to_string()),
    });
    sent.bcc.push(JmapEmailAddress {
        address: "owner-bcc@example.test".to_string(),
        display_name: Some("Owner Bcc".to_string()),
    });
    sent.body_html_sanitized = Some("<p>Canonical <b>HTML</b></p>".to_string());
    let mut draft = FakeStore::email(
        "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        "44444444-4444-4444-4444-444444444444",
        "drafts",
        "Owner draft item",
    );
    draft.bcc.push(JmapEmailAddress {
        address: "draft-bcc@example.test".to_string(),
        display_name: None,
    });
    let mut ordinary = FakeStore::email(
        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        "44444444-4444-4444-4444-444444444444",
        "custom",
        "Ordinary item",
    );
    ordinary.bcc.push(JmapEmailAddress {
        address: "must-not-leak@example.test".to_string(),
        display_name: None,
    });
    let store = FakeStore {
        session: Some(FakeStore::account()),
        emails: Arc::new(Mutex::new(vec![sent, draft, ordinary])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetItem><m:ItemShape><t:BaseShape>AllProperties</t:BaseShape><t:BodyType>HTML</t:BodyType></m:ItemShape><m:ItemIds><t:ItemId Id="message:99999999-9999-9999-9999-999999999999"/><t:ItemId Id="message:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"/><t:ItemId Id="message:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"/></m:ItemIds></m:GetItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<t:ToRecipients>"));
    assert!(body.contains("<t:EmailAddress>bob@example.test</t:EmailAddress>"));
    assert!(body.contains("<t:CcRecipients>"));
    assert!(body.contains("<t:EmailAddress>carol@example.test</t:EmailAddress>"));
    assert!(body.contains("<t:BccRecipients>"));
    assert!(body.contains("<t:EmailAddress>owner-bcc@example.test</t:EmailAddress>"));
    // [MS-OXWSMSG] sections 2.2.4.3 and 3.1.4.4: BccRecipients are exposed
    // only for the authenticated owner's canonical Sent item.
    assert!(!body.contains("<t:EmailAddress>draft-bcc@example.test</t:EmailAddress>"));
    assert!(body.contains(
        "<t:Body BodyType=\"HTML\">&lt;p&gt;Canonical &lt;b&gt;HTML&lt;/b&gt;&lt;/p&gt;</t:Body>"
    ));
    assert!(!body.contains("must-not-leak@example.test"));
}

#[tokio::test]
async fn get_item_includes_attachment_references_for_message() {
    let message_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap();
    let attachment_id = Uuid::parse_str("abababab-abab-abab-abab-abababababab").unwrap();
    let mut email = FakeStore::email(
        "99999999-9999-9999-9999-999999999999",
        "44444444-4444-4444-4444-444444444444",
        "custom",
        "RCA folder item",
    );
    email.has_attachments = true;
    let file_reference = format!("attachment:{message_id}:{attachment_id}");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        emails: Arc::new(Mutex::new(vec![email])),
        attachments: Arc::new(Mutex::new(HashMap::from([(
            message_id,
            vec![ActiveSyncAttachment {
                id: attachment_id,
                message_id,
                file_name: "brief.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                disposition: None,
                content_id: None,
                size_octets: 5,
                file_reference: file_reference.clone(),
            }],
        )]))),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetItem><m:ItemIds><t:ItemId Id="message:99999999-9999-9999-9999-999999999999"/></m:ItemIds></m:GetItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:GetItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:Attachments>"));
    assert!(body.contains("<t:FileAttachment>"));
    assert!(body.contains(&format!("<t:AttachmentId Id=\"{file_reference}\"/>")));
    assert!(body.contains("<t:Name>brief.pdf</t:Name>"));
    assert!(body.contains("<t:ContentType>application/pdf</t:ContentType>"));
    assert!(body.contains("<t:Size>5</t:Size>"));
    assert!(!body.contains("<t:Content>"));
}

#[tokio::test]
async fn get_item_mime_content_includes_canonical_attachments() {
    let message_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap();
    let attachment_id = Uuid::parse_str("abababab-abab-abab-abab-abababababab").unwrap();
    let mut email = FakeStore::email(
        "99999999-9999-9999-9999-999999999999",
        "44444444-4444-4444-4444-444444444444",
        "custom",
        "RCA folder item",
    );
    email.has_attachments = true;
    let file_reference = format!("attachment:{message_id}:{attachment_id}");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        emails: Arc::new(Mutex::new(vec![email])),
        attachments: Arc::new(Mutex::new(HashMap::from([(
            message_id,
            vec![ActiveSyncAttachment {
                id: attachment_id,
                message_id,
                file_name: "brief.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                disposition: None,
                content_id: None,
                size_octets: 5,
                file_reference: file_reference.clone(),
            }],
        )]))),
        attachment_contents: Arc::new(Mutex::new(HashMap::from([(
            file_reference.clone(),
            ActiveSyncAttachmentContent {
                file_reference,
                file_name: "brief.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                blob_bytes: b"hello".to_vec(),
                ..Default::default()
            },
        )]))),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetItem><m:ItemShape><t:AdditionalProperties><t:FieldURI FieldURI="item:MimeContent"/></t:AdditionalProperties></m:ItemShape><m:ItemIds><t:ItemId Id="message:99999999-9999-9999-9999-999999999999"/></m:ItemIds></m:GetItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    let mime = decoded_mime_content(&body);
    assert!(mime.contains("Content-Type: multipart/mixed; boundary=\"lpe-ews-mixed-99999999999999999999999999999999\""));
    assert!(mime.contains("Content-Disposition: attachment; filename=\"brief.pdf\""));
    assert!(mime.contains("Content-Type: application/pdf; name=\"brief.pdf\""));
    assert!(mime.contains("aGVsbG8="));
}

#[tokio::test]
async fn get_attachment_returns_canonical_attachment_content() {
    let message_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap();
    let attachment_id = Uuid::parse_str("abababab-abab-abab-abab-abababababab").unwrap();
    let file_reference = format!("attachment:{message_id}:{attachment_id}");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        attachment_contents: Arc::new(Mutex::new(HashMap::from([(
            file_reference.clone(),
            ActiveSyncAttachmentContent {
                file_reference: file_reference.clone(),
                file_name: "brief.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                disposition: Some("inline".to_string()),
                content_id: Some("brief-cid@example.test".to_string()),
                blob_bytes: b"hello".to_vec(),
            },
        )]))),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let request = format!(
        r#"<s:Envelope><s:Body><m:GetAttachment><m:AttachmentIds><t:AttachmentId Id="{file_reference}"/></m:AttachmentIds></m:GetAttachment></s:Body></s:Envelope>"#
    );

    let response = service
        .handle(&bearer_headers(), request.as_bytes())
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:GetAttachmentResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains(&format!("<t:AttachmentId Id=\"{file_reference}\"/>")));
    assert!(body.contains("<t:Name>brief.pdf</t:Name>"));
    assert!(body.contains("<t:ContentType>application/pdf</t:ContentType>"));
    assert!(body.contains("<t:Size>5</t:Size>"));
    assert!(body.contains("<t:IsInline>true</t:IsInline>"));
    assert!(body.contains("<t:ContentId>brief-cid@example.test</t:ContentId>"));
    assert!(body.contains("<t:Content>aGVsbG8=</t:Content>"));
}

#[tokio::test]
async fn calendar_attachment_get_and_delete_use_the_accessible_canonical_event() {
    let event_id = Uuid::parse_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").unwrap();
    let attachment_id = Uuid::parse_str("dddddddd-dddd-dddd-dddd-dddddddddddd").unwrap();
    let file_reference = format!("calendar-attachment:{event_id}:{attachment_id}");
    let event = AccessibleEvent {
        id: event_id,
        uid: event_id.to_string(),
        collection_id: "default".to_string(),
        owner_account_id: FakeStore::account().account_id,
        owner_email: "alice@example.test".to_string(),
        owner_display_name: "Alice".to_string(),
        rights: FakeStore::rights(),
        date: "2026-05-04".to_string(),
        time: "09:30".to_string(),
        time_zone: "UTC".to_string(),
        duration_minutes: 45,
        all_day: false,
        status: "confirmed".to_string(),
        sequence: 0,
        recurrence_rule: String::new(),
        recurrence_json: "{}".to_string(),
        recurrence_exceptions_json: "[]".to_string(),
        title: "Planning".to_string(),
        location: String::new(),
        organizer_json: "{}".to_string(),
        attendees: String::new(),
        attendees_json: "[]".to_string(),
        notes: String::new(),
        body_html: String::new(),
    };
    let attachments = Arc::new(Mutex::new(HashMap::from([(
        event_id,
        vec![CalendarEventAttachment {
            id: attachment_id,
            event_id,
            file_name: "agenda.pdf".to_string(),
            media_type: "application/pdf".to_string(),
            size_octets: 5,
            file_reference: file_reference.clone(),
        }],
    )])));
    let service = ExchangeService::new(FakeStore {
        session: Some(FakeStore::account()),
        events: Arc::new(Mutex::new(vec![event])),
        calendar_attachments: attachments.clone(),
        attachment_contents: Arc::new(Mutex::new(HashMap::from([(
            file_reference.clone(),
            ActiveSyncAttachmentContent {
                file_reference: file_reference.clone(),
                file_name: "agenda.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                blob_bytes: b"hello".to_vec(),
                ..Default::default()
            },
        )]))),
        ..Default::default()
    });

    let get = format!(
        r#"<s:Envelope><s:Body><m:GetAttachment><m:AttachmentIds><t:AttachmentId Id="{file_reference}"/></m:AttachmentIds></m:GetAttachment></s:Body></s:Envelope>"#
    );
    let response = service.handle(&bearer_headers(), get.as_bytes()).await.unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:Name>agenda.pdf</t:Name>"));

    let delete = format!(
        r#"<s:Envelope><s:Body><m:DeleteAttachment><m:AttachmentIds><t:AttachmentId Id="{file_reference}"/></m:AttachmentIds></m:DeleteAttachment></s:Body></s:Envelope>"#
    );
    let response = service
        .handle(&bearer_headers(), delete.as_bytes())
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains(&format!("RootItemId=\"event:{event_id}\"")));
    assert!(attachments.lock().unwrap()[&event_id].is_empty());
}

#[tokio::test]
async fn calendar_attachment_requires_effective_read_and_delete_rights() {
    let event_id = Uuid::parse_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").unwrap();
    let attachment_id = Uuid::parse_str("dddddddd-dddd-dddd-dddd-dddddddddddd").unwrap();
    let file_reference = format!("calendar-attachment:{event_id}:{attachment_id}");
    let mut rights = FakeStore::rights();
    rights.may_read = false;
    rights.may_delete = false;
    let event = AccessibleEvent {
        id: event_id,
        uid: event_id.to_string(),
        collection_id: "default".to_string(),
        owner_account_id: FakeStore::account().account_id,
        owner_email: "alice@example.test".to_string(),
        owner_display_name: "Alice".to_string(),
        rights,
        date: "2026-05-04".to_string(),
        time: "09:30".to_string(),
        time_zone: "UTC".to_string(),
        duration_minutes: 45,
        all_day: false,
        status: "confirmed".to_string(),
        sequence: 0,
        recurrence_rule: String::new(),
        recurrence_json: "{}".to_string(),
        recurrence_exceptions_json: "[]".to_string(),
        title: "Private".to_string(),
        location: String::new(),
        organizer_json: "{}".to_string(),
        attendees: String::new(),
        attendees_json: "[]".to_string(),
        notes: String::new(),
        body_html: String::new(),
    };
    let attachments = Arc::new(Mutex::new(HashMap::from([(
        event_id,
        vec![CalendarEventAttachment {
            id: attachment_id,
            event_id,
            file_name: "private.pdf".to_string(),
            media_type: "application/pdf".to_string(),
            size_octets: 5,
            file_reference: file_reference.clone(),
        }],
    )])));
    let service = ExchangeService::new(FakeStore {
        session: Some(FakeStore::account()),
        events: Arc::new(Mutex::new(vec![event])),
        calendar_attachments: attachments.clone(),
        ..Default::default()
    });

    for request in [
        format!(r#"<s:Envelope><s:Body><m:GetAttachment><m:AttachmentIds><t:AttachmentId Id="{file_reference}"/></m:AttachmentIds></m:GetAttachment></s:Body></s:Envelope>"#),
        format!(r#"<s:Envelope><s:Body><m:DeleteAttachment><m:AttachmentIds><t:AttachmentId Id="{file_reference}"/></m:AttachmentIds></m:DeleteAttachment></s:Body></s:Envelope>"#),
    ] {
        let response = service.handle(&bearer_headers(), request.as_bytes()).await.unwrap();
        let body = response_text(response).await;
        assert!(body.contains("<m:ResponseCode>ErrorAttachmentNotFound</m:ResponseCode>"));
    }
    assert_eq!(attachments.lock().unwrap()[&event_id].len(), 1);
}

#[tokio::test]
async fn get_attachment_rejects_unknown_attachment_id() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetAttachment><m:AttachmentIds><t:AttachmentId Id="attachment:99999999-9999-9999-9999-999999999999:abababab-abab-abab-abab-abababababab"/></m:AttachmentIds></m:GetAttachment></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:GetAttachmentResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(body.contains("<m:ResponseCode>ErrorAttachmentNotFound</m:ResponseCode>"));
}

#[tokio::test]
async fn create_attachment_validates_and_adds_canonical_attachment() {
    let message_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            "99999999-9999-9999-9999-999999999999",
            "44444444-4444-4444-4444-444444444444",
            "custom",
            "RCA folder item",
        )])),
        ..Default::default()
    };
    let created_attachments = store.created_attachments.clone();
    let attachments = store.attachments.clone();
    let emails = store.emails.clone();
    let service = ExchangeService::new_with_validator(
        store.clone(),
        Validator::new(FakeDetector::pdf(), 0.8),
    );

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:CreateAttachment><m:ParentItemId><t:ItemId Id="message:99999999-9999-9999-9999-999999999999"/></m:ParentItemId><m:Attachments><t:FileAttachment><t:Name>brief.pdf</t:Name><t:ContentType>application/pdf</t:ContentType><t:IsInline>true</t:IsInline><t:ContentId>brief-cid@example.test</t:ContentId><t:Content>aGVsbG8=</t:Content></t:FileAttachment></m:Attachments></m:CreateAttachment></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:CreateAttachmentResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:AttachmentId Id=\"attachment:99999999-9999-9999-9999-999999999999:cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcdcd\"/>"));
    assert!(body.contains("RootItemId=\"message:99999999-9999-9999-9999-999999999999\""));
    assert_eq!(created_attachments.lock().unwrap().len(), 1);
    let attachment = &created_attachments.lock().unwrap()[0];
    assert_eq!(attachment.file_name, "brief.pdf");
    assert_eq!(attachment.media_type, "application/pdf");
    assert_eq!(attachment.disposition.as_deref(), Some("inline"));
    assert_eq!(
        attachment.content_id.as_deref(),
        Some("brief-cid@example.test")
    );
    assert_eq!(attachment.blob_bytes, b"hello");
    assert_eq!(
        attachments.lock().unwrap().get(&message_id).unwrap().len(),
        1
    );
    assert!(emails.lock().unwrap()[0].has_attachments);

    let get_response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetAttachment><m:AttachmentIds><t:AttachmentId Id="attachment:99999999-9999-9999-9999-999999999999:cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcdcd"/></m:AttachmentIds></m:GetAttachment></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let get = response_text(get_response).await;
    assert!(get.contains("<t:Content>aGVsbG8=</t:Content>"));
    assert!(get.contains("<t:IsInline>true</t:IsInline>"));
    assert!(get.contains("<t:ContentId>brief-cid@example.test</t:ContentId>"));
}

#[tokio::test]
// [MS-OXWSATT] sections 2.2.4.5 and 3.1.4.1: unsupported/malformed shapes
// are rejected before the bounded canonical attachment mutation.
async fn create_attachment_rejects_batch_and_malformed_shapes_without_mutation() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            "99999999-9999-9999-9999-999999999999",
            "44444444-4444-4444-4444-444444444444",
            "custom",
            "RCA folder item",
        )])),
        ..Default::default()
    };
    let created_attachments = store.created_attachments.clone();
    let service =
        ExchangeService::new_with_validator(store, Validator::new(FakeDetector::pdf(), 0.8));

    for request in [
        br#"<s:Envelope><s:Body><m:CreateAttachment><m:ParentItemId><t:ItemId Id="message:not-a-uuid"/></m:ParentItemId><m:Attachments><t:FileAttachment><t:Name>brief.pdf</t:Name><t:Content>aGVsbG8=</t:Content></t:FileAttachment></m:Attachments></m:CreateAttachment></s:Body></s:Envelope>"# as &[u8],
        br#"<s:Envelope><s:Body><m:CreateAttachment><m:ParentItemId><t:ItemId Id="message:99999999-9999-9999-9999-999999999999"/></m:ParentItemId><m:Attachments><t:FileAttachment><t:Name>first.pdf</t:Name><t:Content>aGVsbG8=</t:Content></t:FileAttachment><t:FileAttachment><t:Name>second.pdf</t:Name><t:Content>%%%bad-base64%%%</t:Content></t:FileAttachment></m:Attachments></m:CreateAttachment></s:Body></s:Envelope>"#,
        br#"<s:Envelope><s:Body><m:CreateAttachment><m:ParentItemId><t:ItemId Id="message:99999999-9999-9999-9999-999999999999"/></m:ParentItemId><m:Attachments><t:ItemAttachment><t:Name>nested</t:Name></t:ItemAttachment></m:Attachments></m:CreateAttachment></s:Body></s:Envelope>"#,
    ] {
        let response = service.handle(&bearer_headers(), request).await.unwrap();
        let body = response_text(response).await;
        assert!(body.contains("<m:CreateAttachmentResponse>"));
        assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    }
    assert!(created_attachments.lock().unwrap().is_empty());
}

#[tokio::test]
async fn create_attachment_rejects_magika_blocked_payload() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            "99999999-9999-9999-9999-999999999999",
            "44444444-4444-4444-4444-444444444444",
            "custom",
            "RCA folder item",
        )])),
        ..Default::default()
    };
    let created_attachments = store.created_attachments.clone();
    let service =
        ExchangeService::new_with_validator(store, Validator::new(FakeDetector::executable(), 0.8));

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:CreateAttachment><m:ParentItemId><t:ItemId Id="message:99999999-9999-9999-9999-999999999999"/></m:ParentItemId><m:Attachments><t:FileAttachment><t:Name>brief.pdf</t:Name><t:ContentType>application/pdf</t:ContentType><t:Content>aGVsbG8=</t:Content></t:FileAttachment></m:Attachments></m:CreateAttachment></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:CreateAttachmentResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert!(created_attachments.lock().unwrap().is_empty());
}

#[tokio::test]
async fn create_attachment_rejects_stale_parent_change_key_without_mutation() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            "99999999-9999-9999-9999-999999999999",
            "44444444-4444-4444-4444-444444444444",
            "custom",
            "Attachment parent",
        )])),
        ..Default::default()
    };
    let created_attachments = store.created_attachments.clone();
    let service =
        ExchangeService::new_with_validator(store, Validator::new(FakeDetector::pdf(), 0.8));

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:CreateAttachment><m:ParentItemId><t:ItemId Id="message:99999999-9999-9999-9999-999999999999" ChangeKey="stale"/></m:ParentItemId><m:Attachments><t:FileAttachment><t:Name>brief.pdf</t:Name><t:ContentType>application/pdf</t:ContentType><t:Content>aGVsbG8=</t:Content></t:FileAttachment></m:Attachments></m:CreateAttachment></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>ErrorIrresolvableConflict</m:ResponseCode>"));
    assert!(created_attachments.lock().unwrap().is_empty());
}

#[tokio::test]
async fn create_attachment_rejects_unknown_parent_message() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service =
        ExchangeService::new_with_validator(store, Validator::new(FakeDetector::pdf(), 0.8));

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:CreateAttachment><m:ParentItemId><t:ItemId Id="message:99999999-9999-9999-9999-999999999999"/></m:ParentItemId><m:Attachments><t:FileAttachment><t:Name>brief.pdf</t:Name><t:ContentType>application/pdf</t:ContentType><t:Content>aGVsbG8=</t:Content></t:FileAttachment></m:Attachments></m:CreateAttachment></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:CreateAttachmentResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(body.contains("<m:ResponseCode>ErrorItemNotFound</m:ResponseCode>"));
}

#[tokio::test]
async fn delete_attachment_removes_canonical_attachment_reference() {
    let message_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap();
    let attachment_id = Uuid::parse_str("abababab-abab-abab-abab-abababababab").unwrap();
    let mut email = FakeStore::email(
        "99999999-9999-9999-9999-999999999999",
        "44444444-4444-4444-4444-444444444444",
        "custom",
        "RCA folder item",
    );
    email.has_attachments = true;
    let file_reference = format!("attachment:{message_id}:{attachment_id}");
    let attachments = Arc::new(Mutex::new(HashMap::from([(
        message_id,
        vec![ActiveSyncAttachment {
            id: attachment_id,
            message_id,
            file_name: "brief.pdf".to_string(),
            media_type: "application/pdf".to_string(),
            disposition: None,
            content_id: None,
            size_octets: 5,
            file_reference: file_reference.clone(),
        }],
    )])));
    let emails = Arc::new(Mutex::new(vec![email]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        emails: emails.clone(),
        attachments: attachments.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let request = format!(
        r#"<s:Envelope><s:Body><m:DeleteAttachment><m:AttachmentIds><t:AttachmentId Id="{file_reference}"/></m:AttachmentIds></m:DeleteAttachment></s:Body></s:Envelope>"#
    );

    let response = service
        .handle(&bearer_headers(), request.as_bytes())
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:DeleteAttachmentResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("RootItemId=\"message:99999999-9999-9999-9999-999999999999\""));
    assert!(attachments
        .lock()
        .unwrap()
        .get(&message_id)
        .unwrap()
        .is_empty());
    assert!(!emails.lock().unwrap()[0].has_attachments);
}

#[tokio::test]
async fn delete_attachment_rejects_unknown_attachment_id() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:DeleteAttachment><m:AttachmentIds><t:AttachmentId Id="attachment:99999999-9999-9999-9999-999999999999:abababab-abab-abab-abab-abababababab"/></m:AttachmentIds></m:DeleteAttachment></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:DeleteAttachmentResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(body.contains("<m:ResponseCode>ErrorAttachmentNotFound</m:ResponseCode>"));
}

#[tokio::test]
// [MS-OXWSATT] sections 2.2.4.2 and 3.1.4.2: bounded deletion validates the
// complete request before the one canonical attachment delete.
async fn delete_attachment_rejects_multiple_ids_without_mutation() {
    let message_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap();
    let attachment_id = Uuid::parse_str("abababab-abab-abab-abab-abababababab").unwrap();
    let file_reference = format!("attachment:{message_id}:{attachment_id}");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            "99999999-9999-9999-9999-999999999999",
            "44444444-4444-4444-4444-444444444444",
            "custom",
            "RCA folder item",
        )])),
        attachments: Arc::new(Mutex::new(HashMap::from([(
            message_id,
            vec![ActiveSyncAttachment {
                id: attachment_id,
                message_id,
                file_name: "brief.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                disposition: Some("attachment".to_string()),
                content_id: None,
                size_octets: 5,
                file_reference: file_reference.clone(),
            }],
        )]))),
        ..Default::default()
    };
    let attachments = store.attachments.clone();
    let service = ExchangeService::new(store);
    let request = format!(
        r#"<s:Envelope><s:Body><m:DeleteAttachment><m:AttachmentIds><t:AttachmentId Id="{file_reference}"/><t:AttachmentId Id="attachment:not-a-uuid"/></m:AttachmentIds></m:DeleteAttachment></s:Body></s:Envelope>"#
    );

    let response = service
        .handle(&bearer_headers(), request.as_bytes())
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert_eq!(attachments.lock().unwrap()[&message_id].len(), 1);
}

#[tokio::test]
async fn delete_item_removes_custom_mailbox_message() {
    let message_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            "99999999-9999-9999-9999-999999999999",
            "44444444-4444-4444-4444-444444444444",
            "custom",
            "RCA folder item",
        )])),
        ..Default::default()
    };
    let deleted_emails = store.deleted_emails.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:DeleteItem DeleteType="HardDelete"><m:ItemIds><t:ItemId Id="message:99999999-9999-9999-9999-999999999999"/></m:ItemIds></m:DeleteItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:DeleteItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert_eq!(deleted_emails.lock().unwrap().as_slice(), &[message_id]);
}

#[tokio::test]
async fn move_item_moves_custom_mailbox_message_to_target_folder() {
    let message_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap();
    let target_mailbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox("44444444-4444-4444-4444-444444444444", "custom", "RCA Sync"),
            FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "custom", "Archive"),
        ])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            "99999999-9999-9999-9999-999999999999",
            "44444444-4444-4444-4444-444444444444",
            "custom",
            "RCA folder item",
        )])),
        ..Default::default()
    };
    let moved_emails = store.moved_emails.clone();
    let emails = store.emails.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:MoveItem><m:ToFolderId><t:FolderId Id="mailbox:55555555-5555-5555-5555-555555555555"/></m:ToFolderId><m:ItemIds><t:ItemId Id="message:99999999-9999-9999-9999-999999999999"/></m:ItemIds></m:MoveItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:MoveItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("message:99999999-9999-9999-9999-999999999999"));
    assert!(body.contains("mailbox:55555555-5555-5555-5555-555555555555"));
    assert_eq!(
        moved_emails.lock().unwrap().as_slice(),
        &[(message_id, target_mailbox_id)]
    );
    let stored = emails.lock().unwrap();
    let moved = stored.iter().find(|email| email.id == message_id).unwrap();
    assert_eq!(moved.mailbox_ids, vec![target_mailbox_id]);
}

#[tokio::test]
async fn archive_item_moves_message_to_canonical_archive_mailbox() {
    let message_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap();
    let archive_mailbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox("44444444-4444-4444-4444-444444444444", "inbox", "Inbox"),
            FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "archive", "Archive"),
        ])),
        emails: Arc::new(Mutex::new(vec![{
            let mut message = FakeStore::email(
                "99999999-9999-9999-9999-999999999999",
                "44444444-4444-4444-4444-444444444444",
                "inbox",
                "Archive target",
            );
            message.bcc.push(JmapEmailAddress {
                address: "hidden-archive@example.test".to_string(),
                display_name: None,
            });
            message
        }])),
        ..Default::default()
    };
    let moved_emails = store.moved_emails.clone();
    let emails = store.emails.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:ArchiveItem><m:ArchiveSourceFolderId><t:FolderId Id="mailbox:44444444-4444-4444-4444-444444444444"/></m:ArchiveSourceFolderId><m:ItemIds><t:ItemId Id="message:99999999-9999-9999-9999-999999999999"/></m:ItemIds></m:ArchiveItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:ArchiveItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("message:99999999-9999-9999-9999-999999999999"));
    assert!(body.contains("mailbox:55555555-5555-5555-5555-555555555555"));
    assert!(!body.contains("hidden-archive@example.test"));
    assert_eq!(
        moved_emails.lock().unwrap().as_slice(),
        &[(message_id, archive_mailbox_id)]
    );
    let stored = emails.lock().unwrap();
    let archived = stored.iter().find(|email| email.id == message_id).unwrap();
    assert_eq!(archived.mailbox_id, archive_mailbox_id);
    assert_eq!(archived.mailbox_role, "archive");
}

#[tokio::test]
async fn archive_item_rejects_an_inaccessible_source_before_moving_or_projecting_bcc() {
    let message_id = Uuid::parse_str("99999999-9999-9999-9999-999999999998").unwrap();
    let archive_mailbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let mut message = FakeStore::email(
        "99999999-9999-9999-9999-999999999998",
        "44444444-4444-4444-4444-444444444444",
        "inbox",
        "Protected archive target",
    );
    message.bcc.push(JmapEmailAddress {
        address: "hidden-archive@example.test".to_string(),
        display_name: None,
    });
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox("44444444-4444-4444-4444-444444444444", "inbox", "Inbox"),
            FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "archive", "Archive"),
        ])),
        emails: Arc::new(Mutex::new(vec![message])),
        ..Default::default()
    };
    let moved_emails = store.moved_emails.clone();
    let emails = store.emails.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:ArchiveItem><m:ArchiveSourceFolderId><t:FolderId Id="mailbox:66666666-6666-6666-6666-666666666666"/></m:ArchiveSourceFolderId><m:ItemIds><t:ItemId Id="message:99999999-9999-9999-9999-999999999998"/></m:ItemIds></m:ArchiveItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:ArchiveItemResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(!body.contains("hidden-archive@example.test"));
    assert!(moved_emails.lock().unwrap().is_empty());
    assert_eq!(
        emails.lock().unwrap()[0].mailbox_ids,
        vec![Uuid::parse_str("44444444-4444-4444-4444-444444444444").unwrap()]
    );
    assert_ne!(emails.lock().unwrap()[0].mailbox_ids, vec![archive_mailbox_id]);
    assert_eq!(emails.lock().unwrap()[0].id, message_id);
}

#[tokio::test]
async fn archive_item_preflights_source_membership_for_the_entire_batch() {
    let inbox_message_id = Uuid::parse_str("99999999-9999-9999-9999-999999999997").unwrap();
    let other_message_id = Uuid::parse_str("99999999-9999-9999-9999-999999999996").unwrap();
    let inbox_mailbox_id = Uuid::parse_str("44444444-4444-4444-4444-444444444444").unwrap();
    let other_mailbox_id = Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap();
    let archive_mailbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox("44444444-4444-4444-4444-444444444444", "inbox", "Inbox"),
            FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "archive", "Archive"),
            FakeStore::mailbox("77777777-7777-7777-7777-777777777777", "", "Other"),
        ])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                "99999999-9999-9999-9999-999999999997",
                "44444444-4444-4444-4444-444444444444",
                "inbox",
                "Inbox archive target",
            ),
            FakeStore::email(
                "99999999-9999-9999-9999-999999999996",
                "77777777-7777-7777-7777-777777777777",
                "",
                "Other archive target",
            ),
        ])),
        ..Default::default()
    };
    let moved_emails = store.moved_emails.clone();
    let emails = store.emails.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:ArchiveItem><m:ArchiveSourceFolderId><t:FolderId Id="mailbox:44444444-4444-4444-4444-444444444444"/></m:ArchiveSourceFolderId><m:ItemIds><t:ItemId Id="message:99999999-9999-9999-9999-999999999997"/><t:ItemId Id="message:99999999-9999-9999-9999-999999999996"/></m:ItemIds></m:ArchiveItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:ArchiveItemResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(moved_emails.lock().unwrap().is_empty());
    let emails = emails.lock().unwrap();
    assert_eq!(
        emails
            .iter()
            .find(|email| email.id == inbox_message_id)
            .unwrap()
            .mailbox_ids,
        vec![inbox_mailbox_id]
    );
    assert_eq!(
        emails
            .iter()
            .find(|email| email.id == other_message_id)
            .unwrap()
            .mailbox_ids,
        vec![other_mailbox_id]
    );
    assert!(emails
        .iter()
        .all(|email| !email.mailbox_ids.contains(&archive_mailbox_id)));
}

#[tokio::test]
async fn move_item_moves_public_folder_item_to_target_public_folder() {
    let source_item_id = Uuid::parse_str("abababab-abab-abab-abab-abababababab").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![
            FakeStore::public_folder(
                "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                None,
                "Source Public",
            ),
            FakeStore::public_folder(
                "bbbbbbbb-cccc-dddd-eeee-ffffffffffff",
                None,
                "Target Public",
            ),
        ])),
        public_folder_items: Arc::new(Mutex::new(vec![FakeStore::public_folder_item(
            "abababab-abab-abab-abab-abababababab",
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "Public post",
        )])),
        ..Default::default()
    };
    let deleted_public_folder_items = store.deleted_public_folder_items.clone();
    let public_folder_items = store.public_folder_items.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:MoveItem><m:ToFolderId><t:FolderId Id="public-folder:bbbbbbbb-cccc-dddd-eeee-ffffffffffff"/></m:ToFolderId><m:ItemIds><t:ItemId Id="public-folder-item:abababab-abab-abab-abab-abababababab"/></m:ItemIds></m:MoveItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:MoveItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("public-folder-item:efefefef-efef-efef-efef-efefefefefef"));
    assert!(body.contains("public-folder:bbbbbbbb-cccc-dddd-eeee-ffffffffffff"));
    assert_eq!(
        deleted_public_folder_items.lock().unwrap().as_slice(),
        &[source_item_id]
    );
    let stored = public_folder_items.lock().unwrap();
    assert_eq!(stored.len(), 2);
    assert_eq!(stored[0].lifecycle_state, "deleted");
    assert_eq!(
        stored[1].public_folder_id,
        Uuid::parse_str("bbbbbbbb-cccc-dddd-eeee-ffffffffffff").unwrap()
    );
    assert_eq!(stored[1].subject, "Public post");
}

#[tokio::test]
async fn move_item_rejects_public_folder_target_without_write_access() {
    let non_owner = AuthenticatedAccount {
        tenant_id: FakeStore::account().tenant_id,
        account_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
        email: "delegate@example.test".to_string(),
        display_name: "Public Delegate".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let source = FakeStore::public_folder(
        "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        None,
        "Source Public",
    );
    let mut target = FakeStore::public_folder(
        "bbbbbbbb-cccc-dddd-eeee-ffffffffffff",
        None,
        "Target Public",
    );
    target.rights.may_write = false;
    target.rights.may_delete = false;
    target.rights.may_share = false;
    let store = FakeStore {
        session: Some(non_owner),
        public_folders: Arc::new(Mutex::new(vec![source, target])),
        public_folder_items: Arc::new(Mutex::new(vec![FakeStore::public_folder_item(
            "abababab-abab-abab-abab-abababababab",
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "Public post",
        )])),
        ..Default::default()
    };
    let deleted_public_folder_items = store.deleted_public_folder_items.clone();
    let public_folder_items = store.public_folder_items.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:MoveItem><m:ToFolderId><t:FolderId Id="public-folder:bbbbbbbb-cccc-dddd-eeee-ffffffffffff"/></m:ToFolderId><m:ItemIds><t:ItemId Id="public-folder-item:abababab-abab-abab-abab-abababababab"/></m:ItemIds></m:MoveItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:MoveItemResponse>"));
    assert!(body.contains("<m:ResponseCode>ErrorAccessDenied</m:ResponseCode>"));
    assert!(body.contains("public folder write access is not granted"));
    assert!(deleted_public_folder_items.lock().unwrap().is_empty());
    let stored = public_folder_items.lock().unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].lifecycle_state, "active");
    assert_eq!(
        stored[0].public_folder_id,
        Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap()
    );
}

#[tokio::test]
async fn move_item_rejects_non_message_ids() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-5555-5555-555555555555",
            "custom",
            "Archive",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:MoveItem><m:ToFolderId><t:FolderId Id="mailbox:55555555-5555-5555-5555-555555555555"/></m:ToFolderId><m:ItemIds><t:ItemId Id="contact:cccccccc-cccc-cccc-cccc-cccccccccccc"/></m:ItemIds></m:MoveItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:MoveItemResponse>"));
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert!(body.contains("supports only canonical message ids"));
}

#[tokio::test]
async fn copy_item_copies_custom_mailbox_message_to_target_folder() {
    let message_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap();
    let target_mailbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox("44444444-4444-4444-4444-444444444444", "custom", "RCA Sync"),
            FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "custom", "Archive"),
        ])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            "99999999-9999-9999-9999-999999999999",
            "44444444-4444-4444-4444-444444444444",
            "custom",
            "RCA folder item",
        )])),
        ..Default::default()
    };
    let copied_emails = store.copied_emails.clone();
    let emails = store.emails.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:CopyItem><m:ToFolderId><t:FolderId Id="mailbox:55555555-5555-5555-5555-555555555555"/></m:ToFolderId><m:ItemIds><t:ItemId Id="message:99999999-9999-9999-9999-999999999999"/></m:ItemIds></m:CopyItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:CopyItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("message:99999999-9999-9999-9999-999999999999"));
    assert!(body.contains("mailbox:55555555-5555-5555-5555-555555555555"));
    assert_eq!(
        copied_emails.lock().unwrap().as_slice(),
        &[(message_id, target_mailbox_id)]
    );
    let stored = emails.lock().unwrap();
    let source = stored.iter().find(|email| email.id == message_id).unwrap();
    let copy = stored
        .iter()
        .find(|email| email.id != message_id && email.mailbox_id == target_mailbox_id)
        .unwrap();
    assert_eq!(source.mailbox_ids, vec![source.mailbox_id]);
    assert_eq!(copy.mailbox_id, target_mailbox_id);
}

#[tokio::test]
async fn copy_item_rejects_misplaced_item_ids_without_copying_a_message() {
    let source_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap();
    let target_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &target_id.to_string(),
            "custom",
            "Copy target",
        )])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            &source_id.to_string(),
            "44444444-4444-4444-4444-444444444444",
            "inbox",
            "Copy source",
        )])),
        ..Default::default()
    };
    let copied_emails = store.copied_emails.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:CopyItem><m:ToFolderId><t:FolderId Id="mailbox:{target_id}"/></m:ToFolderId><m:Unexpected><m:ItemIds><t:ItemId Id="message:{source_id}"/></m:ItemIds></m:Unexpected></m:CopyItem></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:CopyItemResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(body.contains("CopyItem requires exactly one direct ItemIds collection"));
    assert!(copied_emails.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mark_all_items_as_read_updates_canonical_mailbox_message_flags() {
    let mailbox_id = "55555555-5555-5555-5555-555555555555";
    let other_mailbox_id = "66666666-6666-6666-6666-666666666666";
    let message_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap();
    let other_message_id = Uuid::parse_str("88888888-8888-8888-8888-888888888888").unwrap();
    let mut unread = FakeStore::email(
        &message_id.to_string(),
        mailbox_id,
        "custom",
        "Unread payload",
    );
    unread.unread = true;
    let mut other = FakeStore::email(
        &other_message_id.to_string(),
        other_mailbox_id,
        "custom",
        "Other payload",
    );
    other.unread = true;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(mailbox_id, "custom", "RCA Sync"),
            FakeStore::mailbox(other_mailbox_id, "custom", "Other"),
        ])),
        emails: Arc::new(Mutex::new(vec![unread, other])),
        ..Default::default()
    };
    let service = ExchangeService::new(store.clone());

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:MarkAllItemsAsRead><m:ReadFlag>true</m:ReadFlag><m:FolderIds><t:FolderId Id="mailbox:{mailbox_id}"/></m:FolderIds></m:MarkAllItemsAsRead></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:MarkAllItemsAsReadResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    let emails = store.emails.lock().unwrap().clone();
    assert!(
        !emails
            .iter()
            .find(|email| email.id == message_id)
            .unwrap()
            .unread
    );
    assert!(
        emails
            .iter()
            .find(|email| email.id == other_message_id)
            .unwrap()
            .unread
    );

    let response = service
        .handle(
            &bearer_headers(),
            format!(
                r#"<s:Envelope><s:Body><m:MarkAllItemsAsRead><m:ReadFlag>false</m:ReadFlag><m:FolderIds><t:FolderId Id="mailbox:{mailbox_id}"/></m:FolderIds></m:MarkAllItemsAsRead></s:Body></s:Envelope>"#
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:MarkAllItemsAsReadResponse>"));
    assert!(
        store
            .emails
            .lock()
            .unwrap()
            .iter()
            .find(|email| email.id == message_id)
            .unwrap()
            .unread
    );
}

#[tokio::test]
async fn mark_all_items_as_read_changes_only_the_requested_visible_membership() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let archive_id = Uuid::parse_str("66666666-6666-6666-6666-666666666666").unwrap();
    let message_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap();
    let mut email = FakeStore::email(
        &message_id.to_string(),
        &inbox_id.to_string(),
        "inbox",
        "Shared",
    );
    email.unread = true;
    email.mailbox_ids.push(archive_id);
    let mut archive_state = email.mailbox_states[0].clone();
    archive_state.mailbox_id = archive_id;
    archive_state.role = "archive".to_string();
    archive_state.unread = true;
    email.mailbox_states.push(archive_state);
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox"),
            FakeStore::mailbox(&archive_id.to_string(), "archive", "Archive"),
        ])),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    let service = ExchangeService::new(store.clone());

    let response = service
        .handle(
            &bearer_headers(),
            format!(r#"<s:Envelope><s:Body><m:MarkAllItemsAsRead><m:ReadFlag>true</m:ReadFlag><m:FolderIds><t:FolderId Id="mailbox:{inbox_id}"/></m:FolderIds></m:MarkAllItemsAsRead></s:Body></s:Envelope>"#).as_bytes(),
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    let email = store.emails.lock().unwrap()[0].clone();
    assert!(
        !email
            .mailbox_states
            .iter()
            .find(|state| state.mailbox_id == inbox_id)
            .unwrap()
            .unread
    );
    assert!(
        email
            .mailbox_states
            .iter()
            .find(|state| state.mailbox_id == archive_id)
            .unwrap()
            .unread
    );
}

#[tokio::test]
async fn empty_and_update_folder_reject_protected_or_invalid_requests_before_mutation() {
    let inbox_id = "11111111-1111-1111-1111-111111111111";
    let custom_id = "22222222-2222-2222-2222-222222222222";
    let message_id = "33333333-3333-3333-3333-333333333333";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(inbox_id, "inbox", "Inbox"),
            FakeStore::mailbox(custom_id, "custom", "Projects"),
        ])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            message_id,
            inbox_id,
            "inbox",
            "Protected",
        )])),
        ..Default::default()
    };
    let deleted_emails = store.deleted_emails.clone();
    let updated_mailboxes = store.updated_mailboxes.clone();
    let service = ExchangeService::new(store);

    for request in [
        format!(
            r#"<s:Envelope><s:Body><m:EmptyFolder DeleteType="HardDelete"><m:FolderIds><t:FolderId Id="mailbox:{inbox_id}"/></m:FolderIds></m:EmptyFolder></s:Body></s:Envelope>"#
        ),
        format!(
            r#"<s:Envelope><s:Body><m:UpdateFolder><m:FolderChanges><t:FolderChange><t:FolderId Id="mailbox:{custom_id}"/><t:Updates><t:SetFolderField><t:FieldURI FieldURI="folder:FolderClass"/><t:Folder><t:DisplayName>Renamed</t:DisplayName></t:Folder></t:SetFolderField></t:Updates></t:FolderChange></m:FolderChanges></m:UpdateFolder></s:Body></s:Envelope>"#
        ),
    ] {
        let response = service
            .handle(&bearer_headers(), request.as_bytes())
            .await
            .unwrap();
        let body = response_text(response).await;
        assert!(body.contains("ResponseClass=\"Error\""));
    }
    assert!(deleted_emails.lock().unwrap().is_empty());
    assert!(updated_mailboxes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn find_conversation_uses_current_canonical_memberships_and_rejects_invalid_parent() {
    let inbox_id = Uuid::parse_str("44444444-4444-4444-4444-444444444444").unwrap();
    let archive_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let message_id = Uuid::parse_str("66666666-6666-6666-6666-666666666666").unwrap();
    let thread_id = Uuid::parse_str("aaaaaaaa-1111-1111-1111-111111111111").unwrap();
    let mut email = FakeStore::email(
        &message_id.to_string(),
        &inbox_id.to_string(),
        "inbox",
        "Copied",
    );
    email.thread_id = thread_id;
    email.mailbox_ids.push(archive_id);
    let mut archive_state = email.mailbox_states[0].clone();
    archive_state.mailbox_id = archive_id;
    archive_state.role = "archive".to_string();
    email.mailbox_states.push(archive_state);
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox"),
            FakeStore::mailbox(&archive_id.to_string(), "archive", "Archive"),
        ])),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service.handle(&bearer_headers(), format!(r#"<s:Envelope><s:Body><m:FindConversation><m:ParentFolderId><t:FolderId Id="mailbox:{archive_id}"/></m:ParentFolderId></m:FindConversation></s:Body></s:Envelope>"#).as_bytes()).await.unwrap();
    let body = response_text(response).await;
    assert!(body.contains(&format!("conversation:{thread_id}")));

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:FindConversation/></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(!body.contains(&message_id.to_string()));
}

#[tokio::test]
async fn convert_id_is_stateless_for_a_valid_inaccessible_canonical_id() {
    let service = ExchangeService::new(FakeStore {
        session: Some(FakeStore::account()),
        fail_fetch_all_jmap_email_ids: true,
        ..Default::default()
    });
    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:ConvertId DestinationFormat="EntryId"><m:SourceIds><t:AlternateId Format="EwsId" Id="message:99999999-9999-9999-9999-999999999999"/></m:SourceIds></m:ConvertId></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("LPEEWS1."));
}

#[tokio::test]
async fn sync_folder_items_replays_a_moved_membership_as_a_target_create() {
    let mailbox_id = Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap();
    let message_id = Uuid::parse_str("88888888-8888-8888-8888-888888888888").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &mailbox_id.to_string(),
            "custom",
            "Target",
        )])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            &message_id.to_string(),
            &mailbox_id.to_string(),
            "custom",
            "Moved",
        )])),
        mapi_notification_cursor: Arc::new(Mutex::new(Some(7))),
        ews_mailbox_item_sync_replays: Arc::new(Mutex::new(vec![EwsNotificationReplay {
            expired: false,
            current_cursor: Some(8),
            next_cursor: 8,
            more_events: false,
            events: vec![EwsNotificationLogEvent {
                cursor: 8,
                mailbox_id,
                message_id,
                change_kind: "moved".to_string(),
                modseq: 8,
                created_at: "2026-08-17T08:00:00.000000Z".to_string(),
            }],
        }])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let initial = service.handle(&bearer_headers(), format!(r#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="mailbox:{mailbox_id}"/></m:SyncFolderId></m:SyncFolderItems></s:Body></s:Envelope>"#).as_bytes()).await.unwrap();
    let sync_state = test_xml_text(&response_text(initial).await, "SyncState").unwrap();
    let response = service.handle(&bearer_headers(), format!(r#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncState>{sync_state}</m:SyncState><m:SyncFolderId><t:FolderId Id="mailbox:{mailbox_id}"/></m:SyncFolderId></m:SyncFolderItems></s:Body></s:Envelope>"#).as_bytes()).await.unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:Create><t:Message>"));
    assert!(body.contains(&format!("message:{message_id}")));
}

#[tokio::test]
async fn copy_item_copies_public_folder_item_to_target_public_folder() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![
            FakeStore::public_folder(
                "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                None,
                "Source Public",
            ),
            FakeStore::public_folder(
                "bbbbbbbb-cccc-dddd-eeee-ffffffffffff",
                None,
                "Target Public",
            ),
        ])),
        public_folder_items: Arc::new(Mutex::new(vec![FakeStore::public_folder_item(
            "abababab-abab-abab-abab-abababababab",
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "Public post",
        )])),
        ..Default::default()
    };
    let public_folder_items = store.public_folder_items.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:CopyItem><m:ToFolderId><t:FolderId Id="public-folder:bbbbbbbb-cccc-dddd-eeee-ffffffffffff"/></m:ToFolderId><m:ItemIds><t:ItemId Id="public-folder-item:abababab-abab-abab-abab-abababababab"/></m:ItemIds></m:CopyItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:CopyItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("public-folder-item:efefefef-efef-efef-efef-efefefefefef"));
    assert!(body.contains("public-folder:bbbbbbbb-cccc-dddd-eeee-ffffffffffff"));
    let stored = public_folder_items.lock().unwrap();
    assert_eq!(stored.len(), 2);
    assert_eq!(stored[0].lifecycle_state, "active");
    assert_eq!(
        stored[1].public_folder_id,
        Uuid::parse_str("bbbbbbbb-cccc-dddd-eeee-ffffffffffff").unwrap()
    );
    assert_eq!(stored[1].subject, "Public post");
}

#[tokio::test]
async fn copy_item_rejects_public_folder_target_without_write_access() {
    let non_owner = AuthenticatedAccount {
        tenant_id: FakeStore::account().tenant_id,
        account_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
        email: "delegate@example.test".to_string(),
        display_name: "Public Delegate".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let source = FakeStore::public_folder(
        "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        None,
        "Source Public",
    );
    let mut target = FakeStore::public_folder(
        "bbbbbbbb-cccc-dddd-eeee-ffffffffffff",
        None,
        "Target Public",
    );
    target.rights.may_write = false;
    target.rights.may_delete = false;
    target.rights.may_share = false;
    let store = FakeStore {
        session: Some(non_owner),
        public_folders: Arc::new(Mutex::new(vec![source, target])),
        public_folder_items: Arc::new(Mutex::new(vec![FakeStore::public_folder_item(
            "abababab-abab-abab-abab-abababababab",
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "Public post",
        )])),
        ..Default::default()
    };
    let public_folder_items = store.public_folder_items.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:CopyItem><m:ToFolderId><t:FolderId Id="public-folder:bbbbbbbb-cccc-dddd-eeee-ffffffffffff"/></m:ToFolderId><m:ItemIds><t:ItemId Id="public-folder-item:abababab-abab-abab-abab-abababababab"/></m:ItemIds></m:CopyItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:CopyItemResponse>"));
    assert!(body.contains("<m:ResponseCode>ErrorAccessDenied</m:ResponseCode>"));
    assert!(body.contains("public folder write access is not granted"));
    let stored = public_folder_items.lock().unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(
        stored[0].public_folder_id,
        Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap()
    );
}

#[tokio::test]
async fn copy_item_rejects_non_message_ids() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-5555-5555-555555555555",
            "custom",
            "Archive",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:CopyItem><m:ToFolderId><t:FolderId Id="mailbox:55555555-5555-5555-5555-555555555555"/></m:ToFolderId><m:ItemIds><t:ItemId Id="event:cccccccc-cccc-cccc-cccc-cccccccccccc"/></m:ItemIds></m:CopyItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:CopyItemResponse>"));
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert!(body.contains("supports only canonical message ids"));
}

#[tokio::test]
async fn find_item_returns_calendar_items_from_canonical_store() {
    let event_id = Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap();
    let collection = FakeStore::collection("default", "calendar", "Calendar");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        calendar_collections: Arc::new(Mutex::new(vec![collection.clone()])),
        events: Arc::new(Mutex::new(vec![AccessibleEvent {
            id: event_id,
            uid: event_id.to_string(),
            collection_id: collection.id.clone(),
            owner_account_id: collection.owner_account_id,
            owner_email: collection.owner_email.clone(),
            owner_display_name: collection.owner_display_name.clone(),
            rights: collection.rights.clone(),
            date: "2026-05-04".to_string(),
            time: "23:30".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 1_560,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Planning".to_string(),
            location: "Room 1".to_string(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: String::new(),
            notes: "Agenda".to_string(),
            body_html: String::new(),
        }])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:FindItem><m:ParentFolderIds><t:DistinguishedFolderId Id="calendar"/></m:ParentFolderIds></m:FindItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:FindItemResponse>"));
    assert!(body.contains("<t:CalendarItem>"));
    assert!(body.contains("event:cccccccc-cccc-cccc-cccc-cccccccccccc"));
    assert!(body.contains("<t:Start>2026-05-04T23:30:00Z</t:Start>"));
    assert!(body.contains("<t:End>2026-05-06T01:30:00Z</t:End>"));
}

fn retention_policy_tag(
    id: Uuid,
    display_name: &str,
    tag_type: &str,
    action: &str,
    retention_days: Option<i32>,
    is_visible: bool,
    description: &str,
) -> EwsRetentionPolicyTag {
    EwsRetentionPolicyTag {
        id,
        display_name: display_name.to_string(),
        tag_type: tag_type.to_string(),
        action: action.to_string(),
        retention_days,
        is_visible,
        description: description.to_string(),
        opted_into: false,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EwsCatalogCoverageKind {
    Behavioral,
    Unsupported,
}

#[derive(Clone, Copy, Debug)]
struct EwsCatalogCoverage {
    operation: &'static str,
    kind: EwsCatalogCoverageKind,
    test_name: &'static str,
}

const MICROSOFT_EWS_OPERATION_CATALOG_SOURCE: &str =
    "https://learn.microsoft.com/en-us/exchange/client-developer/web-service-reference/ews-operations-in-exchange";

const MICROSOFT_EWS_OPERATION_CATALOG_LAST_UPDATED: &str = "2023-03-29";

const MICROSOFT_EWS_OPERATION_CATALOG: &[&str] = &[
    "AddDelegate",
    "AddDistributionGroupToImList",
    "AddImContactToGroup",
    "AddImGroup",
    "AddNewImContactToGroup",
    "AddNewTelUriContactToGroup",
    "ApplyConversationAction",
    "ArchiveItem",
    "ConvertId",
    "CopyFolder",
    "CopyItem",
    "CreateAttachment",
    "CreateFolder",
    "CreateFolderPath",
    "CreateItem",
    "CreateManagedFolder",
    "CreateUserConfiguration",
    "DeleteAttachment",
    "DeleteFolder",
    "DeleteItem",
    "DeleteUserConfiguration",
    "DisableApp",
    "DisconnectPhoneCall",
    "EmptyFolder",
    "ExpandDL",
    "ExportItems",
    "FindConversation",
    "FindFolder",
    "FindItem",
    "FindMessageTrackingReport",
    "FindPeople",
    "GetAppManifests",
    "GetAppMarketplaceUrl",
    "GetAttachment",
    "GetClientAccessToken",
    "GetConversationItems",
    "GetDelegate",
    "GetDiscoverySearchConfiguration",
    "GetEvents",
    "GetFolder",
    "GetHoldOnMailboxes",
    "GetImItemList",
    "GetImItems",
    "GetInboxRules",
    "GetItem",
    "GetMailTips",
    "GetMessageTrackingReport",
    "GetNonIndexableItemDetails",
    "GetNonIndexableItemStatistics",
    "GetPasswordExpirationDate",
    "GetPersona",
    "GetPhoneCallInformation",
    "GetReminders",
    "GetRoomLists",
    "GetRooms",
    "GetSearchableMailboxes",
    "GetServerTimeZones",
    "GetServiceConfiguration",
    "GetSharingFolder",
    "GetSharingMetadata",
    "GetStreamingEvents",
    "GetUserAvailability",
    "GetUserConfiguration",
    "GetUserOofSettings",
    "GetUserPhoto",
    "GetUserRetentionPolicyTags",
    "InstallApp",
    "MarkAllItemsAsRead",
    "MarkAsJunk",
    "MoveFolder",
    "MoveItem",
    "PerformReminderAction",
    "PlayOnPhone",
    "RefreshSharingFolder",
    "RemoveContactFromImList",
    "RemoveDelegate",
    "RemoveDistributionGroupFromImList",
    "RemoveImContactFromGroup",
    "RemoveImGroup",
    "ResolveNames",
    "SearchMailboxes",
    "SendItem",
    "SetHoldOnMailboxes",
    "SetImGroup",
    "SetUserOofSettings",
    "Subscribe",
    "SyncFolderHierarchy",
    "SyncFolderItems",
    "UninstallApp",
    "Unsubscribe",
    "UpdateDelegate",
    "UpdateFolder",
    "UpdateInboxRules",
    "UpdateItem",
    "UpdateUserConfiguration",
    "UploadItems",
];

const EWS_UNSUPPORTED_REASONS: &[(&str, &str)] = &[];

const EWS_CATALOG_COVERAGE: &[EwsCatalogCoverage] = &[
    EwsCatalogCoverage {
        operation: "AddDelegate",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "delegate_operations_use_canonical_permissions_and_preferences",
    },
    EwsCatalogCoverage {
        operation: "AddDistributionGroupToImList",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "ucs_distribution_list_membership_stays_tenant_scoped",
    },
    EwsCatalogCoverage {
        operation: "AddImContactToGroup",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "ucs_im_group_operations_use_canonical_contact_group_state",
    },
    EwsCatalogCoverage {
        operation: "AddImGroup",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "ucs_im_group_operations_use_canonical_contact_group_state",
    },
    EwsCatalogCoverage {
        operation: "AddNewImContactToGroup",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "ucs_im_group_operations_use_canonical_contact_group_state",
    },
    EwsCatalogCoverage {
        operation: "AddNewTelUriContactToGroup",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "ucs_im_group_operations_use_canonical_contact_group_state",
    },
    EwsCatalogCoverage {
        operation: "ApplyConversationAction",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "apply_conversation_action_moves_current_thread_messages",
    },
    EwsCatalogCoverage {
        operation: "ArchiveItem",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "archive_item_moves_message_to_canonical_archive_mailbox",
    },
    EwsCatalogCoverage {
        operation: "ConvertId",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "convert_id_round_trips_supported_canonical_object_families",
    },
    EwsCatalogCoverage {
        operation: "CopyFolder",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "copy_move_and_update_folder_use_canonical_mailbox_changes",
    },
    EwsCatalogCoverage {
        operation: "CopyItem",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "copy_item_copies_custom_mailbox_message_to_target_folder",
    },
    EwsCatalogCoverage {
        operation: "CreateAttachment",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "create_attachment_validates_and_adds_canonical_attachment",
    },
    EwsCatalogCoverage {
        operation: "CreateFolder",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "create_folder_uses_canonical_mailbox_store",
    },
    EwsCatalogCoverage {
        operation: "CreateFolderPath",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "create_folder_path_creates_nested_mailboxes_and_sync_reports_changes",
    },
    EwsCatalogCoverage {
        operation: "CreateItem",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "create_item_saveonly_stores_message_as_canonical_draft",
    },
    EwsCatalogCoverage {
        operation: "CreateManagedFolder",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "create_managed_folder_uses_canonical_retention_folder_api",
    },
    EwsCatalogCoverage {
        operation: "CreateUserConfiguration",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "user_configuration_create_get_update_and_delete_use_canonical_storage",
    },
    EwsCatalogCoverage {
        operation: "DeleteAttachment",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "delete_attachment_removes_canonical_attachment_reference",
    },
    EwsCatalogCoverage {
        operation: "DeleteFolder",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "delete_folder_uses_canonical_mailbox_destroy",
    },
    EwsCatalogCoverage {
        operation: "DeleteItem",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "delete_item_hard_deletes_canonical_message",
    },
    EwsCatalogCoverage {
        operation: "DeleteUserConfiguration",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "user_configuration_create_get_update_and_delete_use_canonical_storage",
    },
    EwsCatalogCoverage {
        operation: "DisableApp",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "mail_app_operations_use_canonical_catalog_install_and_token_state",
    },
    EwsCatalogCoverage {
        operation: "DisconnectPhoneCall",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "unified_messaging_operations_use_canonical_call_state",
    },
    EwsCatalogCoverage {
        operation: "EmptyFolder",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "empty_folder_deletes_messages_and_subfolders_through_canonical_paths",
    },
    EwsCatalogCoverage {
        operation: "ExpandDL",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "expand_dl_projects_same_tenant_directory_group_members",
    },
    EwsCatalogCoverage {
        operation: "ExportItems",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "bulk_transfer_operations_record_canonical_transfer_jobs",
    },
    EwsCatalogCoverage {
        operation: "FindConversation",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "find_conversation_groups_messages_by_canonical_thread_in_folder",
    },
    EwsCatalogCoverage {
        operation: "FindFolder",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "find_folder_lists_contact_and_calendar_folders",
    },
    EwsCatalogCoverage {
        operation: "FindItem",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "find_item_lists_custom_mailbox_messages",
    },
    EwsCatalogCoverage {
        operation: "FindMessageTrackingReport",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "message_tracking_reports_project_canonical_trace_state",
    },
    EwsCatalogCoverage {
        operation: "FindPeople",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "find_people_projects_canonical_accounts_and_contacts",
    },
    EwsCatalogCoverage {
        operation: "GetAppManifests",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "mail_app_operations_use_canonical_catalog_install_and_token_state",
    },
    EwsCatalogCoverage {
        operation: "GetAppMarketplaceUrl",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "mail_app_operations_use_canonical_catalog_install_and_token_state",
    },
    EwsCatalogCoverage {
        operation: "GetAttachment",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "get_attachment_returns_canonical_attachment_content",
    },
    EwsCatalogCoverage {
        operation: "GetClientAccessToken",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "mail_app_operations_use_canonical_catalog_install_and_token_state",
    },
    EwsCatalogCoverage {
        operation: "GetConversationItems",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "get_conversation_items_returns_current_canonical_thread_nodes",
    },
    EwsCatalogCoverage {
        operation: "GetDelegate",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "delegate_operations_use_canonical_permissions_and_preferences",
    },
    EwsCatalogCoverage {
        operation: "GetDiscoverySearchConfiguration",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name:
            "ediscovery_configuration_and_searchable_mailboxes_project_canonical_compliance_state",
    },
    EwsCatalogCoverage {
        operation: "GetEvents",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "pull_subscription_get_events_replays_canonical_changes_after_restart",
    },
    EwsCatalogCoverage {
        operation: "GetFolder",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "get_folder_returns_multiple_supported_folder_kinds",
    },
    EwsCatalogCoverage {
        operation: "GetHoldOnMailboxes",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "hold_operations_use_canonical_compliance_hold_state",
    },
    EwsCatalogCoverage {
        operation: "GetImItemList",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "ucs_im_group_operations_use_canonical_contact_group_state",
    },
    EwsCatalogCoverage {
        operation: "GetImItems",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "ucs_im_group_operations_use_canonical_contact_group_state",
    },
    EwsCatalogCoverage {
        operation: "GetInboxRules",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "inbox_rules_project_and_update_canonical_sieve_rules",
    },
    EwsCatalogCoverage {
        operation: "GetItem",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "get_item_returns_custom_mailbox_message_body",
    },
    EwsCatalogCoverage {
        operation: "GetMailTips",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "get_mail_tips_projects_directory_and_oof_without_local_tip_state",
    },
    EwsCatalogCoverage {
        operation: "GetMessageTrackingReport",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "message_tracking_reports_project_canonical_trace_state",
    },
    EwsCatalogCoverage {
        operation: "GetNonIndexableItemDetails",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "non_indexable_reports_project_canonical_search_diagnostics",
    },
    EwsCatalogCoverage {
        operation: "GetNonIndexableItemStatistics",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "non_indexable_reports_project_canonical_search_diagnostics",
    },
    EwsCatalogCoverage {
        operation: "GetPasswordExpirationDate",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "get_password_expiration_date_returns_parseable_canonical_account_gap",
    },
    EwsCatalogCoverage {
        operation: "GetPersona",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "get_persona_resolves_only_visible_stateless_persona_ids",
    },
    EwsCatalogCoverage {
        operation: "GetPhoneCallInformation",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "unified_messaging_operations_use_canonical_call_state",
    },
    EwsCatalogCoverage {
        operation: "GetReminders",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "reminders_are_read_and_dismissed_from_canonical_reminder_state",
    },
    EwsCatalogCoverage {
        operation: "GetRoomLists",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "rooms_are_projected_from_canonical_directory_entries",
    },
    EwsCatalogCoverage {
        operation: "GetRooms",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "rooms_are_projected_from_canonical_directory_entries",
    },
    EwsCatalogCoverage {
        operation: "GetSearchableMailboxes",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name:
            "ediscovery_configuration_and_searchable_mailboxes_project_canonical_compliance_state",
    },
    EwsCatalogCoverage {
        operation: "GetServerTimeZones",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "get_server_time_zones_returns_minimal_definitions",
    },
    EwsCatalogCoverage {
        operation: "GetServiceConfiguration",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "get_service_configuration_reports_bounded_mail_tips_and_parseable_gaps",
    },
    EwsCatalogCoverage {
        operation: "GetSharingFolder",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "get_sharing_folder_returns_accessible_same_tenant_calendar_grant",
    },
    EwsCatalogCoverage {
        operation: "GetSharingMetadata",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "get_sharing_metadata_returns_owned_calendar_metadata_without_exchange_tokens",
    },
    EwsCatalogCoverage {
        operation: "GetStreamingEvents",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "pull_and_streaming_notifications_replay_canonical_sql_change_cursor",
    },
    EwsCatalogCoverage {
        operation: "GetUserAvailability",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "get_user_availability_returns_canonical_busy_events",
    },
    EwsCatalogCoverage {
        operation: "GetUserConfiguration",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "user_configuration_create_get_update_and_delete_use_canonical_storage",
    },
    EwsCatalogCoverage {
        operation: "GetUserOofSettings",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "get_user_oof_settings_projects_canonical_sieve_vacation",
    },
    EwsCatalogCoverage {
        operation: "GetUserPhoto",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "get_user_photo_returns_parseable_canonical_photo_gap",
    },
    EwsCatalogCoverage {
        operation: "GetUserRetentionPolicyTags",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "get_user_retention_policy_tags_projects_same_tenant_assignment_visibility",
    },
    EwsCatalogCoverage {
        operation: "InstallApp",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "mail_app_operations_use_canonical_catalog_install_and_token_state",
    },
    EwsCatalogCoverage {
        operation: "MarkAllItemsAsRead",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "mark_all_items_as_read_updates_canonical_mailbox_message_flags",
    },
    EwsCatalogCoverage {
        operation: "MarkAsJunk",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "mark_as_junk_moves_messages_to_canonical_junk_mailbox",
    },
    EwsCatalogCoverage {
        operation: "MoveFolder",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "copy_move_and_update_folder_use_canonical_mailbox_changes",
    },
    EwsCatalogCoverage {
        operation: "MoveItem",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "move_item_moves_custom_mailbox_message_to_target_folder",
    },
    EwsCatalogCoverage {
        operation: "PerformReminderAction",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "reminders_are_read_and_dismissed_from_canonical_reminder_state",
    },
    EwsCatalogCoverage {
        operation: "PlayOnPhone",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "unified_messaging_operations_use_canonical_call_state",
    },
    EwsCatalogCoverage {
        operation: "RefreshSharingFolder",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "refresh_sharing_folder_verifies_accessible_shared_contacts_folder",
    },
    EwsCatalogCoverage {
        operation: "RemoveContactFromImList",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "ucs_im_group_operations_use_canonical_contact_group_state",
    },
    EwsCatalogCoverage {
        operation: "RemoveDelegate",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "delegate_operations_use_canonical_permissions_and_preferences",
    },
    EwsCatalogCoverage {
        operation: "RemoveDistributionGroupFromImList",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "ucs_distribution_list_membership_stays_tenant_scoped",
    },
    EwsCatalogCoverage {
        operation: "RemoveImContactFromGroup",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "ucs_im_group_operations_use_canonical_contact_group_state",
    },
    EwsCatalogCoverage {
        operation: "RemoveImGroup",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "ucs_im_group_operations_use_canonical_contact_group_state",
    },
    EwsCatalogCoverage {
        operation: "ResolveNames",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "resolve_names_returns_tenant_directory_account_match",
    },
    EwsCatalogCoverage {
        operation: "SearchMailboxes",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "search_mailboxes_records_canonical_discovery_search_results_without_bcc",
    },
    EwsCatalogCoverage {
        operation: "SendItem",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "send_item_submits_existing_draft_through_canonical_submission",
    },
    EwsCatalogCoverage {
        operation: "SetHoldOnMailboxes",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "hold_operations_use_canonical_compliance_hold_state",
    },
    EwsCatalogCoverage {
        operation: "SetImGroup",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "ucs_im_group_operations_use_canonical_contact_group_state",
    },
    EwsCatalogCoverage {
        operation: "SetUserOofSettings",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "set_user_oof_settings_writes_canonical_sieve_vacation",
    },
    EwsCatalogCoverage {
        operation: "Subscribe",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "pull_subscription_get_events_and_unsubscribe_return_status_flow",
    },
    EwsCatalogCoverage {
        operation: "SyncFolderHierarchy",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "sync_folder_hierarchy_lists_contact_and_calendar_folders",
    },
    EwsCatalogCoverage {
        operation: "SyncFolderItems",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "sync_folder_items_returns_contacts_from_canonical_store",
    },
    EwsCatalogCoverage {
        operation: "UninstallApp",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "mail_app_operations_use_canonical_catalog_install_and_token_state",
    },
    EwsCatalogCoverage {
        operation: "Unsubscribe",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "pull_subscription_get_events_and_unsubscribe_return_status_flow",
    },
    EwsCatalogCoverage {
        operation: "UpdateDelegate",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "delegate_operations_use_canonical_permissions_and_preferences",
    },
    EwsCatalogCoverage {
        operation: "UpdateFolder",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "copy_move_and_update_folder_use_canonical_mailbox_changes",
    },
    EwsCatalogCoverage {
        operation: "UpdateInboxRules",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "inbox_rules_project_and_update_canonical_sieve_rules",
    },
    EwsCatalogCoverage {
        operation: "UpdateItem",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "update_item_updates_message_read_and_flag_state",
    },
    EwsCatalogCoverage {
        operation: "UpdateUserConfiguration",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "user_configuration_create_get_update_and_delete_use_canonical_storage",
    },
    EwsCatalogCoverage {
        operation: "UploadItems",
        kind: EwsCatalogCoverageKind::Behavioral,
        test_name: "bulk_transfer_operations_record_canonical_transfer_jobs",
    },
];

#[tokio::test]
async fn ews_catalog_gate_covers_documented_operations_and_unsupported_gaps() {
    let documented = documented_ews_operation_names();
    assert_eq!(
        MICROSOFT_EWS_OPERATION_CATALOG_SOURCE,
        "https://learn.microsoft.com/en-us/exchange/client-developer/web-service-reference/ews-operations-in-exchange"
    );
    assert_eq!(MICROSOFT_EWS_OPERATION_CATALOG_LAST_UPDATED, "2023-03-29");

    let parity_matrix = parity_matrix_ews_operation_names();
    assert_eq!(
        documented, parity_matrix,
        "docs/architecture/ews-operation-contract.md must list exactly the Microsoft EWS operation catalog snapshot"
    );

    let mut covered = std::collections::BTreeSet::new();
    let mut duplicate_coverage = Vec::new();
    for entry in EWS_CATALOG_COVERAGE {
        if !covered.insert(entry.operation) {
            duplicate_coverage.push(entry.operation);
        }
    }
    assert!(
        duplicate_coverage.is_empty(),
        "duplicate EWS catalog coverage entries: {duplicate_coverage:?}"
    );
    assert_eq!(
        documented, covered,
        "EWS operation catalog coverage manifest must match the Microsoft EWS operation catalog snapshot"
    );

    let contract_status_entries = contract_ews_operation_status_entries();
    let contract_statuses = contract_status_entries
        .iter()
        .copied()
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(
        contract_status_entries.len(),
        contract_statuses.len(),
        "docs/architecture/ews-operation-contract.md must not duplicate catalog operation status rows"
    );
    let contract_operations = contract_statuses
        .keys()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        documented, contract_operations,
        "docs/architecture/ews-operation-contract.md statuses must cover exactly the Microsoft EWS operation catalog snapshot"
    );

    let mut manifest_status_counts = std::collections::BTreeMap::new();
    let mut contract_status_counts = std::collections::BTreeMap::new();
    for entry in EWS_CATALOG_COVERAGE {
        let expected_status = contract_status_for_coverage_kind(entry.kind);
        *manifest_status_counts
            .entry(expected_status)
            .or_insert(0usize) += 1;
        let actual_status = contract_statuses
            .get(entry.operation)
            .copied()
            .unwrap_or("missing contract status");
        *contract_status_counts
            .entry(actual_status)
            .or_insert(0usize) += 1;
        assert_eq!(
            actual_status, expected_status,
            "{} contract status must match the EWS catalog coverage manifest",
            entry.operation
        );
    }
    assert_eq!(
        contract_status_counts, manifest_status_counts,
        "docs/architecture/ews-operation-contract.md status counts must match the EWS catalog coverage manifest"
    );

    let unsupported_reasons = unsupported_reason_map();
    let mut duplicate_unsupported_reasons = Vec::new();
    let mut seen_unsupported_reasons = std::collections::BTreeSet::new();
    for (operation, _) in EWS_UNSUPPORTED_REASONS {
        if !seen_unsupported_reasons.insert(*operation) {
            duplicate_unsupported_reasons.push(*operation);
        }
    }
    assert!(
        duplicate_unsupported_reasons.is_empty(),
        "duplicate EWS unsupported reasons: {duplicate_unsupported_reasons:?}"
    );

    let ews_tests_source = include_str!("ews.rs");
    let mut missing_soap_tests = Vec::new();
    for entry in EWS_CATALOG_COVERAGE {
        if !ews_tests_source.contains(&format!("async fn {}(", entry.test_name)) {
            missing_soap_tests.push((entry.operation, entry.test_name));
        }
    }
    assert!(
        missing_soap_tests.is_empty(),
        "every Microsoft EWS catalog operation must name an existing SOAP test: {missing_soap_tests:?}"
    );

    let unsupported: Vec<_> = EWS_CATALOG_COVERAGE
        .iter()
        .filter(|entry| entry.kind == EwsCatalogCoverageKind::Unsupported)
        .copied()
        .collect();
    let mut missing_unsupported_reasons = Vec::new();
    for entry in &unsupported {
        match unsupported_reasons.get(entry.operation) {
            Some(reason) if !reason.trim().is_empty() => {}
            _ => missing_unsupported_reasons.push(entry.operation),
        }
    }
    assert!(
        missing_unsupported_reasons.is_empty(),
        "unsupported EWS operations must have a tracked reason: {missing_unsupported_reasons:?}"
    );
    let unsupported_operations = unsupported
        .iter()
        .map(|entry| entry.operation)
        .collect::<std::collections::BTreeSet<_>>();
    let extra_unsupported_reasons = unsupported_reasons
        .keys()
        .copied()
        .filter(|operation| !unsupported_operations.contains(operation))
        .collect::<Vec<_>>();
    assert!(
        extra_unsupported_reasons.is_empty(),
        "unsupported reason table contains non-unsupported operations: {extra_unsupported_reasons:?}"
    );
    for entry in EWS_CATALOG_COVERAGE {
        let service = ExchangeService::new(FakeStore {
            session: Some(FakeStore::account()),
            ..Default::default()
        });
        let request = ews_catalog_gate_soap_request(entry.operation);
        let response = service
            .handle(&bearer_headers(), request.as_bytes())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{}", entry.operation);
        let body = response_text(response).await;
        assert!(
            body.contains(&format!("<m:{}Response", entry.operation)),
            "{} did not return an operation-shaped parseable SOAP response: {body}",
            entry.operation,
        );
        assert!(
            body.contains("ResponseClass=\"") && body.contains("<m:ResponseCode>"),
            "{} did not return a parseable EWS response class and code: {body}",
            entry.operation,
        );
        if entry.kind == EwsCatalogCoverageKind::Unsupported {
            let reason = unsupported_reasons
                .get(entry.operation)
                .copied()
                .unwrap_or("missing unsupported reason");
            assert!(
                body.contains("ResponseClass=\"Error\""),
                "{} did not return an explicit error response for tracked gap `{reason}`: {body}",
                entry.operation,
            );
            assert!(
                body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"),
                "{} did not return the unsupported EWS response code for tracked gap `{reason}`: {body}",
                entry.operation,
            );
            assert!(
                body.contains("is not implemented by the EWS MVP"),
                "{} did not document unsupported behavior in the SOAP payload for tracked gap `{reason}`: {body}",
                entry.operation,
            );
        }
    }

    let soap_evidence_count = EWS_CATALOG_COVERAGE.len();
    let behavioral_count = EWS_CATALOG_COVERAGE.len() - unsupported.len();
    println!(
        "EWS catalog gate coverage: SOAP evidence {}/{} ({:.1}%); canonical behavioral coverage {}/{} ({:.1}%); explicit unsupported coverage {}/{} ({:.1}%)",
        soap_evidence_count,
        documented.len(),
        percentage(soap_evidence_count, documented.len()),
        behavioral_count,
        documented.len(),
        percentage(behavioral_count, documented.len()),
        unsupported.len(),
        documented.len(),
        percentage(unsupported.len(), documented.len())
    );
}

fn documented_ews_operation_names() -> std::collections::BTreeSet<&'static str> {
    MICROSOFT_EWS_OPERATION_CATALOG.iter().copied().collect()
}

fn unsupported_reason_map() -> std::collections::BTreeMap<&'static str, &'static str> {
    EWS_UNSUPPORTED_REASONS.iter().copied().collect()
}

fn parity_matrix_ews_operation_names() -> std::collections::BTreeSet<&'static str> {
    include_str!("../../../../docs/architecture/ews-operation-contract.md")
        .lines()
        .filter_map(|line| {
            let rest = line.strip_prefix("| `")?;
            let (operation, _) = rest.split_once('`')?;
            Some(operation)
        })
        .collect()
}

fn contract_ews_operation_status_entries() -> Vec<(&'static str, &'static str)> {
    include_str!("../../../../docs/architecture/ews-operation-contract.md")
        .lines()
        .filter_map(|line| {
            let rest = line.strip_prefix("| `")?;
            let (operation, rest) = rest.split_once('`')?;
            let status = rest.strip_prefix(" | ")?.split(" |").next()?.trim();
            Some((operation, status))
        })
        .collect()
}

fn contract_status_for_coverage_kind(kind: EwsCatalogCoverageKind) -> &'static str {
    match kind {
        EwsCatalogCoverageKind::Behavioral => "Partial",
        EwsCatalogCoverageKind::Unsupported => "Explicitly unsupported",
    }
}

fn ews_catalog_gate_soap_request(operation: &str) -> String {
    format!(
        r#"<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types"><s:Body><m:{operation}/></s:Body></s:Envelope>"#
    )
}

fn percentage(part: usize, total: usize) -> f64 {
    if total == 0 {
        0.0
    } else {
        (part as f64 * 100.0) / total as f64
    }
}
