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
    assert!(body.contains("<t:FolderId Id=\"mailbox:44444444-4444-4444-4444-444444444444\" ChangeKey=\"ck-44444444-4444-4444-4444-444444444444\"/>"));
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
    assert!(body.contains("<t:FolderId Id=\"mailbox:44444444-4444-4444-4444-444444444444\" ChangeKey=\"ck-44444444-4444-4444-4444-444444444444\"/>"));
    assert!(
        body.find("<t:Create><t:Folder>").unwrap()
            < body.find("<t:Create><t:ContactsFolder>").unwrap()
    );
    assert!(body.contains("<t:ParentFolderId Id=\"msgfolderroot\" ChangeKey=\"root\"/>"));
    assert!(body.contains("<t:DisplayName>RCA Sync</t:DisplayName>"));
    assert!(body.contains("<t:UnreadCount>0</t:UnreadCount>"));
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
    assert!(body.contains("<t:FolderId Id=\"mailbox:44444444-4444-4444-4444-444444444444\" ChangeKey=\"ck-44444444-4444-4444-4444-444444444444\"/>"));
    assert!(body.contains("<t:TotalCount>0</t:TotalCount>"));
    assert_eq!(created_mailboxes.lock().unwrap()[0].name, "RCA Sync");
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
async fn delete_folder_uses_canonical_mailbox_destroy() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
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
async fn delete_folder_uses_canonical_public_folder_store() {
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
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert_eq!(
        deleted_public_folders.lock().unwrap().as_slice(),
        &[folder_id]
    );
    assert_eq!(public_folders.lock().unwrap()[0].lifecycle_state, "deleted");
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
    assert!(body.contains("<t:FolderId Id=\"mailbox:55555555-5555-5555-5555-555555555555\" ChangeKey=\"ck-55555555-5555-5555-5555-555555555555\"/>"));
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
    assert!(body.contains("<t:TimeZoneDefinition Id=\"W. Europe Standard Time\""));
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
async fn get_user_availability_returns_canonical_busy_events() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
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
async fn write_operations_return_ews_unsupported_errors() {
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
async fn update_item_updates_public_folder_item() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
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
                      <t:DueDate>2026-05-06T09:00:00Z</t:DueDate>
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
    assert!(body.contains("<t:CompleteDate>2026-05-06T10:00:00Z</t:CompleteDate>"));
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
async fn create_item_saveonly_stores_public_folder_post() {
    let public_folder_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
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
    assert_eq!(recorded[0].to[0].address, "carol@example.test");
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
async fn out_of_scope_bootstrap_operations_return_ews_unsupported_errors() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    for operation in [
        "GetRoomLists",
        "FindPeople",
        "ExpandDL",
        "GetDelegate",
        "GetUserConfiguration",
        "GetSharingMetadata",
        "GetSharingFolder",
    ] {
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
async fn pull_subscription_get_events_returns_created_for_empty_watermarked_mailbox() {
    let mailbox_id = "12121212-1212-1212-1212-121212121212";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            mailbox_id, "inbox", "Inbox",
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
    assert!(body.contains("<t:CreatedEvent>"));
    assert!(!body.contains("<t:StatusEvent>"));
    assert!(body.contains(&format!("<t:ParentFolderId Id=\"mailbox:{mailbox_id}\"")));
    assert!(body.contains("ChangeKey=\"notification\""));
}

#[tokio::test]
async fn pull_subscription_get_events_returns_queued_create_after_subscribe() {
    let mailbox_id = "55555555-5555-5555-5555-555555555555";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            mailbox_id, "inbox", "Inbox",
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

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:CreateItem MessageDisposition="SaveOnly">
                  <m:SavedItemFolderId><t:FolderId Id="mailbox:55555555-5555-5555-5555-555555555555"/></m:SavedItemFolderId>
                  <m:Items><t:Message><t:Subject>RCA pull create</t:Subject></t:Message></m:Items>
                </m:CreateItem>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));

    let request = format!(
        r#"<s:Envelope><s:Body><m:GetEvents><m:SubscriptionId>{subscription_id}</m:SubscriptionId><m:Watermark>{watermark}</m:Watermark></m:GetEvents></s:Body></s:Envelope>"#
    );
    let response = service
        .handle(&bearer_headers(), request.as_bytes())
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:CreatedEvent>"));
    assert!(body.contains("<t:ItemId Id=\"message:99999999-9999-9999-9999-999999999999\""));
    assert!(body.contains(&format!("<t:ParentFolderId Id=\"mailbox:{mailbox_id}\"")));
    assert!(!body.contains("<t:StatusEvent>"));
}

#[tokio::test]
async fn pull_subscription_get_events_returns_queued_delete_after_subscribe() {
    let mailbox_id = "66666666-6666-6666-6666-666666666666";
    let message_id = "77777777-7777-7777-7777-777777777777";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            mailbox_id, "inbox", "Inbox",
        )])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            message_id,
            mailbox_id,
            "inbox",
            "RCA pull delete",
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

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:DeleteItem DeleteType="HardDelete"><m:ItemIds><t:ItemId Id="message:77777777-7777-7777-7777-777777777777"/></m:ItemIds></m:DeleteItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));

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
async fn pull_subscription_watermark_replays_delete_after_resubscribe() {
    let mailbox_id = "88888888-8888-8888-8888-888888888888";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            mailbox_id, "inbox", "Inbox",
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
                    <t:FolderIds><t:FolderId Id="mailbox:88888888-8888-8888-8888-888888888888"/></t:FolderIds>
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

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:CreateItem MessageDisposition="SaveOnly">
                  <m:SavedItemFolderId><t:FolderId Id="mailbox:88888888-8888-8888-8888-888888888888"/></m:SavedItemFolderId>
                  <m:Items><t:Message><t:Subject>RCA replay delete</t:Subject></t:Message></m:Items>
                </m:CreateItem>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));

    let request = format!(
        r#"<s:Envelope><s:Body><m:GetEvents><m:SubscriptionId>{subscription_id}</m:SubscriptionId><m:Watermark>{watermark}</m:Watermark></m:GetEvents></s:Body></s:Envelope>"#
    );
    let response = service
        .handle(&bearer_headers(), request.as_bytes())
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:CreatedEvent>"));
    let created_watermark = body
        .split("<t:CreatedEvent>")
        .nth(1)
        .and_then(|rest| rest.split("<t:Watermark>").nth(1))
        .and_then(|rest| rest.split("</t:Watermark>").next())
        .unwrap()
        .to_string();

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:DeleteItem DeleteType="HardDelete"><m:ItemIds><t:ItemId Id="message:99999999-9999-9999-9999-999999999999"/></m:ItemIds></m:DeleteItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));

    let subscribe = format!(
        r#"
        <s:Envelope>
          <s:Body>
            <m:Subscribe>
              <m:PullSubscriptionRequest>
                <t:FolderIds><t:FolderId Id="mailbox:{mailbox_id}"/></t:FolderIds>
                <t:EventTypes><t:EventType>DeletedEvent</t:EventType></t:EventTypes>
                <t:Watermark>{created_watermark}</t:Watermark>
                <t:Timeout>10</t:Timeout>
              </m:PullSubscriptionRequest>
            </m:Subscribe>
          </s:Body>
        </s:Envelope>
        "#
    );
    let response = service
        .handle(&bearer_headers(), subscribe.as_bytes())
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    let replay_subscription_id = body
        .split("<m:SubscriptionId>")
        .nth(1)
        .and_then(|rest| rest.split("</m:SubscriptionId>").next())
        .unwrap()
        .to_string();
    let replay_watermark = body
        .split("<m:Watermark>")
        .nth(1)
        .and_then(|rest| rest.split("</m:Watermark>").next())
        .unwrap()
        .to_string();
    assert_eq!(replay_watermark, created_watermark);

    let request = format!(
        r#"<s:Envelope><s:Body><m:GetEvents><m:SubscriptionId>{replay_subscription_id}</m:SubscriptionId><m:Watermark>{replay_watermark}</m:Watermark></m:GetEvents></s:Body></s:Envelope>"#
    );
    let response = service
        .handle(&bearer_headers(), request.as_bytes())
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:DeletedEvent>"));
    assert!(body.contains("<t:ItemId Id=\"message:99999999-9999-9999-9999-999999999999\""));
    assert!(!body.contains("<t:CreatedEvent>"));
}

#[tokio::test]
async fn pull_subscription_get_events_returns_new_mail_for_watermarked_mailbox() {
    let mailbox_id = "13131313-1313-1313-1313-131313131313";
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
                    <t:EventTypes><t:EventType>NewMailEvent</t:EventType></t:EventTypes>
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
    assert!(body.contains("<t:CreatedEvent>"));
    assert!(body.contains("<t:NewMailEvent>"));
    assert!(body.contains(&format!("<t:ItemId Id=\"message:{message_id}\"")));
    assert!(body.contains(&format!("<t:ParentFolderId Id=\"mailbox:{mailbox_id}\"")));
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
        r#"require ["vacation"]; vacation "Away";"#.to_string(),
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
async fn unknown_ews_operations_return_parseable_invalid_operation_errors() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    for operation in [
        "SendItem",
        "GetMailTips",
        "GetInboxRules",
        "ConvertId",
        "FindConversation",
        "GetConversationItems",
        "GetStreamingEvents",
    ] {
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
    assert!(
        body.contains("<m:SyncState>contacts:default:v2:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb=ck-")
    );

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:DeleteItem DeleteType="HardDelete"><m:ItemIds><t:ItemId Id="contact:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"/></m:ItemIds></m:DeleteItem></s:Body></s:Envelope>"#,
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
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="default"/></m:SyncFolderId><m:SyncState>contacts:default:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb</m:SyncState></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(
        body.contains("<t:Delete><t:ItemId Id=\"contact:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb\"")
    );
    assert!(body.contains("<m:SyncState>contacts:default:v2:0</m:SyncState>"));
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
async fn sync_folder_items_returns_contact_update_for_legacy_id_only_sync_state() {
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
    assert!(body.contains("<t:Update><t:Contact>"));
    assert!(body.contains("contact:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"));
    assert!(body.contains("<t:DisplayName>Updated RCA Contact</t:DisplayName>"));
    assert!(
        body.contains("<m:SyncState>contacts:default:v2:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb=ck-")
    );
}

#[tokio::test]
async fn sync_folder_items_returns_contact_update_for_legacy_keyed_sync_state() {
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
    assert!(body.contains("<t:Update><t:Contact>"));
    assert!(body.contains("contact:e77d919d-df4f-488d-bb4c-2defdfd8d6ec"));
    assert!(
        body.contains("<m:SyncState>contacts:default:v2:e77d919d-df4f-488d-bb4c-2defdfd8d6ec=ck-")
    );
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
    assert!(current_sync_state.starts_with("contacts:default:v2:"));

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
    assert!(body.contains(&format!("<m:SyncState>{current_sync_state}</m:SyncState>")));
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

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:UpdateItem ConflictResolution="AlwaysOverwrite">
                  <m:ItemChanges>
                    <t:ItemChange>
                      <t:ItemId Id="contact:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"/>
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
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:UpdateItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:DisplayName>Updated RCA Contact</t:DisplayName>"));

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

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:UpdateItem ConflictResolution="AlwaysOverwrite">
                  <m:ItemChanges>
                    <t:ItemChange>
                      <t:ItemId Id="contact:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"/>
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
                      <t:Start>2026-05-04T09:30:00Z</t:Start>
                      <t:End>2026-05-04T10:15:00Z</t:End>
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
    assert!(body.contains("<t:Start>2026-05-04T09:30:00Z</t:Start>"));
    assert!(body.contains("<t:End>2026-05-04T10:15:00Z</t:End>"));
    assert!(body.contains("<t:WeeklyRecurrence>"));
    assert!(body.contains("<t:DaysOfWeek>Monday Wednesday</t:DaysOfWeek>"));
    assert!(body.contains("<t:NumberOfOccurrences>5</t:NumberOfOccurrences>"));
    assert!(body.contains("<t:RequiredAttendees>"));
    assert!(body.contains("<t:OptionalAttendees>"));
    assert!(body.contains("<t:EmailAddress>bob@example.test</t:EmailAddress>"));
    assert!(body.contains("<t:EmailAddress>carol@example.test</t:EmailAddress>"));
    assert!(body.contains("<t:ResponseType>Accept</t:ResponseType>"));
    assert!(body.contains("<t:ResponseType>Tentative</t:ResponseType>"));
    assert!(
        body.contains("<m:SyncState>calendar:default:v2:cccccccc-cccc-cccc-cccc-cccccccccccc=ck-")
    );

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:DeleteItem DeleteType="HardDelete"><m:ItemIds><t:ItemId Id="event:cccccccc-cccc-cccc-cccc-cccccccccccc"/></m:ItemIds></m:DeleteItem></s:Body></s:Envelope>"#,
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
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="default"/></m:SyncFolderId><m:SyncState>calendar:default:cccccccc-cccc-cccc-cccc-cccccccccccc</m:SyncState></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:Delete><t:ItemId Id=\"event:cccccccc-cccc-cccc-cccc-cccccccccccc\""));
    assert!(body.contains("<m:SyncState>calendar:default:v2:0</m:SyncState>"));
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
    assert!(
        body.contains("<m:SyncState>mailbox:44444444-4444-4444-4444-444444444444:</m:SyncState>")
    );
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
    assert!(
        body.contains("<m:SyncState>mailbox:44444444-4444-4444-4444-444444444444:</m:SyncState>")
    );
}

#[tokio::test]
async fn sync_folder_items_uses_mailbox_id_from_sync_state_when_folder_id_is_omitted() {
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
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncState>mailbox:44444444-4444-4444-4444-444444444444:</m:SyncState></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<t:Create><t:Message>"));
    assert!(body.contains("message:99999999-9999-9999-9999-999999999999"));
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
    let emails = Arc::new(Mutex::new(vec![FakeStore::email(
        "99999999-9999-9999-9999-999999999999",
        "44444444-4444-4444-4444-444444444444",
        "custom",
        "RCA folder item",
    )]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        emails: emails.clone(),
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
    assert!(body.contains("<m:SyncState>mailbox:44444444-4444-4444-4444-444444444444:99999999-9999-9999-9999-999999999999</m:SyncState>"));

    emails.lock().unwrap().clear();
    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncState>mailbox:44444444-4444-4444-4444-444444444444:99999999-9999-9999-9999-999999999999</m:SyncState><m:SyncFolderId><t:FolderId Id="mailbox:44444444-4444-4444-4444-444444444444"/></m:SyncFolderId></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(
        body.contains("<t:Delete><t:ItemId Id=\"message:99999999-9999-9999-9999-999999999999\"")
    );
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
    assert!(body.contains("<m:SyncState>mailbox:55555555-5555-5555-5555-555555555555:88888888-8888-8888-8888-888888888888</m:SyncState>"));
}

#[tokio::test]
async fn find_item_lists_public_folder_items() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
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
async fn sync_folder_items_reports_public_folder_items() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
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
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="public-folder:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"/></m:SyncFolderId></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:SyncFolderItemsResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:Create><t:Message>"));
    assert!(body.contains("public-folder-item:abababab-abab-abab-abab-abababababab"));
    assert!(body.contains("<m:SyncState>public-folder:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:v2:abababab-abab-abab-abab-abababababab="));
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
    assert!(body.contains("<t:Content>aGVsbG8=</t:Content>"));
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
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:AttachmentId Id=\"attachment:99999999-9999-9999-9999-999999999999:cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcdcd\"/>"));
    assert!(body.contains("RootItemId=\"message:99999999-9999-9999-9999-999999999999\""));
    assert_eq!(created_attachments.lock().unwrap().len(), 1);
    let attachment = &created_attachments.lock().unwrap()[0];
    assert_eq!(attachment.file_name, "brief.pdf");
    assert_eq!(attachment.media_type, "application/pdf");
    assert_eq!(attachment.blob_bytes, b"hello");
    assert_eq!(
        attachments.lock().unwrap().get(&message_id).unwrap().len(),
        1
    );
    assert!(emails.lock().unwrap()[0].has_attachments);
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
}

#[tokio::test]
async fn move_item_moves_public_folder_item_to_target_public_folder() {
    let source_item_id = Uuid::parse_str("abababab-abab-abab-abab-abababababab").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
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
    assert!(body.contains("message:77777777-7777-7777-7777-777777777777"));
    assert!(body.contains("mailbox:55555555-5555-5555-5555-555555555555"));
    assert_eq!(
        copied_emails.lock().unwrap().as_slice(),
        &[(message_id, target_mailbox_id)]
    );
}

#[tokio::test]
async fn copy_item_copies_public_folder_item_to_target_public_folder() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
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
    assert!(body.contains("<t:Start>2026-05-04T09:30:00Z</t:Start>"));
    assert!(body.contains("<t:End>2026-05-04T10:15:00Z</t:End>"));
}
